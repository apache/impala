// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#include "exec/kudu/kudu-scanner.h"

#include <string>
#include <vector>

#include <glog/logging.h>
#include <kudu/client/resource_metrics.h>
#include <kudu/client/row_result.h>
#include <kudu/client/value.h>
#include <thrift/protocol/TDebugProtocol.h>

#include "common/names.h"
#include "exec/exec-node.inline.h"
#include "exec/kudu/kudu-util.h"
#include "exprs/scalar-expr-evaluator.h"
#include "exprs/scalar-expr.h"
#include "exprs/slot-ref.h"
#include "gutil/gscoped_ptr.h"
#include "gutil/strings/substitute.h"
#include "kudu/util/block_bloom_filter.h"
#include "kudu/util/logging.h"
#include "kudu/util/slice.h"
#include "runtime/mem-pool.h"
#include "runtime/mem-tracker.h"
#include "runtime/raw-value.h"
#include "runtime/row-batch.h"
#include "runtime/runtime-filter.inline.h"
#include "runtime/runtime-state.h"
#include "runtime/string-value.h"
#include "runtime/timestamp-value.inline.h"
#include "runtime/tuple-row.h"
#include "util/bloom-filter.h"
#include "util/debug-util.h"
#include "util/jni-util.h"
#include "util/min-max-filter.h"
#include "util/periodic-counter-updater.h"
#include "util/runtime-profile-counters.h"

using kudu::client::KuduClient;
using kudu::client::KuduPredicate;
using kudu::client::KuduScanBatch;
using kudu::client::KuduSchema;
using kudu::client::KuduTable;
using kudu::client::KuduValue;
using kudu::client::ResourceMetrics;

DEFINE_string(kudu_read_mode, "READ_LATEST", "(Advanced) Sets the Kudu scan ReadMode. "
    "Supported Kudu read modes are READ_LATEST and READ_AT_SNAPSHOT. Can be overridden "
    "with the query option of the same name.");
DEFINE_int32(kudu_scanner_keep_alive_period_sec, 15,
    "The period at which Kudu Scanners should send keep-alive requests to the tablet "
    "server to ensure that scanners do not time out.");

DECLARE_int32(kudu_operation_timeout_ms);

namespace impala {

KuduScanner::KuduScanner(KuduScanNodeBase* scan_node, RuntimeState* state)
  : scan_node_(scan_node),
    state_(state),
    expr_perm_pool_(new MemPool(scan_node->expr_mem_tracker())),
    expr_results_pool_(new MemPool(scan_node->expr_mem_tracker())),
    cur_kudu_batch_num_read_(0),
    last_alive_time_micros_(0) {
}

Status KuduScanner::Open() {
  for (int i = 0; i < scan_node_->tuple_desc()->slots().size(); ++i) {
    const SlotDescriptor* slot = scan_node_->tuple_desc()->slots()[i];
    if (slot->type().type == TYPE_TIMESTAMP) {
      timestamp_slots_.push_back(slot);
    } else if (slot->type().type == TYPE_VARCHAR) {
      varchar_slots_.push_back(slot);
    }
  }
  return ScalarExprEvaluator::Clone(&obj_pool_, state_, expr_perm_pool_.get(),
      expr_results_pool_.get(), scan_node_->conjunct_evals(), &conjunct_evals_);
}

void KuduScanner::KeepKuduScannerAlive() {
  if (scanner_ == nullptr) {
    return;
  }
  int64_t now = MonotonicMicros();
  int64_t keepalive_us = FLAGS_kudu_scanner_keep_alive_period_sec * 1e6;
  if (now < last_alive_time_micros_ + keepalive_us) {
    return;
  }
  // If we fail to send a keepalive, it isn't a big deal. The Kudu
  // client code doesn't handle cross-replica failover or retries when
  // the server is busy, so it's better to just ignore errors here. In
  // the worst case, we will just fail next time we try to fetch a batch
  // if the scan is unrecoverable.
  kudu::Status s = scanner_->KeepAlive();
  if (!s.ok()) {
    KLOG_EVERY_N_SECS(WARNING, 60) << BuildErrorString(
        Substitute("$0: unable to keep scanner alive", s.ToString()).c_str());
    return;
  }
  last_alive_time_micros_ = now;
}

Status KuduScanner::GetNextWithCountStarOptimization(RowBatch* row_batch, bool* eos) {
  int64_t counter = 0;
  while (scanner_->HasMoreRows()) {
    RETURN_IF_CANCELLED(state_);
    RETURN_IF_ERROR(GetNextScannerBatch());

    cur_kudu_batch_num_read_ = static_cast<int64_t>(cur_kudu_batch_.NumRows());
    counter += cur_kudu_batch_num_read_;
  }
  *eos = true;
  int64_t tuple_buffer_size;
  uint8_t* tuple_buffer;
  int capacity = 1;
  SCOPED_TIMER(scan_node_->materialize_tuple_timer());
  RETURN_IF_ERROR(row_batch->ResizeAndAllocateTupleBuffer(state_,
      row_batch->tuple_data_pool(), row_batch->row_desc()->GetRowSize(), &capacity,
      &tuple_buffer_size, &tuple_buffer));
  Tuple* tuple = reinterpret_cast<Tuple*>(tuple_buffer);
  Tuple::ClearNullBits(tuple, scan_node_->tuple_desc()->null_bytes_offset(),
      scan_node_->tuple_desc()->num_null_bytes());
  int64_t* counter_slot = tuple->GetBigIntSlot(scan_node_->count_star_slot_offset());
  *counter_slot = counter;
  TupleRow* dst_row = row_batch->GetRow(row_batch->AddRow());
  dst_row->SetTuple(0, tuple);
  row_batch->CommitLastRow();

  CloseCurrentClientScanner();
  return Status::OK();
}

Status KuduScanner::GetNext(RowBatch* row_batch, bool* eos) {
  // Optimized scanning for count(*), only write the NumRows
  if (scan_node_->optimize_count_star()) {
    return GetNextWithCountStarOptimization(row_batch, eos);
  }
  int64_t tuple_buffer_size;
  uint8_t* tuple_buffer;
  RETURN_IF_ERROR(
      row_batch->ResizeAndAllocateTupleBuffer(state_, &tuple_buffer_size, &tuple_buffer));

  // Main scan loop:
  // Tries to fill 'row_batch' with rows from cur_kudu_batch_.
  // If there are no rows to decode, tries to get the next row batch from kudu.
  // If this scanner has no more rows, the scanner is closed and eos is returned.
  Tuple* tuple = reinterpret_cast<Tuple*>(tuple_buffer);
  while (!*eos) {
    RETURN_IF_CANCELLED(state_);

    if (cur_kudu_batch_num_read_ < cur_kudu_batch_.NumRows()) {
      RETURN_IF_ERROR(DecodeRowsIntoRowBatch(row_batch, &tuple));
      if (row_batch->AtCapacity()) break;
    }

    if (scanner_->HasMoreRows() && !scan_node_->ReachedLimitShared()) {
      RETURN_IF_ERROR(GetNextScannerBatch());
      continue;
    }

    CloseCurrentClientScanner();
    *eos = true;
  }
  return Status::OK();
}

void KuduScanner::Close() {
  if (scanner_) CloseCurrentClientScanner();
  ScalarExprEvaluator::Close(conjunct_evals_, state_);
  expr_perm_pool_->FreeAll();
  expr_results_pool_->FreeAll();
}

Status KuduScanner::OpenNextScanToken(const string& scan_token, bool* eos) {
  DCHECK(scanner_ == nullptr);
  kudu::client::KuduScanner* scanner;
  KUDU_RETURN_IF_ERROR(kudu::client::KuduScanToken::DeserializeIntoScanner(
                           scan_node_->kudu_client(), scan_token, &scanner),
      BuildErrorString("Unable to deserialize scan token"));
  scanner_.reset(scanner);

  if (state_->query_options().kudu_replica_selection
      == TKuduReplicaSelection::LEADER_ONLY) {
    KUDU_RETURN_IF_ERROR(scanner_->SetSelection(kudu::client::KuduClient::LEADER_ONLY),
        BuildErrorString("Could not set replica selection"));
  }
  kudu::client::KuduScanner::ReadMode mode;
  RETURN_IF_ERROR(StringToKuduReadMode(FLAGS_kudu_read_mode, &mode));
  if (state_->query_options().kudu_read_mode != TKuduReadMode::DEFAULT) {
    RETURN_IF_ERROR(StringToKuduReadMode(
        PrintValue(state_->query_options().kudu_read_mode), &mode));
  }
  KUDU_RETURN_IF_ERROR(
      scanner_->SetReadMode(mode), BuildErrorString("Could not set scanner ReadMode"));
  if (state_->query_options().kudu_snapshot_read_timestamp_micros > 0) {
    KUDU_RETURN_IF_ERROR(scanner_->SetSnapshotMicros(
        state_->query_options().kudu_snapshot_read_timestamp_micros),
        BuildErrorString("Could not set snapshot timestamp"));
  }
  KUDU_RETURN_IF_ERROR(scanner_->SetTimeoutMillis(FLAGS_kudu_operation_timeout_ms),
      BuildErrorString("Could not set scanner timeout"));
  VLOG_ROW << "Starting KuduScanner with ReadMode=" << mode
           << " timeout=" << FLAGS_kudu_operation_timeout_ms
           << " node with id=" << scan_node_->id()
           << " Kudu table=" << scan_node_->table_desc()->table_name();

  if (!timestamp_slots_.empty()) {
    uint64_t row_format_flags =
        kudu::client::KuduScanner::PAD_UNIXTIME_MICROS_TO_16_BYTES;
    scanner_->SetRowFormatFlags(row_format_flags);
  }

  if (scan_node_->filter_ctxs_.size() > 0) {
    for (const FilterContext& ctx : scan_node_->filter_ctxs_) {
      if (!ctx.filter->HasFilter() || ctx.filter->AlwaysTrue()) {
        // If it's always true, the filter won't actually remove any rows so we
        // don't need to push it down to Kudu.
        continue;
      } else if (ctx.filter->AlwaysFalse()) {
        // We can skip this entire scan if it's always false.
        CloseCurrentClientScanner();
        *eos = true;
        return Status::OK();
      }

      auto it = ctx.filter->filter_desc().planid_to_target_ndx.find(scan_node_->id());
      const TRuntimeFilterTargetDesc& target_desc =
          ctx.filter->filter_desc().targets[it->second];
      const string& col_name = target_desc.kudu_col_name;
      DCHECK(col_name != "");

      if (ctx.filter->is_bloom_filter()) {
        BloomFilter* filter = ctx.filter->get_bloom_filter();
        DCHECK(filter != nullptr);

        kudu::BlockBloomFilter* bbf = filter->GetBlockBloomFilter();
        vector<kudu::Slice> bbf_vec = {
            kudu::Slice(reinterpret_cast<const uint8_t*>(bbf), sizeof(*bbf))};

        KUDU_RETURN_IF_ERROR(
            scanner_->AddConjunctPredicate(
                scanner_->GetKuduTable()->NewInBloomFilterPredicate(col_name, bbf_vec)),
            BuildErrorString("Failed to add bloom filter predicate"));
      } else {
        DCHECK(ctx.filter->is_min_max_filter());
        MinMaxFilter* filter = ctx.filter->get_min_max();
        DCHECK(filter != nullptr);

        const void* min = filter->GetMin();
        const void* max = filter->GetMax();
        // If the type of the filter is not the same as the type of the target column,
        // there must be an implicit integer cast and we need to ensure the min/max we
        // pass to Kudu are within the range of the target column.
        int64_t int_min;
        int64_t int_max;
        const ColumnType& col_type = ColumnType::FromThrift(target_desc.kudu_col_type);
        if (col_type.type != filter->type()) {
          DCHECK(col_type.IsIntegerType());

          if (!filter->GetCastIntMinMax(col_type, &int_min, &int_max)) {
            // The min/max for this filter is outside the range for the target column,
            // so all rows are filtered out and we can skip the scan.
            CloseCurrentClientScanner();
            *eos = true;
            return Status::OK();
          }
          min = &int_min;
          max = &int_max;
        }

        TimestampValue ts_min;
        TimestampValue ts_max;
        if (state_->query_options().convert_kudu_utc_timestamps &&
            col_type.type == TYPE_TIMESTAMP) {
          ts_min = *reinterpret_cast<const TimestampValue*>(min);
          ts_max = *reinterpret_cast<const TimestampValue*>(max);
          ConvertLocalTimeMinStatToUTC(&ts_min);
          ConvertLocalTimeMaxStatToUTC(&ts_max);
          min = &ts_min;
          max = &ts_max;
        }

        KuduValue* min_value;
        RETURN_IF_ERROR(CreateKuduValue(col_type, min, &min_value));
        KUDU_RETURN_IF_ERROR(scanner_->AddConjunctPredicate(
          scanner_->GetKuduTable()->NewComparisonPredicate(
              col_name, KuduPredicate::ComparisonOp::GREATER_EQUAL, min_value)),
            BuildErrorString("Failed to add min predicate"));

        KuduValue* max_value;
        RETURN_IF_ERROR(CreateKuduValue(col_type, max, &max_value));
        KUDU_RETURN_IF_ERROR(scanner_->AddConjunctPredicate(
            scanner_->GetKuduTable()->NewComparisonPredicate(
                col_name, KuduPredicate::ComparisonOp::LESS_EQUAL, max_value)),
            BuildErrorString("Failed to add max predicate"));
      }
    }
  }

  if (scan_node_->limit() != -1 && conjunct_evals_.empty()) {
    KUDU_RETURN_IF_ERROR(scanner_->SetLimit(scan_node_->limit()),
        BuildErrorString("Failed to set limit on scan"));
  }

  {
    SCOPED_TIMER2(state_->total_storage_wait_timer(), scan_node_->kudu_client_time());
    KUDU_RETURN_IF_ERROR(scanner_->Open(), BuildErrorString("Unable to open scanner"));
  }
  *eos = false;
  return Status::OK();
}

void KuduScanner::CloseCurrentClientScanner() {
  DCHECK_NOTNULL(scanner_.get());

  std::map<std::string, int64_t> metrics = scanner_->GetResourceMetrics().Get();
  COUNTER_ADD(scan_node_->bytes_read_counter(), metrics["bytes_read"]);
  COUNTER_ADD(
      scan_node_->kudu_scanner_total_duration_time(), metrics["total_duration_nanos"]);
  COUNTER_ADD(
      scan_node_->kudu_scanner_queue_duration_time(), metrics["queue_duration_nanos"]);
  COUNTER_ADD(scan_node_->kudu_scanner_cpu_user_time(), metrics["cpu_user_nanos"]);
  COUNTER_ADD(scan_node_->kudu_scanner_cpu_sys_time(), metrics["cpu_system_nanos"]);
  COUNTER_ADD(
      scan_node_->kudu_scanner_cfile_cache_hit_bytes(), metrics["cfile_cache_hit_bytes"]);
  COUNTER_ADD(scan_node_->kudu_scanner_cfile_cache_miss_bytes(),
      metrics["cfile_cache_miss_bytes"]);
  scanner_->Close();
  scanner_.reset();
}

Status KuduScanner::HandleEmptyProjection(RowBatch* row_batch) {
  int num_rows_remaining = cur_kudu_batch_.NumRows() - cur_kudu_batch_num_read_;
  int rows_to_add = std::min(row_batch->capacity() - row_batch->num_rows(),
      num_rows_remaining);
  int num_to_commit = 0;
  if (LIKELY(conjunct_evals_.empty())) {
    num_to_commit = rows_to_add;
  } else {
    for (int i = 0; i < rows_to_add; ++i) {
      if (ExecNode::EvalConjuncts(conjunct_evals_.data(),
              conjunct_evals_.size(), nullptr)) {
        ++num_to_commit;
      }
    }
  }
  for (int i = 0; i < num_to_commit; ++i) {
    // IMPALA-6258: Initialize tuple ptrs to non-null value
    TupleRow* row = row_batch->GetRow(row_batch->AddRow());
    row->SetTuple(0, Tuple::POISON);
    row_batch->CommitLastRow();
  }
  cur_kudu_batch_num_read_ += rows_to_add;
  return Status::OK();
}

Status KuduScanner::DecodeRowsIntoRowBatch(RowBatch* row_batch, Tuple** tuple_mem) {
  SCOPED_TIMER(scan_node_->materialize_tuple_timer());
  // Short-circuit for empty projection cases.
  if (scan_node_->tuple_desc()->slots().empty()) {
    return HandleEmptyProjection(row_batch);
  }

  // Iterate through the Kudu rows, evaluate conjuncts and deep-copy survivors into
  // 'row_batch'.
  bool has_conjuncts = !conjunct_evals_.empty();
  int num_rows = cur_kudu_batch_.NumRows();

  for (int krow_idx = cur_kudu_batch_num_read_; krow_idx < num_rows; ++krow_idx) {
    Tuple* kudu_tuple = const_cast<Tuple*>(
        reinterpret_cast<const Tuple*>(cur_kudu_batch_.direct_data().data()
            + (krow_idx * scan_node_->row_desc()->GetRowSize())));
    ++cur_kudu_batch_num_read_;

    // Kudu tuples containing TIMESTAMP columns (UNIXTIME_MICROS in Kudu, stored as an
    // int64) have 8 bytes of padding following the timestamp. Because this padding is
    // provided, Impala can convert these unixtime values to Impala's TimestampValue
    // format in place and copy the rows to Impala row batches.
    // TODO: avoid mem copies with a Kudu mem 'release' mechanism, attaching mem to the
    // batch.
    // TODO: consider codegen for this per-timestamp col fixup
    for (const SlotDescriptor* slot : timestamp_slots_) {
      DCHECK(slot->type().type == TYPE_TIMESTAMP);
      if (slot->is_nullable() && kudu_tuple->IsNull(slot->null_indicator_offset())) {
        continue;
      }
      int64_t ts_micros = *reinterpret_cast<int64_t*>(
          kudu_tuple->GetSlot(slot->tuple_offset()));

      TimestampValue tv;
      if (state_->query_options().convert_kudu_utc_timestamps) {
        tv = TimestampValue::FromUnixTimeMicros(ts_micros, state_->local_time_zone());
      } else {
        tv = TimestampValue::UtcFromUnixTimeMicros(ts_micros);
      }

      if (tv.HasDateAndTime()) {
        RawValue::Write(&tv, kudu_tuple, slot, nullptr);
      } else {
        kudu_tuple->SetNull(slot->null_indicator_offset());
        RETURN_IF_ERROR(state_->LogOrReturnError(
            ErrorMsg::Init(TErrorCode::KUDU_TIMESTAMP_OUT_OF_RANGE,
              scan_node_->table_desc()->table_name(),
              scanner_->GetKuduTable()->schema().Column(slot->col_pos()).name())));
      }
    }

    // Kudu tuples containing VARCHAR columns use characters instead of bytes to limit
    // the length. In the case of ASCII values there is no difference. However, if
    // multi-byte characters are written to Kudu the length could be longer than allowed.
    // This checks the actual length and truncates the value length if it is too long.
    // TODO(IMPALA-5675): Remove this when Impala supports UTF-8 character VARCHAR length.
    for (const SlotDescriptor* slot : varchar_slots_) {
      DCHECK(slot->type().type == TYPE_VARCHAR);
      if (slot->is_nullable() && kudu_tuple->IsNull(slot->null_indicator_offset())) {
        continue;
      }
      StringValue* sv = reinterpret_cast<StringValue*>(
          kudu_tuple->GetSlot(slot->tuple_offset()));
      int src_len = sv->Len();
      int dst_len = slot->type().len;
      if (src_len > dst_len) {
        sv->SetLen(dst_len);
      }
    }

    // Evaluate the conjuncts that haven't been pushed down to Kudu. Conjunct evaluation
    // is performed directly on the Kudu tuple because its memory layout is identical to
    // Impala's. We only copy the surviving tuples to Impala's output row batch.
    if (has_conjuncts && !ExecNode::EvalConjuncts(conjunct_evals_.data(),
            conjunct_evals_.size(), reinterpret_cast<TupleRow*>(&kudu_tuple))) {
      continue;
    }
    // Deep copy the tuple, set it in a new row, and commit the row.
    kudu_tuple->DeepCopy(*tuple_mem, *scan_node_->tuple_desc(),
        row_batch->tuple_data_pool());
    TupleRow* row = row_batch->GetRow(row_batch->AddRow());
    row->SetTuple(0, *tuple_mem);
    row_batch->CommitLastRow();
    // If we've reached the capacity, or the LIMIT for the scan, return.
    if (row_batch->AtCapacity() || scan_node_->ReachedLimitShared()) break;
    // Move to the next tuple in the tuple buffer.
    *tuple_mem = next_tuple(*tuple_mem);
  }
  expr_results_pool_->Clear();

  // Check the status in case an error status was set during conjunct evaluation.
  return state_->GetQueryStatus();
}

Status KuduScanner::GetNextScannerBatch() {
  SCOPED_TIMER2(state_->total_storage_wait_timer(), scan_node_->kudu_client_time());
  int64_t now = MonotonicMicros();
  KUDU_RETURN_IF_ERROR(scanner_->NextBatch(&cur_kudu_batch_),
      BuildErrorString("Unable to advance iterator"));
  COUNTER_ADD(scan_node_->kudu_round_trips(), 1);
  cur_kudu_batch_num_read_ = 0;
  COUNTER_ADD(scan_node_->rows_read_counter(), cur_kudu_batch_.NumRows());
  last_alive_time_micros_ = now;
  return Status::OK();
}

string KuduScanner::BuildErrorString(const char* msg) const {
  return Substitute("$0 for node with id '$1' for Kudu table '$2'",
      msg, scan_node_->id(), scan_node_->table_desc()->table_name());
}

void KuduScanner::ConvertLocalTimeMinStatToUTC(TimestampValue* v) const {
  if (!v->HasDateAndTime()) return;
  TimestampValue pre_repeated_utc_time;
  const Timezone* local_tz = state_->local_time_zone();
  v->LocalToUtc(*local_tz, &pre_repeated_utc_time);
  if (pre_repeated_utc_time.HasDateAndTime()) *v = pre_repeated_utc_time;
}

void KuduScanner::ConvertLocalTimeMaxStatToUTC(TimestampValue* v) const {
  if (!v->HasDateAndTime()) return;
  TimestampValue post_repeated_utc_time;
  const Timezone* local_tz = state_->local_time_zone();
  v->LocalToUtc(*local_tz, nullptr, &post_repeated_utc_time);
  if (post_repeated_utc_time.HasDateAndTime()) *v = post_repeated_utc_time;
}

}  // namespace impala
