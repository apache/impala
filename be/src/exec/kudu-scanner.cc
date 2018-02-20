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

#include "exec/kudu-scanner.h"

#include <kudu/client/row_result.h>
#include <kudu/client/value.h>
#include <thrift/protocol/TDebugProtocol.h>
#include <vector>
#include <string>

#include "exec/kudu-util.h"
#include "exprs/scalar-expr.h"
#include "exprs/scalar-expr-evaluator.h"
#include "exprs/slot-ref.h"
#include "runtime/mem-pool.h"
#include "runtime/mem-tracker.h"
#include "runtime/raw-value.h"
#include "runtime/runtime-filter.h"
#include "runtime/runtime-state.h"
#include "runtime/row-batch.h"
#include "runtime/string-value.h"
#include "runtime/timestamp-value.inline.h"
#include "runtime/tuple-row.h"
#include "gutil/gscoped_ptr.h"
#include "gutil/strings/substitute.h"
#include "util/jni-util.h"
#include "util/min-max-filter.h"
#include "util/periodic-counter-updater.h"
#include "util/runtime-profile-counters.h"

#include "common/names.h"

using kudu::client::KuduClient;
using kudu::client::KuduPredicate;
using kudu::client::KuduScanBatch;
using kudu::client::KuduSchema;
using kudu::client::KuduTable;
using kudu::client::KuduValue;

DEFINE_string(kudu_read_mode, "READ_LATEST", "(Advanced) Sets the Kudu scan ReadMode. "
    "Supported Kudu read modes are READ_LATEST and READ_AT_SNAPSHOT.");
DEFINE_bool(pick_only_leaders_for_tests, false,
            "Whether to pick only leader replicas, for tests purposes only.");
DEFINE_int32(kudu_scanner_keep_alive_period_sec, 15,
    "The period at which Kudu Scanners should send keep-alive requests to the tablet "
    "server to ensure that scanners do not time out.");

DECLARE_int32(kudu_operation_timeout_ms);

namespace impala {

const string MODE_READ_AT_SNAPSHOT = "READ_AT_SNAPSHOT";

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
    if (slot->type().type != TYPE_TIMESTAMP) continue;
    timestamp_slots_.push_back(slot);
  }
  return ScalarExprEvaluator::Clone(&obj_pool_, state_, expr_perm_pool_.get(),
      expr_results_pool_.get(), scan_node_->conjunct_evals(), &conjunct_evals_);
}

void KuduScanner::KeepKuduScannerAlive() {
  if (scanner_ == NULL) return;
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
    VLOG(1) << "Unable to keep the Kudu scanner alive: " << s.ToString();
    return;
  }
  last_alive_time_micros_ = now;
}

Status KuduScanner::GetNext(RowBatch* row_batch, bool* eos) {
  int64_t tuple_buffer_size;
  uint8_t* tuple_buffer;
  RETURN_IF_ERROR(
      row_batch->ResizeAndAllocateTupleBuffer(state_, &tuple_buffer_size, &tuple_buffer));
  Tuple* tuple = reinterpret_cast<Tuple*>(tuple_buffer);

  // Main scan loop:
  // Tries to fill 'row_batch' with rows from cur_kudu_batch_.
  // If there are no rows to decode, tries to get the next row batch from kudu.
  // If this scanner has no more rows, the scanner is closed and eos is returned.
  while (!*eos) {
    RETURN_IF_CANCELLED(state_);

    if (cur_kudu_batch_num_read_ < cur_kudu_batch_.NumRows()) {
      RETURN_IF_ERROR(DecodeRowsIntoRowBatch(row_batch, &tuple));
      if (row_batch->AtCapacity()) break;
    }

    if (scanner_->HasMoreRows() && !scan_node_->ReachedLimit()) {
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
  DCHECK(scanner_ == NULL);
  kudu::client::KuduScanner* scanner;
  KUDU_RETURN_IF_ERROR(kudu::client::KuduScanToken::DeserializeIntoScanner(
      scan_node_->kudu_client(), scan_token, &scanner),
      "Unable to deserialize scan token");
  scanner_.reset(scanner);

  if (UNLIKELY(FLAGS_pick_only_leaders_for_tests)) {
    KUDU_RETURN_IF_ERROR(scanner_->SetSelection(kudu::client::KuduClient::LEADER_ONLY),
                         "Could not set replica selection.");
  }
  kudu::client::KuduScanner::ReadMode mode =
      MODE_READ_AT_SNAPSHOT == FLAGS_kudu_read_mode ?
          kudu::client::KuduScanner::READ_AT_SNAPSHOT :
          kudu::client::KuduScanner::READ_LATEST;
  KUDU_RETURN_IF_ERROR(scanner_->SetReadMode(mode), "Could not set scanner ReadMode");
  KUDU_RETURN_IF_ERROR(scanner_->SetTimeoutMillis(FLAGS_kudu_operation_timeout_ms),
      "Could not set scanner timeout");
  VLOG_ROW << "Starting KuduScanner with ReadMode=" << mode << " timeout=" <<
      FLAGS_kudu_operation_timeout_ms;

  if (!timestamp_slots_.empty()) {
    uint64_t row_format_flags =
        kudu::client::KuduScanner::PAD_UNIXTIME_MICROS_TO_16_BYTES;
    scanner_->SetRowFormatFlags(row_format_flags);
  }

  if (scan_node_->filter_ctxs_.size() > 0) {
    for (const FilterContext& ctx : scan_node_->filter_ctxs_) {
      MinMaxFilter* filter = ctx.filter->get_min_max();
      if (filter != nullptr && !filter->AlwaysTrue()) {
        if (filter->AlwaysFalse()) {
          // We can skip this entire scan.
          CloseCurrentClientScanner();
          *eos = true;
          return Status::OK();
        } else {
          auto it = ctx.filter->filter_desc().planid_to_target_ndx.find(scan_node_->id());
          const TRuntimeFilterTargetDesc& target_desc =
              ctx.filter->filter_desc().targets[it->second];
          const string& col_name = target_desc.kudu_col_name;
          DCHECK(col_name != "");
          const ColumnType& col_type = ColumnType::FromThrift(target_desc.kudu_col_type);

          void* min = filter->GetMin();
          void* max = filter->GetMax();
          // If the type of the filter is not the same as the type of the target column,
          // there must be an implicit integer cast and we need to ensure the min/max we
          // pass to Kudu are within the range of the target column.
          int64_t int_min;
          int64_t int_max;
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

          KuduValue* min_value;
          RETURN_IF_ERROR(CreateKuduValue(col_type, min, &min_value));
          KUDU_RETURN_IF_ERROR(
              scanner_->AddConjunctPredicate(scan_node_->table_->NewComparisonPredicate(
                  col_name, KuduPredicate::ComparisonOp::GREATER_EQUAL, min_value)),
              "Failed to add min predicate");

          KuduValue* max_value;
          RETURN_IF_ERROR(CreateKuduValue(col_type, max, &max_value));
          KUDU_RETURN_IF_ERROR(
              scanner_->AddConjunctPredicate(scan_node_->table_->NewComparisonPredicate(
                  col_name, KuduPredicate::ComparisonOp::LESS_EQUAL, max_value)),
              "Failed to add max predicate");
        }
      }
    }
  }

  {
    SCOPED_TIMER(state_->total_storage_wait_timer());
    KUDU_RETURN_IF_ERROR(scanner_->Open(), "Unable to open scanner");
  }
  *eos = false;
  return Status::OK();
}

void KuduScanner::CloseCurrentClientScanner() {
  DCHECK_NOTNULL(scanner_.get());
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
  // Short-circuit the count(*) case.
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
      TimestampValue tv = TimestampValue::UtcFromUnixTimeMicros(ts_micros);
      if (tv.HasDateAndTime()) {
        RawValue::Write(&tv, kudu_tuple, slot, NULL);
      } else {
        kudu_tuple->SetNull(slot->null_indicator_offset());
        RETURN_IF_ERROR(state_->LogOrReturnError(
            ErrorMsg::Init(TErrorCode::KUDU_TIMESTAMP_OUT_OF_RANGE,
              scan_node_->table_->name(),
              scan_node_->table_->schema().Column(slot->col_pos()).name())));
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
    if (row_batch->AtCapacity() || scan_node_->ReachedLimit()) break;
    // Move to the next tuple in the tuple buffer.
    *tuple_mem = next_tuple(*tuple_mem);
  }
  expr_results_pool_->Clear();

  // Check the status in case an error status was set during conjunct evaluation.
  return state_->GetQueryStatus();
}

Status KuduScanner::GetNextScannerBatch() {
  SCOPED_TIMER(state_->total_storage_wait_timer());
  int64_t now = MonotonicMicros();
  KUDU_RETURN_IF_ERROR(scanner_->NextBatch(&cur_kudu_batch_), "Unable to advance iterator");
  COUNTER_ADD(scan_node_->kudu_round_trips(), 1);
  cur_kudu_batch_num_read_ = 0;
  COUNTER_ADD(scan_node_->rows_read_counter(), cur_kudu_batch_.NumRows());
  last_alive_time_micros_ = now;
  return Status::OK();
}

}  // namespace impala
