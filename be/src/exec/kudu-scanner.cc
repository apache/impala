// Copyright 2015 Cloudera Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#include "exec/kudu-scanner.h"

#include <boost/foreach.hpp>
#include <kudu/client/row_result.h>
#include <thrift/protocol/TDebugProtocol.h>
#include <vector>

#include "exprs/expr.h"
#include "exec/kudu-util.h"
#include "runtime/mem-pool.h"
#include "runtime/runtime-state.h"
#include "runtime/row-batch.h"
#include "runtime/string-value.h"
#include "runtime/tuple-row.h"
#include "gutil/gscoped_ptr.h"
#include "gutil/strings/substitute.h"
#include "util/jni-util.h"
#include "util/periodic-counter-updater.h"
#include "util/runtime-profile.h"

#include "common/names.h"

using kudu::client::KuduClient;
using kudu::client::KuduColumnSchema;
using kudu::client::KuduPredicate;
using kudu::client::KuduRowResult;
using kudu::client::KuduSchema;
using kudu::client::KuduTable;

namespace impala {

namespace {

// Sets up the scan range predicate on the scanner, i.e. the start/stop keys
// of the range the scanner is supposed to scan.
Status SetupScanRangePredicate(const TKuduKeyRange& key_range,
    kudu::client::KuduScanner* scanner) {
  if (key_range.startKey.empty() && key_range.stopKey.empty()) return Status::OK();

  if (!key_range.startKey.empty()) {
    KUDU_RETURN_IF_ERROR(scanner->AddLowerBoundRaw(key_range.startKey),
        "adding scan range lower bound");
  }
  if (!key_range.stopKey.empty()) {
    KUDU_RETURN_IF_ERROR(scanner->AddExclusiveUpperBoundRaw(key_range.stopKey),
        "adding scan range upper bound");
  }

  return Status::OK();
}

} // anonymous namespace

KuduScanner::KuduScanner(KuduScanNode* scan_node, RuntimeState* state,
    const vector<TKuduKeyRange>& scan_ranges)
  : scan_node_(scan_node),
    state_(state),
    scan_ranges_(scan_ranges),
    cur_scan_range_idx_(0) {}

Status KuduScanner::Open(const std::tr1::shared_ptr<KuduClient>& client,
    const std::tr1::shared_ptr<KuduTable>& table) {
  client_ = client;
  table_ = table;
  return GetNextScanner();
}

Status KuduScanner::GetNext(RowBatch* row_batch, bool* eos) {
  int tuple_buffer_size = row_batch->capacity() * scan_node_->tuple_desc()->byte_size();
  void* tuple_buffer = row_batch->tuple_data_pool()->TryAllocate(tuple_buffer_size);
  if (tuple_buffer_size > 0 && tuple_buffer == NULL) return Status::MEM_LIMIT_EXCEEDED;
  Tuple* tuple = reinterpret_cast<Tuple*>(tuple_buffer);

  // Main scan loop:
  // Tries to fill 'row_batch' with rows from the last fetched block.
  // If there are no rows to decode tries to get the next block from kudu.
  // If there are no more blocks in the current range tries to get the next range.
  // If there aren't any more rows, blocks or ranges we're done.
  while(true) {
    RETURN_IF_CANCELLED(state_);
    RETURN_IF_ERROR(scan_node_->QueryMaintenance(state_));

    // If the last fetched block has more rows, decode and if we filled up the batch
    // return.
    if (CurrentBlockHasMoreRows()) {
      bool batch_done = false;
      RETURN_IF_ERROR(DecodeRowsIntoRowBatch(row_batch, &tuple, &batch_done));
      if (batch_done) return Status::OK();
    }

    // If the current scanner has more blocks, fetch them.
    if (CurrentScannerHasMoreBlocks()) {
      RETURN_IF_ERROR(GetNextBlock());
      continue;
    }

    // No more blocks in the current scanner, close it.
    CloseCurrentScanner();

    // Check if there are more scanners and continue if there are.
    if (HasMoreScanners()) {
      RETURN_IF_ERROR(GetNextScanner());
      continue;
    }

    // No more rows, blocks or scanners, we're done.
    *eos = true;
    return Status::OK();
  }

  return Status::OK();
}

Status KuduScanner::Close() {
  scanner_->Close();
  return Status::OK();
}

Status KuduScanner::GetNextScanner()  {
  DCHECK(scanner_ == NULL);
  DCHECK_LT(cur_scan_range_idx_, scan_ranges_.size());
  const TKuduKeyRange& key_range = scan_ranges_[cur_scan_range_idx_];

  VLOG_FILE << "Starting scanner " << (cur_scan_range_idx_ + 1)
      << "/" << scan_ranges_.size();

  scanner_.reset(new kudu::client::KuduScanner(table_.get()));
  KUDU_RETURN_IF_ERROR(scanner_->SetProjectedColumns(scan_node_->projected_columns()),
      "Unable to set projected columns");

  RETURN_IF_ERROR(SetupScanRangePredicate(key_range, scanner_.get()));
  KUDU_RETURN_IF_ERROR(scanner_->SetReadMode(
      kudu::client::KuduScanner::READ_AT_SNAPSHOT), "Unable to set snapshot read mode.");

  BOOST_FOREACH(KuduPredicate* predicate, scan_node_->kudu_predicates_) {
    scanner_->AddConjunctPredicate(predicate);
  }

  KUDU_RETURN_IF_ERROR(scanner_->Open(), "Unable to open scanner");
  return Status::OK();
}

void KuduScanner::CloseCurrentScanner() {
  DCHECK_NOTNULL(scanner_.get());
  scanner_->Close();
  scanner_.reset();
  ++cur_scan_range_idx_;
}

Status KuduScanner::DecodeRowsIntoRowBatch(RowBatch* row_batch,
    Tuple** tuple_mem, bool* batch_done) {

  // Add the first row, this should never fail since we're getting a new batch.
  int idx = row_batch->AddRow();
  DCHECK(idx != RowBatch::INVALID_ROW_INDEX);

  TupleRow* row;
  // Skip advancing/initializing the tuple buffer if we're not actually
  // materializing any rows, e.g. for count(*).
  if (!scan_node_->materialized_slots().empty()) {
    row = row_batch->GetRow(idx);
    (*tuple_mem)->Init(scan_node_->tuple_desc()->num_null_bytes());
    row->SetTuple(0, *tuple_mem);
  }

  // Now iterate through the Kudu rows.
  for (int krow_idx = rows_scanned_current_block_; krow_idx < cur_rows_.size();
       ++krow_idx) {

    // Transform a Kudu row into an Impala row.
    const KuduRowResult& krow = cur_rows_[krow_idx];
    RETURN_IF_ERROR(KuduRowToImpalaTuple(krow, row_batch, *tuple_mem));
    ++rows_scanned_current_block_;

    // Evaluate the conjuncts that haven't been pushed down.
    if (scan_node_->conjunct_ctxs().empty() ||
        ExecNode::EvalConjuncts(&scan_node_->conjunct_ctxs()[0],
            scan_node_->conjunct_ctxs().size(), row)) {
      // If the conjuncts pass on the row commit it.
      row_batch->CommitLastRow();

      // Add another row.
      idx = row_batch->AddRow();

      // If we've reached the capacity, or the LIMIT for the scan, return.
      if (idx == RowBatch::INVALID_ROW_INDEX || row_batch->AtCapacity() ||
          scan_node_->ReachedLimit()) {
        *batch_done = true;
        break;
      }

      // Skip advancing/initializing the tuple buffer if we're not actually
      // materializing any rows, e.g. for count(*).
      if (scan_node_->materialized_slots().empty()) continue;

      // Move to the next tuple in the tuple buffer.
      *tuple_mem = next_tuple(*tuple_mem);
      (*tuple_mem)->Init(scan_node_->tuple_desc()->num_null_bytes());

      // Make 'row' point to the new row.
      row = row_batch->GetRow(idx);
      row->SetTuple(0, *tuple_mem);
    }
  }

  return Status::OK();
}

void KuduScanner::SetSlotToNull(Tuple* tuple, int mat_slot_idx) {
  DCHECK(scan_node_->materialized_slots()[mat_slot_idx]->is_nullable());
  tuple->SetNull(scan_node_->materialized_slots()[mat_slot_idx]->null_indicator_offset());
}


Status KuduScanner::KuduRowToImpalaTuple(const KuduRowResult& row,
    RowBatch* row_batch, Tuple* tuple) {
  for (int i = 0; i < scan_node_->materialized_slots().size(); ++i) {
    const SlotDescriptor* info = scan_node_->materialized_slots()[i];
    void* slot = tuple->GetSlot(info->tuple_offset());

    if (row.IsNull(i)) {
      SetSlotToNull(tuple, i);
      continue;
    }

    switch (info->type().type) {
      case TYPE_STRING: {
        // TODO Below we do a memcpy to get the slice from Kudu and another one to copy
        // the slice into the tuple data pool. This is done so that we know the size of
        // the slice to perform the allocation, but is inneficcient, Kudu's client should
        // allow to get the size of the string without a copy.
        kudu::Slice slice;
        KUDU_RETURN_IF_ERROR(row.GetString(i, &slice),
            "Error getting column value from Kudu.");
        char* buffer = reinterpret_cast<char*>(
            row_batch->tuple_data_pool()->TryAllocate(slice.size()));
        if (UNLIKELY(buffer == NULL)) {
          if (UNLIKELY(slice.size() > 0)) return Status::MEM_LIMIT_EXCEEDED;
        } else {
          // If we ever change TryAllocate() to return something other than NULL
          // when size is 0 (e.g. return a valid pointer) we need to change this logic.
          DCHECK(slice.size() > 0);
          memcpy(buffer, slice.data(), slice.size());
        }
        reinterpret_cast<StringValue*>(slot)->ptr = buffer;
        reinterpret_cast<StringValue*>(slot)->len = slice.size();
        break;
      }
      case TYPE_TINYINT:
        KUDU_RETURN_IF_ERROR(row.GetInt8(i, reinterpret_cast<int8_t*>(slot)),
            "Error getting column value from Kudu.");
        break;
      case TYPE_SMALLINT:
        KUDU_RETURN_IF_ERROR(row.GetInt16(i, reinterpret_cast<int16_t*>(slot)),
            "Error getting column value from Kudu.");
        break;
      case TYPE_INT:
        KUDU_RETURN_IF_ERROR(row.GetInt32(i, reinterpret_cast<int32_t*>(slot)),
            "Error getting column value from Kudu.");
        break;
      case TYPE_BIGINT:
        KUDU_RETURN_IF_ERROR(row.GetInt64(i, reinterpret_cast<int64_t*>(slot)),
            "Error getting column value from Kudu.");
        break;
      case TYPE_FLOAT:
        KUDU_RETURN_IF_ERROR(row.GetFloat(i, reinterpret_cast<float*>(slot)),
            "Error getting column value from Kudu.");
        break;
      case TYPE_DOUBLE:
        KUDU_RETURN_IF_ERROR(row.GetDouble(i, reinterpret_cast<double*>(slot)),
            "Error getting column value from Kudu.");
        break;
      default:
        DCHECK(false) << "Impala type unsupported in Kudu: "
            << TypeToString(info->type().type);
        return Status(TErrorCode::IMPALA_KUDU_TYPE_MISSING,
            TypeToString(info->type().type));
    }
  }
  return Status::OK();
}


Status KuduScanner::GetNextBlock() {
  SCOPED_TIMER(scan_node_->kudu_read_timer_);
  cur_rows_.clear();
  KUDU_RETURN_IF_ERROR(scanner_->NextBatch(&cur_rows_), "Unable to advance iterator");
  scan_node_->kudu_round_trips_->Add(1);
  rows_scanned_current_block_ = 0;
  COUNTER_ADD(scan_node_->rows_read_counter(), cur_rows_.size());
  return Status::OK();
}

}  // namespace impala
