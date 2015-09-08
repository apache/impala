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
#include "exprs/expr-context.h"
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
using strings::Substitute;

DEFINE_bool(pick_only_leaders_for_tests, false,
            "Whether to pick only leader replicas, for tests purposes only.");

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

KuduScanner::KuduScanner(KuduScanNode* scan_node, RuntimeState* state)
  : scan_node_(scan_node),
    state_(state) {}

Status KuduScanner::Open(const std::tr1::shared_ptr<KuduClient>& client,
    const std::tr1::shared_ptr<KuduTable>& table) {
  client_ = client;
  table_ = table;
  materialized_slots_ = scan_node_->materialized_slots();
  tuple_byte_size_ = scan_node_->tuple_desc()->byte_size();
  tuple_num_null_bytes_ = scan_node_->tuple_desc()->num_null_bytes();
  projected_columns_ = scan_node_->projected_columns();

  // Store columns that need relocation when materialized into the
  // destination row batch.
  for (int i = 0; i < materialized_slots_.size(); ++i) {
    if (materialized_slots_[i]->type().type == TYPE_STRING) {
      string_slots_.push_back(materialized_slots_[i]);
    }
  }

  return scan_node_->GetConjunctCtxs(&conjunct_ctxs_);
}

Status KuduScanner::GetNext(RowBatch* row_batch, bool* eos) {
  int tuple_buffer_size = row_batch->capacity() * tuple_byte_size_;
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

    // No more rows or blocks, we're done.
    *eos = true;
    return Status::OK();
  }

  return Status::OK();
}

void KuduScanner::Close() {
  if (scanner_) CloseCurrentScanner();
  Expr::Close(conjunct_ctxs_, state_);
}

Status KuduScanner::OpenNextScanner(const TKuduKeyRange& key_range)  {
  DCHECK(scanner_ == NULL);
  scanner_.reset(new kudu::client::KuduScanner(table_.get()));
  KUDU_RETURN_IF_ERROR(scanner_->SetProjectedColumns(projected_columns_),
      "Unable to set projected columns");

  RETURN_IF_ERROR(SetupScanRangePredicate(key_range, scanner_.get()));

  vector<KuduPredicate*> predicates;
  scan_node_->ClonePredicates(&predicates);
  BOOST_FOREACH(KuduPredicate* predicate, predicates) {
    scanner_->AddConjunctPredicate(predicate);
  }

  if (UNLIKELY(FLAGS_pick_only_leaders_for_tests)) {
    KUDU_RETURN_IF_ERROR(scanner_->SetSelection(kudu::client::KuduClient::LEADER_ONLY),
                         "Could not set replica selection.");
  }

  {
    SCOPED_TIMER(scan_node_->kudu_read_timer());
    KUDU_RETURN_IF_ERROR(scanner_->Open(), "Unable to open scanner");
  }
  return Status::OK();
}

void KuduScanner::CloseCurrentScanner() {
  DCHECK_NOTNULL(scanner_.get());
  scanner_->Close();
  scanner_.reset();
  ExprContext::FreeLocalAllocations(conjunct_ctxs_);
}

Status KuduScanner::DecodeRowsIntoRowBatch(RowBatch* row_batch,
    Tuple** tuple_mem, bool* batch_done) {

  // Add the first row, this should never fail since we're getting a new batch.
  int idx = row_batch->AddRow();
  DCHECK(idx != RowBatch::INVALID_ROW_INDEX);

  TupleRow* row;
  // Skip advancing/initializing the tuple buffer if we're not actually
  // materializing any rows, e.g. for count(*).
  if (!materialized_slots_.empty()) {
    row = row_batch->GetRow(idx);
    (*tuple_mem)->Init(tuple_num_null_bytes_);
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
    if (conjunct_ctxs_.empty() || ExecNode::EvalConjuncts(&conjunct_ctxs_[0],
        conjunct_ctxs_.size(), row)) {

      // Materialize those slots that require auxiliary memory
      RETURN_IF_ERROR(RelocateValuesFromKudu(*tuple_mem, row_batch->tuple_data_pool()));

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
      if (materialized_slots_.empty()) continue;

      // Move to the next tuple in the tuple buffer.
      *tuple_mem = next_tuple(*tuple_mem);
      (*tuple_mem)->Init(tuple_num_null_bytes_);

      // Make 'row' point to the new row.
      row = row_batch->GetRow(idx);
      row->SetTuple(0, *tuple_mem);
    }
  }
  return Status::OK();
}

void KuduScanner::SetSlotToNull(Tuple* tuple, const SlotDescriptor& slot) {
  DCHECK(slot.is_nullable());
  tuple->SetNull(slot.null_indicator_offset());
}

bool KuduScanner::IsSlotNull(Tuple* tuple, const SlotDescriptor& slot) {
  return slot.is_nullable() && tuple->IsNull(slot.null_indicator_offset());
}

Status KuduScanner::RelocateValuesFromKudu(Tuple* tuple, MemPool* mem_pool) {
  for (int i = 0; i < string_slots_.size(); ++i) {
    const SlotDescriptor* slot = string_slots_[i];
    // NULL handling was done in KuduRowToImpalaTuple.
    if (IsSlotNull(tuple, *slot)) continue;

    // Extract the string value.
    void* slot_ptr = tuple->GetSlot(slot->tuple_offset());
    DCHECK(slot->type().type == TYPE_STRING);

    // The string value of the slot has a pointer to memory from the Kudu row.
    StringValue* val = reinterpret_cast<StringValue*>(slot_ptr);
    char* old_buf = val->ptr;
    // KUDU never returns values larger than 8MB
    DCHECK_LE(val->len, 8 * (1 << 20));
    val->ptr = reinterpret_cast<char*>(mem_pool->TryAllocate(val->len));
    if (UNLIKELY(val->ptr == NULL)) {
      if (UNLIKELY(val->len > 0)) return Status::MEM_LIMIT_EXCEEDED;
    } else {
      DCHECK(val->len > 0);
      memcpy(val->ptr, old_buf, val->len);
    }
  }
  return Status::OK();
}


Status KuduScanner::KuduRowToImpalaTuple(const KuduRowResult& row,
    RowBatch* row_batch, Tuple* tuple) {
  for (int i = 0; i < materialized_slots_.size(); ++i) {
    const SlotDescriptor* info = materialized_slots_[i];
    void* slot = tuple->GetSlot(info->tuple_offset());

    if (row.IsNull(i)) {
      SetSlotToNull(tuple, *info);
      continue;
    }

    switch (info->type().type) {
      case TYPE_STRING: {
        // For types with auxiliary memory (String, Binary,...) store the original memory
        // location in the tuple. Relocate the memory into the row batch's memory in a
        // later step.
        kudu::Slice slice;
        KUDU_RETURN_IF_ERROR(row.GetString(i, &slice),
            "Error getting column value from Kudu.");
        reinterpret_cast<StringValue*>(slot)->ptr =
            const_cast<char*>(reinterpret_cast<const char*>(slice.data()));
        reinterpret_cast<StringValue*>(slot)->len = static_cast<int>(slice.size());
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
  SCOPED_TIMER(scan_node_->kudu_read_timer());
  cur_rows_.clear();
  KUDU_RETURN_IF_ERROR(scanner_->NextBatch(&cur_rows_), "Unable to advance iterator");
  COUNTER_ADD(scan_node_->kudu_round_trips(), 1);
  rows_scanned_current_block_ = 0;
  COUNTER_ADD(scan_node_->rows_read_counter(), cur_rows_.size());
  return Status::OK();
}

}  // namespace impala
