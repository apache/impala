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

#include "exec/paimon/paimon-jni-scan-node.h"

#include "common/status.h"
#include "exec/arrow-converter.h"
#include "exec/exec-node-util.h"
#include "exec/exec-node.inline.h"
#include "exec/paimon/paimon-jni-scanner.h"
#include "exec/paimon/paimon-scan-plan-node.h"
#include "exec/scratch-tuple-batch.h"
#include "rpc/thrift-util.h"
#include "runtime/exec-env.h"
#include "runtime/runtime-state.h"
#include "runtime/tuple-row.h"
#include "util/jni-util.h"
#include "util/periodic-counter-updater.h"
#include "util/runtime-profile-counters.h"

#include <jni.h>
#include <vector>
#include <arrow/c/bridge.h>
#include <glog/logging.h>
using namespace impala;

PaimonJniScanNode::PaimonJniScanNode(
    ObjectPool* pool, const PaimonScanPlanNode& pnode, const DescriptorTbl& descs)
  : ScanNode(pool, pnode, descs),
    tuple_id_(pnode.tnode_->paimon_table_scan_node.tuple_id),
    table_name_(pnode.tnode_->paimon_table_scan_node.table_name),
    splits_empty_(false),
    paimon_last_arrow_record_batch_consumed_bytes_(0) {}

Status PaimonJniScanNode::Prepare(RuntimeState* state) {
  RETURN_IF_ERROR(ScanNode::Prepare(state));
  DCHECK(scan_range_params_ != NULL)
      << "Must call SetScanRanges() before calling Prepare()";

  scan_open_timer_ = ADD_TIMER(runtime_profile(), "ScanOpenTime");
  paimon_api_scan_timer_ = ADD_TIMER(runtime_profile(), "PaimonApiScanTime");

  tuple_desc_ = state->desc_tbl().GetTupleDescriptor(tuple_id_);
  if (tuple_desc_ == nullptr) {
    return Status(
        "Failed to get tuple descriptor, tuple id: " + std::to_string(tuple_id_));
  }
  arrow_batch_mem_tracker_.reset(
      new MemTracker(-1, "Arrow Batch", this->mem_tracker(), false));
  /// Construct the jni scan param, the param will be used in PaimonJniScanner.
  paimon_jni_scan_param_.__set_paimon_table_obj(
      plan_node_.tnode_->paimon_table_scan_node.paimon_table_obj);
  /// update projection id, will get the top-level field ids of each tuple.
  std::vector<int32_t> field_ids;
  RETURN_IF_ERROR(CollectProjectionFieldIds(tuple_desc_, field_ids));
  paimon_jni_scan_param_.__set_projection(field_ids);
  LOG(INFO) << table_name_ << " Contains " << field_ids.size() << " field ids."
            << std::endl;
  paimon_jni_scan_param_.__set_mem_limit_bytes(
      arrow_batch_mem_tracker_->GetLowestLimit(MemLimit::HARD));
  paimon_jni_scan_param_.__set_batch_size(state->batch_size());
  paimon_jni_scan_param_.__set_fragment_id(state->fragment_instance_id());
  std::vector<std::string> scan_range_vector;
  for (const ScanRangeParamsPB& params : *scan_range_params_) {
    DCHECK(params.scan_range().has_file_metadata());
    const std::string& split = params.scan_range().file_metadata();
    scan_range_vector.push_back(split);
  }
  paimon_jni_scan_param_.__set_splits(scan_range_vector);
  /// Check if splits is empty
  splits_empty_ = scan_range_vector.empty();
  impala::ThriftSerializer serializer(false);
  /// serialize the jni scan param to binary.
  RETURN_IF_ERROR(serializer.SerializeToString<TPaimonJniScanParam>(
      &paimon_jni_scan_param_, &paimon_jni_scan_param_serialized_));
  scratch_batch_.reset(
      new ScratchTupleBatch(*tuple_desc_, state->batch_size(), mem_tracker()));
  arrow_converter_.reset(new ArrowToTupleConverter(tuple_desc_));
  RETURN_IF_ERROR(arrow_converter_->Init(state));
  return Status::OK();
}

Status PaimonJniScanNode::Open(RuntimeState* state) {
  SCOPED_TIMER(scan_open_timer_);
  RETURN_IF_ERROR(ScanNode::Open(state));
  /// Skip if splits is empty.
  if (splits_empty_) {
    return Status::OK();
  }
  JNIEnv* env = JniUtil::GetJNIEnv();
  if (env == nullptr) return Status("Failed to get/create JVM");
  jni_scanner_.reset(
      new PaimonJniScanner(paimon_jni_scan_param_serialized_, tuple_desc_, table_name_));
  RETURN_IF_ERROR(jni_scanner_->Init(env));
  SCOPED_TIMER(paimon_api_scan_timer_);
  RETURN_IF_ERROR(jni_scanner_->ScanTable(env));
  return Status::OK();
}

Status PaimonJniScanNode::CollectProjectionFieldIds(
    const TupleDescriptor* tuple_desc_, vector<int32_t>& projection) {
  for (const SlotDescriptor* slot_desc : tuple_desc_->slots()) {
    int field_id = -1;
    if (slot_desc->col_path().size() == 1) {
      // Top level slots have ColumnDescriptors that store the field ids.
      field_id = tuple_desc_->table_desc()->GetColumnDesc(slot_desc).field_id();
    } else {
      // TODO: support the nested field later.
      return Status("Paimon Scanner currently doesn't support nested type now");
    }
    DCHECK_NE(field_id, -1);
    projection.push_back(field_id);
  }
  return Status::OK();
}

Status PaimonJniScanNode::GetNextBatch(bool* is_empty_batch) {
  SCOPED_TIMER(paimon_api_scan_timer_);
  JNIEnv* env = JniUtil::GetJNIEnv();
  struct ArrowArray* array;
  struct ArrowSchema* schema;
  long row_count;
  long offheap_consumed_bytes;

  DCHECK(is_empty_batch != nullptr);

  RETURN_IF_ERROR(jni_scanner_->GetNextBatchDirect(
      env, &array, &schema, &row_count, &offheap_consumed_bytes));

  *is_empty_batch = false;
  if (row_count > 0) {
    auto resultImportVectorSchemaRoot = arrow::ImportRecordBatch(array, schema);
    /// since the result type is arrow::Status, need to check status manually
    /// without using macro RETURN_IF_ERROR
    if (!resultImportVectorSchemaRoot.ok()) {
      return Status(resultImportVectorSchemaRoot.status().message());
    }
    paimon_arrow_record_batch_holder_ = resultImportVectorSchemaRoot.ValueUnsafe();
    DCHECK_EQ(row_count, paimon_arrow_record_batch_holder_->num_rows());
    /// update allocated offheap memory reported from Jni.
    /// need to do for each batch since we need to check the
    /// memory usage hit the limit of mem tracker.
    OffheapTrackFree();
    RETURN_IF_ERROR(OffheapTrackAllocation(offheap_consumed_bytes));

  } else {
    /// No more batches to fetch.
    paimon_arrow_record_batch_holder_ = nullptr;
    OffheapTrackFree();
  }

  *is_empty_batch = row_count == 0;

  return Status::OK();
}

// Arrow record batches are converted to row batch in two steps:
//   1. Arrow record batches are converted into scratch_batch_
//   2. Rows are added to row_batch from scratch_batch_
// If at either point unconverted rows remain, they will be converted in the following
// GetNext call.
Status PaimonJniScanNode::GetNext(RuntimeState* state, RowBatch* row_batch, bool* eos) {
  SCOPED_TIMER(runtime_profile_->total_time_counter());
  ScopedGetNextEventAdder ea(this, eos);
  RETURN_IF_ERROR(ExecDebugAction(TExecNodePhase::GETNEXT, state));
  RETURN_IF_CANCELLED(state);
  RETURN_IF_ERROR(QueryMaintenance(state));

  /// Return eos if empty splits or reached limit.
  if (splits_empty_ || ReachedLimit()) {
    *eos = true;
    return Status::OK();
  }
  *eos = false;

  SCOPED_TIMER(materialize_tuple_timer());
  while (!ReachedLimit() && !row_batch->AtCapacity()) {
    /// Break the loop if is canceled, or maintainance is needed.
    if (state->is_cancelled() || !QueryMaintenance(state).ok()) break;

    // If no remaining rows in scratch_batch_, fetch a new batch from arrow.
    // Otherwise, handle remaining rows first.
    if (scratch_batch_->AtEnd()) {
      // Step 1: write values from arrow batch to scratch batch
      if (paimon_arrow_record_batch_holder_ == nullptr
          || arrow_batch_row_idx_ >= paimon_arrow_record_batch_holder_->num_rows()) {
        bool is_empty_batch;
        RETURN_IF_ERROR(GetNextBatch(&is_empty_batch));
        if (is_empty_batch) {
          *eos = true;
          return Status::OK();
        }
        arrow_batch_row_idx_ = 0;
      }

      // Start a new scratch batch.
      RETURN_IF_ERROR(scratch_batch_->Reset(state));

      // The scratch batch may hold fewer rows than the arrow batch.
      int num_rows = std::min<int64_t>(scratch_batch_->capacity,
          paimon_arrow_record_batch_holder_->num_rows() - arrow_batch_row_idx_);
      int end_row_idx = arrow_batch_row_idx_ + num_rows;

      RETURN_IF_ERROR(arrow_converter_->Convert(*paimon_arrow_record_batch_holder_,
          arrow_batch_row_idx_, end_row_idx, scratch_batch_->tuple_mem,
          &scratch_batch_->aux_mem_pool));

      arrow_batch_row_idx_ = end_row_idx;
      scratch_batch_->num_tuples = num_rows;
      COUNTER_ADD(rows_read_counter(), num_rows);
    }

    // Step 2: add rows from scratch batch to tuple row
    int num_rows_added = 0;
    while (!scratch_batch_->AtEnd()) {
      int row_batch_row_idx = row_batch->num_rows() + num_rows_added;

      if (ReachedLimit() || row_batch_row_idx >= row_batch->capacity()) break;

      Tuple* tuple = scratch_batch_->GetTuple(scratch_batch_->tuple_idx);
      scratch_batch_->tuple_idx++;

      TupleRow* tuple_row = row_batch->GetRow(row_batch_row_idx);
      tuple_row->SetTuple(0, tuple);

      if (ExecNode::EvalConjuncts(conjunct_evals().data(), conjunct_evals().size(),
          tuple_row)) {
        IncrementNumRowsReturned(1);
        num_rows_added++;
      }
    }

    scratch_batch_->FinalizeTupleTransfer(row_batch, num_rows_added);
    row_batch->CommitRows(num_rows_added);
  }

  *eos = ReachedLimit();
  return Status::OK();
}

void PaimonJniScanNode::Close(RuntimeState* state) {
  if (is_closed()) return;
  /// Eagerly release arrow batch before calling
  /// frontend method close, in case of
  /// mem leak. leak was observed when query was
  /// canceled.
  if (paimon_arrow_record_batch_holder_ != nullptr) {
    paimon_arrow_record_batch_holder_.reset();
  }
  /// 'jni_scanner_' is only created in Open(), and only if there are splits. Close()
  /// is also called if Prepare() or Open() failed before that, or Open() was not called.
  if (jni_scanner_ != nullptr) jni_scanner_->Close(state);
  OffheapTrackFree();
  if (arrow_batch_mem_tracker_ != nullptr) {
    arrow_batch_mem_tracker_->Close();
  }
  if (scratch_batch_ != nullptr) {
    scratch_batch_->ReleaseResources(nullptr);
  }
  ScanNode::Close(state);
}
