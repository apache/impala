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
#include "exec/exec-node-util.h"
#include "exec/exec-node.inline.h"
#include "exec/paimon/paimon-jni-row-reader.h"
#include "exec/paimon/paimon-jni-scanner.h"
#include "exec/paimon/paimon-scan-plan-node.h"
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
    paimon_last_arrow_record_batch_consumed_bytes_(0),
    arrow_record_batch_row_count_(0),
    arrow_record_batch_row_index_(0) {}

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
  paimon_row_reader_.reset(new PaimonJniRowReader());
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

Status PaimonJniScanNode::GetNextBatchIfNeeded(bool* is_empty_batch) {
  /// Check if we need to fetch next arrow record batch from jni. if yes,
  /// fetch it.
  if (paimon_arrow_record_batch_holder_ == nullptr
      || arrow_record_batch_row_index_ >= arrow_record_batch_row_count_) {
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
      arrow_record_batch_row_count_ = row_count;
      DCHECK_EQ(row_count, paimon_arrow_record_batch_holder_->num_rows());
      arrow_record_batch_row_index_ = 0;
      /// update allocated offheap memory reported from Jni.
      /// need to do for each batch since we need to check the
      /// memory usage hit the limit of mem tracker.
      OffheapTrackFree();
      RETURN_IF_ERROR(OffheapTrackAllocation(offheap_consumed_bytes));

    } else {
      /// No more batches to fetch.
      paimon_arrow_record_batch_holder_ = nullptr;
      arrow_record_batch_row_count_ = 0;
      arrow_record_batch_row_index_ = 0;
      OffheapTrackFree();
    }
  }

  *is_empty_batch = arrow_record_batch_row_count_ > 0
      && arrow_record_batch_row_index_ < arrow_record_batch_row_count_;

  return Status::OK();
}

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

  /// Set eos to false initially for the batch.
  *eos = false;

  /// Allocate buffer for RowBatch and init the tuple
  uint8_t* tuple_buffer;
  int64_t tuple_buffer_size;
  RETURN_IF_ERROR(
      row_batch->ResizeAndAllocateTupleBuffer(state, &tuple_buffer_size, &tuple_buffer));
  Tuple* tuple = reinterpret_cast<Tuple*>(tuple_buffer);
  tuple->Init(tuple_buffer_size);

  SCOPED_TIMER(materialize_tuple_timer());
  while (!ReachedLimit() && !row_batch->AtCapacity()) {
    /// Break the loop if is canceled, or maintainance is needed.
    if (state->is_cancelled() || !QueryMaintenance(state).ok()) {
      break;
    }
    int row_idx = row_batch->AddRow();
    TupleRow* tuple_row = row_batch->GetRow(row_idx);
    tuple_row->SetTuple(0, tuple);

    /// Get the next arrow batch from 'org.apache.impala.util.paimon.PaimonJniScanner'
    /// if the current arrow batch is already consumed.
    bool fetched;
    RETURN_IF_ERROR(GetNextBatchIfNeeded(&fetched));
    /// When fetched is false, there are no more arrow batches to read
    if (!fetched) {
      *eos = true;
      break;
    }
    DCHECK(paimon_arrow_record_batch_holder_ != nullptr);
    COUNTER_ADD(rows_read_counter(), 1);
    RETURN_IF_ERROR(paimon_row_reader_->MaterializeTuple(
        *paimon_arrow_record_batch_holder_, arrow_record_batch_row_index_, tuple_desc_,
        tuple, row_batch->tuple_data_pool(), state));
    /// Evaluate conjuncts on this tuple row
    if (ExecNode::EvalConjuncts(
            conjunct_evals().data(), conjunct_evals().size(), tuple_row)) {
      row_batch->CommitLastRow();
      tuple = reinterpret_cast<Tuple*>(
          reinterpret_cast<uint8_t*>(tuple) + tuple_desc_->byte_size());
      IncrementNumRowsReturned(1);
    } else {
      /// Reset the null bits, everyhing else will be overwritten
      Tuple::ClearNullBits(
          tuple, tuple_desc_->null_bytes_offset(), tuple_desc_->num_null_bytes());
    }
    /// will process the next row of arrow RecordBatch in next iteration.
    arrow_record_batch_row_index_++;
  }
  if (ReachedLimit()) {
    *eos = true;
  }
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
    arrow_record_batch_row_index_ = 0;
    arrow_record_batch_row_count_ = 0;
  }
  /// Close jni scanner if splits is not empty.
  if (!splits_empty_) {
    jni_scanner_->Close(state);
  }
  OffheapTrackFree();
  if (arrow_batch_mem_tracker_ != nullptr) {
    arrow_batch_mem_tracker_->Close();
  }
  ScanNode::Close(state);
}
