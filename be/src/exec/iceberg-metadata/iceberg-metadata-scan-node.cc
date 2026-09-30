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

#include "exec/iceberg-metadata/iceberg-metadata-scan-node.h"

#include <boost/algorithm/string/case_conv.hpp>

#include "exec/exec-node.inline.h"
#include "exec/exec-node-util.h"
#include "gutil/strings/substitute.h"
#include "runtime/exec-env.h"
#include "runtime/runtime-state.h"
#include "runtime/tuple-row.h"
#include "service/frontend.h"
#include "util/jni-util.h"

using namespace impala;

Status IcebergMetadataScanPlanNode::CreateExecNode(
    RuntimeState* state, ExecNode** node) const {
  ObjectPool* pool = state->obj_pool();
  *node = pool->Add(new IcebergMetadataScanNode(pool, *this, state->desc_tbl()));
  return Status::OK();
}

IcebergMetadataScanNode::IcebergMetadataScanNode(ObjectPool* pool,
    const IcebergMetadataScanPlanNode& pnode, const DescriptorTbl& descs)
  : ScanNode(pool, pnode, descs),
    tuple_id_(pnode.tnode_->iceberg_scan_metadata_node.tuple_id),
    table_name_(pnode.tnode_->iceberg_scan_metadata_node.table_name),
    metadata_table_name_(pnode.tnode_->iceberg_scan_metadata_node.metadata_table_name) {}

Status IcebergMetadataScanNode::Prepare(RuntimeState* state) {
  RETURN_IF_ERROR(ScanNode::Prepare(state));
  scan_prepare_timer_ = ADD_TIMER(runtime_profile(), "ScanPrepareTime");
  iceberg_api_scan_timer_ = ADD_TIMER(runtime_profile(), "IcebergApiScanTime");
  tuple_desc_ = state->desc_tbl().GetTupleDescriptor(tuple_id_);
  if (tuple_desc_ == nullptr) {
    return Status("Failed to get tuple descriptor, tuple id: " +
        std::to_string(tuple_id_));
  }
  return Status::OK();
}

Status IcebergMetadataScanNode::Open(RuntimeState* state) {
  SCOPED_TIMER(runtime_profile_->total_time_counter());
  SCOPED_TIMER(scan_prepare_timer_);
  ScopedOpenEventAdder ea(this);
  RETURN_IF_ERROR(ScanNode::Open(state));
  JNIEnv* env = JniUtil::GetJNIEnv();
  if (env == nullptr) return Status("Failed to get/create JVM");
  // Get the FeTable object from the Frontend. 'metadata_scanner_' takes ownership of
  // the global reference.
  jobject jtable = nullptr;
  RETURN_IF_ERROR(GetCatalogTable(&jtable));
  metadata_scanner_.reset(new IcebergMetadataScanner(jtable, metadata_table_name_.c_str(),
      tuple_desc_));
  RETURN_IF_ERROR(metadata_scanner_->Init(env));
  iceberg_row_reader_.reset(new IcebergRowReader(this, metadata_scanner_.get()));
  SCOPED_TIMER(iceberg_api_scan_timer_);
  RETURN_IF_ERROR(metadata_scanner_->ScanMetadataTable(env));
  return Status::OK();
}

Status IcebergMetadataScanNode::GetNext(RuntimeState* state, RowBatch* row_batch,
    bool* eos) {
  SCOPED_TIMER(runtime_profile_->total_time_counter());
  ScopedGetNextEventAdder ea(this, eos);
  RETURN_IF_ERROR(ExecDebugAction(TExecNodePhase::GETNEXT, state));
  RETURN_IF_CANCELLED(state);
  JNIEnv* env = JniUtil::GetJNIEnv();
  if (env == nullptr) return Status("Failed to get/create JVM");
  SCOPED_TIMER(materialize_tuple_timer());
  // Allocate buffer for RowBatch and init the tuple
  uint8_t* tuple_buffer;
  int64_t tuple_buffer_size;
  RETURN_IF_ERROR(row_batch->ResizeAndAllocateTupleBuffer(state, &tuple_buffer_size,
      &tuple_buffer));
  Tuple* tuple = reinterpret_cast<Tuple*>(tuple_buffer);
  tuple->Init(tuple_buffer_size);
  while (!ReachedLimit() && !row_batch->AtCapacity()) {
    // This thread is attached to the JVM, so the local references created while reading
    // the row would only be freed when it detaches. Free them after each row instead.
    JniLocalFrame jni_frame;
    RETURN_IF_ERROR(jni_frame.push(env));
    int row_idx = row_batch->AddRow();
    TupleRow* tuple_row = row_batch->GetRow(row_idx);
    tuple_row->SetTuple(0, tuple);

    // Get the next row from 'org.apache.impala.util.IcebergMetadataScanner'
    jobject struct_like_row;
    RETURN_IF_ERROR(metadata_scanner_->GetNext(env, &struct_like_row));
    // When 'struct_like_row' is null, there are no more rows to read
    if (struct_like_row == nullptr) {
      *eos = true;
      return Status::OK();
    }
    // Translate a StructLikeRow from Iceberg to Tuple
    RETURN_IF_ERROR(iceberg_row_reader_->MaterializeTuple(env, struct_like_row,
        tuple_desc_, tuple, row_batch->tuple_data_pool(), state));
    env->DeleteLocalRef(struct_like_row);
    RETURN_ERROR_IF_EXC(env);
    COUNTER_ADD(rows_read_counter(), 1);

    // Evaluate conjuncts on this tuple row
    if (ExecNode::EvalConjuncts(conjunct_evals().data(),
        conjunct_evals().size(), tuple_row)) {
      row_batch->CommitLastRow();
      tuple = reinterpret_cast<Tuple*>(
          reinterpret_cast<uint8_t*>(tuple) + tuple_desc_->byte_size());
      IncrementNumRowsReturned(1);
    } else {
      // Reset the null bits, everyhing else will be overwritten
      Tuple::ClearNullBits(tuple, tuple_desc_->null_bytes_offset(),
          tuple_desc_->num_null_bytes());
    }
  }
  if (ReachedLimit()) *eos = true;
  return Status::OK();
}

void IcebergMetadataScanNode::Close(RuntimeState* state) {
  if (is_closed()) return;
  // 'metadata_scanner_' is only created in Open(). Close() is also called if Prepare()
  // or Open() failed before that, and for nodes that were never opened, e.g. the
  // remaining children of a UNION that was cancelled or reached its limit.
  if (metadata_scanner_ != nullptr) metadata_scanner_->Close(state);
  ScanNode::Close(state);
}

Status IcebergMetadataScanNode::GetCatalogTable(jobject* jtable) {
  Frontend* fe = ExecEnv::GetInstance()->frontend();
  Status status = fe->GetCatalogTable(table_name_, jtable);
  if (!status.ok()) {
    // E.g. the table or its database was dropped after the query was analyzed. Use the
    // same prefix as IcebergMetadataScanner.checkIcebergTable() in the frontend.
    return Status(strings::Substitute("Cannot scan metadata table '$0.$1.$2': $3",
        table_name_.db_name, table_name_.table_name,
        boost::algorithm::to_lower_copy(metadata_table_name_), status.msg().msg()));
  }
  return Status::OK();
}
