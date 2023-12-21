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
#include "exec/exec-node.inline.h"
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

Status IcebergMetadataScanNode::InitJNI() {
  DCHECK(impala_iceberg_metadata_scanner_cl_ == nullptr) << "InitJNI() already called!";
  JNIEnv* env = JniUtil::GetJNIEnv();
  if (env == nullptr) return Status("Failed to get/create JVM");
  // Global class references:
  RETURN_IF_ERROR(JniUtil::GetGlobalClassRef(env,
      "org/apache/impala/util/IcebergMetadataScanner",
      &impala_iceberg_metadata_scanner_cl_));
  // Method ids:
  RETURN_IF_ERROR(JniUtil::GetMethodID(env, impala_iceberg_metadata_scanner_cl_,
      "<init>", "(Lorg/apache/impala/catalog/FeIcebergTable;Ljava/lang/String;)V",
      &iceberg_metadata_scanner_ctor_));
  RETURN_IF_ERROR(JniUtil::GetMethodID(env, impala_iceberg_metadata_scanner_cl_,
      "ScanMetadataTable", "()V", &scan_metadata_table_));
  RETURN_IF_ERROR(JniUtil::GetMethodID(env, impala_iceberg_metadata_scanner_cl_,
      "GetNext", "()Lorg/apache/iceberg/StructLike;", &get_next_));
  RETURN_IF_ERROR(JniUtil::GetMethodID(env, impala_iceberg_metadata_scanner_cl_,
      "GetAccessor", "(I)Lorg/apache/iceberg/Accessor;", &get_accessor_));
  return Status::OK();
}

Status IcebergMetadataScanNode::Prepare(RuntimeState* state) {
  RETURN_IF_ERROR(ScanNode::Prepare(state));
  scan_prepare_timer_ = ADD_TIMER(runtime_profile(), "ScanPrepareTime");
  iceberg_api_scan_timer_ = ADD_TIMER(runtime_profile(), "IcebergApiScanTime");
  SCOPED_TIMER(scan_prepare_timer_);
  JNIEnv* env = JniUtil::GetJNIEnv();
  if (env == nullptr) return Status("Failed to get/create JVM");
  tuple_desc_ = state->desc_tbl().GetTupleDescriptor(tuple_id_);
  if (tuple_desc_ == nullptr) {
    return Status("Failed to get tuple descriptor, tuple id: " +
        std::to_string(tuple_id_));
  }
  // Get the FeTable object from the Frontend
  jobject jtable;
  RETURN_IF_ERROR(GetCatalogTable(&jtable));
  // Create the Java Scanner object and scan the table
  jstring jstr_metadata_table_name = env->NewStringUTF(metadata_table_name_.c_str());
  jobject jmetadata_scanner = env->NewObject(impala_iceberg_metadata_scanner_cl_,
      iceberg_metadata_scanner_ctor_, jtable, jstr_metadata_table_name);
  RETURN_ERROR_IF_EXC(env);
  RETURN_IF_ERROR(JniUtil::LocalToGlobalRef(env, jmetadata_scanner, &jmetadata_scanner_));
  RETURN_ERROR_IF_EXC(env);
  RETURN_IF_ERROR(ScanMetadataTable());
  RETURN_IF_ERROR(CreateFieldAccessors());
  return Status::OK();
}

Status IcebergMetadataScanNode::CreateFieldAccessors() {
  JNIEnv* env = JniUtil::GetJNIEnv();
  if (env == nullptr) return Status("Failed to get/create JVM");
  for (SlotDescriptor* slot_desc: tuple_desc_->slots()) {
    if (slot_desc->type().IsStructType()) {
      // Get the top level struct's field id from the ColumnDescriptor then recursively
      // get the field ids for struct fields
      int field_id = tuple_desc_->table_desc()->GetColumnDesc(slot_desc).field_id();
      RETURN_IF_ERROR(AddAccessorForFieldId(env, field_id, slot_desc->id()));
      RETURN_IF_ERROR(CreateFieldAccessors(env, slot_desc));
    } else if (slot_desc->col_path().size() > 1) {
      DCHECK(!slot_desc->type().IsComplexType());
      // Slot that is child of a struct without tuple, can occur when a struct member is
      // in the select list. ColumnType has a tree structure, and this loop finds the
      // STRUCT node that stores the primitive type. Because, that struct node has the
      // field id list of its childs.
      int root_type_index = slot_desc->col_path()[0];
      ColumnType* current_type = &const_cast<ColumnType&>(
          tuple_desc_->table_desc()->col_descs()[root_type_index].type());
      for (int i = 1; i < slot_desc->col_path().size() - 1; ++i) {
        current_type = &current_type->children[slot_desc->col_path()[i]];
      }
      int field_id = current_type->field_ids[slot_desc->col_path().back()];
      RETURN_IF_ERROR(AddAccessorForFieldId(env, field_id, slot_desc->id()));
    } else {
      // For primitives in the top level tuple, use the ColumnDescriptor
      int field_id = tuple_desc_->table_desc()->GetColumnDesc(slot_desc).field_id();
      RETURN_IF_ERROR(AddAccessorForFieldId(env, field_id, slot_desc->id()));
    }
  }
  return Status::OK();
}

Status IcebergMetadataScanNode::CreateFieldAccessors(JNIEnv* env,
    const SlotDescriptor* struct_slot_desc) {
  if (!struct_slot_desc->type().IsStructType()) return Status::OK();
  const std::vector<int>& struct_field_ids = struct_slot_desc->type().field_ids;
  for (SlotDescriptor* child_slot_desc:
      struct_slot_desc->children_tuple_descriptor()->slots()) {
    int field_id = struct_field_ids[child_slot_desc->col_path().back()];
    RETURN_IF_ERROR(AddAccessorForFieldId(env, field_id, child_slot_desc->id()));
    if (child_slot_desc->type().IsStructType()) {
      RETURN_IF_ERROR(CreateFieldAccessors(env, child_slot_desc));
    }
  }
  return Status::OK();
}

Status IcebergMetadataScanNode::AddAccessorForFieldId(JNIEnv* env, int field_id,
    SlotId slot_id) {
  jobject accessor_for_field = env->CallObjectMethod(jmetadata_scanner_,
      get_accessor_, field_id);
  RETURN_ERROR_IF_EXC(env);
  jobject accessor_for_field_global_ref;
  RETURN_IF_ERROR(JniUtil::LocalToGlobalRef(env, accessor_for_field,
      &accessor_for_field_global_ref));
  jaccessors_[slot_id] = accessor_for_field_global_ref;
  return Status::OK();
}

Status IcebergMetadataScanNode::Open(RuntimeState* state) {
  RETURN_IF_ERROR(ScanNode::Open(state));
  iceberg_row_reader_.reset(new IcebergRowReader(jaccessors_));
  return Status::OK();
}

Status IcebergMetadataScanNode::GetNext(RuntimeState* state, RowBatch* row_batch,
    bool* eos) {
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
  while (!row_batch->AtCapacity()) {
    int row_idx = row_batch->AddRow();
    TupleRow* tuple_row = row_batch->GetRow(row_idx);
    tuple_row->SetTuple(0, tuple);
    // Get the next row from 'org.apache.impala.util.IcebergMetadataScanner'
    jobject struct_like_row = env->CallObjectMethod(jmetadata_scanner_, get_next_);
    RETURN_ERROR_IF_EXC(env);
    // When 'struct_like_row' is null, there are no more rows to read
    if (struct_like_row == nullptr) {
      *eos = true;
      return Status::OK();
    }
    // Translate a StructLikeRow from Iceberg to Tuple
    RETURN_IF_ERROR(iceberg_row_reader_->MaterializeTuple(env, struct_like_row,
        tuple_desc_, tuple, row_batch->tuple_data_pool()));
    env->DeleteLocalRef(struct_like_row);
    RETURN_ERROR_IF_EXC(env);
    COUNTER_ADD(rows_read_counter(), 1);

    // Evaluate conjuncts on this tuple row
    if (ExecNode::EvalConjuncts(conjunct_evals().data(),
        conjunct_evals().size(), tuple_row)) {
      row_batch->CommitLastRow();
      tuple = reinterpret_cast<Tuple*>(
          reinterpret_cast<uint8_t*>(tuple) + tuple_desc_->byte_size());
    } else {
      // Reset the null bits, everyhing else will be overwritten
      Tuple::ClearNullBits(tuple, tuple_desc_->null_bytes_offset(),
          tuple_desc_->num_null_bytes());
    }
  }
  return Status::OK();
}

void IcebergMetadataScanNode::Close(RuntimeState* state) {
  if (is_closed()) return;
  JNIEnv* env = JniUtil::GetJNIEnv();
  if (env != nullptr) {
    // Close global references
    if (jmetadata_scanner_ != nullptr) env->DeleteGlobalRef(jmetadata_scanner_);
    for (auto accessor : jaccessors_) {
      if (accessor.second != nullptr) env->DeleteGlobalRef(accessor.second);
    }
  } else {
    LOG(ERROR) << "Couldn't get JNIEnv, unable to release Global JNI references";
  }
  ScanNode::Close(state);
}

Status IcebergMetadataScanNode::GetCatalogTable(jobject* jtable) {
  Frontend* fe = ExecEnv::GetInstance()->frontend();
  RETURN_IF_ERROR(fe->GetCatalogTable(table_name_, jtable));
  return Status::OK();
}

Status IcebergMetadataScanNode::ScanMetadataTable() {
  JNIEnv* env = JniUtil::GetJNIEnv();
  if (env == nullptr) return Status("Failed to get/create JVM");
  SCOPED_TIMER(iceberg_api_scan_timer_);
  env->CallObjectMethod(jmetadata_scanner_, scan_metadata_table_);
  RETURN_ERROR_IF_EXC(env);
  return Status::OK();
}
