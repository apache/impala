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

#include "exec/iceberg-metadata/iceberg-metadata-scanner.h"
#include "util/jni-util.h"

namespace impala {

IcebergMetadataScanner::IcebergMetadataScanner(jobject jtable,
    const char* metadata_table_name, const TupleDescriptor* tuple_desc)
  : jtable_(jtable),
    metadata_table_name_(metadata_table_name),
    tuple_desc_(tuple_desc) {}

Status IcebergMetadataScanner::InitJNI() {
  DCHECK(iceberg_metadata_scanner_cl_ == nullptr) << "InitJNI() already called!";
  JNIEnv* env = JniUtil::GetJNIEnv();
  if (env == nullptr) return Status("Failed to get/create JVM");

  // Global class references:
  RETURN_IF_ERROR(JniUtil::GetGlobalClassRef(env,
      "org/apache/impala/util/IcebergMetadataScanner",
      &iceberg_metadata_scanner_cl_));
  RETURN_IF_ERROR(JniUtil::GetGlobalClassRef(env,
      "org/apache/impala/util/IcebergMetadataScanner$CollectionScanner",
      &iceberg_metadata_scanner_collection_scanner_cl_));
  RETURN_IF_ERROR(JniUtil::GetGlobalClassRef(env,
      "java/util/Map$Entry", &map_entry_cl_));
  RETURN_IF_ERROR(JniUtil::GetGlobalClassRef(env, "java/util/List", &list_cl_));
  RETURN_IF_ERROR(JniUtil::GetGlobalClassRef(env, "java/util/Map", &map_cl_));
  RETURN_IF_ERROR(JniUtil::GetGlobalClassRef(env, "[B", &byte_array_cl_));

  // Method ids:
  RETURN_IF_ERROR(JniUtil::GetMethodID(env, iceberg_metadata_scanner_cl_,
      "<init>", "(Lorg/apache/impala/catalog/FeIcebergTable;Ljava/lang/String;)V",
      &iceberg_metadata_scanner_ctor_));
  RETURN_IF_ERROR(JniUtil::GetMethodID(env, iceberg_metadata_scanner_cl_,
      "ScanMetadataTable", "()V", &iceberg_metadata_scanner_scan_metadata_table_));
  RETURN_IF_ERROR(JniUtil::GetMethodID(env, iceberg_metadata_scanner_cl_,
      "GetNext", "()Lorg/apache/iceberg/StructLike;",
      &iceberg_metadata_scanner_get_next_));
  RETURN_IF_ERROR(JniUtil::GetMethodID(env, iceberg_metadata_scanner_cl_,
      "GetValueByFieldId", "(Lorg/apache/iceberg/StructLike;I)Ljava/lang/Object;",
      &iceberg_metadata_scanner_get_value_by_field_id_));
  RETURN_IF_ERROR(JniUtil::GetMethodID(env, iceberg_metadata_scanner_cl_,
      "GetValueByPosition",
      "(Lorg/apache/iceberg/StructLike;ILjava/lang/Class;)Ljava/lang/Object;",
      &iceberg_metadata_scanner_get_value_by_position_));
  RETURN_IF_ERROR(JniUtil::GetStaticMethodID(env,
      iceberg_metadata_scanner_collection_scanner_cl_, "fromArray",
      "(Ljava/util/List;)Lorg/apache/impala/util/"
      "IcebergMetadataScanner$CollectionScanner;",
      &iceberg_metadata_scanner_collection_scanner_from_array_));
  RETURN_IF_ERROR(JniUtil::GetStaticMethodID(env,
      iceberg_metadata_scanner_collection_scanner_cl_, "fromMap",
      "(Ljava/util/Map;)Lorg/apache/impala/util/"
      "IcebergMetadataScanner$CollectionScanner;",
      &iceberg_metadata_scanner_collection_scanner_from_map_));
  RETURN_IF_ERROR(JniUtil::GetMethodID(env,
      iceberg_metadata_scanner_collection_scanner_cl_,
      "GetNextCollectionItem", "()Ljava/lang/Object;",
      &iceberg_metadata_scanner_collection_scanner_get_next_collection_item_));
  RETURN_IF_ERROR(JniUtil::GetMethodID(env, iceberg_metadata_scanner_cl_,
      "ByteBufferToByteArray", "(Ljava/nio/ByteBuffer;)[B",
      &iceberg_metadata_scanner_byte_buffer_to_byte_array_));

  RETURN_IF_ERROR(JniUtil::GetMethodID(env, map_entry_cl_, "getKey",
      "()Ljava/lang/Object;", &map_entry_get_key_));
  RETURN_IF_ERROR(JniUtil::GetMethodID(env, map_entry_cl_, "getValue",
      "()Ljava/lang/Object;", &map_entry_get_value_));
  return Status::OK();
}

Status IcebergMetadataScanner::Init(JNIEnv* env) {
  jstring jstr_metadata_table_name = env->NewStringUTF(metadata_table_name_);
  jobject jmetadata_scanner = env->NewObject(iceberg_metadata_scanner_cl_,
      iceberg_metadata_scanner_ctor_, jtable_, jstr_metadata_table_name);
  RETURN_ERROR_IF_EXC(env);
  RETURN_IF_ERROR(JniUtil::LocalToGlobalRef(env, jmetadata_scanner, &jmetadata_scanner_));
  RETURN_IF_ERROR(InitSlotIdFieldIdMap(env));
  return Status::OK();
}

Status IcebergMetadataScanner::ScanMetadataTable(JNIEnv* env) {
  env->CallObjectMethod(jmetadata_scanner_,
      iceberg_metadata_scanner_scan_metadata_table_);
  RETURN_ERROR_IF_EXC(env);
  return Status::OK();
}

Status IcebergMetadataScanner::InitSlotIdFieldIdMap(JNIEnv* env) {
  for (const SlotDescriptor* slot_desc: tuple_desc_->slots()) {
    int field_id = -1;
    if (slot_desc->col_path().size() == 1) {
      // Top level slots have ColumnDescriptors that store the field ids.
      field_id = tuple_desc_->table_desc()->GetColumnDesc(slot_desc).field_id();
    } else {
      // Non top level slots are fields of a nested type. This code path is to handle
      // slots that do not have their nested type's tuple available.
      // This loop finds the struct ColumnType node that stores the slot as it has the
      // field id list of its children.
      int root_type_index = slot_desc->col_path()[0];
      const ColumnType* current_type =
          &(tuple_desc_->table_desc()->col_descs()[root_type_index].type());
      for (int i = 1; i < slot_desc->col_path().size() - 1; ++i) {
        current_type = &current_type->children[slot_desc->col_path()[i]];
      }
      field_id = current_type->field_ids[slot_desc->col_path().back()];
    }
    DCHECK_NE(field_id, -1);
    slot_id_to_field_id_map_[slot_desc->id()] = field_id;
    if (slot_desc->type().IsStructType()) {
      RETURN_IF_ERROR(InitSlotIdFieldIdMapForStruct(env, slot_desc));
    }
  }
  return Status::OK();
}

Status IcebergMetadataScanner::InitSlotIdFieldIdMapForStruct(JNIEnv* env,
    const SlotDescriptor* struct_slot_desc) {
  DCHECK(struct_slot_desc->type().IsStructType());
  const std::vector<int>& struct_field_ids = struct_slot_desc->type().field_ids;
  for (const SlotDescriptor* child_slot_desc:
      struct_slot_desc->children_tuple_descriptor()->slots()) {
    int field_id = struct_field_ids[child_slot_desc->col_path().back()];
    slot_id_to_field_id_map_[child_slot_desc->id()] = field_id;
    if (child_slot_desc->type().IsStructType()) {
      RETURN_IF_ERROR(InitSlotIdFieldIdMapForStruct(env, child_slot_desc));
    }
  }
  return Status::OK();
}

Status IcebergMetadataScanner::GetNext(JNIEnv* env, jobject* result) {
  *result = env->CallObjectMethod(jmetadata_scanner_, iceberg_metadata_scanner_get_next_);
  RETURN_ERROR_IF_EXC(env);
  return Status::OK();
}

Status IcebergMetadataScanner::GetNextArrayItem(JNIEnv* env, const jobject& scanner,
    jobject* result) {
  return GetNextCollectionScannerItem(env, scanner, result);
}

Status IcebergMetadataScanner::GetNextMapKeyAndValue(JNIEnv* env, const jobject& scanner,
    jobject* key, jobject* value) {
  jobject map_entry;
  RETURN_IF_ERROR(GetNextCollectionScannerItem(env, scanner, &map_entry));
  DCHECK(env->IsInstanceOf(map_entry, map_entry_cl_) == JNI_TRUE);

  *key = env->CallObjectMethod(map_entry, map_entry_get_key_);
  RETURN_ERROR_IF_EXC(env);

  *value = env->CallObjectMethod(map_entry, map_entry_get_value_);
  RETURN_ERROR_IF_EXC(env);
  env->DeleteLocalRef(map_entry);
  return Status::OK();
}

Status IcebergMetadataScanner::GetNextCollectionScannerItem(JNIEnv* env,
    const jobject& scanner, jobject* result) {
  *result = env->CallObjectMethod(scanner,
      iceberg_metadata_scanner_collection_scanner_get_next_collection_item_);
  RETURN_ERROR_IF_EXC(env);
  return Status::OK();
}


Status IcebergMetadataScanner::GetValue(JNIEnv* env, const SlotDescriptor* slot_desc,
    const jobject &struct_like_row, const jclass& clazz, jobject* result) {
  DCHECK(slot_desc != nullptr);
  auto field_id_it = slot_id_to_field_id_map_.find(slot_desc->id());
  if (field_id_it != slot_id_to_field_id_map_.end()) {
    // Use accessor when it is available, these are top level primitive types, top level
    // structs and structs inside structs.
    RETURN_IF_ERROR(GetValueByFieldId(env, struct_like_row, field_id_it->second, result));
  } else {
    // Accessor is not available, this must be a STRUCT inside an ARRAY.
    DCHECK(slot_desc->parent()->isTupleOfStructSlot());
    int pos = slot_desc->col_path().back();
    RETURN_IF_ERROR(GetValueByPosition(env, struct_like_row, pos, clazz, result));
  }

  return Status::OK();
}

Status IcebergMetadataScanner::GetValueByFieldId(JNIEnv* env, const jobject &struct_like,
    int field_id, jobject* result) {
  *result = env->CallObjectMethod(jmetadata_scanner_,
      iceberg_metadata_scanner_get_value_by_field_id_, struct_like, field_id);
  RETURN_ERROR_IF_EXC(env);
  return Status::OK();
}

Status IcebergMetadataScanner::GetValueByPosition(JNIEnv* env, const jobject &struct_like,
    int pos, const jclass &clazz, jobject* result) {
  *result = env->CallObjectMethod(jmetadata_scanner_,
      iceberg_metadata_scanner_get_value_by_position_, struct_like, pos, clazz);
  RETURN_ERROR_IF_EXC(env);
  return Status::OK();
}

Status IcebergMetadataScanner::CreateArrayScanner(JNIEnv* env, const jobject &list,
    jobject* result) {
  return CreateArrayOrMapScanner</*IS_ARRAY*/ true>(env, list, result);
}

Status IcebergMetadataScanner::CreateMapScanner(JNIEnv* env, const jobject &map,
    jobject* result) {
  return CreateArrayOrMapScanner</*IS_ARRAY*/ false>(env, map, result);
}

template <bool IS_ARRAY>
Status IcebergMetadataScanner::CreateArrayOrMapScanner(JNIEnv* env,
    const jobject &list_or_map, jobject* result) {
  DCHECK(result != nullptr);

  jmethodID* factory_method;
  if constexpr (IS_ARRAY) {
    DCHECK(env->IsInstanceOf(list_or_map, list_cl_) == JNI_TRUE);
    factory_method = &iceberg_metadata_scanner_collection_scanner_from_array_;
  } else {
    DCHECK(env->IsInstanceOf(list_or_map, map_cl_) == JNI_TRUE);
    factory_method = &iceberg_metadata_scanner_collection_scanner_from_map_;
  }

  *result = env->CallStaticObjectMethod(iceberg_metadata_scanner_collection_scanner_cl_,
      *factory_method, list_or_map);
  RETURN_ERROR_IF_EXC(env);
  return Status::OK();
}

Status IcebergMetadataScanner::ConvertJavaByteBufferToByteArray(JNIEnv* env,
    const jobject& byte_buffer, jbyteArray* result) {
  jobject arr = env->CallObjectMethod(jmetadata_scanner_,
      iceberg_metadata_scanner_byte_buffer_to_byte_array_, byte_buffer);
  RETURN_ERROR_IF_EXC(env);
  DCHECK(env->IsInstanceOf(arr, byte_array_cl_));
  *result = static_cast<jbyteArray>(arr);
  return Status::OK();
}

void IcebergMetadataScanner::Close(RuntimeState* state) {
  JNIEnv* env = JniUtil::GetJNIEnv();
  if (env != nullptr) {
    if (jmetadata_scanner_ != nullptr) env->DeleteGlobalRef(jmetadata_scanner_);
  }
}

string IcebergMetadataScanner::DebugString() {
  std::stringstream out;
  out << "IcebergMetadataScanner: [ Metadata table name: "
      << metadata_table_name_ << "; ";
  std::unordered_map<impala::SlotId, int>::iterator it = slot_id_to_field_id_map_.begin();
  if (it != slot_id_to_field_id_map_.end()) {
    out << "SlotId to FieldId map: [" << it->first << " : " << it->second;
    for (it++; it != slot_id_to_field_id_map_.end(); it++) {
      out << ", " << it->first << " : " << it->second;
    }
    out << "]; ";
  }
  out << tuple_desc_->DebugString() << "]";
  return out.str();
}

}
