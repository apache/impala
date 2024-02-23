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

#include "exec/exec-node.inline.h"
#include "exec/iceberg-metadata/iceberg-metadata-scanner.h"
#include "exec/iceberg-metadata/iceberg-row-reader.h"
#include "exec/scan-node.h"
#include "runtime/collection-value-builder.h"
#include "runtime/runtime-state.h"
#include "runtime/timestamp-value.inline.h"
#include "runtime/tuple-row.h"
#include "util/jni-util.h"

namespace impala {

IcebergRowReader::IcebergRowReader(ScanNode* scan_node,
    IcebergMetadataScanner* metadata_scanner)
  : scan_node_(scan_node),
    metadata_scanner_(metadata_scanner) {}

Status IcebergRowReader::InitJNI() {
  DCHECK(list_cl_ == nullptr) << "InitJNI() already called!";
  JNIEnv* env = JniUtil::GetJNIEnv();
  if (env == nullptr) return Status("Failed to get/create JVM");

  // Global class references:
  RETURN_IF_ERROR(JniUtil::GetGlobalClassRef(env, "java/util/List", &list_cl_));
  RETURN_IF_ERROR(JniUtil::GetGlobalClassRef(env, "java/lang/Boolean", &boolean_cl_));
  RETURN_IF_ERROR(JniUtil::GetGlobalClassRef(env, "java/lang/Integer", &integer_cl_));
  RETURN_IF_ERROR(JniUtil::GetGlobalClassRef(env, "java/lang/Long", &long_cl_));
  RETURN_IF_ERROR(JniUtil::GetGlobalClassRef(env, "java/lang/CharSequence",
      &char_sequence_cl_));

  // Method ids:
  RETURN_IF_ERROR(JniUtil::GetMethodID(env, list_cl_, "get", "(I)Ljava/lang/Object;",
      &list_get_));
  RETURN_IF_ERROR(JniUtil::GetMethodID(env, list_cl_, "size", "()I", &list_size_));
  RETURN_IF_ERROR(JniUtil::GetMethodID(env, boolean_cl_, "booleanValue", "()Z",
      &boolean_value_));
  RETURN_IF_ERROR(JniUtil::GetMethodID(env, integer_cl_, "intValue", "()I",
      &integer_value_));
  RETURN_IF_ERROR(JniUtil::GetMethodID(env, long_cl_, "longValue", "()J",
      &long_value_));
  RETURN_IF_ERROR(JniUtil::GetMethodID(env, char_sequence_cl_, "toString",
      "()Ljava/lang/String;", &char_sequence_to_string_));
  return Status::OK();
}

Status IcebergRowReader::MaterializeTuple(JNIEnv* env,
    jobject struct_like_row, const TupleDescriptor* tuple_desc, Tuple* tuple,
    MemPool* tuple_data_pool, RuntimeState* state) {
  DCHECK(env != nullptr);
  DCHECK(struct_like_row != nullptr);
  DCHECK(tuple != nullptr);
  DCHECK(tuple_data_pool != nullptr);
  DCHECK(tuple_desc != nullptr);

  for (const SlotDescriptor* slot_desc: tuple_desc->slots()) {
    jobject accessed_value;
    RETURN_IF_ERROR(metadata_scanner_->GetValue(env, slot_desc, struct_like_row,
        JavaClassFromImpalaType(slot_desc->type()), &accessed_value));
    if (accessed_value == nullptr) {
      tuple->SetNull(slot_desc->null_indicator_offset());
      continue;
    }
    void* slot = tuple->GetSlot(slot_desc->tuple_offset());
    switch (slot_desc->type().type) {
      case TYPE_BOOLEAN: { // java.lang.Boolean
        RETURN_IF_ERROR(WriteBooleanSlot(env, accessed_value, slot));
        break;
      } case TYPE_INT: { // java.lang.Integer
        RETURN_IF_ERROR(WriteIntSlot(env, accessed_value, slot));
        break;
      } case TYPE_BIGINT: { // java.lang.Long
        RETURN_IF_ERROR(WriteLongSlot(env, accessed_value, slot));
        break;
      } case TYPE_TIMESTAMP: { // org.apache.iceberg.types.TimestampType
        RETURN_IF_ERROR(WriteTimeStampSlot(env, accessed_value, slot));
        break;
      } case TYPE_STRING: { // java.lang.String
        RETURN_IF_ERROR(WriteStringSlot(env, accessed_value, slot, tuple_data_pool));
        break;
      } case TYPE_STRUCT: { // Struct type is not used by Impala to access values.
        RETURN_IF_ERROR(WriteStructSlot(env, struct_like_row, slot_desc, tuple,
            tuple_data_pool, state));
        break;
      } case TYPE_ARRAY: { // java.lang.ArrayList
        RETURN_IF_ERROR(WriteArraySlot(env, accessed_value, (CollectionValue*)slot,
            slot_desc, tuple, tuple_data_pool, state));
        break;
      }
      default:
        // Skip the unsupported type and set it to NULL
        tuple->SetNull(slot_desc->null_indicator_offset());
        VLOG(3) << "Skipping unsupported column type: " << slot_desc->type().type;
    }
    env->DeleteLocalRef(accessed_value);
    RETURN_ERROR_IF_EXC(env);
  }
  return Status::OK();
}

Status IcebergRowReader::WriteBooleanSlot(JNIEnv* env, const jobject &accessed_value,
    void* slot) {
  DCHECK(accessed_value != nullptr);
  DCHECK(env->IsInstanceOf(accessed_value, boolean_cl_) == JNI_TRUE);
  jboolean result = env->CallBooleanMethod(accessed_value, boolean_value_);
  RETURN_ERROR_IF_EXC(env);
  *reinterpret_cast<bool*>(slot) = (bool)(result == JNI_TRUE);
  return Status::OK();
}

Status IcebergRowReader::WriteIntSlot(JNIEnv* env, const jobject &accessed_value,
    void* slot) {
  DCHECK(accessed_value != nullptr);
  DCHECK(env->IsInstanceOf(accessed_value, integer_cl_) == JNI_TRUE);
  jint result = env->CallIntMethod(accessed_value, integer_value_);
  RETURN_ERROR_IF_EXC(env);
  *reinterpret_cast<int32_t*>(slot) = reinterpret_cast<int32_t>(result);
  return Status::OK();
}

Status IcebergRowReader::WriteLongSlot(JNIEnv* env, const jobject &accessed_value,
    void* slot) {
  DCHECK(accessed_value != nullptr);
  DCHECK(env->IsInstanceOf(accessed_value, long_cl_) == JNI_TRUE);
  jlong result = env->CallLongMethod(accessed_value, long_value_);
  RETURN_ERROR_IF_EXC(env);
  *reinterpret_cast<int64_t*>(slot) = reinterpret_cast<int64_t>(result);
  return Status::OK();
}

Status IcebergRowReader::WriteTimeStampSlot(JNIEnv* env, const jobject &accessed_value,
    void* slot) {
  DCHECK(accessed_value != nullptr);
  DCHECK(env->IsInstanceOf(accessed_value, long_cl_) == JNI_TRUE);
  jlong result = env->CallLongMethod(accessed_value, long_value_);
  RETURN_ERROR_IF_EXC(env);
  *reinterpret_cast<TimestampValue*>(slot) = TimestampValue::FromUnixTimeMicros(result,
      UTCPTR);
  return Status::OK();
}

Status IcebergRowReader::WriteStringSlot(JNIEnv* env, const jobject &accessed_value,
    void* slot, MemPool* tuple_data_pool) {
  DCHECK(accessed_value != nullptr);
  DCHECK(env->IsInstanceOf(accessed_value, char_sequence_cl_) == JNI_TRUE);
  jstring result = static_cast<jstring>(env->CallObjectMethod(accessed_value,
      char_sequence_to_string_));
  RETURN_ERROR_IF_EXC(env);
  JniUtfCharGuard str_guard;
  RETURN_IF_ERROR(JniUtfCharGuard::create(env, result, &str_guard));
  // Allocate memory and copy the string from the JVM to the RowBatch
  int str_len = strlen(str_guard.get());
  char* buffer = reinterpret_cast<char*>(tuple_data_pool->TryAllocateUnaligned(str_len));
  if (UNLIKELY(buffer == nullptr)) {
    string details = strings::Substitute("Failed to allocate $1 bytes for string.",
        str_len);
    return tuple_data_pool->mem_tracker()->MemLimitExceeded(nullptr, details, str_len);
  }
  memcpy(buffer, str_guard.get(), str_len);
  reinterpret_cast<StringValue*>(slot)->Assign(buffer, str_len);
  return Status::OK();
}

Status IcebergRowReader::WriteStructSlot(JNIEnv* env, const jobject &struct_like_row,
    const SlotDescriptor* slot_desc, Tuple* tuple, MemPool* tuple_data_pool,
    RuntimeState* state) {
  DCHECK(slot_desc != nullptr);
  DCHECK(struct_like_row != nullptr);
  DCHECK(slot_desc->type().IsStructType());
  RETURN_IF_ERROR(MaterializeTuple(env, struct_like_row,
      slot_desc->children_tuple_descriptor(), tuple, tuple_data_pool, state));
  return Status::OK();
}

Status IcebergRowReader::WriteArraySlot(JNIEnv* env, const jobject &struct_like_row,
    CollectionValue* slot, const SlotDescriptor* slot_desc, Tuple* tuple,
    MemPool* tuple_data_pool, RuntimeState* state) {
  DCHECK(slot_desc != nullptr);
  DCHECK(slot_desc->type().IsCollectionType());
  DCHECK(env->IsInstanceOf(struct_like_row, list_cl_) == JNI_TRUE);
  const TupleDescriptor* item_tuple_desc = slot_desc->children_tuple_descriptor();
  *slot = CollectionValue();
  CollectionValueBuilder coll_value_builder(slot, *item_tuple_desc, tuple_data_pool,
      state);
  jobject array_scanner;
  RETURN_IF_ERROR(metadata_scanner_->CreateArrayScanner(env, struct_like_row,
      array_scanner));
  int remaining_array_size = env->CallIntMethod(struct_like_row, list_size_);
  RETURN_ERROR_IF_EXC(env);
  while (!scan_node_->ReachedLimit() && remaining_array_size > 0) {
    RETURN_IF_CANCELLED(state);
    int num_tuples;
    MemPool* tuple_data_pool_collection = coll_value_builder.pool();
    Tuple* tuple;
    RETURN_IF_ERROR(coll_value_builder.GetFreeMemory(&tuple, &num_tuples));
    // 'num_tuples' can be very high if we're writing to a large CollectionValue. Limit
    // the number of tuples we read at one time so we don't spend too long in the
    // 'num_tuples' loop below before checking for cancellation or limit reached.
    num_tuples = std::min(num_tuples, scan_node_->runtime_state()->batch_size());
    int num_to_commit = 0;
    while (num_to_commit < num_tuples && remaining_array_size > 0) {
      tuple->Init(item_tuple_desc->byte_size());
      jobject item;
      RETURN_IF_ERROR(metadata_scanner_->GetNextArrayItem(env, array_scanner, &item));
      RETURN_IF_ERROR(MaterializeTuple(env, item, item_tuple_desc, tuple,
          tuple_data_pool_collection, state));
      // For filtering please see IMPALA-12853.
      tuple += item_tuple_desc->byte_size();
      ++num_to_commit;
      --remaining_array_size;
    }
    coll_value_builder.CommitTuples(num_to_commit);
  }
  env->DeleteLocalRef(array_scanner);
  RETURN_ERROR_IF_EXC(env);
  return Status::OK();
}

jclass IcebergRowReader::JavaClassFromImpalaType(const ColumnType type) {
  switch (type.type) {
    case TYPE_BOOLEAN: {     // java.lang.Boolean
      return boolean_cl_;
    } case TYPE_INT: {       // java.lang.Integer
      return integer_cl_;
    } case TYPE_BIGINT:      // java.lang.Long
      case TYPE_TIMESTAMP: { // org.apache.iceberg.types.TimestampType
      return long_cl_;
    } case TYPE_STRING: {    // java.lang.String
      return char_sequence_cl_;
    } case TYPE_ARRAY: {     // java.lang.util.List
      return list_cl_;
    }
    default:
      VLOG(3) << "Skipping unsupported column type: " << type.type;
  }
  return nullptr;
}

}