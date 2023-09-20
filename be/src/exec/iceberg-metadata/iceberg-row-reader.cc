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
#include "exec/iceberg-metadata/iceberg-row-reader.h"
#include "runtime/runtime-state.h"
#include "runtime/timestamp-value.inline.h"
#include "runtime/tuple-row.h"
#include "util/jni-util.h"

namespace impala {

IcebergRowReader::IcebergRowReader(
    const TupleDescriptor* tuple_desc, const std::unordered_map<int, jobject>& jaccessor)
  : tuple_desc_(tuple_desc),
    jaccessors_(jaccessor) {}

Status IcebergRowReader::InitJNI() {
  DCHECK(iceberg_accessor_cl_ == nullptr) << "InitJNI() already called!";
  JNIEnv* env = JniUtil::GetJNIEnv();
  if (env == nullptr) return Status("Failed to get/create JVM");
  // Global class references:
  RETURN_IF_ERROR(JniUtil::GetGlobalClassRef(env,
      "org/apache/iceberg/Accessor", &iceberg_accessor_cl_));
  RETURN_IF_ERROR(JniUtil::GetGlobalClassRef(env,
      "java/util/List", &list_cl_));
  RETURN_IF_ERROR(JniUtil::GetGlobalClassRef(env,
      "org/apache/iceberg/types/Types$NestedField", &iceberg_nested_field_cl_));
  RETURN_IF_ERROR(JniUtil::GetGlobalClassRef(env,
      "java/lang/Boolean", &java_boolean_cl_));
  RETURN_IF_ERROR(JniUtil::GetGlobalClassRef(env,
      "java/lang/Integer", &java_int_cl_));
  RETURN_IF_ERROR(JniUtil::GetGlobalClassRef(env,
      "java/lang/Long", &java_long_cl_));
  RETURN_IF_ERROR(JniUtil::GetGlobalClassRef(env,
      "java/lang/CharSequence", &java_char_sequence_cl_));

  // Method ids:
  RETURN_IF_ERROR(JniUtil::GetMethodID(env, list_cl_, "get",
      "(I)Ljava/lang/Object;", &list_get_));
  RETURN_IF_ERROR(JniUtil::GetMethodID(env, iceberg_accessor_cl_, "get",
      "(Ljava/lang/Object;)Ljava/lang/Object;", &iceberg_accessor_get_));
  RETURN_IF_ERROR(JniUtil::GetMethodID(env, java_boolean_cl_, "booleanValue", "()Z",
      &boolean_value_));
  RETURN_IF_ERROR(JniUtil::GetMethodID(env, java_int_cl_, "intValue", "()I",
      &int_value_));
  RETURN_IF_ERROR(JniUtil::GetMethodID(env, java_long_cl_, "longValue", "()J",
      &long_value_));
  RETURN_IF_ERROR(JniUtil::GetMethodID(env, java_char_sequence_cl_, "toString",
      "()Ljava/lang/String;", &char_sequence_to_string_));
  return Status::OK();
}

Status IcebergRowReader::MaterializeRow(JNIEnv* env,
    jobject struct_like_row, Tuple* tuple, MemPool* tuple_data_pool) {
  DCHECK(env != nullptr);
  DCHECK(struct_like_row != nullptr);
  DCHECK(tuple != nullptr);
  DCHECK(tuple_data_pool != nullptr);
  for (SlotDescriptor* slot_desc: tuple_desc_->slots()) {
    jobject accessed_value = env->CallObjectMethod(jaccessors_.at(slot_desc->col_pos()),
        iceberg_accessor_get_, struct_like_row);
    RETURN_ERROR_IF_EXC(env);
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
      }
      default:
        // Skip the unsupported type and set it to NULL
        tuple->SetNull(slot_desc->null_indicator_offset());
        VLOG(3) << "Skipping unsupported column type: " << slot_desc->type().type;
    }
  }
  return Status::OK();
}

Status IcebergRowReader::WriteBooleanSlot(JNIEnv* env, jobject accessed_value,
    void* slot) {
  DCHECK(env->IsInstanceOf(accessed_value, java_boolean_cl_) == JNI_TRUE);
  jboolean result = env->CallBooleanMethod(accessed_value, boolean_value_);
  RETURN_ERROR_IF_EXC(env);
  *reinterpret_cast<bool*>(slot) = (bool)(result == JNI_TRUE);
  return Status::OK();
}

Status IcebergRowReader::WriteIntSlot(JNIEnv* env, jobject accessed_value, void* slot) {
  DCHECK(env->IsInstanceOf(accessed_value, java_int_cl_) == JNI_TRUE);
  jint result = env->CallIntMethod(accessed_value, int_value_);
  RETURN_ERROR_IF_EXC(env);
  *reinterpret_cast<int32_t*>(slot) = reinterpret_cast<int32_t>(result);
  return Status::OK();
}

Status IcebergRowReader::WriteLongSlot(JNIEnv* env, jobject accessed_value, void* slot) {
  DCHECK(env->IsInstanceOf(accessed_value, java_long_cl_) == JNI_TRUE);
  jlong result = env->CallLongMethod(accessed_value, long_value_);
  RETURN_ERROR_IF_EXC(env);
  *reinterpret_cast<int64_t*>(slot) = reinterpret_cast<int64_t>(result);
  return Status::OK();
}

Status IcebergRowReader::WriteTimeStampSlot(JNIEnv* env, jobject accessed_value,
    void* slot) {
  DCHECK(env->IsInstanceOf(accessed_value, java_long_cl_) == JNI_TRUE);
  jlong result = env->CallLongMethod(accessed_value, long_value_);
  RETURN_ERROR_IF_EXC(env);
  *reinterpret_cast<TimestampValue*>(slot) = TimestampValue::FromUnixTimeMicros(result,
      UTCPTR);
  return Status::OK();
}

Status IcebergRowReader::WriteStringSlot(JNIEnv* env, jobject accessed_value, void* slot,
      MemPool* tuple_data_pool) {
  DCHECK(env->IsInstanceOf(accessed_value, java_char_sequence_cl_) == JNI_TRUE);
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

}