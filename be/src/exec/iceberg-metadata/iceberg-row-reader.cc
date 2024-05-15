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

#include "exec/iceberg-metadata/iceberg-row-reader.h"

#include <type_traits>

#include "exec/exec-node.inline.h"
#include "exec/iceberg-metadata/iceberg-metadata-scanner.h"
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
    metadata_scanner_(metadata_scanner),
    unsupported_decimal_warning_emitted_(false) {}

Status IcebergRowReader::InitJNI() {
  DCHECK(list_cl_ == nullptr) << "InitJNI() already called!";
  JNIEnv* env = JniUtil::GetJNIEnv();
  if (env == nullptr) return Status("Failed to get/create JVM");

  // Global class references:
  RETURN_IF_ERROR(JniUtil::GetGlobalClassRef(env, "java/util/List", &list_cl_));
  RETURN_IF_ERROR(JniUtil::GetGlobalClassRef(env, "java/util/Map", &map_cl_));
  RETURN_IF_ERROR(JniUtil::GetGlobalClassRef(env, "java/lang/Boolean", &boolean_cl_));
  RETURN_IF_ERROR(JniUtil::GetGlobalClassRef(env, "java/lang/Integer", &integer_cl_));
  RETURN_IF_ERROR(JniUtil::GetGlobalClassRef(env, "java/lang/Long", &long_cl_));
  RETURN_IF_ERROR(JniUtil::GetGlobalClassRef(env, "java/lang/Float", &float_cl_));
  RETURN_IF_ERROR(JniUtil::GetGlobalClassRef(env, "java/lang/Double", &double_cl_));
  RETURN_IF_ERROR(JniUtil::GetGlobalClassRef(env, "java/lang/CharSequence",
      &char_sequence_cl_));
  RETURN_IF_ERROR(JniUtil::GetGlobalClassRef(env, "java/nio/ByteBuffer",
      &byte_buffer_cl_));

  // Method ids:
  RETURN_IF_ERROR(JniUtil::GetMethodID(env, list_cl_, "size", "()I", &list_size_));
  RETURN_IF_ERROR(JniUtil::GetMethodID(env, map_cl_, "size", "()I", &map_size_));
  RETURN_IF_ERROR(JniUtil::GetMethodID(env, boolean_cl_, "booleanValue", "()Z",
      &boolean_value_));
  RETURN_IF_ERROR(JniUtil::GetMethodID(env, integer_cl_, "intValue", "()I",
      &integer_value_));
  RETURN_IF_ERROR(JniUtil::GetMethodID(env, long_cl_, "longValue", "()J",
      &long_value_));
  RETURN_IF_ERROR(JniUtil::GetMethodID(env, long_cl_, "floatValue", "()F",
      &float_value_));
  RETURN_IF_ERROR(JniUtil::GetMethodID(env, long_cl_, "doubleValue", "()D",
      &double_value_));
  RETURN_IF_ERROR(JniUtil::GetMethodID(env, char_sequence_cl_, "toString",
      "()Ljava/lang/String;", &char_sequence_to_string_));
  return Status::OK();
}

Status IcebergRowReader::MaterializeTuple(JNIEnv* env,
    const jobject& struct_like_row, const TupleDescriptor* tuple_desc, Tuple* tuple,
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
    RETURN_IF_ERROR(WriteSlot(env, &struct_like_row, accessed_value, slot_desc, tuple,
          tuple_data_pool, state));
    env->DeleteLocalRef(accessed_value);
    RETURN_ERROR_IF_EXC(env);
  }
  return Status::OK();
}

Status IcebergRowReader::WriteSlot(JNIEnv* env, const jobject* struct_like_row,
    const jobject& accessed_value, const SlotDescriptor* slot_desc, Tuple* tuple,
    MemPool* tuple_data_pool, RuntimeState* state) {
  if (accessed_value == nullptr) {
    tuple->SetNull(slot_desc->null_indicator_offset());
    return Status::OK();
  }
  void* slot = tuple->GetSlot(slot_desc->tuple_offset());
  const ColumnType& type = slot_desc->type();
  switch (type.type) {
    case TYPE_BOOLEAN: { // java.lang.Boolean
      RETURN_IF_ERROR(WriteBooleanSlot(env, accessed_value, slot));
      break;
    } case TYPE_DATE: { // java.lang.Integer
      RETURN_IF_ERROR(WriteDateSlot(env, accessed_value, slot));
      break;
    } case TYPE_INT: { // java.lang.Integer
      RETURN_IF_ERROR(WriteIntSlot(env, accessed_value, slot));
      break;
    } case TYPE_BIGINT: { // java.lang.Long
      RETURN_IF_ERROR(WriteLongSlot(env, accessed_value, slot));
      break;
    } case TYPE_FLOAT: { // java.lang.Float
      RETURN_IF_ERROR(WriteFloatSlot(env, accessed_value, slot));
      break;
    } case TYPE_DOUBLE: { // java.lang.Double
      RETURN_IF_ERROR(WriteDoubleSlot(env, accessed_value, slot));
      break;
    } case TYPE_DECIMAL: {
      RETURN_IF_ERROR(WriteDecimalSlot(slot_desc, tuple, state));
      break;
    }case TYPE_TIMESTAMP: { // org.apache.iceberg.types.TimestampType
      RETURN_IF_ERROR(WriteTimeStampSlot(env, accessed_value, slot));
      break;
    } case TYPE_STRING: {
      if (type.IsBinaryType()) { // byte[]
        RETURN_IF_ERROR(WriteStringOrBinarySlot</* IS_BINARY */ true>(
            env, accessed_value, slot, tuple_data_pool));
      } else { // java.lang.String
        RETURN_IF_ERROR(WriteStringOrBinarySlot</* IS_BINARY */ false>(
            env, accessed_value, slot, tuple_data_pool));
      }
      break;
    } case TYPE_STRUCT: { // Struct type is not used by Impala to access values.
      DCHECK(struct_like_row != nullptr);
      RETURN_IF_ERROR(WriteStructSlot(env, *struct_like_row, slot_desc, tuple,
          tuple_data_pool, state));
      break;
    } case TYPE_ARRAY: { // java.lang.ArrayList
      RETURN_IF_ERROR(WriteCollectionSlot</*IS_ARRAY*/ true>(env, accessed_value,
          (CollectionValue*) slot, slot_desc, tuple_data_pool, state));
      break;
    } case TYPE_MAP: { // java.lang.Map
      RETURN_IF_ERROR(WriteCollectionSlot</*IS_ARRAY*/ false>(env, accessed_value,
          (CollectionValue*) slot, slot_desc, tuple_data_pool, state));
      break;
    }
    default:
      DCHECK(false) << "Unsupported column type: " << slot_desc->type().type;
      tuple->SetNull(slot_desc->null_indicator_offset());
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

Status IcebergRowReader::WriteDateSlot(JNIEnv* env, const jobject &accessed_value,
    void* slot) {
  int32_t days_since_epoch;
  RETURN_IF_ERROR(ExtractJavaInteger(env, accessed_value, &days_since_epoch));

  // This will set the value to DateValue::INVALID_DAYS_SINCE_EPOCH if it is out of range.
  DateValue result(days_since_epoch);
  *reinterpret_cast<int32_t*>(slot) = result.Value();
  return Status::OK();
}

Status IcebergRowReader::WriteIntSlot(JNIEnv* env, const jobject &accessed_value,
    void* slot) {
  return ExtractJavaInteger(env, accessed_value, reinterpret_cast<int32_t*>(slot));
}

Status IcebergRowReader::ExtractJavaInteger(JNIEnv* env, const jobject& jinteger,
    int32_t* res) {
  DCHECK(jinteger != nullptr);
  DCHECK(env->IsInstanceOf(jinteger, integer_cl_) == JNI_TRUE);
  jint result = env->CallIntMethod(jinteger, integer_value_);
  RETURN_ERROR_IF_EXC(env);

  *res = reinterpret_cast<int32_t>(result);
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

Status IcebergRowReader::WriteFloatSlot(JNIEnv* env, const jobject &accessed_value,
    void* slot) {
  DCHECK(accessed_value != nullptr);
  DCHECK(env->IsInstanceOf(accessed_value, float_cl_) == JNI_TRUE);
  jfloat result = env->CallFloatMethod(accessed_value, float_value_);
  RETURN_ERROR_IF_EXC(env);
  *reinterpret_cast<float*>(slot) = result;
  return Status::OK();
}

Status IcebergRowReader::WriteDoubleSlot(JNIEnv* env, const jobject &accessed_value,
    void* slot) {
  DCHECK(accessed_value != nullptr);
  DCHECK(env->IsInstanceOf(accessed_value, double_cl_) == JNI_TRUE);
  jdouble result = env->CallDoubleMethod(accessed_value, double_value_);
  RETURN_ERROR_IF_EXC(env);
  *reinterpret_cast<double*>(slot) = result;
  return Status::OK();
}

Status IcebergRowReader::WriteDecimalSlot(const SlotDescriptor* slot_desc, Tuple* tuple,
    RuntimeState* state) {
  // TODO IMPALA-13080: Handle DECIMALs without NULLing them out.
  constexpr const char* warning = "DECIMAL values from Iceberg metadata tables "
    "are displayed as NULL. See IMPALA-13080.";
  if (!unsupported_decimal_warning_emitted_) {
    unsupported_decimal_warning_emitted_ = true;
    LOG(WARNING) << warning;
    state->LogError(ErrorMsg(TErrorCode::NOT_IMPLEMENTED_ERROR, warning));
  }
  tuple->SetNull(slot_desc->null_indicator_offset());
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

/// To obtain bytes from JNI the JniByteArrayGuard or the JniUtfCharGuard class is used.
/// Then the data has to be copied to the tuple_data_pool, because the JVM releases the
/// reference and reclaims the memory area.
template <bool IS_BINARY>
Status IcebergRowReader::WriteStringOrBinarySlot(JNIEnv* env,
    const jobject &accessed_value, void* slot, MemPool* tuple_data_pool) {
  using jbufferType = typename std::conditional<IS_BINARY, jbyteArray, jstring>::type;
  using GuardType = typename std::conditional<
      IS_BINARY, JniByteArrayGuard, JniUtfCharGuard>::type;
  const jclass& jobject_subclass = IS_BINARY ? byte_buffer_cl_ : char_sequence_cl_;

  DCHECK(accessed_value != nullptr);
  DCHECK(env->IsInstanceOf(accessed_value, jobject_subclass) == JNI_TRUE);

  jbufferType jbuffer;
  if constexpr (IS_BINARY) {
    RETURN_IF_ERROR(metadata_scanner_->ConvertJavaByteBufferToByteArray(
        env, accessed_value, &jbuffer));
  } else {
    jbuffer = static_cast<jstring>(env->CallObjectMethod(accessed_value,
        char_sequence_to_string_));
    RETURN_ERROR_IF_EXC(env);
  }

  GuardType jbuffer_guard;
  RETURN_IF_ERROR(GuardType::create(env, jbuffer, &jbuffer_guard));
  uint32_t jbuffer_size = jbuffer_guard.get_size();

  // Allocate memory and copy the bytes from the JVM to the RowBatch.
  char* buffer = reinterpret_cast<char*>(
      tuple_data_pool->TryAllocateUnaligned(jbuffer_size));
  if (UNLIKELY(buffer == nullptr)) {
    string details = strings::Substitute("Failed to allocate $0 bytes for $1.",
        jbuffer_size, IS_BINARY ? "binary" : "string");
    return tuple_data_pool->mem_tracker()->MemLimitExceeded(
        nullptr, details, jbuffer_size);
  }

  memcpy(buffer, jbuffer_guard.get(), jbuffer_size);
  reinterpret_cast<StringValue*>(slot)->Assign(buffer, jbuffer_size);
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

template <bool IS_ARRAY>
Status IcebergRowReader::WriteCollectionSlot(JNIEnv* env, const jobject &struct_like_row,
    CollectionValue* slot, const SlotDescriptor* slot_desc,
    MemPool* tuple_data_pool, RuntimeState* state) {
  DCHECK(slot_desc != nullptr);

  if constexpr (IS_ARRAY) {
    DCHECK(slot_desc->type().IsArrayType());
    DCHECK(env->IsInstanceOf(struct_like_row, list_cl_) == JNI_TRUE);
  } else {
    DCHECK(slot_desc->type().IsMapType());
    DCHECK(env->IsInstanceOf(struct_like_row, map_cl_) == JNI_TRUE);
  }

  const TupleDescriptor* item_tuple_desc = slot_desc->children_tuple_descriptor();
  DCHECK(item_tuple_desc != nullptr);
  DCHECK_EQ(item_tuple_desc->slots().size(), IS_ARRAY ? 1 : 2);

  *slot = CollectionValue();
  CollectionValueBuilder coll_value_builder(slot, *item_tuple_desc, tuple_data_pool,
      state);

  jobject collection_scanner;
  if constexpr (IS_ARRAY) {
    RETURN_IF_ERROR(metadata_scanner_->CreateArrayScanner(env, struct_like_row,
        &collection_scanner));
  } else {
    RETURN_IF_ERROR(metadata_scanner_->CreateMapScanner(env, struct_like_row,
        &collection_scanner));
  }

  int remaining_items = env->CallIntMethod(struct_like_row,
      IS_ARRAY ? list_size_ : map_size_);
  RETURN_ERROR_IF_EXC(env);

  while (!scan_node_->ReachedLimit() && remaining_items > 0) {
    RETURN_IF_CANCELLED(state);
    MemPool* tuple_data_pool_collection = coll_value_builder.pool();
    Tuple* tuple;
    int num_tuples;
    RETURN_IF_ERROR(coll_value_builder.GetFreeMemory(&tuple, &num_tuples));
    // 'num_tuples' can be very high if we're writing to a large CollectionValue. Limit
    // the number of tuples we read at one time so we don't spend too long in the
    // 'num_tuples' loop below before checking for cancellation or limit reached.
    num_tuples = std::min(num_tuples, scan_node_->runtime_state()->batch_size());
    int num_to_commit = 0;
    while (num_to_commit < num_tuples && remaining_items > 0) {
      tuple->Init(item_tuple_desc->byte_size());

      if constexpr (IS_ARRAY) {
        RETURN_IF_ERROR(WriteArrayItem(env, collection_scanner, item_tuple_desc, tuple,
            tuple_data_pool_collection, state));
      } else {
        RETURN_IF_ERROR(WriteMapKeyAndValue(env, collection_scanner, item_tuple_desc,
            tuple, tuple_data_pool_collection, state));
      }

      // For filtering please see IMPALA-12853.
      tuple += item_tuple_desc->byte_size();
      ++num_to_commit;
      --remaining_items;
    }
    coll_value_builder.CommitTuples(num_to_commit);
  }
  env->DeleteLocalRef(collection_scanner);
  RETURN_ERROR_IF_EXC(env);
  return Status::OK();
}

Status IcebergRowReader::WriteArrayItem(JNIEnv* env, const jobject& array_scanner,
    const TupleDescriptor* item_tuple_desc, Tuple* tuple,
    MemPool* tuple_data_pool_collection, RuntimeState* state) {
  jobject item;
  RETURN_IF_ERROR(metadata_scanner_->GetNextArrayItem(env, array_scanner, &item));

  const SlotDescriptor* child_slot_desc = item_tuple_desc->slots()[0];
  const jobject* struct_like_row = child_slot_desc->type().IsStructType()
    ? &item : nullptr;

  RETURN_IF_ERROR(WriteSlot(env, struct_like_row, item, child_slot_desc, tuple,
      tuple_data_pool_collection, state));

  env->DeleteLocalRef(item);
  RETURN_ERROR_IF_EXC(env);
  return Status::OK();
}

Status IcebergRowReader::WriteMapKeyAndValue(JNIEnv* env, const jobject& map_scanner,
    const TupleDescriptor* item_tuple_desc, Tuple* tuple,
    MemPool* tuple_data_pool_collection, RuntimeState* state) {
  jobject key;
  jobject value;
  RETURN_IF_ERROR(metadata_scanner_->GetNextMapKeyAndValue(env, map_scanner,
      &key, &value));

  const SlotDescriptor* key_slot_desc = item_tuple_desc->slots()[0];
  DCHECK(!key_slot_desc->type().IsStructType());
  const jobject* key_struct_like_row = nullptr;
  RETURN_IF_ERROR(WriteSlot(env, key_struct_like_row, key, key_slot_desc, tuple,
        tuple_data_pool_collection, state));

  const SlotDescriptor* value_slot_desc = item_tuple_desc->slots()[1];
  const jobject* value_struct_like_row = value_slot_desc->type().IsStructType()
    ? &value : nullptr;
  RETURN_IF_ERROR(WriteSlot(env, value_struct_like_row, value, value_slot_desc,
        tuple, tuple_data_pool_collection, state));

  return Status::OK();
}

jclass IcebergRowReader::JavaClassFromImpalaType(const ColumnType type) {
  switch (type.type) {
    case TYPE_BOOLEAN: {         // java.lang.Boolean
      return boolean_cl_;
    } case TYPE_DATE:
      case TYPE_INT: {           // java.lang.Integer
      return integer_cl_;
    } case TYPE_BIGINT:          // java.lang.Long
      case TYPE_TIMESTAMP: {     // org.apache.iceberg.types.TimestampType
      return long_cl_;
    } case TYPE_STRING: {
      if (type.IsBinaryType()) { // java.nio.ByteBuffer
        return byte_buffer_cl_;
      } else {                   // java.lang.CharSequence
        return char_sequence_cl_;
      }
    } case TYPE_ARRAY: {         // java.util.List
      return list_cl_;
    } case TYPE_MAP: {           // java.util.Map
      return map_cl_;
    }
    default:
      VLOG(3) << "Skipping unsupported column type: " << type.type;
  }
  return nullptr;
}

}
