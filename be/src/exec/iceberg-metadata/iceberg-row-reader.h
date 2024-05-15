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

#pragma once

#include "common/global-types.h"
#include "common/status.h"
#include "runtime/collection-value-builder.h"

#include <jni.h>
#include <unordered_map>

namespace impala {

class IcebergMetadataScanner;
class MemPool;
class RuntimeState;
class ScanNode;
class Status;
class SlotDescriptor;
class Tuple;
class TupleDescriptor;

/// Row reader for Iceberg table scans, it translates a {StructLike} Java object to an
/// Impala row. It utilizes IcebergMetadataScanner to handle this translation.
class IcebergRowReader {
 public:
  IcebergRowReader(ScanNode* scan_node, IcebergMetadataScanner* metadata_scanner);

  /// JNI setup. Creates global references for Java classes and finds method ids.
  /// Initializes static members, should be called once per process lifecycle.
  static Status InitJNI() WARN_UNUSED_RESULT;

  /// Materialize the StructLike Java objects into Impala rows.
  Status MaterializeTuple(JNIEnv* env, const jobject& struct_like_row,
      const TupleDescriptor* tuple_desc, Tuple* tuple,  MemPool* tuple_data_pool,
      RuntimeState* state);

 private:
  /// Global class references created with JniUtil.
  inline static jclass list_cl_ = nullptr;
  inline static jclass map_cl_ = nullptr;
  inline static jclass boolean_cl_ = nullptr;
  inline static jclass integer_cl_ = nullptr;
  inline static jclass long_cl_ = nullptr;
  inline static jclass float_cl_ = nullptr;
  inline static jclass double_cl_ = nullptr;
  inline static jclass char_sequence_cl_ = nullptr;
  inline static jclass byte_buffer_cl_ = nullptr;

  /// Method references created with JniUtil.
  inline static jmethodID list_size_ = nullptr;
  inline static jmethodID map_size_ = nullptr;
  inline static jmethodID boolean_value_ = nullptr;
  inline static jmethodID integer_value_ = nullptr;
  inline static jmethodID long_value_ = nullptr;
  inline static jmethodID float_value_ = nullptr;
  inline static jmethodID double_value_ = nullptr;
  inline static jmethodID char_sequence_to_string_ = nullptr;

  /// The scan node that started this row reader.
  ScanNode* scan_node_;

  /// IcebergMetadataScanner class, used to get and access values inside java objects.
  IcebergMetadataScanner* metadata_scanner_;

  /// We want to emit a warning about DECIMAL values being NULLed out at most once. This
  /// member keeps track of whether the warning has already been emitted.
  bool unsupported_decimal_warning_emitted_;

  // Writes a Java value into the target tuple. 'struct_like_row' is only used for struct
  // types. It is needed because struct children reside directly in the parent tuple of
  // the struct.
  Status WriteSlot(JNIEnv* env, const jobject* struct_like_row,
      const jobject& accessed_value, const SlotDescriptor* slot_desc, Tuple* tuple,
      MemPool* tuple_data_pool, RuntimeState* state) WARN_UNUSED_RESULT;

  /// Translates the value of a Java primitive to the matching Impala type and writes it
  /// into the target slot.
  Status WriteBooleanSlot(JNIEnv* env, const jobject &accessed_value, void* slot)
      WARN_UNUSED_RESULT;
  Status WriteDateSlot(JNIEnv* env, const jobject &accessed_value, void* slot)
      WARN_UNUSED_RESULT;
  Status WriteIntSlot(JNIEnv* env, const jobject &accessed_value, void* slot)
      WARN_UNUSED_RESULT;
  Status WriteLongSlot(JNIEnv* env, const jobject &accessed_value, void* slot)
      WARN_UNUSED_RESULT;
  Status WriteFloatSlot(JNIEnv* env, const jobject &accessed_value, void* slot)
      WARN_UNUSED_RESULT;
  Status WriteDoubleSlot(JNIEnv* env, const jobject &accessed_value, void* slot)
      WARN_UNUSED_RESULT;
  Status WriteDecimalSlot(const SlotDescriptor* slot_desc, Tuple* tuple,
      RuntimeState* state) WARN_UNUSED_RESULT;
  /// Iceberg TimeStamp is parsed into TimestampValue.
  Status WriteTimeStampSlot(JNIEnv* env, const jobject &accessed_value, void* slot)
      WARN_UNUSED_RESULT;
  template <bool IS_BINARY>
  Status WriteStringOrBinarySlot(JNIEnv* env, const jobject &accessed_value, void* slot,
      MemPool* tuple_data_pool) WARN_UNUSED_RESULT;

  /// Nested types recursively call MaterializeTuple method with their child tuple.
  Status WriteStructSlot(JNIEnv* env, const jobject &struct_like_row,
      const SlotDescriptor* slot_desc, Tuple* tuple, MemPool* tuple_data_pool,
      RuntimeState* state) WARN_UNUSED_RESULT;
  template <bool IS_ARRAY>
  Status WriteCollectionSlot(JNIEnv* env, const jobject &accessed_value,
      CollectionValue* slot, const SlotDescriptor* slot_desc, MemPool* tuple_data_pool,
      RuntimeState* state) WARN_UNUSED_RESULT;
  Status WriteArrayItem(JNIEnv* env, const jobject& collection_scanner,
      const TupleDescriptor* item_tuple_desc, Tuple* tuple,
      MemPool* tuple_data_pool_collection, RuntimeState* state) WARN_UNUSED_RESULT;
  Status WriteMapKeyAndValue(JNIEnv* env, const jobject& collection_scanner,
      const TupleDescriptor* item_tuple_desc, Tuple* tuple,
      MemPool* tuple_data_pool_collection, RuntimeState* state) WARN_UNUSED_RESULT;

  /// Extracts an int32_t from a Java integer object and writes it into 'res'. 'jinteger'
  /// must not be null and of type java.lang.Integer.
  Status ExtractJavaInteger(JNIEnv* env, const jobject &jinteger, int32_t* res)
      WARN_UNUSED_RESULT;

  /// Helper method that gives back the Iceberg Java class for a ColumnType. It is
  /// specified in this class, to avoid defining all the Java type classes in other
  /// classes.
  jclass JavaClassFromImpalaType(const ColumnType type);
};

}
