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

#include <jni.h>
#include <unordered_map>

namespace impala {

class MemPool;
class Status;
class SlotDescriptor;
class Tuple;
class TupleDescriptor;

/// Row reader for Iceberg table scans, it translates a {StructLike} Java object to Impala
/// rows. It utilizes the provided {Accessor} objects to do this translation.
class IcebergRowReader {
 public:
  /// Initialize the tuple descriptor and accessors
  IcebergRowReader(const std::unordered_map<SlotId, jobject>& jaccessors);

  /// JNI setup. Create global references for Java classes and find method ids.
  /// Initializes static members, should be called once per process lifecycle.
  static Status InitJNI();

  /// Materialize the StructLike Java objects into Impala rows.
  Status MaterializeTuple(JNIEnv* env, jobject struct_like_row,
      const TupleDescriptor* tuple_desc, Tuple* tuple,  MemPool* tuple_data_pool);

 private:
  /// Global class references created with JniUtil.
  inline static jclass iceberg_accessor_cl_ = nullptr;
  inline static jclass iceberg_nested_field_cl_ = nullptr;
  inline static jclass list_cl_ = nullptr;
  inline static jclass java_boolean_cl_ = nullptr;
  inline static jclass java_int_cl_ = nullptr;
  inline static jclass java_long_cl_ = nullptr;
  inline static jclass java_char_sequence_cl_ = nullptr;

  /// Method references created with JniUtil.
  inline static jmethodID iceberg_accessor_get_ = nullptr;
  inline static jmethodID list_get_ = nullptr;
  inline static jmethodID boolean_value_ = nullptr;
  inline static jmethodID int_value_ = nullptr;
  inline static jmethodID long_value_ = nullptr;
  inline static jmethodID char_sequence_to_string_ = nullptr;

  /// Accessor map for the scan result, pairs the slot ids with the java Accessor
  /// objects.
  const std::unordered_map<SlotId, jobject> jaccessors_;

  /// Reads the value of a primitive from the StructLike, translates it to a matching
  /// Impala type and writes it into the target tuple. The related Accessor objects are
  /// stored in the jaccessors_ map and created during Prepare.
  Status WriteBooleanSlot(JNIEnv* env, jobject accessed_value, void* slot);
  Status WriteIntSlot(JNIEnv* env, jobject accessed_value, void* slot);
  Status WriteLongSlot(JNIEnv* env, jobject accessed_value, void* slot);
  /// Iceberg TimeStamp is parsed into TimestampValue.
  Status WriteTimeStampSlot(JNIEnv* env, jobject accessed_value, void* slot);
  /// To obtain a character sequence from JNI the JniUtfCharGuard class is used. Then the
  /// data has to be copied to the tuple_data_pool, because the JVM releases the reference
  /// and reclaims the memory area.
  Status WriteStringSlot(JNIEnv* env, jobject accessed_value, void* slot,
      MemPool* tuple_data_pool);
  /// Recursively calls MaterializeTuple method with the child tuple of the struct slot.
  Status WriteStructSlot(JNIEnv* env, jobject struct_like_row, SlotDescriptor* slot_desc,
      Tuple* tuple, MemPool* tuple_data_pool);
};

}
