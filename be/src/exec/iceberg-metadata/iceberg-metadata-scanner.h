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
#include "runtime/descriptors.h"

#include <jni.h>

namespace impala {

class RuntimeState;

/// Adapter class of the FE IcebergMetadataScanner, wraps the JNI calls as C++ methods.
class IcebergMetadataScanner {
 public:
  IcebergMetadataScanner(jobject jtable, const char* metadata_table_name,
      const TupleDescriptor* tuple_desc);

  /// JNI setup. Creates global references for Java classes and finds method ids.
  /// Initializes static members, should be called once per process lifecycle.
  static Status InitJNI() WARN_UNUSED_RESULT;

  // Initializes this object, creates the java metadata scanner object.
  Status Init(JNIEnv* env) WARN_UNUSED_RESULT;

  /// Executes an Iceberg scan through JNI.
  Status ScanMetadataTable(JNIEnv* env);

  /// Gets the next row of 'org.apache.impala.util.IcebergMetadataScanner'.
  Status GetNext(JNIEnv* env, jobject* result);

  /// Wrapper over value access methods, decides whether to access the value by accessor
  /// or by position.
  Status GetValue(JNIEnv* env, const SlotDescriptor* slot_desc,
      const jobject &struct_like_row, const jclass &clazz, jobject* result);

  /// Creates a Java ArrayScanner object that can be used to access Array items.
  /// Note that it returns a GlobalRef, that has to be released explicitly.
  Status CreateArrayScanner(JNIEnv* env, const jobject &list, jobject& result);

  /// Gets the next item of 'org.apache.impala.util.IcebergMetadataScanner.ArrayScanner'.
  Status GetNextArrayItem(JNIEnv* env, const jobject &list, jobject* result);

  /// Removes global references.
  void Close(RuntimeState* state);

 private:
  /// Global class references created with JniUtil.
  inline static jclass accessor_cl_ = nullptr;
  inline static jclass iceberg_metadata_scanner_cl_ = nullptr;
  inline static jclass iceberg_metadata_scanner_array_scanner_cl_ = nullptr;

  /// Method references created with JniUtil.
  inline static jmethodID accessor_get_ = nullptr;
  inline static jmethodID iceberg_metadata_scanner_ctor_ = nullptr;
  inline static jmethodID iceberg_metadata_scanner_scan_metadata_table_ = nullptr;
  inline static jmethodID iceberg_metadata_scanner_get_next_ = nullptr;
  inline static jmethodID iceberg_metadata_scanner_get_value_by_field_id_ = nullptr;
  inline static jmethodID iceberg_metadata_scanner_get_value_by_position_ = nullptr;
  inline static jmethodID iceberg_metadata_scanner_array_scanner_ctor_ = nullptr;
  inline static jmethodID iceberg_metadata_scanner_array_scanner_get_next_array_item_ =
      nullptr;

  /// The Impala FeTable object in Java, used to scan the metadata table.
  jobject jtable_;

  /// The name of the metadata table, used to identify which metadata table is needed.
  const char* metadata_table_name_;

  /// Top level TupleDescriptor.
  const TupleDescriptor* tuple_desc_;

  /// Iceberg metadata scanner Java object, it helps preparing the metadata table and
  /// executes an Iceberg table scan. Allows the ScanNode to fetch the metadata from
  /// the Java Heap.
  jobject jmetadata_scanner_;

  /// Maps the SlotId to a FieldId, used when obtaining an Accessor for a field.
  std::unordered_map<SlotId, int> slot_id_to_field_id_map_;

  /// Populates the slot_id_to_field_id_map_ map, with the field id an accessor can be
  /// obtained from the Iceberg library.
  /// For primitive type columns that are not a field of a struct, this can be found in
  /// the ColumnDescriptor. However, ColumnDescriptors are not available for struct
  /// fields, in this case the InitSlotIdFieldIdMapForStruct can be used.
  /// Collection types cannot be accessed through accessors, so those field ids won't be
  /// part of this map.
  Status InitSlotIdFieldIdMap(JNIEnv* env);

  /// Recursive part of the slot_id_to_field_id_map_ collection, when there is a struct in
  /// the tuple. Collects the field ids of the struct members. The type_ field inside the
  /// struct slot stores an ordered list of Iceberg Struct member field ids. This list can
  /// be indexed with the last element of SchemaPath col_path to obtain the correct field
  /// id of the struct member.
  Status InitSlotIdFieldIdMapForStruct(JNIEnv* env,
      const SlotDescriptor* struct_slot_desc);

  /// Wrappers around the Java methods.
  Status GetValueByFieldId(JNIEnv* env, const jobject &struct_like, int field_id,
      jobject* result);
  Status GetValueByPosition(JNIEnv* env, const jobject &struct_like, int pos,
      const jclass &clazz, jobject* result);

  string DebugString();
};
}
