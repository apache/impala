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
  Status ScanMetadataTable(JNIEnv* env) WARN_UNUSED_RESULT;

  /// Gets the next row of 'org.apache.impala.util.IcebergMetadataScanner'.
  Status GetNext(JNIEnv* env, jobject* result) WARN_UNUSED_RESULT;

  /// Wrapper over value access methods, decides whether to access the value by accessor
  /// or by position.
  Status GetValue(JNIEnv* env, const SlotDescriptor* slot_desc,
      const jobject &struct_like_row, const jclass &clazz, jobject* result)
      WARN_UNUSED_RESULT;

  /// Creates a Java CollectionScanner object that can be used to access array items
  /// (elements of a Java List).
  /// Note that it returns a LocalRef, that can only be used in the current thread.
  Status CreateArrayScanner(JNIEnv* env, const jobject &list, jobject* result)
      WARN_UNUSED_RESULT;

  /// Creates a Java CollectionScanner object that can be used to access map entries
  /// (Map.Entry objects).
  /// Note that it returns a LocalRef, that can only be used in the current thread.
  Status CreateMapScanner(JNIEnv* env, const jobject &map, jobject* result)
      WARN_UNUSED_RESULT;

  /// Gets the next array item in 'result' from the given
  /// 'org.apache.impala.util.IcebergMetadataScanner.CollectionScanner'.
  Status GetNextArrayItem(JNIEnv* env, const jobject& list, jobject* result)
      WARN_UNUSED_RESULT;

  /// Gets the next map key and value in 'key' and 'value' from the given
  /// 'org.apache.impala.util.IcebergMetadataScanner.CollectionScanner'.
  Status GetNextMapKeyAndValue(JNIEnv* env, const jobject& scanner,
      jobject* key, jobject* value) WARN_UNUSED_RESULT;

  /// Helper function that extracts the contents of a java.nio.ByteBuffer into a Java
  /// primitive byte array. This is used with BINARY fields.
  Status ConvertJavaByteBufferToByteArray(JNIEnv* env, const jobject& byte_buffer,
      jbyteArray* result) WARN_UNUSED_RESULT;

  /// Removes global references.
  void Close(RuntimeState* state);

 private:
  /// Global class references created with JniUtil.
  inline static jclass iceberg_metadata_scanner_cl_ = nullptr;
  inline static jclass iceberg_metadata_scanner_collection_scanner_cl_ = nullptr;
  inline static jclass list_cl_ = nullptr;
  inline static jclass map_cl_ = nullptr;
  inline static jclass map_entry_cl_ = nullptr;
  inline static jclass byte_array_cl_ = nullptr;

  /// Method references created with JniUtil.
  inline static jmethodID iceberg_metadata_scanner_ctor_ = nullptr;
  inline static jmethodID iceberg_metadata_scanner_scan_metadata_table_ = nullptr;
  inline static jmethodID iceberg_metadata_scanner_get_next_ = nullptr;
  inline static jmethodID iceberg_metadata_scanner_get_value_by_field_id_ = nullptr;
  inline static jmethodID iceberg_metadata_scanner_get_value_by_position_ = nullptr;
  inline static jmethodID
      iceberg_metadata_scanner_collection_scanner_from_array_ = nullptr;
  inline static jmethodID
      iceberg_metadata_scanner_collection_scanner_from_map_ = nullptr;
  inline static jmethodID
      iceberg_metadata_scanner_collection_scanner_get_next_collection_item_ = nullptr;
  inline static jmethodID
      iceberg_metadata_scanner_byte_buffer_to_byte_array_ = nullptr;

  inline static jmethodID map_entry_get_key_ = nullptr;
  inline static jmethodID map_entry_get_value_ = nullptr;

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
  Status InitSlotIdFieldIdMap(JNIEnv* env) WARN_UNUSED_RESULT;

  /// Recursive part of the slot_id_to_field_id_map_ collection, when there is a struct in
  /// the tuple. Collects the field ids of the struct members. The type_ field inside the
  /// struct slot stores an ordered list of Iceberg Struct member field ids. This list can
  /// be indexed with the last element of SchemaPath col_path to obtain the correct field
  /// id of the struct member.
  Status InitSlotIdFieldIdMapForStruct(JNIEnv* env,
      const SlotDescriptor* struct_slot_desc) WARN_UNUSED_RESULT;

  /// Wrappers around the Java methods.
  Status GetNextCollectionScannerItem(JNIEnv* env, const jobject& scanner,
      jobject* result) WARN_UNUSED_RESULT;
  Status GetValueByFieldId(JNIEnv* env, const jobject &struct_like, int field_id,
      jobject* result) WARN_UNUSED_RESULT;
  Status GetValueByPosition(JNIEnv* env, const jobject &struct_like, int pos,
      const jclass &clazz, jobject* result) WARN_UNUSED_RESULT;
  template <bool IS_ARRAY>
  Status CreateArrayOrMapScanner(JNIEnv* env, const jobject &list_or_map, jobject* result)
      WARN_UNUSED_RESULT;

  string DebugString();
};
}
