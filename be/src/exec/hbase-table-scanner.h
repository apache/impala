// Copyright (c) 2011 Cloudera, Inc. All rights reserved.

#ifndef IMPALA_EXEC_HBASE_TABLE_SCANNER_H
#define IMPALA_EXEC_HBASE_TABLE_SCANNER_H

#include <jni.h>
#include <string>
#include <boost/scoped_ptr.hpp>

namespace impala {

class TupleDescriptor;
class RuntimeState;
class MemPool;
class Status;

// JNI wrapper class implementing minimal functionality for scanning an HBase table.
// Note: When none of the requested family/qualifiers exist in a particular row,
// HBase will not return the row at all, leading to "missing" NULL values.
// TODO: Push down predicates via HBase server-side filters.
// TODO: Related to filtering, there is a special filter that allows only selecting the keyvalues.
//       Currently, if only the row key is requested
//       all keyvalues are fetched from HBase (since there is no family/qualifier restriction).
// TODO: Enable time travel.
class HBaseTableScanner {
 public:

  // Initialize all members to NULL and set JNIEnv.
  HBaseTableScanner(JNIEnv* env);

  // JNI setup. Create global references to classes,
  // and find method ids.
  static Status Init();

  // Perform a table scan, retrieving the families/qualifiers referenced in tuple_desc.
  // If start_/stop_key is not empty, is used for the corresponding role in the scan.
  Status StartScan(const TupleDescriptor* tuple_desc,
                   const std::string& start_key, const std::string& stop_key);

  // Position cursor to next row. Sets has_next to true if more rows exist, false otherwise.
  // Returns non-ok status if an error occurred.
  Status Next(bool* has_next);

  // Release the native byte array backing the current KeyValue[] created by JNI.
  void ReleaseBuffer();

  // Get the current HBase row key.
  void GetRowKey(void** key, int* key_length);

  // Used to fetch HBase values in order of family/qualifier.
  // Fetch the next value matching family and qualifier into value/value_length.
  // If there is no match, value is set to NULL and value_length to 0.
  void GetValue(const std::string& family, const std::string& qualifier,
      void** value, int* value_length);

  // Close HTable and ResultScanner.
  void Close();

  void set_hbase_conf(jobject hbase_conf) { hbase_conf_ = hbase_conf; }

  void set_num_requested_keyvalues(int num_requested_keyvalues) {
    num_requested_keyvalues_ = num_requested_keyvalues;
  }

 private:
  static const int DEFAULT_ROWS_CACHED = 1024;

  JNIEnv* env_;

  // Global class references created with JniUtil. Cleanup is done in JniUtil::Cleanup().
  static jclass htable_cl_;
  static jclass scan_cl_;
  static jclass resultscanner_cl_;
  static jclass result_cl_;
  static jclass immutable_bytes_writable_cl_;
  static jclass keyvalue_cl_;
  static jclass hconstants_cl_;

  static jmethodID htable_ctor_;
  static jmethodID htable_get_scanner_id_;
  static jmethodID htable_close_id_;
  static jmethodID scan_ctor_;
  static jmethodID scan_set_max_versions_id_;
  static jmethodID scan_set_caching_id_;
  static jmethodID scan_add_column_id_;
  static jmethodID resultscanner_next_id_;
  static jmethodID resultscanner_close_id_;
  static jmethodID result_get_bytes_id_;
  static jmethodID result_raw_id_;
  static jmethodID immutable_bytes_writable_get_id_;
  static jmethodID keyvalue_get_buffer_id_;
  static jmethodID keyvalue_get_family_offset_id_;
  static jmethodID keyvalue_get_family_length_id_;
  static jmethodID keyvalue_get_qualifier_offset_id_;
  static jmethodID keyvalue_get_qualifier_length_id_;
  static jmethodID keyvalue_get_row_offset_id_;
  static jmethodID keyvalue_get_row_length_id_;
  static jmethodID keyvalue_get_value_offset_id_;
  static jmethodID keyvalue_get_value_length_id_;

  static jobject empty_row_;

  // HBaseConfiguration instance from runtime state.
  jobject hbase_conf_;

  // Instances related to scanning a table. Set in StartScan().
  jobject htable_;        // Java type HTable
  jobject scan_;          // Java type Scan
  jobject resultscanner_; // Java type ResultScanner

  // Helper members for retrieving results from a scan. Updated in Next(), and
  // NextValue() methods.
  jobject result_;  // Java type Result
  jobjectArray keyvalues_;  // Java type KeyValue[]. Result of result_.raw();
  jobject keyvalue_; // Java type KeyValue
  jbyteArray byte_array_;  // Byte array backing the KeyValues[] created in one call to Next().
  void* buffer_;  // C version of byte_array_.

  // Current position in keyvalues_. Incremented in NextValue(). Reset in Next().
  int keyvalue_index_;

  // Number of requested keyvalues (i.e., the number of added family/qualifier pairs).
  // Set in StartScan().
  int num_requested_keyvalues_;

  // Number of keyvalues returned from last result_.raw().
  int num_keyvalues_;

  // Indicates whether all requested keyvalues are present in the current keyvalues_.
  // If set to true, all family/qualifier comparisons are avoided in NextValue().
  bool all_keyvalues_present_;

  // Pool for allocating keys/values retrieved from HBase scan.
  // We need a temporary copy for each value, in order to
  // add a null-terminator which is required for our current text-conversion functions.
  // Memory allocated from this pool is valid until the following Next().
  boost::scoped_ptr<MemPool> value_pool_;

  // Number of rows for caching that will be passed to scanners.
  // Set in the HBase call Scan.setCaching();
  int rows_cached_;

  // Lexicographically compares s with the string at offset in buffer_ having given length.
  // Returns a value > 0 if s is greater, a value < 0 if s is smaller,
  // and 0 if they are equal.
  int CompareStrings(const std::string& s, int offset, int length);

  // Turn string row key into java byte array.
  Status CreateRowKey(const std::string& key, jbyteArray* bytes);
};

}

#endif
