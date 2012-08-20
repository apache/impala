// Copyright (c) 2012 Cloudera, Inc. All rights reserved.

#ifndef IMPALA_EXEC_HBASE_TABLE_SCANNER_H
#define IMPALA_EXEC_HBASE_TABLE_SCANNER_H

#include <jni.h>
#include <string>
#include <sstream>
#include <boost/scoped_ptr.hpp>
#include "gen-cpp/PlanNodes_types.h"
#include "exec/scan-node.h"
#include "runtime/hbase-table-cache.h"

namespace impala {

class TupleDescriptor;
class RuntimeState;
class MemPool;
class Status;

// JNI wrapper class implementing minimal functionality for scanning an HBase table.
// Note: When none of the requested family/qualifiers exist in a particular row,
// HBase will not return the row at all, leading to "missing" NULL values.
// TODO: Related to filtering, there is a special filter that allows only selecting the
//       keyvalues.
//       Currently, if only the row key is requested
//       all keyvalues are fetched from HBase (since there is no family/qualifier
//       restriction).
// TODO: Enable time travel.
class HBaseTableScanner {
 public:

  // Initialize all members to NULL, except ScanNode and HTable cache
  // scan_node is the enclosing hbase scan node and its performance counter will be
  // updated.
  HBaseTableScanner(ScanNode* scan_node, HBaseTableCache* htable_cache);

  // JNI setup. Create global references to classes,
  // and find method ids.
  static Status Init();

  // HBase scan range; "" means unbounded
  class ScanRange {
   public:
    ScanRange()
      : start_key_(),
        stop_key_() {
    }

    const std::string& start_key() const { return start_key_; }
    const std::string& stop_key() const {return stop_key_; }
    void set_start_key(const std::string& key) { start_key_ = key; }
    void set_stop_key(const std::string& key) { stop_key_ = key; }

    // Write debug string of this ScanRange into out.
    void DebugString(int indentation_level, std::stringstream* out);

   private:
    std::string start_key_;
    std::string stop_key_;
  };

  typedef std::vector<ScanRange> ScanRangeVector;

  // Perform a table scan, retrieving the families/qualifiers referenced in tuple_desc.
  // If start_/stop_key is not empty, is used for the corresponding role in the scan.
  // Note: scan_range_vector cannot be modified for the duration of the scan.
  Status StartScan(JNIEnv* env, const TupleDescriptor* tuple_desc,
                   const ScanRangeVector& scan_range_vector,
                   const std::vector<THBaseFilter>& filters);

  // Position cursor to next row. Sets has_next to true if more rows exist, false
  // otherwise.
  // Returns non-ok status if an error occurred.
  Status Next(JNIEnv* env, bool* has_next);

  // Get the current HBase row key.
  void GetRowKey(JNIEnv* env, void** key, int* key_length);

  // Used to fetch HBase values in order of family/qualifier.
  // Fetch the next value matching family and qualifier into value/value_length.
  // If there is no match, value is set to NULL and value_length to 0.
  void GetValue(JNIEnv* env, const std::string& family, const std::string& qualifier,
      void** value, int* value_length);

  // Close HTable and ResultScanner.
  void Close(JNIEnv* env);

  void set_num_requested_keyvalues(int num_requested_keyvalues) {
    num_requested_keyvalues_ = num_requested_keyvalues;
  }

 private:
  static const int DEFAULT_ROWS_CACHED = 1024;

  // The enclosing ScanNode; it is used to update performance counters.
  ScanNode* scan_node_;

  // Global class references created with JniUtil. Cleanup is done in JniUtil::Cleanup().
  static jclass htable_cl_;
  static jclass scan_cl_;
  static jclass resultscanner_cl_;
  static jclass result_cl_;
  static jclass immutable_bytes_writable_cl_;
  static jclass keyvalue_cl_;
  static jclass hconstants_cl_;
  static jclass filter_list_cl_;
  static jclass filter_list_op_cl_;
  static jclass single_column_value_filter_cl_;
  static jclass compare_op_cl_;

  static jmethodID htable_ctor_;
  static jmethodID htable_get_scanner_id_;
  static jmethodID htable_close_id_;
  static jmethodID scan_ctor_;
  static jmethodID scan_set_max_versions_id_;
  static jmethodID scan_set_caching_id_;
  static jmethodID scan_add_column_id_;
  static jmethodID scan_set_filter_id_;
  static jmethodID scan_set_start_row_id_;
  static jmethodID scan_set_stop_row_id_;
  static jmethodID resultscanner_next_id_;
  static jmethodID resultscanner_close_id_;
  static jmethodID result_get_bytes_id_;
  static jmethodID result_raw_id_;
  static jmethodID immutable_bytes_writable_get_id_;
  static jmethodID immutable_bytes_writable_get_length_id_;
  static jmethodID immutable_bytes_writable_get_offset_id_;
  static jmethodID keyvalue_get_buffer_id_;
  static jmethodID keyvalue_get_family_offset_id_;
  static jmethodID keyvalue_get_family_length_id_;
  static jmethodID keyvalue_get_qualifier_offset_id_;
  static jmethodID keyvalue_get_qualifier_length_id_;
  static jmethodID keyvalue_get_row_offset_id_;
  static jmethodID keyvalue_get_row_length_id_;
  static jmethodID keyvalue_get_value_offset_id_;
  static jmethodID keyvalue_get_value_length_id_;
  static jmethodID filter_list_ctor_;
  static jmethodID filter_list_add_filter_id_;
  static jmethodID single_column_value_filter_ctor_;

  static jobject empty_row_;
  static jobject must_pass_all_op_;
  static jobjectArray compare_ops_;

  // HBase Table cache from runtime state.
  HBaseTableCache* htable_cache_;

  // Vector of ScanRange
  const ScanRangeVector* scan_range_vector_;
  int current_scan_range_idx_; // the index of the current scan range

  // Instances related to scanning a table. Set in StartScan().
  jobject htable_;        // Java type HTable
  jobject scan_;          // Java type Scan
  jobject resultscanner_; // Java type ResultScanner

  // Helper members for retrieving results from a scan. Updated in Next(), and
  // NextValue() methods.
  jobject result_;  // Java type Result
  jobjectArray keyvalues_;  // Java type KeyValue[]. Result of result_.raw();
  jobject keyvalue_; // Java type KeyValue
  // Byte array backing the KeyValues[] created in one call to Next().
  jbyteArray byte_array_;
  void* buffer_;  // C version of byte_array_.
  int buffer_length_; // size of buffer

  // The offset of the ImmutableByteWritable (returned by result_.getBytes()).
  int result_bytes_offset_;

  // Current position in keyvalues_. Incremented in NextValue(). Reset in Next().
  int keyvalue_index_;

  // Number of requested keyvalues (i.e., the number of added family/qualifier pairs).
  // Set in StartScan().
  int num_requested_keyvalues_;

  // number of cols requested in addition to num_requested_keyvalues_, to work around
  // hbase bug
  int num_addl_requested_cols_;

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

  // Pool for allocating buffer_
  boost::scoped_ptr<MemPool> buffer_pool_;

  // Number of rows for caching that will be passed to scanners.
  // Set in the HBase call Scan.setCaching();
  int rows_cached_;

  // HBase specific counter
  RuntimeProfile::Counter* scan_setup_timer_;

  // Lexicographically compares s with the string at offset in buffer_ having given
  // length.
  // Returns a value > 0 if s is greater, a value < 0 if s is smaller,
  // and 0 if they are equal.
  int CompareStrings(const std::string& s, int offset, int length);

  // Turn string s into java byte array.
  Status CreateByteArray(JNIEnv* env, const std::string& s, jbyteArray* bytes);

  // First time scanning the table, do some setup
  Status ScanSetup(JNIEnv* env, const TupleDescriptor* tuple_desc,
                   const std::vector<THBaseFilter>& filters);

  // Initialize the scan to the given range
  Status InitScanRange(JNIEnv* env, const ScanRange& scan_range);
};

}

#endif
