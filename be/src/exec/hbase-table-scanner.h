// Copyright 2012 Cloudera Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#ifndef IMPALA_EXEC_HBASE_TABLE_SCANNER_H
#define IMPALA_EXEC_HBASE_TABLE_SCANNER_H

#include <boost/scoped_ptr.hpp>
#include <jni.h>
#include <string>
#include <sstream>
#include <vector>
#include "gen-cpp/PlanNodes_types.h"
#include "exec/scan-node.h"
#include "runtime/hbase-table-factory.h"
#include "runtime/hbase-table.h"

namespace impala {

class TupleDescriptor;
class Tuple;
class RuntimeState;
class MemPool;
class Status;
class HBaseScanNode;

// JNI wrapper class implementing minimal functionality for scanning an HBase table.
// Caching behavior is tuned by setting hbase.client.Scan.setCaching() and
// hbase.client.setCacheBlocks().
//
// hbase.client.setCacheBlocks() is controlled by query option hbase_cache_blocks. When
// set to true, HBase region server will cache the blocks. Subsequent retrieval of the
// same data will be faster. If the table is large and the query is doing big scan, it
// should be set to false to avoid polluting the cache in the hbase region server. On the
// other hand, if the table is small and will be used several time, set it to true
// to improve query performance.
//
// hbase.client.Scan.setCaching() is DEFAULT_ROWS_CACHED by default. This value controls
// the number of rows batched together when fetching from a HBase region server. Having a
// high value will put more memory pressure on the HBase region server and having a small
// value will cause extra round trips to the HBase region server. This value can
// be overridden by the query option hbase_caching. FE will also suggest a max value such
// that it won't put too much memory pressure on the region server.

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
  HBaseTableScanner(HBaseScanNode* scan_node, HBaseTableFactory* htable_factory,
                    RuntimeState* state);

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
  Status GetRowKey(JNIEnv* env, void** key, int* key_length);

  // Write the current HBase row key into the tuple slot.
  // This is used for retrieving binary encoded data directly into the tuple.
  Status GetRowKey(JNIEnv* env, const SlotDescriptor* slot_desc, Tuple* tuple);

  // Used to fetch HBase values in order of family/qualifier.
  // Fetch the next value matching family and qualifier into value/value_length.
  // If there is no match, value is set to NULL and value_length to 0.
  Status GetValue(JNIEnv* env, const std::string& family, const std::string& qualifier,
      void** value, int* value_length);

  // Used to fetch HBase values in order of family/qualifier.
  // Fetch the next value matching family and qualifier into the tuple slot.
  // If there is no match, the tuple slot is set to null.
  // This is used for retrieving binary encoded data directly into the tuple.
  Status GetValue(JNIEnv* env, const std::string& family, const std::string& qualifier,
      const SlotDescriptor* slot_desc, Tuple* tuple);

  // Close HTable and ResultScanner.
  void Close(JNIEnv* env);

  void set_num_requested_keyvalues(int num_requested_keyvalues) {
    num_requested_keyvalues_ = num_requested_keyvalues;
  }

 private:
  static const int DEFAULT_ROWS_CACHED = 1024;

  // The enclosing HBaseScanNode.
  HBaseScanNode* scan_node_;

  // Global class references created with JniUtil. Cleanup is done in JniUtil::Cleanup().
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

  static jmethodID scan_ctor_;
  static jmethodID scan_set_max_versions_id_;
  static jmethodID scan_set_caching_id_;
  static jmethodID scan_set_cache_blocks_id_;
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

  // HBase Table factory from runtime state.
  HBaseTableFactory* htable_factory_;

  // Vector of ScanRange
  const ScanRangeVector* scan_range_vector_;
  int current_scan_range_idx_;  // the index of the current scan range

  // C++ wrapper for HTable
  boost::scoped_ptr<HBaseTable> htable_;

  // Instances related to scanning a table. Set in StartScan(). They are global references
  // because they cannot be automatically garbage collected by the JVM.
  jobject scan_;           // Java type Scan
  jobject resultscanner_;  // Java type ResultScanner

  // Helper members for retrieving results from a scan. Updated in Next() and
  // used by GetRowKey() and GetValue().
  jobjectArray keyvalues_;  // Java type KeyValue[]. Result of resultscanner_.next().raw()
  void* buffer_;  // C version of resultscanner_.next().getBytes()
  int buffer_length_;  // size of buffer

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

  // True if the scanner should set Scan.setCacheBlocks to true.
  bool cache_blocks_;

  // HBase specific counters
  RuntimeProfile::Counter* scan_setup_timer_;

  // Lexicographically compares s with the string at offset in buffer_ having given
  // length.
  // Returns a value > 0 if s is greater, a value < 0 if s is smaller,
  // and 0 if they are equal.
  int CompareStrings(const std::string& s, int offset, int length);

  // Turn strings into Java byte array.
  Status CreateByteArray(JNIEnv* env, const std::string& s, jbyteArray* bytes);

  // First time scanning the table, do some setup
  Status ScanSetup(JNIEnv* env, const TupleDescriptor* tuple_desc,
                   const std::vector<THBaseFilter>& filters);

  // Initialize the scan to the given range
  Status InitScanRange(JNIEnv* env, const ScanRange& scan_range);

  // Return the row key length and row key offset from buffer_
  inline Status GetRowKeyLengthAndOffSet(JNIEnv* env, int* length, int* offset);

  // Return the column value length and offset for the given column family name and
  // qualifier. If there's no match for the family and qualifier, is_null is set to true.
  inline Status GetValueLengthAndOffset(JNIEnv* env, const std::string& family,
      const std::string& qualifier, int* length, int* offset, bool* is_null);

  // Write to a tuple slot with the given hbase binary formatted data, which is in
  // big endian.
  // Only boolean, tinyint, smallint, int, bigint, float and double should have binary
  // formatted data.
  inline void WriteTupleSlot(const SlotDescriptor* slot_desc, Tuple* tuple, void* data);
};

}  // namespace impala

#endif
