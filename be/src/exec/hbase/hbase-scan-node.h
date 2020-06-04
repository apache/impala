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


#ifndef IMPALA_EXEC_HBASE_SCAN_NODE_H_
#define IMPALA_EXEC_HBASE_SCAN_NODE_H_

#include <boost/scoped_ptr.hpp>
#include "runtime/descriptors.h"
#include "exec/hbase/hbase-table-scanner.h"
#include "exec/scan-node.h"

namespace impala {

class TextConverter;
class Tuple;

/// Counters:
///
///   TotalRawHbaseReadTime - the total wall clock time spent in HBase read calls.
///
class HBaseScanNode : public ScanNode {
 public:
  HBaseScanNode(ObjectPool* pool, const ScanPlanNode& pnode, const DescriptorTbl& descs);

  ~HBaseScanNode();

  /// Prepare conjuncts, create HBase columns to slots mapping,
  /// initialize hbase_scanner_, and create text_converter_.
  virtual Status Prepare(RuntimeState* state) override;

  /// Start HBase scan using hbase_scanner_.
  virtual Status Open(RuntimeState* state) override;

  /// Fill the next row batch by calling Next() on the hbase_scanner_,
  /// converting text data in HBase cells to binary data.
  virtual Status GetNext(RuntimeState* state, RowBatch* row_batch, bool* eos) override;

  /// NYI
  virtual Status Reset(RuntimeState* state, RowBatch* row_batch) override;

  /// Close the hbase_scanner_, and report errors.
  virtual void Close(RuntimeState* state) override;

  int suggested_max_caching() const { return suggested_max_caching_; }

  RuntimeProfile::Counter* hbase_read_timer() const { return hbase_read_timer_; }

 protected:
  /// Write debug string of this into out.
  virtual void DebugString(int indentation_level, std::stringstream* out) const override;

 private:
  const static int SKIP_COLUMN = -1;

  /// Compare two slots based on their column position, to sort them ascending.
  static bool CmpColPos(const SlotDescriptor* a, const SlotDescriptor* b);

  /// Name of HBase table (not necessarily the table name mapped to Hive).
  const std::string table_name_;

  /// Tuple id resolved in Prepare() to set tuple_desc_;
  TupleId tuple_id_;

  /// Descriptor of tuples read from HBase table.
  const TupleDescriptor* tuple_desc_;

  /// Descriptor of the HBase table. Set during Prepare().
  const HBaseTableDescriptor* hbase_table_;

  /// Tuple index in tuple row.
  int tuple_idx_;

  /// scan ranges of a region server
  HBaseTableScanner::ScanRangeVector scan_range_vector_;

  /// HBase Filters to be set in HBaseTableScanner.
  std::vector<THBaseFilter> filters_;

  /// Jni helper for scanning an HBase table.
  boost::scoped_ptr<HBaseTableScanner> hbase_scanner_;

  /// List of non-row-key slots sorted by col_pos(). Populated in Prepare().
  std::vector<SlotDescriptor*> sorted_non_key_slots_;

  /// List of pointers to family/qualifier/binary encoding in same sort order as
  /// sorted_non_key_slots_.
  /// The memory pointed to by the list-elements is owned by the corresponding
  /// HBaseTableDescriptor.
  std::vector<const HBaseTableDescriptor::HBaseColumnDescriptor* > sorted_cols_;

  /// Slot into which the HBase row key is written.
  /// NULL if row key is not requested.
  SlotDescriptor* row_key_slot_;

  /// True, if row key is binary encoded
  bool row_key_binary_encoded_;

  /// Helper class for converting text to other types;
  boost::scoped_ptr<TextConverter> text_converter_;

  /// Max value for "setCaching" suggested by FE. If no value was suggested by the FE, this
  /// will be 0.
  int suggested_max_caching_;

  /// Definition can be found in hbase_scan_node.cc
  RuntimeProfile::Counter* hbase_read_timer_ = nullptr;

  /// Writes a slot in tuple from an HBase value containing text data.
  /// The HBase value is converted into the appropriate target type.
  void WriteTextSlot(
      const std::string& family, const std::string& qualifier,
      void* value, int value_length, SlotDescriptor* slot,
      RuntimeState* state, MemPool* pool, Tuple* tuple, bool* error_in_row);
};

}

#endif
