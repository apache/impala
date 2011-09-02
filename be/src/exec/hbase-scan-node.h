// Copyright (c) 2011 Cloudera, Inc. All rights reserved.

#ifndef IMPALA_EXEC_HBASE_SCAN_NODE_H_
#define IMPALA_EXEC_HBASE_SCAN_NODE_H_

#include <boost/scoped_ptr.hpp>
#include "runtime/descriptors.h"
// TODO: Why can't we forward declare hbase-table-scanner and text-converter?
#include "exec/hbase-table-scanner.h"
#include "exec/text-converter.h"
#include "exec/exec-node.h"

namespace impala {

class HBaseScanNode : public ExecNode {
 public:
  HBaseScanNode(ObjectPool* pool, const TPlanNode& tnode);

  // Prepare conjuncts, create HBase columns to slots mapping,
  // initialize hbase_scanner_, and create text_converter_.
  virtual Status Prepare(RuntimeState* state);

  // Start HBase scan using hbase_scanner_.
  virtual Status Open(RuntimeState* state);

  // Fill the next row batch by calling Next() on the hbase_scanner_,
  // converting text data in HBase cells to binary data.
  virtual Status GetNext(RuntimeState* state, RowBatch* row_batch);

  // Close the hbase_scanner_, and report errors.
  virtual Status Close(RuntimeState* state);

 protected:
  // Write debug string of this into out.
  virtual void DebugString(int indentation_level, std::stringstream* out) const;

 private:
  const static int POOL_INIT_SIZE = 4096;
  const static int SKIP_COLUMN = -1;
  // Column 0 in the Impala metadata refers to the HBasw row key.
  const static int ROW_KEY = 0;

  // Compare two slots based on their column position, to sort them ascending.
  static bool CmpColPos(const SlotDescriptor* a, const SlotDescriptor* b);

  // Name of HBase table (not necessarily the table name mapped to Hive).
  const std::string table_name_;

  // Tuple id resolved in Prepare() to set tuple_desc_;
  TupleId tuple_id_;

  // Descriptor of tuples read from HBase table.
  const TupleDescriptor* tuple_desc_;

  // Tuple index in tuple row.
  int tuple_idx_;

  // scan range; "" means bound is not set
  std::string start_key_;
  std::string stop_key_;

  // HBase Filters to be set in HBaseTableScanner.
  std::vector<THBaseFilter> filters_;

  // Counts the total number of conversion errors for this table.
  int num_errors_;

  // Memory pools created in c'tor and destroyed in d'tor.

  // Pool for allocating tuple buffer.
  boost::scoped_ptr<MemPool> tuple_buf_pool_;

  // Pool for allocating memory for variable-length slots.
  boost::scoped_ptr<MemPool> var_len_pool_;

  // Jni helper for scanning an HBase table.
  boost::scoped_ptr<HBaseTableScanner> hbase_scanner_;

  // List of non-row-key slots sorted by col_pos(). Populated in Prepare().
  std::vector<SlotDescriptor*> sorted_non_key_slots_;

  // List of pointers to family/qualifier in same sort order as sorted_non_key_slots_.
  // The memory pointed to by the list-elements is owned by the corresponding HBaseTableDescriptor.
  std::vector<const std::pair<std::string, std::string>* > sorted_cols_;

  // Slot into which the HBase row key is written.
  // NULL if row key is not requested.
  SlotDescriptor* row_key_slot_;

  // Size of tuple buffer determined by size of tuples and capacity of row batches.
  int tuple_buf_size_;

  // Buffer where tuples are written into.
  // Must be valid until next GetNext().
  void* tuple_buf_;

  // Current tuple.
  Tuple* tuple_;

  // Helper class for converting text to other types;
  boost::scoped_ptr<TextConverter> text_converter_;

  // Writes a slot in tuple_ from an HBase value containing text data.
  // The HBase value is converted into the appropriate target type.
  void WriteTextSlot(const std::string& family, const std::string& qualifier,
      void* value, int value_length, SlotDescriptor* slot,
      RuntimeState* state, bool* error_in_row);
};

}

#endif
