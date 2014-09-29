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


#ifndef IMPALA_RUNTIME_DESCRIPTORS_H
#define IMPALA_RUNTIME_DESCRIPTORS_H

#include <vector>
#include <tr1/unordered_map>
#include <vector>
#include <boost/scoped_ptr.hpp>
#include <ostream>

#include "common/status.h"
#include "common/global-types.h"
#include "runtime/types.h"

#include "gen-cpp/Descriptors_types.h"  // for TTupleId
#include "gen-cpp/Types_types.h"

namespace llvm {
  class Function;
  class PointerType;
  class StructType;
};

namespace impala {

class LlvmCodeGen;
class ObjectPool;
class TDescriptorTable;
class TSlotDescriptor;
class TTable;
class TTupleDescriptor;
class Expr;
class ExprContext;
class RuntimeState;

struct LlvmTupleStruct {
  llvm::StructType* tuple_struct;
  llvm::PointerType* tuple_ptr;
  std::vector<int> indices;
};

// Location information for null indicator bit for particular slot.
// For non-nullable slots, the byte_offset will be 0 and the bit_mask will be 0.
// This allows us to do the NullIndicatorOffset operations (tuple + byte_offset &/|
// bit_mask) regardless of whether the slot is nullable or not.
// This is more efficient than branching to check if the slot is non-nullable.
struct NullIndicatorOffset {
  int byte_offset;
  uint8_t bit_mask;  // to extract null indicator

  NullIndicatorOffset(int byte_offset, int bit_offset)
    : byte_offset(byte_offset),
      bit_mask(bit_offset == -1 ? 0 : 1 << bit_offset) {
  }

  std::string DebugString() const;
};

std::ostream& operator<<(std::ostream& os, const NullIndicatorOffset& null_indicator);

class SlotDescriptor {
 public:
  SlotId id() const { return id_; }
  const ColumnType& type() const { return type_; }
  TupleId parent() const { return parent_; }
  // Returns the column index of this slot, including partition keys.
  // (e.g., col_pos - num_partition_keys = the table column this slot corresponds to)
  int col_pos() const { return col_pos_; }
  // Returns the field index in the generated llvm struct for this slot's tuple
  int field_idx() const { return field_idx_; }
  int tuple_offset() const { return tuple_offset_; }
  const NullIndicatorOffset& null_indicator_offset() const {
    return null_indicator_offset_;
  }
  bool is_materialized() const { return is_materialized_; }
  bool is_nullable() const { return null_indicator_offset_.bit_mask != 0; }
  int slot_size() const { return slot_size_; }

  std::string DebugString() const;

  // Codegen for: bool IsNull(Tuple* tuple)
  // The codegen function is cached.
  llvm::Function* CodegenIsNull(LlvmCodeGen*, llvm::StructType* tuple);

  // Codegen for: void SetNull(Tuple* tuple) / SetNotNull
  // The codegen function is cached.
  llvm::Function* CodegenUpdateNull(LlvmCodeGen*, llvm::StructType* tuple, bool set_null);

 private:
  friend class DescriptorTbl;
  friend class TupleDescriptor;

  const SlotId id_;
  const ColumnType type_;
  const TupleId parent_;
  const int col_pos_;
  const int tuple_offset_;
  const NullIndicatorOffset null_indicator_offset_;

  // the idx of the slot in the tuple descriptor (0-based).
  // this is provided by the FE
  const int slot_idx_;

  // the byte size of this slot.
  const int slot_size_;

  // the idx of the slot in the llvm codegen'd tuple struct
  // this is set by TupleDescriptor during codegen and takes into account
  // leading null bytes.
  int field_idx_;

  const bool is_materialized_;

  // Cached codegen'd functions
  llvm::Function* is_null_fn_;
  llvm::Function* set_not_null_fn_;
  llvm::Function* set_null_fn_;

  SlotDescriptor(const TSlotDescriptor& tdesc);
};

// Base class for table descriptors.
class TableDescriptor {
 public:
  TableDescriptor(const TTableDescriptor& tdesc);
  virtual ~TableDescriptor() {}
  int num_cols() const { return num_cols_; }
  int num_clustering_cols() const { return num_clustering_cols_; }
  virtual std::string DebugString() const;

  // The first num_clustering_cols_ columns by position are clustering
  // columns.
  bool IsClusteringCol(const SlotDescriptor* slot_desc) const {
    return slot_desc->col_pos() < num_clustering_cols_;
  }

  const std::string& name() const { return name_; }
  const std::string& database() const { return database_; }
  const std::vector<std::string>& col_names() const { return col_names_; }

 protected:
  std::string name_;
  std::string database_;
  TableId id_;
  int num_cols_;
  int num_clustering_cols_;
  std::vector<std::string> col_names_;
};

// Metadata for a single partition inside an Hdfs table.
class HdfsPartitionDescriptor {
 public:
  HdfsPartitionDescriptor(const THdfsPartition& thrift_partition, ObjectPool* pool);
  char line_delim() const { return line_delim_; }
  char field_delim() const { return field_delim_; }
  char collection_delim() const { return collection_delim_; }
  char escape_char() const { return escape_char_; }
  THdfsFileFormat::type file_format() const { return file_format_; }
  const std::vector<ExprContext*>& partition_key_value_ctxs() const {
    return partition_key_value_ctxs_;
  }
  int block_size() const { return block_size_; }
  const std::string& location() const { return location_; }
  int64_t id() const { return id_; }

  // Calls Prepare()/Open()/Close() on all partition key exprs. Idempotent (this is
  // because both HdfsScanNode and HdfsTableSink may both use the same partition desc).
  Status PrepareExprs(RuntimeState* state);
  Status OpenExprs(RuntimeState* state);
  void CloseExprs(RuntimeState* state);

  std::string DebugString() const;

 private:
  char line_delim_;
  char field_delim_;
  char collection_delim_;
  char escape_char_;
  int block_size_;
  std::string location_;
  int64_t id_;

  // True if PrepareExprs has been called, to prevent repeating expensive codegen
  bool exprs_prepared_;
  bool exprs_opened_;
  bool exprs_closed_;

  // List of literal (and therefore constant) expressions for each partition key. Their
  // order corresponds to the first num_clustering_cols of the parent table.
  std::vector<ExprContext*> partition_key_value_ctxs_;

  // The format (e.g. text, sequence file etc.) of data in the files in this partition
  THdfsFileFormat::type file_format_;

  // For allocating expression objects in partition_key_values_
  // Owned by DescriptorTbl, supplied in constructor.
  ObjectPool* object_pool_;
};

class HdfsTableDescriptor : public TableDescriptor {
 public:
  HdfsTableDescriptor(const TTableDescriptor& tdesc, ObjectPool* pool);
  const std::string& hdfs_base_dir() const { return hdfs_base_dir_; }
  const std::string& null_partition_key_value() const {
    return null_partition_key_value_;
  }
  const std::string& null_column_value() const { return null_column_value_; }
  const std::string& avro_schema() const { return avro_schema_; }

  typedef std::map<int64_t, HdfsPartitionDescriptor*> PartitionIdToDescriptorMap;

  HdfsPartitionDescriptor* GetPartition(int64_t partition_id) const {
    PartitionIdToDescriptorMap::const_iterator it =
        partition_descriptors_.find(partition_id);
    if (it == partition_descriptors_.end()) return NULL;
    return it->second;
  }

  const PartitionIdToDescriptorMap& partition_descriptors() const {
    return partition_descriptors_;
  }

  virtual std::string DebugString() const;

 protected:
  std::string hdfs_base_dir_;
  std::string null_partition_key_value_;
  // Special string to indicate NULL values in text-encoded columns.
  std::string null_column_value_;
  PartitionIdToDescriptorMap partition_descriptors_;
  // Set to the table's Avro schema if this is an Avro table, empty string otherwise
  std::string avro_schema_;
  // Owned by DescriptorTbl
  ObjectPool* object_pool_;
};

class HBaseTableDescriptor : public TableDescriptor {
 public:
  HBaseTableDescriptor(const TTableDescriptor& tdesc);
  virtual std::string DebugString() const;
  const std::string table_name() const { return table_name_; }

  struct HBaseColumnDescriptor {
    std::string family;
    std::string qualifier;
    bool binary_encoded;

    HBaseColumnDescriptor(const std::string& col_family, const std::string& col_qualifier,
        bool col_binary_encoded)
      : family(col_family),
        qualifier(col_qualifier),
        binary_encoded(col_binary_encoded){
    }
  };
  const std::vector<HBaseColumnDescriptor>& cols() const { return cols_; }

 protected:
  // native name of hbase table
  std::string table_name_;

  // List of family/qualifier pairs.
  std::vector<HBaseColumnDescriptor> cols_;
};

// Descriptor for a DataSourceTable
class DataSourceTableDescriptor : public TableDescriptor {
 public:
  DataSourceTableDescriptor(const TTableDescriptor& tdesc) : TableDescriptor(tdesc) { }
  virtual std::string DebugString() const;
};

class TupleDescriptor {
 public:
  int byte_size() const { return byte_size_; }
  int num_null_bytes() const { return num_null_bytes_; }
  const std::vector<SlotDescriptor*>& slots() const { return slots_; }
  const std::vector<SlotDescriptor*>& string_slots() const { return string_slots_; }
  const TableDescriptor* table_desc() const { return table_desc_; }

  TupleId id() const { return id_; }
  std::string DebugString() const;

  // Creates a typed struct description for llvm.  The layout of the struct is computed
  // by the FE which includes the order of the fields in the resulting struct.
  // Returns the struct type or NULL if the type could not be created.
  // For example, the aggregation tuple for this query: select count(*), min(int_col_a)
  // would map to:
  // struct Tuple {
  //   int8_t   null_byte;
  //   int32_t  min_a;
  //   int64_t  count_val;
  // };
  // The resulting struct definition is cached.
  llvm::StructType* GenerateLlvmStruct(LlvmCodeGen* codegen);

 protected:
  friend class DescriptorTbl;

  const TupleId id_;
  TableDescriptor* table_desc_;
  const int byte_size_;
  const int num_null_bytes_;
  int num_materialized_slots_;
  std::vector<SlotDescriptor*> slots_;  // contains all slots
  std::vector<SlotDescriptor*> string_slots_;  // contains only materialized string slots
  llvm::StructType* llvm_struct_; // cache for the llvm struct type for this tuple desc

  TupleDescriptor(const TTupleDescriptor& tdesc);
  void AddSlot(SlotDescriptor* slot);
};

class DescriptorTbl {
 public:
  // Creates a descriptor tbl within 'pool' from thrift_tbl and returns it via 'tbl'.
  // Returns OK on success, otherwise error (in which case 'tbl' will be unset).
  static Status Create(ObjectPool* pool, const TDescriptorTable& thrift_tbl,
                       DescriptorTbl** tbl);

  TableDescriptor* GetTableDescriptor(TableId id) const;
  TupleDescriptor* GetTupleDescriptor(TupleId id) const;
  SlotDescriptor* GetSlotDescriptor(SlotId id) const;

  // return all registered tuple descriptors
  void GetTupleDescs(std::vector<TupleDescriptor*>* descs) const;

  std::string DebugString() const;

 private:
  typedef std::tr1::unordered_map<TableId, TableDescriptor*> TableDescriptorMap;
  typedef std::tr1::unordered_map<TupleId, TupleDescriptor*> TupleDescriptorMap;
  typedef std::tr1::unordered_map<SlotId, SlotDescriptor*> SlotDescriptorMap;

  TableDescriptorMap tbl_desc_map_;
  TupleDescriptorMap tuple_desc_map_;
  SlotDescriptorMap slot_desc_map_;

  DescriptorTbl(): tbl_desc_map_(), tuple_desc_map_(), slot_desc_map_() {}
};

// Records positions of tuples within row produced by ExecNode.
// TODO: this needs to differentiate between tuples contained in row
// and tuples produced by ExecNode (parallel to PlanNode.rowTupleIds and
// PlanNode.tupleIds); right now, we conflate the two (and distinguish based on
// context; for instance, HdfsScanNode uses these tids to create row batches, ie, the
// first case, whereas TopNNode uses these tids to copy output rows, ie, the second
// case)
class RowDescriptor {
 public:
  RowDescriptor(const DescriptorTbl& desc_tbl, const std::vector<TTupleId>& row_tuples,
      const std::vector<bool>& nullable_tuples);

  // standard copy c'tor, made explicit here
  RowDescriptor(const RowDescriptor& desc)
    : tuple_desc_map_(desc.tuple_desc_map_),
      tuple_idx_map_(desc.tuple_idx_map_) {
  }

  // c'tor for a row assembled from two rows
  RowDescriptor(const RowDescriptor& lhs_row_desc, const RowDescriptor& rhs_row_desc);

  RowDescriptor(const std::vector<TupleDescriptor*>& tuple_descs,
      const std::vector<bool>& nullable_tuples);

  RowDescriptor(TupleDescriptor* tuple_desc, bool is_nullable);

  // dummy descriptor, needed for the JNI EvalPredicate() function
  RowDescriptor() {}

  // Returns total size in bytes.
  // TODO: also take avg string lengths into account, ie, change this
  // to GetAvgRowSize()
  int GetRowSize() const;

  static const int INVALID_IDX = -1;

  // Returns INVALID_IDX if id not part of this row.
  int GetTupleIdx(TupleId id) const;

  // Return true if the Tuple of the given Tuple index is nullable.
  bool TupleIsNullable(int tuple_idx) const;

  // Return true if any Tuple of the row is nullable.
  bool IsAnyTupleNullable() const;

  // Return descriptors for all tuples in this row, in order of appearance.
  const std::vector<TupleDescriptor*>& tuple_descriptors() const {
    return tuple_desc_map_;
  }

  // Populate row_tuple_ids with our ids.
  void ToThrift(std::vector<TTupleId>* row_tuple_ids);

  // Return true if the tuple ids of this descriptor are a prefix
  // of the tuple ids of other_desc.
  bool IsPrefixOf(const RowDescriptor& other_desc) const;

  // Return true if the tuple ids of this descriptor match tuple ids of other desc.
  bool Equals(const RowDescriptor& other_desc) const;

  std::string DebugString() const;

 private:
  // Initializes tupleIdxMap during c'tor using the tuple_desc_map_.
  void InitTupleIdxMap();

  // map from position of tuple w/in row to its descriptor
  std::vector<TupleDescriptor*> tuple_desc_map_;

  // tuple_idx_nullable_map_[i] is true if tuple i can be null
  std::vector<bool> tuple_idx_nullable_map_;

  // map from TupleId to position of tuple w/in row
  std::vector<int> tuple_idx_map_;
};

}

#endif
