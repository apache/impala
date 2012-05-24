// Copyright (c) 2011 Cloudera, Inc. All rights reserved.

#ifndef IMPALA_RUNTIME_DESCRIPTORS_H
#define IMPALA_RUNTIME_DESCRIPTORS_H

#include <vector>
#include <tr1/unordered_map>
#include <vector>
#include <boost/scoped_ptr.hpp>
#include <ostream>

#include "common/status.h"
#include "gen-cpp/Descriptors_types.h"  // for TTupleId
#include "gen-cpp/Types_types.h"
#include "runtime/primitive-type.h"

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

// for now, these are simply ints; if we find we need to generate ids in the
// backend, we can also introduce separate classes for these to make them
// assignment-incompatible
typedef int TupleId;
typedef int SlotId;
typedef int TableId;
typedef int PlanNodeId;

std::string TypeToString(PrimitiveType t);

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
// TODO: find a better location for this
std::ostream& operator<<(std::ostream& os, const TUniqueId& id);

class SlotDescriptor {
 public:
  SlotId id() const { return id_; }
  PrimitiveType type() const { return type_; }
  TupleId parent() const { return parent_; }
  int col_pos() const { return col_pos_; }
  // Returns the field index in the generated llvm struct for this slot's tuple
  int field_idx() const { return field_idx_; }
  int tuple_offset() const { return tuple_offset_; }
  const NullIndicatorOffset& null_indicator_offset() const {
    return null_indicator_offset_;
  }
  bool is_materialized() const { return is_materialized_; }
  bool is_nullable() const { return null_indicator_offset_.bit_mask != 0; }

  std::string DebugString() const;

  // Codegen for: bool IsNull(Tuple* tuple)
  // The codegen function is cached.
  llvm::Function* CodegenIsNull(LlvmCodeGen*, llvm::StructType* tuple);

  // Codegen for: void SetNull(Tuple* tuple) / SetNotNull
  // The codegen function is cached.
  llvm::Function* CodegenUpdateNull(LlvmCodeGen*, llvm::StructType* tuple, bool set_null);

 protected:
  friend class DescriptorTbl;
  friend class TupleDescriptor;

  const SlotId id_;
  const PrimitiveType type_;
  const TupleId parent_;
  const int col_pos_;
  const int tuple_offset_;
  const NullIndicatorOffset null_indicator_offset_;
  
  // the idx of the slot in the tuple descriptor (0-based).
  // this is provided by the FE
  const int slot_idx_;

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
  int num_cols() const { return num_cols_; }
  int num_clustering_cols() const { return num_clustering_cols_; }
  virtual std::string DebugString() const;

  // The first num_clustering_cols_ columns by position are clustering
  // columns.
  bool IsClusteringCol(const SlotDescriptor* slot_desc) const {
    return slot_desc->col_pos() < num_clustering_cols_;
  }

 protected:
  TableId id_;
  int num_cols_;
  int num_clustering_cols_;
};

class HdfsTableDescriptor : public TableDescriptor {
 public:
  HdfsTableDescriptor(const TTableDescriptor& tdesc);
  char line_delim() const { return line_delim_; }
  char field_delim() const { return field_delim_; }
  char collection_delim() const { return collection_delim_; }
  char escape_char() const { return escape_char_; }
  const std::string& hdfs_base_dir() const { return hdfs_base_dir_; }
  const std::vector<std::string>& partition_key_names() const {
    return partition_key_names_;
  }
  const std::string& null_partition_key_value() const {
    return null_partition_key_value_;
  }

  virtual std::string DebugString() const;

 protected:
  char line_delim_;
  char field_delim_;
  char collection_delim_;
  char escape_char_;
  std::string hdfs_base_dir_;
  std::vector<std::string> partition_key_names_;
  std::string null_partition_key_value_;
};

class HBaseTableDescriptor : public TableDescriptor {
 public:
  HBaseTableDescriptor(const TTableDescriptor& tdesc);
  virtual std::string DebugString() const;
  const std::string table_name() const { return table_name_; }
  const std::vector<std::pair<std::string, std::string> >& cols() const { return cols_; }

 protected:
  // native name of hbase table
  std::string table_name_;

  // List of family/qualifier pairs.
  std::vector<std::pair<std::string, std::string> > cols_;
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
class RowDescriptor {
 public:
  RowDescriptor(const DescriptorTbl& desc_tbl, const std::vector<TTupleId>& row_tuples,
      const std::vector<bool>& nullable_tuples);

  // standard copy c'tor, made explicit here
  RowDescriptor(const RowDescriptor& desc)
    : tuple_desc_map_(desc.tuple_desc_map_),
      tuple_idx_map_(desc.tuple_idx_map_) {
  }

  // dummy descriptor, needed for the JNI EvalPredicate() function
  RowDescriptor() {}

  // Returns total size in bytes.
  int GetRowSize() const;

  static const int INVALID_IDX = -1;

  // Returns INVALID_IDX if id not part of this row.
  int GetTupleIdx(TupleId id) const;

  // Return true if the Tuple of the given Tuple index is nullable.
  bool TupleIsNullable(int tuple_idx) const;

  // Return descriptors for all tuples in this row, in order of appearance.
  const std::vector<TupleDescriptor*>& tuple_descriptors() const {
    return tuple_desc_map_;
  }

  // Populate row_tuple_ids with our ids.
  void ToThrift(std::vector<TTupleId>* row_tuple_ids);

  // Return true if the tuple ids of this descriptor are a prefix
  // of the tuple ids of other_desc.
  bool IsPrefixOf(const RowDescriptor& other_desc) const;

 private:
  // map from position of tuple w/in row to its descriptor
  std::vector<TupleDescriptor*> tuple_desc_map_;

  // tuple_idx_nullable_map_[i] is true if tuple i can be null
  std::vector<bool> tuple_idx_nullable_map_;

  // map from TupleId to position of tuple w/in row
  std::vector<int> tuple_idx_map_;
};

}

#endif

