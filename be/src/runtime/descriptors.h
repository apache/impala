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

namespace impala {

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

// Location information for null indicator bit for particular slot.
struct NullIndicatorOffset {
  int byte_offset;
  uint8_t bit_mask;  // to extract null indicator

  NullIndicatorOffset(int byte_offset, int bit_offset)
    : byte_offset(byte_offset), bit_mask(1 << bit_offset) {
    //assert(bit_offset >= 0 && bit_offset <= 7);
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
  int tuple_offset() const { return tuple_offset_; }
  const NullIndicatorOffset& null_indicator_offset() const {
    return null_indicator_offset_;
  }
  bool is_materialized() const { return is_materialized_; }

  std::string DebugString() const;

 protected:
  friend class DescriptorTbl;

  const SlotId id_;
  const PrimitiveType type_;
  const TupleId parent_;
  const int col_pos_;
  const int tuple_offset_;
  const NullIndicatorOffset null_indicator_offset_;
  const bool is_materialized_;

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
  char quote_char() const { return quote_char_; }
  bool strings_are_quoted() const { return strings_are_quoted_; }

  virtual std::string DebugString() const;

 protected:
  char line_delim_;
  char field_delim_;
  char collection_delim_;
  char escape_char_;
  char quote_char_;
  bool strings_are_quoted_;
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
  const std::vector<SlotDescriptor*>& slots() const { return slots_; }
  const std::vector<SlotDescriptor*>& string_slots() const { return string_slots_; }
  const TableDescriptor* table_desc() const { return table_desc_; }

  TupleId id() const { return id_; }
  std::string DebugString() const;

 protected:
  friend class DescriptorTbl;

  const TupleId id_;
  TableDescriptor* table_desc_;
  const int byte_size_;
  std::vector<SlotDescriptor*> slots_;  // contains all slots
  std::vector<SlotDescriptor*> string_slots_;  // contains only materialized string slots

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
  RowDescriptor(const DescriptorTbl& desc_tbl, const std::vector<TTupleId>& row_tuples);

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

  // map from TupleId to position of tuple w/in row
  std::vector<int> tuple_idx_map_;
};

}

#endif

