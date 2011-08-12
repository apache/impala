// Copyright (c) 2011 Cloudera, Inc. All rights reserved.

#ifndef IMPALA_RUNTIME_DESCRIPTORS_H
#define IMPALA_RUNTIME_DESCRIPTORS_H

#include <vector>
#include <tr1/unordered_map>
#include <vector>
#include <boost/scoped_ptr.hpp>
#include <ostream>

#include "common/status.h"
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

class SlotDescriptor {
 public:
  PrimitiveType type() const { return type_; }
  int col_pos() const { return col_pos_; }
  int tuple_offset() const { return tuple_offset_; }
  const NullIndicatorOffset& null_indicator_offset() const {
    return null_indicator_offset_;
  }

  std::string DebugString() const;

 protected:
  friend class DescriptorTbl;

  const SlotId id_;
  const PrimitiveType type_;
  const int col_pos_;
  const int tuple_offset_;
  const NullIndicatorOffset null_indicator_offset_;

  SlotDescriptor(const TSlotDescriptor& tdesc);
};

// Base class for table descriptors.
class TableDescriptor {
 public:
  TableDescriptor(const TTable& ttable);
  int num_cols() const { return num_cols_; }
  int num_clustering_cols() const { return num_clustering_cols_; }
  virtual std::string DebugString() const;

  // The first num_clustering_cols_ columns by position are clustering
  // columns.
  bool IsClusteringCol(const SlotDescriptor* slot_desc) const {
    return slot_desc->col_pos() < num_clustering_cols_;
  }

 protected:
  int num_cols_;
  int num_clustering_cols_;
};

class HdfsTableDescriptor : public TableDescriptor {
 public:
  HdfsTableDescriptor(const TTable& ttable);
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
  HBaseTableDescriptor(const TTable& ttable);
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
  const TableDescriptor* table_desc() const { return table_desc_.get(); }

  std::string DebugString() const;

 protected:
  friend class DescriptorTbl;

  const TupleId id_;
  boost::scoped_ptr<TableDescriptor> table_desc_;
  const int byte_size_;
  std::vector<SlotDescriptor*> slots_;

  TupleDescriptor(const TTupleDescriptor& tdesc);
  void AddSlot(SlotDescriptor* slot);
};

class DescriptorTbl {
 public:
  // Creates a descriptor tbl within 'pool' from thrift_tbl and returns it via 'tbl'.
  // Returns OK on success, otherwise error (in which case 'tbl' will be unset).
  static Status Create(ObjectPool* pool, const TDescriptorTable& thrift_tbl,
                       DescriptorTbl** tbl);

  TupleDescriptor* GetTupleDescriptor(TupleId id) const;
  SlotDescriptor* GetSlotDescriptor(SlotId id) const;

  // return all registered tuple descriptors
  void GetTupleDescs(std::vector<TupleDescriptor*>* descs) const;

  std::string DebugString() const;

 private:
  typedef std::tr1::unordered_map<TupleId, TupleDescriptor*> TupleDescriptorMap;
  typedef std::tr1::unordered_map<SlotId, SlotDescriptor*> SlotDescriptorMap;

  TupleDescriptorMap tuple_desc_map_;
  SlotDescriptorMap slot_desc_map_;

  DescriptorTbl(): tuple_desc_map_(), slot_desc_map_() {}
};

}

#endif

