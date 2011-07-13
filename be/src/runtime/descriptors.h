// Copyright (c) 2011 Cloudera, Inc. All rights reserved.

#ifndef IMPALA_RUNTIME_DESCRIPTORS_H
#define IMPALA_RUNTIME_DESCRIPTORS_H

#include <vector>
#include <tr1/unordered_map>
#include <vector>

#include "common/status.h"
#include "gen-cpp/Types_types.h"

namespace impala {

class ObjectPool;
class TDescriptorTable;
class TSlotDescriptor;
class TTupleDescriptor;

// for now, these are simply ints; if we find we need to generate ids in the
// backend, we can also introduce separate classes for these to make them
// assignment-incompatible
typedef int TupleId;
typedef int SlotId;

// Location information for null indicator bit for particular slot.
struct NullIndicatorOffset {
  int byte_offset;
  char bit_mask;  // to extract null indicator

  NullIndicatorOffset(int byte_offset, int bit_offset)
    : byte_offset(byte_offset), bit_mask(1 << bit_offset) {
    //assert(bit_offset >= 0 && bit_offset <= 7);
  }
};

// split this off into separate .h?
enum PrimitiveType {
  INVALID_TYPE = 0,
  TYPE_BOOLEAN,
  TYPE_TINYINT,
  TYPE_SMALLINT,
  TYPE_INT,
  TYPE_BIGINT,
  TYPE_FLOAT,
  TYPE_DOUBLE,
  TYPE_DATE,
  TYPE_DATETIME,
  TYPE_TIMESTAMP,
  TYPE_STRING
};

extern PrimitiveType ThriftToType(TPrimitiveType::type ttype);

class SlotDescriptor {
 public:
  PrimitiveType data_type() const { return type_; }
  int tuple_offset() const { return tuple_offset_; }
  const NullIndicatorOffset& null_indicator_offset() const {
    return null_indicator_offset_;
  }

 protected:
  friend class DescriptorTbl;

  PrimitiveType type_;
  int tuple_offset_;
  NullIndicatorOffset null_indicator_offset_;

  SlotDescriptor(const TSlotDescriptor& tdesc);
};

class TupleDescriptor {
 public:
  int byte_size() const { return byte_size_; }

 protected:
  friend class DescriptorTbl;

  TupleDescriptor(const TTupleDescriptor& tdesc);
  int byte_size_;
  std::vector<SlotDescriptor*> slots_;

  void AddSlot(SlotDescriptor* slot);
};

class DescriptorTbl {
 public:
  // Creates a descriptor tbl within 'pool' from thrift_tbl and returns it via 'tbl'.
  // Returns OK on success, otherwise error (in which case 'tbl' will be unset).
  static Status Create(ObjectPool* pool, const TDescriptorTable& thrift_tbl,
                       DescriptorTbl** tbl);

  const TupleDescriptor* GetTupleDescriptor(TupleId id) const;
  const SlotDescriptor* GetSlotDescriptor(SlotId id) const;

  // return all registered tuple descriptors
  void GetTupleDescs(std::vector<const TupleDescriptor*>* descs) const;

 private:
  typedef std::tr1::unordered_map<TupleId, TupleDescriptor*> TupleDescriptorMap;
  typedef std::tr1::unordered_map<SlotId, SlotDescriptor*> SlotDescriptorMap;

  TupleDescriptorMap tuple_desc_map_;
  SlotDescriptorMap slot_desc_map_;

  DescriptorTbl(): tuple_desc_map_(), slot_desc_map_() {}
};

}

#endif

