// Copyright (c) 2011 Cloudera, Inc. All rights reserved.

namespace cpp impala
namespace java com.cloudera.impala.thrift

include "Types.thrift"

typedef i32 TTupleId
typedef i32 TSlotId

struct TSlotDescriptor {
  1: required TSlotId id
  2: required TTupleId parent
  3: required Types.TPrimitiveType type
  4: required i32 byte_offset  // into tuple
  5: required i32 null_indicator_byte
  6: required i32 null_indicator_bit
}

struct TTupleDescriptor {
  1: required TTupleId id
}

struct TDescriptorTable {
  1: required list<TSlotDescriptor> slot_descriptors;
  2: required list<TTupleDescriptor> tuple_descriptors;
}
