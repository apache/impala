// Copyright (c) 2011 Cloudera, Inc. All rights reserved.

namespace cpp impala
namespace java com.cloudera.impala.thrift

include "Types.thrift"

typedef i32 TTupleId
typedef i32 TSlotId

struct TSlotDescriptor {
  1: required TSlotId id
  2: required TTupleId parent
  3: required Types.TPrimitiveType slotType
  4: required i32 columnPos   // in originating table
  5: required i32 byteOffset  // into tuple
  6: required i32 nullIndicatorByte
  7: required i32 nullIndicatorBit
}

struct TTable {
  1: required i32 numCols
  2: required i32 numPartitionKeys
  3: required byte lineDelim
  4: required byte fieldDelim
  5: required byte collectionDelim
  6: required byte mapKeyDelim
  7: required byte escapeChar
  8: optional byte quoteChar
}

struct TTupleDescriptor {
  1: required TTupleId id
  2: required i32 byteSize
  3: optional TTable table
}

struct TDescriptorTable {
  1: required list<TSlotDescriptor> slotDescriptors;
  2: required list<TTupleDescriptor> tupleDescriptors;
}
