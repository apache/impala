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

enum TTableType {
  HDFS_TABLE,
  HBASE_TABLE
}

struct THdfsTable {
  1: required byte lineDelim
  2: required byte fieldDelim
  3: required byte collectionDelim
  4: required byte mapKeyDelim
  5: required byte escapeChar
  6: optional byte quoteChar
}

struct THBaseTable {
  1: required string tableName
  2: required list<binary> families
  3: required list<binary> qualifiers
}

// "Union" of all table types.
struct TTable {
  1: required TTableType tableType
  2: required i32 numCols
  3: required i32 numClusteringCols
  4: optional THdfsTable hdfsTable
  5: optional THBaseTable hbaseTable
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
