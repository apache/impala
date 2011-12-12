// Copyright (c) 2011 Cloudera, Inc. All rights reserved.

namespace cpp impala
namespace java com.cloudera.impala.thrift

include "Types.thrift"

struct TSlotDescriptor {
  1: required Types.TSlotId id
  2: required Types.TTupleId parent
  3: required Types.TPrimitiveType slotType
  4: required i32 columnPos   // in originating table
  5: required i32 byteOffset  // into tuple
  6: required i32 nullIndicatorByte
  7: required i32 nullIndicatorBit
  8: required bool isMaterialized
}

enum TTableType {
  HDFS_TEXT_TABLE,
  HDFS_RCFILE_TABLE,
  HBASE_TABLE
}

struct THdfsTable {
  1: required string hdfsBaseDir
  2: required byte lineDelim
  3: required byte fieldDelim
  4: required byte collectionDelim
  5: required byte mapKeyDelim
  6: required byte escapeChar
  7: optional byte quoteChar
}

struct THBaseTable {
  1: required string tableName
  2: required list<string> families
  3: required list<string> qualifiers
}

// "Union" of all table types.
struct TTableDescriptor {
  1: required Types.TTableId id
  2: required TTableType tableType
  3: required i32 numCols
  4: required i32 numClusteringCols
  5: optional THdfsTable hdfsTable
  6: optional THBaseTable hbaseTable
}

struct TTupleDescriptor {
  1: required Types.TTupleId id
  2: required i32 byteSize
  3: optional Types.TTableId tableId
}

struct TDescriptorTable {
  1: required list<TSlotDescriptor> slotDescriptors;
  2: required list<TTupleDescriptor> tupleDescriptors;

  // all table descriptors referenced by tupleDescriptors
  3: required list<TTableDescriptor> tableDescriptors;
}
