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

namespace cpp impala
namespace java com.cloudera.impala.thrift

include "CatalogObjects.thrift"
include "Types.thrift"
include "Exprs.thrift"

struct TSlotDescriptor {
  1: required Types.TSlotId id
  2: required Types.TTupleId parent
  3: required Types.TColumnType slotType

  // Absolute path into the table schema pointing to the column/field materialized into
  // this slot. Contains only a single element for slots that do not materialize a
  // table column/field, e.g., slots materializing an aggregation result.
  // columnPath[i] is the the ordinal position of the column/field of the table schema
  // at level i. For example, columnPos[0] is an ordinal into the list of table columns,
  // columnPos[1] is an ordinal into the list of fields of the complex-typed column at
  // position columnPos[0], etc.
  4: required list<i32> columnPath

  5: required i32 byteOffset  // into tuple
  6: required i32 nullIndicatorByte
  7: required i32 nullIndicatorBit
  8: required i32 slotIdx
  9: required bool isMaterialized
}

// "Union" of all table types.
struct TTableDescriptor {
  1: required Types.TTableId id
  2: required CatalogObjects.TTableType tableType
  3: required i32 numCols
  4: required i32 numClusteringCols
  // Names of the columns. Clustering columns come first.
  10: optional list<string> colNames;
  5: optional CatalogObjects.THdfsTable hdfsTable
  6: optional CatalogObjects.THBaseTable hbaseTable
  9: optional CatalogObjects.TDataSourceTable dataSourceTable

  // Unqualified name of table
  7: required string tableName;

  // Name of the database that the table belongs to
  8: required string dbName;
}

struct TTupleDescriptor {
  1: required Types.TTupleId id
  2: required i32 byteSize
  3: required i32 numNullBytes
  4: optional Types.TTableId tableId

  // Absolute path into the table schema pointing to the collection whose fields
  // are materialized into this tuple. Non-empty if this tuple belongs to a
  // nested collection, empty otherwise.
  5: optional list<i32> tuplePath
}

struct TDescriptorTable {
  1: optional list<TSlotDescriptor> slotDescriptors;
  2: required list<TTupleDescriptor> tupleDescriptors;

  // all table descriptors referenced by tupleDescriptors
  3: optional list<TTableDescriptor> tableDescriptors;
}
