// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

namespace cpp impala
namespace java org.apache.impala.thrift

include "CatalogObjects.thrift"
include "Types.thrift"
include "Exprs.thrift"

struct TSlotDescriptor {
  1: required Types.TSlotId id
  2: required Types.TTupleId parent
  // Only set for collection slots. The tuple ID of the item tuple for the collection.
  3: optional Types.TTupleId itemTupleId
  4: required Types.TColumnType slotType

  // Absolute path into the table schema pointing to the column/field materialized into
  // this slot. Empty for slots that do not materialize a table column/field, e.g., slots
  // materializing an aggregation result.
  //
  // materializedPath[i] is the ordinal position of the column/field of the table schema
  // at level i. For example, materializedPath[0] is an ordinal into the list of table
  // columns, materializedPath[1] is an ordinal into the list of fields of the
  // complex-typed column at position materializedPath[0], etc.
  //
  // The materialized path is used to determine when a new tuple (containing a new
  // instance of this slot) should be created. A tuple is emitted for every data item
  // pointed to by the materialized path. For scalar slots this trivially means that every
  // data item goes into a different tuple. For collection slots, the materialized path
  // determines how many data items go into a single collection value.
  5: required list<i32> materializedPath

  6: required i32 byteOffset  // into tuple
  7: required i32 nullIndicatorByte
  8: required i32 nullIndicatorBit
  9: required i32 slotIdx
  10: required CatalogObjects.TVirtualColumnType virtual_col_type =
      CatalogObjects.TVirtualColumnType.NONE
}

struct TColumnDescriptor {
  1: required string name
  2: required Types.TColumnType type

  // Field id of an iceberg column.
  3: optional i32 icebergFieldId
  // Key and value field id for Iceberg column with Map type.
  4: optional i32 icebergFieldMapKeyId
  5: optional i32 icebergFieldMapValueId
}

// "Union" of all table types.
struct TTableDescriptor {
  // Query local id assigned in DescriptorTable:toThrift()
  1: required Types.TTableId id
  2: required CatalogObjects.TTableType tableType
  // Clustering/partition columns come first.
  3: required list<TColumnDescriptor> columnDescriptors
  4: required i32 numClusteringCols

  5: optional CatalogObjects.THdfsTable hdfsTable
  6: optional CatalogObjects.THBaseTable hbaseTable
  9: optional CatalogObjects.TDataSourceTable dataSourceTable
  10: optional CatalogObjects.TKuduTable kuduTable
  11: optional CatalogObjects.TIcebergTable icebergTable
  12: optional CatalogObjects.TSystemTable systemTable

  // Unqualified name of table
  7: required string tableName

  // Name of the database that the table belongs to
  8: required string dbName
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
  1: optional list<TSlotDescriptor> slotDescriptors
  2: required list<TTupleDescriptor> tupleDescriptors

  // TTableDescriptor(s) referenced by tupleDescriptors and scan nodes in
  // the fragment.
  3: optional list<TTableDescriptor> tableDescriptors
}

// Binary blob containing a serialized TDescriptorTable. See desc_tbl_* fields on
// TQueryCtx for more context on when this is used.
struct TDescriptorTableSerialized {
  // TDescriptorTable serialized
  1: required binary thrift_desc_tbl
} (cpp.customostream)
