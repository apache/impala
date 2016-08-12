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

include "Data.thrift"
include "Types.thrift"
include "CatalogObjects.thrift"

// Serialized, self-contained version of a RowBatch (in be/src/runtime/row-batch.h).
struct TRowBatch {
  // total number of rows contained in this batch
  1: required i32 num_rows

  // row composition
  2: required list<Types.TTupleId> row_tuples

  // There are a total of num_rows * num_tuples_per_row offsets
  // pointing into tuple_data.
  // An offset of -1 records a NULL.
  3: list<i32> tuple_offsets

  // binary tuple data
  // TODO: figure out how we can avoid copying the data during TRowBatch construction
  4: string tuple_data

  // Indicates the type of compression used
  5: required CatalogObjects.THdfsCompression compression_type

  // Indicates the uncompressed size
  6:i32 uncompressed_size
}

struct TResultSetMetadata {
  1: required list<CatalogObjects.TColumn> columns
}

// List of rows and metadata describing their columns.
struct TResultSet {
  1: required list<Data.TResultRow> rows
  2: required TResultSetMetadata schema
}

