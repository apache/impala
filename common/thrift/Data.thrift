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

  // Indicates whether tuple_data is snappy-compressed
  5: bool is_compressed
}

// this is a union over all possible return types
struct TColumnValue {
  // TODO: use <type>_val instead of camelcase
  1: optional bool boolVal
  2: optional i32 intVal
  3: optional i64 longVal
  4: optional double doubleVal
  5: optional string stringVal
}

struct TResultRow {
  1: list<TColumnValue> colVals
}

// A union over all possible return types for a column of data
// Currently only used by ExternalDataSource types
struct TColumnData {
  // One element in the list for every row in the column indicating if there is
  // a value in the vals list or a null.
  1: required list<bool> is_null;

  // Only one is set, only non-null values are set.
  2: optional list<bool> bool_vals;
  3: optional list<byte> byte_vals;
  4: optional list<i16> short_vals;
  5: optional list<i32> int_vals;
  6: optional list<i64> long_vals;
  7: optional list<double> double_vals;
  8: optional list<string> string_vals;
  9: optional list<binary> binary_vals;
}

struct TResultSetMetadata {
  1: required list<CatalogObjects.TColumn> columns
}

// List of rows and metadata describing their columns.
struct TResultSet {
  1: required list<TResultRow> rows
  2: required TResultSetMetadata schema
}

