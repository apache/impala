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

namespace cpp impala.extdatasource
namespace java org.apache.impala.extdatasource.thrift

include "Status.thrift"
include "Data.thrift"
include "Types.thrift"

// A result set column descriptor.
struct TColumnDesc {
  // The column name as given in the Create .. statement. Always set.
  1: optional string name

  // The column type. Always set.
  2: optional Types.TColumnType type
}

// Metadata used to describe the schema (column names, types, comments)
// of result sets.
struct TTableSchema {
  // List of columns. Always set.
  1: optional list<TColumnDesc> cols
}

// Serialized batch of rows returned by getNext().
struct TRowBatch {
  // Each TColumnData contains the data for an entire column. Always set.
  1: optional list<Data.TColumnData> cols

  // The number of rows returned. For count(*) queries, there may not be
  // any materialized columns so cols will be an empty list and this value
  // will indicate how many rows are returned. When there are materialized
  // columns, this number should be the same as the size of each
  // TColumnData.is_null list.
  2: optional i64 num_rows
}

// Comparison operators used in predicates.
enum TComparisonOp {
  LT = 0
  LE = 1
  EQ = 2
  NE = 3
  GE = 4
  GT = 5
  DISTINCT_FROM = 6
  NOT_DISTINCT = 7
}

// Binary predicates that can be pushed to the external data source and
// are of the form <col> <op> <val>. Sources can choose to accept or reject
// predicates via the return value of prepare(), see TPrepareResult.
// The column and the value are guaranteed to be type compatible in Impala,
// but they are not necessarily the same type, so the data source
// implementation may need to do an implicit cast.
struct TBinaryPredicate {
  // Column on which the predicate is applied. Always set.
  1: optional TColumnDesc col

  // Comparison operator. Always set.
  2: optional TComparisonOp op

  // Value on the right side of the binary predicate. Always set.
  3: optional Data.TColumnValue value
}

// Parameters to prepare().
struct TPrepareParams {
  // The name of the table. Always set.
  1: optional string table_name

  // A string specified for the table that is passed to the external data source.
  // Always set, may be an empty string.
  2: optional string init_string

  // A list of conjunctive (AND) clauses, each of which contains a list of
  // disjunctive (OR) binary predicates. Always set, may be an empty list.
  3: optional list<list<TBinaryPredicate>> predicates
}

// Returned by prepare().
struct TPrepareResult {
  1: required Status.TStatus status

  // Estimate of the total number of rows returned when applying the predicates indicated
  // by accepted_conjuncts. Not set if the data source does not support providing
  // this statistic.
  2: optional i64 num_rows_estimate

  // Accepted conjuncts. References the 'predicates' parameter in the prepare()
  // call. It contains the 0-based indices of the top-level list elements (the
  // AND elements) that the library will be able to apply during the scan. Those
  // elements that aren’t referenced in accepted_conjuncts will be evaluated by
  // Impala itself.
  3: optional list<i32> accepted_conjuncts
}

// Parameters to open().
struct TOpenParams {
  // A unique identifier for the query. Always set.
  1: optional Types.TUniqueId query_id

  // The name of the table. Always set.
  2: optional string table_name

  // A string specified for the table that is passed to the external data source.
  // Always set, may be an empty string.
  3: optional string init_string

  // The authenticated user name. Always set.
  4: optional string authenticated_user_name

  // The schema of the rows that the scan needs to return. Always set.
  5: optional TTableSchema row_schema

  // The expected size of the row batches it returns in the subsequent getNext() calls.
  // Always set.
  6: optional i32 batch_size

  // A representation of the scan predicates that were accepted in the preceding
  // prepare() call. Always set.
  7: optional list<list<TBinaryPredicate>> predicates

  // The query limit, if specified.
  8: optional i64 limit

  // Indicate if external JDBC table handler should clean DBCP DataSource object from
  // cache when its reference count equals 0. Note that the reference count is tracked
  // across all queries for a given data source in the coordinator.
  9: optional bool clean_dbcp_ds_cache
}

// Returned by open().
struct TOpenResult {
  1: required Status.TStatus status

  // An opaque handle used in subsequent getNext()/close() calls. Required.
  2: optional string scan_handle
}

// Parameters to getNext()
struct TGetNextParams {
  // The opaque handle returned by the previous open() call. Always set.
  1: optional string scan_handle
}

// Returned by getNext().
struct TGetNextResult {
  1: required Status.TStatus status

  // If true, reached the end of the result stream; subsequent calls to
  // getNext() won’t return any more results. Required.
  2: optional bool eos

  // A batch of rows to return, if any exist. The number of rows in the batch
  // should be less than or equal to the batch_size specified in TOpenParams.
  3: optional TRowBatch rows
}

// Parameters to close()
struct TCloseParams {
  // The opaque handle returned by the previous open() call. Always set.
  1: optional string scan_handle
}

// Returned by close().
struct TCloseResult {
  1: required Status.TStatus status
}
