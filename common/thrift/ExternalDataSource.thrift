// Copyright 2014 Cloudera Inc.
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

namespace cpp impala.extdatasource
namespace java com.cloudera.impala.extdatasource.thrift

include "Status.thrift"
include "Data.thrift"
include "Types.thrift"

// A result set column descriptor.
struct TColumnDesc {
  // the column name as given in the Create .. statement
  1: required string name

  2: required Types.TColumnType type
}

// Metadata used to describe the schema (column names, types, comments)
// of result sets.
struct TTableSchema {
  1: required list<TColumnDesc> cols
}

// A single row containing a number of column values.
struct TRow {
  1: required list<Data.TColumnValue> col_vals
}

// Serialized batch of rows returned by getNext().
struct TRowBatch {
  1: required list<TRow> rows
}

// Comparison operators used in predicates.
enum TComparisonOp {
  LT, LE, EQ, NE, GE, GT
}

// Binary predicates that can be pushed to the external data source and
// are of the form <col> <op> <val>. Sources can choose to accept or reject
// predicates via the return value of getStats(), see TGetStatsResult.
// The column and the value are guaranteed to be type compatible in Impala,
// but they are not necessarily the same type, so the data source
// implementation may need to do an implicit cast.
struct TBinaryPredicate {
  // Column on which the predicate is applied.
  1: required TColumnDesc col

  // Comparison operator.
  2: required TComparisonOp op

  // Value on the right side of the binary predicate.
  3: required Data.TColumnValue value
}

// Parameters to getStats().
struct TGetStatsParams {
  // The name of the table.
  1: required string table_name

  // A string specified for the table that is passed to the external data source.
  2: optional string init_string

  // A list of conjunctive (AND) clauses, each of which contains a list of
  // disjunctive (OR) binary predicates.
  3: required list<list<TBinaryPredicate>> predicates
}

// Returned by getStats().
struct TGetStatsResult {
  1: required Status.TStatus status

  // Estimate of the total number of rows returned when applying the predicates indicated
  // by accepted_conjuncts. Not set if the data source does not support providing
  // this statistic.
  2: optional i64 num_rows_estimate

  // Accepted conjuncts. References the 'predicates' parameter in the getStats()
  // call. It contains the 0-based indices of the top-level list elements (the
  // AND elements) that the library will be able to apply during the scan. Those
  // elements that aren’t referenced in accepted_conjuncts will be evaluated by
  // Impala itself.
  3: list<i32> accepted_conjuncts
}

// Parameters to open().
struct TOpenParams {
  // A unique identifier for the query.
  1: required Types.TUniqueId query_id

  // The name of the table.
  2: required string table_name

  // A string specified for the table that is passed to the external data source.
  3: required string init_string

  // The authenticated user name.
  4: required string authenticated_user_name

  // The schema of the rows that the scan needs to return.
  5: required TTableSchema row_schema

  // The expected size of the row batches it returns in the subsequent getNext() calls.
  6: required i32 batch_size

  // A representation of the scan predicates that were accepted in the preceding
  // getStats() call.
  7: required list<list<TBinaryPredicate>> predicates
}

// Returned by open().
struct TOpenResult {
  1: required Status.TStatus status

  // An opaque handle used in subsequent getNext()/close() calls.
  2: optional string scan_handle
}

// Parameters to getNext()
struct TGetNextParams {
  // The opaque handle returned by the previous open() call.
  1: required string scan_handle
}

// Returned by getNext().
struct TGetNextResult {
  1: required Status.TStatus status

  // If true, reached the end of the result stream; subsequent calls to
  // getNext() won’t return any more results
  2: required bool eos

  // A batch of rows to return, if any exist. The number of rows in the batch
  // should be less than or equal to the batch_size specified in TOpenParams.
  3: optional TRowBatch rows
}

// Parameters to close()
struct TCloseParams {
  // The opaque handle returned by the previous open() call.
  1: required string scan_handle
}

// Returned by close().
struct TCloseResult {
  1: required Status.TStatus status
}
