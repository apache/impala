// Copyright (c) 2011 Cloudera, Inc. All rights reserved.

namespace cpp impala
namespace java com.cloudera.impala.thrift

include "Types.thrift"

struct TQueryRequest {
  // select stmt to be executed
  1: string stmt

  // if true, return query results in ASCII format (TColumnValue.stringVal),
  // otherwise return results in their native format (each TColumnValue
  // uses the field corresponding to the column's native type).
  2: bool returnAsAscii

  // specifies the degree of parallelism with which to execute the query;
  // 1: single-node execution
  // NUM_NODES_ALL: executes on all nodes that contain relevant data
  // NUM_NODES_ALL_RACKS: executes on one node per rack that holds relevant data
  // > 1: executes on at most that many nodes at any point in time (ie, there can be
  //      more nodes than numNodes with plan fragments for this query, but at most
  //      numNodes would be active at any point in time)
  3: i32 numNodes
}

// this is a union over all possible return types
struct TColumnValue {
  1: optional bool boolVal
  2: optional i32 intVal
  3: optional i64 longVal
  4: optional double doubleVal
  5: optional string stringVal
}

struct TResultRow {
  1: list<TColumnValue> colVals
}

struct TQueryResult {
  // a set of result rows; if the request specified returnAsAscii,
  // all column values are strings; optional because it is only set for select queries.
  1: optional list<TResultRow> rows

  // if true, there are no more results to be fetched
  2: bool eos
}

// result of RunQuery() rpc
struct TRunQueryResult {
  1: required Types.TStatus status
  2: optional Types.TUniqueId query_id

  // specifies the data types of the columns of the result set
  3: list<Types.TPrimitiveType> col_types
}

// result of FetchResults() rpc
struct TFetchResultsResult {
  1: required Types.TStatus status
  2: optional TQueryResult query_result
}

// For all rpc that return a TStatus as part of their result type,
// if the status_code field is set to anything other than OK, the contents
// of the remainder of the result type is undefined (typically not set)
service ImpalaService {
  // Starts asynchronous query execution and returns a handle to the
  // query.
  TRunQueryResult RunQuery(1:TQueryRequest request);

  // Returns a batch of result rows. Call this repeatedly until 'eos'
  // is set in order to retrieve all result rows.
  // The first batch has 'colTypes' set.
  TFetchResultsResult FetchResults(1:Types.TUniqueId query_id);

  // Cancel execution of query. Returns RUNTIME_ERROR if query_id
  // unknown.
  // This terminates all threads running on behalf of this query at
  // all nodes that were involved in the execution.
  Types.TStatus CancelQuery(1:Types.TUniqueId query_id);
}
