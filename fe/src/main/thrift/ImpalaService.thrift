// Copyright (c) 2011 Cloudera, Inc. All rights reserved.

namespace cpp impala
namespace java com.cloudera.impala.thrift

include "Types.thrift"

// constants for TQueryRequest.numNodes
const i32 NUM_NODES_ALL = 0
const i32 NUM_NODES_ALL_RACKS = -1

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
  // all column values are strings
  1: list<TResultRow> rows

  // if true, there are no more results to be fetched
  2: bool eos

  // specifies the data types of the columns of the result set
  3: optional list<Types.TPrimitiveType> colTypes
}

service ImpalaService {
  // Starts asynchronous query execution and returns a handle to the
  // query.
  Types.TUniqueId RunQuery(1:TQueryRequest request);

  // Returns a batch of result rows. Call this repeatedly until 'eos'
  // is set in order to retrieve all result rows.
  // The first batch has 'colTypes' set.
  TQueryResult FetchResults(1:Types.TUniqueId query_id);

  void CancelQuery(1:Types.TUniqueId query_id);
}
