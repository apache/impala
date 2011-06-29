// Copyright (c) 2011 Cloudera, Inc. All rights reserved.

namespace cpp impala
namespace java com.cloudera.impala.thrift

include "Types.thrift"

typedef i64 TQueryId

struct TQueryRequest {
  // select stmt to be executed
  1: string stmt

  // if true, return query results in ASCII format
  2: bool returnAsAscii
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
  TQueryId RunQuery(1:TQueryRequest request);

  // Returns a batch of result rows. Call this repeatedly until 'eos'
  // is set in order to retrieve all result rows.
  // The first batch has 'colTypes' set.
  TQueryResult FetchResults(1:TQueryId id);

  void CancelQuery(1:TQueryId id);
}
