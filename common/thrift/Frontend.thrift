// Copyright (c) 2012 Cloudera, Inc. All rights reserved.

namespace cpp impala
namespace java com.cloudera.impala.thrift

include "Types.thrift"
include "ImpalaInternalService.thrift"

// These are supporting structs for JniFrontend.java, which serves as the glue
// between our C++ execution environment and the Java frontend.

struct TQueryRequest {
  // select stmt to be executed
  1: required string stmt

  // if true, return query results in ASCII format (TColumnValue.stringVal),
  // otherwise return results in their native format (each TColumnValue
  // uses the field corresponding to the column's native type).
  2: required bool returnAsAscii

  // specifies the degree of parallelism with which to execute the query;
  // 1: single-node execution
  // NUM_NODES_ALL: executes on all nodes that contain relevant data
  // NUM_NODES_ALL_RACKS: executes on one node per rack that holds relevant data
  // > 1: executes on at most that many nodes at any point in time (ie, there can be
  //      more nodes than numNodes with plan fragments for this query, but at most
  //      numNodes would be active at any point in time)
  3: required i32 numNodes
}

struct TColumnDesc {
  1: required string columnName
  2: required Types.TPrimitiveType columnType
}

struct TResultSetMetadata {
  1: required list<TColumnDesc> columnDescs
}

// Describes a set of changes to make to the metastore
struct TCatalogUpdate {
  // Unqualified name of the table to change
  1: required string target_table;

  // Database that the table belongs to
  2: required string db_name;

  // List of partitions that are new and need to be created
  3: required list<string> created_partitions;
}

// Result of call to createQueryExecRequest()
struct TCreateQueryExecRequestResult {
  // TQueryExecRequest for the backend
  1: required ImpalaInternalService.TQueryExecRequest queryExecRequest

  // Metadata of the query result set (only for select)
  2: optional TResultSetMetadata resultSetMetadata
}
