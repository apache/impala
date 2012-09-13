// Copyright (c) 2012 Cloudera, Inc. All rights reserved.

namespace cpp impala
namespace java com.cloudera.impala.thrift

include "Types.thrift"
include "ImpalaInternalService.thrift"

// These are supporting structs for JniFrontend.java, which serves as the glue
// between our C++ execution environment and the Java frontend.

struct TClientRequest {
  // select stmt to be executed
  1: required string stmt

  // query options
  2: required ImpalaInternalService.TQueryOptions queryOptions
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
  3: required set<string> created_partitions;
}

// Result of call to createExecRequest()
struct TCreateExecRequestResult {
  1: required Types.TStmtType stmt_type;

  // TQueryExecRequest for the backend
  // Set iff stmt_type is QUERY
  2: optional ImpalaInternalService.TQueryExecRequest queryExecRequest

  // Metadata of the query result set (only for select)
  3: optional TResultSetMetadata resultSetMetadata
}
