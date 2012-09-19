// Copyright (c) 2012 Cloudera, Inc. All rights reserved.

namespace cpp impala
namespace java com.cloudera.impala.thrift

include "Types.thrift"
include "ImpalaInternalService.thrift"
include "PlanNodes.thrift"
include "Planner.thrift"
include "Descriptors.thrift"

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

// Result of call to ImpalaPlanService/JniFrontend.CreateQueryRequest()
// TODO: move to ImpalaInternalService.thrift
// TODO: remove existing TQueryExecRequest
struct TQueryExecRequest2 {
  1: required Types.TUniqueId query_id

  // global descriptor tbl for all fragments
  2: optional Descriptors.TDescriptorTable desc_tbl

  3: required list<Planner.TPlanFragment> fragments

  // A map from scan node ids to a list of scan range locations
  // The node ids refer to scan nodes in fragments[].plan_tree
  4: optional map<Types.TPlanNodeId, list<Planner.TScanRangeLocations>>
      per_node_scan_ranges

  // Metadata of the query result set (only for select)
  5: optional TResultSetMetadata result_set_metadata
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
