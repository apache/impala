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

// Arguments to getTableNames, which returns a list of tables that match an 
// optional pattern.
struct TGetTablesParams {
  // If not set, match tables in all DBs
  1: optional string db 

  // If not set, match every table
  2: optional string pattern 
}

// getTableNames returns a list of fully-qualified table names
struct TGetTablesResult {
  1: list<string> tables
}

struct TColumnDesc {
  1: required string columnName
  2: required Types.TPrimitiveType columnType
}

// Arguments to DescribeTable, which returns a list of column descriptors for a 
// given table
struct TDescribeTableParams {
  1: optional string db
  2: required string table_name
}

// Results of a call to describeTable()
struct TDescribeTableResult {
  1: required list<TColumnDesc> columns
}

struct TClientRequest {
  // select stmt to be executed
  1: required string stmt

  // query options
  2: required ImpalaInternalService.TQueryOptions queryOptions
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

  // List of partitions that are new and need to be created. May
  // include the root partition (represented by the empty string).
  3: required set<string> created_partitions;
}

// Metadata required to finalize a query - that is, to clean up after the query is done. 
// Only relevant for INSERT queries.
struct TFinalizeParams {
  // True if the INSERT query was OVERWRITE, rather than INTO
  1: required bool is_overwrite;
 
  // The base directory in hdfs of the table targeted by this INSERT
  2: required string hdfs_base_dir;
 
  // The target table name
  3: required string table_name;

  // The target table database
  4: required string table_db;
}

// Result of call to ImpalaPlanService/JniFrontend.CreateQueryRequest()
// TODO: move to ImpalaInternalService.thrift
// TODO: remove existing TQueryExecRequest
struct TQueryExecRequest2 {
  // a globally unique id
  1: required Types.TUniqueId query_id

  // global descriptor tbl for all fragments
  2: optional Descriptors.TDescriptorTable desc_tbl

  // fragments[i] may consume the output of fragments[j > i];
  // fragments[0] is the root fragment and will contain the coordinator fragment, if
  // one exists.
  // (fragments[0] is executed by the coordinator itself if it is unpartitioned.)
  3: required list<Planner.TPlanFragment> fragments

  // Specifies the destination fragment of the output of each fragment.
  // parent_fragment_idx.size() == fragments.size() - 1 and
  // fragments[i] sends its output to fragments[dest_fragment_idx[i-1]]
  4: required list<i32> dest_fragment_idx

  // A map from scan node ids to a list of scan range locations.
  // The node ids refer to scan nodes in fragments[].plan_tree
  5: required map<Types.TPlanNodeId, list<Planner.TScanRangeLocations>>
      per_node_scan_ranges

  // Metadata of the query result set (only for select)
  6: optional TResultSetMetadata result_set_metadata

  // Set if the query needs finalization after it executes
  7: optional TFinalizeParams finalize_params;
}

enum TDdlType {
  SHOW,
  USE,
  DESCRIBE
}

struct TDdlExecRequest {
  1: required TDdlType ddl_type

  // Used for USE and DESCRIBE
  2: optional string database;

  // Table name (not fully-qualified) for DESCRIBE
  3: optional string describe_table;

  // Patterns to match table names against for SHOW
  4: optional string show_pattern;
}

// TQueryExecRequest encapsulates everything needed to execute all plan fragments
// for a single query. 
// If only a single plan fragment is present, it is executed by the coordinator itself.
struct TQueryExecRequest {
  // Globally unique id for this query. Assigned by the planner. Same as 
  // TExecRequest.request_id.
  1: required Types.TUniqueId query_id

  // True if the coordinator should execute a fragment, located in fragment_requests[0]
  2: required bool has_coordinator_fragment;

  // one request per plan fragment;
  // fragmentRequests[i] may consume the output of fragmentRequests[j > i];
  // fragmentRequests[0] will contain the coordinator fragment if one exists
  3: list<ImpalaInternalService.TPlanExecRequest> fragment_requests

  // list of host/port pairs that serve the data for the plan fragments
  // If has_coordinator_fragment == true then:
  //   data_locations.size() == fragment_requests.size() - 1, and fragment_requests[i+1]
  //   is executed on data_locations[i], since (fragment_requests[0] is the coordinator
  //   fragment, which is executed by the coordinator itself) 
  // else: 
  //   data_locations.size() == fragment_requests.size(), and fragment_requests[i]
  // is executed on data_locations[i]
  4: list<list<Types.THostPort>> data_locations

  // node-specific request parameters;
  // nodeRequestParams[i][j] is the parameter for fragmentRequests[i] executed on 
  // execNodes[i][j]
  5: list<list<ImpalaInternalService.TPlanExecParams>> node_request_params

  // for debugging purposes (to allow backend to log the query string)
  6: optional string sql_stmt;

  // Set if the query needs finalization after it executes
  7: optional TFinalizeParams finalize_params;
}

// Result of call to createExecRequest()
struct TExecRequest {
  1: required Types.TStmtType stmt_type;

  // Globally unique id for this request. Assigned by the planner.
  2: required Types.TUniqueId request_id

  // Copied from the corresponding TClientRequest
  3: required ImpalaInternalService.TQueryOptions query_options;

  // TQueryExecRequest for the backend
  // Set iff stmt_type is QUERY or DML
  4: optional TQueryExecRequest queryExecRequest

  // Set iff stmt_type is DDL
  5: optional TDdlExecRequest ddlExecRequest

  // Metadata of the query result set (not set for DML)
  6: optional TResultSetMetadata resultSetMetadata
}

