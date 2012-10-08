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

// getTableNames returns a list of unqualified table names
struct TGetTablesResult {
  1: list<string> tables
}

// Arguments to getDbNames, which returns a list of dbs that match an optional
// pattern
struct TGetDbsParams {
  // If not set, match every database
  1: optional string pattern
}

// getDbNames returns a list of database names
struct TGetDbsResult {
  1: list<string> dbs
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

// Per-client session state
struct TSessionState {
  // The default database, changed by USE <database> queries.
  1: required string database
}

struct TClientRequest {
  // select stmt to be executed
  1: required string stmt

  // query options
  2: required ImpalaInternalService.TQueryOptions queryOptions

  // session state
  3: required TSessionState sessionState;
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
struct TQueryExecRequest {
  // global descriptor tbl for all fragments
  1: optional Descriptors.TDescriptorTable desc_tbl

  // fragments[i] may consume the output of fragments[j > i];
  // fragments[0] is the root fragment and also the coordinator fragment, if
  // it is unpartitioned.
  2: required list<Planner.TPlanFragment> fragments

  // Specifies the destination fragment of the output of each fragment.
  // parent_fragment_idx.size() == fragments.size() - 1 and
  // fragments[i] sends its output to fragments[dest_fragment_idx[i-1]]
  3: optional list<i32> dest_fragment_idx

  // A map from scan node ids to a list of scan range locations.
  // The node ids refer to scan nodes in fragments[].plan_tree
  4: optional map<Types.TPlanNodeId, list<Planner.TScanRangeLocations>>
      per_node_scan_ranges

  // Metadata of the query result set (only for select)
  5: optional TResultSetMetadata result_set_metadata

  // Set if the query needs finalization after it executes
  6: optional TFinalizeParams finalize_params

  7: required ImpalaInternalService.TQueryGlobals query_globals
}

enum TDdlType {
  SHOW_TABLES,
  SHOW_DBS,
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

// Result of call to createExecRequest()
struct TExecRequest {
  1: required Types.TStmtType stmt_type;

  2: optional string sql_stmt;

  // Globally unique id for this request. Assigned by the planner.
  3: required Types.TUniqueId request_id

  // Copied from the corresponding TClientRequest
  4: required ImpalaInternalService.TQueryOptions query_options;

  // TQueryExecRequest for the backend
  // Set iff stmt_type is QUERY or DML
  5: optional TQueryExecRequest query_exec_request

  // Set iff stmt_type is DDL
  6: optional TDdlExecRequest ddl_exec_request

  // Metadata of the query result set (not set for DML)
  7: optional TResultSetMetadata result_set_metadata
}

