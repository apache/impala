// Copyright 2012 Cloudera Inc.
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

namespace cpp impala
namespace java com.cloudera.impala.thrift

include "Types.thrift"
include "ImpalaInternalService.thrift"
include "PlanNodes.thrift"
include "Planner.thrift"
include "Descriptors.thrift"
include "Data.thrift"
include "cli_service.thrift"

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

// Parameters of CREATE DATABASE commands
struct TCreateDbParams {
  // Name of the database to create
  1: required string db

  // Optional comment to attach to the database
  2: optional string comment

  // Optional HDFS path for the database. This will be the default location for all
  // new tables created in the database.
  3: optional string location

  // Do not throw an error if a database of the same name already exists.
  4: optional bool if_not_exists
}

// Valid table file formats
enum TFileFormat {
  PARQUETFILE,
  RCFILE,
  SEQUENCEFILE,
  TEXTFILE,
}

// Parameters of CREATE TABLE commands
struct TCreateTableParams {
  // Name of the database the table should be created in
  1: required string db

  // Name of the table to create
  2: required string table_name

  // List of columns to create
  3: required list<TColumnDesc> columns

  // List of partition columns
  4: optional list<TColumnDesc> partition_columns

  // The file format for this table
  5: required TFileFormat file_format

  // True if the table is an "EXTERNAL" table. Dropping an external table will NOT remove
  // table data from the file system. If EXTERNAL is not specified, all table data will be
  // removed when the table is dropped.
  6: optional bool is_external

  // Optional comment for the table
  7: optional string comment

  // Optional storage location for the table
  8: optional string location

  // Optional terminator string used to delimit fields (columns) in the table
  9: optional string field_terminator

  // Optional terminator string used to delimit lines (rows) in a table
  10: optional string line_terminator

  // Do not throw an error if a table of the same name already exists.
  11: optional bool if_not_exists
}

// Parameters of DROP DATABASE commands
struct TDropDbParams {
  // Name of the database to drop
  1: required string db

  // If true, no error is raised if the target db does not exist
  2: optional bool if_exists
}

// Parameters of DROP TABLE commands
struct TDropTableParams {
  // Name of the database the table resides in
  1: required string db

  // Name of the table to drop for DROP TABLE
  2: required string table_name

  // If true, no error is raised if the target table does not exist
  3: optional bool if_exists
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

// Parameters for SHOW DATABASES commands
struct TShowDbsParams {
  // Optional pattern to match database names. If not set, all databases are returned.
  1: optional string show_pattern
}

// Parameters for SHOW TABLES commands
struct TShowTablesParams {
  // Database to use for SHOW TABLE
  1: optional string db

  // Optional pattern to match tables names. If not set, all tables from the given
  // database are returned.
  2: optional string show_pattern
}

// Parameters for the USE db command
struct TUseDbParams {
  1: required string db
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
  DESCRIBE,
  CREATE_DATABASE,
  CREATE_TABLE,
  DROP_DATABASE,
  DROP_TABLE,
}

struct TDdlExecRequest {
  1: required TDdlType ddl_type

  // Parameters for USE commands
  2: optional TUseDbParams use_db_params;

  // Parameters for DESCRIBE table commands
  3: optional TDescribeTableParams describe_table_params

  // Parameters for SHOW DATABASES
  4: optional TShowDbsParams show_dbs_params

  // Parameters for SHOW TABLES
  5: optional TShowTablesParams show_tables_params

  // Parameters for CREATE DATABASE
  6: optional TCreateDbParams create_db_params

  // Parameters for CREATE TABLE
  7: optional TCreateTableParams create_table_params

  // Paramaters for DROP DATABAE
  8: optional TDropDbParams drop_db_params

  // Parameters for DROP TABLE
  9: optional TDropTableParams drop_table_params
}

// HiveServer2 Metadata operations (JniFrontend.hiveServer2MetadataOperation)
enum TMetadataOpcode {
  GET_TYPE_INFO,
  GET_CATALOGS,
  GET_SCHEMAS,
  GET_TABLES,
  GET_TABLE_TYPES,
  GET_COLUMNS,
  GET_FUNCTIONS
}

// Input parameter to JniFrontend.hiveServer2MetadataOperation
// Each request has an opcode and a corresponding TGet*Req input parameter
struct TMetadataOpRequest {
  // opcode
  1: required TMetadataOpcode opcode
  
  // input parameter
  2: optional cli_service.TGetInfoReq get_info_req
  3: optional cli_service.TGetTypeInfoReq get_type_info_req
  4: optional cli_service.TGetCatalogsReq get_catalogs_req
  5: optional cli_service.TGetSchemasReq get_schemas_req
  6: optional cli_service.TGetTablesReq get_tables_req
  7: optional cli_service.TGetTableTypesReq get_table_types_req
  8: optional cli_service.TGetColumnsReq get_columns_req
  9: optional cli_service.TGetFunctionsReq get_functions_req
}

// Output of JniFrontend.hiveServer2MetadataOperation
struct TMetadataOpResponse {
  // Globally unique id for this request.
  1: required Types.TUniqueId request_id
  
  // Schema of the result
  2: required TResultSetMetadata result_set_metadata
  
  // Result set
  3: required list<Data.TResultRow> results
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
