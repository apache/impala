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
include "Exprs.thrift"
include "cli_service.thrift"
include "Status.thrift"
include "CatalogObjects.thrift"
include "CatalogService.thrift"

// These are supporting structs for JniFrontend.java, which serves as the glue
// between our C++ execution environment and the Java frontend.

// Impala currently has two types of sessions: Beeswax and HiveServer2
enum TSessionType {
  BEESWAX,
  HIVESERVER2
}

// Per-client session state
struct TSessionState {
  // A unique identifier for this session
  3: required Types.TUniqueId session_id

  // Session Type (Beeswax or HiveServer2)
  5: required TSessionType session_type

  // The default database for the session
  1: required string database

  // The user to whom this session belongs
  2: required string user

  // Client network address
  4: required Types.TNetworkAddress network_address
}

// Struct for HiveUdf expr to create the proper execution object in the FE
// java side. See exprs/hive-udf-call.h for how hive Udfs are executed in general.
// TODO: this could be the UdfID, collapsing the first 3 arguments but synchronizing
// the id will will not be possible without the catalog service.
struct THiveUdfExecutorCtorParams {
  1: required Exprs.TUdfCallExpr expr

  // Return type of Udf
  2: required Types.TPrimitiveType ret_type

  // Argument types of Udf
  3: required list<Types.TPrimitiveType> arg_types

  // The byte offset for each argument in the input buffer. The BE will
  // call the Java executor with a buffer for all the inputs.
  // input_byte_offsets[0] is the byte offset in the buffer for the first
  // argument; input_byte_offsets[1] is the second, etc.
  4: required list<i32> input_byte_offsets;

  // Native input buffer ptr (cast as i64) for the inputs. The input arguments
  // are written to this buffer directly and read from java with no copies
  // input_null_ptr[i] is true if the i-th input is null.
  // input_buffer_ptr[input_byte_offsets[i]] is the value of the i-th input.
  5: required i64 input_nulls_ptr;
  6: required i64 input_buffer_ptr;

  // Native output buffer ptr. For non-variable length types, the output is
  // written here and read from the native side with no copies.
  // The UDF should set *output_null_ptr to true, if the result of the UDF is
  // NULL.
  7: required i64 output_null_ptr;
  8: required i64 output_buffer_ptr;
}

// Arguments to getTableNames, which returns a list of tables that match an
// optional pattern.
struct TGetTablesParams {
  // If not set, match tables in all DBs
  1: optional string db

  // If not set, match every table
  2: optional string pattern

  // Session state for the user who initiated this request. If authorization is
  // enabled, only the tables this user has access to will be returned. If not
  // set, access checks will be skipped (used for internal Impala requests)
  3: optional TSessionState session
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

  // Session state for the user who initiated this request. If authorization is
  // enabled, only the databases this user has access to will be returned. If not
  // set, access checks will be skipped (used for internal Impala requests)
  2: optional TSessionState session
}

// getDbNames returns a list of database names
struct TGetDbsResult {
  1: list<string> dbs
}

// Used by DESCRIBE <table> statements to control what information is returned and how to
// format the output.
enum TDescribeTableOutputStyle {
  // The default output style if no options are specified for DESCRIBE <table>.
  MINIMAL,
  // Output additional information on the table in formatted style.
  // Set for DESCRIBE FORMATTED statements.
  FORMATTED
}

// Arguments to DescribeTable, which returns a list of column descriptors and additional
// metadata for a given table. What information is returned is controlled by the
// given TDescribeTableOutputStyle.
// NOTE: This struct should only be used for intra-process communication.
struct TDescribeTableParams {
  1: required string db
  2: required string table_name

  // Controls the output style for this describe command.
  3: required TDescribeTableOutputStyle output_style
}

// Results of a call to describeTable()
// NOTE: This struct should only be used for intra-process communication.
struct TDescribeTableResult {
  // Output from a DESCRIBE TABLE command.
  1: required list<Data.TResultRow> results
}

struct TClientRequest {
  // select stmt to be executed
  1: required string stmt

  // query options
  2: required ImpalaInternalService.TQueryOptions queryOptions

  // session state
  3: required TSessionState sessionState
}

// Parameters for SHOW DATABASES commands
struct TShowDbsParams {
  // Optional pattern to match database names. If not set, all databases are returned.
  1: optional string show_pattern
}

// Parameters for SHOW FUNCTIONS commands
struct TShowFunctionsParams {
  // Type of function to show.
  1: Types.TFunctionType type

  // Database to use for SHOW FUNCTIONS
  2: optional string db

  // Optional pattern to match function names. If not set, all functions are returned.
  3: optional string show_pattern
}

// Parameters for SHOW TABLES commands
struct TShowTablesParams {
  // Database to use for SHOW TABLES
  1: optional string db

  // Optional pattern to match tables names. If not set, all tables from the given
  // database are returned.
  2: optional string show_pattern
}

// Arguments to getFunctions(), which returns a list of non-qualified function
// signatures that match an optional pattern. Parameters for SHOW FUNCTIONS.
struct TGetFunctionsParams {
  1: required Types.TFunctionType type

  // Database to use for SHOW FUNCTIONS
  2: optional string db

  // If not set, match every function
  3: optional string pattern

  // Session state for the user who initiated this request. If authorization is
  // enabled, only the functions this user has access to will be returned. If not
  // set, access checks will be skipped (used for internal Impala requests)
  4: optional TSessionState session
}

// getFunctions() returns a list of function signatures
struct TGetFunctionsResult {
  1: list<string> fn_signatures
}

// Parameters for the USE db command
struct TUseDbParams {
  1: required string db
}

// Results of an EXPLAIN
struct TExplainResult {
  // each line in the explain plan occupies an entry in the list
  1: required list<Data.TResultRow> results
}

struct TResultSetMetadata {
  1: required list<CatalogObjects.TColumnDesc> columnDescs
}

// Metadata required to finalize a query - that is, to clean up after the query is done.
// Only relevant for INSERT queries.
struct TFinalizeParams {
  // True if the INSERT query was OVERWRITE, rather than INTO
  1: required bool is_overwrite

  // The base directory in hdfs of the table targeted by this INSERT
  2: required string hdfs_base_dir

  // The target table name
  3: required string table_name

  // The target table database
  4: required string table_db

  // The full path in HDFS of a directory under which temporary files may be written
  // during an INSERT. For a query with id a:b, files are written to <staging_dir>/.a_b/,
  // and that entire directory is removed after the INSERT completes.
  5: optional string staging_dir
}

// Request for a LOAD DATA statement. LOAD DATA is only supported for HDFS backed tables.
struct TLoadDataReq {
  // Fully qualified table name to load data into.
  1: required CatalogObjects.TTableName table_name

  // The source data file or directory to load into the table.
  2: required string source_path

  // If true, loaded files will overwrite all data in the target table/partition's
  // directory. If false, new files will be added alongside existing files. If there are
  // any file name conflicts, the new files will be uniquified by appending a UUID to the
  // base file name preserving the extension if one exists.
  3: required bool overwrite

  // An optional partition spec. Set if this operation should apply to a specific
  // partition rather than the base table.
  4: optional list<CatalogObjects.TPartitionKeyValue> partition_spec
}

// Response of a LOAD DATA statement.
struct TLoadDataResp {
  // A result row that contains information on the result of the LOAD operation. This
  // includes details like the number of files moved as part of the request.
  1: required Data.TResultRow load_summary
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

  // The same as the output of 'explain <query>'
  8: optional string query_plan

  // The statement type governs when the coordinator can judge a query to be finished.
  // DML queries are complete after Wait(), SELECTs may not be. Generally matches
  // the stmt_type of the parent TExecRequest, but in some cases (such as CREATE TABLE
  // AS SELECT), these may differ.
  9: required Types.TStmtType stmt_type

  // Estimated per-host peak memory consumption in bytes. Used for resource management.
  10: optional i64 per_host_mem_req

  // Estimated per-host CPU requirements in YARN virtual cores.
  // Used for resource management.
  11: optional i16 per_host_vcores
}

enum TCatalogOpType {
  SHOW_TABLES,
  SHOW_DBS,
  USE,
  DESCRIBE,
  SHOW_FUNCTIONS,
  RESET_METADATA,
  DDL,
}

struct TCatalogOpRequest {
  1: required TCatalogOpType op_type

  // Parameters for USE commands
  2: optional TUseDbParams use_db_params

  // Parameters for DESCRIBE table commands
  3: optional TDescribeTableParams describe_table_params

  // Parameters for SHOW DATABASES
  4: optional TShowDbsParams show_dbs_params

  // Parameters for SHOW TABLES
  5: optional TShowTablesParams show_tables_params

  // Parameters for SHOW FUNCTIONS
  6: optional TShowFunctionsParams show_fns_params

  // Parameters for DDL requests executed using the CatalogServer
  // such as CREATE, ALTER, and DROP. See CatalogService.TDdlExecRequest
  // for details.
  7: optional CatalogService.TDdlExecRequest ddl_params

  // Parameters for RESET/INVALIDATE METADATA, executed using the CatalogServer.
  // See CatalogService.TResetMetadataRequest for more details.
  8: optional CatalogService.TResetMetadataRequest reset_metadata_params
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

  // Session state for the user who initiated this request. If authorization is
  // enabled, only the server objects this user has access to will be returned.
  // If not set, access checks will be skipped (used for internal Impala requests)
  10: optional TSessionState session
}

// Output of JniFrontend.hiveServer2MetadataOperation
struct TMetadataOpResponse {
  // Schema of the result
  1: required TResultSetMetadata result_set_metadata

  // Result set
  2: required list<Data.TResultRow> results
}

// Tracks accesses to Catalog objects for use during auditing. This information, paired
// with the current session information, provides a view into what objects a user's
// query accessed
struct TAccessEvent {
  // Fully qualified object name
  1: required string name

  // The object type (ex. DATABASE, VIEW, TABLE)
  2: required CatalogObjects.TCatalogObjectType object_type

  // The requested privilege on the object
  // TODO: Create an enum for this?
  3: required string privilege
}

// Result of call to createExecRequest()
struct TExecRequest {
  1: required Types.TStmtType stmt_type

  // Copied from the corresponding TClientRequest
  2: required ImpalaInternalService.TQueryOptions query_options

  // TQueryExecRequest for the backend
  // Set iff stmt_type is QUERY or DML
  3: optional TQueryExecRequest query_exec_request

  // Set iff stmt_type is DDL
  4: optional TCatalogOpRequest catalog_op_request

  // Metadata of the query result set (not set for DML)
  5: optional TResultSetMetadata result_set_metadata

  // Result of EXPLAIN. Set iff stmt_type is EXPLAIN
  6: optional TExplainResult explain_result

  // Request for LOAD DATA statements.
  7: optional TLoadDataReq load_data_request

  // List of catalog objects accessed by this request. May be empty in this
  // case that the query did not access any Catalog objects.
  8: optional list<TAccessEvent> access_events
}

// Parameters to pass to validate that the binary contains the symbol. If the
// symbols is fully specified (i.e. full mangled name), we validate that the
// mangled name is correct. If only the function name is specified, we try
// to find the fully mangled name in the binary.
// The result is returned in TSymbolLookupResult.
struct TSymbolLookupParams {
  // HDFS path for the function binary. This binary must exist at the time the
  // function is created.
  1: required string location

  // This can either be a mangled symbol or before mangling function name.
  2: required string symbol

  // Type of the udf. e.g. hive, native, ir
  3: required Types.TFunctionBinaryType fn_binary_type

  // The types of the arguments to the function
  4: required list<Types.TColumnType> arg_types

  // If true, this function takes var args.
  5: required bool has_var_args

  // If set this function needs to have an return out argument of this type.
  6: optional Types.TColumnType ret_arg_type
}

enum TSymbolLookupResultCode {
  SYMBOL_FOUND,
  BINARY_NOT_FOUND,
  SYMBOL_NOT_FOUND,
}

struct TSymbolLookupResult {
  // The result of the symbol lookup.
  1: required TSymbolLookupResultCode result_code

  // The symbol that was found. set if result_code == SYMBOL_FOUND.
  2: optional string symbol

  // The error message if the symbol found not be found.
  3: optional string error_msg
}

// Sent to an impalad FE during each CatalogUpdate heartbeat. Contains details on all
// catalog objects that need to be updated.
struct TInternalCatalogUpdateRequest {
  // True if update only contains entries changed from the previous update. Otherwise,
  // contains the entire topic.
  1: required bool is_delta

  // The Catalog Service ID this update came from.
  2: required Types.TUniqueId catalog_service_id

  // New or modified items. Empty list if no items were updated.
  3: required list<CatalogObjects.TCatalogObject> updated_objects

  // Empty of no items were removed or is_delta is false.
  4: required list<CatalogObjects.TCatalogObject> removed_objects
}

// Response from a TCatalogUpdateRequest. Returns the new max catalog version after
// applying the update.
struct TInternalCatalogUpdateResponse {
  // The catalog service id this version is from.
  1: required Types.TUniqueId catalog_service_id;
}
