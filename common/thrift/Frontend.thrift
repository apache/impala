// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

namespace cpp impala
namespace java org.apache.impala.thrift

include "Types.thrift"
include "ImpalaInternalService.thrift"
include "PlanNodes.thrift"
include "Planner.thrift"
include "RuntimeProfile.thrift"
include "Descriptors.thrift"
include "Data.thrift"
include "Results.thrift"
include "Exprs.thrift"
include "TCLIService.thrift"
include "Status.thrift"
include "CatalogObjects.thrift"
include "CatalogService.thrift"
include "LineageGraph.thrift"

// These are supporting structs for JniFrontend.java, which serves as the glue
// between our C++ execution environment and the Java frontend.

// Struct for HiveUdf expr to create the proper execution object in the FE
// java side. See exprs/hive-udf-call.h for how hive Udfs are executed in general.
// TODO: this could be the UdfID, collapsing the first 3 arguments but synchronizing
// the id will will not be possible without the catalog service.
struct THiveUdfExecutorCtorParams {
  1: required Types.TFunction fn

  // Local path to the UDF's jar file
  2: required string local_location

  // The byte offset for each argument in the input buffer. The BE will
  // call the Java executor with a buffer for all the inputs.
  // input_byte_offsets[0] is the byte offset in the buffer for the first
  // argument; input_byte_offsets[1] is the second, etc.
  3: required list<i32> input_byte_offsets

  // Native input buffer ptr (cast as i64) for the inputs. The input arguments
  // are written to this buffer directly and read from java with no copies
  // input_null_ptr[i] is true if the i-th input is null.
  // input_buffer_ptr[input_byte_offsets[i]] is the value of the i-th input.
  4: required i64 input_nulls_ptr
  5: required i64 input_buffer_ptr

  // Native output buffer ptr. For non-variable length types, the output is
  // written here and read from the native side with no copies.
  // The UDF should set *output_null_ptr to true, if the result of the UDF is
  // NULL.
  6: required i64 output_null_ptr
  7: required i64 output_buffer_ptr
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
  3: optional ImpalaInternalService.TSessionState session
}

// getTableNames returns a list of unqualified table names
struct TGetTablesResult {
  1: list<string> tables
}

// Arguments to getDbs, which returns a list of dbs that match an optional pattern
struct TGetDbsParams {
  // If not set, match every database
  1: optional string pattern

  // Session state for the user who initiated this request. If authorization is
  // enabled, only the databases this user has access to will be returned. If not
  // set, access checks will be skipped (used for internal Impala requests)
  2: optional ImpalaInternalService.TSessionState session
}

// getDbs returns a list of databases
struct TGetDbsResult {
  1: list<CatalogObjects.TDatabase> dbs
}

// Arguments to getDataSrcsNames, which returns a list of data sources that match an
// optional pattern
struct TGetDataSrcsParams {
  // If not set, match every data source
  1: optional string pattern
}

// getDataSrcsNames returns a list of data source names
struct TGetDataSrcsResult {
  1: required list<string> data_src_names
  2: required list<string> locations
  3: required list<string> class_names
  4: required list<string> api_versions
}

// Used by DESCRIBE DATABASE <db> and DESCRIBE <table> statements to control
// what information is returned and how to format the output.
enum TDescribeOutputStyle {
  // The default output style if no options are specified for
  // DESCRIBE DATABASE <db> and DESCRIBE <table>.
  MINIMAL,

  // Output additional information on the database or table.
  // Set for both DESCRIBE DATABASE FORMATTED|EXTENDED <db>
  // and DESCRIBE FORMATTED|EXTENDED <table> statements.
  EXTENDED,
  FORMATTED
}

// Arguments to DescribeDb, which returns a list of properties for a given database.
// What information is returned is controlled by the given TDescribeOutputStyle.
// NOTE: This struct should only be used for intra-process communication.
struct TDescribeDbParams {
  1: required string db

  // Controls the output style for this describe command.
  2: required TDescribeOutputStyle output_style
}

// Arguments to DescribeTable, which returns a list of column descriptors and additional
// metadata for a given table. What information is returned is controlled by the
// given TDescribeOutputStyle.
// NOTE: This struct should only be used for intra-process communication.
struct TDescribeTableParams {
  // Controls the output style for this describe command.
  1: required TDescribeOutputStyle output_style

  // Set when describing a table.
  2: optional CatalogObjects.TTableName table_name

  // Set when describing a path to a nested collection.
  3: optional Types.TColumnType result_struct
}

// Results of a call to describeDb() and describeTable()
// NOTE: This struct should only be used for intra-process communication.
struct TDescribeResult {
  // Output from a DESCRIBE DATABASE command or a DESCRIBE TABLE command.
  1: required list<Data.TResultRow> results
}

// Parameters for SHOW DATA SOURCES commands
struct TShowDataSrcsParams {
  // Optional pattern to match data source names. If not set, all data sources are
  // returned.
  1: optional string show_pattern
}

// Parameters for SHOW DATABASES commands
struct TShowDbsParams {
  // Optional pattern to match database names. If not set, all databases are returned.
  1: optional string show_pattern
}

// Used by SHOW STATS and SHOW PARTITIONS to control what information is returned.
enum TShowStatsOp {
  TABLE_STATS,
  COLUMN_STATS,
  PARTITIONS,
  RANGE_PARTITIONS
}

// Parameters for SHOW TABLE/COLUMN STATS and SHOW PARTITIONS commands
struct TShowStatsParams {
  1: TShowStatsOp op
  2: CatalogObjects.TTableName table_name
}

// Parameters for SHOW FUNCTIONS commands
struct TShowFunctionsParams {
  // Category of function to show.
  1: Types.TFunctionCategory category

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

// Parameters for SHOW FILES commands
struct TShowFilesParams {
  1: required CatalogObjects.TTableName table_name

  // An optional partition set. Set if this operation should apply to a list of
  // partitions rather than the base table.
  2: optional list<list<CatalogObjects.TPartitionKeyValue>> partition_set
}

// Parameters for SHOW [CURRENT] ROLES and SHOW ROLE GRANT GROUP <groupName> commands
struct TShowRolesParams {
  // The effective user who submitted this request.
  1: optional string requesting_user

  // True if this opertion requires admin privileges on the Sentry Service. This is
  // needed to check for the case where an operation is_user_scope, but the user does
  // not belong to the specified grant_group.
  2: required bool is_admin_op

  // True if the statement is "SHOW CURRENT ROLES".
  3: required bool is_show_current_roles

  // Filters roles to the specified grant group. If null or not set, show roles for all
  // groups.
  4: optional string grant_group
}

// Result of a SHOW ROLES command
struct TShowRolesResult {
  1: required list<string> role_names
}

// Parameters for SHOW GRANT ROLE commands
struct TShowGrantRoleParams {
  // The effective user who submitted this request.
  1: optional string requesting_user

  // The target role name.
  2: required string role_name

  // True if this operation requires admin privileges on the Sentry Service (when
  // the requesting user has not been granted the target role name).
  3: required bool is_admin_op

  // An optional filter to show grants that match a specific privilege spec.
  4: optional CatalogObjects.TPrivilege privilege
}

// Arguments to getFunctions(), which returns a list of non-qualified function
// signatures that match an optional pattern. Parameters for SHOW FUNCTIONS.
struct TGetFunctionsParams {
  1: required Types.TFunctionCategory category

  // Database to use for SHOW FUNCTIONS
  2: optional string db

  // If not set, match every function
  3: optional string pattern

  // Session state for the user who initiated this request. If authorization is
  // enabled, only the functions this user has access to will be returned. If not
  // set, access checks will be skipped (used for internal Impala requests)
  4: optional ImpalaInternalService.TSessionState session
}

// getFunctions() returns a list of function signatures
struct TGetFunctionsResult {
  1: list<string> fn_signatures
  2: list<string> fn_ret_types
  3: list<string> fn_binary_types
  4: list<string> fn_persistence
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

  // Identifier for the target table in the query-wide descriptor table (see
  // TDescriptorTable and TTableDescriptor).
  6: optional i64 table_id;
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

// Execution parameters for a single plan; component of TQueryExecRequest
struct TPlanExecInfo {
  // fragments[i] may consume the output of fragments[j > i];
  // fragments[0] is the root fragment and also the coordinator fragment, if
  // it is unpartitioned.
  1: required list<Planner.TPlanFragment> fragments

  // A map from scan node ids to a list of scan range locations.
  // The node ids refer to scan nodes in fragments[].plan
  2: optional map<Types.TPlanNodeId, list<Planner.TScanRangeLocationList>>
      per_node_scan_ranges
}

// Result of call to ImpalaPlanService/JniFrontend.CreateQueryRequest()
struct TQueryExecRequest {
  // global descriptor tbl for all fragments
  1: optional Descriptors.TDescriptorTable desc_tbl

  // exec info for all plans; the first one materializes the query result
  2: optional list<TPlanExecInfo> plan_exec_info

  // Metadata of the query result set (only for select)
  3: optional Results.TResultSetMetadata result_set_metadata

  // Set if the query needs finalization after it executes
  4: optional TFinalizeParams finalize_params

  5: required ImpalaInternalService.TQueryCtx query_ctx

  // The same as the output of 'explain <query>'
  6: optional string query_plan

  // The statement type governs when the coordinator can judge a query to be finished.
  // DML queries are complete after Wait(), SELECTs may not be. Generally matches
  // the stmt_type of the parent TExecRequest, but in some cases (such as CREATE TABLE
  // AS SELECT), these may differ.
  7: required Types.TStmtType stmt_type

  // List of replica hosts.  Used by the host_idx field of TScanRangeLocation.
  9: required list<Types.TNetworkAddress> host_list

  // Column lineage graph
  10: optional LineageGraph.TLineageGraph lineage_graph

  // Estimated per-host peak memory consumption in bytes. Used by admission control.
  // TODO: Remove when AC doesn't rely on this any more.
  8: optional i64 per_host_mem_estimate

  // Maximum possible (in the case all fragments are scheduled on all hosts with
  // max DOP) minimum reservation required per host, in bytes.
  11: optional i64 max_per_host_min_reservation;
}

enum TCatalogOpType {
  SHOW_TABLES,
  SHOW_DBS,
  SHOW_STATS,
  USE,
  DESCRIBE_TABLE,
  DESCRIBE_DB,
  SHOW_FUNCTIONS,
  RESET_METADATA,
  DDL,
  SHOW_CREATE_TABLE,
  SHOW_DATA_SRCS,
  SHOW_ROLES,
  SHOW_GRANT_ROLE,
  SHOW_FILES,
  SHOW_CREATE_FUNCTION
}

// TODO: Combine SHOW requests with a single struct that contains a field
// indicating which type of show request it is.
struct TCatalogOpRequest {
  1: required TCatalogOpType op_type

  // Parameters for USE commands
  2: optional TUseDbParams use_db_params

  // Parameters for DESCRIBE DATABASE db commands
  17: optional TDescribeDbParams describe_db_params

  // Parameters for DESCRIBE table commands
  3: optional TDescribeTableParams describe_table_params

  // Parameters for SHOW DATABASES
  4: optional TShowDbsParams show_dbs_params

  // Parameters for SHOW TABLES
  5: optional TShowTablesParams show_tables_params

  // Parameters for SHOW FUNCTIONS
  6: optional TShowFunctionsParams show_fns_params

  // Parameters for SHOW DATA SOURCES
  11: optional TShowDataSrcsParams show_data_srcs_params

  // Parameters for SHOW ROLES
  12: optional TShowRolesParams show_roles_params

  // Parameters for SHOW GRANT ROLE
  13: optional TShowGrantRoleParams show_grant_role_params

  // Parameters for DDL requests executed using the CatalogServer
  // such as CREATE, ALTER, and DROP. See CatalogService.TDdlExecRequest
  // for details.
  7: optional CatalogService.TDdlExecRequest ddl_params

  // Parameters for RESET/INVALIDATE METADATA, executed using the CatalogServer.
  // See CatalogService.TResetMetadataRequest for more details.
  8: optional CatalogService.TResetMetadataRequest reset_metadata_params

  // Parameters for SHOW TABLE/COLUMN STATS
  9: optional TShowStatsParams show_stats_params

  // Parameters for SHOW CREATE TABLE
  10: optional CatalogObjects.TTableName show_create_table_params

  // Parameters for SHOW FILES
  14: optional TShowFilesParams show_files_params

  // Column lineage graph
  15: optional LineageGraph.TLineageGraph lineage_graph

  // Parameters for SHOW_CREATE_FUNCTION
  16: optional TGetFunctionsParams show_create_function_params
}

// Parameters for the SET query option command
struct TSetQueryOptionRequest {
  // Set for "SET key=value", unset for "SET" statement.
  1: optional string key
  2: optional string value
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

  // input parameters
  2: optional TCLIService.TGetInfoReq get_info_req
  3: optional TCLIService.TGetTypeInfoReq get_type_info_req
  4: optional TCLIService.TGetCatalogsReq get_catalogs_req
  5: optional TCLIService.TGetSchemasReq get_schemas_req
  6: optional TCLIService.TGetTablesReq get_tables_req
  7: optional TCLIService.TGetTableTypesReq get_table_types_req
  8: optional TCLIService.TGetColumnsReq get_columns_req
  9: optional TCLIService.TGetFunctionsReq get_functions_req

  // Session state for the user who initiated this request. If authorization is
  // enabled, only the server objects this user has access to will be returned.
  // If not set, access checks will be skipped (used for internal Impala requests)
  10: optional ImpalaInternalService.TSessionState session
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
  5: optional Results.TResultSetMetadata result_set_metadata

  // Result of EXPLAIN. Set iff stmt_type is EXPLAIN
  6: optional TExplainResult explain_result

  // Request for LOAD DATA statements.
  7: optional TLoadDataReq load_data_request

  // List of catalog objects accessed by this request. May be empty in this
  // case that the query did not access any Catalog objects.
  8: optional set<TAccessEvent> access_events

  // List of warnings that were generated during analysis. May be empty.
  9: required list<string> analysis_warnings

  // Set iff stmt_type is SET
  10: optional TSetQueryOptionRequest set_query_option_request

  // Timeline of planner's operation, for profiling
  11: optional RuntimeProfile.TEventSequence timeline

  // If false, the user that runs this statement doesn't have access to the runtime
  // profile. For example, a user can't access the runtime profile of a query
  // that has a view for which the user doesn't have access to the underlying tables.
  12: optional bool user_has_profile_access
}

// Parameters to FeSupport.cacheJar().
struct TCacheJarParams {
  // HDFS URI for the jar
  1: required string hdfs_location
}

// Result from FeSupport.cacheJar().
struct TCacheJarResult {
  1: required Status.TStatus status

  // Local path for the jar. Set only if status is OK.
  2: optional string local_path
}

// A UDF may include optional prepare and close functions in addition the main evaluation
// function. This enum distinguishes between these when doing a symbol lookup.
enum TSymbolType {
  UDF_EVALUATE,
  UDF_PREPARE,
  UDF_CLOSE,
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

  // Determines the signature of the mangled symbol
  7: required TSymbolType symbol_type;
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

// Sent from the impalad BE to FE with the results of each CatalogUpdate heartbeat.
// Contains details on all catalog objects that need to be updated.
struct TUpdateCatalogCacheRequest {
  // True if update only contains entries changed from the previous update. Otherwise,
  // contains the entire topic.
  1: required bool is_delta

  // The Catalog Service ID this update came from.
  2: required Types.TUniqueId catalog_service_id

  // New or modified items. Empty list if no items were updated.
  3: required list<CatalogObjects.TCatalogObject> updated_objects

  // Empty if no items were removed or is_delta is false.
  4: required list<CatalogObjects.TCatalogObject> removed_objects
}

// Response from a TUpdateCatalogCacheRequest.
struct TUpdateCatalogCacheResponse {
  // The catalog service id this version is from.
  1: required Types.TUniqueId catalog_service_id
}

// Sent from the impalad BE to FE with the latest cluster membership snapshot resulting
// from the Membership heartbeat.
struct TUpdateMembershipRequest {
  1: required set<string> hostnames
  2: required set<string> ip_addresses
  3: i32 num_nodes
}

// Contains all interesting statistics from a single 'memory pool' in the JVM.
// All numeric values are measured in bytes.
struct TJvmMemoryPool {
  // Memory committed by the operating system to this pool (i.e. not just virtual address
  // space)
  1: required i64 committed

  // The initial amount of memory committed to this pool
  2: required i64 init

  // The maximum amount of memory this pool will use.
  3: required i64 max

  // The amount of memory currently in use by this pool (will be <= committed).
  4: required i64 used

  // Maximum committed memory over time
  5: required i64 peak_committed

  // Should be always == init
  6: required i64 peak_init

  // Peak maximum memory over time (usually will not change)
  7: required i64 peak_max

  // Peak consumed memory over time
  8: required i64 peak_used

  // Name of this pool, defined by the JVM
  9: required string name
}

// Request to get one or all sets of memory pool metrics.
struct TGetJvmMetricsRequest {
  // If set, return all pools
  1: required bool get_all

  // If get_all is false, this must be set to the name of the memory pool to return.
  2: optional string memory_pool
}

// Response from JniUtil::GetJvmMetrics()
struct TGetJvmMetricsResponse {
  // One entry for every pool tracked by the Jvm, plus a synthetic aggregate pool called
  // 'total'
  1: required list<TJvmMemoryPool> memory_pools
}

// Contains information about a JVM thread
struct TJvmThreadInfo {
  // Summary of a JVM thread. Includes stacktraces, locked monitors
  // and synchronizers.
  1: required string summary

  // The total CPU time for this thread in nanoseconds
  2: required i64 cpu_time_in_ns

  // The CPU time that this thread has executed in user mode in nanoseconds
  3: required i64 user_time_in_ns

  // The number of times this thread blocked to enter or reenter a monitor
  4: required i64 blocked_count

  // Approximate accumulated elapsed time (in milliseconds) that this thread has blocked
  // to enter or reenter a monitor
  5: required i64 blocked_time_in_ms

  // True if this thread is executing native code via the Java Native Interface (JNI)
  6: required bool is_in_native
}

// Request to get information about JVM threads
struct TGetJvmThreadsInfoRequest {
  // If set, return complete info about JVM threads. Otherwise, return only
  // the total number of live JVM threads.
  1: required bool get_complete_info
}

struct TGetJvmThreadsInfoResponse {
  // The current number of live threads including both daemon and non-daemon threads
  1: required i32 total_thread_count

  // The current number of live daemon threads
  2: required i32 daemon_thread_count

  // The peak live thread count since the Java virtual machine started
  3: required i32 peak_thread_count

  // Information about JVM threads. It is not included when
  // TGetJvmThreadsInfoRequest.get_complete_info is false.
  4: optional list<TJvmThreadInfo> threads
}

struct TGetHadoopConfigRequest {
  // The value of the <name> in the config <property>
  1: required string name
}

struct TGetHadoopConfigResponse {
  // The corresponding value if one exists
  1: optional string value
}

struct TGetAllHadoopConfigsResponse {
  1: optional map<string, string> configs;
}

// For creating a test descriptor table. The tuples and their memory layout are computed
// in the FE.
struct TBuildTestDescriptorTableParams {
  // Every entry describes the slot types of one tuple.
  1: required list<list<Types.TColumnType>> slot_types
}
