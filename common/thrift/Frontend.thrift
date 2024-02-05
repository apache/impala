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
include "RuntimeProfile.thrift"
include "Descriptors.thrift"
include "Data.thrift"
include "Results.thrift"
include "TCLIService.thrift"
include "Status.thrift"
include "CatalogObjects.thrift"
include "CatalogService.thrift"
include "LineageGraph.thrift"
include "Query.thrift"

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

// Arguments to getTableNames, which returns a list of tables that are of specified table
// types and match an optional pattern.
struct TGetTablesParams {
  // If not set, match tables in all DBs
  1: optional string db

  // If not set, match every table
  2: optional string pattern

  // Session state for the user who initiated this request. If authorization is
  // enabled, only the tables this user has access to will be returned. If not
  // set, access checks will be skipped (used for internal Impala requests)
  3: optional Query.TSessionState session

  // This specifies the types of tables that should be returned. If not set, all types of
  // tables are considered when their names are matched against pattern.
  4: optional set<CatalogService.TImpalaTableType> table_types = []
}

// Arguments to getMetadataTableNames, which returns the list of metadata tables of the
// specified table.
struct TGetMetadataTablesParams {
  1: required string db

  2: required string tbl

  // If not set, match every table
  3: optional string pattern

  // Session state for the user who initiated this request. If authorization is
  // enabled, only the tables this user has access to will be returned. If not
  // set, access checks will be skipped (used for internal Impala requests)
  4: optional Query.TSessionState session
}

// getTableNames returns a list of unqualified table names
struct TGetTablesResult {
  1: list<string> tables
}

// Arguments to getTableMetrics, which returns the metrics of a specific table.
struct TGetTableMetricsParams {
  1: required CatalogObjects.TTableName table_name
}

// Response to a getTableMetrics request. The response contains all the collected metrics
// pretty-printed into a string.
struct TGetTableMetricsResponse {
  1: required string metrics
}

// Response from a call to getCatalogMetrics.
struct TGetCatalogMetricsResult {
  1: required i32 num_dbs
  2: required i32 num_tables
  // Following cache metrics are set only in local catalog mode. These map to Guava's
  // CacheStats. Accounts for all the cache requests since the process boot time.
  3: optional i64 cache_eviction_count
  4: optional i64 cache_hit_count
  5: optional i64 cache_load_count
  6: optional i64 cache_load_exception_count
  7: optional i64 cache_load_success_count
  8: optional i64 cache_miss_count
  9: optional i64 cache_request_count
  10: optional i64 cache_total_load_time
  11: optional double cache_avg_load_time
  12: optional double cache_hit_rate
  13: optional double cache_load_exception_rate
  14: optional double cache_miss_rate
  15: optional double cache_entry_median_size
  16: optional double cache_entry_99th_size
}

// Arguments to getDbs, which returns a list of dbs that match an optional pattern
struct TGetDbsParams {
  // If not set, match every database
  1: optional string pattern

  // Session state for the user who initiated this request. If authorization is
  // enabled, only the databases this user has access to will be returned. If not
  // set, access checks will be skipped (used for internal Impala requests)
  2: optional Query.TSessionState session
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
  MINIMAL = 0

  // Output additional information on the database or table.
  // Set for both DESCRIBE DATABASE FORMATTED|EXTENDED <db>
  // and DESCRIBE FORMATTED|EXTENDED <table> statements.
  EXTENDED = 1
  FORMATTED = 2
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

  // Set for metadata tables
  3: optional string metadata_table_name

  // Set when describing a path to a nested collection.
  4: optional Types.TColumnType result_struct

  // Session state for the user who initiated this request.
  5: optional Query.TSessionState session
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
  TABLE_STATS = 0
  COLUMN_STATS = 1
  PARTITIONS = 2
  RANGE_PARTITIONS = 3
  HASH_SCHEMA = 4
}

// Parameters for SHOW TABLE/COLUMN STATS and SHOW PARTITIONS commands
struct TShowStatsParams {
  1: TShowStatsOp op
  2: CatalogObjects.TTableName table_name
  3: optional bool show_column_minmax_stats
}

// Parameters for DESCRIBE HISTORY command
struct TDescribeHistoryParams {
  1: CatalogObjects.TTableName table_name
  2: optional i64 between_start_time
  3: optional i64 between_end_time
  4: optional i64 from_time
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

// Parameters for SHOW TABLES, SHOW METADATA TABLES and SHOW VIEWS commands
struct TShowTablesParams {
  // Database to use for SHOW TABLES
  1: optional string db

  // Set for querying the metadata tables of the given table.
  2: optional string tbl

  // Optional pattern to match tables names. If not set, all tables from the given
  // database are returned.
  3: optional string show_pattern

  // This specifies the types of tables that should be returned. If not set, all types of
  // tables are considered when their names are matched against pattern.
  4: optional set<CatalogService.TImpalaTableType> table_types = []
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
  // REMOVED: 2: required bool is_admin_op

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

// Represents one row in the DESCRIBE HISTORY command's result.
struct TGetTableHistoryResultItem {
  // Timestamp in millis
  1: required i64 creation_time
  2: required i64 snapshot_id
  3: optional i64 parent_id
  4: required bool is_current_ancestor
}

// Result of the DESCRIBE HISTORY command.
struct TGetTableHistoryResult {
  1: required list<TGetTableHistoryResultItem> result
}

// Parameters for SHOW GRANT ROLE/USER commands
struct TShowGrantPrincipalParams {
  // The effective user who submitted this request.
  1: optional string requesting_user

  // The target name.
  2: required string name

  // The principal type.
  3: required CatalogObjects.TPrincipalType principal_type;

  // True if this operation requires admin privileges on the Sentry Service (when
  // the requesting user has not been granted the target role name).
  // REMOVED: 4: required bool is_admin_op

  // An optional filter to show grants that match a specific privilege spec.
  5: optional CatalogObjects.TPrivilege privilege
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
  4: optional Query.TSessionState session
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

  // True if the destination table is an Iceberg table, in this case we need to insert
  // data to the Iceberg table based on the given files.
  5: optional bool iceberg_tbl

  // For Iceberg data load. Query template to create a temporary with table location
  // pointing to the new files. The table location is unknown during planning, these are
  // filled during execution.
  6: optional string create_tmp_tbl_query_template

  // For Iceberg data load. Query to insert into the destination table from the
  // temporary table.
  7: optional string insert_into_dst_tbl_query

  // For Iceberg data load. Query to drop the temporary table.
  8: optional string drop_tmp_tbl_query
}

// Response of a LOAD DATA statement.
struct TLoadDataResp {
  // A result row that contains information on the result of the LOAD operation. This
  // includes details like the number of files moved as part of the request.
  1: required Data.TResultRow load_summary

  // The loaded file paths
  2: required list<string> loaded_files

  // This is needed to issue TUpdateCatalogRequest
  3: string partition_name = ""

  // For Iceberg data load. The query template after the required fields are substituted.
  4: optional string create_tmp_tbl_query

  // For Iceberg data load. The temporary table location, used to restore data in case of
  // query failure.
  5: optional string create_location
}

enum TCatalogOpType {
  SHOW_TABLES = 0
  SHOW_DBS = 1
  SHOW_STATS = 2
  USE = 3
  DESCRIBE_TABLE = 4
  DESCRIBE_DB = 5
  SHOW_FUNCTIONS = 6
  RESET_METADATA = 7
  DDL = 8
  SHOW_CREATE_TABLE = 9
  SHOW_DATA_SRCS = 10
  SHOW_ROLES = 11
  SHOW_GRANT_PRINCIPAL = 12
  SHOW_FILES = 13
  SHOW_CREATE_FUNCTION = 14
  DESCRIBE_HISTORY = 15
  SHOW_VIEWS = 16
  SHOW_METADATA_TABLES = 17
}

// TODO: Combine SHOW requests with a single struct that contains a field
// indicating which type of show request it is.
struct TCatalogOpRequest {
  1: required TCatalogOpType op_type

  // True if SYNC_DDL is used in the query options
  2: required bool sync_ddl

  // Parameters for USE commands
  3: optional TUseDbParams use_db_params

  // Parameters for DESCRIBE DATABASE db commands
  4: optional TDescribeDbParams describe_db_params

  // Parameters for DESCRIBE table commands
  5: optional TDescribeTableParams describe_table_params

  // Parameters for SHOW DATABASES
  6: optional TShowDbsParams show_dbs_params

  // Parameters for SHOW TABLES
  7: optional TShowTablesParams show_tables_params

  // Parameters for SHOW FUNCTIONS
  8: optional TShowFunctionsParams show_fns_params

  // Parameters for SHOW DATA SOURCES
  9: optional TShowDataSrcsParams show_data_srcs_params

  // Parameters for SHOW ROLES
  10: optional TShowRolesParams show_roles_params

  // Parameters for SHOW GRANT ROLE/USER
  11: optional TShowGrantPrincipalParams show_grant_principal_params

  // Parameters for DDL requests executed using the CatalogServer
  // such as CREATE, ALTER, and DROP. See CatalogService.TDdlExecRequest
  // for details.
  12: optional CatalogService.TDdlExecRequest ddl_params

  // Parameters for RESET/INVALIDATE METADATA, executed using the CatalogServer.
  // See CatalogService.TResetMetadataRequest for more details.
  13: optional CatalogService.TResetMetadataRequest reset_metadata_params

  // Parameters for SHOW TABLE/COLUMN STATS
  14: optional TShowStatsParams show_stats_params

  // Parameters for SHOW CREATE TABLE
  15: optional CatalogObjects.TTableName show_create_table_params

  // Parameters for SHOW FILES
  16: optional TShowFilesParams show_files_params

  // Column lineage graph
  17: optional LineageGraph.TLineageGraph lineage_graph

  // Parameters for SHOW_CREATE_FUNCTION
  18: optional TGetFunctionsParams show_create_function_params

  // Parameters for DESCRIBE HISTORY
  19: optional TDescribeHistoryParams describe_history_params
}

// Query options type
enum TQueryOptionType {
  SET_ONE = 0
  SET_ALL = 1
  UNSET_ALL = 2
}

// Parameters for the SET query option command
struct TSetQueryOptionRequest {
  // Set for "SET key=value", unset for "SET" and "SET ALL" statements.
  1: optional string key
  2: optional string value
  // query option type
  3: optional TQueryOptionType query_option_type
}

struct TShutdownParams {
  // Set if a backend was specified as an argument to the shutdown function. If not set,
  // the current impala daemon will be shut down. If the port was specified, it is set
  // in 'backend'. If it was not specified, it is 0 and the port configured for this
  // Impala daemon is assumed.
  1: optional Types.TNetworkAddress backend

  // Deadline in seconds for shutting down.
  2: optional i64 deadline_s
}

// The type of administrative function to be executed.
enum TAdminRequestType {
  SHUTDOWN = 0
}

// Parameters for administrative function statement. This is essentially a tagged union
// that contains parameters for the type of administrative statement to be executed.
struct TAdminRequest {
  1: required TAdminRequestType type

  // The below member corresponding to 'type' should be set.
  2: optional TShutdownParams shutdown_params
}

// HiveServer2 Metadata operations (JniFrontend.hiveServer2MetadataOperation)
enum TMetadataOpcode {
  GET_TYPE_INFO = 0
  GET_CATALOGS = 1
  GET_SCHEMAS = 2
  GET_TABLES = 3
  GET_TABLE_TYPES = 4
  GET_COLUMNS = 5
  GET_FUNCTIONS = 6
  GET_PRIMARY_KEYS = 7
  GET_CROSS_REFERENCE = 8
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
  10: optional Query.TSessionState session
  11: optional TCLIService.TGetPrimaryKeysReq get_primary_keys_req
  12: optional TCLIService.TGetCrossReferenceReq get_cross_reference_req
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

// Request for "ALTER TABLE ... CONVERT TO" statements
struct TConvertTableRequest {
  1: required CatalogObjects.TTableName table_name
  2: required CatalogObjects.TTableName hdfs_table_name
  3: required CatalogObjects.THdfsFileFormat file_format
  4: optional map<string, string> properties
  5: optional string set_hdfs_table_properties_query
  6: optional string rename_hdfs_table_to_temporary_query
  7: optional string refresh_temporary_hdfs_table_query
  8: optional string reset_table_name_query
  9: optional string create_iceberg_table_query
  10: optional string invalidate_metadata_query
  11: optional string post_create_alter_table_query
  12: optional string drop_temporary_hdfs_table_query
}

// Result of call to createExecRequest()
struct TExecRequest {
  1: required Types.TStmtType stmt_type = TStmtType.UNKNOWN

  // Copied from the corresponding TClientRequest
  2: required Query.TQueryOptions query_options

  // TQueryExecRequest for the backend
  // Set iff stmt_type is QUERY or DML
  3: optional Query.TQueryExecRequest query_exec_request

  // Set if stmt_type is DDL
  4: optional TCatalogOpRequest catalog_op_request

  // Metadata of the query result set (not set for DML)
  5: optional Results.TResultSetMetadata result_set_metadata

  // Result of EXPLAIN. Set iff stmt_type is EXPLAIN
  6: optional TExplainResult explain_result

  // Request for LOAD DATA statements.
  7: optional TLoadDataReq load_data_request

  // List of catalog objects accessed by this request. May be empty in this
  // case that the query did not access any Catalog objects.
  8: optional list<TAccessEvent> access_events

  // List of warnings that were generated during analysis. May be empty.
  9: required list<string> analysis_warnings

  // Set if stmt_type is SET
  10: optional TSetQueryOptionRequest set_query_option_request

  // Timeline of planner's operation, for profiling
  // TODO(todd): should integrate this with the 'profile' member instead.
  11: optional RuntimeProfile.TEventSequence timeline

  // If false, the user that runs this statement doesn't have access to the runtime
  // profile. For example, a user can't access the runtime profile of a query
  // that has a view for which the user doesn't have access to the underlying tables.
  12: optional bool user_has_profile_access

  // Set iff stmt_type is ADMIN_FN.
  13: optional TAdminRequest admin_request

  // Profile information from the planning process.
  14: optional RuntimeProfile.TRuntimeProfileNode profile

  // Set iff stmt_type is TESTCASE
  15: optional string testcase_data_path

  // Coordinator time when plan was submitted by external frontend
  16: optional i64 remote_submit_time

  // Additional profile nodes to be displayed nested right under 'profile' field.
  17: optional list<RuntimeProfile.TRuntimeProfileNode> profile_children

  // True if request pool is set by Frontend rather than user specifically setting it via
  // REQUEST_POOL query option.
  18: optional bool request_pool_set_by_frontend = false

  // Request for "ALTER TABLE ... CONVERT TO" statements.
  19: optional TConvertTableRequest convert_table_request

  20: optional list<CatalogObjects.TTableName> tables
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
  UDF_EVALUATE = 0
  UDF_PREPARE = 1
  UDF_CLOSE = 2
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
  7: required TSymbolType symbol_type

  // Does the lookup require the backend lib-cache entry be refreshed?
  // If so, the file system is checked for a newer version of the file
  // referenced by 'location'. If not, the entry in the lib-cache is used
  // if present, otherwise the file is read from file-system.
  8: required bool needs_refresh
}

enum TSymbolLookupResultCode {
  SYMBOL_FOUND = 0
  BINARY_NOT_FOUND = 1
  SYMBOL_NOT_FOUND = 2
}

struct TSymbolLookupResult {
  // The result of the symbol lookup.
  1: required TSymbolLookupResultCode result_code

  // The symbol that was found. set if result_code == SYMBOL_FOUND.
  2: optional string symbol

  // The error message if the symbol found not be found.
  3: optional string error_msg

  // Last modified time in backend lib-cache entry for the file referenced by 'location'.
  4: optional i64 last_modified_time
}

// Sent from the impalad BE to FE with the results of each CatalogUpdate heartbeat.
// The catalog object updates are passed separately via NativeGetCatalogUpdate() callback.
struct TUpdateCatalogCacheRequest {
  // True if update only contains entries changed from the previous update. Otherwise,
  // contains the entire topic.
  1: required bool is_delta

  // The Catalog Service ID this update came from. A request should has either this field
  // set or a Catalog typed catalog object in the update list.
  2: optional Types.TUniqueId catalog_service_id

  // New or modified items. Empty list if no items were updated. Deprecated after
  // IMPALA-5990.
  3: optional list<CatalogObjects.TCatalogObject> updated_objects_deprecated

  // Empty if no items were removed or is_delta is false. Deprecated after IMPALA-5990.
  4: optional list<CatalogObjects.TCatalogObject> removed_objects_deprecated

  // The native ptr for calling back NativeGetCatalogUpdate().
  5: required i64 native_iterator_ptr
}

// Response from a TUpdateCatalogCacheRequest.
struct TUpdateCatalogCacheResponse {
  // The catalog service id this version is from.
  1: required Types.TUniqueId catalog_service_id

  // The lower bound of catalog object versions after CatalogUpdate() was processed.
  2: required i64 catalog_object_version_lower_bound

  // The updated catalog version needed by the backend.
  3: required i64 new_catalog_version
}

// Types of executor groups
struct TExecutorGroupSet {
  // The current max number of executors among all healthy groups of this group set.
  1: i32 curr_num_executors = 0

  // The expected size of the executor groups. Can be used to plan queries when
  // no healthy executor groups are present(curr_num_executors is 0).
  2: i32 expected_num_executors = 0

  // The name of the request pool associated with this executor group type. All
  // executor groups that match this prefix will be included as a part of this set.
  // Note: this will be empty when 'default' executor group is used or
  // 'expected_executor_group_sets' startup flag is not specified.
  3: string exec_group_name_prefix

  // The optional max_mem_limit to determine which executor group set to run for a query.
  // The max_mem_limit value is set to the max_query_mem_limit attribute of the group set
  // with name prefix 'exec_group_name_prefix' from the pool service. For each query,
  // the frontend computes the per host estimated-memory after a compilation with a
  // number of executor nodes from this group set and compares it with this variable.
  4: optional i64 max_mem_limit

  // The optional num_cores_per_executor is used to determine which executor group set to
  // run for a query. The num_cores_per_executor value is set to
  // max_query_cpu_core_per_node_limit attribute of the group set with name prefix
  // 'exec_group_name_prefix' from the pool service.
  // The total number of CPU cores among all executors in this executor group equals
  // num_cores_per_executor * curr_num_executors if curr_num_executors is greater than 0,
  // otherwise it equals num_cores_per_executor * expected_num_executors.
  // For each query, the frontend computes the estimated total CPU core count required
  // for a query to run efficiently after a compilation with a number of executor nodes
  // from this group set and compare it with the total number of CPU cores in this
  // executor group.
  5: optional i32 num_cores_per_executor
}

// Sent from the impalad BE to FE with the latest membership snapshot of the
// executors on the cluster resulting from the Membership heartbeat.
struct TUpdateExecutorMembershipRequest {
  // The hostnames of the executor nodes.
  // Note: There can be multiple executors running on the same host.
  1: required set<string> hostnames

  // The ip addresses of the executor nodes.
  // Note: There can be multiple executors running on the same ip addresses.
  2: required set<string> ip_addresses

  // Info about existing executor group sets.
  3: list<TExecutorGroupSet> exec_group_sets
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

// Response from JniUtil::GetJvmMemoryMetrics()
struct TGetJvmMemoryMetricsResponse {
  // One entry for every pool tracked by the Jvm, plus a synthetic aggregate pool called
  // 'total'
  1: required list<TJvmMemoryPool> memory_pools

  // Metrics from JvmPauseMonitor, measuring how much time is spend
  // pausing, presumably because of Garbage Collection. These
  // names are consistent with Hadoop's metric names.
  2: required i64 gc_num_warn_threshold_exceeded
  3: required i64 gc_num_info_threshold_exceeded
  4: required i64 gc_total_extra_sleep_time_millis

  // Metrics for JVM Garbage Collection, from the management beans;
  // these are cumulative across all types of GCs.
  5: required i64 gc_count
  6: required i64 gc_time_millis
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

struct TGetJMXJsonResponse {
  // JMX of the JVM serialized to a json string.
  1: required string jmx_json
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

struct TGetHadoopGroupsRequest {
  // The user name to get the groups from.
  1: required string user
}

struct TGetHadoopGroupsResponse {
  // The list of groups that the user belongs to.
  1: required list<string> groups
}

// For creating a test descriptor table. The tuples and their memory layout are computed
// in the FE.
struct TBuildTestDescriptorTableParams {
  // Every entry describes the slot types of one tuple.
  1: required list<list<Types.TColumnType>> slot_types
}

// Output format for generating a testcase for a given query_stmt. The resulting bytes
// are compressed before writing to a file.
// TODO: Add the EXPLAIN string from the source cluster on which the testcase was
// collected.
struct TTestCaseData {
  // Query statemnt for which this test case data is generated.
  1: required string query_stmt

  // All referenced table and view defs.
  2: optional list<CatalogObjects.TTable> tables_and_views

  // All databases referenced in the query.
  3: optional list<CatalogObjects.TDatabase> dbs

  // Output path
  4: required string testcase_data_path

  // Impala version that was used to generate this testcase.
  // TODO: How to deal with version incompatibilities? E.g: A testcase collected on
  // Impala version v1 may or may not be compatible to Impala version v2 if the
  // underlying thrift layout changes.
  5: required string impala_version
}

// Information about a query sent to the FE QueryEventHooks
// after query execution
struct TQueryCompleteContext {
  // the serialized lineage graph of the query, with optional BE-populated information
  //
  // this is an experimental feature and the format will likely change
  // in a future version
  1: required string lineage_string
}

// Contains all information from a HTTP request.
// Currently used to pass from BE to FE to do SAML authentication in Java.
struct TWrappedHttpRequest {
  1: required string method // Currently only POST is used.
  // The following members come from parsing the URL:
  // server_name:server_port/path?params...
  2: required string server_name
  3: required i32 server_port
  4: required string path
  5: required map<string, string> params
  // Headers and cookies come from parsing the HTTP header.
  6: required map<string, string> headers
  7: required map<string, string> cookies
  // Filling the content is optional to allow inspecting the header in FE and
  // continue processing the request in BE.
  8: optional string content
  9: required string remote_ip
  10: required bool secure // True if TLS/SSL was used.
}

// Contains all information needed to respond to a HTTP request.
// Currently used to pass from FE to BE to do SAML authentication in Java.
struct TWrappedHttpResponse {
  1: required i16 status_code
  2: required string status_text
  3: required map<string, string> headers
  4: required map<string, string> cookies
  5: optional string content
  6: optional string content_type
}
