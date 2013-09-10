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

struct TColumnDesc {
  1: required string columnName
  2: required Types.TPrimitiveType columnType
}

// A column definition; used by CREATE TABLE and DESCRIBE <table> statements. A column
// definition has a different meaning (and additional fields) from a column descriptor,
// so this is a separate struct from TColumnDesc.
struct TColumnDef {
  1: required TColumnDesc columnDesc
  2: optional string comment
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

// Represents a fully qualified function name.
struct TFunctionName {
  // Name of the function's parent database. Null to specify an unqualified function name.
  1: required string db_name

  // Name of the function
  2: required string function_name
}

// Parameters of CREATE FUNCTION commands
struct TCreateFunctionParams {
  // Fully qualified function name of the function to create
  1: required TFunctionName fn_name

  // HDFS path for the function binary. This binary must exist at the time the
  // function is created.
  2: required string location

  // Name of function in the binary
  3: required string binary_name;

  // The types of the arguments to the function
  4: required list<Types.TPrimitiveType> arg_types;

  // Return type for the function.
  5: required Types.TPrimitiveType ret_type;

  // Optional comment to attach to the function
  6: optional string comment

  // Do not throw an error if a function of the same signature already exists.
  7: optional bool if_not_exists
}

// Valid table file formats
enum TFileFormat {
  PARQUETFILE,
  RCFILE,
  SEQUENCEFILE,
  TEXTFILE,
}

// Represents a fully qualified table name.
struct TTableName {
  // Name of the table's parent database. Null to specify an unqualified table name.
  1: required string db_name

  // Name of the table
  2: required string table_name
}

// The row format specifies how to interpret the fields (columns) and lines (rows) in a
// data file when creating a new table.
struct TTableRowFormat {
  // Optional terminator string used to delimit fields (columns) in the table
  1: optional string field_terminator

  // Optional terminator string used to delimit lines (rows) in a table
  2: optional string line_terminator

  // Optional string used to specify a special escape character sequence
  3: optional string escaped_by
}

// Types of ALTER TABLE commands supported.
enum TAlterTableType {
  ADD_REPLACE_COLUMNS,
  ADD_PARTITION,
  CHANGE_COLUMN,
  DROP_COLUMN,
  DROP_PARTITION,
  RENAME_TABLE,
  RENAME_VIEW,
  SET_FILE_FORMAT,
  SET_LOCATION,
  SET_TBL_PROPERTIES,
}

// Represents a single item in a partition spec (column name + value)
struct TPartitionKeyValue {
  // Partition column name
  1: required string name,

  // Partition value
  2: required string value
}

// Parameters for ALTER TABLE rename commands
struct TAlterTableOrViewRenameParams {
  // The new table name
  1: required TTableName new_table_name
}

// Parameters for ALTER TABLE ADD|REPLACE COLUMNS commands.
struct TAlterTableAddReplaceColsParams {
  // List of columns to add to the table
  1: required list<TColumnDef> columns

  // If true, replace all existing columns. If false add (append) columns to the table.
  2: required bool replace_existing_cols
}

// Parameters for ALTER TABLE ADD PARTITION commands
struct TAlterTableAddPartitionParams {
  // The partition spec (list of keys and values) to add.
  1: required list<TPartitionKeyValue> partition_spec

  // If true, no error is raised if a partition with the same spec already exists.
  3: required bool if_not_exists

  // Optional HDFS storage location for the Partition. If not specified the
  // default storage location is used.
  2: optional string location
}

// Parameters for ALTER TABLE DROP COLUMN commands.
struct TAlterTableDropColParams {
  // Column name to drop.
  1: required string col_name
}

// Parameters for ALTER TABLE DROP PARTITION commands
struct TAlterTableDropPartitionParams {
  // The partition spec (list of keys and values) to add.
  1: required list<TPartitionKeyValue> partition_spec

  // If true, no error is raised if no partition with the specified spec exists.
  2: required bool if_exists
}

// Parameters for ALTER TABLE CHANGE COLUMN commands
struct TAlterTableChangeColParams {
  // Target column to change.
  1: required string col_name

  // New column definition for the target column.
  2: required TColumnDef new_col_def
}

// Parameters for ALTER TABLE SET TBLPROPERTIES commands.
struct TAlterTableSetTblPropertiesParams {
  // Map of property names to property values
  1: required map<string, string> table_properties
}

// Parameters for ALTER TABLE SET [PARTITION partitionSpec] FILEFORMAT commands.
struct TAlterTableSetFileFormatParams {
  // New file format
  1: required TFileFormat file_format

  // An optional partition spec, set if modifying the fileformat of a partition.
  2: optional list<TPartitionKeyValue> partition_spec
}

// Parameters for ALTER TABLE SET [PARTITION partitionSpec] location commands.
struct TAlterTableSetLocationParams {
  // New HDFS storage location of the table
  1: required string location

  // An optional partition spec, set if modifying the location of a partition.
  2: optional list<TPartitionKeyValue> partition_spec
}

// Parameters for all ALTER TABLE commands.
struct TAlterTableParams {
  1: required TAlterTableType alter_type

  // Fully qualified name of the target table being altered
  2: required TTableName table_name

  // Parameters for ALTER TABLE/VIEW RENAME
  3: optional TAlterTableOrViewRenameParams rename_params

  // Parameters for ALTER TABLE ADD COLUMNS
  4: optional TAlterTableAddReplaceColsParams add_replace_cols_params

  // Parameters for ALTER TABLE ADD PARTITION
  5: optional TAlterTableAddPartitionParams add_partition_params

  // Parameters for ALTER TABLE CHANGE COLUMN
  6: optional TAlterTableChangeColParams change_col_params

  // Parameters for ALTER TABLE DROP COLUMN
  7: optional TAlterTableDropColParams drop_col_params

  // Parameters for ALTER TABLE DROP PARTITION
  8: optional TAlterTableDropPartitionParams drop_partition_params

  // Parameters for ALTER TABLE SET FILEFORMAT
  9: optional TAlterTableSetFileFormatParams set_file_format_params

  // Parameters for ALTER TABLE SET LOCATION
  10: optional TAlterTableSetLocationParams set_location_params

  // Parameters for ALTER TABLE SET TBLPROPERTIES
  11: optional TAlterTableSetTblPropertiesParams set_tbl_properties_params
}

// Parameters of CREATE TABLE LIKE commands
struct TCreateTableLikeParams {
  // Fully qualified name of the table to create
  1: required TTableName table_name

  // Fully qualified name of the source table
  2: required TTableName src_table_name

  // True if the table is an "EXTERNAL" table. Dropping an external table will NOT remove
  // table data from the file system. If EXTERNAL is not specified, all table data will be
  // removed when the table is dropped.
  3: required bool is_external

  // Do not throw an error if a table of the same name already exists.
  4: required bool if_not_exists

  // Owner of the table
  5: required string owner

  // Optional file format for this table
  6: optional TFileFormat file_format

  // Optional comment for the table
  7: optional string comment

  // Optional storage location for the table
  8: optional string location
}

// Parameters of CREATE TABLE commands
struct TCreateTableParams {
  // Fully qualified name of the table to create
  1: required TTableName table_name

  // List of columns to create
  2: required list<TColumnDef> columns

  // List of partition columns
  3: optional list<TColumnDef> partition_columns

  // The file format for this table
  4: required TFileFormat file_format

  // True if the table is an "EXTERNAL" table. Dropping an external table will NOT remove
  // table data from the file system. If EXTERNAL is not specified, all table data will be
  // removed when the table is dropped.
  5: required bool is_external

  // Do not throw an error if a table of the same name already exists.
  6: required bool if_not_exists

  // The owner of the table
  7: required string owner

  // Specifies how rows and columns are interpreted when reading data from the table
  8: optional TTableRowFormat row_format

  // Optional comment for the table
  9: optional string comment

  // Optional storage location for the table
  10: optional string location

  // Map of property names to property values
  11: optional map<string, string> table_properties
}

// Parameters of a CREATE VIEW or ALTER VIEW AS SELECT command
struct TCreateOrAlterViewParams {
  // Fully qualified name of the view to create
  1: required TTableName view_name

  // List of column definitions for the view
  2: required list<TColumnDef> columns

  // The owner of the view
  3: required string owner

  // Original SQL string of view definition
  4: required string original_view_def

  // Expanded SQL string of view definition used in view substitution
  5: required string expanded_view_def

  // Optional comment for the view
  6: optional string comment

  // Do not throw an error if a table or view of the same name already exists
  7: optional bool if_not_exists
}

// Parameters of DROP DATABASE commands
struct TDropDbParams {
  // Name of the database to drop
  1: required string db

  // If true, no error is raised if the target db does not exist
  2: required bool if_exists
}

// Parameters of DROP TABLE/VIEW commands
struct TDropTableOrViewParams {
  // Fully qualified name of the table/view to drop
  1: required TTableName table_name

  // If true, no error is raised if the target table/view does not exist
  2: required bool if_exists
}

// Parameters of DROP FUNCTION commands
struct TDropFunctionParams {
  // Fully qualified name of the function to drop
  1: required TFunctionName fn_name

  // The types of the arguments to the function
  2: required list<Types.TPrimitiveType> arg_types;

  // If true, no error is raised if the target fn does not exist
  3: required bool if_exists
}

// Parameters of REFRESH/INVALIDATE METADATA commands
// NOTE: This struct should only be used for intra-process communication.
struct TResetMetadataParams {
  // If true, refresh. Otherwise, invalidate metadata
  1: required bool is_refresh

  // Fully qualified name of the table to refresh or invalidate; not set if invalidating
  // the entire catalog
  2: optional TTableName table_name
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

// Parameters for SHOW FUNCTIONS commands
struct TShowFunctionsParams {
  // Database to use for SHOW FUNCTIONS
  1: optional string db

  // Optional pattern to match function names. If not set, all functions are returned.
  2: optional string show_pattern
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
  // Database to use for SHOW FUNCTIONS
  1: optional string db

  // If not set, match every function
  2: optional string pattern

  // Session state for the user who initiated this request. If authorization is
  // enabled, only the functions this user has access to will be returned. If not
  // set, access checks will be skipped (used for internal Impala requests)
  3: optional TSessionState session
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

// Request for a LOAD DATA statement. LOAD DATA is only supported for HDFS backed tables.
struct TLoadDataReq {
  // Fully qualified table name to load data into.
  1: required TTableName table_name

  // The source data file or directory to load into the table.
  2: required string source_path

  // If true, loaded files will overwrite all data in the target table/partition's
  // directory. If false, new files will be added alongside existing files. If there are
  // any file name conflicts, the new files will be uniquified by appending a UUID to the
  // base file name preserving the extension if one exists.
  3: required bool overwrite

  // An optional partition spec. Set if this operation should apply to a specific
  // partition rather than the base table.
  4: optional list<TPartitionKeyValue> partition_spec
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
}

enum TDdlType {
  SHOW_TABLES,
  SHOW_DBS,
  USE,
  DESCRIBE,
  ALTER_TABLE,
  ALTER_VIEW,
  CREATE_DATABASE,
  CREATE_TABLE,
  CREATE_TABLE_AS_SELECT,
  CREATE_TABLE_LIKE,
  CREATE_VIEW,
  DROP_DATABASE,
  DROP_TABLE,
  DROP_VIEW,
  RESET_METADATA
  SHOW_FUNCTIONS,
  CREATE_FUNCTION,
  DROP_FUNCTION,
}

struct TDdlExecResponse {
  // Set only for CREATE TABLE AS SELECT statements. Will be true iff the statement
  // resulted in a new table being created in the Metastore. This is used to
  // determine if a CREATE TABLE IF NOT EXISTS AS SELECT ... actually creates a new
  // table or whether creation was skipped because the table already existed, in which
  // case this flag would be false
  1: optional bool new_table_created;
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

  // Parameters for ALTER TABLE
  6: optional TAlterTableParams alter_table_params

  // Parameters for ALTER VIEW
  14: optional TCreateOrAlterViewParams alter_view_params

  // Parameters for CREATE DATABASE
  7: optional TCreateDbParams create_db_params

  // Parameters for CREATE TABLE
  8: optional TCreateTableParams create_table_params

  // Parameters for CREATE TABLE LIKE
  9: optional TCreateTableLikeParams create_table_like_params

  // Parameters for CREATE VIEW
  13: optional TCreateOrAlterViewParams create_view_params

  // Paramaters for DROP DATABAE
  10: optional TDropDbParams drop_db_params

  // Parameters for DROP TABLE/VIEW
  11: optional TDropTableOrViewParams drop_table_or_view_params

  // Parameters for REFRESH/INVALIDATE METADATA
  12: optional TResetMetadataParams reset_metadata_params

  // Parameters for SHOW FUNCTIONS
  15: optional TShowFunctionsParams show_fns_params

  // Parameters for CREATE FUNCTION
  16: optional TCreateFunctionParams create_fn_params

  // Parameters for DROP FUNCTION
  17: optional TDropFunctionParams drop_fn_params
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

// Enum used by TAccessEvent to mark what type of Catalog object was accessed
// in a query statement
enum TCatalogObjectType {
  DATABASE,
  TABLE,
  VIEW,
}

// Tracks accesses to Catalog objects for use during auditing. This information, paired
// with the current session information, provides a view into what objects a user's
// query accessed
struct TAccessEvent {
  // Fully qualified object name
  1: required string name

  // The object type (DATABASE, VIEW, TABLE)
  2: required TCatalogObjectType object_type

  // The requested privilege on the object
  // TODO: Create an enum for this?
  3: required string privilege
}

// Result of call to createExecRequest()
struct TExecRequest {
  1: required Types.TStmtType stmt_type;

  // Copied from the corresponding TClientRequest
  2: required ImpalaInternalService.TQueryOptions query_options;

  // TQueryExecRequest for the backend
  // Set iff stmt_type is QUERY or DML
  3: optional TQueryExecRequest query_exec_request

  // Set iff stmt_type is DDL
  4: optional TDdlExecRequest ddl_exec_request

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

// Convenience type to map between log4j levels and glog severity
enum TLogLevel {
  VLOG_3,
  VLOG_2
  VLOG,
  INFO,
  WARN,
  ERROR,
  FATAL
}
