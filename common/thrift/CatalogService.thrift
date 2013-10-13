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

include "CatalogObjects.thrift"
include "Types.thrift"
include "Status.thrift"

enum CatalogServiceVersion {
   V1
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

// Parameters of CREATE FUNCTION commands
struct TCreateFunctionParams {
  // The function to create
  1: required Types.TFunction fn

  // Do not throw an error if a function of the same signature already exists.
  2: optional bool if_not_exists
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

// Parameters for ALTER TABLE rename commands
struct TAlterTableOrViewRenameParams {
  // The new table name
  1: required CatalogObjects.TTableName new_table_name
}

// Parameters for ALTER TABLE ADD|REPLACE COLUMNS commands.
struct TAlterTableAddReplaceColsParams {
  // List of columns to add to the table
  1: required list<CatalogObjects.TColumnDef> columns

  // If true, replace all existing columns. If false add (append) columns to the table.
  2: required bool replace_existing_cols
}

// Parameters for ALTER TABLE ADD PARTITION commands
struct TAlterTableAddPartitionParams {
  // The partition spec (list of keys and values) to add.
  1: required list<CatalogObjects.TPartitionKeyValue> partition_spec

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
  1: required list<CatalogObjects.TPartitionKeyValue> partition_spec

  // If true, no error is raised if no partition with the specified spec exists.
  2: required bool if_exists
}

// Parameters for ALTER TABLE CHANGE COLUMN commands
struct TAlterTableChangeColParams {
  // Target column to change.
  1: required string col_name

  // New column definition for the target column.
  2: required CatalogObjects.TColumnDef new_col_def
}

// Parameters for ALTER TABLE SET TBLPROPERTIES|SERDEPROPERTIES commands.
struct TAlterTableSetTblPropertiesParams {
  // The target table property that is being altered.
  1: required CatalogObjects.TTablePropertyType target

  // Map of property names to property values.
  2: required map<string, string> properties
}

// Parameters for ALTER TABLE SET [PARTITION partitionSpec] FILEFORMAT commands.
struct TAlterTableSetFileFormatParams {
  // New file format.
  1: required CatalogObjects.TFileFormat file_format

  // An optional partition spec, set if modifying the fileformat of a partition.
  2: optional list<CatalogObjects.TPartitionKeyValue> partition_spec
}

// Parameters for ALTER TABLE SET [PARTITION partitionSpec] location commands.
struct TAlterTableSetLocationParams {
  // New HDFS storage location of the table.
  1: required string location

  // An optional partition spec, set if modifying the location of a partition.
  2: optional list<CatalogObjects.TPartitionKeyValue> partition_spec
}

// Parameters for all ALTER TABLE commands.
struct TAlterTableParams {
  1: required TAlterTableType alter_type

  // Fully qualified name of the target table being altered
  2: required CatalogObjects.TTableName table_name

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
  1: required CatalogObjects.TTableName table_name

  // Fully qualified name of the source table
  2: required CatalogObjects.TTableName src_table_name

  // True if the table is an "EXTERNAL" table. Dropping an external table will NOT remove
  // table data from the file system. If EXTERNAL is not specified, all table data will be
  // removed when the table is dropped.
  3: required bool is_external

  // Do not throw an error if a table of the same name already exists.
  4: required bool if_not_exists

  // Owner of the table
  5: required string owner

  // Optional file format for this table
  6: optional CatalogObjects.TFileFormat file_format

  // Optional comment for the table
  7: optional string comment

  // Optional storage location for the table
  8: optional string location
}

// Parameters of CREATE TABLE commands
struct TCreateTableParams {
  // Fully qualified name of the table to create
  1: required CatalogObjects.TTableName table_name

  // List of columns to create
  2: required list<CatalogObjects.TColumnDef> columns

  // List of partition columns
  3: optional list<CatalogObjects.TColumnDef> partition_columns

  // The file format for this table
  4: required CatalogObjects.TFileFormat file_format

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

  // Map of table property names to property values
  11: optional map<string, string> table_properties

  // Map of serde property names to property values
  12: optional map<string, string> serde_properties
}

// Parameters of a CREATE VIEW or ALTER VIEW AS SELECT command
struct TCreateOrAlterViewParams {
  // Fully qualified name of the view to create
  1: required CatalogObjects.TTableName view_name

  // List of column definitions for the view
  2: required list<CatalogObjects.TColumnDef> columns

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
  1: required CatalogObjects.TTableName table_name

  // If true, no error is raised if the target table/view does not exist
  2: required bool if_exists
}

// Parameters of DROP FUNCTION commands
struct TDropFunctionParams {
  // Fully qualified name of the function to drop
  1: required Types.TFunctionName fn_name

  // The types of the arguments to the function
  2: required list<Types.TPrimitiveType> arg_types;

  // If true, no error is raised if the target fn does not exist
  3: required bool if_exists
}

enum TDdlType {
  ALTER_TABLE,
  ALTER_VIEW,
  CREATE_DATABASE,
  CREATE_TABLE,
  CREATE_TABLE_AS_SELECT,
  CREATE_TABLE_LIKE,
  CREATE_VIEW,
  CREATE_FUNCTION,
  DROP_DATABASE,
  DROP_TABLE,
  DROP_VIEW,
  DROP_FUNCTION,
}

// Request for executing a DDL operation (CREATE, ALTER, DROP).
struct TDdlExecRequest {
  1: required CatalogServiceVersion protocol_version = CatalogServiceVersion.V1

  2: required TDdlType ddl_type

  // Parameters for ALTER TABLE
  3: optional TAlterTableParams alter_table_params

  // Parameters for ALTER VIEW
  4: optional TCreateOrAlterViewParams alter_view_params

  // Parameters for CREATE DATABASE
  5: optional TCreateDbParams create_db_params

  // Parameters for CREATE TABLE
  6: optional TCreateTableParams create_table_params

  // Parameters for CREATE TABLE LIKE
  7: optional TCreateTableLikeParams create_table_like_params

  // Parameters for CREATE VIEW
  8: optional TCreateOrAlterViewParams create_view_params

  // Parameters for CREATE FUNCTION
  9: optional TCreateFunctionParams create_fn_params

  // Paramaters for DROP DATABASE
  10: optional TDropDbParams drop_db_params

  // Parameters for DROP TABLE/VIEW
  11: optional TDropTableOrViewParams drop_table_or_view_params

  // Parameters for DROP FUNCTION
  12: optional TDropFunctionParams drop_fn_params
}

// Returns details on the result of an operation that updates the Catalog Service's
// catalog, such as the Status of the result and catalog version that will contain
// the update.
struct TCatalogUpdateResult {
  // The CatalogService service ID this result came from.
  1: required Types.TUniqueId catalog_service_id

  // The Catalog version that will contain this update.
  2: required i64 version

  // The status of the operation, OK if the operation was successful.
  3: required Status.TStatus status
}

// Response from executing a TDdlExecRequest
struct TDdlExecResponse {
  1: required TCatalogUpdateResult result

  // Set only for CREATE TABLE AS SELECT statements. Will be true iff the statement
  // resulted in a new table being created in the Metastore. This is used to
  // determine if a CREATE TABLE IF NOT EXISTS AS SELECT ... actually creates a new
  // table or whether creation was skipped because the table already existed, in which
  // case this flag would be false
  2: optional bool new_table_created;
}

// Request for getting all objects names and optionally, extended metadata, for objects
// that exist in the Catalog. Used by the CatalogServer to build a list of catalog
// updates/deletions to send to the StateStore.
struct TGetAllCatalogObjectsRequest {
  // Send the full metadata for objects that are >= this catalog version. Objects that
  // are < this version will only have their object names returned. A version of 0 will
  // return will return full metadata for all objects in the Catalog.
  1: required i64 from_version
}

// Updates the metastore with new partition information and returns a response
// with details on the result of the operation. Used to add partitions after executing
// DML operations, and could potentially be used in the future to update column stats
// after DML operations.
// TODO: Rename this struct to something more descriptive.
struct TUpdateMetastoreRequest {
  1: required CatalogServiceVersion protocol_version = CatalogServiceVersion.V1

  // Unqualified name of the table to change
  2: required string target_table;

  // Database that the table belongs to
  3: required string db_name;

  // List of partitions that are new and need to be created. May
  // include the root partition (represented by the empty string).
  4: required set<string> created_partitions;
}

// Response from a TUpdateMetastoreRequest
struct TUpdateMetastoreResponse {
  1: required TCatalogUpdateResult result
}

// Parameters of REFRESH/INVALIDATE METADATA commands
struct TResetMetadataRequest {
  1: required CatalogServiceVersion protocol_version = CatalogServiceVersion.V1

  // If true, refresh. Otherwise, invalidate metadata
  2: required bool is_refresh

  // Fully qualified name of the table to refresh or invalidate; not set if invalidating
  // the entire catalog
  3: optional CatalogObjects.TTableName table_name
}

// Response from TResetMetadataRequest
struct TResetMetadataResponse {
  1: required TCatalogUpdateResult result
}

// Returns all known Catalog objects (databases, tables/views, and udfs)
// that meet the specified TGetCatalogObjectsRequest criteria.
struct TGetAllCatalogObjectsResponse {
  // The maximum catalog version of all objects in this response or 0 if the Catalog
  // contained no objects.
  1: required i64 max_catalog_version

  // List of catalog objects (empty list if no objects detected in the Catalog).
  2: required list<CatalogObjects.TCatalogObject> objects
}

// The CatalogService API
service CatalogService {
  // Executes a DDL request and returns details on the result of the operation.
  TDdlExecResponse ExecDdl(1: TDdlExecRequest req);

  // Resets the Catalog metadata. Used to explicitly trigger reloading of the Hive
  // Metastore metadata and/or HDFS block location metadata.
  TResetMetadataResponse ResetMetadata(1: TResetMetadataRequest req);

  // Updates the metastore with new partition information and returns a response
  // with details on the result of the operation.
  TUpdateMetastoreResponse UpdateMetastore(1: TUpdateMetastoreRequest req);
}
