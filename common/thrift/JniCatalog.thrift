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
include "TCLIService.thrift"

// This is a short value due to the HDFS API limits
const i16 HDFS_DEFAULT_CACHE_REPLICATION_FACTOR = 1

// Structs used to execute DDL operations using the JniCatalog.

enum TDdlType {
  ALTER_TABLE,
  ALTER_VIEW,
  CREATE_DATABASE,
  CREATE_TABLE,
  CREATE_TABLE_AS_SELECT,
  CREATE_TABLE_LIKE,
  CREATE_VIEW,
  CREATE_FUNCTION,
  COMPUTE_STATS,
  DROP_DATABASE,
  DROP_TABLE,
  DROP_VIEW,
  DROP_FUNCTION,
  CREATE_DATA_SOURCE,
  DROP_DATA_SOURCE,
  DROP_STATS,
  CREATE_ROLE,
  DROP_ROLE,
  GRANT_ROLE,
  REVOKE_ROLE,
  GRANT_PRIVILEGE,
  REVOKE_PRIVILEGE,
  TRUNCATE_TABLE,
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
  // Used internally by the COMPUTE STATS DDL command.
  UPDATE_STATS,
  SET_CACHED,
  RECOVER_PARTITIONS,
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

// Parameters of CREATE DATA SOURCE commands
struct TCreateDataSourceParams {
  // Data source to create
  1: required CatalogObjects.TDataSource data_source

  // Do not throw an error if a data source of the same name already exists.
  2: optional bool if_not_exists
}

// Parameters of DROP DATA SOURCE command
struct TDropDataSourceParams {
  // Name of the data source to drop
  1: required string data_source

  // If true, no error is raised if the target data source does not exist
  2: optional bool if_exists
}

// Parameters of DROP STATS commands
struct TDropStatsParams {
  // Fully qualified name of the target table
  1: required CatalogObjects.TTableName table_name

  // If set, delete the stats only for a particular partition, but do not recompute the
  // stats for the whole table. This is set only for
  // DROP INCREMENTAL STATS <table> PARTITION(...)
  2: optional list<CatalogObjects.TPartitionKeyValue> partition_spec
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

// A caching operation to perform on a target table or partition. Used by ALTER and CREATE
// statements.
struct THdfsCachingOp {
  // True if this op should cache the target table/partition, false if it should uncache
  // the table/partition.
  1: required bool set_cached

  // Set only if set_cached=true. Provides the name of the pool to use when caching.
  2: optional string cache_pool_name

  // The optional cache replication factor to use. If the replication factor is not
  // specified it's either inherited from the table if the underlying object is a
  // partition or is set to our default HDFS cache replication factor.
  3: optional i16 replication
}

// Parameters for ALTER TABLE rename commands
struct TAlterTableOrViewRenameParams {
  // The new table name
  1: required CatalogObjects.TTableName new_table_name
}

// Parameters for ALTER TABLE ADD|REPLACE COLUMNS commands.
struct TAlterTableAddReplaceColsParams {
  // List of columns to add to the table
  1: required list<CatalogObjects.TColumn> columns

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

  // Optional caching operation to perform on the newly added partition.
  4: optional THdfsCachingOp cache_op
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

  // If true, underlying data is purged using -skipTrash
  3: required bool purge
}

// Parameters for ALTER TABLE CHANGE COLUMN commands
struct TAlterTableChangeColParams {
  // Target column to change.
  1: required string col_name

  // New column definition for the target column.
  2: required CatalogObjects.TColumn new_col_def
}

// Parameters for ALTER TABLE SET [PARTITION ('k1'='a', 'k2'='b'...)]
// TBLPROPERTIES|SERDEPROPERTIES commands.
struct TAlterTableSetTblPropertiesParams {
  // The target table property that is being altered.
  1: required CatalogObjects.TTablePropertyType target

  // Map of property names to property values.
  2: required map<string, string> properties

  // If set, alters the properties of the given partition, otherwise
  // those of the table.
  3: optional list<CatalogObjects.TPartitionKeyValue> partition_spec
}

// Parameters for ALTER TABLE SET [PARTITION partitionSpec] FILEFORMAT commands.
struct TAlterTableSetFileFormatParams {
  // New file format.
  1: required CatalogObjects.THdfsFileFormat file_format

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

// Parameters for updating the table and/or column statistics
// of a table. Used internally by a COMPUTE STATS command.
struct TAlterTableUpdateStatsParams {
  // Fully qualified name of the table to be updated.
  1: required CatalogObjects.TTableName table_name

  // Table-level stats.
  2: optional CatalogObjects.TTableStats table_stats

  // Partition-level stats. Maps from a list of partition-key values
  // to its partition stats.
  3: optional map<list<string>, CatalogObjects.TPartitionStats> partition_stats

  // Column-level stats. Maps from column name to column stats.
  4: optional map<string, CatalogObjects.TColumnStats> column_stats

  // If true, the computation should produce results for all partitions (partitions with
  // no results from the stats queries will be given an empty entry)
  5: optional bool expect_all_partitions

  // If true, this is the result of an incremental stats computation
  6: optional bool is_incremental
}

// Parameters for ALTER TABLE SET [PARTITION partitionSpec] CACHED|UNCACHED
struct TAlterTableSetCachedParams {
  // Details on what operation to perform (cache or uncache)
  1: required THdfsCachingOp cache_op

  // An optional partition spec, set if marking a partition as cached/uncached
  // rather than a table.
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

  // Parameters for updating table/column stats. Used internally by COMPUTE STATS
  12: optional TAlterTableUpdateStatsParams update_stats_params

  // Parameters for ALTER TABLE SET CACHED|UNCACHED
  13: optional TAlterTableSetCachedParams set_cached_params
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
  6: optional CatalogObjects.THdfsFileFormat file_format

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
  2: required list<CatalogObjects.TColumn> columns

  // List of partition columns
  3: optional list<CatalogObjects.TColumn> partition_columns

  // The file format for this table
  4: required CatalogObjects.THdfsFileFormat file_format

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

  // If set, the table will be cached after creation with details specified in cache_op.
  13: optional THdfsCachingOp cache_op
}

// Parameters of a CREATE VIEW or ALTER VIEW AS SELECT command
struct TCreateOrAlterViewParams {
  // Fully qualified name of the view to create
  1: required CatalogObjects.TTableName view_name

  // List of column definitions for the view
  2: required list<CatalogObjects.TColumn> columns

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

// Parameters of a COMPUTE STATS command
struct TComputeStatsParams {
  // Fully qualified name of the table to compute stats for.
  1: required CatalogObjects.TTableName table_name

  // Query for gathering per-partition row count.
  // Not set if this is an incremental computation and no partitions are selected.
  2: optional string tbl_stats_query

  // Query for gathering per-column NDVs and number of NULLs.
  // Not set if there are no columns we can compute stats for, or if this is an
  // incremental computation and no partitions are selected
  3: optional string col_stats_query

  // If true, stats will be gathered incrementally (i.e. only for partitions that have no
  // valid statistics). Ignore for non-partitioned tables.
  4: optional bool is_incremental

  // The intermediate state for all partitions that have valid stats. Only set if
  // is_incremental is true.
  5: optional list<CatalogObjects.TPartitionStats> existing_part_stats

  // List of partitions that we expect to see results for when performing an incremental
  // computation. Only set if is_incremental is true. Used to ensure that even empty
  // partitions emit results.
  6: optional list<list<string>> expected_partitions

  // If true, all partitions are expected, to avoid sending every partition in
  // expected_partitions.
  7: optional bool expect_all_partitions

  // The number of partition columns for the target table. Only set if this is_incremental
  // is true.
  8: optional i32 num_partition_cols
}

// Parameters for CREATE/DROP ROLE
struct TCreateDropRoleParams {
  // True if this is a DROP ROLE statement, false if this is a CREATE ROLE statement.
  1: required bool is_drop

  // The role name to create or drop.
  2: required string role_name
}

// Parameters for GRANT/REVOKE ROLE.
struct TGrantRevokeRoleParams {
  // The role names this change applies to.
  1: required list<string> role_names

  // The group names that are being granted/revoked.
  2: required list<string> group_names

  // True if this is a GRANT statement false if this is a REVOKE statement.
  3: required bool is_grant
}

// Parameters for GRANT/REVOKE privilege TO/FROM role.
struct TGrantRevokePrivParams {
  // List of privileges being granted or revoked. The 'has_grant_opt' for each
  // TPrivilege is inherited from the 'has_grant_opt' of this object.
  1: required list<CatalogObjects.TPrivilege> privileges

  // The role name this change should apply to.
  2: required string role_name

  // True if this is a GRANT statement false if this is a REVOKE statement.
  3: required bool is_grant

  // True if WITH GRANT OPTION is set.
  4: required bool has_grant_opt
}

// Parameters of DROP DATABASE commands
struct TDropDbParams {
  // Name of the database to drop
  1: required string db

  // If true, no error is raised if the target db does not exist
  2: required bool if_exists

  // If true, drops all tables of the database
  3: required bool cascade
}

// Parameters of DROP TABLE/VIEW commands
struct TDropTableOrViewParams {
  // Fully qualified name of the table/view to drop
  1: required CatalogObjects.TTableName table_name

  // If true, no error is raised if the target table/view does not exist
  2: required bool if_exists

  // If true, underlying data is purged using -skipTrash
  3: required bool purge
}

// Parameters of TRUNCATE commands
struct TTruncateParams {
  // Fully qualified name of table to truncate
  1: required CatalogObjects.TTableName table_name

  // If true, no error is raised if the target table does not exist
  2: required bool if_exists
}

// Parameters of DROP FUNCTION commands
struct TDropFunctionParams {
  // Fully qualified name of the function to drop
  1: required Types.TFunctionName fn_name

  // The types of the arguments to the function
  2: required list<Types.TColumnType> arg_types

  // If true, no error is raised if the target fn does not exist
  3: required bool if_exists

  // Signature of the function to drop. Used to look up the function in the catalog.
  // This needs to be the identical string as what is generated by the FE (in general,
  // the signature generated by the FE should just be plumbed through).
  4: optional string signature
}
