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

include "CatalogObjects.thrift"
include "Types.thrift"
include "Status.thrift"
include "TCLIService.thrift"
include "hive_metastore.thrift"

// This is a short value due to the HDFS API limits
const i16 HDFS_DEFAULT_CACHE_REPLICATION_FACTOR = 1

// Structs used to execute DDL operations using the JniCatalog.

enum TDdlType {
  ALTER_TABLE = 0
  ALTER_VIEW = 1
  CREATE_DATABASE = 2
  CREATE_TABLE = 3
  CREATE_TABLE_AS_SELECT = 4
  CREATE_TABLE_LIKE = 5
  CREATE_VIEW = 6
  CREATE_FUNCTION = 7
  COMPUTE_STATS = 8
  DROP_DATABASE = 9
  DROP_TABLE = 10
  DROP_VIEW = 11
  DROP_FUNCTION = 12
  CREATE_DATA_SOURCE = 13
  DROP_DATA_SOURCE = 14
  DROP_STATS = 15
  CREATE_ROLE = 16
  DROP_ROLE = 17
  GRANT_ROLE = 18
  REVOKE_ROLE = 19
  GRANT_PRIVILEGE = 20
  REVOKE_PRIVILEGE = 21
  TRUNCATE_TABLE = 22
  COMMENT_ON = 23
  ALTER_DATABASE = 24
  COPY_TESTCASE = 25
}

enum TOwnerType {
  USER = 0
  ROLE = 1
}

// Types of ALTER DATABASE commands supported.
enum TAlterDbType {
  SET_OWNER = 0
}

// Parameters for ALTER DATABASE SET OWNER commands.
struct TAlterDbSetOwnerParams {
  // The owner type.
  1: required TOwnerType owner_type

  // The owner name.
  2: required string owner_name

  // The server name for security privileges when authorization is enabled.
  // TODO: Need to cleanup:IMPALA-7553
  3: optional string server_name
}

struct TAlterDbParams {
  // The type of ALTER DATABASE command.
  1: required TAlterDbType alter_type

  // Name of the database to alter.
  2: required string db

  // Parameters for ALTER DATABASE SET OWNER commands.
  3: optional TAlterDbSetOwnerParams set_owner_params
}

// Types of ALTER TABLE commands supported.
enum TAlterTableType {
  ADD_COLUMNS = 0
  REPLACE_COLUMNS = 1
  ADD_PARTITION = 2
  ADD_DROP_RANGE_PARTITION = 3
  ALTER_COLUMN = 4
  DROP_COLUMN = 5
  DROP_PARTITION = 6
  RENAME_TABLE = 7
  RENAME_VIEW = 8
  SET_FILE_FORMAT = 9
  SET_LOCATION = 10
  SET_TBL_PROPERTIES = 11
  // Used internally by COMPUTE STATS and by ALTER TABLE SET COLUMN STATS.
  UPDATE_STATS = 12
  SET_CACHED = 13
  RECOVER_PARTITIONS = 14
  SET_ROW_FORMAT = 15
  SET_OWNER = 16
  UNSET_TBL_PROPERTIES = 17
  SET_PARTITION_SPEC = 18
  EXECUTE = 19
  SET_VIEW_PROPERTIES = 20
  UNSET_VIEW_PROPERTIES = 21
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

  // Owner of the database
  5: required string owner

  // The server name for security privileges when authorization is enabled.
  // TODO: Need to cleanup:IMPALA-7553
  6: optional string server_name

  // Optional HDFS path for the database. Overrides location as the default location for
  // all managed tables created in the database.
  7: optional string managed_location
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

  // If set, delete the stats only for specified partitions, but do not recompute the
  // stats for the whole table. This is set only for
  // DROP INCREMENTAL STATS <table> PARTITION(...)
  2: optional list<list<CatalogObjects.TPartitionKeyValue>> partition_set
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

// Parameters for ALTER TABLE ADD COLUMNS commands.
struct TAlterTableAddColsParams {
  // List of columns to add to the table
  1: optional list<CatalogObjects.TColumn> columns

  // If true, no error is raised when a column already exists.
  2: required bool if_not_exists
}

// Parameters for ALTER TABLE REPLACE COLUMNS commands.
struct TAlterTableReplaceColsParams {
  // List of columns to replace to the table
  1: required list<CatalogObjects.TColumn> columns
}

// Parameters for specifying a single partition in ALTER TABLE ADD PARTITION
struct TPartitionDef {
  // The partition spec (list of keys and values) to add.
  1: required list<CatalogObjects.TPartitionKeyValue> partition_spec

  // Optional HDFS storage location for the Partition. If not specified the
  // default storage location is used.
  2: optional string location

  // Optional caching operation to perform on the newly added partition.
  3: optional THdfsCachingOp cache_op
}

// Parameters for ALTER TABLE ADD PARTITION commands
struct TAlterTableAddPartitionParams {
  // If 'if_not_exists' is true, no error is raised when a partition with the same spec
  // already exists. If multiple partitions are specified, the statement will ignore
  // those that exist and add the rest.
  1: required bool if_not_exists

  // The list of partitions to add
  2: required list<TPartitionDef> partitions
}

enum TRangePartitionOperationType {
  ADD = 0
  DROP = 1
}

// Parameters for ALTER TABLE ADD/DROP RANGE PARTITION command
struct TAlterTableAddDropRangePartitionParams {
  // Range partition to add/drop
  1: required CatalogObjects.TRangePartition range_partition_spec

  // If true, ignore errors raised while adding/dropping a range
  // partition
  2: required bool ignore_errors

  // Operation
  3: required TRangePartitionOperationType type
}

// Parameters for ALTER TABLE DROP COLUMN commands.
struct TAlterTableDropColParams {
  // Column name to drop.
  1: required string col_name
}

// Parameters for ALTER TABLE DROP PARTITION commands
struct TAlterTableDropPartitionParams {
  // The partition set used to drop partitions.
  1: required list<list<CatalogObjects.TPartitionKeyValue>> partition_set

  // If true, no error is raised if no partition with the specified spec exists.
  2: required bool if_exists

  // If true, underlying data is purged using -skipTrash
  3: required bool purge

  // Summary of partitions to delete for Iceberg tables
  4: optional CatalogObjects.TIcebergDropPartitionRequest iceberg_drop_partition_request
}

// Parameters for ALTER TABLE ALTER/CHANGE COLUMN commands
struct TAlterTableAlterColParams {
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

  // If set, alters the properties of the given partitions, otherwise
  // those of the table.
  3: optional list<list<CatalogObjects.TPartitionKeyValue>> partition_set
}

// Parameters for ALTER TABLE SET [PARTITION partitionSet] FILEFORMAT commands.
struct TAlterTableSetFileFormatParams {
  // New file format.
  1: required CatalogObjects.THdfsFileFormat file_format

  // An optional partition set, set if modifying the fileformat of the partitions.
  2: optional list<list<CatalogObjects.TPartitionKeyValue>> partition_set
}

// Parameters for ALTER TABLE SET [PARTITION partitionSet] ROW FORMAT commands.
struct TAlterTableSetRowFormatParams {
  // New row format.
  1: required TTableRowFormat row_format

  // An optional partition set, set if modifying the row format of the partitions.
  2: optional list<list<CatalogObjects.TPartitionKeyValue>> partition_set
}

// Parameters for ALTER TABLE SET [PARTITION partitionSpec] location commands.
struct TAlterTableSetLocationParams {
  // New HDFS storage location of the table.
  1: required string location

  // An optional partition spec, set if modifying the location of a partition.
  2: optional list<CatalogObjects.TPartitionKeyValue> partition_spec
}

// Parameters for ALTER TABLE/VIEW SET OWNER commands.
struct TAlterTableOrViewSetOwnerParams {
  // The owner type.
  1: required TOwnerType owner_type

  // The owner name.
  2: required string owner_name

  // The server name for security privileges when authorization is enabled.
  // TODO: Need to cleanup:IMPALA-7553
  3: optional string server_name
}

// Parameters for updating the table and/or column statistics
// of a table. Used by ALTER TABLE SET COLUMN STATS, and internally by
// a COMPUTE STATS command.
struct TAlterTableUpdateStatsParams {
  // Fully qualified name of the table to be updated.
  1: required CatalogObjects.TTableName table_name

  // Table-level stats.
  2: optional CatalogObjects.TTableStats table_stats

  // Partition-level stats. Maps from a list of partition-key values
  // to its partition stats. Only set for partitioned Hdfs tables.
  3: optional map<list<string>, CatalogObjects.TPartitionStats> partition_stats

  // Column-level stats. Maps from column name to column stats.
  4: optional map<string, CatalogObjects.TColumnStats> column_stats

  // If true, the computation should produce results for all partitions (partitions with
  // no results from the stats queries will be given an empty entry)
  5: optional bool expect_all_partitions

  // If true, this is the result of an incremental stats computation
  6: optional bool is_incremental
}

// Parameters for ALTER TABLE SET [PARTITION partitionSet] CACHED|UNCACHED
struct TAlterTableSetCachedParams {
  // Details on what operation to perform (cache or uncache)
  1: required THdfsCachingOp cache_op

  // An optional partition set, set if marking the partitions as cached/uncached
  // rather than a table.
  2: optional list<list<CatalogObjects.TPartitionKeyValue>> partition_set
}

// Parameters for ALTER TABLE UNSET [PARTITION ('p1'='a', 'p2'='b'...)]
// TBLPROPERTIES|SERDEPROPERTIES commands.
struct TAlterTableUnSetTblPropertiesParams {
  // The target table property that is being altered.
  1: required CatalogObjects.TTablePropertyType target

  // List of property keys to be unset.
  2: required list<string> property_keys

  // Remove table property only if exists else fail.
  3: required bool if_exists

  // If set, alters the properties of the given partitions, otherwise
  // those of the table.
  4: optional list<list<CatalogObjects.TPartitionKeyValue>> partition_set
}

// Parameters for ALTER TABLE SET PARTITION SPEC partitionSpec.
struct TAlterTableSetPartitionSpecParams {
  1: required CatalogObjects.TIcebergPartitionSpec partition_spec
}

// Parameters for ALTER TABLE EXECUTE EXPIRE_SNAPSHOTS operations.
struct TAlterTableExecuteExpireSnapshotsParams {
  1: required i64 older_than_millis
}

// ALTER TABLE EXECUTE ROLLBACK can be to a date or snapshot id.
enum TRollbackType {
  TIME_ID = 0
  VERSION_ID = 1
}

// Parameters for ALTER TABLE EXECUTE ROLLBACK operations.
struct TAlterTableExecuteRollbackParams {
  // Is rollback to a date or snapshot id.
  1: required TRollbackType kind

  // If kind is TIME_ID this is the date to rollback to.
  2: optional i64 timestamp_millis

  // If kind is VERSION_ID this is the id to rollback to.
  3: optional i64 snapshot_id
}

// Parameters for ALTER TABLE EXECUTE ... operations.
struct TAlterTableExecuteParams {
  // Parameters for ALTER TABLE EXECUTE EXPIRE_SNAPSHOTS
  1: optional TAlterTableExecuteExpireSnapshotsParams expire_snapshots_params

  // Parameters for ALTER TABLE EXECUTE ROLLBACK
  2: optional TAlterTableExecuteRollbackParams execute_rollback_params
}

// Parameters for all ALTER TABLE commands.
struct TAlterTableParams {
  1: required TAlterTableType alter_type

  // Fully qualified name of the target table being altered
  2: required CatalogObjects.TTableName table_name

  // Parameters for ALTER TABLE/VIEW RENAME
  3: optional TAlterTableOrViewRenameParams rename_params

  // Parameters for ALTER TABLE ADD COLUMNS
  4: optional TAlterTableAddColsParams add_cols_params

  // Parameters for ALTER TABLE ADD PARTITION
  5: optional TAlterTableAddPartitionParams add_partition_params

  // Parameters for ALTER TABLE ALTER/CHANGE COLUMN
  6: optional TAlterTableAlterColParams alter_col_params

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

  // Parameters for ALTER TABLE ADD/ADD RANGE PARTITION
  14: optional TAlterTableAddDropRangePartitionParams add_drop_range_partition_params

  // Parameters for ALTER TABLE SET ROW FORMAT
  15: optional TAlterTableSetRowFormatParams set_row_format_params

  // Parameters for ALTER TABLE/VIEW SET OWNER
  16: optional TAlterTableOrViewSetOwnerParams set_owner_params

  // Parameters for ALTER TABLE REPLACE COLUMNS
  17: optional TAlterTableReplaceColsParams replace_cols_params

  // Parameters for ALTER TABLE UNSET TBLPROPERTIES
  18: optional TAlterTableUnSetTblPropertiesParams unset_tbl_properties_params

  // Parameters for ALTER TABLE SET PARTITION SPEC
  19: optional TAlterTableSetPartitionSpecParams set_partition_spec_params

  // Parameters for ALTER TABLE EXECUTE operations
  20: optional TAlterTableExecuteParams set_execute_params
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

  // Optional list of sort columns for the new table. If specified, these will override
  // any such columns of the source table. If unspecified, the destination table will
  // inherit the sort columns of the source table.
  9: optional list<string> sort_columns

  // The server name for security privileges when authorization is enabled.
  // TODO: Need to cleanup:IMPALA-7553
  10: optional string server_name

  // The sorting order used in SORT BY clauses.
  11: required Types.TSortingOrder sorting_order
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

  // If set, the table is automatically partitioned according to this parameter.
  // Kudu-only.
  14: optional list<CatalogObjects.TKuduPartitionParam> partition_by

  // Primary key column names (Kudu-only)
  15: optional list<string> primary_key_column_names;

  // Optional list of sort columns for the new table.
  16: optional list<string> sort_columns

  // The server name for security privileges when authorization is enabled.
  // TODO: Need to cleanup:IMPALA-7553
  17: optional string server_name

  // The sorting order used in SORT BY clauses.
  18: required Types.TSortingOrder sorting_order

  // Primary Keys Structures for Hive API
  19: optional list<hive_metastore.SQLPrimaryKey> primary_keys;

  // Foreign Keys Structure for Hive API
  20: optional list<hive_metastore.SQLForeignKey> foreign_keys;

  // Just one PartitionSpec when create iceberg table
  21: optional CatalogObjects.TIcebergPartitionSpec partition_spec

  // Bucket desc for created bucketed table
  22: optional CatalogObjects.TBucketInfo bucket_info

  // Primary key is unique (Kudu-only)
  23: optional bool is_primary_key_unique
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

  // The server name for security privileges when authorization is enabled.
  // TODO: Need to cleanup:IMPALA-7553
  8: optional string server_name

  // Tblproperties of the view
  9: optional map<string, string> tblproperties
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

  // Sum of file sizes in the table. Only set for tables of type HDFS_TABLE and if
  // is_incremental is false.
  9: optional i64 total_file_bytes
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

// Parameters for GRANT/REVOKE privilege TO/FROM user/role.
struct TGrantRevokePrivParams {
  // List of privileges being granted or revoked. The 'has_grant_opt' for each
  // TPrivilege is inherited from the 'has_grant_opt' of this object.
  1: required list<CatalogObjects.TPrivilege> privileges

  // The principal name this change should apply to.
  2: required string principal_name

  // True if this is a GRANT statement false if this is a REVOKE statement.
  3: required bool is_grant

  // True if WITH GRANT OPTION is set.
  4: required bool has_grant_opt

  // The type of principal
  5: required CatalogObjects.TPrincipalType principal_type

  // The name of the resource owner.
  6: optional string owner_name
}

// Parameters of DROP DATABASE commands
struct TDropDbParams {
  // Name of the database to drop
  1: required string db

  // If true, no error is raised if the target db does not exist
  2: required bool if_exists

  // If true, drops all tables of the database
  3: required bool cascade

  // The server name for security privileges when authorization is enabled.
  // TODO: Need to cleanup:IMPALA-7553
  4: optional string server_name
}

// Parameters of DROP TABLE/VIEW commands
struct TDropTableOrViewParams {
  // Fully qualified name of the table/view to drop
  1: required CatalogObjects.TTableName table_name

  // If true, no error is raised if the target table/view does not exist
  2: required bool if_exists

  // If true, underlying data is purged using -skipTrash
  3: required bool purge

  // Set to true for tables and false for views
  4: optional bool is_table

  // The server name for security privileges when authorization is enabled.
  // TODO: Need to cleanup:IMPALA-7553
  5: optional string server_name
}

// Parameters of TRUNCATE commands
struct TTruncateParams {
  // Fully qualified name of table to truncate
  1: required CatalogObjects.TTableName table_name

  // If true, no error is raised if the target table does not exist
  2: required bool if_exists

  // If false, table stats  will not be deleted as part of
  // the truncate operation
  3: optional bool delete_stats = true
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

// Stores metrics of a catalog table.
struct TTableUsageMetrics {
  1: required CatalogObjects.TTableName table_name

  // Estimated memory usage of that table.
  2: optional i64 memory_estimate_bytes

  // Number of metadata operations performed on the table since it was loaded.
  3: optional i64 num_metadata_operations

  // Number of files in this table. For partitioned table, this includes file counts
  // across all the partitions.
  4: optional i64 num_files

  // The median time spent on table metadata loading
  5: optional i64 median_table_loading_ns

  // The maximum time spent on table metadata loading
  6: optional i64 max_table_loading_ns

  // Number of table loading counts
  7: optional i64 num_table_loading

  // The 75th percentile table loading time
  8: optional i64 p75_loading_time_ns

  // The 95th percentile table loading time
  9: optional i64 p95_loading_time_ns

  // The 99th percentile table loading time
  10: optional i64 p99_loading_time_ns
}

// Response to a GetCatalogUsage request.
struct TGetCatalogUsageResponse {
  // List of the largest (in terms of memory requirements) tables.
  1: required list<TTableUsageMetrics> large_tables

  // List of the most frequently accessed (in terms of number of metadata operations)
  // tables.
  2: required list<TTableUsageMetrics> frequently_accessed_tables

  // List of the tables that have most number of files
  3: required list<TTableUsageMetrics> high_file_count_tables

  // List of the tables that have the longest table metadata loading time
  4: required list<TTableUsageMetrics> long_metadata_loading_tables
}

// Stores the number of in-progress operations aggregated based on the
// catalog operation and/or table name.
struct TOperationUsageCounter {
  // Name of the catalog operation request
  1: required string catalog_op_name

  // Fully qualified table name
  2: required string table_name

  // Number of catalog operation requests in progress
  3: optional i64 op_counter
}

struct TCatalogOpRecord {
  1: required i64 thread_id
  2: required Types.TUniqueId query_id
  3: required string client_ip
  4: required string coordinator_hostname
  5: required string catalog_op_name
  // Name of the target depends on the operation types, e.g. table name for CreateTable,
  // db name for CreateDb, function name for CreateFunction, etc.
  6: required string target_name
  7: required string user
  8: required i64 start_time_ms
  9: required i64 finish_time_ms
  10: required string status
  11: required string details
}

// Response to getOperationUsage request.
struct TGetOperationUsageResponse {
  // List of the number of running catalog operations
  1: required list<TOperationUsageCounter> catalog_op_counters
  // List of the in-flight catalog operations. Sorted by start time.
  2: optional list<TCatalogOpRecord> in_flight_catalog_operations;
  // List of the finished catalog operations. Sorted by finish time descendingly, i.e.
  // the most recently finished operations is shown first.
  3: optional list<TCatalogOpRecord> finished_catalog_operations;
}

struct TColumnName {
  // Name of table/view.
  1: required CatalogObjects.TTableName table_name

  // Name of column.
  2: required string column_name
}

struct TCommentOnParams {
  // Contents of comment to alter. When this field is not set, the comment will be removed.
  1: optional string comment

  //--------------------------------------
  // Only one of these fields can be set.
  //--------------------------------------

  // Name of database to alter.
  2: optional string db

  // Name of table/view to alter.
  3: optional CatalogObjects.TTableName table_name

  // Name of column to alter.
  4: optional TColumnName column_name
}

struct TEventProcessorMetrics {
  // status of event processor
  1: required string status

  // Total number of events received so far
  2: optional i64 events_received

  // Total number of events skipped so far
  3: optional i64 events_skipped

  // Time in sec for the fetching metastore events
  4: optional double events_fetch_duration_mean
  5: optional double events_fetch_duration_p75
  6: optional double events_fetch_duration_p95
  7: optional double events_fetch_duration_p99

  // Duration in sec for fetching the last event batch
  8: optional double last_events_fetch_duration

  // Time in sec for processing a given batch of events
  9: optional double events_process_duration_mean
  10: optional double events_process_duration_p75
  11: optional double events_process_duration_p95
  12: optional double events_process_duration_p99

  // Duration in sec for processing the last event batch
  13: optional double last_events_process_duration

  // Average number of events received in 1 min
  14: optional double events_received_1min_rate

  // Average number of events received in 5 min
  15: optional double events_received_5min_rate

  // Average number of events received in 15 min
  16: optional double events_received_15min_rate

  // Average number events skipped in a polling interval
  17: optional double events_skipped_per_poll_mean

  // Last metastore event id that the catalog server synced to
  18: optional i64 last_synced_event_id

  // Event time of the last synced event
  19: optional i64 last_synced_event_time

  // Latest metastore event id
  20: optional i64 latest_event_id

  // Event time of the latest metastore event
  21: optional i64 latest_event_time
}

struct TCatalogHmsCacheApiMetrics {
  // name of the API
  1: required string api_name

  // number of API requests
  2: optional i64 api_requests

  // p99 response time in milliseconds
  3: optional double p99_response_time_ms

  // p95 response time in milliseconds
  4: optional double p95_response_time_ms

  // Mean response time in milliseconds
  5: optional double response_time_mean_ms

  // Max response time in milliseconds
  6: optional double response_time_max_ms

  // Min response time in milliseconds
  7: optional double response_time_min_ms

  // Average number of API requests in 1 minute
  8: optional double api_requests_1min_rate

  // Average number of API requests in 5 minutes
  9: optional double api_requests_5min_rate

  // Average number of API requests in 15 min
  10: optional double api_requests_15min_rate

  // Cache hit ratio
  11: optional double cache_hit_ratio
}

struct TCatalogdHmsCacheMetrics {

  // API specific Catalogd HMS cache metrics
  1: required list<TCatalogHmsCacheApiMetrics> api_metrics

  // overall cache hit ratio
  2: optional double cache_hit_ratio

  // total number of API requests
  3: optional i64 api_requests

  // Average number of API requests in 1 minute
  4: optional double api_requests_1min_rate

  // Average number of API requests in 5 minutes
  5: optional double api_requests_5min_rate

  // Average number of API requests in 15 min
  6: optional double api_requests_15min_rate
}

// Response to GetCatalogServerMetrics() call.
struct TGetCatalogServerMetricsResponse {
  // Partial fetch RPC queue length.
  1: required i32 catalog_partial_fetch_rpc_queue_len

  // gets the events processor metrics if configured
  2: optional TEventProcessorMetrics event_metrics

  // get the catalogd Hive metastore server metrics, if configured
  3: optional TCatalogdHmsCacheMetrics catalogd_hms_cache_metrics

  // Metrics of table metadata loading
  4: optional i32 catalog_num_file_metadata_loading_threads
  5: optional i32 catalog_num_file_metadata_loading_tasks
  6: optional i32 catalog_num_tables_loading_file_metadata
  7: optional i32 catalog_num_tables_loading_metadata
  8: optional i32 catalog_num_tables_async_loading_metadata
  9: optional i32 catalog_num_tables_waiting_for_async_loading

  // Metrics of the catalog
  10: optional i32 catalog_num_dbs
  11: optional i32 catalog_num_tables
  12: optional i32 catalog_num_functions

  // Metrics of HMS clients
  13: optional i32 catalog_num_hms_clients_idle
  14: optional i32 catalog_num_hms_clients_in_use
}

// Request to copy the generated testcase from a given input path.
struct TCopyTestCaseReq {
  1: required string input_path
}

struct TEventBatchProgressInfo {
  // Number of original HMS events received in the current batch.
  1: required i32 num_hms_events
  // Number of filtered MetastoreEvents generated from the original HMS events.
  2: required i32 num_filtered_events
  3: required i32 current_event_index
  // Number of HMS events represented by this filtered event. For most events this is 1.
  // In case of BatchPartitionEvent this could be more than 1.
  4: required i32 current_event_batch_size
  5: required i64 min_event_id
  6: required i64 min_event_time_s
  7: required i64 max_event_id
  8: required i64 max_event_time_s
  // Timestamp when we start to process the current event batch
  9: required i64 current_batch_start_time_ms
  // Timestamp when we start to process the current event
  10: required i64 current_event_start_time_ms
  11: required i64 last_synced_event_id
  12: required i64 last_synced_event_time_s
  13: required i64 latest_event_id
  14: required i64 latest_event_time_s
  15: optional hive_metastore.NotificationEvent current_event
}

struct TEventProcessorMetricsSummaryResponse {
  // summary view of the events processor which can include status,
  // metrics and other details
  1: required string summary
  // Error messages if the events processor goes into ERROR/NEEDS_INVALIDATE states
  2: optional string error_msg
  3: optional TEventBatchProgressInfo progress
}
