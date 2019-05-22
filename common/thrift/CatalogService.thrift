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
include "JniCatalog.thrift"
include "Types.thrift"
include "Status.thrift"
include "Results.thrift"
include "hive_metastore.thrift"

// CatalogServer service API and related structs.

enum CatalogServiceVersion {
  V1
}

// Prefix used on statestore topic entry keys to indicate that the entry
// should be sent to "v1" impalads that receive all of their metadata
// via the topic itself.
const string CATALOG_TOPIC_V1_PREFIX = "1:";

// Prefix used on statestore topic entry keys to indicate that the entry
// should be sent to "v2" impalads that fetch metadata on demand.
const string CATALOG_TOPIC_V2_PREFIX = "2:";

// Common header included in all CatalogService requests.
// TODO: The CatalogServiceVersion/protocol version should be part of the header.
// This would require changes in BDR and break their compatibility story. We should
// coordinate a joint change somewhere down the line.
struct TCatalogServiceRequestHeader {
  // The effective user who submitted this request.
  1: optional string requesting_user
}

// Returns details on the result of an operation that updates the catalog. Information
// returned includes the Status of the operations, the catalog version that will contain
// the update, and the catalog service ID. If SYNC_DDL was set in the query options, it
// also returns the version of the catalog update that this operation must wait for
// before returning the response to the client.
struct TCatalogUpdateResult {
  // The CatalogService service ID this result came from.
  1: required Types.TUniqueId catalog_service_id

  // The Catalog version that will contain this update.
  2: required i64 version

  // The status of the operation, OK if the operation was successful.
  3: required Status.TStatus status

  // True if this is a result of an INVALIDATE METADATA operation.
  4: required bool is_invalidate

  // The resulting TCatalogObjects that were added or modified, if applicable.
  5: optional list<CatalogObjects.TCatalogObject> updated_catalog_objects

  // The resulting TCatalogObjects that were removed, if applicable.
  6: optional list<CatalogObjects.TCatalogObject> removed_catalog_objects
}

// Request for executing a DDL operation (CREATE, ALTER, DROP).
struct TDdlExecRequest {
  1: required CatalogServiceVersion protocol_version = CatalogServiceVersion.V1

  // Common header included in all CatalogService requests.
  2: optional TCatalogServiceRequestHeader header

  3: required JniCatalog.TDdlType ddl_type

  // Parameters for ALTER TABLE
  4: optional JniCatalog.TAlterTableParams alter_table_params

  // Parameters for ALTER VIEW
  5: optional JniCatalog.TCreateOrAlterViewParams alter_view_params

  // Parameters for CREATE DATABASE
  6: optional JniCatalog.TCreateDbParams create_db_params

  // Parameters for CREATE TABLE
  7: optional JniCatalog.TCreateTableParams create_table_params

  // Parameters for CREATE TABLE LIKE
  8: optional JniCatalog.TCreateTableLikeParams create_table_like_params

  // Parameters for CREATE VIEW
  9: optional JniCatalog.TCreateOrAlterViewParams create_view_params

  // Parameters for CREATE FUNCTION
  10: optional JniCatalog.TCreateFunctionParams create_fn_params

  // Parameters for DROP DATABASE
  11: optional JniCatalog.TDropDbParams drop_db_params

  // Parameters for DROP TABLE/VIEW
  12: optional JniCatalog.TDropTableOrViewParams drop_table_or_view_params

  // Parameters for TRUNCATE TABLE
  13: optional JniCatalog.TTruncateParams truncate_params

  // Parameters for DROP FUNCTION
  14: optional JniCatalog.TDropFunctionParams drop_fn_params

  // Parameters for COMPUTE STATS
  15: optional JniCatalog.TComputeStatsParams compute_stats_params

  // Parameters for CREATE DATA SOURCE
  16: optional JniCatalog.TCreateDataSourceParams create_data_source_params

  // Parameters for DROP DATA SOURCE
  17: optional JniCatalog.TDropDataSourceParams drop_data_source_params

  // Parameters for DROP STATS
  18: optional JniCatalog.TDropStatsParams drop_stats_params

  // Parameters for CREATE/DROP ROLE
  19: optional JniCatalog.TCreateDropRoleParams create_drop_role_params

  // Parameters for GRANT/REVOKE ROLE
  20: optional JniCatalog.TGrantRevokeRoleParams grant_revoke_role_params

  // Parameters for GRANT/REVOKE privilege
  21: optional JniCatalog.TGrantRevokePrivParams grant_revoke_priv_params

  // True if SYNC_DDL is set in query options
  22: required bool sync_ddl

  // Parameters for COMMENT ON
  23: optional JniCatalog.TCommentOnParams comment_on_params

  // Parameters for ALTER DATABASE
  24: optional JniCatalog.TAlterDbParams alter_db_params

  // Parameters for replaying an exported testcase.
  25: optional JniCatalog.TCopyTestCaseReq copy_test_case_params
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

  // Result of DDL operation to be returned to the client. Currently only set
  // by COMPUTE STATS and ALTER TABLE.
  3: optional Results.TResultSet result_set

  // The table/view name in HMS. Set only for CREATE TABLE, CREATE TABLE AS SELECT,
  // CREATE TABLE LIKE, and CREATE VIEW statements.
  4: optional string table_name

  // The table/view create time stored in HMS. Set only for CREATE TABLE,
  // CREATE TABLE AS SELECT, CREATE TABLE LIKE, and CREATE VIEW statements.
  5: optional i64 table_create_time
}

// Updates the metastore with new partition information and returns a response
// with details on the result of the operation. Used to add partitions after executing
// DML operations, and could potentially be used in the future to update column stats
// after DML operations.
// TODO: Rename this struct to something more descriptive.
struct TUpdateCatalogRequest {
  1: required CatalogServiceVersion protocol_version = CatalogServiceVersion.V1

  // True if SYNC_DDL is set in query options.
  2: required bool sync_ddl

  // Common header included in all CatalogService requests.
  3: optional TCatalogServiceRequestHeader header

  // Unqualified name of the table to change
  4: required string target_table;

  // Database that the table belongs to
  5: required string db_name;

  // List of partitions that are new and need to be created. May
  // include the root partition (represented by the empty string).
  6: required set<string> created_partitions;

  // True if the update corresponds to an "insert overwrite" operation
  7: required bool is_overwrite;
}

// Response from a TUpdateCatalogRequest
struct TUpdateCatalogResponse {
  1: required TCatalogUpdateResult result
}

// Parameters of REFRESH/INVALIDATE METADATA commands
struct TResetMetadataRequest {
  1: required CatalogServiceVersion protocol_version = CatalogServiceVersion.V1

  // Common header included in all CatalogService requests.
  2: optional TCatalogServiceRequestHeader header

  // If true, refresh. Otherwise, invalidate metadata
  3: required bool is_refresh

  // Fully qualified name of the table to refresh or invalidate; not set if invalidating
  // the entire catalog
  4: optional CatalogObjects.TTableName table_name

  // If set, refreshes the specified partition, otherwise
  // refreshes the whole table
  5: optional list<CatalogObjects.TPartitionKeyValue> partition_spec

  // If set, refreshes functions in the specified database.
  6: optional string db_name

  // True if SYNC_DDL is set in query options
  7: required bool sync_ddl

  // If set, refreshes authorization metadata.
  8: optional bool authorization
}

// Response from TResetMetadataRequest
struct TResetMetadataResponse {
  1: required TCatalogUpdateResult result
}

// Request to GetFunctions()
struct TGetFunctionsRequest {
  1: required CatalogServiceVersion protocol_version = CatalogServiceVersion.V1

  // Common header included in all CatalogService requests.
  3: optional TCatalogServiceRequestHeader header

  // The parent database name.
  2: optional string db_name;
}

// Response a call to GetFunctions()
struct TGetFunctionsResponse {
  // The status of the operation, OK if the operation was successful.
  1: optional Status.TStatus status

  // List of functions returned to the caller. Functions are not returned in a
  // defined order.
  2: optional list<Types.TFunction> functions;
}

// Selector for partial information about Catalog-scoped objects
// (i.e. those that are not within a particular database or table).
struct TCatalogInfoSelector {
  1: bool want_db_names
  // TODO(todd): add objects like DataSources, etc.
}

// Returned info from a catalog request which selected items in
// TCatalogInfoSelector.
struct TPartialCatalogInfo {
  1: list<string> db_names
}

// Selector for partial information about a Table.
struct TTableInfoSelector {
  // The response should include the HMS table struct.
  1: bool want_hms_table

  // If set, the response should include information about the given list of
  // partitions. If this is unset, information about all partitions will be
  // returned, so long as at least one of the following 'want_partition_*'
  // flags is specified.
  //
  // If a partition ID is passed, but that partition does not exist in the
  // table, then an exception will be thrown. It is assumed that the partition
  // IDs passed here are a result of a prior successful call to fetch the partition
  // list of this table.
  //
  // NOTE: "unset" and "set to empty" are different -- "set to empty" causes
  // no partitions to be returned, whereas "unset" causes all partitions to be
  // returned, so long as one of the following 'want_partition_*' is set.
  2: optional list<i64> partition_ids

  // ... each such partition should include its name.
  3: bool want_partition_names

  // ... each such partition should include metadata (location, etc).
  4: bool want_partition_metadata

  // ... each such partition should include its file info
  5: bool want_partition_files

  // List of columns to fetch stats for.
  6: optional list<string> want_stats_for_column_names

  // ... each partition should include the partition stats serialized as a byte[]
  // and that is deflate-compressed.
  7: bool want_partition_stats
}

// Returned information about a particular partition.
struct TPartialPartitionInfo {
  1: required i64 id

  // Set if 'want_partition_names' was set in TTableInfoSelector.
  2: optional string name

  // Set if 'want_partition_metadata' was set in TTableInfoSelector.
  3: optional hive_metastore.Partition hms_partition

  // Set if 'want_partition_files' was set in TTableInfoSelector.
  4: optional list<CatalogObjects.THdfsFileDesc> file_descriptors

  // Deflate-compressed byte[] representation of TPartitionStats for this partition.
  // Set if 'want_partition_stats' was set in TTableInfoSelector. Not set if the
  // partition does not have stats.
  5: optional binary partition_stats

  // Set to true if the partition contains intermediate column stats computed via
  // incremental statistics. Set when 'want_partition_metadata' is true in
  // TTableInfoSelector. Incremental stats data can be fetched by setting
  // 'want_partition_stats' in TTableInfoSelector.
  6: optional bool has_incremental_stats
}

// Returned information about a Table, as selected by TTableInfoSelector.
struct TPartialTableInfo {
  1: optional hive_metastore.Table hms_table

  // The partition metadata for the requested partitions.
  //
  // If explicit partitions were passed, then it is guaranteed that this list
  // is the same size and the same order as the requested list of IDs.
  //
  // See TPartialPartitionInfo for details on which fields will be set based
  // on the caller-provided selector.
  2: optional list<TPartialPartitionInfo> partitions

  3: optional list<hive_metastore.ColumnStatisticsObj> column_stats

  // Each TNetworkAddress is a datanode which contains blocks of a file in the table.
  // Used so that each THdfsFileBlock can just reference an index in this list rather
  // than duplicate the list of network address, which helps reduce memory usage.
  // Only used when partition files are fetched.
  7: optional list<Types.TNetworkAddress> network_addresses
}

// Selector for partial information about a Database.
struct TDbInfoSelector {
  // The response should include the HMS Database object.
  1: bool want_hms_database

  // The response should include the list of table names in the DB.
  2: bool want_table_names

  // The response should include the list of function names in the DB.
  3: bool want_function_names
}

// Returned information about a Database, as selected by TDbInfoSelector.
struct TPartialDbInfo {
  1: optional hive_metastore.Database hms_database
  2: optional list<string> table_names
  3: optional list<string> function_names
}

// RPC request for GetPartialCatalogObject.
struct TGetPartialCatalogObjectRequest {
  1: required CatalogServiceVersion protocol_version = CatalogServiceVersion.V1

  // A catalog object descriptor: a TCatalogObject with the object name and type fields
  // set. This may be a TABLE, DB, CATALOG, or FUNCTION. The selectors below can
  // further restrict what information should be returned.
  2: required CatalogObjects.TCatalogObject object_desc

  3: optional TTableInfoSelector table_info_selector
  4: optional TDbInfoSelector db_info_selector
  5: optional TCatalogInfoSelector catalog_info_selector
}

enum CatalogLookupStatus {
  OK,
  DB_NOT_FOUND,
  TABLE_NOT_FOUND,
  TABLE_NOT_LOADED,
  FUNCTION_NOT_FOUND,
  // Partial fetch RPCs currently look up partitions by IDs instead of names. These IDs
  // change over the lifetime of a table with queries like invalidate metadata. In such
  // cases this lookup status is set and the caller can retry the fetch.
  // TODO: Fix partition lookup logic to not do it with IDs.
  PARTITION_NOT_FOUND
}

// RPC response for GetPartialCatalogObject.
struct TGetPartialCatalogObjectResponse {
  // The status of the operation, OK if the operation was successful.
  // Unset indicates "OK".
  1: optional Status.TStatus status

  // Catalog-specific error codes (eg if the object no longer exists).
  2: optional CatalogLookupStatus lookup_status = CatalogLookupStatus.OK

  3: optional i64 object_version_number
  4: optional TPartialTableInfo table_info
  5: optional TPartialDbInfo db_info
  6: optional TPartialCatalogInfo catalog_info

  // Functions are small enough that we return them wholesale.
  7: optional list<Types.TFunction> functions
}


// Request the complete metadata for a given catalog object. May trigger a metadata load
// if the object is not already in the catalog cache.
struct TGetCatalogObjectRequest {
  1: required CatalogServiceVersion protocol_version = CatalogServiceVersion.V1

  // Common header included in all CatalogService requests.
  3: optional TCatalogServiceRequestHeader header

  // A catalog object descriptor: a TCatalogObject with the object name and type fields
  // set.
  2: required CatalogObjects.TCatalogObject object_desc
}

// Response from TGetCatalogObjectRequest
struct TGetCatalogObjectResponse {
  1: required CatalogObjects.TCatalogObject catalog_object
}

// Request the partition statistics for the specified table.
struct TGetPartitionStatsRequest {
  1: required CatalogServiceVersion protocol_version = CatalogServiceVersion.V1
  2: required CatalogObjects.TTableName table_name
}

// Response for requesting partition statistics. All partition statistics
// are returned. If a partition does not have statistics, it is not returned.
// Partitions are identified by name, consisting of partition column name/value pairs.
// The returned statistics are deflate-compressed bytes that represent
// CatalogObject.TPartitionStats when decompressed.
// An OK or null status means that the call succeeded.
// If there was an error, an error status is returned and partition_stats
// is left unset.
struct TGetPartitionStatsResponse {
  1: optional Status.TStatus status
  2: optional map<string, binary> partition_stats
}

// Instructs the Catalog Server to prioritizing loading of metadata for the specified
// catalog objects. Currently only used for controlling the priority of loading
// tables/views since Db/Function metadata is loaded on startup.
struct TPrioritizeLoadRequest {
  1: required CatalogServiceVersion protocol_version = CatalogServiceVersion.V1

  // Common header included in all CatalogService requests.
  2: optional TCatalogServiceRequestHeader header

  // A list of catalog objects descriptors for which to prioritize loading. A catalog
  // object descriptor is a TCatalogObject with only the object name and type fields set.
  3: required list<CatalogObjects.TCatalogObject> object_descs
}

struct TPrioritizeLoadResponse {
  // The status of the operation, OK if the operation was successful.
  1: required Status.TStatus status
}

// Request to perform a privilege check with the Sentry Service to determine
// if the requesting user is a Sentry Service admin.
struct TSentryAdminCheckRequest {
  1: required CatalogServiceVersion protocol_version = CatalogServiceVersion.V1

  // Common header included in all CatalogService requests.
  2: optional TCatalogServiceRequestHeader header
}

struct TSentryAdminCheckResponse {
  // Returns OK if the operation was successful.
  1: optional Status.TStatus status

  // Returns true if the user is a Sentry admin user.
  2: required bool is_admin
}

struct TTableUsage {
  1: required CatalogObjects.TTableName table_name
  // count of usages since the last report
  2: required i32 num_usages
}

struct TUpdateTableUsageRequest {
  1: required list<TTableUsage> usages
}

struct TUpdateTableUsageResponse {
  // The operation may fail if the catalogd is in a bad state or if there is a bug.
  1: optional Status.TStatus status
}

// The CatalogService API
service CatalogService {
  // Executes a DDL request and returns details on the result of the operation.
  TDdlExecResponse ExecDdl(1: TDdlExecRequest req);

  // Gets the catalog object corresponding to the given request.
  TGetCatalogObjectResponse GetCatalogObject(1: TGetCatalogObjectRequest req);

  // Gets the statistics that are associated with table partitions.
  TGetPartitionStatsResponse GetPartitionStats(1: TGetPartitionStatsRequest req);

  // Resets the Catalog metadata. Used to explicitly trigger reloading of the Hive
  // Metastore metadata and/or HDFS block location metadata.
  TResetMetadataResponse ResetMetadata(1: TResetMetadataRequest req);

  // Updates the metastore with new partition information and returns a response
  // with details on the result of the operation.
  TUpdateCatalogResponse UpdateCatalog(1: TUpdateCatalogRequest req);

  // Gets all user defined functions (aggregate and scalar) in the catalog matching
  // the parameters of TGetFunctionsRequest.
  TGetFunctionsResponse GetFunctions(1: TGetFunctionsRequest req);

  // Prioritize the loading of metadata for the CatalogObjects specified in the
  // TPrioritizeLoadRequest.
  TPrioritizeLoadResponse PrioritizeLoad(1: TPrioritizeLoadRequest req);

  // Performs a check with the Sentry Service to determine if the requesting user
  // is configured as an admin on the Sentry Service. This API may be removed in
  // the future and external clients should not rely on using it.
  // TODO: When Sentry Service has a better mechanism to perform these changes this API
  // should be deprecated.
  TSentryAdminCheckResponse SentryAdminCheck(1: TSentryAdminCheckRequest req);

  // Fetch partial information about some object in the catalog.
  TGetPartialCatalogObjectResponse GetPartialCatalogObject(
      1: TGetPartialCatalogObjectRequest req);

  // Update recently used tables and their usage counts in an impalad since the last
  // report.
  TUpdateTableUsageResponse UpdateTableUsage(1: TUpdateTableUsageRequest req);
}
