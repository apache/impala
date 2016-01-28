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
include "JniCatalog.thrift"
include "Types.thrift"
include "Status.thrift"
include "Results.thrift"

// CatalogServer service API and related structs.

enum CatalogServiceVersion {
  V1
}

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
// the update, and the catalog service ID.
struct TCatalogUpdateResult {
  // The CatalogService service ID this result came from.
  1: required Types.TUniqueId catalog_service_id

  // The Catalog version that will contain this update.
  2: required i64 version

  // The status of the operation, OK if the operation was successful.
  3: required Status.TStatus status

  // The resulting TCatalogObjects that were added or modified, if applicable.
  4: optional list<CatalogObjects.TCatalogObject> updated_catalog_objects

  // The resulting TCatalogObjects that were removed, if applicable.
  5: optional list<CatalogObjects.TCatalogObject> removed_catalog_objects
}

// Request for executing a DDL operation (CREATE, ALTER, DROP).
struct TDdlExecRequest {
  1: required CatalogServiceVersion protocol_version = CatalogServiceVersion.V1

  // Common header included in all CatalogService requests.
  17: optional TCatalogServiceRequestHeader header

  2: required JniCatalog.TDdlType ddl_type

  // Parameters for ALTER TABLE
  3: optional JniCatalog.TAlterTableParams alter_table_params

  // Parameters for ALTER VIEW
  4: optional JniCatalog.TCreateOrAlterViewParams alter_view_params

  // Parameters for CREATE DATABASE
  5: optional JniCatalog.TCreateDbParams create_db_params

  // Parameters for CREATE TABLE
  6: optional JniCatalog.TCreateTableParams create_table_params

  // Parameters for CREATE TABLE LIKE
  7: optional JniCatalog.TCreateTableLikeParams create_table_like_params

  // Parameters for CREATE VIEW
  8: optional JniCatalog.TCreateOrAlterViewParams create_view_params

  // Parameters for CREATE FUNCTION
  9: optional JniCatalog.TCreateFunctionParams create_fn_params

  // Parameters for DROP DATABASE
  10: optional JniCatalog.TDropDbParams drop_db_params

  // Parameters for DROP TABLE/VIEW
  11: optional JniCatalog.TDropTableOrViewParams drop_table_or_view_params

  // Parameters for TRUNCATE TABLE
  21: optional JniCatalog.TTruncateParams truncate_params

  // Parameters for DROP FUNCTION
  12: optional JniCatalog.TDropFunctionParams drop_fn_params

  // Parameters for COMPUTE STATS
  13: optional JniCatalog.TComputeStatsParams compute_stats_params

  // Parameters for CREATE DATA SOURCE
  14: optional JniCatalog.TCreateDataSourceParams create_data_source_params

  // Parameters for DROP DATA SOURCE
  15: optional JniCatalog.TDropDataSourceParams drop_data_source_params

  // Parameters for DROP STATS
  16: optional JniCatalog.TDropStatsParams drop_stats_params

  // Parameters for CREATE/DROP ROLE
  18: optional JniCatalog.TCreateDropRoleParams create_drop_role_params

  // Parameters for GRANT/REVOKE ROLE
  19: optional JniCatalog.TGrantRevokeRoleParams grant_revoke_role_params

  // Parameters for GRANT/REVOKE privilege
  20: optional JniCatalog.TGrantRevokePrivParams grant_revoke_priv_params
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
  // by COMPUTE STATS.
  3: optional Results.TResultSet result_set
}

// Updates the metastore with new partition information and returns a response
// with details on the result of the operation. Used to add partitions after executing
// DML operations, and could potentially be used in the future to update column stats
// after DML operations.
// TODO: Rename this struct to something more descriptive.
struct TUpdateCatalogRequest {
  1: required CatalogServiceVersion protocol_version = CatalogServiceVersion.V1

  // Common header included in all CatalogService requests.
  2: optional TCatalogServiceRequestHeader header

  // Unqualified name of the table to change
  3: required string target_table;

  // Database that the table belongs to
  4: required string db_name;

  // List of partitions that are new and need to be created. May
  // include the root partition (represented by the empty string).
  5: required set<string> created_partitions;
}

// Response from a TUpdateCatalogRequest
struct TUpdateCatalogResponse {
  1: required TCatalogUpdateResult result
}

// Parameters of REFRESH/INVALIDATE METADATA commands
struct TResetMetadataRequest {
  1: required CatalogServiceVersion protocol_version = CatalogServiceVersion.V1

  // Common header included in all CatalogService requests.
  4: optional TCatalogServiceRequestHeader header

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
  1: optional Status.TStatus status
}

// Request to perform a privilege check with the Sentry Service to determine
// if the requesting user is a Sentry Service admin.
struct TSentryAdminCheckRequest {
  1: required CatalogServiceVersion protocol_version = CatalogServiceVersion.V1

  // Common header included in all CatalogService requests.
  2: optional TCatalogServiceRequestHeader header
}

struct TSentryAdminCheckResponse {
  // Contains an error if the user does not have privileges to access the Sentry Service
  // or if the Sentry Service is unavailable. Returns OK if the operation was successful.
  1: optional Status.TStatus status
}

// The CatalogService API
service CatalogService {
  // Executes a DDL request and returns details on the result of the operation.
  TDdlExecResponse ExecDdl(1: TDdlExecRequest req);

  // Gets the catalog object corresponding to the given request.
  TGetCatalogObjectResponse GetCatalogObject(1: TGetCatalogObjectRequest req);

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
}
