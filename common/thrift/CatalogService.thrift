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
include "Data.thrift"

// CatalogServer service API and related structs.

enum CatalogServiceVersion {
  V1
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

  // The resulting TCatalogObject that was added or modified, if applicable.
  4: optional CatalogObjects.TCatalogObject updated_catalog_object

  // The resulting TCatalogObject that was removed, if applicable.
  5: optional CatalogObjects.TCatalogObject removed_catalog_object
}

// Request for executing a DDL operation (CREATE, ALTER, DROP).
struct TDdlExecRequest {
  1: required CatalogServiceVersion protocol_version = CatalogServiceVersion.V1

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

  // Paramaters for DROP DATABASE
  10: optional JniCatalog.TDropDbParams drop_db_params

  // Parameters for DROP TABLE/VIEW
  11: optional JniCatalog.TDropTableOrViewParams drop_table_or_view_params

  // Parameters for DROP FUNCTION
  12: optional JniCatalog.TDropFunctionParams drop_fn_params

  // Parameters for COMPUTE STATS
  13: optional JniCatalog.TComputeStatsParams compute_stats_params
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
  3: optional Data.TResultSet result_set
}

// Updates the metastore with new partition information and returns a response
// with details on the result of the operation. Used to add partitions after executing
// DML operations, and could potentially be used in the future to update column stats
// after DML operations.
// TODO: Rename this struct to something more descriptive.
struct TUpdateCatalogRequest {
  1: required CatalogServiceVersion protocol_version = CatalogServiceVersion.V1

  // Unqualified name of the table to change
  2: required string target_table;

  // Database that the table belongs to
  3: required string db_name;

  // List of partitions that are new and need to be created. May
  // include the root partition (represented by the empty string).
  4: required set<string> created_partitions;
}

// Response from a TUpdateCatalogRequest
struct TUpdateCatalogResponse {
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

// Request to GetFunctions()
struct TGetFunctionsRequest {
  1: required CatalogServiceVersion protocol_version = CatalogServiceVersion.V1

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

  // A catalog object descriptor: a TCatalogObject with the object name and type fields
  // set.
  2: required CatalogObjects.TCatalogObject object_desc
}

// Response from TGetCatalogObjectRequest
struct TGetCatalogObjectResponse {
  1: required CatalogObjects.TCatalogObject catalog_object
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
}
