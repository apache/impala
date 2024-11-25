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

/// Implements the CatalogServer::InitWorkloadManagement() function to create/upgrade all
/// workload mangement database tables. Catalog methods such as ResetMetadata and ExecDdl
/// are directly called to manipulate the database objects into the desired state.

#include "catalog/catalog-server.h"

#include <functional>
#include <mutex>
#include <optional>
#include <utility>

#include <boost/algorithm/string.hpp>
#include <boost/algorithm/string/case_conv.hpp>
#include <boost/algorithm/string/predicate.hpp>
#include <boost/algorithm/string/trim.hpp>
#include <gflags/gflags.h>
#include <glog/logging.h>
#include <gutil/strings/split.h>
#include <gutil/strings/strcat.h>

#include "gen-cpp/CatalogObjects_constants.h"
#include "gen-cpp/CatalogObjects_types.h"
#include "gen-cpp/CatalogService.h"
#include "gen-cpp/CatalogService_types.h"
#include "gen-cpp/ErrorCodes_types.h"
#include "gen-cpp/SystemTables_types.h"
#include "gen-cpp/Types_types.h"

#include "common/status.h"
#include "kudu/util/version_util.h"
#include "util/debug-util.h"
#include "util/network-util.h"
#include "util/time.h"
#include "util/version-util.h"
#include "workload_mgmt/workload-management.h"

DECLARE_string(debug_actions);
DECLARE_string(hostname);
DECLARE_string(query_log_table_location);
DECLARE_string(query_log_table_props);
DECLARE_string(workload_mgmt_drop_tables);
DECLARE_string(workload_mgmt_maintenance_user);

using namespace apache::thrift;
using namespace impala::workloadmgmt;
using namespace std;

using boost::is_any_of;
using boost::split;
using boost::algorithm::starts_with;
using boost::algorithm::to_lower_copy;
using boost::algorithm::trim_copy;
using kudu::ParseVersion;
using kudu::Version;

namespace impala {

/// Name of the table property where the table schema version is stored.
const string WM_SCHEMA_VER_PROP_NAME_1_0_0 = "schema_version";
const string WM_SCHEMA_VER_PROP_NAME = "wm_schema_version";

/// Returns the fully qualified table name in format "db.tbl_name".
static const string _fullTableName(const string& db, const string& tbl_name) {
  return StrCat(db, ".", tbl_name);
} // function _fullTableName

/// Builds a catalog service request header that can be used in any catalog operation.
static TCatalogServiceRequestHeader _getHeader(const string& ip_addr) {
  TCatalogServiceRequestHeader req_header;
  req_header.__set_client_ip(ip_addr);
  req_header.__set_requesting_user(FLAGS_workload_mgmt_maintenance_user);
  req_header.__set_want_minimal_response(true);

  return req_header;
} // function _getHeader

/// Equivalent to: DROP TABLE {{db}}.{{tbl_name}} IF NOT EXISTS PURGE.
static Status _dropTable(CatalogServiceIf* svc, const string& ip_addr, const string& db,
    const string& tbl_name) {
  TDdlExecResponse resp;
  TDdlExecRequest req;
  Status stat;
  TDropTableOrViewParams drop_tbl;
  TTableName t_tbl_name;
  TDdlQueryOptions t_query_opts;

  t_query_opts.__set_sync_ddl(true);

  t_tbl_name.__set_db_name(db);
  t_tbl_name.__set_table_name(tbl_name);

  drop_tbl.__set_if_exists(true);
  drop_tbl.__set_is_table(true);
  drop_tbl.__set_purge(true);
  drop_tbl.__set_table_name(t_tbl_name);

  req.__set_header(_getHeader(ip_addr));
  req.__set_ddl_type(TDdlType::type::DROP_TABLE);
  req.__set_query_options(t_query_opts);
  req.__set_drop_table_or_view_params(drop_tbl);

  svc->ExecDdl(resp, req);

  stat = Status(resp.result.status);
  if (!stat.ok()) {
    stat.AddDetail(StrCat("could not drop table '", _fullTableName(db, tbl_name), "'"));
  }

  return stat;
} // function _dropTable

/// Equivalent to: CREATE DATABASE IF NOT EXISTS {{db}}.
static Status _setupDb(CatalogServiceIf* svc, const string& ip_addr, const string& db) {
  TDdlExecResponse resp;
  TDdlExecRequest req;
  TCreateDbParams create_db;

  create_db.__set_comment("System database for Impala introspection");
  create_db.__set_db(db);
  create_db.__set_if_not_exists(true);
  create_db.__set_owner(FLAGS_workload_mgmt_maintenance_user);

  TDdlQueryOptions t_query_opts;
  t_query_opts.__set_sync_ddl(true);

  req.__set_header(_getHeader(ip_addr));
  req.__set_ddl_type(TDdlType::type::CREATE_DATABASE);
  req.__set_create_db_params(create_db);
  req.__set_query_options(t_query_opts);

  svc->ExecDdl(resp, req);
  return Status(resp.result.status);
} // function _setupDb

/// Loops through all workload management columns definied in FIELD_DEFINITIONS and builds
/// a TColumn object for each column if the provided function returns true for that column
/// returning a vector of all columns that were included.
static vector<TColumn> _buildCols(
    const function<bool(const FieldDefinition& item)>& shouldIncludeCol) {
  vector<TColumn> cols;

  for (const auto& field : FIELD_DEFINITIONS) {
    if (shouldIncludeCol(field.second)) {
      TScalarType t_scalar_type;
      TTypeNode t_type_node;
      TColumnType t_col_type;
      TColumn t_col;

      t_scalar_type.__set_type(field.second.db_column_type);
      t_scalar_type.__set_precision(field.second.precision);
      t_scalar_type.__set_scale(field.second.scale);

      t_type_node.__set_type(TTypeNodeType::SCALAR);
      t_type_node.__set_scalar_type(t_scalar_type);

      t_col_type.__set_types({t_type_node});

      t_col.__set_columnName(to_lower_copy(to_string(field.first)));
      t_col.__set_columnType(t_col_type);
      t_col.__set_is_nullable(true);

      cols.push_back(t_col);
    }
  }

  DCHECK(cols.size() > 0);

  return cols;
} // function _buildCols

/// Sets up the query table by generating and executing the necessary DML statements.
/// System tables are external. The columns on the table will be all columns with a
/// schema version less than or equal to the specified target_version. Non-system tables
/// are partitioned on cluster_id and hour(start_time_utc).
/// Equivalent to: CREATE [EXTERNAL] TABLE IF NOT EXISTS {{table}} (...)
///                PARTITIONED BY SPEC (cluster_id, HOUR(start_time_utc))
///                STORED AS ICEBERG
static Status _createTable(CatalogServiceIf* svc, const string& ip_addr,
    const string& table_name, const Version& target_version, bool is_system_table) {
  LOG(INFO) << "Creating workload management table '" << table_name
            << "' on schema version '" << target_version.ToString() << "'";

  TDdlExecResponse resp;
  TDdlExecRequest req;
  TCreateTableParams t_create_tbl;

  // General table properties.
  t_create_tbl.__set_if_not_exists(true);
  t_create_tbl.__set_owner(FLAGS_workload_mgmt_maintenance_user);
  map<string, string> table_props = {
      make_pair(WM_SCHEMA_VER_PROP_NAME_1_0_0, VERSION_1_0_0.ToString()),
      make_pair(WM_SCHEMA_VER_PROP_NAME, target_version.ToString()),
      make_pair("format-version", "2"), make_pair("OBJCAPABILITIES", "EXTREAD,EXTWRITE")};

  // User provided properties.
  if (!FLAGS_query_log_table_props.empty()) {
    for (const auto& prop : strings::Split(FLAGS_query_log_table_props, ",")) {
      vector<string> prop_parts = strings::Split(prop, "=");
      if (prop_parts.size() != 2) {
        return Status(StrCat("property '", prop,
            "' is not in the expected 'key=value' format"));
      }
      table_props.insert(make_pair(trim_copy(prop_parts[0]), trim_copy(prop_parts[1])));
    }
  }

  // Table name and database.
  TTableName t_table_name;
  t_table_name.__set_db_name(WM_DB);
  t_table_name.__set_table_name(table_name);
  t_create_tbl.__set_table_name(t_table_name);

  // Table columns.
  t_create_tbl.__set_columns(_buildCols([&target_version](const FieldDefinition& f) {
    return f.schema_version <= target_version;
  }));

  if (is_system_table) {
    // Table properties unique to system tables.
    t_create_tbl.__set_is_external(true);
    table_props.insert(
        make_pair(g_CatalogObjects_constants.TBL_PROP_SYSTEM_TABLE, "true"));
  } else {
    // Table properties unique to non-system tables.
    t_create_tbl.__set_is_external(false);
    t_create_tbl.__set_file_format(THdfsFileFormat::type::ICEBERG);
    table_props.insert(make_pair("engine.hive.enabled", "true"));
    table_props.insert(make_pair("write.delete.mode", "merge-on-read"));
    table_props.insert(make_pair("write.format.default", "parquet"));
    table_props.insert(make_pair("write.merge.mode", "merge-on-read"));
    table_props.insert(make_pair("write.parquet.compression-codec", "snappy"));
    table_props.insert(make_pair("write.update.mode", "merge-on-read"));

    // Table partitioning.
    TIcebergPartitionSpec t_partion_spec;
    TIcebergPartitionField t_partition_identity;
    TIcebergPartitionField t_partition_start_hour;
    TIcebergPartitionTransform t_transform_identity;
    TIcebergPartitionTransform t_transform_hour;

    t_transform_identity.__set_transform_type(
        TIcebergPartitionTransformType::type::IDENTITY);
    t_partition_identity.__set_field_name(
        to_lower_copy(to_string(TQueryTableColumn::CLUSTER_ID)));
    t_partition_identity.__set_orig_field_name(
        to_lower_copy(to_string(TQueryTableColumn::CLUSTER_ID)));
    t_partition_identity.__set_transform(t_transform_identity);

    t_transform_hour.__set_transform_type(TIcebergPartitionTransformType::type::HOUR);
    t_partition_start_hour.__set_field_name(
        to_lower_copy(to_string(TQueryTableColumn::START_TIME_UTC)));
    t_partition_start_hour.__set_orig_field_name(
        to_lower_copy(to_string(TQueryTableColumn::START_TIME_UTC)));
    t_partition_start_hour.__set_transform(t_transform_hour);

    t_partion_spec.__set_partition_fields({t_partition_identity, t_partition_start_hour});
    t_create_tbl.__set_partition_spec(t_partion_spec);

    // Table location (if startup flag specified)
    if (!FLAGS_query_log_table_location.empty()) {
      t_create_tbl.__set_location(FLAGS_query_log_table_location);
    }
  }

  t_create_tbl.__set_table_properties(table_props);

  TDdlQueryOptions query_opts;
  query_opts.__set_sync_ddl(true);

  req.__set_query_options(query_opts);
  req.__set_create_table_params(t_create_tbl);
  req.__set_header(_getHeader(ip_addr));
  req.__set_ddl_type(TDdlType::type::CREATE_TABLE);

  svc->ExecDdl(resp, req);
  return Status(resp.result.status);
} // function _createTable

/// Upgrades a table by running alter table statements. Columns with a schema version
/// greater than the current table schema version and less than or equal to the target
/// version will be added to the table.
/// Equivalent to: ALTER TABLE {{table}} ADD IF NOT EXISTS COLUMNS (...)
///                ALTER TABLE {{table}} SET TBLPROPERTIES ('wm_schema_version'='X.X.X')
static Status _upgradeTable(CatalogServiceIf* svc, const string& ip_addr,
    const string& table_name, const Version& current_ver, const Version& target_ver) {
  DCHECK_NE(current_ver, target_ver);

  // Add new columns.
  TDdlExecResponse resp;
  TDdlExecRequest req;
  TAlterTableParams t_alter_params;
  TAlterTableAddColsParams t_add_cols;
  TTableName t_table_name;

  t_table_name.__set_db_name(WM_DB);
  t_table_name.__set_table_name(table_name);

  t_add_cols.__set_if_not_exists(true);
  t_add_cols.__set_columns(
      _buildCols([&current_ver, &target_ver](const FieldDefinition& f) {
        return f.schema_version > current_ver && f.schema_version <= target_ver;
      }));

  t_alter_params.__set_add_cols_params(t_add_cols);
  t_alter_params.__set_alter_type(TAlterTableType::type::ADD_COLUMNS);
  t_alter_params.__set_table_name(t_table_name);

  TDdlQueryOptions query_opts;
  query_opts.__set_sync_ddl(true);

  req.__set_query_options(query_opts);
  req.__set_header(_getHeader(ip_addr));
  req.__set_ddl_type(TDdlType::type::ALTER_TABLE);
  req.__set_alter_table_params(t_alter_params);

  svc->ExecDdl(resp, req);
  if (resp.result.status.status_code != TErrorCode::type::OK) {
    return Status(resp.result.status);
  }

  // Update table schema version.
  TAlterTableSetTblPropertiesParams t_set_props_params;
  TAlterTableParams t_alter_schema_ver;

  t_set_props_params.__set_properties(
      {make_pair(WM_SCHEMA_VER_PROP_NAME, target_ver.ToString())});
  t_set_props_params.__set_target(TTablePropertyType::type::TBL_PROPERTY);

  t_alter_schema_ver.__set_alter_type(TAlterTableType::type::SET_TBL_PROPERTIES);
  t_alter_schema_ver.__set_set_tbl_properties_params(t_set_props_params);
  t_alter_schema_ver.__set_table_name(t_table_name);

  resp = TDdlExecResponse();
  req = TDdlExecRequest();

  req.__set_header(_getHeader(ip_addr));
  req.__set_ddl_type(TDdlType::type::ALTER_TABLE);
  req.__set_query_options(query_opts);
  req.__set_alter_table_params(t_alter_schema_ver);

  svc->ExecDdl(resp, req);
  return Status(resp.result.status);
} // function _upgradeTable

/// Retrieves the schema version of the specified table by reading its table properties.
static Status _getTableSchemaVersion(CatalogServiceIf* svc, const string& ip_addr,
    const string& db, const string& tbl, Version* table_version) {
  DCHECK_NE(nullptr, table_version);

  // Reset the table metadata to get the full metadata so its catalog object is fully
  // populated enabling its properties to be read.
  TResetMetadataRequest reset_req;
  TResetMetadataResponse reset_resp;
  TTableName table_name;
  table_name.__set_db_name(db);
  table_name.__set_table_name(tbl);
  reset_req.__set_table_name(table_name);
  reset_req.__set_is_refresh(true);
  reset_req.__set_header(_getHeader(ip_addr));
  reset_req.__set_sync_ddl(true);

  svc->ResetMetadata(reset_resp, reset_req);
  if (reset_resp.result.status.status_code != TErrorCode::type::OK) {
    Status reset_stat = Status(reset_resp.result.status);
    if (reset_stat.code() == TErrorCode::type::GENERAL
        && starts_with(reset_stat.msg().msg(), "TableNotFoundException")) {
      *table_version = NO_TABLE_EXISTS;
      return Status::OK();
    } else {
      return reset_stat;
    }
  }

  // Read the table's catalog object to retrieve its table properties.
  TGetCatalogObjectResponse o_resp;
  TGetCatalogObjectRequest o_req;
  TCatalogObject catalog_obj;
  TTable tbl_obj;

  tbl_obj.__set_db_name(db);
  tbl_obj.__set_tbl_name(tbl);

  catalog_obj.__set_type(TCatalogObjectType::type::TABLE);
  catalog_obj.__set_table(tbl_obj);

  o_req.__set_header(_getHeader(ip_addr));
  o_req.__set_object_desc(catalog_obj);

  svc->GetCatalogObject(o_resp, o_req);
  if (o_resp.status.status_code != TErrorCode::type::OK) {
    return Status(o_resp.status);
  }

  // Determine the actual table schema version. The "wm_schema_version" table property
  // takes precendence over the former "schema_version" table property.
  optional<string> schema_ver_value;
  optional<string> wm_schema_ver_value;

  // To maintain backwards compatibility, a new table schema version property had to be
  // added, otherwise Impala daemons running older code will error on startup when
  // workload management is enabled.
  for (const auto& iter : o_resp.catalog_object.table.metastore_table.parameters) {
    if (iter.first == WM_SCHEMA_VER_PROP_NAME_1_0_0) {
      schema_ver_value = iter.second;
    } else if (iter.first == WM_SCHEMA_VER_PROP_NAME) {
      wm_schema_ver_value = iter.second;

      // Since the "wm_schema_version" table property takes precendence, there is no need
      // to continue after it is encountered.
      break;
    }
  }

  // Property "wm_schema_version" takes precendence over the former "schema_version"
  // property if both exist.
  if (wm_schema_ver_value.has_value()) {
    Version v;
    if (!ParseVersion(wm_schema_ver_value.value(), &v).ok()) {
      return Status(StrCat("could not parse version string '",
          wm_schema_ver_value.value(), "' found on the '", WM_SCHEMA_VER_PROP_NAME,
          "' property of table '", db, ".", tbl, "'"));
    }
    *table_version = move(v);
    return Status::OK();
  } else if (schema_ver_value.has_value()) {
    Version v;
    if (!ParseVersion(schema_ver_value.value(), &v).ok()) {
      return Status(StrCat("could not parse version string '", schema_ver_value.value(),
          "' found on the '", WM_SCHEMA_VER_PROP_NAME_1_0_0, "' property of table '", db,
          ".", tbl, "'"));
    }
    *table_version = move(v);
    return Status::OK();
  }

  return Status(StrCat("Neither property '", WM_SCHEMA_VER_PROP_NAME_1_0_0,
      "' nor property '", WM_SCHEMA_VER_PROP_NAME, "' exists on table '", db, ".", tbl,
      "'"));
} // function _getTableSchemaVersion

/// Manages the schema for a workload management table. If the table does not exist, it is
/// created. If it does exist, but is not on the schema version specified via the command
/// line flag, it is altered to bring it up to the new version.
static Status _tableSchemaManagement(CatalogServiceIf* svc, const string& ip_addr,
    const string& table_name, const Version& target_schema_version,
    const bool is_system_table) {
  Version parsed_actual_schema_version;

  // Create and/or update the table if needed.
  RETURN_IF_ERROR(_getTableSchemaVersion(
      svc, ip_addr, WM_DB, table_name, &parsed_actual_schema_version));

  const string full_table_name = _fullTableName(WM_DB, table_name);

  if (parsed_actual_schema_version == target_schema_version) {
    LOG(INFO) << "Target schema version '" << target_schema_version.ToString()
              << "' matches actual schema version '"
              << parsed_actual_schema_version.ToString() << "' for the '"
              << full_table_name << "' table";
  } else if (parsed_actual_schema_version == NO_TABLE_EXISTS) {
    LOG(INFO) << "Creating table '" << full_table_name << "' on schema version '"
              << target_schema_version.ToString() << "'";
    RETURN_IF_ERROR(
        _createTable(svc, ip_addr, table_name, target_schema_version, is_system_table));
  } else if (parsed_actual_schema_version > target_schema_version) {
    // Actual schema version is greater than the target schema version.
    LOG(WARNING) << "Target schema version '" << target_schema_version.ToString()
                 << "' of the '" << full_table_name
                 << "' table is lower than the actual schema "
                 << "version '" << parsed_actual_schema_version.ToString() << "'";
  } else {
    // Target schema version is greater than the actual schema version. Upgrade the table.
    LOG(INFO) << "Workload management table '" << full_table_name << "' is at version '"
              << parsed_actual_schema_version.ToString() << "' and will be upgraded";
    RETURN_IF_ERROR(_upgradeTable(
        svc, ip_addr, table_name, parsed_actual_schema_version, target_schema_version));
  }

  return Status::OK();
} // function _logTableSchemaManagement

inline bool CatalogServer::IsCatalogInitialized() {
  lock_guard<mutex> l(catalog_lock_);

  // The first expression evaluates to true when the first catalog update is sent. If
  // catalog HA is enabled, the last_sent_catalog_version_ variable will only be
  // incremented on the active catalogd.
  // The second expression evaluates to true when the the standby catalogd determines that
  // it is the standby.
  return last_sent_catalog_version_ > 0 || (is_ha_determined_ && !is_active_);
} // CatalogServer::IsCatalogInitialized

bool CatalogServer::WaitForCatalogReady() {
  while (!IsCatalogInitialized()) {
    LOG(INFO) << "Waiting for first catalog update";
    SleepForMs(WM_INIT_CHECK_SLEEP_MS);
  }

  return IsActive();
} // function CatalogServer::WaitForCatalogReady

Status CatalogServer::InitWorkloadManagement() {
  DCHECK_NE(nullptr, thrift_iface_.get());

  LOG(INFO) << "Starting workload management initialization";

  // Set the default hostname if no hostname was specified on the startup flag.
  if (FLAGS_hostname.empty()) {
    RETURN_IF_ERROR(GetHostname(&FLAGS_hostname));
  }

  // Determine local ip address.
  string ip_addr;
  Status ip_status = HostnameToIpAddr(FLAGS_hostname, &ip_addr);
  if (!ip_status.ok()) {
    LOG(ERROR) << "Could not convert hostname " << FLAGS_hostname
               << " to ip address, error: " << ip_status.GetDetail();
    return ip_status;
  }

  // Drop tables specified on the startup flag.
  if (UNLIKELY(!FLAGS_workload_mgmt_drop_tables.empty())) {
    vector<string> tables_to_drop;
    split(tables_to_drop, FLAGS_workload_mgmt_drop_tables, is_any_of(","));

    for (auto& iter : tables_to_drop) {
      LOG(INFO) << "Attempting to drop table '" << _fullTableName(WM_DB, iter) << "'";
      Status stat = _dropTable(thrift_iface_.get(), ip_addr, WM_DB, iter);
      if (stat.ok()) {
        LOG(INFO) << "Successfully dropped table '" << _fullTableName(WM_DB, iter) << "'";
      } else {
        LOG(INFO) << stat;
      }
    }
  }

  RETURN_IF_ERROR(DebugAction(FLAGS_debug_actions, "CATALOG_WORKLOADMGMT_STARTUP"));

  Version target_schema_version;
  RETURN_IF_ERROR(ParseSchemaVersionFlag(&target_schema_version));
  RETURN_IF_ERROR(StartupChecks(target_schema_version));

  // Create the 'sys' db if it does not exist;
  RETURN_IF_ERROR(_setupDb(thrift_iface_.get(), ip_addr, WM_DB));

  // Create and/or update the query log table if needed.
  // Fully qualified table name based on startup flags.
  RETURN_IF_ERROR(_tableSchemaManagement(thrift_iface_.get(), ip_addr,
      QueryLogTableName(false), target_schema_version, false));

  // Create and/or update the query live table if needed.
  // Fully qualified table name based on startup flags.;
  RETURN_IF_ERROR(_tableSchemaManagement(thrift_iface_.get(), ip_addr,
      QueryLiveTableName(false), target_schema_version, true));

  LOG(INFO) << "Completed workload management initialization";

  return Status::OK();
} // function InitWorkloadManagement

} // namespace impala
