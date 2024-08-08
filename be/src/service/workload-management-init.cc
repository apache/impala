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

/// This file contains the code for the initialization process for workload management.
/// The init process handles:
///   1. Checking the state of the workload management db and tables.
///   2. Creating the db/tables if necessary.
///   3. Starting the workload management thread which  runs the completed queries
///      processing loop.

#include "service/workload-management.h"

#include <mutex>

#include <boost/algorithm/string/case_conv.hpp>
#include <boost/algorithm/string/predicate.hpp>
#include <boost/algorithm/string/trim.hpp>
#include <gflags/gflags_declare.h>
#include <glog/logging.h>
#include <gutil/strings/strcat.h>

#include "common/status.h"
#include "gen-cpp/CatalogObjects_constants.h"
#include "gen-cpp/SystemTables_types.h"
#include "gen-cpp/TCLIService_types.h"
#include "gen-cpp/Types_types.h"
#include "kudu/util/version_util.h"
#include "service/impala-server.h"

using namespace std;
using namespace impala;
using boost::algorithm::starts_with;
using boost::algorithm::trim_copy;
using kudu::Version;
using kudu::ParseVersion;

DECLARE_bool(enable_workload_mgmt);
DECLARE_int32(query_log_write_interval_s);
DECLARE_int32(query_log_write_timeout_s);
DECLARE_string(query_log_request_pool);
DECLARE_string(query_log_table_location);
DECLARE_string(query_log_table_name);
DECLARE_string(query_log_table_props);
DECLARE_string(workload_mgmt_user);
DECLARE_string(workload_mgmt_schema_version);

namespace impala {

/// Name of the database where all workload management tables will be stored.
static const string DB = "sys";

/// Sets up the sys database generating and executing the necessary DML statements.
static void _setupDb(InternalServer* server,
    InternalServer::QueryOptionMap& insert_query_opts) {
  insert_query_opts[TImpalaQueryOptions::SYNC_DDL] = "true";
  ABORT_IF_ERROR(server->ExecuteIgnoreResults(FLAGS_workload_mgmt_user,
      StrCat("CREATE DATABASE IF NOT EXISTS ", DB, " COMMENT "
      "'System database for Impala introspection'"), insert_query_opts, false));
  insert_query_opts[TImpalaQueryOptions::SYNC_DDL] = "false";
} // function _setupDb

/// Appends all relevant fields to a create or alter table sql statement.
static void _appendCols(StringStreamPop& stream,
    std::function<bool(const FieldDefinition& item)> shouldIncludeCol) {
  bool match = false;

  for (const auto& field : FIELD_DEFINITIONS) {
   if (shouldIncludeCol(field)) {
    match = true;
    stream << field.db_column << " " << field.db_column_type;

      if (field.db_column_type == TPrimitiveType::DECIMAL) {
        stream << "(" << field.precision << "," << field.scale << ")";
      }

      stream << ",";
   }
  }

  DCHECK_EQ(match, true);
  stream.move_back();
} // function _appendCols

/// Sets up the query table by generating and executing the necessary DML statements.
static void _setupTable(InternalServer* server, const string& table_name,
    InternalServer::QueryOptionMap& insert_query_opts, const Version& target_version,
    bool is_system_table = false) {
  insert_query_opts[TImpalaQueryOptions::SYNC_DDL] = "true";

  StringStreamPop create_table_sql;
  create_table_sql << "CREATE ";
  // System tables do not have anything to purge, and must not be managed tables.
  if (is_system_table) create_table_sql << "EXTERNAL ";
  create_table_sql << "TABLE IF NOT EXISTS " << table_name << "(";

  _appendCols(create_table_sql, [target_version](const FieldDefinition& f){
      return f.schema_version <= target_version;});

  create_table_sql << ") ";

  if (!is_system_table) {
    create_table_sql << "PARTITIONED BY SPEC(identity(cluster_id), HOUR(start_time_utc)) "
        << "STORED AS iceberg ";

    if (!FLAGS_query_log_table_location.empty()) {
      create_table_sql << "LOCATION '" << FLAGS_query_log_table_location << "' ";
    }
  }

  create_table_sql << "TBLPROPERTIES ('schema_version'='" << target_version.ToString()
      << "','format-version'='2'";

  if (is_system_table) {
    create_table_sql << ",'"
                     << g_CatalogObjects_constants.TBL_PROP_SYSTEM_TABLE <<"'='true'";
  } else if (!FLAGS_query_log_table_props.empty()) {
    create_table_sql << "," << FLAGS_query_log_table_props;
  }

  create_table_sql << ")";

  VLOG(2) << "Creating workload management table '" << table_name
      << "' on schema version '" << target_version.ToString() << "'";
  ABORT_IF_ERROR(server->ExecuteIgnoreResults(FLAGS_workload_mgmt_user,
      create_table_sql.str(), insert_query_opts, false));

  insert_query_opts[TImpalaQueryOptions::SYNC_DDL] = "false";

  LOG(INFO) << "Completed " << table_name << " initialization. write_interval=\"" <<
      FLAGS_query_log_write_interval_s << "s\"";
} // function _setupTable

static Version _retrieveSchemaVersion(InternalServer* server, const string table_name,
     const InternalServer::QueryOptionMap& insert_query_opts) {

  vector<apache::hive::service::cli::thrift::TRow> query_results;

  const Status describe_table = server->ExecuteAndFetchAllHS2(FLAGS_workload_mgmt_user,
      StrCat("DESCRIBE EXTENDED ", table_name), query_results, insert_query_opts, false);

  // If an error, ignore the error and run as if workload management has never
  // executed. Since all the DDLs use the "if not exists" clause, extra runs of the
  // DDLs will not cause any harm.
  if (describe_table.ok()) {
    const string SCHEMA_VER_PROP_NAME = "schema_version";

    // Table exists, search for its schema_version table property.
    for(auto& res : query_results) {
      if (starts_with(res.colVals[1].stringVal.value, SCHEMA_VER_PROP_NAME) ){
        const string schema_ver = trim_copy(res.colVals[2].stringVal.value);
        Version parsed_schema_ver;

        VLOG(2) << "Actual current workload management schema version of the '"
            << table_name << "' table is '" << schema_ver << "'";

        if(!ParseVersion(schema_ver, &parsed_schema_ver).ok()) {
          ABORT_WITH_ERROR(StrCat("Invalid actual workload management schema version '",
              schema_ver, "' for table '", table_name, "'"));
        }

        return parsed_schema_ver;
      }
    }

    // If the for loop does not find the schema_version table property, then it has been
    // removed outside of the workload management code.
    ABORT_WITH_ERROR(StrCat("Table '", table_name, "' is missing required property '",
        SCHEMA_VER_PROP_NAME, "'"));
  }

  return NO_TABLE_EXISTS;
} // _retrieveSchemaVersion

/// Aborts with error if the target_ver is less than the actual_ver.
static void _errorIfDowngrade(const Version target_ver, const Version actual_ver,
    const string table_name) {
  if (target_ver < actual_ver) {
      ABORT_WITH_ERROR(StrCat("Target schema version '", target_ver.ToString(),
          " of the '", table_name, "' table is lower than the actual schema version '",
          actual_ver.ToString(), "'. Downgrades are not supported. The target schema "
          "version must be greater than or equal to the actual schema version."));
    }
} // _errorIfDowngrade

void ImpalaServer::InitWorkloadManagement() {
  if (!FLAGS_enable_workload_mgmt) {
    return;
  }

  // Fully qualified table name based on startup flags.
  const string log_table_name = StrCat(DB, ".", FLAGS_query_log_table_name);

  // Verify FIELD_DEFINITIONS includes all QueryTableColumns.
  DCHECK_EQ(_TQueryTableColumn_VALUES_TO_NAMES.size(), FIELD_DEFINITIONS.size());
  for (const auto& field : FIELD_DEFINITIONS) {
    // Verify all fields match their column position.
    DCHECK_EQ(FIELD_DEFINITIONS[field.db_column].db_column, field.db_column);
  }

  // Ensure a valid schema version was specified on the command line flag.
  Version target_schema_version;
  if (!ParseVersion(FLAGS_workload_mgmt_schema_version,
      &target_schema_version).ok()) {
    ABORT_WITH_ERROR(StrCat("Invalid workload management schema version '",
        FLAGS_workload_mgmt_schema_version, "'"));
  }
  VLOG(2) << "Target workload management schema version is '"
      << target_schema_version.ToString() << "'";

  if (target_schema_version != VERSION_1_0_0) {
    ABORT_WITH_ERROR(StrCat("Workload management schema version '",
        FLAGS_workload_mgmt_schema_version, "' does not match any valid version"));
  }

  // Setup default query options that will be provided on all queries that insert rows
  // into the completed queries table.
  InternalServer::QueryOptionMap insert_query_opts;

  insert_query_opts[TImpalaQueryOptions::TIMEZONE] = "UTC";
  insert_query_opts[TImpalaQueryOptions::QUERY_TIMEOUT_S] = std::to_string(
      FLAGS_query_log_write_timeout_s < 1 ?
      FLAGS_query_log_write_interval_s : FLAGS_query_log_write_timeout_s);
  if (!FLAGS_query_log_request_pool.empty()) {
    insert_query_opts[TImpalaQueryOptions::REQUEST_POOL] = FLAGS_query_log_request_pool;
  }

  {
    lock_guard<mutex> l(workload_mgmt_threadstate_mu_);
    workload_mgmt_thread_state_ = INITIALIZING;
  }

  Version parsed_actual_schema_version;

  // Create and/or update the completed queries table if needed.
  parsed_actual_schema_version = _retrieveSchemaVersion(internal_server_.get(),
      log_table_name, insert_query_opts);

  if (parsed_actual_schema_version == NO_TABLE_EXISTS) {
    // First time setting up workload management.
    // Setup the sys database.
    _setupDb(internal_server_.get(), insert_query_opts);

    // Create the query log table at the target schema version.
    _setupTable(internal_server_.get(), log_table_name, insert_query_opts,
        VERSION_1_0_0);

    parsed_actual_schema_version = VERSION_1_0_0;
  } else {
    _errorIfDowngrade(target_schema_version, parsed_actual_schema_version,
        log_table_name);
  }

  // Create and/or update the live queries table if needed.
  // Determine the live queries table name.
  string live_table_name = StrCat(DB, ".",
      to_string(TSystemTableName::IMPALA_QUERY_LIVE));
  boost::algorithm::to_lower(live_table_name);

  parsed_actual_schema_version = _retrieveSchemaVersion(internal_server_.get(),
      live_table_name, insert_query_opts);

  if (parsed_actual_schema_version == NO_TABLE_EXISTS) {
    // First time setting up workload management.
    // Create the query live table on the target schema version.
    _setupTable(internal_server_.get(), live_table_name, insert_query_opts,
        target_schema_version, true);
  } else {
    _errorIfDowngrade(target_schema_version, parsed_actual_schema_version,
        live_table_name);
  }

  LOG(INFO) << "Completed workload management initialization";
  WorkloadManagementWorker(insert_query_opts, log_table_name);
} // ImpalaServer::InitWorkloadManagement

} // namespace impala
