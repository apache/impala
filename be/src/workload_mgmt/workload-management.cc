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

// This file contains the definition of the FIELD_DEFINITIONS list from the associated
// header file. Each field definition consists of the database column name for the field,
// the sql type of the database column, and a function that extracts the actual value from
// a `QueryStateExpanded` instance and writes it to the stream that is collecting all the
// values for the insert dml.

#include "workload_mgmt/workload-management.h"

#include <algorithm>
#include <mutex>
#include <optional>
#include <string>
#include <utility>
#include <vector>

#include <boost/algorithm/string/case_conv.hpp>
#include <boost/algorithm/string/join.hpp>
#include <gflags/gflags_declare.h>
#include <glog/logging.h>
#include <gutil/strings/strcat.h>

#include "common/compiler-util.h"
#include "common/status.h"
#include "gen-cpp/SystemTables_types.h"
#include "gen-cpp/Types_types.h"
#include "kudu/util/version_util.h"
#include "util/version-util.h"

DECLARE_string(query_log_table_name);
DECLARE_string(workload_mgmt_schema_version);

using namespace std;
using namespace impala;

using kudu::ParseVersion;
using kudu::Version;

namespace impala {
namespace workloadmgmt {

const std::map<TQueryTableColumn::type, FieldDefinition> FIELD_DEFINITIONS = {
    // Schema Version 1.0.0 Columns
    {TQueryTableColumn::CLUSTER_ID,
        FieldDefinition(TPrimitiveType::STRING, VERSION_1_0_0)},
    {TQueryTableColumn::QUERY_ID, FieldDefinition(TPrimitiveType::STRING, VERSION_1_0_0)},
    {TQueryTableColumn::SESSION_ID,
        FieldDefinition(TPrimitiveType::STRING, VERSION_1_0_0)},
    {TQueryTableColumn::SESSION_TYPE,
        FieldDefinition(TPrimitiveType::STRING, VERSION_1_0_0)},
    {TQueryTableColumn::HIVESERVER2_PROTOCOL_VERSION,
        FieldDefinition(TPrimitiveType::STRING, VERSION_1_0_0)},
    {TQueryTableColumn::DB_USER, FieldDefinition(TPrimitiveType::STRING, VERSION_1_0_0)},
    {TQueryTableColumn::DB_USER_CONNECTION,
        FieldDefinition(TPrimitiveType::STRING, VERSION_1_0_0)},
    {TQueryTableColumn::DB_NAME, FieldDefinition(TPrimitiveType::STRING, VERSION_1_0_0)},
    {TQueryTableColumn::IMPALA_COORDINATOR,
        FieldDefinition(TPrimitiveType::STRING, VERSION_1_0_0)},
    {TQueryTableColumn::QUERY_STATUS,
        FieldDefinition(TPrimitiveType::STRING, VERSION_1_0_0)},
    {TQueryTableColumn::QUERY_STATE,
        FieldDefinition(TPrimitiveType::STRING, VERSION_1_0_0)},
    {TQueryTableColumn::IMPALA_QUERY_END_STATE,
        FieldDefinition(TPrimitiveType::STRING, VERSION_1_0_0)},
    {TQueryTableColumn::QUERY_TYPE,
        FieldDefinition(TPrimitiveType::STRING, VERSION_1_0_0)},
    {TQueryTableColumn::NETWORK_ADDRESS,
        FieldDefinition(TPrimitiveType::STRING, VERSION_1_0_0)},
    {TQueryTableColumn::START_TIME_UTC,
        FieldDefinition(TPrimitiveType::TIMESTAMP, VERSION_1_0_0)},
    {TQueryTableColumn::TOTAL_TIME_MS,
        FieldDefinition(TPrimitiveType::DECIMAL, VERSION_1_0_0,
            DURATION_DECIMAL_PRECISION, DURATION_DECIMAL_SCALE)},
    {TQueryTableColumn::QUERY_OPTS_CONFIG,
        FieldDefinition(TPrimitiveType::STRING, VERSION_1_0_0)},
    {TQueryTableColumn::RESOURCE_POOL,
        FieldDefinition(TPrimitiveType::STRING, VERSION_1_0_0)},
    {TQueryTableColumn::PER_HOST_MEM_ESTIMATE,
        FieldDefinition(TPrimitiveType::BIGINT, VERSION_1_0_0)},
    {TQueryTableColumn::DEDICATED_COORD_MEM_ESTIMATE,
        FieldDefinition(TPrimitiveType::BIGINT, VERSION_1_0_0)},
    {TQueryTableColumn::PER_HOST_FRAGMENT_INSTANCES,
        FieldDefinition(TPrimitiveType::STRING, VERSION_1_0_0)},
    {TQueryTableColumn::BACKENDS_COUNT,
        FieldDefinition(TPrimitiveType::INT, VERSION_1_0_0)},
    {TQueryTableColumn::ADMISSION_RESULT,
        FieldDefinition(TPrimitiveType::STRING, VERSION_1_0_0)},
    {TQueryTableColumn::CLUSTER_MEMORY_ADMITTED,
        FieldDefinition(TPrimitiveType::BIGINT, VERSION_1_0_0)},
    {TQueryTableColumn::EXECUTOR_GROUP,
        FieldDefinition(TPrimitiveType::STRING, VERSION_1_0_0)},
    {TQueryTableColumn::EXECUTOR_GROUPS,
        FieldDefinition(TPrimitiveType::STRING, VERSION_1_0_0)},
    {TQueryTableColumn::EXEC_SUMMARY,
        FieldDefinition(TPrimitiveType::STRING, VERSION_1_0_0)},
    {TQueryTableColumn::NUM_ROWS_FETCHED,
        FieldDefinition(TPrimitiveType::BIGINT, VERSION_1_0_0)},
    {TQueryTableColumn::ROW_MATERIALIZATION_ROWS_PER_SEC,
        FieldDefinition(TPrimitiveType::BIGINT, VERSION_1_0_0)},
    {TQueryTableColumn::ROW_MATERIALIZATION_TIME_MS,
        FieldDefinition(TPrimitiveType::DECIMAL, VERSION_1_0_0,
            DURATION_DECIMAL_PRECISION, DURATION_DECIMAL_SCALE)},
    {TQueryTableColumn::COMPRESSED_BYTES_SPILLED,
        FieldDefinition(TPrimitiveType::BIGINT, VERSION_1_0_0)},
    {TQueryTableColumn::EVENT_PLANNING_FINISHED,
        FieldDefinition(TPrimitiveType::DECIMAL, VERSION_1_0_0,
            DURATION_DECIMAL_PRECISION, DURATION_DECIMAL_SCALE)},
    {TQueryTableColumn::EVENT_SUBMIT_FOR_ADMISSION,
        FieldDefinition(TPrimitiveType::DECIMAL, VERSION_1_0_0,
            DURATION_DECIMAL_PRECISION, DURATION_DECIMAL_SCALE)},
    {TQueryTableColumn::EVENT_COMPLETED_ADMISSION,
        FieldDefinition(TPrimitiveType::DECIMAL, VERSION_1_0_0,
            DURATION_DECIMAL_PRECISION, DURATION_DECIMAL_SCALE)},
    {TQueryTableColumn::EVENT_ALL_BACKENDS_STARTED,
        FieldDefinition(TPrimitiveType::DECIMAL, VERSION_1_0_0,
            DURATION_DECIMAL_PRECISION, DURATION_DECIMAL_SCALE)},
    {TQueryTableColumn::EVENT_ROWS_AVAILABLE,
        FieldDefinition(TPrimitiveType::DECIMAL, VERSION_1_0_0,
            DURATION_DECIMAL_PRECISION, DURATION_DECIMAL_SCALE)},
    {TQueryTableColumn::EVENT_FIRST_ROW_FETCHED,
        FieldDefinition(TPrimitiveType::DECIMAL, VERSION_1_0_0,
            DURATION_DECIMAL_PRECISION, DURATION_DECIMAL_SCALE)},
    {TQueryTableColumn::EVENT_LAST_ROW_FETCHED,
        FieldDefinition(TPrimitiveType::DECIMAL, VERSION_1_0_0,
            DURATION_DECIMAL_PRECISION, DURATION_DECIMAL_SCALE)},
    {TQueryTableColumn::EVENT_UNREGISTER_QUERY,
        FieldDefinition(TPrimitiveType::DECIMAL, VERSION_1_0_0,
            DURATION_DECIMAL_PRECISION, DURATION_DECIMAL_SCALE)},
    {TQueryTableColumn::READ_IO_WAIT_TOTAL_MS,
        FieldDefinition(TPrimitiveType::DECIMAL, VERSION_1_0_0,
            DURATION_DECIMAL_PRECISION, DURATION_DECIMAL_SCALE)},
    {TQueryTableColumn::READ_IO_WAIT_MEAN_MS,
        FieldDefinition(TPrimitiveType::DECIMAL, VERSION_1_0_0,
            DURATION_DECIMAL_PRECISION, DURATION_DECIMAL_SCALE)},
    {TQueryTableColumn::BYTES_READ_CACHE_TOTAL,
        FieldDefinition(TPrimitiveType::BIGINT, VERSION_1_0_0)},
    {TQueryTableColumn::BYTES_READ_TOTAL,
        FieldDefinition(TPrimitiveType::BIGINT, VERSION_1_0_0)},
    {TQueryTableColumn::PERNODE_PEAK_MEM_MIN,
        FieldDefinition(TPrimitiveType::BIGINT, VERSION_1_0_0)},
    {TQueryTableColumn::PERNODE_PEAK_MEM_MAX,
        FieldDefinition(TPrimitiveType::BIGINT, VERSION_1_0_0)},
    {TQueryTableColumn::PERNODE_PEAK_MEM_MEAN,
        FieldDefinition(TPrimitiveType::BIGINT, VERSION_1_0_0)},
    {TQueryTableColumn::SQL, FieldDefinition(TPrimitiveType::STRING, VERSION_1_0_0)},
    {TQueryTableColumn::PLAN, FieldDefinition(TPrimitiveType::STRING, VERSION_1_0_0)},
    {TQueryTableColumn::TABLES_QUERIED,
        FieldDefinition(TPrimitiveType::STRING, VERSION_1_0_0)},

    // Schema Version 1.1.0 Columns
    {TQueryTableColumn::SELECT_COLUMNS,
        FieldDefinition(TPrimitiveType::STRING, VERSION_1_1_0)},
    {TQueryTableColumn::WHERE_COLUMNS,
        FieldDefinition(TPrimitiveType::STRING, VERSION_1_1_0)},
    {TQueryTableColumn::JOIN_COLUMNS,
        FieldDefinition(TPrimitiveType::STRING, VERSION_1_1_0)},
    {TQueryTableColumn::AGGREGATE_COLUMNS,
        FieldDefinition(TPrimitiveType::STRING, VERSION_1_1_0)},
    {TQueryTableColumn::ORDERBY_COLUMNS,
        FieldDefinition(
            TPrimitiveType::STRING, VERSION_1_1_0)}}; // FIELD_DEFINITIONS constant list

/// Variable to cache the Version object created by parsing the workload management schema
/// version startup flag. Variable must only be modified during the workload management
/// StartupChecks() function.
optional<Version> parsed_target_schema_version;

/// Determines if the provided Version matches one of the known schema versions.
static Status _isVersionKnown(const Version& v) {
  if (auto iter = KNOWN_VERSIONS.find(v); UNLIKELY(iter == KNOWN_VERSIONS.end())) {
    vector<string> transformed(KNOWN_VERSIONS.size());

    transform(KNOWN_VERSIONS.cbegin(), KNOWN_VERSIONS.cend(), transformed.begin(),
        [](const Version& v) -> string { return v.ToString(); });

    return Status(StrCat("Workload management schema version '", v.ToString(),
        "' is not one of the known versions: '",
        boost::algorithm::join(transformed, "', '"), "'"));
  }

  return Status::OK();
} // function _isVersionKnown

Status ParseSchemaVersionFlag(kudu::Version* target_schema_version) {
  // Target schema version defaults to the latest version.
  *target_schema_version = *KNOWN_VERSIONS.rbegin();

  // Ensure a valid schema version was specified on the command line flag and, if
  // specified, parses the flag value into the function parameter `target_schema_version`.
  if (!FLAGS_workload_mgmt_schema_version.empty()
      && !ParseVersion(
          FLAGS_workload_mgmt_schema_version, target_schema_version).ok()) {
    return Status(StrCat("Invalid workload management schema version '",
        FLAGS_workload_mgmt_schema_version, "'"));
  }

  return Status::OK();
}

Status StartupChecks(const kudu::Version& target_schema_version) {
  RETURN_IF_ERROR(_isVersionKnown(target_schema_version));

  parsed_target_schema_version = target_schema_version;
  LOG(INFO) << "Target workload management schema version is '"
            << parsed_target_schema_version->ToString() << "'";

  // Warn if not targeting the latest version.
  const Version latest_schema_version = *KNOWN_VERSIONS.rbegin();
  if (parsed_target_schema_version != latest_schema_version) {
    LOG(WARNING) << "Target schema version '" << parsed_target_schema_version->ToString()
                 << "' is not the latest schema version '"
                 << latest_schema_version.ToString() << "'";
  }

  return Status::OK();
} // function StartupChecks

string QueryLogTableName(bool with_db) {
  string log_table_name = FLAGS_query_log_table_name;

  if (with_db) {
    log_table_name = StrCat(WM_DB, ".", log_table_name);
  }

  return boost::algorithm::to_lower_copy(log_table_name);
} // function QueryLogTableName

string QueryLiveTableName(bool with_db) {
  string live_table_name = to_string(TSystemTableName::IMPALA_QUERY_LIVE);

  if (with_db) {
    live_table_name = StrCat(WM_DB, ".", live_table_name);
  }

  return boost::algorithm::to_lower_copy(live_table_name);
} // function QueryLiveTableName

bool IncludeField(const TQueryTableColumn::type& col) {
  DCHECK(parsed_target_schema_version.has_value());
  DCHECK_EQ(FIELD_DEFINITIONS.count(col), 1);

  return FIELD_DEFINITIONS.at(col).Include(parsed_target_schema_version.value());
} // function IncludeField

} // namespace workloadmgmt
} // namespace impala
