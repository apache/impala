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

#include <regex>
#include <string>

#include <gflags/gflags.h>
#include <glog/logging.h>

using namespace std;

static regex alphanum_underscore_dash("^[A-Za-z0-9\\-_]+$");

DEFINE_bool(enable_workload_mgmt, false,
    "Specifies if Impala will automatically write completed queries in the query log "
    "table. If this value is set to true and then later removed, the query log table "
    "will remain intact and accessible.");

DEFINE_string_hidden(query_log_table_name, "impala_query_log", "Specifies the name of "
    "the query log table where completed queries will be stored.");

DEFINE_validator(query_log_table_name, [](const char* name, const string& val) {
  if (regex_match(val, alphanum_underscore_dash)) return true;

  LOG(ERROR) << "Invalid value for --" << name << ": must only contain alphanumeric "
      "characters, underscores, or dashes and must not be empty";
  return false;
});

DEFINE_string_hidden(query_log_table_location, "", "Specifies the location of the query "
    "log table where completed queries will be stored.");

DEFINE_validator(query_log_table_location, [](const char* name, const string& val) {
  if (val.find_first_of('\'') != string::npos) {
    LOG(ERROR) << "Invalid value for --" << name << ": must not contain single quotes.";
    return false;
  }

  if (val.find_first_of('"') != string::npos) {
    LOG(ERROR) << "Invalid value for --" << name << ": must not contain double quotes.";
    return false;
  }

  if (val.find_first_of('\n') != string::npos) {
    LOG(ERROR) << "Invalid value for --" << name << ": must not contain newlines.";
    return false;
  }

  return true;
});

DEFINE_int32(query_log_write_interval_s, 300, "Number of seconds to wait between "
    "batches of inserts to the query log table. The countdown to the next write starts "
    "immediately when a write begins, but a new write will not start until the prior "
    "write has completed. Min value is 1. Max value is 14400.");

DEFINE_validator(query_log_write_interval_s, [](const char* name, int32_t val) {
  if (val > 0 && val <= 14400) return true;
  LOG(ERROR) << "Invalid value for --" << name << ": must be greater than 0 and less "
      "than or equal to 14,400.";
  return false;
});

DEFINE_int32_hidden(query_log_write_timeout_s, 0, "Specifies the query timeout in "
    "seconds for inserts to the query log table. A value less than 1 indicates to use "
    "the same value as the query_log_write_interval_s flag.");

DEFINE_int32(query_log_max_queued, 5000, "Maximum number of records that can be queued "
    "before they are written to the impala query log table. This flag operates "
    "independently of the 'query_log_write_interval_s' flag. If the number of queued "
    "records reaches this value, the records will be written to the query log table no "
    "matter how much time has passed since the last write. The countdown to the next "
    "write (based on the time period defined in the 'query_log_write_interval_s' flag) "
    "is not restarted.");

DEFINE_validator(query_log_max_queued, [](const char* name, int32_t val) {
  if (val >= 0) return true;
  LOG(ERROR) << "Invalid value for --" << name << ": must be greater than or equal to 0";
  return false;
});

DEFINE_string_hidden(workload_mgmt_user, "impala", "Specifies the user that will be used "
    "to create, update, and insert records into the query log table.");

DEFINE_validator(workload_mgmt_user, [](const char* name, const string& val) {
  if (FLAGS_enable_workload_mgmt && !regex_match(val, alphanum_underscore_dash)) {
    LOG(ERROR) << "Invalid value for --" << name << ": must be a valid user when "
        "workload management is enabled and must only contain alphanumeric characters, "
        "underscores, or dashes";
    return false;
  }

  return true;
});

DEFINE_int32(query_log_max_sql_length, 16777216, "Maximum length of a sql statement that "
    "will be recorded in the completed queries table. If a sql statement with a length "
    "longer than this value is executed, the sql inserted into the completed queries "
    "table will be trimmed to this length. Any characters that need escaping will have "
    "their backslash character counted towards this limit.");

DEFINE_validator(query_log_max_sql_length, [](const char* name, int32_t val) {
  if (val >= 0) return true;
  LOG(ERROR) << "Invalid value for --" << name << ": must be greater than or equal to 0";
  return false;
});

DEFINE_int32(query_log_max_plan_length, 16777216, "Maximum length of the sql plan that "
    "will be recorded in the completed queries table. If a plan has a length longer than "
    "this value, the plan inserted into the completed queries table will be trimmed to "
    "this length. Any characters that need escaping will have their backslash character "
    "counted towards this limit.");

DEFINE_validator(query_log_max_plan_length, [](const char* name, int32_t val) {
  if (val >= 0) return true;
  LOG(ERROR) << "Invalid value for --" << name << ": must be greater than or equal to 0";
  return false;
});

DEFINE_int32_hidden(query_log_shutdown_timeout_s, 30, "Number of seconds to wait for "
    "the queue of completed queries to be drained to the query log table before timing "
    "out and continuing the shutdown process. The completed queries drain process runs "
    "after the shutdown process completes, thus the max shutdown time is extended by the "
    "value specified in this flag.");

DEFINE_validator(query_log_shutdown_timeout_s, [](const char* name, int32_t val) {
  if (val >= 0) return true;
  LOG(ERROR) << "Invalid value for --" << name << ": must be a positive value";
  return false;
});

DEFINE_string(cluster_id, "", "Specifies an identifier string that uniquely represents "
    "this cluster. This identifier is included in the query log table if enabled.");

DEFINE_validator(cluster_id, [](const char* name, const string& val) {
  if (val.length() == 0 || regex_match(val, alphanum_underscore_dash)) return true;

  LOG(ERROR) << "Invalid value for --" << name << ": must only contain alphanumeric "
      "characters, underscores, or dashes and must not be empty";
  return false;
});

DEFINE_int32_hidden(query_log_max_insert_attempts, 3, "Maximum number of times to "
    "attempt to insert a record into the completed queries table before abandining it.");

DEFINE_validator(query_log_max_insert_attempts, [](const char* name, int32_t val) {
  if (val > 0) return true;
  LOG(ERROR) << "Invalid value for --" << name << ": must be greater than 1";
  return false;
});

DEFINE_string(query_log_request_pool, "", "Specifies a pool or queue used by the queries "
    "that insert into the query log table. Empty value causes no pool to be set.");

DEFINE_string_hidden(query_log_table_props, "", "Comma separated list of additional "
    "Iceberg table properties in the format 'key'='value' to apply when creating the "
    "query log table. Only applies when the table is being created. After table "
    "creation, this property does nothing");
