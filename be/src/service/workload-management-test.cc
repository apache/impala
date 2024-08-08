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

#include "service/workload-management.h"

#include <sstream>
#include <string>

#include <gutil/strings/strcat.h>

#include "gen-cpp/SystemTables_types.h"
#include "testutil/gtest-util.h"

using std::stringstream;
using std::string;

namespace impala {

static constexpr int8_t EXPECTED_DECIMAL_PRECISION = 18;
static constexpr int8_t EXPECTED_DECIMAL_SCALE = 3;

// Builds and returns a string that can be used to identify the column name during a test
// failure situation.
static string _eMsg(const string& expected_name) {
  return StrCat("for field: ", expected_name);
}

// Asserts the common fields on a FieldDefinition instance.
static void _assertCol(const string& expected_name,
    const TPrimitiveType::type& expected_type, const string& expected_version,
    const FieldDefinition& actual){
  stringstream actual_col_name;
  actual_col_name << actual.db_column;

  stringstream actual_col_type;
  actual_col_type << actual.db_column_type;

  EXPECT_EQ(expected_name, actual_col_name.str()) << _eMsg(expected_name);
  EXPECT_EQ(to_string(expected_type), actual_col_type.str()) << _eMsg(expected_name);
  EXPECT_EQ(expected_version, actual.schema_version.ToString()) << _eMsg(expected_name);
}

// Asserts the common fields and the decimal related fields on a FieldDefinition instance.
static void _assertColDecimal(const string& expected_name,
    const string& expected_version, const FieldDefinition& actual){
  _assertCol(expected_name, TPrimitiveType::DECIMAL, expected_version, actual);

  EXPECT_EQ(EXPECTED_DECIMAL_PRECISION, actual.precision) << _eMsg(expected_name);
  EXPECT_EQ(EXPECTED_DECIMAL_SCALE, actual.scale) << _eMsg(expected_name);
}

TEST(WorkloadManagementTest, CheckColumnNames) {
  EXPECT_EQ(49, FIELD_DEFINITIONS.size());

  _assertCol("CLUSTER_ID", TPrimitiveType::STRING, "1.0.0", FIELD_DEFINITIONS[0]);
  _assertCol("QUERY_ID", TPrimitiveType::STRING, "1.0.0", FIELD_DEFINITIONS[1]);
  _assertCol("SESSION_ID", TPrimitiveType::STRING, "1.0.0", FIELD_DEFINITIONS[2]);
  _assertCol("SESSION_TYPE", TPrimitiveType::STRING, "1.0.0", FIELD_DEFINITIONS[3]);
  _assertCol("HIVESERVER2_PROTOCOL_VERSION", TPrimitiveType::STRING, "1.0.0",
      FIELD_DEFINITIONS[4]);
  _assertCol("DB_USER", TPrimitiveType::STRING, "1.0.0", FIELD_DEFINITIONS[5]);
  _assertCol("DB_USER_CONNECTION", TPrimitiveType::STRING, "1.0.0", FIELD_DEFINITIONS[6]);
  _assertCol("DB_NAME", TPrimitiveType::STRING, "1.0.0", FIELD_DEFINITIONS[7]);
  _assertCol("IMPALA_COORDINATOR", TPrimitiveType::STRING, "1.0.0", FIELD_DEFINITIONS[8]);
  _assertCol("QUERY_STATUS", TPrimitiveType::STRING, "1.0.0", FIELD_DEFINITIONS[9]);
  _assertCol("QUERY_STATE", TPrimitiveType::STRING, "1.0.0", FIELD_DEFINITIONS[10]);
  _assertCol("IMPALA_QUERY_END_STATE", TPrimitiveType::STRING, "1.0.0",
      FIELD_DEFINITIONS[11]);
  _assertCol("QUERY_TYPE", TPrimitiveType::STRING, "1.0.0", FIELD_DEFINITIONS[12]);
  _assertCol("NETWORK_ADDRESS", TPrimitiveType::STRING, "1.0.0", FIELD_DEFINITIONS[13]);
  _assertCol("START_TIME_UTC", TPrimitiveType::TIMESTAMP, "1.0.0", FIELD_DEFINITIONS[14]);
  _assertColDecimal("TOTAL_TIME_MS", "1.0.0", FIELD_DEFINITIONS[15]);
  _assertCol("QUERY_OPTS_CONFIG", TPrimitiveType::STRING, "1.0.0", FIELD_DEFINITIONS[16]);
  _assertCol("RESOURCE_POOL", TPrimitiveType::STRING, "1.0.0", FIELD_DEFINITIONS[17]);
  _assertCol("PER_HOST_MEM_ESTIMATE", TPrimitiveType::BIGINT, "1.0.0",
      FIELD_DEFINITIONS[18]);
  _assertCol("DEDICATED_COORD_MEM_ESTIMATE", TPrimitiveType::BIGINT, "1.0.0",
      FIELD_DEFINITIONS[19]);
  _assertCol("PER_HOST_FRAGMENT_INSTANCES", TPrimitiveType::STRING, "1.0.0",
      FIELD_DEFINITIONS[20]);
  _assertCol("BACKENDS_COUNT", TPrimitiveType::INT, "1.0.0", FIELD_DEFINITIONS[21]);
  _assertCol("ADMISSION_RESULT", TPrimitiveType::STRING, "1.0.0", FIELD_DEFINITIONS[22]);
  _assertCol("CLUSTER_MEMORY_ADMITTED", TPrimitiveType::BIGINT, "1.0.0",
      FIELD_DEFINITIONS[23]);
  _assertCol("EXECUTOR_GROUP", TPrimitiveType::STRING, "1.0.0", FIELD_DEFINITIONS[24]);
  _assertCol("EXECUTOR_GROUPS", TPrimitiveType::STRING, "1.0.0", FIELD_DEFINITIONS[25]);
  _assertCol("EXEC_SUMMARY", TPrimitiveType::STRING, "1.0.0", FIELD_DEFINITIONS[26]);
  _assertCol("NUM_ROWS_FETCHED", TPrimitiveType::BIGINT, "1.0.0", FIELD_DEFINITIONS[27]);
  _assertCol("ROW_MATERIALIZATION_ROWS_PER_SEC", TPrimitiveType::BIGINT, "1.0.0",
      FIELD_DEFINITIONS[28]);
  _assertColDecimal("ROW_MATERIALIZATION_TIME_MS", "1.0.0", FIELD_DEFINITIONS[29]);
  _assertCol("COMPRESSED_BYTES_SPILLED", TPrimitiveType::BIGINT, "1.0.0",
      FIELD_DEFINITIONS[30]);
  _assertColDecimal("EVENT_PLANNING_FINISHED", "1.0.0", FIELD_DEFINITIONS[31]);
  _assertColDecimal("EVENT_SUBMIT_FOR_ADMISSION", "1.0.0", FIELD_DEFINITIONS[32]);
  _assertColDecimal("EVENT_COMPLETED_ADMISSION", "1.0.0", FIELD_DEFINITIONS[33]);
  _assertColDecimal("EVENT_ALL_BACKENDS_STARTED", "1.0.0", FIELD_DEFINITIONS[34]);
  _assertColDecimal("EVENT_ROWS_AVAILABLE", "1.0.0", FIELD_DEFINITIONS[35]);
  _assertColDecimal("EVENT_FIRST_ROW_FETCHED", "1.0.0", FIELD_DEFINITIONS[36]);
  _assertColDecimal("EVENT_LAST_ROW_FETCHED", "1.0.0", FIELD_DEFINITIONS[37]);
  _assertColDecimal("EVENT_UNREGISTER_QUERY", "1.0.0", FIELD_DEFINITIONS[38]);
  _assertColDecimal("READ_IO_WAIT_TOTAL_MS", "1.0.0", FIELD_DEFINITIONS[39]);
  _assertColDecimal("READ_IO_WAIT_MEAN_MS", "1.0.0", FIELD_DEFINITIONS[40]);
  _assertCol("BYTES_READ_CACHE_TOTAL", TPrimitiveType::BIGINT, "1.0.0",
      FIELD_DEFINITIONS[41]);
  _assertCol("BYTES_READ_TOTAL", TPrimitiveType::BIGINT, "1.0.0", FIELD_DEFINITIONS[42]);
  _assertCol("PERNODE_PEAK_MEM_MIN", TPrimitiveType::BIGINT, "1.0.0",
      FIELD_DEFINITIONS[43]);
  _assertCol("PERNODE_PEAK_MEM_MAX", TPrimitiveType::BIGINT, "1.0.0",
      FIELD_DEFINITIONS[44]);
  _assertCol("PERNODE_PEAK_MEM_MEAN", TPrimitiveType::BIGINT, "1.0.0",
      FIELD_DEFINITIONS[45]);
  _assertCol("SQL", TPrimitiveType::STRING, "1.0.0", FIELD_DEFINITIONS[46]);
  _assertCol("PLAN", TPrimitiveType::STRING, "1.0.0", FIELD_DEFINITIONS[47]);
  _assertCol("TABLES_QUERIED", TPrimitiveType::STRING, "1.0.0", FIELD_DEFINITIONS[48]);
}

} // namespace impala
