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

#include "workload_mgmt/workload-management.h"

#include <sstream>
#include <string>
#include <vector>

#include <gtest/gtest.h>

#include "gen-cpp/SystemTables_types.h"
#include "gen-cpp/Types_types.h"
#include "kudu/util/version_util.h"

using namespace std;
using namespace impala;
using namespace impala::workloadmgmt;

using kudu::ParseVersion;
using kudu::Version;

/// Add the equality operator to the FieldDefinition class.
namespace impala {
namespace workloadmgmt {

bool operator==(const FieldDefinition& lhs, const FieldDefinition& rhs) {
  bool is_equal = lhs.db_column_type == rhs.db_column_type
      && lhs.schema_version == rhs.schema_version;

  if (lhs.db_column_type == TPrimitiveType::DECIMAL) {
    return is_equal && lhs.precision == rhs.precision && lhs.scale == rhs.scale;
  }

  return is_equal;
}

} // namespace workloadmgmt
} // namespace impala

using FieldDefEntry = pair<TQueryTableColumn::type, FieldDefinition>;

FieldDefEntry _createV100(TQueryTableColumn::type db_col, TPrimitiveType::type t,
    const int16_t precision = 0, const int16_t scale = 0) {
  Version v;
  EXPECT_TRUE(ParseVersion("1.0.0", &v).ok());

  return make_pair<>(db_col, FieldDefinition(t, v, precision, scale));
}

FieldDefEntry _createV100String(TQueryTableColumn::type db_col) {
  return _createV100(db_col, TPrimitiveType::STRING);
}

FieldDefEntry _createV100BigInt(TQueryTableColumn::type db_col) {
  return _createV100(db_col, TPrimitiveType::BIGINT);
}

FieldDefEntry _createV100Int(TQueryTableColumn::type db_col) {
  return _createV100(db_col, TPrimitiveType::INT);
}

FieldDefEntry _createV100Timestamp(TQueryTableColumn::type db_col) {
  return _createV100(db_col, TPrimitiveType::TIMESTAMP);
}

FieldDefEntry _createV100Decimal(TQueryTableColumn::type db_col) {
  return _createV100(db_col, TPrimitiveType::DECIMAL, 18, 3);
}

FieldDefEntry _createV110String(TQueryTableColumn::type db_col) {
  Version v;
  EXPECT_TRUE(ParseVersion("1.1.0", &v).ok());

  return make_pair<>(db_col, FieldDefinition(TPrimitiveType::STRING, v));
}

FieldDefEntry _createV120BigInt(TQueryTableColumn::type db_col) {
  Version v;
  EXPECT_TRUE(ParseVersion("1.2.0", &v).ok());

  return make_pair<>(db_col, FieldDefinition(TPrimitiveType::BIGINT, v));
}

// Predicate function that can be passed to gtest EXPECT/ASSERT calls to determine
// correctness of a FIELD_DEFINITIONS map entry.
bool _fieldDefsEqual(const FieldDefEntry& lhs,
    const pair<const TQueryTableColumn::type, FieldDefinition>& rhs) {
  return lhs.first == rhs.first && lhs.second == rhs.second;
}

TEST(WorkloadManagementFieldsTest, CheckFieldDefinitions) {
  // Asserts FIELD_DEFINITIONS includes one entry for each member of the Thrift
  // TQueryTableColumn enum and has correctly defined each column.
  ASSERT_EQ(_TQueryTableColumn_VALUES_TO_NAMES.size(), FIELD_DEFINITIONS.size());

  vector<FieldDefEntry> expected_field_defs = {
      _createV100String(TQueryTableColumn::CLUSTER_ID),
      _createV100String(TQueryTableColumn::QUERY_ID),
      _createV100String(TQueryTableColumn::SESSION_ID),
      _createV100String(TQueryTableColumn::SESSION_TYPE),
      _createV100String(TQueryTableColumn::HIVESERVER2_PROTOCOL_VERSION),
      _createV100String(TQueryTableColumn::DB_USER),
      _createV100String(TQueryTableColumn::DB_USER_CONNECTION),
      _createV100String(TQueryTableColumn::DB_NAME),
      _createV100String(TQueryTableColumn::IMPALA_COORDINATOR),
      _createV100String(TQueryTableColumn::QUERY_STATUS),
      _createV100String(TQueryTableColumn::QUERY_STATE),
      _createV100String(TQueryTableColumn::IMPALA_QUERY_END_STATE),
      _createV100String(TQueryTableColumn::QUERY_TYPE),
      _createV100String(TQueryTableColumn::NETWORK_ADDRESS),
      _createV100Timestamp(TQueryTableColumn::START_TIME_UTC),
      _createV100Decimal(TQueryTableColumn::TOTAL_TIME_MS),
      _createV100String(TQueryTableColumn::QUERY_OPTS_CONFIG),
      _createV100String(TQueryTableColumn::RESOURCE_POOL),
      _createV100BigInt(TQueryTableColumn::PER_HOST_MEM_ESTIMATE),
      _createV100BigInt(TQueryTableColumn::DEDICATED_COORD_MEM_ESTIMATE),
      _createV100String(TQueryTableColumn::PER_HOST_FRAGMENT_INSTANCES),
      _createV100Int(TQueryTableColumn::BACKENDS_COUNT),
      _createV100String(TQueryTableColumn::ADMISSION_RESULT),
      _createV100BigInt(TQueryTableColumn::CLUSTER_MEMORY_ADMITTED),
      _createV100String(TQueryTableColumn::EXECUTOR_GROUP),
      _createV100String(TQueryTableColumn::EXECUTOR_GROUPS),
      _createV100String(TQueryTableColumn::EXEC_SUMMARY),
      _createV100BigInt(TQueryTableColumn::NUM_ROWS_FETCHED),
      _createV100BigInt(TQueryTableColumn::ROW_MATERIALIZATION_ROWS_PER_SEC),
      _createV100Decimal(TQueryTableColumn::ROW_MATERIALIZATION_TIME_MS),
      _createV100BigInt(TQueryTableColumn::COMPRESSED_BYTES_SPILLED),
      _createV100Decimal(TQueryTableColumn::EVENT_PLANNING_FINISHED),
      _createV100Decimal(TQueryTableColumn::EVENT_SUBMIT_FOR_ADMISSION),
      _createV100Decimal(TQueryTableColumn::EVENT_COMPLETED_ADMISSION),
      _createV100Decimal(TQueryTableColumn::EVENT_ALL_BACKENDS_STARTED),
      _createV100Decimal(TQueryTableColumn::EVENT_ROWS_AVAILABLE),
      _createV100Decimal(TQueryTableColumn::EVENT_FIRST_ROW_FETCHED),
      _createV100Decimal(TQueryTableColumn::EVENT_LAST_ROW_FETCHED),
      _createV100Decimal(TQueryTableColumn::EVENT_UNREGISTER_QUERY),
      _createV100Decimal(TQueryTableColumn::READ_IO_WAIT_TOTAL_MS),
      _createV100Decimal(TQueryTableColumn::READ_IO_WAIT_MEAN_MS),
      _createV100BigInt(TQueryTableColumn::BYTES_READ_CACHE_TOTAL),
      _createV100BigInt(TQueryTableColumn::BYTES_READ_TOTAL),
      _createV100BigInt(TQueryTableColumn::PERNODE_PEAK_MEM_MIN),
      _createV100BigInt(TQueryTableColumn::PERNODE_PEAK_MEM_MAX),
      _createV100BigInt(TQueryTableColumn::PERNODE_PEAK_MEM_MEAN),
      _createV100String(TQueryTableColumn::SQL),
      _createV100String(TQueryTableColumn::PLAN),
      _createV100String(TQueryTableColumn::TABLES_QUERIED),
      _createV110String(TQueryTableColumn::SELECT_COLUMNS),
      _createV110String(TQueryTableColumn::WHERE_COLUMNS),
      _createV110String(TQueryTableColumn::JOIN_COLUMNS),
      _createV110String(TQueryTableColumn::AGGREGATE_COLUMNS),
      _createV110String(TQueryTableColumn::ORDERBY_COLUMNS),
      _createV120BigInt(TQueryTableColumn::COORDINATOR_SLOTS),
      _createV120BigInt(TQueryTableColumn::EXECUTOR_SLOTS),
  };

  ASSERT_EQ(FIELD_DEFINITIONS.size(), expected_field_defs.size());

  int i = 0;
  for (const auto& it : FIELD_DEFINITIONS) {
    EXPECT_PRED2(_fieldDefsEqual, expected_field_defs[i], it);
    i++;
  }
}
