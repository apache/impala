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

#include <gflags/gflags_declare.h>
#include <gtest/gtest.h>

#include "gen-cpp/ErrorCodes_types.h"
#include "gen-cpp/SystemTables_types.h"

#include "kudu/util/version_util.h"
#include "testutil/death-test-util.h"
#include "testutil/gtest-util.h"

DECLARE_string(query_log_table_name);
DECLARE_string(workload_mgmt_schema_version);

using namespace std;
using namespace impala;
using namespace impala::workloadmgmt;

using kudu::ParseVersion;
using kudu::Version;

TEST(WorkloadManagementTest, ParseSchemaVersionFlagHappyPath) {
  // Asserts any valid version can be parsed.
  FLAGS_workload_mgmt_schema_version = "0.0.0";

  Version v;

  ASSERT_OK(ParseSchemaVersionFlag(&v));
  ASSERT_EQ("0.0.0", v.ToString());
}

TEST(WorkloadManagementTest, ParseSchemaVersionFlagEmpty) {
  // Asserts an empty schema version flag defaults to the latest version;
  FLAGS_workload_mgmt_schema_version = "";

  Version v;

  ASSERT_OK(ParseSchemaVersionFlag(&v));
  ASSERT_EQ("1.1.0", v.ToString());
}

TEST(WorkloadManagementTest, ParseSchemaVersionFlagInvalid) {
  // Asserts the correct error is returned when the workload management schema version is
  // set to an invalid version string.
  FLAGS_workload_mgmt_schema_version = "notaversion";

  Version v;

  Status actual = ParseSchemaVersionFlag(&v);
  EXPECT_EQ(TErrorCode::GENERAL, actual.code());
  EXPECT_EQ(
      actual.msg().msg(), "Invalid workload management schema version 'notaversion'");
}

TEST(WorkloadManagementTest, StartupChecksHappyPath) {
  // Asserts the StartupChecks function works on older schema versions.
  Version v;

  ASSERT_TRUE(ParseVersion("1.1.0", &v).ok());

  ASSERT_OK(StartupChecks(v));
  EXPECT_TRUE(IncludeField(TQueryTableColumn::CLUSTER_ID));
  EXPECT_TRUE(IncludeField(TQueryTableColumn::SELECT_COLUMNS));
}

TEST(WorkloadManagementTest, StartupChecksHappyPathOlderVersion) {
  // Asserts the StartupChecks function works on older schema versions.
  Version v;

  ASSERT_TRUE(ParseVersion("1.0.0", &v).ok());

  ASSERT_OK(StartupChecks(v));
  EXPECT_TRUE(IncludeField(TQueryTableColumn::CLUSTER_ID));
  EXPECT_FALSE(IncludeField(TQueryTableColumn::SELECT_COLUMNS));
}

TEST(WorkloadManagementTest, StartupChecksUnknownVersion) {
  Version v;
  ASSERT_TRUE(ParseVersion("99999.99999.99999", &v).ok());

  Status ret = StartupChecks(v);

  EXPECT_EQ(TErrorCode::type::GENERAL, ret.code());
  EXPECT_EQ("Workload management schema version '99999.99999.99999' is not one of the "
            "known versions: '1.0.0', '1.1.0'",
      ret.msg().msg());
}

TEST(WorkloadManagementDeathTest, IncludeFieldMissing) {
  // Asserts the case where the FIELD_DEFINITIONS map is missing a column, and that
  // column is checked for inclusion.
  GTEST_FLAG_SET(death_test_style, "threadsafe");
  IMPALA_ASSERT_DEBUG_DEATH(IncludeField(static_cast<TQueryTableColumn::type>(9999)),
      "impala::workloadmgmt::IncludeField()");
}

TEST(WorkloadManagementTest, QueryLogTableNameWithoutDb) {
  FLAGS_query_log_table_name = "fOObaR";
  ASSERT_EQ("foobar", QueryLogTableName(false));
}

TEST(WorkloadManagementTest, QueryLogTableNameWithDb) {
  FLAGS_query_log_table_name = "FOobAr";
  ASSERT_EQ("sys.foobar", QueryLogTableName(true));
}

TEST(WorkloadManagementTest, QueryLiveTableNameWithoutDb) {
  ASSERT_EQ("impala_query_live", QueryLiveTableName(false));
}

TEST(WorkloadManagementTest, QueryLiveTableNameWithDb) {
  ASSERT_EQ("sys.impala_query_live", QueryLiveTableName(true));
}

TEST(WorkloadManagementTest, KnownVersions) {
  // Asserts the known versions are correct and are in the correct order in the
  // KNOWN_VERSIONS set.
  ASSERT_EQ(2, KNOWN_VERSIONS.size());

  auto iter = KNOWN_VERSIONS.cbegin();

  EXPECT_EQ("1.0.0", iter->ToString());
  iter++;

  EXPECT_EQ("1.1.0", iter->ToString());
  iter++;

  EXPECT_EQ(KNOWN_VERSIONS.cend(), iter);
}
