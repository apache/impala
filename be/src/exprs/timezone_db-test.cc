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

#include "exprs/timezone_db.h"
#include "testutil/gtest-util.h"

#include "common/names.h"

using std::ios_base;

namespace impala {

class TimezoneDbNamesTest : public testing::Test {
 protected:
  void TestValidTimezoneOffset(const string& tz_offset, int64_t expected_offset) {
    int64_t result;
    EXPECT_TRUE(TimezoneDatabase::IsTimezoneOffsetValid(tz_offset, &result));
    EXPECT_EQ(expected_offset, result);
  }

  void TestInvalidTimezoneOffset(const string& tz_offset) {
    int64_t result;
    EXPECT_FALSE(TimezoneDatabase::IsTimezoneOffsetValid(tz_offset, &result));
  }

  void TestValidTimezoneName(const string& tz_name) {
    EXPECT_TRUE(TimezoneDatabase::IsTimezoneNameValid(tz_name));
  }

  void TestInvalidTimezoneName(const string& tz_name) {
    EXPECT_FALSE(TimezoneDatabase::IsTimezoneNameValid(tz_name));
  }
};

TEST_F(TimezoneDbNamesTest, TzOffset) {
  TestValidTimezoneOffset("0", 0);
  TestValidTimezoneOffset("+00", 0);
  TestValidTimezoneOffset("+1", 1);
  TestValidTimezoneOffset("-1234", -1234);
  TestValidTimezoneOffset("86399", 86399);
  TestValidTimezoneOffset("+86399", 86399);
  TestValidTimezoneOffset("-86399", -86399);

  // Inalid UTC offsets.
  TestInvalidTimezoneOffset("");
  TestInvalidTimezoneOffset(".");
  TestInvalidTimezoneOffset("0.1");
  TestInvalidTimezoneOffset("PST");
  TestInvalidTimezoneOffset(":0");
  TestInvalidTimezoneOffset("0&");
  // UTC offset has to be less then 24 hours in seconds.
  TestInvalidTimezoneOffset("86400");
  TestInvalidTimezoneOffset("-86400");
}

TEST_F(TimezoneDbNamesTest, TzName) {
  TestValidTimezoneName("PST");
  TestValidTimezoneName("PST8PDT");
  TestValidTimezoneName("UTC+1");
  TestValidTimezoneName("UTC-10");
  TestValidTimezoneName("GMT-10:00");
  TestValidTimezoneName("W-SU");
  TestValidTimezoneName("Singapore");
  TestValidTimezoneName("Etc/GMT+5");
  TestValidTimezoneName("America/Tijuana");
  TestValidTimezoneName("America/Argentina/San_Jose");

  // Misformatted time-zone names.
  TestInvalidTimezoneName("");
  TestInvalidTimezoneName(".");
  TestInvalidTimezoneName("zone.tab");
  TestInvalidTimezoneName("/");
  TestInvalidTimezoneName("/PST");
  TestInvalidTimezoneName(":GMT-10");
  TestInvalidTimezoneName("PST8PDT/");
  TestInvalidTimezoneName("America//Tijuana");
  TestInvalidTimezoneName("America/Tijuana/");

  // Time-zone name segments must start with uppercase letters.
  TestInvalidTimezoneName("pST");
  TestInvalidTimezoneName("pst");
  TestInvalidTimezoneName("singapore");
  TestInvalidTimezoneName("america/Tijuana");
  TestInvalidTimezoneName("America/tijuana");
  TestInvalidTimezoneName("America/Argentina/san_Jose");
  TestInvalidTimezoneName("posixrules");
}


class TimezoneDbLoadAliasTest : public testing::Test {
 protected:
  virtual void SetUp() {
    TimezoneDatabase::tz_name_map_["UTC"] =
        make_shared<Timezone>(TimezoneDatabase::GetUtcTimezone());
    TimezoneDatabase::tz_name_map_["Etc/GMT"] =
        make_shared<Timezone>(TimezoneDatabase::GetUtcTimezone());
    TimezoneDatabase::tz_name_map_["Etc/GMT-0"] =
        make_shared<Timezone>(TimezoneDatabase::GetUtcTimezone());
    TimezoneDatabase::tz_name_map_["Zulu"] =
        make_shared<Timezone>(TimezoneDatabase::GetUtcTimezone());
  }

  virtual void TearDown() {
    TimezoneDatabase::tz_name_map_.clear();
  }

  void TestLoadZoneAliasesSuccess(const string& aliases,
      const vector<pair<string, string>>& expected_aliases,
      const vector<pair<string, int>>& expected_offsets) {
    stringstream ss(aliases, ios_base::in);
    Status status = TimezoneDatabase::LoadZoneAliases(ss);
    EXPECT_TRUE(status.ok());

    // Check aliases defined as existing time-zones.
    for (const auto& alias : expected_aliases) {
      const Timezone* tz1 = TimezoneDatabase::FindTimezone(alias.first);
      const Timezone* tz2 = TimezoneDatabase::FindTimezone(alias.second);
      EXPECT_TRUE(tz1 != nullptr && tz1 == tz2);
    }

    // Check aliases defined as UTC offsets.
    cctz::time_point<cctz::sys_seconds> epoch =
        std::chrono::time_point_cast<cctz::sys_seconds>(
             std::chrono::system_clock::from_time_t(0));
    for (const auto& offset : expected_offsets) {
      const Timezone* tz = TimezoneDatabase::FindTimezone(offset.first);
      EXPECT_TRUE(tz != nullptr && tz->lookup(epoch).offset == offset.second);
    }
  }

  void TestLoadZoneAliasesFailure(const string& aliases,
      const string& expected_err_msg) {
    stringstream ss(aliases, ios_base::in);
    Status status = TimezoneDatabase::LoadZoneAliases(ss);
    EXPECT_FALSE(status.ok());
    ASSERT_NE(string::npos, status.msg().msg().find(expected_err_msg));
  }
};

TEST_F(TimezoneDbLoadAliasTest, LoadZoneAliases) {
  TestLoadZoneAliasesSuccess(
      "# Test time-zone aliases\n"
      "# First = Zulu   # Entire line is commented out\n"
      "First = UTC \n"
      "Second =   Zulu ###  \n"
      "Second =   UTC  # This line will be skipped: Second is already defined\n"
      "Third  = Etc/GMT  \n"
      "Fourth =Etc/GMT-0 \n"
      "Fourth = 1234   # This line will be skipped: Fourth is already defined\n"
      "Fifth = ZULU    # This line will be skipped: value is an invalid time-zone name\n"
      "Fifth  =1234    # Alias defined as UTC offset in seconds\n"
      "# Aliases may contain non-alphabetic characters\n"
      "+00:00  = Etc/GMT-0  \n"
      "Coordinated Universal Time  = UTC\n",
      { make_pair("First", "UTC"), make_pair("Second", "Zulu"),
      make_pair("Third", "Etc/GMT"), make_pair("Fourth", "Etc/GMT-0"),
      make_pair("+00:00", "Etc/GMT-0"),
      make_pair("Coordinated Universal Time", "UTC") },
      { make_pair("Fifth", 1234) });

  // Value is commented out.
  TestLoadZoneAliasesFailure("UTC1= # Zulu\n", "Error in line 1. Missing value.");

  // '=' delimiter is missing.
  TestLoadZoneAliasesFailure("UTC2  Zulu\n", "Error in line 1. '=' is missing.");

  // Missing alias
  TestLoadZoneAliasesFailure("   = Zulu\n",
      "Error in line 1. Time-zone alias name is missing.");
}


class TimezoneDbLoadZoneInfoTest : public testing::Test {
 protected:
  void TestLoadZoneInfoSuccess(const string& path) {
    EXPECT_OK(TimezoneDatabase::LoadZoneInfo(path));
  }

  void TestFindTimezoneSuccess(const vector<string>& expected_names) {
    // Check that all the expected time-zones have been loaded.
    for (const auto& tz_name : expected_names) {
      ASSERT_NE(nullptr, TimezoneDatabase::FindTimezone(tz_name));
    }
  }

  void TestFindTimezoneFailure(const vector<string>& expected_names) {
    // Check that all the expected time-zones have been loaded.
    for (const auto& tz_name : expected_names) {
      ASSERT_EQ(nullptr, TimezoneDatabase::FindTimezone(tz_name));
    }
  }
};

TEST_F(TimezoneDbLoadZoneInfoTest, LoadZoneInfo) {
  const string& path = Substitute("$0/testdata/tzdb_tiny", getenv("IMPALA_HOME"));
  TestLoadZoneInfoSuccess(path);

  // Test that timezones containing non-alphabetic characters were loaded successfully.
  TestFindTimezoneSuccess({ "Etc/GMT+4" });

  // Test that symbolic links are handled properly.
  // "UTC" is a symbolic link to "Zulu". Both should be loaded.
  TestFindTimezoneSuccess({ "UTC", "Zulu" });
  // "America/New_York" and "US/Eastern" are symbolic links to "posixrules". It is
  // expected that "posixrules" is not loaded, but "America/New_York" and "US/Eastern"
  // are.
  TestFindTimezoneFailure({ "posixrules" });
  TestFindTimezoneSuccess({ "America/New_York", "US/Eastern" });

  // Test that "posix" directory was skipped.
  TestFindTimezoneFailure({ "posix/UTC" });
}

}
