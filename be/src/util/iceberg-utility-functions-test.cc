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

#include "util/iceberg-utility-functions.h"

#include "testutil/gtest-util.h"

#include "common/names.h"

namespace impala {
namespace iceberg {

TEST(IcebergPartitions, HumanReadableYearTest) {
  EXPECT_EQ("1969", HumanReadableYear(-1));
  EXPECT_EQ("1970", HumanReadableYear(0));
  EXPECT_EQ("1971", HumanReadableYear(1));
  EXPECT_EQ("2000", HumanReadableYear(30));
  EXPECT_EQ("2023", HumanReadableYear(53));
}

TEST(IcebergPartitions, HumanReadableMonthTest) {
  EXPECT_EQ("1969-01", HumanReadableMonth(-12));
  EXPECT_EQ("1969-12", HumanReadableMonth(-1));
  EXPECT_EQ("1970-01", HumanReadableMonth(0));
  EXPECT_EQ("2000-01", HumanReadableMonth(360));
  EXPECT_EQ("2023-11", HumanReadableMonth(360+23*12+10));
}

TEST(IcebergPartitions, HumanReadableDayTest) {
  // Used https://www.timeanddate.com/ to verify the followings:
  EXPECT_EQ("1934-02-28", HumanReadableDay(-13091));
  EXPECT_EQ("1969-01-01", HumanReadableDay(-365));
  EXPECT_EQ("1969-12-30", HumanReadableDay(-2));
  EXPECT_EQ("1969-12-31", HumanReadableDay(-1));
  EXPECT_EQ("1970-01-01", HumanReadableDay(0));
  EXPECT_EQ("1970-07-24", HumanReadableDay(204));
  EXPECT_EQ("1987-10-06", HumanReadableDay(6487));
  EXPECT_EQ("1991-01-23", HumanReadableDay(7692));
  EXPECT_EQ("1994-05-06", HumanReadableDay(8891));
  EXPECT_EQ("2000-09-19", HumanReadableDay(11219));
  EXPECT_EQ("2023-11-11", HumanReadableDay(19672));
}

TEST(IcebergPartitions, HumanReadableHourTest) {
  // Used https://www.timeanddate.com/ to verify the followings:
  EXPECT_EQ("1934-02-28-10", HumanReadableHour(-314174));
  EXPECT_EQ("1969-01-01-01", HumanReadableHour(-365*24+1));
  EXPECT_EQ("1969-12-30-23", HumanReadableHour(-25));
  EXPECT_EQ("1969-12-31-23", HumanReadableHour(-1));
  EXPECT_EQ("1970-01-01-00", HumanReadableHour(0));
  EXPECT_EQ("1970-01-01-01", HumanReadableHour(1));
  EXPECT_EQ("1970-01-01-02", HumanReadableHour(2));
  EXPECT_EQ("1970-07-24-13", HumanReadableHour(4909));
  EXPECT_EQ("1971-01-01-00", HumanReadableHour(365*24));
  EXPECT_EQ("1987-10-06-18", HumanReadableHour(155706));
  EXPECT_EQ("1991-01-23-08", HumanReadableHour(184616));
  EXPECT_EQ("1994-05-06-11", HumanReadableHour(213395));
  EXPECT_EQ("2000-09-19-13", HumanReadableHour(269269));
  EXPECT_EQ("2023-11-11-22", HumanReadableHour(472150));
}

TEST(IcebergPartitions, HumanReadableTimeTest) {
  Status st;
  EXPECT_EQ("1967", HumanReadableTime(
      TIcebergPartitionTransformType::YEAR, "-3", &st));
  EXPECT_OK(st);
  EXPECT_EQ("", HumanReadableTime(
      TIcebergPartitionTransformType::YEAR, "invalid", &st));
  EXPECT_ERROR(st, TErrorCode::GENERAL);
  EXPECT_EQ("Failed to parse time partition value 'invalid' as int.\n", st.GetDetail());
  EXPECT_EQ("1977", HumanReadableTime(
      TIcebergPartitionTransformType::YEAR, "7", &st));
  EXPECT_OK(st);
  // 1970-01 + 7 months is 1970-08
  EXPECT_EQ("1970-08", HumanReadableTime(
      TIcebergPartitionTransformType::MONTH, "7", &st));
  EXPECT_OK(st);
  EXPECT_EQ("1970-01-08", HumanReadableTime(
      TIcebergPartitionTransformType::DAY, "7", &st));
  EXPECT_OK(st);
  EXPECT_EQ("1970-01-01-07", HumanReadableTime(
      TIcebergPartitionTransformType::HOUR, "7", &st));
  EXPECT_OK(st);
}

}
}
