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

#include <stdlib.h>
#include <stdio.h>
#include <iostream>
#include <gtest/gtest.h>
#include "runtime/raw-value.h"
#include "runtime/timestamp-value.h"

using namespace std;
using namespace boost;
using namespace boost::gregorian;

namespace impala {

TEST(TimestampTest, Basic) {
  char s1[] = "2012-01-20 01:10:01";
  char s2[] = "1990-10-20 10:10:10.123456789  ";
  char s3[] = "  1990-10-20 10:10:10.123456789";
  TimestampValue v1(s1, strlen(s1));
  TimestampValue v2(s2, strlen(s2));
  TimestampValue v3(s3, strlen(s3));

  EXPECT_EQ(v1.date().year(), 2012);
  EXPECT_EQ(v1.date().month(), 1);
  EXPECT_EQ(v1.date().day(), 20);
  EXPECT_EQ(v1.time_of_day().hours(), 1);
  EXPECT_EQ(v1.time_of_day().minutes(), 10);
  EXPECT_EQ(v1.time_of_day().seconds(), 1);
  EXPECT_EQ(v1.time_of_day().fractional_seconds(), 0);
  EXPECT_EQ(v2.time_of_day().fractional_seconds(), 123456789);

  EXPECT_NE(v1, v2);
  EXPECT_EQ(v2, v3);
  EXPECT_LT(v2, v1);
  EXPECT_LE(v2, v1);
  EXPECT_GT(v1, v2);
  EXPECT_GE(v2, v3);

  EXPECT_NE(RawValue::GetHashValue(&v1, TYPE_TIMESTAMP, 0),
            RawValue::GetHashValue(&v2, TYPE_TIMESTAMP, 0));
  EXPECT_EQ(RawValue::GetHashValue(&v3, TYPE_TIMESTAMP, 0),
            RawValue::GetHashValue(&v2, TYPE_TIMESTAMP, 0));

  // Test Dates and Times as timestamps.
  char d1[] = "2012-01-20";
  char d2[] = "1990-10-20";
  TimestampValue dv1(d1, strlen(d1));
  TimestampValue dv2(d2, strlen(d2));

  EXPECT_NE(dv1, dv2);
  EXPECT_LT(dv1, v1);
  EXPECT_LE(dv1, v1);
  EXPECT_GT(v1, dv1);
  EXPECT_GE(v1, dv1);
  EXPECT_NE(dv2, s2);

  EXPECT_EQ(dv1.date().year(), 2012);
  EXPECT_EQ(dv1.date().month(), 1);
  EXPECT_EQ(dv1.date().day(), 20);

  char t1[] = "10:11:12.123456789";
  char t2[] = "00:00:00";
  TimestampValue tv1(t1, strlen(t1));
  TimestampValue tv2(t2, strlen(t2));

  EXPECT_NE(tv1, tv2);
  EXPECT_NE(tv1, v2);

  EXPECT_EQ(tv1.time_of_day().hours(), 10);
  EXPECT_EQ(tv1.time_of_day().minutes(), 11);
  EXPECT_EQ(tv1.time_of_day().seconds(), 12);
  EXPECT_EQ(tv1.time_of_day().fractional_seconds(), 123456789);
  EXPECT_EQ(tv2.time_of_day().fractional_seconds(), 0);

  // Bad formats
  char b1[] = "1990-10 10:10:10.123456789";
  TimestampValue bv1(b1, strlen(b1));
  boost::gregorian::date not_a_date;

  EXPECT_EQ(bv1.date(), not_a_date);
  EXPECT_EQ(bv1.time_of_day(), not_a_date_time);

  char b2[] = "1991-10-10 99:10:10.123456789";
  TimestampValue bv2(b2, strlen(b2));

  EXPECT_EQ(bv2.time_of_day(), not_a_date_time);
  EXPECT_EQ(bv2.date(), not_a_date);

  char b3[] = "1990-10- 10:10:10.123456789";
  TimestampValue bv3(b3, strlen(b3));

  EXPECT_EQ(bv3.date(), not_a_date);
  EXPECT_EQ(bv3.time_of_day(), not_a_date_time);

  char b4[] = "10:1010.123456789";
  TimestampValue bv4(b4, strlen(b4));

  EXPECT_EQ(bv4.date(), not_a_date);
  EXPECT_EQ(bv4.time_of_day(), not_a_date_time);

  char b5[] = "10:11:12.123456 1991-10-10";
  TimestampValue bv5(b5, strlen(b5));

  EXPECT_EQ(bv5.date(), not_a_date);
  EXPECT_EQ(bv5.time_of_day(), not_a_date_time);

  char b6[] = "2012-01-20 01:10:00.123.466";
  TimestampValue bv6(b6, strlen(b6));

  EXPECT_EQ(bv6.date(), not_a_date);
  EXPECT_EQ(bv6.time_of_day(), not_a_date_time);

  char b7[] = "2012-01-20 01:10:00.123 477 ";
  TimestampValue bv7(b7, strlen(b7));

  EXPECT_EQ(bv7.date(), not_a_date);
  EXPECT_EQ(bv7.time_of_day(), not_a_date_time);
}

}

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  impala::CpuInfo::Init();
  return RUN_ALL_TESTS();
}
