// Copyright (c) 2012 Cloudera, Inc. All rights reserved.

#include <stdlib.h>
#include <stdio.h>
#include <iostream>
#include <gtest/gtest.h>
#include "runtime/raw-value.h"
#include "runtime/timestamp-value.h"

using namespace std;
using namespace boost;

namespace impala {

TEST(TimestampTest, Basic) {
  string s1("2012-01-20 01:10:01");
  string s2("1990-10-20 10:10:10.123456789");
  string s3("1990-10-20 10:10:10.123456789");
  TimestampValue v1(s1);
  TimestampValue v2(s2);
  TimestampValue v3(s3);

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
}

}

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  impala::CpuInfo::Init();
  return RUN_ALL_TESTS();
}

