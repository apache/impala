// Copyright (c) 2011 Cloudera, Inc. All rights reserved.

#include <stdlib.h>
#include <stdio.h>
#include <iostream>
#include <gtest/gtest.h>
#include "util/cpu-info.h"
#include "util/perf-counters.h"

using namespace std;

namespace impala {

TEST(PerfCounterTest, Basic) { 
  PerfCounters counters;
  EXPECT_TRUE(counters.AddDefaultCounters());

  counters.Snapshot("Before");

  double result = 0;
  for (int i = 0; i < 1000000; i++) {
    double d1 = rand() / (double) RAND_MAX;
    double d2 = rand() / (double) RAND_MAX;
    result = d1*d1 + d2*d2;
  }
  counters.Snapshot("After");

  for (int i = 0; i < 1000000; i++) {
    double d1 = rand() / (double) RAND_MAX;
    double d2 = rand() / (double) RAND_MAX;
    result = d1*d1 + d2*d2;
  }
  counters.Snapshot("After2");
  counters.PrettyPrint(&cout);
}

TEST(CpuInfoTest, Basic) {
  cout << CpuInfo::DebugString();
}

}

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  impala::CpuInfo::Init();
  return RUN_ALL_TESTS();
}

