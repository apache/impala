// Copyright (c) 2012 Cloudera, Inc. All rights reserved.

#include <stdlib.h>
#include <stdio.h>
#include <iostream>
#include <gtest/gtest.h>
#include "common/object-pool.h"
#include "util/benchmark.h"
#include "util/cpu-info.h"

using namespace std;

// This is not much of a test but demonstrates how to use the Benchmark
// utility.
namespace impala {

struct MemcpyData {
  char* src;
  char* dst;
  int size;
};

void TestFunction(int batch_size, void* d) {
  MemcpyData* data = reinterpret_cast<MemcpyData*>(d);
  for (int i = 0; i < batch_size; ++i) {
    memcpy(data->dst, data->src, data->size);
  }
}

TEST(BenchmarkTest, Basic) { 
  MemcpyData data;
  data.src = reinterpret_cast<char*>(malloc(128));
  data.dst = reinterpret_cast<char*>(malloc(128));

  data.size = 16;
  double rate_copy_16 = Benchmark::Measure(TestFunction, &data);

  data.size = 128;
  double rate_copy_128 = Benchmark::Measure(TestFunction, &data);

  cout << "Rate 16 Byte: " << rate_copy_16 << endl;
  cout << "Rate 128 Byte: " << rate_copy_128 << endl;

  ASSERT_LT(rate_copy_128, rate_copy_16);

  free(data.src);
  free(data.dst);
}

}

int main(int argc, char **argv) {
  impala::CpuInfo::Init();
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}

