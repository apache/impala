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

// Utility class to expose private functions for testing
class BenchmarkTest {
 public:
  static double Measure(Benchmark::BenchmarkFunction fn, void* data) {
    return Benchmark::Measure(fn, data);
  };
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
  double rate_copy_16 = BenchmarkTest::Measure(TestFunction, &data);

  data.size = 128;
  double rate_copy_128 = BenchmarkTest::Measure(TestFunction, &data);

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

