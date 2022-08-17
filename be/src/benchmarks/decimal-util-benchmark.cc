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

#include <iostream>
#include <vector>

#include "util/benchmark.h"
#include "util/cpu-info.h"
#include "util/decimal-util.h"

// Summary result (ran within a Docker container):
//
// Machine Info: AMD Ryzen 9 5950X 16-Core Processor
// ScaleMultiplierBenchmark:  Function  iters/ms   10%ile   50%ile   90%ile     10%ile     50%ile     90%ile
//                                                                          (relative) (relative) (relative)
// ---------------------------------------------------------------------------------------------------------
//                                 raw              0.852    0.852    0.852         1X         1X         1X
//                         specialized           4.71e+03 4.76e+03 4.81e+03  5.53e+03X  5.59e+03X  5.64e+03X

using namespace impala;

namespace int256_scale_multiplier {

void AddTestData(vector<int>* data, int n) {
  for (int i = 0; i < n; i++) {
    int m = rand() % DecimalUtil::INT256_SCALE_UPPER_BOUND;
    data->push_back(m);
  }
}

/// Copied from Decimal::GetScaleMultiplier
static int256_t GetScaleMultiplier(int scale) {
  int256_t result = 1;
  for (int i = 0; i < scale; ++i) {
    result *= 10;
  }
  return result;
}

// A volatile variable to hold the return value of the benchmarked functions.
// Used to prevent the compiler from optimising out the calls.
static volatile int64_t volatile_var = 0;

static void TestRawTemplateCall(int batch_size, void* d) {
  vector<int>* data = reinterpret_cast<vector<int>*>(d);
  for (int i = 0; i < batch_size; i++) {
    for (int j = 0; j < data->size(); j++) {
      auto m = GetScaleMultiplier(j);
      volatile_var = m.convert_to<int64_t>();
    }
  }
}

static void TestSpecializedTemplateCall(int batch_size, void* d) {
  vector<int>* data = reinterpret_cast<vector<int>*>(d);
  for (int i = 0; i < batch_size; i++) {
    for (int j = 0; j < data->size(); j++) {
      auto m = DecimalUtil::GetScaleMultiplier<int256_t>(j);
      volatile_var = m.convert_to<int64_t>();
    }
  }
}

} // namespace int256_scale_multiplier

int main(int argc, char** argv) {
  CpuInfo::Init();
  std::cout << Benchmark::GetMachineInfo() << std::endl;
  vector<int> data;
  int256_scale_multiplier::AddTestData(&data, 1000);
  Benchmark int256_scale_multiplier_suite("ScaleMultiplierBenchmark");
  int256_scale_multiplier_suite.AddBenchmark(
      "raw", int256_scale_multiplier::TestRawTemplateCall, &data);
  int256_scale_multiplier_suite.AddBenchmark(
      "specialized", int256_scale_multiplier::TestSpecializedTemplateCall, &data);
  std::cout << int256_scale_multiplier_suite.Measure() << std::endl;
  return 0;
}
