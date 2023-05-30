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
#include <stdlib.h>

#include <algorithm>
#include <iostream>
#include <memory>

#include "exec/parquet/parquet-common.h"
#include "gutil/strings/substitute.h"
#include "runtime/decimal-value.h"
#include "testutil/mem-util.h"
#include "util/benchmark.h"
#include "util/bit-util.h"
#include "util/cpu-info.h"

#include "common/names.h"

using std::numeric_limits;
using namespace impala;

// This benchmark is to compare the performance for all available byteswap approaches:
// 1. FastScalar: use the ByteSwapScalar routine in bit-util.inline.h to byte-swap
// the input array with subdivided byte sizes, which is proposed by Zuo Wang.
// 2. SSSE3: use the SSSE3 SIMD routine to byte-swap the input array
// without arch-selector branches;
// 3. AVX2: use the AVX2 SIMD routine to byte-swap the input array
// without arch-selector branches;
// 4. SIMD: use the comprehensive SIMD routine to byte-swap the input array
// with arch-selector branches;
//
// The benchmark is executed on both aligned and misaligned memory.
//
// Result:
// I0901 15:00:40.777019 21251 bswap-benchmark.cc:164] Machine Info: Intel(R) Core(TM) i7-4790 CPU @ 3.60GHz
// ByteSwap benchmark misalignment=0:Function  iters/ms   10%ile   50%ile   90%ile     10%ile     50%ile     90%ile
//                                                                          (relative) (relative) (relative)
// ---------------------------------------------------------------------------------------------------------
//                          FastScalar                940 1.06e+03 1.08e+03         1X         1X         1X
//                               SSSE3           8.36e+03  9.8e+03 9.97e+03       8.9X      9.27X      9.26X
//                                AVX2           2.57e+04 3.73e+04  3.8e+04      27.3X      35.3X      35.3X
//                                SIMD            2.9e+04 3.72e+04  3.8e+04      30.8X      35.2X      35.3X
// ByteSwap benchmark misalignment=1:Function  iters/ms   10%ile   50%ile   90%ile     10%ile     50%ile     90%ile
//                                                                          (relative) (relative) (relative)
// ---------------------------------------------------------------------------------------------------------
//                          FastScalar                815 1.01e+03 1.07e+03         1X         1X         1X
//                               SSSE3           5.97e+03 8.42e+03 8.97e+03      7.32X      8.35X      8.38X
//                                AVX2           1.83e+04 2.52e+04 2.77e+04      22.5X        25X      25.9X
//                                SIMD           1.78e+04 2.63e+04 2.75e+04      21.8X      26.1X      25.7X
// ByteSwap benchmark misalignment=4:Function  iters/ms   10%ile   50%ile   90%ile     10%ile     50%ile     90%ile
//                                                                          (relative) (relative) (relative)
// ---------------------------------------------------------------------------------------------------------
//                          FastScalar           1.04e+03 1.08e+03 1.12e+03         1X         1X         1X
//                               SSSE3           7.81e+03 8.97e+03 9.09e+03       7.5X      8.33X      8.09X
//                                AVX2           2.47e+04 2.76e+04  2.8e+04      23.7X      25.7X      24.9X
//                                SIMD           2.62e+04 2.77e+04 2.79e+04      25.2X      25.7X      24.9X
// ByteSwap benchmark misalignment=8:Function  iters/ms   10%ile   50%ile   90%ile     10%ile     50%ile     90%ile
//                                                                          (relative) (relative) (relative)
// ---------------------------------------------------------------------------------------------------------
//                          FastScalar                989 1.08e+03 1.14e+03         1X         1X         1X
//                               SSSE3           8.06e+03 9.01e+03 9.13e+03      8.15X      8.37X      8.02X
//                                AVX2           2.24e+04 2.77e+04 2.81e+04      22.7X      25.8X      24.7X
//                                SIMD           2.42e+04 2.77e+04  2.8e+04      24.4X      25.7X      24.6X

// Data structure used in the benchmark;
struct TestData {
  int32_t num_values;
  uint8_t* inbuffer;
  uint8_t* outbuffer;
};

// Initialization routine for benchmark data;
void InitData(uint8_t* input, const int len) {
  srand(time(NULL));
  for (int i = 0; i < len; ++i) {
    input[i] = rand() % 256;
  }
}

// Test for the scalar approach;
void TestFastScalarSwap(int batch_size, void* d) {
  TestData* data = reinterpret_cast<TestData*>(d);
  SimdByteSwap::ByteSwapScalar(data->inbuffer, data->num_values, data->outbuffer);
}

// Test for the 16-byte SIMD;
void TestSIMD16Swap(int batch_size, void* d) {
  TestData* data = reinterpret_cast<TestData*>(d);
  SimdByteSwap::ByteSwapSimd<16>(data->inbuffer, data->num_values, data->outbuffer);
}

// Test for the 32-byte SIMD;
void TestSIMD32Swap(int batch_size, void* d) {
  TestData* data = reinterpret_cast<TestData*>(d);
  SimdByteSwap::ByteSwapSimd<32>(data->inbuffer, data->num_values, data->outbuffer);
}

// Test for the SIMD approach in a general way;
void TestSIMDSwap(int batch_size, void* d) {
  TestData* data = reinterpret_cast<TestData*>(d);
  BitUtil::ByteSwap(data->outbuffer, data->inbuffer, data->num_values);
}

// Benchmark routine for FastScalar/"Pure" SSSE3/"Pure" AVX2/SIMD approaches
void PerfBenchmark() {
  // Measure perf both when memory is perfectly aligned for SIMD and also misaligned.
  const int max_misalignment = 8;
  const vector<int> misalignments({0, 1, 4, max_misalignment});
  const int data_len = 1 << 20;

  AlignedAllocation inbuffer(data_len + max_misalignment);
  AlignedAllocation outbuffer(data_len + max_misalignment);

  for (const int misalign : misalignments) {
    Benchmark suite(Substitute("ByteSwap benchmark misalignment=$0", misalign));
    TestData data;

    data.num_values = data_len;
    data.inbuffer = inbuffer.data() + misalign;
    data.outbuffer = outbuffer.data() + misalign;
    InitData(data.inbuffer, data_len);

    const int baseline = suite.AddBenchmark("FastScalar", TestFastScalarSwap, &data, -1);
    suite.AddBenchmark("SIMD16", TestSIMD16Swap, &data, baseline);
    suite.AddBenchmark("SIMD32", TestSIMD32Swap, &data, baseline);
    suite.AddBenchmark("SIMD", TestSIMDSwap, &data, baseline);
    cout << suite.Measure();
  }
}

int main(int argc, char **argv) {
  CpuInfo::Init();
  LOG(INFO) << Benchmark::GetMachineInfo();
  PerfBenchmark();
  return 0;
}
