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
#include <algorithm>
#include <stdlib.h>
#include <immintrin.h>

#include "exec/parquet-common.h"
#include "runtime/decimal-value.h"
#include "util/benchmark.h"
#include "util/cpu-info.h"
#include "util/bit-util.h"

#include "common/names.h"

using std::numeric_limits;
using namespace impala;

// This benchmark is to compare the performance for all available byteswap approaches:
// 1. OldImpala: use the old Impala routine to byte-swap the input array.
// Corresponding performance is used as the baseline.
// 2. FastScalar: use the ByteSwapScalar routine in bit-util.inline.h to byte-swap
// the input array with subdivided byte sizes, which is proposed by Zuo Wang.
// 3. SSSE3: use the SSSE3 SIMD routine to byte-swap the input array
// without arch-selector branches;
// 4. AVX2: use the AVX2 SIMD routine to byte-swap the input array
// without arch-selector branches;
// 5. SIMD: use the comprehensive SIMD routine to byte-swap the input array
// with arch-selector branches;
// Result:
// I0725 20:47:02.402506  2078 bswap-benchmark.cc:117] Machine Info: Intel(R) Core(TM) i5-4460  CPU @ 3.20GHz
// ByteSwap benchmark:        Function  iters/ms   10%ile   50%ile   90%ile     10%ile     50%ile     90%ile
//                                                                          (relative) (relative) (relative)
// ---------------------------------------------------------------------------------------------------------
//                          FastScalar                675      725      731         1X         1X         1X
//                               SSSE3           6.12e+03  6.2e+03 6.23e+03      9.06X      8.55X      8.53X
//                                AVX2           1.87e+04 1.88e+04 1.89e+04      27.7X      25.9X      25.9X
//                                SIMD           1.82e+04 1.88e+04 1.89e+04        27X      25.9X      25.9X


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

// Test for the SSSE3 subroutine;
void TestSSSE3Swap(int batch_size, void* d) {
  TestData* data = reinterpret_cast<TestData*>(d);
  SimdByteSwap::ByteSwapSimd<16>(data->inbuffer, data->num_values, data->outbuffer);
}

// Test for the AVX2 subroutine;
void TestAVX2Swap(int batch_size, void* d) {
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
  const int data_len = 1 << 20;
  Benchmark suite("ByteSwap benchmark");
  vector<uint8_t> inbuffer_vector(data_len, 0);
  vector<uint8_t> outbuffer_vector(data_len, 0);
  TestData data;

  data.num_values = data_len;
  data.inbuffer = &inbuffer_vector[0];
  data.outbuffer = &outbuffer_vector[0];
  InitData(data.inbuffer, data_len);

  const int baseline = suite.AddBenchmark("FastScalar", TestFastScalarSwap, &data, -1);
  suite.AddBenchmark("SSSE3", TestSSSE3Swap, &data, baseline);
  suite.AddBenchmark("AVX2", TestAVX2Swap, &data, baseline);
  suite.AddBenchmark("SIMD", TestSIMDSwap, &data, baseline);
  cout << suite.Measure();
}

int main(int argc, char **argv) {
  CpuInfo::Init();
  LOG(INFO) << Benchmark::GetMachineInfo();
  PerfBenchmark();
  return 0;
}
