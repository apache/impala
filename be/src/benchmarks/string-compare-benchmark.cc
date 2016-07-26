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
#include <stdio.h>
#include <iostream>
#include "runtime/string-value.h"
#include "util/benchmark.h"
#include "util/cpu-info.h"
#include "util/sse-util.h"

#include "common/names.h"

using namespace impala;

// Machine Info: Intel(R) Core(TM) i7-4790 CPU @ 3.60GHz
//
// Long strings (10000):      Function  iters/ms   10%ile   50%ile   90%ile     10%ile     50%ile     90%ile
//                                                                          (relative) (relative) (relative)
// ---------------------------------------------------------------------------------------------------------
//                            Original                 85     86.5     86.5         1X         1X         1X
//                  Simplified, broken               76.6       78       78     0.901X     0.901X     0.901X
//                   Simplified, fixed               95.8     97.5     97.5      1.13X      1.13X      1.13X
//
// Med strings (100):         Function  iters/ms   10%ile   50%ile   90%ile     10%ile     50%ile     90%ile
//                                                                          (relative) (relative) (relative)
// ---------------------------------------------------------------------------------------------------------
//                            Original           6.55e+03 6.66e+03 6.74e+03         1X         1X         1X
//                  Simplified, broken           6.25e+03 6.32e+03 6.38e+03     0.955X     0.949X     0.947X
//                   Simplified, fixed           7.38e+03 7.49e+03 7.55e+03      1.13X      1.12X      1.12X
//
// Short strings (10):        Function  iters/ms   10%ile   50%ile   90%ile     10%ile     50%ile     90%ile
//                                                                          (relative) (relative) (relative)
// ---------------------------------------------------------------------------------------------------------
//                            Original           1.59e+04 1.62e+04 1.63e+04         1X         1X         1X
//                  Simplified, broken            2.8e+04 2.85e+04 2.87e+04      1.76X      1.76X      1.76X
//                   Simplified, fixed           2.92e+04 2.96e+04 2.99e+04      1.83X      1.83X      1.84X

// Original
int StringCompare1(const char* s1, int n1, const char* s2, int n2, int len) {
  DCHECK_EQ(len, std::min(n1, n2));
  if (CpuInfo::IsSupported(CpuInfo::SSE4_2)) {
    while (len >= SSEUtil::CHARS_PER_128_BIT_REGISTER) {
      __m128i xmm0 = _mm_loadu_si128(reinterpret_cast<const __m128i*>(s1));
      __m128i xmm1 = _mm_loadu_si128(reinterpret_cast<const __m128i*>(s2));
      int chars_match = SSE4_cmpestri<SSEUtil::STRCMP_MODE>(xmm0,
          SSEUtil::CHARS_PER_128_BIT_REGISTER, xmm1, SSEUtil::CHARS_PER_128_BIT_REGISTER);
      if (chars_match != SSEUtil::CHARS_PER_128_BIT_REGISTER) {
        return s1[chars_match] - s2[chars_match];
      }
      len -= SSEUtil::CHARS_PER_128_BIT_REGISTER;
      s1 += SSEUtil::CHARS_PER_128_BIT_REGISTER;
      s2 += SSEUtil::CHARS_PER_128_BIT_REGISTER;
    }
    if (len >= SSEUtil::CHARS_PER_64_BIT_REGISTER) {
      // Load 64 bits at a time, the upper 64 bits of the xmm register is set to 0
      __m128i xmm0 = _mm_loadl_epi64(reinterpret_cast<const __m128i*>(s1));
      __m128i xmm1 = _mm_loadl_epi64(reinterpret_cast<const __m128i*>(s2));
      // The upper bits always match (always 0), hence the comparison to
      // CHAR_PER_128_REGISTER
      int chars_match = SSE4_cmpestri<SSEUtil::STRCMP_MODE>(xmm0,
          SSEUtil::CHARS_PER_128_BIT_REGISTER, xmm1, SSEUtil::CHARS_PER_128_BIT_REGISTER);
      if (chars_match != SSEUtil::CHARS_PER_128_BIT_REGISTER) {
        return s1[chars_match] - s2[chars_match];
      }
      len -= SSEUtil::CHARS_PER_64_BIT_REGISTER;
      s1 += SSEUtil::CHARS_PER_64_BIT_REGISTER;
      s2 += SSEUtil::CHARS_PER_64_BIT_REGISTER;
    }
  }
  // TODO: for some reason memcmp is way slower than strncmp (2.5x)  why?
  int result = strncmp(s1, s2, len);
  if (result != 0) return result;
  return n1 - n2;
}

// Simplified but broken (can't safely load s1 and s2)
int StringCompare2(const char* s1, int n1, const char* s2, int n2, int len) {
  DCHECK_EQ(len, std::min(n1, n2));
  if (CpuInfo::IsSupported(CpuInfo::SSE4_2)) {
    while (len > 0) {
      __m128i xmm0 = _mm_loadu_si128(reinterpret_cast<const __m128i*>(s1));
      __m128i xmm1 = _mm_loadu_si128(reinterpret_cast<const __m128i*>(s2));
      int n = std::min(len, 16);
      int chars_match = SSE4_cmpestri<SSEUtil::STRCMP_MODE>(xmm0, n, xmm1, n);
      if (chars_match != SSEUtil::CHARS_PER_128_BIT_REGISTER) {
        return s1[chars_match] - s2[chars_match];
      }
      len -= SSEUtil::CHARS_PER_128_BIT_REGISTER;
      s1 += SSEUtil::CHARS_PER_128_BIT_REGISTER;
      s2 += SSEUtil::CHARS_PER_128_BIT_REGISTER;
    }
    return n1 - n2;
  }
  // TODO: for some reason memcmp is way slower than strncmp (2.5x)  why?
  int result = strncmp(s1, s2, len);
  if (result != 0) return result;
  return n1 - n2;
}

// Simplified and not broken
int StringCompare3(const char* s1, int n1, const char* s2, int n2, int len) {
  DCHECK_EQ(len, std::min(n1, n2));
  if (CpuInfo::IsSupported(CpuInfo::SSE4_2)) {
    while (len >= SSEUtil::CHARS_PER_128_BIT_REGISTER) {
      __m128i xmm0 = _mm_loadu_si128(reinterpret_cast<const __m128i*>(s1));
      __m128i xmm1 = _mm_loadu_si128(reinterpret_cast<const __m128i*>(s2));
      int chars_match = SSE4_cmpestri<SSEUtil::STRCMP_MODE>(xmm0,
          SSEUtil::CHARS_PER_128_BIT_REGISTER, xmm1, SSEUtil::CHARS_PER_128_BIT_REGISTER);
      if (chars_match != SSEUtil::CHARS_PER_128_BIT_REGISTER) {
        return s1[chars_match] - s2[chars_match];
      }
      len -= SSEUtil::CHARS_PER_128_BIT_REGISTER;
      s1 += SSEUtil::CHARS_PER_128_BIT_REGISTER;
      s2 += SSEUtil::CHARS_PER_128_BIT_REGISTER;
    }
  }
  // TODO: for some reason memcmp is way slower than strncmp (2.5x)  why?
  int result = strncmp(s1, s2, len);
  if (result != 0) return result;
  return n1 - n2;
}

struct TestData {
  char* s1;
  int n1;
  char* s2;
  int n2;
  int result;
};

void TestStringCompare1(int batch_size, void* d) {
  TestData* data = reinterpret_cast<TestData*>(d);
  int len = std::min(data->n1, data->n2);
  for (int i = 0; i < batch_size; ++i) {
    data->result = StringCompare1(data->s1, data->n1, data->s2, data->n2, len);
    data->result = StringCompare1(data->s1, data->n1, data->s2, data->n2, len);
    data->result = StringCompare1(data->s1, data->n1, data->s2, data->n2, len);
    data->result = StringCompare1(data->s1, data->n1, data->s2, data->n2, len);
    data->result = StringCompare1(data->s1, data->n1, data->s2, data->n2, len);
    data->result = StringCompare1(data->s1, data->n1, data->s2, data->n2, len);
    data->result = StringCompare1(data->s1, data->n1, data->s2, data->n2, len);
    data->result = StringCompare1(data->s1, data->n1, data->s2, data->n2, len);
    data->result = StringCompare1(data->s1, data->n1, data->s2, data->n2, len);
  }
}

void TestStringCompare2(int batch_size, void* d) {
  TestData* data = reinterpret_cast<TestData*>(d);
  int len = std::min(data->n1, data->n2);
  for (int i = 0; i < batch_size; ++i) {
    data->result = StringCompare2(data->s1, data->n1, data->s2, data->n2, len);
    data->result = StringCompare2(data->s1, data->n1, data->s2, data->n2, len);
    data->result = StringCompare2(data->s1, data->n1, data->s2, data->n2, len);
    data->result = StringCompare2(data->s1, data->n1, data->s2, data->n2, len);
    data->result = StringCompare2(data->s1, data->n1, data->s2, data->n2, len);
    data->result = StringCompare2(data->s1, data->n1, data->s2, data->n2, len);
    data->result = StringCompare2(data->s1, data->n1, data->s2, data->n2, len);
    data->result = StringCompare2(data->s1, data->n1, data->s2, data->n2, len);
    data->result = StringCompare2(data->s1, data->n1, data->s2, data->n2, len);
  }
}

void TestStringCompare3(int batch_size, void* d) {
  TestData* data = reinterpret_cast<TestData*>(d);
  int len = std::min(data->n1, data->n2);
  for (int i = 0; i < batch_size; ++i) {
    data->result = StringCompare3(data->s1, data->n1, data->s2, data->n2, len);
    data->result = StringCompare3(data->s1, data->n1, data->s2, data->n2, len);
    data->result = StringCompare3(data->s1, data->n1, data->s2, data->n2, len);
    data->result = StringCompare3(data->s1, data->n1, data->s2, data->n2, len);
    data->result = StringCompare3(data->s1, data->n1, data->s2, data->n2, len);
    data->result = StringCompare3(data->s1, data->n1, data->s2, data->n2, len);
    data->result = StringCompare3(data->s1, data->n1, data->s2, data->n2, len);
    data->result = StringCompare3(data->s1, data->n1, data->s2, data->n2, len);
    data->result = StringCompare3(data->s1, data->n1, data->s2, data->n2, len);
  }
}

TestData InitTestData(int len) {
  TestData data;
  data.s1 = new char[len];
  data.s2 = new char[len];
  data.n1 = data.n2 = len;
  for (int i = 0; i < len; ++i) {
    data.s1[i] = data.s2[i] = 'a';
  }
  return data;
}

int main(int argc, char **argv) {
  CpuInfo::Init();
  cout << Benchmark::GetMachineInfo() << endl << endl;

  Benchmark long_suite("Long strings (10000)");
  TestData long_data = InitTestData(10000);
  long_suite.AddBenchmark("Original", TestStringCompare1, &long_data);
  long_suite.AddBenchmark("Simplified, broken", TestStringCompare2, &long_data);
  long_suite.AddBenchmark("Simplified, fixed", TestStringCompare3, &long_data);
  cout << long_suite.Measure() << endl;

  Benchmark med_suite("Med strings (100)");
  TestData med_data = InitTestData(100);
  med_suite.AddBenchmark("Original", TestStringCompare1, &med_data);
  med_suite.AddBenchmark("Simplified, broken", TestStringCompare2, &med_data);
  med_suite.AddBenchmark("Simplified, fixed", TestStringCompare3, &med_data);
  cout << med_suite.Measure() << endl;

  Benchmark short_suite("Short strings (10)");
  TestData short_data = InitTestData(10);
  short_suite.AddBenchmark("Original", TestStringCompare1, &short_data);
  short_suite.AddBenchmark("Simplified, broken", TestStringCompare2, &short_data);
  short_suite.AddBenchmark("Simplified, fixed", TestStringCompare3, &short_data);
  cout << short_suite.Measure() << endl;

  return 0;
}
