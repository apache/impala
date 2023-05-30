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

#include "gutil/strings/substitute.h"

#include "common/names.h"

using namespace impala;

// Machine Info: Intel(R) Core(TM) i7-4790 CPU @ 3.60GHz
//
// Length 1:                  Function  iters/ms   10%ile   50%ile   90%ile     10%ile     50%ile     90%ile
//                                                                          (relative) (relative) (relative)
// ---------------------------------------------------------------------------------------------------------
//                       StringCompare           2.19e+05 2.32e+05 2.36e+05         1X         1X         1X
//                             strncmp           3.68e+05 3.83e+05  3.9e+05      1.68X      1.65X      1.65X
//                              memcmp           3.88e+05 4.01e+05 4.05e+05      1.77X      1.73X      1.72X
//
// Length 10:                 Function  iters/ms   10%ile   50%ile   90%ile     10%ile     50%ile     90%ile
//                                                                          (relative) (relative) (relative)
// ---------------------------------------------------------------------------------------------------------
//                       StringCompare           1.86e+05 1.89e+05 1.92e+05         1X         1X         1X
//                             strncmp           2.76e+05 2.78e+05  2.8e+05      1.48X      1.47X      1.46X
//                              memcmp           3.24e+05 3.27e+05  3.3e+05      1.75X      1.73X      1.72X
//
// Length 100:                Function  iters/ms   10%ile   50%ile   90%ile     10%ile     50%ile     90%ile
//                                                                          (relative) (relative) (relative)
// ---------------------------------------------------------------------------------------------------------
//                       StringCompare            4.9e+04 4.95e+04    5e+04         1X         1X         1X
//                             strncmp            9.5e+04 9.65e+04 9.77e+04      1.94X      1.95X      1.95X
//                              memcmp           1.69e+05 1.72e+05 1.74e+05      3.46X      3.47X      3.47X
//
// Length 10000:              Function  iters/ms   10%ile   50%ile   90%ile     10%ile     50%ile     90%ile
//                                                                          (relative) (relative) (relative)
// ---------------------------------------------------------------------------------------------------------
//                       StringCompare                640      642      643         1X         1X         1X
//                             strncmp           2.17e+03 2.18e+03  2.2e+03      3.39X       3.4X      3.42X
//                              memcmp           4.62e+03 4.64e+03 4.69e+03      7.22X      7.23X      7.29X

int StringCompare(const char* s1, int n1, const char* s2, int n2, int len) {
  DCHECK_EQ(len, std::min(n1, n2));
#ifdef __x86_64__
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
#endif
  // memcmp has undefined behavior when called on nullptr for either pointer
  int result = (len == 0) ? 0 : strncmp(s1, s2, len);
  if (result != 0) return result;
  return n1 - n2;
}

template<typename T, int COMPARE(const T*, const T*, size_t)>
int SimpleCompare(const char* s1, int n1, const char* s2, int n2, int len) {
  DCHECK_EQ(len, std::min(n1, n2));
  // memcmp has undefined behavior when called on nullptr for either pointer
  const int result = (len == 0) ? 0 : COMPARE(s1, s2, len);
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

template<int (*STRING_COMPARE)(const char* s1, int n1, const char* s2, int n2, int len)>
void TestStringCompare(int batch_size, void* d) {
  TestData* data = reinterpret_cast<TestData*>(d);
  const int len = std::min(data->n1, data->n2);
  data->result = 0;
  for (int i = 0; i < batch_size; ++i) {
    data->result += STRING_COMPARE(data->s1, data->n1, data->s2, data->n2, len);
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

void BenchmarkAll(int len) {
  Benchmark suite(Substitute("Length $0", len));
  TestData data = InitTestData(len);
  suite.AddBenchmark("StringCompare", TestStringCompare<StringCompare>, &data);
  suite.AddBenchmark("strncmp", TestStringCompare<SimpleCompare<char, strncmp>>, &data);
  suite.AddBenchmark("memcmp", TestStringCompare<SimpleCompare<void, memcmp>>, &data);
  cout << suite.Measure() << endl;
}

int main(int argc, char **argv) {
  CpuInfo::Init();
  cout << Benchmark::GetMachineInfo() << endl << endl;

  for (int len : {1, 10, 100, 10000}) {
    BenchmarkAll(len);
  }

  return 0;
}
