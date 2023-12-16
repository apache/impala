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
#include <vector>
#include <sstream>
#include "runtime/string-value.h"
#include "util/benchmark.h"
#include "util/cpu-info.h"
#include "util/string-parser.h"

#include "common/names.h"

using namespace impala;

// Benchmark for doing atoi.  This benchmark compares various implementations
// to convert string to int32s.  The data is mostly positive, relatively small
// numbers.
//
// Machine Info: Intel(R) Core(TM) i7-2600 CPU @ 3.40GHz
// atoi:                 Function     Rate (iters/ms)          Comparison
// ----------------------------------------------------------------------
//                         strtol               59.71                  1X
//                           atoi               59.87              1.003X
//                         impala               162.6              2.723X
//                  impala_unsafe               200.9              3.365X
//                impala_unrolled               176.5              2.956X
//                   impala_cased               232.1              3.887X

#define VALIDATE 0

#if VALIDATE
#define VALIDATE_RESULT(actual, expected, str) \
  if (actual != expected) { \
    cout << "Parse Error. " \
         << "String: " << str \
         << ". Parsed: " << actual << endl; \
    exit(-1); \
  }
#else
#define VALIDATE_RESULT(actual, expected, str)
#endif


struct TestData {
  vector<StringValue> data;
  vector<string> memory;
  vector<int32_t> result;
};

void AddTestData(TestData* data, const string& input) {
  data->memory.push_back(input);
  const string& str = data->memory.back();
  data->data.push_back(StringValue(const_cast<char*>(str.c_str()), str.length()));
}

void AddTestData(TestData* data, int n, int32_t min = -10, int32_t max = 10,
                 bool leading_space = false, bool trailing_space = false) {
  for (int i = 0; i < n; ++i) {
    double val = rand();
    val /= RAND_MAX;
    val = static_cast<int32_t>((val * (max - min)) + min);
    stringstream ss;
    if (leading_space) ss << "   ";
    ss << val;
    if (trailing_space) ss << "   ";
    AddTestData(data, ss.str());
  }
}

#define DIGIT(c) (c -'0')

inline int32_t AtoiUnsafe(const char* s, int len) {
  int32_t val = 0;
  bool negative = false;
  int i = 0;
  switch (*s) {
    case '-':
      negative = true;
      [[fallthrough]];
    case '+': ++i;
  }

  for (; i < len; ++i) {
    val = val * 10 + DIGIT(s[i]);
  }

  return negative ? -val : val;
}

inline int32_t AtoiUnrolled(const char* s, int len) {
  if (LIKELY(len <= 8)) {
    int32_t val = 0;
    bool negative = false;
    switch (*s) {
      case '-':
        negative = true;
        [[fallthrough]];
      case '+': --len; ++s;
    }

    switch(len) {
      case 8:
        val += (DIGIT(s[len - 8])) * 10000;
        [[fallthrough]];
      case 7:
        val += (DIGIT(s[len - 7])) * 10000;
        [[fallthrough]];
      case 6:
        val += (DIGIT(s[len - 6])) * 10000;
        [[fallthrough]];
      case 5:
        val += (DIGIT(s[len - 5])) * 10000;
        [[fallthrough]];
      case 4:
        val += (DIGIT(s[len - 4])) * 1000;
        [[fallthrough]];
      case 3:
        val += (DIGIT(s[len - 3])) * 100;
        [[fallthrough]];
      case 2:
        val += (DIGIT(s[len - 2])) * 10;
        [[fallthrough]];
      case 1:
        val += (DIGIT(s[len - 1]));
    }
    return negative ? -val : val;
  } else {
    return AtoiUnsafe(s, len);
  }
}

inline int32_t AtoiCased(const char* s, int len) {
  if (LIKELY(len <= 5)) {
    int32_t val = 0;
    bool negative = false;
    switch (*s) {
      case '-':
        negative = true;
        [[fallthrough]];
      case '+': --len; ++s;
    }

    switch(len) {
      case 5:
        val = DIGIT(s[0])*10000 + DIGIT(s[1])*1000 + DIGIT(s[2])*100 +
              DIGIT(s[3])*10 + DIGIT(s[4]);
        break;
      case 4:
        val = DIGIT(s[0])*1000 + DIGIT(s[1])*100 + DIGIT(s[2])*10 + DIGIT(s[3]);
        break;
      case 3:
        val = DIGIT(s[0])*100 + DIGIT(s[1])*10 + DIGIT(s[2]);
        break;
      case 2:
        val = DIGIT(s[0])*10 + DIGIT(s[1]);
        break;
      case 1:
        val = DIGIT(s[0]);
        break;
    }
    return negative ? -val : val;
  } else {
    return AtoiUnsafe(s, len);
  }
}

void TestAtoi(int batch_size, void* d) {
  TestData* data = reinterpret_cast<TestData*>(d);
  for (int i = 0; i < batch_size; ++i) {
    int n = data->data.size();
    for (int j = 0; j < n; ++j) {
      data->result[j] = atoi(data->data[j].Ptr());
    }
  }
}

void TestStrtol(int batch_size, void* d) {
  TestData* data = reinterpret_cast<TestData*>(d);
  for (int i = 0; i < batch_size; ++i) {
    int n = data->data.size();
    for (int j = 0; j < n; ++j) {
      data->result[j] = strtol(data->data[j].Ptr(), NULL, 10);
    }
  }
}

void TestImpala(int batch_size, void* d) {
  TestData* data = reinterpret_cast<TestData*>(d);
  for (int i = 0; i < batch_size; ++i) {
    int n = data->data.size();
    for (int j = 0; j < n; ++j) {
      const StringValue& str = data->data[j];
      StringParser::ParseResult dummy;
      int32_t val = StringParser::StringToInt<int32_t>(str.Ptr(), str.Len(), &dummy);
      VALIDATE_RESULT(val, data->result[j], str.Ptr());
      data->result[j] = val;
    }
  }
}

void TestImpalaUnsafe(int batch_size, void* d) {
  TestData* data = reinterpret_cast<TestData*>(d);
  for (int i = 0; i < batch_size; ++i) {
    int n = data->data.size();
    for (int j = 0; j < n; ++j) {
      const StringValue& str = data->data[j];
      int32_t val = AtoiUnsafe(str.Ptr(), str.Len());
      VALIDATE_RESULT(val, data->result[j], str.Ptr());
      data->result[j] = val;
    }
  }
}

void TestImpalaUnrolled(int batch_size, void* d) {
  TestData* data = reinterpret_cast<TestData*>(d);
  for (int i = 0; i < batch_size; ++i) {
    int n = data->data.size();
    for (int j = 0; j < n; ++j) {
      const StringValue& str = data->data[j];
      int32_t val = AtoiUnrolled(str.Ptr(), str.Len());
      VALIDATE_RESULT(val, data->result[j], str.Ptr());
      data->result[j] = val;
    }
  }
}

void TestImpalaCased(int batch_size, void* d) {
  TestData* data = reinterpret_cast<TestData*>(d);
  for (int i = 0; i < batch_size; ++i) {
    int n = data->data.size();
    for (int j = 0; j < n; ++j) {
      const StringValue& str = data->data[j];
      int32_t val = AtoiCased(str.Ptr(), str.Len());
      VALIDATE_RESULT(val, data->result[j], str.Ptr());
      data->result[j] = val;
    }
  }
}

int main(int argc, char **argv) {
  CpuInfo::Init();
  cout << Benchmark::GetMachineInfo() << endl;

  TestData data;

  // Most data is probably positive
  AddTestData(&data, 1000, -5, 1000);
  data.result.resize(data.data.size());

  TestData data_leading_space;
  AddTestData(&data_leading_space, 1000, -5, 1000, true, false);
  data_leading_space.result.resize(data_leading_space.data.size());

  TestData data_trailing_space;
  AddTestData(&data_trailing_space, 1000, -5, 1000, false, true);
  data_trailing_space.result.resize(data_trailing_space.data.size());

  TestData data_both_space;
  AddTestData(&data_both_space, 1000, -5, 1000, true, true);
  data_both_space.result.resize(data_trailing_space.data.size());

  TestData data_garbage;
  for (int i = 0; i < 1000; ++i) {
    AddTestData(&data_garbage, "sdfsfdsfasd");
  }
  data_garbage.result.resize(data_garbage.data.size());

  TestData data_trailing_garbage;
  for (int i = 0; i < 1000; ++i) {
    AddTestData(&data_trailing_garbage, "123    a");
  }
  data_trailing_garbage.result.resize(data_trailing_garbage.data.size());

  Benchmark suite("atoi");
  suite.AddBenchmark("strtol", TestStrtol, &data);
  suite.AddBenchmark("atoi", TestAtoi, &data);
  suite.AddBenchmark("impala_unsafe", TestImpalaUnsafe, &data);
  suite.AddBenchmark("impala_unrolled", TestImpalaUnrolled, &data);
  suite.AddBenchmark("impala_cased", TestImpalaCased, &data);

  suite.AddBenchmark("impala", TestImpala, &data);
  suite.AddBenchmark("impala_leading_space", TestImpala, &data_leading_space);
  suite.AddBenchmark("impala_trailing_space", TestImpala, &data_trailing_space);
  suite.AddBenchmark("impala_both_space", TestImpala, &data_both_space);
  suite.AddBenchmark("impala_garbage", TestImpala, &data_garbage);
  suite.AddBenchmark("impala_trailing_garbage", TestImpala, &data_trailing_garbage);

  cout << suite.Measure();

  return 0;
}
