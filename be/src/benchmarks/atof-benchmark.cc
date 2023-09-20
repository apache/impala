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

// Benchmark for computing atof.  This benchmarks tests converting from
// strings into floats on what we expect to be typical data.  The data
// is mostly positive numbers with just a couple of them in scientific
// notation.
//
// Machine Info: Intel(R) Core(TM) i7-2600 CPU @ 3.40GHz
// atof:                 Function                Rate          Comparison
// ----------------------------------------------------------------------
//                         Strtod               8.171                  1X
//                           Atof               8.057             0.9861X
//                         Impala               67.86              8.306X

#define VALIDATE 0

#if VALIDATE
// Use fabs?
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
  vector<double> result;
};

void AddTestData(TestData* data, const string& input) {
  data->memory.push_back(input);
  const string& str = data->memory.back();
  data->data.push_back(StringValue(const_cast<char*>(str.c_str()), str.length()));
}

void AddTestData(TestData* data, int n, double min = -10, double max = 10) {
  for (int i = 0; i < n; ++i) {
    double val = rand();
    val /= RAND_MAX;
    val = (val * (max - min)) + min;
    stringstream ss;
    ss << val;
    AddTestData(data, ss.str());
  }
}

void TestAtof(int batch_size, void* d) {
  TestData* data = reinterpret_cast<TestData*>(d);
  for (int i = 0; i < batch_size; ++i) {
    int n = data->data.size();
    for (int j = 0; j < n; ++j) {
      data->result[j] = atof(data->data[j].Ptr());
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
      double val = StringParser::StringToFloat<double>(str.Ptr(), str.Len(), &dummy);
      VALIDATE_RESULT(val, data->result[j], str.ptr);
      data->result[j] = val;
    }
  }
}

void TestStrtod(int batch_size, void* d) {
  TestData* data = reinterpret_cast<TestData*>(d);
  for (int i = 0; i < batch_size; ++i) {
    int n = data->data.size();
    for (int j = 0; j < n; ++j) {
      data->result[j] = strtod(data->data[j].Ptr(), NULL);
    }
  }
}

int main(int argc, char **argv) {
  CpuInfo::Init();
  cout << Benchmark::GetMachineInfo() << endl;

  TestData data;

  // Most data is probably positive
  AddTestData(&data, 1000, -5, 1000);
  AddTestData(&data, "1.1e12");

  data.result.resize(data.data.size());

  Benchmark suite("atof");
  suite.AddBenchmark("Strtod", TestStrtod, &data);
  suite.AddBenchmark("Atof", TestAtof, &data);
  suite.AddBenchmark("Impala", TestImpala, &data);
  cout << suite.Measure();

  return 0;
}
