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
#include <vector>
#include <sstream>
#include "runtime/string-value.h"
#include "runtime/timestamp-value.h"
#include "util/benchmark.h"
#include "util/cpu-info.h"

using namespace impala;
using namespace std;
using namespace boost;
using namespace boost::date_time;
using namespace boost::posix_time;
using namespace boost::gregorian;

// Benchmark for parsing timestamps.
// Machine Info: Intel(R) Core(TM) i7-2600 CPU @ 3.40GHz
// ParseDate:            Function                Rate          Comparison
// ----------------------------------------------------------------------
//                BoostStringDate              0.6793                  1X
//                      BoostDate              0.6583             0.9691X
//                         Impala               28.75              42.32X
// 
// ParseTimestamp:       Function                Rate          Comparison
// ----------------------------------------------------------------------
//                      BoostTime               0.455                  1X
//                         Impala               28.39              62.39X

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
  vector<TimestampValue> result;
};

void AddTestData(TestData* data, const string& input) {
  data->memory.push_back(input);
  const string& str = data->memory.back();
  data->data.push_back(StringValue(const_cast<char*>(str.c_str()), str.length()));
}

void AddTestDataDates(TestData* data, int n, const string& startstr) {
  gregorian::date start(from_string(startstr));
  for (int i = 0; i < n; ++i) {
    int val = rand();
    val %= 100;
    gregorian::date_duration days(val);
    start += days;
    stringstream ss;
    ss << to_iso_extended_string(start);
    AddTestData(data, ss.str());
  }
}

void AddTestDataTimes(TestData* data, int n, const string& startstr) {
  posix_time::time_duration start(posix_time::duration_from_string(startstr));
  for (int i = 0; i < n; ++i) {
    int val = rand();
    start += nanoseconds(val);
    if (start.hours() >= 24) start -= hours(24);
    stringstream ss;
    ss << to_simple_string(start);
    AddTestData(data, ss.str());
  }
}

void TestImpalaDate(int batch_size, void* d) {
  TestData* data = reinterpret_cast<TestData*>(d);
  for (int i = 0; i < batch_size; ++i) {
    int n = data->data.size();
    for (int j = 0; j < n; ++j) {
      data->result[j] = TimestampValue(data->data[j].ptr, data->data[j].len);
    }
  }
}

void TestBoostStringDate(int batch_size, void* d) {
  TestData* data = reinterpret_cast<TestData*>(d);
  for (int i = 0; i < batch_size; ++i) {
    int n = data->data.size();
    for (int j = 0; j < n; ++j) {
      data->result[j].set_date(from_string(data->memory[j]));
    }
  }
}

void TestBoostDate(int batch_size, void* d) {
  TestData* data = reinterpret_cast<TestData*>(d);
  for (int i = 0; i < batch_size; ++i) {
    int n = data->data.size();
    for (int j = 0; j < n; ++j) {
      string s(data->data[j].ptr, data->data[j].len);
      data->result[j].set_date(from_string(s));
    }
  }
}

void TestBoostTime(int batch_size, void* d) {
  TestData* data = reinterpret_cast<TestData*>(d);
  for (int i = 0; i < batch_size; ++i) {
    int n = data->data.size();
    for (int j = 0; j < n; ++j) {
      string s(data->data[j].ptr, data->data[j].len);
      data->result[j].set_time(duration_from_string(s));
    }
  }
}


int main(int argc, char **argv) {
  CpuInfo::Init();
  cout << Benchmark::GetMachineInfo() << endl;

  TestData dates, times;

  AddTestDataDates(&dates, 1000, "1953-04-22");
  AddTestDataTimes(&times, 1000, "01:02:03.45678");

  dates.result.resize(dates.data.size());
  times.result.resize(times.data.size());

  Benchmark date_suite("ParseDate");
  date_suite.AddBenchmark("BoostStringDate", TestBoostStringDate, &dates);
  date_suite.AddBenchmark("BoostDate", TestBoostDate, &dates);
  date_suite.AddBenchmark("Impala", TestImpalaDate, &dates);
  
  Benchmark timestamp_suite("ParseTimestamp");
  timestamp_suite.AddBenchmark("BoostTime", TestBoostTime, &times);
  timestamp_suite.AddBenchmark("Impala", TestImpalaDate, &times);
  
  cout << date_suite.Measure();
  cout << endl;
  cout << timestamp_suite.Measure();

  return 0;
}

