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
// Dates:
// Impala Rate (per ms): 32.4125
// Boost String Rate (per ms): 0.683995
// Boost Rate (per ms): 0.679348
//
// Times:
// Impala Rate (per ms): 29.3199
// Boost Rate (per ms): 0.491159
//

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

  TestData dates, times;

  AddTestDataDates(&dates, 1000, "1953-04-22");
  AddTestDataTimes(&times, 1000, "01:02:03.45678");

  dates.result.resize(dates.data.size());
  times.result.resize(times.data.size());

  // Run a warmup to iterate through the data.  
  TestBoostDate(1000, &dates);

  double impala_rate = Benchmark::Measure(TestImpalaDate, &dates);
  double boostString_rate = Benchmark::Measure(TestBoostStringDate, &dates);
  double boost_rate = Benchmark::Measure(TestBoostDate, &dates);

  // Run a warmup to iterate through the data.  
  TestBoostTime(1000, &times);
  double impala_time_rate = Benchmark::Measure(TestImpalaDate, &times);
  double boost_time_rate = Benchmark::Measure(TestBoostTime, &times);

  cout << "Dates:" << endl;
  cout << "Impala Rate (per ms): " << impala_rate << endl;
  cout << "Boost String Rate (per ms): " << boostString_rate << endl;
  cout << "Boost Rate (per ms): " << boost_rate << endl;
  cout << endl;
  cout << "Times:" << endl;
  cout << "Impala Rate (per ms): " << impala_time_rate << endl;
  cout << "Boost Rate (per ms): " << boost_time_rate << endl;


  return 0;
}

