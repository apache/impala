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
#include "runtime/timestamp-parse-util.h"
#include "runtime/timestamp-value.h"
#include "util/benchmark.h"
#include "util/cpu-info.h"
#include "common/names.h"

namespace gregorian = boost::gregorian;
using boost::posix_time::duration_from_string;
using boost::posix_time::hours;
using boost::posix_time::nanoseconds;
using boost::posix_time::ptime;
using boost::posix_time::time_duration;
using boost::posix_time::to_iso_extended_string;
using boost::posix_time::to_simple_string;
using namespace impala;

// Benchmark for parsing timestamps.
// Machine Info: Intel(R) Core(TM) i7-6700 CPU @ 3.40GHz
// ParseDate:            Function     Rate (iters/ms)          Comparison
// ----------------------------------------------------------------------
//                BoostStringDate               1.277                  1X
//                      BoostDate               1.229              0.962X
//                         Impala               16.83              13.17X
//
// ParseTimestamp:       Function     Rate (iters/ms)          Comparison
// ----------------------------------------------------------------------
//                      BoostTime              0.9074                  1X
//                         Impala               15.01              16.54X
//
// ParseTimestampWithFormat:Function  Rate (iters/ms)          Comparison
// ----------------------------------------------------------------------
//                  BoostDateTime              0.4488                  1X
//                ImpalaTimeStamp               37.41              83.35X
//              ImpalaTZTimeStamp               37.39               83.3X

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

DateTimeFormatContext dt_ctx;
DateTimeFormatContext dt_ctx_tz;

void AddTestData(TestData* data, const string& input) {
  data->memory.push_back(input);
  const string& str = data->memory.back();
  data->data.push_back(StringValue(const_cast<char*>(str.c_str()), str.length()));
}

void AddTestDataDates(TestData* data, int n, const string& startstr) {
  gregorian::date start(gregorian::from_string(startstr));
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
  time_duration start(duration_from_string(startstr));
  for (int i = 0; i < n; ++i) {
    int val = rand();
    start += nanoseconds(val);
    if (start.hours() >= 24) start -= hours(24);
    stringstream ss;
    ss << to_simple_string(start);
    AddTestData(data, ss.str());
  }
}

void AddTestDataDateTimes(TestData* data, int n, const string& startstr) {
  ptime start(boost::posix_time::time_from_string(startstr));
  for (int i = 0; i < n; ++i) {
    int val = rand();
    start += gregorian::date_duration(rand() % 100);
    start += nanoseconds(val);
    stringstream ss;
    ss << to_simple_string(start);
    AddTestData(data, ss.str());
  }
}

void AddTestDataTZDateTimes(TestData* data, int n, const string& startstr) {
  ptime start(boost::posix_time::time_from_string(startstr));
  for (int i = 0; i < n; ++i) {
    int val = rand();
    start += gregorian::date_duration(rand() % 100);
    start += nanoseconds(val);
    stringstream ss;
    ss << to_simple_string(start);
    ss << "+03:30";
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
      data->result[j].set_date(gregorian::from_string(data->memory[j]));
    }
  }
}

void TestBoostDate(int batch_size, void* d) {
  TestData* data = reinterpret_cast<TestData*>(d);
  for (int i = 0; i < batch_size; ++i) {
    int n = data->data.size();
    for (int j = 0; j < n; ++j) {
      string s(data->data[j].ptr, data->data[j].len);
      data->result[j].set_date(gregorian::from_string(s));
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

void TestImpalaTimestamp(int batch_size, void* d) {
  TestData* data = reinterpret_cast<TestData*>(d);
  for (int i = 0; i < batch_size; ++i) {
    int n = data->data.size();
    for (int j = 0; j < n; ++j) {
      data->result[j] = TimestampValue(data->data[j].ptr, data->data[j].len, dt_ctx);
    }
  }
}

void TestImpalaTZTimestamp(int batch_size, void* d) {
  TestData* data = reinterpret_cast<TestData*>(d);
  for (int i = 0; i < batch_size; ++i) {
    int n = data->data.size();
    for (int j = 0; j < n; ++j) {
      data->result[j] = TimestampValue(data->data[j].ptr, data->data[j].len, dt_ctx_tz);
    }
  }
}

void TestBoostDateTime(int batch_size, void* d) {
  TestData* data = reinterpret_cast<TestData*>(d);
  for (int i = 0; i < batch_size; ++i) {
    int n = data->data.size();
    for (int j = 0; j < n; ++j) {
      string s(data->data[j].ptr, data->data[j].len);
      data->result[j] = TimestampValue(boost::posix_time::time_from_string(s));
    }
  }
}

int main(int argc, char **argv) {
  CpuInfo::Init();
  cout << Benchmark::GetMachineInfo() << endl;

  TimestampParser::Init();

  TestData dates, times, datetimes, tzdatetimes;

  AddTestDataDates(&dates, 1000, "1953-04-22");
  AddTestDataTimes(&times, 1000, "01:02:03.45678");
  AddTestDataDateTimes(&datetimes, 1000, "1953-04-22 01:02:03");
  AddTestDataTZDateTimes(&tzdatetimes, 1000, "1990-04-22 01:10:03");

  dates.result.resize(dates.data.size());
  times.result.resize(times.data.size());
  datetimes.result.resize(datetimes.data.size());
  tzdatetimes.result.resize(tzdatetimes.data.size());

  Benchmark date_suite("ParseDate");
  date_suite.AddBenchmark("BoostStringDate", TestBoostStringDate, &dates);
  date_suite.AddBenchmark("BoostDate", TestBoostDate, &dates);
  date_suite.AddBenchmark("Impala", TestImpalaDate, &dates);

  Benchmark timestamp_suite("ParseTimestamp");
  timestamp_suite.AddBenchmark("BoostTime", TestBoostTime, &times);
  timestamp_suite.AddBenchmark("Impala", TestImpalaDate, &times);

  dt_ctx.Reset("yyyy-MM-dd HH:mm:ss", 19);
  TimestampParser::ParseFormatTokens(&dt_ctx);
  dt_ctx_tz.Reset("yyyy-MM-dd HH:mm:ss+hh:mm", 25);
  TimestampParser::ParseFormatTokens(&dt_ctx_tz);

  Benchmark timestamp_with_format_suite("ParseTimestampWithFormat");
  timestamp_with_format_suite.AddBenchmark("BoostDateTime",
      TestBoostDateTime, &datetimes);
  timestamp_with_format_suite.AddBenchmark("ImpalaTimeStamp",
      TestImpalaTimestamp, &datetimes);
  timestamp_with_format_suite.AddBenchmark("ImpalaTZTimeStamp",
      TestImpalaTZTimestamp, &tzdatetimes);

  cout << date_suite.Measure();
  cout << endl;
  cout << timestamp_suite.Measure();
  cout << endl;
  cout << timestamp_with_format_suite.Measure();

  return 0;
}
