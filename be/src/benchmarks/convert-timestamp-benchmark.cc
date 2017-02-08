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

#include <time.h>
#include <stdlib.h>
#include <stdio.h>
#include <iostream>
#include <vector>
#include <sstream>
#include <boost/date_time/posix_time/posix_time.hpp>
#include <boost/date_time/local_time/local_time.hpp>
#include <boost/thread/thread.hpp>
#include <boost/shared_ptr.hpp>

#include "exprs/timezone_db.h"
#include "runtime/timestamp-parse-util.h"
#include "runtime/timestamp-value.h"
#include "util/benchmark.h"
#include "util/cpu-info.h"
#include "util/pretty-printer.h"
#include "util/stopwatch.h"

#include "common/names.h"

namespace gregorian = boost::gregorian;
using boost::posix_time::duration_from_string;
using boost::posix_time::hours;
using boost::posix_time::nanoseconds;
using boost::posix_time::ptime;
using boost::posix_time::time_duration;
using boost::posix_time::to_iso_extended_string;
using boost::posix_time::to_simple_string;
using boost::local_time::time_zone_ptr;
using boost::local_time::posix_time_zone;
using namespace impala;

// Benchmark for converting timestamps from UTC to local time and from UTC to a given time
// zone.
// Machine Info: Intel(R) Core(TM) i5-6600 CPU @ 3.30GHz
// ConvertTimestamp:  Function  10%ile  50%ile  90%ile     10%ile     50%ile     90%ile
//                                                     (relative) (relative) (relative)
// ------------------------------------------------------------------------------------
//                     FromUtc  0.0147  0.0152  0.0155         1X         1X         1X
//                  UtcToLocal  0.0216  0.0228  0.0234      1.47X       1.5X      1.51X

time_zone_ptr LOCAL_TZ;

struct TestData {
  vector<TimestampValue> data;
  vector<TimestampValue> result;
};

void AddTestDataDateTimes(TestData* data, int n, const string& startstr) {
  DateTimeFormatContext dt_ctx;
  dt_ctx.Reset("yyyy-MMM-dd HH:mm:ss", 19);
  TimestampParser::ParseFormatTokens(&dt_ctx);

  ptime start(boost::posix_time::time_from_string(startstr));
  for (int i = 0; i < n; ++i) {
    int val = rand();
    start += gregorian::date_duration(rand() % 100);
    start += nanoseconds(val);
    stringstream ss;
    ss << to_simple_string(start);
    string ts = ss.str();
    data->data.push_back(TimestampValue(ts.c_str(), ts.size(), dt_ctx));
  }
}

void TestFromUtc(int batch_size, void* d) {
  TestData* data = reinterpret_cast<TestData*>(d);
  for (int i = 0; i < batch_size; ++i) {
    int n = data->data.size();
    for (int j = 0; j < n; ++j) {
      TimestampValue ts = data->data[j];
      ts.FromUtc(LOCAL_TZ);
      data->result[j] = ts;
    }
  }
}

void TestUtcToLocal(int batch_size, void* d) {
  TestData* data = reinterpret_cast<TestData*>(d);
  for (int i = 0; i < batch_size; ++i) {
    int n = data->data.size();
    for (int j = 0; j < n; ++j) {
      TimestampValue ts = data->data[j];
      ts.UtcToLocal();
      data->result[j] = ts;
    }
  }
}

int main(int argc, char **argv) {
  CpuInfo::Init();
  cout << Benchmark::GetMachineInfo() << endl;

  TimestampParser::Init();
  ABORT_IF_ERROR(TimezoneDatabase::Initialize());

  // Initialize LOCAL_TZ to local time zone
  tzset();
  time_t now = time(0);
  LOCAL_TZ = time_zone_ptr(new posix_time_zone(tzname[localtime(&now)->tm_isdst]));

  TestData datetimes;
  AddTestDataDateTimes(&datetimes, 10000, "1953-04-22 01:02:03");
  datetimes.result.resize(datetimes.data.size());

  Benchmark timestamp_suite("ConvertTimestamp");
  timestamp_suite.AddBenchmark("FromUtc", TestFromUtc, &datetimes);
  timestamp_suite.AddBenchmark("UtcToLocal", TestUtcToLocal, &datetimes);

  cout << timestamp_suite.Measure() << endl;

  // If number of threads is specified, run multithreaded tests.
  int num_of_threads = (argc < 2) ? 0 : atoi(argv[1]);
  if (num_of_threads >= 1) {
    vector<boost::shared_ptr<boost::thread> > threads(num_of_threads);
    StopWatch sw;
    // Test UtcToLocal()
    sw.Start();
    for (auto& t: threads) {
      t = boost::shared_ptr<boost::thread>(
          new boost::thread(TestUtcToLocal, 100, &datetimes));
    }
    for (auto& t: threads) t->join();
    uint64_t utc_to_local_elapsed_time = sw.ElapsedTime();
    sw.Stop();

    // Test FromUtc()
    sw.Start();
    for (auto& t: threads) {
      t = boost::shared_ptr<boost::thread>(
          new boost::thread(TestFromUtc, 100, &datetimes));
    }
    for (auto& t: threads) t->join();
    uint64_t from_utc_elapsed_time = sw.ElapsedTime();
    sw.Stop();

    cout << "Number of threads: " << num_of_threads << endl
         << "TestFromUtc: "
         << PrettyPrinter::Print(from_utc_elapsed_time, TUnit::CPU_TICKS) << endl
         << "TestUtcToLocal: "
         << PrettyPrinter::Print(utc_to_local_elapsed_time, TUnit::CPU_TICKS) << endl;
  }

  return 0;
}
