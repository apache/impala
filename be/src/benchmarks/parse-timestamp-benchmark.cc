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
#include <boost/date_time/posix_time/posix_time.hpp>
#include "runtime/datetime-iso-sql-format-tokenizer.h"
#include "runtime/datetime-simple-date-format-parser.h"
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

using namespace datetime_parse_util;

// ParseDate:                 Function  iters/ms   10%ile   50%ile   90%ile     10%ile     50%ile     90%ile
//                                                                          (relative) (relative) (relative)
// ---------------------------------------------------------------------------------------------------------
//                     BoostStringDate              0.654    0.682    0.717         1X         1X         1X
//                           BoostDate              0.642    0.667    0.691     0.981X     0.978X     0.964X
//                              Impala               6.58     6.79     7.11      10.1X      9.96X      9.91X
//
// ParseTimestamp:            Function  iters/ms   10%ile   50%ile   90%ile     10%ile     50%ile     90%ile
//                                                                          (relative) (relative) (relative)
// ---------------------------------------------------------------------------------------------------------
//                           BoostTime               0.48    0.491     0.51         1X         1X         1X
//                              Impala               6.03     6.38     6.54      12.6X        13X      12.8X
//
// ParseTimestampWithFormat:  Function  iters/ms   10%ile   50%ile   90%ile     10%ile     50%ile     90%ile
//                                                                          (relative) (relative) (relative)
// ---------------------------------------------------------------------------------------------------------
//                       BoostDateTime               0.24     0.25     0.26         1X         1X         1X
//     ImpalaSimpleDateFormatTimeStamp                 16     16.7     17.2      66.8X      66.8X      66.3X
//   ImpalaSimpleDateFormatTZTimeStamp               16.1     16.7     17.1      67.2X      66.7X      65.8X
//         ImpalaIsoSqlFormatTimeStamp               8.12     8.42     8.71      33.8X      33.7X      33.5X
//       ImpalaIsoSqlFormatTZTimeStamp               8.12     8.56     8.85      33.8X      34.2X        34X

struct TestData {
  vector<StringValue> data;
  vector<string> memory;
  vector<TimestampValue> result;
};

DateTimeFormatContext dt_ctx_simple_date_format;
DateTimeFormatContext dt_ctx_tz_simple_date_format;
DateTimeFormatContext dt_ctx_iso_sql_format;
DateTimeFormatContext dt_ctx_tz_iso_sql_format;

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

void TestImpalaSimpleDateFormat(int batch_size, void* d) {
  TestData* data = reinterpret_cast<TestData*>(d);
  for (int i = 0; i < batch_size; ++i) {
    int n = data->data.size();
    for (int j = 0; j < n; ++j) {
      data->result[j] = TimestampValue::ParseSimpleDateFormat(data->data[j].Ptr(),
          data->data[j].Len());
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
      string s(data->data[j].Ptr(), data->data[j].Len());
      data->result[j].set_date(gregorian::from_string(s));
    }
  }
}

void TestBoostTime(int batch_size, void* d) {
  TestData* data = reinterpret_cast<TestData*>(d);
  for (int i = 0; i < batch_size; ++i) {
    int n = data->data.size();
    for (int j = 0; j < n; ++j) {
      string s(data->data[j].Ptr(), data->data[j].Len());
      data->result[j].set_time(duration_from_string(s));
    }
  }
}

void TestImpalaSimpleDateFormatTimestamp(int batch_size, void* d) {
  TestData* data = reinterpret_cast<TestData*>(d);
  for (int i = 0; i < batch_size; ++i) {
    int n = data->data.size();
    for (int j = 0; j < n; ++j) {
      data->result[j] = TimestampValue::ParseSimpleDateFormat(data->data[j].Ptr(),
          data->data[j].Len(), dt_ctx_simple_date_format);
    }
  }
}

void TestImpalaIsoSqlFormatTimestamp(int batch_size, void* d) {
  TestData* data = reinterpret_cast<TestData*>(d);
  for (int i = 0; i < batch_size; ++i) {
    int n = data->data.size();
    for (int j = 0; j < n; ++j) {
      data->result[j] = TimestampValue::ParseIsoSqlFormat(data->data[j].Ptr(),
          data->data[j].Len(), dt_ctx_iso_sql_format);
    }
  }
}

void TestImpalaIsoSqlFormatTZTimestamp(int batch_size, void* d) {
  TestData* data = reinterpret_cast<TestData*>(d);
  for (int i = 0; i < batch_size; ++i) {
    int n = data->data.size();
    for (int j = 0; j < n; ++j) {
      data->result[j] = TimestampValue::ParseIsoSqlFormat(data->data[j].Ptr(),
          data->data[j].Len(), dt_ctx_tz_iso_sql_format);
    }
  }
}

void TestImpalaSimpleDateFormatTZTimestamp(int batch_size, void* d) {
  TestData* data = reinterpret_cast<TestData*>(d);
  for (int i = 0; i < batch_size; ++i) {
    int n = data->data.size();
    for (int j = 0; j < n; ++j) {
      data->result[j] = TimestampValue::ParseSimpleDateFormat(data->data[j].Ptr(),
          data->data[j].Len(), dt_ctx_tz_simple_date_format);
    }
  }
}

void TestBoostDateTime(int batch_size, void* d) {
  TestData* data = reinterpret_cast<TestData*>(d);
  for (int i = 0; i < batch_size; ++i) {
    int n = data->data.size();
    for (int j = 0; j < n; ++j) {
      string s(data->data[j].Ptr(), data->data[j].Len());
      data->result[j] = TimestampValue(boost::posix_time::time_from_string(s));
    }
  }
}

int main(int argc, char **argv) {
  CpuInfo::Init();
  cout << Benchmark::GetMachineInfo() << endl;

  SimpleDateFormatTokenizer::InitCtx();

  TestData dates, times, datetimes, tzdatetimes;

  AddTestDataDates(&dates, 700, "1953-04-22");
  AddTestDataTimes(&times, 700, "01:02:03.45678");
  AddTestDataDateTimes(&datetimes, 700, "1953-04-22 01:02:03");
  AddTestDataTZDateTimes(&tzdatetimes, 700, "1990-04-22 01:10:03");

  dates.result.resize(dates.data.size());
  times.result.resize(times.data.size());
  datetimes.result.resize(datetimes.data.size());
  tzdatetimes.result.resize(tzdatetimes.data.size());

  Benchmark date_suite("ParseDate");
  date_suite.AddBenchmark("BoostStringDate", TestBoostStringDate, &dates);
  date_suite.AddBenchmark("BoostDate", TestBoostDate, &dates);
  date_suite.AddBenchmark("Impala", TestImpalaSimpleDateFormat, &dates);

  Benchmark timestamp_suite("ParseTimestamp");
  timestamp_suite.AddBenchmark("BoostTime", TestBoostTime, &times);
  timestamp_suite.AddBenchmark("Impala", TestImpalaSimpleDateFormat, &times);

  dt_ctx_simple_date_format.Reset("yyyy-MM-dd HH:mm:ss", 19);
  SimpleDateFormatTokenizer::Tokenize(&dt_ctx_simple_date_format, PARSE);
  dt_ctx_tz_simple_date_format.Reset("yyyy-MM-dd HH:mm:ss+hh:mm", 25);
  SimpleDateFormatTokenizer::Tokenize(&dt_ctx_tz_simple_date_format, PARSE);

  dt_ctx_iso_sql_format.Reset("YYYY-MM-DD HH24:MI:SS", 21);
  datetime_parse_util::IsoSqlFormatTokenizer tokenizer(&dt_ctx_iso_sql_format,
      datetime_parse_util::PARSE, true);
  tokenizer.Tokenize();

  dt_ctx_tz_iso_sql_format.Reset("YYYY-MM-DD HH24:MI:SSTZH:TZM", 28);
  tokenizer.Reset(&dt_ctx_tz_iso_sql_format, datetime_parse_util::PARSE, true);
  tokenizer.Tokenize();

  Benchmark timestamp_with_format_suite("ParseTimestampWithFormat");
  timestamp_with_format_suite.AddBenchmark("BoostDateTime",
      TestBoostDateTime, &datetimes);
  timestamp_with_format_suite.AddBenchmark("ImpalaSimpleDateFormatTimeStamp",
      TestImpalaSimpleDateFormatTimestamp, &datetimes);
  timestamp_with_format_suite.AddBenchmark("ImpalaSimpleDateFormatTZTimeStamp",
      TestImpalaSimpleDateFormatTZTimestamp, &tzdatetimes);
  timestamp_with_format_suite.AddBenchmark("ImpalaIsoSqlFormatTimeStamp",
      TestImpalaIsoSqlFormatTimestamp, &datetimes);
  timestamp_with_format_suite.AddBenchmark("ImpalaIsoSqlFormatTZTimeStamp",
        TestImpalaIsoSqlFormatTZTimestamp, &tzdatetimes);

  cout << date_suite.Measure();
  cout << endl;
  cout << timestamp_suite.Measure();
  cout << endl;
  cout << timestamp_with_format_suite.Measure();

  return 0;
}
