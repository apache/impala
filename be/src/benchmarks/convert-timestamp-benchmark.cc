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

#include <chrono>
#include <iomanip>
#include <iostream>
#include <memory>
#include <random>
#include <sstream>
#include <thread>
#include <utility>
#include <vector>

#include <boost/date_time/compiler_config.hpp>
#include <boost/date_time/posix_time/posix_time.hpp>
#include <boost/date_time/local_time/local_time.hpp>

#include <time.h>
#include <stdlib.h>
#include <stdio.h>

#include "cctz/civil_time.h"
#include "exprs/timezone_db.h"
#include "exprs/timestamp-functions.h"
#include "runtime/datetime-simple-date-format-parser.h"
#include "runtime/timestamp-value.h"
#include "runtime/timestamp-value.inline.h"
#include "util/benchmark.h"
#include "util/cpu-info.h"
#include "util/pretty-printer.h"
#include "util/stopwatch.h"

#include "common/names.h"

using std::random_device;
using std::mt19937;
using std::uniform_int_distribution;
using std::thread;

using namespace impala;
using namespace datetime_parse_util;

// Benchmark tests for timestamp time-zone conversions
/*
Machine Info: Intel(R) Core(TM) i5-6600 CPU @ 3.30GHz

UtcToUnixTime:             Function  iters/ms   10%ile   50%ile   90%ile     10%ile     50%ile     90%ile
                                                                         (relative) (relative) (relative)
---------------------------------------------------------------------------------------------------------
                            (glibc)               7.98     8.14      8.3         1X         1X         1X
                      (Google/CCTZ)               17.9     18.2     18.5      2.24X      2.24X      2.23X
                            (boost)                301      306      311      37.7X      37.5X      37.5X

LocalToUnixTime:           Function  iters/ms   10%ile   50%ile   90%ile     10%ile     50%ile     90%ile
                                                                         (relative) (relative) (relative)
---------------------------------------------------------------------------------------------------------
                            (glibc)              0.717    0.732    0.745         1X         1X         1X
                      (Google/CCTZ)               15.3     15.5     15.8      21.3X      21.2X      21.2X

FromUtc:                   Function  iters/ms   10%ile   50%ile   90%ile     10%ile     50%ile     90%ile
                                                                         (relative) (relative) (relative)
---------------------------------------------------------------------------------------------------------
                            (boost)                1.6     1.63     1.67         1X         1X         1X
                      (Google/CCTZ)               14.5     14.8     15.2      9.06X      9.09X      9.11X

ToUtc:                     Function  iters/ms   10%ile   50%ile   90%ile     10%ile     50%ile     90%ile
                                                                         (relative) (relative) (relative)
---------------------------------------------------------------------------------------------------------
                            (boost)               1.63     1.67     1.68         1X         1X         1X
                      (Google/CCTZ)                8.7      8.9     9.05      5.34X      5.34X      5.38X

UtcToLocal:                Function  iters/ms   10%ile   50%ile   90%ile     10%ile     50%ile     90%ile
                                                                         (relative) (relative) (relative)
---------------------------------------------------------------------------------------------------------
                            (glibc)               2.68     2.75      2.8         1X         1X         1X
                      (Google/CCTZ)                 15     15.2     15.5      5.59X      5.55X      5.53X

UnixTimeToLocalPtime:      Function  iters/ms   10%ile   50%ile   90%ile     10%ile     50%ile     90%ile
                                                                         (relative) (relative) (relative)
---------------------------------------------------------------------------------------------------------
                            (glibc)               2.69     2.75      2.8         1X         1X         1X
                      (Google/CCTZ)               14.8     15.1     15.4       5.5X      5.49X      5.52X

UnixTimeToUtcPtime:        Function  iters/ms   10%ile   50%ile   90%ile     10%ile     50%ile     90%ile
                                                                         (relative) (relative) (relative)
---------------------------------------------------------------------------------------------------------
                            (glibc)                 17     17.6     17.9         1X         1X         1X
                      (Google/CCTZ)               6.45     6.71     6.81     0.379X     0.382X      0.38X
                        (fast path)               25.1       26     26.4      1.47X      1.48X      1.48X
                        (day split)               48.6     50.3     51.3      2.85X      2.87X      2.86X

UtcFromUnixTimeMicros:     Function  iters/ms   10%ile   50%ile   90%ile     10%ile     50%ile     90%ile
                                                                         (relative) (relative) (relative)
---------------------------------------------------------------------------------------------------------
                  (sec split (old))               17.9     18.7     19.1         1X         1X         1X
                        (day split)                111      116      118      6.21X      6.19X      6.19X

FromUnixTimeNanos:         Function  iters/ms   10%ile   50%ile   90%ile     10%ile     50%ile     90%ile
                                                                         (relative) (relative) (relative)
---------------------------------------------------------------------------------------------------------
                  (sec split (old))               18.7     19.5     19.8         1X         1X         1X
                  (sec split (new))                104      108      110      5.58X      5.55X      5.57X

FromSubsecondUnixTime:     Function  iters/ms   10%ile   50%ile   90%ile     10%ile     50%ile     90%ile
                                                                         (relative) (relative) (relative)
---------------------------------------------------------------------------------------------------------
                              (old)               18.7     18.7     18.7         1X         1X         1X
                              (new)               73.5     74.1     74.1      3.94X      3.96X      3.96X

Number of threads: 8

UtcToUnixTime:
             (glibc) elapsed time: 1s020ms
       (Google/CCTZ) elapsed time: 144ms
             (boost) elapsed time: 10ms
cctz speedup: 7.0784
boost speedup: 95.1732

LocalToUnixTime:
             (glibc) elapsed time: 18s050ms
       (Google/CCTZ) elapsed time: 212ms
speedup: 84.9949

FromUtc:
             (boost) elapsed time: 1s519ms
       (Google/CCTZ) elapsed time: 263ms
speedup: 5.77003

ToUtc:
             (boost) elapsed time: 1s674ms
       (Google/CCTZ) elapsed time: 325ms
speedup: 5.13874

UtcToLocal:
             (glibc) elapsed time: 4s862ms
       (Google/CCTZ) elapsed time: 263ms
speedup: 18.4253

UnixTimeToLocalPtime:
             (glibc) elapsed time: 4s856ms
       (Google/CCTZ) elapsed time: 259ms
speedup: 18.7398

UnixTimeToUtcPtime:
             (glibc) elapsed time: 928ms
       (Google/CCTZ) elapsed time: 282ms
         (fast path) elapsed time: 90ms
cctz speedup: 3.28187
fast path speedup: 10.2951
*/

vector<TimestampValue> AddTestDataDateTimes(int n, const string& startstr) {
  DateTimeFormatContext dt_ctx;
  dt_ctx.Reset("yyyy-MMM-dd HH:mm:ss");
  SimpleDateFormatTokenizer::Tokenize(&dt_ctx, PARSE);

  random_device rd;
  mt19937 gen(rd());
  // Random days in a [0..99] days range.
  uniform_int_distribution<int64_t> dis_days(0, 99);
  // Random nanoseconds in a [0..15] minutes range.
  uniform_int_distribution<int64_t> dis_nanosec(0, 900000000000L);

  boost::posix_time::ptime start(boost::posix_time::time_from_string(startstr));
  vector<TimestampValue> data;
  data.reserve(n);
  for (int i = 0; i < n; ++i) {
    start += boost::gregorian::date_duration(dis_days(gen));
    start += boost::posix_time::nanoseconds(dis_nanosec(gen));
    string ts = to_simple_string(start);
    data.push_back(TimestampValue::ParseSimpleDateFormat(ts.c_str(), ts.size(), dt_ctx));
  }

  return data;
}

template <class FROM, class TO, TO (*converter)(const FROM &)>
class TestData {
public:
  TestData(const vector<FROM>& data) : data_(data), result_(data.size()) {}

  void test_batch(int batch_size) {
    for (int i = 0; i < batch_size; ++i) {
      for (size_t j = 0; j < data_.size(); ++j) {
        this->result_[j] = converter(data_[j]);
      }
    }
  }

  const vector<TO>& result() const { return result_; }

  void add_to_benchmark(Benchmark& bm, const char* name) {
    bm.AddBenchmark(name, run_test, this);
  }

  static void run_test(int batch_size, void *d) {
    TestData<FROM, TO, converter>* data =
        reinterpret_cast<TestData<FROM, TO, converter>*>(d);
    data->test_batch(batch_size);
  }

  static uint64_t measure_multithreaded_elapsed_time(int num_of_threads, int batch_size,
      const vector<FROM>& data, const char* label) {
    // Create TestData for each thread.
    vector<unique_ptr<TestData<FROM, TO, converter>>> test_data;
    test_data.reserve(num_of_threads);
    for (int i = 0; i < num_of_threads; ++i) {
      test_data.push_back(make_unique<TestData<FROM, TO, converter>>(data));
    }

    // Create and start threads.
    vector<unique_ptr<thread>> threads(num_of_threads);
    StopWatch sw;
    sw.Start();
    for (int i = 0; i < num_of_threads; ++i) {
      threads[i] = make_unique<thread>(
          run_test, batch_size, test_data[i].get());
    }

    // Wait until every thread terminates.
    for (auto& t: threads) t->join();
    uint64_t elapsed_time = sw.ElapsedTime();
    sw.Stop();

    cout << setw(20) << label << " elapsed time: "
         << PrettyPrinter::Print(elapsed_time, TUnit::CPU_TICKS)
         << endl;

    return elapsed_time;
  }

private:
  const vector<FROM>& data_;
  vector<TO> result_;
};

template <class T>
void bail_if_results_dont_match(const vector<const vector<T>*>& test_result_vec) {
  if (test_result_vec.size() <= 1) return;

  auto b = test_result_vec.begin();
  for (auto it = b + 1; it != test_result_vec.end(); ++it) {
    auto& references = **b;
    auto& results = **it;
    for (int i = 0; i < references.size(); ++i) {
      if (references[i] != results[i]) {
        cerr << "Results don't match: " << references[i] << " vs " << results[i] << endl;
        exit(1);
      }
    }
  }
}

//
// Test UtcToUnixTime (boost is expected to be the fastest, followed by CCTZ and glibc)
//

// CCTZ
const Timezone* PTR_CCTZ_LOCAL_TZ = nullptr;

time_t cctz_utc_to_unix_time(const TimestampValue& ts) {
  const boost::gregorian::date& d = ts.date();
  const boost::posix_time::time_duration& t = ts.time();
  cctz::civil_second cs(d.year(), d.month(), d.day(), t.hours(), t.minutes(),
      t.seconds());
  cctz::time_point<cctz::sys_seconds> tp = cctz::convert(cs,
      TimezoneDatabase::GetUtcTimezone());
  cctz::sys_seconds seconds = tp.time_since_epoch();
  return seconds.count();
}

// glibc
time_t glibc_utc_to_unix_time(const TimestampValue& ts) {
  const boost::posix_time::ptime temp(ts.date(), ts.time());
  tm temp_tm = boost::posix_time::to_tm(temp);
  return timegm(&temp_tm);
}

// boost
time_t boost_utc_to_unix_time(const TimestampValue& ts) {
  static const boost::gregorian::date epoch(1970,1,1);
  return (ts.date() - epoch).days() * 24 * 60 * 60 + ts.time().total_seconds();
}

//
// Test LocalToUnixTime (CCTZ is expected to be faster than glibc)
//

// CCTZ
time_t cctz_local_to_unix_time(const TimestampValue& ts) {
  const boost::gregorian::date& d = ts.date();
  const boost::posix_time::time_duration& t = ts.time();
  cctz::civil_second cs(d.year(), d.month(), d.day(), t.hours(), t.minutes(),
      t.seconds());
  cctz::time_point<cctz::sys_seconds> tp = cctz::convert(cs, *PTR_CCTZ_LOCAL_TZ);
  cctz::sys_seconds seconds = tp.time_since_epoch();
  return seconds.count();
}

// glibc
time_t glibc_local_to_unix_time(const TimestampValue& ts) {
  const boost::posix_time::ptime temp(ts.date(), ts.time());
  tm temp_tm = boost::posix_time::to_tm(temp);
  return mktime(&temp_tm);
}

//
// Test UtcToLocal (CCTZ is expected to be faster than glibc)
//

// glibc
// Constants for use with Unix times. Leap-seconds do not apply.
const int32_t SECONDS_IN_MINUTE = 60;
const int32_t SECONDS_IN_HOUR = 60 * SECONDS_IN_MINUTE;
const int32_t SECONDS_IN_DAY = 24 * SECONDS_IN_HOUR;

// struct tm stores month/year data as an offset
const unsigned short TM_YEAR_OFFSET = 1900;
const unsigned short TM_MONTH_OFFSET = 1;

// Boost stores dates as an uint32_t. Since subtraction is needed, convert to signed.
const int64_t EPOCH_DAY_NUMBER =
    static_cast<int64_t>(
    boost::gregorian::date(1970, boost::gregorian::Jan, 1).day_number());

TimestampValue glibc_utc_to_local(const TimestampValue& ts_value) {
  DCHECK(ts_value.HasDateAndTime());
  try {
    boost::gregorian::date d = ts_value.date();
    boost::posix_time::time_duration t = ts_value.time();
    time_t utc =
        (static_cast<int64_t>(d.day_number()) - EPOCH_DAY_NUMBER) * SECONDS_IN_DAY +
        t.hours() * SECONDS_IN_HOUR +
        t.minutes() * SECONDS_IN_MINUTE +
        t.seconds();
    tm temp;
    if (UNLIKELY(localtime_r(&utc, &temp) == nullptr)) {
      return TimestampValue(boost::posix_time::ptime(boost::posix_time::not_a_date_time));
    }
    // Unlikely but a time zone conversion may push the value over the min/max
    // boundary resulting in an exception.
    d = boost::gregorian::date(
        static_cast<unsigned short>(temp.tm_year + TM_YEAR_OFFSET),
        static_cast<unsigned short>(temp.tm_mon + TM_MONTH_OFFSET),
        static_cast<unsigned short>(temp.tm_mday));
    t = boost::posix_time::time_duration(temp.tm_hour, temp.tm_min, temp.tm_sec,
        t.fractional_seconds());
    return TimestampValue(d, t);
  } catch (std::exception& /* from Boost */) {
    return TimestampValue(boost::posix_time::ptime(boost::posix_time::not_a_date_time));
  }
}

// CCTZ
inline cctz::time_point<cctz::sys_seconds> cctz_unix_time_to_time_point(time_t t) {
  static const cctz::time_point<cctz::sys_seconds> epoch =
      std::chrono::time_point_cast<cctz::sys_seconds>(
          std::chrono::system_clock::from_time_t(0));
  return epoch + cctz::sys_seconds(t);
}

inline bool cctz_check_if_date_out_of_range(const cctz::civil_second& cs) {
  return cs.year() < 1400 || cs.year() > 9999;
}

TimestampValue cctz_utc_to_local(const TimestampValue& ts_value) {
  DCHECK(ts_value.HasDateAndTime());
  boost::gregorian::date d;
  boost::posix_time::time_duration t;
  time_t unix_time;
  if (UNLIKELY(!ts_value.UtcToUnixTime(&unix_time))) {
    d = boost::gregorian::date(boost::gregorian::not_a_date_time);
    t = boost::posix_time::time_duration(boost::posix_time::not_a_date_time);
  } else {
    cctz::time_point<cctz::sys_seconds> from_tp =
        cctz_unix_time_to_time_point(unix_time);
    cctz::civil_second to_cs = cctz::convert(from_tp, *PTR_CCTZ_LOCAL_TZ);
    // boost::gregorian::date() throws boost::gregorian::bad_year if year is not in the
    // 1400..9999 range. Need to check validity before creating the date object.
    if (UNLIKELY(cctz_check_if_date_out_of_range(to_cs))) {
      d = boost::gregorian::date(boost::gregorian::not_a_date_time);
      t = boost::posix_time::time_duration(boost::posix_time::not_a_date_time);
    } else {
      d = boost::gregorian::date(to_cs.year(), to_cs.month(), to_cs.day());
      t = boost::posix_time::time_duration(to_cs.hour(), to_cs.minute(), to_cs.second(),
          ts_value.time().fractional_seconds());
    }
  }
  return TimestampValue(d, t);
}


//
// Test FromUtc (CCTZ is expected to be faster than boost)
//

// boost
const boost::local_time::time_zone_ptr BOOST_PST_TZ(
    new boost::local_time::posix_time_zone(string("PST-8PDT,M4.1.0,M10.1.0")));

void boost_throw_if_date_out_of_range(const boost::gregorian::date& date) {
  // Boost checks the ranges when instantiating the year/month/day representations.
  boost::gregorian::greg_year year = date.year();
  boost::gregorian::greg_month month = date.month();
  boost::gregorian::greg_day day = date.day();
  // Ensure Boost's validation is effective.
  DCHECK_GE(year, boost::gregorian::greg_year::min());
  DCHECK_LE(year, boost::gregorian::greg_year::max());
  DCHECK_GE(month, boost::gregorian::greg_month::min());
  DCHECK_LE(month, boost::gregorian::greg_month::max());
  DCHECK_GE(day, boost::gregorian::greg_day::min());
  DCHECK_LE(day, boost::gregorian::greg_day::max());
}

TimestampVal boost_from_utc(const TimestampVal& ts_val) {
  if (ts_val.is_null) return TimestampVal::null();
  const TimestampValue& ts_value = TimestampValue::FromTimestampVal(ts_val);
  if (!ts_value.HasDateAndTime()) return TimestampVal::null();

  try {
    boost::posix_time::ptime temp;
    ts_value.ToPtime(&temp);
    boost::local_time::local_date_time lt(temp, BOOST_PST_TZ);
    boost::posix_time::ptime local_time = lt.local_time();
    boost_throw_if_date_out_of_range(local_time.date());
    TimestampVal return_val;
    TimestampValue(local_time).ToTimestampVal(&return_val);
    return return_val;
  } catch (boost::exception&) {
    return TimestampVal::null();
  }
}

// CCTZ
TimestampVal cctz_from_utc(const TimestampVal& ts_val) {
  if (ts_val.is_null) return TimestampVal::null();
  TimestampValue ts_value = TimestampValue::FromTimestampVal(ts_val);
  if (UNLIKELY(!ts_value.HasDateAndTime())) return TimestampVal::null();

  ts_value = cctz_utc_to_local(ts_value);
  if (UNLIKELY(!ts_value.HasDateAndTime())) return TimestampVal::null();

  TimestampVal ts_val_ret;
  ts_value.ToTimestampVal(&ts_val_ret);
  return ts_val_ret;
}


//
// Test UnixTimeToLocalPtime (CCTZ is expected to be faster than glibc)
//

// glibc
boost::posix_time::ptime glibc_unix_time_to_local_ptime(const time_t& unix_time) {
  tm temp_tm;
  if (UNLIKELY(localtime_r(&unix_time, &temp_tm) == nullptr)) {
    return boost::posix_time::ptime(boost::posix_time::not_a_date_time);
  }
  try {
    return boost::posix_time::ptime_from_tm(temp_tm);
  } catch (std::exception&) {
    return boost::posix_time::ptime(boost::posix_time::not_a_date_time);
  }
}

// CCTZ
boost::posix_time::ptime cctz_unix_time_to_local_ptime(const time_t& unix_time) {
  cctz::time_point<cctz::sys_seconds> from_tp =
      cctz_unix_time_to_time_point(unix_time);
  cctz::civil_second to_cs = cctz::convert(from_tp, *PTR_CCTZ_LOCAL_TZ);
  // boost::gregorian::date() throws boost::gregorian::bad_year if year is not in the
  // 1400..9999 range. Need to check validity before creating the date object.
  if (UNLIKELY(cctz_check_if_date_out_of_range(to_cs))) {
    return boost::posix_time::ptime(boost::posix_time::not_a_date_time);
  } else {
    return boost::posix_time::ptime(
        boost::gregorian::date(to_cs.year(), to_cs.month(), to_cs.day()),
        boost::posix_time::time_duration(to_cs.hour(), to_cs.minute(), to_cs.second()));
  }
}


//
// Test UnixTimeToUtcPtime (fast path is expected to be the fastest, followed by glibc
// and CCTZ)
//

// glibc
boost::posix_time::ptime glibc_unix_time_to_utc_ptime(const time_t& unix_time) {
  tm temp_tm;
  if (UNLIKELY(gmtime_r(&unix_time, &temp_tm) == nullptr)) {
    return boost::posix_time::ptime(boost::posix_time::not_a_date_time);
  }
  try {
    return boost::posix_time::ptime_from_tm(temp_tm);
  } catch (std::exception&) {
    return boost::posix_time::ptime(boost::posix_time::not_a_date_time);
  }
}

// Fast path
boost::posix_time::ptime fastpath_unix_time_to_utc_ptime(const time_t& unix_time) {
  // Minimum Unix time that can be converted with from_time_t: 1677-Sep-21 00:12:44
  const int64_t MIN_BOOST_CONVERT_UNIX_TIME = -9223372036;
  // Maximum Unix time that can be converted with from_time_t: 2262-Apr-11 23:47:16
  const int64_t MAX_BOOST_CONVERT_UNIX_TIME = 9223372036;
  if (LIKELY(unix_time >= MIN_BOOST_CONVERT_UNIX_TIME &&
             unix_time <= MAX_BOOST_CONVERT_UNIX_TIME)) {
    try {
      return boost::posix_time::from_time_t(unix_time);
    } catch (std::exception&) {
    }
  }
  return boost::posix_time::ptime(boost::posix_time::not_a_date_time);
}

// CCTZ
boost::posix_time::ptime cctz_unix_time_to_utc_ptime(const time_t& unix_time) {
  cctz::time_point<cctz::sys_seconds> from_tp =
      cctz_unix_time_to_time_point(unix_time);
  cctz::civil_second to_cs = cctz::convert(from_tp, TimezoneDatabase::GetUtcTimezone());
  // boost::gregorian::date() throws boost::gregorian::bad_year if year is not in the
  // 1400..9999 range. Need to check validity before creating the date object.
  if (UNLIKELY(cctz_check_if_date_out_of_range(to_cs))) {
    return boost::posix_time::ptime(boost::posix_time::not_a_date_time);
  } else {
    return boost::posix_time::ptime(
        boost::gregorian::date(to_cs.year(), to_cs.month(), to_cs.day()),
        boost::posix_time::time_duration(to_cs.hour(), to_cs.minute(), to_cs.second()));
  }
}

// Fast path and CCTZ implementation combined.
boost::posix_time::ptime cctz_optimized_unix_time_to_utc_ptime(const time_t& unix_time) {
  // Minimum Unix time that can be converted with from_time_t: 1677-Sep-21 00:12:44
  const int64_t MIN_BOOST_CONVERT_UNIX_TIME = -9223372036;
  // Maximum Unix time that can be converted with from_time_t: 2262-Apr-11 23:47:16
  const int64_t MAX_BOOST_CONVERT_UNIX_TIME = 9223372036;
  if (LIKELY(unix_time >= MIN_BOOST_CONVERT_UNIX_TIME &&
             unix_time <= MAX_BOOST_CONVERT_UNIX_TIME)) {
    try {
      return boost::posix_time::from_time_t(unix_time);
    } catch (std::exception&) {
    }
  }

  return cctz_unix_time_to_utc_ptime(unix_time);
}

boost::posix_time::ptime split_unix_time_to_utc_ptime(const time_t& unix_time) {
  int64_t time = unix_time;
  int32_t days = TimestampValue::SplitTime<24*60*60>(&time);

  return boost::posix_time::ptime(
      boost::gregorian::date(1970, 1, 1) + boost::gregorian::date_duration(days),
      boost::posix_time::nanoseconds(time*NANOS_PER_SEC));
}

TimestampValue sec_split_utc_from_unix_time_micros(const int64_t& unix_time_micros) {
  int64_t ts_seconds = unix_time_micros / MICROS_PER_SEC;
  int64_t micros_part = unix_time_micros - (ts_seconds * MICROS_PER_SEC);
  boost::posix_time::ptime temp = cctz_optimized_unix_time_to_utc_ptime(ts_seconds);
  temp += boost::posix_time::microseconds(micros_part);
  return TimestampValue(temp);
}

TimestampValue day_split_utc_from_unix_time_micros(const int64_t& unix_time_micros) {
  static const boost::gregorian::date EPOCH(1970,1,1);
  int64_t micros = unix_time_micros;
  int32_t days = TimestampValue::SplitTime<24LL*60*60*MICROS_PER_SEC>(&micros);

  return TimestampValue(
      EPOCH + boost::gregorian::date_duration(days),
      boost::posix_time::nanoseconds(micros*1000));
}

struct SplitNanoAndSecond {
  int64_t seconds;
  int64_t nanos;
};

TimestampValue old_split_utc_from_unix_time_nanos(const SplitNanoAndSecond& unix_time) {
  boost::posix_time::ptime temp =
      cctz_optimized_unix_time_to_utc_ptime(unix_time.seconds);
  temp += boost::posix_time::nanoseconds(unix_time.nanos);
  return TimestampValue(temp);
}

TimestampValue new_split_utc_from_unix_time_nanos(const SplitNanoAndSecond& unix_time) {
  // The TimestampValue version is used as it is hard to reproduce the same logic without
  // accessing private members.
  return TimestampValue::FromUnixTimeNanos(unix_time.seconds, unix_time.nanos,
      &TimezoneDatabase::GetUtcTimezone());
}

TimestampValue from_subsecond_unix_time_old(const double& unix_time) {
  const double ONE_BILLIONTH = 0.000000001;
  const time_t unix_time_whole = unix_time;
  boost::posix_time::ptime temp =
      cctz_optimized_unix_time_to_utc_ptime(unix_time_whole);
  int64_t nanos = (unix_time - unix_time_whole) / ONE_BILLIONTH;
  temp += boost::posix_time::nanoseconds(nanos);
  return TimestampValue(temp);
}

TimestampValue from_subsecond_unix_time_new(const double& unix_time) {
  const double ONE_BILLIONTH = 0.000000001;
  int64_t unix_time_whole = unix_time;
  int64_t nanos = (unix_time - unix_time_whole) / ONE_BILLIONTH;
  return TimestampValue::FromUnixTimeNanos(
      unix_time_whole, nanos, &TimezoneDatabase::GetUtcTimezone());
}

//
// Test ToUtc (CCTZ is expected to be faster than boost)
//

// boost
TimestampVal boost_to_utc(const TimestampVal& ts_val) {
  if (ts_val.is_null) return TimestampVal::null();
  const TimestampValue& ts_value = TimestampValue::FromTimestampVal(ts_val);
  if (!ts_value.HasDateAndTime()) return TimestampVal::null();

  try {
    boost::local_time::local_date_time lt(ts_value.date(), ts_value.time(), BOOST_PST_TZ,
        boost::local_time::local_date_time::NOT_DATE_TIME_ON_ERROR);
    boost::posix_time::ptime utc_time = lt.utc_time();
    // The utc_time() conversion does not check ranges - need to explicitly check.
    boost_throw_if_date_out_of_range(utc_time.date());
    TimestampVal return_val;
    TimestampValue(utc_time).ToTimestampVal(&return_val);
    return return_val;
  } catch (boost::exception&) {
    return TimestampVal::null();
  }
}

// CCTZ
inline time_t cctz_time_point_to_unix_time(
    const cctz::time_point<cctz::sys_seconds>& tp) {
  static const cctz::time_point<cctz::sys_seconds> epoch =
      std::chrono::time_point_cast<cctz::sys_seconds>(
          std::chrono::system_clock::from_time_t(0));
  return (tp - epoch).count();
}

TimestampValue cctz_local_to_utc(const TimestampValue& ts_value) {
  DCHECK(ts_value.HasDateAndTime());
  boost::gregorian::date d = ts_value.date();
  boost::posix_time::time_duration t = ts_value.time();

  const cctz::civil_second from_cs(d.year(), d.month(), d.day(),
      t.hours(), t.minutes(), t.seconds());

  // In case of ambiguity invalidate TimestampValue.
  const cctz::time_zone::civil_lookup from_cl = PTR_CCTZ_LOCAL_TZ->lookup(from_cs);
  if (UNLIKELY(from_cl.kind != cctz::time_zone::civil_lookup::UNIQUE)) {
    d = boost::gregorian::date(boost::gregorian::not_a_date_time);
    t = boost::posix_time::time_duration(boost::posix_time::not_a_date_time);
  } else {
    int64_t nanos = t.fractional_seconds();
    boost::posix_time::ptime temp = cctz_optimized_unix_time_to_utc_ptime(
        cctz_time_point_to_unix_time(from_cl.pre));

    d = temp.date();
    // Time-zone conversion rules don't affect fractional seconds, leave them intact.
    t = temp.time_of_day() + boost::posix_time::nanoseconds(nanos);
  }
  return TimestampValue(d, t);
}

TimestampVal cctz_to_utc(const TimestampVal& ts_val) {
  if (ts_val.is_null) return TimestampVal::null();
  TimestampValue ts_value = TimestampValue::FromTimestampVal(ts_val);
  if (UNLIKELY(!ts_value.HasDateAndTime())) return TimestampVal::null();

  ts_value = cctz_local_to_utc(ts_value);
  if (UNLIKELY(!ts_value.HasDateAndTime())) return TimestampVal::null();

  TimestampVal ts_val_ret;
  ts_value.ToTimestampVal(&ts_val_ret);
  return ts_val_ret;
}


int main(int argc, char* argv[]) {
  CpuInfo::Init();
  cout << Benchmark::GetMachineInfo() << endl;

  ABORT_IF_ERROR(TimezoneDatabase::Initialize());

  const string& local_tz_name = TimezoneDatabase::LocalZoneName();
  DCHECK(local_tz_name != "");

  PTR_CCTZ_LOCAL_TZ = TimezoneDatabase::FindTimezone(local_tz_name);
  DCHECK(PTR_CCTZ_LOCAL_TZ != nullptr);

  SimpleDateFormatTokenizer::InitCtx();

  const vector<TimestampValue> tsvalue_data =
      AddTestDataDateTimes(1000, "1953-04-22 01:02:03");

  // Benchmark UtcToUnixTime conversion with glibc/cctz/boost
  Benchmark bm_utc_to_unix("UtcToUnixTime");
  TestData<TimestampValue, time_t, glibc_utc_to_unix_time> glibc_utc_to_unix_data =
      tsvalue_data;
  TestData<TimestampValue, time_t, cctz_utc_to_unix_time> cctz_utc_to_unix_data =
      tsvalue_data;
  TestData<TimestampValue, time_t, boost_utc_to_unix_time> boost_utc_to_unix_data =
      tsvalue_data;

  glibc_utc_to_unix_data.add_to_benchmark(bm_utc_to_unix, "(glibc)");
  cctz_utc_to_unix_data.add_to_benchmark(bm_utc_to_unix, "(Google/CCTZ)");
  boost_utc_to_unix_data.add_to_benchmark(bm_utc_to_unix, "(boost)");
  cout << bm_utc_to_unix.Measure() << endl;

  bail_if_results_dont_match(vector<const vector<time_t>*>{
      &cctz_utc_to_unix_data.result(), &glibc_utc_to_unix_data.result(),
      &boost_utc_to_unix_data.result()});

  // Benchmark LocalToUnixTime conversion with glibc/cctz
  Benchmark bm_local_to_unix("LocalToUnixTime");
  TestData<TimestampValue, time_t, glibc_local_to_unix_time> glibc_local_to_unix_data =
      tsvalue_data;
  TestData<TimestampValue, time_t, cctz_local_to_unix_time> cctz_local_to_unix_data =
      tsvalue_data;

  glibc_local_to_unix_data.add_to_benchmark(bm_local_to_unix, "(glibc)");
  cctz_local_to_unix_data.add_to_benchmark(bm_local_to_unix, "(Google/CCTZ)");
  cout << bm_local_to_unix.Measure() << endl;

  bail_if_results_dont_match(vector<const vector<time_t>*>{
      &glibc_local_to_unix_data.result(), &cctz_local_to_unix_data.result()});

  // Benchmark FromUtc with boost/cctz
  vector<TimestampVal> tsval_data;
  for (const TimestampValue& tsvalue: tsvalue_data) {
    TimestampVal tsval;
    tsvalue.ToTimestampVal(&tsval);
    tsval_data.push_back(tsval);
  }

  Benchmark bm_from_utc("FromUtc");
  TestData<TimestampVal, TimestampVal, boost_from_utc> boost_from_utc_data = tsval_data;
  TestData<TimestampVal, TimestampVal, cctz_from_utc> cctz_from_utc_data = tsval_data;

  boost_from_utc_data.add_to_benchmark(bm_from_utc, "(boost)");
  cctz_from_utc_data.add_to_benchmark(bm_from_utc, "(Google/CCTZ)");
  cout << bm_from_utc.Measure() << endl;

  // We don't expect boost/cctz FromUtc results to match (they use non-compatible
  // time-zone databases for conversion).

  // Benchmark ToUtc with boost/cctz
  Benchmark bm_to_utc("ToUtc");
  TestData<TimestampVal, TimestampVal, boost_to_utc> boost_to_utc_data = tsval_data;
  TestData<TimestampVal, TimestampVal, cctz_to_utc> cctz_to_utc_data = tsval_data;

  boost_to_utc_data.add_to_benchmark(bm_to_utc, "(boost)");
  cctz_to_utc_data.add_to_benchmark(bm_to_utc, "(Google/CCTZ)");
  cout << bm_to_utc.Measure() << endl;

  // We don't expect boost/cctz ToUtc results to match (they use non-compatible time-zone
  // databases for conversion).

  // Benchmark UtcToLocal with glibc/cctz
  Benchmark bm_utc_to_local("UtcToLocal");
  TestData<TimestampValue, TimestampValue, glibc_utc_to_local> glibc_utc_to_local_data =
      tsvalue_data;
  TestData<TimestampValue, TimestampValue, cctz_utc_to_local> cctz_utc_to_local_data =
      tsvalue_data;

  glibc_utc_to_local_data.add_to_benchmark(bm_utc_to_local, "(glibc)");
  cctz_utc_to_local_data.add_to_benchmark(bm_utc_to_local, "(Google/CCTZ)");
  cout << bm_utc_to_local.Measure() << endl;

  bail_if_results_dont_match(vector<const vector<TimestampValue>*>{
      &glibc_utc_to_local_data.result(), &cctz_utc_to_local_data.result()});

  // Benchmark UnixTimeToLocalPtime with glibc/cctz
  vector<time_t> time_data;
  for (const TimestampValue& tsvalue: tsvalue_data) {
    time_t unix_time;
    tsvalue.ToUnixTime(&TimezoneDatabase::GetUtcTimezone(), &unix_time);
    time_data.push_back(unix_time);
  }

  Benchmark bm_unix_time_to_local_ptime("UnixTimeToLocalPtime");
  TestData<
      time_t,
      boost::posix_time::ptime,
      glibc_unix_time_to_local_ptime> glibc_unix_time_to_local_ptime_data = time_data;
  TestData<
    time_t,
    boost::posix_time::ptime,
    cctz_unix_time_to_local_ptime> cctz_unix_time_to_local_ptime_data = time_data;

  glibc_unix_time_to_local_ptime_data.add_to_benchmark(bm_unix_time_to_local_ptime,
      "(glibc)");
  cctz_unix_time_to_local_ptime_data.add_to_benchmark(bm_unix_time_to_local_ptime,
      "(Google/CCTZ)");
  cout << bm_unix_time_to_local_ptime.Measure() << endl;

  bail_if_results_dont_match(vector<const vector<boost::posix_time::ptime>*>{
      &glibc_unix_time_to_local_ptime_data.result(),
      &cctz_unix_time_to_local_ptime_data.result()});

  // Benchmark UnixTimeToUtcPtime with glibc/cctz/fastpath
  Benchmark bm_unix_time_to_utc_ptime("UnixTimeToUtcPtime");
  TestData<
      time_t,
      boost::posix_time::ptime,
      glibc_unix_time_to_utc_ptime> glibc_unix_time_to_utc_ptime_data = time_data;
  TestData<
      time_t,
      boost::posix_time::ptime,
      cctz_unix_time_to_utc_ptime> cctz_unix_time_to_utc_ptime_data = time_data;
  TestData<
      time_t,
      boost::posix_time::ptime,
      fastpath_unix_time_to_utc_ptime> fastpath_unix_time_to_utc_ptime_data = time_data;
  TestData<
      time_t,
      boost::posix_time::ptime,
      split_unix_time_to_utc_ptime> split_unix_time_to_utc_ptime_data = time_data;

  glibc_unix_time_to_utc_ptime_data.add_to_benchmark(bm_unix_time_to_utc_ptime,
      "(glibc)");
  cctz_unix_time_to_utc_ptime_data.add_to_benchmark(bm_unix_time_to_utc_ptime,
      "(Google/CCTZ)");
  fastpath_unix_time_to_utc_ptime_data.add_to_benchmark(bm_unix_time_to_utc_ptime,
      "(fast path)");
  split_unix_time_to_utc_ptime_data.add_to_benchmark(bm_unix_time_to_utc_ptime,
      "(day split)");
  cout << bm_unix_time_to_utc_ptime.Measure() << endl;

  bail_if_results_dont_match(vector<const vector<boost::posix_time::ptime>*>{
      &glibc_unix_time_to_utc_ptime_data.result(),
      &cctz_unix_time_to_utc_ptime_data.result(),
      &fastpath_unix_time_to_utc_ptime_data.result(),
      &split_unix_time_to_utc_ptime_data.result()});

  // Benchmark UtcFromUnixTimeMicros improvement in IMPALA-7417.
  vector<time_t> microsec_data;
  for (int i = 0; i < tsvalue_data.size(); ++i) {
    const TimestampValue& tsvalue = tsvalue_data[i];
    time_t unix_time;
    tsvalue.ToUnixTime(&TimezoneDatabase::GetUtcTimezone(), &unix_time);
    int micros = (i * 1001) % MICROS_PER_SEC; // add some sub-second part
    microsec_data.push_back(unix_time * MICROS_PER_SEC + micros);
  }

  Benchmark bm_utc_from_unix_time_micros("UtcFromUnixTimeMicros");
  TestData<int64_t, TimestampValue, sec_split_utc_from_unix_time_micros>
      sec_split_utc_from_unix_time_micros_data = microsec_data;
  TestData<int64_t, TimestampValue, day_split_utc_from_unix_time_micros>
      day_split_utc_from_unix_time_micros_data = microsec_data;

  sec_split_utc_from_unix_time_micros_data.add_to_benchmark(bm_utc_from_unix_time_micros,
      "(sec split (old))");
  day_split_utc_from_unix_time_micros_data.add_to_benchmark(bm_utc_from_unix_time_micros,
      "(day split)");
  cout << bm_utc_from_unix_time_micros.Measure() << endl;

  bail_if_results_dont_match(vector<const vector<TimestampValue>*>{
      &sec_split_utc_from_unix_time_micros_data.result(),
      &day_split_utc_from_unix_time_micros_data.result()});

  // Benchmark FromUnixTimeNanos improvement in IMPALA-7417.
  vector<SplitNanoAndSecond> nanosec_data;
  for (int i = 0; i < tsvalue_data.size(); ++i) {
    const TimestampValue& tsvalue = tsvalue_data[i];
    time_t unix_time;
    tsvalue.ToUnixTime(&TimezoneDatabase::GetUtcTimezone(), &unix_time);
    int nanos = (i * 1001) % NANOS_PER_SEC; // add some sub-second part
    nanosec_data.push_back(SplitNanoAndSecond {unix_time, nanos} );
  }

  Benchmark bm_utc_from_unix_time_nanos("FromUnixTimeNanos");
  TestData<SplitNanoAndSecond, TimestampValue, old_split_utc_from_unix_time_nanos>
      old_split_utc_from_unix_time_nanos_data = nanosec_data;
  TestData<SplitNanoAndSecond, TimestampValue, new_split_utc_from_unix_time_nanos>
      new_split_utc_from_unix_time_nanos_data = nanosec_data;

  old_split_utc_from_unix_time_nanos_data.add_to_benchmark(bm_utc_from_unix_time_nanos,
      "(sec split (old))");
  new_split_utc_from_unix_time_nanos_data.add_to_benchmark(bm_utc_from_unix_time_nanos,
      "(sec split (new))");
  cout << bm_utc_from_unix_time_nanos.Measure() << endl;

  bail_if_results_dont_match(vector<const vector<TimestampValue>*>{
      &old_split_utc_from_unix_time_nanos_data.result(),
      &new_split_utc_from_unix_time_nanos_data.result()});

  // Benchmark FromSubsecondUnixTime before and after IMPALA-7417.
  vector<double> double_data;
  for (int i = 0; i < tsvalue_data.size(); ++i) {
    const TimestampValue& tsvalue = tsvalue_data[i];
    time_t unix_time;
    tsvalue.ToUnixTime(&TimezoneDatabase::GetUtcTimezone(), &unix_time);
    double nanos = (i * 1001) % NANOS_PER_SEC; // add some sub-second part
    double_data.push_back((double)unix_time + nanos / NANOS_PER_SEC);
  }

  Benchmark from_subsecond_unix_time("FromSubsecondUnixTime");
  TestData<double, TimestampValue, from_subsecond_unix_time_old>
      from_subsecond_unix_time_old_data = double_data;
  TestData<double, TimestampValue, from_subsecond_unix_time_new>
      from_subsecond_unix_time_new_data = double_data;

  from_subsecond_unix_time_old_data.add_to_benchmark(from_subsecond_unix_time, "(old)");
  from_subsecond_unix_time_new_data.add_to_benchmark(from_subsecond_unix_time, "(new)");
  cout << from_subsecond_unix_time.Measure() << endl;

  bail_if_results_dont_match(vector<const vector<TimestampValue>*>{
      &from_subsecond_unix_time_old_data.result(),
      &from_subsecond_unix_time_new_data.result()});

  // If number of threads is specified, run multithreaded tests.
  int num_of_threads = (argc < 2) ? 0 : atoi(argv[1]);
  if (num_of_threads >= 1) {
    const int BATCH_SIZE = 1000;
    cout << "Number of threads: " << num_of_threads << endl;

    uint64_t m1 = 0, m2 = 0, m3 = 0;

    // UtcToUnixTime
    cout << endl << "UtcToUnixTime:" << endl;
    m1 = glibc_utc_to_unix_data.measure_multithreaded_elapsed_time(
        num_of_threads, BATCH_SIZE, tsvalue_data, "(glibc)");
    m2 = cctz_utc_to_unix_data.measure_multithreaded_elapsed_time(
        num_of_threads, BATCH_SIZE, tsvalue_data, "(Google/CCTZ)");
    m3 = boost_utc_to_unix_data.measure_multithreaded_elapsed_time(
        num_of_threads, BATCH_SIZE, tsvalue_data, "(boost)");
    cout << "cctz speedup: " << double(m1)/double(m2) << endl;
    cout << "boost speedup: " << double(m1)/double(m3) << endl;

    // LocalToUnixTime
    cout << endl << "LocalToUnixTime:" << endl;
    m1 = glibc_local_to_unix_data.measure_multithreaded_elapsed_time(
        num_of_threads, BATCH_SIZE, tsvalue_data, "(glibc)");
    m2 = cctz_local_to_unix_data.measure_multithreaded_elapsed_time(
        num_of_threads, BATCH_SIZE, tsvalue_data, "(Google/CCTZ)");
    cout << "speedup: " << double(m1)/double(m2) << endl;

    // FromUtc
    cout << endl << "FromUtc:" << endl;
    m1 = boost_from_utc_data.measure_multithreaded_elapsed_time(
        num_of_threads, BATCH_SIZE, tsval_data, "(boost)");
    m2 = cctz_from_utc_data.measure_multithreaded_elapsed_time(
        num_of_threads, BATCH_SIZE, tsval_data, "(Google/CCTZ)");
    cout << "speedup: " << double(m1)/double(m2) << endl;

    // ToUtc
    cout << endl << "ToUtc:" << endl;
    m1 = boost_to_utc_data.measure_multithreaded_elapsed_time(
        num_of_threads, BATCH_SIZE, tsval_data, "(boost)");
    m2 = cctz_to_utc_data.measure_multithreaded_elapsed_time(
        num_of_threads, BATCH_SIZE, tsval_data, "(Google/CCTZ)");
    cout << "speedup: " << double(m1)/double(m2) << endl;

    // UtcToLocal
    cout << endl <<  "UtcToLocal:" << endl;
    m1 = glibc_utc_to_local_data.measure_multithreaded_elapsed_time(
        num_of_threads, BATCH_SIZE, tsvalue_data, "(glibc)");
    m2 = cctz_utc_to_local_data.measure_multithreaded_elapsed_time(
        num_of_threads, BATCH_SIZE, tsvalue_data, "(Google/CCTZ)");
    cout << "speedup: " << double(m1)/double(m2) << endl;

    // UnixTimeToLocalPtime
    cout << endl << "UnixTimeToLocalPtime:" << endl;
    m1 = glibc_unix_time_to_local_ptime_data.measure_multithreaded_elapsed_time(
        num_of_threads, BATCH_SIZE, time_data, "(glibc)");
    m2 = cctz_unix_time_to_local_ptime_data.measure_multithreaded_elapsed_time(
        num_of_threads, BATCH_SIZE, time_data, "(Google/CCTZ)");
    cout << "speedup: " << double(m1)/double(m2) << endl;

    // UnixTimeToUtcPtime
    cout << endl << "UnixTimeToUtcPtime:" << endl;
    m1 = glibc_unix_time_to_utc_ptime_data.measure_multithreaded_elapsed_time(
        num_of_threads, BATCH_SIZE, time_data, "(glibc)");
    m2 = cctz_unix_time_to_utc_ptime_data.measure_multithreaded_elapsed_time(
        num_of_threads, BATCH_SIZE, time_data, "(Google/CCTZ)");
    m3 = fastpath_unix_time_to_utc_ptime_data.measure_multithreaded_elapsed_time(
        num_of_threads, BATCH_SIZE, time_data, "(fast path)");
    cout << "cctz speedup: " << double(m1)/double(m2) << endl;
    cout << "fast path speedup: " << double(m1)/double(m3) << endl;
  }
  return 0;
}
