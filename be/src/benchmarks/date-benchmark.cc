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

#include <iomanip>
#include <iostream>
#include <random>
#include <vector>

#include "cctz/civil_time.h"
#include "gutil/basictypes.h"
#include "runtime/date-value.h"
#include "runtime/datetime-simple-date-format-parser.h"
#include "runtime/date-parse-util.h"
#include "runtime/timestamp-parse-util.h"
#include "util/benchmark.h"
#include "util/cpu-info.h"

#include "common/names.h"

using std::random_device;
using std::mt19937;
using std::uniform_int_distribution;

using namespace impala;
using datetime_parse_util::SimpleDateFormatTokenizer;

// ToYearMonthDay:            Function  iters/ms   10%ile   50%ile   90%ile     10%ile     50%ile     90%ile
//                                                                          (relative) (relative) (relative)
// ---------------------------------------------------------------------------------------------------------
//             TestCctzToYearMonthDay               16.5     16.6     16.7         1X         1X         1X
//                 TestToYearMonthDay               61.1     62.1     62.3      3.69X      3.75X      3.73X
//                         TestToYear                280      308      308      16.9X      18.6X      18.5X
//                       TestToString                 18     19.5     19.7      1.09X      1.18X      1.18X
//          TestToString_stringstream               1.86     2.08     2.12     0.113X     0.125X     0.127X
//           TestDefaultDateToCharBuf               25.5       27     27.2      1.54X      1.63X      1.63X
//              TestTimestampToString               11.7     12.6     12.6     0.707X      0.76X     0.757X
//      TestDefaultTimestampToCharBuf               15.7     17.2     17.2     0.949X      1.04X      1.03X





const cctz::civil_day EPOCH_DATE(1970, 1, 1);

class TestData {
public:
  void AddRandomRange(const DateValue& dv_min, const DateValue& dv_max,
      int data_size) {
    DCHECK(dv_min.IsValid());
    DCHECK(dv_max.IsValid());

    int32_t min_dse, max_dse;
    ignore_result(dv_min.ToDaysSinceEpoch(&min_dse));
    ignore_result(dv_max.ToDaysSinceEpoch(&max_dse));

    random_device rd;
    mt19937 gen(rd());
    // Random values in a [min_dse..max_dse] days range.
    uniform_int_distribution<int32_t> dis_dse(min_dse, max_dse);
    uniform_int_distribution<int64_t> dis_utc(-9223372036, 9223372036);

    // Add random DateValue values in the [dv_min, dv_max] range.
    for (int i = 0; i <= data_size; ++i) {
      DateValue dv(dis_dse(gen));
      DCHECK(dv.IsValid());
      date_.push_back(dv);
      timestamp_.push_back(TimestampValue::FromUnixTime(dis_utc(gen), UTCPTR));
    }
    cctz_to_ymd_result_.resize(date_.size());
    to_ymd_result_.resize(date_.size());
    to_year_result_.resize(date_.size());
    to_string_result_.resize(date_.size());
    to_string_old_result_.resize(date_.size());
    date_to_char_buf_result_.resize(timestamp_.size());
    timestamp_to_char_buf_result_.resize(timestamp_.size());
    timestamp_to_string_result_.resize(timestamp_.size());
    for(int i = 0; i < timestamp_.size(); ++i) {
      timestamp_to_char_buf_result_[i].reserve(
          SimpleDateFormatTokenizer::DEFAULT_DATE_TIME_FMT_LEN);
      timestamp_to_string_result_[i].reserve(
          SimpleDateFormatTokenizer::DEFAULT_DATE_TIME_FMT_LEN);
    }
  }

  void CctzToYearMonthDay(const DateValue& dv, int* year, int* month, int* day) const {
    int32_t days_since_epoch;
    ignore_result(dv.ToDaysSinceEpoch(&days_since_epoch));

    const cctz::civil_day cd = EPOCH_DATE + days_since_epoch;
    *year = cd.year();
    *month = cd.month();
    *day = cd.day();
  }

  void TestCctzToYearMonthDay(int batch_size) {
    DCHECK(date_.size() == cctz_to_ymd_result_.size());
    for (int i = 0; i < batch_size; ++i) {
      int n = date_.size();
      for (int j = 0; j < n; ++j) {
        CctzToYearMonthDay(date_[j],
            &cctz_to_ymd_result_[j].year_,
            &cctz_to_ymd_result_[j].month_,
            &cctz_to_ymd_result_[j].day_);
      }
    }
  }

  void TestToYearMonthDay(int batch_size) {
    DCHECK(date_.size() == to_ymd_result_.size());
    for (int i = 0; i < batch_size; ++i) {
      int n = date_.size();
      for (int j = 0; j < n; ++j) {
        ignore_result(date_[j].ToYearMonthDay(
            &to_ymd_result_[j].year_,
            &to_ymd_result_[j].month_,
            &to_ymd_result_[j].day_));
      }
    }
  }

  void TestToYear(int batch_size) {
    DCHECK(date_.size() == to_year_result_.size());
    for (int i = 0; i < batch_size; ++i) {
      int n = date_.size();
      for (int j = 0; j < n; ++j) {
        ignore_result(date_[j].ToYear(&to_year_result_[j]));
      }
    }
  }

  void TestToString(int batch_size) {
    for (int i = 0; i < batch_size; ++i) {
      int n = date_.size();
      for (int j = 0; j < n; ++j) {
        to_string_result_[j] = date_[j].ToString();
      }
    }
  }

  void TestDefaultDateToCharBuf(int batch_size) {
    for (int i = 0; i < batch_size; ++i) {
      int n = date_.size();
      for (int j = 0; j < n; ++j) {
        date_to_char_buf_result_[j].resize(
            SimpleDateFormatTokenizer::DEFAULT_DATE_FMT_LEN);
        DateParser::FormatDefault(date_[j], &(date_to_char_buf_result_[j][0]));
      }
    }
  }

  void TestTimestampToString(int batch_size) {
    for (int i = 0; i < batch_size; ++i) {
      int n = timestamp_.size();
      for (int j = 0; j < n; ++j) {
        timestamp_to_string_result_[j] = timestamp_[j].ToString();
      }
    }
  }

  void TestDefaultTimestampToCharBuf(int batch_size) {
    for (int i = 0; i < batch_size; ++i) {
      int n = timestamp_.size();
      for (int j = 0; j < n; ++j) {
        const uint32 len = timestamp_[j].time().fractional_seconds() ?
            SimpleDateFormatTokenizer::DEFAULT_DATE_TIME_FMT_LEN :
            SimpleDateFormatTokenizer::DEFAULT_SHORT_DATE_TIME_FMT_LEN;

        char buf[len + 1];
        TimestampParser::FormatDefault(timestamp_[j].date(), timestamp_[j].time(), buf);
        buf[len] = '\0';
        timestamp_to_char_buf_result_[j] = buf;
      }
    }
  }

  void TestToString_stringstream(int batch_size) {
    for (int i = 0; i < batch_size; ++i) {
      int n = date_.size();
      for (int j = 0; j < n; ++j) {
        // Old implementation before IMPALA-12111.
        stringstream ss;
        int year, month, day;
        if (date_[j].ToYearMonthDay(&year, &month, &day)) {
          ss << std::setfill('0') << setw(4) << year << "-" << setw(2) << month << "-"
            << setw(2) << day;
        }
        to_string_old_result_[j] = ss.str();
      }
    }
  }

  bool CheckResults() {
    DCHECK(to_ymd_result_.size() == cctz_to_ymd_result_.size());
    DCHECK(to_year_result_.size() == cctz_to_ymd_result_.size());
    bool ok = true;
    for (int i = 0; i < cctz_to_ymd_result_.size(); ++i) {
      if (to_ymd_result_[i] != cctz_to_ymd_result_[i]) {
        cerr << "Incorrect results (ToYearMonthDay() vs CctzToYearMonthDay()): "
             << date_[i] << ": " << to_ymd_result_[i].ToString() << " != "
             << cctz_to_ymd_result_[i].ToString() << endl;
        ok = false;
      }
      if (to_year_result_[i] != cctz_to_ymd_result_[i].year_) {
        cerr << "Incorrect results (ToYear() vs CctzToYearMonthDay()): " << date_[i]
             << ": " << to_year_result_[i] << " != " << cctz_to_ymd_result_[i].year_
             << endl;
        ok = false;
      }
      if (to_string_result_[i] != to_string_old_result_[i]) {
        cerr << "Incorrect results (ToString() vs TestToString_stringstream()): "
             << to_string_result_[i] << " != " << to_string_old_result_[i] << endl;
        ok = false;
      }
      if (date_to_char_buf_result_[i] != to_string_result_[i]) {
        cerr << "Incorrect results (TestDefaultDateToCharBuf() vs ToString()): "
             << date_to_char_buf_result_[i] << " != " << to_string_result_[i] << endl;
        ok = false;
      }
      if (timestamp_to_char_buf_result_[i] != timestamp_to_string_result_[i]) {
        cerr << "Incorrect results (TestDefaultTimestampToCharBuf()"
             << " vs TestTimestampToString()): "
             << timestamp_to_char_buf_result_[i] << " != "
             << timestamp_to_string_result_[i] << endl;
        ok = false;
      }
    }
    return ok;
  }

private:
  struct YearMonthDayResult {
    int year_;
    int month_;
    int day_;

    string ToString() const {
      stringstream ss;
      ss << std::setfill('0') << setw(4) << year_ << "-" << setw(2) << month_ << "-"
         << setw(2) << day_;
      return ss.str();
    }

    bool operator!=(const YearMonthDayResult& other) const {
      return year_ != other.year_ || month_ != other.month_ || day_ != other.day_;
    }
  };

  vector<DateValue> date_;
  vector<TimestampValue> timestamp_;
  vector<YearMonthDayResult> cctz_to_ymd_result_;
  vector<YearMonthDayResult> to_ymd_result_;
  vector<int> to_year_result_;
  vector<string> to_string_result_;
  vector<string> to_string_old_result_;
  vector<string> date_to_char_buf_result_;
  vector<string> timestamp_to_char_buf_result_;
  vector<string> timestamp_to_string_result_;
};

void TestCctzToYearMonthDay(int batch_size, void* d) {
  TestData* data = reinterpret_cast<TestData*>(d);
  data->TestCctzToYearMonthDay(batch_size);
}

void TestToYearMonthDay(int batch_size, void* d) {
  TestData* data = reinterpret_cast<TestData*>(d);
  data->TestToYearMonthDay(batch_size);
}

void TestToYear(int batch_size, void* d) {
  TestData* data = reinterpret_cast<TestData*>(d);
  data->TestToYear(batch_size);
}

void TestToString(int batch_size, void* d) {
  TestData* data = reinterpret_cast<TestData*>(d);
  data->TestToString(batch_size);
}

void TestDefaultDateToCharBuf(int batch_size, void* d) {
  TestData* data = reinterpret_cast<TestData*>(d);
  data->TestDefaultDateToCharBuf(batch_size);
}

void TestTimestampToString(int batch_size, void* d) {
  TestData* data = reinterpret_cast<TestData*>(d);
  data->TestTimestampToString(batch_size);
}

void TestDefaultTimestampToCharBuf(int batch_size, void* d) {
  TestData* data = reinterpret_cast<TestData*>(d);
  data->TestDefaultTimestampToCharBuf(batch_size);
}

void TestToString_stringstream(int batch_size, void* d) {
  TestData* data = reinterpret_cast<TestData*>(d);
  data->TestToString_stringstream(batch_size);
}

int main(int argc, char* argv[]) {
  CpuInfo::Init();
  cout << Benchmark::GetMachineInfo() << endl;

  datetime_parse_util::SimpleDateFormatTokenizer::InitCtx();

  TestData data;
  data.AddRandomRange(DateValue(1965, 1, 1), DateValue(2020, 12, 31), 1000);

  // Benchmark TestData::CctzToYearMonthDay(), DateValue::ToYearMonthDay(),
  // DateValue::ToYear() and DateValue().ToString's current and old implementation.
  Benchmark suite("ToYearMonthDay");
  suite.AddBenchmark("TestCctzToYearMonthDay", TestCctzToYearMonthDay, &data);
  suite.AddBenchmark("TestToYearMonthDay", TestToYearMonthDay, &data);
  suite.AddBenchmark("TestToYear", TestToYear, &data);
  suite.AddBenchmark("TestToString", TestToString, &data);
  suite.AddBenchmark("TestToString_stringstream", TestToString_stringstream, &data);
  suite.AddBenchmark("TestDefaultDateToCharBuf", TestDefaultDateToCharBuf, &data);
  suite.AddBenchmark("TestTimestampToString", TestTimestampToString, &data);
  suite.AddBenchmark("TestDefaultTimestampToCharBuf", TestDefaultTimestampToCharBuf,
      &data);
  cout << suite.Measure();

  return data.CheckResults() ? 0 : 1;
}
