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
#include "util/benchmark.h"
#include "util/cpu-info.h"

#include "common/names.h"

using std::random_device;
using std::mt19937;
using std::uniform_int_distribution;

using namespace impala;

// Machine Info: Intel(R) Core(TM) i5-6600 CPU @ 3.30GHz
// ToYearMonthDay:     Function  iters/ms   10%ile   50%ile   90%ile     10%ile     50%ile     90%ile
//                                                                   (relative) (relative) (relative)
// --------------------------------------------------------------------------------------------------
//       TestCctzToYearMonthDay               0.24    0.245     0.25         1X         1X         1X
//           TestToYearMonthDay                1.3     1.33     1.38      5.42X      5.44X      5.53X
//                   TestToYear               8.67     8.86     9.04      36.1X      36.1X      36.2X

const cctz::civil_day EPOCH_DATE(1970, 1, 1);

class TestData {
public:
  void AddRandomRange(const DateValue& dv_min, const DateValue& dv_max) {
    DCHECK(dv_min.IsValid());
    DCHECK(dv_max.IsValid());

    int32_t min_dse, max_dse;
    ignore_result(dv_min.ToDaysSinceEpoch(&min_dse));
    ignore_result(dv_max.ToDaysSinceEpoch(&max_dse));

    random_device rd;
    mt19937 gen(rd());
    // Random values in a [min_dse..max_dse] days range.
    uniform_int_distribution<int32_t> dis_dse(min_dse, max_dse);

    // Add random DateValue values in the [dv_min, dv_max] range.
    for (int i = 0; i <= max_dse - min_dse; ++i) {
      DateValue dv(dis_dse(gen));
      DCHECK(dv.IsValid());
      date_.push_back(dv);
    }
    cctz_to_ymd_result_.resize(date_.size());
    to_ymd_result_.resize(date_.size());
    to_year_result_.resize(date_.size());
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
  vector<YearMonthDayResult> cctz_to_ymd_result_;
  vector<YearMonthDayResult> to_ymd_result_;
  vector<int> to_year_result_;
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

int main(int argc, char* argv[]) {
  CpuInfo::Init();
  cout << Benchmark::GetMachineInfo() << endl;

  TestData data;
  data.AddRandomRange(DateValue(1965, 1, 1), DateValue(2020, 12, 31));

  // Benchmark TestData::CctzToYearMonthDay(), DateValue::ToYearMonthDay() and
  // DateValue::ToYear().
  Benchmark suite("ToYearMonthDay");
  suite.AddBenchmark("TestCctzToYearMonthDay", TestCctzToYearMonthDay, &data);
  suite.AddBenchmark("TestToYearMonthDay", TestToYearMonthDay, &data);
  suite.AddBenchmark("TestToYear", TestToYear, &data);
  cout << suite.Measure();

  return data.CheckResults() ? 0 : 1;
}
