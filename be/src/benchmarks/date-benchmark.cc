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

#include <iostream>
#include <vector>

#include "cctz/civil_time.h"
#include "gutil/basictypes.h"
#include "runtime/date-value.h"
#include "util/benchmark.h"
#include "util/cpu-info.h"

#include "common/names.h"

using namespace impala;

// Machine Info: Intel(R) Core(TM) i5-6600 CPU @ 3.30GHz
// ToYear:             Function  iters/ms   10%ile   50%ile   90%ile     10%ile     50%ile     90%ile
//                                                                   (relative) (relative) (relative)
// --------------------------------------------------------------------------------------------------
//           TestToYearMonthDay              0.167    0.169    0.169         1X         1X         1X
//                   TestToYear               4.63     4.72     4.72      27.8X      27.8X      27.8X

class TestData {
public:
  void AddRange(const DateValue& dv_min, const DateValue& dv_max) {
    DCHECK(dv_min.IsValid());
    DCHECK(dv_max.IsValid());
    DateValue dv = dv_min;
    while (dv < dv_max) {
      date_.push_back(dv);
      dv = dv.AddDays(1);
      DCHECK(dv.IsValid());
    }
    to_ymd_result_year_.resize(date_.size());
    to_y_result_year_.resize(date_.size());
  }

  void TestToYearMonthDay(int batch_size) {
    DCHECK(date_.size() == to_ymd_result_year_.size());
    int month, day;
    for (int i = 0; i < batch_size; ++i) {
      int n = date_.size();
      for (int j = 0; j < n; ++j) {
        ignore_result(date_[j].ToYearMonthDay(&to_ymd_result_year_[j], &month, &day));
      }
    }
  }

  void TestToYear(int batch_size) {
    DCHECK(date_.size() == to_y_result_year_.size());
    for (int i = 0; i < batch_size; ++i) {
      int n = date_.size();
      for (int j = 0; j < n; ++j) {
        ignore_result(date_[j].ToYear(&to_y_result_year_[j]));
      }
    }
  }

  bool CheckResults() {
    DCHECK(to_ymd_result_year_.size() == to_y_result_year_.size());
    bool ok = true;
    for (int i = 0; i < to_ymd_result_year_.size(); ++i) {
      if (to_ymd_result_year_[i] != to_y_result_year_[i]) {
        cerr << "Incorrect results " << date_[i] << ": "
            << to_ymd_result_year_[i] << " != " << to_y_result_year_[i]
            << endl;
        ok = false;
      }
    }
    return ok;
  }

private:
  vector<DateValue> date_;
  vector<int> to_ymd_result_year_;
  vector<int> to_y_result_year_;
};

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
  data.AddRange(DateValue(1965, 1, 1), DateValue(2020, 12, 31));

  // Benchmark DateValue::ToYearMonthDay() vs DateValue::ToYear()
  Benchmark suite("ToYear");
  suite.AddBenchmark("TestToYearMonthDay", TestToYearMonthDay, &data);
  suite.AddBenchmark("TestToYear", TestToYear, &data);
  cout << suite.Measure();

  return data.CheckResults() ? 0 : 1;
}
