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
#include "util/decimal-util.h"
#include "util/string-parser.h"

#include "common/names.h"

using namespace impala;

// Machine Info: Intel(R) Core(TM) i7-4790K CPU @ 4.00GHz
// atod:                 Function     Rate (iters/ms)          Comparison
//----------------------------------------------------------------------
//               Impala Decimal4                84.6                  1X
//               Impala Decimal8               49.77             0.5883X
//              Impala Decimal16               17.08             0.2019X

template <typename Decimal>
struct TestData {
  int precision;
  int scale;
  double probability_negative;
  vector<StringValue> data;
  vector<string> memory;
  vector<Decimal> result;
};

double Rand() {
  return rand() / static_cast<double>(RAND_MAX);
}

template <typename Decimal>
void AddTestData(TestData<Decimal>* data, const string& input) {
  data->memory.push_back(input);
  const string& str = data->memory.back();
  data->data.push_back(StringValue(const_cast<char*>(str.c_str()), str.length()));
}

template <typename Decimal>
void AddTestData(TestData<Decimal>* data, int n) {
  int128_t max_whole = data->precision > data->scale ?
      DecimalUtil::GetScaleMultiplier<int128_t>(data->precision - data->scale) - 1 : 0;
  int128_t max_fraction = data->scale > 0 ?
      DecimalUtil::GetScaleMultiplier<int128_t>(data->scale) - 1 : 0;
  for (int i = 0; i < n; ++i) {
    stringstream ss;
    if (data->probability_negative > Rand()) ss << "-";
    if (max_whole > 0) ss << static_cast<int128_t>(max_whole * Rand());
    if (max_fraction > 0) ss << "." << static_cast<int128_t>(max_fraction * Rand());
    AddTestData(data, ss.str());
  }
}

template <typename Decimal, typename Storage>
void TestImpala(int batch_size, void* d) {
  TestData<Decimal>* data = reinterpret_cast<TestData<Decimal>*>(d);
  ColumnType column_type = ColumnType::CreateDecimalType(data->precision, data->scale);
  Decimal val;
  for (int i = 0; i < batch_size; ++i) {
    int n = data->data.size();
    for (int j = 0; j < n; ++j) {
      const StringValue& str = data->data[j];
      StringParser::ParseResult dummy;
      val = StringParser::StringToDecimal<Storage>(
          str.Ptr(), str.Len(), column_type, false, &dummy);
      data->result[j] = val;
    }
  }
}

int main(int argc, char **argv) {
  CpuInfo::Init();
  cout << Benchmark::GetMachineInfo() << endl;

  Benchmark suite("atod");

  TestData<Decimal4Value> data4;
  data4.precision = ColumnType::MAX_DECIMAL4_PRECISION;
  data4.scale = data4.precision / 2;
  data4.probability_negative = 0.25;
  AddTestData(&data4, 1000);
  data4.result.resize(data4.data.size());
  suite.AddBenchmark("Impala Decimal4", TestImpala<Decimal4Value, int32_t>, &data4);

  TestData<Decimal8Value> data8;
  data8.precision = ColumnType::MAX_DECIMAL8_PRECISION;
  data8.scale = data8.precision / 2;
  data8.probability_negative = 0.25;
  AddTestData(&data8, 1000);
  data8.result.resize(data8.data.size());
  suite.AddBenchmark("Impala Decimal8", TestImpala<Decimal8Value, int64_t>, &data8);

  TestData<Decimal16Value> data16;
  data16.precision = ColumnType::MAX_PRECISION;
  data16.scale = data16.precision / 2;
  data16.probability_negative = 0.25;
  AddTestData(&data16, 1000);
  data16.result.resize(data16.data.size());
  suite.AddBenchmark("Impala Decimal16", TestImpala<Decimal16Value, int128_t>, &data16);

  cout << suite.Measure();
  return 0;
}
