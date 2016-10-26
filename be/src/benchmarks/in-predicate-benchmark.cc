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


// Note: The results do not include the pre-processing in the prepare function that is
// necessary for SetLookup but not Iterate. None of the values searched for are in the
// fabricated IN list (i.e. hit rate is 0).

// Machine Info: Intel(R) Core(TM) i7-2600 CPU @ 3.40GHz
// int n=1:              Function     Rate (iters/ms)          Comparison
// ----------------------------------------------------------------------
//                  SetLookup n=1               902.4                  1X
//                    Iterate n=1               938.3               1.04X

// int n=2:              Function     Rate (iters/ms)          Comparison
// ----------------------------------------------------------------------
//                  SetLookup n=2               888.6                  1X
//                    Iterate n=2               805.6             0.9066X

// int n=3:              Function     Rate (iters/ms)          Comparison
// ----------------------------------------------------------------------
//                  SetLookup n=3               806.5                  1X
//                    Iterate n=3               744.1             0.9227X

// int n=4:              Function     Rate (iters/ms)          Comparison
// ----------------------------------------------------------------------
//                  SetLookup n=4               784.1                  1X
//                    Iterate n=4                 661              0.843X

// int n=5:              Function     Rate (iters/ms)          Comparison
// ----------------------------------------------------------------------
//                  SetLookup n=5               801.6                  1X
//                    Iterate n=5               594.4             0.7415X

// int n=6:              Function     Rate (iters/ms)          Comparison
// ----------------------------------------------------------------------
//                  SetLookup n=6               746.6                  1X
//                    Iterate n=6                 539              0.722X

// int n=7:              Function     Rate (iters/ms)          Comparison
// ----------------------------------------------------------------------
//                  SetLookup n=7               683.4                  1X
//                    Iterate n=7               493.9             0.7226X

// int n=8:              Function     Rate (iters/ms)          Comparison
// ----------------------------------------------------------------------
//                  SetLookup n=8                 772                  1X
//                    Iterate n=8               455.5               0.59X

// int n=9:              Function     Rate (iters/ms)          Comparison
// ----------------------------------------------------------------------
//                  SetLookup n=9               702.9                  1X
//                    Iterate n=9               420.1             0.5976X

// int n=10:             Function     Rate (iters/ms)          Comparison
// ----------------------------------------------------------------------
//                 SetLookup n=10               710.7                  1X
//                   Iterate n=10               392.4             0.5521X

// int n=400:            Function     Rate (iters/ms)          Comparison
// ----------------------------------------------------------------------
//                SetLookup n=400               422.3                  1X
//                  Iterate n=400               14.01            0.03318X


// string n=1:           Function     Rate (iters/ms)          Comparison
// ----------------------------------------------------------------------
//                  SetLookup n=1               250.8                  1X
//                    Iterate n=1               540.4              2.154X

// string n=2:           Function     Rate (iters/ms)          Comparison
// ----------------------------------------------------------------------
//                  SetLookup n=2                 205                  1X
//                    Iterate n=2               297.7              1.453X

// string n=3:           Function     Rate (iters/ms)          Comparison
// ----------------------------------------------------------------------
//                  SetLookup n=3               166.8                  1X
//                    Iterate n=3               240.3              1.441X

// string n=4:           Function     Rate (iters/ms)          Comparison
// ----------------------------------------------------------------------
//                  SetLookup n=4               146.2                  1X
//                    Iterate n=4               177.8              1.216X

// string n=5:           Function     Rate (iters/ms)          Comparison
// ----------------------------------------------------------------------
//                  SetLookup n=5               149.9                  1X
//                    Iterate n=5               144.8             0.9662X

// string n=6:           Function     Rate (iters/ms)          Comparison
// ----------------------------------------------------------------------
//                  SetLookup n=6               135.9                  1X
//                    Iterate n=6               127.4             0.9372X

// string n=7:           Function     Rate (iters/ms)          Comparison
// ----------------------------------------------------------------------
//                  SetLookup n=7               143.4                  1X
//                    Iterate n=7               112.8             0.7866X

// string n=8:           Function     Rate (iters/ms)          Comparison
// ----------------------------------------------------------------------
//                  SetLookup n=8               123.7                  1X
//                    Iterate n=8               117.1             0.9467X

// string n=9:           Function     Rate (iters/ms)          Comparison
// ----------------------------------------------------------------------
//                  SetLookup n=9                 117                  1X
//                    Iterate n=9               89.19              0.762X

// string n=10:          Function     Rate (iters/ms)          Comparison
// ----------------------------------------------------------------------
//                 SetLookup n=10               125.4                  1X
//                   Iterate n=10               81.63             0.6508X

// string n=400:         Function     Rate (iters/ms)          Comparison
// ----------------------------------------------------------------------
//                SetLookup n=400               55.77                  1X
//                  Iterate n=400               1.936            0.03471X

// decimal(4,0) n=1:     Function     Rate (iters/ms)          Comparison
// ----------------------------------------------------------------------
//                  SetLookup n=1               587.9                  1X
//                    Iterate n=1               658.3               1.12X

// decimal(4,0) n=2:     Function     Rate (iters/ms)          Comparison
// ----------------------------------------------------------------------
//                  SetLookup n=2               521.5                  1X
//                    Iterate n=2               478.5             0.9175X

// decimal(4,0) n=3:     Function     Rate (iters/ms)          Comparison
// ----------------------------------------------------------------------
//                  SetLookup n=3                 524                  1X
//                    Iterate n=3               373.7             0.7132X

// decimal(4,0) n=4:     Function     Rate (iters/ms)          Comparison
// ----------------------------------------------------------------------
//                  SetLookup n=4               486.6                  1X
//                    Iterate n=4               308.9             0.6348X

// decimal(4,0) n=400:   Function     Rate (iters/ms)          Comparison
// ----------------------------------------------------------------------
//                SetLookup n=400               258.2                  1X
//                  Iterate n=400               4.272            0.01655X

#include <boost/lexical_cast.hpp>
#include <gutil/strings/substitute.h>

#include "exprs/in-predicate.h"

#include "runtime/decimal-value.h"
#include "runtime/string-value.h"
#include "udf/udf-test-harness.h"
#include "util/benchmark.h"
#include "util/cpu-info.h"

#include "common/names.h"

using namespace impala;
using namespace impala_udf;
using namespace strings;
using std::move;

namespace impala {

template<typename T> T MakeAnyVal(int v) {
  return T(v);
}

template<> StringVal MakeAnyVal(int v) {
  // Leak these strings so we don't have to worry about them going out of scope
  string* s = new string();
  *s = lexical_cast<string>(v);
  return StringVal(reinterpret_cast<uint8_t*>(const_cast<char*>(s->c_str())), s->size());
}

class InPredicateBenchmark {
 public:
  template<typename T, typename SetType>
  struct TestData {
    vector<T> anyvals;
    vector<AnyVal*> anyval_ptrs;
    InPredicate::SetLookupState<SetType> state;

    vector<T> search_vals;

    int total_found_set;
    int total_set;
    int total_found_iter;
    int total_iter;
  };

  template<typename T, typename SetType>
  static TestData<T, SetType> CreateTestData(int num_values,
      const FunctionContext::TypeDesc& type, int num_search_vals = 100) {
    srand(time(NULL));
    TestData<T, SetType> data;
    data.anyvals.resize(num_values);
    data.anyval_ptrs.resize(num_values);
    for (int i = 0; i < num_values; ++i) {
      data.anyvals[i] = MakeAnyVal<T>(rand());
      data.anyval_ptrs[i] = &data.anyvals[i];
    }

    for (int i = 0; i < num_search_vals; ++i) {
      data.search_vals.push_back(MakeAnyVal<T>(rand()));
    }

    FunctionContext* ctx = CreateContext(num_values, type);

    vector<AnyVal*> constant_args;
    constant_args.push_back(NULL);
    for (AnyVal* p : data.anyval_ptrs) constant_args.push_back(p);
    UdfTestHarness::SetConstantArgs(ctx, move(constant_args));

    InPredicate::SetLookupPrepare<T, SetType>(ctx, FunctionContext::FRAGMENT_LOCAL);
    data.state = *reinterpret_cast<InPredicate::SetLookupState<SetType>*>(
        ctx->GetFunctionState(FunctionContext::FRAGMENT_LOCAL));

    data.total_found_set = data.total_set = data.total_found_iter = data.total_iter = 0;
    return data;
  }

  template<typename T, typename SetType>
  static void TestSetLookup(int batch_size, void* d) {
    TestData<T, SetType>* data = reinterpret_cast<TestData<T, SetType>*>(d);
    for (int i = 0; i < batch_size; ++i) {
      for (const T& search_val: data->search_vals) {
        BooleanVal found = InPredicate::SetLookup(&data->state, search_val);
        if (found.val) ++data->total_found_set;
        ++data->total_set;
      }
    }
  }

  template<typename T, typename SetType>
  static void TestIterate(int batch_size, void* d) {
    TestData<T, SetType>* data = reinterpret_cast<TestData<T, SetType>*>(d);
    for (int i = 0; i < batch_size; ++i) {
      for (const T& search_val: data->search_vals) {
        BooleanVal found = InPredicate::Iterate(
            data->state.type, search_val, data->anyvals.size(), &data->anyvals[0]);
        if (found.val) ++data->total_found_iter;
        ++data->total_iter;
      }
    }
  }

  static void RunIntBenchmark(int n) {
    Benchmark suite(Substitute("int n=$0", n));
    FunctionContext::TypeDesc type;
    type.type = FunctionContext::TYPE_INT;
    TestData<IntVal, int32_t> data =
        InPredicateBenchmark::CreateTestData<IntVal, int32_t>(n, type);
    suite.AddBenchmark(Substitute("SetLookup n=$0", n),
                       InPredicateBenchmark::TestSetLookup<IntVal, int32_t>, &data);
    suite.AddBenchmark(Substitute("Iterate n=$0", n),
                       InPredicateBenchmark::TestIterate<IntVal, int32_t>, &data);
    cout << suite.Measure() << endl;
    // cout << "Found set: " << (double)data.total_found_set / data.total_set << endl;
    // cout << "Found iter: " << (double)data.total_found_iter / data.total_iter << endl;
  }

  static void RunStringBenchmark(int n) {
    Benchmark suite(Substitute("string n=$0", n));
    FunctionContext::TypeDesc type;
    type.type = FunctionContext::TYPE_STRING;
    TestData<StringVal, StringValue> data =
        InPredicateBenchmark::CreateTestData<StringVal, StringValue>(n, type);
    suite.AddBenchmark(Substitute("SetLookup n=$0", n),
                       InPredicateBenchmark::TestSetLookup<StringVal, StringValue>, &data);
    suite.AddBenchmark(Substitute("Iterate n=$0", n),
                       InPredicateBenchmark::TestIterate<StringVal, StringValue>, &data);
    cout << suite.Measure() << endl;
    // cout << "Found set: " << (double)data.total_found_set / data.total_set << endl;
    // cout << "Found iter: " << (double)data.total_found_iter / data.total_iter << endl;
  }

  static void RunDecimalBenchmark(int n) {
    Benchmark suite(Substitute("decimal(4,0) n=$0", n));
    FunctionContext::TypeDesc type;
    type.type = FunctionContext::TYPE_DECIMAL;
    type.precision = 4;
    type.scale = 0;
    TestData<DecimalVal, Decimal16Value> data =
        InPredicateBenchmark::CreateTestData<DecimalVal, Decimal16Value>(n, type);
    suite.AddBenchmark(Substitute("SetLookup n=$0", n),
                       InPredicateBenchmark::TestSetLookup<DecimalVal, Decimal16Value>, &data);
    suite.AddBenchmark(Substitute("Iterate n=$0", n),
                       InPredicateBenchmark::TestIterate<DecimalVal, Decimal16Value>, &data);
    cout << suite.Measure() << endl;
  }

 private:
  static FunctionContext* CreateContext(
      int num_args, const FunctionContext::TypeDesc& type) {
    // Types don't matter (but number of args do)
    FunctionContext::TypeDesc ret_type;
    vector<FunctionContext::TypeDesc> arg_types(num_args + 1, type);
    return UdfTestHarness::CreateTestContext(ret_type, arg_types);
  }

};

}

int main(int argc, char **argv) {
  CpuInfo::Init();
  cout << Benchmark::GetMachineInfo() << endl;

  for (int i = 1; i <= 10; ++i) InPredicateBenchmark::RunIntBenchmark(i);
  InPredicateBenchmark::RunIntBenchmark(400);

  cout << endl;

  for (int i = 1; i <= 10; ++i) InPredicateBenchmark::RunStringBenchmark(i);
  InPredicateBenchmark::RunStringBenchmark(400);

  for (int i = 1; i <= 4; ++i) InPredicateBenchmark::RunDecimalBenchmark(i);
  InPredicateBenchmark::RunDecimalBenchmark(400);

  return 0;
}
