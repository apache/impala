// Copyright 2015 Cloudera Inc.
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


// Note: The results do not include the pre-processing in the prepare function that is
// necessary for SetLookup but not Iterate. None of the values searched for are in the
// fabricated IN list (i.e. hit rate is 0).

// Machine Info: Intel(R) Core(TM) i7-2600 CPU @ 3.40GHz
// int n=1:              Function     Rate (iters/ms)          Comparison
// ----------------------------------------------------------------------
//                  SetLookup n=1                1021                  1X
//                    Iterate n=1                1133              1.111X

// int n=2:              Function     Rate (iters/ms)          Comparison
// ----------------------------------------------------------------------
//                  SetLookup n=2                1021                  1X
//                    Iterate n=2               949.1             0.9291X

// int n=3:              Function     Rate (iters/ms)          Comparison
// ----------------------------------------------------------------------
//                  SetLookup n=3               853.9                  1X
//                    Iterate n=3               808.2             0.9465X

// int n=4:              Function     Rate (iters/ms)          Comparison
// ----------------------------------------------------------------------
//                  SetLookup n=4               827.4                  1X
//                    Iterate n=4               715.1             0.8642X

// int n=5:              Function     Rate (iters/ms)          Comparison
// ----------------------------------------------------------------------
//                  SetLookup n=5                 813                  1X
//                    Iterate n=5               631.7              0.777X

// int n=6:              Function     Rate (iters/ms)          Comparison
// ----------------------------------------------------------------------
//                  SetLookup n=6               769.5                  1X
//                    Iterate n=6               566.3             0.7359X

// int n=7:              Function     Rate (iters/ms)          Comparison
// ----------------------------------------------------------------------
//                  SetLookup n=7               791.1                  1X
//                    Iterate n=7               517.3             0.6539X

// int n=8:              Function     Rate (iters/ms)          Comparison
// ----------------------------------------------------------------------
//                  SetLookup n=8               705.7                  1X
//                    Iterate n=8               476.3              0.675X

// int n=9:              Function     Rate (iters/ms)          Comparison
// ----------------------------------------------------------------------
//                  SetLookup n=9               759.4                  1X
//                    Iterate n=9               439.6             0.5789X

// int n=10:             Function     Rate (iters/ms)          Comparison
// ----------------------------------------------------------------------
//                 SetLookup n=10               729.5                  1X
//                   Iterate n=10               407.1             0.5581X

// int n=400:            Function     Rate (iters/ms)          Comparison
// ----------------------------------------------------------------------
//                SetLookup n=400               435.6                  1X
//                  Iterate n=400                13.9             0.0319X


// string n=1:           Function     Rate (iters/ms)          Comparison
// ----------------------------------------------------------------------
//                  SetLookup n=1               224.9                  1X
//                    Iterate n=1               533.5              2.373X

// string n=2:           Function     Rate (iters/ms)          Comparison
// ----------------------------------------------------------------------
//                  SetLookup n=2               143.4                  1X
//                    Iterate n=2               300.9              2.098X

// string n=3:           Function     Rate (iters/ms)          Comparison
// ----------------------------------------------------------------------
//                  SetLookup n=3               142.1                  1X
//                    Iterate n=3                 217              1.527X

// string n=4:           Function     Rate (iters/ms)          Comparison
// ----------------------------------------------------------------------
//                  SetLookup n=4               135.1                  1X
//                    Iterate n=4               182.5               1.35X

// string n=5:           Function     Rate (iters/ms)          Comparison
// ----------------------------------------------------------------------
//                  SetLookup n=5               110.8                  1X
//                    Iterate n=5               143.7              1.297X

// string n=6:           Function     Rate (iters/ms)          Comparison
// ----------------------------------------------------------------------
//                  SetLookup n=6               108.8                  1X
//                    Iterate n=6               130.5                1.2X

// string n=7:           Function     Rate (iters/ms)          Comparison
// ----------------------------------------------------------------------
//                  SetLookup n=7               101.2                  1X
//                    Iterate n=7               107.2              1.059X

// string n=8:           Function     Rate (iters/ms)          Comparison
// ----------------------------------------------------------------------
//                  SetLookup n=8               88.51                  1X
//                    Iterate n=8               99.28              1.122X

// string n=9:           Function     Rate (iters/ms)          Comparison
// ----------------------------------------------------------------------
//                  SetLookup n=9                 110                  1X
//                    Iterate n=9               86.68              0.788X

// string n=10:          Function     Rate (iters/ms)          Comparison
// ----------------------------------------------------------------------
//                 SetLookup n=10               98.66                  1X
//                   Iterate n=10               80.05             0.8114X

// string n=11:          Function     Rate (iters/ms)          Comparison
// ----------------------------------------------------------------------
//                 SetLookup n=11               93.99                  1X
//                   Iterate n=11               74.23             0.7898X

// string n=12:          Function     Rate (iters/ms)          Comparison
// ----------------------------------------------------------------------
//                 SetLookup n=12               89.73                  1X
//                   Iterate n=12               71.59             0.7978X

// string n=400:         Function     Rate (iters/ms)          Comparison
// ----------------------------------------------------------------------
//                SetLookup n=400                41.8                  1X
//                  Iterate n=400               1.578            0.03776X

#include <boost/lexical_cast.hpp>
#include <gutil/strings/substitute.h>

#include "exprs/in-predicate.h"

#include "udf/udf-test-harness.h"
#include "util/benchmark.h"
#include "util/cpu-info.h"

using namespace boost;
using namespace impala;
using namespace impala_udf;
using namespace std;
using namespace strings;

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
  static TestData<T, SetType> CreateTestData(int num_values, int num_search_vals = 100) {
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

    FunctionContext* ctx = CreateContext(num_values);

    vector<AnyVal*> constant_args;
    constant_args.push_back(NULL);
    BOOST_FOREACH(AnyVal* p, data.anyval_ptrs) constant_args.push_back(p);
    UdfTestHarness::SetConstantArgs(ctx, constant_args);

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
      BOOST_FOREACH(const T& search_val, data->search_vals) {
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
      BOOST_FOREACH(const T& search_val, data->search_vals) {
        BooleanVal found = InPredicate::Iterate(
            NULL, search_val, data->anyvals.size(), &data->anyvals[0]);
        if (found.val) ++data->total_found_iter;
        ++data->total_iter;
      }
    }
  }

  static void RunIntBenchmark(int n) {
    Benchmark suite(Substitute("int n=$0", n));
    TestData<IntVal, int32_t> data =
        InPredicateBenchmark::CreateTestData<IntVal, int32_t>(n);
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
    TestData<StringVal, StringValue> data =
        InPredicateBenchmark::CreateTestData<StringVal, StringValue>(n);
    suite.AddBenchmark(Substitute("SetLookup n=$0", n),
                       InPredicateBenchmark::TestSetLookup<StringVal, StringValue>, &data);
    suite.AddBenchmark(Substitute("Iterate n=$0", n),
                       InPredicateBenchmark::TestIterate<StringVal, StringValue>, &data);
    cout << suite.Measure() << endl;
    // cout << "Found set: " << (double)data.total_found_set / data.total_set << endl;
    // cout << "Found iter: " << (double)data.total_found_iter / data.total_iter << endl;
  }

 private:
  static FunctionContext* CreateContext(int num_args) {
    // Types don't matter (but number of args do)
    FunctionContext::TypeDesc ret_type;
    vector<FunctionContext::TypeDesc> arg_types;
    arg_types.resize(num_args + 1);
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

  for (int i = 1; i <= 12; ++i) InPredicateBenchmark::RunStringBenchmark(i);
  InPredicateBenchmark::RunStringBenchmark(400);

  return 0;
}
