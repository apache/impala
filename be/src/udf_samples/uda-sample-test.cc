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

#include <udf/uda-test-harness.h>
#include "uda-sample.h"

#include "common/names.h"

using namespace impala;
using namespace impala_udf;

bool TestCount() {
  // Use the UDA test harness to validate the COUNT UDA.
  UdaTestHarness<BigIntVal, BigIntVal, IntVal> test(
      CountInit, CountUpdate, CountMerge, NULL, CountFinalize);

  // Run the UDA over 10000 non-null values
  vector<IntVal> no_nulls;
  no_nulls.resize(10000);
  if (!test.Execute(no_nulls, BigIntVal(no_nulls.size()))) {
    cerr << test.GetErrorMsg() << endl;
    return false;
  }

  // Run the UDA with some nulls
  vector<IntVal> some_nulls;
  some_nulls.resize(10000);
  int expected = some_nulls.size();
  for (int i = 0; i < some_nulls.size(); i += 100) {
    some_nulls[i] = IntVal::null();
    --expected;
  }
  if (!test.Execute(some_nulls, BigIntVal(expected))) {
    cerr << test.GetErrorMsg() << endl;
    return false;
  }

  return true;
}

bool TestAvg() {
  UdaTestHarness<DoubleVal, BufferVal, DoubleVal> test(
      AvgInit, AvgUpdate, AvgMerge, NULL, AvgFinalize);
  test.SetIntermediateSize(16);

  vector<DoubleVal> vals;
  vals.reserve(1001);
  for (int i = 0; i < 1001; ++i) {
    vals.push_back(DoubleVal(i));
  }
  if (!test.Execute<DoubleVal>(vals, DoubleVal(500))) {
    cerr << test.GetErrorMsg() << endl;
    return false;
  }
  return true;
}

bool TestStringConcat() {
  // Use the UDA test harness to validate the COUNT UDA.
  UdaTestHarness2<StringVal, StringVal, StringVal, StringVal> test(
      StringConcatInit, StringConcatUpdate, StringConcatMerge, NULL,
      StringConcatFinalize);

  vector<StringVal> values;
  values.push_back("Hello");
  values.push_back("World");

  vector<StringVal> separators;
  separators.reserve(values.size());
  for(int i = 0; i < values.size(); ++i) {
    separators.push_back(",");
  }
  if (!test.Execute(values, separators, StringVal("Hello,World"))) {
    cerr << test.GetErrorMsg() << endl;
    return false;
  }

  return true;
}

int main(int argc, char** argv) {
  bool passed = true;
  passed &= TestCount();
  passed &= TestAvg();
  passed &= TestStringConcat();
  cerr << (passed ? "Tests passed." : "Tests failed.") << endl;
  return 0;
}
