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

#include <udf/udf-test-harness.h>
#include "udf-sample.h"

#include "common/names.h"

using namespace impala;
using namespace impala_udf;

int main(int argc, char** argv) {
  bool passed = true;
  // Using the test harness helpers, validate the UDF returns correct results.
  // These tests validate:
  //  AddUdf(1, 2) == 3
  //  AddUdf(null, 2) == null
  passed &= UdfTestHarness::ValidateUdf<IntVal, IntVal, IntVal>(
      AddUdf, IntVal(1), IntVal(2), IntVal(3));
  passed &= UdfTestHarness::ValidateUdf<IntVal, IntVal, IntVal>(
      AddUdf, IntVal::null(), IntVal(2), IntVal::null());

  cout << "Tests " << (passed ? "Passed." : "Failed.") << endl;
  return !passed;
}
