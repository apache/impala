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


#include "service/fe-support.h"
#include "testutil/gtest-util.h"
#include "common/names.h"

// Main function for the unified backend tests. This incorporates multiple common
// initializations (e.g. setting a random seed) that are not necessary for every test,
// but allows unifying more tests into a single executable. The tests are incorporated
// by linking test libraries into this executable.
int main(int argc, char** argv) {
  // If running with --gtest_list_tests, avoid the Impala-specific initialization code.
  // This is particularly useful for the InitFeSupport() call, as that requires an
  // appropriate CLASSPATH set in the environment. It is useful to be able to list the
  // tests without that environment in place.
  // Note: This needs to happen before InitGoogleTest(), because that removes its flags.
  bool list_tests_only = false;
  for (int i = 0; i < argc; i++) {
    if (string(argv[i]) == "--gtest_list_tests") {
      list_tests_only = true;
      break;
    }
  }
  ::testing::InitGoogleTest(&argc, argv);
  if (!list_tests_only) {
    impala::InitCommonRuntime(argc, argv, true, impala::TestInfo::BE_TEST);
    impala::InitFeSupport();
    uint32_t seed = time(NULL);
    cout << "seed = " << seed << endl;
    srand(seed);
  }
  return RUN_ALL_TESTS();
}
