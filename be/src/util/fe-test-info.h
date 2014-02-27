// Copyright 2012 Cloudera Inc.
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

#ifndef IMPALA_UTIL_FE_TEST_INFO_H
#define IMPALA_UTIL_FE_TEST_INFO_H

namespace impala {

// Provides global access to whether this binary is running as part of the FE tests
// (i.e., without a full BE).
class FeTestInfo {
 public:
  // Called in InitCommonRuntime().
  static void Init(bool is_fe_tests) { is_fe_tests_ = is_fe_tests; }
  static bool is_fe_tests() { return is_fe_tests_; }

 private:
  static bool is_fe_tests_;
};

}
#endif
