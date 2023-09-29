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

#ifndef IMPALA_UTIL_TEST_INFO_H
#define IMPALA_UTIL_TEST_INFO_H

namespace impala {

/// Provides global access to whether this binary is running as part of the tests
/// (i.e., without a full BE).
class TestInfo {
 public:
  enum Mode {
    NON_TEST, // Not a test, one of the main daemons
    BE_TEST,         // backend test
    BE_CLUSTER_TEST, // backend test that instantiates an Impala coordinator which joins
                     // an existing, running cluster
    FE_TEST,         // frontend test
  };

  /// Called in InitCommonRuntime().
  static void Init(Mode mode) { mode_ = mode; }

  static bool is_be_test() { return mode_ == BE_TEST; }
  static bool is_fe_test() { return mode_ == FE_TEST; }
  static bool is_be_cluster_test() { return mode_ == BE_CLUSTER_TEST; }
  static bool is_test() { return mode_ == BE_TEST || mode_ == FE_TEST ||
      mode_ == BE_CLUSTER_TEST; }

 private:
  static Mode mode_;
};

}
#endif
