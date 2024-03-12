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

#include "service/workload-management-worker.h"

#include <gtest/gtest.h>

#include "gen-cpp/SystemTables_types.h"
#include "workload_mgmt/workload-management.h"

using namespace impala;
using namespace impala::workloadmgmt;

TEST(WorkloadManagementWorkerTest, ParsersSize) {
  // Asserts FIELD_PARSERS includes one parser for each member of the Thrift
  // TQueryTableColumn enum.
  EXPECT_EQ(_TQueryTableColumn_VALUES_TO_NAMES.size(), FIELD_PARSERS.size());
  EXPECT_EQ(FIELD_DEFINITIONS.size(), FIELD_PARSERS.size());
}
