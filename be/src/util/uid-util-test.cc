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

#include <boost/uuid/uuid_generators.hpp>

#include "testutil/gtest-util.h"
#include "util/container-util.h"
#include "util/uid-util.h"

namespace impala {

TEST(UidUtil, FragmentInstanceId) {
  boost::uuids::random_generator uuid_generator;
  boost::uuids::uuid query_uuid = uuid_generator();
  TUniqueId query_id = UuidToQueryId(query_uuid);

  for (int i = 0; i < 100; ++i) {
    TUniqueId instance_id = CreateInstanceId(query_id, i);
    EXPECT_EQ(GetQueryId(instance_id), query_id);
    EXPECT_EQ(GetInstanceIdx(instance_id), i);
  }
}

TEST(UidUtil, UuidNotEmpty) {
  TUniqueId fixture = GenerateUUID();
  EXPECT_FALSE(UUIDEmpty(fixture));
}

TEST(UidUtil, UuidHalfEmptyHi) {
  TUniqueId fixture;
  fixture.hi = 0;
  fixture.lo = 1;
  EXPECT_FALSE(UUIDEmpty(fixture));
}

TEST(UidUtil, UuidHalfEmptyLo) {
  TUniqueId fixture;
  fixture.hi = 1;
  fixture.lo = 0;
  EXPECT_FALSE(UUIDEmpty(fixture));
}

TEST(UidUtil, UuidEmpty) {
  EXPECT_TRUE(UUIDEmpty(TUniqueId()));
}

}

