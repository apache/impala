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

#include <gtest/gtest.h>
#include <string>
#include <limits>
#include <cmath>

#include "testutil/gtest-util.h"
#include "exprs/aggregate-functions.h"

#include "common/names.h"

using namespace impala;

extern string EncodeNdv(const string& ndv, bool* is_encoded);
extern string DecodeNdv(const string& ndv, bool is_encoded);

static const int HLL_LEN = pow(2, AggregateFunctions::HLL_PRECISION);

TEST(RleTest, TestEmptyRle) {
  string test(HLL_LEN, 0);

  bool is_encoded;
  const string& encoded = EncodeNdv(test, &is_encoded);
  ASSERT_EQ(8, encoded.size());
  ASSERT_TRUE(is_encoded);

  const string& decoded = DecodeNdv(encoded, is_encoded);
  ASSERT_EQ(HLL_LEN, decoded.size());
  ASSERT_EQ(test, decoded);
}

TEST(RleTest, TestNoEncode) {
  string test;
  for (int i = 0; i < HLL_LEN; ++i) {
    test += (i % 2 == 0) ? 'A' : 'B';
  }

  bool is_encoded;
  const string& encoded = EncodeNdv(test, &is_encoded);
  ASSERT_FALSE(is_encoded);
  ASSERT_EQ(encoded, test);

  ASSERT_EQ(DecodeNdv(encoded, is_encoded), test);
}

TEST(RleTest, TestEncode) {
  string test;
  for (int i = 0; i < HLL_LEN; ++i) {
    test += (i < 512) ? 'A' : 'B';
  }

  bool is_encoded;
  const string& encoded = EncodeNdv(test, &is_encoded);
  ASSERT_EQ(8, encoded.size());
  ASSERT_TRUE(is_encoded);
  ASSERT_EQ(DecodeNdv(encoded, is_encoded), test);
}


IMPALA_TEST_MAIN();
