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

#include <string>

#include <boost/math/constants/constants.hpp>
#include "runtime/multi-precision.h"
#include "testutil/gtest-util.h"

#include "common/names.h"

namespace mp = boost::multiprecision;
using std::max;
using std::min;
using std::numeric_limits;

namespace impala {

TEST(MultiPrecisionIntTest, Conversion) {
  int128_t x = 0;
  int256_t y = 0;
  EXPECT_TRUE(ConvertToInt256(x) == 0);

  x = -1;
  EXPECT_TRUE(ConvertToInt256(x) == -1);

  x = 1;
  EXPECT_TRUE(ConvertToInt256(x) == 1);

  x = numeric_limits<int32_t>::max();
  EXPECT_TRUE(ConvertToInt256(x) == numeric_limits<int32_t>::max());

  x = numeric_limits<int32_t>::min();
  EXPECT_TRUE(ConvertToInt256(x) == numeric_limits<int32_t>::min());

  x = numeric_limits<int64_t>::max();
  EXPECT_TRUE(ConvertToInt256(x) == numeric_limits<int64_t>::max());

  x = numeric_limits<int64_t>::min();
  EXPECT_TRUE(ConvertToInt256(x) == numeric_limits<int64_t>::min());

  x = numeric_limits<int64_t>::max();
  x *= 1000;
  y = numeric_limits<int64_t>::max();
  y *= 1000;
  EXPECT_TRUE(ConvertToInt256(x) == y);

  x = -numeric_limits<int64_t>::max();
  x *= 1000;
  y = -numeric_limits<int64_t>::max();
  y *= 1000;
  EXPECT_TRUE(ConvertToInt256(x) == y);

  // Note: numer_limits<> doesn't work for int128_t.
  static int128_t MAX_VALUE;
  memset(&MAX_VALUE, 255, sizeof(MAX_VALUE));
  uint8_t* buf = reinterpret_cast<uint8_t*>(&MAX_VALUE);
  buf[15] = 127;

  bool overflow = false;
  EXPECT_TRUE(ConvertToInt128(ConvertToInt256(x), MAX_VALUE, &overflow) == x);
  EXPECT_FALSE(overflow);
}

TEST(MultiPrecisionIntTest, HighLowBits) {
  // x = 0x0f0e0d0c0b0a09080706050403020100
  int128_t x = 0;
  for (int i = 0; i < sizeof(x); ++i) {
    *(reinterpret_cast<uint8_t*>(&x) + i) = i;
  }
  EXPECT_EQ(LowBits(x), 0x0706050403020100);
  EXPECT_EQ(HighBits(x), 0x0f0e0d0c0b0a0908);
}

// Simple example of adding and subtracting numbers that use more than
// 64 bits.
TEST(MultiPrecisionIntTest, Example) {
  int128_t v128 = 0;
  v128 += int128_t(numeric_limits<uint64_t>::max());
  v128 += int128_t(numeric_limits<uint64_t>::max());

  v128 -= int128_t(numeric_limits<uint64_t>::max());
  EXPECT_EQ(v128, numeric_limits<uint64_t>::max());

  v128 -= int128_t(numeric_limits<uint64_t>::max());
  EXPECT_EQ(v128, 0);
}

// Example taken from:
// http://www.boost.org/doc/libs/1_55_0/libs/multiprecision/doc/html/boost_multiprecision/tut/floats/fp_eg/aos.html

template<typename T> inline T area_of_a_circle(T r) {
   using boost::math::constants::pi;
   return pi<T>() * r * r;
}

TEST(MultiPrecisionFloatTest, Example) {
  const float r_f(float(123) / 100);
  const float a_f = area_of_a_circle(r_f);

  const double r_d(double(123) / 100);
  const double a_d = area_of_a_circle(r_d);

  const mp::cpp_dec_float_50 r_mp(mp::cpp_dec_float_50(123) / 100);
  const mp::cpp_dec_float_50 a_mp = area_of_a_circle(r_mp);

  stringstream ss;

  // Verify the results at different precisions.
  ss.str("");
  ss << setprecision(numeric_limits<float>::digits10)
     << a_f;
  EXPECT_EQ(ss.str(), "4.75292");

  ss.str("");
  ss << std::setprecision(std::numeric_limits<double>::digits10)
     << a_d;
  EXPECT_EQ(ss.str(), "4.752915525616");

  ss.str("");
  ss << std::setprecision(std::numeric_limits<mp::cpp_dec_float_50>::digits10)
     << a_mp;
  EXPECT_EQ(ss.str(), "4.7529155256159981904701331745635599135018975843146");
}

}

