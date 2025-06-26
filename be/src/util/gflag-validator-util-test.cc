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

#include "gflag-validator-util.h"

#include <limits>

#include "testutil/gtest-util.h"

using namespace std;

//
// Tests for the ge_zero validator utility function.
//
template<typename T>
static void run_ge_zero_success_int() {
  EXPECT_TRUE(ge_zero<T>("flg", 0));
  EXPECT_TRUE(ge_zero<T>("flg", std::numeric_limits<T>::max()));
}

TEST(GFlagValidatorUtil, TestGeZeroSuccessInt32) {
  run_ge_zero_success_int<int32_t>();
}

TEST(GFlagValidatorUtil, TestGeZeroSuccessInt64) {
  run_ge_zero_success_int<int64_t>();
}

template<typename T>
static void run_ge_zero_fail_int() {
  EXPECT_FALSE(ge_zero<T>("flg", -1));
  EXPECT_FALSE(ge_zero<T>("flg", std::numeric_limits<T>::min()));
}
TEST(GFlagValidatorUtil, TestGeZeroFailInt32) {
  run_ge_zero_fail_int<int32_t>();
}

TEST(GFlagValidatorUtil, TestGeZeroFailInt64) {
  run_ge_zero_fail_int<int64_t>();
}

TEST(GFlagValidatorUtil, TestGeZeroSuccessDouble) {
  EXPECT_TRUE(ge_zero<double_t>("flg", 0.0));
  EXPECT_TRUE(ge_zero<double_t>("flg", std::numeric_limits<double_t>::max()));
}

TEST(GFlagValidatorUtil, TestGeZeroFailDouble) {
  EXPECT_FALSE(ge_zero<double_t>("flg", -0.000000001));
  EXPECT_FALSE(ge_zero<double_t>("flg", -0.999999999));
  EXPECT_FALSE(ge_zero<double_t>("flg", -1.0));
  EXPECT_FALSE(ge_zero<double_t>("flg", std::numeric_limits<double_t>::lowest()));
}

//
// Tests for the ge_one validator utility function.
//
template<typename T>
static void run_ge_one_success_int() {
  EXPECT_TRUE(ge_one<T>("flg", 1));
  EXPECT_TRUE(ge_one<T>("flg", std::numeric_limits<T>::max()));
}

TEST(GFlagValidatorUtil, TestGeOneSuccessInt32) {
  run_ge_one_success_int<int32_t>();
}

TEST(GFlagValidatorUtil, TestGeOneSuccessInt64) {
  run_ge_one_success_int<int64_t>();
}

template<typename T>
static void run_ge_one_fail_int() {
  EXPECT_FALSE(ge_one<T>("flg", 0));
  EXPECT_FALSE(ge_one<T>("flg", -1));
  EXPECT_FALSE(ge_one<T>("flg", std::numeric_limits<T>::min()));
}

TEST(GFlagValidatorUtil, TestGeOneFailInt32) {
  run_ge_one_fail_int<int32_t>();
}

TEST(GFlagValidatorUtil, TestGeOneFailInt64) {
  run_ge_one_fail_int<int64_t>();
}

TEST(GFlagValidatorUtil, TestGeOneSuccessDouble) {
  EXPECT_TRUE(ge_one<double_t>("flg", 1.0));
  EXPECT_TRUE(ge_one<double_t>("flg", std::numeric_limits<double_t>::max()));
}

TEST(GFlagValidatorUtil, TestGeOneFailDouble) {
  EXPECT_FALSE(ge_one<double_t>("flg", 0.0));
  EXPECT_FALSE(ge_one<double_t>("flg", -1.0));
  EXPECT_FALSE(ge_one<double_t>("flg", std::numeric_limits<double_t>::lowest()));
  EXPECT_FALSE(ge_one<double_t>("flg", 0.9999999999999999));
}

//
// Tests for the gt_zero validator utility function.
//
template<typename T>
static void run_gt_zero_success_int() {
  EXPECT_TRUE(gt_zero<T>("flg", 1));
  EXPECT_TRUE(gt_zero<T>("flg", std::numeric_limits<T>::max()));
}

TEST(GFlagValidatorUtil, TestGtZeroSuccessInt32) {
  run_gt_zero_success_int<int32_t>();
}

TEST(GFlagValidatorUtil, TestGtZeroSuccessInt64) {
  run_gt_zero_success_int<int64_t>();
}

template<typename T>
static void run_gt_zero_fail_int() {
  EXPECT_FALSE(gt_zero<T>("flg", 0));
  EXPECT_FALSE(gt_zero<T>("flg", -1));
  EXPECT_FALSE(gt_zero<T>("flg", std::numeric_limits<T>::min()));
}

TEST(GFlagValidatorUtil, TestGtZeroFailInt32) {
  run_gt_zero_fail_int<int32_t>();
}

TEST(GFlagValidatorUtil, TestGtZeroFailInt64) {
  run_gt_zero_fail_int<int64_t>();
}

TEST(GFlagValidatorUtil, TestGtZeroSuccessDouble) {
  EXPECT_TRUE(gt_zero<double_t>("flg", 1.0));
  EXPECT_TRUE(gt_zero<double_t>("flg", 0.00000001));
  EXPECT_TRUE(gt_zero<double_t>("flg", 0.9999999999999999));
  EXPECT_TRUE(gt_zero<double_t>("flg", std::numeric_limits<double_t>::max()));
}

TEST(GFlagValidatorUtil, TestGtZeroFailDouble) {
  EXPECT_FALSE(gt_zero<double_t>("flg", 0.0));
  EXPECT_FALSE(gt_zero<double_t>("flg", -1.0));
  EXPECT_FALSE(gt_zero<double_t>("flg", std::numeric_limits<double_t>::lowest()));
}
