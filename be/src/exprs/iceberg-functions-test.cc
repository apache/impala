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

#include "exprs/iceberg-functions.h"
#include "thirdparty/murmurhash/MurmurHash3.h"

#include <limits>
#include <string>
#include <vector>

#include "runtime/date-value.h"
#include "runtime/exec-env.h"
#include "runtime/mem-pool.h"
#include "runtime/mem-tracker.h"
#include "runtime/runtime-state.h"
#include "runtime/timestamp-value.h"
#include "testutil/gtest-util.h"
#include "udf/udf-internal.h"

namespace impala {

// Create a FunctionContext for tests that doesn't allocate memory through a mem pool.
FunctionContext* CreateFunctionContext(
    MemPool* pool) {
  FunctionContext::TypeDesc return_type;
  return_type.type = FunctionContext::Type::TYPE_INT;
  FunctionContext::TypeDesc param_desc;
  param_desc.type = FunctionContext::Type::TYPE_INT;
  std::vector<FunctionContext::TypeDesc> arg_types(2, param_desc);
  FunctionContext* ctx = FunctionContextImpl::CreateContext(
      nullptr, pool, pool, return_type, arg_types, 0, true);
  EXPECT_TRUE(ctx != nullptr);
  return ctx;
}

// Create a FunctionContext where the used mem pool comes as a parameter. Can be used
// for tests that allocate memory through mem pool, e.g. truncate strings tests.
FunctionContext* CreateFunctionContext() {
  MemTracker m;
  MemPool pool(&m);
  return CreateFunctionContext(&pool);
}

// numeric_limits seems to have a bug when used with __int128_t as it returns zero
// for both the min and the max value. This function gives the min value for 128 byte
// int.
__int128_t GetMinForInt128() {
  __int128_t num = -1;
  for (int i = 0; i < 126; ++i) {
    num <<= 1;
    num -= 1;
  }
  return num;
}

// All the expected values in the tests below are reflecting what Iceberg would return
// for the given inputs. I used Iceberg's org.apache.iceberg.transforms.TestBucketing
// unit tests to check what is the expected result for a particular input.

class IcebergTruncatePartitionTransformTests {
public:
  static void TestIntegerNumbers();

  static void TestString();

  static void TestDecimal();
private:
  template<typename T>
  static void TestIntegerNumbersHelper();

  template<typename T, typename W>
  static void TestIncorrectWidthParameter(const T& input);
};

class IcebergBucketPartitionTransformTests {
public:
  static void TestIntegerNumbers();

  static void TestString();

  static void TestDecimal();

  static void TestDate();

  static void TestTimestamp();
private:
  template<typename T>
  static void TestIntegerNumbersHelper();

  template<typename T>
  static void TestIncorrectWidthParameter(const T& input);
};

void IcebergTruncatePartitionTransformTests::TestIntegerNumbers() {
  IcebergTruncatePartitionTransformTests::TestIntegerNumbersHelper<IntVal>();
  IcebergTruncatePartitionTransformTests::TestIntegerNumbersHelper<BigIntVal>();

  // Check that BigInt width is valid.
  int64_t max_value64 = std::numeric_limits<int64_t>::max();
  BigIntVal ret_val = IcebergFunctions::TruncatePartitionTransform(nullptr,
      BigIntVal(max_value64), BigIntVal(max_value64));
  EXPECT_EQ(max_value64, ret_val.val);
}

template<typename T>
void IcebergTruncatePartitionTransformTests::TestIntegerNumbersHelper() {
  // Test positive inputs.
  T ret_val = IcebergFunctions::TruncatePartitionTransform(
      nullptr, T(15), T(10));
  EXPECT_EQ(10, ret_val.val);

  ret_val = IcebergFunctions::TruncatePartitionTransform(
      nullptr, T(15), T(4));
  EXPECT_EQ(12, ret_val.val);

  // Test negative inputs.
  ret_val = IcebergFunctions::TruncatePartitionTransform(
      nullptr, T(-1), T(10));
  EXPECT_EQ(-10, ret_val.val);

  ret_val = IcebergFunctions::TruncatePartitionTransform(
      nullptr, T(-15), T(4));
  EXPECT_EQ(-16, ret_val.val);

  ret_val = IcebergFunctions::TruncatePartitionTransform(
      nullptr, T(-15), T(1));
  EXPECT_EQ(-15, ret_val.val);

  // Test NULL input.
  ret_val = IcebergFunctions::TruncatePartitionTransform(
      nullptr, T::null(), T(10));
  EXPECT_TRUE(ret_val.is_null);

  // Check for overflow when truncating from min value
  FunctionContext* ctx = CreateFunctionContext();
  typename T::underlying_type_t num_min =
      std::numeric_limits<typename T::underlying_type_t>::min();
  ret_val = IcebergFunctions::TruncatePartitionTransform(ctx, T(num_min), T(10000));
  EXPECT_TRUE(ret_val.is_null);
  EXPECT_EQ(strcmp("Truncate operation overflows for the given input.",
      ctx->error_msg()), 0);
  ctx->impl()->Close();

  TestIncorrectWidthParameter<T, T>(T(0));
}

void IcebergTruncatePartitionTransformTests::TestString() {
  MemTracker m;
  MemPool pool(&m);
  StringVal input("Some test input");
  FunctionContext* ctx = CreateFunctionContext(&pool);

  int width = 10;
  StringVal ret_val = IcebergFunctions::TruncatePartitionTransform(
      ctx, input, IntVal(width));
  EXPECT_EQ(ret_val.len, width);
  EXPECT_EQ(strncmp((char*)input.ptr, (char*)ret_val.ptr, width), 0);

  // Truncate width is longer than the input string.
  ret_val = IcebergFunctions::TruncatePartitionTransform(ctx, input, 100);
  EXPECT_EQ(ret_val.len, input.len);
  EXPECT_EQ(strncmp((char*)input.ptr, (char*)ret_val.ptr, input.len), 0);

  // Truncate width is the same as the the input string length.
  ret_val = IcebergFunctions::TruncatePartitionTransform(ctx, input, input.len);
  EXPECT_EQ(ret_val.len, input.len);
  EXPECT_EQ(strncmp((char*)input.ptr, (char*)ret_val.ptr, input.len), 0);

  // Test NULL input.
  ret_val = IcebergFunctions::TruncatePartitionTransform(
      nullptr, StringVal::null(), IntVal(10));
  EXPECT_TRUE(ret_val.is_null);

  // Test empty string input.
  ctx = CreateFunctionContext(&pool);
  ret_val = IcebergFunctions::TruncatePartitionTransform(ctx, "", 10);
  EXPECT_EQ(ret_val.len, 0);

  TestIncorrectWidthParameter<StringVal, IntVal>(StringVal("input"));

  pool.FreeAll();
}

void IcebergTruncatePartitionTransformTests::TestDecimal() {
  // Testing decimal in unit tests by invoking TruncatePartitionTransform seems
  // problematic as it queries ARG_TYPE_SIZE from FunctionContext. Apparently, it is not
  // properly set in unit tests. Use TruncateDecimal instead that gets the size as
  // parameter.
  DecimalVal ret_val = IcebergFunctions::TruncateDecimal(nullptr, 10050, 100, 4);
  EXPECT_EQ(10000, ret_val.val4);

  ret_val = IcebergFunctions::TruncateDecimal(nullptr, 100, 8, 4);
  EXPECT_EQ(96, ret_val.val4);

  ret_val = IcebergFunctions::TruncateDecimal(nullptr, 0, 10, 4);
  EXPECT_EQ(0, ret_val.val4);

  ret_val = IcebergFunctions::TruncateDecimal(nullptr, -1, 10, 4);
  EXPECT_EQ(-10, ret_val.val4);

  ret_val = IcebergFunctions::TruncateDecimal(nullptr, 12345, 1, 4);
  EXPECT_EQ(12345, ret_val.val4);

  // Test BigInt width with positive int32 Decimal inputs.
  ret_val = IcebergFunctions::TruncateDecimal(nullptr, 12345, 123456789012, 4);
  EXPECT_EQ(0, ret_val.val4);

  ret_val = IcebergFunctions::TruncateDecimal(nullptr, 12345, 100000000001, 4);
  EXPECT_EQ(0, ret_val.val4);

  // Test BigInt width with negative int32 Decimal inputs. In this case int32 is not big
  // enough to store the result and an overflow error is expected.
  FunctionContext* ctx = CreateFunctionContext();
  ret_val = IcebergFunctions::TruncateDecimal(ctx, -12345, 123456789012, 4);
  EXPECT_TRUE(ret_val.is_null);
  EXPECT_EQ(strcmp("Truncate operation overflows for the given input.",
      ctx->error_msg()), 0);
  ctx->impl()->Close();

  ctx = CreateFunctionContext();
  ret_val = IcebergFunctions::TruncateDecimal(ctx, -12345, 100000000001, 4);
  EXPECT_TRUE(ret_val.is_null);
  EXPECT_EQ(strcmp("Truncate operation overflows for the given input.",
      ctx->error_msg()), 0);
  ctx->impl()->Close();

  // Test the maximum limit for each representation size.
  // Note, decimal type represent int32 max in 8 bytes, int64 max in 16 bytes.
  int32_t int32_max_value = std::numeric_limits<int32_t>::max();
  ret_val = IcebergFunctions::TruncateDecimal(nullptr, int32_max_value, 50, 8);
  EXPECT_EQ(int32_max_value - (int32_max_value % 50), ret_val.val4);

  int64_t int64_max_value = std::numeric_limits<int64_t>::max();
  ret_val = IcebergFunctions::TruncateDecimal(nullptr, int64_max_value, 50, 16);
  EXPECT_EQ(int64_max_value - (int64_max_value % 50), ret_val.val8);

  __int128_t int128_max_value = std::numeric_limits<__int128_t>::max();
  ret_val = IcebergFunctions::TruncateDecimal(nullptr, int128_max_value, 50, 16);
  EXPECT_EQ(int128_max_value - (int128_max_value % 50), ret_val.val16);

  // Test the minimum limit for each representation size.
  // Note, decimal type represent int32 min in 8 bytes, int64 min in 16 bytes.
  int32_t int32_min_value = std::numeric_limits<int32_t>::min();
  int width = 50;
  ret_val = IcebergFunctions::TruncateDecimal(nullptr, int32_min_value, width, 8);
  int64_t expected64 = (int64_t)int32_min_value - (width + (int32_min_value % width));
  EXPECT_EQ(expected64, ret_val.val8);

  int64_t int64_min_value = std::numeric_limits<int64_t>::min();
  ret_val = IcebergFunctions::TruncateDecimal(nullptr, int64_min_value, width, 16);
  __int128_t expected128 =
      (__int128_t)int64_min_value - (width + (int64_min_value % width));
  EXPECT_EQ(expected128, ret_val.val16);

  // Truncation from int128 minimum value causes an overflow.
  ctx = CreateFunctionContext();
  ret_val = IcebergFunctions::TruncateDecimal(ctx, GetMinForInt128(), width, 16);
  EXPECT_TRUE(ret_val.is_null);
  EXPECT_EQ(strcmp("Truncate operation overflows for the given input.",
      ctx->error_msg()), 0);
  ctx->impl()->Close();

  // Test NULL input.
  ret_val = IcebergFunctions::TruncatePartitionTransform(
      nullptr, DecimalVal::null(), BigIntVal(10));
  EXPECT_TRUE(ret_val.is_null);

  TestIncorrectWidthParameter<DecimalVal, IntVal>(DecimalVal(0));
  TestIncorrectWidthParameter<DecimalVal, BigIntVal>(DecimalVal(0));
}

template<typename T, typename W>
void IcebergTruncatePartitionTransformTests::TestIncorrectWidthParameter(
    const T& input) {
  // Check for error when width is zero.
  FunctionContext* ctx = CreateFunctionContext();
  T ret_val = IcebergFunctions::TruncatePartitionTransform(ctx, input, W(0));
  EXPECT_TRUE(ret_val.is_null);
  EXPECT_EQ(strcmp("Width parameter should be greater than zero.", ctx->error_msg()), 0);
  ctx->impl()->Close();

  // Check for error when width is negative.
  ctx = CreateFunctionContext();
  ret_val = IcebergFunctions::TruncatePartitionTransform(ctx, input, W(-1));
  EXPECT_TRUE(ret_val.is_null);
  EXPECT_EQ(strcmp("Width parameter should be greater than zero.", ctx->error_msg()), 0);
  ctx->impl()->Close();

  // Check for error when width is null.
  ctx = CreateFunctionContext();
  ret_val = IcebergFunctions::TruncatePartitionTransform(ctx, input, W::null());
  EXPECT_TRUE(ret_val.is_null);
  EXPECT_EQ(strcmp("Width parameter should be greater than zero.", ctx->error_msg()), 0);
  ctx->impl()->Close();
}

void IcebergBucketPartitionTransformTests::TestIntegerNumbers() {
  TestIntegerNumbersHelper<IntVal>();
  TestIntegerNumbersHelper<BigIntVal>();

  // Check for max input value.
  IntVal ret_val = IcebergFunctions::BucketPartitionTransform(nullptr,
      IntVal(std::numeric_limits<int32_t>::max()), 100);
  EXPECT_EQ(6, ret_val.val);

  ret_val = IcebergFunctions::BucketPartitionTransform(nullptr,
      BigIntVal(std::numeric_limits<int64_t>::max()), 100);
  EXPECT_EQ(99, ret_val.val);

  // Check for min input value.
  ret_val = IcebergFunctions::BucketPartitionTransform(nullptr,
      IntVal(std::numeric_limits<int32_t>::min()), 1000);
  EXPECT_EQ(856, ret_val.val);

  ret_val = IcebergFunctions::BucketPartitionTransform(nullptr,
      BigIntVal(std::numeric_limits<int64_t>::min()), 1000);
  EXPECT_EQ(829, ret_val.val);
}

template<typename T>
void IcebergBucketPartitionTransformTests::TestIntegerNumbersHelper() {
  IntVal ret_val = IcebergFunctions::BucketPartitionTransform(nullptr, T(0), 1000000);
  EXPECT_EQ(671676, ret_val.val);

  ret_val = IcebergFunctions::BucketPartitionTransform(nullptr, T(0), 100);
  EXPECT_EQ(76, ret_val.val);

  ret_val = IcebergFunctions::BucketPartitionTransform(nullptr, T(12345), 50);
  EXPECT_EQ(41, ret_val.val);

  ret_val = IcebergFunctions::BucketPartitionTransform(nullptr, T(12345), 12345);
  EXPECT_EQ(12311, ret_val.val);

  ret_val = IcebergFunctions::BucketPartitionTransform(nullptr, T(-987654321), 14500);
  EXPECT_EQ(7489, ret_val.val);

  ret_val = IcebergFunctions::BucketPartitionTransform(nullptr, T(-155), 100);
  EXPECT_EQ(99, ret_val.val);

  // Check for null input.
  ret_val = IcebergFunctions::BucketPartitionTransform(nullptr, T::null(), 1);
  EXPECT_TRUE(ret_val.is_null);

  TestIncorrectWidthParameter<T>(T(0));
}

void IcebergBucketPartitionTransformTests::TestString() {
  IntVal ret_val = IcebergFunctions::BucketPartitionTransform(nullptr, "Input string",
      100);
  EXPECT_EQ(52, ret_val.val);

  ret_val = IcebergFunctions::BucketPartitionTransform(nullptr, "Input string",
      12345);
  EXPECT_EQ(5717, ret_val.val);

  ret_val = IcebergFunctions::BucketPartitionTransform(nullptr,
      "  ---====1234567890====---        ", 100);
  EXPECT_EQ(27, ret_val.val);

  ret_val = IcebergFunctions::BucketPartitionTransform(nullptr,
      "  ---====1234567890====---        ", 654321);
  EXPECT_EQ(641267, ret_val.val);

  // Check for empty string input.
  ret_val = IcebergFunctions::BucketPartitionTransform(nullptr, "", 10);
  EXPECT_EQ(0, ret_val.val);

  // Check for null input.
  ret_val = IcebergFunctions::BucketPartitionTransform(nullptr, StringVal::null(), 1);
  EXPECT_TRUE(ret_val.is_null);

  TestIncorrectWidthParameter<StringVal>(StringVal("input"));
}

void IcebergBucketPartitionTransformTests::TestDecimal() {
  // Testing decimal in unit tests by invoking BucketPartitionTransform seems
  // problematic as it queries ARG_TYPE_SIZE from FunctionContext. Apparently, it is not
  // properly set in unit tests. Use BucketDecimal instead that skips checking function
  // context for ARG_TYPE_SIZE.

  // Test int32 based decimal inputs.
  IntVal ret_val = IcebergFunctions::BucketDecimal(nullptr, 12345, 100, 4);
  EXPECT_EQ(28, ret_val.val);

  ret_val = IcebergFunctions::BucketDecimal(nullptr, 12345, 555, 4);
  EXPECT_EQ(393, ret_val.val);

  ret_val = IcebergFunctions::BucketDecimal(nullptr, 2047, 900, 4);
  EXPECT_EQ(439, ret_val.val);

  ret_val = IcebergFunctions::BucketDecimal(nullptr, 0, 10, 4);
  EXPECT_EQ(7, ret_val.val);

  ret_val = IcebergFunctions::BucketDecimal(nullptr, -987654321, 15000, 4);
  EXPECT_EQ(8732, ret_val.val);

  ret_val = IcebergFunctions::BucketDecimal(nullptr, -987654321, 2000, 4);
  EXPECT_EQ(1732, ret_val.val);

  // Test int64 based decimal inputs.
  ret_val = IcebergFunctions::BucketDecimal(nullptr, 12345678901L, 10000, 8);
  EXPECT_EQ(5191, ret_val.val);

  ret_val = IcebergFunctions::BucketDecimal(nullptr, 12345678901L, 150, 8);
  EXPECT_EQ(41, ret_val.val);

  ret_val = IcebergFunctions::BucketDecimal(nullptr, 330011994488229955L, 4800, 8);
  EXPECT_EQ(1114, ret_val.val);

  ret_val = IcebergFunctions::BucketDecimal(nullptr, 330011994488229955L, 73400, 8);
  EXPECT_EQ(12914, ret_val.val);

  ret_val = IcebergFunctions::BucketDecimal(nullptr, -360111674415228955L, 5000, 8);
  EXPECT_EQ(187, ret_val.val);

  ret_val = IcebergFunctions::BucketDecimal(nullptr, -360111674415228955L, 9800, 8);
  EXPECT_EQ(9187, ret_val.val);

  // Test int128 based decimal inputs.
  ret_val = IcebergFunctions::BucketDecimal(
      nullptr, __int128_t(9223372036854775807) + 1, 5550, 16);
  EXPECT_EQ(651, ret_val.val);

  ret_val = IcebergFunctions::BucketDecimal(
      nullptr, __int128_t(9223372036854775807) + 1, 18000, 16);
  EXPECT_EQ(12951, ret_val.val);

  ret_val = IcebergFunctions::BucketDecimal(
      nullptr, __int128_t(123321456654789987) * 1000, 18000, 16);
  EXPECT_EQ(15169, ret_val.val);

  ret_val = IcebergFunctions::BucketDecimal(
      nullptr, __int128_t(-123321456654789987) * 1000, 11000, 16);
  EXPECT_EQ(8353, ret_val.val);

  ret_val = IcebergFunctions::BucketDecimal(
      nullptr, __int128_t(-123321456654789987) * 1000, 7300, 16);
  EXPECT_EQ(853, ret_val.val);

  // Test NULL input.
  ret_val = IcebergFunctions::BucketPartitionTransform(
      nullptr, DecimalVal::null(), IntVal(10));
  EXPECT_TRUE(ret_val.is_null);

  TestIncorrectWidthParameter<DecimalVal>(DecimalVal(0));
  TestIncorrectWidthParameter<DecimalVal>(DecimalVal(0));
}

void IcebergBucketPartitionTransformTests::TestDate() {
  IntVal ret_val = IcebergFunctions::BucketPartitionTransform(nullptr,
      DateValue(2017, 11, 16).ToDateVal(), 5000);
  EXPECT_EQ(3226, ret_val.val);

  ret_val = IcebergFunctions::BucketPartitionTransform(nullptr,
      DateValue(2017, 11, 16).ToDateVal(), 150);
  EXPECT_EQ(76, ret_val.val);

  // Test for values pre 1970.
  ret_val = IcebergFunctions::BucketPartitionTransform(nullptr,
      DateValue(1901, 12, 5).ToDateVal(), 30);
  EXPECT_EQ(13, ret_val.val);

  ret_val = IcebergFunctions::BucketPartitionTransform(nullptr,
      DateValue(50, 3, 11).ToDateVal(), 1900);
  EXPECT_EQ(1539, ret_val.val);

  // Check for null input.
  ret_val = IcebergFunctions::BucketPartitionTransform(nullptr, DateVal::null(), 1);
  EXPECT_TRUE(ret_val.is_null);

  TestIncorrectWidthParameter<DateVal>(DateVal(0));
}

void IcebergBucketPartitionTransformTests::TestTimestamp() {
  TimestampVal tv;
  TimestampValue::ParseSimpleDateFormat("2017-11-16 22:31:08").ToTimestampVal(&tv);
  IntVal ret_val = IcebergFunctions::BucketPartitionTransform(nullptr, tv, 100);
  EXPECT_EQ(7, ret_val.val);

  TimestampValue::ParseSimpleDateFormat("2017-11-16 22:31:08").ToTimestampVal(&tv);
  ret_val = IcebergFunctions::BucketPartitionTransform(nullptr, tv, 1150);
  EXPECT_EQ(957, ret_val.val);

  // Check that zero fractional seconts doesn't change the output.
  TimestampValue::ParseSimpleDateFormat(
      "2017-11-16 22:31:08.000000").ToTimestampVal(&tv);
  ret_val = IcebergFunctions::BucketPartitionTransform(nullptr, tv, 100);
  EXPECT_EQ(7, ret_val.val);

  TimestampValue::ParseSimpleDateFormat(
      "2017-11-16 22:31:08.000000000").ToTimestampVal(&tv);
  ret_val = IcebergFunctions::BucketPartitionTransform(nullptr, tv, 100);
  EXPECT_EQ(7, ret_val.val);

  // Checks for non-zero fractional seconds.
  TimestampValue::ParseSimpleDateFormat("2010-10-09 12:39:28.123").ToTimestampVal(&tv);
  ret_val = IcebergFunctions::BucketPartitionTransform(nullptr, tv, 250);
  EXPECT_EQ(107, ret_val.val);

  TimestampValue::ParseSimpleDateFormat(
      "2010-10-09 12:39:28.123456").ToTimestampVal(&tv);
  ret_val = IcebergFunctions::BucketPartitionTransform(nullptr, tv, 250);
  EXPECT_EQ(115, ret_val.val);

  // Check that the 7th, 8th, 9th digit if the fractional second doesn't have effect on
  // the result. This is to follow Iceberg's behaviour.
  TimestampValue::ParseSimpleDateFormat(
      "2010-10-09 12:39:28.123456789").ToTimestampVal(&tv);
  ret_val = IcebergFunctions::BucketPartitionTransform(nullptr, tv, 250);
  EXPECT_EQ(115, ret_val.val);

  // Test for values pre 1970.
  TimestampValue::ParseSimpleDateFormat(
      "1905-01-02 12:20:25").ToTimestampVal(&tv);
  ret_val = IcebergFunctions::BucketPartitionTransform(nullptr, tv, 2000);
  EXPECT_EQ(809, ret_val.val);

  TimestampValue::ParseSimpleDateFormat(
      "1905-01-02 12:20:25.123456").ToTimestampVal(&tv);
  ret_val = IcebergFunctions::BucketPartitionTransform(nullptr, tv, 2000);
  EXPECT_EQ(932, ret_val.val);

  TimestampValue::ParseSimpleDateFormat(
      "1905-01-02 12:20:25.123456789").ToTimestampVal(&tv);
  ret_val = IcebergFunctions::BucketPartitionTransform(nullptr, tv, 2000);
  EXPECT_EQ(932, ret_val.val);

  TimestampValue::ParseSimpleDateFormat(
      "1905-01-02 12:20:25").ToTimestampVal(&tv);
  ret_val = IcebergFunctions::BucketPartitionTransform(nullptr, tv, 8000);
  EXPECT_EQ(2809, ret_val.val);

  // Check for null input.
  ret_val = IcebergFunctions::BucketPartitionTransform(nullptr, TimestampVal::null(), 1);
  EXPECT_TRUE(ret_val.is_null);

  TestIncorrectWidthParameter<TimestampVal>(TimestampVal(0));
}

template<typename T>
void IcebergBucketPartitionTransformTests::TestIncorrectWidthParameter(const T& input) {
  // Check for error when width is zero.
  FunctionContext* ctx = CreateFunctionContext();
  IntVal ret_val = IcebergFunctions::BucketPartitionTransform(ctx, input, 0);
  EXPECT_TRUE(ret_val.is_null);
  EXPECT_EQ(strcmp("Width parameter should be greater than zero.", ctx->error_msg()), 0);
  ctx->impl()->Close();

  // Check for error when width is negative.
  ctx = CreateFunctionContext();
  ret_val = IcebergFunctions::BucketPartitionTransform(ctx, input, -1);
  EXPECT_TRUE(ret_val.is_null);
  EXPECT_EQ(strcmp("Width parameter should be greater than zero.", ctx->error_msg()), 0);
  ctx->impl()->Close();

  // Check for error when width is null.
  ctx = CreateFunctionContext();
  ret_val = IcebergFunctions::BucketPartitionTransform(ctx, input, IntVal::null());
  EXPECT_TRUE(ret_val.is_null);
  EXPECT_EQ(strcmp("Width parameter should be greater than zero.", ctx->error_msg()), 0);
  ctx->impl()->Close();
}


TEST(TestIcebergFunctions, TruncateTransform) {
  IcebergTruncatePartitionTransformTests::TestIntegerNumbers();
  IcebergTruncatePartitionTransformTests::TestString();
  IcebergTruncatePartitionTransformTests::TestDecimal();
}

TEST(TestIcebergFunctions, BucketTransform) {
  IcebergBucketPartitionTransformTests::TestIntegerNumbers();
  IcebergBucketPartitionTransformTests::TestString();
  IcebergBucketPartitionTransformTests::TestDecimal();
  IcebergBucketPartitionTransformTests::TestDate();
  IcebergBucketPartitionTransformTests::TestTimestamp();
}

}
