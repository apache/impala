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

#include <gtest/gtest.h>
#include <iostream>
#include <boost/date_time/posix_time/posix_time.hpp>

#include "common/logging.h"
#include "runtime/multi-precision.h"
#include "testutil/test-udfs.h"
#include "udf/udf-test-harness.h"

using namespace boost;
using namespace boost::posix_time;
using namespace boost::gregorian;
using namespace impala;
using namespace impala_udf;
using namespace std;

DoubleVal ZeroUdf(FunctionContext* context) {
  return DoubleVal(0);
}

StringVal LogUdf(FunctionContext* context, const StringVal& arg1) {
  cerr << (arg1.is_null ? "NULL" : string((char*)arg1.ptr, arg1.len)) << endl;
  return arg1;
}

StringVal UpperUdf(FunctionContext* context, const StringVal& input) {
  if (input.is_null) return StringVal::null();
  // Create a new StringVal object that's the same length as the input
  StringVal result = StringVal(context, input.len);
  for (int i = 0; i < input.len; ++i) {
    result.ptr[i] = toupper(input.ptr[i]);
  }
  return result;
}

FloatVal Min3(FunctionContext* context, const FloatVal& f1,
    const FloatVal& f2, const FloatVal& f3) {
  bool is_null = true;
  float v;
  if (!f1.is_null) {
    if (is_null) {
      v = f1.val;
      is_null = false;
    } else {
      v = std::min(v, f1.val);
    }
  }
  if (!f2.is_null) {
    if (is_null) {
      v = f2.val;
      is_null = false;
    } else {
      v = std::min(v, f2.val);
    }
  }
  if (!f3.is_null) {
    if (is_null) {
      v = f3.val;
      is_null = false;
    } else {
      v = std::min(v, f3.val);
    }
  }
  return is_null ? FloatVal::null() : FloatVal(v);
}

StringVal Concat(FunctionContext* context, int n, const StringVal* args) {
  int size = 0;
  bool all_null = true;
  for (int i = 0; i < n; ++i) {
    if (args[i].is_null) continue;
    size += args[i].len;
    all_null = false;
  }
  if (all_null) return StringVal::null();

  int offset = 0;
  StringVal result(context, size);
  for (int i = 0; i < n; ++i) {
    if (args[i].is_null) continue;
    memcpy(result.ptr + offset, args[i].ptr, args[i].len);
    offset += args[i].len;
  }
  return result;
}

IntVal NumVarArgs(FunctionContext*, const BigIntVal& dummy, int n, const IntVal* args) {
  return IntVal(n);
}

IntVal ValidateUdf(FunctionContext* context) {
  EXPECT_EQ(context->version(), FunctionContext::v1_3);
  EXPECT_FALSE(context->has_error()) << context->error_msg();
  EXPECT_TRUE(context->error_msg() == NULL);
  return IntVal::null();
}

IntVal ValidateFail(FunctionContext* context) {
  EXPECT_FALSE(context->has_error());
  EXPECT_TRUE(context->error_msg() == NULL);
  context->SetError("Fail");
  EXPECT_TRUE(context->has_error());
  EXPECT_TRUE(strcmp(context->error_msg(), "Fail") == 0);
  return IntVal::null();
}

IntVal ValidateMem(FunctionContext* context) {
  EXPECT_TRUE(context->Allocate(0) == NULL);
  uint8_t* buffer = context->Allocate(10);
  EXPECT_TRUE(buffer != NULL);
  memset(buffer, 0, 10);
  context->Free(buffer);
  return IntVal::null();
}

StringVal TimeToString(FunctionContext* context, const TimestampVal& time) {
  ptime t(*(date*)&time.date);
  t += nanoseconds(time.time_of_day);
  stringstream ss;
  ss << to_iso_extended_string(t.date()) << " " << to_simple_string(t.time_of_day());
  string s = ss.str();
  StringVal result(context, s.size());
  memcpy(result.ptr, s.data(), result.len);
  return result;
}

void ValidateSharedStatePrepare(
    FunctionContext* context, FunctionContext::FunctionStateScope scope) {
  if (scope == FunctionContext::THREAD_LOCAL) {
    // TODO: NYI
    // const FunctionContext::TypeDesc* arg_type = context->GetArgType(0);
    // ASSERT_TRUE(arg_type != NULL);
    // ASSERT_EQ(arg_type->type, FunctionContext::TYPE_SMALLINT);
    SmallIntVal* bytes = reinterpret_cast<SmallIntVal*>(context->GetConstantArg(0));
    ASSERT_TRUE(bytes != NULL);
    uint8_t* state = context->Allocate(bytes->val);
    context->SetFunctionState(scope, state);
  }
}

SmallIntVal ValidateSharedState(FunctionContext* context, SmallIntVal bytes) {
  void* state = context->GetFunctionState(FunctionContext::THREAD_LOCAL);
  EXPECT_TRUE(state != NULL);
  memset(state, 0, bytes.val);
  return SmallIntVal::null();
}

void ValidateSharedStateClose(
    FunctionContext* context, FunctionContext::FunctionStateScope scope) {
  if (scope == FunctionContext::THREAD_LOCAL) {
    void* state = context->GetFunctionState(scope);
    context->Free(reinterpret_cast<uint8_t*>(state));
    context->SetFunctionState(scope, NULL);
  }
}

TEST(UdfTest, TestFunctionContext) {
  EXPECT_TRUE(UdfTestHarness::ValidateUdf<IntVal>(ValidateUdf, IntVal::null()));
  EXPECT_FALSE(UdfTestHarness::ValidateUdf<IntVal>(ValidateFail, IntVal::null()));
  EXPECT_TRUE(UdfTestHarness::ValidateUdf<IntVal>(ValidateMem, IntVal::null()));

  scoped_ptr<SmallIntVal> arg(new SmallIntVal(100));
  vector<AnyVal*> constant_args;
  constant_args.push_back(arg.get());
  EXPECT_TRUE((UdfTestHarness::ValidateUdf<SmallIntVal, SmallIntVal>(
      ValidateSharedState, *arg, SmallIntVal::null(),
      ValidateSharedStatePrepare, ValidateSharedStateClose, constant_args)));
}

TEST(UdfTest, TestValidate) {
  EXPECT_TRUE(UdfTestHarness::ValidateUdf<DoubleVal>(ZeroUdf, DoubleVal(0)));
  EXPECT_FALSE(UdfTestHarness::ValidateUdf<DoubleVal>(ZeroUdf, DoubleVal(10)));

  EXPECT_TRUE((UdfTestHarness::ValidateUdf<StringVal, StringVal>(
      LogUdf, StringVal("abcd"), StringVal("abcd"))));

  EXPECT_TRUE((UdfTestHarness::ValidateUdf<StringVal, StringVal>(
      UpperUdf, StringVal("abcd"), StringVal("ABCD"))));

  EXPECT_TRUE((UdfTestHarness::ValidateUdf<FloatVal, FloatVal, FloatVal, FloatVal>(
      Min3, FloatVal(1), FloatVal(2), FloatVal(3), FloatVal(1))));
  EXPECT_TRUE((UdfTestHarness::ValidateUdf<FloatVal, FloatVal, FloatVal, FloatVal>(
      Min3, FloatVal(1), FloatVal::null(), FloatVal(3), FloatVal(1))));
  EXPECT_TRUE((UdfTestHarness::ValidateUdf<FloatVal, FloatVal, FloatVal, FloatVal>(
      Min3, FloatVal::null(), FloatVal::null(), FloatVal::null(), FloatVal::null())));
}

TEST(UdfTest, TestTimestampVal) {
  date d(2003, 3, 15);
  TimestampVal t1(*(int32_t*)&d);
  EXPECT_TRUE((UdfTestHarness::ValidateUdf<StringVal, TimestampVal>(
    TimeToString, t1, "2003-03-15 00:00:00")));

  TimestampVal t2(*(int32_t*)&d, 1000L * 1000L * 5000L);
  EXPECT_TRUE((UdfTestHarness::ValidateUdf<StringVal, TimestampVal>(
    TimeToString, t2, "2003-03-15 00:00:05")));
}

TEST(UdfTest, TestDecimalVal) {
  DecimalVal d1(static_cast<int32_t>(1));
  DecimalVal d2(static_cast<int32_t>(-1));
  DecimalVal d3(static_cast<int64_t>(1));
  DecimalVal d4(static_cast<int64_t>(-1));
  DecimalVal d5(static_cast<int128_t>(1));
  DecimalVal d6(static_cast<int128_t>(-1));
  DecimalVal null1 = DecimalVal::null();
  DecimalVal null2 = DecimalVal::null();

  // 1 != -1
  EXPECT_NE(d1, d2);
  EXPECT_NE(d3, d4);
  EXPECT_NE(d5, d6);

  // 1 == 1
  EXPECT_EQ(d1, d3);
  EXPECT_EQ(d1, d5);
  EXPECT_EQ(d3, d5);

  // -1 == -1
  EXPECT_EQ(d2, d4);
  EXPECT_EQ(d2, d6);
  EXPECT_EQ(d4, d6);

  // nulls
  EXPECT_EQ(null1, null2);
  EXPECT_NE(null1, d1);
}

TEST(UdfTest, TestVarArgs) {
  vector<StringVal> input;
  input.push_back(StringVal("Hello"));
  input.push_back(StringVal("World"));

  EXPECT_TRUE((UdfTestHarness::ValidateUdf<StringVal, StringVal>(
      Concat, input, StringVal("HelloWorld"))));

  input.push_back(StringVal("More"));
  EXPECT_TRUE((UdfTestHarness::ValidateUdf<StringVal, StringVal>(
      Concat, input, StringVal("HelloWorldMore"))));

  vector<IntVal> args;
  args.resize(10);
  EXPECT_TRUE((UdfTestHarness::ValidateUdf<IntVal, BigIntVal, IntVal>(
      NumVarArgs, BigIntVal(0), args, IntVal(args.size()))));
}

TEST(UdfTest, MemTest) {
  BigIntVal bytes_arg(1000);

  EXPECT_TRUE((UdfTestHarness::ValidateUdf<BigIntVal, BigIntVal>(
      ::MemTest, bytes_arg, bytes_arg, ::MemTestPrepare, ::MemTestClose)));

  EXPECT_FALSE((UdfTestHarness::ValidateUdf<BigIntVal, BigIntVal>(
      ::MemTest, bytes_arg, bytes_arg, ::MemTestPrepare, NULL)));

  EXPECT_FALSE((UdfTestHarness::ValidateUdf<BigIntVal, BigIntVal>(
      ::DoubleFreeTest, bytes_arg, bytes_arg)));

}

int main(int argc, char** argv) {
  impala::InitGoogleLoggingSafe(argv[0]);
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
