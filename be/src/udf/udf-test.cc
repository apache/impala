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

#include <iostream>
#include <boost/date_time/posix_time/posix_time.hpp>

#include "common/logging.h"
#include "runtime/date-value.h"
#include "runtime/multi-precision.h"
#include "testutil/test-udfs.h"
#include "testutil/gtest-util.h"
#include "udf/udf-test-harness.h"

#include "common/names.h"

using boost::gregorian::date;
using boost::posix_time::nanoseconds;
using boost::posix_time::ptime;
using boost::posix_time::to_iso_extended_string;
using boost::posix_time::to_simple_string;
using namespace impala;
using namespace impala_udf;

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
  if (result.is_null) return StringVal::null();
  for (int i = 0; i < input.len; ++i) {
    result.ptr[i] = toupper(input.ptr[i]);
  }
  return result;
}

FloatVal Min3(FunctionContext* context, const FloatVal& f1,
    const FloatVal& f2, const FloatVal& f3) {
  bool is_null = true;
  float v = 0;
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
  uint8_t* buffer = context->Allocate(0);
  EXPECT_TRUE(buffer != NULL);
  buffer = context->Reallocate(buffer, 10);
  EXPECT_TRUE(buffer != NULL);
  memset(buffer, 0, 10);
  context->Free(buffer);
  return IntVal::null();
}

StringVal TimeToString(FunctionContext* context, const TimestampVal& time) {
  ptime t(*const_cast<date*>(reinterpret_cast<const date*>(&time.date)));
  // ptime t(*(date*)&time.date); is this conversion correct?
  t += nanoseconds(time.time_of_day);
  stringstream ss;
  ss << to_iso_extended_string(t.date()) << " " << to_simple_string(t.time_of_day());
  string s = ss.str();
  StringVal result(context, s.size());
  memcpy(result.ptr, s.data(), result.len);
  return result;
}

StringVal DateToString(FunctionContext* context, const DateVal& date_val) {
  DateValue date_value = DateValue::FromDateVal(date_val);
  const string s = date_value.ToString();
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
    context->SetFunctionState(scope, nullptr);
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

TEST(UdfTest, TestDateVal) {
  DateVal date_val1 = DateValue(2003, 3, 15).ToDateVal();
  EXPECT_FALSE(date_val1.is_null);
  EXPECT_TRUE((UdfTestHarness::ValidateUdf<StringVal, DateVal>(
    DateToString, date_val1, "2003-03-15")));

  // Test == and != operators
  DateVal date_val2 = DateValue(2003, 3, 15).ToDateVal();
  EXPECT_EQ(date_val1, date_val2);
  EXPECT_NE(date_val1, DateVal(date_val2.val + 1));

  // Test == and != operators with nulls
  DateVal null = DateVal::null();
  EXPECT_TRUE(null.is_null);
  EXPECT_NE(null, date_val1);

  date_val1.is_null = true;
  EXPECT_EQ(null, date_val1);
  EXPECT_NE(date_val1, date_val2);
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

  // TODO: replace these manual comparisons with a DecimalVal equality function
  // 1 != -1
  EXPECT_NE(d1.val16, d2.val16);
  EXPECT_NE(d3.val16, d4.val16);
  EXPECT_NE(d5.val16, d6.val16);

  // 1 == 1
  EXPECT_EQ(d1.val16, d3.val16);
  EXPECT_EQ(d1.val16, d5.val16);
  EXPECT_EQ(d3.val16, d5.val16);

  // -1 == -1
  EXPECT_EQ(d2.val16, d4.val16);
  EXPECT_EQ(d2.val16, d6.val16);
  EXPECT_EQ(d4.val16, d6.val16);

  // nulls
  EXPECT_EQ(null1.is_null, null2.is_null);
  EXPECT_NE(null1.is_null, d1.is_null);
}

TEST(UdfTest, TestFloatVal) {
  FloatVal f1(1.0);
  FloatVal f2(1.0);

  // 1.0 == 1.0
  EXPECT_EQ(f1, f2);

  // convert to nulls
  f1.is_null = true;
  f2.is_null = true;
  // nulls
  EXPECT_EQ(f1, f2);

  // change the value contained in one of the nulls
  f1.val = 0.0;
  // nulls
  EXPECT_EQ(f1, f2);

  // convert to non-nulls
  f1.is_null = false;
  f2.is_null = false;
  // 0.0 != 1.0
  EXPECT_NE(f1, f2);
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

// Creates and manages a FunctionContext for a dummy UDF with no arguments.
class FunctionCtxGuard {
 public:
  FunctionCtxGuard(): ctx_(UdfTestHarness::CreateTestContext(return_type, arg_types)) {}

  ~FunctionCtxGuard() { UdfTestHarness::CloseContext(ctx_.get()); }

  FunctionContext* getCtx() { return ctx_.get(); }
 private:
  static const FunctionContext::TypeDesc return_type;
  static const std::vector<FunctionContext::TypeDesc> arg_types;

  const std::unique_ptr<FunctionContext> ctx_;
};
const FunctionContext::TypeDesc FunctionCtxGuard::return_type {};
const std::vector<FunctionContext::TypeDesc> FunctionCtxGuard::arg_types;

bool StringValEqualsStr(const StringVal& string_val, const std::string& str) {
  return str.length() == string_val.len &&
      std::memcmp(str.c_str(), string_val.ptr, str.length()) == 0;
}

void CheckStringValCopyFromSucceeds(const std::string& str) {
  FunctionCtxGuard ctx_guard;

  const StringVal string_val = StringVal::CopyFrom(ctx_guard.getCtx(),
      reinterpret_cast<const uint8_t*>(str.c_str()), str.length());
  EXPECT_TRUE(StringValEqualsStr(string_val, str));
  EXPECT_FALSE(string_val.is_null);
  EXPECT_FALSE(ctx_guard.getCtx()->has_error());
}

void CheckStringValCopyFromFailsWithLength(uint64_t len) {
  FunctionCtxGuard ctx_guard;
  const StringVal string_val = StringVal::CopyFrom(ctx_guard.getCtx(), nullptr, len);
  EXPECT_TRUE(string_val.is_null);
  EXPECT_TRUE(ctx_guard.getCtx()->has_error());
}

TEST(UdfTest, TestStringValCopyFrom) {
  const std::string empty_string = "";
  const std::string small_string = "small";
  const std::string longer_string = "This is a somewhat longer string.";

  // Test that copying works correctly.
  CheckStringValCopyFromSucceeds(empty_string);
  CheckStringValCopyFromSucceeds(small_string);
  CheckStringValCopyFromSucceeds(longer_string);

  // Test that if a length bigger than StringVal::MAX_LENGTH is passed, the result is null
  // and an error.
  constexpr int len32 = StringVal::MAX_LENGTH + 1;
  CheckStringValCopyFromFailsWithLength(len32);

  // Test that if a length bigger than StringVal::MAX_LENGTH is passed as a size_t, but
  // when truncated to int it is no longer too big, the result is still null and an error.
  // Regression test for IMPALA-13150.
  constexpr uint64_t len64 = (1UL << 40) + 1;
  CheckStringValCopyFromFailsWithLength(len64);
}

IMPALA_TEST_MAIN();
