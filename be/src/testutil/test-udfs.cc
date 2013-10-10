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

#include <udf/udf.h>

using namespace impala_udf;

// These functions are intended to test the "glue" that runs UDFs. Thus, the UDFs
// themselves are kept very simple.

BooleanVal Identity(FunctionContext* context, const BooleanVal& arg) { return arg; }

TinyIntVal Identity(FunctionContext* context, const TinyIntVal& arg) { return arg; }

SmallIntVal Identity(FunctionContext* context, const SmallIntVal& arg) { return arg; }

IntVal Identity(FunctionContext* context, const IntVal& arg) { return arg; }

BigIntVal Identity(FunctionContext* context, const BigIntVal& arg) { return arg; }

FloatVal Identity(FunctionContext* context, const FloatVal& arg) { return arg; }

DoubleVal Identity(FunctionContext* context, const DoubleVal& arg) { return arg; }

StringVal Identity(FunctionContext* context, const StringVal& arg) { return arg; }

TimestampVal Identity(FunctionContext* context, const TimestampVal& arg) { return arg; }

IntVal AllTypes(
    FunctionContext* context, const StringVal& string, const BooleanVal& boolean,
    const TinyIntVal& tiny_int, const SmallIntVal& small_int, const IntVal& int_val,
    const BigIntVal& big_int, const FloatVal& float_val, const DoubleVal& double_val) {
  int result = string.len + boolean.val + tiny_int.val + small_int.val + int_val.val
               + big_int.val + static_cast<int64_t>(float_val.val)
               + static_cast<int64_t>(double_val.val);
  return IntVal(result);
}

StringVal NoArgs(FunctionContext* context) {
  return StringVal(reinterpret_cast<uint8_t*>(const_cast<char*>("string")), 6);
}

IntVal VarSum(FunctionContext* context, int n, const IntVal* args) {
  int result = 0;
  bool is_null = true;
  for (int i = 0; i < n; ++i) {
    if (args[i].is_null) continue;
    result += args[i].val;
    is_null = false;
  }
  if (is_null) return IntVal::null();
  return IntVal(result);
}

DoubleVal VarSumMultiply(FunctionContext* context,
    const DoubleVal& d, int n, const IntVal* args) {
  if (d.is_null) return DoubleVal::null();

  int result = 0;
  bool is_null = true;
  for (int i = 0; i < n; ++i) {
    if (args[i].is_null) continue;
    result += args[i].val;
    is_null = false;
  }
  if (is_null) return DoubleVal::null();
  return DoubleVal(result * d.val);
}

// Dummy functions to test ddl.
IntVal Fn(FunctionContext*) { return IntVal::null(); }
IntVal Fn(FunctionContext*, const IntVal&) { return IntVal::null(); }
IntVal Fn(FunctionContext*, const IntVal&, const StringVal&) { return IntVal::null(); }
IntVal Fn(FunctionContext*, const StringVal&, const IntVal&) { return IntVal::null(); }
IntVal Fn2(FunctionContext*, const IntVal&) { return IntVal::null(); }
IntVal Fn2(FunctionContext*, const IntVal&, const StringVal&) { return IntVal::null(); }

TimestampVal ConstantTimestamp(FunctionContext* context) {
  return TimestampVal(2456575, 1); // 2013-10-09 00:00:00.000000001
}
