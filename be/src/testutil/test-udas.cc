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

#include "testutil/test-udas.h"

#include <assert.h>

// Don't include Impala internal headers - real UDAs won't include them.
#include <udf/udf.h>

using namespace impala_udf;

void TwoArgInit(FunctionContext*, IntVal*) {}
void TwoArgUpdate(FunctionContext*, const IntVal&, const StringVal&, IntVal*) {}
void TwoArgMerge(FunctionContext*, const IntVal&, IntVal*) {}

void VarArgInit(FunctionContext*, IntVal*) {}
void VarArgUpdate(FunctionContext*, const DoubleVal&, int, const StringVal*, IntVal*) {}
void VarArgMerge(FunctionContext*, const IntVal&, IntVal*) {}

// Defines Agg(<some args>) returns int
void AggUpdate(FunctionContext*, const IntVal&, IntVal*) {}
void AggUpdate(FunctionContext*, const IntVal&, const IntVal&, IntVal*) {}
// Update function intentionally not called *Update for FE testing.
void AggFn(FunctionContext*, const IntVal&, IntVal*) {}
void AggInit(FunctionContext*, IntVal*) {}
void AggMerge(FunctionContext*, const IntVal&, IntVal*) {}
IntVal AggSerialize(FunctionContext*, const IntVal& i) { return i; }
IntVal AggFinalize(FunctionContext*, const IntVal&i) { return i; }


// Defines Agg(<some args>) returns string intermediate string
void AggUpdate(FunctionContext*, const StringVal&, const DoubleVal&, StringVal*) {}
void Agg2Update(FunctionContext*, const StringVal&, const DoubleVal&, StringVal*) {}
void Agg(FunctionContext*, const StringVal&, const DoubleVal&, StringVal*) {}

void AggInit(FunctionContext*, StringVal*){}
void AggMerge(FunctionContext*, const StringVal&, StringVal*) {}
StringVal AggSerialize(FunctionContext*, const StringVal& v) { return v;}
StringVal AggFinalize(FunctionContext*, const StringVal& v) {
  return v;
}

// Defines AggIntermediate(int) returns BIGINT intermediate STRING
void AggIntermediate(FunctionContext* context, const IntVal&, StringVal*) {}
static void ValidateFunctionContext(const FunctionContext* context) {
  assert(context->GetNumArgs() == 1);
  assert(context->GetArgType(0)->type == FunctionContext::TYPE_INT);
  assert(context->GetIntermediateType().type == FunctionContext::TYPE_STRING);
  assert(context->GetReturnType().type == FunctionContext::TYPE_BIGINT);
}
void AggIntermediateUpdate(FunctionContext* context, const IntVal&, StringVal*) {
  ValidateFunctionContext(context);
}
void AggIntermediateInit(FunctionContext* context, StringVal*) {
  ValidateFunctionContext(context);
}
void AggIntermediateMerge(FunctionContext* context, const StringVal&, StringVal*) {
  ValidateFunctionContext(context);
}
BigIntVal AggIntermediateFinalize(FunctionContext* context, const StringVal&) {
  ValidateFunctionContext(context);
  return BigIntVal::null();
}

// Defines AggDecimalIntermediate(DECIMAL(1,2), INT) returns DECIMAL(5,6)
// intermediate DECIMAL(3,4)
// Useful to test that type parameters are plumbed through.
static void ValidateFunctionContext2(const FunctionContext* context) {
  assert(context->GetNumArgs() == 2);
  assert(context->GetArgType(0)->type == FunctionContext::TYPE_DECIMAL);
  assert(context->GetArgType(0)->precision == 2);
  assert(context->GetArgType(0)->scale == 1);
  assert(context->GetArgType(1)->type == FunctionContext::TYPE_INT);
  assert(context->GetIntermediateType().type == FunctionContext::TYPE_DECIMAL);
  assert(context->GetIntermediateType().precision == 4);
  assert(context->GetIntermediateType().scale == 3);
  assert(context->GetReturnType().type == FunctionContext::TYPE_DECIMAL);
  assert(context->GetReturnType().precision == 6);
  assert(context->GetReturnType().scale == 5);
}
void AggDecimalIntermediateUpdate(FunctionContext* context, const DecimalVal&,
    const IntVal&, DecimalVal*) {
  ValidateFunctionContext2(context);
}
void AggDecimalIntermediateInit(FunctionContext* context, DecimalVal*) {
  ValidateFunctionContext2(context);
}
void AggDecimalIntermediateMerge(FunctionContext* context, const DecimalVal&,
    DecimalVal*) {
  ValidateFunctionContext2(context);
}
DecimalVal AggDecimalIntermediateFinalize(FunctionContext* context, const DecimalVal&) {
  ValidateFunctionContext2(context);
  return DecimalVal::null();
}

// Defines AggStringIntermediate(DECIMAL(20,10), BIGINT, STRING) returns DECIMAL(20,0)
// intermediate STRING.
// Useful to test decimal input types with string as intermediate types.
static void ValidateFunctionContext3(const FunctionContext* context) {
  assert(context->GetNumArgs() == 3);
  assert(context->GetArgType(0)->type == FunctionContext::TYPE_DECIMAL);
  assert(context->GetArgType(0)->precision == 20);
  assert(context->GetArgType(0)->scale == 10);
  assert(context->GetArgType(1)->type == FunctionContext::TYPE_BIGINT);
  assert(context->GetArgType(2)->type == FunctionContext::TYPE_STRING);
  assert(context->GetIntermediateType().type == FunctionContext::TYPE_STRING);
  assert(context->GetReturnType().type == FunctionContext::TYPE_DECIMAL);
  assert(context->GetReturnType().precision == 20);
  assert(context->GetReturnType().scale == 0);
}
void AggStringIntermediateUpdate(FunctionContext* context, const DecimalVal&,
    const BigIntVal&, const StringVal&, StringVal*) {
  ValidateFunctionContext3(context);
}
void AggStringIntermediateInit(FunctionContext* context, StringVal*) {
  ValidateFunctionContext3(context);
}
void AggStringIntermediateMerge(FunctionContext* context, const StringVal&, StringVal*) {
  ValidateFunctionContext3(context);
}
DecimalVal AggStringIntermediateFinalize(FunctionContext* context, const StringVal&) {
  ValidateFunctionContext3(context);
  return DecimalVal(100);
}

// Defines MemTest(bigint) return bigint
// "Allocates" the specified number of bytes in the update function and frees them in the
// serialize function. Useful for testing mem limits.
void MemTestInit(FunctionContext*, BigIntVal* total) {
  *total = BigIntVal(0);
}

void MemTestUpdate(FunctionContext* context, const BigIntVal& bytes, BigIntVal* total) {
  if (bytes.is_null) return;
  context->TrackAllocation(bytes.val); // freed by serialize()
  total->val += bytes.val;
}

void MemTestMerge(FunctionContext* context, const BigIntVal& src, BigIntVal* dst) {
  if (src.is_null) return;
  context->TrackAllocation(src.val); // freed by finalize()
  if (dst->is_null) {
    *dst = src;
    return;
  }
  dst->val += src.val;
}

BigIntVal MemTestSerialize(FunctionContext* context, const BigIntVal& total) {
  if (total.is_null) return BigIntVal(0);
  context->Free(total.val);
  return total;
}

BigIntVal MemTestFinalize(FunctionContext* context, const BigIntVal& total) {
  if (total.is_null) return BigIntVal(0);
  context->Free(total.val);
  return total;
}

// Defines aggregate function for testing different intermediate/output types that
// computes the truncated bigint sum of many floats.
void TruncSumInit(FunctionContext* context, DoubleVal* total) {
  // Arg types should be logical input types of UDA.
  assert(context->GetNumArgs() == 1);
  assert(context->GetArgType(0)->type == FunctionContext::TYPE_DOUBLE);
  assert(context->GetArgType(-1) == nullptr);
  assert(context->GetArgType(1) == nullptr);
  assert(context->GetIntermediateType().type == FunctionContext::TYPE_DOUBLE);
  assert(context->GetReturnType().type == FunctionContext::TYPE_BIGINT);
  *total = DoubleVal(0);
}

void TruncSumUpdate(FunctionContext* context, const DoubleVal& val, DoubleVal* total) {
  // Arg types should be logical input types of UDA.
  assert(context->GetNumArgs() == 1);
  assert(context->GetArgType(0)->type == FunctionContext::TYPE_DOUBLE);
  assert(context->GetArgType(-1) == nullptr);
  assert(context->GetArgType(1) == nullptr);
  assert(context->GetIntermediateType().type == FunctionContext::TYPE_DOUBLE);
  assert(context->GetReturnType().type == FunctionContext::TYPE_BIGINT);
  total->val += val.val;
}

void TruncSumMerge(FunctionContext* context, const DoubleVal& src, DoubleVal* dst) {
  // Arg types should be logical input types of UDA.
  assert(context->GetNumArgs() == 1);
  assert(context->GetArgType(0)->type == FunctionContext::TYPE_DOUBLE);
  assert(context->GetArgType(-1) == nullptr);
  assert(context->GetArgType(1) == nullptr);
  assert(context->GetIntermediateType().type == FunctionContext::TYPE_DOUBLE);
  assert(context->GetReturnType().type == FunctionContext::TYPE_BIGINT);
  dst->val += src.val;
}

const DoubleVal TruncSumSerialize(FunctionContext* context, const DoubleVal& total) {
  // Arg types should be logical input types of UDA.
  assert(context->GetNumArgs() == 1);
  assert(context->GetArgType(0)->type == FunctionContext::TYPE_DOUBLE);
  assert(context->GetArgType(-1) == nullptr);
  assert(context->GetArgType(1) == nullptr);
  assert(context->GetIntermediateType().type == FunctionContext::TYPE_DOUBLE);
  assert(context->GetReturnType().type == FunctionContext::TYPE_BIGINT);
  return total;
}

BigIntVal TruncSumFinalize(FunctionContext* context, const DoubleVal& total) {
  // Arg types should be logical input types of UDA.
  assert(context->GetNumArgs() == 1);
  assert(context->GetArgType(0)->type == FunctionContext::TYPE_DOUBLE);
  assert(context->GetArgType(-1) == nullptr);
  assert(context->GetArgType(1) == nullptr);
  assert(context->GetIntermediateType().type == FunctionContext::TYPE_DOUBLE);
  assert(context->GetReturnType().type == FunctionContext::TYPE_BIGINT);
  return BigIntVal(static_cast<int64_t>(total.val));
}

// Defines aggregate function for testing constant argument handling. The UDA returns
// true if its second argument is constant for all calls to Update().
void ArgIsConstInit(FunctionContext* context, BooleanVal* is_const) {
  *is_const = BooleanVal(context->IsArgConstant(1));
}

void ArgIsConstUpdate(FunctionContext* context, const IntVal& val,
    const IntVal& const_arg, BooleanVal* is_const) {}

void ArgIsConstMerge(FunctionContext* context, const BooleanVal& src, BooleanVal* dst) {
  dst->val |= src.val;
}

// Defines aggregate function for testing NULL handling. Returns NULL if an even number
// of non-NULL inputs are consumed or 1 if an odd number of non-NULL inputs are consumed.
void ToggleNullInit(FunctionContext* context, IntVal* total) {
  *total = IntVal::null();
}

void ToggleNullUpdate(FunctionContext* context, const IntVal& val, IntVal* total) {
  if (total->is_null) {
    *total = IntVal(1);
  } else {
    *total = IntVal::null();
  }
}

void ToggleNullMerge(FunctionContext* context, const IntVal& src, IntVal* dst) {
  if (src.is_null != dst->is_null) {
    *dst = IntVal(1);
  } else {
    *dst = IntVal::null();
  }
}

// Defines aggregate function for testing input NULL handling. Returns the number of NULL
// input values.
void CountNullsInit(FunctionContext* context, BigIntVal* total) {
  *total = BigIntVal(0);
}

void CountNullsUpdate(FunctionContext* context, const BigIntVal& val, BigIntVal* total) {
  if (val.is_null) ++total->val;
}

void CountNullsMerge(FunctionContext* context, const BigIntVal& src, BigIntVal* dst) {
  dst->val += src.val;
}

// Defines AggCharIntermediate(INT) returns INT with CHAR(10) intermediate.
// The function computes the sum of the input.
static void ValidateCharIntermediateFunctionContext(const FunctionContext* context) {
  assert(context->GetNumArgs() == 1);
  assert(context->GetArgType(0)->type == FunctionContext::TYPE_INT);
  assert(context->GetIntermediateType().type == FunctionContext::TYPE_FIXED_BUFFER);
  assert(context->GetIntermediateType().len == 10);
  assert(context->GetReturnType().type == FunctionContext::TYPE_INT);
}
void AggCharIntermediateInit(FunctionContext* context, StringVal* dst) {
  ValidateCharIntermediateFunctionContext(context);
  assert(dst->len == 10);
  memset(dst->ptr, 0, 10);
}
void AggCharIntermediateUpdate(
    FunctionContext* context, const IntVal& val, StringVal* dst) {
  ValidateCharIntermediateFunctionContext(context);
  assert(dst->len == 10);
  int* dst_val = reinterpret_cast<int*>(dst->ptr);
  if (!val.is_null) *dst_val += val.val;
}
void AggCharIntermediateMerge(FunctionContext* context, const StringVal& src, StringVal* dst) {
  ValidateCharIntermediateFunctionContext(context);
  int* dst_val = reinterpret_cast<int*>(dst->ptr);
  *dst_val += *reinterpret_cast<int*>(src.ptr);
}
StringVal AggCharIntermediateSerialize(FunctionContext* context, const StringVal& in) {
  ValidateCharIntermediateFunctionContext(context);
  return in;
}
IntVal AggCharIntermediateFinalize(FunctionContext* context, const StringVal& src) {
  ValidateCharIntermediateFunctionContext(context);
  return IntVal(*reinterpret_cast<int*>(src.ptr));
}

