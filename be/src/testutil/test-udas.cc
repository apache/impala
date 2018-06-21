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

IMPALA_UDF_EXPORT
void TwoArgInit(FunctionContext*, IntVal*) {}
IMPALA_UDF_EXPORT
void TwoArgUpdate(FunctionContext*, const IntVal&, const StringVal&, IntVal*) {}
IMPALA_UDF_EXPORT
void TwoArgMerge(FunctionContext*, const IntVal&, IntVal*) {}

IMPALA_UDF_EXPORT
void VarArgInit(FunctionContext*, IntVal*) {}
IMPALA_UDF_EXPORT
void VarArgUpdate(FunctionContext*, const DoubleVal&, int, const StringVal*, IntVal*) {}
IMPALA_UDF_EXPORT
void VarArgMerge(FunctionContext*, const IntVal&, IntVal*) {}

// Defines Agg(<some args>) returns int
IMPALA_UDF_EXPORT
void AggUpdate(FunctionContext*, const IntVal&, IntVal*) {}
IMPALA_UDF_EXPORT
void AggUpdate(FunctionContext*, const IntVal&, const IntVal&, IntVal*) {}
// Update function intentionally not called *Update for FE testing.
IMPALA_UDF_EXPORT
void AggFn(FunctionContext*, const IntVal&, IntVal*) {}
IMPALA_UDF_EXPORT
void AggInit(FunctionContext*, IntVal*) {}
IMPALA_UDF_EXPORT
void AggMerge(FunctionContext*, const IntVal&, IntVal*) {}
IMPALA_UDF_EXPORT
IntVal AggSerialize(FunctionContext*, const IntVal& i) { return i; }
IMPALA_UDF_EXPORT
IntVal AggFinalize(FunctionContext*, const IntVal&i) { return i; }


// Defines Agg(<some args>) returns string intermediate string
IMPALA_UDF_EXPORT
void AggUpdate(FunctionContext*, const StringVal&, const DoubleVal&, StringVal*) {}
IMPALA_UDF_EXPORT
void Agg2Update(FunctionContext*, const StringVal&, const DoubleVal&, StringVal*) {}
IMPALA_UDF_EXPORT
void Agg(FunctionContext*, const StringVal&, const DoubleVal&, StringVal*) {}

IMPALA_UDF_EXPORT
void AggInit(FunctionContext*, StringVal*){}
IMPALA_UDF_EXPORT
void AggMerge(FunctionContext*, const StringVal&, StringVal*) {}
IMPALA_UDF_EXPORT
StringVal AggSerialize(FunctionContext*, const StringVal& v) { return v;}
IMPALA_UDF_EXPORT
StringVal AggFinalize(FunctionContext*, const StringVal& v) {
  return v;
}

// Defines AggIntermediate(int) returns BIGINT intermediate STRING
IMPALA_UDF_EXPORT
void AggIntermediate(FunctionContext* context, const IntVal&, StringVal*) {}
static void ValidateFunctionContext(const FunctionContext* context) {
  assert(context->GetNumArgs() == 1);
  assert(context->GetArgType(0)->type == FunctionContext::TYPE_INT);
  assert(context->GetIntermediateType().type == FunctionContext::TYPE_STRING);
  assert(context->GetReturnType().type == FunctionContext::TYPE_BIGINT);
}
IMPALA_UDF_EXPORT
void AggIntermediateUpdate(FunctionContext* context, const IntVal&, StringVal*) {
  ValidateFunctionContext(context);
}
IMPALA_UDF_EXPORT
void AggIntermediateInit(FunctionContext* context, StringVal*) {
  ValidateFunctionContext(context);
}
IMPALA_UDF_EXPORT
void AggIntermediateMerge(FunctionContext* context, const StringVal&, StringVal*) {
  ValidateFunctionContext(context);
}
IMPALA_UDF_EXPORT
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
IMPALA_UDF_EXPORT
void AggDecimalIntermediateUpdate(FunctionContext* context, const DecimalVal&,
    const IntVal&, DecimalVal*) {
  ValidateFunctionContext2(context);
}
IMPALA_UDF_EXPORT
void AggDecimalIntermediateInit(FunctionContext* context, DecimalVal*) {
  ValidateFunctionContext2(context);
}
IMPALA_UDF_EXPORT
void AggDecimalIntermediateMerge(FunctionContext* context, const DecimalVal&,
    DecimalVal*) {
  ValidateFunctionContext2(context);
}
IMPALA_UDF_EXPORT
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
IMPALA_UDF_EXPORT
void AggStringIntermediateUpdate(FunctionContext* context, const DecimalVal&,
    const BigIntVal&, const StringVal&, StringVal*) {
  ValidateFunctionContext3(context);
}
IMPALA_UDF_EXPORT
void AggStringIntermediateInit(FunctionContext* context, StringVal*) {
  ValidateFunctionContext3(context);
}
IMPALA_UDF_EXPORT
void AggStringIntermediateMerge(FunctionContext* context, const StringVal&, StringVal*) {
  ValidateFunctionContext3(context);
}
IMPALA_UDF_EXPORT
DecimalVal AggStringIntermediateFinalize(FunctionContext* context, const StringVal&) {
  ValidateFunctionContext3(context);
  return DecimalVal(100);
}

// Defines AggDateIntermediate(DATE, INT) returns DATE
// intermediate DATE
// Useful to test that type parameters are plumbed through.
static void ValidateFunctionContext4(const FunctionContext* context) {
  assert(context->GetNumArgs() == 2);
  assert(context->GetArgType(0)->type == FunctionContext::TYPE_DATE);
  assert(context->GetArgType(1)->type == FunctionContext::TYPE_INT);
  assert(context->GetIntermediateType().type == FunctionContext::TYPE_DATE);
  assert(context->GetReturnType().type == FunctionContext::TYPE_DATE);
}

IMPALA_UDF_EXPORT
void AggDateIntermediateUpdate(FunctionContext* context, const DateVal&,
    const IntVal&, DateVal*) {
  ValidateFunctionContext4(context);
}

IMPALA_UDF_EXPORT
void AggDateIntermediateInit(FunctionContext* context, DateVal*) {
  ValidateFunctionContext4(context);
}

IMPALA_UDF_EXPORT
void AggDateIntermediateMerge(FunctionContext* context, const DateVal&,
    DateVal*) {
  ValidateFunctionContext4(context);
}

IMPALA_UDF_EXPORT
DateVal AggDateIntermediateFinalize(FunctionContext* context, const DateVal&) {
  ValidateFunctionContext4(context);
  return DateVal::null();
}

// Defines MemTest(bigint) return bigint
// "Allocates" the specified number of bytes in the update function and frees them in the
// serialize function. Useful for testing mem limits.
IMPALA_UDF_EXPORT
void MemTestInit(FunctionContext*, BigIntVal* total) {
  *total = BigIntVal(0);
}

IMPALA_UDF_EXPORT
void MemTestUpdate(FunctionContext* context, const BigIntVal& bytes, BigIntVal* total) {
  if (bytes.is_null) return;
  context->TrackAllocation(bytes.val); // freed by serialize()
  total->val += bytes.val;
}

IMPALA_UDF_EXPORT
void MemTestMerge(FunctionContext* context, const BigIntVal& src, BigIntVal* dst) {
  if (src.is_null) return;
  context->TrackAllocation(src.val); // freed by finalize()
  if (dst->is_null) {
    *dst = src;
    return;
  }
  dst->val += src.val;
}

IMPALA_UDF_EXPORT
BigIntVal MemTestSerialize(FunctionContext* context, const BigIntVal& total) {
  if (total.is_null) return BigIntVal(0);
  context->Free(total.val);
  return total;
}

IMPALA_UDF_EXPORT
BigIntVal MemTestFinalize(FunctionContext* context, const BigIntVal& total) {
  if (total.is_null) return BigIntVal(0);
  context->Free(total.val);
  return total;
}

// Defines aggregate function for testing different intermediate/output types that
// computes the truncated bigint sum of many floats.
IMPALA_UDF_EXPORT
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

IMPALA_UDF_EXPORT
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

IMPALA_UDF_EXPORT
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

IMPALA_UDF_EXPORT
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

IMPALA_UDF_EXPORT
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
IMPALA_UDF_EXPORT
void ArgIsConstInit(FunctionContext* context, BooleanVal* is_const) {
  *is_const = BooleanVal(context->IsArgConstant(1));
}

IMPALA_UDF_EXPORT
void ArgIsConstUpdate(FunctionContext* context, const IntVal& val,
    const IntVal& const_arg, BooleanVal* is_const) {}

IMPALA_UDF_EXPORT
void ArgIsConstMerge(FunctionContext* context, const BooleanVal& src, BooleanVal* dst) {
  dst->val |= src.val;
}

// Defines aggregate function for testing NULL handling. Returns NULL if an even number
// of non-NULL inputs are consumed or 1 if an odd number of non-NULL inputs are consumed.
IMPALA_UDF_EXPORT
void ToggleNullInit(FunctionContext* context, IntVal* total) {
  *total = IntVal::null();
}

IMPALA_UDF_EXPORT
void ToggleNullUpdate(FunctionContext* context, const IntVal& val, IntVal* total) {
  if (total->is_null) {
    *total = IntVal(1);
  } else {
    *total = IntVal::null();
  }
}

IMPALA_UDF_EXPORT
void ToggleNullMerge(FunctionContext* context, const IntVal& src, IntVal* dst) {
  if (src.is_null != dst->is_null) {
    *dst = IntVal(1);
  } else {
    *dst = IntVal::null();
  }
}

// Defines aggregate function for testing input NULL handling. Returns the number of NULL
// input values.
IMPALA_UDF_EXPORT
void CountNullsInit(FunctionContext* context, BigIntVal* total) {
  *total = BigIntVal(0);
}

IMPALA_UDF_EXPORT
void CountNullsUpdate(FunctionContext* context, const BigIntVal& val, BigIntVal* total) {
  if (val.is_null) ++total->val;
}

IMPALA_UDF_EXPORT
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
IMPALA_UDF_EXPORT
void AggCharIntermediateInit(FunctionContext* context, StringVal* dst) {
  ValidateCharIntermediateFunctionContext(context);
  assert(dst->len == 10);
  memset(dst->ptr, 0, 10);
}
IMPALA_UDF_EXPORT
void AggCharIntermediateUpdate(
    FunctionContext* context, const IntVal& val, StringVal* dst) {
  ValidateCharIntermediateFunctionContext(context);
  assert(dst->len == 10);
  int* dst_val = reinterpret_cast<int*>(dst->ptr);
  if (!val.is_null) *dst_val += val.val;
}
IMPALA_UDF_EXPORT
void AggCharIntermediateMerge(FunctionContext* context, const StringVal& src, StringVal* dst) {
  ValidateCharIntermediateFunctionContext(context);
  int* dst_val = reinterpret_cast<int*>(dst->ptr);
  *dst_val += *reinterpret_cast<int*>(src.ptr);
}
IMPALA_UDF_EXPORT
StringVal AggCharIntermediateSerialize(FunctionContext* context, const StringVal& in) {
  ValidateCharIntermediateFunctionContext(context);
  return in;
}
IMPALA_UDF_EXPORT
IntVal AggCharIntermediateFinalize(FunctionContext* context, const StringVal& src) {
  ValidateCharIntermediateFunctionContext(context);
  return IntVal(*reinterpret_cast<int*>(src.ptr));
}

