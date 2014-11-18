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

DecimalVal Identity(FunctionContext* context, const DecimalVal& arg) { return arg; }

IntVal AllTypes(
    FunctionContext* context, const StringVal& string, const BooleanVal& boolean,
    const TinyIntVal& tiny_int, const SmallIntVal& small_int, const IntVal& int_val,
    const BigIntVal& big_int, const FloatVal& float_val, const DoubleVal& double_val,
    const DecimalVal& decimal) {
  int result = string.len + boolean.val + tiny_int.val + small_int.val + int_val.val
               + big_int.val + static_cast<int64_t>(float_val.val)
               + static_cast<int64_t>(double_val.val) + decimal.val4;
  return IntVal(result);
}

StringVal NoArgs(FunctionContext* context) {
  const char* result = "string";
  StringVal ret(context, strlen(result));
  // TODO: llvm 3.3 seems to have a bug if we use memcpy here making
  // the IR udf crash. This is fixed in 3.3.1. Fix this when we upgrade.
  //memcpy(ret.ptr, result, strlen(result));
  // IMPALA-775
  for (int i = 0; i < strlen(result); ++i) ret.ptr[i] = result[i];
  return ret;
}

BooleanVal VarAnd(FunctionContext* context, int n, const BooleanVal* args) {
  bool result = true;
  for (int i = 0; i < n; ++i) {
    if (args[i].is_null) return BooleanVal(false);
    result &= args[i].val;
  }
  return BooleanVal(result);
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

DoubleVal VarSum(FunctionContext* context, int n, const DoubleVal* args) {
  double result = 0;
  bool is_null = true;
  for (int i = 0; i < n; ++i) {
    if (args[i].is_null) continue;
    result += args[i].val;
    is_null = false;
  }
  if (is_null) return DoubleVal::null();
  return DoubleVal(result);
}

// TODO: have this return a StringVal (make sure not to use functions defined in other
// compilation units, or change how this is built).
IntVal VarSum(FunctionContext* context, int n, const StringVal* args) {
  int total_len = 0;
  for (int i = 0; i < n; ++i) {
    if (args[i].is_null) continue;
    total_len += args[i].len;
  }
  return IntVal(total_len);
}

// Decimal4Value... => Decimal8Value
DecimalVal VarSum(FunctionContext* context, int n, const DecimalVal* args) {
  int64_t result = 0;
  bool is_null = true;
  for (int i = 0; i < n; ++i) {
    const FunctionContext::TypeDesc* desc = context->GetArgType(i);
    if (desc->type != FunctionContext::TYPE_DECIMAL || desc->precision > 9) {
      context->SetError("VarSum() only accepts Decimal4Value (precison <= 9)");
      return DecimalVal::null();
    }
    if (args[i].is_null) continue;
    result += args[i].val4;
    is_null = false;
  }
  if (is_null) return DecimalVal::null();
  return DecimalVal(result);
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

BooleanVal TestError(FunctionContext* context) {
  context->SetError("test UDF error");
  context->SetError("this shouldn't show up");
  return BooleanVal(false);
}

BooleanVal TestWarnings(FunctionContext* context) {
  context->AddWarning("test UDF warning 1");
  context->AddWarning("test UDF warning 2");
  return BooleanVal(false);
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

BooleanVal ValidateArgType(FunctionContext* context, const StringVal& dummy) {
  if (context->GetArgType(0)->type != FunctionContext::TYPE_STRING) {
    return BooleanVal(false);
  }
  if (context->GetArgType(-1) != NULL) return BooleanVal(false);
  if (context->GetArgType(1) != NULL) return BooleanVal(false);
  return BooleanVal(true);
}

// Count UDF: counts the number of input rows per thread-local FunctionContext
void CountPrepare(FunctionContext* context, FunctionContext::FunctionStateScope scope) {
  if (scope == FunctionContext::THREAD_LOCAL) {
    uint64_t* state = reinterpret_cast<uint64_t*>(context->Allocate(sizeof(uint64_t)));
    *state = 0;
    context->SetFunctionState(scope, state);
  }
}

BigIntVal Count(FunctionContext* context) {
  uint64_t* state = reinterpret_cast<uint64_t*>(
      context->GetFunctionState(FunctionContext::THREAD_LOCAL));
  return BigIntVal(++(*state));
}

void CountClose(FunctionContext* context, FunctionContext::FunctionStateScope scope) {
  if (scope == FunctionContext::THREAD_LOCAL) {
    void* state = context->GetFunctionState(scope);
    context->Free(reinterpret_cast<uint8_t*>(state));
    context->SetFunctionState(scope, NULL);
  }
}

// ConstantArg UDF: returns the first argument if it's constant, otherwise returns NULL.
void ConstantArgPrepare(
    FunctionContext* context, FunctionContext::FunctionStateScope scope) {
  if (scope == FunctionContext::THREAD_LOCAL) {
    IntVal* state = reinterpret_cast<IntVal*>(context->Allocate(sizeof(IntVal)));
    if (context->IsArgConstant(0)) {
      *state = *reinterpret_cast<IntVal*>(context->GetConstantArg(0));
    } else {
      *state = IntVal::null();
    }
    context->SetFunctionState(scope, state);
  }
}

IntVal ConstantArg(FunctionContext* context, const IntVal& const_val) {
  IntVal* state = reinterpret_cast<IntVal*>(
      context->GetFunctionState(FunctionContext::THREAD_LOCAL));
  return *state;
}

void ConstantArgClose(
    FunctionContext* context, FunctionContext::FunctionStateScope scope) {
  if (scope == FunctionContext::THREAD_LOCAL) {
    void* state = context->GetFunctionState(scope);
    context->Free(reinterpret_cast<uint8_t*>(state));
    context->SetFunctionState(scope, NULL);
  }
}

// ValidateOpen UDF: returns true if the UDF was opened, false otherwise. Can also be
// used to validate close since it will leak if it's not closed.
void ValidateOpenPrepare(
    FunctionContext* context, FunctionContext::FunctionStateScope scope) {
  if (scope == FunctionContext::THREAD_LOCAL) {
    uint8_t* state = context->Allocate(100);
    context->SetFunctionState(scope, state);
  }
}

BooleanVal ValidateOpen(FunctionContext* context, const IntVal& dummy) {
  void* state = context->GetFunctionState(FunctionContext::THREAD_LOCAL);
  return BooleanVal(state != NULL);
}

void ValidateOpenClose(
    FunctionContext* context, FunctionContext::FunctionStateScope scope) {
  if (scope == FunctionContext::THREAD_LOCAL) {
    void* state = context->GetFunctionState(scope);
    context->Free(reinterpret_cast<uint8_t*>(state));
    context->SetFunctionState(scope, NULL);
  }
}

// MemTest UDF: "Allocates" the specified number of bytes per call.
void MemTestPrepare(FunctionContext* context, FunctionContext::FunctionStateScope scope) {
  if (scope == FunctionContext::THREAD_LOCAL) {
    int64_t* total =
        reinterpret_cast<int64_t*>(context->Allocate(sizeof(int64_t)));
    *total = 0;
    context->SetFunctionState(scope, total);
  }
}

BigIntVal MemTest(FunctionContext* context, const BigIntVal& bytes) {
  int64_t* total = reinterpret_cast<int64_t*>(
      context->GetFunctionState(FunctionContext::THREAD_LOCAL));
  context->TrackAllocation(bytes.val);
  *total += bytes.val;
  return bytes;
}

void MemTestClose(FunctionContext* context, FunctionContext::FunctionStateScope scope) {
  if (scope == FunctionContext::THREAD_LOCAL) {
    int64_t* total = reinterpret_cast<int64_t*>(
        context->GetFunctionState(FunctionContext::THREAD_LOCAL));
    context->Free(*total);
    context->Free(reinterpret_cast<uint8_t*>(total));
    context->SetFunctionState(scope, NULL);
  }
}

BigIntVal DoubleFreeTest(FunctionContext* context, BigIntVal bytes) {
  context->TrackAllocation(bytes.val);
  context->Free(bytes.val);
  context->Free(bytes.val);
  return bytes;
}

extern "C" BigIntVal UnmangledSymbol(FunctionContext* context) {
  return BigIntVal(5);
}
