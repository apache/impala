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

#include <udf/udf.h>
#include <cmath>

using namespace impala_udf;

#define NO_INLINE __attribute__((noinline))
#define WEAK_SYM  __attribute__((weak))

// These functions are intended to test the "glue" that runs UDFs. Thus, the UDFs
// themselves are kept very simple.

IMPALA_UDF_EXPORT
BooleanVal Identity(FunctionContext* context, const BooleanVal& arg) { return arg; }

IMPALA_UDF_EXPORT
TinyIntVal Identity(FunctionContext* context, const TinyIntVal& arg) { return arg; }

IMPALA_UDF_EXPORT
SmallIntVal Identity(FunctionContext* context, const SmallIntVal& arg) { return arg; }

IMPALA_UDF_EXPORT
IntVal Identity(FunctionContext* context, const IntVal& arg) { return arg; }

IMPALA_UDF_EXPORT
BigIntVal Identity(FunctionContext* context, const BigIntVal& arg) { return arg; }

IMPALA_UDF_EXPORT
FloatVal Identity(FunctionContext* context, const FloatVal& arg) { return arg; }

IMPALA_UDF_EXPORT
DoubleVal Identity(FunctionContext* context, const DoubleVal& arg) { return arg; }

IMPALA_UDF_EXPORT
StringVal Identity(FunctionContext* context, const StringVal& arg) { return arg; }

IMPALA_UDF_EXPORT
TimestampVal Identity(FunctionContext* context, const TimestampVal& arg) { return arg; }

IMPALA_UDF_EXPORT
DecimalVal Identity(FunctionContext* context, const DecimalVal& arg) { return arg; }

IMPALA_UDF_EXPORT
DateVal Identity(FunctionContext* context, const DateVal& arg) { return arg; }

IMPALA_UDF_EXPORT
IntVal AllTypes(
    FunctionContext* context, const StringVal& string, const BooleanVal& boolean,
    const TinyIntVal& tiny_int, const SmallIntVal& small_int, const IntVal& int_val,
    const BigIntVal& big_int, const FloatVal& float_val, const DoubleVal& double_val,
    const DecimalVal& decimal, const DateVal& date_val, const StringVal& binary_val) {
  int result = string.len + boolean.val + tiny_int.val + small_int.val + int_val.val
               + big_int.val + static_cast<int64_t>(float_val.val)
               + static_cast<int64_t>(double_val.val) + decimal.val4 + date_val.val
               + binary_val.len;
  return IntVal(result);
}

IMPALA_UDF_EXPORT
StringVal NoArgs(FunctionContext* context) {
  const char* result = "string";
  StringVal ret(context, strlen(result));
  memcpy(ret.ptr, result, strlen(result));
  return ret;
}

IMPALA_UDF_EXPORT
BooleanVal VarAnd(FunctionContext* context, int n, const BooleanVal* args) {
  bool result = true;
  for (int i = 0; i < n; ++i) {
    if (args[i].is_null) return BooleanVal(false);
    result &= args[i].val;
  }
  return BooleanVal(result);
}

IMPALA_UDF_EXPORT
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

IMPALA_UDF_EXPORT
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
IMPALA_UDF_EXPORT
IntVal VarSum(FunctionContext* context, int n, const StringVal* args) {
  int total_len = 0;
  for (int i = 0; i < n; ++i) {
    if (args[i].is_null) continue;
    total_len += args[i].len;
  }
  return IntVal(total_len);
}

// Decimal4Value... => Decimal8Value
IMPALA_UDF_EXPORT
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

IMPALA_UDF_EXPORT
DoubleVal NO_INLINE VarSumMultiply(FunctionContext* context,
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

// Call the non-inlined function in the same module to make sure linking works correctly.
IMPALA_UDF_EXPORT
DoubleVal VarSumMultiply2(FunctionContext* context,
    const DoubleVal& d, int n, const IntVal* args) {
  return VarSumMultiply(context, d, n, args);
}

// Call a function defined in Impalad proper to make sure linking works correctly.
extern "C" StringVal
    _ZN6impala15StringFunctions5LowerEPN10impala_udf15FunctionContextERKNS1_9StringValE(
        FunctionContext* context, const StringVal& str);

IMPALA_UDF_EXPORT
StringVal ToLower(FunctionContext* context, const StringVal& str) {
  // StringVal::null() doesn't inline its callee when compiled without optimization.
  // Useful for testing cases such as IMPALA-4595.
  if (str.is_null) return StringVal::null();
  return
      _ZN6impala15StringFunctions5LowerEPN10impala_udf15FunctionContextERKNS1_9StringValE(
          context, str);
}

// Call a function defined in Impalad proper to make sure linking works correctly.
extern "C" StringVal
    _ZN6impala15StringFunctions5UpperEPN10impala_udf15FunctionContextERKNS1_9StringValE(
        FunctionContext* context, const StringVal& str);

typedef StringVal (*ToUpperFn)(FunctionContext* context, const StringVal& str);

IMPALA_UDF_EXPORT
StringVal ToUpperWork(FunctionContext* context, const StringVal& str, ToUpperFn fn) {
  return fn(context, str);
}

IMPALA_UDF_EXPORT
StringVal ToUpper(FunctionContext* context, const StringVal& str) {
  // StringVal::null() doesn't inline its callee when compiled without optimization.
  // Useful for testing cases such as IMPALA-4595.
  if (str.is_null) return StringVal::null();
  // Test for IMPALA-4705: pass a function as argument and make sure it's materialized.
  return ToUpperWork(context, str,
      _ZN6impala15StringFunctions5UpperEPN10impala_udf15FunctionContextERKNS1_9StringValE);
}

typedef DoubleVal (*TestFn)(const DoubleVal& base, const DoubleVal& exp);

// This function is dropped upon linking when tested as IR UDF as it has internal linkage
// and its only caller Pow() will be overriden upon linking.
static DoubleVal NO_INLINE PrivateFn1(const DoubleVal& base, const DoubleVal& exp) {
#ifdef IR_COMPILE
  return DoubleVal::null();
#else
  return DoubleVal(std::pow(base.val, exp.val));
#endif
}

// This function is referenced in global variable 'global_array_2' even though it
// has no caller. This is to exercise IMPALA-4595 which verifies that this function
// still exists after linking.
static DoubleVal PrivateFn2(const DoubleVal& base, const DoubleVal& exp) {
  return DoubleVal(base.val + exp.val);
}

// This is a constant array with internal linkage type. Its only reference is from Pow()
// which will be overridden during linking. This array will essentially not be in the
// module after linking. Used to exercise IMPALA-4595 when testing IR UDF.
static volatile const TestFn global_array[1] = {PrivateFn1};

volatile const TestFn global_array_2[1] = {PrivateFn2};

namespace impala {
  class MathFunctions {
    static DoubleVal Pow(FunctionContext* ctx, const DoubleVal& base,
        const DoubleVal& exp);
  };
}

// This function has the same signature as a built-in function (pow()) in Impalad.
// It has a weak linkage type so it can be overridden at linking when tested as IR UDF.
IMPALA_UDF_EXPORT
DoubleVal WEAK_SYM impala::MathFunctions::Pow(FunctionContext* context,
    const DoubleVal& base, const DoubleVal& exp) {
  // Just references 'global_array' to stop the compiler from complaining.
  // This function will be overridden after linking so 'global_array' is dead
  // when tested as an IR UDF.
  if (base.is_null || exp.is_null || global_array[0] == NULL) return DoubleVal::null();
  return PrivateFn1(base, exp);
}

IMPALA_UDF_EXPORT
BooleanVal TestError(FunctionContext* context) {
  context->SetError("test UDF error");
  context->SetError("this shouldn't show up");
  return BooleanVal(false);
}

IMPALA_UDF_EXPORT
BooleanVal TestWarnings(FunctionContext* context) {
  context->AddWarning("test UDF warning 1");
  context->AddWarning("test UDF warning 2");
  return BooleanVal(false);
}

// Dummy functions to test ddl.
IMPALA_UDF_EXPORT
IntVal Fn(FunctionContext*) { return IntVal::null(); }
IMPALA_UDF_EXPORT
IntVal Fn(FunctionContext*, const IntVal&) { return IntVal::null(); }
IMPALA_UDF_EXPORT
IntVal Fn(FunctionContext*, const IntVal&, const StringVal&) { return IntVal::null(); }
IMPALA_UDF_EXPORT
IntVal Fn(FunctionContext*, const StringVal&, const IntVal&) { return IntVal::null(); }
IMPALA_UDF_EXPORT
IntVal Fn2(FunctionContext*, const IntVal&) { return IntVal::null(); }
IMPALA_UDF_EXPORT
IntVal Fn2(FunctionContext*, const IntVal&, const StringVal&) { return IntVal::null(); }

IMPALA_UDF_EXPORT
TimestampVal ConstantTimestamp(FunctionContext* context) {
  return TimestampVal(2456575, 1); // 2013-10-09 00:00:00.000000001
}

IMPALA_UDF_EXPORT
DateVal ConstantDate(FunctionContext* context) {
  return DateVal(15987); // 2013-10-09
}

IMPALA_UDF_EXPORT
BooleanVal ValidateArgType(FunctionContext* context, const StringVal& dummy) {
  if (context->GetArgType(0)->type != FunctionContext::TYPE_STRING) {
    return BooleanVal(false);
  }
  if (context->GetArgType(-1) != NULL) return BooleanVal(false);
  if (context->GetArgType(1) != NULL) return BooleanVal(false);
  return BooleanVal(true);
}

// Count UDF: counts the number of input rows per thread-local FunctionContext
IMPALA_UDF_EXPORT
void CountPrepare(FunctionContext* context, FunctionContext::FunctionStateScope scope) {
  if (scope == FunctionContext::THREAD_LOCAL) {
    uint64_t* state = reinterpret_cast<uint64_t*>(context->Allocate(sizeof(uint64_t)));
    *state = 0;
    context->SetFunctionState(scope, state);
  }
}

IMPALA_UDF_EXPORT
BigIntVal Count(FunctionContext* context) {
  uint64_t* state = reinterpret_cast<uint64_t*>(
      context->GetFunctionState(FunctionContext::THREAD_LOCAL));
  return BigIntVal(++(*state));
}

IMPALA_UDF_EXPORT
void CountClose(FunctionContext* context, FunctionContext::FunctionStateScope scope) {
  if (scope == FunctionContext::THREAD_LOCAL) {
    void* state = context->GetFunctionState(scope);
    context->Free(reinterpret_cast<uint8_t*>(state));
    context->SetFunctionState(scope, nullptr);
  }
}

// ConstantArg UDF: returns the first argument if it's constant, otherwise returns NULL.
IMPALA_UDF_EXPORT
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

IMPALA_UDF_EXPORT
IntVal ConstantArg(FunctionContext* context, const IntVal& const_val) {
  IntVal* state = reinterpret_cast<IntVal*>(
      context->GetFunctionState(FunctionContext::THREAD_LOCAL));
  return *state;
}

IMPALA_UDF_EXPORT
void ConstantArgClose(
    FunctionContext* context, FunctionContext::FunctionStateScope scope) {
  if (scope == FunctionContext::THREAD_LOCAL) {
    void* state = context->GetFunctionState(scope);
    context->Free(reinterpret_cast<uint8_t*>(state));
    context->SetFunctionState(scope, nullptr);
  }
}

// ValidateOpen UDF: returns true if the UDF was opened, false otherwise. Can also be
// used to validate close since it will leak if it's not closed.
IMPALA_UDF_EXPORT
void ValidateOpenPrepare(
    FunctionContext* context, FunctionContext::FunctionStateScope scope) {
  if (scope == FunctionContext::THREAD_LOCAL) {
    uint8_t* state = context->Allocate(100);
    context->SetFunctionState(scope, state);
  }
}

IMPALA_UDF_EXPORT
BooleanVal ValidateOpen(FunctionContext* context, const IntVal& dummy) {
  void* state = context->GetFunctionState(FunctionContext::THREAD_LOCAL);
  return BooleanVal(state != NULL);
}

IMPALA_UDF_EXPORT
void ValidateOpenClose(
    FunctionContext* context, FunctionContext::FunctionStateScope scope) {
  if (scope == FunctionContext::THREAD_LOCAL) {
    void* state = context->GetFunctionState(scope);
    context->Free(reinterpret_cast<uint8_t*>(state));
    context->SetFunctionState(scope, nullptr);
  }
}

// This prepare function always fails to make sure clean up is done afterwards.
IMPALA_UDF_EXPORT
void BadExprPrepare(FunctionContext* context, FunctionContext::FunctionStateScope scope) {
  if (scope == FunctionContext::FRAGMENT_LOCAL) {
    int32_t* state = reinterpret_cast<int32_t*>(context->Allocate(sizeof(int32_t)));
    *state = 0xf001cafe;
    context->SetFunctionState(scope, state);
  }
  context->SetError("BadExpr prepare error");
}

// This prepare function always fails for cloned evaluators to exercise IMPALA-6184.
// It does so by detecting whether the caller is a cloned evaluator and inserts an error
// in FunctionContext if that's the case.
IMPALA_UDF_EXPORT
void BadExpr2Prepare(FunctionContext* context,
    FunctionContext::FunctionStateScope scope) {
  if (scope == FunctionContext::FRAGMENT_LOCAL) {
    int32_t* state = reinterpret_cast<int32_t*>(context->Allocate(sizeof(int32_t)));
    *state = 0xf001cafe;
    context->SetFunctionState(scope, state);
    // Set the thread local state too to differentiate from cloned evaluators.
    context->SetFunctionState(FunctionContext::THREAD_LOCAL, state);
  } else {
    if (context->GetFunctionState(FunctionContext::THREAD_LOCAL) == nullptr) {
      context->SetError("BadExpr2 prepare error");
    }
  }
}

// Used by both BadExprPrepare() and BadExpr2Prepare() above.
IMPALA_UDF_EXPORT
BooleanVal BadExpr(FunctionContext* context, const DoubleVal& slot) {
  static int32_t count = 0;
  if (slot.is_null) return BooleanVal(false);
  if (++count > 100) {
    context->SetError("BadExpr error");
    return BooleanVal(false);
  }
  return BooleanVal(true);
}

// Used by both BadExprPrepare() and BadExpr2Prepare() above.
IMPALA_UDF_EXPORT
void BadExprClose(FunctionContext* context, FunctionContext::FunctionStateScope scope) {
  if (scope == FunctionContext::FRAGMENT_LOCAL) {
    int32_t* state = reinterpret_cast<int32_t*>(context->GetFunctionState(scope));
    assert(*state == 0xf001cafe);
    context->Free(reinterpret_cast<uint8_t*>(state));
    context->SetFunctionState(scope, nullptr);
  }
}

// MemTest UDF: "Allocates" the specified number of bytes per call.
IMPALA_UDF_EXPORT
void MemTestPrepare(FunctionContext* context, FunctionContext::FunctionStateScope scope) {
  if (scope == FunctionContext::THREAD_LOCAL) {
    int64_t* total =
        reinterpret_cast<int64_t*>(context->Allocate(sizeof(int64_t)));
    *total = 0;
    context->SetFunctionState(scope, total);
  }
}

IMPALA_UDF_EXPORT
BigIntVal MemTest(FunctionContext* context, const BigIntVal& bytes) {
  int64_t* total = reinterpret_cast<int64_t*>(
      context->GetFunctionState(FunctionContext::THREAD_LOCAL));
  context->TrackAllocation(bytes.val);
  *total += bytes.val;
  return bytes;
}

IMPALA_UDF_EXPORT
void MemTestClose(FunctionContext* context, FunctionContext::FunctionStateScope scope) {
  if (scope == FunctionContext::THREAD_LOCAL) {
    int64_t* total = reinterpret_cast<int64_t*>(
        context->GetFunctionState(FunctionContext::THREAD_LOCAL));
    // Initialization could have failed. Prepare() may not have been called.
    if (total == nullptr) return;
    context->Free(*total);
    context->Free(reinterpret_cast<uint8_t*>(total));
    context->SetFunctionState(scope, nullptr);
  }
}

IMPALA_UDF_EXPORT
BigIntVal DoubleFreeTest(FunctionContext* context, BigIntVal bytes) {
  context->TrackAllocation(bytes.val);
  context->Free(bytes.val);
  context->Free(bytes.val);
  return bytes;
}

extern "C"
IMPALA_UDF_EXPORT
BigIntVal UnmangledSymbol(FunctionContext* context) {
  return BigIntVal(5);
}

// Test that unexported symbols can't be found.
BigIntVal UnexportedSymbol(FunctionContext* context) {
  return BigIntVal(5);
}

// Functions to test interpreted path
IMPALA_UDF_EXPORT
IntVal FourArgs(FunctionContext* context, const IntVal& v1, const IntVal& v2,
    const IntVal& v3, const IntVal& v4) {
  return IntVal(v1.val + v2.val + v3.val + v4.val);
}

IMPALA_UDF_EXPORT
IntVal FiveArgs(FunctionContext* context, const IntVal& v1, const IntVal& v2,
    const IntVal& v3, const IntVal& v4, const IntVal& v5) {
  return IntVal(v1.val + v2.val + v3.val + v4.val + v5.val);
}

IMPALA_UDF_EXPORT
IntVal SixArgs(FunctionContext* context, const IntVal& v1, const IntVal& v2,
    const IntVal& v3, const IntVal& v4, const IntVal& v5, const IntVal& v6) {
  return IntVal(v1.val + v2.val + v3.val + v4.val + v5.val + v6.val);
}

IMPALA_UDF_EXPORT
IntVal SevenArgs(FunctionContext* context, const IntVal& v1, const IntVal& v2,
    const IntVal& v3, const IntVal& v4, const IntVal& v5, const IntVal& v6,
    const IntVal& v7) {
  return IntVal(v1.val + v2.val + v3.val + v4.val + v5.val + v6.val + v7.val);
}

IMPALA_UDF_EXPORT
IntVal EightArgs(FunctionContext* context, const IntVal& v1, const IntVal& v2,
    const IntVal& v3, const IntVal& v4, const IntVal& v5, const IntVal& v6,
    const IntVal& v7, const IntVal& v8) {
  return IntVal(v1.val + v2.val + v3.val + v4.val + v5.val + v6.val + v7.val + v8.val);
}

IMPALA_UDF_EXPORT
IntVal NineArgs(FunctionContext* context, const IntVal& v1, const IntVal& v2,
    const IntVal& v3, const IntVal& v4, const IntVal& v5, const IntVal& v6,
    const IntVal& v7, const IntVal& v8, const IntVal& v9) {
  return IntVal(v1.val + v2.val + v3.val + v4.val + v5.val + v6.val + v7.val + v8.val +
      v9.val);
}

IMPALA_UDF_EXPORT
IntVal TwentyArgs(FunctionContext* context, const IntVal& v1, const IntVal& v2,
    const IntVal& v3, const IntVal& v4, const IntVal& v5, const IntVal& v6,
    const IntVal& v7, const IntVal& v8, const IntVal& v9, const IntVal& v10,
    const IntVal& v11, const IntVal& v12, const IntVal& v13, const IntVal& v14,
    const IntVal& v15, const IntVal& v16, const IntVal& v17, const IntVal& v18,
    const IntVal& v19, const IntVal& v20) {
  return IntVal(v1.val + v2.val + v3.val + v4.val + v5.val + v6.val + v7.val + v8.val +
      v9.val + v10.val + v11.val + v12.val + v13.val + v14.val + v15.val + v16.val +
      v17.val + v18.val + v19.val + v20.val);
}

IMPALA_UDF_EXPORT
IntVal TwentyOneArgs(FunctionContext* context, const IntVal& v1, const IntVal& v2,
    const IntVal& v3, const IntVal& v4, const IntVal& v5, const IntVal& v6,
    const IntVal& v7, const IntVal& v8, const IntVal& v9, const IntVal& v10,
    const IntVal& v11, const IntVal& v12, const IntVal& v13, const IntVal& v14,
    const IntVal& v15, const IntVal& v16, const IntVal& v17, const IntVal& v18,
    const IntVal& v19, const IntVal& v20, const IntVal& v21) {
  return IntVal(v1.val + v2.val + v3.val + v4.val + v5.val + v6.val + v7.val + v8.val +
      v9.val + v10.val + v11.val + v12.val + v13.val + v14.val + v15.val + v16.val +
      v17.val + v18.val + v19.val + v20.val + v21.val);
}
