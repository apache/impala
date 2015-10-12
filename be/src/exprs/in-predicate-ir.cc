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

#include <sstream>

#include "exprs/in-predicate.h"

#include "exprs/anyval-util.h"
#include "runtime/string-value.inline.h"

#include "common/names.h"

using namespace impala_udf;

namespace impala {

// Templated getter functions for extracting 'SetType' values from AnyVals
template<typename T, typename SetType>
SetType GetVal(const FunctionContext::TypeDesc* type, const T& x) {
  DCHECK(!x.is_null);
  return x.val;
}

template<> StringValue GetVal(const FunctionContext::TypeDesc* type, const StringVal& x) {
  DCHECK(!x.is_null);
  return StringValue::FromStringVal(x);
}

template<> TimestampValue GetVal(
    const FunctionContext::TypeDesc* type, const TimestampVal& x) {
  return TimestampValue::FromTimestampVal(x);
}

template<> Decimal16Value GetVal(
    const FunctionContext::TypeDesc* type, const DecimalVal& x) {
  if (type->precision <= ColumnType::MAX_DECIMAL4_PRECISION) {
    return Decimal16Value(x.val4);
  } else if (type->precision <= ColumnType::MAX_DECIMAL8_PRECISION) {
    return Decimal16Value(x.val8);
  } else {
    return Decimal16Value(x.val16);
  }
}

template<typename T, typename SetType>
void InPredicate::SetLookupPrepare(
    FunctionContext* ctx, FunctionContext::FunctionStateScope scope) {
  if (scope != FunctionContext::FRAGMENT_LOCAL) return;

  SetLookupState<SetType>* state = new SetLookupState<SetType>;
  state->type = ctx->GetArgType(0);
  state->contains_null = false;
  for (int i = 1; i < ctx->GetNumArgs(); ++i) {
    DCHECK(ctx->IsArgConstant(i));
    T* arg = reinterpret_cast<T*>(ctx->GetConstantArg(i));
    if (arg->is_null) {
      state->contains_null = true;
    } else {
      state->val_set.insert(GetVal<T, SetType>(state->type, *arg));
    }
  }
  ctx->SetFunctionState(scope, state);
}

template<typename SetType>
void InPredicate::SetLookupClose(
    FunctionContext* ctx, FunctionContext::FunctionStateScope scope) {
  if (scope != FunctionContext::FRAGMENT_LOCAL) return;
  SetLookupState<SetType>* state =
      reinterpret_cast<SetLookupState<SetType>*>(ctx->GetFunctionState(scope));
  delete state;
}

template<typename T, typename SetType, bool not_in, InPredicate::Strategy strategy>
BooleanVal InPredicate::TemplatedIn(
    FunctionContext* ctx, const T& val, int num_args, const T* args) {
  if (val.is_null) return BooleanVal::null();

  BooleanVal found;
  if (strategy == SET_LOOKUP) {
    SetLookupState<SetType>* state = reinterpret_cast<SetLookupState<SetType>*>(
        ctx->GetFunctionState(FunctionContext::FRAGMENT_LOCAL));
    DCHECK(state != NULL);
    found = SetLookup(state, val);
  } else {
    DCHECK_EQ(strategy, ITERATE);
    found = Iterate(ctx->GetArgType(0), val, num_args, args);
  }
  if (found.is_null) return BooleanVal::null();
  return BooleanVal(found.val ^ not_in);
}

template<typename T, typename SetType>
BooleanVal InPredicate::SetLookup(
    SetLookupState<SetType>* state, const T& v) {
  DCHECK(state != NULL);
  SetType val = GetVal<T, SetType>(state->type, v);
  bool found = state->val_set.find(val) != state->val_set.end();
  if (found) return BooleanVal(true);
  if (state->contains_null) return BooleanVal::null();
  return BooleanVal(false);
}

template<typename T>
BooleanVal InPredicate::Iterate(
    const FunctionContext::TypeDesc* type, const T& val, int num_args, const T* args) {
  bool found_null = false;
  for (int i = 0; i < num_args; ++i) {
    if (args[i].is_null) {
      found_null = true;
    } else if (AnyValUtil::Equals(*type, val, args[i])) {
      return BooleanVal(true);
    }
  }
  if (found_null) return BooleanVal::null();
  return BooleanVal(false);
}

#define IN_FUNCTIONS(AnyValType, SetType, type_name) \
  BooleanVal InPredicate::InSetLookup( \
      FunctionContext* context, const AnyValType& val, int num_args, \
      const AnyValType* args) { \
    return TemplatedIn<AnyValType, SetType, false, SET_LOOKUP>( \
        context, val, num_args, args); \
  } \
\
  BooleanVal InPredicate::NotInSetLookup( \
      FunctionContext* context, const AnyValType& val, int num_args, \
      const AnyValType* args) { \
    return TemplatedIn<AnyValType, SetType, true, SET_LOOKUP>( \
        context, val, num_args, args); \
  } \
\
  BooleanVal InPredicate::InIterate( \
      FunctionContext* context, const AnyValType& val, int num_args, \
      const AnyValType* args) { \
    return TemplatedIn<AnyValType, SetType, false, ITERATE>( \
        context, val, num_args, args); \
  } \
\
  BooleanVal InPredicate::NotInIterate( \
      FunctionContext* context, const AnyValType& val, int num_args, \
      const AnyValType* args) { \
    return TemplatedIn<AnyValType, SetType, true, ITERATE>( \
        context, val, num_args, args); \
  } \
\
  void InPredicate::SetLookupPrepare_##type_name( \
      FunctionContext* ctx, FunctionContext::FunctionStateScope scope) { \
    SetLookupPrepare<AnyValType, SetType>(ctx, scope); \
  } \
\
  void InPredicate::SetLookupClose_##type_name( \
      FunctionContext* ctx, FunctionContext::FunctionStateScope scope) { \
    SetLookupClose<SetType>(ctx, scope); \
  }

IN_FUNCTIONS(BooleanVal, bool, boolean)
IN_FUNCTIONS(TinyIntVal, int8_t, tinyint)
IN_FUNCTIONS(SmallIntVal, int16_t, smallint)
IN_FUNCTIONS(IntVal, int32_t, int)
IN_FUNCTIONS(BigIntVal, int64_t, bigint)
IN_FUNCTIONS(FloatVal, float, float)
IN_FUNCTIONS(DoubleVal, double, double)
IN_FUNCTIONS(StringVal, StringValue, string)
IN_FUNCTIONS(TimestampVal, TimestampValue, timestamp)
IN_FUNCTIONS(DecimalVal, Decimal16Value, decimal)

// Needed for in-predicate-benchmark to build
template BooleanVal InPredicate::Iterate<IntVal>(
    const FunctionContext::TypeDesc*, const IntVal&, int, const IntVal*);

}
