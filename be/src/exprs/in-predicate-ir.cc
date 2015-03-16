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
#include "runtime/string-value.inline.h"

using namespace impala_udf;
using namespace std;

namespace impala {

// Templated equality functions
template<typename T>
bool Equals(const T& x, const T& y) {
  return x.val == y.val;
}

template<> bool Equals(const StringVal& x, const StringVal& y) {
  StringValue x_sv = StringValue::FromStringVal(x);
  StringValue y_sv = StringValue::FromStringVal(y);
  return x_sv == y_sv;
}

template<> bool Equals(const TimestampVal& x, const TimestampVal& y) {
  TimestampValue x_tv = TimestampValue::FromTimestampVal(x);
  TimestampValue y_tv = TimestampValue::FromTimestampVal(y);
  return x_tv == y_tv;
}

// TODO: take byte size into account
template<> bool Equals(const DecimalVal& x, const DecimalVal& y) {
  return x == y;
}

// Templated getter functions for extracting 'SetType' values from AnyVals
template<typename T, typename SetType>
SetType GetVal(const T& x) {
  DCHECK(!x.is_null);
  return x.val;
}

template<> StringValue GetVal(const StringVal& x) {
  DCHECK(!x.is_null);
  return StringValue::FromStringVal(x);
}

template<> int GetVal(const TimestampVal& x) { // TODO
  DCHECK(false) << "Should never be called";
  return 0;
}

template<> int GetVal(const DecimalVal& x) { // TODO
  DCHECK(false) << "Should never be called";
  return 0;
}

template<typename T, typename SetType>
void InPredicate::SetLookupPrepare(
    FunctionContext* ctx, FunctionContext::FunctionStateScope scope) {
  if (scope != FunctionContext::FRAGMENT_LOCAL) return;

  SetLookupState<SetType>* state = new SetLookupState<SetType>();
  state->contains_null = false;
  for (int i = 1; i < ctx->GetNumArgs(); ++i) {
    DCHECK(ctx->IsArgConstant(i));
    T* arg = reinterpret_cast<T*>(ctx->GetConstantArg(i));
    if (arg->is_null) {
      state->contains_null = true;
    } else {
      state->val_set.insert(GetVal<T, SetType>(*arg));
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
    found = SetLookup(state, val);
  } else {
    DCHECK_EQ(strategy, ITERATE);
    found = Iterate<T>(val, num_args, args);
  }
  if (found.is_null) return BooleanVal::null();
  return BooleanVal(found.val ^ not_in);
}

template<typename T, typename SetType>
BooleanVal InPredicate::SetLookup(
    SetLookupState<SetType>* state, const T& v) {
  DCHECK_NOTNULL(state);
  bool found = state->val_set.find(GetVal<T, SetType>(v)) != state->val_set.end();
  if (found) return BooleanVal(true);
  if (state->contains_null) return BooleanVal::null();
  return BooleanVal(false);
}

template<typename T>
BooleanVal InPredicate::Iterate(const T& val, int num_args, const T* args) {
  bool found_null = false;
  for (int i = 0; i < num_args; ++i) {
    if (args[i].is_null) {
      found_null = true;
    } else if (Equals(val, args[i])) {
      return BooleanVal(true);
    }
  }
  if (found_null) return BooleanVal::null();
  return BooleanVal(false);
}

#define IN_FUNCTIONS(AnyValType, SetType, type_name) \
  BooleanVal InPredicate::In_SetLookup( \
      FunctionContext* context, const AnyValType& val, int num_args, \
      const AnyValType* args) { \
    return TemplatedIn<AnyValType, SetType, false, SET_LOOKUP>( \
        context, val, num_args, args); \
  } \
\
  BooleanVal InPredicate::NotIn_SetLookup( \
      FunctionContext* context, const AnyValType& val, int num_args, \
      const AnyValType* args) { \
    return TemplatedIn<AnyValType, SetType, true, SET_LOOKUP>( \
        context, val, num_args, args); \
  } \
\
  BooleanVal InPredicate::In_Iterate( \
      FunctionContext* context, const AnyValType& val, int num_args, \
      const AnyValType* args) { \
    return TemplatedIn<AnyValType, SetType, false, ITERATE>( \
        context, val, num_args, args); \
  } \
\
  BooleanVal InPredicate::NotIn_Iterate( \
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

// Passing int as the 'SetType' for TimestampVal and DecimalVal is arbitrary, since we
// don't yet support sets for them yet.
BooleanVal InPredicate::In_Iterate(FunctionContext* context, const TimestampVal& val,
    int num_args, const TimestampVal* args) {
  return TemplatedIn<TimestampVal, int, false, ITERATE>(context, val, num_args, args);
}

BooleanVal InPredicate::NotIn_Iterate(FunctionContext* context, const TimestampVal& val,
    int num_args, const TimestampVal* args) {
  return TemplatedIn<TimestampVal, int, true, ITERATE>(context, val, num_args, args);
}

BooleanVal InPredicate::In_Iterate(FunctionContext* context, const DecimalVal& val,
    int num_args, const DecimalVal* args) {
  return TemplatedIn<DecimalVal, int, false, ITERATE>(context, val, num_args, args);
}

BooleanVal InPredicate::NotIn_Iterate(FunctionContext* context, const DecimalVal& val,
    int num_args, const DecimalVal* args) {
  return TemplatedIn<DecimalVal, int, true, ITERATE>(context, val, num_args, args);
}

}
