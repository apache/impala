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

using namespace impala;
using namespace impala_udf;

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

template<typename T, bool not_in>
BooleanVal InPredicate::TemplatedIn(
    FunctionContext* context, const T& val, int num_args, const T* args) {
  if (val.is_null) return BooleanVal::null();
  bool found_null = false;
  for (int32_t i = 0; i < num_args; ++i) {
    if (args[i].is_null) {
      found_null = true;
      continue;
    }
    if (Equals(val, args[i])) return BooleanVal(!not_in);
  }
  if (found_null) return BooleanVal::null();
  return BooleanVal(not_in);
}

// These wrappers are so we can use unmangled symbol names to register these functions
// TODO: figure out how to mangle templated functions
BooleanVal InPredicate::In(FunctionContext* context, const BooleanVal& val,
                           int num_args, const BooleanVal* args) {
  return TemplatedIn<BooleanVal, false>(context, val, num_args, args);
}
BooleanVal InPredicate::NotIn(FunctionContext* context, const BooleanVal& val,
                              int num_args, const BooleanVal* args) {
  return TemplatedIn<BooleanVal, true>(context, val, num_args, args);
}

BooleanVal InPredicate::In(FunctionContext* context, const TinyIntVal& val,
                           int num_args, const TinyIntVal* args) {
  return TemplatedIn<TinyIntVal, false>(context, val, num_args, args);
}
BooleanVal InPredicate::NotIn(FunctionContext* context, const TinyIntVal& val,
                              int num_args, const TinyIntVal* args) {
  return TemplatedIn<TinyIntVal, true>(context, val, num_args, args);
}

BooleanVal InPredicate::In(FunctionContext* context, const SmallIntVal& val,
                           int num_args, const SmallIntVal* args) {
  return TemplatedIn<SmallIntVal, false>(context, val, num_args, args);
}
BooleanVal InPredicate::NotIn(FunctionContext* context, const SmallIntVal& val,
                              int num_args, const SmallIntVal* args) {
  return TemplatedIn<SmallIntVal, true>(context, val, num_args, args);
}

BooleanVal InPredicate::In(FunctionContext* context, const IntVal& val,
                           int num_args, const IntVal* args) {
  return TemplatedIn<IntVal, false>(context, val, num_args, args);
}
BooleanVal InPredicate::NotIn(FunctionContext* context, const IntVal& val,
                              int num_args, const IntVal* args) {
  return TemplatedIn<IntVal, true>(context, val, num_args, args);
}

BooleanVal InPredicate::In(FunctionContext* context, const BigIntVal& val,
                           int num_args, const BigIntVal* args) {
  return TemplatedIn<BigIntVal, false>(context, val, num_args, args);
}
BooleanVal InPredicate::NotIn(FunctionContext* context, const BigIntVal& val,
                              int num_args, const BigIntVal* args) {
  return TemplatedIn<BigIntVal, true>(context, val, num_args, args);
}

BooleanVal InPredicate::In(FunctionContext* context, const FloatVal& val,
                           int num_args, const FloatVal* args) {
  return TemplatedIn<FloatVal, false>(context, val, num_args, args);
}
BooleanVal InPredicate::NotIn(FunctionContext* context, const FloatVal& val,
                              int num_args, const FloatVal* args) {
  return TemplatedIn<FloatVal, true>(context, val, num_args, args);
}

BooleanVal InPredicate::In(FunctionContext* context, const DoubleVal& val,
                           int num_args, const DoubleVal* args) {
  return TemplatedIn<DoubleVal, false>(context, val, num_args, args);
}
BooleanVal InPredicate::NotIn(FunctionContext* context, const DoubleVal& val,
                              int num_args, const DoubleVal* args) {
  return TemplatedIn<DoubleVal, true>(context, val, num_args, args);
}

BooleanVal InPredicate::In(FunctionContext* context, const StringVal& val,
                           int num_args, const StringVal* args) {
  return TemplatedIn<StringVal, false>(context, val, num_args, args);
}
BooleanVal InPredicate::NotIn(FunctionContext* context, const StringVal& val,
                              int num_args, const StringVal* args) {
  return TemplatedIn<StringVal, true>(context, val, num_args, args);
}

BooleanVal InPredicate::In(FunctionContext* context, const TimestampVal& val,
                           int num_args, const TimestampVal* args) {
  return TemplatedIn<TimestampVal, false>(context, val, num_args, args);
}
BooleanVal InPredicate::NotIn(FunctionContext* context, const TimestampVal& val,
                              int num_args, const TimestampVal* args) {
  return TemplatedIn<TimestampVal, true>(context, val, num_args, args);
}

BooleanVal InPredicate::In(FunctionContext* context, const DecimalVal& val,
                           int num_args, const DecimalVal* args) {
  return TemplatedIn<DecimalVal, false>(context, val, num_args, args);
}
BooleanVal InPredicate::NotIn(FunctionContext* context, const DecimalVal& val,
                              int num_args, const DecimalVal* args) {
  return TemplatedIn<DecimalVal, true>(context, val, num_args, args);
}
