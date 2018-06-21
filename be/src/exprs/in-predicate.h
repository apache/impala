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


#ifndef IMPALA_EXPRS_IN_PREDICATE_H_
#define IMPALA_EXPRS_IN_PREDICATE_H_

#include <string>
#include <boost/container/flat_set.hpp>
#include <boost/unordered_set.hpp>
#include "exprs/predicate.h"
#include "exprs/anyval-util.h"
#include "runtime/decimal-value.inline.h"
#include "runtime/string-value.inline.h"
#include "udf/udf.h"

namespace impala {

/// Utilities for evaluating expressions of the form "val [NOT] IN (x1, x2, x3...)".
/// In predicates are implemented using ScalarFnCall and the UDF interface.
///
/// There are two strategies for evaluating the IN predicate:
//
/// 1) SET_LOOKUP: This strategy is for when all the values in the IN list are constant.
///    In the prepare function, we create a set of the constant values from the IN list,
///    and use this set to lookup a given 'val'.
//
/// 2) ITERATE: This is the fallback strategy for when there are non-constant IN list
///    values, or very few values in the IN list. We simply iterate through every
///    expression and compare it to val. This strategy has no prepare function.
//
/// The FE chooses which strategy we should use by choosing the appropriate function
/// (e.g., InIterate() or InSetLookup()). If it chooses SET_LOOKUP, it also sets the
/// appropriate SetLookupPrepare and SetLookupClose functions.
class InPredicate {
 public:
  /// Functions for every type
  static impala_udf::BooleanVal InIterate(impala_udf::FunctionContext* context,
      const impala_udf::BooleanVal& val, int num_args,
      const impala_udf::BooleanVal* args);

  static impala_udf::BooleanVal NotInIterate(impala_udf::FunctionContext* context,
      const impala_udf::BooleanVal& val, int num_args,
      const impala_udf::BooleanVal* args);

  static void SetLookupPrepare_boolean(impala_udf::FunctionContext* ctx,
      impala_udf::FunctionContext::FunctionStateScope scope);

  static void SetLookupClose_boolean(impala_udf::FunctionContext* ctx,
      impala_udf::FunctionContext::FunctionStateScope scope);

  static impala_udf::BooleanVal InSetLookup(
      impala_udf::FunctionContext* context, const impala_udf::BooleanVal& val,
      int num_args, const impala_udf::BooleanVal* args);

  static impala_udf::BooleanVal NotInSetLookup(
      impala_udf::FunctionContext* context, const impala_udf::BooleanVal& val,
      int num_args, const impala_udf::BooleanVal* args);

  static impala_udf::BooleanVal InIterate(
      impala_udf::FunctionContext* context, const impala_udf::TinyIntVal& val,
      int num_args, const impala_udf::TinyIntVal* args);

  static impala_udf::BooleanVal NotInIterate(
      impala_udf::FunctionContext* context, const impala_udf::TinyIntVal& val,
      int num_args, const impala_udf::TinyIntVal* args);

  static void SetLookupPrepare_tinyint(impala_udf::FunctionContext* ctx,
      impala_udf::FunctionContext::FunctionStateScope scope);

  static void SetLookupClose_tinyint(impala_udf::FunctionContext* ctx,
      impala_udf::FunctionContext::FunctionStateScope scope);

  static impala_udf::BooleanVal InSetLookup(
      impala_udf::FunctionContext* context, const impala_udf::TinyIntVal& val,
      int num_args, const impala_udf::TinyIntVal* args);

  static impala_udf::BooleanVal NotInSetLookup(
      impala_udf::FunctionContext* context, const impala_udf::TinyIntVal& val,
      int num_args, const impala_udf::TinyIntVal* args);

  static impala_udf::BooleanVal InIterate(
      impala_udf::FunctionContext* context, const impala_udf::SmallIntVal& val,
      int num_args, const impala_udf::SmallIntVal* args);

  static impala_udf::BooleanVal NotInIterate(
      impala_udf::FunctionContext* context, const impala_udf::SmallIntVal& val,
      int num_args, const impala_udf::SmallIntVal* args);

  static void SetLookupPrepare_smallint(impala_udf::FunctionContext* ctx,
      impala_udf::FunctionContext::FunctionStateScope scope);

  static void SetLookupClose_smallint(impala_udf::FunctionContext* ctx,
      impala_udf::FunctionContext::FunctionStateScope scope);

  static impala_udf::BooleanVal InSetLookup(
      impala_udf::FunctionContext* context, const impala_udf::SmallIntVal& val,
      int num_args, const impala_udf::SmallIntVal* args);

  static impala_udf::BooleanVal NotInSetLookup(
      impala_udf::FunctionContext* context, const impala_udf::SmallIntVal& val,
      int num_args, const impala_udf::SmallIntVal* args);

  static impala_udf::BooleanVal InIterate(
      impala_udf::FunctionContext* context, const impala_udf::IntVal& val,
      int num_args, const impala_udf::IntVal* args);

  static impala_udf::BooleanVal NotInIterate(
      impala_udf::FunctionContext* context, const impala_udf::IntVal& val,
      int num_args, const impala_udf::IntVal* args);

  static void SetLookupPrepare_int(impala_udf::FunctionContext* ctx,
      impala_udf::FunctionContext::FunctionStateScope scope);

  static void SetLookupClose_int(impala_udf::FunctionContext* ctx,
      impala_udf::FunctionContext::FunctionStateScope scope);

  static impala_udf::BooleanVal InSetLookup(
      impala_udf::FunctionContext* context, const impala_udf::IntVal& val,
      int num_args, const impala_udf::IntVal* args);

  static impala_udf::BooleanVal NotInSetLookup(
      impala_udf::FunctionContext* context, const impala_udf::IntVal& val,
      int num_args, const impala_udf::IntVal* args);

  static impala_udf::BooleanVal InIterate(
      impala_udf::FunctionContext* context, const impala_udf::DateVal& val,
      int num_args, const impala_udf::DateVal* args);

  static impala_udf::BooleanVal NotInIterate(
      impala_udf::FunctionContext* context, const impala_udf::DateVal& val,
      int num_args, const impala_udf::DateVal* args);

  static void SetLookupPrepare_date(impala_udf::FunctionContext* ctx,
      impala_udf::FunctionContext::FunctionStateScope scope);

  static void SetLookupClose_date(impala_udf::FunctionContext* ctx,
      impala_udf::FunctionContext::FunctionStateScope scope);

  static impala_udf::BooleanVal InSetLookup(
      impala_udf::FunctionContext* context, const impala_udf::DateVal& val,
      int num_args, const impala_udf::DateVal* args);

  static impala_udf::BooleanVal NotInSetLookup(
      impala_udf::FunctionContext* context, const impala_udf::DateVal& val,
      int num_args, const impala_udf::DateVal* args);

  static impala_udf::BooleanVal InIterate(
      impala_udf::FunctionContext* context, const impala_udf::BigIntVal& val,
      int num_args, const impala_udf::BigIntVal* args);

  static impala_udf::BooleanVal NotInIterate(
      impala_udf::FunctionContext* context, const impala_udf::BigIntVal& val,
      int num_args, const impala_udf::BigIntVal* args);

  static void SetLookupPrepare_bigint(impala_udf::FunctionContext* ctx,
      impala_udf::FunctionContext::FunctionStateScope scope);

  static void SetLookupClose_bigint(impala_udf::FunctionContext* ctx,
      impala_udf::FunctionContext::FunctionStateScope scope);

  static impala_udf::BooleanVal InSetLookup(
      impala_udf::FunctionContext* context, const impala_udf::BigIntVal& val,
      int num_args, const impala_udf::BigIntVal* args);

  static impala_udf::BooleanVal NotInSetLookup(
      impala_udf::FunctionContext* context, const impala_udf::BigIntVal& val,
      int num_args, const impala_udf::BigIntVal* args);

  static impala_udf::BooleanVal InIterate(
      impala_udf::FunctionContext* context, const impala_udf::FloatVal& val,
      int num_args, const impala_udf::FloatVal* args);

  static impala_udf::BooleanVal NotInIterate(
      impala_udf::FunctionContext* context, const impala_udf::FloatVal& val,
      int num_args, const impala_udf::FloatVal* args);

  static void SetLookupPrepare_float(impala_udf::FunctionContext* ctx,
      impala_udf::FunctionContext::FunctionStateScope scope);

  static void SetLookupClose_float(impala_udf::FunctionContext* ctx,
      impala_udf::FunctionContext::FunctionStateScope scope);

  static impala_udf::BooleanVal InSetLookup(
      impala_udf::FunctionContext* context, const impala_udf::FloatVal& val,
      int num_args, const impala_udf::FloatVal* args);

  static impala_udf::BooleanVal NotInSetLookup(
      impala_udf::FunctionContext* context, const impala_udf::FloatVal& val,
      int num_args, const impala_udf::FloatVal* args);

  static impala_udf::BooleanVal InIterate(
      impala_udf::FunctionContext* context, const impala_udf::DoubleVal& val,
      int num_args, const impala_udf::DoubleVal* args);

  static impala_udf::BooleanVal NotInIterate(
      impala_udf::FunctionContext* context, const impala_udf::DoubleVal& val,
      int num_args, const impala_udf::DoubleVal* args);

  static void SetLookupPrepare_double(impala_udf::FunctionContext* ctx,
      impala_udf::FunctionContext::FunctionStateScope scope);

  static void SetLookupClose_double(impala_udf::FunctionContext* ctx,
      impala_udf::FunctionContext::FunctionStateScope scope);

  static impala_udf::BooleanVal InSetLookup(
      impala_udf::FunctionContext* context, const impala_udf::DoubleVal& val,
      int num_args, const impala_udf::DoubleVal* args);

  static impala_udf::BooleanVal NotInSetLookup(
      impala_udf::FunctionContext* context, const impala_udf::DoubleVal& val,
      int num_args, const impala_udf::DoubleVal* args);

  static impala_udf::BooleanVal InIterate(
      impala_udf::FunctionContext* context, const impala_udf::StringVal& val,
      int num_args, const impala_udf::StringVal* args);

  static impala_udf::BooleanVal NotInIterate(
      impala_udf::FunctionContext* context, const impala_udf::StringVal& val,
      int num_args, const impala_udf::StringVal* args);

  static void SetLookupPrepare_string(impala_udf::FunctionContext* ctx,
      impala_udf::FunctionContext::FunctionStateScope scope);

  static void SetLookupClose_string(impala_udf::FunctionContext* ctx,
      impala_udf::FunctionContext::FunctionStateScope scope);

  static impala_udf::BooleanVal InSetLookup(
      impala_udf::FunctionContext* context, const impala_udf::StringVal& val,
      int num_args, const impala_udf::StringVal* args);

  static impala_udf::BooleanVal NotInSetLookup(
      impala_udf::FunctionContext* context, const impala_udf::StringVal& val,
      int num_args, const impala_udf::StringVal* args);

  static impala_udf::BooleanVal InIterate(
      impala_udf::FunctionContext* context, const impala_udf::TimestampVal& val,
      int num_args, const impala_udf::TimestampVal* args);

  static impala_udf::BooleanVal NotInIterate(
      impala_udf::FunctionContext* context, const impala_udf::TimestampVal& val,
      int num_args, const impala_udf::TimestampVal* args);

  static void SetLookupPrepare_timestamp(impala_udf::FunctionContext* ctx,
      impala_udf::FunctionContext::FunctionStateScope scope);

  static void SetLookupClose_timestamp(impala_udf::FunctionContext* ctx,
      impala_udf::FunctionContext::FunctionStateScope scope);

  static impala_udf::BooleanVal InSetLookup(
      impala_udf::FunctionContext* context, const impala_udf::TimestampVal& val,
      int num_args, const impala_udf::TimestampVal* args);

  static impala_udf::BooleanVal NotInSetLookup(
      impala_udf::FunctionContext* context, const impala_udf::TimestampVal& val,
      int num_args, const impala_udf::TimestampVal* args);

  static impala_udf::BooleanVal InIterate(
      impala_udf::FunctionContext* context, const impala_udf::DecimalVal& val,
      int num_args, const impala_udf::DecimalVal* args);

  static impala_udf::BooleanVal NotInIterate(
      impala_udf::FunctionContext* context, const impala_udf::DecimalVal& val,
      int num_args, const impala_udf::DecimalVal* args);

  static void SetLookupPrepare_decimal(impala_udf::FunctionContext* ctx,
      impala_udf::FunctionContext::FunctionStateScope scope);

  static void SetLookupClose_decimal(impala_udf::FunctionContext* ctx,
      impala_udf::FunctionContext::FunctionStateScope scope);

  static impala_udf::BooleanVal InSetLookup(
      impala_udf::FunctionContext* context, const impala_udf::DecimalVal& val,
      int num_args, const impala_udf::DecimalVal* args);

  static impala_udf::BooleanVal NotInSetLookup(
      impala_udf::FunctionContext* context, const impala_udf::DecimalVal& val,
      int num_args, const impala_udf::DecimalVal* args);

 private:
  friend class InPredicateBenchmark;

  enum Strategy {
    /// Indicates we should use SetLookUp().
    SET_LOOKUP,
    /// Indicates we should use Iterate().
    ITERATE
  };

  template<typename SetType>
  struct SetLookupState {
    /// If true, there is at least one NULL constant in the IN list.
    bool contains_null;

    /// The set of all non-NULL constant values in the IN list.
    boost::unordered_set<SetType> val_set;

    /// The type of the arguments
    const FunctionContext::TypeDesc* type;
  };

  /// The templated function that provides the implementation for all the In() and NotIn()
  /// functions.
  template<typename T, typename SetType, bool not_in, Strategy strategy>
  static inline impala_udf::BooleanVal TemplatedIn(
      impala_udf::FunctionContext* ctx, const T& val, int num_args, const T* args);

  /// Initializes an SetLookupState in ctx.
  template<typename T, typename SetType>
  static void SetLookupPrepare(
      FunctionContext* ctx, FunctionContext::FunctionStateScope scope);

  template<typename SetType>
  static void SetLookupClose(
      FunctionContext* ctx, FunctionContext::FunctionStateScope scope);

  /// Looks up v in state->val_set.
  template<typename T, typename SetType>
  static BooleanVal SetLookup(SetLookupState<SetType>* state, const T& v);

  /// Iterates through each vararg looking for val. 'type' is the type of 'val' and
  /// 'args'.
  template <typename T>
  static BooleanVal Iterate(
      const FunctionContext::TypeDesc* type, const T& val, int num_args, const T* args);

  // Templated getter functions for extracting 'SetType' values from AnyVals
  template <typename T, typename SetType>
  static inline SetType GetVal(const FunctionContext::TypeDesc* type, const T& x);
};

template <typename T, typename SetType, bool not_in, InPredicate::Strategy strategy>
inline impala_udf::BooleanVal InPredicate::TemplatedIn(
    impala_udf::FunctionContext* ctx, const T& val, int num_args, const T* args) {
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

template <typename T, typename SetType>
void InPredicate::SetLookupPrepare(
    FunctionContext* ctx, FunctionContext::FunctionStateScope scope) {
  if (scope != FunctionContext::FRAGMENT_LOCAL) return;

  SetLookupState<SetType>* state = new SetLookupState<SetType>;
  state->type = ctx->GetArgType(0);
  state->contains_null = false;
  // Collect all values in a vector to use the bulk insert API to avoid N^2 behavior
  // with flat_set.
  std::vector<SetType> element_list;
  element_list.reserve(ctx->GetNumArgs() - 1);
  for (int i = 1; i < ctx->GetNumArgs(); ++i) {
    DCHECK(ctx->IsArgConstant(i));
    T* arg = reinterpret_cast<T*>(ctx->GetConstantArg(i));
    if (arg->is_null) {
      state->contains_null = true;
    } else {
      element_list.push_back(GetVal<T, SetType>(state->type, *arg));
    }
  }
  state->val_set.insert(element_list.begin(), element_list.end());
  ctx->SetFunctionState(scope, state);
}

template <typename SetType>
void InPredicate::SetLookupClose(
    FunctionContext* ctx, FunctionContext::FunctionStateScope scope) {
  if (scope != FunctionContext::FRAGMENT_LOCAL) return;
  SetLookupState<SetType>* state =
      reinterpret_cast<SetLookupState<SetType>*>(ctx->GetFunctionState(scope));
  delete state;
  ctx->SetFunctionState(scope, nullptr);
}

template <typename T, typename SetType>
BooleanVal InPredicate::SetLookup(SetLookupState<SetType>* state, const T& v) {
  DCHECK(state != NULL);
  SetType val = GetVal<T, SetType>(state->type, v);
  bool found = state->val_set.find(val) != state->val_set.end();
  if (found) return BooleanVal(true);
  if (state->contains_null) return BooleanVal::null();
  return BooleanVal(false);
}

template <typename T>
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

template <typename T, typename SetType>
inline SetType InPredicate::GetVal(const FunctionContext::TypeDesc* type, const T& x) {
  DCHECK(!x.is_null);
  return x.val;
}

template <>
inline StringValue InPredicate::GetVal(
    const FunctionContext::TypeDesc* type, const StringVal& x) {
  DCHECK(!x.is_null);
  return StringValue::FromStringVal(x);
}

template <>
inline TimestampValue InPredicate::GetVal(
    const FunctionContext::TypeDesc* type, const TimestampVal& x) {
  return TimestampValue::FromTimestampVal(x);
}

template <>
inline Decimal16Value InPredicate::GetVal(
    const FunctionContext::TypeDesc* type, const DecimalVal& x) {
  if (type->precision <= ColumnType::MAX_DECIMAL4_PRECISION) {
    return Decimal16Value(x.val4);
  } else if (type->precision <= ColumnType::MAX_DECIMAL8_PRECISION) {
    return Decimal16Value(x.val8);
  } else {
    return Decimal16Value(x.val16);
  }
}

// IMPALA-6621: Due to an implementation detail in boost, these 3 types are slower when
// using unordered_map as compared to flat_set.
template <>
struct InPredicate::SetLookupState<int32_t> {
  bool contains_null;
  boost::container::flat_set<int32_t> val_set;
  const FunctionContext::TypeDesc* type;
};

template <>
struct InPredicate::SetLookupState<int64_t> {
  bool contains_null;
  boost::container::flat_set<int64_t> val_set;
  const FunctionContext::TypeDesc* type;
};

template <>
struct InPredicate::SetLookupState<float> {
  bool contains_null;
  boost::container::flat_set<float> val_set;
  const FunctionContext::TypeDesc* type;
};
}

#endif
