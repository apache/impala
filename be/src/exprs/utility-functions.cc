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

#include "exprs/utility-functions.h"

#include <gutil/strings/substitute.h>

#include "exprs/anyval-util.h"
#include "runtime/runtime-state.h"
#include "udf/udf-internal.h"
#include "util/debug-util.h"
#include "util/time.h"

#include "common/names.h"

using namespace strings;

namespace impala {

BigIntVal UtilityFunctions::FnvHashString(FunctionContext* ctx,
                                          const StringVal& input_val) {
  if (input_val.is_null) return BigIntVal::null();
  return BigIntVal(HashUtil::FnvHash64(input_val.ptr, input_val.len, HashUtil::FNV_SEED));
}

BigIntVal UtilityFunctions::FnvHashTimestamp(FunctionContext* ctx,
                                             const TimestampVal& input_val) {
  if (input_val.is_null) return BigIntVal::null();
  TimestampValue tv = TimestampValue::FromTimestampVal(input_val);
  return BigIntVal(HashUtil::FnvHash64(&tv, 12, HashUtil::FNV_SEED));
}

template<typename T>
BigIntVal UtilityFunctions::FnvHash(FunctionContext* ctx, const T& input_val) {
  if (input_val.is_null) return BigIntVal::null();
  return BigIntVal(
      HashUtil::FnvHash64(&input_val.val, sizeof(input_val.val), HashUtil::FNV_SEED));
}

// Note that this only hashes the unscaled value and not the scale or precision, so this
// function is only valid when used over a single decimal type.
BigIntVal UtilityFunctions::FnvHashDecimal(FunctionContext* ctx,
                                           const DecimalVal& input_val) {
  if (input_val.is_null) return BigIntVal::null();
  const FunctionContext::TypeDesc& input_type = *ctx->GetArgType(0);
  int byte_size = ColumnType::GetDecimalByteSize(input_type.precision);
  return BigIntVal(HashUtil::FnvHash64(&input_val.val16, byte_size, HashUtil::FNV_SEED));
}

template BigIntVal UtilityFunctions::FnvHash(
    FunctionContext* ctx, const BooleanVal& input_val);
template BigIntVal UtilityFunctions::FnvHash(
    FunctionContext* ctx, const TinyIntVal& input_val);
template BigIntVal UtilityFunctions::FnvHash(
    FunctionContext* ctx, const SmallIntVal& input_val);
template BigIntVal UtilityFunctions::FnvHash(
    FunctionContext* ctx, const IntVal& input_val);
template BigIntVal UtilityFunctions::FnvHash(
    FunctionContext* ctx, const BigIntVal& input_val);
template BigIntVal UtilityFunctions::FnvHash(
    FunctionContext* ctx, const FloatVal& input_val);
template BigIntVal UtilityFunctions::FnvHash(
    FunctionContext* ctx, const DoubleVal& input_val);

StringVal UtilityFunctions::User(FunctionContext* ctx) {
  StringVal user(ctx->user());
  // An empty string indicates the user wasn't set in the session or in the query request.
  return (user.len > 0) ? user : StringVal::null();
}

StringVal UtilityFunctions::EffectiveUser(FunctionContext* ctx) {
  StringVal effective_user(ctx->effective_user());
  // An empty string indicates the user wasn't set in the session or in the query request.
  return (effective_user.len > 0) ? effective_user : StringVal::null();
}

StringVal UtilityFunctions::Version(FunctionContext* ctx) {
  return AnyValUtil::FromString(ctx, GetVersionString());
}

IntVal UtilityFunctions::Pid(FunctionContext* ctx) {
  int pid = ctx->impl()->state()->query_ctx().pid;
  // Will be -1 if the PID could not be determined
  if (pid == -1) return IntVal::null();
  // Otherwise the PID should be greater than 0
  DCHECK(pid > 0);
  return IntVal(pid);
}

BooleanVal UtilityFunctions::Sleep(FunctionContext* ctx, const IntVal& milliseconds ) {
  if (milliseconds.is_null) return BooleanVal::null();
  SleepForMs(milliseconds.val);
  return BooleanVal(true);
}

StringVal UtilityFunctions::CurrentDatabase(FunctionContext* ctx) {
  StringVal database =
      AnyValUtil::FromString(ctx, ctx->impl()->state()->query_ctx().session.database);
  // An empty string indicates the current database wasn't set.
  return (database.len > 0) ? database : StringVal::null();
}

template<typename T>
StringVal UtilityFunctions::TypeOf(FunctionContext* ctx, const T& /*input_val*/) {
  FunctionContext::TypeDesc type_desc = *(ctx->GetArgType(0));
  ColumnType column_type = AnyValUtil::TypeDescToColumnType(type_desc);
  const string& type_string = TypeToString(column_type.type);

  switch(column_type.type) {
    // Show the precision and scale of DECIMAL type.
    case TYPE_DECIMAL:
      return AnyValUtil::FromString(ctx, Substitute("$0($1,$2)", type_string,
          type_desc.precision, type_desc.scale));
    // Show length of CHAR and VARCHAR.
    case TYPE_CHAR:
    case TYPE_VARCHAR:
      return AnyValUtil::FromString(ctx, Substitute("$0($1)", type_string,
          type_desc.len));
    default:
      return AnyValUtil::FromString(ctx, type_string);
  }
}

template StringVal UtilityFunctions::TypeOf(FunctionContext* ctx, const BooleanVal& input_val);
template StringVal UtilityFunctions::TypeOf(FunctionContext* ctx, const TinyIntVal& input_val);
template StringVal UtilityFunctions::TypeOf(FunctionContext* ctx, const SmallIntVal& input_val);
template StringVal UtilityFunctions::TypeOf(FunctionContext* ctx, const IntVal& input_val);
template StringVal UtilityFunctions::TypeOf(FunctionContext* ctx, const BigIntVal& input_val);
template StringVal UtilityFunctions::TypeOf(FunctionContext* ctx, const FloatVal& input_val);
template StringVal UtilityFunctions::TypeOf(FunctionContext* ctx, const DoubleVal& input_val);
template StringVal UtilityFunctions::TypeOf(FunctionContext* ctx, const StringVal& input_val);
template StringVal UtilityFunctions::TypeOf(FunctionContext* ctx, const TimestampVal& input_val);
template StringVal UtilityFunctions::TypeOf(FunctionContext* ctx, const DecimalVal& input_val);
}
