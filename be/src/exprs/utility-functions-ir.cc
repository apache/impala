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

#include "exprs/utility-functions.h"
#include <gutil/strings/substitute.h>
#include "common/compiler-util.h"
#include "exprs/anyval-util.h"
#include "exprs/math-functions.h"
#include "exprs/string-functions.h"

#include <openssl/md5.h>
#include <openssl/sha.h>
#include "runtime/runtime-state.h"
#include "udf/udf-internal.h"
#include "util/debug-util.h"
#include "util/openssl-util.h"
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
  // val4, val8 and val16 all start at the same memory address.
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
template BigIntVal UtilityFunctions::FnvHash(
    FunctionContext* ctx, const DateVal& input_val);

BigIntVal UtilityFunctions::MurmurHashString(FunctionContext* ctx,
    const StringVal& input_val) {
  if (input_val.is_null) return BigIntVal::null();
  return BigIntVal(HashUtil::MurmurHash2_64(input_val.ptr, input_val.len,
        HashUtil::MURMUR_DEFAULT_SEED));
}

BigIntVal UtilityFunctions::MurmurHashTimestamp(FunctionContext* ctx,
    const TimestampVal& input_val) {
  if (input_val.is_null) return BigIntVal::null();
  TimestampValue tv = TimestampValue::FromTimestampVal(input_val);
  return BigIntVal(HashUtil::MurmurHash2_64(&tv, 12, HashUtil::MURMUR_DEFAULT_SEED));
}

template<typename T>
BigIntVal UtilityFunctions::MurmurHash(FunctionContext* ctx, const T& input_val) {
  if (input_val.is_null) return BigIntVal::null();
  return BigIntVal(
      HashUtil::MurmurHash2_64(&input_val.val, sizeof(input_val.val),
        HashUtil::MURMUR_DEFAULT_SEED));
}

// Note that this only hashes the unscaled value and not the scale or precision, so this
// function is only valid when used over a single decimal type.
BigIntVal UtilityFunctions::MurmurHashDecimal(FunctionContext* ctx,
    const DecimalVal& input_val) {
  if (input_val.is_null) return BigIntVal::null();
  const FunctionContext::TypeDesc& input_type = *ctx->GetArgType(0);
  int byte_size = ColumnType::GetDecimalByteSize(input_type.precision);
  // val4, val8 and val16 all start at the same memory address.
  return BigIntVal(HashUtil::MurmurHash2_64(&input_val.val16, byte_size,
        HashUtil::MURMUR_DEFAULT_SEED));
}

template BigIntVal UtilityFunctions::MurmurHash(
    FunctionContext* ctx, const BooleanVal& input_val);
template BigIntVal UtilityFunctions::MurmurHash(
    FunctionContext* ctx, const TinyIntVal& input_val);
template BigIntVal UtilityFunctions::MurmurHash(
    FunctionContext* ctx, const SmallIntVal& input_val);
template BigIntVal UtilityFunctions::MurmurHash(
    FunctionContext* ctx, const IntVal& input_val);
template BigIntVal UtilityFunctions::MurmurHash(
    FunctionContext* ctx, const BigIntVal& input_val);
template BigIntVal UtilityFunctions::MurmurHash(
    FunctionContext* ctx, const FloatVal& input_val);
template BigIntVal UtilityFunctions::MurmurHash(
    FunctionContext* ctx, const DoubleVal& input_val);
template BigIntVal UtilityFunctions::MurmurHash(
    FunctionContext* ctx, const DateVal& input_val);

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

StringVal UtilityFunctions::Uuid(FunctionContext* ctx) {
  return GenUuid(ctx);
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

StringVal UtilityFunctions::CurrentSession(FunctionContext* ctx) {
  return AnyValUtil::FromString(ctx, PrintId(ctx->impl()->state()->session_id()));
}

StringVal UtilityFunctions::Coordinator(FunctionContext* ctx) {
  const TQueryCtx& query_ctx = ctx->impl()->state()->query_ctx();
  // An empty string indicates the coordinator was not set in the query request.
  return query_ctx.__isset.coord_hostname ?
      AnyValUtil::FromString(ctx, query_ctx.coord_hostname) :
      StringVal::null();
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
    // Show types parameterised by length.
    case TYPE_CHAR:
    case TYPE_VARCHAR:
    case TYPE_FIXED_UDA_INTERMEDIATE:
      return AnyValUtil::FromString(ctx, Substitute("$0($1)", type_string,
          type_desc.len));
    default:
      return AnyValUtil::FromString(ctx, type_string);
  }
}

StringVal UtilityFunctions::TypeOfBinary(
    FunctionContext* ctx, const StringVal& /*input_val*/) {
  return AnyValUtil::FromString(ctx, "BINARY");
}

template StringVal UtilityFunctions::TypeOf(
    FunctionContext* ctx, const BooleanVal& input_val);
template StringVal UtilityFunctions::TypeOf(
    FunctionContext* ctx, const TinyIntVal& input_val);
template StringVal UtilityFunctions::TypeOf(
    FunctionContext* ctx, const SmallIntVal& input_val);
template StringVal UtilityFunctions::TypeOf(
    FunctionContext* ctx, const IntVal& input_val);
template StringVal UtilityFunctions::TypeOf(
    FunctionContext* ctx, const BigIntVal& input_val);
template StringVal UtilityFunctions::TypeOf(
    FunctionContext* ctx, const FloatVal& input_val);
template StringVal UtilityFunctions::TypeOf(
    FunctionContext* ctx, const DoubleVal& input_val);
template StringVal UtilityFunctions::TypeOf(
    FunctionContext* ctx, const StringVal& input_val);
template StringVal UtilityFunctions::TypeOf(
    FunctionContext* ctx, const TimestampVal& input_val);
template StringVal UtilityFunctions::TypeOf(
    FunctionContext* ctx, const DecimalVal& input_val);
template StringVal UtilityFunctions::TypeOf(
    FunctionContext* ctx, const DateVal& input_val);

StringVal UtilityFunctions::Sha1(FunctionContext* ctx, const StringVal& input_str) {
  // Validate the input string.
  if (input_str.is_null) {
    return StringVal::null();
  }

  // SHA-1 is not supported for FIPS.
  if (IsFIPSMode()) {
    ctx->SetError("sha1 is not supported in FIPS mode.");
    return StringVal::null();
  } else {
    StringVal sha1_hash(ctx, SHA_DIGEST_LENGTH);
    if (UNLIKELY(sha1_hash.is_null)) return StringVal::null();
    SHA1(input_str.ptr, input_str.len, sha1_hash.ptr);
    return StringFunctions::Lower(ctx, MathFunctions::HexString(ctx, sha1_hash));
  }
}

StringVal UtilityFunctions::Sha2(FunctionContext* ctx, const StringVal& input_str,
    const IntVal& bit_len) {
  // Validate the input string.
  if (input_str.is_null || bit_len.is_null) {
    return StringVal::null();
  }

  // SHA-224 and SHA-256 are deprecated for FIPS mode.
  if (IsFIPSMode() && (bit_len.val == 224 || bit_len.val == 256)) {
    ctx->SetError("Only bit lengths 384 and 512 are supported in FIPS mode.");
    return StringVal::null();
  }

  StringVal sha_hash;

  switch(bit_len.val) {
    case 224:
      sha_hash = StringVal(ctx, SHA224_DIGEST_LENGTH);
      if (UNLIKELY(sha_hash.is_null)) return StringVal::null();
      SHA224(input_str.ptr, input_str.len, sha_hash.ptr);
      break;
    case 256:
      sha_hash = StringVal(ctx, SHA256_DIGEST_LENGTH);
      if (UNLIKELY(sha_hash.is_null)) return StringVal::null();
      SHA256(input_str.ptr, input_str.len, sha_hash.ptr);
      break;
    case 384:
      sha_hash = StringVal(ctx, SHA384_DIGEST_LENGTH);
      if (UNLIKELY(sha_hash.is_null)) return StringVal::null();
      SHA384(input_str.ptr, input_str.len, sha_hash.ptr);
      break;
    case 512:
      sha_hash = StringVal(ctx, SHA512_DIGEST_LENGTH);
      if (UNLIKELY(sha_hash.is_null)) return StringVal::null();
      SHA512(input_str.ptr, input_str.len, sha_hash.ptr);
      break;
    default:
      // Unsupported bit length.
      ctx->SetError(Substitute("Bit Length $0 is not supported", bit_len.val).c_str());
      return StringVal::null();
  }

  return StringFunctions::Lower(ctx, MathFunctions::HexString(ctx, sha_hash));
}

StringVal UtilityFunctions::Md5(FunctionContext* ctx, const StringVal& input_str) {
  // Validate the input string.
  if (input_str.is_null) {
    return StringVal::null();
  }
  if (UNLIKELY(IsFIPSMode())) {
    ctx->SetError("MD5 is not supported in FIPS mode.");
    return StringVal::null();
  } else {
    StringVal md5 = StringVal(ctx, MD5_DIGEST_LENGTH);
    if (UNLIKELY(md5.is_null)) return StringVal::null();
    MD5(input_str.ptr, input_str.len, md5.ptr);
    return StringFunctions::Lower(ctx, MathFunctions::HexString(ctx, md5));
  }
}
}
