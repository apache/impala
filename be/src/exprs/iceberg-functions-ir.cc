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

#include "exprs/iceberg-functions.h"

#include <limits>
#include <vector>

#include "common/compiler-util.h"
#include "common/logging.h"
#include "runtime/timestamp-value.inline.h"
#include "thirdparty/murmurhash/MurmurHash3.h"
#include "udf/udf-internal.h"
#include "util/bit-util.h"

namespace impala {

using std::numeric_limits;

const std::string IcebergFunctions::INCORRECT_WIDTH_ERROR_MSG =
    "Width parameter should be greater than zero.";

const std::string IcebergFunctions::TRUNCATE_OVERFLOW_ERROR_MSG =
    "Truncate operation overflows for the given input.";

const unsigned IcebergFunctions::BUCKET_TRANSFORM_SEED = 0;

IntVal IcebergFunctions::TruncatePartitionTransform(FunctionContext* ctx,
    const IntVal& input, const IntVal& width) {
  return TruncatePartitionTransformNumericImpl(ctx, input, width);
}

BigIntVal IcebergFunctions::TruncatePartitionTransform(FunctionContext* ctx,
    const BigIntVal& input, const BigIntVal& width) {
  return TruncatePartitionTransformNumericImpl(ctx, input, width);
}

DecimalVal IcebergFunctions::TruncatePartitionTransform(FunctionContext* ctx,
    const DecimalVal& input, const IntVal& width) {
  return TruncatePartitionTransform(ctx, input, BigIntVal(width.val));
}

DecimalVal IcebergFunctions::TruncatePartitionTransform(FunctionContext* ctx,
    const DecimalVal& input, const BigIntVal& width) {
  if (!CheckInputsAndSetError(ctx, input, width)) return DecimalVal::null();
  int decimal_size = ctx->impl()->GetConstFnAttr(FunctionContextImpl::ARG_TYPE_SIZE, 0);
  return TruncateDecimal(ctx, input, width, decimal_size);
}

DecimalVal IcebergFunctions::TruncateDecimal(FunctionContext* ctx,
    const DecimalVal& input, const BigIntVal& width, int decimal_size) {
  switch (decimal_size) {
    case 4: {
      // If the input is negative and within int32 range while the width is bigger than
      // int32 max value than the expected would be to return the width. However, it
      // won't fit into an int32 so an overflow error is returned instead.
      if (UNLIKELY(input.val4 < 0 && width.val > std::numeric_limits<int32_t>::max())) {
        ctx->SetError(TRUNCATE_OVERFLOW_ERROR_MSG.c_str());
        return DecimalVal::null();
      }
      return TruncatePartitionTransformDecimalImpl<int32_t>(input.val4, width.val);
    }
    case 8: {
      return TruncatePartitionTransformDecimalImpl<int64_t>(input.val8, width.val);
    }
    case 16: {
      DecimalVal result = TruncatePartitionTransformDecimalImpl(input.val16, width.val);
      if (input.val16 < 0 && result.val16 > 0) {
        ctx->SetError(TRUNCATE_OVERFLOW_ERROR_MSG.c_str());
        return DecimalVal::null();
      }
      return result;
    }
    default:
      return DecimalVal::null();
  }
}

template<typename T>
DecimalVal IcebergFunctions::TruncatePartitionTransformDecimalImpl(const T& decimal_val,
    int64_t width) {
  DCHECK(width > 0);
  if (decimal_val > 0) return decimal_val - (decimal_val % width);
  return decimal_val - (((decimal_val % width) + width) % width);
}

StringVal IcebergFunctions::TruncatePartitionTransform(FunctionContext* ctx,
    const StringVal& input, const IntVal& width) {
  if (!CheckInputsAndSetError(ctx, input, width)) return StringVal::null();
  if (input.len <= width.val) return input;
  return StringVal::CopyFrom(ctx, input.ptr, width.val);
}

template<typename T, typename W>
T IcebergFunctions::TruncatePartitionTransformNumericImpl(FunctionContext* ctx,
    const T& input, const W& width) {
  if (!CheckInputsAndSetError(ctx, input, width)) return T::null();
  if (input.val >= 0) return input.val - (input.val % width.val);
  T result = input.val - (((input.val % width.val) + width.val) % width.val);
  if (UNLIKELY(result.val > 0)) {
    ctx->SetError(TRUNCATE_OVERFLOW_ERROR_MSG.c_str());
    return T::null();
  }
  return result;
}

IntVal IcebergFunctions::BucketPartitionTransform(FunctionContext* ctx,
    const IntVal& input, const IntVal& width) {
  return BucketPartitionTransformNumericImpl(ctx, input, width);
}

IntVal IcebergFunctions::BucketPartitionTransform(FunctionContext* ctx,
    const BigIntVal& input, const IntVal& width) {
  return BucketPartitionTransformNumericImpl(ctx, input, width);
}

IntVal IcebergFunctions::BucketPartitionTransform(FunctionContext* ctx,
    const DecimalVal& input, const IntVal& width) {
  if (!CheckInputsAndSetError(ctx, input, width)) return IntVal::null();
  int decimal_size = ctx->impl()->GetConstFnAttr(FunctionContextImpl::ARG_TYPE_SIZE, 0);
  return BucketDecimal(ctx, input, width, decimal_size);
}

IntVal IcebergFunctions::BucketDecimal(FunctionContext* ctx,
    const DecimalVal& input, const IntVal& width, int decimal_size) {
  DCHECK(!input.is_null || !width.is_null);
  // Iceberg converts the decimal input's unscaled value into a byte array and hash-es
  // that byte array instead of the underlying integer representation. We do the same
  // here to be compatible with Iceberg.
  std::string buffer;
  switch (decimal_size) {
    case 4: {
      buffer = BitUtil::IntToByteBuffer(input.val4);
      break;
    }
    case 8: {
      buffer = BitUtil::IntToByteBuffer(input.val8);
      break;
    }
    case 16: {
      buffer = BitUtil::IntToByteBuffer(input.val16);
      break;
    }
    default: {
      return IntVal::null();
    }
  }
  int hash_result;
  MurmurHash3_x86_32(buffer.data(), buffer.size(), BUCKET_TRANSFORM_SEED,
      &hash_result);
  return (hash_result & std::numeric_limits<int>::max()) % width.val;
}

IntVal IcebergFunctions::BucketPartitionTransform(FunctionContext* ctx,
    const StringVal& input, const IntVal& width) {
  if (!CheckInputsAndSetError(ctx, input, width)) return IntVal::null();
  int hash_result;
  MurmurHash3_x86_32(input.ptr, input.len, BUCKET_TRANSFORM_SEED, &hash_result);
  return (hash_result & std::numeric_limits<int>::max()) % width.val;
}

IntVal IcebergFunctions::BucketPartitionTransform(FunctionContext* ctx,
    const DateVal& input, const IntVal& width) {
  if (input.is_null) return IntVal::null();
  return BucketPartitionTransformNumericImpl(ctx, IntVal(input.val), width);
}

IntVal IcebergFunctions::BucketPartitionTransform(FunctionContext* ctx,
    const TimestampVal& input, const IntVal& width) {
  if (input.is_null) return IntVal::null();
  TimestampValue tv = TimestampValue::FromTimestampVal(input);
  // Iceberg stores timestamps in int64 so to be in line Impala's bucket partition
  // transform also uses an int64 representation.
  int64_t micros_since_epoch;
  tv.FloorUtcToUnixTimeMicros(&micros_since_epoch);
  return BucketPartitionTransformNumericImpl(ctx, BigIntVal(micros_since_epoch), width);
}

template<typename T, typename W>
IntVal IcebergFunctions::BucketPartitionTransformNumericImpl(FunctionContext* ctx,
    const T& input, const W& width) {
  if (!CheckInputsAndSetError(ctx, input, width)) return IntVal::null();
  // Iceberg's int based bucket partition transform converts the int input to long before
  // getting the hash value of it. We do the same to guarantee compatibility.
  long key = (long)input.val;
  int hash_result;
  MurmurHash3_x86_32(&key, sizeof(long), BUCKET_TRANSFORM_SEED, &hash_result);
  return (hash_result & std::numeric_limits<int>::max()) % width.val;
}

template<typename T, typename W>
bool IcebergFunctions::CheckInputsAndSetError(FunctionContext* ctx, const T& input,
      const W& width) {
  if (width.is_null || width.val <= 0) {
    ctx->SetError(INCORRECT_WIDTH_ERROR_MSG.c_str());
    return false;
  }
  if (input.is_null) return false;
  return true;
}

}
