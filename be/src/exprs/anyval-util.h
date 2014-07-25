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


#ifndef IMPALA_EXPRS_ANYVAL_UTIL_H
#define IMPALA_EXPRS_ANYVAL_UTIL_H

#include "runtime/timestamp-value.h"
#include "udf/udf-internal.h"
#include "util/hash-util.h"

using namespace impala_udf;

namespace impala {

class ObjectPool;

// Utilities for AnyVals
class AnyValUtil {
 public:
  static uint32_t Hash(const BooleanVal& v, const FunctionContext::TypeDesc&, int seed) {
    return HashUtil::Hash(&v.val, 1, seed);
  }

  static uint32_t Hash(const TinyIntVal& v, const FunctionContext::TypeDesc&, int seed) {
    return HashUtil::Hash(&v.val, 1, seed);
  }

  static uint32_t Hash(const SmallIntVal& v, const FunctionContext::TypeDesc&, int seed) {
    return HashUtil::Hash(&v.val, 2, seed);
  }

  static uint32_t Hash(const IntVal& v, const FunctionContext::TypeDesc&, int seed) {
    return HashUtil::Hash(&v.val, 4, seed);
  }

  static uint32_t Hash(const BigIntVal& v, const FunctionContext::TypeDesc&, int seed) {
    return HashUtil::Hash(&v.val, 8, seed);
  }

  static uint32_t Hash(const FloatVal& v, const FunctionContext::TypeDesc&, int seed) {
    return HashUtil::Hash(&v.val, 4, seed);
  }

  static uint32_t Hash(const DoubleVal& v, const FunctionContext::TypeDesc&, int seed) {
    return HashUtil::Hash(&v.val, 8, seed);
  }

  static uint32_t Hash(const StringVal& v, const FunctionContext::TypeDesc&, int seed) {
    return HashUtil::Hash(v.ptr, v.len, seed);
  }

  static uint32_t Hash(const TimestampVal& v, const FunctionContext::TypeDesc&,
      int seed) {
    TimestampValue tv = TimestampValue::FromTimestampVal(v);
    return tv.Hash(seed);
  }

  static uint64_t Hash(const DecimalVal& v, const FunctionContext::TypeDesc& t,
      int64_t seed) {
    DCHECK_GT(t.precision, 0);
    switch (ColumnType::GetDecimalByteSize(t.precision)) {
      case 4: return HashUtil::Hash(&v.val4, 4, seed);
      case 8: return HashUtil::Hash(&v.val8, 8, seed);
      case 16: return HashUtil::Hash(&v.val16, 16, seed);
      default:
        DCHECK(false);
        return 0;
    }
  }

  static uint64_t Hash64(const BooleanVal& v, const FunctionContext::TypeDesc&,
      int64_t seed) {
    return HashUtil::FnvHash64(&v.val, 1, seed);
  }

  static uint64_t Hash64(const TinyIntVal& v, const FunctionContext::TypeDesc&,
      int64_t seed) {
    return HashUtil::FnvHash64(&v.val, 1, seed);
  }

  static uint64_t Hash64(const SmallIntVal& v, const FunctionContext::TypeDesc&,
      int64_t seed) {
    return HashUtil::FnvHash64(&v.val, 2, seed);
  }

  static uint64_t Hash64(const IntVal& v, const FunctionContext::TypeDesc&,
      int64_t seed) {
    return HashUtil::FnvHash64(&v.val, 4, seed);
  }

  static uint64_t Hash64(const BigIntVal& v, const FunctionContext::TypeDesc&,
      int64_t seed) {
    return HashUtil::FnvHash64(&v.val, 8, seed);
  }

  static uint64_t Hash64(const FloatVal& v, const FunctionContext::TypeDesc&,
      int64_t seed) {
    return HashUtil::FnvHash64(&v.val, 4, seed);
  }

  static uint64_t Hash64(const DoubleVal& v, const FunctionContext::TypeDesc&,
      int64_t seed) {
    return HashUtil::FnvHash64(&v.val, 8, seed);
  }

  static uint64_t Hash64(const StringVal& v, const FunctionContext::TypeDesc&,
      int64_t seed) {
    return HashUtil::FnvHash64(v.ptr, v.len, seed);
  }

  static uint64_t Hash64(const TimestampVal& v, const FunctionContext::TypeDesc&,
      int64_t seed) {
    TimestampValue tv = TimestampValue::FromTimestampVal(v);
    return HashUtil::FnvHash64(&tv, 12, seed);
  }

  static uint64_t Hash64(const DecimalVal& v, const FunctionContext::TypeDesc& t,
      int64_t seed) {
    switch (ColumnType::GetDecimalByteSize(t.precision)) {
      case 4: return HashUtil::FnvHash64(&v.val4, 4, seed);
      case 8: return HashUtil::FnvHash64(&v.val8, 8, seed);
      case 16: return HashUtil::FnvHash64(&v.val16, 16, seed);
      default:
        DCHECK(false);
        return 0;
    }
  }

  // Returns the byte size of *Val for type t.
  static int AnyValSize(const ColumnType& t) {
    switch (t.type) {
      case TYPE_BOOLEAN: return sizeof(BooleanVal);
      case TYPE_TINYINT: return sizeof(TinyIntVal);
      case TYPE_SMALLINT: return sizeof(SmallIntVal);
      case TYPE_INT: return sizeof(IntVal);
      case TYPE_BIGINT: return sizeof(BigIntVal);
      case TYPE_FLOAT: return sizeof(FloatVal);
      case TYPE_DOUBLE: return sizeof(DoubleVal);
      case TYPE_STRING: return sizeof(StringVal);
      case TYPE_TIMESTAMP: return sizeof(TimestampVal);
      case TYPE_DECIMAL: return sizeof(DecimalVal);
      default:
        DCHECK(false) << t;
        return 0;
    }
  }

  static StringVal FromString(FunctionContext* ctx, const std::string& s) {
    return FromBuffer(ctx, s.c_str(), s.size());
  }

  static StringVal FromBuffer(FunctionContext* ctx, const char* ptr, int len) {
    StringVal result(ctx, len);
    memcpy(result.ptr, ptr, len);
    return result;
  }

  static FunctionContext::TypeDesc ColumnTypeToTypeDesc(const ColumnType& type);

  static ColumnType TypeDescToColumnType(const FunctionContext::TypeDesc& type);
};

// Creates the corresponding AnyVal subclass for type. The object is added to the pool.
impala_udf::AnyVal* CreateAnyVal(ObjectPool* pool, const ColumnType& type);

}

#endif
