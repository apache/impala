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

#include "udf/udf.h"
#include "util/hash-util.h"

using namespace impala_udf;

namespace impala {

// Utilities for AnyVals
class AnyValUtil {
 public:
  static uint32_t Hash(const BooleanVal& v, int seed) {
    return HashUtil::Hash(&v.val, 1, seed);
  }

  static uint32_t Hash(const TinyIntVal& v, int seed) {
    return HashUtil::Hash(&v.val, 1, seed);
  }

  static uint32_t Hash(const SmallIntVal& v, int seed) {
    return HashUtil::Hash(&v.val, 2, seed);
  }

  static uint32_t Hash(const IntVal& v, int seed) {
    return HashUtil::Hash(&v.val, 4, seed);
  }

  static uint32_t Hash(const BigIntVal& v, int seed) {
    return HashUtil::Hash(&v.val, 8, seed);
  }

  static uint32_t Hash(const FloatVal& v, int seed) {
    return HashUtil::Hash(&v.val, 4, seed);
  }

  static uint32_t Hash(const DoubleVal& v, int seed) {
    return HashUtil::Hash(&v.val, 8, seed);
  }

  static uint32_t Hash(const StringVal& v, int seed) {
    return HashUtil::Hash(v.ptr, v.len, seed);
  }

  static uint32_t Hash(const TimestampVal& v, int seed) {
    TimestampValue tv = TimestampValue::FromTimestampVal(v);
    return tv.Hash(seed);
  }
};

}

#endif
