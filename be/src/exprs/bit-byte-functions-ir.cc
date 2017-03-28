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

#include "exprs/bit-byte-functions.h"

#include <boost/type_traits/make_unsigned.hpp>

#include "gutil/strings/substitute.h"

#include "util/bit-util.h"

#include "common/names.h"

using namespace impala_udf;

using boost::make_unsigned;

using impala::BitUtil;

namespace impala {

// Generic algorithm for shifting and rotating signed integers
// Declare here to resolve mutual recursion
template<typename T>
static T RotateLeftImpl(T v, int32_t shift);
template<typename T>
static T RotateRightImpl(T v, int32_t shift);
template<typename T>
static T ShiftLeftImpl(T v, int32_t shift);
template<typename T>
static T ShiftRightLogicalImpl(T v, int32_t shift);

template <typename T>
IntVal BitByteFunctions::CountSet(FunctionContext* ctx, const T& v) {
  if (v.is_null) return IntVal::null();
  return IntVal(BitUtil::PopcountSigned(v.val));
}

template IntVal BitByteFunctions::CountSet(FunctionContext*, const TinyIntVal&);
template IntVal BitByteFunctions::CountSet(FunctionContext*, const SmallIntVal&);
template IntVal BitByteFunctions::CountSet(FunctionContext*, const IntVal&);
template IntVal BitByteFunctions::CountSet(FunctionContext*, const BigIntVal&);

template <typename T>
IntVal BitByteFunctions::CountSet(FunctionContext* ctx, const T& v, const IntVal &bitval) {
  if (v.is_null || bitval.is_null) return IntVal::null();
  if (bitval.val == 0) {
    return IntVal(sizeof(v.val) * 8 - BitUtil::PopcountSigned(v.val));
  } else if (bitval.val == 1) {
    return IntVal(BitUtil::PopcountSigned(v.val));
  }

  ctx->SetError(Substitute("Invalid bit val: $0", bitval.val).c_str());
  return IntVal::null();
}

template IntVal BitByteFunctions::CountSet(FunctionContext*, const TinyIntVal&,
    const IntVal&);
template IntVal BitByteFunctions::CountSet(FunctionContext*, const SmallIntVal&,
    const IntVal&);
template IntVal BitByteFunctions::CountSet(FunctionContext*, const IntVal&,
    const IntVal&);
template IntVal BitByteFunctions::CountSet(FunctionContext*, const BigIntVal&,
    const IntVal&);

template<typename T>
TinyIntVal BitByteFunctions::GetBit(FunctionContext* ctx, const T& v,
    const IntVal& bitpos) {
  if (v.is_null || bitpos.is_null) return TinyIntVal::null();
  if (bitpos.val < 0 || bitpos.val >= sizeof(v.val) * 8) {
    ctx->SetError(Substitute("Invalid bit position: $0", bitpos.val).c_str());
    return TinyIntVal::null();
  }
  return TinyIntVal(BitUtil::GetBit(v.val, bitpos.val));
}

template TinyIntVal BitByteFunctions::GetBit(FunctionContext*, const TinyIntVal&,
    const IntVal&);
template TinyIntVal BitByteFunctions::GetBit(FunctionContext*, const SmallIntVal&,
    const IntVal&);
template TinyIntVal BitByteFunctions::GetBit(FunctionContext*, const IntVal&,
    const IntVal&);
template TinyIntVal BitByteFunctions::GetBit(FunctionContext*, const BigIntVal&,
    const IntVal&);

template<typename T>
T BitByteFunctions::SetBit(FunctionContext* ctx, const T& v,
    const IntVal& bitpos) {
  if (v.is_null || bitpos.is_null) return T::null();
  if (bitpos.val < 0 || bitpos.val >= sizeof(v.val) * 8) {
    ctx->SetError(Substitute("Invalid bit position: $0", bitpos.val).c_str());
    return T::null();
  }
  return T(BitUtil::SetBit(v.val, bitpos.val));
}

template<typename T>
T BitByteFunctions::SetBit(FunctionContext* ctx, const T& v,
    const IntVal& bitpos, const IntVal& bitval) {
  if (v.is_null || bitpos.is_null || bitval.is_null) return T::null();
  if (bitpos.val < 0 || bitpos.val >= sizeof(v.val) * 8) {
    ctx->SetError(Substitute("Invalid bit position: $0", bitpos.val).c_str());
    return T::null();
  }
  if (bitval.val == 0) {
    return T(BitUtil::UnsetBit(v.val, bitpos.val));
  } else if (bitval.val == 1) {
    return T(BitUtil::SetBit(v.val, bitpos.val));
  }
  ctx->SetError(Substitute("Invalid bit val: $0", bitval.val).c_str());
  return T::null();
}

template TinyIntVal BitByteFunctions::SetBit(FunctionContext*, const TinyIntVal&,
    const IntVal&);
template SmallIntVal BitByteFunctions::SetBit(FunctionContext*, const SmallIntVal&,
    const IntVal&);
template IntVal BitByteFunctions::SetBit(FunctionContext*, const IntVal&, const IntVal&);
template BigIntVal BitByteFunctions::SetBit(FunctionContext*, const BigIntVal&,
    const IntVal&);
template TinyIntVal BitByteFunctions::SetBit(FunctionContext*, const TinyIntVal&,
    const IntVal&, const IntVal&);
template SmallIntVal BitByteFunctions::SetBit(FunctionContext*, const SmallIntVal&,
    const IntVal&, const IntVal&);
template IntVal BitByteFunctions::SetBit(FunctionContext*, const IntVal&, const IntVal&,
    const IntVal&);
template BigIntVal BitByteFunctions::SetBit(FunctionContext*, const BigIntVal&,
    const IntVal&, const IntVal&);

template<typename T>
static T RotateLeftImpl(T v, int32_t shift) {
  // Handle negative shifts
  if (shift < 0) return RotateRightImpl(v, -shift);

  // Handle wrapping around multiple times
  shift = shift % (sizeof(T) * 8);
  return static_cast<T>((static_cast<std::make_unsigned_t<T>>(v) << shift)
      | BitUtil::ShiftRightLogical(v, sizeof(T) * 8 - shift));
}

template<typename T>
static T RotateRightImpl(T v, int32_t shift) {
  // Handle negative shifts
  if (shift < 0) return RotateLeftImpl(v, -shift);

  // Handle wrapping around multiple times
  shift = shift % (sizeof(T) * 8);
  return static_cast<T>(BitUtil::ShiftRightLogical(v, shift)
      | (static_cast<std::make_unsigned_t<T>>(v) << (sizeof(T) * 8 - shift)));
}

template<typename T>
static T ShiftLeftImpl(T v, int32_t shift) {
  if (shift < 0) return ShiftRightLogicalImpl(v, -shift);
  return static_cast<T>(static_cast<std::make_unsigned_t<T>>(v) << shift);
}

// Logical right shift rather than arithmetic right shift
template<typename T>
static T ShiftRightLogicalImpl(T v, int32_t shift) {
  if (shift < 0) return ShiftLeftImpl(v, -shift);
  // Conversion to unsigned ensures most significant bits always filled with 0's
  return BitUtil::ShiftRightLogical(v, shift);
}

// Generates a shift/rotate function for Impala integer value type based  on the shift
// algorithm implemented by ALGO
#define SHIFT_FN(NAME, INPUT_TYPE, ALGO) \
  INPUT_TYPE BitByteFunctions::NAME(FunctionContext* ctx, const INPUT_TYPE& v, \
      const IntVal& shift) { \
    if (v.is_null || shift.is_null) return INPUT_TYPE::null(); \
    return INPUT_TYPE(ALGO(v.val, shift.val)); \
  }

SHIFT_FN(RotateLeft, TinyIntVal, RotateLeftImpl);
SHIFT_FN(RotateLeft, SmallIntVal, RotateLeftImpl);
SHIFT_FN(RotateLeft, IntVal, RotateLeftImpl);
SHIFT_FN(RotateLeft, BigIntVal, RotateLeftImpl);
SHIFT_FN(RotateRight, TinyIntVal, RotateRightImpl);
SHIFT_FN(RotateRight, SmallIntVal, RotateRightImpl);
SHIFT_FN(RotateRight, IntVal, RotateRightImpl);
SHIFT_FN(RotateRight, BigIntVal, RotateRightImpl);
SHIFT_FN(ShiftLeft, TinyIntVal, ShiftLeftImpl);
SHIFT_FN(ShiftLeft, SmallIntVal, ShiftLeftImpl);
SHIFT_FN(ShiftLeft, IntVal, ShiftLeftImpl);
SHIFT_FN(ShiftLeft, BigIntVal, ShiftLeftImpl);
SHIFT_FN(ShiftRight, TinyIntVal, ShiftRightLogicalImpl);
SHIFT_FN(ShiftRight, SmallIntVal, ShiftRightLogicalImpl);
SHIFT_FN(ShiftRight, IntVal, ShiftRightLogicalImpl);
SHIFT_FN(ShiftRight, BigIntVal, ShiftRightLogicalImpl);

}
