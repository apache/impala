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

#ifndef IMPALA_RUNTIME_DECIMAL_VALUE_INLINE_H
#define IMPALA_RUNTIME_DECIMAL_VALUE_INLINE_H

#include "runtime/decimal-value.h"

#include <cmath>
#include <iomanip>
#include <limits>
#include <ostream>
#include <sstream>

#include "common/logging.h"
#include "util/bit-util.h"
#include "util/decimal-util.h"
#include "util/hash-util.h"

namespace impala {

template<typename T>
inline DecimalValue<T> DecimalValue<T>::FromDouble(int precision, int scale, double d,
    bool round, bool* overflow) {

  // Multiply the double by the scale.
  // Unfortunately, this conversion is not exact, and there is a loss of precision.
  // The error starts around 1.0e23 and can take either positive or negative values.
  // This means the multiplication can cause an unwanted decimal overflow.
  d *= DecimalUtil::GetScaleMultiplier<double>(scale);

  // Decimal V2 behavior
  // TODO: IMPALA-4924: remove DECIMAL V1 code
  if (round) d = std::round(d);

  const T max_value = DecimalUtil::GetScaleMultiplier<T>(precision);
  DCHECK(max_value > 0);  // no DCHECK_GT because of int128_t
  if (UNLIKELY(std::isnan(d)) || UNLIKELY(std::fabs(d) >= max_value)) {
    *overflow = true;
    return DecimalValue();
  }

  // Return the rounded or truncated integer part.
  return DecimalValue(static_cast<T>(d));
}

template<typename T>
inline DecimalValue<T> DecimalValue<T>::FromInt(int precision, int scale, int64_t d,
    bool* overflow) {
  // Check overflow. For scale 3, the max value is 10^3 - 1 = 999.
  T max_value = DecimalUtil::GetScaleMultiplier<T>(precision - scale);
  if (abs(d) >= max_value) {
    *overflow = true;
    return DecimalValue();
  }
  return DecimalValue(DecimalUtil::MultiplyByScale<T>(d, scale));
}

template<typename T>
inline int DecimalValue<T>::Compare(const DecimalValue& other) const {
  T x = value();
  T y = other.value();
  if (x == y) return 0;
  if (x < y) return -1;
  return 1;
}

template<typename T>
inline const T DecimalValue<T>::whole_part(int scale) const {
  return value() / DecimalUtil::GetScaleMultiplier<T>(scale);
}

template<typename T>
inline const T DecimalValue<T>::fractional_part(int scale) const {
  return abs(value()) % DecimalUtil::GetScaleMultiplier<T>(scale);
}

// Note: this expects RESULT_T to be a UDF AnyVal subclass which defines
// RESULT_T::underlying_type_t to be the representative type
template<typename T>
template<typename RESULT_T>
inline typename RESULT_T::underlying_type_t DecimalValue<T>::ToInt(int scale,
    bool* overflow) const {
  const T divisor = DecimalUtil::GetScaleMultiplier<T>(scale);
  const T v = value();
  T result;
  if (divisor == 1) {
    result = v;
  } else {
    result = v / divisor;
    const T remainder = v % divisor;
    // Divisor is always a multiple of 2, so no loss of precision when shifting down
    DCHECK(divisor % 2 == 0);  // No DCHECK_EQ as this is possibly an int128_t
    // N.B. also - no std::abs for int128_t
    if (abs(remainder) >= divisor >> 1) {
      // Round away from zero.
      // Bias at zero must be corrected by sign of dividend.
      result += BitUtil::Sign(v);
    }
  }
  *overflow |=
      result > std::numeric_limits<typename RESULT_T::underlying_type_t>::max() ||
      result < std::numeric_limits<typename RESULT_T::underlying_type_t>::min();
  return result;
}

template<typename T>
inline DecimalValue<T> DecimalValue<T>::ScaleTo(int src_scale, int dst_scale,
    int dst_precision, bool* overflow) const {
  int delta_scale = src_scale - dst_scale;
  T result = value();
  T max_value = DecimalUtil::GetScaleMultiplier<T>(dst_precision);
  if (delta_scale >= 0) {
    if (delta_scale != 0) result /= DecimalUtil::GetScaleMultiplier<T>(delta_scale);
    // Even if we are decreasing the absolute unscaled value, we can still overflow.
    // This path is also used to convert between precisions so for example, converting
    // from 100 as decimal(3,0) to decimal(2,0) should be considered an overflow.
    *overflow |= abs(result) >= max_value;
  } else if (delta_scale < 0) {
    T mult = DecimalUtil::GetScaleMultiplier<T>(-delta_scale);
    *overflow |= abs(result) >= max_value / mult;
    result *= mult;
  }
  return DecimalValue(result);
}

// Use __builtin_add_overflow on GCC if available.
// Avoid using on Clang: it regresses performance.
#if 5 <= __GNUC__
template<typename T>
template<typename RESULT_T>
inline DecimalValue<RESULT_T> DecimalValue<T>::Add(int this_scale,
    const DecimalValue& other, int other_scale, int result_precision, int result_scale,
    bool round, bool* overflow) const {
  DCHECK_EQ(result_scale, std::max(this_scale, other_scale));
  RESULT_T x = 0;
  RESULT_T y = 0;
  *overflow |= AdjustToSameScale(*this, this_scale, other, other_scale,
      result_precision, &x, &y);
  if (result_precision == ColumnType::MAX_PRECISION) {
    DCHECK_EQ(sizeof(RESULT_T), 16);
    RESULT_T result = 0;
    *overflow |= __builtin_add_overflow(x, y, &result);
    *overflow |= abs(result) > DecimalUtil::MAX_UNSCALED_DECIMAL16;
    return DecimalValue<RESULT_T>(result);
  } else {
    DCHECK(!*overflow) << "Cannot overflow unless result is Decimal16Value";
  }
  return DecimalValue<RESULT_T>(x + y);
}
#else
template<typename T>
template<typename RESULT_T>
inline DecimalValue<RESULT_T> DecimalValue<T>::Add(int this_scale,
    const DecimalValue& other, int other_scale, int result_precision, int result_scale,
    bool round, bool* overflow) const {
  DCHECK_EQ(result_scale, std::max(this_scale, other_scale));
  RESULT_T x = 0;
  RESULT_T y = 0;
  *overflow |= AdjustToSameScale(*this, this_scale, other, other_scale,
      result_precision, &x, &y);
  if (sizeof(RESULT_T) == 16) {
    // Check overflow.
    if (!*overflow && is_negative() == other.is_negative() &&
        result_precision == ColumnType::MAX_PRECISION) {
      // Can only overflow if the signs are the same and result precision reaches
      // max precision.
      *overflow |= DecimalUtil::MAX_UNSCALED_DECIMAL16 - abs(x) < abs(y);
      // TODO: faster to return here? We don't care at all about the perf on
      // the overflow case but what makes the normal path faster?
    }
  } else {
    DCHECK(!*overflow) << "Cannot overflow unless result is Decimal16Value";
  }
  return DecimalValue<RESULT_T>(x + y);
}
#endif

namespace detail {

// Helper function to scale down over multiplied values back into result type,
// truncating if round is false or rounding otherwise.
template<typename T, typename RESULT_T>
inline RESULT_T ScaleDownAndRound(RESULT_T value, int delta_scale, bool round) {
  DCHECK_GT(delta_scale, 0);
  // Multiplier can always be computed in potentially smaller type T
  T multiplier = DecimalUtil::GetScaleMultiplier<T>(delta_scale);
  DCHECK(multiplier > 1 && multiplier % 2 == 0);
  RESULT_T result = value / multiplier;
  if (round) {
    RESULT_T remainder = value % multiplier;
    // In general, shifting down the multiplier is not safe, but we know
    // here that it is a multiple of two.
    if (abs(remainder) >= (multiplier >> 1)) {
      // Bias at zero must be corrected by sign of dividend.
      result += BitUtil::Sign(value);
    }
  }
  return result;
}
}

template<typename T>
template<typename RESULT_T>
DecimalValue<RESULT_T> DecimalValue<T>::Multiply(int this_scale,
    const DecimalValue& other, int other_scale, int result_precision, int result_scale,
    bool round, bool* overflow) const {
  // In the non-overflow case, we don't need to adjust by the scale since
  // that is already handled by the FE when it computes the result decimal type.
  // e.g. 1.23 * .2 (scale 2, scale 1 respectively) is identical to:
  // 123 * 2 with a resulting scale 3. We can do the multiply on the unscaled values.
  // The result scale in this case is the sum of the input scales.
  RESULT_T x = value();
  RESULT_T y = other.value();
  if (x == 0 || y == 0) {
    // Handle zero to avoid divide by zero in the overflow check below.
    return DecimalValue<RESULT_T>(0);
  }
  RESULT_T result = 0;
  bool needs_int256 = false;
  int delta_scale = this_scale + other_scale - result_scale;
  if (result_precision == ColumnType::MAX_PRECISION) {
    DCHECK_EQ(sizeof(RESULT_T), 16);
    int total_leading_zeros = BitUtil::CountLeadingZeros(abs(x)) +
        BitUtil::CountLeadingZeros(abs(y));
    // This check is quick, but conservative. In some cases it will indicate that
    // converting to 256 bits is necessary, when it's not actually the case.
    needs_int256 = total_leading_zeros <= 128;
    if (UNLIKELY(needs_int256 && delta_scale == 0)) {
      if (LIKELY(abs(x) > DecimalUtil::MAX_UNSCALED_DECIMAL16 / abs(y))) {
        // If the intermediate value does not fit into 128 bits, we indicate overflow
        // because the final value would also not fit into 128 bits since delta_scale is
        // zero.
        *overflow = true;
      } else {
        // We've verified that the intermediate (and final) value will fit into 128 bits.
        needs_int256 = false;
      }
    }
  }
  if (UNLIKELY(needs_int256)) {
    if (delta_scale == 0) {
      DCHECK(*overflow);
    } else {
      int256_t intermediate_result = ConvertToInt256(x) * ConvertToInt256(y);
      intermediate_result = detail::ScaleDownAndRound<int256_t, int256_t>(
          intermediate_result, delta_scale, round);
      result = ConvertToInt128(
          intermediate_result, DecimalUtil::MAX_UNSCALED_DECIMAL16, overflow);
    }
  } else {
    if (delta_scale == 0) {
      result = x * y;
      if (UNLIKELY(result_precision == ColumnType::MAX_PRECISION &&
          abs(result) > DecimalUtil::MAX_UNSCALED_DECIMAL16)) {
        // An overflow is possible here, if, for example, x = (2^64 - 1) and
        // y = (2^63 - 1).
        *overflow = true;
      }
    } else if (LIKELY(delta_scale <= 38)) {
      result = x * y;
      // The largest value that result can have here is (2^64 - 1) * (2^63 - 1), which is
      // greater than MAX_UNSCALED_DECIMAL16.
      result = detail::ScaleDownAndRound<T, RESULT_T>(result, delta_scale, round);
      // Since delta_scale is greater than zero, result can now be at most
      // ((2^64 - 1) * (2^63 - 1)) / 10, which is less than MAX_UNSCALED_DECIMAL16, so
      // there is no need to check for overflow.
    } else {
      // We are multiplying decimal(38, 38) by decimal(38, 38). The result should be a
      // decimal(38, 37), so delta scale = 38 + 38 - 37 = 39. Since we are not in the
      // 256 bit intermediate value case and we are scaling down by 39, then we are
      // guaranteed that the result is 0 (even if we try to round). The largest possible
      // intermediate result is 38 "9"s. If we scale down by 39, the leftmost 9 is now
      // two digits to the right of the rightmost "visible" one. The reason why we have
      // to handle this case separately is because a scale multiplier with a delta_scale
      // 39 does not fit into int128.
      DCHECK_EQ(delta_scale, 39);
      DCHECK(round);
      result = 0;
    }
  }
  DCHECK(*overflow || abs(result) <= DecimalUtil::MAX_UNSCALED_DECIMAL16);
  return DecimalValue<RESULT_T>(result);
}

template<typename T>
template<typename RESULT_T>
inline DecimalValue<RESULT_T> DecimalValue<T>::Divide(int this_scale,
    const DecimalValue& other, int other_scale, int result_precision, int result_scale,
    bool round, bool* is_nan, bool* overflow) const {
  DCHECK_GE(result_scale + other_scale, this_scale);
  if (other.value() == 0) {
    // Divide by 0.
    *is_nan = true;
    return DecimalValue<RESULT_T>();
  }
  // We need to scale x up by the result scale and then do an integer divide.
  // This truncates the result to the output scale.
  int scale_by = result_scale + other_scale - this_scale;
  // Use higher precision ints for intermediates to avoid overflows. Divides lead to
  // large numbers very quickly (and get eliminated by the int divide).
  if (sizeof(T) == 16) {
    int128_t x_sp = value();
    int256_t x = DecimalUtil::MultiplyByScale<int256_t>(ConvertToInt256(x_sp), scale_by);
    int128_t y_sp = other.value();
    int256_t y = ConvertToInt256(y_sp);
    int128_t r = ConvertToInt128(x / y, DecimalUtil::MAX_UNSCALED_DECIMAL16, overflow);
    if (round) {
      int256_t remainder = x % y;
      // The following is frought with apparent difficulty, as there is only 1 bit
      // free in the implementation of int128_t representing our maximum value and
      // doubling such a value would overflow in two's complement.  However, we
      // converted y to a 256 bit value, and remainder must be less than y, so there
      // is plenty of space.  Building a value to DCHECK for this is rather awkward, but
      // quite obviously 2 * MAX_UNSCALED_DECIMAL16 has plenty of room in 256 bits.
      // This will need to be fixed if we optimize to get back a 128-bit signed value.
      if (abs(2 * remainder) >= abs(y)) {
        // Bias at zero must be corrected by sign of divisor and dividend.
        r += (BitUtil::Sign(x_sp) ^ BitUtil::Sign(y_sp)) + 1;
      }
    }
    // Check overflow again after rounding since +/-1 could cause decimal overflow
    if (result_precision == ColumnType::MAX_PRECISION) {
      *overflow |= abs(r) > DecimalUtil::MAX_UNSCALED_DECIMAL16;
    }
    return DecimalValue<RESULT_T>(r);
  } else {
    DCHECK(DecimalUtil::GetScaleMultiplier<RESULT_T>(scale_by) > 0);
    int128_t x = DecimalUtil::MultiplyByScale<RESULT_T>(value(), scale_by);
    int128_t y = other.value();
    int128_t r = x / y;
    if (round) {
      int128_t remainder = x % y;
      // No overflow because doubling the result of 8-byte integers fits in 128 bits
      DCHECK_LT(sizeof(T), sizeof(remainder));
      if (abs(2 * remainder) >= abs(y)) {
        // No bias at zero.  The result scale was chosen such that the smallest non-zero
        // 'x' divided by the largest 'y' will always produce a non-zero result.
        // If higher precision were required due to a very large scale, we would be
        // computing in 256 bits, where getting a zero result is actually a posibility.
        // In addition, we know the dividend is non-zero, since there was a remainder.
        // The two conditions combined mean that the result must also be non-zero.
        DCHECK(r != 0);
        r += BitUtil::Sign(r);
      }
    }
    DCHECK(abs(r) <= DecimalUtil::MAX_UNSCALED_DECIMAL16 &&
        (sizeof(RESULT_T) > 8 || abs(r) <= DecimalUtil::MAX_UNSCALED_DECIMAL8) &&
        (sizeof(RESULT_T) > 4 || abs(r) <= DecimalUtil::MAX_UNSCALED_DECIMAL4));
    return DecimalValue<RESULT_T>(static_cast<RESULT_T>(r));
  }
}

template<typename T>
template<typename RESULT_T>
inline DecimalValue<RESULT_T> DecimalValue<T>::Mod(int this_scale,
    const DecimalValue& other, int other_scale, int result_precision, int result_scale,
    bool round, bool* is_nan, bool* overflow) const {
  DCHECK_EQ(result_scale, std::max(this_scale, other_scale));
  if (other.value() == 0) {
    // Mod by 0.
    *is_nan = true;
    return DecimalValue<RESULT_T>();
  }
  *is_nan = false;
  RESULT_T x = 0;
  RESULT_T y = 1; // Initialize y to avoid mod by 0.
  *overflow |= AdjustToSameScale(*this, this_scale, other, other_scale,
      result_precision, &x, &y);
  return DecimalValue<RESULT_T>(x % y);
}

template<typename T>
template <typename RESULT_T>
inline bool DecimalValue<T>::AdjustToSameScale(const DecimalValue<T>& x, int x_scale,
    const DecimalValue<T>& y, int y_scale, int result_precision, RESULT_T* x_scaled,
    RESULT_T* y_scaled) {
  int delta_scale = x_scale - y_scale;
  RESULT_T scale_factor = DecimalUtil::GetScaleMultiplier<RESULT_T>(abs(delta_scale));
  if (delta_scale == 0) {
    *x_scaled = x.value();
    *y_scaled = y.value();
  } else if (delta_scale > 0) {
    if (sizeof(RESULT_T) == 16 && result_precision == ColumnType::MAX_PRECISION &&
        DecimalUtil::GetScaleQuotient(delta_scale) < abs(y.value())) {
      return true;
    }
    *x_scaled = x.value();
    *y_scaled = y.value() * scale_factor;
  } else {
    if (sizeof(RESULT_T) == 16 && result_precision == ColumnType::MAX_PRECISION &&
        DecimalUtil::GetScaleQuotient(-delta_scale) < abs(x.value())) {
      return true;
    }
    *x_scaled = x.value() * scale_factor;
    *y_scaled = y.value();
  }
  return false;
}

/// For comparisons, we need the intermediate to be at the next precision
/// to avoid overflows.
/// TODO: is there a more efficient way to do this?
template <>
inline int Decimal4Value::Compare(int this_scale, const Decimal4Value& other,
    int other_scale) const {
  int64_t x, y;
  bool overflow = AdjustToSameScale(*this, this_scale, other, other_scale, 0, &x, &y);
  DCHECK(!overflow) << "Overflow cannot happen with Decimal4Value";
  if (x == y) return 0;
  if (x < y) return -1;
  return 1;
}

template <>
inline int Decimal8Value::Compare(int this_scale, const Decimal8Value& other,
    int other_scale) const {
  int128_t x = 0, y = 0;
  bool overflow = AdjustToSameScale(*this, this_scale, other, other_scale, 0, &x, &y);
  DCHECK(!overflow) << "Overflow cannot happen with Decimal8Value";
  if (x == y) return 0;
  if (x < y) return -1;
  return 1;
}

template <>
inline int Decimal16Value::Compare(int this_scale, const Decimal16Value& other,
     int other_scale) const {
  int256_t x = ConvertToInt256(this->value());
  int256_t y = ConvertToInt256(other.value());
  int delta_scale = this_scale - other_scale;
  if (delta_scale > 0) {
    y = DecimalUtil::MultiplyByScale<int256_t>(y, delta_scale);
  } else if (delta_scale < 0) {
    x = DecimalUtil::MultiplyByScale<int256_t>(x, -delta_scale);
  }
  if (x == y) return 0;
  if (x < y) return -1;
  return 1;
}

/// Returns as string with full 0 padding on the right and single 0 padded on the left
/// if the whole part is zero otherwise there will be no left padding.
template<typename T>
inline std::string DecimalValue<T>::ToString(const ColumnType& type) const {
  DCHECK_EQ(type.type, TYPE_DECIMAL);
  return ToString(type.precision, type.scale);
}

template<typename T>
inline std::string DecimalValue<T>::ToString(int precision, int scale) const {
  // Decimal values are sent to clients as strings so in the interest of
  // speed the string will be created without the using stringstream with the
  // whole/fractional_part().
  int last_char_idx = precision
      + (scale > 0)   // Add a space for decimal place
      + (scale == precision)   // Add a space for leading 0
      + (value_ < 0);   // Add a space for negative sign
  std::string str = std::string(last_char_idx, '0');
  // Start filling in the values in reverse order by taking the last digit
  // of the value. Use a positive value and worry about the sign later. At this
  // point the last_char_idx points to the string terminator.
  T remaining_value = value_;
  int first_digit_idx = 0;
  if (value_ < 0) {
    remaining_value = -value_;
    first_digit_idx = 1;
  }
  if (scale > 0) {
    int remaining_scale = scale;
    do {
      str[--last_char_idx] = (remaining_value % 10) + '0';   // Ascii offset
      remaining_value /= 10;
    } while (--remaining_scale > 0);
    str[--last_char_idx] = '.';
    DCHECK_GT(last_char_idx, first_digit_idx) << "Not enough space remaining";
  }
  do {
    str[--last_char_idx] = (remaining_value % 10) + '0';   // Ascii offset
    remaining_value /= 10;
    if (remaining_value == 0) {
      // Trim any extra leading 0's.
      if (last_char_idx > first_digit_idx) str.erase(0, last_char_idx - first_digit_idx);
      break;
    }
    // For safety, enforce string length independent of remaining_value.
  } while (last_char_idx > first_digit_idx);
  if (value_ < 0) str[0] = '-';
  return str;
}

template<typename T>
inline double DecimalValue<T>::ToDouble(int scale) const {
  return static_cast<double>(value_) / powf(10.0, scale);
}

template<typename T>
inline uint32_t DecimalValue<T>::Hash(int seed) const {
  return HashUtil::Hash(&value_, sizeof(value_), seed);
}

/// This function must be called 'hash_value' to be picked up by boost.
inline std::size_t hash_value(const Decimal4Value& v) {
  return v.Hash();
}
inline std::size_t hash_value(const Decimal8Value& v) {
  return v.Hash();
}
inline std::size_t hash_value(const Decimal16Value& v) {
  return v.Hash();
}

template<typename T>
inline DecimalValue<T> DecimalValue<T>::Abs() const {
  return DecimalValue<T>(abs(value_));
}

/// Conversions from different decimal types to one another. This does not
/// alter the scale. Checks for overflow. Although in some cases (going from Decimal4Value
/// to Decimal8Value) cannot overflow, the signature is the same to allow for templating.
inline Decimal4Value ToDecimal4(const Decimal4Value& v, bool* overflow) {
  return v;
}

inline Decimal8Value ToDecimal8(const Decimal4Value& v, bool* overflow) {
  return Decimal8Value(static_cast<int64_t>(v.value()));
}

inline Decimal16Value ToDecimal16(const Decimal4Value& v, bool* overflow) {
  return Decimal16Value(static_cast<int128_t>(v.value()));
}

inline Decimal4Value ToDecimal4(const Decimal8Value& v, bool* overflow) {
  *overflow |= abs(v.value()) > std::numeric_limits<int32_t>::max();
  return Decimal4Value(static_cast<int32_t>(v.value()));
}

inline Decimal8Value ToDecimal8(const Decimal8Value& v, bool* overflow) {
  return v;
}

inline Decimal16Value ToDecimal16(const Decimal8Value& v, bool* overflow) {
  return Decimal16Value(static_cast<int128_t>(v.value()));
}

inline Decimal4Value ToDecimal4(const Decimal16Value& v, bool* overflow) {
  *overflow |= abs(v.value()) > std::numeric_limits<int32_t>::max();
  return Decimal4Value(static_cast<int32_t>(v.value()));
}

inline Decimal8Value ToDecimal8(const Decimal16Value& v, bool* overflow) {
  *overflow |= abs(v.value()) > std::numeric_limits<int64_t>::max();
  return Decimal8Value(static_cast<int64_t>(v.value()));
}

inline Decimal16Value ToDecimal16(const Decimal16Value& v, bool* overflow) {
  return v;
}

}

#endif
