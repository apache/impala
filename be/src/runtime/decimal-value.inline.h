// Copyright 2016 Cloudera Inc.
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

#ifndef IMPALA_RUNTIME_DECIMAL_VALUE_INLINE_H
#define IMPALA_RUNTIME_DECIMAL_VALUE_INLINE_H

#include "runtime/decimal-value.h"

#include <iomanip>
#include <ostream>
#include <sstream>

#include "common/logging.h"
#include "util/decimal-util.h"
#include "util/hash-util.h"

namespace impala {

template<typename T>
inline DecimalValue<T> DecimalValue<T>::FromDouble(int precision, int scale, double d,
    bool* overflow) {
  // Check overflow.
  T max_value = DecimalUtil::GetScaleMultiplier<T>(precision - scale);
  if (abs(d) >= max_value) {
    *overflow = true;
    return DecimalValue();
  }

  // Multiply the double by the scale.
  d *= DecimalUtil::GetScaleMultiplier<double>(scale);
  // Truncate and just take the integer part.
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

template<typename T>
inline DecimalValue<T> DecimalValue<T>::ScaleTo(int src_scale, int dst_scale,
    int dst_precision, bool* overflow) const { int delta_scale = src_scale - dst_scale;
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

#if 5 <= __GNUC__ || __has_builtin(__builtin_add_overflow)
template<typename T>
template<typename RESULT_T>
inline DecimalValue<RESULT_T> DecimalValue<T>::Add(int this_scale,
    const DecimalValue& other, int other_scale, int result_precision, int result_scale,
    bool* overflow) const {
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
    bool* overflow) const {
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

#if 5 <= __GNUC__ || __has_builtin(__builtin_mul_overflow)
template<typename T>
template<typename RESULT_T>
inline DecimalValue<RESULT_T> DecimalValue<T>::Multiply(int this_scale,
    const DecimalValue& other, int other_scale, int result_precision, int result_scale,
    bool* overflow) const {
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
  if (result_precision == ColumnType::MAX_PRECISION) {
    DCHECK_EQ(sizeof(RESULT_T), 16);
    // Check overflow
    *overflow |= __builtin_mul_overflow(x, y, &result);
    *overflow |= abs(result) > DecimalUtil::MAX_UNSCALED_DECIMAL16;
  } else {
    result = x * y;
  }
  int delta_scale = this_scale + other_scale - result_scale;
  if (UNLIKELY(delta_scale != 0)) {
    // In this case, the required resulting scale is larger than the max we support.
    // We cap the resulting scale to the max supported scale (e.g. truncate) in the FE.
    // TODO: we could also return NULL.
    DCHECK_GT(delta_scale, 0);
    result /= DecimalUtil::GetScaleMultiplier<T>(delta_scale);
  }
  return DecimalValue<RESULT_T>(result);
}
#else
template<typename T>
template<typename RESULT_T>
DecimalValue<RESULT_T> DecimalValue<T>::Multiply(int this_scale,
    const DecimalValue& other, int other_scale, int result_precision, int result_scale,
    bool* overflow) const {
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
  if (result_precision == ColumnType::MAX_PRECISION) {
    DCHECK_EQ(sizeof(RESULT_T), 16);
    // Check overflow
    *overflow |= DecimalUtil::MAX_UNSCALED_DECIMAL16 / abs(y) < abs(x);
  }
  RESULT_T result = x * y;
  int delta_scale = this_scale + other_scale - result_scale;
  if (UNLIKELY(delta_scale != 0)) {
    // In this case, the required resulting scale is larger than the max we support.
    // We cap the resulting scale to the max supported scale (e.g. truncate) in the FE.
    // TODO: we could also return NULL.
    DCHECK_GT(delta_scale, 0);
    result /= DecimalUtil::GetScaleMultiplier<T>(delta_scale);
  }
  return DecimalValue<RESULT_T>(result);
}
#endif

template<typename T>
template<typename RESULT_T>
inline DecimalValue<RESULT_T> DecimalValue<T>::Divide(int this_scale,
    const DecimalValue& other, int other_scale, int result_precision, int result_scale,
    bool* is_nan, bool* overflow) const {
  DCHECK_GE(result_scale, this_scale);
  if (other.value() == 0) {
    // Divide by 0.
    *is_nan = true;
    return DecimalValue<RESULT_T>();
  }
  // We need to scale x up by the result precision and then do an integer divide.
  // This truncates the result to the output precision.
  // TODO: confirm with standard that truncate is okay.
  int scale_by = result_scale + other_scale - this_scale;
  // Use higher precision ints for intermediates to avoid overflows. Divides lead to
  // large numbers very quickly (and get eliminated by the int divide).
  if (sizeof(T) == 16) {
    int256_t x = DecimalUtil::MultiplyByScale<int256_t>(
        ConvertToInt256(value()), scale_by);
    int256_t y = ConvertToInt256(other.value());
    int128_t r = ConvertToInt128(x / y, DecimalUtil::MAX_UNSCALED_DECIMAL16, overflow);
    return DecimalValue<RESULT_T>(r);
  } else {
    int128_t x = DecimalUtil::MultiplyByScale<RESULT_T>(value(), scale_by);
    int128_t y = other.value();
    int128_t r = x / y;
    return DecimalValue<RESULT_T>(static_cast<RESULT_T>(r));
  }
}

template<typename T>
template<typename RESULT_T>
inline DecimalValue<RESULT_T> DecimalValue<T>::Mod(int this_scale,
    const DecimalValue& other, int other_scale, int result_precision, int result_scale,
    bool* is_nan, bool* overflow) const {
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
