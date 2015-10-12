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


#ifndef IMPALA_RUNTIME_DECIMAL_VALUE_H
#define IMPALA_RUNTIME_DECIMAL_VALUE_H

#include <math.h>
#include <iomanip>
#include <ostream>
#include <sstream>

#include "common/logging.h"
#include "runtime/multi-precision.h"
#include "util/decimal-util.h"
#include "util/hash-util.h"

namespace impala {

/// Implementation of decimal types. The type is parametrized on the underlying
/// storage type, which must implement all operators (example of a storage type
/// is int32_t). The decimal does not store its precision and scale since we'd
/// like to keep the storage as small as possible.
/// Overflow handling: anytime the value is assigned, we need to consider overflow.
/// Overflow is handled by an output return parameter. Functions should set this
/// to true if overflow occured and leave it *unchanged* otherwise (e.g. |= rather than =).
/// This allows the caller to not have to check overflow after every call.
template<typename T>
class DecimalValue {
 public:
  DecimalValue() : value_(0) { }
  DecimalValue(const T& s) : value_(s) { }

  DecimalValue& operator=(const T& s) {
    value_ = s;
    return *this;
  }

  /// Returns the closest Decimal to 'd' of type 't', truncating digits that
  /// cannot be represented.
  static DecimalValue FromDouble(const ColumnType& t, double d, bool* overflow) {
    return FromDouble(t.precision, t.scale, d, overflow);
  }

  static DecimalValue FromDouble(int precision, int scale, double d, bool* overflow) {
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

  /// Assigns *result as a decimal.
  static DecimalValue FromInt(const ColumnType& t, int64_t d, bool* overflow) {
    return FromInt(t.precision, t.scale, d, overflow);
  }

  static DecimalValue FromInt(int precision, int scale, int64_t d, bool* overflow) {
    // Check overflow. For scale 3, the max value is 10^3 - 1 = 999.
    T max_value = DecimalUtil::GetScaleMultiplier<T>(precision - scale);
    if (abs(d) >= max_value) {
      *overflow = true;
      return DecimalValue();
    }
    return DecimalValue(DecimalUtil::MultiplyByScale<T>(d, scale));
  }

  /// The overloaded operators assume that this and other have the same scale. They are
  /// more efficient than the comparison functions that handle differing scale and should
  /// be used if the scales are known to be the same.  (e.g. min(decimal_col) or order by
  /// decimal_col.
  bool operator==(const DecimalValue& other) const {
    return value_ == other.value_;
  }
  bool operator!=(const DecimalValue& other) const {
    return value_ != other.value_;
  }
  bool operator<=(const DecimalValue& other) const {
    return value_ <= other.value_;
  }
  bool operator<(const DecimalValue& other) const {
    return value_ < other.value_;
  }
  bool operator>=(const DecimalValue& other) const {
    return value_ >= other.value_;
  }
  bool operator>(const DecimalValue& other) const {
    return value_ > other.value_;
  }

  DecimalValue operator-() const {
    return DecimalValue(-value_);
  }

  bool is_negative() const { return value_ < 0; }

  /// Compares this and other. Returns 0 if equal, < 0 if this < other and > 0 if
  /// this > other.
  int Compare(const DecimalValue& other) const {
    T x = value();
    T y = other.value();
    if (x == y) return 0;
    if (x < y) return -1;
    return 1;
  }

  /// Returns a new decimal scaled by from src_type to dst_type.
  /// e.g. If this value was 1100 at scale 3 and the dst_type had scale two, the
  /// result would be 110. (In both cases representing the decimal 1.1).
  DecimalValue ScaleTo(const ColumnType& src_type, const ColumnType& dst_type,
      bool* overflow) const {
    return ScaleTo(src_type.scale, dst_type.scale, dst_type.precision, overflow);
  }

  DecimalValue ScaleTo(int src_scale, int dst_scale, int dst_precision, bool* overflow)
      const {
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

  /// Implementations of the basic arithmetic operators. In all these functions,
  /// we take the precision and scale of both inputs. The return type is assumed
  /// to be known by the caller (generated by the planner).
  /// Although these functions accept the result scale, that should be seen as
  /// an optimization to avoid having to recompute it in the function. The
  /// functions implement the SQL decimal rules *only* so other result scales are
  /// not valid.
  /// RESULT_T needs to be larger than T to avoid overflow issues.
  template<typename RESULT_T>
  DecimalValue<RESULT_T> Add(const ColumnType& this_type, const DecimalValue& other,
      const ColumnType& other_type, int result_scale, bool* overflow) const {
    return Add<RESULT_T>(
        this_type.scale, other, other_type.scale, result_scale, overflow);
  }

  template<typename RESULT_T>
  DecimalValue<RESULT_T> Add(int this_scale, const DecimalValue& other, int other_scale,
      int result_scale, bool* overflow) const {
    DCHECK_EQ(result_scale, std::max(this_scale, other_scale));
    RESULT_T x = 0;
    RESULT_T y = 0;
    *overflow |= AdjustToSameScale(*this, this_scale, other, other_scale, &x, &y);
    if (sizeof(RESULT_T) == 16) {
      // Check overflow.
      if (!*overflow && is_negative() == other.is_negative()) {
        // Can only overflow if the signs are the same
        *overflow |= DecimalUtil::MAX_UNSCALED_DECIMAL - abs(x) < abs(y);
        // TODO: faster to return here? We don't care at all about the perf on
        // the overflow case but what makes the normal path faster?
      }
    } else {
      DCHECK(!*overflow) << "Cannot overflow unless result is Decimal16Value";
    }
    return DecimalValue<RESULT_T>(x + y);
  }

  template<typename RESULT_T>
  DecimalValue<RESULT_T> Subtract(const ColumnType& this_type, const DecimalValue& other,
      const ColumnType& other_type, int result_scale, bool* overflow) const {
    return Add<RESULT_T>(this_type, -other, other_type, result_scale, overflow);
  }

  template<typename RESULT_T>
  DecimalValue<RESULT_T> Subtract(int this_scale, const DecimalValue& other,
      int other_scale, int result_scale, bool* overflow) const {
    return Add<RESULT_T>(this_scale, -other, other_scale, result_scale, overflow);
  }

  template<typename RESULT_T>
  DecimalValue<RESULT_T> Multiply(const ColumnType& this_type, const DecimalValue& other,
      const ColumnType& other_type, int result_scale, bool* overflow) const {
    return Multiply<RESULT_T>(
        this_type.scale, other, other_type.scale, result_scale, overflow);
  }

  template<typename RESULT_T>
  DecimalValue<RESULT_T> Multiply(int this_scale, const DecimalValue& other,
      int other_scale, int result_scale, bool* overflow) const {
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
    if (sizeof(RESULT_T) == 16) {
      // Check overflow
      *overflow |= DecimalUtil::MAX_UNSCALED_DECIMAL / abs(y) < abs(x);
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

  /// is_nan is set to true if 'other' is 0. The value returned is undefined.
  template<typename RESULT_T>
  DecimalValue<RESULT_T> Divide(const ColumnType& this_type, const DecimalValue& other,
      const ColumnType& other_type, int result_scale, bool* is_nan, bool* overflow)
      const {
    return Divide<RESULT_T>(
        this_type.scale, other, other_type.scale, result_scale, is_nan, overflow);
  }

  template<typename RESULT_T>
  DecimalValue<RESULT_T> Divide(int this_scale, const DecimalValue& other,
      int other_scale, int result_scale, bool* is_nan, bool* overflow) const {
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
      int128_t r = ConvertToInt128(x / y, DecimalUtil::MAX_UNSCALED_DECIMAL, overflow);
      return DecimalValue<RESULT_T>(r);
    } else {
      int128_t x = DecimalUtil::MultiplyByScale<RESULT_T>(value(), scale_by);
      int128_t y = other.value();
      int128_t r = x / y;
      return DecimalValue<RESULT_T>(static_cast<RESULT_T>(r));
    }
  }

  /// is_nan is set to true if 'other' is 0. The value returned is undefined.
  template<typename RESULT_T>
  DecimalValue<RESULT_T> Mod(const ColumnType& this_type, const DecimalValue& other,
      const ColumnType& other_type, int result_scale, bool* is_nan, bool* overflow)
      const {
    return Mod<RESULT_T>(
        this_type.scale, other, other_type.scale, result_scale, is_nan, overflow);
  }

  template<typename RESULT_T>
  DecimalValue<RESULT_T> Mod(int this_scale, const DecimalValue& other, int other_scale,
      int result_scale, bool* is_nan, bool* overflow) const {
    DCHECK_EQ(result_scale, std::max(this_scale, other_scale));
    if (other.value() == 0) {
      // Mod by 0.
      *is_nan = true;
      return DecimalValue<RESULT_T>();
    }
    *is_nan = false;
    RESULT_T x = 0;
    RESULT_T y = 1; // Initialize y to avoid mod by 0.
    *overflow |= AdjustToSameScale(*this, this_scale, other, other_scale, &x, &y);
    return DecimalValue<RESULT_T>(x % y);
  }

  /// Compares this and other. Returns 0 if equal, < 0 if this < other and > 0 if
  /// this > other.
  int Compare(int this_scale, const DecimalValue& other, int other_scale) const;

  /// Comparison utilities.
  bool Eq(int this_scale, const DecimalValue& other, int other_scale) const {
    return Compare(this_scale, other, other_scale) == 0;
  }
  bool Ne(int this_scale, const DecimalValue& other, int other_scale) const {
    return Compare(this_scale, other, other_scale) != 0;
  }
  bool Ge(int this_scale, const DecimalValue& other, int other_scale) const {
    return Compare(this_scale, other, other_scale) >= 0;
  }
  bool Gt(int this_scale, const DecimalValue& other, int other_scale) const {
    return Compare(this_scale, other, other_scale) > 0;
  }
  bool Le(int this_scale, const DecimalValue& other, int other_scale) const {
    return Compare(this_scale, other, other_scale) <= 0;
  }
  bool Lt(int this_scale, const DecimalValue& other, int other_scale) const {
    return Compare(this_scale, other, other_scale) < 0;
  }

  /// Returns the underlying storage. For a particular storage size, there is
  /// only one representation for any decimal and the storage is directly comparable.
  const T& value() const { return value_; }
  T& value() { return value_; }

  /// Returns the value of the decimal before the decimal point.
  const T whole_part(const ColumnType& t) const {
    return whole_part(t.scale);
  }
  const T whole_part(int scale) const {
    return value() / DecimalUtil::GetScaleMultiplier<T>(scale);
  }

  /// Returns the value of the decimal after the decimal point.
  const T fractional_part(const ColumnType& t) const {
    return fractional_part(t.scale);
  }
  const T fractional_part(int scale) const {
    return abs(value()) % DecimalUtil::GetScaleMultiplier<T>(scale);
  }

  /// Returns an approximate double for this decimal.
  double ToDouble(const ColumnType& type) const {
    return ToDouble(type.scale);
  }
  double ToDouble(int scale) const {
    return static_cast<double>(value_) / powf(10.0, scale);
  }

  inline uint32_t Hash(int seed = 0) const {
    return HashUtil::Hash(&value_, sizeof(value_), seed);
  }

  std::string ToString(const ColumnType& type) const;
  std::string ToString(int precision, int scale) const;

  DecimalValue<T> Abs() const { return DecimalValue<T>(abs(value_)); }

 private:
  T value_;

  /// Returns in *x_val and *y_val, the adjusted values so that both
  /// are at the same scale. The scale is the number of digits after the decimal.
  /// Returns true if the adjusted causes overflow in which case the values in
  /// x_scaled and y_scaled are unmodified.
  template <typename RESULT_T>
  static bool AdjustToSameScale(const DecimalValue& x, int x_scale, const DecimalValue& y,
      int y_scale, RESULT_T* x_scaled, RESULT_T* y_scaled) {
    int delta_scale = x_scale - y_scale;
    RESULT_T scale_factor = DecimalUtil::GetScaleMultiplier<RESULT_T>(abs(delta_scale));
    if (delta_scale == 0) {
      *x_scaled = x.value();
      *y_scaled = y.value();
    } else if (delta_scale > 0) {
      if (sizeof(RESULT_T) == 16 &&
          DecimalUtil::MAX_UNSCALED_DECIMAL / scale_factor < abs(y.value())) {
        return true;
      }
      *x_scaled = x.value();
      *y_scaled = y.value() * scale_factor;
    } else {
      if (sizeof(RESULT_T) == 16 &&
          DecimalUtil::MAX_UNSCALED_DECIMAL / scale_factor < abs(x.value())) {
        return true;
      }
      *x_scaled = x.value() * scale_factor;
      *y_scaled = y.value();
    }
    return false;
  }
};

typedef DecimalValue<int32_t> Decimal4Value;
typedef DecimalValue<int64_t> Decimal8Value;
/// TODO: should we support Decimal12Value? We pad it to 16 bytes in the tuple
/// anyway.
typedef DecimalValue<int128_t> Decimal16Value;

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

inline std::ostream& operator<<(std::ostream& os, const Decimal4Value& d) {
  return os << d.value();
}
inline std::ostream& operator<<(std::ostream& os, const Decimal8Value& d) {
  return os << d.value();
}
inline std::ostream& operator<<(std::ostream& os, const Decimal16Value& d) {
  return os << d.value();
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

/// For comparisons, we need the intermediate to be at the next precision
/// to avoid overflows.
/// TODO: is there a more efficient way to do this?
template <>
inline int Decimal4Value::Compare(int this_scale, const Decimal4Value& other,
    int other_scale) const {
  int64_t x, y;
  bool overflow = AdjustToSameScale(*this, this_scale, other, other_scale, &x, &y);
  DCHECK(!overflow) << "Overflow cannot happen with Decimal4Value";
  if (x == y) return 0;
  if (x < y) return -1;
  return 1;
}
template <>
inline int Decimal8Value::Compare(int this_scale, const Decimal8Value& other,
    int other_scale) const {
  int128_t x = 0, y = 0;
  bool overflow = AdjustToSameScale(*this, this_scale, other, other_scale, &x, &y);
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

}

#endif
