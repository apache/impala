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


#ifndef IMPALA_UTIL_STRING_PARSER_H
#define IMPALA_UTIL_STRING_PARSER_H

#include <limits>
#include <boost/type_traits.hpp>
#include "common/compiler-util.h"

#include "common/logging.h"
#include "runtime/decimal-value.h"
#include "util/decimal-util.h"

namespace impala {

/// Utility functions for doing atoi/atof on non-null terminated strings.  On micro
/// benchmarks, this is significantly faster than libc (atoi/strtol and atof/strtod).
//
/// Strings with leading and trailing whitespaces are accepted.
/// Branching is heavily optimized for the non-whitespace successful case.
/// All the StringTo* functions first parse the input string assuming it has no leading
/// whitespace. If that first attempt was unsuccessful, these functions retry the parsing
/// after removing whitespace. Therefore, strings with whitespace take a perf hit on
/// branch mis-prediction.
//
/// For overflows, we are following the mysql behavior, to cap values at the max/min value
/// for that data type.  This is different from hive, which returns NULL for overflow
/// slots for int types and inf/-inf for float types.
//
/// Things we tried that did not work:
///  - lookup table for converting character to digit
/// Improvements (TODO):
///  - Validate input using _sidd_compare_ranges
///  - Since we know the length, we can parallelize this: i.e. result = 100*s[0] +
///    10*s[1] + s[2]
class StringParser {
 public:
  enum ParseResult {
    PARSE_SUCCESS = 0,
    PARSE_FAILURE,
    PARSE_OVERFLOW,
    PARSE_UNDERFLOW,
  };

  template <typename T>
  static inline T StringToInt(const char* s, int len, ParseResult* result) {
    T ans = StringToIntInternal<T>(s, len, result);
    if (LIKELY(*result == PARSE_SUCCESS)) return ans;

    int i = SkipLeadingWhitespace(s, len);
    return StringToIntInternal<T>(s + i, len - i, result);
  }

  /// Convert a string s representing a number in given base into a decimal number.
  template <typename T>
  static inline T StringToInt(const char* s, int len, int base, ParseResult* result) {
    T ans = StringToIntInternal<T>(s, len, base, result);
    if (LIKELY(*result == PARSE_SUCCESS)) return ans;

    int i = SkipLeadingWhitespace(s, len);
    return StringToIntInternal<T>(s + i, len - i, base, result);
  }

  template <typename T>
  static inline T StringToFloat(const char* s, int len, ParseResult* result) {
    T ans = StringToFloatInternal<T>(s, len, result);
    if (LIKELY(*result == PARSE_SUCCESS)) return ans;

    int i = SkipLeadingWhitespace(s, len);
    return StringToFloatInternal<T>(s + i, len - i, result);
  }

  /// Parses a string for 'true' or 'false', case insensitive.
  static inline bool StringToBool(const char* s, int len, ParseResult* result) {
    bool ans = StringToBoolInternal(s, len, result);
    if (LIKELY(*result == PARSE_SUCCESS)) return ans;

    int i = SkipLeadingWhitespace(s, len);
    return StringToBoolInternal(s + i, len - i, result);
  }

  /// Parses a decimal from s, returning the result.
  /// The parse status is returned in *result.
  /// On overflow or invalid values, the return value is undefined.
  /// On underflow, the truncated value is returned.
  template <typename T>
  static inline DecimalValue<T> StringToDecimal(const uint8_t* s, int len,
      const ColumnType& type, StringParser::ParseResult* result) {
    return StringToDecimal<T>(reinterpret_cast<const char*>(s), len, type, result);
  }

  template <typename T>
  static inline DecimalValue<T> StringToDecimal(const char* s, int len,
      const ColumnType& type, StringParser::ParseResult* result) {
    return StringToDecimal<T>(s, len, type.precision, type.scale, result);
  }

  template <typename T>
  static inline DecimalValue<T> StringToDecimal(const char* s, int len,
      int type_precision, int type_scale, StringParser::ParseResult* result) {
    // Special cases:
    //   1) '' == Fail, an empty string fails to parse.
    //   2) '   #   ' == #, leading and trailing white space is ignored.
    //   3) '.' == 0, a single dot parses as zero (for consistency with other types).
    //   4) '#.' == '#', a trailing dot is ignored.

    // Ignore leading and trailing spaces.
    while (len > 0 && IsWhitespace(*s)) {
      ++s;
      --len;
    }
    while (len > 0 && IsWhitespace(s[len - 1])) {
      --len;
    }

    bool is_negative = false;
    if (len > 0) {
      switch (*s) {
        case '-':
          is_negative = true;
        case '+':
          ++s;
          --len;
      }
    }

    // Ignore leading zeros.
    bool found_value = false;
    while (len > 0 && UNLIKELY(*s == '0')) {
      found_value = true;
      ++s;
      --len;
    }

    // Ignore leading zeros even after a dot. This allows for differetiating between
    // cases like 0.01e2, which would fit in a DECIMAL(1, 0), and 0.10e2, which would
    // overflow.
    int scale = 0;
    int found_dot = 0;
    if (len > 0 && *s == '.') {
      found_dot = 1;
      ++s;
      --len;
      while (len > 0 && UNLIKELY(*s == '0')) {
        found_value = true;
        ++scale;
        ++s;
        --len;
      }
    }

    int precision = 0;
    bool found_exponent = false;
    int8_t exponent = 0;
    T value = 0;
    for (int i = 0; i < len; ++i) {
      const char& c = s[i];
      if (LIKELY('0' <= c && c <= '9')) {
        found_value = true;
        // Ignore digits once the type's precision limit is reached. This avoids
        // overflowing the underlying storage while handling a string like
        // 10000000000e-10 into a DECIMAL(1, 0). Adjustments for ignored digits and
        // an exponent will be made later.
        if (LIKELY(type_precision > precision)) {
          value = (value * 10) + (c - '0');  // Benchmarks are faster with parenthesis...
        }
        DCHECK(value >= 0);  // For some reason DCHECK_GE doesn't work with int128_t.
        ++precision;
        scale += found_dot;
      } else if (c == '.' && LIKELY(!found_dot)) {
        found_dot = 1;
      } else if ((c == 'e' || c == 'E') && LIKELY(!found_exponent)) {
        found_exponent = true;
        exponent = StringToIntInternal<int8_t>(s + i + 1, len - i - 1, result);
        if (UNLIKELY(*result != StringParser::PARSE_SUCCESS)) return DecimalValue<T>(0);
        break;
      } else {
        *result = StringParser::PARSE_FAILURE;
        return DecimalValue<T>(0);
      }
    }

    // Find the number of truncated digits before adjusting the precision for an exponent.
    int truncated_digit_count = precision - type_precision;
    if (exponent > scale) {
      // Ex: 0.1e3 (which at this point would have precision == 1 and scale == 1), the
      //     scale must be set to 0 and the value set to 100 which means a precision of 3.
      precision += exponent - scale;
      value *= DecimalUtil::GetScaleMultiplier<T>(exponent - scale);
      scale = 0;
    } else {
      // Ex: 100e-4, the scale must be set to 4 but no adjustment to the value is needed,
      //     the precision must also be set to 4 but that will be done below for the
      //     non-exponent case anyways.
      scale -= exponent;
    }
    // Ex: 0.001, at this point would have precision 1 and scale 3 since leading zeros
    //     were ignored during previous parsing.
    if (scale > precision) precision = scale;

    // Microbenchmarks show that beyond this point, returning on parse failure is slower
    // than just letting the function run out.
    *result = StringParser::PARSE_SUCCESS;
    if (UNLIKELY(precision - scale > type_precision - type_scale)) {
      *result = StringParser::PARSE_OVERFLOW;
    } else if (UNLIKELY(scale > type_scale)) {
      *result = StringParser::PARSE_UNDERFLOW;
      int shift = scale - type_scale;
      if (truncated_digit_count > 0) shift -= truncated_digit_count;
      if (shift > 0) value /= DecimalUtil::GetScaleMultiplier<T>(shift);
      DCHECK(value >= 0);
    } else if (UNLIKELY(!found_value && !found_dot)) {
      *result = StringParser::PARSE_FAILURE;
    }

    if (type_scale > scale) {
      value *= DecimalUtil::GetScaleMultiplier<T>(type_scale - scale);
    }

    return DecimalValue<T>(is_negative ? -value : value);
  }

 private:
  /// This is considerably faster than glibc's implementation.
  /// In the case of overflow, the max/min value for the data type will be returned.
  /// Assumes s represents a decimal number.
  /// Return PARSE_FAILURE on leading whitespace. Trailing whitespace is allowed.
  template <typename T>
  static inline T StringToIntInternal(const char* s, int len, ParseResult* result) {
    if (UNLIKELY(len <= 0)) {
      *result = PARSE_FAILURE;
      return 0;
    }

    typedef typename boost::make_unsigned<T>::type UnsignedT;
    UnsignedT val = 0;
    UnsignedT max_val = std::numeric_limits<T>::max();
    bool negative = false;
    int i = 0;
    switch (*s) {
      case '-':
        negative = true;
        max_val = std::numeric_limits<T>::max() + 1;
      case '+': ++i;
    }

    // This is the fast path where the string cannot overflow.
    if (LIKELY(len - i < StringParseTraits<T>::max_ascii_len())) {
      val = StringToIntNoOverflow<UnsignedT>(s + i, len - i, result);
      return static_cast<T>(negative ? -val : val);
    }

    const T max_div_10 = max_val / 10;
    const T max_mod_10 = max_val % 10;

    int first = i;
    for (; i < len; ++i) {
      if (LIKELY(s[i] >= '0' && s[i] <= '9')) {
        T digit = s[i] - '0';
        // This is a tricky check to see if adding this digit will cause an overflow.
        if (UNLIKELY(val > (max_div_10 - (digit > max_mod_10)))) {
          *result = PARSE_OVERFLOW;
          return negative ? -max_val : max_val;
        }
        val = val * 10 + digit;
      } else {
        if ((UNLIKELY(i == first || !IsAllWhitespace(s + i, len - i)))) {
          // Reject the string because either the first char was not a digit,
          // or the remaining chars are not all whitespace
          *result = PARSE_FAILURE;
          return 0;
        }
        // Returning here is slightly faster than breaking the loop.
        *result = PARSE_SUCCESS;
        return static_cast<T>(negative ? -val : val);
      }
    }
    *result = PARSE_SUCCESS;
    return static_cast<T>(negative ? -val : val);
  }

  /// Convert a string s representing a number in given base into a decimal number.
  /// Return PARSE_FAILURE on leading whitespace. Trailing whitespace is allowed.
  template <typename T>
  static inline T StringToIntInternal(const char* s, int len, int base,
                                      ParseResult* result) {
    typedef typename boost::make_unsigned<T>::type UnsignedT;
    UnsignedT val = 0;
    UnsignedT max_val = std::numeric_limits<T>::max();
    bool negative = false;
    if (UNLIKELY(len <= 0)) {
      *result = PARSE_FAILURE;
      return 0;
    }
    int i = 0;
    switch (*s) {
      case '-':
        negative = true;
        max_val = std::numeric_limits<T>::max() + 1;
      case '+': i = 1;
    }

    const T max_div_base = max_val / base;
    const T max_mod_base = max_val % base;

    int first = i;
    for (; i < len; ++i) {
      T digit;
      if (LIKELY(s[i] >= '0' && s[i] <= '9')) {
        digit = s[i] - '0';
      } else if (s[i] >= 'a' && s[i] <= 'z') {
        digit = (s[i] - 'a' + 10);
      } else if (s[i] >= 'A' && s[i] <= 'Z') {
        digit = (s[i] - 'A' + 10);
      } else {
        if ((UNLIKELY(i == first || !IsAllWhitespace(s + i, len - i)))) {
          // Reject the string because either the first char was not an alpha/digit,
          // or the remaining chars are not all whitespace
          *result = PARSE_FAILURE;
          return 0;
        }
        // skip trailing whitespace.
        break;
      }

      // Bail, if we encounter a digit that is not available in base.
      if (digit >= base) break;

      // This is a tricky check to see if adding this digit will cause an
      // overflow.
      if (UNLIKELY(val > (max_div_base - (digit > max_mod_base)))) {
        *result = PARSE_OVERFLOW;
        return static_cast<T>(negative ? -max_val : max_val);
      }
      val = val * base + digit;
    }
    *result = PARSE_SUCCESS;
    return static_cast<T>(negative ? -val : val);
  }

  /// This is considerably faster than glibc's implementation (>100x why???)
  /// No special case handling needs to be done for overflows, the floating point spec
  /// already does it and will cap the values to -inf/inf
  /// To avoid inaccurate conversions this function falls back to strtod for
  /// scientific notation.
  /// Return PARSE_FAILURE on leading whitespace. Trailing whitespace is allowed.
  /// TODO: Investigate using intrinsics to speed up the slow strtod path.
  /// TODO: there are other possible optimizations, see IMPALA-1729
  template <typename T>
  static inline T StringToFloatInternal(const char* s, int len, ParseResult* result) {
    if (UNLIKELY(len <= 0)) {
      *result = PARSE_FAILURE;
      return 0;
    }

    // Use double here to not lose precision while accumulating the result
    double val = 0;
    bool negative = false;
    int i = 0;
    double divide = 1;
    bool decimal = false;
    int64_t remainder = 0;
    // The number of significant figures we've encountered so far (i.e., digits excluding
    // leading 0s). This technically shouldn't count trailing 0s either, but for us it
    // doesn't matter if we count them based on the implementation below.
    int sig_figs = 0;
    switch (*s) {
      case '-': negative = true;
      case '+': i = 1;
    }
    int first = i;
    for (; i < len; ++i) {
      if (LIKELY(s[i] >= '0' && s[i] <= '9')) {
        if (s[i] != '0' || sig_figs > 0) ++sig_figs;
        if (decimal) {
          // According to the IEEE floating-point spec, a double has up to 15-17
          // significant decimal digits (see
          // http://en.wikipedia.org/wiki/Double-precision_floating-point_format). We stop
          // processing digits after we've already seen at least 18 sig figs to avoid
          // overflowing 'remainder' (we stop after 18 instead of 17 to get the rounding
          // right).
          if (sig_figs <= 18) {
            remainder = remainder * 10 + s[i] - '0';
            divide *= 10;
          }
        } else {
          val = val * 10 + s[i] - '0';
        }
      } else if (s[i] == '.') {
        decimal = true;
      } else if (s[i] == 'e' || s[i] == 'E') {
        break;
      } else if (s[i] == 'i' || s[i] == 'I') {
        if (len > i + 2 && (s[i+1] == 'n' || s[i+1] == 'N') &&
            (s[i+2] == 'f' || s[i+2] == 'F')) {
          // Note: Hive writes inf as Infinity, at least for text. We'll be a little loose
          // here and interpret any column with inf as a prefix as infinity rather than
          // checking every remaining byte.
          *result = PARSE_SUCCESS;
          return negative ? -INFINITY : INFINITY;
        } else {
          // Starts with 'i', but isn't inf...
          *result = PARSE_FAILURE;
          return 0;
        }
      } else if (s[i] == 'n' || s[i] == 'N') {
        if (len > i + 2 && (s[i+1] == 'a' || s[i+1] == 'A') &&
            (s[i+2] == 'n' || s[i+2] == 'N')) {
          *result = PARSE_SUCCESS;
          return negative ? -NAN : NAN;
        } else {
          // Starts with 'n', but isn't NaN...
          *result = PARSE_FAILURE;
          return 0;
        }
      } else {
        if ((UNLIKELY(i == first || !IsAllWhitespace(s + i, len - i)))) {
          // Reject the string because either the first char was not a digit, "," or "e",
          // or the remaining chars are not all whitespace
          *result = PARSE_FAILURE;
          return 0;
        }
        // skip trailing whitespace.
        break;
      }
    }

    val += remainder / divide;

    if (i < len && (s[i] == 'e' || s[i] == 'E')) {
      // Create a C-string from s starting after the optional '-' sign and fall back to
      // strtod to avoid conversion inaccuracy for scientific notation.
      // Do not use boost::lexical_cast because it causes codegen to crash for an
      // unknown reason (exception handling?).
      char c_str[len - negative + 1];
      memcpy(c_str, s + negative, len - negative);
      c_str[len - negative] = '\0';
      char* s_end;
      val = strtod(c_str, &s_end);
      if (s_end != c_str + len - negative) {
        // skip trailing whitespace
        int trailing_len = len - negative - (int)(s_end - c_str);
        if (UNLIKELY(!IsAllWhitespace(s_end, trailing_len))) {
          *result = PARSE_FAILURE;
          return val;
        }
      }
    }

    // Determine if it is an overflow case and update the result
    if (UNLIKELY(val == std::numeric_limits<T>::infinity())) {
      *result = PARSE_OVERFLOW;
    } else {
      *result = PARSE_SUCCESS;
    }
    return (T)(negative ? -val : val);
  }

  /// Parses a string for 'true' or 'false', case insensitive.
  /// Return PARSE_FAILURE on leading whitespace. Trailing whitespace is allowed.
  static inline bool StringToBoolInternal(const char* s, int len, ParseResult* result) {
    *result = PARSE_SUCCESS;
    if (len >= 4 && (s[0] == 't' || s[0] == 'T')) {
      bool match = (s[1] == 'r' || s[1] == 'R') &&
                   (s[2] == 'u' || s[2] == 'U') &&
                   (s[3] == 'e' || s[3] == 'E');
      if (match && LIKELY(IsAllWhitespace(s+4, len-4))) return true;
    } else if (len >= 5 && (s[0] == 'f' || s[0] == 'F')) {
      bool match = (s[1] == 'a' || s[1] == 'A') &&
                   (s[2] == 'l' || s[2] == 'L') &&
                   (s[3] == 's' || s[3] == 'S') &&
                   (s[4] == 'e' || s[4] == 'E');
      if (match && LIKELY(IsAllWhitespace(s+5, len-5))) return false;
    }
    *result = PARSE_FAILURE;
    return false;
  }

  /// Returns the position of the first non-whitespace character in s.
  static inline int SkipLeadingWhitespace(const char* s, int len) {
    int i = 0;
    while(i < len && IsWhitespace(s[i])) ++i;
    return i;
  }

  /// Returns true if s only contains whitespace.
  static inline bool IsAllWhitespace(const char* s, int len) {
    for (int i = 0; i < len; ++i) {
      if (!LIKELY(IsWhitespace(s[i]))) return false;
    }
    return true;
  }

  template<typename T>
  class StringParseTraits {
   public:
    /// Returns the maximum ascii string length for this type.
    /// e.g. the max/min int8_t has 3 characters.
    static int max_ascii_len();
  };

  /// Converts an ascii string to an integer of type T assuming it cannot overflow
  /// and the number is positive. Leading whitespace is not allowed. Trailing whitespace
  /// will be skipped.
  template <typename T>
  static inline T StringToIntNoOverflow(const char* s, int len, ParseResult* result) {
    T val = 0;
    if (UNLIKELY(len == 0)) {
      *result = PARSE_SUCCESS;
      return val;
    }
    // Factor out the first char for error handling speeds up the loop.
    if (LIKELY(s[0] >= '0' && s[0] <= '9')) {
      val = s[0] - '0';
    } else {
      *result = PARSE_FAILURE;
      return 0;
    }
    for (int i = 1; i < len; ++i) {
      if (LIKELY(s[i] >= '0' && s[i] <= '9')) {
        T digit = s[i] - '0';
        val = val * 10 + digit;
      } else {
        if ((UNLIKELY(!IsAllWhitespace(s + i, len - i)))) {
          *result = PARSE_FAILURE;
          return 0;
        }
        *result = PARSE_SUCCESS;
        return val;
      }
    }
    *result = PARSE_SUCCESS;
    return val;
  }

  static inline bool IsWhitespace(const char& c) {
    return c == ' ' || UNLIKELY(c == '\t' || c == '\n' || c == '\v' || c == '\f'
        || c == '\r');
  }
};

template<>
inline int StringParser::StringParseTraits<int8_t>::max_ascii_len() { return 3; }

template<>
inline int StringParser::StringParseTraits<int16_t>::max_ascii_len() { return 5; }

template<>
inline int StringParser::StringParseTraits<int32_t>::max_ascii_len() { return 10; }

template<>
inline int StringParser::StringParseTraits<int64_t>::max_ascii_len() { return 19; }

}

#endif
