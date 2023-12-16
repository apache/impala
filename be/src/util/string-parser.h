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


#ifndef IMPALA_UTIL_STRING_PARSER_H
#define IMPALA_UTIL_STRING_PARSER_H

#include <limits>
#include <boost/type_traits.hpp>
#include "common/compiler-util.h"

#include "common/logging.h"
#include <gtest/gtest_prod.h>
#include "runtime/decimal-value.h"
#include "runtime/date-parse-util.h"
#include "runtime/timestamp-parse-util.h"
#include "runtime/timestamp-value.h"
#include "thirdparty/fast_double_parser/fast_double_parser.h"
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
///
/// TODO: people went crazy with huge inline functions in this file - most should be
/// moved out-of-line.
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

  /// Parse a TimestampValue from s.
  static inline TimestampValue StringToTimestamp(const char* s, int len,
      ParseResult* result) {
    boost::gregorian::date d;
    boost::posix_time::time_duration t;
    *result = TimestampParser::ParseSimpleDateFormat(s, len, &d, &t) ? PARSE_SUCCESS :
        PARSE_FAILURE;
    return {d, t};
  }

  /// Parse a DateValue from s.
  static inline DateValue StringToDate(const char* s, int len, ParseResult* result) {
    DateValue d;
    *result = DateParser::ParseSimpleDateFormat(s, len, false, &d) ? PARSE_SUCCESS :
        PARSE_FAILURE;
    return d;
  }

  /// Parses a decimal from s, returning the result.
  /// The parse status is returned in *result.
  /// On overflow or invalid values, the return value is undefined.
  /// On underflow, the truncated value is returned.
  template <typename T>
  static inline DecimalValue<T> StringToDecimal(const char* s, int len,
      const ColumnType& type, bool round, StringParser::ParseResult* result) {
    return StringToDecimal<T>(s, len, type.precision, type.scale, round, result);
  }

  template <typename T>
  static inline DecimalValue<T> StringToDecimal(const char* s, int len,
      int type_precision, int type_scale, bool round, StringParser::ParseResult* result) {
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
          [[fallthrough]];
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

    // Ignore leading zeros even after a dot. This allows for differentiating between
    // cases like 0.01e2, which would fit in a DECIMAL(1, 0), and 0.10e2, which would
    // overflow.
    int digits_after_dot_count = 0;
    int found_dot = 0;
    if (len > 0 && *s == '.') {
      found_dot = 1;
      ++s;
      --len;
      while (len > 0 && UNLIKELY(*s == '0')) {
        found_value = true;
        ++digits_after_dot_count;
        ++s;
        --len;
      }
    }

    int total_digits_count = 0;
    bool found_exponent = false;
    int8_t exponent = 0;
    int first_truncated_digit = 0;
    T value = 0;
    for (int i = 0; i < len; ++i) {
      const char c = s[i];
      if (LIKELY('0' <= c && c <= '9')) {
        found_value = true;
        // Ignore digits once the type's precision limit is reached. This avoids
        // overflowing the underlying storage while handling a string like
        // 10000000000e-10 into a DECIMAL(1, 0). Adjustments for ignored digits and
        // an exponent will be made later.
        if (LIKELY(total_digits_count < type_precision)) {
          // Benchmarks are faster with parenthesis.
          T new_value = (value * 10) + (c - '0');
          DCHECK(new_value >= value);
          value = new_value;
        } else if (UNLIKELY(round && total_digits_count == type_precision)) {
          first_truncated_digit = c - '0';
        }
        DCHECK(value >= 0); // DCHECK_GE does not work with int128_t
        ++total_digits_count;
        digits_after_dot_count += found_dot;
      } else if (c == '.' && LIKELY(!found_dot)) {
        found_dot = 1;
      } else if ((c == 'e' || c == 'E') && LIKELY(!found_exponent)) {
        found_exponent = true;
        exponent = StringToIntInternal<int8_t>(s + i + 1, len - i - 1, result);
        if (UNLIKELY(*result != StringParser::PARSE_SUCCESS)) {
          if (*result == StringParser::PARSE_OVERFLOW && exponent < 0) {
            *result = StringParser::PARSE_UNDERFLOW;
          }
          return DecimalValue<T>(0);
        }
        break;
      } else {
        *result = StringParser::PARSE_FAILURE;
        return DecimalValue<T>(0);
      }
    }

    // Find the number of truncated digits before adjusting the precision for an exponent.
    int truncated_digit_count = std::max(total_digits_count - type_precision, 0);
    // 'scale' and 'precision' refer to the scale and precision of the number that
    // is contained the string that we are parsing. The scale of 'value' may be
    // different because some digits may have been truncated.
    int scale, precision;
    ApplyExponent(total_digits_count, digits_after_dot_count,
        exponent, &value, &precision, &scale);

    // Microbenchmarks show that beyond this point, returning on parse failure is slower
    // than just letting the function run out.
    *result = StringParser::PARSE_SUCCESS;
    if (UNLIKELY(precision - scale > type_precision - type_scale)) {
      // The number in the string has too many digits to the left of the dot,
      // so we overflow.
      *result = StringParser::PARSE_OVERFLOW;
    } else if (UNLIKELY(scale > type_scale)) {
      // There are too many digits to the right of the dot in the string we are parsing.
      *result = StringParser::PARSE_UNDERFLOW;
      // The scale of 'value'.
      int value_scale = scale - truncated_digit_count;
      int shift = value_scale - type_scale;
      if (shift > 0) {
        // There are less than maximum number of digits to the left of the dot.
        value = DecimalUtil::ScaleDownAndRound<T>(value, shift, round);
        DCHECK(value >= 0);
        DCHECK(value < DecimalUtil::GetScaleMultiplier<int128_t>(type_precision));
      } else {
        // There are a maximum number of digits to the left of the dot. We round by
        // looking at the first truncated digit.
        DCHECK_EQ(shift, 0);
        DCHECK(0 <= first_truncated_digit && first_truncated_digit <= 9);
        DCHECK(first_truncated_digit == 0 || truncated_digit_count != 0);
        DCHECK(first_truncated_digit == 0 || round);
        // Apply the rounding.
        value += (first_truncated_digit >= 5);
        DCHECK(value >= 0);
        DCHECK(value <= DecimalUtil::GetScaleMultiplier<int128_t>(type_precision));
        if (UNLIKELY(value == DecimalUtil::GetScaleMultiplier<T>(type_precision))) {
          // Overflow due to rounding.
          *result = StringParser::PARSE_OVERFLOW;
        }
      }
    } else if (UNLIKELY(!found_value && !found_dot)) {
      *result = StringParser::PARSE_FAILURE;
    } else if (type_scale > scale) {
      // There were not enough digits after the dot, so we have scale up the value.
      DCHECK_EQ(truncated_digit_count, 0);
      value *= DecimalUtil::GetScaleMultiplier<T>(type_scale - scale);
      // Overflow should be impossible.
      DCHECK(value < DecimalUtil::GetScaleMultiplier<int128_t>(type_precision));
    }

    return DecimalValue<T>(is_negative ? -value : value);
  }

 private:
  // Max length of string passed to 'fast_double_parser::parse_number'.
  static const int MAX_LEN_FAST_FLOAT_PARSER = 50;

  // Not using FRIEND_TEST as TestStringToFloatPreprocess is not a TestSuite
  friend void TestStringToFloatPreprocess(const char* s, const char* expected);

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
        max_val = static_cast<UnsignedT>(std::numeric_limits<T>::max()) + 1;
        [[fallthrough]];
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
        max_val = static_cast<UnsignedT>(std::numeric_limits<T>::max()) + 1;
        [[fallthrough]];
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

  /// Helper function that applies the exponent to the value. Also computes and
  /// sets the precision and scale.
  template <typename T>
  static inline void ApplyExponent(int total_digits_count,
      int digits_after_dot_count,int8_t exponent, T* value, int* precision, int* scale) {
    if (exponent > digits_after_dot_count) {
      // Ex: 0.1e3 (which at this point would have precision == 1 and scale == 1), the
      //     scale must be set to 0 and the value set to 100 which means a precision of 3.
      *value = ArithmeticUtil::AsUnsigned<std::multiplies>(
          *value, DecimalUtil::GetScaleMultiplier<T>(exponent - digits_after_dot_count));
      *precision = total_digits_count + (exponent - digits_after_dot_count);
      *scale = 0;
    } else {
      // Ex: 100e-4, the scale must be set to 4 but no adjustment to the value is needed,
      //     the precision must also be set to 4 but that will be done below for the
      //     non-exponent case anyways.
      *precision = total_digits_count;
      *scale = digits_after_dot_count - exponent;
      // Ex: 0.001, at this point would have precision 1 and scale 3 since leading zeros
      //     were ignored during previous parsing.
      if (*precision < *scale) *precision = *scale;
    }
    DCHECK_GE(*precision, *scale);
  }

  /// Checks if "inf" or "infinity" matches 's' in a case-insensitive manner. The match
  /// has to start at the beginning of 's', leading whitespace is considered invalid.
  /// Trailing whitespace characters are allowed.
  /// Returns true if a match was found and false otherwise.
  static inline bool IsInfinity(const char *s, int len) {
    if (len >= 3 && strncasecmp(s, "inf", 3) == 0) {
      int i = 3;
      if (len >= 8 && strncasecmp(s + 3, "inity", 5) == 0) {
        i = 8;
      }
      return IsAllWhitespace(s + i, len - i);
    }
    return false;
  }

  /// Checks if "nan" matches 's' in a case-insensitive manner. The match has to start at
  /// the beginning of 's', leading whitespace is considered invalid. Trailing whitespace
  /// characters are allowed.
  /// Returns true if a match was found and false otherwise.
  static inline bool IsNaN(const char *s, int len) {
    return len >= 3 && strncasecmp(s, "nan", 3) == 0 && IsAllWhitespace(s + 3, len - 3);
  }

  /// Function to convert string to float. It is a wrapper over
  /// fast_double_parser::parse_number which provides fast function to parse string into
  /// double. It accepts string with notations like "1.0e10". It would not sacrifise
  /// accuracy and match exactly (down the smallest bit) the result of a standard
  /// function like strtod.
  /// On failure result is set to PARSE_FAILURE and 0 is returned.
  /// Return PARSE_FAILURE on leading whitespace. Trailing whitespace is allowed.
  template <typename T>
  static inline T StringToFloatInternal(const char* s, int len, ParseResult* result) {
    if (UNLIKELY(len <= 0)) {
      *result = PARSE_FAILURE;
      return 0;
    }

    bool negative = false;
    int i = 0;
    switch (*s) {
      case '-':
        negative = true;
        [[fallthrough]];
      case '+': i = 1;
    }

    // Check if we have inf or NaN.
    if (IsInfinity(s + i, len - i)) {
      *result = PARSE_SUCCESS;
      return negative ? -std::numeric_limits<T>::infinity()
          : std::numeric_limits<T>::infinity();
    }
    if (IsNaN(s + i, len - i)) {
      *result = PARSE_SUCCESS;
      return negative ? -std::numeric_limits<T>::quiet_NaN()
          : std::numeric_limits<T>::quiet_NaN();
    }

    // Using the fast_double_parser library function to parse strings
    // containing decimal numbers into double-precision (binary64)
    // floating-point values. Function is performant and does not sacrifice accuracy.
    // The function will match exactly (down the smallest bit) the result of a
    // standard function like strtod.

    const char * s_end; // post-parsing will point after last character parsed.
    int trailing_len; // length of remaning string not parsed by library

    // Below  are used for making copy of 's' if required.
    // For short string c_str will be used and for longer string 's_copy' is used.
    char c_str[StringParser::MAX_LEN_FAST_FLOAT_PARSER + 2];
    std::string s_copy;

    // Needs non-zero intitialization, as fast_parser library returns 0 for
    // UNDERFLOW error.
    double val = 1;
    // Use stack for copying short string and use std::string (malloc) for
    // longer string.
    if (LIKELY(len - i < StringParser::MAX_LEN_FAST_FLOAT_PARSER)) {
      // Process the string and make it compatible for the library.
      int input_len = StringToFloatPreprocess(s + i, len - i, c_str);
      s_end = fast_double_parser::parse_number(c_str, &val);
      trailing_len = input_len - (int) (s_end - c_str);
    } else {
      // Parser library will invoke 'strtod' for longer string so directly invoke
      // it to avoid preprocessing.
      s_copy.insert(0, s + i, len - i);
      s_end = fast_double_parser::parse_float_strtod(s_copy.c_str(), &val);
      trailing_len = s_copy.length() - (s_end - s_copy.c_str());
    }

    if (UNLIKELY(s_end == nullptr)) {
      // Determine if it is an overflow case
      if (UNLIKELY(!std::isfinite(val))) {
        *result = PARSE_OVERFLOW;
      } else if (UNLIKELY(val == 0)) {
        *result = PARSE_UNDERFLOW;
      } else {
        *result = PARSE_FAILURE;
        return 0;
      }
      return (T)(negative ? -val : val);
    }

    // check if trailing characters are present and are all white spaces
    DCHECK(trailing_len >= 0);
    if (UNLIKELY(trailing_len > 0 && !IsAllWhitespace(s_end, trailing_len))) {
      *result = PARSE_FAILURE;
      return 0;
    }

    // Determine if it is an overflow case and update the result
    if (UNLIKELY(val == std::numeric_limits<T>::infinity())) {
      *result = PARSE_OVERFLOW;
    } else {
      *result = PARSE_SUCCESS;
    }
    return (T)(negative ? -val : val);
  }

  /// Internal utility function for `StringToFloatInternal`.
  /// It performs these 3 things to make strings compatible with
  /// `fast_double_parser::parse_number`:
  /// 1. Removes leading 0s before another integer i.e.,'000012'->'12', '01.2'->'1.2'
  /// 2. Collapsing leading 0s before any digit
  ///    i.e., '000.7'->'0.7', '0000'->'0', '00077.7' -> '77.7'.
  /// 3. Prefix strings starting with '.' like '.56' or '-.56' with 0.
  /// 'result' will point to processed string which will be null-terminated.
  /// length of null-terminated string pointed to by 'result' is returned.
  /// It doesn't handle strings starting with sign like "+" or "-".
  static inline int StringToFloatPreprocess(const char* s, int len, char * result) {
    int result_len = 0; // current length of 'result'
    const char* curr = StripLeadingZeros(s, len);
    int curr_len = (int) (s + len - curr); // remanining length of 'curr'
    if (UNLIKELY(*curr == '.')) {
      // Prefix strings starting with '.' like '.56' or '-.56' with 0.
      result[result_len++] = '0';
    }
    // copy rest 'curr' to result 'res'
    memcpy(result + result_len, curr, curr_len);
    result_len += curr_len;
    result[result_len] = '\0';
    return result_len;
  }

  /// Utility to collapse leading 0s before any digit
  /// E.g., '000.7'->'0.7', '0000'->'0', '00077.7'->'77.7'.
  static inline const char* StripLeadingZeros(const char* s, int len) {
    const char* curr = s;
    const char* end = s + len - 1;
    while(curr < end && *curr == '0'
        && fast_double_parser::is_integer(*(curr + 1))) {
      curr++;
    }
    return curr;
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

  static inline bool IsWhitespace(const char c) {
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

// These functions are too large to benefit from inlining.
Decimal4Value StringToDecimal4(const char* s, int len, int type_precision,
    int type_scale, bool round, StringParser::ParseResult* result);

Decimal8Value StringToDecimal8(const char* s, int len, int type_precision,
    int type_scale, bool round, StringParser::ParseResult* result);

Decimal16Value StringToDecimal16(const char* s, int len, int type_precision,
    int type_scale, bool round, StringParser::ParseResult* result);

}
#endif
