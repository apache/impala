// Copyright (c) 2011 Cloudera, Inc. All rights reserved.

#ifndef IMPALA_UTIL_PARSE_UTIL_H
#define IMPALA_UTIL_PARSE_UTIL_H

#include <limits>
#include "common/compiler-util.h"

namespace impala {

// Utility functions for doing atoi/atof on non-null terminated strings.  On micro benchmarks,
// this is significantly faster than libc (atoi/strtol and atof/strtod).
//
// For overflows, we are following the mysql behavior, to cap values at the max/min value for that
// data type.  This is different from hive, which returns NULL for overflow slots for int types
// and inf/-inf for float types.
//
// Things we tried that did not work:
//  - lookup table for converting character to digit
// Improvements (TODO):
//  - Validate input using _sidd_compare_ranges
//  - Since we know the length, we can parallelize this: i.e. result = 100*s[0] + 10*s[1] + s[2]
class StringParser {
 public:
  enum ParseResult {
    PARSE_SUCCESS = 0,
    PARSE_FAILURE,
    PARSE_OVERFLOW
  };

  // This is considerably faster than glibc's implementation (25x).  
  // In the case of overflow, the max/min value for the data type will be returned.
  // Assumes s represents a decimal number.
  template <typename T>
  static inline T StringToInt(const char* s, int len, ParseResult* result) {
    T val = 0;
    bool negative = false;
    int i = 0;
    switch (*s) {
      case '-': negative = true;
      case '+': i = 1;
    }
    for (; i < len; ++i) {
      if (LIKELY(s[i] >= '0' && s[i] <= '9')) {
        T digit = s[i] - '0';
        val = val * 10 + digit;
        // Overflow
        if (UNLIKELY(val < digit)) {
          *result = PARSE_OVERFLOW;
          return negative ? std::numeric_limits<T>::min() : std::numeric_limits<T>::max();
        }
      } else {
        *result = PARSE_FAILURE;
        return 0;
      }
    }
    *result = PARSE_SUCCESS;
    return (T)(negative ? -val : val);
  }

  // Convert a string s representing a number in given base into a decimal number.
  template <typename T>
  static inline T StringToInt(const char* s, int len, int base, ParseResult* result) {
    T val = 0;
    bool negative = false;
    int i = 0;
    switch (*s) {
      case '-': negative = true;
      case '+': i = 1;
    }
    for (; i < len; ++i) {
      T digit;
      if (LIKELY(s[i] >= '0' && s[i] <= '9')) {
        digit = s[i] - '0';
      } else if (s[i] >= 'a' && s[i] <= 'z') {
        digit = (s[i] - 'a' + 10);
      } else if (s[i] >= 'A' && s[i] <= 'Z') {
        digit = (s[i] - 'A' + 10);
      } else {
        *result = PARSE_FAILURE;
        return 0;
      }
      // Bail, if we encounter a digit that is not available in base.
      if (digit >= base) {
        break;
      }
      val = val * base + digit;
      // Overflow
      if (UNLIKELY(val < digit)) {
        *result = PARSE_OVERFLOW;
        return negative ? std::numeric_limits<T>::min() : std::numeric_limits<T>::max();
      }
    }
    *result = PARSE_SUCCESS;
    return (T)(negative ? -val : val);
  }

  // This is considerably faster than glibc's implementation (>100x why???)
  // No special case handling needs to be done for overflows, the floating point spec
  // already does it and will cap the values to -inf/inf
  // TODO: Also, if input contains scientific notation, fall back to strtod
  template <typename T>
  static inline T StringToFloat(const char* s, int len, ParseResult* result) {
    // Use double here to not lose precision while accumulating the result
    double val = 0;
    bool negative = false;
    int i = 0;
    double divide = 1;
    double magnitude = 1;
    switch (*s) {
      case '-': negative = true;
      case '+': i = 1;
    }
    for (; i < len; ++i) {
      if (LIKELY(s[i] >= '0' && s[i] <= '9')) {
        val = val * 10 + s[i] - '0';
        divide *= magnitude;
      } else if (s[i] == '.') {
        magnitude = 10;
      } else if (s[i] == 'e' || s[i] == 'E') {
        break;
      } else {
        *result = PARSE_FAILURE;
        return 0;
      }
    }

    val /= divide;

    if (i < len && (s[i] == 'e' || s[i] == 'E')) {
      ++i;
      int exp = StringToInt<int>(s + i, len - i, result);
      if (UNLIKELY(*result != PARSE_SUCCESS)) return 0;
      while (exp > 0) {
        val *= 10;
        exp--;
      }
      while (exp < 0) {
        val *= 0.1;
        exp++;
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

  // parses a string for 'true' or 'false', case insensitive
  static inline bool StringToBool(const char* s, int len, ParseResult* result) {
    *result = PARSE_SUCCESS;
    if (len == 4) {
      bool val = (s[0] == 't' || s[0] == 'T') &&
                 (s[1] == 'r' || s[1] == 'R') &&
                 (s[2] == 'u' || s[2] == 'U') &&
                 (s[3] == 'e' || s[3] == 'E');
      if (val) return val;
    } else if (len == 5) {
      bool val = (s[0] == 'f' || s[0] == 'F') &&
                 (s[1] == 'a' || s[1] == 'A') &&
                 (s[2] == 'l' || s[2] == 'L') &&
                 (s[3] == 's' || s[3] == 'S') &&
                 (s[4] == 'e' || s[4] == 'E');
      if (val) return !val;
    }
    *result = PARSE_FAILURE;
    return false;
  }
};

}

#endif
