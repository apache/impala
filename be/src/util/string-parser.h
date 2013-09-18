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
namespace impala {

// Utility functions for doing atoi/atof on non-null terminated strings.  On micro 
// benchmarks, this is significantly faster than libc (atoi/strtol and atof/strtod).
//
// For overflows, we are following the mysql behavior, to cap values at the max/min value
// for that data type.  This is different from hive, which returns NULL for overflow 
// slots for int types and inf/-inf for float types.
//
// Things we tried that did not work:
//  - lookup table for converting character to digit
// Improvements (TODO):
//  - Validate input using _sidd_compare_ranges
//  - Since we know the length, we can parallelize this: i.e. result = 100*s[0] + 
//    10*s[1] + s[2]
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
        *result = PARSE_FAILURE;
        return 0;
      }
    }
    *result = PARSE_SUCCESS;
    return static_cast<T>(negative ? -val : val);
  }

  // Convert a string s representing a number in given base into a decimal number.
  template <typename T>
  static inline T StringToInt(const char* s, int len, int base, ParseResult* result) {
    typedef typename boost::make_unsigned<T>::type UnsignedT;
    UnsignedT val = 0;
    UnsignedT max_val = std::numeric_limits<T>::max();
    bool negative = false;
    int i = 0;
    switch (*s) {
      case '-': 
        negative = true;
        max_val = std::numeric_limits<T>::max() + 1;
      case '+': i = 1;
    }
    
    const T max_div_base = max_val / base;
    const T max_mod_base = max_val % base;
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

  // This is considerably faster than glibc's implementation (>100x why???)
  // No special case handling needs to be done for overflows, the floating point spec
  // already does it and will cap the values to -inf/inf
  // To avoid inaccurate conversions this function falls back to strtod for
  // scientific notation.
  // TOOD: Investigate using intrinsics to speed up the slow strtod path.
  template <typename T>
  static inline T StringToFloat(const char* s, int len, ParseResult* result) {
    // Use double here to not lose precision while accumulating the result
    double val = 0;
    bool negative = false;
    int i = 0;
    double divide = 1;
    bool decimal = false;
    int64_t remainder = 0;
    switch (*s) {
    case '-': negative = true;
    case '+': i = 1;
    }
    for (; i < len; ++i) {
      if (LIKELY(s[i] >= '0' && s[i] <= '9')) {
        if (decimal) {
          remainder = remainder * 10 + s[i] - '0';
          divide *= 10;
        } else {
          val = val * 10 + s[i] - '0';
        }
      } else if (s[i] == '.') {
        decimal = true;
      } else if (s[i] == 'e' || s[i] == 'E') {
        break;
      } else {
        *result = PARSE_FAILURE;
        return 0;
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
      // Require the entire string to be parsed, otherwise return an error.
      if (UNLIKELY((val == 0 && c_str == s_end) || (s_end != c_str + len - negative))) {
        *result = PARSE_FAILURE;
        return val;
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
      bool match = (s[0] == 't' || s[0] == 'T') &&
                   (s[1] == 'r' || s[1] == 'R') &&
                   (s[2] == 'u' || s[2] == 'U') &&
                   (s[3] == 'e' || s[3] == 'E');
      if (match) return true;
    } else if (len == 5) {
      bool match = (s[0] == 'f' || s[0] == 'F') &&
                   (s[1] == 'a' || s[1] == 'A') &&
                   (s[2] == 'l' || s[2] == 'L') &&
                   (s[3] == 's' || s[3] == 'S') &&
                   (s[4] == 'e' || s[4] == 'E');
      if (match) return false;
    }
    *result = PARSE_FAILURE;
    return false;
  }

 private:
  template<typename T>
  class StringParseTraits {
   public:
    // Returns the maximum ascii string length for this type.
    // e.g. the max/min int8_t has 3 characters.
    static int max_ascii_len();
  };
  
  // Converts an ascii string to an integer of type T assuming it cannot overflow
  // and the number is positive.
  template <typename T>
  static inline T StringToIntNoOverflow(const char* s, int len, ParseResult* result) {
    T val = 0;
    for (int i = 0; i < len; ++i) {
      if (LIKELY(s[i] >= '0' && s[i] <= '9')) {
        T digit = s[i] - '0';
        val = val * 10 + digit;
      } else {
        *result = PARSE_FAILURE;
        return 0;
      }
    }
    *result = PARSE_SUCCESS;
    return val;
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
