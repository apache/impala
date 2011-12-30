// Copyright (c) 2011 Cloudera, Inc. All rights reserved.

#ifndef IMPALA_UTIL_PARSE_UTIL_H
#define IMPALA_UTIL_PARSE_UTIL_H

#include <ostream>
#include <string>

namespace impala {

// atoi for non-null terminated strings.
// This is considerably faster than glibc's implementation (25x).  
// TODO: what to do with overflows.  Investigate glibc's behavior.           
// TODO: validate input using _sidd_compare_ranges 
template <typename T>
inline T StringToInt(const char* s, int len, bool* error) {
  T val = 0;
  bool negative = false;
  int i = 0;
  switch (*s) {
    case '-': negative = true;
    case '+': i = 1;
  }
  for (; i < len; ++i) {
    // TODO: Added compiler directive for always taking the first branch
    // TODO: faster with lookup table?
    if (s[i] >= '0' && s[i] <= '9') {
      val = val * 10 + s[i] - '0';
    } else {
      *error = true;
      return 0;
    }
  }
  return (T)(negative ? -val : val);
}

// atof for non-null terminated strings
// This is considerably faster than glibc's implementation (7.5x)
// TODO: what to do with overflows.  Investigate glibc's behavior.           
// TODO: validate input using _sidd_compare_ranges.  Also, if input contains
// scientific notation, fallback to strtod
template <typename T>
inline T StringToFloat(const char* s, int len, bool* error) {
  // Use double here to not lose precision while acculumating the result
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
    // TODO: Added compiler directive for always taking the first branch
    // TODO: faster with lookup table?
    if (s[i] >= '0' && s[i] <= '9') {
      val = val * 10 + s[i] - '0';
      divide *= magnitude;
    } else if (s[i] == '.') {
      magnitude = 10;
    } else if (s[i] == 'e' || s[i] == 'E') {
      break;
    } else {
      *error = true;
      return 0;
    }
  }

  val /= divide;

  if (i < len && (s[i] == 'e' || s[i] == 'E')) {
    ++i;
    int exp = StringToInt<int>(s + i, len - i, error);
    if (error) return 0;
    while (exp) {
      val *= 10;
      exp--;
    }
    while (exp < 0) {
      val *= 0.1;
      exp++;
    }
  }
  return (T)(negative ? -val : val);
}

// parses a string for 'true' or 'false', case insensitive
inline bool StringToBool(const char* s, int len, bool* error_in_row) {
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
  *error_in_row = true;
  return false;
}

}

#endif
