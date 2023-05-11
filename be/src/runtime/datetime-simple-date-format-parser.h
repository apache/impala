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

#pragma once

#include "runtime/datetime-parser-common.h"

#include <boost/unordered_map.hpp>

#include "runtime/string-value.h"

namespace impala {

/// The functionality here covers SimpleDateFormat pattern handling in Impala.
/// Adds support for dealing with custom date/time formats in Impala. The following
/// date/time tokens are supported:
///   y - Year
///   M - Month
///   d - Day
///   H - Hour
///   m - Minute
///   s - second
///   S - Fractional second
///
///   TimeZone offset formats (Must be at the end of format string):
///   +/-hh:mm
///   +/-hhmm
///   +/-hh
///
///
/// The token names and usage have been modeled after the SimpleDateFormat class used in
/// Java, with only the above list of tokens being supported. All fields will consume
/// variable length inputs when parsing an input string and must therefore use separators
/// to specify the boundaries of the fields, with the exception of TimeZone values, which
/// have to be of fixed width. Repeating tokens can be used to specify fields of exact
/// width, e.g. in yy-MM both fields must be of exactly length two. When using fixed width
/// fields values must be zero-padded and output values will be zero padded during
/// formatting. There is one exception to this: a month field of length 3 will specify
/// literal month names instead of zero padding, i.e., yyyy-MMM-dd will parse from and
/// format to strings like 2013-Nov-21. When using fields of fixed width the separators
/// can be omitted.
///
///
/// Formatting character groups can appear in any order along with any separators
/// except TimeZone offset.
/// e.g.
///   yyyy/MM/dd
///   dd-MMM-yy
///   (dd)(MM)(yyyy) HH:mm:ss
///   yyyy-MM-dd HH:mm:ss+hh:mm
/// ..etc..
///
/// The following features are not supported:
///   Long literal months e.g. MMMM
///   Nested strings e.g. "Year: " yyyy "Month: " mm "Day: " dd
///   Lazy formatting
namespace datetime_parse_util {

class SimpleDateFormatTokenizer {
public:
  /// Constants to hold default format lengths.
  static const int DEFAULT_DATE_FMT_LEN;
  static const int DEFAULT_TIME_FMT_LEN;
  static const int DEFAULT_TIME_FRAC_FMT_LEN;
  static const int DEFAULT_SHORT_DATE_TIME_FMT_LEN;
  static const int DEFAULT_DATE_TIME_FMT_LEN;
  static const int FRACTIONAL_MAX_LEN;

  /// Parse the date/time format into tokens and place them in the context.
  /// dt_ctx -- output date/time format context
  /// accept_time_toks -- if true, time tokens are accepted. Otherwise time tokens are
  /// rejected.
  /// cast_mode -- indicates if it is a 'datetime to string' or 'string to datetime' cast
  /// Return true if the parse was successful.
  static bool Tokenize(DateTimeFormatContext* dt_ctx, CastDirection cast_mode,
      bool accept_time_toks = true, bool accept_time_toks_only = false);

  /// Parse the date/time string to generate the DateTimeFormatToken required by
  /// DateTimeFormatContext. Similar to Tokenize() this function will take the string
  /// and length, then heuristically determine whether the value contains date tokens,
  /// time tokens, or both. Unlike Tokenize(), it does not require the template format
  /// string.
  /// dt_ctx -- date/time format context (must contain valid tokens)
  /// accept_time_toks -- if true, time tokens are accepted, otherwise time tokens are
  /// rejected.
  /// Otherwise, they are rejected.
  /// Return true if the date/time was successfully parsed.
  static bool TokenizeByStr(DateTimeFormatContext* dt_ctx,
      bool accept_time_toks = true);

  /// Parse date/time string to find the corresponding default date/time format context.
  /// The string must adhere to a default date/time format.
  /// str -- valid pointer to the string to parse.
  /// len -- length of the string to parse (must be > 0)
  /// accept_time_toks -- if true, time tokens are accepted. Otherwise time tokens are
  /// rejected.
  /// accept_time_toks_only -- if true, time tokens without date tokens are accepted.
  /// Otherwise, they are rejected.
  /// Return the corresponding default format context if parsing succeeded, or nullptr
  /// otherwise.
  static const DateTimeFormatContext* GetDefaultFormatContext(const char* str, int len,
      bool accept_time_toks, bool accept_time_toks_only);

  /// Return default date/time format context for a timestamp parsing.
  /// If 'time' has a fractional seconds, context with pattern
  /// "yyyy-MM-dd HH:mm:ss.SSSSSSSSS" will be returned. Otherwise, return context with
  /// pattern "yyyy-MM-dd HH:mm:ss".
  static ALWAYS_INLINE const DateTimeFormatContext* GetDefaultTimestampFormatContext(
      const boost::posix_time::time_duration& time) {
    return time.fractional_seconds() > 0 ? &DEFAULT_DATE_TIME_CTX[9] :
                                           &DEFAULT_SHORT_DATE_TIME_CTX;
  }

  static ALWAYS_INLINE const DateTimeFormatContext* GetDefaultDateFormatContext() {
    return &DEFAULT_DATE_CTX;
  }

  /// Initialize the default format contexts. This *must* be called before using
  /// GetDefaultFormatContext().
  static void InitCtx();
private:
  /// Used to indicate if the state has been initialized.
  static bool initialized;

  /// Pseudo-constant default date/time contexts. Backwards compatibility is provided on
  /// variable length fractional components by defining a format context for each expected
  /// length (0 - 9). This logic will be refactored when the parser supports lazy tokens.
  static DateTimeFormatContext DEFAULT_SHORT_DATE_TIME_CTX;
  static DateTimeFormatContext DEFAULT_SHORT_ISO_DATE_TIME_CTX;
  static DateTimeFormatContext DEFAULT_DATE_CTX;
  static DateTimeFormatContext DEFAULT_TIME_CTX;
  static DateTimeFormatContext DEFAULT_DATE_TIME_CTX[10];
  static DateTimeFormatContext DEFAULT_ISO_DATE_TIME_CTX[10];
  static DateTimeFormatContext DEFAULT_TIME_FRAC_CTX[10];

  /// Checks if str_begin point to the beginning of a valid timezone offset.
  static bool IsValidTZOffset(const char* str_begin, const char* str_end);

  // Parse out the next digit token from the date/time string by checking for contiguous
  // digit characters and return a pointer to the end of that token.
  // str -- pointer to the string to be parsed
  // str_end -- the pointer to the end of the string to be parsed
  // Returns the pointer within the string to the end of the valid digit token.
  static const char* ParseDigitToken(const char* str, const char* str_end);

  // Parse out the next separator token from the date/time string against an expected
  // character.
  // str -- pointer to the string to be parsed
  // str_end -- the pointer to the end of the string to be parsed
  // sep -- the separator char to compare the token to
  // Returns the pointer within the string to the end of the valid separator token.
  static const char* ParseSeparatorToken(const char* str, const char* str_end,
      const char sep);
};

class SimpleDateFormatParser {
public:
  /// Parse a date/time string. The data must adhere to the format, otherwise it will be
  /// rejected i.e. no missing tokens.
  /// Does only a basic validation on the parsed date/time values. The caller is
  /// responsible for implementing rigid data validation and range-check.
  /// str -- valid pointer to the string to parse
  /// len -- length of the string to parse (must be > 0)
  /// dt_ctx -- date/time format context (must contain valid tokens)
  /// dt_result -- the struct where the results of the parsing will be placed
  /// Return true if the date/time was successfully parsed.
  static bool ParseDateTime(const char* str, int len,
      const DateTimeFormatContext& dt_ctx, DateTimeParseResult* dt_result);
};

/// Helper function for formatting small numbers with leading zeros
/// This is used inline with data and timestamp formatting functions
inline void ZeroPad(char* const dst, uint32 val, const uint32 digits) {
  char* p = dst + digits;
  while(val) {
    *--p = '0' + (val % 10);
    val /= 10;
  }
  while(p != dst) {
    *--p = '0';
  }
}

}

}
