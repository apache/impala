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

#ifndef IMPALA_RUNTIME_TIMESTAMP_PARSE_UTIL_H
#define IMPALA_RUNTIME_TIMESTAMP_PARSE_UTIL_H

#include <cstddef>
#include <vector>

namespace boost {
  namespace gregorian {
    class date;
  }
  namespace posix_time {
    class time_duration;
  }
}

namespace impala {

struct DateTimeParseResult;

/// Add support for dealing with custom date/time formats in Impala. The following
/// date/time tokens are supported:
///   y – Year
///   M – Month
///   d – Day
///   H – Hour
///   m – Minute
///   s – second
///   S – Fractional second
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
/// witdh, e.g. in yy-MM both fields must be of exactly length two. When using fixed width
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
///   Nested strings e.g. “Year: “ yyyy “Month: “ mm “Day: “ dd
///   Lazy formatting

/// Used to indicate the type of a date/time format token group.
enum DateTimeFormatTokenType {
  UNKNOWN = 0,
  SEPARATOR,
  YEAR,
  MONTH_IN_YEAR,
  /// Indicates a short literal month e.g. MMM (Aug). Note that the month name is case
  /// insensitive for an input scenario and printed in camel case for an output scenario.
  MONTH_IN_YEAR_SLT,
  DAY_IN_MONTH,
  HOUR_IN_DAY,
  MINUTE_IN_HOUR,
  SECOND_IN_MINUTE,
  /// Indicates fractional seconds e.g.14:52:36.2334. By default this provides nanosecond
  /// resolution.
  FRACTION,
  TZ_OFFSET,
};

/// Used to store metadata about a token group within a date/time format.
struct DateTimeFormatToken {
  /// Indicates the type of date/time format token e.g. year
  DateTimeFormatTokenType type;
  /// The position of where this token group is supposed to start in the date/time string
  /// to be parsed
  int pos;
  /// The length of the token group
  int len;
  /// A pointer to the date/time format string that is positioned at the start of this
  /// token group
  const char* val;

  DateTimeFormatToken(DateTimeFormatTokenType type, int pos, int len, const char* val)
    : type(type),
      pos(pos),
      len(len),
      val(val) {
  }
};

/// This structure is used to hold metadata for a date/time format. Each token group
/// within the raw format is parsed and placed in this structure along with other high
/// level information e.g. if the format contains date and/or time tokens. This context
/// is used during date/time parsing.
struct DateTimeFormatContext {
  const char* fmt;
  int fmt_len;
  /// Holds the expanded length of fmt_len plus any required space when short format
  /// tokens are used. The output buffer size is driven from this value. For example, in
  /// an output scenario a user may provide the format yyyy-M-d, if the day and month
  /// equates to 12, 21 then extra space is needed in the buffer to hold the values. The
  /// short format type e.g. yyyy-M-d is valid where no zero padding is required on single
  /// digits.
  int fmt_out_len;
  std::vector<DateTimeFormatToken> toks;
  bool has_date_toks;
  bool has_time_toks;

  DateTimeFormatContext() {
    Reset(NULL, 0);
  }

  DateTimeFormatContext(const char* fmt, int fmt_len) {
    Reset(fmt, fmt_len);
  }

  void Reset(const char* fmt, int fmt_len) {
    this->fmt = fmt;
    this->fmt_len = fmt_len;
    this->fmt_out_len = fmt_len;
    this->has_date_toks = false;
    this->has_time_toks = false;
    this->toks.clear();
  }
};

/// Used for parsing both default and custom formatted timestamp values.
class TimestampParser {
 public:
  /// Initializes the static parser context which includes default date/time formats and
  /// lookup tables. This *must* be called before any of the Parse* related functions can
  /// be used.
  static void Init();

  /// Parse the date/time format into tokens and place them in the context.
  /// dt_ctx -- date/time format context
  /// Return true if the parse was successful.
  static bool ParseFormatTokens(DateTimeFormatContext* dt_ctx);

  /// Parse a default date/time string. The default timestamp format is:
  /// yyyy-MM-dd HH:mm:ss.SSSSSSSSS or yyyy-MM-ddTHH:mm:ss.SSSSSSSSS. Either just the
  /// date or just the time may be specified. All components are required in either the
  /// date or time except for the fractional seconds following the period. In the case
  /// of just a date, the time will be set to 00:00:00. In the case of just a time, the
  /// date will be set to invalid.
  /// str -- valid pointer to the string to parse
  /// len -- length of the string to parse (must be > 0)
  /// dt_ctx -- date/time format context (must contain valid tokens)
  /// d -- the date value where the results of the parsing will be placed
  /// t -- the time value where the results of the parsing will be placed
  /// Returns true if the date/time was successfully parsed.
  static bool Parse(const char* str, int len, boost::gregorian::date* d,
      boost::posix_time::time_duration* t);

  /// Parse a date/time string. The data must adhere to the format, otherwise it will be
  /// rejected i.e. no missing tokens. In the case of just a date, the time will be set
  /// to 00:00:00. In the case of just a time, the date will be set to invalid.
  /// str -- valid pointer to the string to parse
  /// len -- length of the string to parse (must be > 0)
  /// d -- the date value where the results of the parsing will be placed
  /// t -- the time value where the results of the parsing will be placed
  /// Returns true if the date/time was successfully parsed.
  static bool Parse(const char* str, int len, const DateTimeFormatContext& dt_ctx,
      boost::gregorian::date* d, boost::posix_time::time_duration* t);

  /// Format the date/time values using the given format context. Note that a string
  /// terminator will be appended to the string.
  /// dt_ctx -- date/time format context
  /// d -- the date value
  /// t -- the time value
  /// len -- the output buffer length (should be at least dt_ctx.fmt_exp_len + 1)
  /// buff -- the output string buffer (must be large enough to hold value)
  /// Return the number of characters copied in to the buffer (excluding terminator).
  static int Format(const DateTimeFormatContext& dt_ctx,
      const boost::gregorian::date& d, const boost::posix_time::time_duration& t,
      int len, char* buff);

 private:
  static bool ParseDateTime(const char* str, int str_len,
      const DateTimeFormatContext& dt_ctx, DateTimeParseResult* dt_result);

  /// Check if the string is a TimeZone offset token.
  /// Valid offset token format are 'hh:mm', 'hhmm', 'hh'.
  static bool IsValidTZOffset(const char* str_begin, const char* str_end);

  /// Constants to hold default format lengths.
  static const int DEFAULT_DATE_FMT_LEN = 10;
  static const int DEFAULT_TIME_FMT_LEN = 8;
  static const int DEFAULT_TIME_FRAC_FMT_LEN = 18;
  static const int DEFAULT_SHORT_DATE_TIME_FMT_LEN = 19;
  static const int DEFAULT_DATE_TIME_FMT_LEN = 29;

  /// Used to indicate if the parsing state has been initialized.
  static bool initialized_;
  /// Pseudo-constant default date/time contexts. Backwards compatibility is provided on
  /// variable length fractional components by defining a format context for each expected
  /// length (0 - 9). This logic will be refactored when the parser supports lazy token
  /// groups.
  static DateTimeFormatContext DEFAULT_SHORT_DATE_TIME_CTX;
  static DateTimeFormatContext DEFAULT_SHORT_ISO_DATE_TIME_CTX;
  static DateTimeFormatContext DEFAULT_DATE_CTX;
  static DateTimeFormatContext DEFAULT_TIME_CTX;
  static DateTimeFormatContext DEFAULT_DATE_TIME_CTX[10];
  static DateTimeFormatContext DEFAULT_ISO_DATE_TIME_CTX[10];
  static DateTimeFormatContext DEFAULT_TIME_FRAC_CTX[10];
};

}

#endif
