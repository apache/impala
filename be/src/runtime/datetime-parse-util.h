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

#include <vector>
#include <boost/date_time/gregorian/gregorian.hpp>
#include <boost/date_time/posix_time/ptime.hpp>

#include "common/logging.h"
#include "runtime/string-value.inline.h"

namespace impala {

class TimestampValue;

/// This namespace contains helper functions and types to parse default and custom
/// formatted date-time values. They are used in TimestampParser and DateParser classes.
namespace datetime_parse_util {

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
  /// Current time - 80 years to determine the actual year when
  /// parsing 1 or 2-digit year token.
  boost::posix_time::ptime century_break_ptime;

  DateTimeFormatContext() {
    Reset(nullptr);
  }

  DateTimeFormatContext(const char* fmt) {
    Reset(fmt);
  }

  DateTimeFormatContext(const char* fmt, int fmt_len) {
    Reset(fmt, fmt_len);
  }

  /// Set the century break when parsing 1 or 2-digit year format.
  /// When parsing 1 or 2-digit year, the year should be in the interval
  /// [now - 80 years, now + 20 years), according to Hive.
  void SetCenturyBreak(const TimestampValue& now);

  void Reset(const char* fmt, int fmt_len) {
    this->fmt = fmt;
    this->fmt_len = fmt_len;
    this->fmt_out_len = fmt_len;
    this->has_date_toks = false;
    this->has_time_toks = false;
    this->toks.clear();
    this->century_break_ptime = boost::posix_time::not_a_date_time;
  }

  void Reset(const char* fmt) {
    Reset(fmt, (fmt == nullptr) ? 0 : strlen(fmt));
  }
};

/// Stores the results of parsing a date/time string.
struct DateTimeParseResult {
  int year = -1;
  int month = 0;
  int day = 0;
  int hour = 0;
  int minute = 0;
  int second = 0;
  int32_t fraction = 0;
  boost::posix_time::time_duration tz_offset =
      boost::posix_time::time_duration(0, 0, 0, 0);
  // Whether to realign the year for 2-digit year format
  bool realign_year = false;
};

/// Constants to hold default format lengths.
const int DEFAULT_DATE_FMT_LEN = 10;
const int DEFAULT_TIME_FMT_LEN = 8;
const int DEFAULT_TIME_FRAC_FMT_LEN = 18;
const int DEFAULT_SHORT_DATE_TIME_FMT_LEN = 19;
const int DEFAULT_DATE_TIME_FMT_LEN = 29;

/// Pseudo-constant default date/time contexts. Backwards compatibility is provided on
/// variable length fractional components by defining a format context for each expected
/// length (0 - 9). This logic will be refactored when the parser supports lazy token
/// groups.
extern DateTimeFormatContext DEFAULT_SHORT_DATE_TIME_CTX;
extern DateTimeFormatContext DEFAULT_SHORT_ISO_DATE_TIME_CTX;
extern DateTimeFormatContext DEFAULT_DATE_CTX;
extern DateTimeFormatContext DEFAULT_TIME_CTX;
extern DateTimeFormatContext DEFAULT_DATE_TIME_CTX[10];
extern DateTimeFormatContext DEFAULT_ISO_DATE_TIME_CTX[10];
extern DateTimeFormatContext DEFAULT_TIME_FRAC_CTX[10];

/// Initialize the static parser context which includes default date/time formats and
/// lookup tables. This *must* be called before any of the Parse* related functions can
/// be used.
void InitParseCtx();

/// Return true iff InitParseCtx() has been called.
bool IsParseCtxInitialized();

/// Parse the date/time format into tokens and place them in the context.
/// dt_ctx -- date/time format context
/// accept_time_toks -- if true, time tokens are accepted. Otherwise time tokens are
/// rejected.
/// Return true if the parse was successful.
bool ParseFormatTokens(DateTimeFormatContext* dt_ctx, bool accept_time_toks = true);

/// Parse the date/time string to generate the DateTimeFormatToken required by
/// DateTimeFormatContext. Similar to ParseFormatTokens() this function will take the
/// string and length, then heuristically determine whether the value contains date
/// tokens, time tokens, or both. Unlike ParseFormatTokens, it does not require the
/// template format string.
/// dt_ctx -- date/time format context (must contain valid tokens)
/// accept_time_toks -- if true, time tokens are accepted, otherwise time tokens are
/// rejected.
/// accept_time_toks_only -- if true, time tokens w/o date tokens are accepted. Otherwise,
/// they are rejected.
/// Return true if the date/time was successfully parsed.
bool ParseFormatTokensByStr(DateTimeFormatContext* dt_ctx, bool accept_time_toks = true,
    bool accept_time_toks_only = true);

/// Parse date/time string to find the corresponding default date/time format context. The
/// string must adhere to a default date/time format.
/// str -- valid pointer to the string to parse.
/// len -- length of the string to parse (must be > 0)
/// accept_time_toks -- if true, time tokens are accepted. Otherwise time tokens are
/// rejected.
/// accept_time_toks_only -- if true, time tokens without date tokens are accepted.
/// Otherwise, they are rejected.
/// Return the corresponding default format context if parsing succeeded, or nullptr
/// otherwise.
const DateTimeFormatContext* ParseDefaultFormatTokensByStr(const char* str, int len,
    bool accept_time_toks, bool accept_time_toks_only);

/// Parse a date/time string. The data must adhere to the format, otherwise it will be
/// rejected i.e. no missing tokens.
/// ParseDateTime() does only a basic validation on the parsed date/time values. The
/// caller is responsible for implementing rigid data validation and range-check.
/// str -- valid pointer to the string to parse
/// len -- length of the string to parse (must be > 0)
/// dt_ctx -- date/time format context (must contain valid tokens)
/// dt_result -- the struct where the results of the parsing will be placed
/// Return true if the date/time was successfully parsed.
bool ParseDateTime(const char* str, int len, const DateTimeFormatContext& dt_ctx,
    DateTimeParseResult* dt_result);

}

}
