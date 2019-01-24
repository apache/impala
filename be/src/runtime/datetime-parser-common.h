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

#include <boost/date_time/posix_time/ptime.hpp>
#include "gutil/macros.h"
#include <unordered_set>
#include <vector>

#include "runtime/timestamp-value.h"
#include "udf/udf.h"

namespace impala {

using impala_udf::FunctionContext;
using impala_udf::StringVal;

// Impala provides multiple algorithms to parse datetime formats:
//   - SimpleDateFormat: This is the one that is traditionally used with functions such
//     as to_timestamp() and from_timestamp().
//   - ISO SQL:2016 compliant datetime pattern matching. CAST(..FORMAT..) comes with
//     support for this pattern only.
// This is a collection of the logic that is shared between the 2 types of pattern
// matching including result codes, error reporting, format token types etc.
namespace datetime_parse_util {
const int MONTH_LENGTHS[12] = {31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31};

const int FRACTIONAL_SECOND_MAX_LENGTH = 9;

// Describes ranges for months in a non-leap year expressed as number of days since
// January 1.
const std::vector<int> MONTH_RANGES = {
    0, 31, 59, 90, 120, 151, 181, 212, 243, 273, 304, 334, 365 };

// Describes ranges for months in a leap year expressed as number of days since January 1.
const std::vector<int> LEAP_YEAR_MONTH_RANGES = {
    0, 31, 60, 91, 121, 152, 182, 213, 244, 274, 305, 335, 366 };

// Contains all the possible result codes that can come from parsing a datetime format
// pattern.
enum FormatTokenizationResult {
  SUCCESS,
  GENERAL_ERROR,
  DUPLICATE_FORMAT,
  YEAR_WITH_ROUNDED_YEAR_ERROR,
  CONFLICTING_YEAR_TOKENS_ERROR,
  DAY_OF_YEAR_TOKEN_CONFLICT,
  CONFLICTING_HOUR_TOKENS_ERROR,
  CONFLICTING_MERIDIEM_TOKENS_ERROR,
  MERIDIEM_CONFLICTS_WITH_HOUR_ERROR,
  MISSING_HOUR_TOKEN_ERROR,
  SECOND_IN_DAY_CONFLICT,
  TOO_LONG_FORMAT_ERROR,
  TIMEZONE_OFFSET_NOT_ALLOWED_ERROR,
  MISSING_TZH_TOKEN_ERROR,
  DATE_WITH_TIME_ERROR,
  CONFLICTING_FRACTIONAL_SECOND_TOKENS_ERROR
};

/// Holds all the token types that serve as building blocks for datetime format patterns.
enum DateTimeFormatTokenType {
  UNKNOWN = 0,
  SEPARATOR,
  YEAR,
  ROUND_YEAR,
  MONTH_IN_YEAR,
  MONTH_IN_YEAR_SLT,
  DAY_IN_MONTH,
  DAY_IN_YEAR,
  HOUR_IN_DAY,
  HOUR_IN_HALF_DAY,
  MINUTE_IN_HOUR,
  SECOND_IN_DAY,
  SECOND_IN_MINUTE,
  FRACTION,
  TZ_OFFSET,
  TIMEZONE_HOUR,
  TIMEZONE_MIN,
  MERIDIEM_INDICATOR,
  ISO8601_TIME_INDICATOR,
  ISO8601_ZULU_INDICATOR
};

/// Indicates whether the cast is a 'datetime to string' or a 'string to datetime' cast.
/// PARSE is a string type to datetime type cast.
/// FORMAT is a datetime type to string type cast.
enum CastDirection {
  PARSE,
  FORMAT
};

typedef std::pair<const char*, const char*> MERIDIEM_INDICATOR_TEXT;
const MERIDIEM_INDICATOR_TEXT AM = {"AM", "am"};
const MERIDIEM_INDICATOR_TEXT AM_LONG = {"A.M.", "a.m."};
const MERIDIEM_INDICATOR_TEXT PM = {"PM", "pm"};
const MERIDIEM_INDICATOR_TEXT PM_LONG = {"P.M.", "p.m."};

/// Stores metadata about a token within a datetime format.
struct DateTimeFormatToken {
  /// Indicates the type of datetime format token.
  DateTimeFormatTokenType type;
  /// The position of where this token is supposed to start in the datetime string
  /// to be parsed.
  int pos;
  /// The length of the token.
  int len;
  /// A pointer to the beginning of this token in the format string.
  const char* val;

  DateTimeFormatToken(DateTimeFormatTokenType type, int pos, int len, const char* val)
    : type(type),
      pos(pos),
      len(len),
      val(val) {
  }
};

/// Holds metadata about the datetime format. In the format parsing process the members of
/// this struct are populated gradually as the process advances. After the parsing process
/// this holds the found format tokens alongside with auxiliary information such as
/// whether the input format contains date or time tokens or both.
struct DateTimeFormatContext {
  /// Pointer to the beginning of the format string.
  const char* fmt;
  /// Length of the format string.
  int fmt_len;
  /// Expected length of the output of a 'datetime to string' cast. This usually equals to
  /// the length of the input format string. However, there are some edge cases where this
  /// is not true:
  ///   - SimpleDateFormat parsing on '2019-11-10' as input and 'yyyy-d-m' as format
  ///     produces output that is longer than the format string.
  ///   - ISO SQL parsing has token types where the output length is different from the
  ///     token length like: 'MONTH', 'DAY', 'HH12', 'HH24', FF1, FF2, FF4, etc.
  int fmt_out_len;
  /// Vector of tokens found in the format string.
  std::vector<DateTimeFormatToken> toks;
  bool has_date_toks;
  bool has_time_toks;
  /// Used for casting with SimpleDateFormat to handle rounded year. Make sure you call
  /// SetCenturyBreakAndCurrentTime() before using this member.
  boost::posix_time::ptime century_break_ptime;
  /// Used for round year and less than 4-digit year calculation in ISO:SQL:2016 parsing.
  /// Make sure you call SetCenturyBreakAndCurrentTime() before using this member. Not
  /// owned by this object.
  const TimestampValue* current_time;

  DateTimeFormatContext() {
    Reset(nullptr);
  }

  DateTimeFormatContext(const char* fmt) {
    Reset(fmt);
  }

  DateTimeFormatContext(const char* fmt, int fmt_len) {
    Reset(fmt, fmt_len);
  }

  /// Set the century break for parsing 1 or 2-digit year format. When parsing 1 or
  /// 2-digit year, the year should be in the interval [now - 80 years, now + 20 years),
  /// according to Hive. Also sets the current time that is used for round year
  /// calculation in ISO:SQL:2016 parsing.
  void SetCenturyBreakAndCurrentTime(const TimestampValue& now);

  /// Initializes all the members of this object.
  void Reset(const char* fmt, int fmt_len);

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
  bool realign_year = false;
};

/// This function is used to indicate an error or warning when the input format
/// tokenization fails for some reason. Constructs an error message based on 'error_type'
/// and pushes it to 'context'. Depending on 'is_error' the message can be an error or
/// warning.
void ReportBadFormat(FunctionContext* context, FormatTokenizationResult error_type,
    const StringVal& format, bool is_error);

bool ParseAndValidate(const char* token, int token_len, int min, int max,
    int* result) WARN_UNUSED_RESULT;

bool ParseFractionToken(const char* token, int token_len,
    DateTimeParseResult* result) WARN_UNUSED_RESULT;

inline bool IsLeapYear(int year) {
  return year % 4 == 0 && (year % 100 != 0 || year % 400 == 0);
}

/// Given the year, month and the day in month calculates the day in year using
/// MONTH_LENGTHS.
int GetDayInYear(int year, int month, int day_in_month);

/// Gets a year and the number of days passed since 1st of January that year. Calculates
/// the month and the day of that year. Returns false if any of the in parameters are
/// invalid e.g. if calling this function with a non-leap year and 'days_since_jan1' is
/// 365. Returns true on success.
bool GetMonthAndDayFromDaysSinceJan1(int year, int days_since_jan1, int* month, int* day)
    WARN_UNUSED_RESULT;
}
}
