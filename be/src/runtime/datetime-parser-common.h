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
#include <unordered_map>
#include <unordered_set>
#include <vector>

#include "exprs/timestamp-functions.h"
#include "runtime/timestamp-value.h"
#include "udf/udf.h"

namespace impala {

class TimestampValue;

using impala_udf::FunctionContext;
using impala_udf::StringVal;

/// Impala provides multiple algorithms to parse datetime formats:
///   - SimpleDateFormat: This is the one that is traditionally used with functions such
///     as to_timestamp() and from_timestamp().
///   - ISO SQL:2016 compliant datetime pattern matching. CAST(..FORMAT..) comes with
///     support for this pattern only.
/// This is a collection of the logic that is shared between the 2 types of pattern
/// matching including result codes, error reporting, format token types etc.
namespace datetime_parse_util {
const int FRACTIONAL_SECOND_MAX_LENGTH = 9;

/// Describes ranges for months in a non-leap year expressed as number of days since
/// January 1.
const std::vector<int> MONTH_RANGES = {
    0, 31, 59, 90, 120, 151, 181, 212, 243, 273, 304, 334, 365 };

/// Describes ranges for months in a leap year expressed as number of days since
/// January 1.
const std::vector<int> LEAP_YEAR_MONTH_RANGES = {
    0, 31, 60, 91, 121, 152, 182, 213, 244, 274, 305, 335, 366 };

/// Maps the 3-letter prefix of a month name to the suffix of the month name and the
/// ordinal number of month. The key of this map can be used to uniquely identify the
/// month while the suffix part of the value can be used for checking if the full month
/// name was given correctly in the input of a string to datetime conversion. The number
/// part of the value can be used as a result of the string to datetime conversion.
const std::unordered_map<std::string, std::pair<std::string, int>>
    MONTH_PREFIX_TO_SUFFIX = {
        {"jan", {"uary", 1}},
        {"feb", {"ruary", 2}},
        {"mar", {"ch", 3}},
        {"apr", {"il", 4}},
        {"may", {"", 5}},
        {"jun", {"e", 6}},
        {"jul", {"y", 7}},
        {"aug", {"ust", 8}},
        {"sep", {"tember", 9}},
        {"oct", {"ober", 10}},
        {"nov", {"ember", 11}},
        {"dec", {"ember", 12}}
};

/// Similar to 'MONTH_PREFIX_TO_SUFFIX' but maps the 3-letter prefix of a day name to the
/// suffix of the day name and the ordinal number of the day (1 means Monday and 7 means
/// Sunday).
const std::unordered_map<std::string, std::pair<std::string, int>>
    DAY_PREFIX_TO_SUFFIX = {
        {"mon", {"day", 1}},
        {"tue", {"sday", 2}},
        {"wed", {"nesday", 3}},
        {"thu", {"rsday", 4}},
        {"fri", {"day", 5}},
        {"sat", {"urday", 6}},
        {"sun", {"day", 7}}
};

/// Length of short month names like 'JAN', 'FEB', etc.
const int SHORT_MONTH_NAME_LENGTH = 3;

/// Length of the longest month name 'SEPTEMBER'.
const int MAX_MONTH_NAME_LENGTH = 9;

/// Length of short day names like 'MON', 'TUE', etc.
const int SHORT_DAY_NAME_LENGTH = 3;

/// Length of the longest day name 'WEDNESDAY'.
const int MAX_DAY_NAME_LENGTH = 9;

/// Contains all the possible result codes that can come from parsing a datetime format
/// pattern.
enum FormatTokenizationResult {
  SUCCESS,
  GENERAL_ERROR,
  DUPLICATE_FORMAT,
  YEAR_WITH_ROUNDED_YEAR_ERROR,
  CONFLICTING_YEAR_TOKENS_ERROR,
  CONFLICTING_MONTH_TOKENS_ERROR,
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
  CONFLICTING_FRACTIONAL_SECOND_TOKENS_ERROR,
  TEXT_TOKEN_NOT_CLOSED,
  NO_DATE_TOKENS_ERROR,
  NO_DATETIME_TOKENS_ERROR,
  MISPLACED_FX_MODIFIER_ERROR,
  QUARTER_NOT_ALLOWED_FOR_PARSING,
  DAY_OF_WEEK_NOT_ALLOWED_FOR_PARSING,
  DAY_NAME_NOT_ALLOWED_FOR_PARSING,
  WEEK_NUMBER_NOT_ALLOWED_FOR_PARSING,
  CONFLICTING_DAY_OF_WEEK_TOKENS_ERROR,
  MISSING_ISO8601_WEEK_BASED_TOKEN_ERROR,
  CONFLICTING_DATE_TOKENS_ERROR
};

/// Holds all the token types that serve as building blocks for datetime format patterns.
enum DateTimeFormatTokenType {
  UNKNOWN = 0,
  SEPARATOR,
  YEAR,
  ROUND_YEAR,
  MONTH_IN_YEAR,
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
  ISO8601_ZULU_INDICATOR,
  TEXT,
  FM_MODIFIER,
  FX_MODIFIER,
  MONTH_NAME,
  MONTH_NAME_SHORT,
  DAY_NAME,
  DAY_NAME_SHORT,
  DAY_OF_WEEK,
  QUARTER_OF_YEAR,
  WEEK_OF_YEAR,
  WEEK_OF_MONTH,
  ISO8601_WEEK_NUMBERING_YEAR,
  ISO8601_WEEK_OF_YEAR,
  ISO8601_DAY_OF_WEEK
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
  /// True if FM modifier is active for this token. This overrides the FX modifier active
  /// for the whole format.
  bool fm_modifier;

  /// True if this is a text token that is surrounded by escaped double quotes making the
  /// content of the token double-escaped.
  bool is_double_escaped;

  /// Helper for fast div/modulo on FRACTION and YEAR tokens.
  int divisor;

  DateTimeFormatToken(DateTimeFormatTokenType type, int pos, int len, const char* val)
    : type(type),
      pos(pos),
      len(len),
      val(val),
      fm_modifier(false),
      is_double_escaped(false),
      divisor(1000000000) {}
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
  /// In those edge cases, 'fmt_out_len' will be set as maximum possible length that might
  /// be produced from the input format string.
  int fmt_out_len;
  /// Vector of tokens found in the format string.
  std::vector<DateTimeFormatToken> toks;
  bool has_date_toks;
  bool has_time_toks;

  /// True if the format contains an FX modifier effective for all the tokens.
  bool fx_modifier;

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

// Given the month calculates the quarter of year.
int GetQuarter(int month);

bool ParseFractionToken(const char* token, int token_len,
    DateTimeParseResult* result) WARN_UNUSED_RESULT;

/// Gets a month name token (either full or short name) and converts it to the ordinal
/// number of month between 1 and 12. Make sure 'tok.type' is either MONTH_NAME or
/// MONTH_NAME_SHORT. Result is stored in 'month'. Returns false if the given month name
/// is invalid. 'fx_modifier' indicates if there is an active FX modifier on the whole
/// format.
/// If the month part of the input is not followed by a separator then the end of the
/// month part is found using MONTH_PREFIX_TO_SUFFIX. First, the 3 letter prefix of the
/// month name identifies a particular month and then checks if the rest of the month
/// name matches. If it does then '*token_end' is adjusted to point to the character
/// right after the end of the month part.
bool ParseMonthNameToken(const DateTimeFormatToken& tok, const char* token_start,
    const char** token_end, bool fx_modifier, int* month)
    WARN_UNUSED_RESULT;

/// Gets a day name token (either full or short name) and converts it to the ordinal
/// number of day between 1 and 7. Make sure 'tok.type' is either DAY_NAME or
/// DAY_NAME_SHORT.
/// Result is stored in 'day'. Returns false if the given day name is invalid.
/// 'fx_modifier' indicates if there is an active FX modifier on the whole format.
/// If the day part of the input is not followed by a separator then the end of the day
/// part is found using DAY_PREFIX_TO_SUFFIX. First, the 3 letter prefix of the day name
/// identifies a particular day and then checks if the rest of the day name matches. If it
/// does then '*token_end' is adjusted to point to the character right after the end of
/// the day part.
bool ParseDayNameToken(const DateTimeFormatToken& tok, const char* token_start,
    const char** token_end, bool fx_modifier, int* day)
    WARN_UNUSED_RESULT;

inline bool IsLeapYear(int year) {
  return year % 4 == 0 && (year % 100 != 0 || year % 400 == 0);
}

/// Given the year, month and the day in month calculates the day in year.
int GetDayInYear(int year, int month, int day_in_month);

/// Gets a year and the number of days passed since 1st of January that year. Calculates
/// the month and the day of that year. Returns false if any of the in parameters are
/// invalid e.g. if calling this function with a non-leap year and 'days_since_jan1' is
/// 365. Returns true on success.
bool GetMonthAndDayFromDaysSinceJan1(int year, int days_since_jan1, int* month, int* day)
    WARN_UNUSED_RESULT;

// Receives a text token and gives its string formatted representation. This is used in
// a string to datetime conversion path.
std::string FormatTextToken(const DateTimeFormatToken& tok);

/// Taking 'num_of_month' this function provides the name of the month. Based on the
/// casing of the month format token in 'tok' this can format the results in 3 cases:
/// Capitalized, full lowercase and full uppercase. E.g. "March", "march" and "MARCH".
const std::string& FormatMonthName(int num_of_month, const DateTimeFormatToken& tok);

/// Gets 'day' as a number between 1 and 7 that represents the day of week where Sunday
/// is 1 and returns the name of the day. Based on the casing of the day format token in
/// 'tok' this can format the results in 3 cases: Capitalized, full lowercase and full
/// uppercase. E.g. "Monday", "monday" and "MONDAY".
const std::string& FormatDayName(int day, const DateTimeFormatToken& tok);

/// Returns how the output of a month or day token should be formatted. Make sure to
/// call this when 'tok.type' is any of the month or day name tokens.
TimestampFunctions::TextCase GetOutputCase(const DateTimeFormatToken& tok);

/// Given the year, month and the day in month calculates the week in year where the
/// first week of the year starts from 1st January.
int GetWeekOfYear(int year, int month, int day);

/// Given the day of month calculates the week in the month where the first week of the
/// month starts from the first day of the month.
int GetWeekOfMonth(int day);

/// Returns the year modulo 'adjust_factor'.
/// E.g. AdjustYearToLength(1789, 1000) returns 789.
int AdjustYearToLength(int year, int adjust_factor);
}

}
