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


#ifndef IMPALA_EXPRS_TIMESTAMP_FUNCTIONS_H
#define IMPALA_EXPRS_TIMESTAMP_FUNCTIONS_H

#include <map>

#include "common/status.h"
#include "udf/udf.h"

namespace impala {

using impala_udf::FunctionContext;
using impala_udf::AnyVal;
using impala_udf::BooleanVal;
using impala_udf::TinyIntVal;
using impala_udf::SmallIntVal;
using impala_udf::IntVal;
using impala_udf::BigIntVal;
using impala_udf::FloatVal;
using impala_udf::DoubleVal;
using impala_udf::TimestampVal;
using impala_udf::StringVal;
using impala_udf::DecimalVal;

class Expr;
class OpcodeRegistry;
class StringValue;
class TimestampValue;
class TupleRow;

/// TODO: Reconsider whether this class needs to exist.
class TimestampFunctions {
 public:
  /// To workaround a boost bug (where adding very large intervals to ptimes causes the
  /// value to wrap around instead or throwing an exception -- the root cause of
  /// IMPALA-1675), max interval value are defined below. Some values below are less than
  /// the minimum interval needed to trigger IMPALA-1675 but the values are greater or
  /// equal to the interval that would definitely result in an out of bounds value. The
  /// min and max year are also defined for manual error checking. The min / max years
  /// are derived from date(min_date_time).year() / date(max_date_time).year().
  static const int64_t MAX_YEAR = 9999;
  static const int64_t MIN_YEAR = 1400;
  static const int64_t MAX_YEAR_INTERVAL = MAX_YEAR - MIN_YEAR;
  static const int64_t MAX_MONTH_INTERVAL = MAX_YEAR_INTERVAL * 12;
  static const int64_t MAX_WEEK_INTERVAL = MAX_YEAR_INTERVAL * 53;
  static const int64_t MAX_DAY_INTERVAL = MAX_YEAR_INTERVAL * 366;
  static const int64_t MAX_HOUR_INTERVAL = MAX_DAY_INTERVAL * 24;
  static const int64_t MAX_MINUTE_INTERVAL = MAX_HOUR_INTERVAL * 60;
  static const int64_t MAX_SEC_INTERVAL = MAX_MINUTE_INTERVAL * 60;
  static const int64_t MAX_MILLI_INTERVAL = MAX_SEC_INTERVAL * 1000;
  static const int64_t MAX_MICRO_INTERVAL = MAX_MILLI_INTERVAL * 1000;

  /// Use this as the first index for SHORT_MONTH_NAMES, MONTH_NAMES etc.
  enum TextCase {CAPITALIZED, LOWERCASE, UPPERCASE};

  /// Contains the short names of the months in 3 different case: Capitalized, full
  /// lowercase and full uppercase.
  static const std::string SHORT_MONTH_NAMES[3][12];

  /// Contains the full names of the months in 3 different case: Capitalized, full
  /// lowercase and full uppercase. E.g. "January", "january", "JANUARY", etc.
  static const std::string MONTH_NAMES[3][12];

  /// Similar to MONTH_NAMES but here the values are padded with spaces to the maximum
  /// length.
  static const std::string MONTH_NAMES_PADDED[3][12];

  /// Contains the short day names in 3 different case: Capitalized, full
  /// lowercase and full uppercase. E.g. "Sun", "sun", "SUN", etc.
  static const std::string SHORT_DAY_NAMES[3][7];

  /// Contains the full names of the days in 3 different case: Capitalized, full
  /// lowercase and full uppercase. E.g. "Sunday", "sunday", "SUNDAY", etc.
  static const std::string DAY_NAMES[3][7];

  /// Similar to DAY_NAMES but here the values are padded with spaces to the maximum
  /// length.
  static const std::string DAY_NAMES_PADDED[3][7];

  /// Maps full and abbreviated lowercase names of day-of-week to an int in the 0-6 range.
  /// Sunday is mapped to 0 and Saturday is mapped to 6.
  /// It is used in NextDay() function.
  static const std::map<std::string, int> DAYNAME_MAP;

  /// Parse and initialize format string if it is a constant. Raise error if invalid.
  static void UnixAndFromUnixClose(FunctionContext* context,
      FunctionContext::FunctionStateScope scope);
  static void ToUnixPrepare(FunctionContext* context,
      FunctionContext::FunctionStateScope scope);
  static void FromUnixPrepare(FunctionContext* context,
      FunctionContext::FunctionStateScope scope);

  /// Parses 'string_val' based on the format 'fmt'.
  /// The time zone interpretation of the parsed timestamp is determined by
  /// query option use_local_tz_for_unix_timestamp_conversions. If it is true, the
  /// instance is interpreted as a local value. If the flag is false, UTC is assumed.
  static BigIntVal Unix(FunctionContext* context, const StringVal& string_val,
      const StringVal& fmt);

  /// Converts 'tv_val' to a unix time_t.
  /// The time zone interpretation of the specified timestamp is determined by
  /// query option use_local_tz_for_unix_timestamp_conversions. If it is true, the
  /// instance is interpreted as a local value. If the flag is false, UTC is assumed.
  static BigIntVal Unix(FunctionContext* context, const TimestampVal& tv_val);

  /// Returns the current time.
  /// The time zone interpretation of the current time is determined by
  /// query option use_local_tz_for_unix_timestamp_conversions. If it is true, the
  /// instance is interpreted as a local value. If the flag is false, UTC is assumed.
  static BigIntVal Unix(FunctionContext* context);

  /// Interpret 'tv_val' as a timestamp in UTC and convert to unix time in microseconds.
  static BigIntVal UtcToUnixMicros(FunctionContext* context, const TimestampVal& tv_val);

  // Functions to convert to and from TimestampVal type
  static TimestampVal ToTimestamp(FunctionContext* context, const BigIntVal& bigint_val);
  static TimestampVal ToTimestamp(FunctionContext* context, const StringVal& date,
      const StringVal& fmt);
  static StringVal FromTimestamp(FunctionContext* context, const TimestampVal& date,
      const StringVal& fmt);

  static StringVal StringValFromTimestamp(FunctionContext* context,
      const TimestampValue& tv, const StringVal& fmt);
  static BigIntVal UnixFromString(FunctionContext* context, const StringVal& sv);

  /// Return a timestamp string from a unix time_t
  /// Optional second argument is the format of the string.
  /// TIME is the integer type of the unix time argument.
  template <class TIME>
  static StringVal FromUnix(FunctionContext* context, const TIME& unix_time);
  template <class TIME>
  static StringVal FromUnix(FunctionContext* context, const TIME& unix_time,
      const StringVal& fmt);

  /// Return a timestamp in UTC from a unix time in microseconds.
  static TimestampVal UnixMicrosToUtcTimestamp(FunctionContext* context,
      const BigIntVal& unix_time_micros);

  /// Convert a timestamp to or from a particular timezone based time.
  static TimestampVal FromUtc(FunctionContext* context,
    const TimestampVal& ts_val, const StringVal& tz_string_val);
  static TimestampVal ToUtc(FunctionContext* context,
      const TimestampVal& ts_val, const StringVal& tz_string_val);

  /// Convert a timestamp in particular timezone to UTC unambiguously.
  /// If the conversion is unique or 'expect_pre_bool_val' is null, this function behaves
  /// consistently with ToUtc.
  /// In cases of ambiguous conversion (e.g., when the timestamp falls within the DST
  /// repeated interval), if 'expect_pre_bool_val' is true, it returns the previous
  /// possible value, otherwise it returns the posterior possible value.
  /// In cases of invalid conversion (e.g., when the timestamp falls within the DST
  /// skipped interval), if 'expect_pre_bool_val' is true, it returns the transition point
  /// value, otherwise it returns null.
  static TimestampVal ToUtcUnambiguous(FunctionContext* context,
      const TimestampVal& ts_val, const StringVal& tz_string_val,
      const BooleanVal& expect_pre_bool_val);

  /// Functions to extract parts of the timestamp, return integers.
  static IntVal Year(FunctionContext* context, const TimestampVal& ts_val);
  static IntVal Quarter(FunctionContext* context, const TimestampVal& ts_val);
  static IntVal Month(FunctionContext* context, const TimestampVal& ts_val);
  static IntVal DayOfWeek(FunctionContext* context, const TimestampVal& ts_val);
  static IntVal DayOfMonth(FunctionContext* context, const TimestampVal& ts_val);
  static IntVal DayOfYear(FunctionContext* context, const TimestampVal& ts_val);
  static IntVal WeekOfYear(FunctionContext* context, const TimestampVal& ts_val);
  static IntVal Hour(FunctionContext* context, const TimestampVal& ts_val);
  static IntVal Hour(FunctionContext* context, const StringVal& str_val);
  static IntVal Minute(FunctionContext* context, const TimestampVal& ts_val);
  static IntVal Minute(FunctionContext* context, const StringVal& str_val);
  static IntVal Second(FunctionContext* context, const TimestampVal& ts_val);
  static IntVal Second(FunctionContext* context, const StringVal& str_val);
  static IntVal Millisecond(FunctionContext* context, const TimestampVal& ts_val);
  static IntVal Millisecond(FunctionContext* context, const StringVal& str_val);

  /// Date/time functions.
  static TimestampVal Now(FunctionContext* context);
  static TimestampVal UtcTimestamp(FunctionContext* context);
  static StringVal ToDate(FunctionContext* context, const TimestampVal& ts_val);
  static IntVal DateDiff(FunctionContext* context, const TimestampVal& ts_val1,
      const TimestampVal& ts_val2);
  static std::string ShortDayName(FunctionContext* context, const TimestampVal& ts);
  static StringVal LongDayName(FunctionContext* context, const TimestampVal& ts);
  static std::string ShortMonthName(FunctionContext* context, const TimestampVal& ts);
  static StringVal LongMonthName(FunctionContext* context, const TimestampVal& ts);

  /// Return verbose string version of current time of day
  /// e.g. Mon Dec 01 16:25:05 2003 EST.
  static StringVal TimeOfDay(FunctionContext* context);

  /// Compare value of two timestamps.
  /// Return 0 if 'ts1' is equal to 'ts2'.
  /// Return 1 if 'ts1' is later than 'ts2'.
  /// Return -1 if 'ts1' is earlier than 'ts2'.
  static IntVal TimestampCmp(FunctionContext* context,
      const TimestampVal& ts_val1, const TimestampVal& ts_val2);

  /// Return total number of full months between two timestamps.
  /// If 'ts_val1' is later than 'ts_val2' result is positive if there is a gap
  /// of more than 1 month between the two dates.
  /// If 'ts_val1' is earlier than 'ts_val2' result is negative if there is a
  /// gap of more than 1 month between the two dates.
  /// If the gap between the two dates is less than a full month, result is
  /// zero.
  static IntVal IntMonthsBetween(FunctionContext* context,
      const TimestampVal& ts_val1, const TimestampVal& ts_val2);

  /// Return number of months between two timestamps:
  /// If 'ts_val1' is later than 'ts_val2' result is positive.
  /// If 'ts_val1' is earlier than 'ts_val2' result is negative.
  /// If 'ts_val1' and 'ts_val2' are both same days of the month or both last
  /// days of their respective months, result is an integer.
  /// Otherwise result includes a fractional portion based on 31 day month.
  static DoubleVal MonthsBetween(FunctionContext* context,
      const TimestampVal& ts_val1, const TimestampVal& ts_val2);

  /// Return the date of the weekday that follows a particular date.
  /// The 'weekday' argument is a string literal indicating the day of the week.
  /// Also this argument is case-insensitive. Available values are:
  /// "Sunday"/"SUN", "Monday"/"MON", "Tuesday"/"TUE",
  /// "Wednesday"/"WED", "Thursday"/"THU", "Friday"/"FRI", "Saturday"/"SAT".
  /// For example, the first Saturday after Wednesday, 25 December 2013
  /// is on 28 December 2013.
  /// select next_day('2013-12-25','Saturday') returns '2013-12-28'
  static TimestampVal NextDay(FunctionContext* context, const TimestampVal& date,
      const StringVal& weekday);

  /// Add/sub functions on the timestamp. This handles three forms of adding/subtracting
  /// intervals to/from timestamps:
  ///   1) ADD/SUB_<INTERVAL>(<TIMESTAMP>, <NUMBER>),
  ///        ex: ADD_DAYS(CAST('2015-01-01' AS TIMESTAMP), 1)
  ///            SUB_YEARS(CAST('2015-01-01' AS TIMESTAMP), 1)
  ///   2) <TIMESTAMP> +/- INTERVAL '<NUMBER>' <INTERVAL>,
  ///        ex: CAST('2015-01-01' AS TIMESTAMP) + INTERVAL '1' DAY
  ///            CAST('2015-01-01' AS TIMESTAMP) - INTERVAL '1' YEAR
  ///   3) DATE_ADD/SUB(<TIMESTAMP>, INTERVAL <NUMBER> <INTERVAL>)
  ///        ex: DATE_ADD(CAST('2015-01-01' AS TIMESTAMP), INTERVAL 1 DAY)
  ///            DATE_SUB(CAST('2015-01-01' AS TIMESTAMP), INTERVAL 1 YEAR)
  /// These three forms are provided for compatibility with other databases so we inherit
  /// their behavior. For all intervals except MONTH the three forms above produce the
  /// same results. MONTH is a special case where the result may differ if the input
  /// TIMESTAMP is the last day of the month (ADD_MONTH() always sets the result to the
  /// last day of the month if the input is the last day of the month). A value of true
  /// for the template parameter 'is_add_months_keep_last_day' corresponds to the
  /// ADD_MONTH() case.
  template <bool is_add, typename AnyIntVal, typename Interval,
      bool is_add_months_keep_last_day>
  static TimestampVal AddSub(FunctionContext* context, const TimestampVal& timestamp,
      const AnyIntVal& num_interval_units);

  /// Return the last date in the month of a specified input date.
  /// The TIMESTAMP argument requires a date component,
  /// it may or may not have a time component.
  /// The function will return a NULL TimestampVal when:
  ///   1) The TIMESTAMP argument is missing a date component.
  ///   2) The TIMESTAMP argument is outside of the supported range:
  ///         between 1400-01-31 00:00:00 and 9999-12-31 23:59:59
  static TimestampVal LastDay(FunctionContext* context, const TimestampVal& ts);

  /// Helper function to check date/time format strings.
  /// TODO: eventually return format converted from Java to Boost.
  static StringValue* CheckFormat(StringValue* format);
};

} // namespace impala

#endif
