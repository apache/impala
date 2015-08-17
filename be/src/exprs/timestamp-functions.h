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


#ifndef IMPALA_EXPRS_TIMESTAMP_FUNCTIONS_H
#define IMPALA_EXPRS_TIMESTAMP_FUNCTIONS_H

#include <boost/date_time/posix_time/posix_time.hpp>
#include <boost/date_time/gregorian/gregorian.hpp>
#include <boost/date_time/time_zone_base.hpp>
#include <boost/date_time/local_time/local_time.hpp>
#include <boost/thread/thread.hpp>

#include "runtime/string-value.h"
#include "runtime/timestamp-value.h"
#include "udf/udf.h"

using namespace impala_udf;

using namespace impala_udf;

namespace impala {

class Expr;
class OpcodeRegistry;
class TupleRow;

/// TODO: Reconsider whether this class needs to exist.
class TimestampFunctions {
 public:
  // To workaround IMPALA-1675 (boost doesn't throw an error for very large intervals),
  // define the max intervals.
  static const int64_t MAX_YEAR;
  static const int64_t MIN_YEAR;
  static const int64_t MAX_YEAR_INTERVAL;
  static const int64_t MAX_MONTH_INTERVAL;
  static const int64_t MAX_WEEK_INTERVAL;
  static const int64_t MAX_DAY_INTERVAL;
  static const int64_t MAX_HOUR_INTERVAL;
  static const int64_t MAX_MINUTE_INTERVAL;
  static const int64_t MAX_SEC_INTERVAL;
  static const int64_t MAX_MILLI_INTERVAL;
  static const int64_t MAX_MICRO_INTERVAL;

  /// Parse and initialize format string if it is a constant. Raise error if invalid.
  static void UnixAndFromUnixPrepare(FunctionContext* context,
      FunctionContext::FunctionStateScope scope);
  static void UnixAndFromUnixClose(FunctionContext* context,
      FunctionContext::FunctionStateScope scope);

  /// Parses 'string_val' based on the format 'fmt'.
  static BigIntVal Unix(FunctionContext* context, const StringVal& string_val,
      const StringVal& fmt);
  /// Converts 'tv_val' to a unix time_t
  static BigIntVal Unix(FunctionContext* context, const TimestampVal& tv_val);
  /// Returns the current time.
  static BigIntVal Unix(FunctionContext* context);

  // Functions to convert to and from TimestampVal type
  static TimestampVal ToTimestamp(FunctionContext* context, const BigIntVal& bigint_val);
  static TimestampVal ToTimestamp(FunctionContext* context, const StringVal& date,
      const StringVal& fmt);
  static StringVal FromTimestamp(FunctionContext* context, const TimestampVal& date,
      const StringVal& fmt);

  static StringVal StringValFromTimestamp(FunctionContext* context, TimestampValue tv,
      const StringVal& fmt);
  static BigIntVal UnixFromString(FunctionContext* context, const StringVal& sv);

  /// Return a timestamp string from a unix time_t
  /// Optional second argument is the format of the string.
  /// TIME is the integer type of the unix time argument.
  template <class TIME>
  static StringVal FromUnix(FunctionContext* context, const TIME& unix_time);
  template <class TIME>
  static StringVal FromUnix(FunctionContext* context, const TIME& unix_time,
      const StringVal& fmt);

  /// Convert a timestamp to or from a particular timezone based time.
  static TimestampVal FromUtc(FunctionContext* context,
    const TimestampVal& ts_val, const StringVal& tz_string_val);
  static TimestampVal ToUtc(FunctionContext* context,
      const TimestampVal& ts_val, const StringVal& tz_string_val);

  /// Returns the day's name as a string (e.g. 'Saturday').
  static StringVal DayName(FunctionContext* context, const TimestampVal& dow);

  /// Functions to extract parts of the timestamp, return integers.
  static IntVal Year(FunctionContext* context, const TimestampVal& ts_val);
  static IntVal Month(FunctionContext* context, const TimestampVal& ts_val);
  static IntVal DayOfWeek(FunctionContext* context, const TimestampVal& ts_val);
  static IntVal DayOfMonth(FunctionContext* context, const TimestampVal& ts_val);
  static IntVal DayOfYear(FunctionContext* context, const TimestampVal& ts_val);
  static IntVal WeekOfYear(FunctionContext* context, const TimestampVal& ts_val);
  static IntVal Hour(FunctionContext* context, const TimestampVal& ts_val);
  static IntVal Minute(FunctionContext* context, const TimestampVal& ts_val);
  static IntVal Second(FunctionContext* context, const TimestampVal& ts_val);

  /// Date/time functions.
  static TimestampVal Now(FunctionContext* context);
  static StringVal ToDate(FunctionContext* context, const TimestampVal& ts_val);
  static IntVal DateDiff(FunctionContext* context, const TimestampVal& ts_val1,
      const TimestampVal& ts_val2);
  static string ShortDayName(FunctionContext* context, const TimestampVal& ts);
  static string ShortMonthName(FunctionContext* context, const TimestampVal& ts);

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

  /// Helper function to check date/time format strings.
  /// TODO: eventually return format converted from Java to Boost.
  static StringValue* CheckFormat(StringValue* format);

  /// Issue a warning for a bad format string.
  static void ReportBadFormat(FunctionContext* context,
      const StringVal& format, bool is_error);

 private:
  /// Static result values for DayName() function.
  static const char* MONDAY;
  static const char* TUESDAY;
  static const char* WEDNESDAY;
  static const char* THURSDAY;
  static const char* FRIDAY;
  static const char* SATURDAY;
  static const char* SUNDAY;
};

/// Functions to load and access the timestamp database.
class TimezoneDatabase {
 public:
   TimezoneDatabase();
   ~TimezoneDatabase();

  /// Converts the name of a timezone to a boost timezone object.
  /// Some countries change their timezones, the tiemstamp is required to correctly
  /// determine the timezone information.
  static boost::local_time::time_zone_ptr FindTimezone(const std::string& tz,
      const TimestampValue& tv);

  /// Moscow Timezone No Daylight Savings Time (GMT+4), for use after March 2011
  static const boost::local_time::time_zone_ptr TIMEZONE_MSK_PRE_2011_DST;

 private:
  static const char* TIMEZONE_DATABASE_STR;
  static boost::local_time::tz_database tz_database_;
  static std::vector<std::string> tz_region_list_;
};

} // namespace impala

#endif
