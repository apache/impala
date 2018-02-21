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

#include "exprs/timestamp-functions.h"

#include <boost/date_time/compiler_config.hpp>
#include <boost/date_time/posix_time/posix_time_types.hpp>
#include <boost/date_time/gregorian/gregorian_types.hpp>
#include <ctime>
#include <gutil/strings/substitute.h>

#include "exprs/anyval-util.h"
#include "runtime/string-value.inline.h"
#include "runtime/timestamp-value.inline.h"
#include "runtime/timestamp-parse-util.h"
#include "runtime/timestamp-value.h"
#include "udf/udf.h"
#include "udf/udf-internal.h"

#include "common/names.h"

using boost::gregorian::greg_month;
using boost::gregorian::max_date_time;
using boost::gregorian::min_date_time;
using boost::posix_time::not_a_date_time;
using boost::posix_time::ptime;
using boost::posix_time::time_duration;
using namespace impala_udf;
using namespace strings;

typedef boost::gregorian::date Date;
typedef boost::gregorian::days Days;
typedef boost::gregorian::months Months;
typedef boost::gregorian::weeks Weeks;
typedef boost::gregorian::years Years;
typedef boost::posix_time::hours Hours;
typedef boost::posix_time::microseconds Microseconds;
typedef boost::posix_time::milliseconds Milliseconds;
typedef boost::posix_time::minutes Minutes;
typedef boost::posix_time::nanoseconds Nanoseconds;
typedef boost::posix_time::seconds Seconds;

namespace impala {

StringVal TimestampFunctions::StringValFromTimestamp(FunctionContext* context,
    const TimestampValue& tv, const StringVal& fmt) {
  void* state = context->GetFunctionState(FunctionContext::THREAD_LOCAL);
  DateTimeFormatContext* dt_ctx = reinterpret_cast<DateTimeFormatContext*>(state);
  if (!context->IsArgConstant(1)) {
    dt_ctx->Reset(reinterpret_cast<const char*>(fmt.ptr), fmt.len);
    if (!TimestampParser::ParseFormatTokens(dt_ctx)){
      TimestampFunctions::ReportBadFormat(context, fmt, false);
      return StringVal::null();
    }
  }

  int buff_len = dt_ctx->fmt_out_len + 1;
  StringVal result(context, buff_len);
  if (UNLIKELY(result.is_null)) return StringVal::null();
  result.len = tv.Format(*dt_ctx, buff_len, reinterpret_cast<char*>(result.ptr));
  if (result.len <= 0) return StringVal::null();
  return result;
}

template <class TIME>
StringVal TimestampFunctions::FromUnix(FunctionContext* context, const TIME& intp) {
  if (intp.is_null) return StringVal::null();
  const TimestampValue tv = TimestampValue::FromUnixTime(intp.val);
  if (!tv.HasDateAndTime()) return StringVal::null();
  return AnyValUtil::FromString(context, tv.ToString());
}

template <class TIME>
StringVal TimestampFunctions::FromUnix(FunctionContext* context, const TIME& intp,
    const StringVal& fmt) {
  if (fmt.is_null || fmt.len == 0) {
    TimestampFunctions::ReportBadFormat(context, fmt, false);
    return StringVal::null();
  }
  if (intp.is_null) return StringVal::null();

  const TimestampValue& t = TimestampValue::FromUnixTime(intp.val);
  return StringValFromTimestamp(context, t, fmt);
}

BigIntVal TimestampFunctions::Unix(FunctionContext* context, const StringVal& string_val,
    const StringVal& fmt) {
  const TimestampVal& tv_val = ToTimestamp(context, string_val, fmt);
  if (tv_val.is_null) return BigIntVal::null();
  const TimestampValue& tv = TimestampValue::FromTimestampVal(tv_val);
  time_t result;
  return (tv.ToUnixTime(&result)) ? BigIntVal(result) : BigIntVal::null();
}

BigIntVal TimestampFunctions::Unix(FunctionContext* context, const TimestampVal& ts_val) {
  if (ts_val.is_null) return BigIntVal::null();
  const TimestampValue& tv = TimestampValue::FromTimestampVal(ts_val);
  time_t result;
  return (tv.ToUnixTime(&result)) ? BigIntVal(result) : BigIntVal::null();
}

BigIntVal TimestampFunctions::Unix(FunctionContext* context) {
  time_t result;
  if (context->impl()->state()->now()->ToUnixTime(&result)) {
    return BigIntVal(result);
  } else {
    return BigIntVal::null();
  }
}

BigIntVal TimestampFunctions::UtcToUnixMicros(FunctionContext* context,
    const TimestampVal& ts_val) {
  if (ts_val.is_null) return BigIntVal::null();
  const TimestampValue& tv = TimestampValue::FromTimestampVal(ts_val);
  int64_t result;
  return (tv.UtcToUnixTimeMicros(&result)) ? BigIntVal(result) : BigIntVal::null();
}

TimestampVal TimestampFunctions::UnixMicrosToUtcTimestamp(FunctionContext* context,
    const BigIntVal& unix_time_micros) {
  if (unix_time_micros.is_null) return TimestampVal::null();
  TimestampValue tv = TimestampValue::UtcFromUnixTimeMicros(unix_time_micros.val);
  TimestampVal result;
  tv.ToTimestampVal(&result);
  return result;
}

TimestampVal TimestampFunctions::ToTimestamp(FunctionContext* context,
    const BigIntVal& bigint_val) {
  if (bigint_val.is_null) return TimestampVal::null();
  const TimestampValue& tv = TimestampValue::FromUnixTime(bigint_val.val);
  TimestampVal tv_val;
  tv.ToTimestampVal(&tv_val);
  return tv_val;
}

TimestampVal TimestampFunctions::ToTimestamp(FunctionContext* context,
    const StringVal& date, const StringVal& fmt) {
  if (fmt.is_null || fmt.len == 0) {
    TimestampFunctions::ReportBadFormat(context, fmt, false);
    return TimestampVal::null();
  }
  if (date.is_null || date.len == 0) return TimestampVal::null();
  void* state = context->GetFunctionState(FunctionContext::THREAD_LOCAL);
  DateTimeFormatContext* dt_ctx = reinterpret_cast<DateTimeFormatContext*>(state);
  if (!context->IsArgConstant(1)) {
     dt_ctx->Reset(reinterpret_cast<const char*>(fmt.ptr), fmt.len);
     if (!TimestampParser::ParseFormatTokens(dt_ctx)) {
       ReportBadFormat(context, fmt, false);
       return TimestampVal::null();
     }
  }
  const TimestampValue& tv = TimestampValue::Parse(
      reinterpret_cast<const char*>(date.ptr), date.len, *dt_ctx);
  TimestampVal tv_val;
  tv.ToTimestampVal(&tv_val);
  return tv_val;
}

StringVal TimestampFunctions::FromTimestamp(FunctionContext* context,
    const TimestampVal& date, const StringVal& fmt) {
  if (date.is_null) return StringVal::null();
  const TimestampValue& tv = TimestampValue::FromTimestampVal(date);
  if (!tv.HasDate()) return StringVal::null();
  return StringValFromTimestamp(context, tv, fmt);
}

BigIntVal TimestampFunctions::UnixFromString(FunctionContext* context,
    const StringVal& sv) {
  if (sv.is_null) return BigIntVal::null();
  const TimestampValue& tv = TimestampValue::Parse(
      reinterpret_cast<const char *>(sv.ptr), sv.len);
  time_t result;
  return (tv.ToUnixTime(&result)) ? BigIntVal(result) : BigIntVal::null();
}

void TimestampFunctions::ReportBadFormat(FunctionContext* context,
    const StringVal& format, bool is_error) {
  stringstream ss;
  const StringValue& fmt = StringValue::FromStringVal(format);
  if (format.is_null || format.len == 0) {
    ss << "Bad date/time conversion format: format string is NULL or has 0 length";
  } else {
    ss << "Bad date/time conversion format: " << fmt.DebugString();
  }
  if (is_error) {
    context->SetError(ss.str().c_str());
  } else {
    context->AddWarning(ss.str().c_str());
  }
}

IntVal TimestampFunctions::Year(FunctionContext* context, const TimestampVal& ts_val) {
  if (ts_val.is_null) return IntVal::null();
  const TimestampValue& ts_value = TimestampValue::FromTimestampVal(ts_val);
  if (!ts_value.HasDate()) return IntVal::null();
  return IntVal(ts_value.date().year());
}

IntVal TimestampFunctions::Month(FunctionContext* context, const TimestampVal& ts_val) {
  if (ts_val.is_null) return IntVal::null();
  const TimestampValue& ts_value = TimestampValue::FromTimestampVal(ts_val);
  if (!ts_value.HasDate()) return IntVal::null();
  return IntVal(ts_value.date().month());
}

IntVal TimestampFunctions::Quarter(FunctionContext* context, const TimestampVal& ts_val) {
  if (ts_val.is_null) return IntVal::null();
  const TimestampValue& ts_value = TimestampValue::FromTimestampVal(ts_val);
  if (!ts_value.HasDate()) return IntVal::null();
  int m = ts_value.date().month();
  return IntVal((m - 1) / 3 + 1);
}

IntVal TimestampFunctions::DayOfWeek(FunctionContext* context,
    const TimestampVal& ts_val) {
  if (ts_val.is_null) return IntVal::null();
  const TimestampValue& ts_value = TimestampValue::FromTimestampVal(ts_val);
  if (!ts_value.HasDate()) return IntVal::null();
  // Sql has the result in [1,7] where 1 = Sunday. Boost has 0 = Sunday.
  return IntVal(ts_value.date().day_of_week() + 1);
}

IntVal TimestampFunctions::DayOfMonth(FunctionContext* context,
    const TimestampVal& ts_val) {
  if (ts_val.is_null) return IntVal::null();
  const TimestampValue& ts_value = TimestampValue::FromTimestampVal(ts_val);
  if (!ts_value.HasDate()) return IntVal::null();
  return IntVal(ts_value.date().day());
}

IntVal TimestampFunctions::DayOfYear(FunctionContext* context,
    const TimestampVal& ts_val) {
  if (ts_val.is_null) return IntVal::null();
  const TimestampValue& ts_value = TimestampValue::FromTimestampVal(ts_val);
  if (!ts_value.HasDate()) return IntVal::null();
  return IntVal(ts_value.date().day_of_year());
}

IntVal TimestampFunctions::WeekOfYear(FunctionContext* context,
    const TimestampVal& ts_val) {
  if (ts_val.is_null) return IntVal::null();
  const TimestampValue& ts_value = TimestampValue::FromTimestampVal(ts_val);
  if (!ts_value.HasDate()) return IntVal::null();
  return IntVal(ts_value.date().week_number());
}

IntVal TimestampFunctions::Hour(FunctionContext* context, const TimestampVal& ts_val) {
  if (ts_val.is_null) return IntVal::null();
  const TimestampValue& ts_value = TimestampValue::FromTimestampVal(ts_val);
  if (!ts_value.HasTime()) return IntVal::null();
  return IntVal(ts_value.time().hours());
}

IntVal TimestampFunctions::Minute(FunctionContext* context, const TimestampVal& ts_val) {
  if (ts_val.is_null) return IntVal::null();
  const TimestampValue& ts_value = TimestampValue::FromTimestampVal(ts_val);
  if (!ts_value.HasTime()) return IntVal::null();
  return IntVal(ts_value.time().minutes());
}

IntVal TimestampFunctions::Second(FunctionContext* context, const TimestampVal& ts_val) {
  if (ts_val.is_null) return IntVal::null();
  const TimestampValue& ts_value = TimestampValue::FromTimestampVal(ts_val);
  if (!ts_value.HasTime()) return IntVal::null();
  return IntVal(ts_value.time().seconds());
}

IntVal TimestampFunctions::Millisecond(FunctionContext* context,
    const TimestampVal& ts_val) {
  if (ts_val.is_null) return IntVal::null();
  const TimestampValue& ts_value = TimestampValue::FromTimestampVal(ts_val);
  if (!ts_value.HasTime()) return IntVal::null();
  const boost::posix_time::time_duration& time = ts_value.time();
  return IntVal(time.total_milliseconds() - time.total_seconds() * 1000);
}

TimestampVal TimestampFunctions::Now(FunctionContext* context) {
  const TimestampValue* now = context->impl()->state()->now();
  TimestampVal return_val;
  now->ToTimestampVal(&return_val);
  return return_val;
}

TimestampVal TimestampFunctions::UtcTimestamp(FunctionContext* context) {
  const TimestampValue* utc_timestamp = context->impl()->state()->utc_timestamp();
  TimestampVal return_val;
  utc_timestamp->ToTimestampVal(&return_val);
  return return_val;
}

// Writes 'num' as ASCII into 'dst'. If necessary, adds leading zeros to make the ASCII
// representation exactly 'len' characters. Both 'num' and 'len' must be >= 0.
static inline void IntToChar(uint8_t* dst, int num, int len) {
  DCHECK_GE(len, 0);
  DCHECK_GE(num, 0);
  for (int i = len - 1; i >= 0; --i) {
    *(dst + i) = '0' + (num % 10);
    num /= 10;
  }
}

StringVal TimestampFunctions::ToDate(FunctionContext* context,
    const TimestampVal& ts_val) {
  if (ts_val.is_null) return StringVal::null();
  const TimestampValue ts_value = TimestampValue::FromTimestampVal(ts_val);
  // Defensively, return NULL if the timestamp does not have a date portion. Some of
  // our built-in functions might incorrectly return such a malformed timestamp.
  if (!ts_value.HasDate()) return StringVal::null();
  StringVal result(context, 10);
  result.len = 10;
  // Fill in year, month, and day.
  IntToChar(result.ptr, ts_value.date().year(), 4);
  IntToChar(result.ptr + 5, ts_value.date().month(), 2);
  IntToChar(result.ptr + 8, ts_value.date().day(), 2);
  // Fill in dashes.
  result.ptr[7] = '-';
  result.ptr[4] = '-';
  return result;
}

inline bool IsLeapYear(int year) {
  return year % 4 == 0 && (year % 100 != 0 || year % 400 == 0);
}

inline unsigned short GetLastDayOfMonth(int month, int year) {
  switch (month) {
    case 1: return 31;
    case 2: return IsLeapYear(year) ? 29 : 28;
    case 3: return 31;
    case 4: return 30;
    case 5: return 31;
    case 6: return 30;
    case 7: return 31;
    case 8: return 31;
    case 9: return 30;
    case 10: return 31;
    case 11: return 30;
    case 12: return 31;
    default:
      DCHECK(false);
      return -1;
  }
}

/// The functions below help workaround IMPALA-1675: if a very large interval is added
/// to a date, boost fails to throw an exception.
template <class Interval>
bool IsOverMaxInterval(const int64_t count) {
  DCHECK(false) << "NYI";
  return false;
}

template <>
inline bool IsOverMaxInterval<Years>(const int64_t val) {
  return val < -TimestampFunctions::MAX_YEAR_INTERVAL ||
      TimestampFunctions::MAX_YEAR_INTERVAL < val;
}

template <>
inline bool IsOverMaxInterval<Months>(const int64_t val) {
  return val < -TimestampFunctions::MAX_MONTH_INTERVAL ||
      TimestampFunctions::MAX_MONTH_INTERVAL < val;
}

template <>
inline bool IsOverMaxInterval<Weeks>(const int64_t val) {
  return val < -TimestampFunctions::MAX_WEEK_INTERVAL ||
      TimestampFunctions::MAX_WEEK_INTERVAL < val;
}

template <>
inline bool IsOverMaxInterval<Days>(const int64_t val) {
  return val < -TimestampFunctions::MAX_DAY_INTERVAL ||
      TimestampFunctions::MAX_DAY_INTERVAL < val;
}

template <>
inline bool IsOverMaxInterval<Hours>(const int64_t val) {
  return val < -TimestampFunctions::MAX_HOUR_INTERVAL ||
      TimestampFunctions::MAX_HOUR_INTERVAL < val;
}

template <>
inline bool IsOverMaxInterval<Minutes>(const int64_t val) {
  return val < -TimestampFunctions::MAX_MINUTE_INTERVAL ||
      TimestampFunctions::MAX_MINUTE_INTERVAL < val;
}

template <>
inline bool IsOverMaxInterval<Seconds>(const int64_t val) {
  return val < -TimestampFunctions::MAX_SEC_INTERVAL ||
      TimestampFunctions::MAX_SEC_INTERVAL < val;
}

template <>
inline bool IsOverMaxInterval<Milliseconds>(const int64_t val) {
  return val < -TimestampFunctions::MAX_MILLI_INTERVAL ||
      TimestampFunctions::MAX_MILLI_INTERVAL < val;
}

template <>
inline bool IsOverMaxInterval<Microseconds>(const int64_t val) {
  return val < -TimestampFunctions::MAX_MICRO_INTERVAL ||
      TimestampFunctions::MAX_MICRO_INTERVAL < val;
}

template <>
inline bool IsOverMaxInterval<Nanoseconds>(const int64_t val) {
  return false;
}

inline bool IsUnsupportedYear(int64_t year) {
  return year < TimestampFunctions::MIN_YEAR || TimestampFunctions::MAX_YEAR < year;
}

/// The AddInterval() functions provide a unified interface for adding intervals of all
/// types. To subtract, the 'interval' can be negative. 'context' and 'interval' are
/// input params, 'datetime' is an output param.
template <typename Interval>
inline void AddInterval(FunctionContext* context, int64_t interval, ptime* datetime) {
  *datetime += Interval(interval);
}

/// IMPALA-2086: Avoid boost changing Feb 28th into Feb 29th when the resulting year is
/// a leap year. Doing the work rather than using boost then adjusting the boost result
/// is a little faster (and about the same speed as the default boost logic).
template <>
inline void AddInterval<Years>(FunctionContext* context, int64_t interval,
    ptime* datetime) {
  const Date& date = datetime->date();
  int year = date.year() + interval;
  if (UNLIKELY(IsUnsupportedYear(year))) {
    context->AddWarning(Substitute("Add/sub year resulted in an out of range year: $0",
          year).c_str());
    *datetime = ptime(not_a_date_time);
  }
  greg_month month = date.month();
  int day = date.day().as_number();
  if (day == 29 && month == boost::gregorian::Feb && !IsLeapYear(year)) day = 28;
  *datetime = ptime(boost::gregorian::date(year, month, day), datetime->time_of_day());
}

string TimestampFunctions::ShortDayName(FunctionContext* context,
    const TimestampVal& ts) {
  if (ts.is_null) return NULL;
  IntVal dow = DayOfWeek(context, ts);
  DCHECK_GT(dow.val, 0);
  DCHECK_LT(dow.val, 8);
  return DAY_ARRAY[dow.val - 1];
}

StringVal TimestampFunctions::LongDayName(FunctionContext* context,
    const TimestampVal& ts) {
  if (ts.is_null) return StringVal::null();
  IntVal dow = DayOfWeek(context, ts);
  DCHECK_GT(dow.val, 0);
  DCHECK_LT(dow.val, 8);
  const string& day_name = DAYNAME_ARRAY[dow.val - 1];
  return StringVal(reinterpret_cast<uint8_t*>(const_cast<char*>(day_name.data())), day_name.size());
}

string TimestampFunctions::ShortMonthName(FunctionContext* context,
    const TimestampVal& ts) {
  if (ts.is_null) return NULL;
  IntVal mth = Month(context, ts);
  DCHECK_GT(mth.val, 0);
  DCHECK_LT(mth.val, 13);
  return MONTH_ARRAY[mth.val - 1];
}

StringVal TimestampFunctions::LongMonthName(FunctionContext* context,
    const TimestampVal& ts) {
  if (ts.is_null) return StringVal::null();
  IntVal mth = Month(context, ts);
  DCHECK_GT(mth.val, 0);
  DCHECK_LT(mth.val, 13);
  const string& mn = MONTHNAME_ARRAY[mth.val - 1];
  return StringVal(reinterpret_cast<uint8_t*>(const_cast<char*>(mn.data())), mn.size());
}

StringVal TimestampFunctions::TimeOfDay(FunctionContext* context) {
  const TimestampVal curr = Now(context);
  if (curr.is_null) return StringVal::null();
  const string& day = ShortDayName(context, curr);
  const string& month = ShortMonthName(context, curr);
  IntVal dayofmonth = DayOfMonth(context, curr);
  IntVal hour = Hour(context, curr);
  IntVal min = Minute(context, curr);
  IntVal sec = Second(context, curr);
  IntVal year = Year(context, curr);
  time_t rawtime;
  time(&rawtime);
  struct tm tzone;
  localtime_r(&rawtime, &tzone);
  stringstream result;
  result << day << " " << month << " " << setw(2) << setfill('0')
         << dayofmonth.val << " " << setw(2) << setfill('0') << hour.val << ":"
         << setw(2) << setfill('0') << min.val << ":"
         << setw(2) << setfill('0') << sec.val << " " << year.val << " "
         << tzone.tm_zone;
  return AnyValUtil::FromString(context, result.str());
}

IntVal TimestampFunctions::TimestampCmp(FunctionContext* context,
    const TimestampVal& ts1, const TimestampVal& ts2) {
  if (ts1.is_null || ts2.is_null) return IntVal::null();
  const TimestampValue& ts_value1 = TimestampValue::FromTimestampVal(ts1);
  const TimestampValue& ts_value2 = TimestampValue::FromTimestampVal(ts2);
  if (ts_value1 > ts_value2) return 1;
  if (ts_value1 < ts_value2) return -1;
  return 0;
}

DoubleVal TimestampFunctions::MonthsBetween(FunctionContext* context,
    const TimestampVal& ts1, const TimestampVal& ts2) {
  if (ts1.is_null || ts2.is_null) return DoubleVal::null();
  const TimestampValue& ts_value1 = TimestampValue::FromTimestampVal(ts1);
  const TimestampValue& ts_value2 = TimestampValue::FromTimestampVal(ts2);
  if (!ts_value1.HasDate() || !ts_value2.HasDate()) return DoubleVal::null();
  IntVal year1 = Year(context, ts1);
  IntVal year2 = Year(context, ts2);
  IntVal month1 = Month(context, ts1);
  IntVal month2 = Month(context, ts2);
  IntVal day1 = DayOfMonth(context, ts1);
  IntVal day2 = DayOfMonth(context, ts2);
  int days_diff = 0;
  // If both timestamps are last days of different months they don't contribute
  // a fractional value to the number of months, therefore there is no need to
  // calculate difference in their days.
  if (!(day1.val == GetLastDayOfMonth(month1.val, year1.val) &&
      day2.val == GetLastDayOfMonth(month2.val, year2.val))) {
    days_diff = day1.val - day2.val;
  }
  double months_between = (year1.val - year2.val) * 12 +
      month1.val - month2.val + (static_cast<double>(days_diff) / 31.0);
  return DoubleVal(months_between);
}

TimestampVal TimestampFunctions::NextDay(FunctionContext* context,
    const TimestampVal& date, const StringVal& weekday) {
  string weekday_str = string(reinterpret_cast<const char*>(weekday.ptr), weekday.len);
  transform(weekday_str.begin(), weekday_str.end(), weekday_str.begin(), tolower);
  int day_idx = 0;
  if (weekday_str == "sunday" || weekday_str == "sun") {
    day_idx = 1;
  } else if (weekday_str == "monday" || weekday_str == "mon") {
    day_idx = 2;
  } else if (weekday_str == "tuesday" || weekday_str == "tue") {
    day_idx = 3;
  } else if (weekday_str == "wednesday" || weekday_str == "wed") {
    day_idx = 4;
  } else if (weekday_str == "thursday" || weekday_str == "thu") {
    day_idx = 5;
  } else if (weekday_str == "friday" || weekday_str == "fri") {
    day_idx = 6;
  } else if (weekday_str == "saturday" || weekday_str == "sat") {
    day_idx = 7;
  }
  DCHECK_GE(day_idx, 1);
  DCHECK_LE(day_idx, 7);

  int delta_days = day_idx - DayOfWeek(context, date).val;
  delta_days = delta_days <= 0 ? delta_days + 7 : delta_days;
  DCHECK_GE(delta_days, 1);
  DCHECK_LE(delta_days, 7);

  IntVal delta(delta_days);
  return AddSub<true, IntVal, Days, false>(context, date, delta);
}

TimestampVal TimestampFunctions::LastDay(FunctionContext* context,
    const TimestampVal& ts) {
  if (ts.is_null) return TimestampVal::null();
  const TimestampValue& timestamp =  TimestampValue::FromTimestampVal(ts);
  if (!timestamp.HasDate()) return TimestampVal::null();
  TimestampValue tsv(timestamp.date().end_of_month(), time_duration(0,0,0,0));
  TimestampVal rt_date;
  tsv.ToTimestampVal(&rt_date);
  return rt_date;
}

IntVal TimestampFunctions::IntMonthsBetween(FunctionContext* context,
    const TimestampVal& ts1, const TimestampVal& ts2) {
  if (ts1.is_null || ts2.is_null) return IntVal::null();
  const TimestampValue& ts_value1 = TimestampValue::FromTimestampVal(ts1);
  const TimestampValue& ts_value2 = TimestampValue::FromTimestampVal(ts2);
  if (!ts_value1.HasDate() || !ts_value2.HasDate()) return IntVal::null();
  DoubleVal months_between = MonthsBetween(context, ts1, ts2);
  return IntVal(static_cast<int32_t>(months_between.val));
}

/// The MONTH interval is a special case. The different ways of adding a month interval
/// are:
///   1) ADD_MONTHS(<TIMESTAMP>, <NUMBER>)
///   2) ADD_DATE(<TIMESTAMP>, INTERVAL <NUMBER> MONTH)
///   3) <TIMESTAMP> + INTERVAL <NUMBER> MONTH
/// For other interval types, all three produce the same result. For MONTH and case #1, if
/// the input TIMESTAMP is the last day of the month, the result will always be the last
/// day of the month. Cases #2 and #3 are equivalent and do not have the special handling
/// as case #1. In all cases, if the result would be on a day that is beyond the last day
/// of the month, the day is reduced to be the last day of the month. A value of true
/// for the 'keep_max_day' argument corresponds to case #1.
inline void AddMonths(FunctionContext* context, int64_t months, bool keep_max_day,
    ptime* datetime) {
  int64_t years = months / 12;
  months %= 12;
  const Date& date = datetime->date();
  int year = date.year() + years;
  int month = date.month().as_number() + months;
  if (month <= 0) {
    --year;
    month += 12;
  } else if (month > 12) {
    ++year;
    month -= 12;
  }
  if (UNLIKELY(IsUnsupportedYear(year))) {
    context->AddWarning(Substitute("Add/sub month resulted in an out of range year: $0",
          year).c_str());
    *datetime = ptime(not_a_date_time);
  }
  DCHECK_GE(month, 1);
  DCHECK_LE(month, 12);
  int day = date.day().as_number();
  if (keep_max_day && GetLastDayOfMonth(date.month().as_number(), date.year()) == day) {
    day = GetLastDayOfMonth(month, year);
  } else {
    day = min(day, static_cast<int>(GetLastDayOfMonth(month, year)));
  }
  *datetime = ptime(boost::gregorian::date(year, month, day), datetime->time_of_day());
}

template <>
inline void AddInterval<Months>(FunctionContext* context, int64_t interval,
    ptime* datetime) {
  AddMonths(context, interval, false, datetime);
}

/// The AddInterval() functions below workaround various boost bugs in adding large
/// intervals -- if the interval is too large, some sort of overflow/wrap around
/// happens resulting in an incorrect value. There is no way to predict what input value
/// will cause a wrap around. The values below were chosen arbitrarily and shown to
/// work through testing.
template <>
inline void AddInterval<Hours>(FunctionContext* context, int64_t interval,
    ptime* datetime) {
  int64_t weeks = interval / (7 * 24);
  int64_t hours = interval % (7 * 24);
  AddInterval<Weeks>(context, weeks, datetime);
  *datetime += Hours(hours);
}

template <>
inline void AddInterval<Minutes>(FunctionContext* context, int64_t interval,
    ptime* datetime) {
  int64_t days = interval / (60 * 24);
  int64_t minutes = interval % (60 * 24);
  AddInterval<Days>(context, days, datetime);
  *datetime += Minutes(minutes);
}

/// Workaround a boost bug in adding large second intervals.
template <>
inline void AddInterval<Seconds>(FunctionContext* context, int64_t interval,
    ptime* datetime) {
  int64_t days = interval / (60 * 60 * 24);
  int64_t seconds = interval % (60 * 60 * 24);
  AddInterval<Days>(context, days, datetime);
  *datetime += Seconds(seconds);
}

/// Workaround a boost bug in adding large microsecond intervals.
template <>
inline void AddInterval<Microseconds>(FunctionContext* context, int64_t interval,
    ptime* datetime) {
  int64_t seconds = interval / 1000000;
  int64_t microseconds = interval % 1000000;
  AddInterval<Seconds>(context, seconds, datetime);
  *datetime += Microseconds(microseconds);
}

/// Workaround a boost bug in adding large nanosecond intervals.
template <>
inline void AddInterval<Nanoseconds>(FunctionContext* context, int64_t interval,
    ptime* datetime) {
  int64_t seconds = interval / 1000000000;
  int64_t nanoseconds = interval % 1000000000;
  AddInterval<Seconds>(context, seconds, datetime);
  *datetime += Nanoseconds(nanoseconds);
}

inline void DcheckAddSubResult(const TimestampVal& input, const TimestampVal& result,
    bool is_add, int64_t interval) {
#ifndef NDEBUG
  if (!result.is_null) {
    const TimestampValue& input_value = TimestampValue::FromTimestampVal(input);
    const TimestampValue& result_value = TimestampValue::FromTimestampVal(result);
    if (interval == 0) {
      DCHECK_EQ(result_value, input_value);
    } else if (is_add == (interval > 0)) {
      DCHECK_GT(result_value, input_value);
    } else {
      DCHECK_LT(result_value, input_value);
    }
  }
#endif
}

/// Template parameters:
///   'is_add': Set to false for subtraction.
///   'AnyIntVal': TinyIntVal, SmallIntVal, ...
///   'Interval': A boost interval type -- Years, Months, ...
///   'is_add_months_keep_last_day': Should only be set to true if 'Interval' is Months.
///       When true, AddMonths() will be called with 'keep_max_day' set to true.
template <bool is_add, typename AnyIntVal, typename Interval,
    bool is_add_months_keep_last_day>
TimestampVal TimestampFunctions::AddSub(FunctionContext* context,
    const TimestampVal& timestamp, const AnyIntVal& num_interval_units) {
  DCHECK_EQ(Date(max_date_time).year(), MAX_YEAR);
  DCHECK_EQ(Date(min_date_time).year(), MIN_YEAR);
  if (timestamp.is_null || num_interval_units.is_null) return TimestampVal::null();
  const TimestampValue& value = TimestampValue::FromTimestampVal(timestamp);
  if (!value.HasDate()) return TimestampVal::null();
  // Adding/subtracting boost::gregorian::dates can throw if the result exceeds the
  // min/max supported dates. (Sometimes the exception is thrown lazily and calling an
  // accessor functions is needed to trigger validation.)
  if (UNLIKELY(IsOverMaxInterval<Interval>(num_interval_units.val))) {
    context->AddWarning(Substitute("Cannot $0 interval $1: Interval value too large",
        is_add ? "add" : "subtract", num_interval_units.val).c_str());
    return TimestampVal::null();
  }
  try {
    ptime datetime;
    value.ToPtime(&datetime);
    if (is_add_months_keep_last_day) {
      AddMonths(context, is_add ? num_interval_units.val : -num_interval_units.val, true,
          &datetime);
    } else {
      AddInterval<Interval>(context,
          is_add ? num_interval_units.val : -num_interval_units.val, &datetime);
    }
    // Validate that the ptime is not "special" (ie not_a_date_time) and has a valid year.
    // If validation fails, an exception is thrown.
    datetime.date().year();
    const TimestampValue result_value(datetime);
    TimestampVal result_val;
    result_value.ToTimestampVal(&result_val);
    DcheckAddSubResult(timestamp, result_val, is_add, num_interval_units.val);
    return result_val;
  } catch (const std::exception& e) {
    context->AddWarning(Substitute("Cannot $0 interval $1: $2",
        is_add ? "add" : "subtract", num_interval_units.val, e.what()).c_str());
    return TimestampVal::null();
  }
}

IntVal TimestampFunctions::DateDiff(FunctionContext* context,
    const TimestampVal& ts_val1,
    const TimestampVal& ts_val2) {
  if (ts_val1.is_null || ts_val2.is_null) return IntVal::null();
  const TimestampValue& ts_value1 = TimestampValue::FromTimestampVal(ts_val1);
  const TimestampValue& ts_value2 = TimestampValue::FromTimestampVal(ts_val2);
  if (!ts_value1.HasDate() || !ts_value2.HasDate()) {
    return IntVal::null();
  }
  return IntVal((ts_value1.date() - ts_value2.date()).days());
}

// Explicit template instantiation is required for proper linking. These functions
// are only indirectly called via a function pointer provided by the opcode registry
// which does not trigger implicit template instantiation.
// Must be kept in sync with common/function-registry/impala_functions.py.
template StringVal
TimestampFunctions::FromUnix<IntVal>(FunctionContext* context, const IntVal& intp, const
  StringVal& fmt);
template StringVal
TimestampFunctions::FromUnix<BigIntVal>(FunctionContext* context, const BigIntVal& intp,
    const StringVal& fmt);
template StringVal
TimestampFunctions::FromUnix<IntVal>(FunctionContext* context , const IntVal& intp);
template StringVal
TimestampFunctions::FromUnix<BigIntVal>(FunctionContext* context, const BigIntVal& intp);

template TimestampVal
TimestampFunctions::AddSub<true, IntVal, Years, false>(FunctionContext* context,
    const TimestampVal& ts_val, const IntVal& count);
template TimestampVal
TimestampFunctions::AddSub<true, BigIntVal, Years, false>(FunctionContext* context,
    const TimestampVal& ts_val, const BigIntVal& count);
template TimestampVal
TimestampFunctions::AddSub<false, IntVal, Years, false>(FunctionContext* context,
    const TimestampVal& ts_val, const IntVal& count);
template TimestampVal
TimestampFunctions::AddSub<false, BigIntVal, Years, false>(FunctionContext* context,
    const TimestampVal& ts_val, const BigIntVal& count);
template TimestampVal
TimestampFunctions::AddSub<true, IntVal, Months, false>(FunctionContext* context,
    const TimestampVal& ts_val, const IntVal& count);
template TimestampVal
TimestampFunctions::AddSub<true, IntVal, Months, true>(FunctionContext* context,
    const TimestampVal& ts_val, const IntVal& count);
template TimestampVal
TimestampFunctions::AddSub<true, BigIntVal, Months, false>(FunctionContext* context,
    const TimestampVal& ts_val, const BigIntVal& count);
template TimestampVal
TimestampFunctions::AddSub<true, BigIntVal, Months, true>(FunctionContext* context,
    const TimestampVal& ts_val, const BigIntVal& count);
template TimestampVal
TimestampFunctions::AddSub<false, IntVal, Months, false>(FunctionContext* context,
    const TimestampVal& ts_val, const IntVal& count);
template TimestampVal
TimestampFunctions::AddSub<false, IntVal, Months, true>(FunctionContext* context,
    const TimestampVal& ts_val, const IntVal& count);
template TimestampVal
TimestampFunctions::AddSub<false, BigIntVal, Months, false>(FunctionContext* context,
    const TimestampVal& ts_val, const BigIntVal& count);
template TimestampVal
TimestampFunctions::AddSub<false, BigIntVal, Months, true>(FunctionContext* context,
    const TimestampVal& ts_val, const BigIntVal& count);
template TimestampVal
TimestampFunctions::AddSub<true, IntVal, Weeks, false>(FunctionContext* context,
    const TimestampVal& ts_val, const IntVal& count);
template TimestampVal
TimestampFunctions::AddSub<true, BigIntVal, Weeks, false>(FunctionContext* context,
    const TimestampVal& ts_val, const BigIntVal& count);
template TimestampVal
TimestampFunctions::AddSub<false, IntVal, Weeks, false>(FunctionContext* context,
    const TimestampVal& ts_val, const IntVal& count);
template TimestampVal
TimestampFunctions::AddSub<false, BigIntVal, Weeks, false>(FunctionContext* context,
    const TimestampVal& ts_val, const BigIntVal& count);
template TimestampVal
TimestampFunctions::AddSub<true, IntVal, Days, false>(FunctionContext* context,
    const TimestampVal& ts_val, const IntVal& count);
template TimestampVal
TimestampFunctions::AddSub<true, BigIntVal, Days, false>(FunctionContext* context,
    const TimestampVal& ts_val, const BigIntVal& count);
template TimestampVal
TimestampFunctions::AddSub<false, IntVal, Days, false>(FunctionContext* context,
    const TimestampVal& ts_val, const IntVal& count);
template TimestampVal
TimestampFunctions::AddSub<false, BigIntVal, Days, false>(FunctionContext* context,
    const TimestampVal& ts_val, const BigIntVal& count);

template TimestampVal
TimestampFunctions::AddSub<true, IntVal, Hours, false>(FunctionContext* context,
    const TimestampVal& ts_val, const IntVal& count);
template TimestampVal
TimestampFunctions::AddSub<true, BigIntVal, Hours, false>(FunctionContext* context,
    const TimestampVal& ts_val, const BigIntVal& count);
template TimestampVal
TimestampFunctions::AddSub<false, IntVal, Hours, false>(FunctionContext* context,
    const TimestampVal& ts_val, const IntVal& count);
template TimestampVal
TimestampFunctions::AddSub<false, BigIntVal, Hours, false>(FunctionContext* context,
    const TimestampVal& ts_val, const BigIntVal& count);
template TimestampVal
TimestampFunctions::AddSub<true, IntVal, Minutes, false>(FunctionContext* context,
    const TimestampVal& ts_val, const IntVal& count);
template TimestampVal
TimestampFunctions::AddSub<true, BigIntVal, Minutes, false>(FunctionContext* context,
    const TimestampVal& ts_val, const BigIntVal& count);
template TimestampVal
TimestampFunctions::AddSub<false, IntVal, Minutes, false>(FunctionContext* context,
    const TimestampVal& ts_val, const IntVal& count);
template TimestampVal
TimestampFunctions::AddSub<false, BigIntVal, Minutes, false>(FunctionContext* context,
    const TimestampVal& ts_val, const BigIntVal& count);
template TimestampVal
TimestampFunctions::AddSub<true, IntVal, Seconds, false>(FunctionContext* context,
    const TimestampVal& ts_val, const IntVal& count);
template TimestampVal
TimestampFunctions::AddSub<true, BigIntVal, Seconds, false>(FunctionContext* context,
    const TimestampVal& ts_val, const BigIntVal& count);
template TimestampVal
TimestampFunctions::AddSub<false, IntVal, Seconds, false>(FunctionContext* context,
    const TimestampVal& ts_val, const IntVal& count);
template TimestampVal
TimestampFunctions::AddSub<false, BigIntVal, Seconds, false>(FunctionContext* context,
    const TimestampVal& ts_val, const BigIntVal& count);
template TimestampVal
TimestampFunctions::AddSub<true, IntVal, Milliseconds, false>(FunctionContext* context,
    const TimestampVal& ts_val, const IntVal& count);
template TimestampVal
TimestampFunctions::AddSub<true, BigIntVal, Milliseconds, false>(FunctionContext* context,
    const TimestampVal& ts_val, const BigIntVal& count);
template TimestampVal
TimestampFunctions::AddSub<false, IntVal, Milliseconds, false>(FunctionContext* context,
    const TimestampVal& ts_val, const IntVal& count);
template TimestampVal
TimestampFunctions::AddSub<false, BigIntVal, Milliseconds, false>(
    FunctionContext* context, const TimestampVal& ts_val, const BigIntVal& count);
template TimestampVal
TimestampFunctions::AddSub<true, IntVal, Microseconds, false>(FunctionContext* context,
    const TimestampVal& ts_val, const IntVal& count);
template TimestampVal
TimestampFunctions::AddSub<true, BigIntVal, Microseconds, false>(FunctionContext* context,
    const TimestampVal& ts_val, const BigIntVal& count);
template TimestampVal
TimestampFunctions::AddSub<false, IntVal, Microseconds, false>(FunctionContext* context,
    const TimestampVal& ts_val, const IntVal& count);
template TimestampVal
TimestampFunctions::AddSub<false, BigIntVal, Microseconds, false>(
    FunctionContext* context, const TimestampVal& ts_val, const BigIntVal& count);
template TimestampVal
TimestampFunctions::AddSub<true, IntVal, Nanoseconds, false>(FunctionContext* context,
    const TimestampVal& ts_val, const IntVal& count);
template TimestampVal
TimestampFunctions::AddSub<true, BigIntVal, Nanoseconds, false>(FunctionContext* context,
    const TimestampVal& ts_val, const BigIntVal& count);
template TimestampVal
TimestampFunctions::AddSub<false, IntVal, Nanoseconds, false>(FunctionContext* context,
    const TimestampVal& ts_val, const IntVal& count);
template TimestampVal
TimestampFunctions::AddSub<false, BigIntVal, Nanoseconds, false>(FunctionContext* context,
    const TimestampVal& ts_val, const BigIntVal& count);
}
