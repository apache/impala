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

#include <boost/date_time/posix_time/posix_time.hpp>
#include <boost/date_time/gregorian/gregorian.hpp>
#include <boost/date_time/time_zone_base.hpp>
#include <boost/date_time/local_time/local_time.hpp>
#include <boost/algorithm/string.hpp>
#include <ctime>
#include <gutil/strings/substitute.h>

#include "exprs/timestamp-functions.h"
#include "exprs/expr.h"
#include "exprs/anyval-util.h"

#include "runtime/tuple-row.h"
#include "runtime/timestamp-value.h"
#include "util/path-builder.h"
#include "runtime/string-value.inline.h"
#include "udf/udf.h"
#include "udf/udf-internal.h"
#include "runtime/runtime-state.h"

#define TIMEZONE_DATABASE "be/files/date_time_zonespec.csv"

#include "common/names.h"

using boost::algorithm::iequals;
using boost::gregorian::greg_month;
using boost::gregorian::min_date_time;
using boost::local_time::local_date_time;
using boost::local_time::time_zone_ptr;
using boost::posix_time::not_a_date_time;
using boost::posix_time::ptime;
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

// Constant strings used for DayName function.
const char* TimestampFunctions::SUNDAY = "Sunday";
const char* TimestampFunctions::MONDAY = "Monday";
const char* TimestampFunctions::TUESDAY = "Tuesday";
const char* TimestampFunctions::WEDNESDAY = "Wednesday";
const char* TimestampFunctions::THURSDAY = "Thursday";
const char* TimestampFunctions::FRIDAY = "Friday";
const char* TimestampFunctions::SATURDAY = "Saturday";

// To workaround a boost bug (where adding very large intervals to ptimes causes the
// value to wrap around instead or throwing an exception -- the root cause of
// IMPALA-1675), max interval value are defined below. Some values below are less than
// the minimum interval needed to trigger IMPALA-1675 but the values are greater or
// equal to the interval that would definitely result in an out of bounds value. The
// min and max year are also defined for manual error checking. Boost is inconsistent
// with its defined max year. date(max_date_time).year() will give 9999 but testing shows
// the actual max date is 1 year later.
const int64_t TimestampFunctions::MAX_YEAR = 10000;
const int64_t TimestampFunctions::MIN_YEAR = Date(min_date_time).year();
const int64_t TimestampFunctions::MAX_YEAR_INTERVAL =
    TimestampFunctions::MAX_YEAR - TimestampFunctions::MIN_YEAR;
const int64_t TimestampFunctions::MAX_MONTH_INTERVAL =
    TimestampFunctions::MAX_YEAR_INTERVAL * 12;
const int64_t TimestampFunctions::MAX_WEEK_INTERVAL =
    TimestampFunctions::MAX_YEAR_INTERVAL * 53;
const int64_t TimestampFunctions::MAX_DAY_INTERVAL =
    TimestampFunctions::MAX_YEAR_INTERVAL * 366;
const int64_t TimestampFunctions::MAX_HOUR_INTERVAL =
    TimestampFunctions::MAX_DAY_INTERVAL * 24;
const int64_t TimestampFunctions::MAX_MINUTE_INTERVAL =
    TimestampFunctions::MAX_DAY_INTERVAL * 60;
const int64_t TimestampFunctions::MAX_SEC_INTERVAL =
    TimestampFunctions::MAX_MINUTE_INTERVAL * 60;
const int64_t TimestampFunctions::MAX_MILLI_INTERVAL =
    TimestampFunctions::MAX_SEC_INTERVAL * 1000;
const int64_t TimestampFunctions::MAX_MICRO_INTERVAL =
    TimestampFunctions::MAX_MILLI_INTERVAL * 1000;

void TimestampFunctions::UnixAndFromUnixPrepare(FunctionContext* context,
    FunctionContext::FunctionStateScope scope) {
  if (scope != FunctionContext::THREAD_LOCAL) return;
  DateTimeFormatContext* dt_ctx = NULL;
  if (context->IsArgConstant(1)) {
    StringVal fmt_val = *reinterpret_cast<StringVal*>(context->GetConstantArg(1));
    const StringValue& fmt_ref = StringValue::FromStringVal(fmt_val);
    if (fmt_val.is_null || fmt_ref.len == 0) {
      TimestampFunctions::ReportBadFormat(context, fmt_val, true);
      return;
    }
    dt_ctx = new DateTimeFormatContext(fmt_ref.ptr, fmt_ref.len);
    bool parse_result = TimestampParser::ParseFormatTokens(dt_ctx);
    if (!parse_result) {
      delete dt_ctx;
      TimestampFunctions::ReportBadFormat(context, fmt_val, true);
      return;
    }
  } else {
    // If our format string is constant, then we benefit from it only being parsed once in
    // the code above. If it's not constant, then we can reuse a context by resetting it.
    // This is much cheaper vs alloc/dealloc'ing a context for each evaluation.
    dt_ctx = new DateTimeFormatContext();
  }
  context->SetFunctionState(scope, dt_ctx);
}

void TimestampFunctions::UnixAndFromUnixClose(FunctionContext* context,
    FunctionContext::FunctionStateScope scope) {
  if (scope == FunctionContext::THREAD_LOCAL) {
    DateTimeFormatContext* dt_ctx =
        reinterpret_cast<DateTimeFormatContext*>(context->GetFunctionState(scope));
    delete dt_ctx;
  }
}

StringVal TimestampFunctions::StringValFromTimestamp(FunctionContext* context,
    TimestampValue tv, const StringVal& fmt) {
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
  result.len = tv.Format(*dt_ctx, buff_len, reinterpret_cast<char*>(result.ptr));
  if (result.len <= 0) return StringVal::null();
  return result;
}

template <class TIME>
StringVal TimestampFunctions::FromUnix(FunctionContext* context, const TIME& intp) {
  if (intp.is_null) return StringVal::null();
  TimestampValue t(intp.val);
  return AnyValUtil::FromString(context, lexical_cast<string>(t));
}

template <class TIME>
StringVal TimestampFunctions::FromUnix(FunctionContext* context, const TIME& intp,
    const StringVal& fmt) {
  if (fmt.is_null || fmt.len == 0) {
    TimestampFunctions::ReportBadFormat(context, fmt, false);
    return StringVal::null();
  }
  if (intp.is_null) return StringVal::null();

  TimestampValue t(intp.val);
  return StringValFromTimestamp(context, t, fmt);
}

BigIntVal TimestampFunctions::Unix(FunctionContext* context, const StringVal& string_val,
    const StringVal& fmt) {
  const TimestampVal& tv_val = ToTimestamp(context, string_val, fmt);
  if (tv_val.is_null) return BigIntVal::null();
  const TimestampValue& tv = TimestampValue::FromTimestampVal(tv_val);
  return BigIntVal(tv.ToUnixTime());
}

BigIntVal TimestampFunctions::Unix(FunctionContext* context, const TimestampVal& ts_val) {
  if (ts_val.is_null) return BigIntVal::null();
  const TimestampValue& tv = TimestampValue::FromTimestampVal(ts_val);
  if (!tv.HasDate()) return BigIntVal::null();
  return BigIntVal(tv.ToUnixTime());
}

BigIntVal TimestampFunctions::Unix(FunctionContext* context) {
  if (!context->impl()->state()->now()->HasDate()) return BigIntVal::null();
  return BigIntVal(context->impl()->state()->now()->ToUnixTime());
}

TimestampVal TimestampFunctions::ToTimestamp(FunctionContext* context,
    const BigIntVal& bigint_val) {
  if (bigint_val.is_null) return TimestampVal::null();
  TimestampValue tv(bigint_val.val);
  if (!tv.HasDate()) return TimestampVal::null();
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
  TimestampValue tv = TimestampValue(
      reinterpret_cast<const char*>(date.ptr), date.len, *dt_ctx);
  if (!tv.HasDate()) return TimestampVal::null();
  TimestampVal tv_val;
  tv.ToTimestampVal(&tv_val);
  return tv_val;
}

StringVal TimestampFunctions::FromTimestamp(FunctionContext* context,
    const TimestampVal& date, const StringVal& fmt) {
  if (date.is_null) return StringVal::null();
  TimestampValue tv = TimestampValue::FromTimestampVal(date);
  if (!tv.HasDate()) return StringVal::null();
  return StringValFromTimestamp(context, tv, fmt);
}

BigIntVal TimestampFunctions::UnixFromString(FunctionContext* context,
    const StringVal& sv) {
  if (sv.is_null) return BigIntVal::null();
  TimestampValue tv(reinterpret_cast<const char *>(sv.ptr), sv.len);
  if (!tv.HasDate()) return BigIntVal::null();
  return BigIntVal(tv.ToUnixTime());
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

StringVal TimestampFunctions::DayName(FunctionContext* context, const TimestampVal& ts) {
  if (ts.is_null) return StringVal::null();
  IntVal dow = DayOfWeek(context, ts);
  switch(dow.val) {
    case 1: return StringVal(SUNDAY);
    case 2: return StringVal(MONDAY);
    case 3: return StringVal(TUESDAY);
    case 4: return StringVal(WEDNESDAY);
    case 5: return StringVal(THURSDAY);
    case 6: return StringVal(FRIDAY);
    case 7: return StringVal(SATURDAY);
    default: return StringVal::null();
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

TimestampVal TimestampFunctions::Now(FunctionContext* context) {
  const TimestampValue* now = context->impl()->state()->now();
  if (!now->HasDateOrTime()) return TimestampVal::null();
  TimestampVal return_val;
  now->ToTimestampVal(&return_val);
  return return_val;
}

StringVal TimestampFunctions::ToDate(FunctionContext* context,
    const TimestampVal& ts_val) {
  if (ts_val.is_null) return StringVal::null();
  const TimestampValue ts_value = TimestampValue::FromTimestampVal(ts_val);
  string result = to_iso_extended_string(ts_value.date());
  return AnyValUtil::FromString(context, result);
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
  static const string DAY_ARRAY[7] = {"Sun", "Mon", "Tue", "Wed", "Thu", "Fri",
      "Sat"};
  IntVal dow = DayOfWeek(context, ts);
  DCHECK_GT(dow.val, 0);
  DCHECK_LT(dow.val, 8);
  return DAY_ARRAY[dow.val - 1];
}

string TimestampFunctions::ShortMonthName(FunctionContext* context,
    const TimestampVal& ts) {
  if (ts.is_null) return NULL;
  static const string MONTH_ARRAY[12] = {"Jan", "Feb", "Mar", "Apr", "May",
      "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"};
  IntVal mth = Month(context, ts);
  DCHECK_GT(mth.val, 0);
  DCHECK_LT(mth.val, 13);
  return MONTH_ARRAY[mth.val - 1];
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

// This function uses inline asm functions, which we believe to be from the boost library.
// Inline asm is not currently supported by JIT, so this function should always be run in
// the interpreted mode. This is handled in ScalarFnCall::GetUdf().
TimestampVal TimestampFunctions::FromUtc(FunctionContext* context,
    const TimestampVal& ts_val, const StringVal& tz_string_val) {
  if (ts_val.is_null || tz_string_val.is_null) return TimestampVal::null();
  const TimestampValue& ts_value = TimestampValue::FromTimestampVal(ts_val);
  if (!ts_value.HasDateOrTime()) return TimestampVal::null();

  const StringValue& tz_string_value = StringValue::FromStringVal(tz_string_val);
  time_zone_ptr timezone =
      TimezoneDatabase::FindTimezone(tz_string_value.DebugString(), ts_value);
  if (timezone == NULL) {
    // This should return null. Hive just ignores it.
    stringstream ss;
    ss << "Unknown timezone '" << tz_string_value << "'" << endl;
    context->AddWarning(ss.str().c_str());
    return ts_val;
  }

  ptime temp;
  ts_value.ToPtime(&temp);
  local_date_time lt(temp, timezone);
  TimestampValue return_value = lt.local_time();
  TimestampVal return_val;
  return_value.ToTimestampVal(&return_val);
  return return_val;
}

// This function uses inline asm functions, which we believe to be from the boost library.
// Inline asm is not currently supported by JIT, so this function should always be run in
// the interpreted mode. This is handled in ScalarFnCall::GetUdf().
TimestampVal TimestampFunctions::ToUtc(FunctionContext* context,
    const TimestampVal& ts_val, const StringVal& tz_string_val) {
  if (ts_val.is_null || tz_string_val.is_null) return TimestampVal::null();
  const TimestampValue& ts_value = TimestampValue::FromTimestampVal(ts_val);
  if (!ts_value.HasDateOrTime()) return TimestampVal::null();

  const StringValue& tz_string_value = StringValue::FromStringVal(tz_string_val);
  time_zone_ptr timezone =
      TimezoneDatabase::FindTimezone(tz_string_value.DebugString(), ts_value);
  // This should raise some sort of error or at least null. Hive Just ignores it.
  if (timezone == NULL) {
    stringstream ss;
    ss << "Unknown timezone '" << tz_string_value << "'" << endl;
    context->AddWarning(ss.str().c_str());
    return ts_val;
  }

  local_date_time lt(ts_value.date(), ts_value.time(),
      timezone, local_date_time::NOT_DATE_TIME_ON_ERROR);
  TimestampValue return_value(lt.utc_time());
  TimestampVal return_val;
  return_value.ToTimestampVal(&return_val);
  return return_val;
}

TimezoneDatabase::TimezoneDatabase() {
  // Create a temporary file and write the timezone information.  The boost
  // interface only loads this format from a file.  We don't want to raise
  // an error here since this is done when the backend is created and this
  // information might not actually get used by any queries.
  char filestr[] = "/tmp/impala.tzdb.XXXXXXX";
  FILE* file;
  int fd;
  if ((fd = mkstemp(filestr)) == -1) {
    LOG(ERROR) << "Could not create temporary timezone file: " << filestr;
    return;
  }
  if ((file = fopen(filestr, "w")) == NULL) {
    unlink(filestr);
    close(fd);
    LOG(ERROR) << "Could not open temporary timezone file: " << filestr;
    return;
  }
  if (fputs(TIMEZONE_DATABASE_STR, file) == EOF) {
    unlink(filestr);
    close(fd);
    fclose(file);
    LOG(ERROR) << "Could not load temporary timezone file: " << filestr;
    return;
  }
  fclose(file);
  tz_database_.load_from_file(string(filestr));
  tz_region_list_ = tz_database_.region_list();
  unlink(filestr);
  close(fd);
}

TimezoneDatabase::~TimezoneDatabase() { }

time_zone_ptr TimezoneDatabase::FindTimezone(const string& tz, const TimestampValue& tv) {
  // The backing database does not capture some subtleties, there are special cases
  if ((tv.date().year() < 2011 || (tv.date().year() == 2011 && tv.date().month() < 4)) &&
      (iequals("Europe/Moscow", tz) || iequals("Moscow", tz) || iequals("MSK", tz))) {
    // We transition in pre April 2011 from using the tz_database_ to a custom rule
    // Russia stopped using daylight savings in 2011, the tz_database_ is
    // set up assuming Russia uses daylight saving every year.
    // Sun, Mar 27, 2:00AM Moscow clocks moved forward +1 hour (a total of GMT +4)
    // Specifically,
    // UTC Time 26 Mar 2011 22:59:59 +0000 ===> Sun Mar 27 01:59:59 MSK 2011
    // UTC Time 26 Mar 2011 23:00:00 +0000 ===> Sun Mar 27 03:00:00 MSK 2011
    // This means in 2011, The database rule will apply DST starting March 26 2011.
    // This will be a correct +4 offset, and the database rule can apply until
    // Oct 31 when tz_database_ will incorrectly attempt to turn clocks backwards 1 hour.
    return TIMEZONE_MSK_PRE_2011_DST;
  }

  // See if they specified a zone id
  time_zone_ptr tzp = tz_database_.time_zone_from_region(tz);
  if (tzp != NULL) return tzp;

  for (vector<string>::const_iterator iter = tz_region_list_.begin();
       iter != tz_region_list_.end(); ++iter) {
    time_zone_ptr tzp = tz_database_.time_zone_from_region(*iter);
    DCHECK(tzp != NULL);
    if (tzp->dst_zone_abbrev() == tz)
      return tzp;
    if (tzp->std_zone_abbrev() == tz)
      return tzp;
    if (tzp->dst_zone_name() == tz)
      return tzp;
    if (tzp->std_zone_name() == tz)
      return tzp;
  }
  return time_zone_ptr();
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
