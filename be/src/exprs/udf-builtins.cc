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

// The functions in this file are specifically not cross-compiled to IR because there
// is no signifcant performance benefit to be gained.

#include "exprs/udf-builtins.h"

#include <boost/date_time/date.hpp>
#include <boost/date_time/gregorian/greg_calendar.hpp>
#include <boost/date_time/gregorian/greg_date.hpp>
#include <boost/date_time/gregorian/greg_duration.hpp>
#include <boost/date_time/gregorian_calendar.hpp>
#include <boost/date_time/posix_time/posix_time_config.hpp>
#include <boost/date_time/posix_time/posix_time_duration.hpp>
#include <boost/date_time/posix_time/ptime.hpp>
#include <boost/date_time/time.hpp>
#include <boost/date_time/time_duration.hpp>

#include <gutil/walltime.h>

#include "gen-cpp/Exprs_types.h"
#include "runtime/date-value.h"
#include "runtime/runtime-state.h"
#include "runtime/timestamp-value.h"
#include "udf/udf-internal.h"
#include "util/bit-util.h"

#include "common/names.h"

using boost::gregorian::date;
using boost::gregorian::date_duration;
using boost::posix_time::ptime;
using boost::posix_time::time_duration;
using boost::posix_time::milliseconds;
using boost::posix_time::microseconds;
using namespace impala;
using namespace strings;

// The units which can be used when Truncating a Timestamp
enum class TruncUnit {
  UNIT_INVALID,
  YEAR,
  QUARTER,
  MONTH,
  WW,
  W,
  DAY,
  DAY_OF_WEEK,
  HOUR,
  MINUTE,
  // Below units are only used by DateTrunc
  MILLENNIUM,
  CENTURY,
  DECADE,
  WEEK,
  SECOND,
  MILLISECONDS,
  MICROSECONDS,
};

// Put non-exported functions in anonymous namespace to encourage inlining.
namespace {

// Returns the most recent date, no later than orig_date, which is on week_day
// week_day: 0==Sunday, 1==Monday, ...
date GoBackToWeekday(const date& orig_date, int week_day) {
  int current_week_day = orig_date.day_of_week();
  int diff = current_week_day - week_day;
  if (diff == 0) return orig_date;
  if (diff > 0) {
    // ex. Weds(3) shifts to Tues(2), so we go back 1 day
    return orig_date - date_duration(diff);
  }
  // ex. Tues(2) shifts to Weds(3), so we go back 6 days
  DCHECK_LT(diff, 0);
  return orig_date - date_duration(7 + diff);
}

// Maps the user facing name of a unit to a TruncUnit used by DateTrunc function
// Returns the TruncUnit for the given string
TruncUnit StrToDateTruncUnit(FunctionContext* ctx, const StringVal& unit_str) {
  StringVal unit = UdfBuiltins::Lower(ctx, unit_str);
  if (UNLIKELY(unit.is_null)) return TruncUnit::UNIT_INVALID;

  if (unit == "millennium") {
    return TruncUnit::MILLENNIUM;
  } else if (unit == "century") {
    return TruncUnit::CENTURY;
  } else if (unit == "decade") {
    return TruncUnit::DECADE;
  } else if (unit == "year") {
    return TruncUnit::YEAR;
  } else if (unit == "month") {
    return TruncUnit::MONTH;
  } else if (unit == "week") {
    return TruncUnit::WEEK;
  } else if (unit == "day") {
    return TruncUnit::DAY;
  } else if (unit == "hour") {
    return TruncUnit::HOUR;
  } else if (unit == "minute") {
    return TruncUnit::MINUTE;
  } else if (unit == "second") {
    return TruncUnit::SECOND;
  } else if (unit == "milliseconds") {
    return TruncUnit::MILLISECONDS;
  } else if (unit == "microseconds") {
    return TruncUnit::MICROSECONDS;
  } else {
    return TruncUnit::UNIT_INVALID;
  }
}

// Maps the user facing name of a unit to a TruncUnit
// Returns the TruncUnit for the given string
TruncUnit StrToTruncUnit(FunctionContext* ctx, const StringVal& unit_str) {
  StringVal unit = UdfBuiltins::Lower(ctx, unit_str);
  if (UNLIKELY(unit.is_null)) return TruncUnit::UNIT_INVALID;
  if ((unit == "syyyy") || (unit == "yyyy") || (unit == "year") || (unit == "syear")
      || (unit == "yyy") || (unit == "yy") || (unit == "y")) {
    return TruncUnit::YEAR;
  } else if (unit == "q") {
    return TruncUnit::QUARTER;
  } else if ((unit == "month") || (unit == "mon") || (unit == "mm") || (unit == "rm")) {
    return TruncUnit::MONTH;
  } else if (unit == "ww") {
    return TruncUnit::WW;
  } else if (unit == "w") {
    return TruncUnit::W;
  } else if ((unit == "ddd") || (unit == "dd") || (unit == "j")) {
    return TruncUnit::DAY;
  } else if ((unit == "day") || (unit == "dy") || (unit == "d")) {
    return TruncUnit::DAY_OF_WEEK;
  } else if ((unit == "hh") || (unit == "hh12") || (unit == "hh24")) {
    return TruncUnit::HOUR;
  } else if (unit == "mi") {
    return TruncUnit::MINUTE;
  } else {
    return TruncUnit::UNIT_INVALID;
  }
}

// Truncate to first day of millennium
TimestampValue TruncMillennium(const date& orig_date) {
  DCHECK_GT(orig_date.year(), 2000);
  // First year of current millennium is 2001
  date new_date((orig_date.year() - 1) / 1000 * 1000 + 1, 1, 1);
  time_duration new_time(0, 0, 0, 0);
  return TimestampValue(new_date, new_time);
}

// Truncate to first day of century
TimestampValue TruncCentury(const date& orig_date) {
  DCHECK_GT(orig_date.year(), 1400);
  // First year of current century is 2001
  date new_date((orig_date.year() - 1) / 100 * 100 + 1, 1, 1);
  time_duration new_time(0, 0, 0, 0);
  return TimestampValue(new_date, new_time);
}

// Truncate to first day of decade
TimestampValue TruncDecade(const date& orig_date) {
  // Decades start with years ending in '0'.
  date new_date(orig_date.year() / 10 * 10, 1, 1);
  time_duration new_time(0, 0, 0, 0);
  return TimestampValue(new_date, new_time);
}

// Truncate to first day of year
TimestampValue TruncYear(const date& orig_date) {
  date new_date(orig_date.year(), 1, 1);
  time_duration new_time(0, 0, 0, 0);
  return TimestampValue(new_date, new_time);
}

// Truncate to first day of quarter
TimestampValue TruncQuarter(const date& orig_date) {
  int first_month_of_quarter = BitUtil::RoundDown(orig_date.month() - 1, 3) + 1;
  date new_date(orig_date.year(), first_month_of_quarter, 1);
  time_duration new_time(0, 0, 0, 0);
  return TimestampValue(new_date, new_time);
}

// Truncate to first day of month
TimestampValue TruncMonth(const date& orig_date) {
  date new_date(orig_date.year(), orig_date.month(), 1);
  time_duration new_time(0, 0, 0, 0);
  return TimestampValue(new_date, new_time);
}

// Truncate to first day of the week (monday)
TimestampValue TruncWeek(const date& orig_date) {
  // ISO-8601 week starts on monday. go back to monday
  date new_date = GoBackToWeekday(orig_date, 1);
  time_duration new_time(0, 0, 0, 0);
  return TimestampValue(new_date, new_time);
}

// Same day of the week as the first day of the year
TimestampValue TruncWW(const date& orig_date) {
  date first_day_of_year(orig_date.year(), 1, 1);
  int target_week_day = first_day_of_year.day_of_week();
  date new_date = GoBackToWeekday(orig_date, target_week_day);
  time_duration new_time(0, 0, 0, 0);
  return TimestampValue(new_date, new_time);
}

// Same day of the week as the first day of the month
TimestampValue TruncW(const date& orig_date) {
  date first_day_of_mon(orig_date.year(), orig_date.month(), 1);
  date new_date = GoBackToWeekday(orig_date, first_day_of_mon.day_of_week());
  time_duration new_time(0, 0, 0, 0);
  return TimestampValue(new_date, new_time);
}

// Truncate to midnight on the given date
TimestampValue TruncDay(const date& orig_date) {
  time_duration new_time(0, 0, 0, 0);
  return TimestampValue(orig_date, new_time);
}

// Date of the previous Monday
TimestampValue TruncDayOfWeek(const date& orig_date) {
  date new_date = GoBackToWeekday(orig_date, 1);
  time_duration new_time(0, 0, 0, 0);
  return TimestampValue(new_date, new_time);
}

// Truncate minutes, seconds, and parts of seconds
TimestampValue TruncHour(const date& orig_date, const time_duration& orig_time) {
  time_duration new_time(orig_time.hours(), 0, 0, 0);
  return TimestampValue(orig_date, new_time);
}

// Truncate seconds and parts of seconds
TimestampValue TruncMinute(const date& orig_date, const time_duration& orig_time) {
  time_duration new_time(orig_time.hours(), orig_time.minutes(), 0, 0);
  return TimestampValue(orig_date, new_time);
}

// Truncate parts of seconds
TimestampValue TruncSecond(const date& orig_date, const time_duration& orig_time) {
  time_duration new_time(orig_time.hours(), orig_time.minutes(), orig_time.seconds());
  return TimestampValue(orig_date, new_time);
}

// Truncate parts of milliseconds
TimestampValue TruncMilliseconds(const date& orig_date, const time_duration& orig_time) {
  time_duration new_time(orig_time.hours(), orig_time.minutes(), orig_time.seconds());
  // Fractional seconds are nanoseconds because Boost is configured to use nanoseconds
  // precision.
  time_duration fraction = milliseconds(orig_time.fractional_seconds() / 1000000);
  new_time = new_time + fraction;
  return TimestampValue(orig_date, new_time);
}

// Truncate parts of microseconds
TimestampValue TruncMicroseconds(const date& orig_date, const time_duration& orig_time) {
  time_duration new_time(orig_time.hours(), orig_time.minutes(), orig_time.seconds());
  // Fractional seconds are nanoseconds because Boost is configured to use nanoseconds
  // precision.
  time_duration fraction = microseconds(orig_time.fractional_seconds() / 1000);
  new_time = new_time + fraction;
  return TimestampValue(orig_date, new_time);
}

// Used by both TRUNC and DATE_TRUNC functions to perform the truncation
TimestampVal DoTrunc(
    const TimestampValue& ts, TruncUnit trunc_unit, FunctionContext* ctx) {
  const date& orig_date = ts.date();
  const time_duration& orig_time = ts.time();
  TimestampValue ret;
  TimestampVal ret_val;

  // check for invalid or malformed timestamps
  switch (trunc_unit) {
    case TruncUnit::MILLENNIUM:
      // for millenium <= 2000 year value goes to 1001 (outside the supported range)
      if (orig_date.is_special()) return TimestampVal::null();
      if (orig_date.year() <= 2000) return TimestampVal::null();
      break;
    case TruncUnit::CENTURY:
      // for century <= 1400 year value goes to 1301 (outside the supported range)
      if (orig_date.is_special()) return TimestampVal::null();
      if (orig_date.year() <= 1400) return TimestampVal::null();
      break;
    case TruncUnit::WEEK:
      // anything less than 1400-1-6 we have to move to year 1399
      if (orig_date.is_special()) return TimestampVal::null();
      if (orig_date < date(1400, 1, 6)) return TimestampVal::null();
      break;
    case TruncUnit::YEAR:
    case TruncUnit::QUARTER:
    case TruncUnit::MONTH:
    case TruncUnit::WW:
    case TruncUnit::W:
    case TruncUnit::DAY:
    case TruncUnit::DAY_OF_WEEK:
    case TruncUnit::DECADE:
      if (orig_date.is_special()) return TimestampVal::null();
      break;
    case TruncUnit::HOUR:
    case TruncUnit::MINUTE:
    case TruncUnit::SECOND:
    case TruncUnit::MILLISECONDS:
    case TruncUnit::MICROSECONDS:
      if (orig_time.is_special()) return TimestampVal::null();
      break;
    case TruncUnit::UNIT_INVALID:
      DCHECK(false);
  }

  switch(trunc_unit) {
    case TruncUnit::YEAR:
      ret = TruncYear(orig_date);
      break;
    case TruncUnit::QUARTER:
      ret = TruncQuarter(orig_date);
      break;
    case TruncUnit::MONTH:
      ret = TruncMonth(orig_date);
      break;
    case TruncUnit::WW:
      ret = TruncWW(orig_date);
      break;
    case TruncUnit::W:
      ret = TruncW(orig_date);
      break;
    case TruncUnit::DAY:
      ret = TruncDay(orig_date);
      break;
    case TruncUnit::DAY_OF_WEEK:
      ret = TruncDayOfWeek(orig_date);
      break;
    case TruncUnit::HOUR:
      ret = TruncHour(orig_date, orig_time);
      break;
    case TruncUnit::MINUTE:
      ret = TruncMinute(orig_date, orig_time);
      break;
    case TruncUnit::MILLENNIUM:
      ret = TruncMillennium(orig_date);
      break;
    case TruncUnit::CENTURY:
      ret = TruncCentury(orig_date);
      break;
    case TruncUnit::DECADE:
      ret = TruncDecade(orig_date);
      break;
    case TruncUnit::WEEK:
      ret = TruncWeek(orig_date);
      break;
    case TruncUnit::SECOND:
      ret = TruncSecond(orig_date, orig_time);
      break;
    case TruncUnit::MILLISECONDS:
      ret = TruncMilliseconds(orig_date, orig_time);
      break;
    case TruncUnit::MICROSECONDS:
      ret = TruncMicroseconds(orig_date, orig_time);
      break;
    default:
      // internal error: implies StrToTruncUnit out of sync with this switch
      ctx->SetError("truncate unit not supported");
      return TimestampVal::null();
  }

  ret.ToTimestampVal(&ret_val);
  return ret_val;
}

// Returns the most recent date, no later than 'orig_date', which is on 'week_day'
// 'week_day' is in [0, 6]; 0 = Monday, 6 = Sunday.
DateValue GoBackToWeekday(const DateValue& orig_date, int week_day) {
  DCHECK(orig_date.IsValid());
  DCHECK(week_day >= 0 && week_day <= 6);

  // Week days are in [0, 6]; 0 = Monday, 6 = Sunday.
  int current_week_day = orig_date.WeekDay();
  DCHECK(current_week_day >= 0 && current_week_day <= 6);

  if (current_week_day == week_day) {
    return orig_date;
  } else if (current_week_day > week_day) {
    return orig_date.AddDays(week_day - current_week_day);
  } else {
    return orig_date.AddDays(week_day - current_week_day - 7);
  }
}

// Used by both TRUNC and DATE_TRUNC functions to perform the truncation
DateVal DoTrunc(const DateValue& date, TruncUnit trunc_unit, FunctionContext* ctx) {
  if (!date.IsValid()) return DateVal::null();

  DCHECK(trunc_unit != TruncUnit::UNIT_INVALID
      && trunc_unit != TruncUnit::MICROSECONDS
      && trunc_unit != TruncUnit::HOUR
      && trunc_unit != TruncUnit::MINUTE
      && trunc_unit != TruncUnit::SECOND
      && trunc_unit != TruncUnit::MILLISECONDS);

  DateValue ret;

  switch(trunc_unit) {
    case TruncUnit::YEAR: {
      int year;
      discard_result(date.ToYear(&year));
      ret = DateValue(year, 1, 1);
      break;
    }
    case TruncUnit::QUARTER: {
      int year, month, day;
      discard_result(date.ToYearMonthDay(&year, &month, &day));
      ret = DateValue(year, BitUtil::RoundDown(month - 1, 3) + 1, 1);
      break;
    }
    case TruncUnit::MONTH: {
      int year, month, day;
      discard_result(date.ToYearMonthDay(&year, &month, &day));
      ret = DateValue(year, month, 1);
      break;
    }
    case TruncUnit::DAY: {
      ret = date;
      break;
    }
    case TruncUnit::WW: {
      int year;
      discard_result(date.ToYear(&year));
      ret = GoBackToWeekday(date, DateValue(year, 1, 1).WeekDay());
      break;
    }
    case TruncUnit::W: {
      int year, month, day;
      discard_result(date.ToYearMonthDay(&year, &month, &day));
      ret = GoBackToWeekday(date, DateValue(year, month, 1).WeekDay());
      break;
    }
    case TruncUnit::DAY_OF_WEEK: {
      // Date of the previous Monday
      ret = GoBackToWeekday(date, 0);
      break;
    }
    case TruncUnit::WEEK: {
      // ISO-8601 week starts on monday. go back to monday
      ret = GoBackToWeekday(date, 0);
      break;
    }
    case TruncUnit::MILLENNIUM: {
      int year;
      discard_result(date.ToYear(&year));
      if (year <= 0) return DateVal::null();
      // First year of current millennium is 2001
      ret = DateValue((year - 1) / 1000 * 1000 + 1, 1, 1);
      break;
    }
    case TruncUnit::CENTURY: {
      int year;
      discard_result(date.ToYear(&year));
      if (year <= 0) return DateVal::null();
      // First year of current century is 2001
      ret = DateValue((year - 1) / 100 * 100 + 1, 1, 1);
      break;
    }
    case TruncUnit::DECADE: {
      int year;
      // Decades start with years ending in '0'.
      discard_result(date.ToYear(&year));
      ret = DateValue(year / 10 * 10, 1, 1);
      break;
    }
    default:
      // internal error: implies StrToTruncUnit out of sync with this switch
      ctx->SetError("truncate unit not supported");
      return DateVal::null();
  }

  return ret.ToDateVal();
}

// Maps the user facing name of a unit to a TExtractField
// Returns the TExtractField for the given unit
TExtractField::type StrToExtractField(FunctionContext* ctx,
    const StringVal& unit_str) {
  StringVal unit = UdfBuiltins::Lower(ctx, unit_str);
  if (UNLIKELY(unit.is_null)) return TExtractField::INVALID_FIELD;
  if (unit == "year") return TExtractField::YEAR;
  if (unit == "quarter") return TExtractField::QUARTER;
  if (unit == "month") return TExtractField::MONTH;
  if (unit == "day") return TExtractField::DAY;
  if (unit == "hour") return TExtractField::HOUR;
  if (unit == "minute") return TExtractField::MINUTE;
  if (unit == "second") return TExtractField::SECOND;
  if (unit == "millisecond") return TExtractField::MILLISECOND;
  if (unit == "epoch") return TExtractField::EPOCH;
  return TExtractField::INVALID_FIELD;
}

static int64_t ExtractMillisecond(const time_duration& time) {
  // Fractional seconds are nanoseconds because Boost is configured
  // to use nanoseconds precision
  return time.fractional_seconds() / (NANOS_PER_MICRO * MICROS_PER_MILLI)
       + time.seconds() * MILLIS_PER_SEC;
}

// Used by both EXTRACT and DATE_PART functions to perform field extraction.
BigIntVal DoExtract(const TimestampValue& tv, TExtractField::type field,
    FunctionContext* ctx) {
  switch (field) {
    case TExtractField::YEAR:
    case TExtractField::QUARTER:
    case TExtractField::MONTH:
    case TExtractField::DAY:
      if (!tv.HasDate()) return BigIntVal::null();
      break;
    case TExtractField::HOUR:
    case TExtractField::MINUTE:
    case TExtractField::SECOND:
    case TExtractField::MILLISECOND:
      if (!tv.HasTime()) return BigIntVal::null();
      break;
    case TExtractField::EPOCH:
      if (!tv.HasDateAndTime()) return BigIntVal::null();
      break;
    case TExtractField::INVALID_FIELD:
      DCHECK(false);
  }

  const date& orig_date = tv.date();
  const time_duration& time = tv.time();

  switch (field) {
    case TExtractField::YEAR: {
      return BigIntVal(orig_date.year());
    }
    case TExtractField::QUARTER: {
      int m = orig_date.month();
      return BigIntVal((m - 1) / 3 + 1);
    }
    case TExtractField::MONTH: {
      return BigIntVal(orig_date.month());
    }
    case TExtractField::DAY: {
      return BigIntVal(orig_date.day());
    }
    case TExtractField::HOUR: {
      return BigIntVal(time.hours());
    }
    case TExtractField::MINUTE: {
      return BigIntVal(time.minutes());
    }
    case TExtractField::SECOND: {
      return BigIntVal(time.seconds());
    }
    case TExtractField::MILLISECOND: {
      return BigIntVal(ExtractMillisecond(time));
    }
    case TExtractField::EPOCH: {
      ptime epoch_date(date(1970, 1, 1), time_duration(0, 0, 0));
      ptime cur_date(orig_date, time);
      time_duration diff = cur_date - epoch_date;
      return BigIntVal(diff.total_seconds());
    }
    default: {
      // internal error: implies StrToExtractField out of sync with this switch
      ctx->SetError("extract unit not supported");
      return BigIntVal::null();
    }
  }
}

// Used by both EXTRACT and DATE_PART functions to perform field extraction.
BigIntVal DoExtract(const DateValue& dv, TExtractField::type field,
    FunctionContext* ctx) {
  if (!dv.IsValid()) return BigIntVal::null();

  DCHECK(field != TExtractField::INVALID_FIELD
      && field != TExtractField::HOUR
      && field != TExtractField::MINUTE
      && field != TExtractField::SECOND
      && field != TExtractField::MILLISECOND
      && field != TExtractField::EPOCH);

  switch (field) {
    case TExtractField::YEAR: {
      int year;
      discard_result(dv.ToYear(&year));
      return BigIntVal(year);
    }
    case TExtractField::QUARTER: {
      int year, month, day;
      discard_result(dv.ToYearMonthDay(&year, &month, &day));
      return BigIntVal((month - 1) / 3 + 1);
    }
    case TExtractField::MONTH: {
      int year, month, day;
      discard_result(dv.ToYearMonthDay(&year, &month, &day));
      return BigIntVal(month);
    }
    case TExtractField::DAY: {
      int year, month, day;
      discard_result(dv.ToYearMonthDay(&year, &month, &day));
      return BigIntVal(day);
    }
    default: {
      // internal error: implies StrToExtractField out of sync with this switch
      ctx->SetError("extract unit not supported");
      return BigIntVal::null();
    }
  }
}

inline TimestampValue FromVal(const TimestampVal& val) {
  return TimestampValue::FromTimestampVal(val);
}

inline DateValue FromVal(const DateVal& val) {
  return DateValue::FromDateVal(val);
}

inline bool IsTimeOfDayUnit(TruncUnit unit) {
  return (unit == TruncUnit::HOUR
      || unit == TruncUnit::MINUTE
      || unit == TruncUnit::SECOND
      || unit == TruncUnit::MILLISECONDS
      || unit == TruncUnit::MICROSECONDS);
}

inline bool IsTimeOfDayUnit(TExtractField::type unit) {
  return (unit == TExtractField::HOUR
      || unit == TExtractField::MINUTE
      || unit == TExtractField::SECOND
      || unit == TExtractField::MILLISECOND
      || unit == TExtractField::EPOCH);
}

inline bool IsInvalidUnit(TruncUnit unit) {
  return (unit == TruncUnit::UNIT_INVALID);
}

inline bool IsInvalidUnit(TExtractField::type unit) {
  return (unit == TExtractField::INVALID_FIELD);
}

/// Used for TRUNC/DATE_TRUNC/EXTRACT/DATE_PART built-in functions.
/// ALLOW_TIME_OF_DAY_UNIT: true iff the built-in function call accepts time-of-day units.
/// UdfType: udf type the built-in function works with.
/// InternalType: Impla's internal type that corresponds to UdfType.
/// ReturnUdfType: The built-in function's return type.
/// UnitType: type to represent unit values.
/// to_unit: function to parse unit strings.
/// do_func: function to implement the built-in function.
/// func_descr: description of the built-in function.
/// unit_descr: description of the unit parameter.
template <
    bool ALLOW_TIME_OF_DAY_UNIT,
    typename UdfType,
    typename InternalType,
    typename ReturnUdfType,
    typename UnitType,
    UnitType to_unit(FunctionContext*, const StringVal&),
    ReturnUdfType do_func(const InternalType&, UnitType, FunctionContext*)>
ReturnUdfType ExtractTruncFuncTempl(FunctionContext* ctx, const UdfType& val,
    const StringVal& unit_str, const string& func_descr, const string& unit_descr) {
  if (val.is_null) return ReturnUdfType::null();

  // resolve 'unit' using the prepared state if possible, o.w. parse now
  // ExtractTruncFuncPrepareTempl() can only parse unit if user passes it as a string
  // literal
  // TODO: it would be nice to resolve the branch before codegen so we can optimise
  // this better.
  UnitType unit;
  void* state = ctx->GetFunctionState(FunctionContext::THREAD_LOCAL);
  if (state != NULL) {
    unit = *reinterpret_cast<UnitType*>(state);
  } else if (unit_str.is_null) {
    ctx->SetError(Substitute("Invalid $0 $1: NULL", func_descr, unit_descr).c_str());
    return ReturnUdfType::null();
  } else {
    unit = to_unit(ctx, unit_str);
    if (!ALLOW_TIME_OF_DAY_UNIT && IsTimeOfDayUnit(unit)) {
      string string_unit(reinterpret_cast<char*>(unit_str.ptr), unit_str.len);
      ctx->SetError(Substitute(
          "Unsupported $0 $1: $2", func_descr, unit_descr, string_unit).c_str());
      return ReturnUdfType::null();
    } else if (IsInvalidUnit(unit)) {
      string string_unit(reinterpret_cast<char*>(unit_str.ptr), unit_str.len);
      ctx->SetError(Substitute(
          "Invalid $0 $1: $2", func_descr, unit_descr, string_unit).c_str());
      return ReturnUdfType::null();
    }
  }
  return do_func(FromVal(val), unit, ctx);
}

/// Does the preparation for TRUNC/DATE_TRUNC/EXTRACT/DATE_PART built-in functions.
/// ALLOW_TIME_OF_DAY_UNIT: true iff the built-in function call accepts time-of-day units.
/// UNIT_IDX: indicates which parameter of the function call is the unit parameter.
/// UnitType: type to represent unit values.
/// to_unit: function to parse unit strings.
/// func_descr: description of the built-in function.
/// unit_descr: description of the unit parameter.
template <
    bool ALLOW_TIME_OF_DAY_UNIT,
    int UNIT_IDX,
    typename UnitType,
    UnitType to_unit(FunctionContext*, const StringVal&)>
void ExtractTruncFuncPrepareTempl(FunctionContext* ctx,
    FunctionContext::FunctionStateScope scope,
    const string& func_descr, const string& unit_descr) {
  // Parse the unit up front if we can, otherwise do it on the fly in trunc_templ()
  if (ctx->IsArgConstant(UNIT_IDX)) {
    StringVal* unit_str = reinterpret_cast<StringVal*>(ctx->GetConstantArg(UNIT_IDX));
    if (unit_str == nullptr || unit_str->is_null) {
      ctx->SetError(Substitute("Invalid $0 $1: NULL", func_descr, unit_descr).c_str());
    } else {
      UnitType unit = to_unit(ctx, *unit_str);
      if (!ALLOW_TIME_OF_DAY_UNIT && IsTimeOfDayUnit(unit)) {
        string string_unit(reinterpret_cast<char*>(unit_str->ptr), unit_str->len);
        ctx->SetError(Substitute(
            "Unsupported $0 $1: $2", func_descr, unit_descr, string_unit).c_str());
      } else if (IsInvalidUnit(unit)) {
        string string_unit(reinterpret_cast<char*>(unit_str->ptr), unit_str->len);
        ctx->SetError(Substitute(
            "Invalid $0 $1: $2", func_descr, unit_descr, string_unit).c_str());
      } else {
        UnitType* state = ctx->Allocate<UnitType>();
        RETURN_IF_NULL(ctx, state);
        *state = unit;
        ctx->SetFunctionState(scope, state);
      }
    }
  }
}

}

void UdfBuiltins::TruncForTimestampPrepareImpl(FunctionContext* ctx,
    FunctionContext::FunctionStateScope scope) {
  return ExtractTruncFuncPrepareTempl<true,
      1,
      TruncUnit,
      StrToTruncUnit>(ctx, scope, "Truncate", "Unit");
}

TimestampVal UdfBuiltins::TruncForTimestampImpl(FunctionContext* ctx,
    const TimestampVal& tv, const StringVal &unit_str) {
  return ExtractTruncFuncTempl<true,
      TimestampVal,
      TimestampValue,
      TimestampVal,
      TruncUnit,
      StrToTruncUnit,
      DoTrunc>(ctx, tv, unit_str, "Truncate", "Unit");
}

void UdfBuiltins::TruncForDatePrepareImpl(FunctionContext* ctx,
    FunctionContext::FunctionStateScope scope) {
  return ExtractTruncFuncPrepareTempl<false,
      1,
      TruncUnit,
      StrToTruncUnit>(ctx, scope, "Truncate", "Unit");
}

DateVal UdfBuiltins::TruncForDateImpl(FunctionContext* ctx, const DateVal& dv,
    const StringVal &unit_str) {
  return ExtractTruncFuncTempl<false,
      DateVal,
      DateValue,
      DateVal,
      TruncUnit,
      StrToTruncUnit,
      DoTrunc>(ctx, dv, unit_str, "Truncate", "Unit");
}

void UdfBuiltins::DateTruncForTimestampPrepareImpl(FunctionContext* ctx,
    FunctionContext::FunctionStateScope scope) {
  return ExtractTruncFuncPrepareTempl<true,
      0,
      TruncUnit,
      StrToDateTruncUnit>(ctx, scope, "Date Truncate", "Unit");
}

TimestampVal UdfBuiltins::DateTruncForTimestampImpl(FunctionContext* ctx,
    const StringVal &unit_str, const TimestampVal& tv) {
  return ExtractTruncFuncTempl<true,
      TimestampVal,
      TimestampValue,
      TimestampVal,
      TruncUnit,
      StrToDateTruncUnit,
      DoTrunc>(ctx, tv, unit_str, "Date Truncate", "Unit");
}

void UdfBuiltins::DateTruncForDatePrepareImpl(FunctionContext* ctx,
    FunctionContext::FunctionStateScope scope) {
  return ExtractTruncFuncPrepareTempl<false,
      0,
      TruncUnit,
      StrToDateTruncUnit>(ctx, scope, "Date Truncate", "Unit");
}

DateVal UdfBuiltins::DateTruncForDateImpl(FunctionContext* ctx, const StringVal &unit_str,
    const DateVal& dv) {
  return ExtractTruncFuncTempl<false,
      DateVal,
      DateValue,
      DateVal,
      TruncUnit,
      StrToDateTruncUnit,
      DoTrunc>(ctx, dv, unit_str, "Date Truncate", "Unit");
}

void UdfBuiltins::ExtractForTimestampPrepareImpl(FunctionContext* ctx,
    FunctionContext::FunctionStateScope scope) {
  return ExtractTruncFuncPrepareTempl<true,
      1,
      TExtractField::type,
      StrToExtractField>(ctx, scope, "Extract", "Field");
}

BigIntVal UdfBuiltins::ExtractForTimestampImpl(FunctionContext* ctx,
    const TimestampVal& tv, const StringVal& unit_str) {
  return ExtractTruncFuncTempl<true,
      TimestampVal,
      TimestampValue,
      BigIntVal,
      TExtractField::type,
      StrToExtractField,
      DoExtract>(ctx, tv, unit_str, "Extract", "Field");
}

void UdfBuiltins::ExtractForDatePrepareImpl(FunctionContext* ctx,
    FunctionContext::FunctionStateScope scope) {
  return ExtractTruncFuncPrepareTempl<false,
      1,
      TExtractField::type,
      StrToExtractField>(ctx, scope, "Extract", "Field");
}

BigIntVal UdfBuiltins::ExtractForDateImpl(FunctionContext* ctx, const DateVal& dv,
    const StringVal& unit_str) {
  return ExtractTruncFuncTempl<false,
      DateVal,
      DateValue,
      BigIntVal,
      TExtractField::type,
      StrToExtractField,
      DoExtract>(ctx, dv, unit_str, "Extract", "Field");
}

void UdfBuiltins::DatePartForTimestampPrepareImpl(FunctionContext* ctx,
    FunctionContext::FunctionStateScope scope) {
  return ExtractTruncFuncPrepareTempl<true,
      0,
      TExtractField::type,
      StrToExtractField>(ctx, scope, "Date Part", "Field");
}

BigIntVal UdfBuiltins::DatePartForTimestampImpl(FunctionContext* ctx,
    const StringVal& unit_str, const TimestampVal& tv) {
  return ExtractTruncFuncTempl<true,
      TimestampVal,
      TimestampValue,
      BigIntVal,
      TExtractField::type,
      StrToExtractField,
      DoExtract>(ctx, tv, unit_str, "Date Part", "Field");
}

void UdfBuiltins::DatePartForDatePrepareImpl(FunctionContext* ctx,
    FunctionContext::FunctionStateScope scope) {
  return ExtractTruncFuncPrepareTempl<false,
      0,
      TExtractField::type,
      StrToExtractField>(ctx, scope, "Date Part", "Field");
}

BigIntVal UdfBuiltins::DatePartForDateImpl(FunctionContext* ctx,
    const StringVal& unit_str, const DateVal& dv) {
  return ExtractTruncFuncTempl<false,
      DateVal,
      DateValue,
      BigIntVal,
      TExtractField::type,
      StrToExtractField,
      DoExtract>(ctx, dv, unit_str, "Date Part", "Field");
}
