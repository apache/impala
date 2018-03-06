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

#include "gen-cpp/Exprs_types.h"
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
TimestampValue TruncMilliSeconds(const date& orig_date, const time_duration& orig_time) {
  time_duration new_time(orig_time.hours(), orig_time.minutes(), orig_time.seconds());
  // Fractional seconds are nanoseconds because Boost is configured to use nanoseconds
  // precision.
  time_duration fraction = milliseconds(orig_time.fractional_seconds() / 1000000);
  new_time = new_time + fraction;
  return TimestampValue(orig_date, new_time);
}

// Truncate parts of microseconds
TimestampValue TruncMicroSeconds(const date& orig_date, const time_duration& orig_time) {
  time_duration new_time(orig_time.hours(), orig_time.minutes(), orig_time.seconds());
  // Fractional seconds are nanoseconds because Boost is configured to use nanoseconds
  // precision.
  time_duration fraction = microseconds(orig_time.fractional_seconds() / 1000);
  new_time = new_time + fraction;
  return TimestampValue(orig_date, new_time);
}

// used by both Trunc and DateTrunc functions to perform the truncation
TimestampVal DoTrunc(
    const TimestampValue ts, TruncUnit trunc_unit, FunctionContext* context) {
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
      ret = TruncMilliSeconds(orig_date, orig_time);
      break;
    case TruncUnit::MICROSECONDS:
      ret = TruncMicroSeconds(orig_date, orig_time);
      break;
    default:
      // internal error: implies StrToTruncUnit out of sync with this switch
      context->SetError("truncate unit not supported");
      return TimestampVal::null();
  }

  ret.ToTimestampVal(&ret_val);
  return ret_val;
}
}

TimestampVal UdfBuiltins::TruncImpl(
    FunctionContext* context, const TimestampVal& tv, const StringVal& unit_str) {
  if (tv.is_null) return TimestampVal::null();
  TimestampValue ts = TimestampValue::FromTimestampVal(tv);

  // resolve trunc_unit using the prepared state if possible, o.w. parse now
  // TruncPrepare() can only parse trunc_unit if user passes it as a string literal
  // TODO: it would be nice to resolve the branch before codegen so we can optimise
  // this better.
  TruncUnit trunc_unit;
  void* state = context->GetFunctionState(FunctionContext::THREAD_LOCAL);
  if (state != NULL) {
    trunc_unit = *reinterpret_cast<TruncUnit*>(state);
  } else {
    trunc_unit = StrToTruncUnit(context, unit_str);
    if (trunc_unit == TruncUnit::UNIT_INVALID) {
      string string_unit(reinterpret_cast<char*>(unit_str.ptr), unit_str.len);
      context->SetError(Substitute("Invalid Truncate Unit: $0", string_unit).c_str());
      return TimestampVal::null();
    }
  }
  return DoTrunc(ts, trunc_unit, context);
}

void UdfBuiltins::TruncPrepare(
    FunctionContext* ctx, FunctionContext::FunctionStateScope scope) {
  // Parse the unit up front if we can, otherwise do it on the fly in Trunc()
  if (ctx->IsArgConstant(1)) {
    StringVal* unit_str = reinterpret_cast<StringVal*>(ctx->GetConstantArg(1));
    TruncUnit trunc_unit = StrToTruncUnit(ctx, *unit_str);
    if (trunc_unit == TruncUnit::UNIT_INVALID) {
      string string_unit(reinterpret_cast<char*>(unit_str->ptr), unit_str->len);
      ctx->SetError(Substitute("Invalid Truncate Unit: $0", string_unit).c_str());
    } else {
      TruncUnit* state = ctx->Allocate<TruncUnit>();
      RETURN_IF_NULL(ctx, state);
      *state = trunc_unit;
      ctx->SetFunctionState(scope, state);
    }
  }
}

void UdfBuiltins::TruncClose(FunctionContext* ctx,
    FunctionContext::FunctionStateScope scope) {
  void* state = ctx->GetFunctionState(scope);
  ctx->Free(reinterpret_cast<uint8_t*>(state));
  ctx->SetFunctionState(scope, nullptr);
}

TimestampVal UdfBuiltins::DateTruncImpl(
    FunctionContext* context, const TimestampVal& tv, const StringVal& unit_str) {
  if (tv.is_null) return TimestampVal::null();
  TimestampValue ts = TimestampValue::FromTimestampVal(tv);

  // resolve date_trunc_unit using the prepared state if possible, o.w. parse now
  // DateTruncPrepare() can only parse trunc_unit if user passes it as a string literal
  TruncUnit date_trunc_unit;
  void* state = context->GetFunctionState(FunctionContext::THREAD_LOCAL);
  if (state != NULL) {
    date_trunc_unit = *reinterpret_cast<TruncUnit*>(state);
  } else {
    date_trunc_unit = StrToDateTruncUnit(context, unit_str);
    if (date_trunc_unit == TruncUnit::UNIT_INVALID) {
      string string_unit(reinterpret_cast<char*>(unit_str.ptr), unit_str.len);
      context->SetError(
          Substitute("Invalid Date Truncate Unit: $0", string_unit).c_str());
      return TimestampVal::null();
    }
  }
  return DoTrunc(ts, date_trunc_unit, context);
}

void UdfBuiltins::DateTruncPrepare(
    FunctionContext* ctx, FunctionContext::FunctionStateScope scope) {
  // Parse the unit up front if we can, otherwise do it on the fly in DateTrunc()
  if (ctx->IsArgConstant(0)) {
    StringVal* unit_str = reinterpret_cast<StringVal*>(ctx->GetConstantArg(0));
    TruncUnit date_trunc_unit = StrToDateTruncUnit(ctx, *unit_str);
    if (date_trunc_unit == TruncUnit::UNIT_INVALID) {
      string string_unit(reinterpret_cast<char*>(unit_str->ptr), unit_str->len);
      ctx->SetError(Substitute("Invalid Date Truncate Unit: $0", string_unit).c_str());
    } else {
      TruncUnit* state = ctx->Allocate<TruncUnit>();
      RETURN_IF_NULL(ctx, state);
      *state = date_trunc_unit;
      ctx->SetFunctionState(scope, state);
    }
  }
}

void UdfBuiltins::DateTruncClose(
    FunctionContext* ctx, FunctionContext::FunctionStateScope scope) {
  void* state = ctx->GetFunctionState(scope);
  ctx->Free(reinterpret_cast<uint8_t*>(state));
  ctx->SetFunctionState(scope, nullptr);
}
