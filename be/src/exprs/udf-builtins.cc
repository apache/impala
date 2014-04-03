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

#include "exprs/udf-builtins.h"
#include "runtime/timestamp-value.h"
#include "util/bit-util.h"

#include <ctype.h>
#include <math.h>
#include <gutil/strings/substitute.h>

using namespace std;
using namespace boost::gregorian;
using namespace boost::posix_time;
using namespace strings;

namespace impala {

DoubleVal UdfBuiltins::Abs(FunctionContext* context, const DoubleVal& v) {
  if (v.is_null) return v;
  return DoubleVal(fabs(v.val));
}

DoubleVal UdfBuiltins::Pi(FunctionContext* context) {
  return DoubleVal(M_PI);
}

StringVal UdfBuiltins::Lower(FunctionContext* context, const StringVal& v) {
  if (v.is_null) return v;
  StringVal result(context, v.len);
  for (int i = 0; i < v.len; ++i) {
    result.ptr[i] = tolower(v.ptr[i]);
  }
  return result;
}

// The units which can be used when Truncating a Timestamp
struct TruncUnit {
  enum Type {
    YEAR,
    QUARTER,
    MONTH,
    WW,
    W,
    DAY,
    DAY_OF_WEEK,
    HOUR,
    MINUTE,
    UNIT_INVALID
  };
};

// Maps the user facing name of a unit to a TruncUnit
// Returns the TruncUnit via parameter trunc_unit
// Returns true if unit is a known unit, else false
TruncUnit::Type StrToTruncUnit(const StringVal& unit) {
  if ((unit == "SYYYY") || (unit == "YYYY") || (unit == "YEAR") || (unit == "SYEAR") ||
      (unit == "YYY") || (unit == "YY") || (unit == "Y")) {
    return TruncUnit::YEAR;
  } else if (unit == "Q") {
    return TruncUnit::QUARTER;
  } else if ((unit == "MONTH") || (unit == "MON") || (unit == "MM") || (unit == "RM")) {
    return TruncUnit::MONTH;
  } else if (unit == "WW") {
    return TruncUnit::WW;
  } else if (unit == "W") {
    return TruncUnit::W;
  } else if ((unit == "DDD") || (unit == "DD") || (unit == "J")) {
    return TruncUnit::DAY;
  } else if ((unit == "DAY") || (unit == "DY") || (unit == "D")) {
    return TruncUnit::DAY_OF_WEEK;
  } else if ((unit == "HH") || (unit == "HH12") || (unit == "HH24")) {
    return TruncUnit::HOUR;
  } else if (unit == "MI") {
    return TruncUnit::MINUTE;
  } else {
    return TruncUnit::UNIT_INVALID;
  }
}

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

// Same day of the week as the first day of the year
TimestampValue TruncWW(const date& orig_date) {
  const date& first_day_of_year = TruncYear(orig_date).get_date();
  int target_week_day = first_day_of_year.day_of_week();
  date new_date = GoBackToWeekday(orig_date, target_week_day);
  time_duration new_time(0, 0, 0, 0);
  return TimestampValue(new_date, new_time);
}

// Same day of the week as the first day of the month
TimestampValue TruncW(const date& orig_date) {
  const date& first_day_of_mon = TruncMonth(orig_date).get_date();
  const date& new_date = GoBackToWeekday(orig_date, first_day_of_mon.day_of_week());
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
  const date& new_date = GoBackToWeekday(orig_date, 1);
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

TimestampVal UdfBuiltins::Trunc(
    FunctionContext* context, const TimestampVal& tv, const StringVal &unit_str) {
  const TimestampValue& ts = TimestampValue::FromTimestampVal(tv);
  const date& orig_date = ts.get_date();
  const time_duration& orig_time = ts.get_time();

  // resolve trunc_unit using the prepared state if possible, o.w. parse now
  // TruncPrepare() can only parse trunc_unit if user passes it as a string literal
  TruncUnit::Type trunc_unit;
  void* state = context->GetFunctionState(FunctionContext::THREAD_LOCAL);
  if (state != NULL) {
    trunc_unit = *reinterpret_cast<TruncUnit::Type*>(state);
  } else {
    trunc_unit = StrToTruncUnit(unit_str);
    if (trunc_unit == TruncUnit::UNIT_INVALID) {
      string string_unit(reinterpret_cast<char*>(unit_str.ptr), unit_str.len);
      context->SetError(Substitute("Invalid Truncate Unit: $0", string_unit).c_str());
      return TimestampVal::null();
    }
  }

  TimestampValue ret;
  TimestampVal ret_val;
  switch (trunc_unit) {
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
    default:
      // internal error: implies StrToTruncUnit out of sync with this switch
      context->SetError(Substitute("truncate unit $0 not supported", trunc_unit).c_str());
      return TimestampVal::null();
  }
  ret.ToTimestampVal(&ret_val);
  return ret_val;
}

void UdfBuiltins::TruncPrepare(FunctionContext* ctx,
                               FunctionContext::FunctionStateScope scope) {
  // Parse the unit up front if we can, otherwise do it on the fly in Trunc()
  if (ctx->IsArgConstant(1)) {
    StringVal* unit_str = reinterpret_cast<StringVal*>(ctx->GetConstantArg(1));
    TruncUnit::Type trunc_unit = StrToTruncUnit(*unit_str);
    if (trunc_unit == TruncUnit::UNIT_INVALID) {
      string string_unit(reinterpret_cast<char*>(unit_str->ptr), unit_str->len);
      ctx->SetError(Substitute("Invalid Truncate Unit: $0", string_unit).c_str());
    } else {
      TruncUnit::Type* state = reinterpret_cast<TruncUnit::Type*>(
          ctx->Allocate(sizeof(TruncUnit::Type)));
      *state = trunc_unit;
      ctx->SetFunctionState(scope, state);
    }
  }
}

void UdfBuiltins::TruncClose(FunctionContext* ctx,
                             FunctionContext::FunctionStateScope scope) {
  void* state = ctx->GetFunctionState(scope);
  if (state != NULL) {
    ctx->Free(reinterpret_cast<uint8_t*>(state));
    ctx->SetFunctionState(scope, NULL);
  }
}
} // namespace impala
