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

#include "exprs/date-functions.h"

#include <boost/date_time/posix_time/posix_time.hpp>
#include <boost/date_time/local_time/local_time.hpp>

#include "cctz/civil_time.h"
#include "exprs/anyval-util.h"
#include "exprs/timestamp-functions.h"
#include "exprs/udf-builtins.h"
#include "runtime/timestamp-value.h"
#include "runtime/timestamp-value.inline.h"
#include "udf/udf.h"
#include "udf/udf-internal.h"

#include "common/names.h"

namespace impala {

IntVal DateFunctions::Year(FunctionContext* context, const DateVal& d_val) {
  if (d_val.is_null) return IntVal::null();
  DateValue dv = DateValue::FromDateVal(d_val);

  int year;
  if (!dv.ToYear(&year)) return IntVal::null();
  return IntVal(year);
}

IntVal DateFunctions::Quarter(FunctionContext* context, const DateVal& d_val) {
  if (d_val.is_null) return IntVal::null();
  DateValue dv = DateValue::FromDateVal(d_val);

  int year, month, day;
  if (!dv.ToYearMonthDay(&year, &month, &day)) return IntVal::null();
  return IntVal((month - 1) / 3 + 1);
}

IntVal DateFunctions::Month(FunctionContext* context, const DateVal& d_val) {
  if (d_val.is_null) return IntVal::null();
  DateValue dv = DateValue::FromDateVal(d_val);

  int year, month, day;
  if (!dv.ToYearMonthDay(&year, &month, &day)) return IntVal::null();
  return IntVal(month);
}

IntVal DateFunctions::DayOfWeek(FunctionContext* context, const DateVal& d_val) {
  if (d_val.is_null) return IntVal::null();
  DateValue dv = DateValue::FromDateVal(d_val);

  // DAYOFWEEK(DATE) sql function returns day-of-week in [1, 7] range, where 1 = Sunday.
  // dv.WeekDay() returns day-of-week in [0, 6] range. 0 = Monday and 6 = Sunday.
  int wday = dv.WeekDay();
  if (wday == -1) return IntVal::null();
  return IntVal((wday + 1) % 7 + 1);
}

IntVal DateFunctions::DayOfMonth(FunctionContext* context, const DateVal& d_val) {
  if (d_val.is_null) return IntVal::null();
  DateValue dv = DateValue::FromDateVal(d_val);

  int year, month, day;
  if (!dv.ToYearMonthDay(&year, &month, &day)) return IntVal::null();
  return IntVal(day);
}

IntVal DateFunctions::DayOfYear(FunctionContext* context, const DateVal& d_val) {
  if (d_val.is_null) return IntVal::null();
  DateValue dv = DateValue::FromDateVal(d_val);

  // Get the day of the year. DAYOFYEAR(DATE) sql function returns day-of-year in
  // [1, 366]  range.
  int yday = dv.DayOfYear();
  if (yday == -1) return IntVal::null();
  return IntVal(yday);
}

IntVal DateFunctions::WeekOfYear(FunctionContext* context, const DateVal& d_val) {
  if (d_val.is_null) return IntVal::null();
  DateValue dv = DateValue::FromDateVal(d_val);

  int yweek = dv.Iso8601WeekOfYear();
  if (yweek == -1) return IntVal::null();
  return IntVal(yweek);
}

StringVal DateFunctions::LongDayName(FunctionContext* context, const DateVal& d_val) {
  if (d_val.is_null) return StringVal::null();
  DateValue dv = DateValue::FromDateVal(d_val);

  // CCTZ has 0 = Monday and 6 = Sunday.
  int wday = dv.WeekDay();
  if (wday == -1) return StringVal::null();

  DCHECK_GE(wday, 0);
  DCHECK_LE(wday, 6);
  wday = (wday + 1) % 7;
  const string& day_name =
      TimestampFunctions::DAY_NAMES[TimestampFunctions::CAPITALIZED][wday];
  return StringVal(reinterpret_cast<uint8_t*>(const_cast<char*>(day_name.data())),
      day_name.size());
}

StringVal DateFunctions::LongMonthName(FunctionContext* context, const DateVal& d_val) {
  if (d_val.is_null) return StringVal::null();
  DateValue dv = DateValue::FromDateVal(d_val);

  int year, month, day;
  if (!dv.ToYearMonthDay(&year, &month, &day)) return StringVal::null();

  DCHECK_GE(month, 1);
  DCHECK_LE(month, 12);
  const string& mn =
      TimestampFunctions::MONTH_NAMES[TimestampFunctions::CAPITALIZED][month - 1];
  return StringVal(reinterpret_cast<uint8_t*>(const_cast<char*>(mn.data())), mn.size());
}

DateVal DateFunctions::NextDay(FunctionContext* context, const DateVal& d_val,
    const StringVal& weekday) {
  if (weekday.is_null) {
    context->SetError("Invalid Day: NULL");
    return DateVal::null();
  }

  StringVal lweekday = UdfBuiltins::Lower(context, weekday);
  string weekday_str = string(reinterpret_cast<const char*>(lweekday.ptr),
      lweekday.len);

  // DAYNAME_MAP maps Sunday to 0 and Saturday to 6
  const auto it = TimestampFunctions::DAYNAME_MAP.find(weekday_str);
  if (it == TimestampFunctions::DAYNAME_MAP.end()) {
    context->SetError(Substitute("Invalid Day: $0", weekday_str).c_str());
    return DateVal::null();
  } else {
    if (d_val.is_null) return DateVal::null();
    DateValue dv = DateValue::FromDateVal(d_val);

    // WeekDay() returns 0 for Monday and 6 for Sunday.
    int wday = dv.WeekDay();
    if (wday == -1) return DateVal::null();
    DCHECK_GE(wday, 0);
    DCHECK_LE(wday, 6);

    int delta_days = it->second - (wday + 1) % 7;
    delta_days = delta_days <= 0 ? delta_days + 7 : delta_days;
    DCHECK_GE(delta_days, 1);
    DCHECK_LE(delta_days, 7);

    return dv.AddDays(delta_days).ToDateVal();
  }
}

DateVal DateFunctions::LastDay(FunctionContext* context, const DateVal& d_val) {
  if (d_val.is_null) return DateVal::null();
  return DateValue::FromDateVal(d_val).LastDay().ToDateVal();
}

IntVal DateFunctions::DateDiff(FunctionContext* context, const DateVal& d_val1,
    const DateVal& d_val2) {
  if (d_val1.is_null || d_val2.is_null) return IntVal::null();
  DateValue dv1 = DateValue::FromDateVal(d_val1);
  DateValue dv2 = DateValue::FromDateVal(d_val2);

  int32_t dse1, dse2;
  if (!dv1.ToDaysSinceEpoch(&dse1) || !dv2.ToDaysSinceEpoch(&dse2)) return IntVal::null();
  return IntVal(dse1 - dse2);
}

DateVal DateFunctions::CurrentDate(FunctionContext* context) {
  const TimestampValue* now = context->impl()->state()->now();
  const boost::gregorian::date& d = now->date();
  return DateValue(d.year(), d.month(), d.day()).ToDateVal();
}

IntVal DateFunctions::DateCmp(FunctionContext* context, const DateVal& d_val1,
    const DateVal& d_val2) {
  if (d_val1.is_null || d_val2.is_null) return IntVal::null();
  if (d_val1.val > d_val2.val) return 1;
  if (d_val1.val < d_val2.val) return -1;
  return 0;
}

IntVal DateFunctions::IntMonthsBetween(FunctionContext* context,
    const DateVal& d_val1, const DateVal& d_val2) {
  DoubleVal months_between = MonthsBetween(context, d_val1, d_val2);
  if (months_between.is_null) return IntVal::null();
  return IntVal(static_cast<int32_t>(months_between.val));
}

DoubleVal DateFunctions::MonthsBetween(FunctionContext* context,
    const DateVal& d_val1, const DateVal& d_val2) {
  if (d_val1.is_null || d_val2.is_null) return DoubleVal::null();
  DateValue dv1 = DateValue::FromDateVal(d_val1);
  DateValue dv2 = DateValue::FromDateVal(d_val2);

  double months_between;
  if (!dv1.MonthsBetween(dv2, &months_between)) return DoubleVal::null();
  return DoubleVal(months_between);
}

template <bool is_add, typename AnyIntVal>
DateVal DateFunctions::AddSubYears(FunctionContext* context, const DateVal& d_val,
    const AnyIntVal& num_years) {
  if (d_val.is_null || num_years.is_null) return DateVal::null();

  const DateValue dv = DateValue::FromDateVal(d_val).AddYears(
      is_add ? num_years.val : -num_years.val);
  return dv.ToDateVal();
}

template <bool is_add, typename AnyIntVal, bool keep_last_day>
DateVal DateFunctions::AddSubMonths(FunctionContext* context, const DateVal& d_val,
    const AnyIntVal& num_months) {
  if (d_val.is_null || num_months.is_null) return DateVal::null();

  const DateValue dv = DateValue::FromDateVal(d_val).AddMonths(
      is_add ? num_months.val : -num_months.val, keep_last_day);
  return dv.ToDateVal();
}

template <bool is_add, typename AnyIntVal>
DateVal DateFunctions::AddSubDays(FunctionContext* context, const DateVal& d_val,
    const AnyIntVal& num_days) {
  if (d_val.is_null || num_days.is_null) return DateVal::null();

  const DateValue dv = DateValue::FromDateVal(d_val).AddDays(
      is_add ? num_days.val : -num_days.val);
  return dv.ToDateVal();
}

template <bool is_add, typename AnyIntVal>
DateVal DateFunctions::AddSubWeeks(FunctionContext* context, const DateVal& d_val,
    const AnyIntVal& num_weeks) {
  if (d_val.is_null || num_weeks.is_null) return DateVal::null();

  // Sanity check: make sure that the number of weeks converted to days fits into 64-bits.
  int64_t weeks = is_add ? num_weeks.val : -num_weeks.val;
  if (weeks > numeric_limits<int64_t>::max() / 7
      || weeks < numeric_limits<int64_t>::min() / 7) {
    return DateVal::null();
  }

  const DateValue dv = DateValue::FromDateVal(d_val).AddDays(weeks * 7);
  return dv.ToDateVal();
}

// Explicit template instantiation is required for proper linking. These functions
// are only indirectly called via a function pointer provided by the opcode registry
// which does not trigger implicit template instantiation.
// Must be kept in sync with common/function-registry/impala_functions.py.
template DateVal
DateFunctions::AddSubYears<true, IntVal>(FunctionContext* context,
    const DateVal& d_val, const IntVal& count);
template DateVal
DateFunctions::AddSubYears<true, BigIntVal>(FunctionContext* context,
    const DateVal& d_val, const BigIntVal& count);
template DateVal
DateFunctions::AddSubYears<false, IntVal>(FunctionContext* context,
    const DateVal& d_val, const IntVal& count);
template DateVal
DateFunctions::AddSubYears<false, BigIntVal>(FunctionContext* context,
    const DateVal& d_val, const BigIntVal& count);

template DateVal
DateFunctions::AddSubMonths<true, IntVal, true>(FunctionContext* context,
    const DateVal& d_val, const IntVal& count);
template DateVal
DateFunctions::AddSubMonths<true, IntVal, false>(FunctionContext* context,
    const DateVal& d_val, const IntVal& count);
template DateVal
DateFunctions::AddSubMonths<true, BigIntVal, true>(FunctionContext* context,
    const DateVal& d_val, const BigIntVal& count);
template DateVal
DateFunctions::AddSubMonths<true, BigIntVal, false>(FunctionContext* context,
    const DateVal& d_val, const BigIntVal& count);
template DateVal
DateFunctions::AddSubMonths<false, IntVal, true>(FunctionContext* context,
    const DateVal& d_val, const IntVal& count);
template DateVal
DateFunctions::AddSubMonths<false, IntVal, false>(FunctionContext* context,
    const DateVal& d_val, const IntVal& count);
template DateVal
DateFunctions::AddSubMonths<false, BigIntVal, true>(FunctionContext* context,
    const DateVal& d_val, const BigIntVal& count);
template DateVal
DateFunctions::AddSubMonths<false, BigIntVal, false>(FunctionContext* context,
    const DateVal& d_val, const BigIntVal& count);

template DateVal
DateFunctions::AddSubDays<true, IntVal>(FunctionContext* context,
    const DateVal& d_val, const IntVal& count);
template DateVal
DateFunctions::AddSubDays<true, BigIntVal>(FunctionContext* context,
    const DateVal& d_val, const BigIntVal& count);
template DateVal
DateFunctions::AddSubDays<false, IntVal>(FunctionContext* context,
    const DateVal& d_val, const IntVal& count);
template DateVal
DateFunctions::AddSubDays<false, BigIntVal>(FunctionContext* context,
    const DateVal& d_val, const BigIntVal& count);

template DateVal
DateFunctions::AddSubWeeks<true, IntVal>(FunctionContext* context,
    const DateVal& d_val, const IntVal& count);
template DateVal
DateFunctions::AddSubWeeks<true, BigIntVal>(FunctionContext* context,
    const DateVal& d_val, const BigIntVal& count);
template DateVal
DateFunctions::AddSubWeeks<false, IntVal>(FunctionContext* context,
    const DateVal& d_val, const IntVal& count);
template DateVal
DateFunctions::AddSubWeeks<false, BigIntVal>(FunctionContext* context,
    const DateVal& d_val, const BigIntVal& count);
}
