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

#include <string>
#include <unordered_map>

#include "cctz/civil_time.h"
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
using impala_udf::DateVal;

class DateFunctions {
 public:
  /// YEAR(DATE d)
  /// Extracts year of the 'd_val' date, returns it as an int in 0-9999 range
  static IntVal Year(FunctionContext* context, const DateVal& d_val);

  /// MONTH(DATE d)
  /// Extracts month of the 'd_val' date and returns it as an int in 1-12 range.
  static IntVal Month(FunctionContext* context, const DateVal& d_val);

  /// DAY(DATE d), DAYOFMONTH(DATE d)
  /// Extracts day-of-month of the 'd_val' date and returns it as an int in 1-31 range.
  static IntVal DayOfMonth(FunctionContext* context, const DateVal& d_val);

  /// QUARTER(DATE d)
  /// Extracts quarter of the 'd_val' date and returns it as an int in 1-4 range.
  static IntVal Quarter(FunctionContext* context, const DateVal& d_val);

  /// DAYOFWEEK(DATE d)
  /// Extracts day-of-week of the 'd_val' date and returns it as an int in 1-7 range.
  /// 1 is Sunday and 7 is Saturday.
  static IntVal DayOfWeek(FunctionContext* context, const DateVal& d_val);

  /// DAYOFYEAR(DATE d)
  /// Extracts day-of-year of the 'd_val' date and returns it as an int in 1-366 range.
  static IntVal DayOfYear(FunctionContext* context, const DateVal& d_val);

  /// WEEKOFYEAR(DATE d)
  /// Extracts week-of-year of the 'd_val' date and returns it as an int in 1-53 range.
  static IntVal WeekOfYear(FunctionContext* context, const DateVal& d_val);

  /// DAYNAME(DATE d)
  /// Returns the day field from a 'd_val' date, converted to the string corresponding to
  /// that day name. The range of return values is "Sunday" to "Saturday".
  static StringVal LongDayName(FunctionContext* context, const DateVal& d_val);

  /// MONTHNAME(DATE d)
  /// Returns the month field from a 'd_val' date, converted to the string corresponding
  /// to that month name. The range of return values is "January" to "December".
  static StringVal LongMonthName(FunctionContext* context, const DateVal& d_val);

  /// NEXT_DAY(DATE d, STRING weekday)
  /// Returns the first date which is later than 'd_val' and named as 'weekday'.
  /// 'weekday' is 3 letters or full name of the day of the week.
  static DateVal NextDay(FunctionContext* context, const DateVal& d_val,
      const StringVal& weekday);

  /// LAST_DAY(DATE d)
  /// Returns the last day of the month which the 'd_val' date belongs to.
  static DateVal LastDay(FunctionContext* context, const DateVal& d_val);

  /// DATEDIFF(DATE d1, DATE d2)
  /// Returns the number of days from 'd_val1' date to 'd_val2' date.
  static IntVal DateDiff(FunctionContext* context, const DateVal& d_val1,
      const DateVal& d_val2);

  /// CURRENT_DATE()
  /// Returns the current date (in the local time zone).
  static DateVal CurrentDate(FunctionContext* context);

  /// DATE_CMP(DATE d1, DATE d2)
  /// Compares 'd_val1' and 'd_val2' dates. Returns:
  /// 1. null, if either 'd_val1' or 'd_val2' is null
  /// 2. -1 if d_val1 < d_val2
  /// 3. 1 if d_val1 > d_val2
  /// 4. 1 if d_val1 == d_val2
  static IntVal DateCmp(FunctionContext* context, const DateVal& d_val1,
      const DateVal& d_val2);

  /// INT_MONTHS_BETWEEN(DATE d1, DATE d2)
  /// Returns the number of months between 'd_val1' and 'd_val2' dates, as an int
  /// representing only the full months that passed.
  /// If 'd_val1' represents an earlier date than 'd_val2', the result is negative.
  static IntVal IntMonthsBetween(FunctionContext* context, const DateVal& d_val1,
      const DateVal& d_val2);

  /// MONTHS_BETWEEN(DATE d1, DATE d2)
  /// Returns the number of months between 'd_val1' and 'd_val2' dates. Can include a
  /// fractional part representing extra days in addition to the full months between the
  /// dates. The fractional component is computed by dividing the difference in days by 31
  /// (regardless of the month).
  /// If 'd_val1' represents an earlier date than 'd_val2', the result is negative.
  static DoubleVal MonthsBetween(FunctionContext* context, const DateVal& d_val1,
      const DateVal& d_val2);

  /// ADD_YEARS(DATE d, INT num_years), ADD_YEARS(DATE d, BIGINT num_years)
  /// SUB_YEARS(DATE d, INT num_years), SUB_YEARS(DATE d, BIGINT num_years)
  /// Adds/subtracts a specified number of years to a date value.
  template <bool is_add, typename AnyIntVal>
  static DateVal AddSubYears(FunctionContext* context, const DateVal& d_val,
      const AnyIntVal& num_years);

  /// ADD_MONTHS(DATE d, INT num_months), ADD_MONTHS(DATE d, BIGINT num_months)
  /// SUB_MONTHS(DATE d, INT num_months), SUB_MONTHS(DATE d, BIGINT num_months)
  /// Adds/subtracts a specified number of months to a date value.
  /// If 'keep_last_day' is set and 'd_val' is the last day of a month, the returned date
  /// will fall on the last day of the target month too.
  template <bool is_add, typename AnyIntVal, bool keep_last_day>
  static DateVal AddSubMonths(FunctionContext* context, const DateVal& d_val,
      const AnyIntVal& num_months);

  /// ADD_DAYS(DATE d, INT num_days), ADD_DAYS(DATE d, BIGINT num_days)
  /// SUB_DAYS(DATE d, INT num_days), SUB_DAYS(DATE d, BIGINT num_days)
  /// Adds/subtracts a specified number of days to a date value.
  template <bool is_add, typename AnyIntVal>
  static DateVal AddSubDays(FunctionContext* context, const DateVal& d_val,
      const AnyIntVal& num_days);

  /// ADD_WEEKS(DATE d, INT num_weeks), ADD_WEEKS(DATE d, BIGINT num_weeks)
  /// SUB_WEEKS(DATE d, INT num_weeks), SUB_WEEKS(DATE d, BIGINT num_weeks)
  /// Adds/subtracts a specified number of weeks to a date value.
  template <bool is_add, typename AnyIntVal>
  static DateVal AddSubWeeks(FunctionContext* context, const DateVal& d_val,
      const AnyIntVal& num_weeks);
};

} // namespace impala
