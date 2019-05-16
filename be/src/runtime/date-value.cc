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


#include "runtime/date-value.h"

#include <iomanip>

#include "cctz/civil_time.h"
#include "runtime/date-parse-util.h"

#include "common/names.h"

namespace impala {

namespace {
const int EPOCH_YEAR = 1970;
const cctz::civil_day EPOCH_DATE(EPOCH_YEAR, 1, 1);

inline int32_t CalcDaysSinceEpoch(const cctz::civil_day& date) {
  return date - EPOCH_DATE;
}

}

using datetime_parse_util::DateTimeFormatContext;

const int DateValue::MIN_YEAR = 0;
const int DateValue::MAX_YEAR = 9999;

const int32_t DateValue::MIN_DAYS_SINCE_EPOCH = CalcDaysSinceEpoch(
    cctz::civil_day(MIN_YEAR, 1, 1));
const int32_t DateValue::MAX_DAYS_SINCE_EPOCH = CalcDaysSinceEpoch(
    cctz::civil_day(MAX_YEAR, 12, 31));

const DateValue DateValue::MIN_DATE(MIN_DAYS_SINCE_EPOCH);
const DateValue DateValue::MAX_DATE(MAX_DAYS_SINCE_EPOCH);

DateValue::DateValue(int year, int month, int day)
    : days_since_epoch_(INVALID_DAYS_SINCE_EPOCH) {
  DCHECK(!IsValid());
  // Check year range and whether year-month-day is a valid date.
  if (LIKELY(year >= MIN_YEAR && year <= MAX_YEAR)) {
    // Use CCTZ for validity check.
    cctz::civil_day date(year, month, day);
    if (LIKELY(year == date.year() && month == date.month() && day == date.day())) {
      days_since_epoch_ = CalcDaysSinceEpoch(date);
      DCHECK(IsValid());
    }
  }
}

DateValue DateValue::Parse(const char* str, int len, bool accept_time_toks) {
  DateValue dv;
  discard_result(DateParser::Parse(str, len, accept_time_toks, &dv));
  return dv;
}

DateValue DateValue::Parse(const string& str, bool accept_time_toks) {
  return Parse(str.c_str(), str.size(), accept_time_toks);
}

DateValue DateValue::Parse(const char* str, int len,
    const DateTimeFormatContext& dt_ctx) {
  DateValue dv;
  discard_result(DateParser::Parse(str, len, dt_ctx, &dv));
  return dv;
}

int DateValue::Format(const DateTimeFormatContext& dt_ctx, int len, char* buff) const {
  return DateParser::Format(dt_ctx, *this, len, buff);
}

bool DateValue::ToYearMonthDay(int* year, int* month, int* day) const {
  DCHECK(year != nullptr);
  DCHECK(month != nullptr);
  DCHECK(day != nullptr);
  if (UNLIKELY(!IsValid())) return false;

  const cctz::civil_day cd = EPOCH_DATE + days_since_epoch_;
  *year = cd.year();
  *month = cd.month();
  *day = cd.day();
  return true;
}

namespace {

inline int32_t CalcFirstDayOfYearSinceEpoch(int year) {
  int m400 = year % 400;
  int m100 = m400 % 100;
  int m4 = m100 % 4;

  return (year - EPOCH_YEAR) * 365
      + ((year - EPOCH_YEAR / 4 * 4 + ((m4 != 0) ? 4 - m4 : 0)) / 4 - 1)
      - ((year - EPOCH_YEAR / 100 * 100 + ((m100 != 0) ? 100 - m100 : 0)) / 100 - 1)
      + ((year - EPOCH_YEAR / 400 * 400 + ((m400 != 0) ? 400 - m400 : 0)) / 400 - 1);
}

}

bool DateValue::ToYear(int* year) const {
  DCHECK(year != nullptr);
  if (UNLIKELY(!IsValid())) return false;

  // This function was introduced to extract year of a DateValue efficiently.
  // It will be fast for most days of the year and only slightly slower for days around
  // the beginning and end of the year.
  //
  // Here's a quick explanation. Let's use the following notation:
  // m400 = year % 400
  // m100 = m400 % 100
  // m4 = m100 % 4
  //
  // If 'days' is the number of days between 1970-01-01 and the first day of 'year'
  // (excluding the endpoint), then the following is true:
  // days == (year - 1970) * 365
  //       + ((year - 1968 + ((m4 != 0) ? 4 - m4 : 0)) / 4 - 1)
  //       - ((year - 1900 + ((m100 != 0) ? 100 - m100 : 0)) / 100 - 1)
  //       + ((year - 1600 + ((m400 != 0) ? 400 - m400 : 0)) / 400 - 1)
  //
  // Reordering the equation we get:
  // days * 400 == (year - 1970) * 365 * 400
  //       + ((year - 1968) * 100 + ((m4 != 0) ? (4 - m4) * 100 : 0) - 400)
  //       - ((year - 1900) * 4 + ((m100 != 0) ? (100 - m100) * 4 : 0) - 400)
  //       + (year - 1600 + ((m400 != 0) ? 400 - m400 : 0) - 400)
  //
  // then:
  // days * 400 == year * 146000 - 287620000
  //       + (year * 100 - 196800 + ((m4 != 0) ? (4 - m4) * 100 : 0) - 400)
  //       - (year * 4 - 7600 + ((m100 != 0) ? (100 - m100) * 4 : 0) - 400)
  //       + (year - 1600 + ((m400 != 0) ? 400 - m400 : 0) - 400)
  //
  // which means that (A):
  // year * 146097 == days * 400 + 287811200
  //       - ((m4 != 0) ? (4 - m4) * 100 : 0)
  //       + ((m100 != 0) ? (100 - m100) * 4 : 0)
  //       - ((m400 != 0) ? 400 - m400 : 0)
  //
  // On the other hand, if
  // f(year) = - ((m4 != 0) ? (4 - m4) * 100 : 0)
  //           + ((m100 != 0) ? (100 - m100) * 4 : 0)
  //           - ((m400 != 0) ? 400 - m400 : 0)
  // and 'year' is in the [0, 9999] range, then it follows that (B):
  // f(year) must fall into the [-591, 288] range.
  //
  // Finally, if we put (A) and (B) together we can conclude that 'year' must fall into
  // the
  // [ (days * 400 + 287811200 - 591) / 146097, (days * 400 + 287811200 + 288) / 146097 ]
  // range.

  int tmp = days_since_epoch_ * 400 + 287811200;
  int first_year = (tmp - 591) / 146097;
  int last_year = (tmp + 288) / 146097;

  if (first_year == last_year) {
    *year = first_year;
  } else if (CalcFirstDayOfYearSinceEpoch(last_year) <= days_since_epoch_) {
    *year = last_year;
  } else {
    *year = first_year;
  }

  return true;
}

int DateValue::WeekDay() const {
  if (UNLIKELY(!IsValid())) return -1;
  const cctz::civil_day cd = EPOCH_DATE + days_since_epoch_;
  return static_cast<int>(cctz::get_weekday(cd));
}

DateValue DateValue::AddDays(int days) const {
  if (UNLIKELY(!IsValid())) return DateValue();
  return DateValue(days_since_epoch_ + days);
}

bool DateValue::ToDaysSinceEpoch(int32_t* days) const {
  DCHECK(days != nullptr);
  if (UNLIKELY(!IsValid())) return false;

  *days =  days_since_epoch_;
  return true;
}

string DateValue::ToString() const {
  stringstream ss;
  int year, month, day;
  if (ToYearMonthDay(&year, &month, &day)) {
    ss << std::setfill('0') << setw(4) << year << "-" << setw(2) << month << "-"
       << setw(2) << day;
  }
  return ss.str();
}

ostream& operator<<(ostream& os, const DateValue& date_value) {
  return os << date_value.ToString();
}

}
