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
#include "runtime/datetime-simple-date-format-parser.h"

#include "common/names.h"

namespace impala {

using datetime_parse_util::DateTimeFormatContext;
using datetime_parse_util::GetMonthAndDayFromDaysSinceJan1;
using datetime_parse_util::IsLeapYear;
using datetime_parse_util::SimpleDateFormatTokenizer;

const int EPOCH_YEAR = 1970;
const int MIN_YEAR = 1;
const int MAX_YEAR = 9999;

const cctz::civil_day EPOCH_DATE(EPOCH_YEAR, 1, 1);

const int32_t DateValue::MIN_DAYS_SINCE_EPOCH =
    cctz::civil_day(MIN_YEAR, 1, 1) - EPOCH_DATE;
const int32_t DateValue::MAX_DAYS_SINCE_EPOCH =
    cctz::civil_day(MAX_YEAR, 12, 31) - EPOCH_DATE;

const DateValue DateValue::MIN_DATE(MIN_DAYS_SINCE_EPOCH);
const DateValue DateValue::MAX_DATE(MAX_DAYS_SINCE_EPOCH);

DateValue::DateValue(int64_t year, int64_t month, int64_t day)
    : days_since_epoch_(INVALID_DAYS_SINCE_EPOCH) {
  DCHECK(!IsValid());
  // Check year range and whether year-month-day is a valid date.
  if (LIKELY(year >= MIN_YEAR && year <= MAX_YEAR)) {
    // Use CCTZ for validity check.
    cctz::civil_day date(year, month, day);
    if (LIKELY(year == date.year() && month == date.month() && day == date.day())) {
      days_since_epoch_ = date - EPOCH_DATE;
      DCHECK(IsValid());
    }
  }
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

// Returns the Monday that belongs to the first ISO 8601 week of 'year'. That is:
// - If Jan 1 falls on Monday, Tuesday, Wednesday or Thursday, then the week of Jan 1 is
//   the first ISO 8601 week of 'year'. Therefore the Monday of the first ISO 8601 week in
//   'year' is the first Monday before Jan 2.
// - If Jan 1 falls on Friday, Saturday or Sunday, then the week of Jan 1 is the last ISO
//   8601 week of the previous year. Therefore the Monday of the first ISO 8601 week in
//   'year' is the first Monday following Jan 1.
inline cctz::civil_day GetFirstIso8601MondayOfYear(int year) {
  cctz::civil_day jan1 = cctz::civil_day(year, 1, 1);
  if (cctz::get_weekday(jan1) <= cctz::weekday::thursday) {
    // Get the previous Monday if 'jan1' is not already a Monday.
    return cctz::next_weekday(jan1, cctz::weekday::monday) - 7;
  } else {
    // Get the next Monday.
    return cctz::next_weekday(jan1, cctz::weekday::monday);
  }
}

// Returns Sunday that belongs to the last ISO 8601 week of 'year'. That is:
// - If Dec 31 falls on Thursday, Friday, Saturday or Sunday, then the week of Dec 31
//   is the last ISO 8601 week of 'year'. Therefore the Sunday of the last ISO 8601 week
//   in 'year' is the first Sunday after Dec 30.
// - If Dec 31 falls on Monday, Tuesday or Wednesday, then the week of Dec 31 is the first
//   ISO 8601 week of the next year. Therefore the Sunday of the last ISO 8601 week in
//   'year' is the first Sunday previous to Dec 31.
inline cctz::civil_day GetLastIso8601SundayOfYear(int year) {
  cctz::civil_day dec31 = cctz::civil_day(year, 12, 31);
  if (cctz::get_weekday(dec31) >= cctz::weekday::thursday) {
    // Get the next Sunday if 'dec31' is not already a Sunday.
    return cctz::prev_weekday(dec31, cctz::weekday::sunday) + 7;
  } else {
    // Get the previous Sunday.
    return cctz::prev_weekday(dec31, cctz::weekday::sunday);
  }
}

}

DateValue DateValue::CreateFromIso8601WeekBasedDateVals(int year,
    int week_of_year, int day_of_week) {
  if (year >= MIN_YEAR && year <= MAX_YEAR
      && week_of_year >= 1 && week_of_year <= 53
      && 1 <= day_of_week && day_of_week <= 7) {
    const cctz::civil_day first_monday = GetFirstIso8601MondayOfYear(year);
    const cctz::civil_day last_sunday = GetLastIso8601SundayOfYear(year);

    const cctz::civil_day today = first_monday + (week_of_year - 1) * 7 + day_of_week - 1;
    if (today <= last_sunday) return DateValue(today - EPOCH_DATE);
  }
  return DateValue();
}

DateValue DateValue::ParseSimpleDateFormat(const char* str, int len,
    bool accept_time_toks) {
  DateValue dv;
  discard_result(DateParser::ParseSimpleDateFormat(str, len, accept_time_toks, &dv));
  return dv;
}

DateValue DateValue::ParseSimpleDateFormat(const string& str, bool accept_time_toks) {
  return ParseSimpleDateFormat(str.c_str(), str.size(), accept_time_toks);
}

DateValue DateValue::ParseSimpleDateFormat(const char* str, int len,
    const DateTimeFormatContext& dt_ctx) {
  DateValue dv;
  discard_result(DateParser::ParseSimpleDateFormat(str, len, dt_ctx, &dv));
  return dv;
}

DateValue DateValue::ParseIsoSqlFormat(const char* str, int len,
      const DateTimeFormatContext& dt_ctx) {
  DateValue dv;
  discard_result(DateParser::ParseIsoSqlFormat(str, len, dt_ctx, &dv));
  return dv;
}

string DateValue::Format(const DateTimeFormatContext& dt_ctx) const {
  return DateParser::Format(dt_ctx, *this);
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
  // and 'year' is in the [1, 9999] range, then it follows that (B):
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
  DCHECK(*year >= MIN_YEAR && *year <= MAX_YEAR);

  return true;
}

bool DateValue::ToYearMonthDay(int* year, int* month, int* day) const {
  DCHECK(year != nullptr);
  DCHECK(month != nullptr);
  DCHECK(day != nullptr);
  if (UNLIKELY(!IsValid())) return false;

  // Uses the same method to calculate the year as DateValue::ToYear().
  int tmp = days_since_epoch_ * 400 + 287811200;
  int first_year = (tmp - 591) / 146097;
  int last_year = (tmp + 288) / 146097;

  int jan1_dse = CalcFirstDayOfYearSinceEpoch(last_year);
  if (jan1_dse <= days_since_epoch_) {
    *year = last_year;
  } else {
    *year = first_year;
    jan1_dse -= IsLeapYear(first_year) ? 366 : 365;
  }
  DCHECK(*year >= MIN_YEAR && *year <= MAX_YEAR);

  // Day of year. 0 is used for January 1.
  int days_since_jan1 = days_since_epoch_ - jan1_dse;

  return GetMonthAndDayFromDaysSinceJan1(*year, days_since_jan1, month, day);
}

int DateValue::WeekDay() const {
  if (UNLIKELY(!IsValid())) return -1;
  const cctz::civil_day cd = EPOCH_DATE + days_since_epoch_;
  return static_cast<int>(cctz::get_weekday(cd));
}

int DateValue::DayOfYear() const {
  if (UNLIKELY(!IsValid())) return -1;
  const cctz::civil_day cd = EPOCH_DATE + days_since_epoch_;
  return static_cast<int>(cctz::get_yearday(cd));
}

int DateValue::Iso8601WeekOfYear() const {
  if (UNLIKELY(!IsValid())) return -1;
  const cctz::civil_day today = EPOCH_DATE + days_since_epoch_;
  const cctz::civil_day first_monday = GetFirstIso8601MondayOfYear(today.year());
  const cctz::civil_day last_sunday = GetLastIso8601SundayOfYear(today.year());

  // 0001-01-01 is Monday in the proleptic Gregorian calendar.
  // 0001-01-01 belongs to year 0001.
  // 9999-12-31 is Friday in the proleptic Gregorian calendar.
  // 9999-12-31 belongs to year 9999.
  if (today >= first_monday && today <= last_sunday) {
    return (today - first_monday) / 7 + 1;
  } else if (today > last_sunday) {
    DCHECK(today.year() >= MIN_YEAR && today.year() < MAX_YEAR);
    return 1;
  } else {
    DCHECK(today < first_monday);
    DCHECK(today.year() > MIN_YEAR && today.year() <= MAX_YEAR);
    cctz::civil_day prev_jan1 = cctz::civil_day(today.year() - 1, 1, 1);
    cctz::civil_day prev_first_monday;
    if (cctz::get_weekday(prev_jan1) <= cctz::weekday::thursday) {
      // Get the previous Monday if 'prev_jan1' is not already a Monday.
      prev_first_monday = cctz::next_weekday(prev_jan1, cctz::weekday::monday) - 7;
    } else {
      // Get the next Monday.
      prev_first_monday = cctz::next_weekday(prev_jan1, cctz::weekday::monday);
    }
    return (today - prev_first_monday) / 7 + 1;
  }
}

int DateValue::Iso8601WeekNumberingYear() const {
  if (UNLIKELY(!IsValid())) return -1;
  const cctz::civil_day today = EPOCH_DATE + days_since_epoch_;
  const cctz::civil_day first_monday = GetFirstIso8601MondayOfYear(today.year());
  const cctz::civil_day last_sunday = GetLastIso8601SundayOfYear(today.year());

  // 0001-01-01 is Monday in the proleptic Gregorian calendar.
  // 0001-01-01 belongs to year 0001.
  // 9999-12-31 is Friday in the proleptic Gregorian calendar.
  // 9999-12-31 belongs to year 9999.
  if (today >= first_monday && today <= last_sunday) {
    return today.year();
  } else if (today > last_sunday) {
    DCHECK(today.year() >= MIN_YEAR && today.year() < MAX_YEAR);
    return today.year() + 1;
  } else {
    DCHECK(today < first_monday);
    DCHECK(today.year() > MIN_YEAR && today.year() <= MAX_YEAR);
    return today.year() - 1;
  }
}

DateValue DateValue::AddDays(int64_t days) const {
  if (UNLIKELY(!IsValid()
      || days < MIN_DAYS_SINCE_EPOCH - days_since_epoch_
      || days > MAX_DAYS_SINCE_EPOCH - days_since_epoch_)) {
    return DateValue();
  }
  return DateValue(days_since_epoch_ + days);
}

DateValue DateValue::AddMonths(int64_t months, bool keep_last_day) const {
  if (UNLIKELY(!IsValid())) return DateValue();

  const cctz::civil_day today = EPOCH_DATE + days_since_epoch_;
  int64_t total_months = today.year()*12 + today.month() - 1;
  if (UNLIKELY(months < MIN_YEAR*12 - total_months
      || months > MAX_YEAR*12 + 11 - total_months)) {
    return DateValue();
  }
  const cctz::civil_month month = cctz::civil_month(today);
  const cctz::civil_month result_month = month + months;
  const cctz::civil_day last_day_of_result_month =
      cctz::civil_day(result_month + 1) - 1;

  if (keep_last_day) {
    const cctz::civil_day last_day_of_month = cctz::civil_day(month + 1) - 1;
    if (today == last_day_of_month) {
      return DateValue(last_day_of_result_month.year(),
          last_day_of_result_month.month(), last_day_of_result_month.day());
    }
  }

  const cctz::civil_day ans_normalized = cctz::civil_day(result_month.year(),
      result_month.month(), today.day());
  const cctz::civil_day ans_capped = std::min(ans_normalized, last_day_of_result_month);
  return DateValue(ans_capped.year(), ans_capped.month(), ans_capped.day());
}

DateValue DateValue::AddYears(int64_t years) const {
  if (UNLIKELY(!IsValid())) return DateValue();

  const cctz::civil_day today = EPOCH_DATE + days_since_epoch_;
  if (UNLIKELY(years < MIN_YEAR - today.year() || years > MAX_YEAR - today.year())) {
    return DateValue();
  }
  const int64_t result_year = today.year() + years;

  // Feb 29 in leap years requires special attention.
  if (UNLIKELY(today.month() == 2 && today.day() == 29)) {
    const cctz::civil_month result_month(result_year, today.month());
    const cctz::civil_day last_day_of_result_month =
        cctz::civil_day(result_month + 1) - 1;
    return DateValue(result_year, last_day_of_result_month.month(),
        last_day_of_result_month.day());
  }
  return DateValue(result_year, today.month(), today.day());
}

DateValue DateValue::SubtractDays(int64_t days) const {
  if (UNLIKELY(!IsValid())) {
    return DateValue();
  }
  int64_t delta = days_since_epoch_ - days;
  if (delta < MIN_DAYS_SINCE_EPOCH || delta > MAX_DAYS_SINCE_EPOCH) {
    return DateValue();
  }
  return DateValue(delta);
}

DateValue DateValue::FindMiddleDate(const DateValue& min, const DateValue& max) {
  if (UNLIKELY(!min.IsValid() || !max.IsValid())) {
    return DateValue();
  }
  return DateValue((min.days_since_epoch_ + max.days_since_epoch_) / 2);
}

bool DateValue::ToDaysSinceEpoch(int32_t* days) const {
  DCHECK(days != nullptr);
  if (UNLIKELY(!IsValid())) return false;

  *days = days_since_epoch_;
  return true;
}

DateValue DateValue::LastDay() const {
  if (UNLIKELY(!IsValid())) return DateValue();

  const cctz::civil_day today = EPOCH_DATE + days_since_epoch_;
  const cctz::civil_month month = cctz::civil_month(today);
  const cctz::civil_day last_day_of_month = cctz::civil_day(month + 1) - 1;
  return DateValue(last_day_of_month - EPOCH_DATE);
}

bool DateValue::MonthsBetween(const DateValue& other, double* months_between) const {
  DCHECK(months_between != nullptr);
  if (UNLIKELY(!IsValid() || !other.IsValid())) return false;

  const cctz::civil_day today = EPOCH_DATE + days_since_epoch_;
  const cctz::civil_month month(today);
  const cctz::civil_day last_day_of_month = cctz::civil_day(month + 1) - 1;

  const cctz::civil_day other_date = EPOCH_DATE + other.days_since_epoch_;
  const cctz::civil_month other_month(other_date);
  const cctz::civil_day last_day_of_other_month = cctz::civil_day(other_month + 1) - 1;

  // If both dates are last days of different months they don't contribute
  // a fractional value to the number of months, therefore there is no need to
  // calculate difference in their days.
  int days_diff = 0;
  if (today != last_day_of_month || other_date != last_day_of_other_month) {
    days_diff = today.day() - other_date.day();
  }

  *months_between = (today.year() - other_date.year()) * 12 +
      today.month() - other_date.month() + (static_cast<double>(days_diff) / 31.0);
  return true;
}

string DateValue::ToString() const {
  string s;
  s.resize(SimpleDateFormatTokenizer::DEFAULT_DATE_FMT_LEN);
  const int out_len = DateParser::FormatDefault(*this, s.data());
  if (UNLIKELY(out_len != SimpleDateFormatTokenizer::DEFAULT_DATE_FMT_LEN)) {
    s.clear();
  }
  return s;
}

ostream& operator<<(ostream& os, const DateValue& date_value) {
  char dst[SimpleDateFormatTokenizer::DEFAULT_DATE_FMT_LEN + 1];
  const int out_len = DateParser::FormatDefault(date_value, dst);
  if (LIKELY(out_len >= 0)) {
    dst[out_len] = '\0';
    os << dst;
  }
  return os;
}

}
