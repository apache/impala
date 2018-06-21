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
const cctz::civil_day EPOCH_DATE(1970, 1, 1);

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
