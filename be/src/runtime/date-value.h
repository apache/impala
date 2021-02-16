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

#include <limits>
#include <string>

#include "common/logging.h"
#include "common/status.h"
#include "gen-cpp/common.pb.h"
#include "udf/udf.h"

namespace impala {

namespace datetime_parse_util {
struct DateTimeFormatContext;
}

/// Represents a DATE value.
/// - The minimum and maximum dates are 0001-01-01 and 9999-12-31. Valid dates must fall
///   in this range.
/// - Internally represents DATE values as number of days since 1970-01-01.
/// - This representation was chosen to be the same (bit-by-bit) as Parquet's date type.
///   (https://github.com/apache/parquet-format/blob/master/LogicalTypes.md#date)
/// - Proleptic Gregorian calendar is used to calculate the number of days since epoch,
///   which can lead to different representation of historical dates compared to Hive.
///   (https://en.wikipedia.org/wiki/Proleptic_Gregorian_calendar).
/// - Supports construction from year-month-day triplets. In this case, CCTZ is used to
///   check the validity of date.
/// - A DateValue instance will be invalid in the following cases:
///     - If created using the default constructor.
///     - If constructed from out-of-range or invalid year-month-day values.
///     - If parsed from a date string that is not valid or represents an out-of-range
///       date.
/// - Note, that boost::gregorian::date could not be used for representation/validation
///   due to its limited range.
class DateValue {
 public:
  static const DateValue MIN_DATE;
  static const DateValue MAX_DATE;

  /// Default constructor creates an invalid DateValue instance.
  DateValue() : days_since_epoch_(INVALID_DAYS_SINCE_EPOCH) {
    DCHECK(!IsValid());
  }

  explicit DateValue(int64_t days_since_epoch)
      : days_since_epoch_(INVALID_DAYS_SINCE_EPOCH) {
    DCHECK(!IsValid());
    if (LIKELY(days_since_epoch >= MIN_DAYS_SINCE_EPOCH
        && days_since_epoch <= MAX_DAYS_SINCE_EPOCH)) {
      days_since_epoch_ = static_cast<int32_t>(days_since_epoch);
      DCHECK(IsValid());
    }
  }

  DateValue(int64_t year, int64_t month, int64_t day);

  /// Creates a DateValue instance from the ISO 8601 week-based date values:
  /// 'year' is expected to be in the [1, 9999] range.
  /// 'week_of_year' is expected to be in the [1, 53] range.
  /// 'day_of_week' is expected to be in the [1, 7] range.
  /// If any of the parameters is out of range or 'year' has less than 'week_of_year'
  /// ISO 8601 weeks, returns an invalid DateValue instance.
  static DateValue CreateFromIso8601WeekBasedDateVals(int week_numbering_year,
      int week_of_year, int day_of_week);

  bool IsValid() const {
    return days_since_epoch_ != INVALID_DAYS_SINCE_EPOCH;
  }

  /// If this DateValue instance is valid, return the string representation formatted as
  /// 'yyyy-MM-dd'.
  /// Otherwise, return empty string.
  std::string ToString() const;

  /// If this DateValue instance is valid, convert it to year-month-day and return true.
  /// Result is placed in 'year', 'month', 'day'.
  /// Otherwise, return false.
  bool ToYearMonthDay(int* year, int* month, int* day) const WARN_UNUSED_RESULT;

  /// If this DateValue instance is valid, convert it to year and return true. Result is
  /// placed in 'year'.
  /// Otherwise, return false.
  bool ToYear(int* year) const WARN_UNUSED_RESULT;

  /// If DateValue instance is valid, returns day-of-week in [0, 6] range; 0 = Monday and
  /// 6 = Sunday.
  /// Otherwise, return -1.
  int WeekDay() const;

  /// If DateValue instance is valid, returns day-of-year in [1, 366] range.
  /// Otherwise, return -1.
  int DayOfYear() const;

  /// Returns the ISO 8601 week corresponding to this date.
  /// If this DateValue instance is invalid, the return value is -1; otherwise the return
  /// value is in the [1, 53] range.
  /// ISO 8601 weeks start with Monday. Each ISO 8601 week's year is the Gregorian year in
  /// which the Thursday falls.
  int Iso8601WeekOfYear() const;

  /// Returns the year that the current ISO 8601 week belongs to.
  /// If this DateValue instance is invalid, the return value is -1; otherwise the return
  /// value is in the [current_year - 1, current_year + 1] range, which always falls into
  /// the [1, 9999] range.
  /// ISO 8601 weeks start with Monday. Each ISO 8601 week's year is the Gregorian year in
  /// which the Thursday falls.
  int Iso8601WeekNumberingYear() const;

  /// If this DateValue instance valid, add 'days' days to it and return the result.
  /// Otherwise, return an invalid DateValue instance.
  DateValue AddDays(int64_t days) const;

  /// If this DateValue instance valid, add 'months' months to it and return the result.
  /// Otherwise, return an invalid DateValue instance.
  /// If 'keep_last_day' is set and this DateValue is the last day of a month, the
  /// returned DateValue will fall on the last day of the target month too.
  DateValue AddMonths(int64_t months, bool keep_last_day) const;

  /// If this DateValue instance valid, add 'years' years to it and return the result.
  /// Otherwise, return an invalid DateValue instance.
  DateValue AddYears(int64_t years) const;

  /// If this DateValue instance valid, subtract 'days' days from it and return the
  /// result. Otherwise, return an invalid DateValue instance.
  DateValue SubtractDays(int64_t days) const;

  /// Find a middle point date between 'min' and 'max'.
  static DateValue FindMiddleDate(const DateValue& min, const DateValue& max);

  /// If this DateValue instance is valid, convert it to the number of days since epoch
  /// and return true. Result is placed in 'days'.
  /// Otherwise, return false.
  bool ToDaysSinceEpoch(int32_t* days) const WARN_UNUSED_RESULT;

  /// If this DateValue instance is valid, return DateValue corresponding to the last day
  /// of the current month.
  /// Otherwise, return an invalid DateValue instance.
  DateValue LastDay() const;

  /// If this DateValue and 'other' are both valid, set 'months_between' to the number of
  /// months between dates and return true. Otherwise return false.
  /// If this is later than 'other', then the result is positive. If this is earlier than
  /// 'other', then the result is negative. If this and 'other' are either the same days
  /// of the month or both last days of months, then the result is always an integer.
  /// Otherwise calculate the fractional portion of the result based on a 31-day month.
  bool MonthsBetween(const DateValue& other, double* months_between) const;

  /// Returns a DateVal representation in the output variable.
  /// Returns null if the DateValue instance doesn't have a valid date.
  impala_udf::DateVal ToDateVal() const {
    if (!IsValid()) return impala_udf::DateVal::null();
    return impala_udf::DateVal(days_since_epoch_);
  }

  /// Returns the underlying storage. There is only one representation for any DateValue
  /// (including the invalid DateValue) and the storage is directly comparable.
  int32_t Value() const { return days_since_epoch_; }

  /// Returns a DateValue converted from a DateVal. The caller must ensure the DateVal
  /// does not represent a NULL.
  static DateValue FromDateVal(const impala_udf::DateVal& udf_value) {
    DCHECK(!udf_value.is_null);
    return DateValue(udf_value.val);
  }

  // Store the days_since_epoch_ of this DateValue in 'pvalue'.
  void ToColumnValuePB(ColumnValuePB* pvalue) const {
    DCHECK(pvalue != nullptr);
    pvalue->set_date_val(days_since_epoch_);
  }

  // Returns a new DateValue created from the value in 'value_pb'.
  static DateValue FromColumnValuePB(const ColumnValuePB& value_pb) {
    return DateValue(value_pb.date_val());
  }

  /// Constructors that parse from a date string. See DateParser for details about the
  /// date format.
  static DateValue ParseSimpleDateFormat(const char* str, int len, bool accept_time_toks);
  static DateValue ParseSimpleDateFormat(const std::string& str, bool accept_time_toks);
  static DateValue ParseSimpleDateFormat(const char* str, int len,
      const datetime_parse_util::DateTimeFormatContext& dt_ctx);
  static DateValue ParseIsoSqlFormat(const char* str, int len,
      const datetime_parse_util::DateTimeFormatContext& dt_ctx);

  /// Format the date using the given 'dt_ctx' format context. If *this is invalid
  /// returns an empty string.
  std::string Format(const datetime_parse_util::DateTimeFormatContext& dt_ctx) const;

  bool operator==(const DateValue& other) const {
    return days_since_epoch_ == other.days_since_epoch_;
  }
  bool operator!=(const DateValue& other) const { return !(*this == other); }

  bool operator<(const DateValue& other) const {
    return days_since_epoch_ < other.days_since_epoch_;
  }
  bool operator>(const DateValue& other) const { return other < *this; }
  bool operator<=(const DateValue& other) const { return !(*this > other); }
  bool operator>=(const DateValue& other) const { return !(*this < other); }

 private:
  /// Number of days since 1970.01.01.
  int32_t days_since_epoch_;

  static const int32_t MIN_DAYS_SINCE_EPOCH;
  static const int32_t MAX_DAYS_SINCE_EPOCH;
  static const int32_t INVALID_DAYS_SINCE_EPOCH = std::numeric_limits<int32_t>::min();
};

std::ostream& operator<<(std::ostream& os, const DateValue& date_value);

}
