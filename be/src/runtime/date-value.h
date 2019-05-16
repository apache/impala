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
#include "udf/udf.h"

namespace impala {

namespace datetime_parse_util {
struct DateTimeFormatContext;
}

/// Represents a DATE value.
/// - The minimum and maximum dates are 0000-01-01 and 9999-12-31. Valid dates must fall
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

  DateValue(int year, int month, int day);

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

  /// If DateValue instance is valid, returns day-of-week in [0, 6]; 0 = Monday and
  /// 6 = Sunday.
  /// Otherwise, return -1.
  int WeekDay() const;

  /// If this DateValue instance valid, add 'days' to it and return the result.
  /// Otherwise, return an invalid DateValue instance.
  DateValue AddDays(int days) const;

  /// If this DateValue instance is valid, convert it to the number of days since epoch
  /// and return true. Result is placed in 'days'.
  /// Otherwise, return false.
  bool ToDaysSinceEpoch(int32_t* days) const WARN_UNUSED_RESULT;

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

  /// Constructors that parse from a date string. See DateParser for details about the
  /// date format.
  static DateValue Parse(const char* str, int len, bool accept_time_toks);
  static DateValue Parse(const std::string& str, bool accept_time_toks);
  static DateValue Parse(const char* str, int len,
      const datetime_parse_util::DateTimeFormatContext& dt_ctx);

  /// Format the date using the given 'dt_ctx' format context. The result is placed in
  /// 'buff' buffer which has the size of 'len'. The size of the buffer should be at least
  /// dt_ctx.fmt_out_len + 1. A string terminator will be appended to the string.
  /// Return the number of characters copied in to the buffer (excluding terminator).
  /// If *this is invalid, nothing is placed in 'buff' and -1 is returned.
  int Format(const datetime_parse_util::DateTimeFormatContext& dt_ctx, int len,
      char* buff) const;

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

  static const DateValue MIN_DATE;
  static const DateValue MAX_DATE;

 private:
  /// Number of days since 1970.01.01.
  int32_t days_since_epoch_;

  static const int MIN_YEAR;
  static const int MAX_YEAR;

  static const int32_t MIN_DAYS_SINCE_EPOCH;
  static const int32_t MAX_DAYS_SINCE_EPOCH;
  static const int32_t INVALID_DAYS_SINCE_EPOCH = std::numeric_limits<int32_t>::min();
};

std::ostream& operator<<(std::ostream& os, const DateValue& date_value);

}
