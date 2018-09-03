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

namespace impala {

namespace datetime_parse_util {
struct DateTimeFormatContext;
}

/// Represents a DATE value.
/// - The minimum and maximum dates are 0000-01-01 and 9999-12-31. Valid dates must fall
///   in this range.
/// - Internally represents DATE values as number of days since 1970.01.01.
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

  DateValue(int32_t days_since_epoch) : days_since_epoch_(INVALID_DAYS_SINCE_EPOCH) {
    DCHECK(!IsValid());
    if (LIKELY(days_since_epoch >= MIN_DAYS_SINCE_EPOCH
        && days_since_epoch <= MAX_DAYS_SINCE_EPOCH)) {
      days_since_epoch_ = days_since_epoch;
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

  /// If this DateValue instance is valid, convert it to the number of days since epoch
  /// and return true. Result is placed in 'days'.
  /// Otherwise, return false.
  bool ToDaysSinceEpoch(int32_t* days) const WARN_UNUSED_RESULT;

  /// Constructors that parse from a date string. See DateParser for details about the
  /// date format.
  static DateValue Parse(const char* str, int len);
  static DateValue Parse(const std::string& str);
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
