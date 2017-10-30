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


#ifndef IMPALA_RUNTIME_TIMESTAMP_VALUE_H
#define IMPALA_RUNTIME_TIMESTAMP_VALUE_H

#include <boost/date_time/compiler_config.hpp>
#include <boost/date_time/gregorian/gregorian.hpp>
#include <boost/date_time/local_time/local_time.hpp>
#include <ctime>
#include <gflags/gflags.h>
#include <string>

#include "udf/udf.h"
#include "util/hash-util.h"

/// Users who want a fix for IMPALA-97 (to be Hive compatible) can enable this flag.
/// The flag is disabled by default but should be flipped with the next release that
/// accepts breaking-changes.
DECLARE_bool(use_local_tz_for_unix_timestamp_conversions);

namespace impala {

struct DateTimeFormatContext;

/// Represents either a (1) date and time, (2) a date with an undefined time, or (3)
/// a time with an undefined date. In all cases, times have up to nanosecond resolution
/// and the minimum and maximum dates are 1400-01-01 and 9999-12-31.
//
/// This type is similar to Postgresql TIMESTAMP WITHOUT TIME ZONE datatype and MySQL's
/// DATETIME datatype. Note that because TIMESTAMP does not contain time zone
/// information, the time zone must be inferred by the context when needed. For example,
/// suppose the current date and time is Jan 15 2015 5:37:56 PM PST:
/// SELECT NOW(); -- Returns '2015-01-15 17:37:56' - implicit time zone of NOW() return
///     value is PST
/// SELECT TO_UTC_TIMESTAMP(NOW(), "PST"); -- Returns '2015-01-16 01:54:21' - implicit
///     time zone is UTC, input time zone specified by second parameter.
//
/// Hive describes this data type as "Timestamps are interpreted to be timezoneless and
/// stored as an offset from the UNIX epoch." but storing a value as an offset from
/// Unix epoch, which is defined as being in UTC, is impossible unless the time zone for
/// the value is known. If all files stored values relative to the epoch, then there
/// would be no reason to interpret values as timezoneless.
//
/// TODO: Document what situation leads to #2 at the top. Cases #1 and 3 can be created
///       with literals. A literal "2000-01-01" results in a value with a "00:00:00"
///       time component. It may not be possible to actually create case #2 though
///       the code implies it is possible.
//
/// For collect timings, prefer the functions in util/time.h. If this class is used for
/// timings, the local time should never be used since it is affected by daylight savings.
/// Also keep in mind that the time component rolls over at midnight so the date should
/// always be checked when determining a duration.
class TimestampValue {
 public:
  TimestampValue() {}

  TimestampValue(const boost::gregorian::date& d,
      const boost::posix_time::time_duration& t)
      : time_(t),
        date_(d) {
    Validate();
  }
  TimestampValue(const boost::posix_time::ptime& t)
      : time_(t.time_of_day()),
        date_(t.date()) {
    Validate();
  }
  TimestampValue(const TimestampValue& tv) : time_(tv.time_), date_(tv.date_) {}

  /// Constructors that parse from a date/time string. See TimestampParser for details
  /// about the date-time format.
  static TimestampValue Parse(const std::string& str);
  static TimestampValue Parse(const char* str, int len);
  static TimestampValue Parse(const char* str, int len,
      const DateTimeFormatContext& dt_ctx);

  /// Unix time (seconds since 1970-01-01 UTC by definition) constructors.
  /// Return the corresponding timestamp in the local timezone if
  /// FLAGS_use_local_tz_for_unix_timestamp_conversions is true. Otherwise, return the
  /// corresponding timestamp in UTC.
  static TimestampValue FromUnixTime(time_t unix_time) {
    return TimestampValue(UnixTimeToPtime(unix_time));
  }

  /// Same as FromUnixTime() above, but adds the specified number of nanoseconds to the
  /// resulting TimestampValue. Handles negative nanoseconds too.
  static TimestampValue FromUnixTimeNanos(time_t unix_time, int64_t nanos) {
    boost::posix_time::ptime temp = UnixTimeToPtime(unix_time);
    temp += boost::posix_time::nanoseconds(nanos);
    return TimestampValue(temp);
  }

  /// Return the corresponding timestamp in local time zone for the Unix time specified in
  /// microseconds.
  static TimestampValue FromUnixTimeMicros(int64_t unix_time_micros);

  /// Return the corresponding timestamp in UTC for the Unix time specified in
  /// microseconds.
  static TimestampValue UtcFromUnixTimeMicros(int64_t unix_time_micros);

  /// Returns a TimestampValue where the integer part of the specified 'unix_time'
  /// specifies the number of seconds (see above), and the fractional part is converted
  /// to nanoseconds and added to the resulting TimestampValue.
  static TimestampValue FromSubsecondUnixTime(double unix_time) {
    const time_t unix_time_whole = unix_time;
    boost::posix_time::ptime temp = UnixTimeToPtime(unix_time_whole);
    temp += boost::posix_time::nanoseconds((unix_time - unix_time_whole) / ONE_BILLIONTH);
    return TimestampValue(temp);
  }

  /// Returns a TimestampValue converted from a TimestampVal. The caller must ensure
  /// the TimestampVal does not represent a NULL.
  static TimestampValue FromTimestampVal(const impala_udf::TimestampVal& udf_value) {
    DCHECK(!udf_value.is_null);
    TimestampValue value;
    memcpy(&value.date_, &udf_value.date, sizeof(value.date_));
    memcpy(&value.time_, &udf_value.time_of_day, sizeof(value.time_));
    return value;
  }

  /// Returns a TimestampVal representation in the output variable.
  /// Returns null if the TimestampValue instance doesn't have a valid date or time.
  void ToTimestampVal(impala_udf::TimestampVal* tv) const {
    if (!HasDateOrTime()) {
      tv->is_null = true;
      return;
    }
    memcpy(&tv->date, &date_, sizeof(date_));
    memcpy(&tv->time_of_day, &time_, sizeof(time_));
    tv->is_null = false;
  }

  void ToPtime(boost::posix_time::ptime* ptp) const {
    *ptp = boost::posix_time::ptime(date_, time_);
  }

  bool HasDate() const { return !date_.is_special(); }
  bool HasTime() const { return !time_.is_special(); }
  bool HasDateOrTime() const { return HasDate() || HasTime(); }
  bool HasDateAndTime() const { return HasDate() && HasTime(); }

  std::string ToString() const;

  /// Verifies that the date falls into a valid range (years 1400..9999).
  static inline bool IsValidDate(const boost::gregorian::date& date) {
    // Smallest valid day number.
    const static int64_t MIN_DAY_NUMBER = static_cast<int64_t>(
        boost::gregorian::date(boost::date_time::min_date_time).day_number());
    // Largest valid day number.
    const static int64_t MAX_DAY_NUMBER = static_cast<int64_t>(
        boost::gregorian::date(boost::date_time::max_date_time).day_number());

    return date.day_number() >= MIN_DAY_NUMBER
        && date.day_number() <= MAX_DAY_NUMBER;
  }

  /// Formats the timestamp using the given date/time context and places the result in the
  /// string buffer. The size of the buffer should be at least dt_ctx.fmt_out_len + 1. A
  /// string terminator will be appended to the string.
  /// dt_ctx -- the date/time context containing the format to use
  /// len -- the length of the buffer
  /// buff -- the buffer that will hold the result
  /// Returns the number of characters copied in to the buffer (minus the terminator)
  int Format(const DateTimeFormatContext& dt_ctx, int len, char* buff) const;

  /// Interpret 'this' as a timestamp in UTC and convert to unix time.
  /// Returns false if the conversion failed ('unix_time' will be undefined), otherwise
  /// true.
  bool UtcToUnixTime(time_t* unix_time) const;

  /// Interpret 'this' as a timestamp in UTC and convert to unix time in microseconds.
  /// Nanoseconds are rounded to the nearest microsecond supported by Impala. Returns
  /// false if the conversion failed ('unix_time_micros' will be undefined), otherwise
  /// true.
  bool UtcToUnixTimeMicros(int64_t* unix_time_micros) const;

  /// Converts to Unix time (seconds since the Unix epoch) representation. The time
  /// zone interpretation of the TimestampValue instance is determined by
  /// FLAGS_use_local_tz_for_unix_timestamp_conversions. If the flag is true, the
  /// instance is interpreted as a local value. If the flag is false, UTC is assumed.
  /// Returns false if the conversion failed (unix_time will be undefined), otherwise
  /// true.
  bool ToUnixTime(time_t* unix_time) const;

  /// Converts to Unix time with fractional seconds.
  /// Returns false if the conversion failed (unix_time will be undefined), otherwise
  /// true.
  bool ToSubsecondUnixTime(double* unix_time) const;

  /// Converts from UTC to local time in-place. The caller must ensure the TimestampValue
  /// this function is called upon has both a valid date and time.
  void UtcToLocal();

  void set_date(const boost::gregorian::date d) { date_ = d; }
  void set_time(const boost::posix_time::time_duration t) { time_ = t; }
  const boost::gregorian::date& date() const { return date_; }
  const boost::posix_time::time_duration& time() const { return time_; }

  TimestampValue& operator=(const boost::posix_time::ptime& ptime) {
    date_ = ptime.date();
    time_ = ptime.time_of_day();
    return *this;
  }

  bool operator==(const TimestampValue& other) const {
    return date_ == other.date_ && time_ == other.time_;
  }

  bool operator!=(const TimestampValue& other) const { return !(*this == other); }

  bool operator<(const TimestampValue& other) const {
    return date_ < other.date_ || (date_ == other.date_ && time_ < other.time_);
  }

  bool operator<=(const TimestampValue& other) const {
    return *this < other || *this == other; }

  bool operator>(const TimestampValue& other) const {
    return date_ > other.date_ || (date_ == other.date_ && time_ > other.time_);
  }

  bool operator>=(const TimestampValue& other) const {
    return *this > other || *this == other;
  }

  static size_t Size() {
    return sizeof(boost::posix_time::time_duration) + sizeof(boost::gregorian::date);
  }

  inline uint32_t Hash(int seed = 0) const {
    uint32_t hash = HashUtil::Hash(&time_, sizeof(time_), seed);
    return HashUtil::Hash(&date_, sizeof(date_), hash);
  }

  static const char* LLVM_CLASS_NAME;

 private:
  friend class UnusedClass;

  /// Used when converting a time with fractional seconds which are stored as in integer
  /// to a Unix time stored as a double.
  static const double ONE_BILLIONTH;

  /// Boost ptime leaves a gap in the structure, so we swap the order to make it
  /// 12 contiguous bytes.  We then must convert to and from the boost ptime data type.
  /// See IMP-87 for more information on why using ptime with the 4 byte gap is
  /// problematic.

  /// 8 bytes - stores the nanoseconds within the current day
  boost::posix_time::time_duration time_;

  /// 4 -bytes - stores the date as a day
  boost::gregorian::date date_;

  /// Sets both date and time to invalid if date is outside the valid range.
  void Validate();

  /// Return a ptime representation of the given Unix time (seconds since the Unix epoch).
  /// The time zone of the resulting ptime is determined by
  /// FLAGS_use_local_tz_for_unix_timestamp_conversions. If the flag is true, the value
  /// will be in the local time zone. If the flag is false, the value will be in UTC.
  static boost::posix_time::ptime UnixTimeToPtime(time_t unix_time);

  /// Same as the above, but the time zone of the resulting ptime is always in the local
  /// time zone.
  static boost::posix_time::ptime UnixTimeToLocalPtime(time_t unix_time);

  /// Same as the above, but the time zone of the resulting ptime is always in UTC.
  static boost::posix_time::ptime UnixTimeToUtcPtime(time_t unix_time);
};

/// This function must be called 'hash_value' to be picked up by boost.
inline std::size_t hash_value(const TimestampValue& v) {
  return v.Hash();
}

std::ostream& operator<<(std::ostream& os, const TimestampValue& timestamp_value);
}

#endif
