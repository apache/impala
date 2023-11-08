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

#include <cstdint>
#include <cstring>
#include <iosfwd>
#include <string>

#include <boost/date_time/gregorian/greg_date.hpp>
#include <boost/date_time/posix_time/posix_time_config.hpp>
#include <boost/date_time/posix_time/posix_time_duration.hpp>
#include <boost/date_time/posix_time/ptime.hpp>
#include <boost/date_time/special_defs.hpp>
#include <boost/date_time/time_duration.hpp>

#include "common/compiler-util.h"
#include "common/global-types.h"
#include "common/logging.h"
#include "gen-cpp/common.pb.h"
#include "udf/udf.h"
#include "util/hash-util.h"

namespace impala {

namespace datetime_parse_util {
struct DateTimeFormatContext;
}

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
  static TimestampValue ParseSimpleDateFormat(const std::string& str);
  static TimestampValue ParseSimpleDateFormat(const char* str, int len);
  static TimestampValue ParseSimpleDateFormat(const char* str, int len,
      const datetime_parse_util::DateTimeFormatContext& dt_ctx);
  static TimestampValue ParseIsoSqlFormat(const char* str, int len,
      const datetime_parse_util::DateTimeFormatContext& dt_ctx);

  /// 'days' represents the number of days since 1970-01-01.
  /// The returned timestamp's date component is set so that it would correspond to
  /// 'days', the time-of-day component is set to 00:00:00.
  static TimestampValue FromDaysSinceUnixEpoch(int64_t days);

  // Returns the number of days since 1970-01-01. Expects date_ to be valid.
  int64_t DaysSinceUnixEpoch() const;

  /// Unix time (seconds since 1970-01-01 UTC by definition) constructors.
  /// If 'local_tz' is set and non-UTC, 'unix_time' is assumed to be UTC and
  /// UTC->local_tz conversion takes place.
  static TimestampValue FromUnixTime(time_t unix_time, const Timezone* local_tz);

  /// Same as FromUnixTime() above, but adds the specified number of nanoseconds to the
  /// resulting TimestampValue. Handles negative nanoseconds and the case where
  /// abs(nanos) >= 1e9.
  static TimestampValue FromUnixTimeNanos(time_t unix_time, int64_t nanos,
      const Timezone* local_tz);

  /// Same as FromUnixTime(), but expects the time in microseconds.
  static TimestampValue FromUnixTimeMicros(int64_t unix_time_micros,
      const Timezone* local_tz);

  /// Return the corresponding timestamp in UTC for the Unix time specified in
  /// nanoseconds. The range is smaller than in other conversion function, as the
  /// full [1400 .. 10000) range cannot be represented with an int64.
  /// Supported range: [1677-09-21 00:12:43.145224192 .. 2262-04-11 23:47:16.854775807]
  static TimestampValue UtcFromUnixTimeLimitedRangeNanos(int64_t unix_time_nanos);

  /// Return the corresponding timestamp in UTC for the Unix time specified in
  /// microseconds.
  static TimestampValue UtcFromUnixTimeMicros(int64_t unix_time_micros);

  /// Return the corresponding timestamp in UTC for the Unix time specified in
  /// milliseconds.
  static TimestampValue UtcFromUnixTimeMillis(int64_t unix_time_millis);

  /// Returns a TimestampValue  in 'local_tz' time zone where the integer part of the
  /// specified 'unix_time' specifies the number of seconds (see above), and the
  /// fractional part is converted to nanoseconds and added to the resulting
  /// TimestampValue.
  static TimestampValue FromSubsecondUnixTime(double unix_time, const Timezone* local_tz);

  /// Returns a TimestampValue converted from a TimestampVal. The caller must ensure
  /// the TimestampVal does not represent a NULL.
  static TimestampValue FromTimestampVal(const impala_udf::TimestampVal& udf_value) {
    DCHECK(!udf_value.is_null);
    TimestampValue value;
    memcpy(&value.date_, &udf_value.date, sizeof(value.date_));
    memcpy(&value.time_, &udf_value.time_of_day, sizeof(value.time_));
    value.Validate();
    return value;
  }

  /// Returns a TimestampVal representation in the output variable.
  /// Returns null if the TimestampValue instance doesn't have a valid date or time.
  void ToTimestampVal(impala_udf::TimestampVal* tv) const {
    if (!HasDate()) {
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

  // Store the binary representation of this TimestampValue in 'pvalue'.
  void ToColumnValuePB(ColumnValuePB* pvalue) const {
    const uint8_t* data = reinterpret_cast<const uint8_t*>(this);
    pvalue->mutable_timestamp_val()->assign(data, data + Size());
  }

  // Returns a new TimestampValue created from the value in 'value_pb'.
  static TimestampValue FromColumnValuePB(const ColumnValuePB& value_pb) {
    TimestampValue value;
    memcpy(&value, value_pb.timestamp_val().c_str(), Size());
    value.Validate();
    return value;
  }

  bool HasDate() const { return !date_.is_special(); }
  bool HasTime() const { return !time_.is_special(); }
  bool HasDateAndTime() const { return HasDate() && HasTime(); }

  /// Write the string representation of this TimestampValue.
  /// Caller should try to use the variant with output argument and reuse the 'dst' string
  /// as much as possible if calling ToString multiple times (see IMPALA-10984).
  void ToString(string& dst) const;
  std::string ToString() const;

  /// Return the StringVal representation of TimestampValue.
  /// Return StringVal::null() if timestamp is invalid.
  impala_udf::StringVal ToStringVal(impala_udf::FunctionContext* ctx) const;
  impala_udf::StringVal ToStringVal(impala_udf::FunctionContext* ctx,
      const datetime_parse_util::DateTimeFormatContext& dt_ctx) const;

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

  static inline TimestampValue GetMinValue() {
    return TimestampValue(
        boost::gregorian::date(boost::date_time::min_date_time),
        boost::posix_time::time_duration(0, 0, 0, 0));
  }

  static inline TimestampValue GetMaxValue() {
    return TimestampValue(
        boost::gregorian::date(boost::date_time::max_date_time),
        boost::posix_time::nanoseconds(NANOS_PER_DAY - 1));
  }

  /// Verifies that the time is not negative and is less than a whole day.
  static inline bool IsValidTime(const boost::posix_time::time_duration& time) {
    return !time.is_negative()
        && time.total_nanoseconds() < NANOS_PER_DAY;
  }

  /// Formats the timestamp using the given date/time context and write the result to
  /// destination string. The destination string will be cleared if timestamp is invalid.
  /// dt_ctx -- the date/time context containing the format to use.
  /// dst -- destination string where the result should be written into.
  void Format(
      const datetime_parse_util::DateTimeFormatContext& dt_ctx, string& dst) const;

  /// Interpret 'this' as a timestamp in UTC and convert to unix time.
  /// Returns false if the conversion failed ('unix_time' will be undefined), otherwise
  /// true.
  bool UtcToUnixTime(time_t* unix_time) const;

  /// Interpret 'this' as a timestamp in UTC and convert to unix time in microseconds.
  /// Nanoseconds are rounded to the nearest microsecond supported by Impala.
  /// Returns false if the conversion failed ('unix_time_micros' will be undefined),
  /// otherwise true.
  /// TODO: Rounding towards nearest microsecond should be replaced with rounding
  ///       towards minus infinity. For more details, see IMPALA-8180
  bool UtcToUnixTimeMicros(int64_t* unix_time_micros) const;

  /// Interpret 'this' as a timestamp in UTC and convert to unix time in microseconds.
  /// Nanoseconds are rounded towards minus infinity.
  /// Returns false if the conversion failed ('unix_time_micros' will be undefined),
  /// otherwise true.
  bool FloorUtcToUnixTimeMicros(int64_t* unix_time_micros) const;

  /// Interpret 'this' as a timestamp in UTC and convert to unix time in milliseconds.
  /// Nanoseconds are rounded towards minus infinity.
  /// Returns false if the conversion failed ('unix_time_millis' will be undefined),
  /// otherwise true.
  bool FloorUtcToUnixTimeMillis(int64_t* unix_time_millis) const;

  /// Interpret 'this' as a timestamp in UTC and convert to unix time in nanoseconds.
  /// The full [1400 .. 10000) range cannot be represented with an int64, so the
  /// conversion will fail outside the supported range:
  ///  [1677-09-21 00:12:43.145224192 .. 2262-04-11 23:47:16.854775807]
  /// Returns false if the conversion failed ('unix_time_millis' will be undefined),
  /// otherwise true.
  bool UtcToUnixTimeLimitedRangeNanos(int64_t* unix_time_nanos) const;

  /// Converts to Unix time (seconds since the Unix epoch) representation.
  /// If 'local_tz' is set and non-UTC, then this TimestampValue is assumed to be in
  /// local_tz and local_tz->UTC conversion takes place to return it as UTC Unix time.
  /// Returns false if the conversion failed (unix_time will be undefined),
  /// otherwise true.
  bool ToUnixTime(const Timezone* local_tz, time_t* unix_time) const;

  /// Converts to Unix time with fractional seconds. The time zone interpretation of the
  /// TimestampValue instance is determined as above.
  /// Returns false if the conversion failed (unix_time will be undefined), otherwise
  /// true.
  bool ToSubsecondUnixTime(const Timezone* local_tz, double* unix_time) const;

  /// Converts from UTC to 'local_tz' time zone in-place. The caller must ensure the
  /// TimestampValue this function is called upon has both a valid date and time.
  ///
  /// If start/end_of_repeated_period is not nullptr and timestamp falls into an interval
  /// where LocalToUtc() is ambiguous (e.g. Summer->Winter DST change on Northern
  /// hemisphere), then these arguments are set to the start/end of this period in local
  /// time. This is useful to get some ordering guarantees in the case when the order of
  /// two timestamps is different in UTC and local time (e.g CET Autumn dst change
  /// 00:30:00 -> 02:30:00 vs 01:15:00 -> 02:15:00) - any timestamp that is earlier than
  /// 'this' in UTC is guaranteed to be earlier than 'end_of_repeated_period' in local
  /// time, and any timestamp later than 'this' in UTC is guaranteed to be later than
  /// 'start_of_repeated_period' in local time.
  void UtcToLocal(const Timezone& local_tz,
      TimestampValue* start_of_repeated_period = nullptr,
      TimestampValue* end_of_repeated_period = nullptr);

  /// Converts from 'local_tz' to UTC time zone in-place. The caller must ensure the
  /// TimestampValue this function is called upon has both a valid date and time.
  ///
  /// If pre/post_utc_if_repeated is not nullptr and timestamp falls into an interval
  /// where conversion is ambiguous (e.g. Summer->Winter DST change on Northern
  /// hemisphere), then these arguments are set to the previous/posterior of possible UTC
  /// timestamp. Or if the timestamp falls into a skipped interval (e.g. Winter->Summer
  /// DST change on Northern hemisphere), then these arguments will be both set to the UTC
  /// timestamp of the transition point, and if the above conditions occur 'this' will be
  /// set to invalid timestamp.
  ///
  /// The pre/post_utc_if_repeated is useful to get some ordering guarantees in the case
  /// when the order of two timestamps is different in UTC and local time (e.g CET Autumn
  /// dst change 00:30:00 -> 02:30:00 vs 01:15:00 -> 02:15:00) - any timestamp that is
  /// earlier than 'this' in local time is guaranteed to be earlier than
  /// 'post_utc_if_repeated' in UTC, and any timestamp later than 'this' in local time is
  /// guaranteed to be later than 'pre_utc_if_repeated' in UTC.
  void LocalToUtc(const Timezone& local_tz,
      TimestampValue* pre_utc_if_repeated = nullptr,
      TimestampValue* post_utc_if_repeated = nullptr);

  void set_date(const boost::gregorian::date d) { date_ = d; Validate(); }
  void set_time(const boost::posix_time::time_duration t) { time_ = t; Validate(); }
  const boost::gregorian::date& date() const { return date_; }
  const boost::posix_time::time_duration& time() const { return time_; }

  /// Return this added with a time duration when t is valid and HasDateAndTime() is
  /// true on this. Return an empty TimestampValue object othewise.
  TimestampValue Add(const boost::posix_time::time_duration& t) const;

  /// Return this subtracted from a time duration when t is valid and HasDateAndTime()
  /// is true on this. Return an empty TimestampValue object othewise.
  TimestampValue Subtract(const boost::posix_time::time_duration& t) const;

  TimestampValue& operator=(const boost::posix_time::ptime& ptime) {
    date_ = ptime.date();
    time_ = ptime.time_of_day();
    Validate();
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

  /// Divides 'ticks' with 'GRANULARITY' (truncated towards negative infinity) and
  /// sets 'ticks' to the remainder.
  template <int64_t GRANULARITY>
  inline static int64_t SplitTime(int64_t* ticks) {
    int64_t result = *ticks / GRANULARITY;
    *ticks %= GRANULARITY;
    if (*ticks < 0) {
      result--;
      *ticks += GRANULARITY;
    }
    return result;
  }

  static const char* LLVM_CLASS_NAME;

 private:
  friend class UnusedClass;

  /// Used when converting a time with fractional seconds which are stored as in integer
  /// to a Unix time stored as a double.
  static const double ONE_BILLIONTH;

  static const uint64_t SECONDS_PER_DAY = 24 * 60 * 60;

  static const int64_t NANOS_PER_DAY = 1'000'000'000LL * SECONDS_PER_DAY;

  /// Boost ptime leaves a gap in the structure, so we swap the order to make it
  /// 12 contiguous bytes.  We then must convert to and from the boost ptime data type.
  /// See IMP-87 for more information on why using ptime with the 4 byte gap is
  /// problematic.

  /// 8 bytes - stores the nanoseconds within the current day
  boost::posix_time::time_duration time_;

  /// 4 -bytes - stores the date as a day
  boost::gregorian::date date_;

  /// Sets both date and time to invalid value.
  inline void SetToInvalidDateTime() {
    time_ = boost::posix_time::time_duration(boost::posix_time::not_a_date_time);
    date_ = boost::gregorian::date(boost::gregorian::not_a_date_time);
  }

  /// Sets both date and time to invalid if date is outside the valid range.
  /// Time's validity is only checked in debug builds.
  /// TODO: This could be also checked in release, but I am a bit afraid that it would
  ///       affect performance and probably break some scenarios that are
  ///       currently working more or less correctly.
  inline void Validate() {
    if (HasDate() && UNLIKELY(!IsValidDate(date_))) SetToInvalidDateTime();
    else if (HasTime()) DCHECK(IsValidTime(time_));
  }

  /// Converts 'unix_time' (in UTC seconds) to TimestampValue in timezone 'local_tz'.
  static TimestampValue UnixTimeToLocal(time_t unix_time, const Timezone& local_tz);

  /// Converts 'unix_time_ticks'/TICKS_PER_SEC seconds to TimestampValue.
  template <int32_t TICKS_PER_SEC>
  static TimestampValue UtcFromUnixTimeTicks(int64_t unix_time_ticks);
};

/// This function must be called 'hash_value' to be picked up by boost.
inline std::size_t hash_value(const TimestampValue& v) {
  return v.Hash();
}

std::ostream& operator<<(std::ostream& os, const TimestampValue& timestamp_value);
}
