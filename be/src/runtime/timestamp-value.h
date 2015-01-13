// Copyright 2012 Cloudera Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.


#ifndef IMPALA_RUNTIME_TIMESTAMP_VALUE_H
#define IMPALA_RUNTIME_TIMESTAMP_VALUE_H

#include <string>
#include <ctime>
#include <boost/cstdint.hpp>
#include <boost/date_time/posix_time/posix_time.hpp>
#include "runtime/timestamp-parse-util.h"
#include "udf/udf.h"
#include "util/hash-util.h"

namespace impala {

time_t to_time_t(boost::posix_time::ptime t);

// Represents either a (1) date and time, (2) a date with an undefined time, or (3)
// a time with an undefined date. In all cases, times have up to nanosecond resolution
// and the minimum and maximum dates are 1400-01-01 and 10000-12-31.
//
// This type is similar to Postgresql TIMESTAMP WITHOUT TIME ZONE datatype and MySQL's
// DATETIME datatype. Note that because TIMESTAMP does not contain time zone
// information, the time zone must be inferred by the context when needed. For example,
// suppose the current date and time is Jan 15 2015 5:37:56 PM PST:
// SELECT NOW(); -- Returns '2015-01-15 17:37:56' - implicit time zone of NOW() return
//     value is PST
// SELECT TO_UTC_TIMESTAMP(NOW(), "PST"); -- Returns '2015-01-16 01:54:21' - implicit
//     time zone is UTC, input time zone specified by second parameter.
//
// Hive describes this data type as "Timestamps are interpreted to be timezoneless and
// stored as an offset from the UNIX epoch." but storing a value as an offset from
// Unix epoch, which is defined as being in UTC, is impossible unless the time zone for
// the value is known. If all files stored values relative to the epoch, then there
// would be no reason to interpret values as timezoneless.
//
// TODO: Document what situation leads to #2 at the top. Cases #1 and 3 can be created
//       with literals. A literal "2000-01-01" results in a value with a "00:00:00"
//       time component. It may not be possible to actually create case #2 though
//       the code implies it is possible.
//
// For collect timings, prefer the functions in util/time.h. If this class is used for
// timings, the local time should never be used since it is affected by daylight savings.
// Also keep in mind that the time component rolls over at midnight so the date should
// always be checked when determining a duration.
class TimestampValue {
 public:
  TimestampValue() {}

  TimestampValue(const boost::gregorian::date& d,
      const boost::posix_time::time_duration& t)
      : time_(t),
        date_(d) {}
  TimestampValue(const boost::posix_time::ptime& t)
      : time_(t.time_of_day()),
        date_(t.date()) {}
  TimestampValue(const TimestampValue& tv) : time_(tv.time_), date_(tv.date_) {}
  TimestampValue(const char* str, int len);
  TimestampValue(const char* str, int len, const DateTimeFormatContext& dt_ctx);

  // Unix time (seconds since 1970-01-01 UTC by definition) constructors.
  // TODO: Construct a local time to be consistent with FROM_UNIXTIME() and related
  //       functions.
  template <typename Number>
  explicit TimestampValue(Number unix_time) {
    *this = boost::posix_time::from_time_t(unix_time);
  }

  TimestampValue(int64_t unix_time, int64_t nanos) {
    boost::posix_time::ptime temp = boost::posix_time::from_time_t(unix_time);
    temp += boost::posix_time::nanoseconds(nanos);
    *this = temp;
  }

  explicit TimestampValue(double unix_time) {
    int64_t i = unix_time;
    boost::posix_time::ptime temp = boost::posix_time::from_time_t(i);
    temp += boost::posix_time::nanoseconds((unix_time - i) / ONE_BILLIONTH);
    *this = temp;
  }

  // Returns the current local time with microsecond accuracy. This should not be used
  // to time something because it is affected by adjustments to the system clock such
  // as a daylight savings or a manual correction by a system admin. For timings, use
  // functions in util/time.h.
  static TimestampValue LocalTime() {
    return TimestampValue(boost::posix_time::microsec_clock::local_time());
  }

  static TimestampValue FromTimestampVal(const impala_udf::TimestampVal& udf_value) {
    TimestampValue value;
    memcpy(&value.date_, &udf_value.date, sizeof(value.date_));
    memcpy(&value.time_, &udf_value.time_of_day, sizeof(value.time_));
    return value;
  }

  void ToTimestampVal(impala_udf::TimestampVal* tv) const {
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

  std::string DebugString() const {
    std::stringstream ss;
    if (HasDate()) {
      ss << boost::gregorian::to_iso_extended_string(date_);
    }
    if (HasTime()) {
      if (HasDate()) ss << " ";
      ss << boost::posix_time::to_simple_string(time_);
    }
    return ss.str();
  }

  // Formats the timestamp using the given date/time context and places the result in the
  // string buffer. The size of the buffer should be at least dt_ctx.fmt_out_len + 1. A
  // string terminator will be appended to the string.
  // dt_ctx -- the date/time context containing the format to use
  // len -- the length of the buffer
  // buff -- the buffer that will hold the result
  // Returns the number of characters copied in to the buffer (minus the terminator)
  int Format(const DateTimeFormatContext& dt_ctx, int len, char* buff);

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

  // TODO: If the Unix time constructors convert from local time, these should
  // convert too.
  operator int8_t() const {
    boost::posix_time::ptime temp;
    this->ToPtime(&temp);
    return static_cast<char>(to_time_t(temp));
  }

  operator int16_t() const {
    boost::posix_time::ptime temp;
    this->ToPtime(&temp);
    return static_cast<int16_t>(to_time_t(temp));
  }

  operator int32_t() const {
    boost::posix_time::ptime temp;
    this->ToPtime(&temp);
    return static_cast<int32_t>(to_time_t(temp));
  }

  operator int64_t() const {
    boost::posix_time::ptime temp;
    this->ToPtime(&temp);
    return static_cast<int64_t>(to_time_t(temp));
  }

  operator float() const {
    boost::posix_time::ptime temp;
    this->ToPtime(&temp);
    // TODO: What should happen when either the date or time component is missing? This
    // looks broken when time_ is "special".
    return static_cast<float>(to_time_t(temp)) +
        static_cast<float>(time_.fractional_seconds() * ONE_BILLIONTH);
  }

  operator double() const {
    boost::posix_time::ptime temp;
    this->ToPtime(&temp);
    // TODO: What should happen when either the date or time component is missing? This
    // looks broken when time_ is "special".
    return static_cast<double>(to_time_t(temp)) +
        static_cast<double>(time_.fractional_seconds() * ONE_BILLIONTH);
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

  // Used when converting a time with fractional seconds which are stored as in integer
  // to a Unix time stored as a double.
  static const double ONE_BILLIONTH;

  // Boost ptime leaves a gap in the structure, so we swap the order to make it
  // 12 contiguous bytes.  We then must convert to and from the boost ptime data type.
  // See IMP-87 for more information on why using ptime with the 4 byte gap is
  // problematic.

  // 8 bytes - stores the nanoseconds within the current day
  boost::posix_time::time_duration time_;

  // 4 -bytes - stores the date as a day
  boost::gregorian::date date_;
};

// This function must be called 'hash_value' to be picked up by boost.
inline std::size_t hash_value(const TimestampValue& v) {
  return v.Hash();
}

std::ostream& operator<<(std::ostream& os, const TimestampValue& timestamp_value);
}

#endif
