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


// The format of a timestamp-typed slot.
class TimestampValue {
 public:
  TimestampValue(const boost::gregorian::date& d,
                 const boost::posix_time::time_duration& t)
      : time_of_day_(t),
        date_(d) {
  }
  TimestampValue() { }

  TimestampValue(const boost::posix_time::ptime& t)
      : time_of_day_(t.time_of_day()),
        date_(t.date()) {
  }

  TimestampValue(const TimestampValue& tv)
      : time_of_day_(tv.time_of_day_),
        date_(tv.date_) {
  }

  TimestampValue& operator=(const boost::posix_time::ptime& t) {
    *this =  TimestampValue(t);
    return *this;
  }

  void ToPtime(boost::posix_time::ptime* ptp) const {
    boost::posix_time::ptime temp(this->date_, this->time_of_day_);
    *ptp = temp;
  }

  TimestampValue(int64_t t, int64_t n) {
    boost::posix_time::ptime temp = boost::posix_time::from_time_t(t);
    temp += boost::posix_time::nanoseconds(n);
    *this = temp;
  }
  TimestampValue(double t) {
    int64_t i = t;
    boost::posix_time::ptime temp = boost::posix_time::from_time_t(i);
    temp += boost::posix_time::nanoseconds((t-i)/FRACTIONAL);
    *this = temp;
  }
  TimestampValue(const char* str, int len);
  TimestampValue(const char* str, int len, const DateTimeFormatContext& dt_ctx);
  TimestampValue(int64_t t) {
    *this = TimestampValue(boost::posix_time::from_time_t(t));
  }
  TimestampValue(int32_t t) {
    *this = TimestampValue(boost::posix_time::from_time_t(t));
  }
  TimestampValue(int16_t t) {
    *this = TimestampValue(boost::posix_time::from_time_t(t));
  }
  TimestampValue(int8_t t) {
    *this = TimestampValue(boost::posix_time::from_time_t(t));
  }
  TimestampValue(bool t) {
    *this = TimestampValue(boost::posix_time::from_time_t(t));
  }

  bool valid() const { return valid_date() || valid_time(); }
  bool valid_date() const { return ! date_.is_special(); }
  bool valid_time() const { return ! time_of_day_.is_special(); }

  void set_date(boost::gregorian::date d) { date_ = d; }
  void set_time(boost::posix_time::time_duration t) { time_of_day_ = t; }
  const boost::gregorian::date& get_date() const { return date_; }
  const boost::posix_time::time_duration& get_time() const { return time_of_day_; }

  std::string DebugString() const {
    std::stringstream ss;
    if (!this->date_.is_special()) {
      ss << boost::gregorian::to_iso_extended_string(this->date_);
    }
    if (!this->time_of_day_.is_special()) {
      if (!this->date_.is_special()) ss << " ";
      ss << boost::posix_time::to_simple_string(this->time_of_day_);
    }
    return ss.str();
  }

  bool operator==(const TimestampValue& other) const {
    return this->date_  == other.date_ && this->time_of_day_ == other.time_of_day_;
  }
  bool operator!=(const TimestampValue& other) const {
    return !(*this == other);
  }
  bool operator<=(const TimestampValue& other) const {
    return this->date_ < other.date_ || (this->date_ == other.date_ &&
        (this->time_of_day_ <= other.time_of_day_));
  }
  bool operator>=(const TimestampValue& other) const {
    return this->date_ > other.date_ ||
        (this->date_ == other.date_ && this->time_of_day_ >= other.time_of_day_);
  }
  bool operator<(const TimestampValue& other) const {
    return this->date_ < other.date_ ||
        (this->date_ == other.date_ && this->time_of_day_ < other.time_of_day_);
  }
  bool operator>(const TimestampValue& other) const {
    return this->date_ > other.date_ ||
        (this->date_ == other.date_ && this->time_of_day_ > other.time_of_day_);
  }

  // If the date or time of day are valid then this is valid.
  bool NotADateTime() const {
    return this->date_.is_special() && this->time_of_day_.is_special();
  }

  operator bool() const {
    boost::posix_time::ptime temp;
    this->ToPtime(&temp);
    return static_cast<bool>(to_time_t(temp));
  }
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
    return static_cast<float>(to_time_t(temp)) +
        static_cast<float>(time_of_day_.fractional_seconds() * FRACTIONAL);
  }
  operator double() const {
    boost::posix_time::ptime temp;
    this->ToPtime(&temp);
    return static_cast<double>(to_time_t(temp)) +
        static_cast<double>(time_of_day_.fractional_seconds() * FRACTIONAL);
  }
  static size_t Size() {
    return sizeof(boost::posix_time::time_duration) + sizeof(boost::gregorian::date);
  }

  boost::posix_time::time_duration time_of_day() { return time_of_day_; }
  boost::gregorian::date date() { return date_;}

  // Returns the local time
  static TimestampValue local_time() {
    return TimestampValue(boost::posix_time::second_clock::local_time());
  }

  // Returns the local time with microsecond accuracy
  static TimestampValue local_time_micros() {
    return TimestampValue(boost::posix_time::microsec_clock::local_time());
  }

  inline uint32_t Hash(int seed = 0) const {
    uint32_t hash = HashUtil::Hash(
        &time_of_day_, sizeof(boost::posix_time::time_duration), seed);
    return HashUtil::Hash(&time_of_day_, sizeof(boost::gregorian::date), hash);
  }

  void ToTimestampVal(impala_udf::TimestampVal* tv) const {
    memcpy(&tv->date, &date_, sizeof(date_));
    memcpy(&tv->time_of_day, &time_of_day_, sizeof(time_of_day_));
    tv->is_null = false;
  }

  static TimestampValue FromTimestampVal(const impala_udf::TimestampVal& tv) {
    TimestampValue v;
    memcpy(&v.date_, &tv.date, sizeof(v.date_));
    memcpy(&v.time_of_day_, &tv.time_of_day, sizeof(v.time_of_day_));
    return v;
  }

  // Formats the timestamp using the given date/time context and places the result in the
  // string buffer. The size of the buffer should be at least dt_ctx.fmt_out_len + 1. A
  // string terminator will be appended to the string.
  // dt_ctx -- the date/time context containing the format to use
  // len -- the length of the buffer
  // buff -- the buffer that will hold the result
  // Returns the number of characters copied in to the buffer (minus the terminator)
  int Format(const DateTimeFormatContext& dt_ctx, int len, char* buff);

  static const char* LLVM_CLASS_NAME;

 private:
  friend class UnusedClass;

  // Precision of fractional part of the time: nanoseconds.
  static const double FRACTIONAL;

  // Boost ptime leaves a gap in the structure, so we swap the order to make it
  // 12 contiguous bytes.  We then must convert to and from the boost ptime data type.

  // 8 bytes - stores the nanoseconds within the current day
  boost::posix_time::time_duration time_of_day_;

  // 4 -bytes - stores the date as a Julian day
  boost::gregorian::date date_;
};

// This function must be called 'hash_value' to be picked up by boost.
inline std::size_t hash_value(const TimestampValue& v) {
  return v.Hash();
}

std::ostream& operator<<(std::ostream& os, const TimestampValue& timestamp_value);
}

#endif
