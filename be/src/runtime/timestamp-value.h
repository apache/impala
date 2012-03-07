// Copyright (c) 2011 Cloudera, Inc. All rights reserved.

#ifndef IMPALA_RUNTIME_TIMESTAMP_VALUE_H
#define IMPALA_RUNTIME_TIMESTAMP_VALUE_H


#include <string>
#include <ctime>
#include <boost/cstdint.hpp>
#include <boost/date_time/posix_time/posix_time.hpp>

namespace impala {

time_t to_time_t(boost::posix_time::ptime t);

// The format of a timestamp-typed slot.
struct TimestampValue {

  static const double FRACTIONAL = 0.000000001;

  boost::posix_time::ptime timestamp;

  TimestampValue() { }
  TimestampValue(int64_t t, int64_t n) {
    timestamp = boost::posix_time::from_time_t(t);
    timestamp += boost::posix_time::nanoseconds(n);
  }
  TimestampValue(double t) {
    int64_t i = t;
    timestamp = boost::posix_time::from_time_t(i);
    timestamp += boost::posix_time::nanoseconds((t-i)/FRACTIONAL);
  }
  TimestampValue(const std::string& strbuf);
  
  TimestampValue(int64_t t) { timestamp = boost::posix_time::from_time_t(t); }
  TimestampValue(int32_t t) { timestamp = boost::posix_time::from_time_t(t); }
  TimestampValue(int16_t t) { timestamp = boost::posix_time::from_time_t(t); }
  TimestampValue(int8_t t) { timestamp = boost::posix_time::from_time_t(t); }
  TimestampValue(bool t) { timestamp = boost::posix_time::from_time_t(t); }
  TimestampValue(const TimestampValue& timestamp_value);

  bool operator==(const TimestampValue& other)
      const { return this->timestamp == other.timestamp; }
  bool operator!=(const TimestampValue& other)
      const { return this->timestamp != other.timestamp; }
  bool operator<=(const TimestampValue& other)
      const { return this->timestamp <= other.timestamp; }
  bool operator>=(const TimestampValue& other)
      const { return this->timestamp >= other.timestamp; }
  bool operator<(const TimestampValue& other)
      const { return this->timestamp < other.timestamp; }
  bool operator>(const TimestampValue& other)
      const { return this->timestamp > other.timestamp; }

  std::string DebugString() const {
    std::string timestr = boost::posix_time::to_iso_extended_string(timestamp);
    size_t off = timestr.find_first_of('T');
    if (off != std::string::npos) timestr.replace(off, 1, 1, ' ');
    return timestr;
  }

  operator bool() const { return (bool)to_time_t(timestamp); }
  operator int8_t() const { return (char)to_time_t(timestamp); }
  operator int16_t() const { return (int16_t)to_time_t(timestamp); }
  operator int32_t() const { return (int32_t)to_time_t(timestamp); }
  operator int64_t() const { return (int64_t)to_time_t(timestamp); }
  operator float() const {
    return static_cast<float>(to_time_t(timestamp)) +
        static_cast<float>(timestamp.time_of_day().fractional_seconds() * FRACTIONAL);
  }
  operator double() const {
    return static_cast<double>(to_time_t(timestamp)) +
        static_cast<double>(timestamp.time_of_day().fractional_seconds() * FRACTIONAL);
  }
};

std::ostream& operator<<(std::ostream& os, const TimestampValue& timestamp_value);
std::istream& operator>>(std::istream& is, TimestampValue& timestamp_value);

}

#endif
