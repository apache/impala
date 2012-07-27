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
class  TimestampValue {
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
  TimestampValue(const std::string& strbuf);
  
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
    return this->date_ < other.date_ ||
        (this->date_ == other.date_ && this->time_of_day_ <= other.time_of_day_);
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
    boost::posix_time::ptime temp;
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

 private:
  friend class UnusedClass;

  static const double FRACTIONAL = 0.000000001;

  // Boost ptime leaves a gap in the structure, so we swap the order to make it
  // 12 contiguous bytes.  We then must convert to and from the boost ptime data type.
  boost::posix_time::time_duration time_of_day_;
  boost::gregorian::date date_;
};

std::ostream& operator<<(std::ostream& os, const TimestampValue& timestamp_value);
std::istream& operator>>(std::istream& is, TimestampValue& timestamp_value);
}

#endif
