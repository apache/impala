// Copyright (c) 2011 Cloudera, Inc. All rights reserved.

#include "runtime/timestamp-value.h"
#include "common/status.h"
#include <cstdio>
#include <boost/lexical_cast.hpp>

using namespace std;
using namespace boost::posix_time;
using namespace boost::gregorian;

namespace impala {

time_t to_time_t(ptime t) {
  if (t == not_a_date_time) {
    return 0;
  }
  ptime epoch(date(1970, 1, 1));
  time_duration::sec_type x = (t - epoch).total_seconds();

  return time_t(x);
}

TimestampValue::TimestampValue(const string& strbuf) {
  try {
    // time_from_string has a bug: a missing time component will pass ok but
    // give strange answers.
    // Boost tickets #622 #6034.
    // We look for things that might be just a date: 2012-07-12
    // The only format accepted here is YYYY-DD-MM.
    bool dash = strbuf.find('-') != string::npos;
    if (strbuf.size() < 11 && dash) {
      boost::gregorian::date d(from_string(strbuf));
      // Mark the time component invalid.
      boost::posix_time::time_duration t(not_a_date_time);
      this->date_ = d;
      this->time_of_day_ = t;
    } else if (!dash) {
      // mark the date component invalid.
      boost::gregorian::date d(not_a_date_time);
      this->date_ = d;
      // Try to convert to a time only. The format accepted is HH:MM:SS.sssssssss.
      boost::posix_time::time_duration t(duration_from_string(strbuf));
      // Time durations an be arbitrarily long, we only want a positive time of day.
      boost::posix_time::time_duration one_day(24, 0, 0);
      if (t >= one_day || t.is_negative()) {
        boost::posix_time::time_duration t(not_a_date_time);
        this->time_of_day_ = t;
      } else {
        this->time_of_day_ = t;
      }
    } else {
      *this = TimestampValue(time_from_string(strbuf));
    }
  } catch (exception& e) {
    ptime temp; // created as not_a_date_time
    *this = TimestampValue(temp);
  }
}

ostream& operator<<(ostream& os, const TimestampValue& timestamp_value) {
  return os << timestamp_value.DebugString();
}

istream& operator>>(istream& is, TimestampValue& timestamp_value) {
  char buf[32];
  memset(buf, '\0', sizeof(buf));
  is.readsome(buf, 32);
  string strbuf(buf, strlen(buf));
  timestamp_value = TimestampValue(strbuf);
  if (timestamp_value.NotADateTime()) {
    LOG(WARNING) << "Invalid timestamp string: '" << strbuf << "'";
    // This is called by auto generated functions that detect invalid
    // conversions from text via this exception.
    throw boost::bad_lexical_cast();
  }
  return is;
}

}
