// Copyright (c) 2011 Cloudera, Inc. All rights reserved.

#include "runtime/timestamp-value.h"
#include "common/status.h"
#include <cstdio>
#include <glog/logging.h>

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
    if (strbuf.size() < 11) {
      ptime temp; // created as not_a_date_time
      *this = TimestampValue(temp);
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
  return is;
}

}
