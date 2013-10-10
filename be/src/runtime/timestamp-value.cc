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

#include "runtime/timestamp-value.h"
#include "common/status.h"
#include "common/compiler-util.h"
#include "util/string-parser.h"
#include <cstdio>

using namespace std;
using namespace boost::posix_time;
using namespace boost::gregorian;

namespace impala {
  
const char* TimestampValue::LLVM_CLASS_NAME = "class.impala::TimestampValue";

const double TimestampValue::FRACTIONAL = 0.000000001;

time_t to_time_t(ptime t) {
  if (t == not_a_date_time) {
    return 0;
  }
  ptime epoch(date(1970, 1, 1));
  time_duration::sec_type x = (t - epoch).total_seconds();

  return time_t(x);
}
static const time_duration one_day(24, 0, 0);


inline bool TimestampValue::ParseTime(const char** strp, int* lenp) {
  StringParser::ParseResult status;
  int len = *lenp;
  const char* str = *strp;
  bool time_set = false;

  if (LIKELY(len >= 8 && str[2] == ':' && str[5] == ':' && (len == 8 || str[8] == '.'))) {
    // A duration can be any amount of time, but
    // it must only be within one 24 period to be part of a timestamp.
    int hour = StringParser::StringToInt<int>(str, 2, &status);
    if (LIKELY(status == StringParser::PARSE_SUCCESS && hour >= 0 && hour < 24)) {
      str += 3;
      len -= 3;

      int minute = StringParser::StringToInt<int>(str, 2, &status);
      if (LIKELY(status == StringParser::PARSE_SUCCESS && minute >= 0 && minute < 60)) {
        str += 3;
        len -= 3;

        int second = StringParser::StringToInt<int>(str, 2, &status);
        if (LIKELY(status == StringParser::PARSE_SUCCESS && second >= 0 && second < 60)) {
          str += 2;
          len -= 2;
          int fraction = 0;
          if (LIKELY(len > 0)) {
            ++str;
            --len;

            if (len > 9) len = 9;
            fraction = StringParser::StringToInt<int>(str, len, &status);

            if (LIKELY(status == StringParser::PARSE_SUCCESS && fraction > 0)) {
              // Convert the factional part to a number of nano-seconds.
              for (int i = len; i < 9; ++i) fraction *= 10;
            }
            str +=  len;
            len = 0;
          }
          if (status == StringParser::PARSE_SUCCESS) {
            this->time_of_day_ = time_duration(hour, minute, second, fraction);
            time_set = true;
          }
        }
      }
    }
  }
  if (time_set) {
    *strp = str;
    *lenp = len;
  }
  return time_set;
}

inline bool TimestampValue::ParseDate(const char** strp, int* lenp) {
  StringParser::ParseResult status;
  const char* str = *strp;
  int len = *lenp;
  bool date_set = false;
  // Check for a valid format
  if (LIKELY(len >= 10 &&
      str[4] == '-' && str[7] == '-' && (len == 10 || str[10] == ' '))) {
    int year = StringParser::StringToInt<int>(str, 4, &status);
    if (LIKELY(status == StringParser::PARSE_SUCCESS && year > 0)) {
      str += 5;
      len -= 5;

      int month = StringParser::StringToInt<int>(str, 2, &status);
      if (LIKELY(status == StringParser::PARSE_SUCCESS && month > 0)) {
        str += 3;
        len -= 3;
        
        int day = StringParser::StringToInt<int>(str, 2, &status);
        if (LIKELY(status == StringParser::PARSE_SUCCESS && day > 0)) {
          str += 3;
          len -= 3;

          date_set = true;
          // Catch invalid dates.
          try {
            this->date_ = boost::gregorian::date(year, month, day);
          } catch (exception e) {
            VLOG_ROW << "Invalid date: " << year << "-" << month << "-" << day;
            date_set = false;
          }
        }
      }
    }
  }

  if (date_set) {
    *lenp = len;
    *strp = str;
  }
  return date_set;
}

TimestampValue::TimestampValue(const char* str, int len) {
  // One timestamp format is accepted: YYYY-MM-DD HH:MM:SS.sssssssss
  // Either just the date or just the time may be specified.  This provides
  // minimal support to simulate date and time data types.  All components
  // are required in either the date or time except for the fractional
  // seconds following the '.'.
  // In the case of just a date, the time will be set to 00:00:00.
  // In the case of just a time, the date will be set to invalid.
  // Unfortunately there is no snscanf.

  // Remove leading white space.
  while (len > 0 && isspace(*str)){
    ++str;
    --len;
  }
  // strip the trailing blanks.
  while (len > 0 && isspace(str[len - 1])) --len;


  bool date_set = ParseDate(&str, &len);

  // If there is any data left, it must be a valid time.  If not the whole
  // conversion is considered failed.  We do not return a valid date.
  if (len <= 0) {
    if (date_set) {
      // If there is only a date component then set the time to the start of the day.
      this->time_of_day_ = time_duration(0, 0, 0, 0);
    } else {
      // set the date to be invalid.
      this->date_ = boost::gregorian::date();
    }
    return;
  }

  bool time_set = ParseTime(&str, &len);
  if (time_set) {
    while (len > 0){
      if (!isspace(*str)) {
        time_set = false;
        break;
      }
      ++str;
      --len;
    }
  }

    // If there was a time component it needs to be valid or the whole timestamp
  // is invalid.
  if (!date_set || !time_set) this->date_ = boost::gregorian::date();
  if (!time_set) this->time_of_day_ = time_duration(not_a_date_time);
}
    
ostream& operator<<(ostream& os, const TimestampValue& timestamp_value) {
  return os << timestamp_value.DebugString();
}

}
