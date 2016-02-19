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

#include "common/names.h"

using boost::date_time::not_a_date_time;
using boost::gregorian::date;
using boost::gregorian::date_duration;
using boost::posix_time::nanoseconds;
using boost::posix_time::ptime;
using boost::posix_time::ptime_from_tm;
using boost::posix_time::time_duration;
using boost::posix_time::to_tm;

DEFINE_bool(use_local_tz_for_unix_timestamp_conversions, false,
    "When true, TIMESTAMPs are interpreted in the local time zone when converting to "
    "and from Unix times. When false, TIMESTAMPs are interpreted in the UTC time zone. "
    "Set to true for Hive compatibility.");

// Constants for use with Unix times. Leap-seconds do not apply.
const int32_t SECONDS_IN_MINUTE = 60;
const int32_t SECONDS_IN_HOUR = 60 * SECONDS_IN_MINUTE;
const int32_t SECONDS_IN_DAY = 24 * SECONDS_IN_HOUR;

// struct tm stores month/year data as an offset
const unsigned short TM_YEAR_OFFSET = 1900;
const unsigned short TM_MONTH_OFFSET = 1;

// Boost stores dates as an uint32_t. Since subtraction is needed, convert to signed.
const int64_t EPOCH_DAY_NUMBER =
    static_cast<int64_t>(date(1970, boost::gregorian::Jan, 1).day_number());

namespace impala {

const char* TimestampValue::LLVM_CLASS_NAME = "class.impala::TimestampValue";
const double TimestampValue::ONE_BILLIONTH = 0.000000001;

TimestampValue::TimestampValue(const char* str, int len) {
  TimestampParser::Parse(str, len, &date_, &time_);
}

TimestampValue::TimestampValue(const char* str, int len,
    const DateTimeFormatContext& dt_ctx) {
  TimestampParser::Parse(str, len, dt_ctx, &date_, &time_);
}

int TimestampValue::Format(const DateTimeFormatContext& dt_ctx, int len, char* buff) {
  return TimestampParser::Format(dt_ctx, date_, time_, len, buff);
}

void TimestampValue::UtcToLocal() {
  DCHECK(HasDateAndTime());
  // Previously, conversion was done using boost functions but it was found to be
  // too slow. Doing the conversion without function calls (which also avoids some
  // unnecessary validations) the conversion only take half as long. Original:
  // http://www.boost.org/doc/libs/1_55_0/boost/date_time/c_local_time_adjustor.hpp
  try {
    time_t utc =
        (static_cast<int64_t>(date_.day_number()) - EPOCH_DAY_NUMBER) * SECONDS_IN_DAY +
        time_.hours() * SECONDS_IN_HOUR +
        time_.minutes() * SECONDS_IN_MINUTE +
        time_.seconds();
    tm temp;
    if (UNLIKELY(NULL == localtime_r(&utc, &temp))) {
      *this = ptime(not_a_date_time);
      return;
    }
    // Unlikely but a time zone conversion may push the value over the min/max
    // boundary resulting in an exception.
    date_ = boost::gregorian::date(
        static_cast<unsigned short>(temp.tm_year + TM_YEAR_OFFSET),
        static_cast<unsigned short>(temp.tm_mon + TM_MONTH_OFFSET),
        static_cast<unsigned short>(temp.tm_mday));
    time_ = time_duration(temp.tm_hour, temp.tm_min, temp.tm_sec,
        time().fractional_seconds());
  } catch (std::exception& from_boost) {
    *this = ptime(not_a_date_time);
  }
}

ostream& operator<<(ostream& os, const TimestampValue& timestamp_value) {
  return os << timestamp_value.DebugString();
}

ptime TimestampValue::UnixTimeToPtime(time_t unix_time) const {
  /// Unix times are represented internally in boost as 32 bit ints which limits the
  /// range of dates to 1901-2038 (https://svn.boost.org/trac/boost/ticket/3109), so
  /// libc functions will be used instead.
  tm temp_tm;
  if (FLAGS_use_local_tz_for_unix_timestamp_conversions) {
    if (UNLIKELY(localtime_r(&unix_time, &temp_tm) == NULL)) {
      return ptime(not_a_date_time);
    }
  } else {
    if (UNLIKELY(gmtime_r(&unix_time, &temp_tm) == NULL)) {
      return ptime(not_a_date_time);
    }
  }
  try {
    return ptime_from_tm(temp_tm);
  } catch (std::exception& e) {
    return ptime(not_a_date_time);
  }
}

}
