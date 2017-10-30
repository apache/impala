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

#include "runtime/timestamp-value.h"

#include <boost/date_time/posix_time/posix_time.hpp>

#include "runtime/timestamp-parse-util.h"

#include "common/names.h"

using boost::date_time::not_a_date_time;
using boost::gregorian::date;
using boost::gregorian::date_duration;
using boost::posix_time::from_time_t;
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

TimestampValue TimestampValue::Parse(const char* str, int len) {
  TimestampValue tv;
  TimestampParser::Parse(str, len, &tv.date_, &tv.time_);
  return tv;
}

TimestampValue TimestampValue::Parse(const string& str) {
  return Parse(str.c_str(), str.size());
}

TimestampValue TimestampValue::Parse(const char* str, int len,
    const DateTimeFormatContext& dt_ctx) {
  TimestampValue tv;
  TimestampParser::Parse(str, len, dt_ctx, &tv.date_, &tv.time_);
  return tv;
}

int TimestampValue::Format(const DateTimeFormatContext& dt_ctx, int len, char* buff)
    const {
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
    if (UNLIKELY(localtime_r(&utc, &temp) == nullptr)) {
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
  } catch (std::exception& /* from Boost */) {
    *this = ptime(not_a_date_time);
  }
}

ostream& operator<<(ostream& os, const TimestampValue& timestamp_value) {
  return os << timestamp_value.ToString();
}

void TimestampValue::Validate() {
    if (HasDate() && UNLIKELY(!IsValidDate(date_))) {
      time_ = boost::posix_time::time_duration(boost::posix_time::not_a_date_time);
      date_ = boost::gregorian::date(boost::gregorian::not_a_date_time);
    }
}

/// Return a ptime representation of the given Unix time (seconds since the Unix epoch).
/// The time zone of the resulting ptime is local time. This is called by
/// UnixTimeToPtime.
ptime TimestampValue::UnixTimeToLocalPtime(time_t unix_time) {
  tm temp_tm;
  // TODO: avoid localtime*, which takes a global timezone db lock
  if (UNLIKELY(localtime_r(&unix_time, &temp_tm) == nullptr)) {
    return ptime(not_a_date_time);
  }
  try {
    return ptime_from_tm(temp_tm);
  } catch (std::exception&) {
    return ptime(not_a_date_time);
  }
}

/// Return a ptime representation of the given Unix time (seconds since the Unix epoch).
/// The time zone of the resulting ptime is UTC.
/// In order to avoid a serious performance degredation using libc (IMPALA-5357), this
/// function uses boost to convert the time_t to a ptime. Unfortunately, because the boost
/// conversion relies on time_duration to represent the time_t and internally
/// time_duration stores nanosecond precision ticks, the 'fast path' conversion using
/// boost can only handle a limited range of dates (appx years 1677-2622, while Impala
/// supports years 1600-9999). For dates outside this range, the conversion will instead
/// use the libc function gmtime_r which supports those dates but takes the global lock
/// for the timezone db (even though technically it is not needed for the conversion,
/// again see IMPALA-5357). This is called by UnixTimeToPtime.
ptime TimestampValue::UnixTimeToUtcPtime(time_t unix_time) {
  // Minimum Unix time that can be converted with from_time_t: 1677-Sep-21 00:12:44
  const int64_t MIN_BOOST_CONVERT_UNIX_TIME = -9223372036;
  // Maximum Unix time that can be converted with from_time_t: 2262-Apr-11 23:47:16
  const int64_t MAX_BOOST_CONVERT_UNIX_TIME = 9223372036;
  if (LIKELY(unix_time >= MIN_BOOST_CONVERT_UNIX_TIME &&
             unix_time <= MAX_BOOST_CONVERT_UNIX_TIME)) {
    try {
      return from_time_t(unix_time);
    } catch (std::exception&) {
      return ptime(not_a_date_time);
    }
  }

  tm temp_tm;
  if (UNLIKELY(gmtime_r(&unix_time, &temp_tm) == nullptr)) {
    return ptime(not_a_date_time);
  }
  try {
    return ptime_from_tm(temp_tm);
  } catch (std::exception&) {
    return ptime(not_a_date_time);
  }
}

ptime TimestampValue::UnixTimeToPtime(time_t unix_time) {
  if (FLAGS_use_local_tz_for_unix_timestamp_conversions) {
    return UnixTimeToLocalPtime(unix_time);
  } else {
    return UnixTimeToUtcPtime(unix_time);
  }
}

string TimestampValue::ToString() const {
  stringstream ss;
  if (HasDate()) {
    ss << boost::gregorian::to_iso_extended_string(date_);
  }
  if (HasTime()) {
    if (HasDate()) ss << " ";
    ss << boost::posix_time::to_simple_string(time_);
  }
  return ss.str();
}

}
