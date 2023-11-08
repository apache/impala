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

#include "exprs/timestamp-functions.h"
#include "exprs/timezone_db.h"
#include "runtime/datetime-simple-date-format-parser.h"
#include "runtime/timestamp-parse-util.h"
#include "runtime/timestamp-value.inline.h"

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

namespace impala {

using datetime_parse_util::DateTimeFormatContext;
using datetime_parse_util::SimpleDateFormatTokenizer;
using impala_udf::FunctionContext;
using impala_udf::StringVal;

const char* TimestampValue::LLVM_CLASS_NAME = "class.impala::TimestampValue";
const double TimestampValue::ONE_BILLIONTH = 0.000000001;

TimestampValue TimestampValue::ParseSimpleDateFormat(const char* str, int len) {
  TimestampValue tv;
  discard_result(TimestampParser::ParseSimpleDateFormat(str, len, &tv.date_, &tv.time_));
  return tv;
}

TimestampValue TimestampValue::ParseSimpleDateFormat(const string& str) {
  return ParseSimpleDateFormat(str.c_str(), str.size());
}

TimestampValue TimestampValue::ParseSimpleDateFormat(const char* str, int len,
    const DateTimeFormatContext& dt_ctx) {
  TimestampValue tv;
  discard_result(TimestampParser::ParseSimpleDateFormat(str, len, dt_ctx, &tv.date_,
      &tv.time_));
  return tv;
}

TimestampValue TimestampValue::ParseIsoSqlFormat(const char* str, int len,
    const datetime_parse_util::DateTimeFormatContext& dt_ctx) {
  TimestampValue tv;
  discard_result(TimestampParser::ParseIsoSqlFormat(str, len, dt_ctx, &tv.date_,
      &tv.time_));
  return tv;
}

void TimestampValue::Format(const DateTimeFormatContext& dt_ctx, string& dst) const {
  int max_length = dt_ctx.fmt_out_len;
  dst.resize(max_length);
  int written = TimestampParser::Format(dt_ctx, date_, time_, max_length, &dst[0]);
  if (UNLIKELY(written < 0)) {
    dst.clear();
  } else {
    dst.resize(written);
  }
}

namespace {
inline cctz::time_point<cctz::sys_seconds> UnixTimeToTimePoint(time_t t) {
  static const cctz::time_point<cctz::sys_seconds> epoch =
      std::chrono::time_point_cast<cctz::sys_seconds>(
          std::chrono::system_clock::from_time_t(0));
  return epoch + cctz::sys_seconds(t);
}

inline time_t TimePointToUnixTime(const cctz::time_point<cctz::sys_seconds>& tp) {
  static const cctz::time_point<cctz::sys_seconds> epoch =
      std::chrono::time_point_cast<cctz::sys_seconds>(
          std::chrono::system_clock::from_time_t(0));
  return (tp - epoch).count();
}

// Returns 'true' iff 'cs' is out of valid range (years 1400..9999 are considered valid).
inline bool IsDateOutOfRange(const cctz::civil_second& cs) {
  // Smallest valid year.
  const static int MIN_YEAR =
      boost::gregorian::date(boost::date_time::min_date_time).year();
  // Largest valid year.
  const static int MAX_YEAR =
      boost::gregorian::date(boost::date_time::max_date_time).year();
  return cs.year() < MIN_YEAR || cs.year() > MAX_YEAR;
}

TimestampValue CivilSecondsToTimestampValue(const cctz::civil_second& cs, int64_t nanos) {
  // boost::gregorian::date() throws boost::gregorian::bad_year if year is not in the
  // 1400..9999 range. Need to check validity before creating the date object.
  if (UNLIKELY(IsDateOutOfRange(cs))) {
    return TimestampValue();
  } else {
    return TimestampValue(
        date(cs.year(), cs.month(), cs.day()),
        time_duration(cs.hour(), cs.minute(), cs.second(), nanos));
  }
}

}

void TimestampValue::UtcToLocal(const Timezone& local_tz,
    TimestampValue* start_of_repeated_period, TimestampValue* end_of_repeated_period) {
  DCHECK(HasDateAndTime());
  time_t unix_time;
  if (UNLIKELY(!UtcToUnixTime(&unix_time))) {
    SetToInvalidDateTime();
    return;
  }

  cctz::time_point<cctz::sys_seconds> from_tp = UnixTimeToTimePoint(unix_time);
  cctz::civil_second to_cs = cctz::convert(from_tp, local_tz);

  *this = CivilSecondsToTimestampValue(to_cs, time_.fractional_seconds());

  if (start_of_repeated_period == nullptr && end_of_repeated_period == nullptr) return;
  // Do the reverse conversion if repeated period boundaries are needed.
  const cctz::time_zone::civil_lookup from_cl = local_tz.lookup(to_cs);
  if (UNLIKELY(from_cl.kind == cctz::time_zone::civil_lookup::REPEATED)) {
    if (start_of_repeated_period != nullptr) {
      // Start of the period is simply the transition time converted to local time.
      to_cs = cctz::convert(from_cl.trans, local_tz);
      *start_of_repeated_period = CivilSecondsToTimestampValue(to_cs, 0);
    }
    if (end_of_repeated_period != nullptr) {
      // End of the period is last nanosecond before transition time converted to
      // local time.
      to_cs = cctz::convert(from_cl.trans - std::chrono::seconds(1), local_tz);
      *end_of_repeated_period =
          CivilSecondsToTimestampValue(to_cs, NANOS_PER_SEC - 1);
    }
  }
}

void TimestampValue::LocalToUtc(const Timezone& local_tz,
    TimestampValue* pre_utc_if_repeated, TimestampValue* post_utc_if_repeated) {
  DCHECK(HasDateAndTime());
  // Time-zone conversion rules don't affect fractional seconds, leave them intact.
  const auto nanos = nanoseconds(time_.fractional_seconds());
  const cctz::civil_second from_cs(date_.year(), date_.month(), date_.day(),
      time_.hours(), time_.minutes(), time_.seconds());

  // 'from_cl' represents the 'time_point' that corresponds to 'from_cs' civil time within
  // 'local_tz' time-zone.
  const cctz::time_zone::civil_lookup from_cl = local_tz.lookup(from_cs);

  if (LIKELY(from_cl.kind == cctz::time_zone::civil_lookup::UNIQUE)) {
    *this = UtcFromUnixTimeTicks<1>(TimePointToUnixTime(from_cl.pre));
    time_ += nanos;
    return;
  }

  // In case the resulting 'time_point' is ambiguous, we have to invalidate this
  // TimestampValue and set pre/post_utc_if_repeated if needed.
  // 'civil_lookup' members and the details of handling ambiguity are described at:
  // https://github.com/google/cctz/blob/a2dd3d0fbc811fe0a1d4d2dbb0341f1a3d28cb2a/
  // include/cctz/time_zone.h#L106
  SetToInvalidDateTime();
  if (from_cl.kind == cctz::time_zone::civil_lookup::REPEATED){
    if (pre_utc_if_repeated != nullptr) {
      *pre_utc_if_repeated = UtcFromUnixTimeTicks<1>(TimePointToUnixTime(from_cl.pre));
      pre_utc_if_repeated->time_ += nanos;
    }
    if (post_utc_if_repeated != nullptr) {
      *post_utc_if_repeated = UtcFromUnixTimeTicks<1>(TimePointToUnixTime(from_cl.post));
      post_utc_if_repeated->time_ += nanos;
    }
  } else {
    DCHECK(from_cl.kind == cctz::time_zone::civil_lookup::SKIPPED);
    if (pre_utc_if_repeated != nullptr) {
      *pre_utc_if_repeated = UtcFromUnixTimeTicks<1>(TimePointToUnixTime(from_cl.trans));
      pre_utc_if_repeated->time_ += nanos;
    }
    if (post_utc_if_repeated != nullptr) {
      *post_utc_if_repeated = UtcFromUnixTimeTicks<1>(TimePointToUnixTime(from_cl.trans));
      post_utc_if_repeated->time_ += nanos;
    }
  }
}

ostream& operator<<(ostream& os, const TimestampValue& timestamp_value) {
  char dst[SimpleDateFormatTokenizer::DEFAULT_DATE_TIME_FMT_LEN + 1];
  const int out_len = TimestampParser::FormatDefault(timestamp_value.date(),
      timestamp_value.time(), dst);
  if (LIKELY(out_len >= 0)) {
    dst[out_len] = '\0';
    os << dst;
  }
  return os;
}

TimestampValue TimestampValue::UnixTimeToLocal(
    time_t unix_time, const Timezone& local_tz) {
  cctz::time_point<cctz::sys_seconds> from_tp = UnixTimeToTimePoint(unix_time);
  cctz::civil_second to_cs = cctz::convert(from_tp, local_tz);
  // boost::gregorian::date() throws boost::gregorian::bad_year if year is not in the
  // 1400..9999 range. Need to check validity before creating the date object.
  if (UNLIKELY(IsDateOutOfRange(to_cs))) {
    return ptime(not_a_date_time);
  } else {
    return TimestampValue(
        boost::gregorian::date(to_cs.year(), to_cs.month(), to_cs.day()),
        boost::posix_time::time_duration(to_cs.hour(), to_cs.minute(), to_cs.second()));
  }
}

TimestampValue TimestampValue::FromUnixTime(time_t unix_time, const Timezone* local_tz) {
  if (local_tz != UTCPTR) {
    return UnixTimeToLocal(unix_time, *local_tz);
  } else {
    return UtcFromUnixTimeTicks<1>(unix_time);
  }
}

void TimestampValue::ToString(string& dst) const {
  dst.resize(SimpleDateFormatTokenizer::DEFAULT_DATE_TIME_FMT_LEN);
  const int out_len = TimestampParser::FormatDefault(date(), time(), dst.data());
  if (UNLIKELY(out_len != SimpleDateFormatTokenizer::DEFAULT_DATE_TIME_FMT_LEN)) {
    if (UNLIKELY(out_len < 0)) {
      dst.clear();
    } else {
      dst.resize(out_len);
    }
  }
}

string TimestampValue::ToString() const {
  string dst;
  ToString(dst);
  return dst;
}

TimestampValue TimestampValue::Add(const boost::posix_time::time_duration& t) const {
  if (!IsValidTime(t) || !HasDateAndTime()) return TimestampValue();

  int64_t this_in_nano_sec = time_.total_nanoseconds();
  int64_t t_in_nano_sec = t.total_nanoseconds();

  if (this_in_nano_sec + t_in_nano_sec < NANOS_PER_DAY) {
    return TimestampValue(date_, time_ + t);
  } else {
    int64_t total_in_nano_sec = this_in_nano_sec + t_in_nano_sec;
    int64_t days = total_in_nano_sec / NANOS_PER_DAY;
    int64_t nano_secs_remaining = total_in_nano_sec % NANOS_PER_DAY;
    return TimestampValue(date_ + boost::gregorian::date_duration(days),
        boost::posix_time::time_duration(0, 0, 0, nano_secs_remaining));
  }
}

TimestampValue TimestampValue::Subtract(const boost::posix_time::time_duration& t) const {
  if (!IsValidTime(t) || !HasDateAndTime()) return TimestampValue();

  int64_t this_in_nano_sec = time_.total_nanoseconds();
  int64_t t_in_nano_sec = t.total_nanoseconds();
  if (this_in_nano_sec - t_in_nano_sec >= 0) {
    return TimestampValue(date_, time_ - t);
  } else {
    return TimestampValue(date_ - boost::gregorian::date_duration(1),
        boost::posix_time::time_duration(
            0, 0, 0, NANOS_PER_DAY + this_in_nano_sec - t_in_nano_sec));
  }
}
}
