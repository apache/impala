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


#ifndef IMPALA_RUNTIME_TIMESTAMP_VALUE_INLINE_H
#define IMPALA_RUNTIME_TIMESTAMP_VALUE_INLINE_H

#include "runtime/timestamp-value.h"

#include <boost/date_time/compiler_config.hpp>
#include <boost/date_time/posix_time/conversion.hpp>
#include <chrono>

#include "exprs/timezone_db.h"
#include "gutil/walltime.h"
#include "kudu/util/int128.h"
#include "runtime/datetime-simple-date-format-parser.h"
#include "runtime/timestamp-parse-util.h"
#include "udf/udf.h"
#include "util/arithmetic-util.h"

namespace impala {

using datetime_parse_util::DateTimeFormatContext;
using datetime_parse_util::SimpleDateFormatTokenizer;
using impala_udf::FunctionContext;
using impala_udf::StringVal;

template <int32_t TICKS_PER_SEC>
inline TimestampValue TimestampValue::UtcFromUnixTimeTicks(int64_t unix_time_ticks) {
  static const boost::gregorian::date EPOCH(1970,1,1);
  int64_t days = SplitTime<(uint64_t)TICKS_PER_SEC*SECONDS_PER_DAY>(&unix_time_ticks);

  return TimestampValue(EPOCH + boost::gregorian::date_duration(days),
      boost::posix_time::nanoseconds(unix_time_ticks*(NANOS_PER_SEC/TICKS_PER_SEC)));
}

inline TimestampValue TimestampValue::UtcFromUnixTimeMicros(int64_t unix_time_micros) {
  return UtcFromUnixTimeTicks<MICROS_PER_SEC>(unix_time_micros);
}

inline TimestampValue TimestampValue::FromUnixTimeMicros(int64_t unix_time_micros,
    const Timezone* local_tz) {
  int64_t ts_seconds = SplitTime<MICROS_PER_SEC>(&unix_time_micros);
  TimestampValue result = FromUnixTime(ts_seconds, local_tz);
  if (result.HasDate()) result.time_ += boost::posix_time::microseconds(unix_time_micros);
  return result;
}

inline TimestampValue TimestampValue::UtcFromUnixTimeMillis(int64_t unix_time_millis) {
  return UtcFromUnixTimeTicks<MILLIS_PER_SEC>(unix_time_millis);
}

inline TimestampValue TimestampValue::FromSubsecondUnixTime(
    double unix_time, const Timezone* local_tz) {
  int64_t unix_time_whole = unix_time;
  int64_t nanos = (unix_time - unix_time_whole) / ONE_BILLIONTH;
  return FromUnixTimeNanos(unix_time_whole, nanos, local_tz);
}

inline TimestampValue TimestampValue::UtcFromUnixTimeLimitedRangeNanos(
    int64_t unix_time_nanos) {
  return UtcFromUnixTimeTicks<NANOS_PER_SEC>(unix_time_nanos);
}

inline TimestampValue TimestampValue::FromUnixTimeNanos(time_t unix_time, int64_t nanos,
    const Timezone* local_tz) {
  unix_time =
      ArithmeticUtil::AsUnsigned<std::plus>(unix_time, SplitTime<NANOS_PER_SEC>(&nanos));
  TimestampValue result = FromUnixTime(unix_time, local_tz);
  // 'nanos' is guaranteed to be between [0,NANOS_PER_SEC) at this point, so the
  // next addition cannot change the day or step to a different timezone.
  if (result.HasDate()) {
    DCHECK_GE(nanos, 0);
    DCHECK_LT(nanos, NANOS_PER_SEC);
    result.time_ += boost::posix_time::nanoseconds(nanos);
  }
  return result;
}

inline TimestampValue TimestampValue::FromDaysSinceUnixEpoch(int64_t days) {
  static const boost::gregorian::date EPOCH(1970, 1, 1);
  static const boost::posix_time::time_duration t(0, 0, 0);
  return TimestampValue(EPOCH + boost::gregorian::date_duration(days), t);
}

inline int64_t TimestampValue::DaysSinceUnixEpoch() const {
  DCHECK(HasDate());
  static const boost::gregorian::date epoch(1970,1,1);
  return (date_ - epoch).days();
}

/// Interpret 'this' as a timestamp in UTC and convert to unix time.
/// Returns false if the conversion failed ('unix_time' will be undefined), otherwise
/// true.
inline bool TimestampValue::UtcToUnixTime(time_t* unix_time) const {
  DCHECK(unix_time != nullptr);
  if (UNLIKELY(!HasDateAndTime())) return false;

  *unix_time = DaysSinceUnixEpoch() * SECONDS_PER_DAY + time_.total_seconds();
  return true;
}

/// Interpret 'this' as a timestamp in UTC and convert to unix time in microseconds.
/// Nanoseconds are rounded to the nearest microsecond supported by Impala. Returns
/// false if the conversion failed ('unix_time_micros' will be undefined), otherwise
/// true.
inline bool TimestampValue::UtcToUnixTimeMicros(int64_t* unix_time_micros) const {
  const static int64_t MAX_UNIXTIME = 253402300799; // 9999-12-31 23:59:59
  const static int64_t MAX_UNIXTIME_MICROS =
      MAX_UNIXTIME * MICROS_PER_SEC + (MICROS_PER_SEC - 1);

  DCHECK(unix_time_micros != nullptr);
  time_t unixtime_seconds;
  if (UNLIKELY(!UtcToUnixTime(&unixtime_seconds))) return false;

  DCHECK(HasTime());
  *unix_time_micros =
    (static_cast<int64_t>(unixtime_seconds) * MICROS_PER_SEC) +
    ((time_.fractional_seconds() + (NANOS_PER_MICRO / 2)) / NANOS_PER_MICRO);

  // Rounding may result in the timestamp being MAX_UNIXTIME_MICROS+1 and should be
  // truncated.
  DCHECK_LE(*unix_time_micros, MAX_UNIXTIME_MICROS + 1);
  *unix_time_micros = std::min(MAX_UNIXTIME_MICROS, *unix_time_micros);
  return true;
}

inline bool TimestampValue::FloorUtcToUnixTimeMicros(int64_t* unix_time_micros) const {
  DCHECK(unix_time_micros != nullptr);
  if (UNLIKELY(!HasDateAndTime())) return false;

  *unix_time_micros = DaysSinceUnixEpoch() * SECONDS_PER_DAY * MICROS_PER_SEC
      + time_.total_microseconds();
  return true;
}

inline bool TimestampValue::FloorUtcToUnixTimeMillis(int64_t* unix_time_millis) const {
  DCHECK(unix_time_millis != nullptr);
  if (UNLIKELY(!HasDateAndTime())) return false;

  *unix_time_millis = DaysSinceUnixEpoch() * SECONDS_PER_DAY * MILLIS_PER_SEC
      + time_.total_milliseconds();
  return true;
}

inline bool TimestampValue::UtcToUnixTimeLimitedRangeNanos(
    int64_t* unix_time_nanos) const {
  DCHECK(unix_time_nanos != nullptr);
  time_t unixtime_seconds;
  if (UNLIKELY(!UtcToUnixTime(&unixtime_seconds))) return false;

  DCHECK(HasTime());
  // TODO: consider optimizing this (IMPALA-8268)
  kudu::int128_t nanos128 =
      static_cast<kudu::int128_t>(unixtime_seconds) * NANOS_PER_SEC
      + time_.fractional_seconds();

  if (nanos128 <  std::numeric_limits<int64_t>::min()
      || nanos128 >  std::numeric_limits<int64_t>::max()) {
    return false;
  }
  *unix_time_nanos = static_cast<int64_t>(nanos128);
  return true;
}

/// Converts to Unix time (seconds since the Unix epoch) representation.
/// Returns false if the conversion failed (unix_time will be undefined), otherwise
/// true.
inline bool TimestampValue::ToUnixTime(const Timezone* local_tz,
    time_t* unix_time) const {
  DCHECK(unix_time != nullptr);
  if (UNLIKELY(!HasDateAndTime())) return false;

  if (local_tz == UTCPTR) {
    return UtcToUnixTime(unix_time);
  }

  cctz::civil_second cs(date_.year(), date_.month(), date_.day(), time_.hours(),
      time_.minutes(), time_.seconds());
  cctz::time_point<cctz::sys_seconds> tp = cctz::convert(cs, *local_tz);
  cctz::sys_seconds seconds = tp.time_since_epoch();
  *unix_time = seconds.count();
  return true;
}

/// Converts to Unix time with fractional seconds. The time zone interpretation of the
/// TimestampValue instance is determined as above.
/// Returns false if the conversion failed (unix_time will be undefined), otherwise
/// true.
inline bool TimestampValue::ToSubsecondUnixTime(const Timezone* local_tz,
    double* unix_time) const {
  DCHECK(unix_time != nullptr);
  time_t temp;
  if (UNLIKELY(!ToUnixTime(local_tz, &temp))) return false;
  *unix_time = static_cast<double>(temp);

  DCHECK(HasTime());
  *unix_time += time_.fractional_seconds() * ONE_BILLIONTH;
  return true;
}

inline StringVal TimestampValue::ToStringVal(
    FunctionContext* ctx, const DateTimeFormatContext& dt_ctx) const {
  int max_length = dt_ctx.fmt_out_len;
  StringVal sv(ctx, max_length);
  int written = TimestampParser::Format(
      dt_ctx, date_, time_, max_length, reinterpret_cast<char*>(sv.ptr));
  if (UNLIKELY(written < 0)) {
    sv.is_null = true;
  } else {
    sv.Resize(ctx, written);
  }
  return sv;
}

inline StringVal TimestampValue::ToStringVal(FunctionContext* ctx) const {
  const DateTimeFormatContext* dt_ctx =
      SimpleDateFormatTokenizer::GetDefaultTimestampFormatContext(time_);
  return ToStringVal(ctx, *dt_ctx);
}
}

#endif
