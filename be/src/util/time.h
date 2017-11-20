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

#ifndef IMPALA_UTIL_TIME_H
#define IMPALA_UTIL_TIME_H

#include <stdint.h>
#include <time.h>
#include <string>

#include "common/global-types.h"
#include "gutil/walltime.h"

/// Utilities for collecting timings.
namespace impala {

/// Returns a value representing a point in time that is unaffected by daylight savings or
/// manual adjustments to the system clock. This should not be assumed to be a Unix
/// time. Typically the value corresponds to elapsed time since the system booted. See
/// UnixMillis() below if you need to send a time to a different host.
inline int64_t MonotonicNanos() {
  timespec ts;
  clock_gettime(CLOCK_MONOTONIC, &ts);
  return ts.tv_sec * NANOS_PER_SEC + ts.tv_nsec;
}

inline int64_t MonotonicMicros() {  // 63 bits ~= 5K years uptime
  return GetMonoTimeMicros();
}

inline int64_t MonotonicMillis() {
  return GetMonoTimeMicros() / MICROS_PER_MILLI;
}

inline int64_t MonotonicSeconds() {
  return GetMonoTimeMicros() / MICROS_PER_SEC;
}

/// Returns the number of milliseconds that have passed since the Unix epoch. This is
/// affected by manual changes to the system clock but is more suitable for use across
/// a cluster. For more accurate timings on the local host use the monotonic functions
/// above.
inline int64_t UnixMillis() {
  return GetCurrentTimeMicros() / MICROS_PER_MILLI;
}

/// Return the time 'time_us' microseconds away from now in 'abs_time'.
inline void TimeFromNowMicros(int64_t time_us, timespec* abs_time) {
  clock_gettime(CLOCK_MONOTONIC, abs_time);
  abs_time->tv_nsec += (time_us % MICROS_PER_SEC) * NANOS_PER_MICRO;
  abs_time->tv_sec += time_us / MICROS_PER_SEC + abs_time->tv_nsec / NANOS_PER_SEC;
  abs_time->tv_nsec %= NANOS_PER_SEC;
}

/// Return the time 'time_ms' milliseconds away from now in 'abs_time'.
inline void TimeFromNowMillis(int64_t time_ms, timespec* abs_time) {
  TimeFromNowMicros(time_ms * MICROS_PER_MILLI, abs_time);
}

/// Returns the number of microseconds that have passed since the Unix epoch. This is
/// affected by manual changes to the system clock but is more suitable for use across
/// a cluster. For more accurate timings on the local host use the monotonic functions
/// above.
inline int64_t UnixMicros() {
  return GetCurrentTimeMicros();
}

/// Sleeps the current thread for at least duration_ms milliseconds.
void SleepForMs(const int64_t duration_ms);

// An enum class to use as precision argument for the ToString*() functions below
enum TimePrecision {
  Second,
  Millisecond,
  Microsecond,
  Nanosecond
};

/// Converts the input Unix time, 's', specified in seconds since the Unix epoch, to a
/// date-time string in the local time zone. The precision in the output date-time string
/// is specified by the second argument, 'p'. The returned string is of the format
/// yyyy-MM-dd HH:mm:SS[.ms[us[ns]]. It's worth noting that if the precision specified
/// by 'p' is higher than that of the input timestamp, the part corresponding to
/// 'p' in the fractional second part of the output will just be zero-padded.
std::string ToStringFromUnix(int64_t s, TimePrecision p = TimePrecision::Second);

/// Converts input seconds-since-epoch to date-time string in UTC time zone.
std::string ToUtcStringFromUnix(int64_t s, TimePrecision p = TimePrecision::Second);

/// Converts input milliseconds-since-epoch to date-time string in local time zone.
std::string ToStringFromUnixMillis(int64_t ms,
    TimePrecision p = TimePrecision::Millisecond);

/// Converts input milliseconds-since-epoch to date-time string in UTC time zone.
std::string ToUtcStringFromUnixMillis(int64_t ms,
    TimePrecision p = TimePrecision::Millisecond);

/// Converts input microseconds-since-epoch to date-time string in local time zone.
std::string ToStringFromUnixMicros(int64_t us,
    TimePrecision p = TimePrecision::Microsecond);

/// Converts input microseconds-since-epoch to date-time string in UTC time zone.
std::string ToUtcStringFromUnixMicros(int64_t us,
    TimePrecision p = TimePrecision::Microsecond);

/// Converts input microseconds-since-epoch to date-time string in 'tz' time zone.
/// In the returned string fractional seconds are padded to precision 'p'.
std::string ToStringFromUnixMicros(int64_t us, const Timezone& tz,
    TimePrecision p = TimePrecision::Microsecond);

/// Convenience function to convert the current time, derived from UnixMicros(),
/// to a date-time string in the local time zone, padded to nanosecond precision.
inline std::string CurrentTimeString() {
  return ToStringFromUnixMicros(UnixMicros(), TimePrecision::Nanosecond);
}
} // namespace impala
#endif
