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

#ifndef IMPALA_UTIL_TIME_H
#define IMPALA_UTIL_TIME_H

#include <boost/date_time/posix_time/posix_time.hpp>

// Utilities for dealing with millisecond timestamps without the complexity of
// TimestampValue or the boost time libraries. The emphasis is on simplicity for the
// common case. As soon as you have a more sophisticated requirement, move on to using
// boost::posix_time or similar.
namespace impala {

// Returns the time since the Unix epoch according to the local clock. Convenient for
// monotonic increasing timestamps that don't reset overnight (unlike
// posix_time::local_time()).
inline boost::posix_time::time_duration time_since_epoch() {
  static const boost::posix_time::ptime EPOCH =
    boost::posix_time::time_from_string("1970-01-01 00:00:00.000");
  return boost::posix_time::microsec_clock::local_time() - EPOCH;
}

// Returns the number of milliseconds that have passed since the local-time epoch.
// Calling this method is roughly 1.33x more costly than calling local_time() directly, as
// you might expect. A performance advantage may be accrued by this method when doing a
// lot of arithmetic on the timestamp (e.g. for computing next wakeup times for a thread)
// since those computations are done over the raw int64_t type rather than the structure
// time_duration.
inline int64_t ms_since_epoch() {
  return time_since_epoch().total_milliseconds();
}

// Sleeps the current thread for at least duration_ms.
void SleepForMs(const int64_t duration_ms);

}

#endif
