// Copyright (c) 2012 Cloudera, Inc. All rights reserved.

#ifndef IMPALA_UTIL_DEBUG_COUNTERS_H
#define IMPALA_UTIL_DEBUG_COUNTERS_H

#include "util/runtime-profile.h"

#define ENABLE_DEBUG_COUNTERS 1

namespace impala {

// Runtime counters have a two-phase lifecycle - creation and update. This is not
// convenient for debugging where we would like to add and remove counters with a minimum
// of boilerplate. This header adds a global debug runtime profile, and macros to
// update-or-create counters in one line of code. Counters created this way are not
// intended to remain in the code; they are a tool for identifying hotspots without having
// to run a full profiler.
// The AddCounterIfAbsent call adds some more overhead to each macro, and therefore they
// should not be used where minimal impact on performance is needed. You can still use
// AddCounterIfAbsent in a less-critical section of the code, and then call the standard
// counter macros with DebugRuntimeProfile::profile() as the profile instance in the usual
// way.
class DebugRuntimeProfile {
 public:
  static RuntimeProfile& profile() {
    static RuntimeProfile profile(new ObjectPool(), "DebugProfile");
    return profile;
  }
};

#if ENABLE_DEBUG_COUNTERS

#define DEBUG_SCOPED_TIMER(counter_name) \
  COUNTER_SCOPED_TIMER(DebugRuntimeProfile::profile().AddCounterIfAbsent(counter_name, \
    TCounterType::CPU_TICKS))

#define DEBUG_COUNTER_UPDATE(counter_name, v) \
  COUNTER_UPDATE(DebugRuntimeProfile::profile().AddCounterIfAbsent(counter_name, \
    TCounterType::UNIT), v)

#define DEBUG_COUNTER_SET(counter_name, v) \
  COUNTER_SET(DebugRuntimeProfile::profile().AddCounterIfAbsent(counter_name, \
    TCounterType::UNIT), v)

#define PRETTY_PRINT_DEBUG_COUNTERS(ostream_ptr) \
  DebugRuntimeProfile::profile().PrettyPrint(ostream_ptr)

#else

#define DEBUG_SCOPED_TIMER(counter_name)
#define DEBUG_COUNTER_UPDATE(counter_name, v)
#define DEBUG_COUNTER_SET(counter_name, v)
#define PRETTY_PRINT_DEBUG_COUNTERS(ostream_ptr)

#endif // ENABLE_DEBUG_COUNTERS

}

#endif // IMPALA_UTIL_DEBUG_COUNTERS_H
