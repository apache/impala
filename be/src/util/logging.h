// Copyright (c) 2012 Cloudera, Inc. All rights reserved.

#ifndef IMPALA_UTIL_LOGGING_H
#define IMPALA_UTIL_LOGGING_H

namespace impala {

// glog doesn't allow multiple invocations of InitGoogleLogging. This method conditionally
// calls InitGoogleLogging only if it hasn't been called before.
void InitGoogleLoggingSafe(const char* arg);

}

#endif // IMPALA_UTIL_LOGGING_H
