// Copyright (c) 2012 Cloudera, Inc. All rights reserved.

#ifndef IMPALA_UTIL_LOGGING_H
#define IMPALA_UTIL_LOGGING_H

#include <string>
#include <glog/logging.h>

namespace impala {

// glog doesn't allow multiple invocations of InitGoogleLogging. This method conditionally
// calls InitGoogleLogging only if it hasn't been called before.
void InitGoogleLoggingSafe(const char* arg);

// Returns the full pathname of the symlink to the most recent log
// file corresponding to this severity
void GetFullLogFilename(google::LogSeverity severity, std::string* filename);
}

#endif // IMPALA_UTIL_LOGGING_H
