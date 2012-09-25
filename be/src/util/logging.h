// Copyright (c) 2012 Cloudera, Inc. All rights reserved.

#ifndef IMPALA_UTIL_LOGGING_H
#define IMPALA_UTIL_LOGGING_H

#include <string>
#include "common/logging.h"

namespace impala {

// glog doesn't allow multiple invocations of InitGoogleLogging. This method conditionally
// calls InitGoogleLogging only if it hasn't been called before.
void InitGoogleLoggingSafe(const char* arg);

// Returns the full pathname of the symlink to the most recent log
// file corresponding to this severity
void GetFullLogFilename(google::LogSeverity severity, std::string* filename);

// Shuts down the google logging library. Call before exit to ensure that log files are 
// flushed. May only be called once.
void ShutdownLogging();

// Writes all command-line flags to the log at level INFO. 
void LogCommandLineFlags();
}

#endif // IMPALA_UTIL_LOGGING_H
