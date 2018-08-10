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

#ifndef IMPALA_UTIL_LOGGING_SUPPORT_H
#define IMPALA_UTIL_LOGGING_SUPPORT_H

#include "gen-cpp/Logging_types.h"

namespace impala {

class Webserver;

/// Registers the required native logging functions with JNI. This allows the Java log4j
/// log messages to be forwarded to Glog. Also loads the JNI helper methods to dynamically
/// change these Java log levels.
void InitJvmLoggingSupport();

/// Helper function to convert a command line logging flag value (input as an int) to the
/// matching TLogLevel enum value.
TLogLevel::type FlagToTLogLevel(int flag);

/// Registers the call back methods for handling dynamic log level changes. Since every
/// daemon need not include an embedded jvm, dynamic log4j configuration is supported only
/// when register_log4j_handlers is true.
void RegisterLogLevelCallbacks(Webserver* webserver, bool register_log4j_handlers);

class LoggingSupport {
 public:
  /// Helper function for log rotation that deletes all files matching the path pattern
  /// except for the max_log_files newest. If max_log_files is <= 0, no files will
  /// be deleted.
  static void DeleteOldLogs(const std::string& path_pattern, int max_log_files);
};

}
#endif
