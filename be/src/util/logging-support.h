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

#ifndef IMPALA_UTIL_LOGGING_SUPPORT_H
#define IMPALA_UTIL_LOGGING_SUPPORT_H

#include "util/jni-util.h"
#include "gen-cpp/Logging_types.h"

namespace impala {

/// InitLoggingSupport registers the native logging functions with JNI. This allows
/// the Java log4j log messages to be forwarded to Glog.
void InitJvmLoggingSupport();

/// Helper function to convert a command line logging flag value (input as an int) to the
/// matching TLogLevel enum value.
TLogLevel::type FlagToTLogLevel(int flag);
}
#endif
