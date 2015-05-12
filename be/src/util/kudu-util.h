// Copyright 2015 Cloudera Inc.
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

#ifndef IMPALA_UTIL_KUDU_UTIL_H_
#define IMPALA_UTIL_KUDU_UTIL_H_

#include "kudu/client/client.h"
#include "kudu/gutil/bind.h"
#include "kudu/util/logging_callback.h"

namespace impala {

static void LogKuduMessage(kudu::KuduLogSeverity severity,
                           const char* filename,
                           int line_number,
                           const struct ::tm* time,
                           const char* message,
                           size_t message_len) {

  // Note: we use raw ints instead of the nice LogSeverity typedef
  // that can be found in glog/log_severity.h as it has an import
  // conflict with gutil/logging-inl.h (indirectly imported).
  int glog_severity;

  switch(severity) {
    case kudu::SEVERITY_INFO: glog_severity = 0; break;
    case kudu::SEVERITY_WARNING: glog_severity = 1; break;
    case kudu::SEVERITY_ERROR: glog_severity = 2; break;
    case kudu::SEVERITY_FATAL: glog_severity = 3; break;
    default : CHECK(false) << "Unexpected severity type: " << severity;
  }

  google::LogMessage log_entry(filename, line_number, glog_severity);
  string msg(message, message_len);
  log_entry.stream() << msg;
}

// Initializes Kudu'd logging by binding a callback that logs back to
// Impala's Glog. This also sets Kudu's verbose logging to whatever
// level is set in Impala.
static void InitKuduLogging() {
  kudu::client::InstallLoggingCallback(kudu::Bind(&LogKuduMessage));
  kudu::client::SetVerboseLogLevel(FLAGS_v);
}


} // namespace impala
#endif
