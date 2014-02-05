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

#include "util/logging-support.h"

#include "common/logging.h"

using namespace impala;
using namespace std;

DEFINE_int32(non_impala_java_vlog, 0, "(Advanced) The log level (equivalent to --v) for "
    "non-Impala Java classes (0: INFO, 1 and 2: DEBUG, 3: TRACE)");

// Requires JniUtil::Init() to have been called. Called by the frontend and catalog
// service to log messages to Glog.
extern "C"
JNIEXPORT void JNICALL
Java_com_cloudera_impala_util_NativeLogger_Log(
    JNIEnv* env, jclass caller_class, int severity, jstring msg, jstring file,
    int line_number) {

  // Mimic the behaviour of VLOG(N) by ignoring verbose log messages when appropriate.
  if (severity == TLogLevel::VLOG && !VLOG_IS_ON(1)) return;
  if (severity == TLogLevel::VLOG_2 && !VLOG_IS_ON(2)) return;
  if (severity == TLogLevel::VLOG_3 && !VLOG_IS_ON(3)) return;

  // Unused required argument to GetStringUTFChars
  jboolean dummy;
  const char* filename = env->GetStringUTFChars(file, &dummy);
  const char* str = "";
  if (msg != NULL) str = env->GetStringUTFChars(msg, &dummy);
  int log_level = google::INFO;
  switch (severity) {
    case TLogLevel::VLOG:
    case TLogLevel::VLOG_2:
    case TLogLevel::VLOG_3:
      log_level = google::INFO;
      break;
    case TLogLevel::INFO:
      log_level = google::INFO;
      break;
    case TLogLevel::WARN:
      log_level = google::WARNING;
      break;
    case TLogLevel::ERROR:
      log_level = google::ERROR;
      break;
    case TLogLevel::FATAL:
      log_level = google::FATAL;
      break;
    default:
      DCHECK(false) << "Unrecognised TLogLevel: " << log_level;
  }
  google::LogMessage(filename, line_number, log_level).stream() << string(str);
  if (msg != NULL) env->ReleaseStringUTFChars(msg, str);
  env->ReleaseStringUTFChars(file, filename);
}

namespace impala {

void InitJvmLoggingSupport() {
  JNIEnv* env = getJNIEnv();
  JNINativeMethod nm;
  jclass native_backend_cl = env->FindClass("com/cloudera/impala/util/NativeLogger");
  nm.name = const_cast<char*>("Log");
  nm.signature = const_cast<char*>("(ILjava/lang/String;Ljava/lang/String;I)V");
  nm.fnPtr = reinterpret_cast<void*>(::Java_com_cloudera_impala_util_NativeLogger_Log);
  env->RegisterNatives(native_backend_cl, &nm, 1);
  EXIT_IF_EXC(env);
}

TLogLevel::type FlagToTLogLevel(int flag) {
  switch (flag) {
    case 0: return TLogLevel::INFO;
    case 1: return TLogLevel::VLOG;
    case 2: return TLogLevel::VLOG_2;
    case 3:
    default: return TLogLevel::VLOG_3;
  }
}

}
