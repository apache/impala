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

#include "util/logging-support.h"

#include <glob.h>
#include <sys/stat.h>
#include <rapidjson/document.h>
#include <gflags/gflags.h>

#include "common/logging.h"
#include "rpc/jni-thrift-util.h"
#include "util/jni-util.h"
#include "util/webserver.h"

#include "common/names.h"

using namespace impala;
using namespace rapidjson;
using namespace std;

DEFINE_int32(non_impala_java_vlog, 0, "(Advanced) The log level (equivalent to --v) for "
    "non-Impala Java classes (0: INFO, 1 and 2: DEBUG, 3: TRACE)");

// Requires JniUtil::Init() to have been called. Called by the frontend and catalog
// service to log messages to Glog.
extern "C"
JNIEXPORT void JNICALL
Java_org_apache_impala_util_NativeLogger_Log(
    JNIEnv* env, jclass caller_class, int severity, jstring msg, jstring file,
    int line_number) {
  DCHECK(file != nullptr);
  JniUtfCharGuard filename_guard;
  RETURN_VOID_IF_ERROR(JniUtfCharGuard::create(env, file, &filename_guard));
  JniUtfCharGuard msg_guard;
  const char* str = "";
  if (msg != nullptr) {
    RETURN_VOID_IF_ERROR(JniUtfCharGuard::create(env, msg, &msg_guard));
    str = msg_guard.get();
  }

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
  google::LogMessage(filename_guard.get(), line_number, log_level).stream() << str;
}

namespace {
// Defaults to startup flag --v. FLAGS_v can be overriden at runtime for
// debugging, so we save the original value here in case we need to restore
// the defaults. Set in GetThriftBackendGflags().
int FLAGS_v_original_value;

static jclass log4j_logger_class_;
// Jni method descriptors corresponding to getLogLevel() and setLogLevel() operations.
static jmethodID get_log_level_method; // GlogAppender.getLogLevel()
static jmethodID set_log_level_method; // GlogAppender.setLogLevel()
static jmethodID reset_log_levels_method; // GlogAppender.resetLogLevels()

// Helper method to set a message into a member in the document
void AddDocumentMember(const string& message, const char* member,
    Document* document) {
  Value key(member, document->GetAllocator());
  Value output(message.c_str(), document->GetAllocator());
  document->AddMember(key, output, document->GetAllocator());
}

template<class F>
Webserver::UrlCallback MakeCallback(const F& fnc, bool display_log4j_handlers) {
  return [fnc, display_log4j_handlers](const auto& req, auto* doc) {
    // Display log4j log level handlers only when display_log4j_handlers is true.
    if (display_log4j_handlers) AddDocumentMember("true", "include_log4j_handlers", doc);
    (*fnc)(req, doc);
  };
}

void InitDynamicLoggingSupport() {
  JNIEnv* env = JniUtil::GetJNIEnv();
  ABORT_IF_ERROR(JniUtil::GetGlobalClassRef(env, "org/apache/impala/util/GlogAppender",
        &log4j_logger_class_));
  JniMethodDescriptor get_log_level_method_desc =
      {"getLogLevel", "([B)Ljava/lang/String;", &get_log_level_method};
  JniMethodDescriptor set_log_level_method_desc =
      {"setLogLevel", "([B)Ljava/lang/String;", &set_log_level_method};
  JniMethodDescriptor reset_log_level_method_desc =
      {"resetLogLevels", "()V", &reset_log_levels_method};
  ABORT_IF_ERROR(JniUtil::LoadStaticJniMethod(
      env, log4j_logger_class_, &get_log_level_method_desc));
  ABORT_IF_ERROR(JniUtil::LoadStaticJniMethod(
      env, log4j_logger_class_, &set_log_level_method_desc));
  ABORT_IF_ERROR(JniUtil::LoadStaticJniMethod(
      env, log4j_logger_class_, &reset_log_level_method_desc));

  FLAGS_v_original_value = FLAGS_v;
  // Register a validator function for FLAGS_v to make sure it is in the [0-3]
  // range. This is called everytime we try to override FLAGS_v using
  // SetCommandLineOption().
  google::RegisterFlagValidator(&FLAGS_v,
      [](const char* flagname, int value) { return value >= 0 && value <= 3; });
}

// Helper method to get the log level of given Java class. It is a JNI wrapper around
// GlogAppender.getLogLevel().
Status GetJavaLogLevel(const TGetJavaLogLevelParams& params, string* result) {
  return JniCall::static_method(log4j_logger_class_, get_log_level_method)
      .with_thrift_arg(params).Call(result);
}

Status ResetJavaLogLevels() {
  return JniCall::static_method(log4j_logger_class_, reset_log_levels_method).Call();
}

// Callback handler for /get_java_loglevel.
void GetJavaLogLevelCallback(const Webserver::WebRequest& req, Document* document) {
  const auto& args = req.parsed_args;
  Webserver::ArgumentMap::const_iterator log_getclass = args.find("class");
  if (log_getclass == args.end() || log_getclass->second.empty()) {
    AddDocumentMember("Invalid input class name", "error", document);
    return;
  }
  string result;
  TGetJavaLogLevelParams params;
  params.__set_class_name(log_getclass->second);
  Status status = GetJavaLogLevel(params, &result);
  if (!status.ok()) {
    AddDocumentMember(status.GetDetail(), "error", document);
    return;
  }
  if (result.empty()) {
    AddDocumentMember("Invalid input class name", "error", document);
    return;
  }
  AddDocumentMember(result, "get_java_loglevel_result", document);
}

// Callback handler for /set_java_loglevel.
void SetJavaLogLevelCallback(const Webserver::WebRequest& req, Document* document) {
  const auto& args = req.parsed_args;
  Webserver::ArgumentMap::const_iterator classname = args.find("class");
  Webserver::ArgumentMap::const_iterator level = args.find("level");
  if (classname == args.end() || classname->second.empty() ||
      level == args.end() || level->second.empty()) {
    AddDocumentMember("Invalid input parameters. Either class name or log level "
        "is empty.", "error", document);
    return;
  }
  TSetJavaLogLevelParams params;
  string result;
  params.__set_class_name(classname->second);
  params.__set_log_level(level->second);
  Status status = JniCall::static_method(log4j_logger_class_, set_log_level_method)
    .with_thrift_arg(params).Call(&result);
  if (!status.ok()) {
    AddDocumentMember(status.GetDetail(), "error", document);
    return;
  }
  if (result.empty()) {
    AddDocumentMember("Invalid input parameters. Either class name or log level "
        "is empty.", "error", document);
    return;
  }
  AddDocumentMember(result, "set_java_loglevel_result", document);
}

// Callback handler for /reset_java_loglevel.
void ResetJavaLogLevelCallback(const Webserver::WebRequest& req, Document* document) {
  Status status = ResetJavaLogLevels();
  if (!status.ok()) {
    AddDocumentMember(status.GetDetail(), "error", document);
    return;
  }
  AddDocumentMember("Java log levels reset.", "reset_java_loglevel_result", document);
}

// Callback handler for /set_glog_level
void SetGlogLevelCallback(const Webserver::WebRequest& req, Document* document) {
  const auto& args = req.parsed_args;
  Webserver::ArgumentMap::const_iterator glog_level = args.find("glog");
  if (glog_level == args.end() || glog_level->second.empty()) {
    AddDocumentMember("Bad glog level input. Valid inputs are integers in the "
        "range [0-3].", "error", document);
    return;
  }
  string new_log_level = google::SetCommandLineOption("v", glog_level->second.data());
  if (new_log_level.empty()) {
    AddDocumentMember("Bad glog level input. Valid inputs are integers in the "
        "range [0-3].", "error", document);
    return;
  }
  AddDocumentMember(new_log_level, "set_glog_level_result", document);
  AddDocumentMember(to_string(FLAGS_v_original_value), "default_glog_level", document);
}

// Callback handler for /reset_glog_level
void ResetGlogLevelCallback(const Webserver::WebRequest& req, Document* document) {
  string new_log_level = google::SetCommandLineOption("v",
      to_string(FLAGS_v_original_value).data());
  AddDocumentMember(new_log_level, "reset_glog_level_result", document);
}

}

namespace impala {

void InitJvmLoggingSupport() {
  JNIEnv* env = JniUtil::GetJNIEnv();
  JNINativeMethod nm;
  jclass native_backend_cl = env->FindClass("org/apache/impala/util/NativeLogger");
  nm.name = const_cast<char*>("Log");
  nm.signature = const_cast<char*>("(ILjava/lang/String;Ljava/lang/String;I)V");
  nm.fnPtr = reinterpret_cast<void*>(::Java_org_apache_impala_util_NativeLogger_Log);
  env->RegisterNatives(native_backend_cl, &nm, 1);
  ABORT_IF_EXC(env);
  InitDynamicLoggingSupport();
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

void LoggingSupport::DeleteOldLogs(const string& path_pattern, int max_log_files) {
  // Ignore bad input or disable log rotation
  if (max_log_files <= 0) return;

  // Map capturing mtimes, oldest files first
  typedef map<time_t, string> LogFileMap;

  LogFileMap log_file_mtime;
  glob_t result;
  int glob_ret = glob(path_pattern.c_str(), GLOB_TILDE, NULL, &result);
  if (glob_ret != 0) {
    if (glob_ret != GLOB_NOMATCH) {
      LOG(ERROR) << "glob failed in LoggingSupport::DeleteOldLogs on " << path_pattern
                 << " with ret = " << glob_ret;
    }
    globfree(&result);
    return;
  }

  for (size_t i = 0; i < result.gl_pathc; ++i) {
    // Get the mtime for each match
    struct stat stat_val;
    if (stat(result.gl_pathv[i], &stat_val) != 0) {
      LOG(ERROR) << "Could not read last-modified-timestamp for log file "
                 << result.gl_pathv[i] << ", will not delete (error was: "
                 << strerror(errno) << ")";
      continue;
    }
    log_file_mtime[stat_val.st_mtime] = result.gl_pathv[i];
  }
  globfree(&result);

  // Iterate over the map and remove oldest log files first when too many
  // log files exist
  if (log_file_mtime.size() <= max_log_files) return;
  int files_to_delete = log_file_mtime.size() - max_log_files;
  DCHECK_GT(files_to_delete, 0);
  for (LogFileMap::const_reference val: log_file_mtime) {
    if (unlink(val.second.c_str()) == 0) {
      LOG(INFO) << "Old log file deleted during log rotation: " << val.second;
    } else {
      LOG(ERROR) << "Failed to delete old log file: "
                 << val.second << "(error was: " << strerror(errno) << ")";
    }
    if (--files_to_delete == 0) break;
  }
}

void RegisterLogLevelCallbacks(Webserver* webserver, bool register_log4j_handlers) {
  webserver->RegisterUrlCallback("/log_level", "log_level.tmpl",
      MakeCallback([](const Webserver::WebRequest& req, Document* document){},
      register_log4j_handlers), true);
  webserver->RegisterUrlCallback("/set_glog_level", "log_level.tmpl",
      MakeCallback(&SetGlogLevelCallback, register_log4j_handlers), false);
  webserver->RegisterUrlCallback("/reset_glog_level", "log_level.tmpl",
      MakeCallback(&ResetGlogLevelCallback, register_log4j_handlers), false);
  if (!register_log4j_handlers) return;
  webserver->RegisterUrlCallback("/get_java_loglevel", "log_level.tmpl",
      MakeCallback(&GetJavaLogLevelCallback, register_log4j_handlers), false);
  webserver->RegisterUrlCallback("/set_java_loglevel", "log_level.tmpl",
      MakeCallback(&SetJavaLogLevelCallback, register_log4j_handlers), false);
  webserver->RegisterUrlCallback("/reset_java_loglevel", "log_level.tmpl",
      MakeCallback(&ResetJavaLogLevelCallback, register_log4j_handlers), false);
}

}
