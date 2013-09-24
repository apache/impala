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

// This file contains implementations for the JNI FeSupport interface.

#include "service/fe-support.h"

#include <boost/scoped_ptr.hpp>

#include "common/logging.h"
#include "codegen/llvm-codegen.h"
#include "common/status.h"
#include "exec/exec-node.h"
#include "exec/hbase-table-scanner.h"
#include "exprs/expr.h"
#include "runtime/coordinator.h"
#include "runtime/row-batch.h"
#include "runtime/runtime-state.h"
#include "runtime/data-stream-mgr.h"
#include "runtime/hdfs-fs-cache.h"
#include "runtime/client-cache.h"
#include "runtime/timestamp-value.h"
#include "service/impala-server.h"
#include "util/cpu-info.h"
#include "util/disk-info.h"
#include "util/jni-util.h"
#include "util/logging.h"
#include "rpc/thrift-util.h"
#include "rpc/thrift-server.h"
#include "util/debug-util.h"
#include "gen-cpp/Data_types.h"
#include "gen-cpp/Frontend_types.h"

using namespace impala;
using namespace std;
using namespace boost;
using namespace apache::thrift::server;

// Requires JniUtil::Init() to have been called.
extern "C"
JNIEXPORT jbyteArray JNICALL
Java_com_cloudera_impala_service_FeSupport_NativeEvalConstExpr(
    JNIEnv* env, jclass caller_class, jbyteArray thrift_predicate_bytes,
    jbyteArray thrift_query_globals_bytes) {
  ObjectPool obj_pool;
  TExpr thrift_predicate;
  DeserializeThriftMsg(env, thrift_predicate_bytes, &thrift_predicate);
  TQueryGlobals query_globals;
  DeserializeThriftMsg(env, thrift_query_globals_bytes, &query_globals);
  RuntimeState state(query_globals.now_string, query_globals.user);
  jbyteArray result_bytes = NULL;
  JniLocalFrame jni_frame;
  Expr* e;
  THROW_IF_ERROR_RET(jni_frame.push(env), env, JniUtil::internal_exc_class(),
                     result_bytes);
  THROW_IF_ERROR_RET(Expr::CreateExprTree(&obj_pool, thrift_predicate, &e), env,
                     JniUtil::internal_exc_class(), result_bytes);
  THROW_IF_ERROR_RET(Expr::Prepare(e, &state, RowDescriptor()), env,
                     JniUtil::internal_exc_class(), result_bytes);

  TColumnValue val;
  e->GetValue(NULL, false, &val);
  THROW_IF_ERROR_RET(SerializeThriftMsg(env, &val, &result_bytes), env,
                     JniUtil::internal_exc_class(), result_bytes);
  return result_bytes;
}

// Called by the frontend to log messages to Glog
extern "C"
JNIEXPORT void JNICALL
Java_com_cloudera_impala_service_FeSupport_NativeLogger(
    JNIEnv* env, jclass caller_class, int severity, jstring msg, jstring file,
    int line_number) {

  // Mimic the behaviour of VLOG(N) by ignoring verbose log messages when appropriate.
  if (severity == TLogLevel::VLOG && !VLOG_IS_ON(1)) return;
  if (severity == TLogLevel::VLOG_2 && !VLOG_IS_ON(2)) return;
  if (severity == TLogLevel::VLOG_3 && !VLOG_IS_ON(3)) return;

  // Unused required argument to GetStringUTFChars
  jboolean dummy;
  const char* filename = env->GetStringUTFChars(file, &dummy);
  const char* str = env->GetStringUTFChars(msg, &dummy);
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

  env->ReleaseStringUTFChars(msg, str);
  env->ReleaseStringUTFChars(file, filename);
}

namespace impala {

void InitFeSupport() {
  JNIEnv* env = getJNIEnv();
  JNINativeMethod nm;
  jclass native_backend_cl = env->FindClass("com/cloudera/impala/service/FeSupport");
  nm.name = const_cast<char*>("NativeEvalConstExpr");
  nm.signature = const_cast<char*>("([B[B)[B");
  nm.fnPtr = reinterpret_cast<void*>(
      ::Java_com_cloudera_impala_service_FeSupport_NativeEvalConstExpr);
  env->RegisterNatives(native_backend_cl, &nm, 1);
  EXIT_IF_EXC(env);

  nm.name = const_cast<char*>("NativeLogger");
  nm.signature = const_cast<char*>("(ILjava/lang/String;Ljava/lang/String;I)V");
  nm.fnPtr = reinterpret_cast<void*>(
      ::Java_com_cloudera_impala_service_FeSupport_NativeLogger);
  env->RegisterNatives(native_backend_cl, &nm, 1);
  EXIT_IF_EXC(env);
}

}
