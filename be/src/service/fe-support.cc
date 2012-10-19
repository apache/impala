// Copyright (c) 2012 Cloudera, Inc. All rights reserved.
//
// This file contains implementations for the JNI FeSupport interface. Avoid loading the
// code more than once as each loading will invoke the initialization function
// JNI_OnLoad (which can be executed at most once).
//
// If the execution path to the JNI FeSupport interfaces does not involves Impalad
// ("mvn test") execution, these functions are called through fesupport.so. This will
// execute the JNI_OnLoadImpl, which starts ImpalaServer (both FE and BE).
//
// If the execution path involves Impalad (which is the normal Impalad execution), the
// JNI_OnLoadImpl will not be executed.

#include "service/fe-support.h"

#include <boost/scoped_ptr.hpp>
#include <server/TServer.h>

#include "common/logging.h"
#include "util/uid-util.h"  // for some reasoon needed right here for hash<TUniqueId>
#include "codegen/llvm-codegen.h"
#include "common/status.h"
#include "exec/exec-node.h"
#include "exec/exec-stats.h"
#include "exec/hbase-table-scanner.h"
#include "exprs/expr.h"
#include "runtime/coordinator.h"
#include "runtime/row-batch.h"
#include "runtime/runtime-state.h"
#include "runtime/data-stream-mgr.h"
#include "runtime/hdfs-fs-cache.h"
#include "runtime/client-cache.h"
#include "service/impala-server.h"
#include "testutil/test-exec-env.h"
#include "util/cpu-info.h"
#include "util/disk-info.h"
#include "util/jni-util.h"
#include "util/logging.h"
#include "util/thrift-util.h"
#include "util/thrift-server.h"
#include "util/debug-util.h"
#include "gen-cpp/Data_types.h"
#include "gen-cpp/ImpalaPlanService_types.h"

DECLARE_bool(serialize_batch);
DECLARE_int32(be_port);
DECLARE_int32(fe_port);

using namespace impala;
using namespace std;
using namespace boost;
using namespace apache::thrift::server;

static TestExecEnv* test_env;
static scoped_ptr<ExecStats> exec_stats;
static ThriftServer* fe_server;
static ThriftServer* be_server;

// calling the c'tor of the contained HdfsFsCache crashes
// TODO(marcel): figure out why and fix it
//static scoped_ptr<TestExecEnv> test_env;

extern "C"
JNIEXPORT jint JNICALL JNI_OnLoad(JavaVM* vm, void* pvt) {
  InitGoogleLoggingSafe("fe-support");
  // This surpresses printing errors to screen, such as "unknown row batch
  // destination" in data-stream-mgr.cc. Only affects "mvn test".
  google::SetStderrLogging(google::FATAL);
  InitThriftLogging();
  CpuInfo::Init();
  DiskInfo::Init();
  LlvmCodeGen::InitializeLlvm(true);
  // install libunwind before activating this on 64-bit systems:
  //google::InstallFailureSignalHandler();

  // Get the JNIEnv* corresponding to current thread.
  JNIEnv* env = getJNIEnv();
  if (env == NULL) {
    return -1;
  }
  jclass impala_exc_cl = env->FindClass("com/cloudera/impala/common/ImpalaException");
  if (impala_exc_cl == NULL) {
    if (env->ExceptionOccurred()) env->ExceptionDescribe();
    return -1;
  }
  JniUtil::InitLibhdfs();
  THROW_IF_ERROR_RET(JniUtil::Init(), env, impala_exc_cl, -1);
  THROW_IF_ERROR_RET(HBaseTableScanner::Init(), env, impala_exc_cl, -1);
  THROW_IF_ERROR_RET(HBaseTableCache::Init(), env, impala_exc_cl, -1);

  // Create an in-process Impala server and in-process backends for test environment.
  VLOG_CONNECTION << "creating test env";
  test_env = new TestExecEnv(2, FLAGS_be_port + 1);
  exec_stats.reset(new ExecStats());
  VLOG_CONNECTION << "starting backends";
  test_env->StartBackends();

  CreateImpalaServer(test_env, FLAGS_fe_port, FLAGS_be_port, &fe_server, &be_server);
  fe_server->Start();
  be_server->Start();
  return JNI_VERSION_1_4;
}

extern "C"
JNIEXPORT void JNICALL JNI_OnUnload(JavaVM* vm, void* pvt) {
  // Get the JNIEnv* corresponding to current thread.
  JNIEnv* env = getJNIEnv();
  if (env == NULL) {
    return;
  }
  jclass impala_exc_cl = env->FindClass("com/cloudera/impala/common/ImpalaException");
  if (impala_exc_cl == NULL) {
    if (env->ExceptionOccurred()) env->ExceptionDescribe();
    return;
  }

  //test_env.reset(NULL);
  // Delete all global JNI references.
  THROW_IF_ERROR(JniUtil::Cleanup(), env, impala_exc_cl);
}

extern "C"
JNIEXPORT jboolean JNICALL
Java_com_cloudera_impala_service_FeSupport_NativeEvalPredicate(
    JNIEnv* env, jclass caller_class, jbyteArray thrift_predicate_bytes) {
  ObjectPool obj_pool;
  TExpr thrift_predicate;
  DeserializeThriftMsg(env, thrift_predicate_bytes, &thrift_predicate);
  Expr* e;
  Status status = Expr::CreateExprTree(&obj_pool, thrift_predicate, &e);
  if (status.ok()) {
    // TODO: codegen this as well.
    status = Expr::Prepare(e, NULL, RowDescriptor(), true);
  }
  if (!status.ok()) {
    string error_msg;
    status.GetErrorMsg(&error_msg);
    jclass internal_exc_cl =
        env->FindClass("com/cloudera/impala/common/InternalException");
    if (internal_exc_cl == NULL) {
      if (env->ExceptionOccurred()) env->ExceptionDescribe();
      return false;
    }
    env->ThrowNew(internal_exc_cl, error_msg.c_str());
    return false;
  }

  void* value = e->GetValue(NULL);
  // This can happen if a table has partitions with NULL key values.
  if (value == NULL) {
    return false;
  }
  bool* v = static_cast<bool*>(value);
  return *v;
}


namespace impala {

void InitFeSupport() {
  JNIEnv* env = getJNIEnv();
  JNINativeMethod nm;
  jclass native_backend_cl = env->FindClass("com/cloudera/impala/service/FeSupport");
  nm.name = const_cast<char*>("NativeEvalPredicate");
  nm.signature = const_cast<char*>("([B)Z");
  nm.fnPtr = reinterpret_cast<void*>(
      ::Java_com_cloudera_impala_service_FeSupport_NativeEvalPredicate);
  env->RegisterNatives(native_backend_cl, &nm, 1);
}

}
