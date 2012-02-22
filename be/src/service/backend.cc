// (c) 2011 Cloudera, Inc. All rights reserved.

#include "service/backend.h"

#include <boost/scoped_ptr.hpp>
#include <glog/logging.h>
#include <gflags/gflags.h>
#include <server/TServer.h>

#include "common/status.h"
#include "exec/exec-node.h"
#include "exec/hbase-table-scanner.h"
#include "exprs/expr.h"
#include "runtime/row-batch.h"
#include "runtime/runtime-state.h"
#include "runtime/data-stream-mgr.h"
#include "runtime/hdfs-fs-cache.h"
#include "runtime/client-cache.h"
#include "runtime/simple-scheduler.h"
#include "testutil/test-exec-env.h"
#include "service/jni-coordinator.h"
#include "service/backend-service.h"
#include "util/jni-util.h"
#include "util/debug-util.h"
#include "gen-cpp/ImpalaPlanService_types.h"
#include "gen-cpp/Data_types.h"

DECLARE_bool(serialize_batch);
DEFINE_int32(backend_port, 21000, "port on which ImpalaBackendService is exported");

using namespace impala;
using namespace std;
using namespace boost;
using namespace apache::thrift::server;

static scoped_ptr<TestExecEnv> test_env;

static void RunServer(TServer* server) {
  VLOG(1) << "started backend server thread";
  server->serve();
}

extern "C"
JNIEXPORT jint JNICALL JNI_OnLoad(JavaVM* vm, void* pvt) {
  google::InitGoogleLogging("impala-backend");
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
  THROW_IF_ERROR_RET(RuntimeState::InitHBaseConf(), env, impala_exc_cl, -1);
  THROW_IF_ERROR_RET(JniCoordinator::Init(), env, impala_exc_cl, -1);

  // start backends in process, listening on ports > backend_port
  VLOG(1) << "creating test env";
  test_env.reset(new TestExecEnv(2, FLAGS_backend_port + 1));
  VLOG(1) << "starting backends";
  test_env->StartBackends();

  // start one backend service for the coordinator on backend_port
  TServer* server = StartImpalaBackendService(test_env.get(), FLAGS_backend_port);
  thread server_thread = thread(&RunServer, server);

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

  // Delete all global JNI references.
  THROW_IF_ERROR(JniUtil::Cleanup(), env, impala_exc_cl);
}

extern "C"
JNIEXPORT void JNICALL Java_com_cloudera_impala_service_NativeBackend_ExecQuery(
    JNIEnv* env, jclass caller_class, jbyteArray thrift_query_exec_request,
    jobject error_log, jobject file_errors, jobject result_queue) {
  JniCoordinator coord(env, test_env.get(), error_log, file_errors, result_queue);
  coord.Exec(thrift_query_exec_request);
  RETURN_IF_EXC(env);
  const vector<Expr*>& select_list_exprs = coord.select_list_exprs();

  // Prepare select list expressions.
  for (size_t i = 0; i < select_list_exprs.size(); ++i) {
    select_list_exprs[i]->Prepare(coord.runtime_state(), coord.row_desc());
  }

  if (coord.is_constant_query()) {
    // no FROM clause: the select list only contains constant exprs
    coord.AddResultRow(NULL);
    RETURN_IF_EXC(env);
    return;
  }

  // TODO: turn this into a flag in the TQueryExecRequest
  // FLAGS_serialize_batch = true;
  while (true) {
    RowBatch* batch;
    THROW_IF_ERROR_WITH_LOGGING(coord.GetNext(&batch), env, &coord);
    if (batch == NULL) break;
    LOG(INFO) << "#rows=" << batch->num_rows();
    for (int i = 0; i < batch->num_rows(); ++i) {
      TupleRow* row = batch->GetRow(i);
      LOG(INFO) << PrintRow(row, coord.row_desc());
      coord.AddResultRow(row);
      RETURN_IF_EXC(env);
    }
  }

  // Report error log and file error stats.
  coord.WriteErrorLog();
  coord.WriteFileErrors();
}

extern "C"
JNIEXPORT jboolean JNICALL Java_com_cloudera_impala_service_NativeBackend_EvalPredicate(
    JNIEnv* env, jclass caller_class, jbyteArray thrift_predicate_bytes) {
  ObjectPool obj_pool;
  TExpr thrift_predicate;
  DeserializeThriftMsg(env, thrift_predicate_bytes, &thrift_predicate);
  Expr* e;
  Status status = Expr::CreateExprTree(&obj_pool, thrift_predicate, &e);
  if (!status.ok()) {
    string error_msg;
    status.GetErrorMsg(&error_msg);
    jclass internal_exc_cl = env->FindClass("com/cloudera/impala/common/InternalException");
    if (internal_exc_cl == NULL) {
      if (env->ExceptionOccurred()) env->ExceptionDescribe();
      return false;
    }
    env->ThrowNew(internal_exc_cl, error_msg.c_str());
    return false;
  }

  e->Prepare(NULL, RowDescriptor());
  bool* v = static_cast<bool*>(e->GetValue(NULL));
  return *v;
}
