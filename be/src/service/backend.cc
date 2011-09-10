// (c) 2011 Cloudera, Inc. All rights reserved.

#include "service/backend.h"

#include <boost/scoped_ptr.hpp>
#include <glog/logging.h>

#include "common/status.h"
#include "exec/exec-node.h"
#include "exec/hbase-table-scanner.h"
#include "exprs/expr.h"
#include "runtime/row-batch.h"
#include "runtime/runtime-state.h"
#include "service/plan-executor-adaptor.h"
#include "util/jni-util.h"
#include "gen-cpp/ImpalaPlanService_types.h"

using namespace impala;
using namespace std;
using namespace boost;

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
  THROW_IF_ERROR_RET(JniUtil::Init(), env, impala_exc_cl, -1);
  THROW_IF_ERROR_RET(HBaseTableScanner::Init(), env, impala_exc_cl, -1);
  THROW_IF_ERROR_RET(RuntimeState::InitHBaseConf(), env, impala_exc_cl, -1);
  THROW_IF_ERROR_RET(PlanExecutorAdaptor::Init(), env, impala_exc_cl, -1);
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
JNIEXPORT void JNICALL Java_com_cloudera_impala_service_NativeBackend_ExecPlan(
    JNIEnv* env, jclass caller_class, jbyteArray thrift_execute_plan_request,
    jboolean abort_on_error, jint max_errors, jobject error_log, jobject file_errors,
    jboolean as_ascii, jobject result_queue) {

  PlanExecutorAdaptor adaptor(env, thrift_execute_plan_request, abort_on_error, max_errors,
      error_log, file_errors, as_ascii, result_queue);
  adaptor.Exec();
  RETURN_IF_EXC(env);
  const vector<Expr*>& select_list_exprs = adaptor.select_list_exprs();
  PlanExecutor* executor = adaptor.executor();

  // Prepare select list expressions.
  for (size_t i = 0; i < select_list_exprs.size(); ++i) {
    select_list_exprs[i]->Prepare(executor->runtime_state(), adaptor.plan()->row_desc());
  }

  if (adaptor.plan() == NULL) {
    // no FROM clause: the select list only contains constant exprs
    // TODO: check this somewhere
    adaptor.AddResultRow(NULL);
    RETURN_IF_EXC(env);
    return;
  }

  scoped_ptr<RowBatch> batch;
  while (true) {
    RowBatch* batch_ptr;
    THROW_IF_ERROR_WITH_LOGGING(executor->FetchResult(&batch_ptr), env, &adaptor);
    batch.reset(batch_ptr);
    for (int i = 0; i < batch->num_rows(); ++i) {
      TupleRow* row = batch->GetRow(i);
      adaptor.AddResultRow(row);
      RETURN_IF_EXC(env);
    }
    if (batch->num_rows() < batch->capacity()) {
      break;
    }
  }

  // Report error log and file error stats.
  adaptor.WriteErrorLog();
  adaptor.WriteFileErrors();
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
