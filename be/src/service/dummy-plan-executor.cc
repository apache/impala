// (c) 2011 Cloudera, Inc. All rights reserved.

#include "service/plan-executor.h"

#include <boost/shared_ptr.hpp>
#include <thrift/protocol/TBinaryProtocol.h>
#include <thrift/protocol/TProtocol.h>
#include <thrift/transport/TBufferTransports.h>

#include "common/object-pool.h"
#include "exec/exec-node.h"
#include "exprs/expr.h"
#include "runtime/descriptors.h"
#include "runtime/row-batch.h"
#include "gen-cpp/ImpalaService_types.h"
#include "gen-cpp/LocalExecutor_types.h"

using namespace boost;
using namespace std;
using namespace apache::thrift::protocol;
using namespace apache::thrift::transport;

#define THROW_IF_ERROR(stmt, env, exc_class) \
  do { \
    Status status = (stmt); \
    if (!status.ok()) { \
      string error_msg; \
      status.GetErrorMsg(&error_msg); \
      env->ThrowNew(exc_class, error_msg.c_str()); \
      return; \
    } \
  } while (false)

// TODO: check for exception, and if one is pending, extract
// message and repackage in excl_class
#define THROW_IF_NULL(expr, env, exc_class) \
  do { \
    if ((expr) == 0) { \
      env->ExceptionDescribe(); \
      env->ExceptionClear(); \
      env->ThrowNew(exc_class, "Internal error"); \
      return; \
    } \
  } while (false)

namespace impala {

static void ThrowJavaExc(JNIEnv* env, jclass exc_class, const Status& status) {
  string error_msg;
  status.GetErrorMsg(&error_msg);
  env->ThrowNew(exc_class, error_msg.c_str());
}

extern "C"
JNIEXPORT void JNICALL Java_com_cloudera_impala_service_NativePlanExecutor_ExecPlan(
    JNIEnv* env, jclass caller_class, jbyteArray thrift_execute_plan_request,
    jboolean as_ascii, jobject result_queue) {

  // sanity check
  if (env == NULL) {
    // TODO: log something here; throw something else
    return;
  }
  // setup
  jclass impala_exc_cl = env->FindClass("com/cloudera/impala/common/ImpalaException");
  if (impala_exc_cl == NULL) {
    // we'd like to throw an exception here, but we can't
    return;
  }

  jclass blocking_queue_if = env->FindClass("java/util/concurrent/BlockingQueue");
  THROW_IF_NULL(blocking_queue_if, env, impala_exc_cl);
  jmethodID put_id = env->GetMethodID(blocking_queue_if, "put", "(Ljava/lang/Object;)V");
  THROW_IF_NULL(put_id, env, impala_exc_cl);

  jclass result_row_cl = env->FindClass("com/cloudera/impala/thrift/TResultRow");
  THROW_IF_NULL(result_row_cl, env, impala_exc_cl);
  jmethodID result_row_ctor = env->GetMethodID(result_row_cl, "<init>", "()V");
  THROW_IF_NULL(result_row_ctor, env, impala_exc_cl);
  jmethodID add_to_col_vals_id =
      env->GetMethodID(result_row_cl, "addToColVals",
                       "(Lcom/cloudera/impala/thrift/TColumnValue;)V");
  THROW_IF_NULL(add_to_col_vals_id, env, impala_exc_cl);

  jclass column_value_cl = env->FindClass("com/cloudera/impala/thrift/TColumnValue");
  THROW_IF_NULL(column_value_cl, env, impala_exc_cl);
  jmethodID column_value_ctor = env->GetMethodID(column_value_cl, "<init>", "()V");
  THROW_IF_NULL(column_value_ctor, env, impala_exc_cl);
  jfieldID bool_val_field = env->GetFieldID(column_value_cl, "boolVal", "Z");
  THROW_IF_NULL(bool_val_field, env, impala_exc_cl);
  jfieldID int_val_field = env->GetFieldID(column_value_cl, "intVal", "I");
  THROW_IF_NULL(int_val_field, env, impala_exc_cl);
  jfieldID long_val_field = env->GetFieldID(column_value_cl, "longVal", "J");
  THROW_IF_NULL(long_val_field, env, impala_exc_cl);
  jfieldID double_val_field = env->GetFieldID(column_value_cl, "doubleVal", "D");
  THROW_IF_NULL(double_val_field, env, impala_exc_cl);
  jfieldID string_val_field =
      env->GetFieldID(column_value_cl, "stringVal", "Ljava/lang/String;");
  THROW_IF_NULL(string_val_field, env, impala_exc_cl);

  jboolean is_copy;
  jbyte* buf = env->GetByteArrayElements(thrift_execute_plan_request, &is_copy);
  jsize len = env->GetArrayLength(thrift_execute_plan_request);
  shared_ptr<TTransport> mem_buf(new TMemoryBuffer(reinterpret_cast<uint8_t*>(buf), len));
  shared_ptr<TProtocolFactory> protocol_factory(new TBinaryProtocolFactory());
  shared_ptr<TProtocol> proto(protocol_factory->getProtocol(mem_buf));

  TExecutePlanRequest request;
  request.read(proto.get());

  ObjectPool exec_pool;
  DescriptorTbl* desc_tbl;
  THROW_IF_ERROR(
      DescriptorTbl::Create(&exec_pool, request.descTbl, &desc_tbl), env, impala_exc_cl);
  vector<Expr*> select_list_exprs;
  THROW_IF_ERROR(
      Expr::CreateExprTrees(&exec_pool, request.selectListExprs, &select_list_exprs),
      env, impala_exc_cl);
  ExecNode* plan_root;
  THROW_IF_ERROR(
      ExecNode::CreateTree(&exec_pool, request.plan, &plan_root), env, impala_exc_cl);
  
  // release w/o copying anything back
  env->ReleaseByteArrayElements(thrift_execute_plan_request, buf, JNI_ABORT);
}

}
