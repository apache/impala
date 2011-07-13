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

  // setup
  jclass blocking_queue_if = env->FindClass("java.util.concurrent.BlockingQueue");
  jmethodID put_id = env->GetMethodID(blocking_queue_if, "put", "(Ljava/lang/Object;)V");

  jclass result_row_cl = env->FindClass("com.cloudera.impala.thrift.TResultRow");
  jmethodID result_row_ctor = env->GetMethodID(result_row_cl, "<init>", "()V");
  jmethodID add_to_col_vals_id =
      env->GetMethodID(result_row_cl, "addToColVals",
                       "(Lcom/cloudera/impala/thrift/TColumnValue;)V");
  jfieldID bool_val_field = env->GetFieldID(result_row_cl, "boolVal", "Z");
  jfieldID int_val_field = env->GetFieldID(result_row_cl, "intVal", "I");
  jfieldID long_val_field = env->GetFieldID(result_row_cl, "longVal", "J");
  jfieldID double_val_field = env->GetFieldID(result_row_cl, "doubleVal", "D");
  jfieldID string_val_field =
      env->GetFieldID(result_row_cl, "stringVal", "Ljava/lang/String;");

  jclass column_value_cl = env->FindClass("com.cloudera.impala.thrift.TColumnValue");
  jmethodID column_value_ctor = env->GetMethodID(column_value_cl, "<init>", "()V");

  jclass impala_exc_cl = env->FindClass("com.cloudera.impala.common.ImpalaException");

  jboolean is_copy;
  jbyte* buf = env->GetByteArrayElements(thrift_execute_plan_request, &is_copy);
  jsize len = env->GetArrayLength(thrift_execute_plan_request);
  shared_ptr<TTransport> mem_buf(new TMemoryBuffer(reinterpret_cast<uint8_t*>(buf), len));
  shared_ptr<TProtocolFactory> protocol_factory(new TBinaryProtocolFactory());
  shared_ptr<TProtocol> proto(protocol_factory->getProtocol(mem_buf));

  TExecutePlanRequest request;
  request.read(proto.get());

  ObjectPool exec_pool;
  ExecNode* plan_root;
  THROW_IF_ERROR(
      ExecNode::CreateTree(&exec_pool, request.plan, &plan_root), env, impala_exc_cl);
  DescriptorTbl* desc_tbl;
  THROW_IF_ERROR(
      DescriptorTbl::Create(&exec_pool, request.descTbl, &desc_tbl), env, impala_exc_cl);
  vector<Expr*> select_list_exprs;
  THROW_IF_ERROR(
      Expr::CreateExprTrees(&exec_pool, request.selectListExprs, &select_list_exprs),
      env, impala_exc_cl);
}

}
