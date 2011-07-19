// (c) 2011 Cloudera, Inc. All rights reserved.

#include "service/plan-executor.h"

#include <Thrift.h>
#include <protocol/TBinaryProtocol.h>
#include <transport/TBufferTransports.h>
#include <boost/scoped_ptr.hpp>
#include "common/object-pool.h"
#include "exec/exec-node.h"
#include "exprs/expr.h"
#include "runtime/descriptors.h"
#include "runtime/row-batch.h"
#include "gen-cpp/ImpalaService_types.h"
#include "gen-cpp/LocalExecutor_types.h"

using namespace std;
using namespace boost;
using namespace apache::thrift::protocol;
using namespace apache::thrift::transport;

#define THROW_IF_ERROR(stmt, env, exc_class) \
  do { \
    Status status = (stmt); \
    if (!status.ok()) { \
      string error_msg; \
      status.GetErrorMsg(&error_msg); \
      (env)->ThrowNew((exc_class), error_msg.c_str()); \
      return; \
    } \
  } while (false)

#define THROW_IF_EXC(env, exc_class, throwable_to_string_id) \
  do { \
    jthrowable exc = (env)->ExceptionOccurred(); \
    if (exc != NULL) { \
     jstring msg = (jstring) env->CallObjectMethod(exc, (throwable_to_string_id)); \
     jboolean is_copy; \
     const char* c_msg = reinterpret_cast<const char*>((env)->GetStringUTFChars(msg, &is_copy)); \
     (env)->ExceptionClear(); \
     (env)->ThrowNew((exc_class), c_msg); \
     return; \
    } \
  } while (false)

namespace impala {

PlanExecutor::PlanExecutor(ExecNode* plan, const DescriptorTbl& descs)
  : plan_(plan),
    tuple_descs_(),
    runtime_state_(descs),
    done_(false) {
  // stash these, we need to pass them into the RowBatch c'tor
  descs.GetTupleDescs(&tuple_descs_);
}

PlanExecutor::~PlanExecutor() {
}

Status PlanExecutor::Exec() {
  RETURN_IF_ERROR(plan_->Prepare(&runtime_state_));
  RETURN_IF_ERROR(plan_->Open(&runtime_state_));
  return Status::OK;
}

Status PlanExecutor::FetchResult(RowBatch** batch) {
  if (done_) {
    *batch = NULL;
    return Status::OK;
  }
  *batch = new RowBatch(tuple_descs_, runtime_state_.batch_size());
  RETURN_IF_ERROR(plan_->GetNext(&runtime_state_, *batch));
  return Status::OK;
}

static void ThrowJavaExc(JNIEnv* env, jclass exc_class, const Status& status) {
  string error_msg;
  status.GetErrorMsg(&error_msg);
  env->ThrowNew(exc_class, error_msg.c_str());
}

Status DeserializeRequest(
    JNIEnv* env,
    jbyteArray execute_plan_request_bytes,
    ObjectPool* obj_pool,
    ExecNode** plan,
    DescriptorTbl** descs,
    vector<Expr*>* select_list_exprs) {

  // TODO: Find out why using plan_buf directly does not work.
  // Copy java byte array into native byte array.
  jboolean is_copy = false;
  int plan_buf_size = env->GetArrayLength(execute_plan_request_bytes);
  jbyte* plan_buf = env->GetByteArrayElements(execute_plan_request_bytes, &is_copy);
  uint8_t native_plan_bytes[plan_buf_size];
  for (int i = 0; i < plan_buf_size; i++) {
    native_plan_bytes[i] = plan_buf[i];
  }

  // Deserialize plan bytes into c++ plan request using memory transport.
  TExecutePlanRequest exec_request;
  boost::shared_ptr<TTransport> tmem_transport(new TMemoryBuffer(native_plan_bytes, plan_buf_size));
  TBinaryProtocolFactoryT<TMemoryBuffer> tproto_factory;
  boost::shared_ptr<TProtocol> tproto = tproto_factory.getProtocol(tmem_transport);
  exec_request.read(tproto.get());

  RETURN_IF_ERROR(ExecNode::CreateTree(obj_pool, exec_request.plan, plan));
  RETURN_IF_ERROR(DescriptorTbl::Create(obj_pool, exec_request.descTbl, descs));
  RETURN_IF_ERROR(
      Expr::CreateExprTrees(obj_pool, exec_request.selectListExprs, select_list_exprs));

  return Status::OK;
}

extern "C"
JNIEXPORT void JNICALL Java_com_cloudera_impala_service_NativePlanExecutor_ExecPlan(
    JNIEnv* env, jclass caller_class, jbyteArray thrift_execute_plan_request,
    jboolean as_ascii, jobject result_queue) {

  ExecNode* plan;
  DescriptorTbl* descs;
  vector<Expr*> select_list_exprs;
  ObjectPool obj_pool;
  DeserializeRequest(env, thrift_execute_plan_request, &obj_pool, &plan, &descs, &select_list_exprs);

  // setup
  jclass impala_exc_cl = env->FindClass("com/cloudera/impala/common/ImpalaException");
  if (impala_exc_cl == NULL) {
    if (env->ExceptionOccurred()) env->ExceptionDescribe();
    return;
  }
  jclass throwable_cl = env->FindClass("java/lang/Throwable");
  if (impala_exc_cl == NULL) {
    if (env->ExceptionOccurred()) env->ExceptionDescribe();
    return;
  }
  jmethodID throwable_to_string_id = env->GetMethodID(throwable_cl, "toString", "()Ljava/lang/String;");
  if (throwable_to_string_id == NULL) {
    if (env->ExceptionOccurred()) env->ExceptionDescribe();
    return;
  }

  THROW_IF_EXC(env, impala_exc_cl, throwable_to_string_id);
  jclass blocking_queue_if = env->FindClass("java/util/concurrent/BlockingQueue");
  THROW_IF_EXC(env, impala_exc_cl, throwable_to_string_id);
  jmethodID put_id = env->GetMethodID(blocking_queue_if, "put", "(Ljava/lang/Object;)V");
  THROW_IF_EXC(env, impala_exc_cl, throwable_to_string_id);

  jclass result_row_cl = env->FindClass("com/cloudera/impala/thrift/TResultRow");
  THROW_IF_EXC(env, impala_exc_cl, throwable_to_string_id);
  jmethodID result_row_ctor = env->GetMethodID(result_row_cl, "<init>", "()V");
  THROW_IF_EXC(env, impala_exc_cl, throwable_to_string_id);
  jmethodID add_to_col_vals_id =
      env->GetMethodID(result_row_cl, "addToColVals",
                       "(Lcom/cloudera/impala/thrift/TColumnValue;)V");
  THROW_IF_EXC(env, impala_exc_cl, throwable_to_string_id);

  jclass column_value_cl = env->FindClass("com/cloudera/impala/thrift/TColumnValue");
  THROW_IF_EXC(env, impala_exc_cl, throwable_to_string_id);
  jmethodID column_value_ctor = env->GetMethodID(column_value_cl, "<init>", "()V");
  THROW_IF_EXC(env, impala_exc_cl, throwable_to_string_id);
  jfieldID bool_val_field = env->GetFieldID(column_value_cl, "boolVal", "Z");
  THROW_IF_EXC(env, impala_exc_cl, throwable_to_string_id);
  jfieldID int_val_field = env->GetFieldID(column_value_cl, "intVal", "I");
  THROW_IF_EXC(env, impala_exc_cl, throwable_to_string_id);
  jfieldID long_val_field = env->GetFieldID(column_value_cl, "longVal", "J");
  THROW_IF_EXC(env, impala_exc_cl, throwable_to_string_id);
  jfieldID double_val_field = env->GetFieldID(column_value_cl, "doubleVal", "D");
  THROW_IF_EXC(env, impala_exc_cl, throwable_to_string_id);
  jfieldID string_val_field = env->GetFieldID(column_value_cl, "stringVal", "Ljava/lang/String;");
  THROW_IF_EXC(env, impala_exc_cl, throwable_to_string_id);

  Status status;
  PlanExecutor executor(plan, *descs);

  // Prepare select list expressions.
  for (int i = 0; i < select_list_exprs.size(); ++i) {
    select_list_exprs[i]->Prepare(executor.runtime_state());
  }

  THROW_IF_ERROR(executor.Exec(), env, impala_exc_cl);
  scoped_ptr<RowBatch> batch;
  while (true) {
    RowBatch* batch_ptr;
    THROW_IF_ERROR(executor.FetchResult(&batch_ptr), env, impala_exc_cl);
    batch.reset(batch_ptr);

    // convert batch of rows to TResultRow
    for (int i = 0; i < batch->num_rows(); ++i) {
      TupleRow* row = batch->GetRow(i);
      jobject result_row = env->NewObject(result_row_cl, result_row_ctor);
      THROW_IF_EXC(env, impala_exc_cl, throwable_to_string_id);
      for (int j = 0; j < select_list_exprs.size(); ++j) {
        TColumnValue col_val;
        select_list_exprs[j]->GetValue(row, as_ascii, &col_val);
        jobject java_col_val = env->NewObject(column_value_cl, column_value_ctor);
        THROW_IF_EXC(env, impala_exc_cl, throwable_to_string_id);
        if (col_val.__isset.boolVal) {
          env->SetBooleanField(java_col_val, bool_val_field, col_val.boolVal);
          THROW_IF_EXC(env, impala_exc_cl, throwable_to_string_id);
        } else if (col_val.__isset.intVal) {
          env->SetIntField(java_col_val, int_val_field, col_val.intVal);
          THROW_IF_EXC(env, impala_exc_cl, throwable_to_string_id);
        } else if (col_val.__isset.longVal) {
          env->SetLongField(java_col_val, long_val_field, col_val.longVal);
          THROW_IF_EXC(env, impala_exc_cl, throwable_to_string_id);
        } else if (col_val.__isset.doubleVal) {
          env->SetDoubleField(java_col_val, double_val_field, col_val.doubleVal);
          THROW_IF_EXC(env, impala_exc_cl, throwable_to_string_id);
        } else if (col_val.__isset.stringVal) {
          env->SetObjectField(
              java_col_val, string_val_field, env->NewStringUTF(col_val.stringVal.c_str()));
          THROW_IF_EXC(env, impala_exc_cl, throwable_to_string_id);
        }

        env->CallVoidMethod(result_row, add_to_col_vals_id, java_col_val);
        THROW_IF_EXC(env, impala_exc_cl, throwable_to_string_id);
      }

      // place row on result_queue
      env->CallVoidMethod(result_queue, put_id, result_row);
      THROW_IF_EXC(env, impala_exc_cl, throwable_to_string_id);
    }
    if (batch->num_rows() < batch->capacity()) {
      break;
    }
  }
}

}
