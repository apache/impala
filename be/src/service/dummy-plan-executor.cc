// (c) 2011 Cloudera, Inc. All rights reserved.

#include "service/plan-executor.h"

#include "exec/exec-node.h"
#include "exprs/expr.h"
#include "runtime/descriptors.h"
#include "runtime/row-batch.h"
#include "gen-cpp/ImpalaService_types.h"
#include "gen-cpp/LocalExecutor_types.h"

using namespace std;

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
}

}
