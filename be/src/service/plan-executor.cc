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

PlanExecutor::PlanExecutor(ExecNode* plan, const DescriptorTbl& descs)
  : plan_(plan),
    descs_(descs),
    tuple_descs_(),
    runtime_state_(),
    batch_size_(DEFAULT_BATCH_SIZE),
    done_(false) {
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
  *batch = new RowBatch(tuple_descs_, batch_size_);
  RETURN_IF_ERROR(plan_->GetNext(&runtime_state_, *batch));
  return Status::OK;
}

extern "C"
JNIEXPORT void JNICALL Java_com_cloudera_impala_service_NativePlanExecutor_ExecPlan(
    JNIEnv* env, jclass caller_class, jbyteArray thrift_execute_plan_request,
    jboolean as_ascii, jobject result_queue) {
  // TODO: use boost::scoped_ptr
  // deserialize plan and desc tbl
  TExecutePlanRequest exec_request;
  ExecNode* plan;
  DescriptorTbl* descs;
  vector<Expr*> select_list_exprs;
  vector<PrimitiveType> select_list_expr_types;

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

  PlanExecutor executor(plan, *descs);

  Status status;
  if (!(status = executor.Exec()).ok()) {
    string error_msg;
    status.GetErrorMsg(&error_msg);
    env->ThrowNew(impala_exc_cl, error_msg.c_str());
    return;
  }

  while (true) {
    RowBatch* batch;
    if (!(status = executor.FetchResult(&batch)).ok()) {
      string error_msg;
      status.GetErrorMsg(&error_msg);
      env->ThrowNew(impala_exc_cl, error_msg.c_str());
      return;
    }
    if (batch == NULL) {
      return;
    }

    // convert batch of rows to TResultRow
    for (int i = 0; i < batch->num_rows(); ++i) {
      TupleRow* row = batch->GetRow(i);
      jobject result_row = env->NewObject(result_row_cl, result_row_ctor);
      for (int j = 0; j < select_list_exprs.size(); ++j) {
        TColumnValue col_val;
        if (as_ascii) {
          select_list_exprs[j]->PrintValue(row, select_list_expr_types[j], &col_val.stringVal);
        } else {
          select_list_exprs[j]->GetValue(row, select_list_expr_types[j], &col_val);
        }

        jobject java_col_val = env->NewObject(column_value_cl, column_value_ctor);
        /* TODO: figure out how to check what's been set
        if (col_val.isSetBoolVal()) {
          env->SetBooleanField(java_col_val, bool_val_field, col_val.boolVal);
        } else if (col_val.isSetIntVal()) {
          env->SetIntField(java_col_val, int_val_field, col_val.intVal);
        } else if (col_val.isSetLongVal()) {
          env->SetLongField(java_col_val, long_val_field, col_val.longVal);
        } else if (col_val.isSetDoubleVal()) {
          env->SetDoubleField(java_col_val, double_val_field, col_val.doubleVal);
        } else if (col_val.isSetStringVal()) {
          env->SetObjectField(
              java_col_val, string_val_field, env->NewStringUTF(col_val.stringVal.c_str()));
        } else {
          // TODO: signal error
        }
        */

        env->CallVoidMethod(result_row, add_to_col_vals_id, java_col_val);
      }

      // place row on result_queue
      env->CallVoidMethod(result_queue, put_id, result_row);
    }
  }
}

}
