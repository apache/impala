// (c) 2011 Cloudera, Inc. All rights reserved.

#include "service/plan-executor-adaptor.h"
#include "exec/exec-node.h"
#include "exec/scan-node.h"
#include "exprs/expr.h"
#include "runtime/descriptors.h"
#include "util/jni-util.h"
#include "gen-cpp/ImpalaService_types.h"
#include "gen-cpp/ImpalaPlanService_types.h"

using namespace std;

namespace impala {

Status PlanExecutorAdaptor::DeserializeRequest() {
  descs_ = NULL;
  plan_ = NULL;
  // Deserialize request bytes into c++ request using memory transport.
  DeserializeThriftMsg(env_, thrift_query_exec_request_, &query_exec_request_);

  batch_size_ = query_exec_request_.batchSize;
  abort_on_error_ = query_exec_request_.abortOnError;
  max_errors_ = query_exec_request_.maxErrors;
  as_ascii_ = query_exec_request_.asAscii;

  if (query_exec_request_.fragmentRequests.size() == 0) {
    return Status("query exec request contains no plan fragments");
  }
  const TPlanExecRequest& plan_exec_request = query_exec_request_.fragmentRequests[0];
  if (plan_exec_request.__isset.descTbl) {
    RETURN_IF_ERROR(DescriptorTbl::Create(&obj_pool_, plan_exec_request.descTbl, &descs_));
  }
  if (plan_exec_request.__isset.planFragment) {
    RETURN_IF_ERROR(
        ExecNode::CreateTree(
            &obj_pool_, plan_exec_request.planFragment, *descs_, &plan_));

    // set scan ranges
    vector<ExecNode*> scan_nodes;
    plan_->CollectScanNodes(&scan_nodes);
    DCHECK_GT(query_exec_request_.nodeRequestParams.size(), 0);
    // the first nodeRequestParams list contains exactly one TPlanExecParams
    // (it's meant for the coordinator fragment)
    DCHECK_EQ(query_exec_request_.nodeRequestParams[0].size(), 1);
    vector<TScanRange>& local_scan_ranges =
        query_exec_request_.nodeRequestParams[0][0].scanRanges;
    for (int i = 0; i < scan_nodes.size(); ++i) {
      for (int j = 0; j < local_scan_ranges.size(); ++j) {
        if (scan_nodes[i]->id() == local_scan_ranges[j].nodeId) {
           static_cast<ScanNode*>(scan_nodes[i])->SetScanRange(local_scan_ranges[j]);
        }
      }
    }
  }

  if ((descs_ == NULL) != (plan_ == NULL)) {
    return Status("bad TPlanExecRequest: only one of {plan_fragment, desc_tbl} is set");
  }
  RETURN_IF_ERROR(
      Expr::CreateExprTrees(&obj_pool_, plan_exec_request.outputExprs,
                            &select_list_exprs_));
  return Status::OK;
}

void PlanExecutorAdaptor::Exec() {
  THROW_IF_ERROR(DeserializeRequest(), env_, impala_exc_cl_);
  if (plan_ == NULL) return;
  executor_.reset(new PlanExecutor(plan_, *descs_, abort_on_error_, max_errors_));
  if (batch_size_ != 0) {
    executor_->runtime_state()->set_batch_size(batch_size_);
  }
  THROW_IF_ERROR(executor_->Exec(), env_, impala_exc_cl_);
}

void PlanExecutorAdaptor::AddResultRow(TupleRow* row) {
  jobject result_row = env_->NewObject(result_row_cl_, result_row_ctor_);
  THROW_IF_EXC(env_, impala_exc_cl_, throwable_to_string_id_);
  for (size_t j = 0; j < select_list_exprs_.size(); ++j) {
    TColumnValue col_val;
    select_list_exprs_[j]->GetValue(row, as_ascii_, &col_val);
    jobject java_col_val = env_->NewObject(column_value_cl_, column_value_ctor_);
    THROW_IF_EXC(env_, impala_exc_cl_, throwable_to_string_id_);
    if (col_val.__isset.boolVal) {
      env_->SetBooleanField(java_col_val, bool_val_field_, col_val.boolVal);
      THROW_IF_EXC(env_, impala_exc_cl_, throwable_to_string_id_);
    } else if (col_val.__isset.intVal) {
      env_->SetIntField(java_col_val, int_val_field_, col_val.intVal);
      THROW_IF_EXC(env_, impala_exc_cl_, throwable_to_string_id_);
    } else if (col_val.__isset.longVal) {
      env_->SetLongField(java_col_val, long_val_field_, col_val.longVal);
      THROW_IF_EXC(env_, impala_exc_cl_, throwable_to_string_id_);
    } else if (col_val.__isset.doubleVal) {
      env_->SetDoubleField(java_col_val, double_val_field_, col_val.doubleVal);
      THROW_IF_EXC(env_, impala_exc_cl_, throwable_to_string_id_);
    } else if (col_val.__isset.stringVal) {
      env_->SetObjectField(java_col_val, string_val_field_,
          env_->NewStringUTF(col_val.stringVal.c_str()));
      THROW_IF_EXC(env_, impala_exc_cl_, throwable_to_string_id_);
    }

    env_->CallVoidMethod(result_row, add_to_col_vals_id_, java_col_val);
    THROW_IF_EXC(env_, impala_exc_cl_, throwable_to_string_id_);
  }

  // place row on result_queue
  env_->CallVoidMethod(result_queue_, put_id_, result_row);
  THROW_IF_EXC(env_, impala_exc_cl_, throwable_to_string_id_);
}

void PlanExecutorAdaptor::WriteErrorLog() {
  const vector<string>& runtime_error_log = executor_->runtime_state()->error_log();
  for (int i = 0; i < runtime_error_log.size(); ++i) {
    env_->CallObjectMethod(error_log_, list_add_id_,
        env_->NewStringUTF(runtime_error_log[i].c_str()));
  }
}

void PlanExecutorAdaptor::WriteFileErrors() {
  const vector<pair<string, int> >& runtime_file_errors =
      executor_->runtime_state()->file_errors();
  for (int i = 0; i < runtime_file_errors.size(); ++i) {
    env_->CallObjectMethod(file_errors_, map_put_id_,
        env_->NewStringUTF(runtime_file_errors[i].first.c_str()),
        env_->NewObject(integer_cl_, integer_ctor_, runtime_file_errors[i].second));
  }
}

jclass PlanExecutorAdaptor::impala_exc_cl_ = NULL;
jclass PlanExecutorAdaptor::throwable_cl_ = NULL;
jclass PlanExecutorAdaptor::blocking_queue_if_ = NULL;
jclass PlanExecutorAdaptor::result_row_cl_ = NULL;
jclass PlanExecutorAdaptor::column_value_cl_ = NULL;
jclass PlanExecutorAdaptor::list_cl_ = NULL;
jclass PlanExecutorAdaptor::map_cl_ = NULL;
jclass PlanExecutorAdaptor::integer_cl_ = NULL;
jmethodID PlanExecutorAdaptor::throwable_to_string_id_ = NULL;
jmethodID PlanExecutorAdaptor::put_id_ = NULL;
jmethodID PlanExecutorAdaptor::result_row_ctor_ = NULL;
jmethodID PlanExecutorAdaptor::add_to_col_vals_id_ = NULL;
jmethodID PlanExecutorAdaptor::column_value_ctor_ = NULL;
jmethodID PlanExecutorAdaptor::list_add_id_ = NULL;
jmethodID PlanExecutorAdaptor::map_put_id_ = NULL;
jmethodID PlanExecutorAdaptor::integer_ctor_ = NULL;
jfieldID PlanExecutorAdaptor::bool_val_field_ = NULL;
jfieldID PlanExecutorAdaptor::int_val_field_ = NULL;
jfieldID PlanExecutorAdaptor::long_val_field_ = NULL;
jfieldID PlanExecutorAdaptor::double_val_field_ = NULL;
jfieldID PlanExecutorAdaptor::string_val_field_ = NULL;

Status PlanExecutorAdaptor::Init() {
  JNIEnv* env = getJNIEnv();
  if (env == NULL) {
    return Status("Failed to get/create JVM");
  }
  // Global class references.
  RETURN_IF_ERROR(
        JniUtil::GetGlobalClassRef(env, "com/cloudera/impala/common/ImpalaException",
            &impala_exc_cl_));
  RETURN_IF_ERROR(
          JniUtil::GetGlobalClassRef(env, "java/util/concurrent/BlockingQueue",
              &blocking_queue_if_));
  RETURN_IF_ERROR(
          JniUtil::GetGlobalClassRef(env, "com/cloudera/impala/thrift/TResultRow",
              &result_row_cl_));
  RETURN_IF_ERROR(
            JniUtil::GetGlobalClassRef(env, "com/cloudera/impala/thrift/TColumnValue",
                &column_value_cl_));
  RETURN_IF_ERROR(JniUtil::GetGlobalClassRef(env, "java/util/List", &list_cl_));
  RETURN_IF_ERROR(JniUtil::GetGlobalClassRef(env, "java/util/Map", &map_cl_));
  RETURN_IF_ERROR(JniUtil::GetGlobalClassRef(env, "java/lang/Integer", &integer_cl_));

  // BlockingQueue method ids.
  put_id_ = env->GetMethodID(blocking_queue_if_, "put", "(Ljava/lang/Object;)V");
  RETURN_ERROR_IF_EXC(env, JniUtil::throwable_to_string_id());

  // TResultRow method ids.
  result_row_ctor_ = env->GetMethodID(result_row_cl_, "<init>", "()V");
  RETURN_ERROR_IF_EXC(env, JniUtil::throwable_to_string_id());
  add_to_col_vals_id_ =
      env->GetMethodID(result_row_cl_, "addToColVals",
                       "(Lcom/cloudera/impala/thrift/TColumnValue;)V");
  RETURN_ERROR_IF_EXC(env, JniUtil::throwable_to_string_id());

  // TColumnValue method ids and fields.
  column_value_ctor_ = env->GetMethodID(column_value_cl_, "<init>", "()V");
  RETURN_ERROR_IF_EXC(env, JniUtil::throwable_to_string_id());
  bool_val_field_ = env->GetFieldID(column_value_cl_, "boolVal", "Z");
  RETURN_ERROR_IF_EXC(env, JniUtil::throwable_to_string_id());
  int_val_field_ = env->GetFieldID(column_value_cl_, "intVal", "I");
  RETURN_ERROR_IF_EXC(env, JniUtil::throwable_to_string_id());
  long_val_field_ = env->GetFieldID(column_value_cl_, "longVal", "J");
  RETURN_ERROR_IF_EXC(env, JniUtil::throwable_to_string_id());
  double_val_field_ = env->GetFieldID(column_value_cl_, "doubleVal", "D");
  RETURN_ERROR_IF_EXC(env, JniUtil::throwable_to_string_id());
  string_val_field_ = env->GetFieldID(column_value_cl_, "stringVal", "Ljava/lang/String;");
  RETURN_ERROR_IF_EXC(env, JniUtil::throwable_to_string_id());

  // List method ids.
  list_add_id_ = env->GetMethodID(list_cl_, "add", "(Ljava/lang/Object;)Z");
  RETURN_ERROR_IF_EXC(env, JniUtil::throwable_to_string_id());

  // Map method ids.
  map_put_id_ = env->GetMethodID(map_cl_, "put",
      "(Ljava/lang/Object;Ljava/lang/Object;)Ljava/lang/Object;");
  RETURN_ERROR_IF_EXC(env, JniUtil::throwable_to_string_id());

  // Integer method ids.
  integer_ctor_ = env->GetMethodID(integer_cl_, "<init>", "(I)V");
  RETURN_ERROR_IF_EXC(env, JniUtil::throwable_to_string_id());

  return Status::OK;
}

}
