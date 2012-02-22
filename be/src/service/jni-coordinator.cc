// (c) 2011 Cloudera, Inc. All rights reserved.

#include "service/jni-coordinator.h"

#include <glog/logging.h>
#include <gflags/gflags.h>

#include "exec/exec-node.h"
#include "exec/scan-node.h"
#include "exprs/expr.h"
#include "runtime/descriptors.h"
#include "runtime/runtime-state.h"
#include "runtime/coordinator.h"
#include "util/jni-util.h"
#include "gen-cpp/ImpalaService_types.h"
#include "gen-cpp/ImpalaPlanService_types.h"

using namespace std;

DECLARE_int32(backend_port);

namespace impala {

JniCoordinator::JniCoordinator(
    JNIEnv* env, ExecEnv* exec_env, jobject error_log, jobject file_errors,
    jobject result_queue)
  : env_(env),
    error_log_(error_log),
    file_errors_(file_errors),
    result_queue_(result_queue),
    coord_(new Coordinator(exec_env)) {
}

JniCoordinator::~JniCoordinator() {
}

Status JniCoordinator::DeserializeRequest(jbyteArray thrift_query_exec_request) {
  // Deserialize request bytes into c++ request using memory transport.
  DeserializeThriftMsg(env_, thrift_query_exec_request, &query_exec_request_);
  LOG(INFO) << "query=" << query_exec_request_.sqlStmt
            << " #fragments=" << query_exec_request_.fragmentRequests.size()
            << " batch_size=" << query_exec_request_.batchSize;
  as_ascii_ = query_exec_request_.asAscii;

  if (query_exec_request_.fragmentRequests.size() == 0) {
    return Status("query exec request contains no plan fragments");
  }
  const TPlanExecRequest& coord_request = query_exec_request_.fragmentRequests[0];
  if (coord_request.__isset.descTbl != coord_request.__isset.planFragment) {
      return Status("bad TPlanExecRequest: only one of {planFragment, descTbl} is set");
  }
  is_constant_query_ = !coord_request.__isset.descTbl;
  RETURN_IF_ERROR(
      Expr::CreateExprTrees(
          &obj_pool_, query_exec_request_.fragmentRequests[0].outputExprs,
          &select_list_exprs_));

  if (query_exec_request_.fragmentRequests.size() > 1) {
    // TODO: remove this when we have multi-phase plans
    DCHECK_EQ(query_exec_request_.fragmentRequests.size(), 2);
    // fix up coord ports
    for (int i = 0; i < query_exec_request_.nodeRequestParams[1].size(); ++i) {
      DCHECK_EQ(query_exec_request_.nodeRequestParams[1][i].destinations.size(), 1);
      query_exec_request_.nodeRequestParams[1][i].destinations[0].port =
          FLAGS_backend_port;
    }
  }

  return Status::OK;
}

void JniCoordinator::Exec(jbyteArray thrift_query_exec_request) {
  THROW_IF_ERROR(DeserializeRequest(thrift_query_exec_request), env_, impala_exc_cl_);
  // if this query is missing a FROM clause, don't hand it to the coordinator
  if (is_constant_query_) return;
  THROW_IF_ERROR(coord_->Exec(query_exec_request_), env_, impala_exc_cl_);
  if (query_exec_request_.batchSize != 0) {
    coord_->runtime_state()->set_batch_size(query_exec_request_.batchSize);
  }
}

Status JniCoordinator::GetNext(RowBatch** batch) {
  Status result = coord_->GetNext(batch);
  VLOG(1) << "jnicoord.getnext";
  return result;
}

void JniCoordinator::AddResultRow(TupleRow* row) {
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

void JniCoordinator::WriteErrorLog() {
  const vector<string>& runtime_error_log = coord_->runtime_state()->error_log();
  for (int i = 0; i < runtime_error_log.size(); ++i) {
    env_->CallObjectMethod(error_log_, list_add_id_,
        env_->NewStringUTF(runtime_error_log[i].c_str()));
  }
}

void JniCoordinator::WriteFileErrors() {
  const vector<pair<string, int> >& runtime_file_errors =
      coord_->runtime_state()->file_errors();
  for (int i = 0; i < runtime_file_errors.size(); ++i) {
    env_->CallObjectMethod(file_errors_, map_put_id_,
        env_->NewStringUTF(runtime_file_errors[i].first.c_str()),
        env_->NewObject(integer_cl_, integer_ctor_, runtime_file_errors[i].second));
  }
}

jclass JniCoordinator::impala_exc_cl_ = NULL;
jclass JniCoordinator::throwable_cl_ = NULL;
jclass JniCoordinator::blocking_queue_if_ = NULL;
jclass JniCoordinator::result_row_cl_ = NULL;
jclass JniCoordinator::column_value_cl_ = NULL;
jclass JniCoordinator::list_cl_ = NULL;
jclass JniCoordinator::map_cl_ = NULL;
jclass JniCoordinator::integer_cl_ = NULL;
jmethodID JniCoordinator::throwable_to_string_id_ = NULL;
jmethodID JniCoordinator::put_id_ = NULL;
jmethodID JniCoordinator::result_row_ctor_ = NULL;
jmethodID JniCoordinator::add_to_col_vals_id_ = NULL;
jmethodID JniCoordinator::column_value_ctor_ = NULL;
jmethodID JniCoordinator::list_add_id_ = NULL;
jmethodID JniCoordinator::map_put_id_ = NULL;
jmethodID JniCoordinator::integer_ctor_ = NULL;
jfieldID JniCoordinator::bool_val_field_ = NULL;
jfieldID JniCoordinator::int_val_field_ = NULL;
jfieldID JniCoordinator::long_val_field_ = NULL;
jfieldID JniCoordinator::double_val_field_ = NULL;
jfieldID JniCoordinator::string_val_field_ = NULL;

Status JniCoordinator::Init() {
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

RuntimeState* JniCoordinator::runtime_state() {
  DCHECK(coord_.get() != NULL);
  return coord_->runtime_state();
}

const RowDescriptor& JniCoordinator::row_desc() {
  DCHECK(coord_.get() != NULL);
  return coord_->row_desc();
}

}
