// (c) 2011 Cloudera, Inc. All rights reserved.

#include "service/plan-executor.h"

#include <Thrift.h>
#include <protocol/TBinaryProtocol.h>
#include <transport/TBufferTransports.h>
#include <boost/scoped_ptr.hpp>
#include <glog/logging.h>

#include "common/object-pool.h"
#include "exec/exec-node.h"
#include "exprs/expr.h"
#include "runtime/descriptors.h"
#include "runtime/row-batch.h"
#include "gen-cpp/ImpalaService_types.h"
#include "gen-cpp/ImpalaPlanService_types.h"

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

#define THROW_IF_EXC(env, exc_class, throwable_to_string_id_) \
  do { \
    jthrowable exc = (env)->ExceptionOccurred(); \
    if (exc != NULL) { \
     jstring msg = (jstring) env->CallObjectMethod(exc, (throwable_to_string_id_)); \
     jboolean is_copy; \
     const char* c_msg = reinterpret_cast<const char*>((env)->GetStringUTFChars(msg, &is_copy)); \
     (env)->ExceptionClear(); \
     (env)->ThrowNew((exc_class), c_msg); \
     return; \
    } \
  } while (false)

#define RETURN_IF_EXC(env) \
  do { \
    jthrowable exc = (env)->ExceptionOccurred(); \
    if (exc != NULL) { \
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

class PlanExecutorAdaptor {
 public:
  PlanExecutorAdaptor(JNIEnv* env_, jbyteArray thrift_execute_plan_request,
                      jboolean as_ascii, jobject result_queue)
    : env_(env_), thrift_execute_plan_request_(thrift_execute_plan_request),
      as_ascii_(as_ascii), result_queue_(result_queue) {}

  // Indicate error by throwing a new java exception.
  void Init();

  // Indicate error by throwing a new java exception.
  void Exec();

  // Indicate error by throwing a new java exception.
  void AddResultRow(TupleRow* row);

  PlanExecutor* executor() { return executor_.get(); }

  const vector<Expr*>& select_list_exprs() const { return select_list_exprs_; }

  ExecNode* plan() { return plan_; }

  jclass impala_exc_cl() { return impala_exc_cl_; }

 private:
  JNIEnv* env_;
  jbyteArray thrift_execute_plan_request_;
  bool as_ascii_;
  jobject result_queue_;
  ObjectPool obj_pool_;
  ExecNode* plan_;
  DescriptorTbl* descs_;
  vector<Expr*> select_list_exprs_;
  scoped_ptr<PlanExecutor> executor_;

  // classes
  jclass impala_exc_cl_;
  jclass throwable_cl_;
  jclass blocking_queue_if_;
  jclass result_row_cl_;
  jclass column_value_cl_;

  // methods
  jmethodID throwable_to_string_id_;
  jmethodID put_id_;
  jmethodID result_row_ctor_, add_to_col_vals_id_;
  jmethodID column_value_ctor_;

  // fields
  jfieldID bool_val_field_;
  jfieldID int_val_field_;
  jfieldID long_val_field_;
  jfieldID double_val_field_;
  jfieldID string_val_field_;

  // Indicate error by throwing a new java exception.
  void DeserializeRequest();
};

void PlanExecutorAdaptor::Init() {
  // setup
  impala_exc_cl_ = env_->FindClass("com/cloudera/impala/common/ImpalaException");
  if (impala_exc_cl_ == NULL) {
    if (env_->ExceptionOccurred()) env_->ExceptionDescribe();
    return;
  }
  jclass throwable_cl_ = env_->FindClass("java/lang/Throwable");
  if (throwable_cl_ == NULL) {
    if (env_->ExceptionOccurred()) env_->ExceptionDescribe();
    return;
  }
  jmethodID throwable_to_string_id_ =
      env_->GetMethodID(throwable_cl_, "toString", "()Ljava/lang/String;");
  if (throwable_to_string_id_ == NULL) {
    if (env_->ExceptionOccurred()) env_->ExceptionDescribe();
    return;
  }

  blocking_queue_if_ = env_->FindClass("java/util/concurrent/BlockingQueue");
  THROW_IF_EXC(env_, impala_exc_cl_, throwable_to_string_id_);
  put_id_ = env_->GetMethodID(blocking_queue_if_, "put", "(Ljava/lang/Object;)V");
  THROW_IF_EXC(env_, impala_exc_cl_, throwable_to_string_id_);

  result_row_cl_ = env_->FindClass("com/cloudera/impala/thrift/TResultRow");
  THROW_IF_EXC(env_, impala_exc_cl_, throwable_to_string_id_);
  result_row_ctor_ = env_->GetMethodID(result_row_cl_, "<init>", "()V");
  THROW_IF_EXC(env_, impala_exc_cl_, throwable_to_string_id_);
  add_to_col_vals_id_ =
      env_->GetMethodID(result_row_cl_, "addToColVals",
                       "(Lcom/cloudera/impala/thrift/TColumnValue;)V");
  THROW_IF_EXC(env_, impala_exc_cl_, throwable_to_string_id_);

  column_value_cl_ = env_->FindClass("com/cloudera/impala/thrift/TColumnValue");
  THROW_IF_EXC(env_, impala_exc_cl_, throwable_to_string_id_);
  column_value_ctor_ = env_->GetMethodID(column_value_cl_, "<init>", "()V");
  THROW_IF_EXC(env_, impala_exc_cl_, throwable_to_string_id_);
  bool_val_field_ = env_->GetFieldID(column_value_cl_, "boolVal", "Z");
  THROW_IF_EXC(env_, impala_exc_cl_, throwable_to_string_id_);
  int_val_field_ = env_->GetFieldID(column_value_cl_, "intVal", "I");
  THROW_IF_EXC(env_, impala_exc_cl_, throwable_to_string_id_);
  long_val_field_ = env_->GetFieldID(column_value_cl_, "longVal", "J");
  THROW_IF_EXC(env_, impala_exc_cl_, throwable_to_string_id_);
  double_val_field_ = env_->GetFieldID(column_value_cl_, "doubleVal", "D");
  THROW_IF_EXC(env_, impala_exc_cl_, throwable_to_string_id_);
  string_val_field_ = env_->GetFieldID(column_value_cl_, "stringVal", "Ljava/lang/String;");
  THROW_IF_EXC(env_, impala_exc_cl_, throwable_to_string_id_);
}

void PlanExecutorAdaptor::DeserializeRequest() {
  // TODO: Find out why using plan_buf directly does not work.
  // Copy java byte array into native byte array.
  jboolean is_copy = false;
  int plan_buf_size = env_->GetArrayLength(thrift_execute_plan_request_);
  jbyte* plan_buf = env_->GetByteArrayElements(thrift_execute_plan_request_, &is_copy);
  uint8_t native_plan_bytes[plan_buf_size];
  for (int i = 0; i < plan_buf_size; i++) {
    native_plan_bytes[i] = plan_buf[i];
  }

  // Deserialize plan bytes into c++ plan request using memory transport.
  TExecutePlanRequest exec_request;
  shared_ptr<TTransport> tmem_transport(
      new TMemoryBuffer(native_plan_bytes, plan_buf_size));
  TBinaryProtocolFactoryT<TMemoryBuffer> tproto_factory;
  shared_ptr<TProtocol> tproto = tproto_factory.getProtocol(tmem_transport);
  exec_request.read(tproto.get());

  if (exec_request.__isset.plan) {
    THROW_IF_ERROR(ExecNode::CreateTree(&obj_pool_, exec_request.plan, &plan_), env_,
                   impala_exc_cl_);
  } else {
    plan_ = NULL;
  }
  if (exec_request.__isset.descTbl) {
    THROW_IF_ERROR(DescriptorTbl::Create(&obj_pool_, exec_request.descTbl, &descs_), env_,
                   impala_exc_cl_);
  } else {
    descs_ = NULL;
  }
  // TODO: check that (descs_ == NULL) == (plan_ == NULL)
  THROW_IF_ERROR(
      Expr::CreateExprTrees(&obj_pool_, exec_request.selectListExprs,
                            &select_list_exprs_),
      env_, impala_exc_cl_);
}

void PlanExecutorAdaptor::Exec() {
  DeserializeRequest();
  RETURN_IF_EXC(env_);
  if (plan_ == NULL) return;
  executor_.reset(new PlanExecutor(plan_, *descs_));
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

extern "C"
JNIEXPORT void Java_com_cloudera_impala_service_NativePlanExecutor_Init(
    JNIEnv* env_, jclass caller_class) {
  google::InitGoogleLogging("impala-backend");
  // install libunwind before activating this on 64-bit systems:
  //google::InstallFailureSignalHandler();
}

extern "C"
JNIEXPORT void JNICALL Java_com_cloudera_impala_service_NativePlanExecutor_ExecPlan(
    JNIEnv* env, jclass caller_class, jbyteArray thrift_execute_plan_request,
    jboolean as_ascii, jobject result_queue) {

  PlanExecutorAdaptor adaptor(env, thrift_execute_plan_request, as_ascii, result_queue);
  adaptor.Init();
  RETURN_IF_EXC(env);
  adaptor.Exec();
  RETURN_IF_EXC(env);
  const vector<Expr*>& select_list_exprs = adaptor.select_list_exprs();
  PlanExecutor* executor = adaptor.executor();

  // Prepare select list expressions.
  for (size_t i = 0; i < select_list_exprs.size(); ++i) {
    select_list_exprs[i]->Prepare(executor->runtime_state());
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
    THROW_IF_ERROR(executor->FetchResult(&batch_ptr), env, adaptor.impala_exc_cl());
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
}

}
