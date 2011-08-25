// (c) 2011 Cloudera, Inc. All rights reserved.

#include "service/backend.h"

#include <vector>
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
#include "service/plan-executor.h"
#include "gen-cpp/ImpalaService_types.h"
#include "gen-cpp/ImpalaPlanService_types.h"

using namespace std;
using namespace boost;
using namespace apache::thrift::protocol;
using namespace apache::thrift::transport;

#define THROW_IF_ERROR(stmt, env, adaptor) \
  do { \
    Status status = (stmt); \
    if (!status.ok()) { \
      (adaptor)->WriteErrorLog(); \
      (adaptor)->WriteFileErrors(); \
      string error_msg; \
      status.GetErrorMsg(&error_msg); \
      (env)->ThrowNew((adaptor)->impala_exc_cl(), error_msg.c_str()); \
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

class Expr;
class PlanExecutor;

class PlanExecutorAdaptor {
 public:
  PlanExecutorAdaptor(JNIEnv* env_, jbyteArray thrift_execute_plan_request,
      jboolean abort_on_error, jint max_errors,
      jobject error_log, jobject file_errors,
      jboolean as_ascii, jobject result_queue) :
    env_(env_), thrift_execute_plan_request_(thrift_execute_plan_request),
        abort_on_error_(abort_on_error), max_errors_(max_errors),
        error_log_(error_log), file_errors_(file_errors),
        as_ascii_(as_ascii), result_queue_(result_queue) {
  }

  // Indicate error by throwing a new java exception.
  void Init();

  // Indicate error by throwing a new java exception.
  void Exec();

  // Indicate error by throwing a new java exception.
  void AddResultRow(TupleRow* row);

  // Copy c++ runtime error log into Java error_log.
  void WriteErrorLog();

  // Copy c++ runtime file error stats into Java file_errors.
  void WriteFileErrors();

  PlanExecutor* executor() { return executor_.get(); }

  const std::vector<Expr*>& select_list_exprs() const { return select_list_exprs_; }

  ExecNode* plan() { return plan_; }

  jclass impala_exc_cl() { return impala_exc_cl_; }

 private:
  JNIEnv* env_;
  jbyteArray thrift_execute_plan_request_;
  bool abort_on_error_;
  int max_errors_;
  jobject error_log_;
  jobject file_errors_;
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
  jclass list_cl_;
  jclass map_cl_;
  jclass integer_cl_;

  // methods
  jmethodID throwable_to_string_id_;
  jmethodID put_id_;
  jmethodID result_row_ctor_, add_to_col_vals_id_;
  jmethodID column_value_ctor_;
  jmethodID list_add_id_;
  jmethodID map_put_id_;
  jmethodID integer_ctor_;

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

  list_cl_ = env_->FindClass("java/util/List");
  THROW_IF_EXC(env_, impala_exc_cl_, throwable_to_string_id_);
  list_add_id_ = env_->GetMethodID(list_cl_, "add", "(Ljava/lang/Object;)Z");
  THROW_IF_EXC(env_, impala_exc_cl_, throwable_to_string_id_);

  map_cl_ = env_->FindClass("java/util/Map");
  THROW_IF_EXC(env_, impala_exc_cl_, throwable_to_string_id_);
  map_put_id_ = env_->GetMethodID(map_cl_, "put",
      "(Ljava/lang/Object;Ljava/lang/Object;)Ljava/lang/Object;");
  THROW_IF_EXC(env_, impala_exc_cl_, throwable_to_string_id_);
  integer_cl_ = env_->FindClass("java/lang/Integer");
  THROW_IF_EXC(env_, impala_exc_cl_, throwable_to_string_id_);
  integer_ctor_ = env_->GetMethodID(integer_cl_, "<init>", "(I)V");
  THROW_IF_EXC(env_, impala_exc_cl_, throwable_to_string_id_);
}

template <class T>
void DeserializeThriftMsg(JNIEnv* env, jbyteArray serialized_msg, T* deserialized_msg) {
  // TODO: Find out why using plan_buf directly does not work.
  // Copy java byte array into native byte array.
  jboolean is_copy = false;
  int buf_size = env->GetArrayLength(serialized_msg);
  jbyte* buf = env->GetByteArrayElements(serialized_msg, &is_copy);
  uint8_t native_bytes[buf_size];
  for (int i = 0; i < buf_size; i++) {
    native_bytes[i] = buf[i];
  }

  // Deserialize msg bytes into c++ thrift msg using memory transport.
  shared_ptr<TTransport> tmem_transport(
      new TMemoryBuffer(native_bytes, buf_size));
  TBinaryProtocolFactoryT<TMemoryBuffer> tproto_factory;
  shared_ptr<TProtocol> tproto = tproto_factory.getProtocol(tmem_transport);
  deserialized_msg->read(tproto.get());
}

void PlanExecutorAdaptor::DeserializeRequest() {
  // Deserialize plan bytes into c++ plan request using memory transport.
  TExecutePlanRequest exec_request;
  DeserializeThriftMsg(env_, thrift_execute_plan_request_, &exec_request);

  if (exec_request.__isset.plan) {
    THROW_IF_ERROR(ExecNode::CreateTree(&obj_pool_, exec_request.plan, &plan_), env_,
                   this);
  } else {
    plan_ = NULL;
  }
  if (exec_request.__isset.descTbl) {
    THROW_IF_ERROR(DescriptorTbl::Create(&obj_pool_, exec_request.descTbl, &descs_), env_,
                   this);
  } else {
    descs_ = NULL;
  }
  // TODO: check that (descs_ == NULL) == (plan_ == NULL)
  THROW_IF_ERROR(
      Expr::CreateExprTrees(&obj_pool_, exec_request.selectListExprs,
                            &select_list_exprs_), env_, this);
}

void PlanExecutorAdaptor::Exec() {
  DeserializeRequest();
  RETURN_IF_EXC(env_);
  if (plan_ == NULL) return;
  executor_.reset(new PlanExecutor(plan_, *descs_, abort_on_error_, max_errors_));
  THROW_IF_ERROR(executor_->Exec(), env_, this);
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

extern "C"
JNIEXPORT void Java_com_cloudera_impala_service_NativeBackend_Init(
    JNIEnv* env_, jclass caller_class) {
  google::InitGoogleLogging("impala-backend");
  // install libunwind before activating this on 64-bit systems:
  //google::InstallFailureSignalHandler();
}

extern "C"
JNIEXPORT void JNICALL Java_com_cloudera_impala_service_NativeBackend_ExecPlan(
    JNIEnv* env, jclass caller_class, jbyteArray thrift_execute_plan_request,
    jboolean abort_on_error, jint max_errors, jobject error_log, jobject file_errors,
    jboolean as_ascii, jobject result_queue) {

  PlanExecutorAdaptor adaptor(env, thrift_execute_plan_request, abort_on_error, max_errors,
      error_log, file_errors, as_ascii, result_queue);
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
    THROW_IF_ERROR(executor->FetchResult(&batch_ptr), env, &adaptor);
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

  e->Prepare(NULL);
  bool* v = static_cast<bool*>(e->GetValue(NULL));
  return *v;
}

}
