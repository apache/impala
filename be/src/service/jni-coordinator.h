// (c) 2011 Cloudera, Inc. All rights reserved.

#ifndef IMPALA_SERVICE_PLAN_EXECUTOR_ADAPTOR_H
#define IMPALA_SERVICE_PLAN_EXECUTOR_ADAPTOR_H

#include <jni.h>
#include <vector>
#include <boost/scoped_ptr.hpp>
#include <boost/shared_ptr.hpp>
#include <stdint.h>
#include <Thrift.h>
#include <protocol/TBinaryProtocol.h>
#include <transport/TBufferTransports.h>
#include "common/object-pool.h"
#include "gen-cpp/ImpalaBackendService_types.h"

namespace impala {

class Expr;
class TupleRow;
class Status;
class PlanExecutor;
class Expr;
class ExecNode;
class DescriptorTbl;
class RowBatch;
class TupleDescriptor;
class DataStreamMgr;
class Coordinator;
class RuntimeState;
class RowDescriptor;
class TestEnv;

// Shim class to adapt backend coordinator to jni.
class JniCoordinator {
 public:
  JniCoordinator(
      JNIEnv* env, DataStreamMgr* stream_mgr, TestEnv* test_env, jobject error_log,
      jobject file_errors, jobject result_queue);
  ~JniCoordinator();

  // Create global jni structures.
  static Status Init();

  // Indicate error by throwing a new java exception.
  void Exec(jbyteArray thrift_query_exec_request);

  // Returns results from the coordinator fragment. Results are valid until
  // the next GetNext() call. '*batch' == NULL implies that subsequent calls
  // will not return any more rows.
  Status GetNext(RowBatch** batch);

  // Indicate error by throwing a new java exception.
  void AddResultRow(TupleRow* row);

  // Copy c++ runtime error log into Java error_log.
  void WriteErrorLog();

  // Copy c++ runtime file error stats into Java file_errors.
  void WriteFileErrors();

  Coordinator* coord() { return coord_.get(); }

  const std::vector<Expr*>& select_list_exprs() const { return select_list_exprs_; }

  jclass impala_exc_cl() { return impala_exc_cl_; }
  const TQueryExecRequest& query_exec_request() const { return query_exec_request_; }
  RuntimeState* runtime_state();
  const RowDescriptor& row_desc();
  bool is_constant_query() { return is_constant_query_; }

 private:
  JNIEnv* env_;
  TQueryExecRequest query_exec_request_;
  jobject error_log_;
  jobject file_errors_;
  bool as_ascii_;
  bool is_constant_query_;
  jobject result_queue_;
  ObjectPool obj_pool_;
  std::vector<Expr*> select_list_exprs_;
  DataStreamMgr* stream_mgr_;  // not owned
  boost::scoped_ptr<Coordinator> coord_;

  // Global class references created with JniUtil. Cleanup is done in JniUtil::Cleanup().
  static jclass impala_exc_cl_;
  static jclass throwable_cl_;
  static jclass blocking_queue_if_;
  static jclass result_row_cl_;
  static jclass column_value_cl_;
  static jclass list_cl_;
  static jclass map_cl_;
  static jclass integer_cl_;

  // methods
  static jmethodID throwable_to_string_id_;
  static jmethodID put_id_;
  static jmethodID result_row_ctor_, add_to_col_vals_id_;
  static jmethodID column_value_ctor_;
  static jmethodID list_add_id_;
  static jmethodID map_put_id_;
  static jmethodID integer_ctor_;

  // fields
  static jfieldID bool_val_field_;
  static jfieldID int_val_field_;
  static jfieldID long_val_field_;
  static jfieldID double_val_field_;
  static jfieldID string_val_field_;

  // Deserialize query exec request.
  Status DeserializeRequest(jbyteArray thrift_query_exec_request);

};

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
  boost::shared_ptr<apache::thrift::transport::TTransport> tmem_transport(
      new apache::thrift::transport::TMemoryBuffer(native_bytes, buf_size));
  apache::thrift::protocol::
    TBinaryProtocolFactoryT<apache::thrift::transport::TMemoryBuffer> tproto_factory;
  boost::shared_ptr<apache::thrift::protocol::TProtocol> tproto =
      tproto_factory.getProtocol(tmem_transport);
  deserialized_msg->read(tproto.get());
}

}

#endif
