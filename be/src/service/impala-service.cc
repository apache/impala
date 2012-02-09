// (c) 2012 Cloudera, Inc. All rights reserved.
//
// This file contains the main() function for the impala daemon process,
// which exports the Thrift services ImpalaService and ImpalaBackendService.

#include "service/impala-service.h"

#include <jni.h>
#include <boost/thread/locks.hpp>
#include <boost/scoped_ptr.hpp>
#include <boost/unordered_map.hpp>
#include <glog/logging.h>
#include <gflags/gflags.h>

#include <protocol/TBinaryProtocol.h>
#include <protocol/TDebugProtocol.h>
#include <server/TThreadPoolServer.h>
#include <transport/TServerSocket.h>
#include <server/TServer.h>
#include <transport/TTransportUtils.h>
#include <concurrency/PosixThreadFactory.h>

#include "common/status.h"
#include "exprs/expr.h"
#include "runtime/coordinator.h"
#include "runtime/row-batch.h"
#include "runtime/descriptors.h"
#include "runtime/exec-env.h"
#include "testutil/test-exec-env.h"
#include "util/jni-util.h"
#include "util/thrift-util.h"
#include "util/uid-util.h"
#include "service/backend-service.h"
#include "gen-cpp/ImpalaService.h"
#include "gen-cpp/ImpalaBackendService.h"
#include "gen-cpp/Types_types.h"

using namespace impala;
using namespace std;
using namespace boost;
using namespace apache::thrift;
using namespace apache::thrift::server;
using namespace apache::thrift::protocol;
using namespace apache::thrift::transport;
using namespace apache::thrift::concurrency;

DECLARE_bool(enable_expr_jit);

namespace impala {

ImpalaService::ImpalaService(int port)
  : port_(port),
    exec_env_(new ExecEnv()) {
}

void ImpalaService::Init(JNIEnv* env) {
  // create instance of java class Frontend
  jclass fe_class = env->FindClass("com/cloudera/impala/service/Frontend");
  jmethodID fe_ctor = env->GetMethodID(fe_class, "<init>", "()V");
  EXIT_IF_EXC(env);
  get_exec_request_id_ = env->GetMethodID(fe_class, "GetExecRequest", "([B)[B");
  EXIT_IF_EXC(env);
  jobject fe = env->NewObject(fe_class, fe_ctor);
  EXIT_IF_EXC(env);
  EXIT_IF_ERROR(JniUtil::LocalToGlobalRef(env, fe, &fe_));
}

void ImpalaService::RunQuery(
    impala::TRunQueryResult& result, const TQueryRequest& request) {
  LOG(INFO) << "RunQuery: " << request.stmt;
  result.status.status_code = TStatusCode::OK;
  TQueryExecRequest exec_request;
  SET_TSTATUS_IF_ERROR(GetExecRequest(request, &exec_request), &result.status);
  if (result.status.status_code != TStatusCode::OK) return;
  // we always need at least one plan fragment
  DCHECK_GT(exec_request.fragmentRequests.size(), 0);

  ExecState* exec_state = NULL;
  {
    lock_guard<mutex> l(exec_state_map_lock_);

    // there shouldn't be an active query with that same id
    // (queryId is globally unique)
    ExecStateMap::iterator entry = exec_state_map_.find(exec_request.queryId);
    if (entry != exec_state_map_.end()) {
      // TODO: needs better error code
      result.status.status_code = TStatusCode::INTERNAL_ERROR;
      return;
    }

    result.__isset.query_id = true;
    result.query_id = exec_request.queryId;

    exec_state = new ExecState(request, exec_env_.get());
    exec_state_map_.insert(make_pair(exec_request.queryId, exec_state));
  }

  if (!exec_request.fragmentRequests[0].__isset.descTbl) {
    // query without a FROM clause: don't create a coordinator
    DCHECK(!exec_request.fragmentRequests[0].__isset.planFragment);
    exec_state->local_runtime_state()->Init(
        exec_request.queryId, exec_request.abortOnError, exec_request.maxErrors,
        FLAGS_enable_expr_jit,
        NULL /* = we don't expect to be executing anything here */);
    result.__isset.col_types = true;
    SET_TSTATUS_IF_ERROR(
        exec_state->PrepareSelectListExprs(exec_state->local_runtime_state(),
          exec_request.fragmentRequests[0].outputExprs, RowDescriptor(),
          &result.col_types),
        &result.status);
    VLOG(2) << ThriftDebugString(result);
    return;
  }
  // TODO: pass in exec_state.runtime_state (keep pre-formatted RuntimeStates
  // around, so PlanExecutor doesn't have to keep allocating them)
  exec_state->coord_.reset(new Coordinator(exec_env_.get(), &exec_state->exec_stats_));

  DCHECK_GT(exec_request.nodeRequestParams.size(), 0);
  // the first nodeRequestParams list contains exactly one TPlanExecParams
  // (it's meant for the coordinator fragment)
  DCHECK_EQ(exec_request.nodeRequestParams[0].size(), 1);

  if (request.numNodes != 1) {
    // for distributed execution, for the time being we only expect one slave
    // fragment which will be executed by request.num_nodes - 1 nodes;
    // this will change once we support multiple phases (such as for DISTINCT)
    DCHECK_EQ(exec_request.fragmentRequests.size(), 2);
    DCHECK_EQ(exec_request.nodeRequestParams.size(), 2);
    DCHECK_LE(exec_request.nodeRequestParams[1].size(), request.numNodes - 1);

    // set destinations to be port
    for (int i = 0; i < exec_request.nodeRequestParams[1].size(); ++i) {
      DCHECK_EQ(exec_request.nodeRequestParams[1][i].destinations.size(), 1);
      exec_request.nodeRequestParams[1][i].destinations[0].port = port_;
    }
  }

  SET_TSTATUS_IF_ERROR(exec_state->coord_->Exec(exec_request), &result.status);
  result.__isset.col_types = true;
  SET_TSTATUS_IF_ERROR(
      exec_state->PrepareSelectListExprs(exec_state->coord_->runtime_state(),
        exec_request.fragmentRequests[0].outputExprs, exec_state->coord_->row_desc(),
        &result.col_types),
      &result.status);
  VLOG(2) << ThriftDebugString(result);
}

Status ImpalaService::ExecState::PrepareSelectListExprs(
    RuntimeState* runtime_state, const vector<TExpr>& exprs,
    const RowDescriptor& row_desc, vector<TPrimitiveType::type>* col_types) {
  RETURN_IF_ERROR(
      Expr::CreateExprTrees(runtime_state->obj_pool(), exprs, &output_exprs_));
  for (int i = 0; i < output_exprs_.size(); ++i) {
    Expr::Prepare(output_exprs_[i], runtime_state, row_desc);
    col_types->push_back(ToThrift(output_exprs_[i]->type()));
  }
  return Status::OK;
}

void ImpalaService::ExecState::ConvertRowBatch(
    RowBatch* batch, TQueryResult* query_result) {
  query_result->rows.clear();
  query_result->__isset.eos = true;
  eos_ = query_result->eos = (batch == NULL);
  if (batch == NULL) return;

  query_result->__isset.rows = true;
  query_result->rows.resize(batch->num_rows());
  string out_str;
  for (int i = 0; i < batch->num_rows(); ++i) {
    TupleRow* row = batch->GetRow(i);
    TResultRow& result_row = query_result->rows[i];
    result_row.__isset.colVals = true;
    result_row.colVals.resize(output_exprs_.size());
    stringstream out_stream;
    for (int j = 0; j < output_exprs_.size(); ++j) {
      output_exprs_[j]->GetValue(row, request_.returnAsAscii, &result_row.colVals[j]);
      if (VLOG_IS_ON(2)) {
        output_exprs_[i]->PrintValue(row, &out_str);
        out_stream << (i > 0 ? ", " : "") << out_str;
      }
    }
    VLOG(2) << "returned row " << out_stream.str();
  }
}

void ImpalaService::ExecState::CreateConstantRow(TQueryResult* query_result) {
  query_result->__isset.rows = true;
  query_result->rows.resize(1);
  TResultRow& result_row = query_result->rows.back();
  result_row.__isset.colVals = true;
  result_row.colVals.resize(output_exprs_.size());
  string out_str;
  stringstream out_stream;
  for (int i = 0; i < output_exprs_.size(); ++i) {
    TColumnValue& col_val = result_row.colVals[i];
    if (VLOG_IS_ON(2)) {
      output_exprs_[i]->PrintValue(static_cast<TupleRow*>(NULL), &out_str);
      out_stream << (i > 0 ? ", " : "") << out_str;
    }
    output_exprs_[i]->GetValue(NULL, request_.returnAsAscii, &col_val);
  }
  VLOG(2) << "returned single row " << out_stream.str();

  query_result->__isset.eos = true;
  query_result->eos = true;
}

Status ImpalaService::GetExecRequest(
    const TQueryRequest& query_request, TQueryExecRequest* exec_request) {
  // TODO: figure out if repeated calls to JNI_GetCreatedJavaVMs()/AttachCurrentThread()
  // are too expensive
  JNIEnv* jni_env = getJNIEnv();
  jbyteArray query_request_bytes;
  RETURN_IF_ERROR(SerializeThriftMsg(jni_env, &query_request, &query_request_bytes));
  jbyteArray exec_request_bytes = static_cast<jbyteArray>(
      jni_env->CallObjectMethod(fe_, get_exec_request_id_, query_request_bytes));
  RETURN_ERROR_IF_EXC(jni_env, JniUtil::throwable_to_string_id());
  DeserializeThriftMsg(jni_env, exec_request_bytes, exec_request);
  // TODO: dealloc exec_request_bytes?
  // TODO: figure out if we should detach here
  //RETURN_IF_JNIERROR(jvm_->DetachCurrentThread());
  return Status::OK;
}

void ImpalaService::FetchResults(
    TFetchResultsResult& result, const TUniqueId& query_id) {
  result.status.status_code = TStatusCode::OK;

  ExecState* exec_state = NULL;
  {
    lock_guard<mutex> l(exec_state_map_lock_);
    ExecStateMap::iterator entry = exec_state_map_.find(query_id);
    if (entry == exec_state_map_.end()) {
      // TODO: return different error if query just got cancelled?
      result.status.status_code = TStatusCode::INTERNAL_ERROR;
      return;
    }
    exec_state = entry->second;
  }

  DCHECK(!exec_state->eos_);
  bool eos = false;
  if (exec_state->coord() == NULL) {
    // query without FROM clause: we return exactly one row
    result.__isset.query_result = true;
    exec_state->CreateConstantRow(&result.query_result);
    eos = true;
  } else {
    RowBatch* batch;
    SET_TSTATUS_IF_ERROR(
        exec_state->coord()->GetNext(
          &batch, exec_state->coord()->runtime_state()),
        &result.status);
    // TODO: clean up/remove ExecState if we fail
    if (result.status.status_code == TStatusCode::OK) {
      result.__isset.query_result = true;
      exec_state->ConvertRowBatch(batch, &result.query_result);
      eos = (batch == NULL);
    }
  }

  // clean up
  // TODO: mark entry as garbage instead of getting lock/doing lookup
  // a second time?
  if (eos) {
    lock_guard<mutex> l(exec_state_map_lock_);
    ExecStateMap::iterator entry = exec_state_map_.find(query_id);
    if (entry != exec_state_map_.end()) {
      // TODO: log error if we don't find it?
      exec_state_map_.erase(entry);
    }
  }

  VLOG(2) << ThriftDebugString(result);
  return;
}

void ImpalaService::CancelQuery(TStatus& result, const TUniqueId& query_id) {
  // TODO: implement cancellation in a follow-on cl
#if 0
  result.status_code = TStatusCode::OK;
  ExecStateMap::iterator i = exec_state_map_.find(query_id);
  if (i == exec_state_map_.end()) {
    result.status_code = TStatusCode::INTERNAL_ERROR;
    return;
  }
  Coordinator* coord = i->second->coord.get();
  coord->Cancel();
#endif
}

}
