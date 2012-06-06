// (c) 2012 Cloudera, Inc. All rights reserved.
//
// This file contains the main() function for the impala daemon process,
// which exports the Thrift services ImpalaService.

#include "service/impala-service.h"

#include <jni.h>
#include <boost/foreach.hpp>
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
#include "util/string-parser.h"
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
using namespace beeswax;

DECLARE_bool(enable_jit);

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
  get_explain_plan_id_ = env->GetMethodID(fe_class, "GetExplainPlan",
      "([B)Ljava/lang/String;");
  EXIT_IF_EXC(env);
  jobject fe = env->NewObject(fe_class, fe_ctor);
  EXIT_IF_EXC(env);
  EXIT_IF_ERROR(JniUtil::LocalToGlobalRef(env, fe, &fe_));
}

Status ImpalaService::executeAndWaitInternal(const TQueryRequest& request,
    TUniqueId* query_id) {
  LOG(INFO) << "RunQuery: " << request.stmt;
  TQueryExecRequest exec_request;
  RETURN_IF_ERROR(GetExecRequest(request, &exec_request));
  // we always need at least one plan fragment
  DCHECK_GT(exec_request.fragmentRequests.size(), 0);

  ExecState* exec_state = NULL;
  {
    lock_guard<mutex> l(exec_state_map_lock_);

    // there shouldn't be an active query with that same id
    // (queryId is globally unique)
    ExecStateMap::iterator entry = exec_state_map_.find(exec_request.queryId);
    if (entry != exec_state_map_.end()) {
      // TODO: needs better error string
      return Status("internal error");
    }

    *query_id = exec_request.queryId;

    exec_state = new ExecState(request, exec_env_.get());
    exec_state_map_.insert(make_pair(exec_request.queryId, exec_state));
  }

  if (!exec_request.fragmentRequests[0].__isset.descTbl) {
    // query without a FROM clause: don't create a coordinator
    DCHECK(!exec_request.fragmentRequests[0].__isset.planFragment);
    exec_state->local_runtime_state()->Init(
        exec_request.queryId, exec_request.abortOnError, exec_request.maxErrors,
        FLAGS_enable_jit,
        NULL /* = we don't expect to be executing anything here */);
    return exec_state->PrepareSelectListExprs(exec_state->local_runtime_state(),
        exec_request.fragmentRequests[0].outputExprs, RowDescriptor());
  }
  // TODO: pass in exec_state.runtime_state (keep pre-formatted RuntimeStates
  // around, so PlanExecutor doesn't have to keep allocating them)
  exec_state->ResetCoordinator();

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

  RETURN_IF_ERROR(exec_state->coord()->Exec(exec_request));
  RETURN_IF_ERROR(exec_state->PrepareSelectListExprs(exec_state->coord()->runtime_state(),
        exec_request.fragmentRequests[0].outputExprs, exec_state->coord()->row_desc()));

  return Status::OK;
}

void ImpalaService::ExecState::ResetCoordinator() {
  coord_.reset(new Coordinator(exec_env_, &exec_stats_));
}

void ImpalaService::ExecState::FetchRowsAsAscii(const int32_t max_rows,
    vector<string>* fetched_rows) {
  DCHECK(!eos_);
  if (coord_ == NULL) {
    // query without FROM clause: we return exactly one row
    CreateConstantRowAsAscii(fetched_rows);
  } else {
    // Fetch the next batch if we've returned the current batch entirely
    if (current_batch_ == NULL || current_batch_row_ >= current_batch_->num_rows()) {
      FetchNextBatch();
    }
    ConvertRowBatchToAscii(max_rows, fetched_rows);
  }
}

Status ImpalaService::ExecState::PrepareSelectListExprs(
    RuntimeState* runtime_state, const vector<TExpr>& exprs,
    const RowDescriptor& row_desc) {
  RETURN_IF_ERROR(
      Expr::CreateExprTrees(runtime_state->obj_pool(), exprs, &output_exprs_));
  col_types_.clear();
  for (int i = 0; i < output_exprs_.size(); ++i) {
    Expr::Prepare(output_exprs_[i], runtime_state, row_desc);
    col_types_.push_back(output_exprs_[i]->type());
  }
  return Status::OK;
}

void ImpalaService::ExecState::FetchNextBatch() {
  DCHECK(!eos_);
  Status status = coord_->GetNext(&current_batch_, coord_->runtime_state());
  if (!status.ok()) {
    // TODO: clean up/remove ExecState if we fail
    // TODO: raise proper Beeswax Exception if error
    BeeswaxException exc;
    exc.message = status.GetErrorMsg();
    exc.__isset.message = true;
    throw exc;
  }
  current_batch_row_ = 0;
  eos_ = (current_batch_ == NULL);
}

void ImpalaService::ExecState::ConvertRowBatchToAscii(const int32_t max_rows,
    vector<string>* fetched_rows) {
  if (current_batch_ == NULL) {
    fetched_rows->resize(0);
    return;
  }

  // Convert the available rows, limited by max_rows
  int available = current_batch_->num_rows() - current_batch_row_;
  int fetched_count = (available < max_rows) ? available : max_rows;
  fetched_rows->resize(fetched_count);
  for (int i = 0; i < fetched_count; ++i) {
    TupleRow* row = current_batch_->GetRow(current_batch_row_);
    stringstream out_stream;
    for (int j = 0; j < output_exprs_.size(); ++j) {
      void* value = output_exprs_[j]->GetValue(row);
      if (value != NULL) {
        output_exprs_[j]->PrintValue(row, &out_stream);
      }
      if (j + 1 < output_exprs_.size()) {
        out_stream << ",";
      }
    }
    ++current_row_;
    ++current_batch_row_;
    (*fetched_rows)[i] = out_stream.str();
    VLOG(2) << "returned row as Ascii: " << (*fetched_rows)[i];
  }
}

void ImpalaService::ExecState::CreateConstantRowAsAscii(vector<string>* fetched_rows) {
  string out_str;
  stringstream out_stream;
  fetched_rows->resize(1);
  for (int i = 0; i < output_exprs_.size(); ++i) {
    output_exprs_[i]->PrintValue(static_cast<TupleRow*>(NULL), &out_str);
    out_stream << (i > 0 ? ", " : "") << out_str;
  }
  (*fetched_rows)[0] = out_stream.str();
  VLOG(2) << "returned single row as Ascii: " << (*fetched_rows)[0];
  eos_ = true;
  ++current_row_;
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

Status ImpalaService::GetExplainPlan(
    const TQueryRequest& query_request, string* explain_string) {
  // TODO: figure out if repeated calls to JNI_GetCreatedJavaVMs()/AttachCurrentThread()
  // are too expensive
  JNIEnv* jni_env = getJNIEnv();
  jbyteArray query_request_bytes;
  RETURN_IF_ERROR(SerializeThriftMsg(jni_env, &query_request, &query_request_bytes));
  jstring java_explain_string = static_cast<jstring>(
      jni_env->CallObjectMethod(fe_, get_explain_plan_id_, query_request_bytes));
  RETURN_ERROR_IF_EXC(jni_env, JniUtil::throwable_to_string_id());
  jboolean is_copy;
  const char *str = jni_env->GetStringUTFChars(java_explain_string, &is_copy);
  RETURN_ERROR_IF_EXC(jni_env, JniUtil::throwable_to_string_id());
  *explain_string = str;
  jni_env->ReleaseStringUTFChars(java_explain_string, str);
  RETURN_ERROR_IF_EXC(jni_env, JniUtil::throwable_to_string_id());
  return Status::OK;
}

void ImpalaService::cancel(impala::TStatus& status,
    const beeswax::QueryHandle& query_id) {
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


// Beeswax API Implementation starts here

void ImpalaService::executeAndWait(QueryHandle& query_handle, const Query& query,
  const LogContextId& client_ctx) {
  // Translate Beeswax Query to Impala's QueryRequest and then call executeAndWaitInternal
  TQueryRequest queryRequest;
  QueryToTQueryRequest(query, &queryRequest);

  TUniqueId query_id;
  Status status = executeAndWaitInternal(queryRequest, &query_id);
  if (!status.ok()) {
    // TODO: raise proper Beeswax Exception if error
    BeeswaxException exc;
    exc.message = status.GetErrorMsg();
    exc.__isset.message = true;
    throw exc;
  }

  // Convert TUnique back to QueryHandle
  TUniqueIdToQueryHandle(query_id, &query_handle);

  // If the input log context id is an empty string, then create a new number and
  // set it to _return. Otherwise, set _return with the input log context
  query_handle.log_context = (client_ctx.size() != 0) ? query_handle.id : client_ctx;
}

void ImpalaService::explain(QueryExplanation& query_explanation, const Query& query) {
  // Translate Beeswax Query to Impala's QueryRequest and then set the explain plan bool
  // before shipping to FE
  TQueryRequest queryRequest;
  QueryToTQueryRequest(query, &queryRequest);

  Status status = GetExplainPlan(queryRequest, &query_explanation.textual);
  if (!status.ok()) {
    // TODO: raise proper Beeswax Exception if error
    BeeswaxException exc;
    exc.message = status.GetErrorMsg();
    exc.__isset.message = true;
    throw exc;
  }
  query_explanation.__isset.textual = true;
  VLOG(2) << "explain plan: " << query_explanation.textual;
}

ImpalaService::ExecState* ImpalaService::GetExecState(const TUniqueId& unique_id) {
  ExecState* exec_state = NULL;
  {
    lock_guard<mutex> l(exec_state_map_lock_);
    ExecStateMap::iterator entry = exec_state_map_.find(unique_id);
    if (entry == exec_state_map_.end()) {
      return NULL;
    }
    exec_state = entry->second;
  }
  return exec_state;
}

void ImpalaService::fetch(Results& query_results, const QueryHandle& query_id,
    const bool start_over, const int32_t fetch_size) {
  // We can't do start_over. Raise an exception if start_over is true
  if (start_over) {
    BeeswaxException exc;
    exc.message = "Does not support start over";
    exc.__isset.message = true;
    throw exc;
  }

  // Convert QueryHandle to TUniqueId and get the query exec state.
  TUniqueId uniqueId;
  QueryHandleToTUniqueId(query_id, &uniqueId);
  ExecState* exec_state = GetExecState(uniqueId);
  // TODO: raise proper exception if we can't find exec_state.
  if (exec_state == NULL) {
    BeeswaxException exc;
    exc.message = "Invalid query handle";
    exc.__isset.message = true;
    throw exc;
  }

  // TODO: if we've hit EOS, throw an exception according to the Beeswax specification.
  if (exec_state->eos()) {
    BeeswaxException exc;
    exc.message = "EOS reached";
    exc.__isset.message = true;
    throw exc;
  }

  // TODO: Ignore Beeswax's Results.columns for now; not sure if it's column name or type

  // Results are always ready because we're blocking.
  query_results.ready = true;
  query_results.start_row = exec_state->current_row() + 1; // TODO: start_row from 1?
  exec_state->FetchRowsAsAscii(fetch_size, &query_results.data);
  query_results.has_more = !exec_state->eos();
  query_results.__isset.ready = true;
  query_results.__isset.start_row = true;
  query_results.__isset.data = true;
  query_results.__isset.has_more = true;

  // Remove ExecState when the query is done.
  // TODO: mark entry as garbage instead of getting lock/doing lookup
  // a second time?
  if (!query_results.has_more) {
    lock_guard<mutex> l(exec_state_map_lock_);
    ExecStateMap::iterator entry = exec_state_map_.find(uniqueId);
    if (entry != exec_state_map_.end()) {
      // TODO: log error if we don't find it?
      exec_state_map_.erase(entry);
    }
  }
}

// TODO: return ExecStstate.col_types
void ImpalaService::get_results_metadata(ResultsMetadata& results_metadata,
    const QueryHandle& handle) {
}

//TODO: Do nothing because we always auto close but we might want to move the clean-up
// over here.
void ImpalaService::close(const QueryHandle& handle) {
}

// Always in a finished stated.
// We only do sync query; when we return, we've results already. FINISHED actually means
// result is ready, not the query has finished. (Although for Hive, result won't be ready
// until the query is finished.)
QueryState::type ImpalaService::get_state(const QueryHandle& handle) {
  return QueryState::FINISHED;
}

void ImpalaService::echo(string& echo_string, const string& input_string) {
  echo_string = input_string;
}

// Impala cleans up after the query after the last fetch. So, we don't need to do
// anything here.
// TODO: LogContextId is actually a session. We might want to close all the existing
// query handle of the given session.
void ImpalaService::clean(const LogContextId& log_context) {
}


void ImpalaService::query(QueryHandle& query_handle, const Query& query) {
}

void ImpalaService::dump_config(string& config) {
  config = "";
}

void ImpalaService::get_log(string& log, const LogContextId& context) {
  log = "logs are distributed. Skip it for now";
}

void ImpalaService::get_default_configuration(
    vector<ConfigVariable> & configurations, const bool include_hadoop) {
}

void ImpalaService::QueryToTQueryRequest(const Query& query, TQueryRequest* request) {
  // TODO: hard code the number of nodes to 1 for now.
  request->numNodes = 1;
  request->returnAsAscii = true;
  request->stmt = query.query;
}

// TODO: any better/more efficient mapping?
void ImpalaService::TUniqueIdToQueryHandle(const TUniqueId& query_id,
    QueryHandle* handle) {
  stringstream stringstream;
  stringstream << query_id.hi << " " << query_id.lo;
  handle->id = stringstream.str();
}

void ImpalaService::QueryHandleToTUniqueId(const QueryHandle& handle,
    TUniqueId* query_id) {
  char_separator<char> sep(" ");
  tokenizer< char_separator<char> > tokens(handle.id, sep);
  int i = 0;
  BOOST_FOREACH(string t, tokens)
  {
    StringParser::ParseResult parse_result = StringParser::PARSE_SUCCESS;
    int64_t id = StringParser::StringToInt<int64_t>(t.c_str(), t.length(), &parse_result);
    if (i == 0) {
      query_id->hi = id;
    } else {
      query_id->lo = id;
    }
    ++i;
  }
}

}
