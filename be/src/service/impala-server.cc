// Copyright (c) 2012 Cloudera, Inc. All rights reserved.

#include "service/impala-server.h"

#include <jni.h>
#include <protocol/TBinaryProtocol.h>
#include <protocol/TDebugProtocol.h>
#include <transport/TSocket.h>
#include <server/TThreadPoolServer.h>
#include <transport/TServerSocket.h>
#include <transport/TTransportUtils.h>
#include <server/TServer.h>
//#include <concurrency/Thread.h>
#include <concurrency/PosixThreadFactory.h>
#include <gtest/gtest.h>
#include <glog/logging.h>
// include gflags.h *after* logging.h, otherwise the linker will complain about
// undefined references to FLAGS_v
#include <gflags/gflags.h>
#include <boost/foreach.hpp>
#include <boost/bind.hpp>

#include "exprs/expr.h"
#include "exec/hdfs-table-sink.h"
#include "codegen/llvm-codegen.h"
#include "runtime/data-stream-mgr.h"
#include "runtime/client-cache.h"
#include "runtime/descriptors.h"
#include "runtime/data-stream-sender.h"
#include "runtime/row-batch.h"
#include "runtime/plan-fragment-executor.h"
#include "runtime/hdfs-fs-cache.h"
#include "runtime/exec-env.h"
#include "runtime/coordinator.h"
#include "exec/exec-node.h"
#include "exec/scan-node.h"
#include "exec/exec-stats.h"
#include "util/debug-util.h"
#include "util/string-parser.h"
#include "util/thrift-util.h"
#include "util/thrift-server.h"
#include "util/jni-util.h"
#include "util/webserver.h"
#include "gen-cpp/ImpalaService.h"
#include "gen-cpp/DataSinks_types.h"
#include "gen-cpp/Types_types.h"
#include "gen-cpp/ImpalaService.h"
#include "gen-cpp/ImpalaInternalService.h"
#include "gen-cpp/ImpalaPlanService.h"
#include "gen-cpp/ImpalaPlanService_types.h"
#include "gen-cpp/JavaConstants_constants.h"

using namespace std;
using namespace boost;
using namespace apache::thrift;
using namespace apache::thrift::protocol;
using namespace apache::thrift::transport;
using namespace apache::thrift::server;
using namespace apache::thrift::concurrency;
using namespace beeswax;

DEFINE_bool(use_planservice, false, "Use external planservice if true");
DEFINE_string(planservice_host, "localhost",
    "Host on which external planservice is running");
DEFINE_int32(planservice_port, 20000, "Port on which external planservice is running");
DEFINE_int32(fe_port, 21000, "port on which ImpalaService is exported");
DEFINE_int32(fe_service_threads, 10, "number of threads servicing ImpalaService requests");
DEFINE_int32(be_port, 22000, "port on which ImpalaInternalService is exported");
DEFINE_int32(be_service_threads, 10,
    "number of threads servicing ImpalaInternalService requests");
DEFINE_bool(load_catalog_at_startup, false, "if true, load all catalog data at startup");
DECLARE_bool(enable_jit);
DECLARE_int32(num_nodes);
DECLARE_int32(batch_size);
DECLARE_int32(fe_port);
DECLARE_int32(be_port);

namespace impala {

ThreadManager* fe_tm;
ThreadManager* be_tm;

// Execution state of a query. This captures everything necessary
// to convert row batches received by the coordinator into results
// we can return to the client. It also captures all state required for
// servicing query-related requests from the client.
// Thread safety: this class is generally not thread-safe, callers need to
// synchronize access explicitly via lock().
// To avoid deadlocks, the caller must *not* acquire query_exec_state_map_lock_
// while holding the exec state's lock.
class ImpalaServer::QueryExecState {
 public:
  QueryExecState(const TQueryRequest& request, const TUniqueId& query_id,
      ExecEnv* exec_env, const TResultSetMetadata& metadata)
    : request_(request), query_id_(query_id),
      exec_env_(exec_env),
      coord_(new Coordinator(exec_env_, &exec_stats_)),
      eos_(false),
      result_metadata_(metadata), current_batch_(NULL), current_batch_row_(0),
      current_row_(0) {}

  ~QueryExecState() {
  }

  // Set output_exprs_, based on exprs.
  Status PrepareSelectListExprs(RuntimeState* runtime_state,
      const vector<TExpr>& exprs, const RowDescriptor& row_desc);

  // Return at most max_rows from the current batch. If the entire current batch has
  // been returned, fetch another batch first.
  // Caller should verify that EOS has not be reached before calling.
  Status FetchRowsAsAscii(const int32_t max_rows,
      vector<string>* fetched_rows);

  // call coord_'s UpdateFragmentExecStatus()
  Status UpdateFragmentExecStatus(int backend_num, const TStatus& status,
      bool done, const TRuntimeProfileTree& profile);

  bool eos() { return eos_; }
  Coordinator* coord() const { return coord_.get(); }
  int current_row() const { return current_row_; }
  RuntimeState* local_runtime_state() { return &local_runtime_state_; }
  const TResultSetMetadata* result_metadata() { return &result_metadata_; }
  const TUniqueId& query_id() const { return query_id_; }
  mutex* lock() { return &lock_; }

 private:
  TQueryRequest request_;  // the original request
  TUniqueId query_id_;
  mutex lock_;  // protects all following fields
  ExecStats exec_stats_;
  ExecEnv* exec_env_;
  scoped_ptr<Coordinator> coord_;  // not set for queries w/o FROM
  // local runtime_state_ in case we don't have a coord_
  RuntimeState local_runtime_state_;
  vector<Expr*> output_exprs_;
  bool eos_;  // if true, there are no more rows to return

  TResultSetMetadata result_metadata_; // metadata for select query
  RowBatch* current_batch_; // the current row batch; only applicable if coord is set
  int current_batch_row_; // num of rows fetched within the current batch
  int current_row_; // num of rows that has been fetched for the entire query

  // Fetch the next row batch and store the results in current_batch_
  Status FetchNextBatch();

  // Evaluates output_exprs_ against at most max_rows in the current_batch_ starting
  // from current_batch_row_ and output the evaluated rows in Ascii form in
  // fetched_rows.
  Status ConvertRowBatchToAscii(const int32_t max_rows, vector<string>* fetched_rows);

  // Creates single result row in query_result by evaluating output_exprs_ without
  // a row (ie, the expressions are constants) and put it in query_results_;
  Status CreateConstantRowAsAscii(vector<string>* fetched_rows);

  // Creates a single string out of the ascii expr values and appends that string
  // to converted_rows.
  Status ConvertSingleRowToAscii(TupleRow* row, vector<string>* converted_rows);
};

Status ImpalaServer::QueryExecState::FetchRowsAsAscii(const int32_t max_rows,
    vector<string>* fetched_rows) {
  DCHECK(!eos_);
  if (coord_ == NULL) {
    // query without FROM clause: we return exactly one row
    return CreateConstantRowAsAscii(fetched_rows);
  } else {
    // Fetch the next batch if we've returned the current batch entirely
    if (current_batch_ == NULL || current_batch_row_ >= current_batch_->num_rows()) {
      RETURN_IF_ERROR(FetchNextBatch());
    }
    return ConvertRowBatchToAscii(max_rows, fetched_rows);
  }
}

Status ImpalaServer::QueryExecState::PrepareSelectListExprs(
    RuntimeState* runtime_state, const vector<TExpr>& exprs,
    const RowDescriptor& row_desc) {
  RETURN_IF_ERROR(
      Expr::CreateExprTrees(runtime_state->obj_pool(), exprs, &output_exprs_));
  for (int i = 0; i < output_exprs_.size(); ++i) {
    RETURN_IF_ERROR(Expr::Prepare(output_exprs_[i], runtime_state, row_desc));
  }
  return Status::OK;
}

Status ImpalaServer::QueryExecState::FetchNextBatch() {
  DCHECK(!eos_);
  RETURN_IF_ERROR(coord_->GetNext(&current_batch_, coord_->runtime_state()));

  current_batch_row_ = 0;
  eos_ = (current_batch_ == NULL);
  return Status::OK;
}

Status ImpalaServer::QueryExecState::ConvertRowBatchToAscii(const int32_t max_rows,
    vector<string>* fetched_rows) {
  fetched_rows->clear();
  if (current_batch_ == NULL) return Status::OK;

  // Convert the available rows, limited by max_rows
  int available = current_batch_->num_rows() - current_batch_row_;
  int fetched_count = min(available, max_rows);
  fetched_rows->reserve(fetched_count);
  for (int i = 0; i < fetched_count; ++i) {
    TupleRow* row = current_batch_->GetRow(current_batch_row_);
    RETURN_IF_ERROR(ConvertSingleRowToAscii(row, fetched_rows));
    ++current_row_;
    ++current_batch_row_;
  }
  return Status::OK;
}

Status ImpalaServer::QueryExecState::CreateConstantRowAsAscii(
    vector<string>* fetched_rows) {
  string out_str;
  fetched_rows->reserve(1);
  RETURN_IF_ERROR(ConvertSingleRowToAscii(NULL, fetched_rows));
  eos_ = true;
  ++current_row_;
  return Status::OK;
}

Status ImpalaServer::QueryExecState::ConvertSingleRowToAscii(TupleRow* row,
    vector<string>* converted_rows) {
  stringstream out_stream;
  out_stream.precision(ASCII_PRECISION);
  for (int i = 0; i < output_exprs_.size(); ++i) {
    // ODBC-187 - ODBC can only take "\t" as the delimiter
    out_stream << (i > 0 ? "\t" : "");
    output_exprs_[i]->PrintValue(row, &out_stream);
  }
  converted_rows->push_back(out_stream.str());
  return Status::OK;
}

Status ImpalaServer::QueryExecState::UpdateFragmentExecStatus(
    int backend_num, const TStatus& status,
    bool done, const TRuntimeProfileTree& profile) {
  return coord_->UpdateFragmentExecStatus(backend_num, status, done, profile);
}

// Execution state of a single plan fragment.
class ImpalaServer::FragmentExecState {
 public:
  FragmentExecState(const TUniqueId& query_id, const TUniqueId& fragment_id,
                    ExecEnv* exec_env,
                    const pair<string, int>& coord_hostport)
    : query_id_(query_id),
      fragment_id_(fragment_id),
      executor_(exec_env),
      client_cache_(exec_env->client_cache()),
      coord_hostport_(coord_hostport),
      done_(false) {
  }

  // Calling the d'tor releases all memory and closes all data streams
  // held by executor_.
  ~FragmentExecState() {
  }

  // Acquires lock_ and sets executor_'s cancel flag. Returns current
  // status of execution.
  Status Cancel();

  // Call Prepare() and create and initialize data sink.
  Status Prepare(const TPlanExecRequest& request, const TPlanExecParams& params);

  // Main loop of plan fragment execution.
  Status Exec();

  // Fill in 'params'. Acquires lock_.
  void GetStatus(TReportExecStatusParams* Params);

  // Call our coord's ReportFragmentStatus()
  Status ReportStatus();

  const TUniqueId& query_id() const { return query_id_; }
  const TUniqueId& fragment_id() const { return fragment_id_; }

  // the following all require having locked lock_
  // (Unfortunately, boost doesn't let us check whether a lock is being held,
  // otherwise we'd have something like AssertHeld(lock_) in there. How did they
  // manage to forget that?)
  bool done() const { return done_; }
  Status status() const { return exec_status_; }
  void set_status(const Status& status) { exec_status_ = status; }
  PlanFragmentExecutor* executor() { return &executor_; }
  RuntimeState* runtime_state() { return executor_.runtime_state(); }
  void set_exec_thread(thread* exec_thread) { exec_thread_.reset(exec_thread); }
  DataSink* sink() { return sink_.get(); }

 private:
  Status ExecInternal();

  TUniqueId query_id_;
  TUniqueId fragment_id_;
  PlanFragmentExecutor executor_;
  scoped_ptr<DataSink> sink_;
  BackendClientCache* client_cache_;

  // initiating coordinator to which we occasionally need to report back
  // (it's exported ImpalaInternalService)
  const pair<string, int> coord_hostport_;

  // the thread executing this plan fragment
  scoped_ptr<thread> exec_thread_;

  // protects access to all of the following fields
  mutex lock_;

  // if set to anything other than OK, execution has terminated w/ an error
  Status exec_status_;

  // if true, execution has terminated
  bool done_;

};

void ImpalaServer::FragmentExecState::GetStatus(TReportExecStatusParams* params) {
  params->protocol_version = ImpalaInternalServiceVersion::V1;
  params->__set_query_id(query_id_);
  params->__set_fragment_id(fragment_id_);
  lock_guard<mutex> l(lock_);
  exec_status_.SetTStatus(params);
  params->__set_done(done_);
  executor_.query_profile()->ToThrift(&params->profile);
  params->__isset.profile = true;
}

Status ImpalaServer::FragmentExecState::Cancel() {
  lock_guard<mutex> l(lock_);
  if (done_) return Status::OK;
  RETURN_IF_ERROR(exec_status_);
  executor_.runtime_state()->set_is_cancelled(true);
  return Status::OK;
}

Status ImpalaServer::FragmentExecState::Prepare(
    const TPlanExecRequest& request, const TPlanExecParams& params) {
  RETURN_IF_ERROR(executor_.Prepare(request, params));
  RETURN_IF_ERROR(DataSink::CreateDataSink(request,
      params, executor_.row_desc(), &sink_));
  RETURN_IF_ERROR(sink_->Init(executor_.runtime_state()));
  return Status::OK;
}

Status ImpalaServer::FragmentExecState::Exec() {
  return exec_status_ = ExecInternal();
}

Status ImpalaServer::FragmentExecState::ExecInternal() {
  VLOG_QUERY << "FragmentExecState::Exec(): fragment_id=" << fragment_id_;
  RETURN_IF_ERROR(executor_.Open());

  RowBatch* batch;
  while (true) {
    RETURN_IF_ERROR(executor_.GetNext(&batch));
    if (batch == NULL) break;
    VLOG_FILE << "ExecInternal: #rows=" << batch->num_rows();
    if (VLOG_ROW_IS_ON) {
      for (int i = 0; i < batch->num_rows(); ++i) {
        TupleRow* row = batch->GetRow(i);
        VLOG_ROW << PrintRow(row, executor_.row_desc());
      }
    }
    RETURN_IF_ERROR(
        sink_->Send(executor_.runtime_state(), batch));
    batch = NULL;
  }
  RETURN_IF_ERROR(sink_->Close(executor_.runtime_state()));

  return Status::OK;
}

Status ImpalaServer::FragmentExecState::ReportStatus() {
  ImpalaInternalServiceClient* coord;
  RETURN_IF_ERROR(client_cache_->GetClient(coord_hostport_, &coord));
  DCHECK(coord != NULL);

  TReportExecStatusParams params;
  params.protocol_version = ImpalaInternalServiceVersion::V1;
  params.__set_query_id(query_id_);
  params.__set_fragment_id(fragment_id_);
  exec_status_.SetTStatus(&params);
  params.__set_done(done_);
  executor_.query_profile()->ToThrift(&params.profile);
  params.__isset.profile = true;
  TReportExecStatusResult res;
  coord->ReportExecStatus(res, params);
  client_cache_->ReleaseClient(coord);
  return Status(res.status);
}

const char* ImpalaServer::SQLSTATE_SYNTAX_ERROR_OR_ACCESS_VIOLATION = "42000";
const char* ImpalaServer::SQLSTATE_GENERAL_ERROR = "HY000";
const char* ImpalaServer::SQLSTATE_OPTIONAL_FEATURE_NOT_IMPLEMENTED = "HYC00";
const int ImpalaServer::ASCII_PRECISION = 16; // print 16 digits for double/float

ImpalaServer::ImpalaServer(ExecEnv* exec_env)
  : exec_env_(exec_env) {
  if (!FLAGS_use_planservice) {
    JNIEnv* jni_env = getJNIEnv();
    // create instance of java class JniFrontend
    jclass fe_class = jni_env->FindClass("com/cloudera/impala/service/JniFrontend");
    jmethodID fe_ctor = jni_env->GetMethodID(fe_class, "<init>", "(Z)V");
    EXIT_IF_EXC(jni_env);
    create_query_exec_request_id_ =
        jni_env->GetMethodID(fe_class, "createQueryExecRequest", "([B)[B");
    EXIT_IF_EXC(jni_env);
    get_explain_plan_id_ = jni_env->GetMethodID(fe_class, "getExplainPlan",
        "([B)Ljava/lang/String;");
    EXIT_IF_EXC(jni_env);
    reset_catalog_id_ = jni_env->GetMethodID(fe_class, "resetCatalog", "()V");
    EXIT_IF_EXC(jni_env);
    get_hadoop_config_id_ = jni_env->GetMethodID(fe_class, "getHadoopConfigAsHtml",
        "()Ljava/lang/String;");
    EXIT_IF_EXC(jni_env);

    jboolean lazy = (FLAGS_load_catalog_at_startup ? false : true);
    jobject fe = jni_env->NewObject(fe_class, fe_ctor, lazy);
    EXIT_IF_EXC(jni_env);
    EXIT_IF_ERROR(JniUtil::LocalToGlobalRef(jni_env, fe, &fe_));
  } else {
    planservice_socket_.reset(new TSocket(FLAGS_planservice_host,
        FLAGS_planservice_port));
    planservice_transport_.reset(new TBufferedTransport(planservice_socket_));
    planservice_protocol_.reset(new TBinaryProtocol(planservice_transport_));
    planservice_client_.reset(new ImpalaPlanServiceClient(planservice_protocol_));
    planservice_transport_->open();
  }

  Webserver::PathHandlerCallback default_callback =
      bind<void>(mem_fn(&ImpalaServer::RenderHadoopConfigs), this, _1);
  exec_env->webserver()->RegisterPathHandler("/varz", default_callback);
}

void ImpalaServer::RenderHadoopConfigs(stringstream* output) {
  (*output) << "<h2>Hadoop Configuration</h2>";
  if (FLAGS_use_planservice) {
    (*output) << "Using external PlanService, no Hadoop configs available";
    return;
  }
  (*output) << "<pre>";
  JNIEnv* jni_env = getJNIEnv();
  jstring java_explain_string =
      static_cast<jstring>(jni_env->CallObjectMethod(fe_, get_hadoop_config_id_));
  RETURN_IF_EXC(jni_env);
  jboolean is_copy;
  const char *str = jni_env->GetStringUTFChars(java_explain_string, &is_copy);
  RETURN_IF_EXC(jni_env);
  (*output) << str;
  (*output) << "</pre>";
  jni_env->ReleaseStringUTFChars(java_explain_string, str);
  RETURN_IF_EXC(jni_env);
}

ImpalaServer::~ImpalaServer() {}

void ImpalaServer::executeAndWait(QueryHandle& query_handle, const Query& query,
  const LogContextId& client_ctx) {
  // Translate Beeswax Query to Impala's QueryRequest and then call executeAndWaitInternal
  TQueryRequest query_request;
  QueryToTQueryRequest(query, &query_request);

  TUniqueId query_id;
  Status status = ExecuteAndWaitInternal(query_request, &query_id);
  if (!status.ok()) {
    // raise Syntax error or access violation;
    // it's likely to be syntax/analysis error
    RaiseBeeswaxException(
        status.GetErrorMsg(), SQLSTATE_SYNTAX_ERROR_OR_ACCESS_VIOLATION);
  }

  // Convert TUnique back to QueryHandle
  TUniqueIdToQueryHandle(query_id, &query_handle);

  // If the input log context id is an empty string, then create a new number and
  // set it to _return. Otherwise, set _return with the input log context
  query_handle.log_context = client_ctx.empty() ? query_handle.id : client_ctx;
}

Status ImpalaServer::ExecuteAndWaitInternal(const TQueryRequest& request,
    TUniqueId* query_id) {
  TCreateQueryExecRequestResult result;
  RETURN_IF_ERROR(GetQueryExecRequest(request, &result));
  TQueryExecRequest& exec_request = result.queryExecRequest;
  // we always need at least one plan fragment
  DCHECK_GT(exec_request.fragment_requests.size(), 0);

  shared_ptr<QueryExecState> exec_state;
  {
    lock_guard<mutex> l(query_exec_state_map_lock_);

    // there shouldn't be an active query with that same id
    // (query_id is globally unique)
    QueryExecStateMap::iterator entry =
        query_exec_state_map_.find(exec_request.query_id);
    if (entry != query_exec_state_map_.end()) {
      stringstream ss;
      ss << "query id " << PrintId(exec_request.query_id)
         << " already exists";
      return Status(TStatusCode::INTERNAL_ERROR, ss.str());
    }

    *query_id = exec_request.query_id;

    exec_state.reset(
        new QueryExecState(request, exec_request.query_id, exec_env_,
                           result.resultSetMetadata));
    exec_state->lock()->lock();
    query_exec_state_map_.insert(make_pair(exec_request.query_id, exec_state));
  }

  {
    // at this point, we don't have the query_exec_state_map_lock_, but we do have
    // exec_state->lock()
    lock_guard<mutex> l(*exec_state->lock(), adopt_lock_t());

    if (!exec_request.fragment_requests[0].__isset.desc_tbl) {
      // query without a FROM clause
      DCHECK(!exec_request.fragment_requests[0].__isset.plan_fragment);
      exec_state->local_runtime_state()->Init(
          exec_request.query_id, exec_request.abort_on_error, exec_request.max_errors,
          FLAGS_enable_jit, NULL /* = we don't expect to be executing anything here */);
      return exec_state->PrepareSelectListExprs(exec_state->local_runtime_state(),
          exec_request.fragment_requests[0].output_exprs, RowDescriptor());
    }

    DCHECK_GT(exec_request.node_request_params.size(), 0);
    // the first node_request_params list contains exactly one TPlanExecParams
    // (it's meant for the coordinator fragment)
    DCHECK_EQ(exec_request.node_request_params[0].size(), 1);

    if (request.numNodes != 1) {
      // for distributed execution, for the time being we only expect one slave
      // fragment which will be executed by request.num_nodes - 1 nodes;
      // this will change once we support multiple phases (such as for DISTINCT)
      DCHECK_EQ(exec_request.fragment_requests.size(), 2);
      DCHECK_EQ(exec_request.node_request_params.size(), 2);
      DCHECK_LE(exec_request.node_request_params[1].size(), request.numNodes - 1);

      // set destinations to be port
      for (int i = 0; i < exec_request.node_request_params[1].size(); ++i) {
        DCHECK_EQ(exec_request.node_request_params[1][i].destinations.size(), 1);
        exec_request.node_request_params[1][i].destinations[0].port = FLAGS_be_port;
      }
    }

    // Set the batch size
    exec_request.batch_size = FLAGS_batch_size;
    for (int i = 0; i < exec_request.node_request_params.size(); ++i) {
      for (int j = 0; j < exec_request.node_request_params[i].size(); ++j) {
        exec_request.node_request_params[i][j].batch_size = FLAGS_batch_size;
      }
    }

    RETURN_IF_ERROR(exec_state->coord()->Exec(&exec_request));
    // release the lock on exec_state once execution is underway, a client
    // might want to cancel it while our caller is blocked
  }

  // block until results are ready
  RETURN_IF_ERROR(exec_state->coord()->Wait());
  {
    lock_guard<mutex> l(*exec_state->lock());
    RETURN_IF_ERROR(exec_state->PrepareSelectListExprs(
        exec_state->coord()->runtime_state(),
        exec_request.fragment_requests[0].output_exprs, exec_state->coord()->row_desc()));
  }

  return Status::OK;
}

Status ImpalaServer::GetQueryExecRequest(
    const TQueryRequest& request, TCreateQueryExecRequestResult* result) {
  if (!FLAGS_use_planservice) {
    // TODO: figure out if repeated calls to
    // JNI_GetCreatedJavaVMs()/AttachCurrentThread() are too expensive
    JNIEnv* jni_env = getJNIEnv();
    jbyteArray request_bytes;
    RETURN_IF_ERROR(SerializeThriftMsg(jni_env, &request, &request_bytes));
    jbyteArray result_bytes = static_cast<jbyteArray>(
        jni_env->CallObjectMethod(fe_, create_query_exec_request_id_, request_bytes));
    RETURN_ERROR_IF_EXC(jni_env, JniUtil::throwable_to_string_id());
    DeserializeThriftMsg(jni_env, result_bytes, result);
    // TODO: dealloc result_bytes?
    // TODO: figure out if we should detach here
    //RETURN_IF_JNIERROR(jvm_->DetachCurrentThread());
    return Status::OK;
  } else {
    // TODO: Use Beeswax.Query.configuration to pass in num_nodes
    planservice_client_->CreateQueryExecRequest(*result, request.stmt, FLAGS_num_nodes);
    return Status::OK;
  }
}

Status ImpalaServer::GetExplainPlan(
    const TQueryRequest& query_request, string* explain_string) {
  if (!FLAGS_use_planservice) {
    // TODO: figure out if repeated calls to
    // JNI_GetCreatedJavaVMs()/AttachCurrentThread() are too expensive
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
  } else {
    try {
      planservice_client_->GetExplainString(*explain_string, query_request.stmt, 0);
    } catch (TException& e) {
      return Status(e.what());
    }
    return Status::OK;
  }
}

void ImpalaServer::ResetCatalog(impala::TStatus& status) {
  status.status_code = TStatusCode::INTERNAL_ERROR;
  if (!FLAGS_use_planservice) {
    JNIEnv* jni_env = getJNIEnv();
    jni_env->CallObjectMethod(fe_, reset_catalog_id_);
    RETURN_IF_EXC(jni_env);
  } else {
    try {
      planservice_client_->RefreshMetadata();
    } catch (TTransportException& e) {
      stringstream msg;
      msg << "RefreshMetadata rpc failed: " << e.what();
      LOG(ERROR) << msg.str();
      // TODO: different error code here?
      status.error_msgs.push_back(msg.str());
      return;
    }
  }
  status.status_code = TStatusCode::OK;
}

void ImpalaServer::Cancel(impala::TStatus& status,
    const beeswax::QueryHandle& query_id) {
  // TODO: implement
}

void ImpalaServer::explain(QueryExplanation& query_explanation, const Query& query) {
  // Translate Beeswax Query to Impala's QueryRequest and then set the explain plan bool
  // before shipping to FE
  TQueryRequest query_request;
  QueryToTQueryRequest(query, &query_request);

  Status status = GetExplainPlan(query_request, &query_explanation.textual);
  if (!status.ok()) {
    // raise Syntax error or access violation; this is the closest.
    RaiseBeeswaxException(
        status.GetErrorMsg(), SQLSTATE_SYNTAX_ERROR_OR_ACCESS_VIOLATION);
  }
  query_explanation.__isset.textual = true;
  VLOG_QUERY << "explain():\nstmt=" << query_request.stmt
             << "\nplan: " << query_explanation.textual;
}

void ImpalaServer::fetch(Results& query_results, const QueryHandle& query_handle,
    const bool start_over, const int32_t fetch_size) {
  if (start_over) {
    // We can't start over. Raise "Optional feature not implemented"
    RaiseBeeswaxException(
        "Does not support start over", SQLSTATE_OPTIONAL_FEATURE_NOT_IMPLEMENTED);
  }

  // Convert QueryHandle to TUniqueId and get the query exec state.
  TUniqueId query_id;
  QueryHandleToTUniqueId(query_handle, &query_id);
  VLOG_ROW << "ImpalaServer::fetch query_id=" << PrintId(query_id)
      << " fetch_size=" << fetch_size;
  shared_ptr<QueryExecState> exec_state = GetQueryExecState(query_id);
  if (exec_state == NULL) {
    RaiseBeeswaxException("Invalid query handle", SQLSTATE_GENERAL_ERROR);
  }

  Status fetch_rows_status;
  bool eos;
  {
    // make sure we release the lock on exec_state if we see any error
    lock_guard<mutex> l(*exec_state->lock(), adopt_lock_t());

    // ODBC-190: set Beeswax's Results.columns to work around bug ODBC-190;
    // TODO: remove the block of code when ODBC-190 is resolved.
    const TResultSetMetadata* result_metadata = exec_state->result_metadata();
    query_results.columns.resize(result_metadata->columnDescs.size());
    for (int i = 0; i < result_metadata->columnDescs.size(); ++i) {
      // TODO: As of today, the ODBC driver does not support boolean and timestamp data
      // type but it should. This is tracked by ODBC-189. We should verify that our
      // boolean and timestamp type are correctly recognized when ODBC-189 is closed.
      TPrimitiveType::type col_type = result_metadata->columnDescs[i].columnType;
      query_results.columns[i] = TypeToOdbcString(ThriftToType(col_type));
    }
    query_results.__isset.columns = true;

    // Results are always ready because we're blocking.
    query_results.__set_ready(true);
    // It's likely that ODBC doesn't care about start_row, but set it anyway for
    // completeness.
    query_results.__set_start_row(exec_state->current_row() + 1);
    if (exec_state->eos()) {
      // if we've hit EOS, return no rows and set has_more to false.
      query_results.data.clear();
    } else {
      fetch_rows_status = exec_state->FetchRowsAsAscii(fetch_size, &query_results.data);
    }
    eos = exec_state->eos();
  }

  // release lock before potentially getting the map lock
  // do we have something like DCHECK(exec_state->lock()->AssertNotHeld())?
  if (!fetch_rows_status.ok()) {
    // clean up/remove ExecState
    lock_guard<mutex> l(query_exec_state_map_lock_);
    QueryExecStateMap::iterator entry = query_exec_state_map_.find(query_id);
    if (entry != query_exec_state_map_.end()) {
      query_exec_state_map_.erase(entry);
    } else {
      // Without concurrent fetch, this shouldn't happen.
      LOG(WARNING) << "query id not found during error clean up: " << PrintId(query_id);
    }

    // use general error for runtime exceptions
    RaiseBeeswaxException(fetch_rows_status.GetErrorMsg(), SQLSTATE_GENERAL_ERROR);
  }

  query_results.__set_has_more(!eos);
  query_results.__isset.data = true;
}

void ImpalaServer::get_results_metadata(ResultsMetadata& results_metadata,
    const QueryHandle& handle) {
  // Convert QueryHandle to TUniqueId and get the query exec state.
  TUniqueId query_id;
  QueryHandleToTUniqueId(handle, &query_id);
  VLOG_QUERY << "ImpalaServer::get_results_metadata: query_id=" << PrintId(query_id);
  shared_ptr<QueryExecState> exec_state = GetQueryExecState(query_id);
  if (exec_state.get() == NULL) {
    RaiseBeeswaxException("Invalid query handle", SQLSTATE_GENERAL_ERROR);
  }

  {
    // make sure we release the lock on exec_state if we see any error
    lock_guard<mutex> l(*exec_state->lock(), adopt_lock_t());

    // Convert TResultSetMetadata to Beeswax.ResultsMetadata
    const TResultSetMetadata* result_set_md = exec_state->result_metadata();
    results_metadata.__isset.schema = true;
    results_metadata.schema.__isset.fieldSchemas = true;
    results_metadata.schema.fieldSchemas.resize(result_set_md->columnDescs.size());
    for (int i = 0; i < results_metadata.schema.fieldSchemas.size(); ++i) {
      TPrimitiveType::type col_type = result_set_md->columnDescs[i].columnType;
      results_metadata.schema.fieldSchemas[i].__set_type(
          TypeToOdbcString(ThriftToType(col_type)));

      // Fill column name
      results_metadata.schema.fieldSchemas[i].__set_name(
          result_set_md->columnDescs[i].columnName);
    }
  }

  // ODBC-187 - ODBC can only take "\t" as the delimiter and ignores whatever is set here.
  results_metadata.__set_delim("\t");

  // results_metadata.table_dir and in_tablename are not applicable.
}

void ImpalaServer::close(const QueryHandle& handle) {
  // Convert QueryHandle to TUniqueId and get the query exec state.
  TUniqueId query_id;
  QueryHandleToTUniqueId(handle, &query_id);
  VLOG_QUERY << "ImpalaServer::close: query_id=" << PrintId(query_id);

  // TODO: use timeout to get rid of unwanted exec_state.
  lock_guard<mutex> l(query_exec_state_map_lock_);
  QueryExecStateMap::iterator entry = query_exec_state_map_.find(query_id);
  if (entry != query_exec_state_map_.end()) {
    query_exec_state_map_.erase(entry);
  } else {
    VLOG_QUERY << "ImpalaServer::close invalid handle";
    RaiseBeeswaxException("Invalid query handle", SQLSTATE_GENERAL_ERROR);
  }
}

// Always in a finished stated.
// We only do sync query; when we return, we've results already. FINISHED actually means
// result is ready, not the query has finished. (Although for Hive, result won't be ready
// until the query is finished.)
QueryState::type ImpalaServer::get_state(const QueryHandle& handle) {
  return QueryState::FINISHED;
}

void ImpalaServer::echo(string& echo_string, const string& input_string) {
  echo_string = input_string;
}

void ImpalaServer::clean(const LogContextId& log_context) {
}


void ImpalaServer::query(QueryHandle& query_handle, const Query& query) {
}

void ImpalaServer::dump_config(string& config) {
  config = "";
}

void ImpalaServer::get_log(string& log, const LogContextId& context) {
  log = "logs are distributed. Skip it for now";
}

void ImpalaServer::get_default_configuration(
    vector<ConfigVariable> & configurations, const bool include_hadoop) {
}

void ImpalaServer::QueryToTQueryRequest(const Query& query, TQueryRequest* request) {
  request->numNodes = FLAGS_num_nodes;
  request->returnAsAscii = true;
  request->stmt = query.query;
}

void ImpalaServer::TUniqueIdToQueryHandle(const TUniqueId& query_id,
    QueryHandle* handle) {
  stringstream stringstream;
  stringstream << query_id.hi << " " << query_id.lo;
  handle->id = stringstream.str();
}

void ImpalaServer::QueryHandleToTUniqueId(const QueryHandle& handle,
    TUniqueId* query_id) {
  char_separator<char> sep(" ");
  tokenizer< char_separator<char> > tokens(handle.id, sep);
  int i = 0;
  BOOST_FOREACH(string t, tokens) {
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

void ImpalaServer::RaiseBeeswaxException(const string& msg, const char* sql_state) {
  BeeswaxException exc;
  exc.__set_message(msg);
  exc.__set_SQLState(sql_state);
  throw exc;
}

inline ImpalaServer::FragmentExecState* ImpalaServer::GetFragmentExecState(
    const TUniqueId& fragment_id) {
  lock_guard<mutex> l(fragment_exec_state_map_lock_);
  FragmentExecStateMap::iterator i = fragment_exec_state_map_.find(fragment_id);
  if (i == fragment_exec_state_map_.end()) {
    return NULL;
  } else {
    return i->second;
  }
}

inline shared_ptr<ImpalaServer::QueryExecState> ImpalaServer::GetQueryExecState(
    const TUniqueId& query_id) {
  lock_guard<mutex> l(query_exec_state_map_lock_);
  QueryExecStateMap::iterator i = query_exec_state_map_.find(query_id);
  if (i == query_exec_state_map_.end()) {
    return shared_ptr<QueryExecState>();
  } else {
    i->second->lock()->lock();
    return i->second;
  }
}

void ImpalaServer::ExecPlanFragment(
    TExecPlanFragmentResult& return_val, const TExecPlanFragmentParams& params) {
  StartPlanFragmentExecution(
      params.request, params.params, params.coord).SetTStatus(&return_val);
}

void ImpalaServer::ReportExecStatus(
    TReportExecStatusResult& return_val, const TReportExecStatusParams& params) {
  // TODO: implement something more efficient here, we're currently creating
  // a shared_ptr, acquiring/releasing the map lock and doing a map lookup for
  // every report (assign each query a local int32_t id and use that to index into a
  // vector of QueryExecStates, w/o lookup or locking?)
  shared_ptr<QueryExecState> query_exec_state = GetQueryExecState(params.query_id);
  if (query_exec_state.get() == NULL) {
    return_val.status.status_code = TStatusCode::INTERNAL_ERROR;
    stringstream str;
    str << "unknown query id: " << params.query_id;
    return_val.status.error_msgs.push_back(str.str());
    return;
  }
  // query_exec_state's coordinator is thread-safe, no need to hold the lock
  query_exec_state->lock()->unlock();
  query_exec_state->UpdateFragmentExecStatus(
      params.backend_num, params.status, params.done, params.profile)
      .SetTStatus(&return_val);
}

void ImpalaServer::CancelPlanFragment(
    TCancelPlanFragmentResult& return_val, const TCancelPlanFragmentParams& params) {
  FragmentExecState* exec_state = GetFragmentExecState(params.fragment_id);
  if (exec_state == NULL) {
    stringstream str;
    str << "unknown fragment id: " << params.fragment_id;
    Status status(TStatusCode::INTERNAL_ERROR, str.str());
    status.SetTStatus(&return_val);
    return;
  }
  // we only initiate cancellation here, the map entry as well as the exec state
  // are removed when fragment execution terminates (which is at present still
  // running in exec_state->exec_thread_)
  exec_state->Cancel().SetTStatus(&return_val);
}

void ImpalaServer::TransmitData(
    TTransmitDataResult& return_val, const TTransmitDataParams& params) {
  // TODO: fix Thrift so we can simply take ownership of thrift_batch instead
  // of having to copy its data
  if (params.row_batch.num_rows > 0) {
    Status status = exec_env_->stream_mgr()->AddData(
        params.dest_fragment_id, params.dest_node_id, params.row_batch);
    status.SetTStatus(&return_val);
    if (!status.ok()) {
      // should we close the channel here as well?
      return;
    }
  }

  if (params.eos) {
    exec_env_->stream_mgr()->CloseStream(
        params.dest_fragment_id, params.dest_node_id).SetTStatus(&return_val);
  }
}

Status ImpalaServer::StartPlanFragmentExecution(
    const TPlanExecRequest& request, const TPlanExecParams& params,
    const THostPort& coord_hostport) {
  if (!request.data_sink.__isset.dataStreamSink) {
    return Status("missing data stream sink");
  }

  auto_ptr<FragmentExecState> new_exec_state(
      new FragmentExecState(
        request.query_id, request.fragment_id, exec_env_,
        make_pair(coord_hostport.host, coord_hostport.port)));
  RETURN_IF_ERROR(new_exec_state->Prepare(request, params));

  FragmentExecState* exec_state = new_exec_state.get();
  {
    lock_guard<mutex> l(fragment_exec_state_map_lock_);
    // register exec_state before starting exec thread
    fragment_exec_state_map_[request.fragment_id] = new_exec_state.release();
  }

  // execute plan fragment in new thread
  // TODO: manage threads via global thread pool
  exec_state->set_exec_thread(
      new thread(&ImpalaServer::RunExecPlanFragment, this, exec_state));
  return Status::OK;
}

void ImpalaServer::RunExecPlanFragment(FragmentExecState* exec_state) {
  // don't report status if this fragment was cancelled, the coord may not know
  // about it anymore
  if (!exec_state->Exec().IsCancelled()) exec_state->ReportStatus();

  // we're done with this plan fragment
  {
    lock_guard<mutex> l(fragment_exec_state_map_lock_);
    FragmentExecStateMap::iterator i =
        fragment_exec_state_map_.find(exec_state->fragment_id());
    delete i->second;
    fragment_exec_state_map_.erase(i);
  }
}

ImpalaServer* CreateImpalaServer(ExecEnv* exec_env, int fe_port, int be_port,
    ThriftServer** fe_server, ThriftServer** be_server) {
  DCHECK((fe_port == 0) == (fe_server == NULL));
  DCHECK((be_port == 0) == (be_server == NULL));

  shared_ptr<ImpalaServer> handler(new ImpalaServer(exec_env));
  // TODO: do we want a BoostThreadFactory?
  // TODO: we want separate thread factories here, so that fe requests can't starve
  // be requests
  shared_ptr<ThreadFactory> thread_factory(new PosixThreadFactory());

  if (fe_port != 0 && fe_server != NULL) {
    shared_ptr<TProcessor> fe_processor(new ImpalaServiceProcessor(handler));
    *fe_server = new ThriftServer(fe_processor, fe_port, FLAGS_fe_service_threads);

    LOG(INFO) << "ImpalaService listening on " << fe_port;
  }

  if (be_port != 0 && be_server != NULL) {
    shared_ptr<TProcessor> be_processor(new ImpalaInternalServiceProcessor(handler));
    *be_server = new ThriftServer(be_processor, be_port, FLAGS_be_service_threads);

    LOG(INFO) << "ImpalaInternalService listening on " << be_port;
  }

  return handler.get();
}

}
