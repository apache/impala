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
#include <transport/TSocket.h>
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
#include "util/debug-util.h"
#include "service/backend-service.h"
#include "gen-cpp/ImpalaService.h"
#include "gen-cpp/ImpalaBackendService.h"
#include "gen-cpp/ImpalaPlanService.h"
#include "gen-cpp/ImpalaPlanService_types.h"
#include "gen-cpp/JavaConstants_constants.h"
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
DECLARE_bool(use_planservice);
DECLARE_string(planservice_host);
DECLARE_int32(planservice_port);
DECLARE_int32(num_nodes);
DECLARE_int32(batch_size);

namespace impala {

const char* ImpalaService::SQLSTATE_SYNTAX_ERROR_OR_ACCESS_VIOLATION = "42000";
const char* ImpalaService::SQLSTATE_GENERAL_ERROR = "HY000";
const char* ImpalaService::SQLSTATE_OPTIONAL_FEATURE_NOT_IMPLEMENTED = "HYC00";
const int ImpalaService::ASCII_PRECISION = 16; // print 16 digits for double/float

ImpalaService::ImpalaService(ExecEnv* exec_env, int port)
  : port_(port),
    exec_env_(exec_env) {
}

ImpalaService::~ImpalaService() {}

void ImpalaService::Init(JNIEnv* env) {
  if (!FLAGS_use_planservice) {
    // create instance of java class Frontend
    jclass fe_class = env->FindClass("com/cloudera/impala/service/JniFrontend");
    jmethodID fe_ctor = env->GetMethodID(fe_class, "<init>", "()V");
    EXIT_IF_EXC(env);
    get_query_request_result_id_ =
        env->GetMethodID(fe_class, "getQueryRequestResult", "([B)[B");
    EXIT_IF_EXC(env);
    get_explain_plan_id_ = env->GetMethodID(fe_class, "getExplainPlan",
        "([B)Ljava/lang/String;");
    EXIT_IF_EXC(env);
    reset_catalog_id_ = env->GetMethodID(fe_class, "resetCatalog", "()V");
    EXIT_IF_EXC(env);
    jobject fe = env->NewObject(fe_class, fe_ctor);
    EXIT_IF_EXC(env);
    EXIT_IF_ERROR(JniUtil::LocalToGlobalRef(env, fe, &fe_));
  } else {
    planservice_socket_.reset(new TSocket(FLAGS_planservice_host,
        FLAGS_planservice_port));
    planservice_transport_.reset(new TBufferedTransport(planservice_socket_));
    planservice_protocol_.reset(new TBinaryProtocol(planservice_transport_));
    planservice_client_.reset(new ImpalaPlanServiceClient(planservice_protocol_));
    planservice_transport_->open();
  }
}

Status ImpalaService::executeAndWaitInternal(const TQueryRequest& request,
    TUniqueId* query_id) {
  TQueryRequestResult request_result;
  RETURN_IF_ERROR(GetQueryRequestResult(request, &request_result));
  TQueryExecRequest& exec_request = request_result.queryExecRequest;
  // we always need at least one plan fragment
  DCHECK_GT(exec_request.fragmentRequests.size(), 0);

  ExecState* exec_state = NULL;
  {
    lock_guard<mutex> l(exec_state_map_lock_);

    // there shouldn't be an active query with that same id
    // (queryId is globally unique)
    ExecStateMap::iterator entry = exec_state_map_.find(exec_request.queryId);
    if (entry != exec_state_map_.end()) {
      stringstream ss;
      ss << "internal error: query id (" << PrintId(exec_request.queryId)
         << ") already exists";
      return Status(ss.str());
    }

    *query_id = exec_request.queryId;

    exec_state = new ExecState(request, exec_env_, request_result.resultSetMetadata);
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

  // Set the batch size
  exec_request.batchSize = FLAGS_batch_size;
  for (int i = 0; i < exec_request.nodeRequestParams.size(); ++i) {
    for (int j = 0; j < exec_request.nodeRequestParams[i].size(); ++j) {
      exec_request.nodeRequestParams[i][j].batchSize = FLAGS_batch_size;
    }
  }

  RETURN_IF_ERROR(exec_state->coord()->Exec(&exec_request));
  RETURN_IF_ERROR(exec_state->PrepareSelectListExprs(exec_state->coord()->runtime_state(),
        exec_request.fragmentRequests[0].outputExprs, exec_state->coord()->row_desc()));

  return Status::OK;
}

const TUniqueId& ImpalaService::ExecState::query_id() {
  return (coord_ != NULL) ? coord_->runtime_state()->query_id() :
      local_runtime_state_.query_id();
}

void ImpalaService::ExecState::ResetCoordinator() {
  coord_.reset(new Coordinator(exec_env_, &exec_stats_));
}

Status ImpalaService::ExecState::FetchRowsAsAscii(const int32_t max_rows,
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

Status ImpalaService::ExecState::PrepareSelectListExprs(
    RuntimeState* runtime_state, const vector<TExpr>& exprs,
    const RowDescriptor& row_desc) {
  RETURN_IF_ERROR(
      Expr::CreateExprTrees(runtime_state->obj_pool(), exprs, &output_exprs_));
  for (int i = 0; i < output_exprs_.size(); ++i) {
    Expr::Prepare(output_exprs_[i], runtime_state, row_desc);
  }
  return Status::OK;
}

Status ImpalaService::ExecState::FetchNextBatch() {
  DCHECK(!eos_);
  RETURN_IF_ERROR(coord_->GetNext(&current_batch_, coord_->runtime_state()));

  current_batch_row_ = 0;
  eos_ = (current_batch_ == NULL);
  return Status::OK;
}

Status ImpalaService::ExecState::ConvertRowBatchToAscii(const int32_t max_rows,
    vector<string>* fetched_rows) {
  if (current_batch_ == NULL) {
    fetched_rows->resize(0);
    return Status::OK;
  }

  // Convert the available rows, limited by max_rows
  int available = current_batch_->num_rows() - current_batch_row_;
  int fetched_count = (available < max_rows) ? available : max_rows;
  fetched_rows->clear();
  fetched_rows->reserve(fetched_count);
  for (int i = 0; i < fetched_count; ++i) {
    TupleRow* row = current_batch_->GetRow(current_batch_row_);
    RETURN_IF_ERROR(ConvertSingleRowToAscii(row, fetched_rows));
    ++current_row_;
    ++current_batch_row_;
  }
  return Status::OK;
}

Status ImpalaService::ExecState::CreateConstantRowAsAscii(vector<string>* fetched_rows) {
  string out_str;
  fetched_rows->reserve(1);
  RETURN_IF_ERROR(ConvertSingleRowToAscii(NULL, fetched_rows));
  eos_ = true;
  ++current_row_;
  return Status::OK;
}

Status ImpalaService::ExecState::ConvertSingleRowToAscii(TupleRow* row,
    vector<string>* converted_rows) {
  stringstream out_stream;
  out_stream.precision(ASCII_PRECISION);
  for (int i = 0; i < output_exprs_.size(); ++i) {
    // ODBC-187 - ODBC can only take "\t" as the delimiter
    out_stream << (i > 0 ? "\t" : "");
    output_exprs_[i]->PrintValue(row, &out_stream);
  }
  VLOG_ROW << "query_id(" << PrintId(query_id()) << "): "
      << "returned row as Ascii: " << out_stream.str();
  converted_rows->push_back(out_stream.str());
  return Status::OK;
}

Status ImpalaService::GetQueryRequestResult(
    const TQueryRequest& request, TQueryRequestResult* request_result) {
  if (!FLAGS_use_planservice) {
    // TODO: figure out if repeated calls to JNI_GetCreatedJavaVMs()/AttachCurrentThread()
    // are too expensive
    JNIEnv* jni_env = getJNIEnv();
    jbyteArray request_bytes;
    RETURN_IF_ERROR(SerializeThriftMsg(jni_env, &request, &request_bytes));
    jbyteArray exec_request_bytes = static_cast<jbyteArray>(
        jni_env->CallObjectMethod(fe_, get_query_request_result_id_, request_bytes));
    RETURN_ERROR_IF_EXC(jni_env, JniUtil::throwable_to_string_id());
    DeserializeThriftMsg(jni_env, exec_request_bytes, request_result);
  // TODO: dealloc exec_request_bytes?
  // TODO: figure out if we should detach here
  //RETURN_IF_JNIERROR(jvm_->DetachCurrentThread());
  return Status::OK;
  } else {
    // TODO: Use Beeswax.Query.configuration to pass in num_nodes
    planservice_client_->GetQueryRequestResult(*request_result, request.stmt,
        FLAGS_num_nodes);
    return Status::OK;
  }
}

Status ImpalaService::GetExplainPlan(
    const TQueryRequest& query_request, string* explain_string) {
  if (!FLAGS_use_planservice) {
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
  } else {
    try {
      planservice_client_->GetExplainString(*explain_string, query_request.stmt, 0);
    } catch (TException& e) {
      return Status(e.what());
    }
    return Status::OK;
  }
}

void ImpalaService::ResetCatalog(impala::TStatus& status) {
  status.status_code = TStatusCode::INTERNAL_ERROR;
  if (!FLAGS_use_planservice) {
    JNIEnv* jni_env = getJNIEnv();
    jni_env->CallObjectMethod(fe_, reset_catalog_id_);
    RETURN_IF_EXC(jni_env);
  } else {
    planservice_client_->RefreshMetadata();
  }
  status.status_code = TStatusCode::OK;
}

void ImpalaService::Cancel(impala::TStatus& status,
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
  VLOG_QUERY << "ImpalaService::executeAndWait: " << queryRequest.stmt;

  TUniqueId query_id;
  Status status = executeAndWaitInternal(queryRequest, &query_id);
  if (!status.ok()) {
    // raise Syntax error or access violation;
    // it's likely to be synatx/sematics error
    BeeswaxException exc;
    exc.message = status.GetErrorMsg();
    exc.__isset.message = true;
    exc.SQLState = SQLSTATE_SYNTAX_ERROR_OR_ACCESS_VIOLATION;
    exc.__isset.SQLState = true;
    throw exc;
  }
  VLOG_QUERY << "ImpalaService::executeAndWait: query_id:" << PrintId(query_id);

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
  VLOG_QUERY << "ImpalaService::explain: " << queryRequest.stmt;

  Status status = GetExplainPlan(queryRequest, &query_explanation.textual);
  if (!status.ok()) {
    // raise Syntax error or access violation; this is the closest.
    BeeswaxException exc;
    exc.message = status.GetErrorMsg();
    exc.__isset.message = true;
    exc.SQLState = SQLSTATE_SYNTAX_ERROR_OR_ACCESS_VIOLATION;
    exc.__isset.SQLState = true;
    throw exc;
  }
  query_explanation.__isset.textual = true;
  VLOG_QUERY << "ImpalaService::explain plan: " << query_explanation.textual;
}

ImpalaService::ExecState* ImpalaService::GetExecState(const TUniqueId& unique_id) {
  lock_guard<mutex> l(exec_state_map_lock_);
  ExecStateMap::iterator entry = exec_state_map_.find(unique_id);
  if (entry == exec_state_map_.end()) {
    return NULL;
  }
  return entry->second;
}

void ImpalaService::fetch(Results& query_results, const QueryHandle& query_handle,
    const bool start_over, const int32_t fetch_size) {
  if (start_over) {
    // We can't start over. Raise "Optional feature not implemented"
    BeeswaxException exc;
    exc.message = "Does not support start over";
    exc.__isset.message = true;
    exc.SQLState = SQLSTATE_OPTIONAL_FEATURE_NOT_IMPLEMENTED;
    exc.__isset.SQLState = true;
    throw exc;
  }

  // Convert QueryHandle to TUniqueId and get the query exec state.
  TUniqueId query_id;
  QueryHandleToTUniqueId(query_handle, &query_id);
  VLOG_ROW << "ImpalaService::fetch query_id:" << PrintId(query_id)
      << ", fetch_size:" << fetch_size;
  ExecState* exec_state = GetExecState(query_id);
  if (exec_state == NULL) RaiseBeeswaxHandleNotFoundException();


  // ODBC-190: set Beeswax's Results.columns to work around bug ODBC-190;
  // TODO: remove the block of code when ODBC-190 is resolved.
  const TResultSetMetadata* result_metadata = exec_state->result_metadata();
  query_results.columns.resize(result_metadata->columnDescs.size());
  for (int i = 0; i < result_metadata->columnDescs.size(); ++i) {
    // TODO: As of today, the ODBC driver does not support boolean and timestamp data
    // type but it should. This is tracked by ODBC-189. We should verify that our boolean
    // and timestamp type are correctly recognized when ODBC-189 is closed.
    TPrimitiveType::type col_type = result_metadata->columnDescs[i].columnType;
    query_results.columns[i] = TypeToOdbcString(ThriftToType(col_type));
  }
  query_results.__isset.columns = true;

  // Results are always ready because we're blocking.
  query_results.ready = true;
  // It's likely that ODBC doesn't care about start_row, but set it anyway for
  // completeness.
  query_results.start_row = exec_state->current_row() + 1;
  if (exec_state->eos()) {
    // if we've hit EOS, return no rows and set has_more to false.
    query_results.data.clear();
  } else {
    Status status = exec_state->FetchRowsAsAscii(fetch_size, &query_results.data);
    if (!status.ok()) {
      // use General Error for all kinds of run time exception
      BeeswaxException exc;
      exc.message = status.GetErrorMsg();
      exc.__isset.message = true;
      exc.SQLState = SQLSTATE_GENERAL_ERROR;
      exc.__isset.SQLState = true;

      // clean up/remove ExecState
      lock_guard<mutex> l(exec_state_map_lock_);
      ExecStateMap::iterator entry = exec_state_map_.find(query_id);
      if (entry != exec_state_map_.end()) {
        exec_state_map_.erase(entry);
      } else {
        // Without concurrent fetch, this shouldn't happen.
        LOG(WARNING) << "query id not found during error clean up: " << PrintId(query_id);
      }
      throw exc;
    }
  }
  query_results.has_more = !exec_state->eos();
  query_results.__isset.ready = true;
  query_results.__isset.start_row = true;
  query_results.__isset.data = true;
  query_results.__isset.has_more = true;
}

void ImpalaService::get_results_metadata(ResultsMetadata& results_metadata,
    const QueryHandle& handle) {
  // Convert QueryHandle to TUniqueId and get the query exec state.
  TUniqueId query_id;
  QueryHandleToTUniqueId(handle, &query_id);
  VLOG_QUERY << "ImpalaService::get_results_metadata: query_id:" << PrintId(query_id);
  ExecState* exec_state = GetExecState(query_id);
  if (exec_state == NULL) RaiseBeeswaxHandleNotFoundException();

  // Convert TResultSetMetadata to Beeswax.ResultsMetadata
  const TResultSetMetadata* result_metadata = exec_state->result_metadata();
  results_metadata.schema.fieldSchemas.resize(result_metadata->columnDescs.size());
  for (int i = 0; i < results_metadata.schema.fieldSchemas.size(); ++i) {
    TPrimitiveType::type col_type = result_metadata->columnDescs[i].columnType;
    results_metadata.schema.fieldSchemas[i].type =
        TypeToOdbcString(ThriftToType(col_type));
    results_metadata.schema.fieldSchemas[i].__isset.type = true;

    // Fill column name
    results_metadata.schema.fieldSchemas[i].name =
        result_metadata->columnDescs[i].columnName;
    results_metadata.schema.fieldSchemas[i].__isset.name = true;
  }
  results_metadata.schema.__isset.fieldSchemas = true;
  results_metadata.__isset.schema = true;

  // ODBC-187 - ODBC can only take "\t" as the delimiter and ignores whatever is set here.
  results_metadata.delim = "\t";
  results_metadata.__isset.delim = true;

  // results_metadata.table_dir and in_tablename are not applicable.
}

void ImpalaService::close(const QueryHandle& handle) {
  // Convert QueryHandle to TUniqueId and get the query exec state.
  TUniqueId query_id;
  QueryHandleToTUniqueId(handle, &query_id);
  VLOG_QUERY << "ImpalaService::close: query_id:" << PrintId(query_id);

  // TODO: use timeout to get rid of unwanted exec_state.
  lock_guard<mutex> l(exec_state_map_lock_);
  ExecStateMap::iterator entry = exec_state_map_.find(query_id);
  if (entry != exec_state_map_.end()) {
    exec_state_map_.erase(entry);
  } else {
    VLOG_QUERY << "ImpalaService::close invalid handle";
    RaiseBeeswaxHandleNotFoundException();
  }
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
  request->numNodes = FLAGS_num_nodes;
  request->returnAsAscii = true;
  request->stmt = query.query;
}

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

void ImpalaService::RaiseBeeswaxHandleNotFoundException() {
  // raise general error for invalid query handle
  BeeswaxException exc;
  exc.message = "Invalid query handle";
  exc.__isset.message = true;
  exc.SQLState = SQLSTATE_GENERAL_ERROR;
  exc.__isset.SQLState = true;
  throw exc;
}
}
