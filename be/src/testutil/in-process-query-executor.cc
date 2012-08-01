// (c) 2011 Cloudera, Inc. All rights reserved.

#include "testutil/in-process-query-executor.h"

#include <stdlib.h>  // for system()
#include <unistd.h>  // for sleep()
#include <sys/stat.h>
#include <iostream>
#include <vector>
#include <boost/algorithm/string.hpp>
#include <boost/scoped_ptr.hpp>
#include <boost/thread.hpp>
// reversing the include order of the following two results in linker errors:
// undefined reference to `fLI::FLAGS_v'
// TODO: figure out why
#include <glog/logging.h>
#include <gflags/gflags.h>

#include <protocol/TBinaryProtocol.h>
#include <protocol/TDebugProtocol.h>
#include <transport/TSocket.h>
#include <transport/TTransportUtils.h>

#include "codegen/llvm-codegen.h"
#include "common/object-pool.h"
#include "common/status.h"
#include "exec/exec-node.h"
#include "exec/exec-stats.h"
#include "exec/scan-node.h"
#include "exprs/expr.h"
#include "runtime/coordinator.h"
#include "runtime/descriptors.h"
#include "runtime/raw-value.h"
#include "runtime/row-batch.h"
#include "runtime/runtime-state.h"
#include "runtime/plan-fragment-executor.h"
#include "runtime/exec-env.h"
#include "util/debug-util.h"
#include "gen-cpp/ImpalaPlanService.h"
#include "gen-cpp/ImpalaPlanService_types.h"
#include "gen-cpp/ImpalaInternalService.h"

DEFINE_int32(batch_size, 0,
    "batch size to be used by backend; a batch size of 0 indicates the "
    "backend's default batch size");
DEFINE_int32(file_buffer_size, 0,
    "file buffer size used by text parsing; size of 0 indicates the "
    "backend's default file buffer size");
DEFINE_int32(max_scan_range_length, 0,
    "maximum length of the scan range; only applicable to HDFS scan range; a length of 0"
    " indicates backend default");
DEFINE_bool(abort_on_error, false, "if true, abort query when encountering any error");
DEFINE_int32(max_errors, 100, "number of errors to report");
DEFINE_int32(num_nodes, 1,
    "Number of threads in which to run query; 1 = run only in main thread;"
    "0 = run in # of data nodes + 1 for coordinator");
DECLARE_int32(be_port);
// TODO: we probably want to add finer grain control of what is codegen'd
DEFINE_bool(enable_jit, true, "if true, enable codegen for query execution");
DECLARE_string(host);

using namespace std;
using namespace boost;
using namespace boost::algorithm;
using namespace apache::thrift;
using namespace apache::thrift::protocol;
using namespace apache::thrift::transport;
using namespace impala;

InProcessQueryExecutor::InProcessQueryExecutor(ExecEnv* exec_env)
  : started_server_(false),
    exec_env_(exec_env),
    row_batch_(NULL),
    next_row_(0),
    obj_pool_(new ObjectPool()),
    exec_stats_(new ExecStats()) {
}

InProcessQueryExecutor::~InProcessQueryExecutor() {
}

void InProcessQueryExecutor::DisableJit() {
  FLAGS_enable_jit = false;
}

Status InProcessQueryExecutor::Setup() {
  socket_.reset(new TSocket("localhost", 20000));
  transport_.reset(new TBufferedTransport(socket_));
  protocol_.reset(new TBinaryProtocol(transport_));
  client_.reset(new ImpalaPlanServiceClient(protocol_));

  // loop until we get a connection
  while (true) {
    try {
      transport_->open();
      break;
    } catch (TTransportException& e) {
      cout << "waiting for plan service to start up..." << endl;
      sleep(5);
    }
  }

  return Status::OK;
}

Status InProcessQueryExecutor::Exec(const string& query,
    vector<PrimitiveType>* col_types) {
  query_profile_.reset(new RuntimeProfile(obj_pool_.get(), "InProcessQueryExecutor"));
  RuntimeProfile::Counter* plan_gen_counter =
      ADD_COUNTER(query_profile_, "PlanGeneration", TCounterType::CPU_TICKS);

  COUNTER_SCOPED_TIMER(query_profile_->total_time_counter());
  VLOG_QUERY << "query: " << query;
  eos_ = false;

  TQueryOptions query_options;
  query_options.abort_on_error = FLAGS_abort_on_error;
  query_options.batch_size = FLAGS_batch_size;
  query_options.disable_codegen = !FLAGS_enable_jit;
  query_options.max_errors = FLAGS_max_errors;
  query_options.num_nodes = FLAGS_num_nodes;
  query_options.file_buffer_size = FLAGS_file_buffer_size;
  query_options.max_scan_range_length = FLAGS_max_scan_range_length;

  try {
    COUNTER_SCOPED_TIMER(plan_gen_counter);
    CHECK(client_.get() != NULL) << "didn't call InProcessQueryExecutor::Setup()";
    TQueryRequest query_request;
    query_request.__set_stmt(query.c_str());
    query_request.__set_queryOptions(query_options);
    TCreateQueryExecRequestResult result;
    client_->CreateQueryExecRequest(result, query_request);
    query_request_ = result.queryExecRequest;
  } catch (TImpalaPlanServiceException& e) {
    return Status(e.what());
  }
  VLOG_QUERY << "query request:\n" << ThriftDebugString(query_request_);

  // we always need at least one plan fragment
  DCHECK_GT(query_request_.fragment_requests.size(), 0);
  
  if (query_request_.has_coordinator_fragment 
      && !query_request_.fragment_requests[0].__isset.desc_tbl) {
    // query without a FROM clause: don't create a coordinator
    DCHECK(!query_request_.fragment_requests[0].__isset.plan_fragment);
    // Set now timestamp in local_state_.
    local_state_.reset(new RuntimeState(
        query_request_.query_id, query_options,
        query_request_.fragment_requests[0].query_globals.now_string, NULL));
    query_profile_->AddChild(local_state_->runtime_profile());
    RETURN_IF_ERROR(
        PrepareSelectListExprs(local_state_.get(), RowDescriptor(), col_types));
    return Status::OK;
  }
  if (!query_request_.has_coordinator_fragment) {
    // the first node_request_params list contains exactly one TPlanExecParams
    // (it's meant for the coordinator fragment)
    DCHECK_EQ(query_request_.node_request_params[0].size(), 1);
  }

  for (int i = 0; i < query_request_.fragment_requests.size(); ++i) {
    query_request_.fragment_requests[i].query_options = query_options;
  }

  if (FLAGS_num_nodes != 1) {
    // for distributed execution, we only expect one slave fragment which
    // will be executed by FLAGS_num_nodes - 1 nodes
    DCHECK_EQ(query_request_.fragment_requests.size(), 2);
    DCHECK_EQ(query_request_.node_request_params.size(), 2);
    DCHECK_LE(query_request_.node_request_params[0].size(), FLAGS_num_nodes - 1);

    // set destinations to coord host/port
    for (int i = 0; i < query_request_.node_request_params[1].size(); ++i) {
      DCHECK_EQ(query_request_.node_request_params[1][i].destinations.size(), 1);
      query_request_.node_request_params[1][i].destinations[0].host = FLAGS_host;
      query_request_.node_request_params[1][i].destinations[0].port = FLAGS_be_port;
    }
  }

  coord_.reset(new Coordinator(exec_env_, exec_stats_.get()));
  RETURN_IF_ERROR(coord_->Exec(&query_request_));
  RETURN_IF_ERROR(coord_->Wait());
  if (query_request_.has_coordinator_fragment) {
    RETURN_IF_ERROR(PrepareSelectListExprs(
            coord_->runtime_state(), coord_->row_desc(), col_types));
  }

  query_profile_->AddChild(coord_->query_profile());
  return Status::OK;
}

Status InProcessQueryExecutor::PrepareSelectListExprs(
    RuntimeState* state, const RowDescriptor& row_desc,
    vector<PrimitiveType>* col_types) {
  RETURN_IF_ERROR(
      Expr::CreateExprTrees(
          state->obj_pool(), query_request_.fragment_requests[0].output_exprs,
          &select_list_exprs_));
  for (int i = 0; i < select_list_exprs_.size(); ++i) {
    Expr::Prepare(select_list_exprs_[i], state, row_desc);
    if (col_types != NULL) col_types->push_back(select_list_exprs_[i]->type());
  }

  return Status::OK;
}

Status InProcessQueryExecutor::FetchResult(RowBatch** batch) {
  if (coord_.get() == NULL) {
    *batch = NULL;
    return Status::OK;
  }
  RETURN_IF_ERROR(coord_->GetNext(batch, runtime_state()));
  return Status::OK;
}

Status InProcessQueryExecutor::FetchResult(string* result) {
  COUNTER_SCOPED_TIMER(query_profile_->total_time_counter());
  vector<void*> row;
  RETURN_IF_ERROR(FetchResult(&row));
  result->clear();
  if (row.empty()) return Status::OK;

  string str;
  for (int i = 0; i < select_list_exprs_.size(); ++i) {
    RawValue::PrintValue(row[i], select_list_exprs_[i]->type(), &str);
    if (i > 0) result->append(", ");
    result->append(str);
  }
  return Status::OK;
}

Status InProcessQueryExecutor::FetchResult(vector<void*>* select_list_values) {
  COUNTER_SCOPED_TIMER(query_profile_->total_time_counter());
  select_list_values->clear();
  if (coord_.get() == NULL) {
    // query without FROM clause: we return exactly one row
    if (!eos_) {
      for (int i = 0; i < select_list_exprs_.size(); ++i) {
        select_list_values->push_back(select_list_exprs_[i]->GetValue(NULL));
      }
    }
    exec_stats_->num_rows_ = 1;
    eos_ = true;
    return Status::OK;
  }

  if (row_batch_ == NULL || row_batch_->num_rows() == next_row_) {
    if (eos_) {
      return Status::OK;
    }

    // we need a new row batch
    RETURN_IF_ERROR(coord_->GetNext(&row_batch_, runtime_state()));
    if (coord_->execution_completed()) {
      // no more rows to return
      eos_ = true;
    }
    next_row_ = 0;
  }

  // Might be an INSERT
  if (row_batch_ != NULL && !exec_stats_->is_insert()) {
    DCHECK(next_row_ < row_batch_->num_rows());
    TupleRow* row = row_batch_->GetRow(next_row_);
    for (int i = 0; i < select_list_exprs_.size(); ++i) {
      select_list_values->push_back(select_list_exprs_[i]->GetValue(row));
    }

    ++num_rows_;
    ++next_row_;
  }

  return Status::OK;
}

RuntimeState* InProcessQueryExecutor::runtime_state() {
  DCHECK(coord_.get() != NULL);
  return coord_->runtime_state();
}

const RowDescriptor& InProcessQueryExecutor::row_desc() const {
  DCHECK(coord_.get() != NULL);
  return coord_->row_desc();
}

Status InProcessQueryExecutor::EndQuery() {
  next_row_ = 0;
  return Status::OK;
}

void InProcessQueryExecutor::Shutdown() {
  if (started_server_) {
    // shut down server we started ourselves
    try {
      client_->ShutdownServer();
    } catch (TImpalaPlanServiceException& e) {
      // ignore; the server just quit, so won't respond to the rpc
    }
  }
  transport_->close();
}

string InProcessQueryExecutor::ErrorString() const {
  if (coord_.get() == NULL) {
    return "";
  }
  return coord_->runtime_state()->ErrorLog();
}

string InProcessQueryExecutor::FileErrors() const {
  if (coord_.get() == NULL) {
    return "";
  }
  return coord_->runtime_state()->FileErrors();
}

Status InProcessQueryExecutor::Explain(const string& query, string* explain_plan) {
  try {
    TQueryOptions query_options;
    query_options.abort_on_error = FLAGS_abort_on_error;
    query_options.batch_size = FLAGS_batch_size;
    query_options.disable_codegen = !FLAGS_enable_jit;
    query_options.max_errors = FLAGS_max_errors;
    query_options.num_nodes = FLAGS_num_nodes;
    query_options.file_buffer_size = FLAGS_file_buffer_size;
    query_options.max_scan_range_length = FLAGS_max_scan_range_length;

    TQueryRequest query_request;
    query_request.__set_stmt(query.c_str());
    query_request.__set_queryOptions(query_options);

    client_->GetExplainString(*explain_plan, query_request);
    return Status::OK;
  } catch (TImpalaPlanServiceException& e) {
    return Status(e.what());
  }
}

RuntimeProfile* InProcessQueryExecutor::query_profile() {
  return query_profile_.get();
}
