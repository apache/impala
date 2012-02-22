// (c) 2011 Cloudera, Inc. All rights reserved.

#include "testutil/query-executor.h"

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

#include "common/object-pool.h"
#include "common/status.h"
#include "exec/exec-node.h"
#include "exec/scan-node.h"
#include "exprs/expr.h"
#include "runtime/coordinator.h"
#include "runtime/descriptors.h"
#include "runtime/raw-value.h"
#include "runtime/row-batch.h"
#include "runtime/runtime-state.h"
#include "runtime/plan-executor.h"
#include "runtime/exec-env.h"
#include "util/debug-util.h"
#include "gen-cpp/ImpalaPlanService.h"
#include "gen-cpp/ImpalaPlanService_types.h"
#include "gen-cpp/ImpalaBackendService.h"
#include "gen-cpp/Data_types.h"

DEFINE_int32(batch_size, 0,
    "batch size to be used by backend; a batch size of 0 indicates the "
    "backend's default batch size");
DEFINE_bool(abort_on_error, false, "if true, abort query when encountering any error");
DEFINE_int32(max_errors, 100, "number of errors to report");
DEFINE_int32(num_nodes, 1,
    "Number of threads in which to run query; 1 = run only in main thread;"
    "0 = run in # of data nodes + 1 for coordinator");
DEFINE_int32(backend_port, 21000,
    "start port for backend threads (assigned sequentially)");
DEFINE_string(coord_host, "localhost", "hostname of coordinator");

using namespace std;
using namespace boost;
using namespace boost::algorithm;
using namespace apache::thrift;
using namespace apache::thrift::protocol;
using namespace apache::thrift::transport;
using namespace impala;

QueryExecutor::QueryExecutor(ExecEnv* exec_env)
  : started_server_(false),
    exec_env_(exec_env),
    row_batch_(NULL),
    next_row_(0),
    obj_pool_(new ObjectPool()) {
}

QueryExecutor::~QueryExecutor() {
}

#if 0
// I can't get this to compile.
static void FindRunplanservice(string* path) {
  // first, look for directory 'be' on current path
  filesystem::path wd = filesystem::current_path();
  filesystem::path::iterator i = wd.end();
  do {
    --i;
    if (*i == "be") {
      filesystem::path be_path(wd.begin(), i);
      filesystem::path script_path = be_path.parent_path();
      script_path /= "fe/bin/runplanservice";
      *path = script_path.string();
    }
  } while (i != wd.begin());
}
#else
static void FindRunplanservice(string* path) {
  path->clear();
  string wd(getcwd(NULL, 0));
  vector<string> elems;
  split(elems, wd, is_any_of("/"));
  int i;
  for (i = elems.size() - 1; i >= 0; --i) {
    if (elems[i] == "be") break;
  }
  if (i < 0) return;
  elems.erase(elems.begin() + i, elems.end());
  elems.push_back("bin");
  elems.push_back("runplanservice");
  *path = join(elems, "/");

  struct stat dummy;
  if (stat(path->c_str(), &dummy) != 0) {
    // the path doesn't exist
    path->clear();
  }
}
#endif

static void StartServer() {
  string cmdline;
  FindRunplanservice(&cmdline);
  if (cmdline.empty()) {
    cerr << "couldn't find runplanservice";
    exit(1);
  }
  //cmdline.append(" > /dev/null 2> /dev/null");
  int ret = ::system(cmdline.c_str());
  if (ret == -1) {
    cerr << "couldn't execute command: " << cmdline;
    exit(1);
  }
}

Status QueryExecutor::Setup() {
  socket_.reset(new TSocket("localhost", 20000));
  transport_.reset(new TBufferedTransport(socket_));
  protocol_.reset(new TBinaryProtocol(transport_));
  client_.reset(new ImpalaPlanServiceClient(protocol_));

  try {
    transport_->open();
  } catch (TTransportException& e) {
    // this probably means that the service isn't running;
    // let's start it ourselves
    thread s_thread(StartServer);
    started_server_ = true;

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
  }

  return Status::OK;
}

Status QueryExecutor::Exec(
    const std::string& query, vector<PrimitiveType>* col_types) {
  query_profile_.reset(new RuntimeProfile(obj_pool_.get(), "QueryExecutor"));
  RuntimeProfile::Counter* plan_gen_counter = 
      ADD_COUNTER(query_profile_, "PlanGeneration", TCounterType::CPU_TICKS);

  COUNTER_SCOPED_TIMER(query_profile_->total_time_counter());
  VLOG(1) << "query: " << query;
  eos_ = false;
  try {
    COUNTER_SCOPED_TIMER(plan_gen_counter);
    CHECK(client_.get() != NULL) << "didn't call QueryExecutor::Setup()";
    client_->GetExecRequest(query_request_, query.c_str(), FLAGS_num_nodes);
  } catch (TException& e) {
    return Status(e.msg);
  }
  VLOG(1) << "query request:\n" << ThriftDebugString(query_request_);

  // we always need at least one plan fragment
  DCHECK_GT(query_request_.fragmentRequests.size(), 0);

  if (!query_request_.fragmentRequests[0].__isset.descTbl) {
    // query without a FROM clause: don't create a coordinator
    DCHECK(!query_request_.fragmentRequests[0].__isset.planFragment);
    local_state_.reset(
        new RuntimeState(query_request_.queryId, FLAGS_abort_on_error,
                         FLAGS_max_errors, NULL));
    RETURN_IF_ERROR(
        PrepareSelectListExprs(local_state_.get(), RowDescriptor(), col_types));
    return Status::OK;
  }

  DCHECK_GT(query_request_.nodeRequestParams.size(), 0);
  // the first nodeRequestParams list contains exactly one TPlanExecParams
  // (it's meant for the coordinator fragment)
  DCHECK_EQ(query_request_.nodeRequestParams[0].size(), 1);

  if (FLAGS_num_nodes != 1) {
    // for distributed execution, we only expect one slave fragment which
    // will be executed by FLAGS_num_nodes - 1 nodes
    DCHECK_EQ(query_request_.fragmentRequests.size(), 2);
    DCHECK_EQ(query_request_.nodeRequestParams.size(), 2);
    DCHECK_LE(query_request_.nodeRequestParams[1].size(), FLAGS_num_nodes - 1);

    // set destinations to coord host/port
    for (int i = 0; i < query_request_.nodeRequestParams[1].size(); ++i) {
      DCHECK_EQ(query_request_.nodeRequestParams[1][i].destinations.size(), 1);
      query_request_.nodeRequestParams[1][i].destinations[0].host =
          FLAGS_coord_host;
      query_request_.nodeRequestParams[1][i].destinations[0].port =
          FLAGS_backend_port;
    }
  }

  coord_.reset(new Coordinator(exec_env_));
  RETURN_IF_ERROR(coord_->Exec(query_request_));
  RETURN_IF_ERROR(PrepareSelectListExprs(
      coord_->runtime_state(), coord_->row_desc(), col_types));

  query_profile_->AddChild(coord_->query_profile());
  return Status::OK;
}

Status QueryExecutor::PrepareSelectListExprs(
    RuntimeState* state, const RowDescriptor& row_desc,
    vector<PrimitiveType>* col_types) {
  RETURN_IF_ERROR(
      Expr::CreateExprTrees(
          state->obj_pool(), query_request_.fragmentRequests[0].outputExprs,
          &select_list_exprs_));
  if (FLAGS_batch_size != 0) state->set_batch_size(FLAGS_batch_size);
  for (int i = 0; i < select_list_exprs_.size(); ++i) {
    select_list_exprs_[i]->Prepare(state, row_desc);
    if (col_types != NULL) col_types->push_back(select_list_exprs_[i]->type());
  }
  return Status::OK;
}

Status QueryExecutor::FetchResult(RowBatch** batch) {
  if (coord_.get() == NULL) {
    *batch = NULL;
    return Status::OK;
  }
  RETURN_IF_ERROR(coord_->GetNext(batch));
  return Status::OK;
}

Status QueryExecutor::FetchResult(std::string* result) {
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

Status QueryExecutor::FetchResult(std::vector<void*>* select_list_values) {
  COUNTER_SCOPED_TIMER(query_profile_->total_time_counter());
  select_list_values->clear();
  if (coord_.get() == NULL) {
    // query without FROM clause: we return exactly one row
    if (!eos_) {
      for (int i = 0; i < select_list_exprs_.size(); ++i) {
        select_list_values->push_back(select_list_exprs_[i]->GetValue(NULL));
      }
    }
    eos_ = true;
    return Status::OK;
  }

  if (row_batch_ == NULL || row_batch_->num_rows() == next_row_) {
    if (eos_) {
      return Status::OK;
    }

    // we need a new row batch
    RETURN_IF_ERROR(coord_->GetNext(&row_batch_));
    if (row_batch_ == NULL) {
      // no more rows to return
      eos_ = true;
      return Status::OK;
    }
    next_row_ = 0;
  }

  DCHECK(next_row_ < row_batch_->num_rows());
  TupleRow* row = row_batch_->GetRow(next_row_);
  for (int i = 0; i < select_list_exprs_.size(); ++i) {
    select_list_values->push_back(select_list_exprs_[i]->GetValue(row));
  }

  ++num_rows_;
  ++next_row_;
  return Status::OK;
}

RuntimeState* QueryExecutor::runtime_state() {
  DCHECK(coord_.get() != NULL);
  return coord_->runtime_state();
}

const RowDescriptor& QueryExecutor::row_desc() const {
  DCHECK(coord_.get() != NULL);
  return coord_->row_desc();
}

Status QueryExecutor::EndQuery() {
  next_row_ = 0;
  return Status::OK;
}

void QueryExecutor::Shutdown() {
  if (started_server_) {
    // shut down server we started ourselves
    try {
      client_->ShutdownServer();
    } catch (TException& e) {
      // ignore; the server just quit, so won't respond to the rpc
    }
  }
  transport_->close();
}

std::string QueryExecutor::ErrorString() const {
  if (coord_.get() == NULL) {
    return "";
  }
  return coord_->runtime_state()->ErrorLog();
}

std::string QueryExecutor::FileErrors() const {
  if (coord_.get() == NULL) {
    return "";
  }
  return coord_->runtime_state()->FileErrors();
}

RuntimeProfile* QueryExecutor::query_profile() {
  return query_profile_.get();
}
