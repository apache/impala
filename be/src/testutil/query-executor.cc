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
#include "runtime/descriptors.h"
#include "runtime/raw-value.h"
#include "runtime/row-batch.h"
#include "runtime/runtime-state.h"
#include "service/plan-executor.h"
#include "gen-cpp/ImpalaPlanService.h"
#include "gen-cpp/ImpalaPlanService_types.h"

DEFINE_int32(batch_size, 0,
             "batch size to be used by backend; a batch size of 0 indicates the "
             "backend's default batch size");
DEFINE_int32(num_nodes, 1, "number of (local) nodes on which to run query");
DEFINE_bool(abort_on_error, false, "if true, abort query when encountering any error");
DEFINE_int32(max_errors, 100, "number of errors to report");

using namespace std;
using namespace boost;
using namespace boost::algorithm;
using namespace apache::thrift;
using namespace apache::thrift::protocol;
using namespace apache::thrift::transport;
using namespace impala;

QueryExecutor::QueryExecutor()
  : started_server_(false),
    pool_(new ObjectPool()),
    row_batch_(NULL),
    next_row_(0) {
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
  ::system(cmdline.c_str());
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
  VLOG(1) << "query: " << query;
  TQueryExecRequest query_request;
  try {
    // TODO: use FLAGS_num_nodes here
    client_->GetExecRequest(query_request, query.c_str(), 1);
  } catch (TException& e) {
    return Status(e.msg);
  }
  VLOG(1) << "thrift request: " << ThriftDebugString(query_request);

  DCHECK_EQ(query_request.fragmentRequests.size(), 1);
  TPlanExecRequest& request = query_request.fragmentRequests[0];
  DescriptorTbl* descs = NULL;
  if (request.__isset.descTbl) {
    RETURN_IF_ERROR(DescriptorTbl::Create(pool_.get(), request.descTbl, &descs));
    VLOG(1) << descs->DebugString();
  }
  ExecNode* plan_root = NULL;;
  if (request.__isset.planFragment) {
    RETURN_IF_ERROR(
        ExecNode::CreateTree(pool_.get(), request.planFragment, *descs, &plan_root));
        
    // set scan ranges
    vector<ExecNode*> scan_nodes;
    plan_root->CollectScanNodes(&scan_nodes);
    DCHECK_GT(query_request.nodeRequestParams.size(), 0);
    // the first nodeRequestParams list contains exactly one TPlanExecParams
    // (it's meant for the coordinator fragment)
    DCHECK_EQ(query_request.nodeRequestParams[0].size(), 1);
    vector<TScanRange>& local_scan_ranges =
        query_request.nodeRequestParams[0][0].scanRanges;
    for (int i = 0; i < scan_nodes.size(); ++i) {
      for (int j = 0; j < local_scan_ranges.size(); ++j) {
        if (scan_nodes[i]->id() == local_scan_ranges[j].nodeId) {
           static_cast<ScanNode*>(scan_nodes[i])->SetScanRange(local_scan_ranges[j]);
        }
      }
    }
  }
  RETURN_IF_ERROR(
      Expr::CreateExprTrees(pool_.get(), request.outputExprs, &select_list_exprs_));

  // Prepare select list expressions.
  RuntimeState local_runtime_state(*descs, FLAGS_abort_on_error, FLAGS_max_errors);
  RuntimeState* runtime_state = &local_runtime_state;
  if (plan_root != NULL) {
    executor_.reset(
        new PlanExecutor(plan_root, *descs, FLAGS_abort_on_error, FLAGS_max_errors));
    runtime_state = executor_->runtime_state();
  }
  if (FLAGS_batch_size != 0) runtime_state->set_batch_size(FLAGS_batch_size);

  for (int i = 0; i < select_list_exprs_.size(); ++i) {
    select_list_exprs_[i]->Prepare(runtime_state, plan_root->row_desc());
    if (col_types != NULL) col_types->push_back(select_list_exprs_[i]->type());
  }
  if (plan_root != NULL) {
    executor_->Exec();
    VLOG(1) << plan_root->DebugString();
  }
  eos_ = false;
  next_row_ = 0;

  return Status::OK;
}

Status QueryExecutor::FetchResult(std::string* result) {
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
  select_list_values->clear();
  if (executor_.get() == NULL) {
    // query without FROM clause: we return exactly one row
    if (!eos_) {
      for (int i = 0; i < select_list_exprs_.size(); ++i) {
        select_list_values->push_back(select_list_exprs_[i]->GetValue(NULL));
      }
    }
    eos_ = true;
    return Status::OK;
  }

  select_list_values->clear();
  if (row_batch_.get() == NULL || row_batch_->num_rows() == next_row_) {
    if (eos_) {
      return Status::OK;
    }

    // we need a new row batch
    RowBatch* batch_ptr;
    RETURN_IF_ERROR(executor_->FetchResult(&batch_ptr));
    row_batch_.reset(batch_ptr);
    if (batch_ptr == NULL) {
      return Status("Internal error: row batch is NULL.");
    }
    next_row_ = 0;
    if (row_batch_->num_rows() < row_batch_->capacity()) {
      eos_ = true;
      if (row_batch_->num_rows() == 0) {
        // empty result
        return Status::OK;
      }
    }
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
  return executor_->runtime_state();
}

Status QueryExecutor::EndQuery() {
  row_batch_.reset(NULL);
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
  if (executor_.get() == NULL) {
    return "";
  }
  return executor_->runtime_state()->ErrorLog();
}

std::string QueryExecutor::FileErrors() const {
  if (executor_.get() == NULL) {
    return "";
  }
  return executor_->runtime_state()->FileErrors();
}
