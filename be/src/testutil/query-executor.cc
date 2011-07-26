// (c) 2011 Cloudera, Inc. All rights reserved.

#include "testutil/query-executor.h"

#include <stdlib.h>  // for system()
#include <unistd.h>  // for sleep()
#include <sys/stat.h>
#include <iostream>
#include <vector>
//#include <boost/filesystem.hpp>
#include <boost/algorithm/string.hpp>
#include <boost/scoped_ptr.hpp>
#include <boost/thread.hpp>
#include <glog/logging.h>

#include <protocol/TBinaryProtocol.h>
#include <transport/TSocket.h>
#include <transport/TTransportUtils.h>

#include "common/object-pool.h"
#include "common/status.h"
#include "exec/exec-node.h"
#include "exprs/expr.h"
#include "runtime/descriptors.h"
#include "runtime/row-batch.h"
#include "service/plan-executor.h"
#include "gen-cpp/ImpalaPlanService.h"
#include "gen-cpp/ImpalaPlanService_types.h"

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
  elems.push_back("fe");
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

Status QueryExecutor::Exec(const std::string& query) {
  TExecutePlanRequest request;
  try {
    client_->GetExecRequest(request, query.c_str());
  } catch (TAnalysisException& e) {
    return Status(e.msg);
  }

  ExecNode* plan_root;
  RETURN_IF_ERROR(ExecNode::CreateTree(pool_.get(), request.plan, &plan_root));
  DescriptorTbl* descs;
  RETURN_IF_ERROR(DescriptorTbl::Create(pool_.get(), request.descTbl, &descs));
  VLOG(1) << descs->DebugString();
  RETURN_IF_ERROR(
      Expr::CreateExprTrees(pool_.get(), request.selectListExprs, &select_list_exprs_));

  // Prepare select list expressions.
  executor_.reset(new PlanExecutor(plan_root, *descs));
  for (int i = 0; i < select_list_exprs_.size(); ++i) {
    select_list_exprs_[i]->Prepare(executor_->runtime_state());
  }
  executor_->Exec();
  next_row_ = 0;
  eos_ = false;

  return Status::OK;
}

Status QueryExecutor::FetchResult(std::string* result) {
  if (row_batch_.get() == NULL || row_batch_->num_rows() == next_row_) {
    if (eos_) {
      *result = "";
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
    }
  }

  TupleRow* row = row_batch_->GetRow(next_row_);
  result->clear();
  string str;
  for (int i = 0; i < select_list_exprs_.size(); ++i) {
    select_list_exprs_[i]->PrintValue(row, &str);
    if (i > 0) result->append(", ");
    result->append(str);
  }
  ++num_rows_;
  ++next_row_;
  return Status::OK;
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
