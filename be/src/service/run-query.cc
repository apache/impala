// (c) 2011 Cloudera, Inc. All rights reserved.

#include <iostream>
#include <vector>
#include <boost/scoped_ptr.hpp>

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
using namespace apache::thrift;
using namespace apache::thrift::protocol;
using namespace apache::thrift::transport;
using namespace impala;

#define EXIT_IF_ERROR(stmt) \
  do { \
    Status status = (stmt); \
    if (!status.ok()) { \
      string msg; \
      status.GetErrorMsg(&msg); \
      cerr << msg; \
      exit(1); \
    } \
  } while (false)

static Status ExecutePlan(
    PlanExecutor* executor,
    const vector<Expr*>& select_list_exprs) {
  Status status;
  RETURN_IF_ERROR(executor->Exec());
  scoped_ptr<RowBatch> batch;
  string str;
  int num_rows = 0;
  while (true) {
    RowBatch* batch_ptr;
    RETURN_IF_ERROR(executor->FetchResult(&batch_ptr));
    batch.reset(batch_ptr);
    if (batch == NULL) {
      return Status("Internal error: row batch is NULL.");
    }
    for (int i = 0; i < batch->num_rows(); ++i) {
      TupleRow* row = batch->GetRow(i);
      for (int j = 0; j < select_list_exprs.size(); ++j) {
        select_list_exprs[j]->PrintValue(row, &str);
        cout << (j > 0 ? ", " : "") << str;
      }
      cout << endl;
    }
    num_rows += batch->num_rows();
    if (batch->num_rows() < batch->capacity()) {
      break;
    }
  }
  cout << "returned " << num_rows << (num_rows == 1 ? " row " : " rows ") << endl;

  return Status::OK;
}


int main(int argc, char** argv) {
  if (argc != 2) {
    cerr << "usage: " << argv[0] << " <query>\n";
    exit(1);
  }

  shared_ptr<TTransport> socket(new TSocket("localhost", 20000));
  shared_ptr<TTransport> transport(new TBufferedTransport(socket));
  shared_ptr<TProtocol> protocol(new TBinaryProtocol(transport));
  ImpalaPlanServiceClient client(protocol);

  try {
    transport->open();

    TExecutePlanRequest request;
    try {
      client.GetExecRequest(request, argv[1]);
    } catch (TAnalysisException& e) {
      cerr << e.msg << endl;
      exit(1);
    }

    ObjectPool pool;
    ExecNode* plan_root;
    EXIT_IF_ERROR(ExecNode::CreateTree(&pool, request.plan, &plan_root));
    DescriptorTbl* descs;
    EXIT_IF_ERROR(DescriptorTbl::Create(&pool, request.descTbl, &descs));
    vector<Expr*> select_list_exprs;
    EXIT_IF_ERROR(
        Expr::CreateExprTrees(&pool, request.selectListExprs, &select_list_exprs));

    // Prepare select list expressions.
    PlanExecutor executor(plan_root, *descs);
    for (int i = 0; i < select_list_exprs.size(); ++i) {
      select_list_exprs[i]->Prepare(executor.runtime_state());
    }

    EXIT_IF_ERROR(ExecutePlan(&executor, select_list_exprs));

    transport->close();
  } catch (TException& tx) {
    printf("ERROR: %s\n", tx.what());
  }
}

