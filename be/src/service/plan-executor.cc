// (c) 2011 Cloudera, Inc. All rights reserved.

#include "service/plan-executor.h"

#include <glog/logging.h>

#include "common/object-pool.h"
#include "exec/exec-node.h"
#include "exprs/expr.h"
#include "runtime/descriptors.h"
#include "runtime/row-batch.h"
#include "gen-cpp/ImpalaService_types.h"
#include "gen-cpp/ImpalaPlanService_types.h"

using namespace std;
using namespace boost;

namespace impala {

PlanExecutor::PlanExecutor(ExecNode* plan, const DescriptorTbl& descs,
                           bool abort_on_error, int max_errors)
  : plan_(plan),
    tuple_descs_(),
    runtime_state_(descs, abort_on_error, max_errors),
    done_(false) {
  // stash these, we need to pass them into the RowBatch c'tor
  descs.GetTupleDescs(&tuple_descs_);
}

PlanExecutor::~PlanExecutor() {
}

Status PlanExecutor::Exec() {
  RETURN_IF_ERROR(plan_->Prepare(&runtime_state_));
  RETURN_IF_ERROR(plan_->Open(&runtime_state_));
  return Status::OK;
}

Status PlanExecutor::FetchResult(RowBatch** batch) {
  if (done_) {
    *batch = NULL;
    return Status::OK;
  }
  *batch = new RowBatch(tuple_descs_, runtime_state_.batch_size());
  RETURN_IF_ERROR(plan_->GetNext(&runtime_state_, *batch));
  return Status::OK;
}

}
