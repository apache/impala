// (c) 2011 Cloudera, Inc. All rights reserved.

#include "service/plan-executor.h"

#include <Thrift.h>
#include <protocol/TBinaryProtocol.h>
#include <transport/TBufferTransports.h>
#include <glog/logging.h>
#include <gflags/gflags.h>

#include "common/object-pool.h"
#include "exec/exec-node.h"
#include "exec/hbase-table-scanner.h"
#include "exprs/expr.h"
#include "runtime/descriptors.h"
#include "runtime/row-batch.h"
#include "util/jni-util.h"
#include "gen-cpp/ImpalaService_types.h"
#include "gen-cpp/ImpalaPlanService_types.h"
#include "gen-cpp/Data_types.h"

DEFINE_bool(serialize_batch, false, "serialize and deserialize each returned row batch");

using namespace std;
using namespace boost;
using namespace apache::thrift::protocol;
using namespace apache::thrift::transport;

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
  *batch = new RowBatch(plan_->row_desc().tuple_descriptors(),
                        runtime_state_.batch_size());
  RETURN_IF_ERROR(plan_->GetNext(&runtime_state_, *batch));

  if (FLAGS_serialize_batch) {
    // serialize and deserialize; we need to hang on to the TRowBatch
    // while 'batch' can be referenced
    thrift_batch_.reset(new TRowBatch());
    (*batch)->Serialize(thrift_batch_.get());
    *batch = new RowBatch(runtime_state_.descs(), thrift_batch_.get());
  }

  return Status::OK;
}

}
