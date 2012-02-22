// (c) 2011 Cloudera, Inc. All rights reserved.

#include "runtime/plan-executor.h"

#include <Thrift.h>
#include <protocol/TBinaryProtocol.h>
#include <protocol/TDebugProtocol.h>
#include <transport/TBufferTransports.h>
#include <glog/logging.h>
#include <gflags/gflags.h>

#include "common/object-pool.h"
#include "exec/exec-node.h"
#include "exec/scan-node.h"
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
using namespace apache::thrift;
using namespace apache::thrift::protocol;
using namespace apache::thrift::transport;

namespace impala {

PlanExecutor::PlanExecutor(DataStreamMgr* stream_mgr, HdfsFsCache* fs_cache)
  : stream_mgr_(stream_mgr),
    fs_cache_(fs_cache),
    done_(false) {
}

PlanExecutor::~PlanExecutor() {
}

Status PlanExecutor::Prepare(
    const TPlanExecRequest& request, const TPlanExecParams& params) {
  //VLOG(1) << "plan exec request:\n" << ThriftDebugString(request);
  VLOG(1) << "params:\n" << ThriftDebugString(params);
  runtime_state_.reset(
      new RuntimeState(request.queryId, params.abortOnError, params.maxErrors,
                       stream_mgr_, fs_cache_));

  // set up desc tbl
  DescriptorTbl* desc_tbl = NULL;
  DCHECK(request.__isset.descTbl);
  RETURN_IF_ERROR(DescriptorTbl::Create(obj_pool(), request.descTbl, &desc_tbl));
  runtime_state_->set_desc_tbl(desc_tbl);
  VLOG(1) << desc_tbl->DebugString();

  // set up plan
  DCHECK(request.__isset.planFragment);
  RETURN_IF_ERROR(
      ExecNode::CreateTree(obj_pool(), request.planFragment, *desc_tbl, &plan_));

  // set scan ranges
  vector<ExecNode*> scan_nodes;
  plan_->CollectScanNodes(&scan_nodes);
  for (int i = 0; i < scan_nodes.size(); ++i) {
    for (int j = 0; j < params.scanRanges.size(); ++j) {
      if (scan_nodes[i]->id() == params.scanRanges[j].nodeId) {
         static_cast<ScanNode*>(scan_nodes[i])->SetScanRange(params.scanRanges[j]);
      }
    }
  }

  row_batch_.reset(new RowBatch(plan_->row_desc(), runtime_state_->batch_size()));
  RETURN_IF_ERROR(plan_->Prepare(runtime_state_.get()));
  VLOG(1) << "plan_root=\n" << plan_->DebugString();
  return Status::OK;
}

Status PlanExecutor::Open() {
  RETURN_IF_ERROR(plan_->Open(runtime_state_.get()));
  return Status::OK;
}

Status PlanExecutor::GetNext(RowBatch** batch) {
  if (done_) {
    *batch = NULL;
    RETURN_IF_ERROR(plan_->Close(runtime_state_.get()));
    return Status::OK;
  }

  while (!done_) {
    row_batch_->Reset();
    RETURN_IF_ERROR(plan_->GetNext(runtime_state_.get(), row_batch_.get(), &done_));
    if (row_batch_->num_rows() > 0) {
      *batch = row_batch_.get();
      break;
    }
    *batch = NULL;
  }

#if 0
  // TODO: move this to QueryExecutor
  if (FLAGS_serialize_batch) {
    // serialize and deserialize; we need to hang on to the TRowBatch
    // while 'batch' can be referenced
    thrift_batch_.reset(new TRowBatch());
    row_batch_->Serialize(thrift_batch_.get());
    row_batch_.reset(new RowBatch(runtime_state_->desc_tbl(), thrift_batch_.get()));
  }
#endif

  return Status::OK;
}

const RowDescriptor& PlanExecutor::row_desc() {
  return plan_->row_desc();
}

RuntimeProfile* PlanExecutor::query_profile() {
  // TODO: allow printing the query profile while the query is running as a way
  // to monitor the query status
  DCHECK(done_);
  return plan_->runtime_profile();
}

}
