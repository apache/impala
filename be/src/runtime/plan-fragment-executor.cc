// (c) 2011 Cloudera, Inc. All rights reserved.

#include "runtime/plan-fragment-executor.h"

#include <Thrift.h>
#include <protocol/TBinaryProtocol.h>
#include <protocol/TDebugProtocol.h>
#include <transport/TBufferTransports.h>
#include <glog/logging.h>
#include <gflags/gflags.h>

#include "codegen/llvm-codegen.h"
#include "common/object-pool.h"
#include "exec/data-sink.h"
#include "exec/exec-node.h"
#include "exec/scan-node.h"
#include "exec/hbase-table-scanner.h"
#include "exprs/expr.h"
#include "runtime/descriptors.h"
#include "runtime/row-batch.h"
#include "util/debug-util.h"
#include "gen-cpp/ImpalaPlanService_types.h"

DEFINE_bool(serialize_batch, false, "serialize and deserialize each returned row batch");

using namespace std;
using namespace boost;
using namespace apache::thrift;
using namespace apache::thrift::protocol;
using namespace apache::thrift::transport;

namespace impala {

PlanFragmentExecutor::PlanFragmentExecutor(ExecEnv* exec_env)
  : exec_env_(exec_env),
    done_(false) {
}

PlanFragmentExecutor::~PlanFragmentExecutor() {
}

Status PlanFragmentExecutor::Prepare(
    const TPlanExecRequest& request, const TPlanExecParams& params) {

  VLOG(2) << "params:\n" << ThriftDebugString(params);

  runtime_state_.reset(
      new RuntimeState(request.fragment_id, request.query_options,
          request.query_globals.now_string, exec_env_));

  // set up desc tbl
  DescriptorTbl* desc_tbl = NULL;
  DCHECK(request.__isset.desc_tbl);
  RETURN_IF_ERROR(DescriptorTbl::Create(obj_pool(), request.desc_tbl, &desc_tbl));
  runtime_state_->set_desc_tbl(desc_tbl);
  VLOG_QUERY << desc_tbl->DebugString();

  // set up plan
  DCHECK(request.__isset.plan_fragment);
  RETURN_IF_ERROR(
      ExecNode::CreateTree(obj_pool(), request.plan_fragment, *desc_tbl, &plan_));
  runtime_state_->runtime_profile()->AddChild(plan_->runtime_profile());

  // set scan ranges
  vector<ExecNode*> scan_nodes;
  plan_->CollectScanNodes(&scan_nodes);
  for (int i = 0; i < scan_nodes.size(); ++i) {
    for (int j = 0; j < params.scan_ranges.size(); ++j) {
      if (scan_nodes[i]->id() == params.scan_ranges[j].nodeId) {
        RETURN_IF_ERROR(static_cast<ScanNode*>(
            scan_nodes[i])->SetScanRange(params.scan_ranges[j]));
      }
    }
  }

  // set up sink, if required  
  if (request.__isset.data_sink) {
    RETURN_IF_ERROR(DataSink::CreateDataSink(request, params, row_desc(), &sink_));
    RETURN_IF_ERROR(sink_->Init(runtime_state()));
  } else {
    sink_.reset(NULL);
  }

  row_batch_.reset(new RowBatch(plan_->row_desc(), runtime_state_->batch_size()));
  RETURN_IF_ERROR(plan_->Prepare(runtime_state_.get()));
  VLOG(2) << "plan_root=\n" << plan_->DebugString();
  return Status::OK;
}

Status PlanFragmentExecutor::Open() {
  RETURN_IF_ERROR(plan_->Open(runtime_state_.get()));

  // If there is a sink, do all the work of driving it here, so that
  // when this returns the query has actually finished
  if (sink_.get() != NULL) {
    RowBatch* batch = NULL;
    while (true) {
      RETURN_IF_ERROR(GetNext(&batch));
      if (batch == NULL) break;
      VLOG_FILE << "ExecInternal: #rows=" << batch->num_rows();
      if (VLOG_ROW_IS_ON) {
        for (int i = 0; i < batch->num_rows(); ++i) {
          TupleRow* row = batch->GetRow(i);
          VLOG_ROW << PrintRow(row, row_desc());
        }
      }

      RETURN_IF_ERROR(sink_->Send(runtime_state(), batch));
      batch = NULL;
    }
    RETURN_IF_ERROR(sink_->Close(runtime_state()));
  }

  return Status::OK;
}

Status PlanFragmentExecutor::GetNext(RowBatch** batch) {
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

  if (done_ && *batch == NULL) {
    // make sure to call Close() before returning 'eos'.
    RETURN_IF_ERROR(plan_->Close(runtime_state_.get()));
    if (VLOG_QUERY_IS_ON) {
      stringstream ss;
      plan_->runtime_profile()->PrettyPrint(&ss);
      VLOG_QUERY << "Runtime profile for fragment " << runtime_state_->fragment_id()
                 << endl << ss.str();
    }
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

Status PlanFragmentExecutor::Close() {
  return plan_->Close(runtime_state_.get());
}

const RowDescriptor& PlanFragmentExecutor::row_desc() {
  return plan_->row_desc();
}

RuntimeProfile* PlanFragmentExecutor::query_profile() {
  // TODO: allow printing the query profile while the query is running as a way
  // to monitor the query status
  DCHECK(done_);
  return runtime_state_->runtime_profile();
}

}
