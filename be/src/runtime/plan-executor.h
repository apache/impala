// Copyright (c) 2011 Cloudera, Inc. All rights reserved.

#ifndef IMPALA_SERVICE_PLAN_EXECUTOR_H
#define IMPALA_SERVICE_PLAN_EXECUTOR_H

#include <vector>
#include <boost/scoped_ptr.hpp>

#include "common/status.h"
#include "common/object-pool.h"
#include "runtime/runtime-state.h"

namespace impala {

class HdfsFsCache;
class ExecNode;
class RowDescriptor;
class RowBatch;
class DataStreamMgr;
class RuntimeProfile;
class RuntimeState;
class TRowBatch;
class TPlanExecRequest;
class TPlanExecParams;

class PlanExecutor {
 public:
  PlanExecutor(DataStreamMgr* stream_mgr, HdfsFsCache* fs_cache);
  ~PlanExecutor();

  // Prepare for execution. Call this prior to Open().
  // This call won't block.
  Status Prepare(const TPlanExecRequest& request, const TPlanExecParams& params);

  // Start execution. Call this prior to GetNext().
  // This call may block.
  Status Open();

  // Return results through 'batch'. Sets '*batch' to NULL if no more results.
  // '*batch' is owned by PlanExecutor and must not be deleted.
  // GetNext should not be called after *batch == NULL.
  Status GetNext(RowBatch** batch);

  RuntimeState* runtime_state() { return runtime_state_.get(); }
  const RowDescriptor& row_desc();

  RuntimeProfile* query_profile();

 private:
  DataStreamMgr* stream_mgr_;
  HdfsFsCache* fs_cache_;
  ExecNode* plan_;  // lives in runtime_state_->obj_pool()
  boost::scoped_ptr<RuntimeState> runtime_state_;
  bool done_;
  boost::scoped_ptr<RowBatch> row_batch_;
  boost::scoped_ptr<TRowBatch> thrift_batch_;

  ObjectPool* obj_pool() { return runtime_state_->obj_pool(); }
  const DescriptorTbl& desc_tbl() { return runtime_state_->desc_tbl(); }
};

}

#endif
