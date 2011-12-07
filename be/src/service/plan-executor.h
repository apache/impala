// Copyright (c) 2011 Cloudera, Inc. All rights reserved.

#ifndef IMPALA_SERVICE_PLAN_EXECUTOR_H
#define IMPALA_SERVICE_PLAN_EXECUTOR_H

#include <vector>
#include <boost/scoped_ptr.hpp>

#include "common/status.h"
#include "common/object-pool.h"
#include "runtime/runtime-state.h"

namespace impala {

class Expr;
class ExecNode;
class DescriptorTbl;
class RowBatch;
class TupleDescriptor;
class TRowBatch;

class PlanExecutor {
 public:
  PlanExecutor(ExecNode* plan, const DescriptorTbl& descs, bool abort_on_error,
               int max_errors);
  ~PlanExecutor();

  // Start running query. Call this prior to FetchResult().
  Status Exec();

  // Return results through 'batch'. Sets 'batch' to NULL if no more results.
  // The caller is responsible for deleting *batch.
  Status FetchResult(RowBatch** batch);

  RuntimeState* runtime_state() { return &runtime_state_; }

 private:
  ExecNode* plan_;
  std::vector<TupleDescriptor*> tuple_descs_;
  RuntimeState runtime_state_;
  bool done_;
  boost::scoped_ptr<TRowBatch> thrift_batch_;
};

}

#endif
