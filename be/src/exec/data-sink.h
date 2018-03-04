// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.


#ifndef IMPALA_EXEC_DATA_SINK_H
#define IMPALA_EXEC_DATA_SINK_H

#include <boost/scoped_ptr.hpp>
#include <vector>

#include "common/status.h"
#include "runtime/runtime-state.h"  // for PartitionStatusMap
#include "runtime/mem-tracker.h"
#include "gen-cpp/Exprs_types.h"

namespace impala {

class MemPool;
class ObjectPool;
class RowBatch;
class RuntimeProfile;
class RuntimeState;
class RowDescriptor;
class ScalarExpr;
class ScalarExprEvaluator;
class TDataSink;
class TPlanExecRequest;
class TPlanExecParams;
class TPlanFragmentInstanceCtx;
class TInsertStats;

/// A data sink is an abstract interface for data sinks that consume RowBatches. E.g.
/// a sink may write a HDFS table, send data across the network, or build hash tables
/// for a join.
//
/// Clients of the DataSink interface drive the data sink by repeatedly calling Send()
/// with batches. Before Send() is called, the sink must be initialized by calling
/// Prepare() during the prepare phase of the query fragment, then Open(). After the last
/// batch has been sent, FlushFinal() should be called to complete any processing.
/// Close() is called to release any resources before destroying the sink.
class DataSink {
 public:
  DataSink(const RowDescriptor* row_desc, const string& name, RuntimeState* state);
  virtual ~DataSink();

  /// Setup. Call before Send(), Open(), or Close() during the prepare phase of the query
  /// fragment. Creates a MemTracker for the sink that is a child of 'parent_mem_tracker'.
  /// Also creates a MemTracker and MemPool for the output (and partitioning) expr and
  /// initializes their evaluators. Subclasses must call DataSink::Prepare().
  virtual Status Prepare(RuntimeState* state, MemTracker* parent_mem_tracker);

  /// Call before Send() to open the sink and initialize output expression evaluators.
  virtual Status Open(RuntimeState* state);

  /// Send a row batch into this sink. Send() may modify 'batch' by acquiring its state.
  virtual Status Send(RuntimeState* state, RowBatch* batch) = 0;

  /// Flushes any remaining buffered state.
  /// Further Send() calls are illegal after FlushFinal(). This is to be called only
  /// before calling Close().
  virtual Status FlushFinal(RuntimeState* state) = 0;

  /// Releases all resources that were allocated in Open()/Send().
  /// Further Send() calls or FlushFinal() calls are illegal after calling Close().
  /// Must be idempotent.
  virtual void Close(RuntimeState* state);

  /// Creates a new data sink, allocated in pool and returned through *sink, from
  /// thrift_sink.
  static Status Create(const TPlanFragmentCtx& fragment_ctx,
      const TPlanFragmentInstanceCtx& fragment_instance_ctx,
      const RowDescriptor* row_desc, RuntimeState* state, DataSink** sink);

  /// Merges one update to the DML stats for a partition. dst_stats will have the
  /// combined stats of src_stats and dst_stats after this method returns.
  static void MergeDmlStats(const TInsertStats& src_stats,
      TInsertStats* dst_stats);

  /// Outputs the DML stats contained in the map of partition updates to a string
  static std::string OutputDmlStats(const PartitionStatusMap& stats,
      const std::string& prefix = "");

  MemTracker* mem_tracker() const { return mem_tracker_.get(); }
  RuntimeProfile* profile() const { return profile_; }
  const std::vector<ScalarExprEvaluator*>& output_expr_evals() const {
    return output_expr_evals_;
  }
  bool is_closed() const { return closed_; }

 protected:
  /// Set to true after Close() has been called. Subclasses should check and set this in
  /// Close().
  bool closed_;

  /// The row descriptor for the rows consumed by the sink. Owned by root exec node of
  /// plan tree, which feeds into this sink.
  const RowDescriptor* row_desc_;

  /// The name to be used in profiles etc. Passed by derived classes in the ctor.
  const string name_;

  /// The runtime profile for this DataSink. Initialized in ctor. Not owned.
  RuntimeProfile* profile_ = nullptr;

  /// The MemTracker for all allocations made by the DataSink. Initialized in Prepare().
  boost::scoped_ptr<MemTracker> mem_tracker_;

  /// A child of 'mem_tracker_' that tracks expr allocations. Initialized in Prepare().
  boost::scoped_ptr<MemTracker> expr_mem_tracker_;

  /// MemPool for allocations made by expression evaluators in this sink that are
  /// "permanent" and live until Close() is called.
  boost::scoped_ptr<MemPool> expr_perm_pool_;

  /// MemPool for allocations made by expression evaluators in this sink that hold
  /// intermediate or final results of expression evaluation. Should be cleared
  /// periodically to free accumulated memory.
  boost::scoped_ptr<MemPool> expr_results_pool_;

  /// Output expressions to convert row batches onto output values.
  /// Not used in some sub-classes.
  std::vector<ScalarExpr*> output_exprs_;
  std::vector<ScalarExprEvaluator*> output_expr_evals_;

  /// Initialize the expressions in the data sink and return error status on failure.
  virtual Status Init(const std::vector<TExpr>& thrift_output_exprs,
      const TDataSink& tsink, RuntimeState* state);
};
} // namespace impala
#endif
