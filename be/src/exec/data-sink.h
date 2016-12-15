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
#include "runtime/runtime-state.h"
#include "util/runtime-profile.h"
#include "gen-cpp/DataSinks_types.h"
#include "gen-cpp/Exprs_types.h"

namespace impala {

class MemTracker;
class ObjectPool;
class RowBatch;
class RuntimeProfile;
class RuntimeState;
class TPlanExecRequest;
class TPlanExecParams;
class TPlanFragmentInstanceCtx;
class RowDescriptor;

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
  DataSink(const RowDescriptor& row_desc);
  virtual ~DataSink();

  /// Return the name to use in profiles, etc.
  virtual std::string GetName() = 0;

  /// Setup. Call before Send(), Open(), or Close() during the prepare phase of the query
  /// fragment. Creates a MemTracker for the sink that is a child of 'parent_mem_tracker'.
  /// Subclasses must call DataSink::Prepare().
  virtual Status Prepare(RuntimeState* state, MemTracker* parent_mem_tracker);

  /// Call before Send() to open the sink.
  virtual Status Open(RuntimeState* state) = 0;

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

  /// Creates a new data sink from thrift_sink. A pointer to the
  /// new sink is written to *sink, and is owned by the caller.
  static Status CreateDataSink(ObjectPool* pool,
    const TDataSink& thrift_sink, const std::vector<TExpr>& output_exprs,
    const TPlanFragmentInstanceCtx& fragment_instance_ctx,
    const RowDescriptor& row_desc, boost::scoped_ptr<DataSink>* sink);

  /// Merges one update to the DML stats for a partition. dst_stats will have the
  /// combined stats of src_stats and dst_stats after this method returns.
  static void MergeDmlStats(const TInsertStats& src_stats,
      TInsertStats* dst_stats);

  /// Outputs the DML stats contained in the map of partition updates to a string
  static std::string OutputDmlStats(const PartitionStatusMap& stats,
      const std::string& prefix = "");

  MemTracker* mem_tracker() const { return mem_tracker_.get(); }
  RuntimeProfile* profile() const { return profile_; }

 protected:
  /// Set to true after Close() has been called. Subclasses should check and set this in
  /// Close().
  bool closed_;

  /// The row descriptor for the rows consumed by the sink. Not owned.
  const RowDescriptor& row_desc_;

  /// The runtime profile for this DataSink. Initialized in Prepare(). Not owned.
  RuntimeProfile* profile_;

  /// The MemTracker for all allocations made by the DataSink. Initialized in Prepare().
  boost::scoped_ptr<MemTracker> mem_tracker_;

  /// A child of 'mem_tracker_' that tracks expr allocations. Initialized in Prepare().
  boost::scoped_ptr<MemTracker> expr_mem_tracker_;
};

} // namespace impala
#endif
