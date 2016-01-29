// Copyright 2012 Cloudera Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.


#ifndef IMPALA_EXEC_DATA_SINK_H
#define IMPALA_EXEC_DATA_SINK_H

#include <boost/scoped_ptr.hpp>
#include <vector>

#include "common/status.h"
#include "runtime/runtime-state.h"
#include "gen-cpp/DataSinks_types.h"
#include "gen-cpp/Exprs_types.h"

namespace impala {

class ObjectPool;
class RowBatch;
class RuntimeProfile;
class RuntimeState;
class TPlanExecRequest;
class TPlanExecParams;
class TPlanFragmentExecParams;
class RowDescriptor;

/// Superclass of all data sinks.
class DataSink {
 public:
  DataSink() : closed_(false) { }
  virtual ~DataSink() {}

  /// Setup. Call before Send(), Open(), or Close().
  /// Subclasses must call DataSink::Prepare().
  virtual Status Prepare(RuntimeState* state);

  /// Call before Send() or Close().
  virtual Status Open(RuntimeState* state) = 0;

  /// Send a row batch into this sink.
  /// eos should be true when the last batch is passed to Send()
  virtual Status Send(RuntimeState* state, RowBatch* batch, bool eos) = 0;

  /// Flushes any remaining buffered state.
  /// Further Send() calls are illegal after FlushFinal(). This is to be called only
  /// before calling Close().
  virtual Status FlushFinal(RuntimeState* state) = 0;

  /// Releases all resources that were allocated in Prepare()/Send().
  /// Further Send() calls or FlushFinal() calls are illegal after calling Close().
  /// It must be okay to call this multiple times. Subsequent calls should be ignored.
  virtual void Close(RuntimeState* state);

  /// Creates a new data sink from thrift_sink. A pointer to the
  /// new sink is written to *sink, and is owned by the caller.
  static Status CreateDataSink(ObjectPool* pool,
    const TDataSink& thrift_sink, const std::vector<TExpr>& output_exprs,
    const TPlanFragmentExecParams& params,
    const RowDescriptor& row_desc, boost::scoped_ptr<DataSink>* sink);

  /// Returns the runtime profile for the sink.
  virtual RuntimeProfile* profile() = 0;

  /// Merges one update to the insert stats for a partition. dst_stats will have the
  /// combined stats of src_stats and dst_stats after this method returns.
  static void MergeInsertStats(const TInsertStats& src_stats,
      TInsertStats* dst_stats);

  /// Outputs the insert stats contained in the map of insert partition updates to a string
  static std::string OutputInsertStats(const PartitionStatusMap& stats,
      const std::string& prefix = "");

 protected:
  /// Set to true after Close() has been called. Subclasses should check and set this in
  /// Close().
  bool closed_;

  boost::scoped_ptr<MemTracker> expr_mem_tracker_;

};

}  // namespace impala
#endif
