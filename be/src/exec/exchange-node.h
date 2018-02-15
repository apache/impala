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


#ifndef IMPALA_EXEC_EXCHANGE_NODE_H
#define IMPALA_EXEC_EXCHANGE_NODE_H

#include <boost/scoped_ptr.hpp>
#include "exec/exec-node.h"

#include "runtime/bufferpool/buffer-pool.h"

namespace impala {

class DataStreamRecvrBase;
class RowBatch;
class ScalarExpr;
class TupleRowComparator;

/// Receiver node for data streams. The data stream receiver is created in Prepare()
/// and closed in Close().
/// is_merging is set to indicate that rows from different senders must be merged
/// according to the sort parameters in sort_exec_exprs_. (It is assumed that the rows
/// received from the senders themselves are sorted.)
/// If is_merging_ is true, the exchange node creates a DataStreamRecvrBase with the
/// is_merging_ flag and retrieves retrieves rows from the receiver via calls to
/// DataStreamRecvrBase::GetNext(). It also prepares, opens and closes the ordering exprs
/// in its SortExecExprs member that are used to compare rows.
/// If is_merging_ is false, the exchange node directly retrieves batches from the row
/// batch queue of the DataStreamRecvrBase via calls to DataStreamRecvrBase::GetBatch().
class ExchangeNode : public ExecNode {
 public:
  ExchangeNode(ObjectPool* pool, const TPlanNode& tnode, const DescriptorTbl& descs);

  virtual Status Init(const TPlanNode& tnode, RuntimeState* state);
  virtual Status Prepare(RuntimeState* state);
  virtual void Codegen(RuntimeState* state);
  /// Blocks until the first batch is available for consumption via GetNext().
  virtual Status Open(RuntimeState* state);
  virtual Status GetNext(RuntimeState* state, RowBatch* row_batch, bool* eos);
  virtual Status Reset(RuntimeState* state);
  virtual void Close(RuntimeState* state);

  /// the number of senders needs to be set after the c'tor, because it's not
  /// recorded in TPlanNode, and before calling Prepare()
  void set_num_senders(int num_senders) { num_senders_ = num_senders; }

 protected:
  virtual void DebugString(int indentation_level, std::stringstream* out) const;

 private:
  /// Implements GetNext() for the case where is_merging_ is true. Delegates the GetNext()
  /// call to the underlying DataStreamRecvrBase.
  Status GetNextMerging(RuntimeState* state, RowBatch* output_batch, bool* eos);

  /// Resets input_batch_ to the next batch from the from stream_recvr_'s queue.
  /// Only used when is_merging_ is false.
  Status FillInputRowBatch(RuntimeState* state);

  int num_senders_;  // needed for stream_recvr_ construction

  /// The underlying DataStreamRecvrBase instance. Ownership is shared between this
  /// exchange node instance and the DataStreamMgr used to create the receiver.
  /// stream_recvr_->Close() must be called before this instance is destroyed.
  std::shared_ptr<DataStreamRecvrBase> stream_recvr_;

  /// our input rows are a prefix of the rows we produce
  RowDescriptor input_row_desc_;

  /// Current batch of rows from the receiver queue being processed by this node.
  /// Only valid if is_merging_ is false. (If is_merging_ is true, GetNext() is
  /// delegated to the receiver). Owned by the stream receiver.
  RowBatch* input_batch_;

  /// Next row to copy from input_batch_. For non-merging exchanges, input_batch_
  /// is retrieved directly from the sender queue in the stream recvr, and rows from
  /// input_batch_ must be copied to the output batch in GetNext().
  int next_row_idx_;

  /// The buffer pool client for allocating buffers for tuple pointers and
  /// tuple data in row batches.
  BufferPool::ClientHandle recvr_buffer_pool_client_;

  /// time spent reconstructing received rows
  RuntimeProfile::Counter* convert_row_batch_timer_;

  /// True if this is a merging exchange node. If true, GetNext() is delegated to the
  /// underlying stream_recvr_, and input_batch_ is not used/valid.
  bool is_merging_;

  /// The TupleRowComparator based on 'sort_exec_exprs_' for merging exchange.
  boost::scoped_ptr<TupleRowComparator> less_than_;

  /// Sort expressions and parameters passed to the merging receiver..
  std::vector<ScalarExpr*> ordering_exprs_;
  std::vector<bool> is_asc_order_;
  std::vector<bool> nulls_first_;

  /// Offset specifying number of rows to skip.
  int64_t offset_;

  /// Number of rows skipped so far.
  int64_t num_rows_skipped_;
};

};

#endif
