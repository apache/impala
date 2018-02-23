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


#ifndef IMPALA_RUNTIME_KRPC_DATA_STREAM_SENDER_H
#define IMPALA_RUNTIME_KRPC_DATA_STREAM_SENDER_H

#include <vector>
#include <string>

#include "exec/data-sink.h"
#include "common/global-types.h"
#include "common/object-pool.h"
#include "common/status.h"
#include "runtime/row-batch.h"
#include "util/runtime-profile.h"

namespace impala {

class RowDescriptor;
class MemTracker;
class TDataStreamSink;
class TNetworkAddress;
class TPlanFragmentDestination;

/// Single sender of an m:n data stream.
///
/// Row batch data is routed to destinations based on the provided partitioning
/// specification.
/// *Not* thread-safe.
///
/// TODO: capture stats that describe distribution of rows/data volume
/// across channels.
/// TODO: create a PlanNode equivalent class for DataSink.
class KrpcDataStreamSender : public DataSink {
 public:
  /// Construct a sender according to the output specification (tsink), sending to the
  /// given destinations:
  /// 'sender_id' identifies this sender instance, and is unique within a fragment.
  /// 'row_desc' is the descriptor of the tuple row. It must out-live the sink.
  /// 'destinations' are the receivers' network addresses. There is one channel for each
  /// destination.
  /// 'per_channel_buffer_size' is the soft limit in bytes of the buffering into the
  /// per-channel's accumulating row batch before it will be sent.
  /// NOTE: supported partition types are UNPARTITIONED (broadcast), HASH_PARTITIONED,
  /// and RANDOM.
  KrpcDataStreamSender(int sender_id, const RowDescriptor* row_desc,
      const TDataStreamSink& tsink,
      const std::vector<TPlanFragmentDestination>& destinations,
      int per_channel_buffer_size, RuntimeState* state);

  virtual ~KrpcDataStreamSender();

  /// Initialize the sender by initializing all the channels and allocates all
  /// the stat counters. Return error status if any channels failed to initialize.
  virtual Status Prepare(RuntimeState* state, MemTracker* parent_mem_tracker);

  /// Initialize the evaluator of the partitioning expressions. Return error status
  /// if initialization failed.
  virtual Status Open(RuntimeState* state);

  /// Flush all buffered data and close all existing channels to destination hosts.
  /// Further Send() calls are illegal after calling FlushFinal(). It is legal to call
  /// FlushFinal() no more than once. Return error status if Send() failed or the end
  /// of stream call failed.
  virtual Status FlushFinal(RuntimeState* state);

  /// Send data in 'batch' to destination nodes according to partitioning
  /// specification provided in c'tor.
  /// Blocks until all rows in batch are placed in their appropriate outgoing
  /// buffers (ie, blocks if there are still in-flight rpcs from the last
  /// Send() call).
  virtual Status Send(RuntimeState* state, RowBatch* batch);

  /// Shutdown all existing channels to destination hosts. Further FlushFinal() calls are
  /// illegal after calling Close().
  virtual void Close(RuntimeState* state);

 protected:
  friend class DataStreamTest;

  /// Initialize any partitioning expressions based on 'thrift_output_exprs' and stores
  /// them in 'partition_exprs_'. Returns error status if the initialization failed.
  virtual Status Init(const std::vector<TExpr>& thrift_output_exprs,
      const TDataSink& tsink, RuntimeState* state);

  /// Return total number of bytes sent. If batches are broadcast to multiple receivers,
  /// they are counted once per receiver.
  int64_t GetNumDataBytesSent() const;

 private:
  class Channel;

  /// Serializes the src batch into the serialized row batch 'dest' and updates
  /// various stat counters.
  /// 'num_receivers' is the number of receivers this batch will be sent to. Used for
  /// updating the stat counters.
  Status SerializeBatch(RowBatch* src, OutboundRowBatch* dest, int num_receivers = 1);

  /// Sender instance id, unique within a fragment.
  const int sender_id_;

  /// The type of partitioning to perform.
  const TPartitionType::type partition_type_;

  /// Amount of per-channel buffering for rows before sending them to the destination.
  const int per_channel_buffer_size_;

  /// RuntimeState of the fragment instance.
  RuntimeState* state_ = nullptr;

  /// Index of the current channel to send to if random_ == true.
  int current_channel_idx_ = 0;

  /// Index of the next OutboundRowBatch to use for serialization.
  int next_batch_idx_ = 0;

  /// The outbound row batches are double-buffered so that we can serialize the next
  /// batch while the other is still referenced by the in-flight RPC. Each entry contains
  /// a RowBatchHeaderPB and buffers for the serialized tuple offsets and data. Used only
  /// when the partitioning strategy is UNPARTITIONED.
  static const int NUM_OUTBOUND_BATCHES = 2;
  OutboundRowBatch outbound_batches_[NUM_OUTBOUND_BATCHES];

  /// If true, this sender has called FlushFinal() successfully.
  /// Not valid to call Send() anymore.
  bool flushed_ = false;

  /// If true, this sender has been closed. Not valid to call Send() anymore.
  bool closed_ = false;

  /// List of all channels. One for each destination.
  std::vector<Channel*> channels_;

  /// Expressions of partition keys. It's used to compute the
  /// per-row partition values for shuffling exchange;
  std::vector<ScalarExpr*> partition_exprs_;
  std::vector<ScalarExprEvaluator*> partition_expr_evals_;

  /// Time for serializing row batches.
  RuntimeProfile::Counter* serialize_batch_timer_ = nullptr;

  /// Number of TransmitData() RPC retries due to remote service being busy.
  RuntimeProfile::Counter* rpc_retry_counter_ = nullptr;

  /// Total number of times RPC fails or the remote responds with a non-retryable error.
  RuntimeProfile::Counter* rpc_failure_counter_ = nullptr;

  /// Total number of bytes sent.
  RuntimeProfile::Counter* bytes_sent_counter_ = nullptr;

  /// Total number of EOS sent.
  RuntimeProfile::Counter* eos_sent_counter_ = nullptr;

  /// Total number of bytes of the row batches before compression.
  RuntimeProfile::Counter* uncompressed_bytes_counter_ = nullptr;

  /// Total number of rows sent.
  RuntimeProfile::Counter* total_sent_rows_counter_ = nullptr;

  /// Throughput per total time spent in sender
  RuntimeProfile::Counter* overall_throughput_ = nullptr;

  /// Identifier of the destination plan node.
  PlanNodeId dest_node_id_;

  /// Used for Kudu partitioning to round-robin rows that don't correspond to a partition
  /// or when errors are encountered.
  int next_unknown_partition_;

  /// An arbitrary hash seed used for exchanges.
  static constexpr uint64_t EXCHANGE_HASH_SEED = 0x66bd68df22c3ef37;
};

} // namespace impala

#endif // IMPALA_RUNTIME_KRPC_DATA_STREAM_SENDER_H
