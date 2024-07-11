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

#include <condition_variable>
#include <string>
#include <unordered_map>
#include <vector>

#include <boost/intrusive/list.hpp>

#include "codegen/impala-ir.h"
#include "common/global-types.h"
#include "common/object-pool.h"
#include "common/status.h"
#include "exec/data-sink.h"
#include "exprs/scalar-expr.h"
#include "runtime/mem-tracker.h"
#include "runtime/outbound-row-batch.h"
#include "util/container-util.h"
#include "util/runtime-profile.h"

#include "gen-cpp/common.pb.h"

namespace impala {

class KrpcDataStreamSender;
class MemTracker;
class NetworkAddressPB;
class PlanFragmentDestinationPB;
class RowBatch;
class RowDescriptor;
class TDataStreamSink;
class TNetworkAddress;

class KrpcDataStreamSenderConfig : public DataSinkConfig {
 public:
  DataSink* CreateSink(RuntimeState* state) const override;
  void Close() override;

  /// Codegen KrpcDataStreamSender::HashAndAddRows() if partitioning type is
  /// HASH_PARTITIONED. Replaces KrpcDataStreamSender::HashRow() and
  /// KrpcDataStreamSender::GetNumChannels() based on runtime information.
  void Codegen(FragmentState* state) override;

  /// The type of partitioning to perform.
  TPartitionType::type partition_type_ = TPartitionType::UNPARTITIONED;

  /// Expressions of partition keys. It's used to compute the
  /// per-row partition values for shuffling exchange;
  std::vector<ScalarExpr*> partition_exprs_;

  /// The number of channels that this node will create.
  int num_channels_;

  /// Hash seed used for exchanges. Query id will be used to seed the hash function.
  uint64_t exchange_hash_seed_;

  /// Type and pointer for the codegen'd KrpcDataStreamSender::HashAndAddRows()
  /// function. NULL if codegen is disabled or failed.
  typedef Status (*HashAndAddRowsFn)(KrpcDataStreamSender*, RowBatch* row);
  CodegenFnPtr<HashAndAddRowsFn> hash_and_add_rows_fn_;

  typedef Status (*SerializeBatchFn)(RowBatch*, OutboundRowBatch* output_batch,
      TrackedString* compression_scratch);

  CodegenFnPtr<SerializeBatchFn> serialize_batch_fn_;

  ~KrpcDataStreamSenderConfig() override {}

 protected:
  Status Init(const TDataSink& tsink, const RowDescriptor* input_row_desc,
      FragmentState* state) override;

 private:
  /// Codegen the KrpcDataStreamSender::HashRow() function and returns the codegen'd
  /// function in 'fn'. This involves unrolling the loop in HashRow(), codegens each of
  /// the partition expressions and replaces the column type argument to the hash function
  /// with constants to eliminate some branches. Returns error status on failure.
  Status CodegenHashRow(LlvmCodeGen* codegen, llvm::Function** fn);

  /// Returns the name of the partitioning type of this data stream sender.
  std::string PartitionTypeName() const;
};

/// Single sender of an m:n data stream.
///
/// Row batch data is routed to destinations based on the provided partitioning
/// specification:
/// UNPARTITIONED: each batch is sent to one or more channels (broadcast)
/// RANDOM: each batch is sent to one channel (round robin)
/// HASH_PARTITIONED, KUDU: rows are sent to channel based on hash of key expression
/// DIRECTED: rows are sent to channel based on known key->host mapping
///
/// Multiple targets are handled with class Channel and OutboundQueue.
/// Channel:
/// - represents a single destination (host+fragment instance+node)
/// - can have a single in-flight RPC
/// OutboundQueue:
/// - 1 per Channel, with the exception of broadcast (single queue for all channels)
/// - owns OutboundRowBatch(es) until all target channels finished TransmitData RPC
///
/// Overview of sending a row batch (Send()):
/// 1. wait for buffer(s) (OutboundRowBatch) to serialize into (WaitForCapacity())
/// 2. serialize the input RowBatch into the buffer(s)
///   a. UNPARTITIONED, RANDOM: serialize each RowBatch into an OutboundRowBatch
///   b. HASH_PARTITIONED, KUDU, DIRECTED: serialize rows into per-partition buffers
///      (PartitionRowCollector, IcebergPositionDeleteChannel)
/// 3. once a buffer is ready to send compress it and add it to an OutboundQueue
/// 4. the OutboundQueue calls TransmitData on the appropriate Channel(s) to initiate
///    the (async) RPC to the other host
/// 5. when an RPC completes, the Channel calls back on a KRPC reactor thread to the
///    OutboundQueue which returns free buffers (ReleaseBatch()), potentially unblocking
///    WaitForCapacity() in step 1.
///
/// Send() can return after step 3, allowing working on the execution tree while
/// KrpcDataStreamSender sends the queued OuboundRowBatch(es) in the background.
/// Step 4. can happen both in Send() if the channel is idle, or in the RPC completion
/// callback if there is another ready-to-send OutboundBatch in the OutboundQueue.
///
/// Thread safety:
/// Most functions are always called from the fragment instance thread (through Send()
/// and FlushFinal()). Results of outgoing RPCs arrive on krpc reactor threads
/// (TransmitDataCompleteCb() and EndDataStreamCompleteCb()). Synchronization is done
/// using multiple locks (batch_pool_lock_, OutboundQueue::lock_ and Channel::lock_) with
/// a well-defined lock ordering:
///   1. Channel::lock_
///   2. OutboundQueue::lock_ or batch_pool_lock_ (independent of each other)
/// OutboundQueue::lock_ and batch_pool_lock_ are never held simultaneously.
///
/// TODO: capture stats that describe distribution of rows/data volume
/// across channels.
class KrpcDataStreamSender : public DataSink {
 public:
  /// Constructs a sender according to the config (sink_config), sending to the
  /// given destinations:
  /// 'sender_id' identifies this sender instance, and is unique within a fragment.
  /// 'destinations' are the receivers' network addresses. There is one channel for each
  /// destination.
  /// 'per_channel_buffer_size' is the soft limit in bytes of the buffering into the
  /// per-channel's accumulating row batch before it will be sent.
  /// NOTE: supported partition types are UNPARTITIONED (broadcast), HASH_PARTITIONED,
  /// KUDU, RANDOM, and DIRECTED (used for sending rows from Iceberg delete files).
  KrpcDataStreamSender(TDataSinkId sink_id, int sender_id,
      const KrpcDataStreamSenderConfig& sink_config, const TDataStreamSink& sink,
      const google::protobuf::RepeatedPtrField<PlanFragmentDestinationPB>& destinations,
      int per_channel_buffer_size, RuntimeState* state);

  virtual ~KrpcDataStreamSender();

  /// Initializes the sender by initializing all the channels and allocates all
  /// the stat counters. Return error status if any channels failed to initialize.
  virtual Status Prepare(RuntimeState* state, MemTracker* parent_mem_tracker) override;

  /// Initializes the evaluator of the partitioning expressions. Return error status
  /// if initialization failed.
  virtual Status Open(RuntimeState* state) override;

  /// Flushes all buffered data and close all existing channels to destination hosts.
  /// Further Send() calls are illegal after calling FlushFinal(). It is legal to call
  /// FlushFinal() no more than once. Return error status if Send() failed or the end
  /// of stream call failed.
  virtual Status FlushFinal(RuntimeState* state) override;

  /// Sends data in 'batch' to destination nodes according to partitioning
  /// specification provided in c'tor.
  /// Blocks until all rows in batch are placed in their appropriate outgoing
  /// buffers (ie, blocks if there is no free buffer available).
  virtual Status Send(RuntimeState* state, RowBatch* batch) override;

  /// Shutdown all existing channels to destination hosts. Further FlushFinal() calls are
  /// illegal after calling Close().
  virtual void Close(RuntimeState* state) override;

  /// Counters shared with other parts of the code
  static const char* TOTAL_BYTES_SENT_COUNTER;

  /// KrpcDataStreamSender::HashRow() symbol. Used for call-site replacement.
  static const char* HASH_ROW_SYMBOL;

  /// An arbitrary constant used to seed the hash.
  static constexpr uint64_t EXCHANGE_HASH_SEED_CONST = 0x66bd68df22c3ef37;

  static const char* LLVM_CLASS_NAME;

 protected:
  friend class DataStreamTest;

  /// Returns total number of bytes sent. If batches are broadcast to multiple receivers,
  /// they are counted once per receiver.
  int64_t GetNumDataBytesSent() const;

 private:
  class Channel;
  class IcebergPositionDeleteChannel;
  class OutboundQueue;

  using OutboundBatchList = boost::intrusive::list<OutboundRowBatch>;

  // Per partition structure to collect rows before sending the OutboundRowBatch to
  // Channel. Only used in HASH/KUDU partitioning.
  struct PartitionRowCollector {
    OutboundRowBatch* collector_batch_ = nullptr;
    KrpcDataStreamSender* parent_ = nullptr;
    Channel* channel_ = nullptr;
    OutboundQueue* queue_ = nullptr;
    int num_rows_ = 0;
    int row_batch_capacity_ = 0;

    // Copies a single row into collector_batch_ and flushes it (EnqueueCurrentBatch())
    // once row count or memory capacity is reached. May block in WaitForCapacity() if
    // the batch pool is exhausted. Returns error status if serialization failed or if
    // any RPC failed. Returns OK otherwise.
    Status IR_ALWAYS_INLINE AppendRow(
        const TupleRow* row, const RowDescriptor* row_desc);

    // Finalizes and compresses collector_batch_ and submits it to queue_.
    // May block in WaitForCapacity() if the batch pool is exhausted.
    // Replaces collector_batch_ with a fresh batch obtained from the pool.
    // Returns error status if serialization failed or if any RPC failed.
    // Returns OK otherwise.
    Status EnqueueCurrentBatch();
  };
  std::vector<PartitionRowCollector> partition_row_collectors_;

  // Manages in-flight OutboundRowBatch sends to one or more channels. For broadcast
  // (UNPARTITIONED) senders a single queue fans out to all channels; for partitioned
  // senders each channel has its own queue. The parent KrpcDataStreamSender owns the
  // batch pool and limits the total number of in-flight OutboundRowBatches.
  class OutboundQueue {
  public:
    // Constructs a queue for the given set of channels (UNPARTITIONED).
    OutboundQueue(const std::vector<std::unique_ptr<Channel>>& channels,
        KrpcDataStreamSender* parent);

    // Constructs a single-channel queue for per-partition queuing (HASH/KUDU/RANDOM/
    // DIRECTED).
    OutboundQueue(Channel* channel, KrpcDataStreamSender* parent);

    ~OutboundQueue();

    // Enqueues 'batch' for delivery to all channels. Non-blocking. The caller must
    // have obtained 'batch' from the parent's batch pool (via WaitForCapacity()).
    // Dispatches the batch immediately to any currently idle channels and enqueues it
    // for busy ones.
    Status Add(OutboundRowBatch* batch);

    // Signals that no more batches will be added. Sets 'eos_' so that each in-flight
    // channel will send its EOS RPC immediately from the reactor thread once it delivers
    // its last data batch. Idle channels (currently in idle_channels_) are sent EOS
    // directly before this call returns. Returns without waiting for in-flight batches
    // to complete; call WaitUntilEmpty() after to block until the queue is empty.
    Status FlushFinal();

    // Waits until the queue is fully empty (queue_.empty()) and all channels have had
    // their EOS RPC complete (or are permanently closed). Must be called after
    // FlushFinal().
    Status WaitUntilEmpty();

    // Called from a channel's TransmitData completion callback on the KRPC reactor
    // thread once the RPC for 'batch' has finished on 'channel'.'closed' is true if the
    // remote receiver reported DATASTREAM_RECVR_CLOSED.
    // Returns the next batch to send on 'channel' (taken from the queue), or nullptr
    // if there are no more batches. Sets '*send_eos' to true if the channel has delivered
    // its last data batch and should send an EOS RPC immediately.
    // Invariant: never returns a non-null batch and sets '*eos' at the same time.
    OutboundRowBatch* RpcFinished(OutboundRowBatch* batch, Channel* channel,
        bool closed, bool* send_eos);

    // Called from a channel's EndDataStream completion callback on the KRPC reactor
    // thread once the EOS RPC for 'channel' has finished. 'status' is the RPC result.
    // Increments eos_completed_count_ and wakes WaitUntilEmpty() if all channels are
    // done.
    void EosFinished();

    // Returns the number of batches currently queued. Takes lock_.
    int Size();

    // Records 'status' as the queue error, unblocks WaitUntilEmpty() and
    // WaitForCapacity().
    void SetError(const Status& status);

  private:
    // Protects all fields below.
    // Lock ordering: see class-level comment. Channel::lock_ -> OutboundQueue::lock_.
    SpinLock lock_;

    // Signalled by NotifyIfAllChannelsDone() when all channels have completed (either
    // via EOS or closure), allowing WaitUntilEmpty() to recheck its exit condition.
    std::condition_variable_any queue_empty_cv_;

    // All channels managed (but not owned) by this queue.
    std::vector<Channel*> channels_;

    // Batches currently in flight. The front of the queue is the oldest batch, sent
    // first. A batch is removed from the front only once all channels have finished
    // sending it (OutboundRowBatch::consumers_left_ reaches 0).
    // Intrusive list — no per-node heap allocation.
    OutboundBatchList queue_;

    // Parent sender. Not owned.
    KrpcDataStreamSender* parent_;

    // Channels that are currently idle (not sending any batch). These are sent to
    // immediately when Add() enqueues a new batch.
    // Intrusive list — no per-node heap allocation. Defined in .cc where Channel is
    // complete; stored as opaque struct behind unique_ptr.
    struct IdleList;
    std::unique_ptr<IdleList> idle_channels_;

    // Number of channels that have reported DATASTREAM_RECVR_CLOSED and are therefore
    // permanently excluded from future sends.
    int closed_channel_count_ = 0;

    // Set by FlushFinal(). When true, RpcFinished() signals each channel to send EOS
    // immediately once it has delivered its last data batch.
    bool eos_ = false;

    // Number of channels for which the EOS RPC has completed (EndDataStreamCompleteCb
    // fired and reported a final result). Together with closed_channel_count_, used by
    // WaitUntilEmpty() to determine when all channels are done.
    int eos_completed_count_ = 0;

    // Sticky error status. Set on the first RPC failure.
    Status status_;

    // Notifies queue_empty_cv_ if every channel is accounted for, i.e.
    // eos_completed_count_ + closed_channel_count_ == channels_.size().
    // Must be called with lock_ held.
    void NotifyIfAllChannelsDone();
  };

  /// Serializes the src batch into the serialized row batch 'dest' and updates
  /// various stat counters.
  /// 'compress' decides whether compression is attempted after serialization.
  /// 'num_receivers' is the number of receivers this batch will be sent to. Used for
  /// updating the stat counters.
  Status SerializeBatch(
      RowBatch* src, OutboundRowBatch* dest, bool compress, int num_receivers = 1);

  // Like SerializeBatch, but the batch is already serialized and only compression is
  // needed.
  Status PrepareBatchForSend(OutboundRowBatch* batch, bool compress);

  /// Returns 'partition_expr_evals_[i]'. Used by the codegen'd HashRow() IR function.
  ScalarExprEvaluator* GetPartitionExprEvaluator(int i);

  /// Returns the number of channels in this data stream sender. Not inlined for the
  /// cross-compiled code as it's to be replaced with a constant during codegen.
  int IR_NO_INLINE GetNumChannels() const { return channels_.size(); }

  /// Evaluates the input row against partition expressions and hashes the expression
  /// values. Returns the final hash value.
  uint64_t HashRow(TupleRow* row, uint64_t seed);

  /// Used when 'partition_type_' is HASH_PARTITIONED. Call HashRow() against each row
  /// in the input batch and adds it to the corresponding channel based on the hash value.
  /// Cross-compiled to be patched by Codegen() at runtime. Returns error status if
  /// insertion into the channel fails. Returns OK status otherwise.
  Status HashAndAddRows(RowBatch* batch);

  /// Functions to dump the content of the "filename to hosts" related mappings into logs.
  void DumpFilenameToHostsMapping() const;
  void DumpDestinationHosts() const;

  bool IsDirectedMode() const { return !filepath_to_hosts_.empty(); }

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

  /// Buffer used for compression after serialization. Swapped with the OutboundRowBatch's
  /// tuple_data_ if the compressed data is smaller.
  std::unique_ptr<TrackedString> compression_scratch_;

  /// If true, this sender has called FlushFinal() successfully.
  /// Not valid to call Send() anymore.
  bool flushed_ = false;

  /// List of all channels. One for each destination.
  std::vector<std::unique_ptr<Channel>> channels_;

  /// Expressions of partition keys. It's used to compute the
  /// per-row partition values for shuffling exchange;
  const std::vector<ScalarExpr*>& partition_exprs_;
  std::vector<ScalarExprEvaluator*> partition_expr_evals_;

  /// Time for serializing row batches. In case of Kudu/Hash partitioning
  /// this mainly included compression time, while in other cases also
  /// contains deep copying tuples to OutboundRowBatch.
  RuntimeProfile::Counter* serialize_batch_timer_ = nullptr;

  /// "Active" time spent in TransmitData(). Waiting for the previous RPC to
  /// finish is not included.
  RuntimeProfile::Counter* transmit_data_timer_ = nullptr;

  /// Number of TransmitData() RPC retries due to remote service being busy.
  RuntimeProfile::Counter* rpc_retry_counter_ = nullptr;

  /// Total number of times RPC fails or the remote responds with a non-retryable error.
  RuntimeProfile::Counter* rpc_failure_counter_ = nullptr;

  /// Total number of successful TransmitData() and EndDataStream() RPCs.
  RuntimeProfile::Counter* rpc_success_counter_ = nullptr;

  /// Total number of bytes sent. Updated on RPC completion.
  RuntimeProfile::Counter* bytes_sent_counter_ = nullptr;

  /// Time series of number of bytes sent, samples bytes_sent_counter_.
  RuntimeProfile::TimeSeriesCounter* bytes_sent_time_series_counter_ = nullptr;

  /// Total number of EOS sent.
  RuntimeProfile::Counter* eos_sent_counter_ = nullptr;

  /// Total number of bytes of row batches before compression.
  RuntimeProfile::Counter* uncompressed_bytes_counter_ = nullptr;

  /// Total number of rows sent.
  RuntimeProfile::Counter* total_sent_rows_counter_ = nullptr;

  /// Summary of network throughput for sending row batches. Network time also includes
  /// queuing time in KRPC transfer queue for transmitting the RPC requests and receiving
  /// the responses.
  RuntimeProfile::SummaryStatsCounter* network_throughput_counter_ = nullptr;

  /// Summary of network time for sending row batches and eos. Network time also includes
  /// queuing time in KRPC transfer queue for transmitting the RPC requests and receiving
  /// the responses.
  RuntimeProfile::SummaryStatsCounter* network_time_stats_ = nullptr;

  /// Summary of network time spent processing requests on the receiver side. The total
  /// RPC time is the sum of receiver and network time.
  RuntimeProfile::SummaryStatsCounter* recvr_time_stats_ = nullptr;

  /// Identifier of the destination plan node.
  PlanNodeId dest_node_id_;

  /// Memory tracker from Parent Memory Tracker for tracking memory of OutBoundRowBatch
  /// serialization
  std::shared_ptr<MemTracker> outbound_rb_mem_tracker_;
  std::shared_ptr<CharMemTrackerAllocator> char_mem_tracker_allocator_;

  /// Used for Kudu partitioning to round-robin rows that don't correspond to a partition
  /// or when errors are encountered.
  int next_unknown_partition_;

  /// Hash seed used for exchanges. Query id will be used to seed the hash function.
  uint64_t exchange_hash_seed_;

  /// Pointer for the codegen'd HashAndAddRows() function.
  /// NULL if codegen is disabled or failed.
  const CodegenFnPtr<KrpcDataStreamSenderConfig::HashAndAddRowsFn>& hash_and_add_rows_fn_;

  const CodegenFnPtr<KrpcDataStreamSenderConfig::SerializeBatchFn>& serialize_batch_fn_;

  /// Mapping to store which data file is read on which hosts.
  const std::unordered_map<std::string, std::vector<NetworkAddressPB>>&
      filepath_to_hosts_;

  /// A mapping between host addresses to channels. Used for DIRECTED distribution mode
  /// where only one channel is associated with each host address.
  std::unordered_map<NetworkAddressPB, Channel*> host_to_channel_;

  /// A mapping from Channel to IcebergPositionDeleteChannel. Only used in DIRECTED mode
  /// where IcebergPositionDeleteChannel applies a specific serialization algorithm on
  /// position delete records.
  std::unordered_map<Channel*, std::unique_ptr<IcebergPositionDeleteChannel>>
    channel_to_ice_channel_;

  // Unified queue vector. For UNPARTITIONED there is one queue shared by all channels.
  // For all other partition types there is one queue per channel (same index).
  std::vector<std::unique_ptr<OutboundQueue>> queues_;

  // --- Batch pool ---
  // Owned pool of free OutboundRowBatch buffers shared across all send paths.
  // KrpcDataStreamSender controls the total number of in-flight batches;
  // OutboundQueue calls ReleaseBatch() when a batch is fully delivered.

  // Protects free_batch_pool_, batch_pool_error_, and is used by batch_pool_cv_.
  SpinLock batch_pool_lock_;

  // Signalled when a buffer is returned to free_batch_pool_ or when batch_pool_error_
  // is set, allowing WaitForCapacity() to unblock.
  std::condition_variable_any batch_pool_cv_;

  // Pool of free OutboundRowBatch buffers available for serialization.
  // Intrusive list — no per-node heap allocation.
  OutboundBatchList free_batch_pool_;

  // Owns all allocated OutboundRowBatch objects. Grows monotonically up to
  // batch_pool_max_size_; destroyed in Close(). The intrusive lists (free_batch_pool_,
  // OutboundQueue::queue_) merely organize these objects by state.
  std::vector<std::unique_ptr<OutboundRowBatch>> all_batches_;

  // Maximum number of OutboundRowBatches that may be simultaneously in-flight
  // (i.e. queued or being sent).
  int batch_pool_max_size_ = 0;

  // Maximum number of batches that may be queued in a single OutboundQueue before
  // WaitForCapacity() forces reuse of a pooled batch rather than allocating a new one.
  int queue_depth_limit_before_reuse_ = 0;

  // Number of OutboundRowBatches created so far (both in-pool and in-flight).
  // Protected by batch_pool_lock_. Incremented lazily in WaitForCapacity() up to
  // batch_pool_max_size_; never decremented.
  int batches_allocated_ = 0;

  // Sticky error propagated from a channel RPC failure into WaitForCapacity().
  Status batch_pool_error_;

  // Blocks until a free OutboundRowBatch buffer is available in free_batch_pool_,
  // then sets '*batch' to point to it. If 'queue' is non-null, checks queue->Size()
  // upfront to avoid allocating a new batch when the channel's queue is already at
  // capacity; in that case the call blocks until a pooled batch is available. Returns
  // error status if a channel RPC failed or the query was cancelled. Must be called
  // before SerializeBatch() each iteration.
  // Called only from fragment instance thread.
  Status WaitForCapacity(OutboundRowBatch** batch, OutboundQueue* queue = nullptr);

  // Returns 'batch' to free_batch_pool_ and signals WaitForCapacity(). Called from
  // OutboundQueue::RpcFinished() on the KRPC reactor thread.
  // Called only from KRPC reactor thread.
  void ReleaseBatch(OutboundRowBatch* batch);

  // Sets batch_pool_error_ and wakes WaitForCapacity(). Called from OutboundQueue
  // on the first RPC failure.
  // Called only from KRPC reactor thread.
  void SetBatchPoolError(const Status& status);
};

} // namespace impala

#endif // IMPALA_RUNTIME_KRPC_DATA_STREAM_SENDER_H
