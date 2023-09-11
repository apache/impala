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

#include <string>
#include <unordered_map>
#include <vector>

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
  int  num_channels_;

  /// Hash seed used for exchanges. Query id will be used to seed the hash function.
  uint64_t exchange_hash_seed_;

  /// Type and pointer for the codegen'd KrpcDataStreamSender::HashAndAddRows()
  /// function. NULL if codegen is disabled or failed.
  typedef Status (*HashAndAddRowsFn)(KrpcDataStreamSender*, RowBatch* row);
  CodegenFnPtr<HashAndAddRowsFn> hash_and_add_rows_fn_;

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
/// specification.
/// *Not* thread-safe.
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
  /// RANDOM, and DIRECTED (used for sending rows from Iceberg delete files).
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
  /// buffers (ie, blocks if there are still in-flight rpcs from the last
  /// Send() call).
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

  /// Serializes the src batch into the serialized row batch 'dest' and updates
  /// various stat counters.
  /// 'compress' decides whether compression is attempted after serialization.
  /// 'num_receivers' is the number of receivers this batch will be sent to. Used for
  /// updating the stat counters.
  Status SerializeBatch(
      RowBatch* src, OutboundRowBatch* dest, bool compress, int num_receivers = 1);

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

  /// Adds the given row to 'channels_[channel_id]'.
  Status AddRowToChannel(const int channel_id, TupleRow* row);

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

  /// Pointer to OutboundRowBatch that will be used for serialization.
  /// Swapped with in_flight_batch_ (UNPARTITIONED case) or the channel's outbound_batch_
  /// (PARTITIONED case) after serialization.
  std::unique_ptr<OutboundRowBatch> serialization_batch_;

  /// Buffer used for compression after serialization. Swapped with the OutboundRowBatch's
  /// tuple_data_ if the compressed data is smaller.
  std::unique_ptr<TrackedString> compression_scratch_;

  /// Pointer to OutboundRowBatch referenced by the in-flight RPC(s). Used only
  /// when the partitioning strategy is UNPARTITIONED. In the broadcasting case
  /// multiple channels use it at the same time to hold the buffers backing the RPC
  /// sidecars.
  std::unique_ptr<OutboundRowBatch> in_flight_batch_;

  /// If true, this sender has called FlushFinal() successfully.
  /// Not valid to call Send() anymore.
  bool flushed_ = false;

  /// List of all channels. One for each destination.
  std::vector<std::unique_ptr<Channel>> channels_;

  /// Expressions of partition keys. It's used to compute the
  /// per-row partition values for shuffling exchange;
  const std::vector<ScalarExpr*>& partition_exprs_;
  std::vector<ScalarExprEvaluator*> partition_expr_evals_;

  /// Time for serializing row batches.
  RuntimeProfile::Counter* serialize_batch_timer_ = nullptr;

  /// Number of TransmitData() RPC retries due to remote service being busy.
  RuntimeProfile::Counter* rpc_retry_counter_ = nullptr;

  /// Total number of times RPC fails or the remote responds with a non-retryable error.
  RuntimeProfile::Counter* rpc_failure_counter_ = nullptr;

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

  /// Mapping to store which data file is read on which hosts.
  const std::unordered_map<std::string, std::vector<NetworkAddressPB>>&
      filepath_to_hosts_;

  /// A mapping between host addresses to channels. Used for DIRECTED distribution mode
  /// where only one channel is associated with each host address.
  std::unordered_map<NetworkAddressPB, Channel*> host_to_channel_;
};

} // namespace impala

#endif // IMPALA_RUNTIME_KRPC_DATA_STREAM_SENDER_H
