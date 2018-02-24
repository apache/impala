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

#ifndef IMPALA_RUNTIME_KRPC_DATA_STREAM_RECVR_H
#define IMPALA_RUNTIME_KRPC_DATA_STREAM_RECVR_H

#include "data-stream-recvr-base.h"

#include <boost/scoped_ptr.hpp>
#include <boost/thread/mutex.hpp>

#include "common/object-pool.h"
#include "common/status.h"
#include "gen-cpp/Types_types.h"   // for TUniqueId
#include "runtime/bufferpool/buffer-pool.h"
#include "runtime/descriptors.h"
#include "util/tuple-row-compare.h"

namespace kudu {
namespace rpc {
class RpcContext;
} // namespace rpc
} // namespace kudu

namespace impala {

class KrpcDataStreamMgr;
class MemTracker;
class RowBatch;
class RuntimeProfile;
class SortedRunMerger;
class TransmitDataCtx;
class TransmitDataRequestPB;
class TransmitDataResponsePB;

/// Single receiver of an m:n data stream.
///
/// KrpcDataStreamRecvr maintains one or more queues of row batches received by a
/// KrpcDataStreamMgr from one or more sender fragment instances. Receivers are created
/// via KrpcDataStreamMgr::CreateRecvr(). Ownership of a stream recvr is shared between
/// the KrpcDataStreamMgr that created it and the caller of
/// KrpcDataStreamMgr::CreateRecvr() (i.e. the exchange node).
///
/// The is_merging_ member determines if the recvr merges input streams from different
/// sender fragment instances according to a specified sort order.
/// If is_merging_ is false : Only one batch queue is maintained for row batches from all
/// sender fragment instances. These row batches are returned one at a time via GetBatch()
/// If is_merging_ is true : One queue is created for the batches from each distinct
/// sender. A SortedRunMerger instance must be created via CreateMerger() prior to
/// retrieving any rows from the receiver. Rows are retrieved from the receiver via
/// GetNext(RowBatch* output_batch, int limit, bool eos). After the final call to
/// GetNext(), TransferAllResources() must be called to transfer resources from the input
/// batches from each sender to the caller's output batch.
/// The receiver sets deep_copy to false on the merger - resources are transferred from
/// the input batches from each sender queue to the merger to the output batch by the
/// merger itself as it processes each run.
///
/// KrpcDataStreamRecvr::Close() must be called by the caller of CreateRecvr() to remove
/// the recvr instance from the tracking structure of its KrpcDataStreamMgr in all cases.
class KrpcDataStreamRecvr : public DataStreamRecvrBase {
 public:
  ~KrpcDataStreamRecvr();

  /// Returns next row batch in data stream; blocks if there aren't any.
  /// Retains ownership of the returned batch. The caller must call TransferAllResources()
  /// to acquire the resources from the returned batch before the next call to GetBatch().
  /// A NULL returned batch indicated eos. Must only be called if is_merging_ is false.
  /// TODO: This is currently only exposed to the non-merging version of the exchange.
  /// Refactor so both merging and non-merging exchange use GetNext(RowBatch*, bool* eos).
  Status GetBatch(RowBatch** next_batch);

  /// Deregister from KrpcDataStreamMgr instance, which shares ownership of this instance.
  void Close();

  /// Create a SortedRunMerger instance to merge rows from multiple sender according to
  /// the specified row comparator. Fetches the first batches from the individual sender
  /// queues. The exprs used in less_than must have already been prepared and opened.
  Status CreateMerger(const TupleRowComparator& less_than);

  /// Fill output_batch with the next batch of rows obtained by merging the per-sender
  /// input streams. Must only be called if is_merging_ is true.
  Status GetNext(RowBatch* output_batch, bool* eos);

  /// Transfer all resources from the current batches being processed from each sender
  /// queue to the specified batch.
  void TransferAllResources(RowBatch* transfer_batch);

  const TUniqueId& fragment_instance_id() const { return fragment_instance_id_; }
  PlanNodeId dest_node_id() const { return dest_node_id_; }
  const RowDescriptor* row_desc() const { return row_desc_; }
  MemTracker* deferred_rpc_tracker() const { return deferred_rpc_tracker_.get(); }
  MemTracker* parent_tracker() const { return parent_tracker_; }
  BufferPool::ClientHandle* client() const { return client_; }

 private:
  friend class KrpcDataStreamMgr;
  class SenderQueue;

  KrpcDataStreamRecvr(KrpcDataStreamMgr* stream_mgr, MemTracker* parent_tracker,
      const RowDescriptor* row_desc, const TUniqueId& fragment_instance_id,
      PlanNodeId dest_node_id, int num_senders, bool is_merging,
      int64_t total_buffer_limit, RuntimeProfile* profile,
      BufferPool::ClientHandle* client);

  /// Adds a new row batch to the appropriate sender queue. If the row batch can be
  /// inserted, the RPC will be responded to before this function returns. If the batch
  /// can't be added without exceeding the buffer limit, it is appended to a queue for
  /// deferred processing. The RPC will be responded to when the row batch is deserialized
  /// later.
  void AddBatch(const TransmitDataRequestPB* request, TransmitDataResponsePB* response,
      kudu::rpc::RpcContext* context);

  /// Tries adding the first entry of 'deferred_rpcs_' queue for the sender queue
  /// identified by 'sender_id'. If is_merging_ is false, it always defaults to
  /// queue 0; If is_merging_ is true, the sender queue is identified by 'sender_id_'.
  void DequeueDeferredRpc(int sender_id);

  /// Takes over the RPC state 'ctx' of an early sender for deferred processing and
  /// kicks off a deserialization task to process it asynchronously. This makes sure
  /// new incoming RPCs won't pass the early senders, leading to starvation.
  void TakeOverEarlySender(std::unique_ptr<TransmitDataCtx> ctx);

  /// Indicate that a particular sender is done. Delegated to the appropriate
  /// sender queue. Called from KrpcDataStreamMgr.
  void RemoveSender(int sender_id);

  /// Marks all sender queues as cancelled and notifies all waiting consumers of
  /// cancellation.
  void CancelStream();

  /// Return true if the addition of a new batch of size 'batch_size' would exceed the
  /// total buffer limit.
  bool ExceedsLimit(int64_t batch_size) {
    return num_buffered_bytes_.Load() + batch_size > total_buffer_limit_;
  }

  /// KrpcDataStreamMgr instance used to create this recvr. (Not owned)
  KrpcDataStreamMgr* mgr_;

  /// Fragment and node id of the destination exchange node this receiver is used by.
  TUniqueId fragment_instance_id_;
  PlanNodeId dest_node_id_;

  /// Soft upper limit on the total amount of buffering in bytes allowed for this stream
  /// across all sender queues. We defer processing of incoming RPCs once the amount of
  /// buffered data exceeds this value.
  const int64_t total_buffer_limit_;

  /// Row schema.
  const RowDescriptor* row_desc_;

  /// True if this reciver merges incoming rows from different senders. Per-sender
  /// row batch queues are maintained in this case.
  bool is_merging_;

  /// total number of bytes held across all sender queues.
  AtomicInt32 num_buffered_bytes_;

  /// Memtracker for payloads of deferred Rpcs in the sender queue(s).
  /// This must be accessed with 'lock_' held to avoid race with Close().
  boost::scoped_ptr<MemTracker> deferred_rpc_tracker_;

  /// The MemTracker of the exchange node which owns this receiver. Not owned.
  /// This is the MemTracker which 'client_' below internally references.
  MemTracker* parent_tracker_;

  /// The buffer pool client for allocating buffers of incoming row batches. Not owned.
  BufferPool::ClientHandle* client_;

  /// One or more queues of row batches received from senders. If is_merging_ is true,
  /// there is one SenderQueue for each sender. Otherwise, row batches from all senders
  /// are placed in the same SenderQueue. The SenderQueue instances are owned by the
  /// receiver and placed in sender_queue_pool_.
  std::vector<SenderQueue*> sender_queues_;

  /// SortedRunMerger used to merge rows from different senders.
  boost::scoped_ptr<SortedRunMerger> merger_;

  /// Pool of sender queues.
  ObjectPool sender_queue_pool_;

  /// Runtime profile storing the counters below.
  RuntimeProfile* profile_;

  /// Maintain two child profiles - receiver side measurements (from the GetBatch() path),
  /// and sender side measurements (from AddBatch()).
  RuntimeProfile* recvr_side_profile_;
  RuntimeProfile* sender_side_profile_;

  /// Number of bytes received but not necessarily enqueued.
  RuntimeProfile::Counter* bytes_received_counter_;

  /// Time series of number of bytes received, samples bytes_received_counter_
  RuntimeProfile::TimeSeriesCounter* bytes_received_time_series_counter_;

  /// Total wall-clock time spent deserializing row batches.
  RuntimeProfile::Counter* deserialize_row_batch_timer_;

  /// Number of senders which arrive before the receiver is ready.
  RuntimeProfile::Counter* num_early_senders_;

  /// Time spent waiting until the first batch arrives across all queues.
  /// TODO: Turn this into a wall-clock timer.
  RuntimeProfile::Counter* first_batch_wait_total_timer_;

  /// Total number of batches which arrived at this receiver but not necessarily received
  /// or enqueued. An arrived row batch will eventually be received if there is no error
  /// unpacking the RPC payload and the receiving stream is not cancelled.
  RuntimeProfile::Counter* num_arrived_batches_;

  /// Total number of batches received but not necessarily enqueued.
  RuntimeProfile::Counter* num_received_batches_;

  /// Total number of batches received and enqueued into the row batch queue.
  RuntimeProfile::Counter* num_enqueued_batches_;

  /// Total number of batches deferred because of early senders or full row batch queue.
  RuntimeProfile::Counter* num_deferred_batches_;

  /// Total number of EOS received.
  RuntimeProfile::Counter* num_eos_received_;

  /// Total wall-clock time spent waiting for data to arrive in the recv buffer.
  RuntimeProfile::Counter* data_arrival_timer_;

  /// Pointer to profile's inactive timer.
  RuntimeProfile::Counter* inactive_timer_;

  /// Total time spent in SenderQueue::GetBatch().
  RuntimeProfile::Counter* queue_get_batch_time_;
};

} // namespace impala

#endif // IMPALA_RUNTIME_KRPC_DATA_STREAM_RECVR_H
