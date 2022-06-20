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

#pragma once

#include <mutex>
#include <boost/scoped_ptr.hpp>

#include "common/object-pool.h"
#include "common/status.h"
#include "gen-cpp/Types_types.h"   // for TUniqueId
#include "runtime/bufferpool/buffer-pool.h"
#include "runtime/descriptors.h"
#include "runtime/runtime-state.h"
#include "util/tuple-row-compare.h"
#include "runtime/sorted-run-merger.h"

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
struct TransmitDataCtx;
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
///
/// Unless otherwise stated, class members belong to KrpcDataStreamRecvr. They are safe to
/// access from any threads as long as the caller obtained a shared_ptr to keep the
/// receiver alive. For class members not owned by the receiver, they must stay valid till
/// after Close() is called. Since a receiver is co-owned by an exchange node and the
/// singleton KrpcDataStreamMgr, it's possible that certain threads may race with Close()
/// called from the fragment execution thread. A receiver may also be cancelled at any
/// time due to query cancellation. To avoid resource leak, the following protocol is
/// followed:
/// - callers must obtain the target sender queue's lock and check if it's cancelled
/// - no new row batch or deferred RPCs should be added to a cancelled sender queue
/// - Cancel() will drain the deferred RPCs queue and the row batch queue
///
class KrpcDataStreamRecvr {
 public:
  ~KrpcDataStreamRecvr();

  /// Returns next row batch in data stream; blocks if there aren't any.
  /// Retains ownership of the returned batch. The caller must call TransferAllResources()
  /// to acquire the resources from the returned batch before the next call to GetBatch().
  /// A NULL returned batch indicated eos. Must only be called if is_merging_ is false.
  /// Called from fragment instance execution threads only.
  /// TODO: This is currently only exposed to the non-merging version of the exchange.
  /// Refactor so both merging and non-merging exchange use GetNext(RowBatch*, bool* eos).
  Status GetBatch(RowBatch** next_batch);

  /// Deregister from KrpcDataStreamMgr instance, which shares ownership of this instance.
  /// Called from fragment instance execution threads only.
  void Close();

  /// Create a SortedRunMerger instance to merge rows from multiple sender according to
  /// the specified row comparator. Fetches the first batches from the individual sender
  /// queues. The exprs used in less_than must have already been prepared and opened.
  /// Called from fragment instance execution threads only.
  Status CreateMerger(const TupleRowComparator& less_than,
      const CodegenFnPtr<SortedRunMerger::HeapifyHelperFn>& codegend_heapify_helper_fn);

  /// Fill output_batch with the next batch of rows obtained by merging the per-sender
  /// input streams. Must only be called if is_merging_ is true. Called from fragment
  /// instance execution threads only.
  Status GetNext(RowBatch* output_batch, bool* eos);

  /// Transfer all resources from the current batches being processed from each sender
  /// queue to the specified batch. Called from fragment instance execution threads only.
  void TransferAllResources(RowBatch* transfer_batch);

  /// Marks all sender queues as cancelled and notifies all waiting consumers of
  /// the cancellation.
  void CancelStream();

  const TUniqueId& fragment_instance_id() const { return fragment_instance_id_; }
  PlanNodeId dest_node_id() const { return dest_node_id_; }
  const RowDescriptor* row_desc() const { return row_desc_; }
  MemTracker* deferred_rpc_tracker() const { return deferred_rpc_tracker_.get(); }
  MemTracker* parent_tracker() const { return parent_tracker_; }
  BufferPool::ClientHandle* buffer_pool_client() const { return buffer_pool_client_; }

 private:
  friend class KrpcDataStreamMgr;
  class SenderQueue;

  KrpcDataStreamRecvr(KrpcDataStreamMgr* stream_mgr, MemTracker* parent_tracker,
      const RowDescriptor* row_desc, const RuntimeState& runtime_state,
      const TUniqueId& fragment_instance_id, PlanNodeId dest_node_id, int num_senders,
      bool is_merging, int64_t total_buffer_limit, RuntimeProfile* profile,
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
  /// Called from KrpcDataStreamMgr's deserialization threads only.
  void ProcessDeferredRpc(int sender_id);

  /// Takes over the RPC state 'ctx' of an early sender for deferred processing and
  /// kicks off a deserialization task to process it asynchronously. This makes sure
  /// new incoming RPCs won't pass the early senders, leading to starvation.
  /// Called from fragment instance execution threads only.
  void TakeOverEarlySender(std::unique_ptr<TransmitDataCtx> ctx);

  /// Indicate that a particular sender is done. Delegated to the appropriate
  /// sender queue. Called from KrpcDataStreamMgr.
  void RemoveSender(int sender_id);

  /// Return true if the addition of a new batch of size 'batch_size' would exceed the
  /// total buffer limit.
  bool ExceedsLimit(int64_t batch_size) {
    return num_buffered_bytes_.Load() + batch_size > total_buffer_limit_;
  }

  /// Return the current number of deferred RPCs.
  int64_t num_deferred_rpcs() const { return num_deferred_rpcs_.Load(); }

  /// KrpcDataStreamMgr instance used to create this recvr. Not owned.
  KrpcDataStreamMgr* mgr_;

  /// The runtime state of the fragment instance which owns this receiver. Not owned.
  const RuntimeState& runtime_state_;

  /// Fragment and node id of the destination exchange node this receiver is used by.
  TUniqueId fragment_instance_id_;
  PlanNodeId dest_node_id_;

  /// Soft upper limit on the total amount of buffering in bytes allowed for this stream
  /// across all sender queues. We defer processing of incoming RPCs once the amount of
  /// buffered data exceeds this value.
  const int64_t total_buffer_limit_;

  /// Row schema. Not owned.
  const RowDescriptor* row_desc_;

  /// True if this reciver merges incoming rows from different senders. Per-sender
  /// row batch queues are maintained in this case.
  const bool is_merging_;

  /// True if Close() has been called on this receiver already. Should only be accessed
  /// from the fragment execution thread.
  bool closed_;

  /// Current number of bytes held across all sender queues.
  AtomicInt32 num_buffered_bytes_;

  /// Current number of outstanding deferred RPCs across all sender queues.
  AtomicInt64 num_deferred_rpcs_;

  /// Memtracker for payloads of deferred Rpcs in the sender queue(s). This must be
  /// accessed with a sender queue's lock held to avoid race with Close() of the queue.
  boost::scoped_ptr<MemTracker> deferred_rpc_tracker_;

  /// The MemTracker of the exchange node which owns this receiver. Not owned.
  /// This is the MemTracker which 'client_' below internally references.
  MemTracker* parent_tracker_;

  /// The buffer pool client for allocating buffers of incoming row batches. Not owned.
  BufferPool::ClientHandle* buffer_pool_client_;

  /// One or more queues of row batches received from senders. If is_merging_ is true,
  /// there is one SenderQueue for each sender. Otherwise, row batches from all senders
  /// are placed in the same SenderQueue. The SenderQueue instances are owned by the
  /// receiver and placed in 'pool_'.
  std::vector<SenderQueue*> sender_queues_;

  /// SortedRunMerger used to merge rows from different senders.
  boost::scoped_ptr<SortedRunMerger> merger_;

  /// Pool which owns sender queues and the runtime profiles.
  ObjectPool pool_;

  /// Runtime profile of the owning exchange node. It's the parent of
  /// 'dequeue_profile_' and 'enqueue_profile_'. Not owned.
  RuntimeProfile* profile_;

  /// Maintain two child profiles - receiver side measurements (from the GetBatch() path),
  /// and sender side measurements (from AddBatch()). These two profiles own all counters
  /// below unless otherwise noted. These profiles are owned by the receiver and placed
  /// in 'pool_'. 'dequeue_profile_' and 'enqueue_profile_' must outlive 'profile_'
  /// to prevent accessing freed memory during top-down traversal of 'profile_'. The
  /// receiver is co-owned by the exchange node and the data stream manager so these two
  /// profiles should outlive the exchange node which owns 'profile_'.
  RuntimeProfile* dequeue_profile_;
  RuntimeProfile* enqueue_profile_;

  /// Pointer to profile's inactive timer. Not owned.
  /// Not directly shown in the profile and thus data_wait_time_ below. Used for
  /// subtracting the wait time from the total time spent in exchange node.
  RuntimeProfile::Counter* inactive_timer_;

  /// ------------------------------------------------------------------------------------
  /// Following counters belong to 'dequeue_profile_'.

  /// Number of bytes of deserialized row batches dequeued.
  RuntimeProfile::Counter* bytes_dequeued_counter_;

  /// Time series of bytes of deserialized row batches, samples 'bytes_dequeued_counter_'.
  RuntimeProfile::TimeSeriesCounter* bytes_dequeued_time_series_counter_;

  /// Total wall-clock time spent in SenderQueue::GetBatch().
  RuntimeProfile::Counter* queue_get_batch_timer_;

  /// Total wall-clock time spent waiting for data to be available in queues.
  RuntimeProfile::Counter* data_wait_timer_;

  /// Wall-clock time spent waiting for the first batch arrival across all queues.
  RuntimeProfile::Counter* first_batch_wait_total_timer_;

  /// ------------------------------------------------------------------------------------
  /// Following counters belong to 'enqueue_profile_'.

  /// Total number of bytes of serialized row batches received.
  RuntimeProfile::Counter* bytes_received_counter_;

  /// Time series of number of bytes received, samples 'bytes_received_counter_'.
  RuntimeProfile::TimeSeriesCounter* bytes_received_time_series_counter_;

  /// Total wall-clock time spent deserializing row batches.
  RuntimeProfile::Counter* deserialize_row_batch_timer_;

  /// Total number of EOS received.
  RuntimeProfile::Counter* total_eos_received_counter_;

  /// Total number of senders which arrive before the receiver is ready.
  RuntimeProfile::Counter* total_early_senders_counter_;

  /// Total number of serialized row batches received.
  RuntimeProfile::Counter* total_received_batches_counter_;

  /// Total number of deserialized row batches enqueued into the row batch queues.
  RuntimeProfile::Counter* total_enqueued_batches_counter_;

  /// Total number of RPCs whose responses are deferred because of early senders or
  /// full row batch queue.
  RuntimeProfile::Counter* total_deferred_rpcs_counter_;

  /// Time series of number of deferred row batches, samples 'num_deferred_rpcs_'.
  RuntimeProfile::TimeSeriesCounter* deferred_rpcs_time_series_counter_;

  /// Total wall-clock time in which the 'deferred_rpcs_' queues are not empty.
  RuntimeProfile::Counter* total_has_deferred_rpcs_timer_;

  /// Summary stats of time which RPCs spent in KRPC service queue before
  /// being dispatched to the RPC handlers.
  RuntimeProfile::SummaryStatsCounter* dispatch_timer_;
};

} // namespace impala
