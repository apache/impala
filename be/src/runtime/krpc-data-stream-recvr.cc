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

#include "runtime/krpc-data-stream-recvr.h"

#include <condition_variable>
#include <mutex>
#include <queue>

#include "exec/kudu/kudu-util.h"
#include "kudu/rpc/rpc_context.h"
#include "kudu/util/monotime.h"
#include "kudu/util/trace.h"
#include "runtime/fragment-instance-state.h"
#include "runtime/krpc-data-stream-recvr.h"
#include "runtime/krpc-data-stream-mgr.h"
#include "runtime/mem-tracker.h"
#include "runtime/row-batch.h"
#include "runtime/sorted-run-merger.h"
#include "service/data-stream-service.h"
#include "util/debug-util.h"
#include "util/runtime-profile-counters.h"
#include "util/periodic-counter-updater.h"
#include "util/test-info.h"
#include "util/time.h"

#include "gen-cpp/data_stream_service.pb.h"

#include "common/names.h"

DECLARE_int32(datastream_service_num_deserialization_threads);

using kudu::MonoDelta;
using kudu::MonoTime;
using kudu::rpc::RpcContext;
using std::condition_variable_any;

namespace impala {

// Implements a FIFO queue of row batches from one or more senders. One queue is
// maintained per sender if is_merging_ is true for the enclosing receiver, otherwise rows
// from all senders are placed in the same queue.
//
// Batches are added by senders via AddBatch(), and removed by an enclosing
// KrpcDataStreamRecvr via GetBatch(). There is a soft limit for the total amount of
// memory consumed by buffered row batches in all sender queues of a receiver. If adding
// a batch will push the memory consumption beyond the limit, that RPC is added to the
// 'deferred batches' queue, which will be drained in FIFO order when space opens up.
// Senders in that state will not be replied to until their row batches are deserialized
// or the receiver is cancelled. This ensures that only one batch per sender is buffered
// in the deferred batches queue.
class KrpcDataStreamRecvr::SenderQueue {
 public:
  SenderQueue(KrpcDataStreamRecvr* parent_recvr, int num_senders);

  // Returns the next batch from this sender queue. Sets the returned batch in cur_batch_.
  // A returned batch that is not filled to capacity does *not* indicate end-of-stream.
  // The call blocks until another batch arrives or all senders close their channels.
  // The returned batch is owned by the sender queue. The caller must acquire the
  // resources from the returned batch before the next call to GetBatch().
  Status GetBatch(RowBatch** next_batch);

  // Adds a new row batch to this sender queue if this stream has not been cancelled.
  // If adding this batch causes us to exceed the receiver's buffer limit, the RPC state
  // is copied into 'deferred_rpcs_' for deferred processing and this function returns
  // immediately. The deferred RPCs are replied to later when space becomes available.
  void AddBatch(const TransmitDataRequestPB* request, TransmitDataResponsePB* response,
      RpcContext* context);

  // Tries inserting the front of 'deferred_rpcs_' queue into 'batch_queue_' if possible.
  // On success, the first entry of 'deferred_rpcs_' is removed and the sender of the RPC
  // will be responded to. If the serialized row batch fails to be extracted from the
  // entry, the error status will be sent as reply.
  void ProcessDeferredRpc();

  // Takes over the RPC state 'ctx' of an early sender for deferred processing and
  // kicks off a deserialization task to process it asynchronously. The ownership of
  // 'ctx' is transferred to this sender queue.
  void TakeOverEarlySender(std::unique_ptr<TransmitDataCtx> ctx);

  // Decrements the number of remaining senders for this queue and signal any threads
  // waiting on the arrival of new batch if the count drops to 0. The number of senders
  // will be 1 for a merging KrpcDataStreamRecvr.
  void DecrementSenders();

  // Sets cancellation flag and signals cancellation to receiver and sender. Subsequent
  // incoming batches will be dropped and senders in 'deferred_rpcs_' are replied to.
  void Cancel();

  // Must be called once to cleanup any queued resources.
  void Close();

  // Returns the current batch from this queue being processed by a consumer.
  RowBatch* current_batch() const { return current_batch_.get(); }

 private:
  // Returns true if either (1) 'batch_queue' is empty and there is no pending insertion
  // or (2) inserting a row batch of 'batch_size' into 'batch_queue' will not cause the
  // soft limit of the receiver to be exceeded. Expected to be called with 'lock_' held.
  bool CanEnqueue(int64_t batch_size, const unique_lock<SpinLock>& lock) const;

  // Helper function for inserting 'payload' into 'deferred_rpcs_'. Also does some
  // accounting for various counters. 'lock_' must be held when calling this function.
  void EnqueueDeferredRpc(unique_ptr<TransmitDataCtx> payload,
      const unique_lock<SpinLock>& lock);

  // Helper function for removing the first item from 'deferred_rpcs_'. Also does some
  // accounting for various counters. 'lock_' must be held when calling this function.
  void DequeueDeferredRpc(const unique_lock<SpinLock>& lock);

  // Mark an error 'status' into the overall status. 'lock_' must be held when calling
  // this function. Will notify all threads waiting on 'data_arrival_cv_'.
  void MarkErrorStatus(const Status& status, const unique_lock<SpinLock>& lock);

  // Unpacks a serialized row batch from 'request' and 'rpc_context' and populates
  // 'tuple_offsets' and 'tuple_data'. On success, the deserialized row batch sizes is
  // stored in 'deserialized_size'. If 'serialized_size' is not NULL, also stores the
  // serialized row batch size in it. On failure, the error status is returned.
  Status UnpackRequest(const TransmitDataRequestPB* request,
      RpcContext* rpc_context, kudu::Slice* tuple_offsets, kudu::Slice* tuple_data,
      int64_t* deserialized_size, int64_t* serialized_size = nullptr);

  // Helper function to compute the serialized row batch size from 'request'
  // and 'rpc_context'. Returns 0 on failure to unpack the serialized row batch.
  int64_t GetSerializedBatchSize(const TransmitDataRequestPB* request,
      RpcContext* rpc_context);

  // The workhorse function for deserializing a row batch represented by ('header',
  // 'tuple_offsets' and 'tuple_data') and inserting it into 'batch_queue'. Expects to be
  // called with 'lock_' held and passed into this function via the argument 'lock'. This
  // function may drop lock when deserializing the row batch and re-acquire it after
  // the row batch is deserialized. 'batch_size' is the size in bytes of the deserialized
  // row batch. The caller is expected to have called CanEnqueue() to make sure the row
  // batch can be inserted without exceeding the soft limit of the receiver. Also notify
  // a thread waiting on 'data_arrival_cv_'. Return error status if the row batch creation
  // failed. Returns OK otherwise.
  Status AddBatchWork(int64_t batch_size, const RowBatchHeaderPB& header,
      const kudu::Slice& tuple_offsets, const kudu::Slice& tuple_data,
      unique_lock<SpinLock>* lock, RpcContext* rpc_context) WARN_UNUSED_RESULT;

  // Receiver of which this queue is a member.
  KrpcDataStreamRecvr* recvr_;

  // Protects all subsequent fields.
  SpinLock lock_;

  // Record any error status within KrpcDataStreamRecvr when inserting row batch.
  Status status_;

  // If true, the receiver fragment for this stream got cancelled. This is usually
  // triggered by closing the owning exchange node or cancelling the query.
  bool is_cancelled_ = false;

  // Number of deserialization requests sent to deserialization threads to drain
  // 'deferred_rpcs_' which are yet to be processed. Used to limit the number of
  // requests queued.
  int num_deserialize_tasks_pending_ = 0;

  // Number of senders which haven't closed the channel yet
  // (if it drops to 0, end-of-stream is true)
  int num_remaining_senders_;

  // Number of pending row batch insertion. AddBatchWork() may drop and reacquire 'lock_',
  // causing race between multiple threads calling AddBatch() at the same time or race
  // between threads calling AddBatch() and threads calling Close() concurrently.
  // AddBatchWork() increments this counter before dropping 'lock_' for deserializing
  // the row batch. The counter is decremented after 'lock_' is re-acquired and the row
  // batch is inserted into 'batch_queue'. The races are as follows:
  //
  // 1. Multiple threads inserting into an empty 'batch_queue' concurrently may all see
  // it as empty before the first thread manages to insert into batch_queue. This may
  // cause the soft limit to be exceeded. A queue is truly empty iff this counter is 0.
  //
  // 2. Close() cannot proceed until this counter is 0 to make sure all pending inserts
  // complete before the 'batch_queue' is cleared.
  int num_pending_enqueue_ = 0;

  // Signal the arrival of new batch or the eos/cancelled condition.
  condition_variable_any data_arrival_cv_;

  // Queue of (batch length, batch) pairs. The SenderQueue owns the memory to these
  // batches until they are handed off to the callers of GetBatch().
  typedef list<pair<int, std::unique_ptr<RowBatch>>> RowBatchQueue;
  RowBatchQueue batch_queue_;

  // The batch that was most recently returned via GetBatch(), i.e. the current batch
  // from this queue being processed by a consumer. It's destroyed when the next batch
  // is retrieved.
  scoped_ptr<RowBatch> current_batch_;

  // Set to true when the first batch has been received
  bool received_first_batch_ = false;

  // Queue of deferred RPCs - those that have a batch to deliver, but the queue was
  // full when they last tried to do so. The senders wait here until there is a space for
  // their batches, allowing the receiver-side to implement basic flow-control.
  std::queue<std::unique_ptr<TransmitDataCtx>> deferred_rpcs_;

  // Monotonic time in nanoseconds of when 'deferred_rpcs_' goes from being empty to
  // non-empty. Set to 0 when 'deferred_rpcs_' becomes empty again. Used for computing
  // 'total_has_deferred_rpcs_timer_'.
  int64_t has_deferred_rpcs_start_time_ns_ = 0;
};

KrpcDataStreamRecvr::SenderQueue::SenderQueue(
    KrpcDataStreamRecvr* parent_recvr, int num_senders)
  : recvr_(parent_recvr), num_remaining_senders_(num_senders) { }

Status KrpcDataStreamRecvr::SenderQueue::GetBatch(RowBatch** next_batch) {
  SCOPED_TIMER(recvr_->queue_get_batch_timer_);
  DCHECK(TestInfo::is_test() || FragmentInstanceState::IsFragmentExecThread());
  DCHECK(!recvr_->closed_);
  int num_to_dequeue = 0;
  // The sender id is set below when we decide to dequeue entries from 'deferred_rpcs_'.
  int sender_id = -1;
  {
    unique_lock<SpinLock> l(lock_);
    // current_batch_ must be replaced with the returned batch.
    current_batch_.reset();
    *next_batch = nullptr;

    // Wait until something shows up or we know we're done
    while (batch_queue_.empty() && status_.ok() && !is_cancelled_ &&
        num_remaining_senders_ > 0) {
      // Verify before waiting on 'data_arrival_cv_' that if there are any deferred
      // batches, either there is outstanding deserialization request queued or there
      // is pending insertion so this thread is guaranteed to wake up at some point.
      DCHECK(deferred_rpcs_.empty() ||
          (num_deserialize_tasks_pending_ + num_pending_enqueue_) > 0);
      VLOG_ROW << "wait arrival fragment_instance_id="
               << PrintId(recvr_->fragment_instance_id())
               << " node=" << recvr_->dest_node_id();
      // Don't count time spent waiting on the sender as active time.
      CANCEL_SAFE_SCOPED_TIMER3(recvr_->data_wait_timer_, recvr_->inactive_timer_,
          received_first_batch_ ? nullptr : recvr_->first_batch_wait_total_timer_,
          &is_cancelled_);
      data_arrival_cv_.wait(l);
    }

    // Return early if there is any error when inserting row batches.
    RETURN_IF_ERROR(status_);

    if (UNLIKELY(is_cancelled_)) {
      // Cancellation should have drained the entire 'deferred_rpcs_' queue.
      // Make sure the senders were replied to or they may be stuck waiting for a reply.
      DCHECK(deferred_rpcs_.empty());
      return Status::CANCELLED;
    }

    // All senders have sent their row batches. Nothing to do.
    if (num_remaining_senders_ == 0 && batch_queue_.empty()) {
      // Note that it's an invariant that a sender cannot send the EOS RPC until all
      // outstanding TransmitData() RPCs have been replied to. Therefore, it should be
      // impossible for num_remaining_senders_ to reach 0 before all RPCs in
      // 'deferred_rpcs_' have been replied to.
      DCHECK(deferred_rpcs_.empty());
      DCHECK_EQ(num_pending_enqueue_, 0);
      return Status::OK();
    }

    // Notify the deserialization threads to retry delivering the deferred RPCs.
    if (!deferred_rpcs_.empty()) {
      // Try dequeuing multiple entries from 'deferred_rpcs_' to parallelize the CPU
      // bound deserialization work. No point in dequeuing more than number of
      // deserialization threads available.
      DCHECK_GE(deferred_rpcs_.size(), num_deserialize_tasks_pending_);
      num_to_dequeue = min(FLAGS_datastream_service_num_deserialization_threads,
          (int)deferred_rpcs_.size() - num_deserialize_tasks_pending_);
      num_deserialize_tasks_pending_ += num_to_dequeue;
      sender_id = deferred_rpcs_.front()->request->sender_id();
    }

    DCHECK(!batch_queue_.empty());
    received_first_batch_ = true;
    RowBatch* result = batch_queue_.front().second.release();
    int64_t batch_size = batch_queue_.front().first;
    COUNTER_ADD(recvr_->bytes_dequeued_counter_, batch_size);
    recvr_->num_buffered_bytes_.Add(-batch_size);
    batch_queue_.pop_front();
    VLOG_ROW << "fetched #rows=" << result->num_rows();
    current_batch_.reset(result);
    *next_batch = current_batch_.get();
  }
  // Don't hold lock when calling EnqueueDeserializeTask() as it may block.
  // It's important that the dequeuing of 'deferred_rpcs_' is done after the entry
  // has been removed from 'batch_queue_' or the deserialization threads may fail to
  // insert into a non-empty 'batch_queue_' and the receiver will be waiting forever.
  if (num_to_dequeue > 0) {
    DCHECK_GE(sender_id, 0);
    recvr_->mgr_->EnqueueDeserializeTask(recvr_->fragment_instance_id(),
        recvr_->dest_node_id(), sender_id, num_to_dequeue);
  }
  return Status::OK();
}

inline bool KrpcDataStreamRecvr::SenderQueue::CanEnqueue(int64_t batch_size,
    const unique_lock<SpinLock>& lock) const {
  DCHECK(lock.owns_lock());
  // The queue is truly empty iff there is no pending insert. It's important that we
  // enqueue the new batch regardless of buffer limit if the queue is currently empty.
  // In the case of a merging receiver, batches are received from a specific queue
  // based on data order, and the pipeline will stall if the merger is waiting for data
  // from an empty queue that cannot be filled because the limit has been reached.
  bool queue_empty = batch_queue_.empty() && num_pending_enqueue_ == 0;
  return queue_empty || !recvr_->ExceedsLimit(batch_size);
}

void KrpcDataStreamRecvr::SenderQueue::EnqueueDeferredRpc(
    unique_ptr<TransmitDataCtx> payload, const unique_lock<SpinLock>& lock) {
  DCHECK(lock.owns_lock());
  TRACE_TO(payload->rpc_context->trace(), "Enqueuing deferred RPC");
  if (deferred_rpcs_.empty()) has_deferred_rpcs_start_time_ns_ = MonotonicNanos();
  deferred_rpcs_.push(move(payload));
  recvr_->num_deferred_rpcs_.Add(1);
  COUNTER_ADD(recvr_->total_deferred_rpcs_counter_, 1);
}

void KrpcDataStreamRecvr::SenderQueue::DequeueDeferredRpc(
    const unique_lock<SpinLock>& lock) {
  DCHECK(lock.owns_lock());
  deferred_rpcs_.pop();
  if (deferred_rpcs_.empty()) {
    DCHECK_NE(has_deferred_rpcs_start_time_ns_, 0);
    int64_t duration = MonotonicNanos() - has_deferred_rpcs_start_time_ns_;
    COUNTER_ADD(recvr_->total_has_deferred_rpcs_timer_, duration);
    has_deferred_rpcs_start_time_ns_ = 0;
  }
  recvr_->num_deferred_rpcs_.Add(-1);
}

inline void KrpcDataStreamRecvr::SenderQueue::MarkErrorStatus(const Status& status,
    const unique_lock<SpinLock>& lock) {
  DCHECK(lock.owns_lock());
  DCHECK(!status.ok());
  status_.MergeStatus(status);
  // Notify all threads which are waiting for row batches that an error has occurred.
  data_arrival_cv_.notify_all();
}

Status KrpcDataStreamRecvr::SenderQueue::UnpackRequest(
    const TransmitDataRequestPB* request, RpcContext* rpc_context,
    kudu::Slice* tuple_offsets, kudu::Slice* tuple_data, int64_t* deserialized_size,
    int64_t* serialized_size) {

  RETURN_IF_ERROR(DebugAction(recvr_->runtime_state_.query_options(),
      "RECVR_UNPACK_PAYLOAD"));

  // Unpack the tuple offsets.
  KUDU_RETURN_IF_ERROR(rpc_context->GetInboundSidecar(
      request->tuple_offsets_sidecar_idx(), tuple_offsets),
      "Failed to get the tuple offsets sidecar");
  // Unpack the tuple data.
  KUDU_RETURN_IF_ERROR(rpc_context->GetInboundSidecar(
      request->tuple_data_sidecar_idx(), tuple_data),
      "Failed to get the tuple data sidecar");
  // Compute the size of the deserialized row batch.
  *deserialized_size =
      RowBatch::GetDeserializedSize(request->row_batch_header(), *tuple_offsets);
  // Compute the size of the serialized row batch.
  if (serialized_size != nullptr) {
    *serialized_size = tuple_offsets->size() + tuple_data->size();
  }
  return Status::OK();
}

Status KrpcDataStreamRecvr::SenderQueue::AddBatchWork(int64_t batch_size,
    const RowBatchHeaderPB& header, const kudu::Slice& tuple_offsets,
    const kudu::Slice& tuple_data, unique_lock<SpinLock>* lock,
    RpcContext* rpc_context) {
  DCHECK(lock != nullptr);
  DCHECK(lock->owns_lock());
  DCHECK(!is_cancelled_);

  // Reserve queue space before dropping the lock below.
  recvr_->num_buffered_bytes_.Add(batch_size);
  // Bump 'num_pending_enqueue_' to avoid race with Close() when lock is dropped below.
  DCHECK_GE(num_pending_enqueue_, 0);
  ++num_pending_enqueue_;

  // Deserialization may take some time due to compression and memory allocation.
  // Drop the lock so we can deserialize multiple batches in parallel.
  lock->unlock();
  TRACE_TO(rpc_context->trace(), "Deserializing batch");
  unique_ptr<RowBatch> batch;
  Status status;
  {
    SCOPED_TIMER(recvr_->deserialize_row_batch_timer_);
    status = DebugAction(recvr_->runtime_state_.query_options(), "RECVR_ADD_BATCH");
    if (LIKELY(status.ok())) {
      // At this point, a row batch will be inserted into batch_queue_.
      // Close() will handle deleting any unconsumed batches from batch_queue_.
      // Close() cannot proceed until there are no pending insertion to batch_queue_.
      status = RowBatch::FromProtobuf(recvr_->row_desc(), header, tuple_offsets,
          tuple_data, recvr_->parent_tracker(), recvr_->buffer_pool_client(), &batch);
    }
  }
  lock->lock();

  DCHECK_GT(num_pending_enqueue_, 0);
  --num_pending_enqueue_;
  if (UNLIKELY(!status.ok())) {
    recvr_->num_buffered_bytes_.Add(-batch_size);
    VLOG_QUERY << "Failed to deserialize batch for "
               << PrintId(recvr_->fragment_instance_id());
    TRACE_TO(rpc_context->trace(), "Failed to deserialize batch: $0", status.GetDetail());
    MarkErrorStatus(status, *lock);
    return status;
  }
  VLOG_ROW << "added #rows=" << batch->num_rows() << " batch_size=" << batch_size;
  TRACE_TO(rpc_context->trace(), "Enqueuing deserialized batch");
  COUNTER_ADD(recvr_->total_enqueued_batches_counter_, 1);
  batch_queue_.emplace_back(batch_size, move(batch));
  data_arrival_cv_.notify_one();
  return Status::OK();
}

void KrpcDataStreamRecvr::SenderQueue::AddBatch(const TransmitDataRequestPB* request,
    TransmitDataResponsePB* response, RpcContext* rpc_context) {
  // TODO: Add timers for time spent in this function and queue time in 'batch_queue_'.
  const RowBatchHeaderPB& header = request->row_batch_header();
  kudu::Slice tuple_offsets;
  kudu::Slice tuple_data;
  int64_t batch_size;
  Status status = UnpackRequest(request, rpc_context, &tuple_offsets, &tuple_data,
      &batch_size);
  if (UNLIKELY(!status.ok())) {
    {
      unique_lock<SpinLock> l(lock_);
      MarkErrorStatus(status, l);
    }
    TRACE_TO(rpc_context->trace(), "Error unpacking request: $0", status.GetDetail());
    DataStreamService::RespondRpc(status, response, rpc_context);
    return;
  }
  COUNTER_ADD(recvr_->total_received_batches_counter_, 1);
  // To be consistent with the senders, only count the sidecars size.
  COUNTER_ADD(recvr_->bytes_received_counter_, tuple_data.size() + tuple_offsets.size());

  {
    unique_lock<SpinLock> l(lock_);
    // There should be one or more senders left when this function is called. The reason
    // is that EndDataStream RPC is not sent until all outstanding TransmitData() RPC has
    // been replied to. There is at least one TransmitData() RPC which hasn't yet been
    // responded to if we reach here.
    DCHECK_GT(num_remaining_senders_, 0);
    if (UNLIKELY(is_cancelled_)) {
      TRACE_TO(rpc_context->trace(), "Receiver was cancelled");
      Status cancel_status = Status::Expected(TErrorCode::DATASTREAM_RECVR_CLOSED,
          PrintId(recvr_->fragment_instance_id()), recvr_->dest_node_id());
      DataStreamService::RespondRpc(cancel_status, response, rpc_context);
      return;
    }

    // If there's something in the queue or this batch will push us over the buffer
    // limit we need to wait until the queue gets drained. We store the rpc context
    // so that we can signal it at a later time to resend the batch that we couldn't
    // process here. If there are already deferred RPCs waiting in queue, the new
    // batch needs to line up after the deferred RPCs to avoid starvation of senders
    // in the non-merging case.
    if (UNLIKELY(!deferred_rpcs_.empty() || !CanEnqueue(batch_size, l))) {
      recvr_->deferred_rpc_tracker()->Consume(rpc_context->GetTransferSize());
      auto payload = make_unique<TransmitDataCtx>(request, response, rpc_context);
      EnqueueDeferredRpc(move(payload), l);
      return;
    }

    // At this point, we are committed to inserting the row batch into 'batch_queue_'.
    status = AddBatchWork(batch_size, header, tuple_offsets, tuple_data, &l, rpc_context);
  }

  // Respond to the sender to ack the insertion of the row batches.
  DataStreamService::RespondRpc(status, response, rpc_context);
}

void KrpcDataStreamRecvr::SenderQueue::ProcessDeferredRpc() {
  // Owns the first entry of 'deferred_rpcs_' if it ends up being popped.
  std::unique_ptr<TransmitDataCtx> ctx;
  Status status;
  {
    unique_lock<SpinLock> l(lock_);
    DCHECK_GT(num_deserialize_tasks_pending_, 0);
    --num_deserialize_tasks_pending_;

    if (deferred_rpcs_.empty()) return;
    // A sender queue cannot be cancelled if there is any deferred RPC.
    DCHECK(!is_cancelled_);

    // Try enqueuing the first entry into 'batch_queue_'.
    ctx.swap(deferred_rpcs_.front());
    TRACE_TO(ctx->rpc_context->trace(), "Processing deferred RPC");
    kudu::Slice tuple_offsets;
    kudu::Slice tuple_data;
    int64_t batch_size;
    status = UnpackRequest(ctx->request, ctx->rpc_context, &tuple_offsets,
        &tuple_data, &batch_size);
    // Reply with error status if the entry cannot be unpacked.
    if (UNLIKELY(!status.ok())) {
      TRACE_TO(ctx->rpc_context->trace(),
          "Error unpacking deferred RPC: $0", status.GetDetail());
      MarkErrorStatus(status, l);
      DataStreamService::RespondAndReleaseRpc(status, ctx->response, ctx->rpc_context,
          recvr_->deferred_rpc_tracker());
      DequeueDeferredRpc(l);
      return;
    }

    // Stops if inserting the batch causes us to go over the limit.
    // Put 'ctx' back on the queue.
    if (!CanEnqueue(batch_size, l)) {
      TRACE_TO(ctx->rpc_context->trace(), "Batch queue is full");
      ctx.swap(deferred_rpcs_.front());
      DCHECK(deferred_rpcs_.front().get() != nullptr);
      return;
    }

    // Dequeues the deferred batch and adds it to 'batch_queue_'.
    DequeueDeferredRpc(l);
    const RowBatchHeaderPB& header = ctx->request->row_batch_header();
    status = AddBatchWork(
        batch_size, header, tuple_offsets, tuple_data, &l, ctx->rpc_context);
    DCHECK(!status.ok() || !batch_queue_.empty());

    // Release to MemTracker while still holding the lock to prevent race with Close().
    recvr_->deferred_rpc_tracker()->Release(ctx->rpc_context->GetTransferSize());
  }

  // Responds to the sender to ack the insertion of the row batches.
  // No need to hold lock when enqueuing the response.
  DataStreamService::RespondRpc(status, ctx->response, ctx->rpc_context);
}

int64_t KrpcDataStreamRecvr::SenderQueue::GetSerializedBatchSize(
    const TransmitDataRequestPB* request, RpcContext* rpc_context) {
  kudu::Slice tuple_offsets;
  kudu::Slice tuple_data;
  int64_t unused;
  int64_t serialized_size = 0;
  if (UnpackRequest(request, rpc_context, &tuple_offsets, &tuple_data, &unused,
          &serialized_size).ok()) {
    return serialized_size;
  }
  return 0;
}

void KrpcDataStreamRecvr::SenderQueue::TakeOverEarlySender(
    unique_ptr<TransmitDataCtx> ctx) {
  // TakeOverEarlySender() is called by the same thread which calls Close().
  // The receiver cannot be closed while this function is in progress so
  // 'recvr_->mgr_' shouldn't be NULL.
  DCHECK(TestInfo::is_test() || FragmentInstanceState::IsFragmentExecThread());
  DCHECK(!recvr_->closed_ && recvr_->mgr_ != nullptr);
  COUNTER_ADD(recvr_->total_received_batches_counter_, 1);
  COUNTER_ADD(recvr_->bytes_received_counter_,
      GetSerializedBatchSize(ctx->request, ctx->rpc_context));
  int sender_id = ctx->request->sender_id();
  {
    unique_lock<SpinLock> l(lock_);
    if (UNLIKELY(is_cancelled_)) {
      TRACE_TO(ctx->rpc_context->trace(), "Recvr closed");
      Status cancel_status = Status::Expected(TErrorCode::DATASTREAM_RECVR_CLOSED,
          PrintId(recvr_->fragment_instance_id()), recvr_->dest_node_id());
      DataStreamService::RespondRpc(cancel_status, ctx->response, ctx->rpc_context);
      return;
    }
    // Only enqueue a deferred RPC if the sender queue is not yet cancelled.
    recvr_->deferred_rpc_tracker()->Consume(ctx->rpc_context->GetTransferSize());
    EnqueueDeferredRpc(move(ctx), l);
    ++num_deserialize_tasks_pending_;
  }
  recvr_->mgr_->EnqueueDeserializeTask(recvr_->fragment_instance_id(),
      recvr_->dest_node_id(), sender_id, 1);
}

void KrpcDataStreamRecvr::SenderQueue::DecrementSenders() {
  lock_guard<SpinLock> l(lock_);
  DCHECK_GT(num_remaining_senders_, 0);
  num_remaining_senders_ = max(0, num_remaining_senders_ - 1);
  VLOG_FILE << "decremented senders: fragment_instance_id="
            << PrintId(recvr_->fragment_instance_id())
            << " node_id=" << recvr_->dest_node_id()
            << " #senders=" << num_remaining_senders_;
  if (num_remaining_senders_ == 0) data_arrival_cv_.notify_one();
}

void KrpcDataStreamRecvr::SenderQueue::Cancel() {
  {
    unique_lock<SpinLock> l(lock_);
    if (is_cancelled_) return;
    is_cancelled_ = true;

    // Respond to deferred RPCs.
    while (!deferred_rpcs_.empty()) {
      const unique_ptr<TransmitDataCtx>& ctx = deferred_rpcs_.front();
      Status cancel_status = Status::Expected(TErrorCode::DATASTREAM_RECVR_CLOSED,
          PrintId(recvr_->fragment_instance_id()), recvr_->dest_node_id());
      DataStreamService::RespondAndReleaseRpc(cancel_status, ctx->response,
          ctx->rpc_context, recvr_->deferred_rpc_tracker());
      DequeueDeferredRpc(l);
    }
  }
  VLOG(2) << "cancelled stream: fragment_instance_id="
          << PrintId(recvr_->fragment_instance_id())
          << " node_id=" << recvr_->dest_node_id();
  // Wake up all threads waiting to produce/consume batches. They will all
  // notice that the stream is cancelled and handle it.
  data_arrival_cv_.notify_all();
  PeriodicCounterUpdater::StopTimeSeriesCounter(
      recvr_->bytes_received_time_series_counter_);
}

void KrpcDataStreamRecvr::SenderQueue::Close() {
  unique_lock<SpinLock> l(lock_);
  // Note that the queue must be cancelled first before it can be closed or we may
  // risk running into a race which can leak row batches. Please see IMPALA-3034.
  DCHECK(is_cancelled_);

  // The deferred RPCs should all have been responded to in Cancel().
  DCHECK(deferred_rpcs_.empty());

  // Wait for any pending insertion to complete first.
  while (num_pending_enqueue_ > 0) data_arrival_cv_.wait(l);

  // Delete any batches queued in batch_queue_
  batch_queue_.clear();
  current_batch_.reset();
}

Status KrpcDataStreamRecvr::CreateMerger(const TupleRowComparator& less_than,
    const CodegenFnPtr<SortedRunMerger::HeapifyHelperFn>& codegend_heapify_helper_fn) {
  DCHECK(is_merging_);
  DCHECK(TestInfo::is_test() || FragmentInstanceState::IsFragmentExecThread());
  vector<SortedRunMerger::RunBatchSupplierFn> input_batch_suppliers;
  input_batch_suppliers.reserve(sender_queues_.size());

  // Create the merger that will a single stream of sorted rows.
  merger_.reset(new SortedRunMerger(less_than, row_desc_, profile_, false,
      codegend_heapify_helper_fn));

  for (SenderQueue* queue: sender_queues_) {
    input_batch_suppliers.push_back(
        [queue](RowBatch** next_batch) -> Status {
          return queue->GetBatch(next_batch);
        });
  }

  RETURN_IF_ERROR(merger_->Prepare(input_batch_suppliers));
  return Status::OK();
}

void KrpcDataStreamRecvr::TransferAllResources(RowBatch* transfer_batch) {
  DCHECK(TestInfo::is_test() || FragmentInstanceState::IsFragmentExecThread());
  for (SenderQueue* sender_queue: sender_queues_) {
    if (sender_queue->current_batch() != nullptr) {
      sender_queue->current_batch()->TransferResourceOwnership(transfer_batch);
    }
  }
}

KrpcDataStreamRecvr::KrpcDataStreamRecvr(KrpcDataStreamMgr* stream_mgr,
    MemTracker* parent_tracker, const RowDescriptor* row_desc,
    const RuntimeState& runtime_state, const TUniqueId& fragment_instance_id,
    PlanNodeId dest_node_id, int num_senders, bool is_merging, int64_t total_buffer_limit,
    RuntimeProfile* profile, BufferPool::ClientHandle* client)
  : mgr_(stream_mgr),
    runtime_state_(runtime_state),
    fragment_instance_id_(fragment_instance_id),
    dest_node_id_(dest_node_id),
    total_buffer_limit_(total_buffer_limit),
    row_desc_(row_desc),
    is_merging_(is_merging),
    closed_(false),
    num_buffered_bytes_(0),
    deferred_rpc_tracker_(new MemTracker(-1, "KrpcDeferredRpcs", parent_tracker)),
    parent_tracker_(parent_tracker),
    buffer_pool_client_(client),
    profile_(profile),
    dequeue_profile_(RuntimeProfile::Create(&pool_, RuntimeProfile::DEQUEUE, false)),
    enqueue_profile_(RuntimeProfile::Create(&pool_, RuntimeProfile::ENQUEUE, false)) {
  // Create one queue per sender if is_merging is true.
  int num_queues = is_merging ? num_senders : 1;
  sender_queues_.reserve(num_queues);
  int num_sender_per_queue = is_merging ? 1 : num_senders;
  for (int i = 0; i < num_queues; ++i) {
    SenderQueue* queue = pool_.Add(new SenderQueue(this, num_sender_per_queue));
    sender_queues_.push_back(queue);
  }

  // Add the profiles of the dequeuing side (i.e. GetBatch()) and the enqueuing side
  // (i.e. AddBatchWork()) as children of the owning exchange node's profile.
  profile_->AddChild(dequeue_profile_);
  profile_->AddChild(enqueue_profile_);

  // Initialize various counters for measuring dequeuing from queues.
  bytes_dequeued_counter_ =
      ADD_COUNTER(dequeue_profile_, "TotalBytesDequeued", TUnit::BYTES);
  bytes_dequeued_time_series_counter_ = ADD_SYSTEM_TIME_SERIES_COUNTER(
      dequeue_profile_, "BytesDequeued", bytes_dequeued_counter_);
  queue_get_batch_timer_ = ADD_TIMER(dequeue_profile_, "TotalGetBatchTime");
  data_wait_timer_ =
      ADD_CHILD_TIMER(dequeue_profile_, "DataWaitTime", "TotalGetBatchTime");
  inactive_timer_ = profile_->inactive_timer();
  first_batch_wait_total_timer_ =
      ADD_TIMER(dequeue_profile_, "FirstBatchWaitTime");

  // Initialize various counters for measuring enqueuing into queues.
  bytes_received_counter_ =
      ADD_COUNTER(enqueue_profile_, "TotalBytesReceived", TUnit::BYTES);
  bytes_received_time_series_counter_ = ADD_SYSTEM_TIME_SERIES_COUNTER(
      enqueue_profile_, "BytesReceived", bytes_received_counter_);
  deserialize_row_batch_timer_ =
      ADD_TIMER(enqueue_profile_, "DeserializeRowBatchTime");
  total_eos_received_counter_ =
      ADD_COUNTER(enqueue_profile_, "TotalEosReceived", TUnit::UNIT);
  total_early_senders_counter_ =
      ADD_COUNTER(enqueue_profile_, "TotalEarlySenders", TUnit::UNIT);
  total_received_batches_counter_ =
      ADD_COUNTER(enqueue_profile_, "TotalBatchesReceived", TUnit::UNIT);
  total_enqueued_batches_counter_ =
      ADD_COUNTER(enqueue_profile_, "TotalBatchesEnqueued", TUnit::UNIT);
  total_deferred_rpcs_counter_ =
      ADD_COUNTER(enqueue_profile_, "TotalRPCsDeferred", TUnit::UNIT);
  deferred_rpcs_time_series_counter_ =
      enqueue_profile_->AddSamplingTimeSeriesCounter("DeferredQueueSize", TUnit::UNIT,
      bind<int64_t>(mem_fn(&KrpcDataStreamRecvr::num_deferred_rpcs), this), true);
  total_has_deferred_rpcs_timer_ =
      ADD_TIMER(enqueue_profile_, "TotalHasDeferredRPCsTime");
  dispatch_timer_ =
      ADD_SUMMARY_STATS_TIMER(enqueue_profile_, "DispatchTime");
}

Status KrpcDataStreamRecvr::GetNext(RowBatch* output_batch, bool* eos) {
  DCHECK(TestInfo::is_test() || FragmentInstanceState::IsFragmentExecThread());
  DCHECK(merger_.get() != nullptr);
  return merger_->GetNext(output_batch, eos);
}

void KrpcDataStreamRecvr::AddBatch(const TransmitDataRequestPB* request,
    TransmitDataResponsePB* response, RpcContext* rpc_context) {
  MonoDelta duration(MonoTime::Now() - rpc_context->GetTimeReceived());
  DCHECK_GE(duration.ToNanoseconds(), 0);
  dispatch_timer_->UpdateCounter(duration.ToNanoseconds());
  int use_sender_id = is_merging_ ? request->sender_id() : 0;
  // Add all batches to the same queue if is_merging_ is false.
  sender_queues_[use_sender_id]->AddBatch(request, response, rpc_context);
}

void KrpcDataStreamRecvr::ProcessDeferredRpc(int sender_id) {
  int use_sender_id = is_merging_ ? sender_id : 0;
  // Add all batches to the same queue if is_merging_ is false.
  sender_queues_[use_sender_id]->ProcessDeferredRpc();
}

void KrpcDataStreamRecvr::TakeOverEarlySender(unique_ptr<TransmitDataCtx> ctx) {
  int use_sender_id = is_merging_ ? ctx->request->sender_id() : 0;
  // Add all batches to the same queue if is_merging_ is false.
  sender_queues_[use_sender_id]->TakeOverEarlySender(move(ctx));
  COUNTER_ADD(total_early_senders_counter_, 1);
}

void KrpcDataStreamRecvr::RemoveSender(int sender_id) {
  int use_sender_id = is_merging_ ? sender_id : 0;
  sender_queues_[use_sender_id]->DecrementSenders();
  COUNTER_ADD(total_eos_received_counter_, 1);
}

void KrpcDataStreamRecvr::CancelStream() {
  for (auto& queue: sender_queues_) queue->Cancel();
}

void KrpcDataStreamRecvr::Close() {
  DCHECK(TestInfo::is_test() || FragmentInstanceState::IsFragmentExecThread());
  DCHECK(!closed_);
  closed_ = true;
  // Remove this receiver from the KrpcDataStreamMgr that created it.
  // All the sender queues will be cancelled after this call returns.
  const Status status = mgr_->DeregisterRecvr(fragment_instance_id(), dest_node_id());
  if (!status.ok()) {
    LOG(ERROR) << "Error deregistering receiver: " << status.GetDetail();
  }
  for (auto& queue: sender_queues_) queue->Close();
  merger_.reset();

  // Given all queues have been cancelled and closed already at this point, it's safe to
  // call Close() on 'deferred_rpc_tracker_' without holding any lock here.
  deferred_rpc_tracker_->Close();
  dequeue_profile_->StopPeriodicCounters();
  enqueue_profile_->StopPeriodicCounters();

  // Remove reference to the unowned resources which may be freed after Close().
  mgr_ = nullptr;
  row_desc_ = nullptr;
  parent_tracker_ = nullptr;
  buffer_pool_client_ = nullptr;
  profile_ = nullptr;
}

KrpcDataStreamRecvr::~KrpcDataStreamRecvr() {
  DCHECK(mgr_ == nullptr) << "Must call Close()";
}

Status KrpcDataStreamRecvr::GetBatch(RowBatch** next_batch) {
  DCHECK(!is_merging_);
  DCHECK_EQ(sender_queues_.size(), 1);
  return sender_queues_[0]->GetBatch(next_batch);
}

} // namespace impala
