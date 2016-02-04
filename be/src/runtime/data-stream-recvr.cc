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

#include <boost/thread/locks.hpp>
#include <boost/thread/mutex.hpp>

#include "runtime/data-stream-recvr.h"
#include "runtime/data-stream-mgr.h"
#include "runtime/row-batch.h"
#include "runtime/sorted-run-merger.h"
#include "util/runtime-profile.h"
#include "util/periodic-counter-updater.h"

#include "common/names.h"

using boost::condition_variable;

namespace impala {

// Implements a blocking queue of row batches from one or more senders. One queue
// is maintained per sender if is_merging_ is true for the enclosing receiver, otherwise
// rows from all senders are placed in the same queue.
class DataStreamRecvr::SenderQueue {
 public:
  SenderQueue(DataStreamRecvr* parent_recvr, int num_senders, RuntimeProfile* profile);

  // Return the next batch form this sender queue. Sets the returned batch in cur_batch_.
  // A returned batch that is not filled to capacity does *not* indicate
  // end-of-stream.
  // The call blocks until another batch arrives or all senders close
  // their channels. The returned batch is owned by the sender queue. The caller
  // must acquire data from the returned batch before the next call to GetBatch().
  Status GetBatch(RowBatch** next_batch);

  // Adds a row batch to this sender queue if this stream has not been cancelled;
  // blocks if this will make the stream exceed its buffer limit.
  // If the total size of the batches in this queue would exceed the allowed buffer size,
  // the queue is considered full and the call blocks until a batch is dequeued.
  void AddBatch(const TRowBatch& batch);

  // Decrement the number of remaining senders for this queue and signal eos ("new data")
  // if the count drops to 0. The number of senders will be 1 for a merging
  // DataStreamRecvr.
  void DecrementSenders();

  // Set cancellation flag and signal cancellation to receiver and sender. Subsequent
  // incoming batches will be dropped.
  void Cancel();

  // Must be called once to cleanup any queued resources.
  void Close();

  // Returns the current batch from this queue being processed by a consumer.
  RowBatch* current_batch() const { return current_batch_.get(); }

 private:
  // Receiver of which this queue is a member.
  DataStreamRecvr* recvr_;

  // protects all subsequent data.
  mutex lock_;

  // if true, the receiver fragment for this stream got cancelled
  bool is_cancelled_;

  // number of senders which haven't closed the channel yet
  // (if it drops to 0, end-of-stream is true)
  int num_remaining_senders_;

  // signal arrival of new batch or the eos/cancelled condition
  condition_variable data_arrival_cv_;

  // signal removal of data by stream consumer
  condition_variable data_removal__cv_;

  // queue of (batch length, batch) pairs.  The SenderQueue block owns memory to
  // these batches. They are handed off to the caller via GetBatch.
  typedef list<pair<int, RowBatch*> > RowBatchQueue;
  RowBatchQueue batch_queue_;

  // The batch that was most recently returned via GetBatch(), i.e. the current batch
  // from this queue being processed by a consumer. Is destroyed when the next batch
  // is retrieved.
  scoped_ptr<RowBatch> current_batch_;

  // Set to true when the first batch has been received
  bool received_first_batch_;
};

DataStreamRecvr::SenderQueue::SenderQueue(DataStreamRecvr* parent_recvr, int num_senders,
    RuntimeProfile* profile)
  : recvr_(parent_recvr),
    is_cancelled_(false),
    num_remaining_senders_(num_senders),
    received_first_batch_(false) {
}

Status DataStreamRecvr::SenderQueue::GetBatch(RowBatch** next_batch) {
  unique_lock<mutex> l(lock_);
  // wait until something shows up or we know we're done
  while (!is_cancelled_ && batch_queue_.empty() && num_remaining_senders_ > 0) {
    VLOG_ROW << "wait arrival fragment_instance_id=" << recvr_->fragment_instance_id()
             << " node=" << recvr_->dest_node_id();
    // Don't count time spent waiting on the sender as active time.
    CANCEL_SAFE_SCOPED_TIMER(recvr_->data_arrival_timer_, &is_cancelled_);
    CANCEL_SAFE_SCOPED_TIMER(
        received_first_batch_ ? NULL : recvr_->first_batch_wait_total_timer_,
        &is_cancelled_);
    data_arrival_cv_.wait(l);
  }

  // cur_batch_ must be replaced with the returned batch.
  current_batch_.reset();
  *next_batch = NULL;
  if (is_cancelled_) return Status::CANCELLED;

  if (batch_queue_.empty()) {
    DCHECK_EQ(num_remaining_senders_, 0);
    return Status::OK();
  }

  received_first_batch_ = true;

  DCHECK(!batch_queue_.empty());
  RowBatch* result = batch_queue_.front().second;
  recvr_->num_buffered_bytes_ -= batch_queue_.front().first;
  VLOG_ROW << "fetched #rows=" << result->num_rows();
  batch_queue_.pop_front();
  data_removal__cv_.notify_one();
  current_batch_.reset(result);
  *next_batch = current_batch_.get();
  return Status::OK();
}

void DataStreamRecvr::SenderQueue::AddBatch(const TRowBatch& thrift_batch) {
  unique_lock<mutex> l(lock_);
  if (is_cancelled_) return;

  int batch_size = RowBatch::GetBatchSize(thrift_batch);
  COUNTER_ADD(recvr_->bytes_received_counter_, batch_size);
  DCHECK_GT(num_remaining_senders_, 0);

  // if there's something in the queue and this batch will push us over the
  // buffer limit we need to wait until the batch gets drained.
  // Note: It's important that we enqueue thrift_batch regardless of buffer limit if
  // the queue is currently empty. In the case of a merging receiver, batches are
  // received from a specific queue based on data order, and the pipeline will stall
  // if the merger is waiting for data from an empty queue that cannot be filled because
  // the limit has been reached.
  while (!batch_queue_.empty() && recvr_->ExceedsLimit(batch_size) && !is_cancelled_) {
    CANCEL_SAFE_SCOPED_TIMER(recvr_->buffer_full_total_timer_, &is_cancelled_);
    VLOG_ROW << " wait removal: empty=" << (batch_queue_.empty() ? 1 : 0)
             << " #buffered=" << recvr_->num_buffered_bytes_
             << " batch_size=" << batch_size << "\n";

    // We only want one thread running the timer at any one time. Only
    // one thread may lock the try_lock, and that 'winner' starts the
    // scoped timer.
    bool got_timer_lock = false;
    {
      try_mutex::scoped_try_lock timer_lock(recvr_->buffer_wall_timer_lock_);
      if (timer_lock) {
        CANCEL_SAFE_SCOPED_TIMER(recvr_->buffer_full_wall_timer_, &is_cancelled_);
        data_removal__cv_.wait(l);
        got_timer_lock = true;
      } else {
        data_removal__cv_.wait(l);
        got_timer_lock = false;
      }
    }
    // If we had the timer lock, wake up another writer to make sure
    // that they (if no-one else) starts the timer. The guarantee is
    // that if no thread has the try_lock, the thread that we wake up
    // here will obtain it and run the timer.
    //
    // We must have given up the try_lock by this point, otherwise the
    // woken thread might not successfully take the lock once it has
    // woken up. (In fact, no other thread will run in AddBatch until
    // this thread exits because of mutual exclusion around lock_, but
    // it's good not to rely on that fact).
    //
    // The timer may therefore be an underestimate by the amount of
    // time it takes this thread to finish (and yield lock_) and the
    // notified thread to be woken up and to acquire the try_lock. In
    // practice, this time is small relative to the total wait time.
    if (got_timer_lock) data_removal__cv_.notify_one();
  }

  if (!is_cancelled_) {
    RowBatch* batch = NULL;
    {
      SCOPED_TIMER(recvr_->deserialize_row_batch_timer_);
      // Note: if this function makes a row batch, the batch *must* be added
      // to batch_queue_. It is not valid to create the row batch and destroy
      // it in this thread.
      batch = new RowBatch(recvr_->row_desc(), thrift_batch, recvr_->mem_tracker());
    }
    VLOG_ROW << "added #rows=" << batch->num_rows()
             << " batch_size=" << batch_size << "\n";
    batch_queue_.push_back(make_pair(batch_size, batch));
    recvr_->num_buffered_bytes_ += batch_size;
    data_arrival_cv_.notify_one();
  }
}

void DataStreamRecvr::SenderQueue::DecrementSenders() {
  lock_guard<mutex> l(lock_);
  DCHECK_GT(num_remaining_senders_, 0);
  num_remaining_senders_ = max(0, num_remaining_senders_ - 1);
  VLOG_FILE << "decremented senders: fragment_instance_id="
            << recvr_->fragment_instance_id()
            << " node_id=" << recvr_->dest_node_id()
            << " #senders=" << num_remaining_senders_;
  if (num_remaining_senders_ == 0) data_arrival_cv_.notify_one();
}

void DataStreamRecvr::SenderQueue::Cancel() {
  {
    lock_guard<mutex> l(lock_);
    if (is_cancelled_) return;
    is_cancelled_ = true;
    VLOG_QUERY << "cancelled stream: fragment_instance_id_="
               << recvr_->fragment_instance_id()
               << " node_id=" << recvr_->dest_node_id();
  }
  // Wake up all threads waiting to produce/consume batches.  They will all
  // notice that the stream is cancelled and handle it.
  data_arrival_cv_.notify_all();
  data_removal__cv_.notify_all();
  PeriodicCounterUpdater::StopTimeSeriesCounter(
      recvr_->bytes_received_time_series_counter_);
}

void DataStreamRecvr::SenderQueue::Close() {
  // Delete any batches queued in batch_queue_
  for (RowBatchQueue::iterator it = batch_queue_.begin();
      it != batch_queue_.end(); ++it) {
    delete it->second;
  }

  current_batch_.reset();
}

Status DataStreamRecvr::CreateMerger(const TupleRowComparator& less_than) {
  DCHECK(is_merging_);
  vector<SortedRunMerger::RunBatchSupplier> input_batch_suppliers;
  input_batch_suppliers.reserve(sender_queues_.size());

  // Create the merger that will a single stream of sorted rows.
  merger_.reset(new SortedRunMerger(less_than, &row_desc_, profile_, false));

  for (int i = 0; i < sender_queues_.size(); ++i) {
    input_batch_suppliers.push_back(
        bind(mem_fn(&SenderQueue::GetBatch), sender_queues_[i], _1));
  }
  RETURN_IF_ERROR(merger_->Prepare(input_batch_suppliers));
  return Status::OK();
}

void DataStreamRecvr::TransferAllResources(RowBatch* transfer_batch) {
  BOOST_FOREACH(SenderQueue* sender_queue, sender_queues_) {
    if (sender_queue->current_batch() != NULL) {
      sender_queue->current_batch()->TransferResourceOwnership(transfer_batch);
    }
  }
}

DataStreamRecvr::DataStreamRecvr(DataStreamMgr* stream_mgr, MemTracker* parent_tracker,
    const RowDescriptor& row_desc, const TUniqueId& fragment_instance_id,
    PlanNodeId dest_node_id, int num_senders, bool is_merging, int total_buffer_limit,
    RuntimeProfile* profile)
  : mgr_(stream_mgr),
    fragment_instance_id_(fragment_instance_id),
    dest_node_id_(dest_node_id),
    total_buffer_limit_(total_buffer_limit),
    row_desc_(row_desc),
    is_merging_(is_merging),
    num_buffered_bytes_(0),
    profile_(profile) {
  mem_tracker_.reset(new MemTracker(-1, -1, "DataStreamRecvr", parent_tracker));
  // Create one queue per sender if is_merging is true.
  int num_queues = is_merging ? num_senders : 1;
  sender_queues_.reserve(num_queues);
  int num_sender_per_queue = is_merging ? 1 : num_senders;
  for (int i = 0; i < num_queues; ++i) {
    SenderQueue* queue = sender_queue_pool_.Add(new SenderQueue(this,
        num_sender_per_queue, profile));
    sender_queues_.push_back(queue);
  }

  // Initialize the counters
  bytes_received_counter_ =
      ADD_COUNTER(profile_, "BytesReceived", TUnit::BYTES);
  bytes_received_time_series_counter_ =
      ADD_TIME_SERIES_COUNTER(profile_, "BytesReceived", bytes_received_counter_);
  deserialize_row_batch_timer_ =
      ADD_TIMER(profile_, "DeserializeRowBatchTimer");
  buffer_full_wall_timer_ = ADD_TIMER(profile_, "SendersBlockedTimer");
  buffer_full_total_timer_ = ADD_TIMER(profile_, "SendersBlockedTotalTimer(*)");
  data_arrival_timer_ = profile_->inactive_timer();
  first_batch_wait_total_timer_ = ADD_TIMER(profile_, "FirstBatchArrivalWaitTime");
}

Status DataStreamRecvr::GetNext(RowBatch* output_batch, bool* eos) {
  DCHECK(merger_.get() != NULL);
  return merger_->GetNext(output_batch, eos);
}

void DataStreamRecvr::AddBatch(const TRowBatch& thrift_batch, int sender_id) {
  int use_sender_id = is_merging_ ? sender_id : 0;
  // Add all batches to the same queue if is_merging_ is false.
  sender_queues_[use_sender_id]->AddBatch(thrift_batch);
}

void DataStreamRecvr::RemoveSender(int sender_id) {
  int use_sender_id = is_merging_ ? sender_id : 0;
  sender_queues_[use_sender_id]->DecrementSenders();
}

void DataStreamRecvr::CancelStream() {
  for (int i = 0; i < sender_queues_.size(); ++i) {
    sender_queues_[i]->Cancel();
  }
}

void DataStreamRecvr::Close() {
  for (int i = 0; i < sender_queues_.size(); ++i) {
    sender_queues_[i]->Close();
  }
  // Remove this receiver from the DataStreamMgr that created it.
  // TODO: log error msg
  mgr_->DeregisterRecvr(fragment_instance_id(), dest_node_id());
  mgr_ = NULL;
  merger_.reset();
  mem_tracker_->UnregisterFromParent();
  mem_tracker_.reset();
}

DataStreamRecvr::~DataStreamRecvr() {
  DCHECK(mgr_ == NULL) << "Must call Close()";
}

Status DataStreamRecvr::GetBatch(RowBatch** next_batch) {
  DCHECK(!is_merging_);
  DCHECK_EQ(sender_queues_.size(), 1);
  return sender_queues_[0]->GetBatch(next_batch);
}

}
