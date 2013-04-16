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

#include "runtime/data-stream-mgr.h"

#include <iostream>
#include <boost/functional/hash.hpp>
#include <boost/thread/locks.hpp>
#include <boost/thread/thread.hpp>

#include "runtime/row-batch.h"
#include "runtime/data-stream-recvr.h"
#include "runtime/raw-value.h"
#include "util/debug-util.h"

#include "gen-cpp/ImpalaInternalService.h"
#include "gen-cpp/ImpalaInternalService_types.h"

using namespace std;
using namespace boost;
using namespace apache::thrift;
using namespace apache::thrift::protocol;
using namespace apache::thrift::transport;

namespace impala {

DataStreamMgr::StreamControlBlock::StreamControlBlock(
    const RowDescriptor& row_desc, const TUniqueId& fragment_instance_id,
    PlanNodeId dest_node_id, int num_senders, int buffer_size,
    RuntimeProfile* profile)
  : fragment_instance_id_(fragment_instance_id),
    dest_node_id_(dest_node_id),
    row_desc_(row_desc),
    is_cancelled_(false),
    buffer_limit_(buffer_size),
    num_buffered_bytes_(0),
    num_remaining_senders_(num_senders) {
  bytes_received_counter_ =
      ADD_COUNTER(profile, "BytesReceived", TCounterType::BYTES);
  deserialize_row_batch_timer_ =
      ADD_TIMER(profile, "DeserializeRowBatchTimer");
  buffer_full_wall_timer_ = ADD_TIMER(profile, "SendersBlockedTimer");
  buffer_full_total_timer_ = ADD_TIMER(profile, "SendersBlockedTotalTimer(*)");
  data_arrival_timer_ = ADD_TIMER(profile, "DataArrivalWaitTime");
  first_batch_wait_timer_ = ADD_TIMER(profile, "FirstBatchArrivalWaitTime");
}

RowBatch* DataStreamMgr::StreamControlBlock::GetBatch(bool* is_cancelled) {
  unique_lock<mutex> l(lock_);
  // wait until something shows up or we know we're done
  while (!is_cancelled_ && batch_queue_.empty() && num_remaining_senders_ > 0) {
    VLOG_ROW << "wait arrival fragment_instance_id=" << fragment_instance_id_
             << " node=" << dest_node_id_;
    SCOPED_TIMER(data_arrival_timer_);
    SCOPED_TIMER(received_first_batch_ ? NULL : first_batch_wait_timer_);
    data_arrival_.wait(l);
  }
  if (is_cancelled_) {
    *is_cancelled = true;
    return NULL;
  }
  *is_cancelled = false;
  if (batch_queue_.empty()) {
    DCHECK_EQ(num_remaining_senders_, 0);
    return NULL;
  }

  received_first_batch_ = true;

  DCHECK(!batch_queue_.empty());
  RowBatch* result = batch_queue_.front().second;
  num_buffered_bytes_ -= batch_queue_.front().first;
  VLOG_ROW << "fetched #rows=" << result->num_rows();
  batch_queue_.pop_front();
  data_removal_.notify_one();
  return result;
}

void DataStreamMgr::StreamControlBlock::AddBatch(const TRowBatch& thrift_batch) {
  unique_lock<mutex> l(lock_);
  if (is_cancelled_) return;
  int batch_size = RowBatch::GetBatchSize(thrift_batch);
  RowBatch* batch = NULL;
  {
    SCOPED_TIMER(deserialize_row_batch_timer_);
    batch = new RowBatch(row_desc_, thrift_batch);
  }
  COUNTER_UPDATE(bytes_received_counter_, batch_size);
  DCHECK_GT(num_remaining_senders_, 0);

  // if there's something in the queue and this batch will push us over the
  // buffer limit we need to wait until the batch gets drained
  while (!batch_queue_.empty() && num_buffered_bytes_ + batch_size > buffer_limit_ &&
      !is_cancelled_) {
    SCOPED_TIMER(buffer_full_total_timer_);
    VLOG_ROW << " wait removal: empty=" << (batch_queue_.empty() ? 1 : 0)
             << " #buffered=" << num_buffered_bytes_
             << " batch_size=" << batch_size << "\n";

    // We only want one thread running the timer at any one time. Only
    // one thread may lock the try_lock, and that 'winner' starts the
    // scoped timer.
    bool got_timer_lock = false;
    {
      try_mutex::scoped_try_lock timer_lock(buffer_wall_timer_lock_);
      if (timer_lock) {
        SCOPED_TIMER(buffer_full_wall_timer_);
        data_removal_.wait(l);
        got_timer_lock = true;
      } else {
        data_removal_.wait(l);
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
    if (got_timer_lock) data_removal_.notify_one();
  }

  // If we've been cancelled, just return and drop the incoming row batch.  This lets
  // senders (remote fragments) get unblocked.
  if (is_cancelled_) return;

  VLOG_ROW << "added #rows=" << batch->num_rows()
           << " batch_size=" << batch_size << "\n";
  batch_queue_.push_back(make_pair(batch_size, batch));
  num_buffered_bytes_ += batch_size;
  data_arrival_.notify_one();
}

void DataStreamMgr::StreamControlBlock::DecrementSenders() {
  lock_guard<mutex> l(lock_);
  DCHECK_GT(num_remaining_senders_, 0);
  num_remaining_senders_ = max(0, num_remaining_senders_ - 1);
  VLOG_FILE << "decremented senders: fragment_instance_id=" << fragment_instance_id_
            << " node_id=" << dest_node_id_
            << " #senders=" << num_remaining_senders_;
  if (num_remaining_senders_ == 0) data_arrival_.notify_one();
}

void DataStreamMgr::StreamControlBlock::CancelStream() {
  {
    lock_guard<mutex> l(lock_);
    if (is_cancelled_) return;
    is_cancelled_ = true;
    VLOG_QUERY << "cancelled stream: fragment_instance_id_=" << fragment_instance_id_
              << " node_id=" << dest_node_id_;
  }
  // Wake up all threads waiting to produce/consume batches.  They will all
  // notice that the stream is cancelled and handle it.
  data_arrival_.notify_all();
  data_removal_.notify_all();

  // Delete any batches queued in batch_queue_
  for (RowBatchQueue::iterator it = batch_queue_.begin();
      it != batch_queue_.end(); ++it) {
    delete it->second;
  }
}

inline uint32_t DataStreamMgr::GetHashValue(
    const TUniqueId& fragment_instance_id, PlanNodeId node_id) {
  uint32_t value = RawValue::GetHashValue(&fragment_instance_id.lo, TYPE_BIGINT, 0);
  value = RawValue::GetHashValue(&fragment_instance_id.hi, TYPE_BIGINT, value);
  value = RawValue::GetHashValue(&node_id, TYPE_INT, value);
  return value;
}

DataStreamRecvr* DataStreamMgr::CreateRecvr(
    const RowDescriptor& row_desc, const TUniqueId& fragment_instance_id,
    PlanNodeId dest_node_id, int num_senders, int buffer_size, RuntimeProfile* profile) {
  DCHECK(profile != NULL);
  VLOG_FILE << "creating receiver for fragment="
            << fragment_instance_id << ", node=" << dest_node_id;
  shared_ptr<StreamControlBlock> cb(
      new StreamControlBlock(row_desc, fragment_instance_id, dest_node_id, num_senders,
                             buffer_size, profile));
  size_t hash_value = GetHashValue(fragment_instance_id, dest_node_id);
  lock_guard<mutex> l(lock_);
  fragment_stream_set_.insert(make_pair(fragment_instance_id, dest_node_id));
  stream_map_.insert(make_pair(hash_value, cb));
  return new DataStreamRecvr(this, cb);
}

shared_ptr<DataStreamMgr::StreamControlBlock> DataStreamMgr::FindControlBlock(
    const TUniqueId& fragment_instance_id, PlanNodeId node_id, bool acquire_lock) {
  VLOG_ROW << "looking up fragment_instance_id=" << fragment_instance_id
           << ", node=" << node_id;
  size_t hash_value = GetHashValue(fragment_instance_id, node_id);
  if (acquire_lock) lock_.lock();
  pair<StreamMap::iterator, StreamMap::iterator> range =
      stream_map_.equal_range(hash_value);
  while (range.first != range.second) {
    shared_ptr<StreamControlBlock> cb = range.first->second;
    if (cb->fragment_instance_id() == fragment_instance_id
        && cb->dest_node_id() == node_id) {
      if (acquire_lock) lock_.unlock();
      return cb;
    }
    ++range.first;
  }
  if (acquire_lock) lock_.unlock();
  return shared_ptr<StreamControlBlock>();
}

Status DataStreamMgr::AddData(
    const TUniqueId& fragment_instance_id, PlanNodeId dest_node_id,
    const TRowBatch& thrift_batch) {
  VLOG_ROW << "AddData(): fragment_instance_id=" << fragment_instance_id
           << " node=" << dest_node_id
           << " size=" << RowBatch::GetBatchSize(thrift_batch);
  shared_ptr<StreamControlBlock> cb =
      FindControlBlock(fragment_instance_id, dest_node_id);
  if (cb == NULL) {
    // The receiver may tear down its StreamControlBlock via DeregisterRecvr()
    // at any time without considering the remaining number of senders.
    // As a consequence, FindControlBlock() may return an innocuous NULL if a thread
    // calling DeregisterRecvr() beat the thread calling FindControlBlock()
    // in acquiring lock_.
    // TODO: Rethink the lifecycle of StreamControlBlock to distinguish
    // errors from receiver-initiated teardowns.
    return Status::OK;
  }
  cb->AddBatch(thrift_batch);
  return Status::OK;
}

Status DataStreamMgr::CloseSender(
    const TUniqueId& fragment_instance_id, PlanNodeId dest_node_id) {
  VLOG_FILE << "CloseSender(): fragment_instance_id=" << fragment_instance_id
            << ", node=" << dest_node_id;
  shared_ptr<StreamControlBlock> cb =
      FindControlBlock(fragment_instance_id, dest_node_id);
  if (cb == NULL) {
    // The receiver may tear down its StreamControlBlock via DeregisterRecvr()
    // at any time without considering the remaining number of senders.
    // As a consequence, FindControlBlock() may return an innocuous NULL if a thread
    // calling DeregisterRecvr() beat the thread calling FindControlBlock()
    // in acquiring lock_.
    // TODO: Rethink the lifecycle of StreamControlBlock to distinguish
    // errors from receiver-initiated teardowns.
    return Status::OK;
  }
  cb->DecrementSenders();
  return Status::OK;
}

Status DataStreamMgr::DeregisterRecvr(
    const TUniqueId& fragment_instance_id, PlanNodeId node_id) {
  VLOG_QUERY << "DeregisterRecvr(): fragment_instance_id=" << fragment_instance_id
             << ", node=" << node_id;
  size_t hash_value = GetHashValue(fragment_instance_id, node_id);
  lock_guard<mutex> l(lock_);
  pair<StreamMap::iterator, StreamMap::iterator> range =
      stream_map_.equal_range(hash_value);
  while (range.first != range.second) {
    const shared_ptr<StreamControlBlock>& cb = range.first->second;
    if (cb->fragment_instance_id() == fragment_instance_id
        && cb->dest_node_id() == node_id) {
      // Notify concurrent AddData() requests that the stream has been terminated.
      cb->CancelStream();
      fragment_stream_set_.erase(make_pair(cb->fragment_instance_id(),
          cb->dest_node_id()));
      stream_map_.erase(range.first);
      return Status::OK;
    }
    ++range.first;
  }

  stringstream err;
  err << "unknown row receiver id: fragment_instance_id=" << fragment_instance_id
      << " node_id=" << node_id;
  LOG(ERROR) << err.str();
  return Status(err.str());
}

void DataStreamMgr::Cancel(const TUniqueId& fragment_instance_id) {
  VLOG_QUERY << "cancelling all streams for fragment=" << fragment_instance_id;
  lock_guard<mutex> l(lock_);
  FragmentStreamSet::iterator i =
      fragment_stream_set_.lower_bound(make_pair(fragment_instance_id, 0));
  while (i != fragment_stream_set_.end() && i->first == fragment_instance_id) {
    shared_ptr<StreamControlBlock> cb = FindControlBlock(i->first, i->second, false);
    if (cb == NULL) {
      // keep going but at least log it
      stringstream err;
      err << "Cancel(): missing in stream_map: fragment=" << i->first
          << " node=" << i->second;
      LOG(ERROR) << err.str();
    } else {
      cb->CancelStream();
    }
    ++i;
  }
}

}
