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
    const RowDescriptor& row_desc, const TUniqueId& fragment_id,
    PlanNodeId dest_node_id, int num_senders, int buffer_size,
    RuntimeProfile* profile) 
  : fragment_id_(fragment_id),
    dest_node_id_(dest_node_id),
    row_desc_(row_desc),
    is_cancelled_(false),
    buffer_limit_(buffer_size),
    num_buffered_bytes_(0),
    num_remaining_senders_(num_senders) {
  bytes_received_counter_ = 
      ADD_COUNTER(profile, "BytesReceived", TCounterType::BYTES);
  deserialize_row_batch_timer_ = 
      ADD_COUNTER(profile, "DeserializeRowBatchTimer", TCounterType::CPU_TICKS);
}

RowBatch* DataStreamMgr::StreamControlBlock::GetBatch(bool* is_cancelled) {
  unique_lock<mutex> l(lock_);
  // wait until something shows up or we know we're done
  while (!is_cancelled_ && batch_queue_.empty() && num_remaining_senders_ > 0) {
    VLOG_ROW << "wait arrival query=" << fragment_id_ << " node=" << dest_node_id_;
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
  while (!batch_queue_.empty() && num_buffered_bytes_ + batch_size > buffer_limit_) {
    VLOG_ROW << " wait removal: empty=" << (batch_queue_.empty() ? 1 : 0)
             << " #buffered=" << num_buffered_bytes_
             << " batch_size=" << batch_size << "\n";
    data_removal_.wait(l);
  }
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
  VLOG_FILE << "decremented senders: fragment_id=" << fragment_id_
            << " node_id=" << dest_node_id_
            << " #senders=" << num_remaining_senders_;
  if (num_remaining_senders_ == 0) data_arrival_.notify_one();
}

void DataStreamMgr::StreamControlBlock::CancelStream() {
  lock_guard<mutex> l(lock_);
  is_cancelled_ = true;
  VLOG_QUERY << "cancelled stream: fragment_id=" << fragment_id_
             << " node_id=" << dest_node_id_;
  data_arrival_.notify_one();
}

inline uint32_t DataStreamMgr::GetHashValue(
    const TUniqueId& fragment_id, PlanNodeId node_id) {
  uint32_t value = RawValue::GetHashValue(&fragment_id.lo, TYPE_BIGINT, 0);
  value = RawValue::GetHashValue(&fragment_id.hi, TYPE_BIGINT, value);
  value = RawValue::GetHashValue(&node_id, TYPE_INT, value);
  return value;
}

DataStreamRecvr* DataStreamMgr::CreateRecvr(
    const RowDescriptor& row_desc, const TUniqueId& fragment_id, PlanNodeId dest_node_id,
    int num_senders, int buffer_size, RuntimeProfile* profile) {
  DCHECK(profile != NULL);
  VLOG_FILE << "creating receiver for fragment="
            << fragment_id << ", node=" << dest_node_id;
  shared_ptr<StreamControlBlock> cb(
      new StreamControlBlock(row_desc, fragment_id, dest_node_id, num_senders,
                             buffer_size, profile));
  size_t hash_value = GetHashValue(fragment_id, dest_node_id);
  lock_guard<mutex> l(lock_);
  fragment_stream_set_.insert(make_pair(fragment_id, dest_node_id));
  stream_map_.insert(make_pair(hash_value, cb));
  return new DataStreamRecvr(this, cb);
}

shared_ptr<DataStreamMgr::StreamControlBlock> DataStreamMgr::FindControlBlock(
    const TUniqueId& fragment_id, PlanNodeId node_id, bool acquire_lock) {
  VLOG_ROW << "looking up fragment_id=" << fragment_id << ", node=" << node_id;
  size_t hash_value = GetHashValue(fragment_id, node_id);
  if (acquire_lock) lock_.lock();
  pair<StreamMap::iterator, StreamMap::iterator> range =
      stream_map_.equal_range(hash_value);
  while (range.first != range.second) {
    shared_ptr<StreamControlBlock> cb = range.first->second;
    if (cb->fragment_id() == fragment_id && cb->dest_node_id() == node_id) {
      if (acquire_lock) lock_.unlock();
      return cb;
    }
    ++range.first;
  }
  if (acquire_lock) lock_.unlock();
  return shared_ptr<StreamControlBlock>();
}

Status DataStreamMgr::AddData(
    const TUniqueId& fragment_id, PlanNodeId dest_node_id,
    const TRowBatch& thrift_batch) {
  VLOG_ROW << "AddData(): fragment_id=" << fragment_id << " node=" << dest_node_id
          << " size=" << RowBatch::GetBatchSize(thrift_batch);
  shared_ptr<StreamControlBlock> cb = FindControlBlock(fragment_id, dest_node_id);
  if (cb == NULL) {
    stringstream err;
    err << "unknown row batch destination: fragment_id=" << fragment_id
        << " node_id=" << dest_node_id;
    LOG(ERROR) << err.str();
    return Status(err.str());
  }
  cb->AddBatch(thrift_batch);
  return Status::OK;
}

Status DataStreamMgr::CloseSender(
    const TUniqueId& fragment_id, PlanNodeId dest_node_id) {
  VLOG_FILE << "CloseSender(): fragment_id=" << fragment_id << ", node=" << dest_node_id;
  shared_ptr<StreamControlBlock> cb = FindControlBlock(fragment_id, dest_node_id);
  if (cb == NULL) {
    stringstream err;
    err << "unknown row batch destination: fragment_id=" << fragment_id
        << " node_id=" << dest_node_id;
    LOG(ERROR) << err.str();
    return Status(err.str());
  }
  cb->DecrementSenders();
  return Status::OK;
}

Status DataStreamMgr::DeregisterRecvr(const TUniqueId& fragment_id, PlanNodeId node_id) {
  VLOG_QUERY << "DeregisterRecvr(): fragment_id=" << fragment_id << ", node=" << node_id;
  size_t hash_value = GetHashValue(fragment_id, node_id);
  lock_guard<mutex> l(lock_);
  pair<StreamMap::iterator, StreamMap::iterator> range =
      stream_map_.equal_range(hash_value);
  while (range.first != range.second) {
    const shared_ptr<StreamControlBlock>& cb = range.first->second;
    if (cb->fragment_id() == fragment_id && cb->dest_node_id() == node_id) {
      fragment_stream_set_.erase(make_pair(cb->fragment_id(), cb->dest_node_id()));
      stream_map_.erase(range.first);
      return Status::OK;
    }
    ++range.first;
  }

  stringstream err;
  err << "unknown row receiver id: fragment_id=" << fragment_id
      << " node_id=" << node_id;
  LOG(ERROR) << err.str();
  return Status(err.str());
}

void DataStreamMgr::Cancel(const TUniqueId& fragment_id) {
  VLOG_QUERY << "cancelling all streams for fragment=" << fragment_id;
  lock_guard<mutex> l(lock_);
  FragmentStreamSet::iterator i =
      fragment_stream_set_.lower_bound(make_pair(fragment_id, 0));
  while (i != fragment_stream_set_.end() && i->first == fragment_id) {
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
