// Copyright (c) 2011 Cloudera, Inc. All rights reserved.

#include "runtime/data-stream-mgr.h"

#include <iostream>
#include <boost/functional/hash.hpp>
#include <boost/thread/locks.hpp>
#include <boost/thread/thread.hpp>
#include <glog/logging.h>

#include "runtime/row-batch.h"
#include "runtime/data-stream-recvr.h"
#include "gen-cpp/ImpalaBackendService.h"
#include "gen-cpp/ImpalaBackendService_types.h"

using namespace std;
using namespace boost;
using namespace apache::thrift;
using namespace apache::thrift::protocol;
using namespace apache::thrift::transport;

namespace impala {

DataStreamMgr::StreamControlBlock::StreamControlBlock(
    const RowDescriptor& row_desc, const TUniqueId& query_id, PlanNodeId dest_node_id,
    int num_senders, int buffer_size)
  : query_id_(query_id),
    dest_node_id_(dest_node_id),
    row_desc_(row_desc),
    buffer_limit_(buffer_size),
    num_buffered_bytes_(0),
    num_remaining_senders_(num_senders) {
}

RowBatch* DataStreamMgr::StreamControlBlock::GetBatch() {
  unique_lock<mutex> l(lock_);
  // wait until something shows up or we know we're done
  while (batch_queue_.empty() && num_remaining_senders_ > 0) {
    VLOG(1) << "wait arrival query=" << query_id_ << " node=" << dest_node_id_;
    data_arrival_.wait(l);
  }
  if (batch_queue_.empty()) {
    DCHECK_EQ(num_remaining_senders_, 0);
    return NULL;
  } else {
    DCHECK(!batch_queue_.empty());
    RowBatch* result = batch_queue_.front().second;
    num_buffered_bytes_ -= batch_queue_.front().first;
    VLOG(1) << "fetched #rows=" << result->num_rows();
    batch_queue_.pop_front();
    data_removal_.notify_one();
    return result;
  }
}

void DataStreamMgr::StreamControlBlock::AddBatch(const TRowBatch& thrift_batch) {
  int batch_size = RowBatch::GetBatchSize(thrift_batch);
  RowBatch* batch = new RowBatch(row_desc_, thrift_batch);
  unique_lock<mutex> l(lock_);
  DCHECK_GT(num_remaining_senders_, 0);
  // if there's something in the queue and this batch will push us over the
  // buffer limit we need to wait until the batch gets drained
  while (!batch_queue_.empty() && num_buffered_bytes_ + batch_size > buffer_limit_) {
    VLOG(1) << " wait removal: empty=" << (batch_queue_.empty() ? 1 : 0)
            << " #buffered=" << num_buffered_bytes_
            << " batch_size=" << batch_size << "\n";
    data_removal_.wait(l);
  }
  VLOG(1) << "added #rows=" << batch->num_rows()
          << " batch_size=" << batch_size << "\n";
  batch_queue_.push_back(make_pair(batch_size, batch));
  num_buffered_bytes_ += batch_size;
  data_arrival_.notify_one();
}

void DataStreamMgr::StreamControlBlock::DecrementSenders() {
  lock_guard<mutex> l(lock_);
  DCHECK_GT(num_remaining_senders_, 0);
  num_remaining_senders_ = max(0, num_remaining_senders_ - 1);
  VLOG(1) << "decremented senders: query_id=" << query_id_
          << " node_id=" << dest_node_id_ << " #senders=" << num_remaining_senders_;
  if (num_remaining_senders_ == 0) data_arrival_.notify_one();
}

inline size_t DataStreamMgr::GetHashValue(const TUniqueId& query_id, PlanNodeId node_id) {
  size_t value = hash<int64_t>().operator()(query_id.lo);
  hash_combine(value, hash<int64_t>().operator()(query_id.hi));
  hash_combine(value, hash<int32_t>().operator()(node_id));
  return value;
}

DataStreamRecvr* DataStreamMgr::CreateRecvr(
    const RowDescriptor& row_desc, const TUniqueId& query_id, PlanNodeId dest_node_id,
    int num_senders, int buffer_size) {
  VLOG(2) << "creating receiver for query=" << query_id << ", node=" << dest_node_id;
  StreamControlBlock* cb = pool_.Add(
      new StreamControlBlock(row_desc, query_id, dest_node_id, num_senders,
                             buffer_size));
  size_t hash_value = GetHashValue(query_id, dest_node_id);
  lock_guard<mutex> l(stream_map_lock_);
  stream_map_.insert(make_pair(hash_value, cb));
  return new DataStreamRecvr(this, cb);
}

DataStreamMgr::StreamMap::iterator DataStreamMgr::FindControlBlock(
    const TUniqueId& query_id, PlanNodeId node_id) {
  VLOG(2) << "looking up query=" << query_id << ", node=" << node_id;
  size_t hash_value = GetHashValue(query_id, node_id);
  lock_guard<mutex> l(stream_map_lock_);
  pair<StreamMap::iterator, StreamMap::iterator> range =
      stream_map_.equal_range(hash_value);
  while (range.first != range.second) {
    StreamControlBlock* cb = range.first->second;
    if (cb->query_id() == query_id && cb->dest_node_id() == node_id) return range.first;
    ++range.first;
  }
  return stream_map_.end();
}

Status DataStreamMgr::AddData(
    const TUniqueId& query_id, PlanNodeId dest_node_id, const TRowBatch& thrift_batch) {
  VLOG(2) << "AddData(): " << RowBatch::GetBatchSize(thrift_batch);
  StreamMap::iterator i = FindControlBlock(query_id, dest_node_id);
  if (i == stream_map_.end()) {
    stringstream err;
    err << "unknown row batch destination: query_id=" << query_id
        << " node_id=" << dest_node_id;
    LOG(ERROR) << err.str();
    return Status(err.str());
  }
  i->second->AddBatch(thrift_batch);
  return Status::OK;
}

Status DataStreamMgr::CloseChannel(const TUniqueId& query_id, PlanNodeId dest_node_id) {
  StreamMap::iterator i = FindControlBlock(query_id, dest_node_id);
  if (i == stream_map_.end()) {
    stringstream err;
    err << "unknown row batch destination: query_id=" << query_id
        << " node_id=" << dest_node_id;
    LOG(ERROR) << err.str();
    return Status(err.str());
  }
  i->second->DecrementSenders();
  return Status::OK;
}

Status DataStreamMgr::DeregisterRecvr(const TUniqueId& query_id, PlanNodeId node_id) {
  VLOG(2) << "DeregisterRecvr(): query=" << query_id << ", node=" << node_id;
  StreamMap::iterator i = FindControlBlock(query_id, node_id);
  if (i == stream_map_.end()) {
    stringstream err;
    err << "unknown row receiver id: query_id=" << query_id
        << " node_id=" << node_id;
    LOG(ERROR) << err.str();
    return Status(err.str());
  }
  lock_guard<mutex> l(stream_map_lock_);
  stream_map_.erase(i);
  return Status::OK;
}

}
