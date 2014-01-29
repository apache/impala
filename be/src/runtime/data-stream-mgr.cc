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
#include "runtime/runtime-state.h"
#include "util/debug-util.h"
#include "util/periodic-counter-updater.h"

#include "gen-cpp/ImpalaInternalService.h"
#include "gen-cpp/ImpalaInternalService_types.h"

using namespace std;
using namespace boost;
using namespace apache::thrift;

namespace impala {

inline uint32_t DataStreamMgr::GetHashValue(
    const TUniqueId& fragment_instance_id, PlanNodeId node_id) {
  uint32_t value = RawValue::GetHashValue(&fragment_instance_id.lo, TYPE_BIGINT, 0);
  value = RawValue::GetHashValue(&fragment_instance_id.hi, TYPE_BIGINT, value);
  value = RawValue::GetHashValue(&node_id, TYPE_INT, value);
  return value;
}

shared_ptr<DataStreamRecvr> DataStreamMgr::CreateRecvr(RuntimeState* state,
    const RowDescriptor& row_desc, const TUniqueId& fragment_instance_id,
    PlanNodeId dest_node_id, int num_senders, int buffer_size, RuntimeProfile* profile,
    bool is_merging) {
  DCHECK(profile != NULL);
  VLOG_FILE << "creating receiver for fragment="
            << fragment_instance_id << ", node=" << dest_node_id;
  shared_ptr<DataStreamRecvr> recvr(
      new DataStreamRecvr(this, state->instance_mem_tracker(), row_desc,
          fragment_instance_id, dest_node_id, num_senders, is_merging, buffer_size,
          profile));
  size_t hash_value = GetHashValue(fragment_instance_id, dest_node_id);
  lock_guard<mutex> l(lock_);
  fragment_stream_set_.insert(make_pair(fragment_instance_id, dest_node_id));
  receiver_map_.insert(make_pair(hash_value, recvr));
  return recvr;
}

shared_ptr<DataStreamRecvr> DataStreamMgr::FindRecvr(
    const TUniqueId& fragment_instance_id, PlanNodeId node_id, bool acquire_lock) {
  VLOG_ROW << "looking up fragment_instance_id=" << fragment_instance_id
           << ", node=" << node_id;
  size_t hash_value = GetHashValue(fragment_instance_id, node_id);
  if (acquire_lock) lock_.lock();
  pair<StreamMap::iterator, StreamMap::iterator> range =
      receiver_map_.equal_range(hash_value);
  while (range.first != range.second) {
    shared_ptr<DataStreamRecvr> recvr = range.first->second;
    if (recvr->fragment_instance_id() == fragment_instance_id
        && recvr->dest_node_id() == node_id) {
      if (acquire_lock) lock_.unlock();
      return recvr;
    }
    ++range.first;
  }
  if (acquire_lock) lock_.unlock();
  return shared_ptr<DataStreamRecvr>();
}

Status DataStreamMgr::AddData(
    const TUniqueId& fragment_instance_id, PlanNodeId dest_node_id,
    const TRowBatch& thrift_batch, int sender_id) {
  VLOG_ROW << "AddData(): fragment_instance_id=" << fragment_instance_id
           << " node=" << dest_node_id
           << " size=" << RowBatch::GetBatchSize(thrift_batch);
  shared_ptr<DataStreamRecvr> recvr =
      FindRecvr(fragment_instance_id, dest_node_id);
  if (recvr == NULL) {
    // The receiver may remove itself from the receiver map via DeregisterRecvr()
    // at any time without considering the remaining number of senders.
    // As a consequence, FindRecvr() may return an innocuous NULL if a thread
    // calling DeregisterRecvr() beat the thread calling FindRecvr()
    // in acquiring lock_.
    // TODO: Rethink the lifecycle of DataStreamRecvr to distinguish
    // errors from receiver-initiated teardowns.
    return Status::OK;
  }
  recvr->AddBatch(thrift_batch, sender_id);
  return Status::OK;
}

Status DataStreamMgr::CloseSender(const TUniqueId& fragment_instance_id,
    PlanNodeId dest_node_id, int sender_id) {
  VLOG_FILE << "CloseSender(): fragment_instance_id=" << fragment_instance_id
            << ", node=" << dest_node_id;
  shared_ptr<DataStreamRecvr> recvr = FindRecvr(fragment_instance_id, dest_node_id);
  if (recvr == NULL) {
    // The receiver may remove itself from the receiver map via DeregisterRecvr()
    // at any time without considering the remaining number of senders.
    // As a consequence, FindRecvr() may return an innocuous NULL if a thread
    // calling DeregisterRecvr() beat the thread calling FindRecvr()
    // in acquiring lock_.
    // TODO: Rethink the lifecycle of DataStreamRecvr to distinguish
    // errors from receiver-initiated teardowns.
    return Status::OK;
  }
  recvr->RemoveSender(sender_id);
  return Status::OK;
}

Status DataStreamMgr::DeregisterRecvr(
    const TUniqueId& fragment_instance_id, PlanNodeId node_id) {
  VLOG_QUERY << "DeregisterRecvr(): fragment_instance_id=" << fragment_instance_id
             << ", node=" << node_id;
  size_t hash_value = GetHashValue(fragment_instance_id, node_id);
  lock_guard<mutex> l(lock_);
  pair<StreamMap::iterator, StreamMap::iterator> range =
      receiver_map_.equal_range(hash_value);
  while (range.first != range.second) {
    const shared_ptr<DataStreamRecvr>& recvr = range.first->second;
    if (recvr->fragment_instance_id() == fragment_instance_id
        && recvr->dest_node_id() == node_id) {
      // Notify concurrent AddData() requests that the stream has been terminated.
      recvr->CancelStream();
      fragment_stream_set_.erase(make_pair(recvr->fragment_instance_id(),
          recvr->dest_node_id()));
      receiver_map_.erase(range.first);
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
    shared_ptr<DataStreamRecvr> recvr = FindRecvr(i->first, i->second, false);
    if (recvr == NULL) {
      // keep going but at least log it
      stringstream err;
      err << "Cancel(): missing in stream_map: fragment=" << i->first
          << " node=" << i->second;
      LOG(ERROR) << err.str();
    } else {
      recvr->CancelStream();
    }
    ++i;
  }
}

}
