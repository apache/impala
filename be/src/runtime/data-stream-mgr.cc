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
#include "util/uid-util.h"

#include "gen-cpp/ImpalaInternalService.h"
#include "gen-cpp/ImpalaInternalService_types.h"

#include "common/names.h"

using namespace apache::thrift;
using std::boolalpha;

DEFINE_int32(datastream_timeout_ms, 60000, "(Advanced) The time, in ms, that can elapse"
    " before a plan fragment will time-out trying to send the initial row batch.");

const int32_t STREAM_EXPIRATION_TIME_MS = 30 * 1000;

namespace impala {

DataStreamMgr::DataStreamMgr(MetricGroup* metrics) {
  metrics_ = metrics->GetChildGroup("datastream-manager");
  num_senders_waiting_ =
      metrics_->AddGauge<int64_t>("senders-blocked-on-recvr-creation", 0L);
  total_senders_waited_ =
      metrics_->AddCounter<int64_t>("total-senders-blocked-on-recvr-creation", 0L);
  num_senders_timedout_ = metrics_->AddCounter<int64_t>(
      "total-senders-timedout-waiting-for-recvr-creation", 0L);
}

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
  lock_guard<SpinLock> l(lock_);
  fragment_recvr_set_.insert(make_pair(fragment_instance_id, dest_node_id));
  receiver_map_.insert(make_pair(hash_value, recvr));

  RendezvousMap::iterator it =
      pending_rendezvous_.find(make_pair(fragment_instance_id, dest_node_id));
  if (it != pending_rendezvous_.end()) it->second.promise->Set(recvr);

  return recvr;
}

shared_ptr<DataStreamRecvr> DataStreamMgr::FindRecvrOrWait(
    const TUniqueId& fragment_instance_id, PlanNodeId node_id) {
  RefCountedPromise* promise = NULL;
  RecvrId promise_key = make_pair(fragment_instance_id, node_id);
  {
    lock_guard<SpinLock> l(lock_);
    if (closed_stream_cache_.find(promise_key) != closed_stream_cache_.end()) {
      return shared_ptr<DataStreamRecvr>();
    }
    shared_ptr<DataStreamRecvr> rcvr = FindRecvr(fragment_instance_id, node_id, false);
    if (rcvr.get() != NULL) return rcvr;
    // Find the rendezvous, creating a new one if one does not already exist.
    promise = &pending_rendezvous_[promise_key];
    promise->IncRefCount();
  }
  bool timed_out = false;
  MonotonicStopWatch sw;
  sw.Start();
  num_senders_waiting_->Increment(1L);
  total_senders_waited_->Increment(1L);
  shared_ptr<DataStreamRecvr> recvr =
      promise->promise->Get(FLAGS_datastream_timeout_ms, &timed_out);
  num_senders_waiting_->Increment(-1L);
  const string& time_taken = PrettyPrinter::Print(sw.ElapsedTime(), TUnit::TIME_NS);
  if (timed_out) {
    LOG(INFO) << "Datastream sender timed-out waiting for recvr for fragment instance: "
              << fragment_instance_id << " (time-out was: " << time_taken << "). "
              << "If query was cancelled, this is not an error.";
  } else {
    VLOG_RPC << "Datastream sender waited for " << time_taken
             << ", and did not time-out.";
  }
  if (timed_out) num_senders_timedout_->Increment(1L);
  {
    lock_guard<SpinLock> l(lock_);
    // If we are the last to leave, remove the rendezvous from the pending map. Any new
    // incoming senders will add a new entry to the map themselves.
    if (promise->DecRefCount() == 0) pending_rendezvous_.erase(promise_key);
  }
  return recvr;
}

shared_ptr<DataStreamRecvr> DataStreamMgr::FindRecvr(
    const TUniqueId& fragment_instance_id, PlanNodeId node_id, bool acquire_lock) {
  VLOG_ROW << "looking up fragment_instance_id=" << fragment_instance_id
           << ", node=" << node_id;
  size_t hash_value = GetHashValue(fragment_instance_id, node_id);
  if (acquire_lock) lock_.lock();
  pair<RecvrMap::iterator, RecvrMap::iterator> range =
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

Status DataStreamMgr::AddData(const TUniqueId& fragment_instance_id,
    PlanNodeId dest_node_id, const TRowBatch& thrift_batch, int sender_id) {
  VLOG_ROW << "AddData(): fragment_instance_id=" << fragment_instance_id
           << " node=" << dest_node_id
           << " size=" << RowBatch::GetBatchSize(thrift_batch);
  shared_ptr<DataStreamRecvr> recvr = FindRecvrOrWait(fragment_instance_id, dest_node_id);
  if (recvr.get() == NULL) {
    // The receiver may remove itself from the receiver map via DeregisterRecvr()
    // at any time without considering the remaining number of senders.
    // As a consequence, FindRecvrOrWait() may return an innocuous NULL if a thread
    // calling DeregisterRecvr() beat the thread calling FindRecvr()
    // in acquiring lock_.
    // TODO: Rethink the lifecycle of DataStreamRecvr to distinguish
    // errors from receiver-initiated teardowns.
    return Status::OK();
  }
  recvr->AddBatch(thrift_batch, sender_id);
  return Status::OK();
}

Status DataStreamMgr::CloseSender(const TUniqueId& fragment_instance_id,
    PlanNodeId dest_node_id, int sender_id) {
  VLOG_FILE << "CloseSender(): fragment_instance_id=" << fragment_instance_id
            << ", node=" << dest_node_id;
  shared_ptr<DataStreamRecvr> recvr = FindRecvrOrWait(fragment_instance_id, dest_node_id);
  if (recvr.get() != NULL) {
    // The receiver may remove itself from the receiver map via DeregisterRecvr() at any
    // time without considering the remaining number of senders.  As a consequence,
    // FindRecvrOrWait() may return an innocuous NULL if a thread calling
    // DeregisterRecvr() beat the thread calling FindRecvr() in acquiring lock_.
    // TODO: Rethink the lifecycle of DataStreamRecvr to distinguish errors from
    // receiver-initiated teardowns.
    recvr->RemoveSender(sender_id);
  }

  {
    // Remove any closed streams that have been in the cache for more than
    // STREAM_EXPIRATION_TIME_MS.
    lock_guard<SpinLock> l(lock_);
    ClosedStreamMap::iterator it = closed_stream_expirations_.begin();
    int64_t now = MonotonicMillis();
    int32_t before = closed_stream_cache_.size();
    while (it != closed_stream_expirations_.end() && it->first < now) {
      closed_stream_cache_.erase(it->second);
      closed_stream_expirations_.erase(it++);
    }
    DCHECK_EQ(closed_stream_cache_.size(), closed_stream_expirations_.size());
    int32_t after = closed_stream_cache_.size();
    if (before != after) {
      VLOG_QUERY << "Reduced stream ID cache from " << before << " items, to " << after
                 << ", eviction took: "
                 << PrettyPrinter::Print(MonotonicMillis() - now, TUnit::TIME_MS);
    }
  }
  return Status::OK();
}

Status DataStreamMgr::DeregisterRecvr(
    const TUniqueId& fragment_instance_id, PlanNodeId node_id) {
  VLOG_QUERY << "DeregisterRecvr(): fragment_instance_id=" << fragment_instance_id
             << ", node=" << node_id;
  size_t hash_value = GetHashValue(fragment_instance_id, node_id);
  lock_guard<SpinLock> l(lock_);
  pair<RecvrMap::iterator, RecvrMap::iterator> range =
      receiver_map_.equal_range(hash_value);
  while (range.first != range.second) {
    const shared_ptr<DataStreamRecvr>& recvr = range.first->second;
    if (recvr->fragment_instance_id() == fragment_instance_id
        && recvr->dest_node_id() == node_id) {
      // Notify concurrent AddData() requests that the stream has been terminated.
      recvr->CancelStream();
      RecvrId recvr_id =
          make_pair(recvr->fragment_instance_id(), recvr->dest_node_id());
      fragment_recvr_set_.erase(recvr_id);
      receiver_map_.erase(range.first);
      closed_stream_expirations_.insert(
          make_pair(MonotonicMillis() + STREAM_EXPIRATION_TIME_MS, recvr_id));
      closed_stream_cache_.insert(recvr_id);
      return Status::OK();
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
  lock_guard<SpinLock> l(lock_);
  FragmentRecvrSet::iterator i =
      fragment_recvr_set_.lower_bound(make_pair(fragment_instance_id, 0));
  while (i != fragment_recvr_set_.end() && i->first == fragment_instance_id) {
    shared_ptr<DataStreamRecvr> recvr = FindRecvr(i->first, i->second, false);
    if (recvr.get() == NULL) {
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
