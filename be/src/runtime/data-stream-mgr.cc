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

#include "runtime/data-stream-mgr.h"

#include <iostream>
#include <boost/functional/hash.hpp>
#include <boost/thread/locks.hpp>
#include <boost/thread/thread.hpp>

#include "runtime/row-batch.h"
#include "runtime/data-stream-recvr.h"
#include "runtime/raw-value.inline.h"
#include "runtime/runtime-state.h"
#include "util/debug-util.h"
#include "util/periodic-counter-updater.h"
#include "util/runtime-profile-counters.h"
#include "util/uid-util.h"

#include "gen-cpp/ImpalaInternalService.h"
#include "gen-cpp/ImpalaInternalService_types.h"

#include "common/names.h"

using namespace apache::thrift;
using std::boolalpha;

DEFINE_int32(datastream_sender_timeout_ms, 120000, "(Advanced) The time, in ms, that can "
    "elapse  before a plan fragment will time-out trying to send the initial row batch.");

/// This parameter controls the minimum amount of time a closed stream ID will stay in
/// closed_stream_cache_ before it is evicted. It needs to be set sufficiently high that
/// it will outlive all the calls to FindRecvrOrWait() for that stream ID, to distinguish
/// between was-here-but-now-gone and never-here states for the receiver. If the stream ID
/// expires before a call to FindRecvrOrWait(), the sender will see an error which will
/// lead to query cancellation. Setting this value higher will increase the size of the
/// stream cache (which is roughly 48 bytes per receiver).
/// TODO: We don't need millisecond precision here.
const int32_t STREAM_EXPIRATION_TIME_MS = 300 * 1000;

namespace impala {

DataStreamMgr::DataStreamMgr(MetricGroup* metrics) {
  metrics_ = metrics->GetOrCreateChildGroup("datastream-manager");
  num_senders_waiting_ =
      metrics_->AddGauge("senders-blocked-on-recvr-creation", 0L);
  total_senders_waited_ =
      metrics_->AddCounter("total-senders-blocked-on-recvr-creation", 0L);
  num_senders_timedout_ = metrics_->AddCounter(
      "total-senders-timedout-waiting-for-recvr-creation", 0L);
}

DataStreamMgr::~DataStreamMgr() {
}

inline uint32_t DataStreamMgr::GetHashValue(
    const TUniqueId& fragment_instance_id, PlanNodeId node_id) {
  uint32_t value = RawValue::GetHashValue(&fragment_instance_id.lo, TYPE_BIGINT, 0);
  value = RawValue::GetHashValue(&fragment_instance_id.hi, TYPE_BIGINT, value);
  value = RawValue::GetHashValue(&node_id, TYPE_INT, value);
  return value;
}

shared_ptr<DataStreamRecvrBase> DataStreamMgr::CreateRecvr(const RowDescriptor* row_desc,
    const TUniqueId& fragment_instance_id, PlanNodeId dest_node_id, int num_senders,
    int64_t buffer_size, bool is_merging, RuntimeProfile* profile,
    MemTracker* parent_tracker, BufferPool::ClientHandle* client) {
  DCHECK(profile != nullptr);
  DCHECK(parent_tracker != nullptr);
  VLOG_FILE << "creating receiver for fragment_instance_id="
            << fragment_instance_id << ", node=" << dest_node_id;
  shared_ptr<DataStreamRecvr> recvr(new DataStreamRecvr(this, parent_tracker, row_desc,
      fragment_instance_id, dest_node_id, num_senders, is_merging, buffer_size, profile));
  size_t hash_value = GetHashValue(fragment_instance_id, dest_node_id);
  lock_guard<mutex> l(lock_);
  fragment_recvr_set_.insert(make_pair(fragment_instance_id, dest_node_id));
  receiver_map_.insert(make_pair(hash_value, recvr));

  RendezvousMap::iterator it =
      pending_rendezvous_.find(make_pair(fragment_instance_id, dest_node_id));
  if (it != pending_rendezvous_.end()) it->second.promise->Set(recvr);

  return recvr;
}

shared_ptr<DataStreamRecvr> DataStreamMgr::FindRecvrOrWait(
    const TUniqueId& fragment_instance_id, PlanNodeId node_id,
    bool* already_unregistered) {
  RendezvousPromise* promise = NULL;
  RecvrId promise_key = make_pair(fragment_instance_id, node_id);
  *already_unregistered = false;
  {
    lock_guard<mutex> l(lock_);
    if (closed_stream_cache_.find(promise_key) != closed_stream_cache_.end()) {
      *already_unregistered = true;
      return shared_ptr<DataStreamRecvr>();
    }
    shared_ptr<DataStreamRecvr> rcvr = FindRecvr(fragment_instance_id, node_id, false);
    if (rcvr.get() != NULL) return rcvr;
    // Find the rendezvous, creating a new one if one does not already exist.
    RefCountedPromise* ref_counted_promise = &pending_rendezvous_[promise_key];
    promise = ref_counted_promise->promise;
    ref_counted_promise->IncRefCount();
  }
  bool timed_out = false;
  MonotonicStopWatch sw;
  sw.Start();
  num_senders_waiting_->Increment(1L);
  total_senders_waited_->Increment(1L);
  shared_ptr<DataStreamRecvr> rcvr =
      promise->Get(FLAGS_datastream_sender_timeout_ms, &timed_out);
  num_senders_waiting_->Increment(-1L);
  const string& time_taken = PrettyPrinter::Print(sw.ElapsedTime(), TUnit::TIME_NS);
  if (timed_out) {
    LOG(INFO) << "Datastream sender timed-out waiting for recvr for fragment_instance_id="
              << fragment_instance_id << " (time-out was: " << time_taken << "). "
              << "Increase --datastream_sender_timeout_ms if you see this message "
              << "frequently.";
  } else {
    VLOG_RPC << "Datastream sender waited for " << time_taken
             << ", and did not time-out.";
  }
  if (timed_out) num_senders_timedout_->Increment(1L);
  {
    lock_guard<mutex> l(lock_);
    // If we are the last to leave, remove the rendezvous from the pending map. Any new
    // incoming senders will add a new entry to the map themselves.
    if (pending_rendezvous_[promise_key].DecRefCount() == 0) {
      pending_rendezvous_.erase(promise_key);
    }
  }
  return rcvr;
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
           << " size=" << RowBatch::GetDeserializedSize(thrift_batch);
  bool already_unregistered;
  shared_ptr<DataStreamRecvr> recvr = FindRecvrOrWait(fragment_instance_id, dest_node_id,
      &already_unregistered);
  if (recvr.get() == NULL) {
    // The receiver may remove itself from the receiver map via DeregisterRecvr() at any
    // time without considering the remaining number of senders.  As a consequence,
    // FindRecvrOrWait() may return NULL if a thread calling DeregisterRecvr() beat the
    // thread calling FindRecvr() in acquiring lock_. We detect this case by checking
    // already_unregistered - if true then the receiver was already closed deliberately,
    // and there's no unexpected error here. If already_unregistered is false,
    // FindRecvrOrWait() timed out, which is unexpected and suggests a query setup error;
    // we return DATASTREAM_SENDER_TIMEOUT to trigger tear-down of the query.
    if (already_unregistered) return Status::OK();
    ErrorMsg msg(TErrorCode::DATASTREAM_SENDER_TIMEOUT, "", PrintId(fragment_instance_id),
        dest_node_id);
    VLOG_QUERY << "DataStreamMgr::AddData(): " << msg.msg();
    return Status::Expected(msg);
  }
  DCHECK(!already_unregistered);
  recvr->AddBatch(thrift_batch, sender_id);
  return Status::OK();
}

Status DataStreamMgr::CloseSender(const TUniqueId& fragment_instance_id,
    PlanNodeId dest_node_id, int sender_id) {
  VLOG_FILE << "CloseSender(): fragment_instance_id=" << fragment_instance_id
            << ", node=" << dest_node_id;
  Status status;
  bool already_unregistered;
  shared_ptr<DataStreamRecvr> recvr = FindRecvrOrWait(fragment_instance_id, dest_node_id,
      &already_unregistered);
  if (recvr == nullptr) {
    if (already_unregistered) {
      status = Status::OK();
    } else {
      // Was not able to notify the receiver that this was the end of stream. Notify the
      // sender that this failed so that they can take appropriate action (i.e. failing
      // the query).
      ErrorMsg msg(TErrorCode::DATASTREAM_SENDER_TIMEOUT, "",
          PrintId(fragment_instance_id), dest_node_id);
      VLOG_QUERY << "DataStreamMgr::CloseSender(): " << msg.msg();
      status = Status::Expected(msg);
    }
  } else {
    recvr->RemoveSender(sender_id);
  }

  {
    // Remove any closed streams that have been in the cache for more than
    // STREAM_EXPIRATION_TIME_MS.
    lock_guard<mutex> l(lock_);
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
  return status;
}

Status DataStreamMgr::DeregisterRecvr(
    const TUniqueId& fragment_instance_id, PlanNodeId node_id) {
  VLOG_QUERY << "DeregisterRecvr(): fragment_instance_id=" << fragment_instance_id
             << ", node=" << node_id;
  size_t hash_value = GetHashValue(fragment_instance_id, node_id);
  lock_guard<mutex> l(lock_);
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
  VLOG_QUERY << "cancelling all streams for fragment_instance_id="
             << fragment_instance_id;
  lock_guard<mutex> l(lock_);
  FragmentRecvrSet::iterator i =
      fragment_recvr_set_.lower_bound(make_pair(fragment_instance_id, 0));
  while (i != fragment_recvr_set_.end() && i->first == fragment_instance_id) {
    shared_ptr<DataStreamRecvr> recvr = FindRecvr(i->first, i->second, false);
    if (recvr.get() == NULL) {
      // keep going but at least log it
      stringstream err;
      err << "Cancel(): missing in stream_map: fragment_instance_id=" << i->first
          << " node=" << i->second;
      LOG(ERROR) << err.str();
    } else {
      recvr->CancelStream();
    }
    ++i;
  }
}

}
