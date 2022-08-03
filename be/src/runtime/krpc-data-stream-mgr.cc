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

#include "runtime/krpc-data-stream-mgr.h"

#include <iostream>
#include <mutex>
#include <boost/functional/hash.hpp>

#include "kudu/rpc/rpc_context.h"
#include "kudu/util/net/sockaddr.h"
#include "kudu/util/trace.h"

#include "exec/kudu/kudu-util.h"
#include "runtime/exec-env.h"
#include "runtime/krpc-data-stream-recvr.h"
#include "runtime/mem-tracker.h"
#include "runtime/raw-value.inline.h"
#include "runtime/row-batch.h"
#include "runtime/runtime-state.h"
#include "service/data-stream-service.h"
#include "util/container-util.h"
#include "util/debug-util.h"
#include "util/metrics.h"
#include "util/periodic-counter-updater.h"
#include "util/pretty-printer.h"
#include "util/runtime-profile-counters.h"
#include "util/uid-util.h"

#include "gen-cpp/data_stream_service.pb.h"

#include "common/names.h"

/// This parameter controls the minimum amount of time a closed stream ID will stay in
/// closed_stream_cache_ before it is evicted. It needs to be set sufficiently high that
/// it will outlive all the calls to FindRecvr() for that stream ID, to distinguish
/// between was-here-but-now-gone and never-here states for the receiver. If the stream
/// ID expires before a call to FindRecvr(), the sender will see an error which will lead
/// to query cancellation. Setting this value higher will increase the size of the stream
/// cache (which is roughly 48 bytes per receiver).
/// TODO: We don't need millisecond precision here.
const int32_t STREAM_EXPIRATION_TIME_MS = 300 * 1000;

DEFINE_int32(datastream_sender_timeout_ms, 120000, "(Advanced) The time, in ms, that can "
    "elapse  before a plan fragment will time-out trying to send the initial row batch.");
DEFINE_int32(datastream_service_num_deserialization_threads, 16,
    "Number of threads for deserializing RPC requests deferred due to the receiver "
    "not ready or the soft limit of the receiver is reached.");
DEFINE_int32(datastream_service_deserialization_queue_size, 10000,
    "Number of deferred RPC requests that can be enqueued before being processed by a "
    "deserialization thread.");
using std::mutex;

namespace impala {

KrpcDataStreamMgr::KrpcDataStreamMgr(MetricGroup* metrics)
  : deserialize_pool_("data-stream-mgr", "deserialize",
      FLAGS_datastream_service_num_deserialization_threads,
      FLAGS_datastream_service_deserialization_queue_size,
      boost::bind(&KrpcDataStreamMgr::DeserializeThreadFn, this, _1, _2)) {
  MetricGroup* dsm_metrics = metrics->GetOrCreateChildGroup("datastream-manager");
  num_senders_waiting_ =
      dsm_metrics->AddGauge("senders-blocked-on-recvr-creation", 0L);
  total_senders_waited_ =
      dsm_metrics->AddCounter("total-senders-blocked-on-recvr-creation", 0L);
  num_senders_timedout_ = dsm_metrics->AddCounter(
      "total-senders-timedout-waiting-for-recvr-creation", 0L);
}

Status KrpcDataStreamMgr::Init(MemTracker* service_mem_tracker) {
  // MemTracker for tracking memory used for buffering early RPC calls which
  // arrive before the receiver is ready.
  early_rpcs_tracker_.reset(new MemTracker(-1, "Data Stream Manager Early RPCs",
      ExecEnv::GetInstance()->process_mem_tracker()));
  service_mem_tracker_ = service_mem_tracker;
  RETURN_IF_ERROR(Thread::Create("krpc-data-stream-mgr", "maintenance",
      [this](){ this->Maintenance(); }, &maintenance_thread_));
  RETURN_IF_ERROR(deserialize_pool_.Init());
  return Status::OK();
}

inline uint32_t KrpcDataStreamMgr::GetHashValue(
    const TUniqueId& fragment_instance_id, PlanNodeId dest_node_id) {
  uint32_t value = RawValue::GetHashValue(
      &fragment_instance_id.lo, ColumnType(TYPE_BIGINT), 0);
  value = RawValue::GetHashValue(
      &fragment_instance_id.hi, ColumnType(TYPE_BIGINT), value);
  value = RawValue::GetHashValue(&dest_node_id, ColumnType(TYPE_INT), value);
  return value;
}

shared_ptr<KrpcDataStreamRecvr> KrpcDataStreamMgr::CreateRecvr(
    const RowDescriptor* row_desc, const RuntimeState& runtime_state,
    const TUniqueId& finst_id, PlanNodeId dest_node_id, int num_senders,
    int64_t buffer_size, bool is_merging, RuntimeProfile* profile,
    MemTracker* parent_tracker, BufferPool::ClientHandle* client) {
  DCHECK(profile != nullptr);
  DCHECK(parent_tracker != nullptr);
  DCHECK(client != nullptr);
  VLOG_FILE << "creating receiver for fragment_instance_id="<< PrintId(finst_id)
            << ", node=" << dest_node_id;
  shared_ptr<KrpcDataStreamRecvr> recvr(new KrpcDataStreamRecvr(this, parent_tracker,
      row_desc, runtime_state, finst_id, dest_node_id, num_senders, is_merging,
      buffer_size, profile, client));
  uint32_t hash_value = GetHashValue(finst_id, dest_node_id);
  EarlySendersList early_senders_for_recvr;
  {
    RecvrId recvr_id = make_pair(finst_id, dest_node_id);
    lock_guard<mutex> l(lock_);
    fragment_recvr_set_.insert(recvr_id);
    receiver_map_.insert(make_pair(hash_value, recvr));

    EarlySendersMap::iterator it = early_senders_map_.find(recvr_id);

    if (it != early_senders_map_.end()) {
      // Move the early senders list here so that we can drop 'lock_'. We need to drop
      // the lock before processing the early senders to avoid a deadlock.
      // More details in IMPALA-6346.
      early_senders_for_recvr = std::move(it->second);
      early_senders_map_.erase(it);
    }
  }

  // Let the receiver take over the RPC payloads of early senders and process them
  // asynchronously.
  for (unique_ptr<TransmitDataCtx>& ctx : early_senders_for_recvr.waiting_sender_ctxs) {
    // Release memory. The receiver will track it in its instance tracker.
    int64_t transfer_size = ctx->rpc_context->GetTransferSize();
    recvr->TakeOverEarlySender(move(ctx));
    early_rpcs_tracker_->Release(transfer_size);
    num_senders_waiting_->Increment(-1);
  }
  for (const unique_ptr<EndDataStreamCtx>& ctx :
      early_senders_for_recvr.closed_sender_ctxs) {
    recvr->RemoveSender(ctx->request->sender_id());
    DataStreamService::RespondAndReleaseRpc(Status::OK(), ctx->response, ctx->rpc_context,
        early_rpcs_tracker_.get());
    num_senders_waiting_->Increment(-1);
  }
  return recvr;
}

shared_ptr<KrpcDataStreamRecvr> KrpcDataStreamMgr::FindRecvr(
    const TUniqueId& finst_id, PlanNodeId dest_node_id, bool* already_unregistered) {
  VLOG_ROW << "looking up fragment_instance_id=" << PrintId(finst_id)
           << ", node=" << dest_node_id;
  *already_unregistered = false;
  uint32_t hash_value = GetHashValue(finst_id, dest_node_id);
  pair<RecvrMap::iterator, RecvrMap::iterator> range =
      receiver_map_.equal_range(hash_value);
  while (range.first != range.second) {
    shared_ptr<KrpcDataStreamRecvr> recvr = range.first->second;
    if (recvr->fragment_instance_id() == finst_id &&
        recvr->dest_node_id() == dest_node_id) {
      return recvr;
    }
    ++range.first;
  }
  RecvrId recvr_id = make_pair(finst_id, dest_node_id);
  if (closed_stream_cache_.find(recvr_id) != closed_stream_cache_.end()) {
    *already_unregistered = true;
  }
  return shared_ptr<KrpcDataStreamRecvr>();
}

void KrpcDataStreamMgr::AddEarlySender(const TUniqueId& finst_id,
    const TransmitDataRequestPB* request, TransmitDataResponsePB* response,
    kudu::rpc::RpcContext* rpc_context) {
  const int64_t transfer_size = rpc_context->GetTransferSize();
  early_rpcs_tracker_->Consume(transfer_size);
  service_mem_tracker_->Release(transfer_size);
  RecvrId recvr_id = make_pair(finst_id, request->dest_node_id());
  auto payload = make_unique<TransmitDataCtx>(request, response, rpc_context);
  early_senders_map_[recvr_id].waiting_sender_ctxs.emplace_back(move(payload));
  num_senders_waiting_->Increment(1);
  total_senders_waited_->Increment(1);
}

void KrpcDataStreamMgr::AddEarlyClosedSender(const TUniqueId& finst_id,
    const EndDataStreamRequestPB* request, EndDataStreamResponsePB* response,
    kudu::rpc::RpcContext* rpc_context) {
  const int64_t transfer_size = rpc_context->GetTransferSize();
  early_rpcs_tracker_->Consume(transfer_size);
  service_mem_tracker_->Release(transfer_size);
  RecvrId recvr_id = make_pair(finst_id, request->dest_node_id());
  auto payload = make_unique<EndDataStreamCtx>(request, response, rpc_context);
  early_senders_map_[recvr_id].closed_sender_ctxs.emplace_back(move(payload));
  num_senders_waiting_->Increment(1);
  total_senders_waited_->Increment(1);
}

void KrpcDataStreamMgr::AddData(const TransmitDataRequestPB* request,
    TransmitDataResponsePB* response, kudu::rpc::RpcContext* rpc_context) {
  TUniqueId finst_id;
  finst_id.__set_lo(request->dest_fragment_instance_id().lo());
  finst_id.__set_hi(request->dest_fragment_instance_id().hi());
  TPlanNodeId dest_node_id = request->dest_node_id();
  VLOG_ROW << "AddData(): fragment_instance_id=" << PrintId(finst_id)
           << " node_id=" << request->dest_node_id()
           << " #rows=" << request->row_batch_header().num_rows()
           << " sender_id=" << request->sender_id();
  bool already_unregistered = false;
  shared_ptr<KrpcDataStreamRecvr> recvr;
  {
    lock_guard<mutex> l(lock_);
    recvr = FindRecvr(finst_id, request->dest_node_id(), &already_unregistered);
    // If no receiver is found and it's not in the closed stream cache, best guess is
    // that it is still preparing, so add payload to per-receiver early senders' list.
    // If the receiver doesn't show up after FLAGS_datastream_sender_timeout_ms ms
    // (e.g. if the receiver was closed and has already been retired from the
    // closed_stream_cache_), the sender is timed out by the maintenance thread.
    if (!already_unregistered && recvr == nullptr) {
      AddEarlySender(finst_id, request, response, rpc_context);
      TRACE_TO(rpc_context->trace(), "Added early sender");
      return;
    }
  }
  if (already_unregistered) {
    TRACE_TO(rpc_context->trace(), "Sender already unregistered");
    // The receiver may remove itself from the receiver map via DeregisterRecvr() at any
    // time without considering the remaining number of senders. As a consequence,
    // FindRecvr() may return nullptr even though the receiver was once present. We
    // detect this case by checking already_unregistered - if true then the receiver was
    // already closed deliberately, and there's no unexpected error here.
    ErrorMsg msg(TErrorCode::DATASTREAM_RECVR_CLOSED, PrintId(finst_id), dest_node_id);
    DataStreamService::RespondAndReleaseRpc(Status::Expected(msg), response, rpc_context,
        service_mem_tracker_);
    return;
  }
  DCHECK(recvr != nullptr);
  int64_t transfer_size = rpc_context->GetTransferSize();
  recvr->AddBatch(request, response, rpc_context);
  // Release memory. The receiver already tracks it in its instance tracker.
  service_mem_tracker_->Release(transfer_size);
}

void KrpcDataStreamMgr::EnqueueDeserializeTask(const TUniqueId& finst_id,
    PlanNodeId dest_node_id, int sender_id, int num_requests) {
  for (int i = 0; i < num_requests; ++i) {
    DeserializeTask payload = {finst_id, dest_node_id, sender_id};
    deserialize_pool_.Offer(move(payload));
  }
}

void KrpcDataStreamMgr::DeserializeThreadFn(int thread_id, const DeserializeTask& task) {
  shared_ptr<KrpcDataStreamRecvr> recvr;
  {
    bool already_unregistered;
    lock_guard<mutex> l(lock_);
    recvr = FindRecvr(task.finst_id, task.dest_node_id, &already_unregistered);
    DCHECK(recvr != nullptr || already_unregistered);
  }
  if (recvr != nullptr) recvr->ProcessDeferredRpc(task.sender_id);
}

void KrpcDataStreamMgr::CloseSender(const EndDataStreamRequestPB* request,
    EndDataStreamResponsePB* response, kudu::rpc::RpcContext* rpc_context) {
  TUniqueId finst_id;
  finst_id.__set_lo(request->dest_fragment_instance_id().lo());
  finst_id.__set_hi(request->dest_fragment_instance_id().hi());
  VLOG_ROW << "CloseSender(): fragment_instance_id=" << PrintId(finst_id)
           << " node_id=" << request->dest_node_id()
           << " sender_id=" << request->sender_id();
  shared_ptr<KrpcDataStreamRecvr> recvr;
  {
    lock_guard<mutex> l(lock_);
    bool already_unregistered;
    recvr = FindRecvr(finst_id, request->dest_node_id(), &already_unregistered);
    // If no receiver is found and it's not in the closed stream cache, we still need
    // to make sure that the close operation is performed so add to per-recvr list of
    // pending closes. It's possible for a sender to issue EOS RPC without sending any
    // rows if no rows are materialized at all in the sender side.
    if (!already_unregistered && recvr == nullptr) {
      AddEarlyClosedSender(finst_id, request, response, rpc_context);
      TRACE_TO(rpc_context->trace(), "Added early closed sender");
      return;
    }
  }

  // If we reach this point, either the receiver is found or it has been unregistered
  // already. In either cases, it's safe to just return an OK status.
  TRACE_TO(
      rpc_context->trace(), "Found receiver? $0", recvr != nullptr ? "true" : "false");
  if (LIKELY(recvr != nullptr)) {
    recvr->RemoveSender(request->sender_id());
    TRACE_TO(rpc_context->trace(), "Removed sender from receiver");
  }
  DataStreamService::RespondAndReleaseRpc(Status::OK(), response, rpc_context,
      service_mem_tracker_);
}

Status KrpcDataStreamMgr::DeregisterRecvr(
    const TUniqueId& finst_id, PlanNodeId dest_node_id) {
  VLOG_QUERY << "DeregisterRecvr(): fragment_instance_id=" << PrintId(finst_id)
             << ", node=" << dest_node_id;
  uint32_t hash_value = GetHashValue(finst_id, dest_node_id);
  lock_guard<mutex> l(lock_);
  pair<RecvrMap::iterator, RecvrMap::iterator> range =
      receiver_map_.equal_range(hash_value);
  while (range.first != range.second) {
    const shared_ptr<KrpcDataStreamRecvr>& recvr = range.first->second;
    if (recvr->fragment_instance_id() == finst_id &&
        recvr->dest_node_id() == dest_node_id) {
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

  const string msg = Substitute(
      "Unknown row receiver id: fragment_instance_id=$0, dest_node_id=$1",
      PrintId(finst_id), dest_node_id);
  return Status(msg);
}

void KrpcDataStreamMgr::Cancel(const TUniqueId& query_id) {
  VLOG_QUERY << "cancelling active streams for query_id=" << PrintId(query_id);
  lock_guard<mutex> l(lock_);
  // Fragment instance IDs are the query ID with the lower bits set to the instance
  // index. Therefore all finstances for a query are clustered together, starting
  // after the position in the map where the query_id would be.
  FragmentRecvrSet::iterator iter =
      fragment_recvr_set_.lower_bound(make_pair(query_id, 0));
  while (iter != fragment_recvr_set_.end() &&
         GetQueryId(iter->first) == query_id) {
    bool unused;
    shared_ptr<KrpcDataStreamRecvr> recvr = FindRecvr(iter->first, iter->second, &unused);
    if (recvr != nullptr) {
      recvr->CancelStream();
    } else {
      // keep going but at least log it
      LOG(ERROR) << Substitute(
          "Cancel(): missing in stream_map: fragment_instance_id=$0 node=$1",
              PrintId(iter->first), iter->second);
    }
    ++iter;
  }
}

template<typename ContextType, typename RequestPBType>
void KrpcDataStreamMgr::RespondToTimedOutSender(const std::unique_ptr<ContextType>& ctx) {
  TRACE_TO(ctx->rpc_context->trace(), "Timed out sender");
  const RequestPBType* request = ctx->request;
  TUniqueId finst_id;
  finst_id.__set_lo(request->dest_fragment_instance_id().lo());
  finst_id.__set_hi(request->dest_fragment_instance_id().hi());
  string remote_addr = Substitute(" $0", ctx->rpc_context->remote_address().ToString());
  ErrorMsg msg(TErrorCode::DATASTREAM_SENDER_TIMEOUT, remote_addr, PrintId(finst_id),
      ctx->request->dest_node_id());
  VLOG_QUERY << msg.msg();
  DataStreamService::RespondAndReleaseRpc(Status::Expected(msg), ctx->response,
      ctx->rpc_context, early_rpcs_tracker_.get());
  num_senders_waiting_->Increment(-1);
  num_senders_timedout_->Increment(1);
}

void KrpcDataStreamMgr::Maintenance() {
  const int32_t sleep_time_ms =
      min(max(1, FLAGS_datastream_sender_timeout_ms / 2), 10000);
  while (true) {
    const int64_t now = MonotonicMillis();

    // Notify any senders that have been waiting too long for their receiver to
    // appear. Keep lock_ held for only a short amount of time.
    vector<EarlySendersList> timed_out_senders;
    {
      lock_guard<mutex> l(lock_);
      auto it = early_senders_map_.begin();
      while (it != early_senders_map_.end()) {
        if (now - it->second.arrival_time > FLAGS_datastream_sender_timeout_ms) {
          timed_out_senders.emplace_back(move(it->second));
          it = early_senders_map_.erase(it);
        } else {
          ++it;
        }
      }
    }

    // Send responses to all timed-out senders. We need to propagate the time-out errors
    // to senders which sent EOS RPC so all query fragments will eventually be cancelled.
    // Otherwise, the receiver may hang when it eventually gets created as the timed-out
    // EOS will be lost forever.
    for (const EarlySendersList& senders_queue : timed_out_senders) {
      for (const unique_ptr<TransmitDataCtx>& ctx : senders_queue.waiting_sender_ctxs) {
        RespondToTimedOutSender<TransmitDataCtx, TransmitDataRequestPB>(ctx);
      }
      for (const unique_ptr<EndDataStreamCtx>& ctx : senders_queue.closed_sender_ctxs) {
        RespondToTimedOutSender<EndDataStreamCtx, EndDataStreamRequestPB>(ctx);
      }
    }

    // Remove any closed streams that have been in the cache for more than
    // STREAM_EXPIRATION_TIME_MS.
    {
      lock_guard<mutex> l(lock_);
      ClosedStreamMap::iterator it = closed_stream_expirations_.begin();
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
    bool timed_out = false;
    shutdown_promise_.Get(sleep_time_ms, &timed_out);
    if (!timed_out) return;
  }
}

KrpcDataStreamMgr::~KrpcDataStreamMgr() {
  shutdown_promise_.Set(true);
  deserialize_pool_.Shutdown();
  LOG(INFO) << "Waiting for data-stream-mgr maintenance thread...";
  if (maintenance_thread_.get() != nullptr) maintenance_thread_->Join();
  LOG(INFO) << "Waiting for deserialization thread pool...";
  deserialize_pool_.Join();
}

} // namespace impala
