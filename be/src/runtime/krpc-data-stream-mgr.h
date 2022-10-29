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

#include <list>
#include <mutex>
#include <queue>
#include <set>
#include <boost/unordered_map.hpp>
#include <boost/unordered_set.hpp>

#include "common/object-pool.h"
#include "common/status.h"
#include "gen-cpp/Types_types.h" // for TUniqueId
#include "runtime/descriptors.h" // for PlanNodeId
#include "runtime/row-batch.h"
#include "util/metrics-fwd.h"
#include "util/promise.h"
#include "util/runtime-profile.h"
#include "util/thread-pool.h"
#include "util/unique-id-hash.h"

#include "gutil/macros.h"

namespace kudu {
namespace rpc {
class RpcContext;
} // namespace rpc
} // namespace kudu

namespace impala {

class DescriptorTbl;
class EndDataStreamRequestPB;
class EndDataStreamResponsePB;
class KrpcDataStreamRecvr;
class RuntimeState;
class TransmitDataRequestPB;
class TransmitDataResponsePB;

/// TRANSMIT DATA PROTOCOL
/// ----------------------
///
/// Impala daemons send tuple data between themselves using a transmission protocol that
/// is managed by DataStreamMgr and related classes. Batches of tuples are sent between
/// fragment instances using the TransmitData() RPC; The data transmitted are usually sent
/// in batches across multiple RPCs. The logical connection between a pair of client and
/// server is known as a 'channel'. Clients and servers are referred to as 'senders' and
/// 'receivers' and are implemented by DataStreamSender and DataStreamRecvr respectively.
/// Please note that the number of senders and number of receivers in a stream aren't
/// necessarily the same. We refer to the on-going transmissions between m senders and n
/// receivers as an 'm:n data stream'.
///
/// DataStreamMgr is a singleton class that lives for a long as the Impala process, and
/// manages all streams for all queries. DataStreamRecvr and DataStreamSender have
/// lifetimes tied to their containing fragment instances.
///
/// The protocol proceeds in three phases.
///
/// Phase 1: Channel establishment
/// ------------------------------
///
/// In the first phase the sender initiates a channel with a receiver by sending its
/// first batch. Since the sender may start sending before the receiver is ready, the data
/// stream manager waits until the receiver has finished initialization and then passes
/// the sender's request to the receiver. If the receiver does not appear within a
/// configurable timeout, the data stream manager notifies the sender directly by
/// returning DATASTREAM_SENDER_TIMEOUT. Note that a sender may have multiple channels,
/// each of which needs to be initialized with the corresponding receiver.
///
/// The sender does not distinguish this phase from the steady-state data transmission
/// phase, so may time-out etc. as described below.
///
/// Phase 2: Data transmission
/// --------------------------
///
/// After the first batch has been received, a sender continues to send batches, one at
/// a time (so only one TransmitData() RPC per sender is pending completion at any one
/// time). The rate of transmission is controlled by the receiver: a sender will only
/// schedule batch transmission when the previous transmission completes successfully.
/// When a batch is received, a receiver will do one of two things: (1) deserializes it
/// immediately and adds it to 'batch queue' or (2) defers the deserialization and respond
/// to the RPC later if the batch queue is full. In the second case, the RPC state is
/// saved into the receiver's 'deferred_rpcs_' queue. When space becomes available in the
/// batch queue, the longest-waiting RPC is removed from the 'deferred_rpcs_' queue and
/// the row batch is deserialized. In both cases, the RPC is replied to when the batch
/// has been deserialized and added to the batch queue. The sender will then send its
/// next batch.
///
/// Phase 3: End of stream
/// ----------------------
///
/// When the stream is terminated, clients will send EndDataStream() RPCs to the servers.
/// This RPC will not be sent until after the final TransmitData() RPC has completed and
/// the stream's contents has been delivered. After EndDataStream() is received, no more
/// TransmitData() RPCs should be expected from this sender.
///
/// Exceptional conditions: cancellation, timeouts, failure
/// -------------------------------------------------------
///
/// The protocol must deal with the following complications: asynchronous cancellation of
/// either the receiver or sender, timeouts during RPC transmission, and failure of either
/// the receiver or sender.
///
/// 1. Cancellation
///
/// If the receiver is cancelled (or closed for any other reason, like reaching a limit)
/// before the sender has completed the stream it will be torn down immediately. Any
/// incomplete senders may not be aware of this, and will continue to send batches. The
/// data stream manager on the receiver keeps a record of recently completed receivers so
/// that it may intercept the 'late' data transmissions and immediately reject them with
/// an error that signals the sender should terminate. The record is removed after a
/// certain period of time.
///
/// It's possible for the closed receiver record to be removed before all senders have
/// completed. It is usual that the coordinator will initiate cancellation (e.g. the
/// query is unregistered after initial result rows are fetched once the limit is hit).
/// before the timeout period expires so the sender will be cancelled already. However,
/// it can also occur that the query may not complete before the timeout has elapsed.
/// A sender which sends a row batch after the timeout has elapsed may hit time-out and
/// fail the query. This problem is being tracked in IMPALA-3990.
///
/// The sender RPCs are sent asynchronously to the main thread of fragment instance
/// execution. Senders do not block in TransmitData() RPCs, and may be cancelled at any
/// time. If an in-flight RPC is cancelled at the sender side, the reply from the receiver
/// will be silently dropped by the RPC layer.
///
/// 2. Timeouts during RPC transmission
///
/// Since RPCs may be arbitrarily delayed in the pending sender queue, the TransmitData()
/// RPC has no RPC-level timeout. Instead, the receiver returns an error to the sender if
/// a timeout occurs during the initial channel establishment phase. Since the
/// TransmitData() RPC is asynchronous from the sender, the sender may continue to check
/// for cancellation while it is waiting for a response from the receiver.
///
/// 3. Node or instance failure
///
/// If the receiver node fails, RPCs will fail fast and the sending fragment instance will
/// be cancelled.
///
/// If a sender node fails, or the receiver node hangs, the coordinator should detect the
/// failure and cancel all fragments.
///
/// TODO: Fix IMPALA-3990: use non-timed based approach for removing the closed stream
/// receiver.
///

/// Context for a TransmitData() RPC. This structure is constructed when the processing of
/// a RPC is deferred because the receiver isn't prepared or the 'batch_queue' is full.
struct TransmitDataCtx {
  /// Request data structure, memory owned by 'rpc_context'. This contains various info
  /// such as the destination finst ID, plan node ID and the row batch header.
  const TransmitDataRequestPB* request;

  /// Response data structure, will be serialized back to client after 'rpc_context' is
  /// responded to.
  TransmitDataResponsePB* response;

  /// RpcContext owns the memory of all data structures related to the incoming RPC call
  /// such as the serialized request buffer, response buffer and any sidecars. Must be
  /// responded to once this RPC is finished with. RpcContext will delete itself once it
  /// has been responded to. Not owned.
  kudu::rpc::RpcContext* rpc_context;

  TransmitDataCtx(const TransmitDataRequestPB* request, TransmitDataResponsePB* response,
      kudu::rpc::RpcContext* rpc_context)
    : request(request), response(response), rpc_context(rpc_context) { }
};

/// Context for an EndDataStream() RPC. This structure is constructed when the RPC is
/// queued by the data stream manager for deferred processing when the receiver isn't
/// prepared.
struct EndDataStreamCtx {
  /// Request data structure, memory owned by 'rpc_context'.
  const EndDataStreamRequestPB* request;

  /// Response data structure, will be serialized back to client after 'rpc_context' is
  /// responded to. Memory owned by 'rpc_context'.
  EndDataStreamResponsePB* response;

  /// Must be responded to once this RPC is finished with. RpcContext will delete itself
  /// once it has been responded to. Not owned.
  kudu::rpc::RpcContext* rpc_context;

  EndDataStreamCtx(const EndDataStreamRequestPB* request,
      EndDataStreamResponsePB* response, kudu::rpc::RpcContext* rpc_context)
    : request(request), response(response), rpc_context(rpc_context) { }
};

/// Singleton class which manages all incoming data streams at a backend node.
/// It provides both producer and consumer functionality for each data stream.
///
/// - RPC service threads use this to add incoming data to streams in response to
///   TransmitData() RPCs (AddData()) or to signal end-of-stream conditions
///   (CloseSender()).
/// - Exchange nodes extract data from an incoming stream via a KrpcDataStreamRecvr,
///   which is created with CreateRecvr().
//
/// DataStreamMgr also allows asynchronous cancellation of streams via Cancel()
/// which unblocks all KrpcDataStreamRecvr::GetBatch() calls that are made on behalf
/// of the cancelled query id.
///
/// Exposes three metrics:
///  'senders-blocked-on-recvr-creation' - currently blocked senders.
///  'total-senders-blocked-on-recvr-creation' - total number of blocked senders over
///  time.
///  'total-senders-timedout-waiting-for-recvr-creation' - total number of senders that
///  timed-out while waiting for a receiver.
class KrpcDataStreamMgr : public CacheLineAligned {
 public:
  KrpcDataStreamMgr(MetricGroup* metrics);

  /// Initializes the deserialization thread pool and creates the maintenance thread.
  /// 'service_mem_tracker' is the DataStreamService's MemTracker for tracking memory
  /// used for RPC payloads before being handed over to data stream manager / receiver.
  /// Return error status on failure. Return OK otherwise.
  Status Init(MemTracker* service_mem_tracker);

  /// Create a receiver for a specific fragment_instance_id/dest_node_id.
  /// If is_merging is true, the receiver maintains a separate queue of incoming row
  /// batches for each sender and merges the sorted streams from each sender into a
  /// single stream. 'parent_tracker' is the MemTracker of the exchange node which owns
  /// this receiver. It's the parent of the MemTracker of the newly created receiver.
  /// Ownership of the receiver is shared between this DataStream mgr instance and the
  /// caller. 'client' is the BufferPool's client handle for allocating buffers.
  /// It's owned by the parent exchange node.
  std::shared_ptr<KrpcDataStreamRecvr> CreateRecvr(const RowDescriptor* row_desc,
      const RuntimeState& runtime_state, const TUniqueId& fragment_instance_id,
      PlanNodeId dest_node_id, int num_senders, int64_t buffer_size, bool is_merging,
      RuntimeProfile* profile, MemTracker* parent_tracker,
      BufferPool::ClientHandle* client);

  /// Handler for TransmitData() RPC.
  ///
  /// Adds the serialized row batch pointed to by 'request' and 'rpc_context' to the
  /// receiver identified by the fragment instance id, dest node id and sender id
  /// specified in 'request'. If the receiver is not yet ready, the processing of
  /// 'request' is deferred until the recvr is ready, or is timed out. If the receiver
  /// has already been torn-down (within the last STREAM_EXPIRATION_TIME_MS), the RPC
  /// will be responded to immediately. Otherwise, the sender will be responded to when
  /// time out occurs.
  ///
  /// 'response' is the reply to the caller and the status for deserializing the row batch
  /// should be added to it; 'rpc_context' holds the payload of the incoming RPC calls.
  /// It owns the memory pointed to by 'request','response' and the RPC sidecars. The
  /// request together with the RPC sidecars make up the serialized row batch.
  ///
  /// If the stream would exceed its buffering limit as a result of queuing this batch,
  /// the batch is deferred for processing later by the deserialization thread pool.
  ///
  /// The RPC may not be responded to by the time this function returns if the processing
  /// is deferred.
  ///
  /// TODO: enforce per-sender quotas (something like 200% of buffer_size/#senders),
  /// so that a single sender can't flood the buffer and stall everybody else.
  void AddData(const TransmitDataRequestPB* request, TransmitDataResponsePB* response,
      kudu::rpc::RpcContext* rpc_context);

  /// Handler for EndDataStream() RPC.
  ///
  /// Notifies the receiver associated with the fragment/dest_node id that the specified
  /// sender has closed. The RPC will be responded to if the receiver is found.
  /// Otherwise, the request will be queued in the early senders list and responded
  /// to either when the receiver is created when the request has timed out.
  void CloseSender(const EndDataStreamRequestPB* request,
      EndDataStreamResponsePB* response, kudu::rpc::RpcContext* context);

  /// Cancels all receivers registered for 'query_id' immediately. The receivers will not
  /// accept any row batches after being cancelled. Any buffered row batches will not be
  /// freed until Close() is called on the receivers.
  void Cancel(const TUniqueId& query_id);

  /// Waits for maintenance thread and sender response thread pool to finish.
  ~KrpcDataStreamMgr();

 private:
  friend class KrpcDataStreamRecvr;
  friend class DataStreamTest;

  /// MemTracker for memory used for early transmit data RPCs which arrive before the
  /// receiver is created. The memory of the RPC payload is transferred to the receiver
  /// once it's created.
  std::unique_ptr<MemTracker> early_rpcs_tracker_;

  /// MemTracker used by the DataStreamService to track memory for incoming requests.
  /// Memory for new incoming requests is initially tracked against this tracker before
  /// the requests are handed over to the data stream manager / receiver. It is the
  /// responsibility of data stream manager or receiver to release memory from the
  /// service's tracker and track it in their own trackers. Not owned.
  MemTracker* service_mem_tracker_ = nullptr;

  /// A task for the deserialization threads to work on. The fields identify
  /// the target receiver's sender queue.
  struct DeserializeTask {
    /// The receiver's fragment instance id.
    TUniqueId finst_id;

    /// The plan node id of the exchange node owning the receiver.
    PlanNodeId dest_node_id;

    /// Sender id used for identifying the sender queue for merging exchange.
    int sender_id;
  };

  /// Set of threads which deserialize buffered row batches, and deliver them to their
  /// receivers. Used only if RPCs were deferred when their channel's batch queue was
  /// full or if the receiver was not yet prepared.
  ThreadPool<DeserializeTask> deserialize_pool_;

  /// Periodically, respond to all senders that have waited for too long for their
  /// receivers to show up.
  std::unique_ptr<Thread> maintenance_thread_;

  /// Used to notify maintenance_thread_ that it should exit.
  Promise<bool> shutdown_promise_;

  /// Current number of senders waiting for a receiver to register
  IntGauge* num_senders_waiting_;

  /// Total number of senders that have ever waited for a receiver to register
  IntCounter* total_senders_waited_;

  /// Total number of senders that timed-out waiting for a receiver to register
  IntCounter* num_senders_timedout_;

  /// protects all fields below
  std::mutex lock_;

  /// Map from hash value of fragment instance id/node id pair to stream receivers;
  /// Ownership of the stream revcr is shared between this instance and the caller of
  /// CreateRecvr().
  /// we don't want to create a map<pair<TUniqueId, PlanNodeId>, KrpcDataStreamRecvr*>,
  /// because that requires a bunch of copying of ids for lookup
  typedef
      boost::unordered_multimap<uint32_t, std::shared_ptr<KrpcDataStreamRecvr>> RecvrMap;
  RecvrMap receiver_map_;

  /// (Fragment instance id, Plan node id) pair that uniquely identifies a stream.
  typedef std::pair<impala::TUniqueId, PlanNodeId> RecvrId;

  /// Less-than ordering for RecvrIds.
  /// This ordering clusters all receivers for the same query together, because
  /// the fragment instance ID is the query ID with the lower bits set to the
  /// index of the fragment instance within the query.
  struct ComparisonOp {
    bool operator()(const RecvrId& a, const RecvrId& b) const {
      if (a.first.hi < b.first.hi) {
        return true;
      } else if (a.first.hi > b.first.hi) {
        return false;
      } else if (a.first.lo < b.first.lo) {
        return true;
      } else if (a.first.lo > b.first.lo) {
        return false;
      }
      return a.second < b.second;
    }
  };

  /// An ordered set of receiver IDs so that we can easily find all receiver IDs belonging
  /// to a query (by calling std::set::lower_bound(query_id, 0) to find the
  /// first entry and iterating until the entry's finst_id doesn't belong to the query).
  ///
  /// There is one entry in fragment_recvr_set_ for every entry in receiver_map_.
  typedef std::set<RecvrId, ComparisonOp> FragmentRecvrSet;
  FragmentRecvrSet fragment_recvr_set_;

  /// List of waiting senders that need to be processed when a receiver is created.
  /// Access is only thread-safe when lock_ is held.
  struct EarlySendersList {
    /// Queue of contexts for senders which called AddData() before the receiver was
    /// set up.
    std::vector<std::unique_ptr<TransmitDataCtx>> waiting_sender_ctxs;

    /// Queue of contexts for senders that called EndDataStream() before the receiver was
    /// set up.
    std::vector<std::unique_ptr<EndDataStreamCtx>> closed_sender_ctxs;

    /// Monotonic time of arrival of the first sender in ms. Used to notify senders when
    /// they have waited too long.
    int64_t arrival_time;

    EarlySendersList() : arrival_time(MonotonicMillis()) { }

    /// Defining the move constructor as vectors of unique_ptr are not copyable.
    EarlySendersList(EarlySendersList&& other)
      : waiting_sender_ctxs(move(other.waiting_sender_ctxs)),
        closed_sender_ctxs(move(other.closed_sender_ctxs)),
        arrival_time(other.arrival_time) { }

    /// Defining the move operator= as vectors of unique_ptr are not copyable.
    EarlySendersList& operator=(EarlySendersList&& other) {
      waiting_sender_ctxs = move(other.waiting_sender_ctxs);
      closed_sender_ctxs = move(other.closed_sender_ctxs);
      arrival_time = other.arrival_time;
      return *this;
    }

    DISALLOW_COPY_AND_ASSIGN(EarlySendersList);
  };

  /// Map from stream (which identifies a receiver) to a list of senders that should be
  /// processed when that receiver arrives.
  ///
  /// Entries are removed from early_senders_map_ when either a) a receiver is created
  /// or b) the Maintenance() thread detects that the longest-waiting sender has been
  /// waiting for more than FLAGS_datastream_sender_timeout_ms.
  typedef boost::unordered_map<RecvrId, EarlySendersList> EarlySendersMap;
  EarlySendersMap early_senders_map_;

  /// Map from monotonic time, in ms, that a stream should be evicted from
  /// closed_stream_cache to its RecvrId. Used to evict old streams from cache
  /// efficiently. Using multimap as there may be multiple streams with the same
  /// eviction time.
  typedef std::multimap<int64_t, RecvrId> ClosedStreamMap;
  ClosedStreamMap closed_stream_expirations_;

  /// Cache of recently closed RecvrIds. Used to allow straggling senders to fail fast by
  /// checking this cache, rather than waiting for the missed-receiver timeout to elapse.
  boost::unordered_set<RecvrId> closed_stream_cache_;

  /// Adds a request of TransmitData() RPC to the early senders list. Used for storing
  /// TransmitData() RPC requests which arrive before the receiver finishes preparing.
  void AddEarlySender(const TUniqueId& fragment_instance_id,
      const TransmitDataRequestPB* request, TransmitDataResponsePB* response,
      kudu::rpc::RpcContext* context);

  /// Adds a request of EndDataStream() RPC to the early senders list. Used for storing
  /// EndDataStream() RPC requests which arrive before the receiver finishes preparing.
  void AddEarlyClosedSender(const TUniqueId& fragment_instance_id,
      const EndDataStreamRequestPB* request, EndDataStreamResponsePB* response,
      kudu::rpc::RpcContext* context);

  /// Enqueue 'num_requests' requests to the deserialization thread pool to drain the
  /// deferred RPCs for the receiver with fragment instance id of 'finst_id', plan node
  /// id of 'dest_node_id'. 'sender_id' identifies the sender queue if the receiver
  /// belongs to a merging exchange node. This may block so no lock should be held when
  /// calling this function.
  void EnqueueDeserializeTask(const TUniqueId& finst_id, PlanNodeId dest_node_id,
      int sender_id, int num_requests);

  /// Worker function for deserializing a deferred RPC request stored in task.
  /// Called from the deserialization thread.
  void DeserializeThreadFn(int thread_id, const DeserializeTask& task);

  /// Return a shared_ptr to the receiver for given fragment_instance_id/dest_node_id, or
  /// an empty shared_ptr if not found. Must be called with lock_ already held. If the
  /// stream was recently closed, sets *already_unregistered to true to indicate to caller
  /// that stream will not be available in the future. In that case, the returned
  /// shared_ptr will be empty.
  std::shared_ptr<KrpcDataStreamRecvr> FindRecvr(const TUniqueId& fragment_instance_id,
      PlanNodeId dest_node_id, bool* already_unregistered);

  /// Remove receiver for fragment_instance_id/dest_node_id from the map. Will also
  /// cancel all the sender queues of the receiver.
  Status DeregisterRecvr(const TUniqueId& fragment_instance_id, PlanNodeId dest_node_id);

  /// Returned a hash value generated from the fragment instance id and dest node id.
  /// The hash value is the key in the 'receiver_map_' for the receiver of
  /// fragment_instance_id/dest_node_id.
  uint32_t GetHashValue(const TUniqueId& fragment_instance_id, PlanNodeId dest_node_id);

  /// Responds to a sender when a RPC request has timed out waiting for the receiver to
  /// show up. 'ctx' is the encapsulated RPC request context (e.g. TransmitDataCtx).
  template<typename ContextType, typename RequestPBType>
  void RespondToTimedOutSender(const std::unique_ptr<ContextType>& ctx);

  /// Notifies any sender that has been waiting for its receiver for more than
  /// FLAGS_datastream_sender_timeout_ms.
  ///
  /// Run by maintenance_thread_.
  void Maintenance();
};

} // namespace impala
