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


#ifndef IMPALA_RUNTIME_DATA_STREAM_MGR_H
#define IMPALA_RUNTIME_DATA_STREAM_MGR_H

#include <list>
#include <set>
#include <boost/thread/mutex.hpp>
#include <boost/unordered_map.hpp>
#include <boost/unordered_set.hpp>

#include "common/status.h"
#include "common/object-pool.h"
#include "runtime/data-stream-mgr-base.h"
#include "runtime/descriptors.h"  // for PlanNodeId
#include "util/metrics.h"
#include "util/promise.h"
#include "util/runtime-profile.h"
#include "gen-cpp/Types_types.h"  // for TUniqueId

namespace impala {

class DescriptorTbl;
class DataStreamRecvr;
class RowBatch;
class RuntimeState;
class TRowBatch;

/// Singleton class which manages all incoming data streams at a backend node. It
/// provides both producer and consumer functionality for each data stream.
/// - ImpalaBackend service threads use this to add incoming data to streams
///   in response to TransmitData rpcs (AddData()) or to signal end-of-stream conditions
///   (CloseSender()).
/// - Exchange nodes extract data from an incoming stream via a DataStreamRecvr,
///   which is created with CreateRecvr().
//
/// DataStreamMgr also allows asynchronous cancellation of streams via Cancel()
/// which unblocks all DataStreamRecvr::GetBatch() calls that are made on behalf
/// of the cancelled fragment id.
///
/// Exposes three metrics:
///  'senders-blocked-on-recvr-creation' - currently blocked senders.
///  'total-senders-blocked-on-recvr-creation' - total number of blocked senders over
///  time.
///  'total-senders-timedout-waiting-for-recvr-creation' - total number of senders that
///  timed-out while waiting for a receiver.
///
/// TODO: The recv buffers used in DataStreamRecvr should count against
/// per-query memory limits.
class DataStreamMgr : public DataStreamMgrBase {
 public:
  DataStreamMgr(MetricGroup* metrics);
  virtual ~DataStreamMgr() override;

  /// Create a receiver for a specific fragment_instance_id/node_id destination;
  /// If is_merging is true, the receiver maintains a separate queue of incoming row
  /// batches for each sender and merges the sorted streams from each sender into a
  /// single stream. 'parent_tracker' is the MemTracker of the exchange node which owns
  /// this receiver. It's the parent of the MemTracker of the newly created receiver.
  /// Ownership of the receiver is shared between this DataStream mgr instance and the
  /// caller. 'client' is the BufferPool's client handle for allocating buffers.
  /// It's owned by the parent exchange node.
  std::shared_ptr<DataStreamRecvrBase> CreateRecvr(const RowDescriptor* row_desc,
      const TUniqueId& fragment_instance_id, PlanNodeId dest_node_id, int num_senders,
      int64_t buffer_size, bool is_merging, RuntimeProfile* profile,
      MemTracker* parent_tracker, BufferPool::ClientHandle* client) override;

  /// Adds a row batch to the recvr identified by fragment_instance_id/dest_node_id
  /// if the recvr has not been cancelled. sender_id identifies the sender instance
  /// from which the data came.
  /// The call blocks if this ends up pushing the stream over its buffering limit;
  /// it unblocks when the consumer removed enough data to make space for
  /// row_batch.
  /// TODO: enforce per-sender quotas (something like 200% of buffer_size/#senders),
  /// so that a single sender can't flood the buffer and stall everybody else.
  /// Returns OK if successful, error status otherwise.
  Status AddData(const TUniqueId& fragment_instance_id, PlanNodeId dest_node_id,
                 const TRowBatch& thrift_batch, int sender_id);

  /// Notifies the recvr associated with the fragment/node id that the specified
  /// sender has closed.
  /// Returns OK if successful, error status otherwise.
  Status CloseSender(const TUniqueId& fragment_instance_id, PlanNodeId dest_node_id,
      int sender_id);

  /// Closes all receivers registered for fragment_instance_id immediately.
  void Cancel(const TUniqueId& fragment_instance_id) override;

 private:
  friend class DataStreamRecvr;

  /// Owned by the metric group passed into the constructor
  MetricGroup* metrics_;

  /// Current number of senders waiting for a receiver to register
  IntGauge* num_senders_waiting_;

  /// Total number of senders that have ever waited for a receiver to register
  IntCounter* total_senders_waited_;

  /// Total number of senders that timed-out waiting for a receiver to register
  IntCounter* num_senders_timedout_;

  /// protects all fields below
  boost::mutex lock_;

  /// map from hash value of fragment instance id/node id pair to stream receivers;
  /// Ownership of the stream revcr is shared between this instance and the caller of
  /// CreateRecvr().
  /// we don't want to create a map<pair<TUniqueId, PlanNodeId>, DataStreamRecvr*>,
  /// because that requires a bunch of copying of ids for lookup
  typedef boost::unordered_multimap<uint32_t,
      std::shared_ptr<DataStreamRecvr>> RecvrMap;
  RecvrMap receiver_map_;

  /// (Fragment instance id, Plan node id) pair that uniquely identifies a stream.
  typedef std::pair<impala::TUniqueId, PlanNodeId> RecvrId;

  /// Less-than ordering for RecvrIds.
  struct ComparisonOp {
    bool operator()(const RecvrId& a, const RecvrId& b) {
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

  /// Ordered set of receiver IDs so that we can easily find all receivers for a given
  /// fragment (by starting at (fragment instance id, 0) and iterating until the fragment
  /// instance id changes), which is required for cancellation of an entire fragment.
  ///
  /// There is one entry in fragment_recvr_set_ for every entry in receiver_map_.
  typedef std::set<RecvrId, ComparisonOp> FragmentRecvrSet;
  FragmentRecvrSet fragment_recvr_set_;

  /// Return the receiver for given fragment_instance_id/node_id, or NULL if not found. If
  /// 'acquire_lock' is false, assumes lock_ is already being held and won't try to
  /// acquire it.
  std::shared_ptr<DataStreamRecvr> FindRecvr(const TUniqueId& fragment_instance_id,
      PlanNodeId node_id, bool acquire_lock = true);

  /// Calls FindRecvr(), but if NULL is returned, wait for up to
  /// FLAGS_datastream_sender_timeout_ms for the receiver to be registered.  Senders may
  /// initialise and start sending row batches before a receiver is ready. To accommodate
  /// this, we allow senders to establish a rendezvous between them and the receiver. When
  /// the receiver arrives, it triggers the rendezvous, and all waiting senders can
  /// proceed. A sender that waits for too long (120s by default) will eventually time out
  /// and abort. The output parameter 'already_unregistered' distinguishes between the two
  /// cases in which this method returns NULL:
  ///
  /// 1. *already_unregistered == true: the receiver had previously arrived and was
  /// already closed
  ///
  /// 2. *already_unregistered == false: the receiver has yet to arrive when this method
  /// returns, and the timeout has expired
  std::shared_ptr<DataStreamRecvr> FindRecvrOrWait(
      const TUniqueId& fragment_instance_id, PlanNodeId node_id,
      bool* already_unregistered);

  /// Remove receiver block for fragment_instance_id/node_id from the map.
  Status DeregisterRecvr(const TUniqueId& fragment_instance_id, PlanNodeId node_id);

  inline uint32_t GetHashValue(const TUniqueId& fragment_instance_id, PlanNodeId node_id);

  /// The coordination primitive used to signal the arrival of a waited-for receiver
  typedef Promise<std::shared_ptr<DataStreamRecvr>> RendezvousPromise;

  /// A reference-counted promise-wrapper used to coordinate between senders and
  /// receivers. The ref_count field tracks the number of senders waiting for the arrival
  /// of a particular receiver. When ref_count returns to 0, the last sender has ceased
  /// waiting (either because of a timeout, or because the receiver arrived), and the
  /// rendezvous can be torn down.
  ///
  /// Access is only thread-safe when lock_ is held.
  struct RefCountedPromise {
    uint32_t ref_count;

    // Without a conveniently copyable smart ptr, we keep a raw pointer to the promise and
    // are careful to delete it when ref_count becomes 0.
    RendezvousPromise* promise;

    void IncRefCount() { ++ref_count; }

    uint32_t DecRefCount() {
      if (--ref_count == 0) delete promise;
      return ref_count;
    }

    RefCountedPromise() : ref_count(0), promise(new RendezvousPromise()) { }
  };

  /// Map from stream (which identifies a receiver) to a (count, promise) pair that gives
  /// the number of senders waiting as well as a shared promise whose value is Set() with
  /// a pointer to the receiver when the receiver arrives. The count is used to detect
  /// when no receivers are waiting, to initiate clean-up after the fact.
  ///
  /// If pending_rendezvous_[X] exists, then receiver_map_[hash(X)] and
  /// fragment_recvr_set_[X] may exist (and vice versa), as entries are removed from
  /// pending_rendezvous_ some time after the rendezvous is triggered by the arrival of a
  /// matching receiver.
  typedef boost::unordered_map<RecvrId, RefCountedPromise> RendezvousMap;
  RendezvousMap pending_rendezvous_;

  /// Map from the time, in ms, that a stream should be evicted from closed_stream_cache
  /// to its RecvrId. Used to evict old streams from cache efficiently. multimap in case
  /// there are multiple streams with the same eviction time.
  typedef std::multimap<int64_t, RecvrId> ClosedStreamMap;
  ClosedStreamMap closed_stream_expirations_;

  /// Cache of recently closed RecvrIds. Used to allow straggling senders to fail fast by
  /// checking this cache, rather than waiting for the missed-receiver timeout to elapse
  /// in FindRecvrOrWait().
  boost::unordered_set<RecvrId> closed_stream_cache_;
};

}

#endif
