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


#ifndef IMPALA_RUNTIME_DATA_STREAM_MGR_H
#define IMPALA_RUNTIME_DATA_STREAM_MGR_H

#include <list>
#include <set>
#include <boost/thread/mutex.hpp>
#include <boost/shared_ptr.hpp>
#include <boost/thread/condition_variable.hpp>
#include <boost/unordered_map.hpp>

#include "common/status.h"
#include "common/object-pool.h"
#include "runtime/descriptors.h"  // for PlanNodeId
#include "util/runtime-profile.h"
#include "gen-cpp/Types_types.h"  // for TUniqueId

namespace impala {

class DescriptorTbl;
class DataStreamRecvr;
class RowBatch;
class TRowBatch;

// Singleton class which manages all incoming data streams at a backend node. It
// provides both producer and consumer functionality for each data stream.
// - ImpalaBackend service threads use this to add incoming data to streams
//   in response to TransmitData rpcs (AddData()) or to signal end-of-stream conditions
//   (CloseSender()).
// - Exchange nodes extract data from an incoming stream via a DataStreamRecvr,
//   which is created with CreateRecvr().
//
// DataStreamMgr also allows asynchronous cancellation of streams via Cancel()
// which unblocks all DataStreamRecvr::GetBatch() calls that are made on behalf
// of the cancelled fragment id.
//
// TODO: The recv buffers used in DataStreamRecvr should count against
// per-query memory limits.
class DataStreamMgr {
 public:
  DataStreamMgr() {}

  // Create a receiver for a specific fragment_id/node_id destination; desc_tbl
  // is the query's descriptor table and is needed to decode incoming TRowBatches.
  // The caller is responsible for deleting the returned DataStreamRecvr.
  // TODO: create receivers in someone's pool
  DataStreamRecvr* CreateRecvr(
      const RowDescriptor& row_desc, const TUniqueId& fragment_id,
      PlanNodeId dest_node_id, int num_senders, int buffer_size,
      RuntimeProfile* profile);

  // Adds a row batch to the stream identified by fragment_id/dest_node_id if the stream
  // has not been cancelled.
  // The call blocks if this ends up pushing the stream over its buffering limit;
  // it unblocks when the stream consumer removed enough data to make space for
  // row_batch.
  // TODO: enforce per-sender quotas (something like 200% of buffer_size/#senders),
  // so that a single sender can't flood the buffer and stall everybody else.
  // Returns OK if successful, error status otherwise.
  Status AddData(const TUniqueId& fragment_id, PlanNodeId dest_node_id,
                 const TRowBatch& thrift_batch);

  // Decreases the #remaining_senders count for the stream identified by
  // fragment_id/dest_node_id.
  // Returns OK if successful, error status otherwise.
  Status CloseSender(const TUniqueId& fragment_id, PlanNodeId dest_node_id);

  // Closes all streams registered for fragment_id immediately.
  void Cancel(const TUniqueId& fragment_id);

 private:
  friend class DataStreamRecvr;

  class StreamControlBlock {
   public:
    StreamControlBlock(
        const RowDescriptor& row_desc, const TUniqueId& fragment_id,
        PlanNodeId dest_node_id, int num_senders, int buffer_size,
        RuntimeProfile* profile);

    // Returns next available batch or NULL if end-of-stream or stream got
    // cancelled (sets 'is_cancelled' accordingly).
    // A returned batch that is not filled to capacity does *not* indicate
    // end-of-stream.
    // The call blocks until another batch arrives or all senders close
    // their channels.
    // The caller owns the batch.
    RowBatch* GetBatch(bool* is_cancelled);

    // Adds a row batch to this stream's queue if this stream has not been cancelled;
    // blocks if this will make the stream exceed its buffer limit.
    //
    // For example, for an NxN broadcast, there will be N threads on N
    // clients talking to up-to N threads on N servers. Those server
    // threads share a buffer per-exchange-node in their
    // StreamControlBlock (so one per incoming plan fragment), so
    // typically you'll have N threads contending to write to a single
    // buffer. If there is no space in the buffer, they will block the
    // sender until space is available.
    void AddBatch(const TRowBatch& batch);

    // Decrement the number of remaining senders and signal eos ("new data")
    // if the count drops to 0.
    void DecrementSenders();

    // Set cancellation flag and signal cancellation to receiver and sender. Subsequent
    // incoming batches will be dropped.
    void CancelStream();

    const TUniqueId& fragment_id() const { return fragment_id_; }
    PlanNodeId dest_node_id() const { return dest_node_id_; }

   private:
    TUniqueId fragment_id_;
    PlanNodeId dest_node_id_;
    const RowDescriptor& row_desc_;

    // protects all subsequent data in this block
    boost::mutex lock_;

    // if true, the receiver fragment for this stream got cancelled
    bool is_cancelled_;

    // soft upper limit on the amount of buffering allowed for this stream;
    // we stop acking incoming data once the amount of buffered data
    // exceeds this value
    int buffer_limit_;

    // total number of bytes held in batch_queue_
    int num_buffered_bytes_;

    // number of senders which haven't closed the channel yet
    // (if it drops to 0, end-of-stream is true)
    int num_remaining_senders_;

    // signal arrival of new batch or the eos/cancelled condition
    boost::condition_variable data_arrival_;

    // signal removal of data by stream consumer
    boost::condition_variable data_removal_;

    // queue of (batch length, batch) pairs.  The StreamControl block owns memory to
    // these batches.  They are handed off to the caller via GetBatch.
    typedef std::list<std::pair<int, RowBatch*> > RowBatchQueue;
    RowBatchQueue batch_queue_;

    RuntimeProfile::Counter* bytes_received_counter_;
    RuntimeProfile::Counter* deserialize_row_batch_timer_;

    // Time spent waiting until the first batch arrives
    RuntimeProfile::Counter* first_batch_wait_timer_;

    // Set to true when the first batch has been received
    bool received_first_batch_;

    // Total time (summed across all threads) spent waiting for the
    // recv buffer to be drained so that new batches can be
    // added. Remote plan fragments are blocked for the same amount of
    // time.
    RuntimeProfile::Counter* buffer_full_total_timer_;

    // Protects access to buffer_full_wall_timer_. We only want one
    // thread to be running the timer at any time, and we use this
    // try_mutex to enforce this condition. If a thread does not get
    // the lock, it continues to execute, but without running the
    // timer.
    boost::try_mutex buffer_wall_timer_lock_;

    // Wall time senders spend waiting for the recv buffer to have capacity.
    RuntimeProfile::Counter* buffer_full_wall_timer_;

    // Total time spent waiting for data to arrive in the recv buffer
    RuntimeProfile::Counter* data_arrival_timer_;
  };

  ObjectPool pool_;  // holds control blocks

  // protects all fields below
  boost::mutex lock_;

  // map from hash value of fragment id/node id pair to control blocks;
  // we don't want to create a map<pair<TUniqueId, PlanNodeId>, StreamControlBlock*>,
  // because that requires a bunch of copying of ids for lookup
  typedef boost::unordered_multimap<uint32_t,
      boost::shared_ptr<StreamControlBlock> > StreamMap;
  StreamMap stream_map_;

  // less-than ordering for pair<TUniqueId, PlanNodeId>
  struct ComparisonOp {
    bool operator()(const std::pair<impala::TUniqueId, PlanNodeId>& a,
                    const std::pair<impala::TUniqueId, PlanNodeId>& b) {
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

  // ordered set of registered streams' fragment id/node id
  typedef std::set<std::pair<TUniqueId, PlanNodeId>, ComparisonOp > FragmentStreamSet;
  FragmentStreamSet fragment_stream_set_;

  // Return the StreamControlBlock for given fragment_id/node_id, or NULL if not found.
  // If 'acquire_lock' is false, assumes lock_ is already being held and won't try to
  // acquire it.
  boost::shared_ptr<StreamControlBlock> FindControlBlock(const TUniqueId& fragment_id,
      PlanNodeId node_id, bool acquire_lock = true);

  // Remove control block for fragment_id/node_id.
  Status DeregisterRecvr(const TUniqueId& fragment_id, PlanNodeId node_id);

  inline uint32_t GetHashValue(const TUniqueId& fragment_id, PlanNodeId node_id);
};

}

#endif
