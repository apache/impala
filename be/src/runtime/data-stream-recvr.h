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

#ifndef IMPALA_RUNTIME_DATA_STREAM_RECVR_H
#define IMPALA_RUNTIME_DATA_STREAM_RECVR_H

#include "data-stream-recvr-base.h"

#include <boost/scoped_ptr.hpp>
#include <boost/thread/mutex.hpp>

#include "common/atomic.h"
#include "common/object-pool.h"
#include "common/status.h"
#include "gen-cpp/Types_types.h"   // for TUniqueId
#include "runtime/descriptors.h"
#include "util/runtime-profile.h"

namespace impala {

class DataStreamMgr;
class SortedRunMerger;
class MemTracker;
class RowBatch;
class TRowBatch;
class TupleRowCompare;

/// Single receiver of an m:n data stream.
/// This is for use by the DataStreamMgr, which is the implementation of the abstract
/// class DataStreamMgrBase that depends on Thrift.
/// DataStreamRecvr maintains one or more queues of row batches received by a
/// DataStreamMgr from one or more sender fragment instances.
/// Receivers are created via DataStreamMgr::CreateRecvr().
/// Ownership of a stream recvr is shared between the DataStreamMgr that created it and
/// the caller of DataStreamMgr::CreateRecvr() (i.e. the exchange node)
//
/// The is_merging_ member determines if the recvr merges input streams from different
/// sender fragment instances according to a specified sort order.
/// If is_merging_ = false : Only one batch queue is maintained for row batches from all
/// sender fragment instances. These row batches are returned one at a time via
/// GetBatch().
/// If is_merging_ is true : One queue is created for the batches from each distinct
/// sender. A SortedRunMerger instance must be created via CreateMerger() prior to
/// retrieving any rows from the receiver. Rows are retrieved from the receiver via
/// GetNext(RowBatch* output_batch, int limit, bool eos). After the final call to
/// GetNext(), TransferAllResources() must be called to transfer resources from the input
/// batches from each sender to the caller's output batch.
/// The receiver sets deep_copy to false on the merger - resources are transferred from
/// the input batches from each sender queue to the merger to the output batch by the
/// merger itself as it processes each run.
//
/// DataStreamRecvr::Close() must be called by the caller of CreateRecvr() to remove the
/// recvr instance from the tracking structure of its DataStreamMgr in all cases.
class DataStreamRecvr : public DataStreamRecvrBase {
 public:
  virtual ~DataStreamRecvr() override;

  /// Returns next row batch in data stream; blocks if there aren't any.
  /// Retains ownership of the returned batch. The caller must acquire data from the
  /// returned batch before the next call to GetBatch(). A NULL returned batch indicated
  /// eos. Must only be called if is_merging_ is false.
  /// TODO: This is currently only exposed to the non-merging version of the exchange.
  /// Refactor so both merging and non-merging exchange use GetNext(RowBatch*, bool* eos).
  Status GetBatch(RowBatch** next_batch) override;

  /// Deregister from DataStreamMgr instance, which shares ownership of this instance.
  void Close() override;

  /// Create a SortedRunMerger instance to merge rows from multiple sender according to the
  /// specified row comparator. Fetches the first batches from the individual sender
  /// queues. The exprs used in less_than must have already been prepared and opened.
  Status CreateMerger(const TupleRowComparator& less_than) override;

  /// Fill output_batch with the next batch of rows obtained by merging the per-sender
  /// input streams. Must only be called if is_merging_ is true.
  Status GetNext(RowBatch* output_batch, bool* eos) override;

  /// Transfer all resources from the current batches being processed from each sender
  /// queue to the specified batch.
  void TransferAllResources(RowBatch* transfer_batch) override;

  const TUniqueId& fragment_instance_id() const { return fragment_instance_id_; }
  PlanNodeId dest_node_id() const { return dest_node_id_; }
  const RowDescriptor& row_desc() const { return *row_desc_; }
  MemTracker* mem_tracker() const { return mem_tracker_.get(); }

 private:
  friend class DataStreamMgr;
  class SenderQueue;

  DataStreamRecvr(DataStreamMgr* stream_mgr, MemTracker* parent_tracker,
      const RowDescriptor* row_desc, const TUniqueId& fragment_instance_id,
      PlanNodeId dest_node_id, int num_senders, bool is_merging,
      int64_t total_buffer_limit, RuntimeProfile* parent_profile);

  /// Add a new batch of rows to the appropriate sender queue, blocking if the queue is
  /// full. Called from DataStreamMgr.
  void AddBatch(const TRowBatch& thrift_batch, int sender_id);

  /// Indicate that a particular sender is done. Delegated to the appropriate
  /// sender queue. Called from DataStreamMgr.
  void RemoveSender(int sender_id);

  /// Empties the sender queues and notifies all waiting consumers of cancellation.
  void CancelStream();

  /// Return true if the addition of a new batch of size 'batch_size' would exceed the
  /// total buffer limit.
  bool ExceedsLimit(int64_t batch_size) {
    return num_buffered_bytes_.Load() + batch_size > total_buffer_limit_;
  }

  /// DataStreamMgr instance used to create this recvr. (Not owned)
  DataStreamMgr* mgr_;

  /// Fragment and node id of the destination exchange node this receiver is used by.
  TUniqueId fragment_instance_id_;
  PlanNodeId dest_node_id_;

  /// soft upper limit on the total amount of buffering allowed for this stream across
  /// all sender queues. we stop acking incoming data once the amount of buffered data
  /// exceeds this value
  int total_buffer_limit_;

  /// Row schema.
  const RowDescriptor* row_desc_;

  /// True if this reciver merges incoming rows from different senders. Per-sender
  /// row batch queues are maintained in this case.
  bool is_merging_;

  /// total number of bytes held across all sender queues.
  AtomicInt64 num_buffered_bytes_;

  /// Memtracker for batches in the sender queue(s).
  boost::scoped_ptr<MemTracker> mem_tracker_;

  /// One or more queues of row batches received from senders. If is_merging_ is true,
  /// there is one SenderQueue for each sender. Otherwise, row batches from all senders
  /// are placed in the same SenderQueue. The SenderQueue instances are owned by the
  /// receiver and placed in sender_queue_pool_.
  std::vector<SenderQueue*> sender_queues_;

  /// SortedRunMerger used to merge rows from different senders.
  boost::scoped_ptr<SortedRunMerger> merger_;

  /// Pool of sender queues.
  ObjectPool sender_queue_pool_;

  /// Runtime profile storing the counters below. Child of 'parent_profile' passed into
  /// constructor.
  RuntimeProfile* const profile_;

  /// Number of bytes received
  RuntimeProfile::Counter* bytes_received_counter_;

  /// Time series of number of bytes received, samples bytes_received_counter_
  RuntimeProfile::TimeSeriesCounter* bytes_received_time_series_counter_;

  RuntimeProfile::Counter* deserialize_row_batch_timer_;

  /// Time spent waiting until the first batch arrives across all queues.
  /// TODO: Turn this into a wall-clock timer.
  RuntimeProfile::Counter* first_batch_wait_total_timer_;

  /// Total time (summed across all threads) spent waiting for the
  /// recv buffer to be drained so that new batches can be
  /// added. Remote plan fragments are blocked for the same amount of
  /// time.
  RuntimeProfile::Counter* buffer_full_total_timer_;

  /// Protects access to buffer_full_wall_timer_. We only want one
  /// thread to be running the timer at any time, and we use this
  /// try_mutex to enforce this condition. If a thread does not get
  /// the lock, it continues to execute, but without running the
  /// timer.
  boost::try_mutex buffer_wall_timer_lock_;

  /// Wall time senders spend waiting for the recv buffer to have capacity.
  RuntimeProfile::Counter* buffer_full_wall_timer_;

  /// Total time spent waiting for data to arrive in the recv buffer
  RuntimeProfile::Counter* data_arrival_timer_;
};

}

#endif
