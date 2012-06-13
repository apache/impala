// Copyright (c) 2011 Cloudera, Inc. All rights reserved.

#ifndef IMPALA_RUNTIME_DATA_STREAM_MGR_H
#define IMPALA_RUNTIME_DATA_STREAM_MGR_H

#include <list>
#include <boost/thread/mutex.hpp>
#include <boost/thread/condition_variable.hpp>
#include <boost/unordered_map.hpp>

#include "common/status.h"
#include "common/object-pool.h"
#include "runtime/descriptors.h"  // for PlanNodeId
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
//   in response to CloseChannel rpcs (CloseChannel()).
// - Exchange nodes extract data from an incoming stream via a DataStreamRecvr,
//   which is created via DataStreamMgr::CreateRecvr().
class DataStreamMgr {
 public:
  DataStreamMgr() {}

  // Create a receiver for a specific fragment_id/node_id destination; desc_tbl
  // is the query's descriptor table and is needed to decode incoming TRowBatches.
  // The caller is responsible for deleting the returned DataStreamRecvr.
  // TODO: create receivers in someone's pool
  DataStreamRecvr* CreateRecvr(
      const RowDescriptor& row_desc, const TUniqueId& fragment_id,
      PlanNodeId dest_node_id, int num_senders, int buffer_size);
  
  // Adds a row batch to the stream identified by fragment_id/dest_node_id.
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
  Status CloseStream(const TUniqueId& fragment_id, PlanNodeId dest_node_id);

 private:
  friend class DataStreamRecvr;

  class StreamControlBlock {
   public:
    StreamControlBlock(
        const RowDescriptor& row_desc, const TUniqueId& fragment_id,
        PlanNodeId dest_node_id, int num_senders, int buffer_size);

    // Returns next available batch or NULL if end-of-stream.
    // A returned batch that is not filled to capacity does *not* indicate
    // end-of-stream.
    // The call blocks until another batch arrives or all senders close
    // their channels.
    // The caller owns the batch.
    RowBatch* GetBatch();

    // Adds a row batch to this stream's queue; blocks if this will
    // make the stream exceed its buffer limit.
    void AddBatch(const TRowBatch& batch);

    // Decrement the number of remaining senders and signal eos ("new data")
    // if the count drops to 0.
    void DecrementSenders();

    const TUniqueId& fragment_id() const { return fragment_id_; }
    PlanNodeId dest_node_id() const { return dest_node_id_; }

   private:
    TUniqueId fragment_id_;
    PlanNodeId dest_node_id_;
    const RowDescriptor& row_desc_;

    // protects all subsequent data in this block
    boost::mutex lock_;

    // soft upper limit on the amount of buffering allowed for this stream;
    // we stop acking incoming data once the amount of buffered data
    // exceeds this value
    int buffer_limit_;

    // total number of bytes held in batch_queue_
    int num_buffered_bytes_;

    // number of senders which haven't closed the channel yet
    // (if it drops to 0, end-of-stream is true)
    int num_remaining_senders_;

    // signal arrival of new batch or the eos condition
    boost::condition_variable data_arrival_;

    // signal removal of data by stream consumer
    boost::condition_variable data_removal_;

    // queue of (batch length, batch) pairs
    typedef std::list<std::pair<int, RowBatch*> > RowBatchQueue;
    RowBatchQueue batch_queue_;
  };

  ObjectPool pool_;  // holds control blocks

  // map from hash value of fragment id/node id pair to control blocks;
  // we don't want to create a map<pair<TUniqueId, PlanNodeId>, StreamControlBlock*>,
  // because that requires a bunch of copying of ids for lookup
  typedef boost::unordered_multimap<size_t, StreamControlBlock*> StreamMap;
  StreamMap stream_map_;
  boost::mutex stream_map_lock_;

  // Return iterator into stream_map_ for given fragment_id/node_id, or stream_map_.end()
  // if not found.
  StreamMap::iterator FindControlBlock(const TUniqueId& fragment_id, PlanNodeId node_id);

  // Remove control block for fragment_id/node_id.
  Status DeregisterRecvr(const TUniqueId& fragment_id, PlanNodeId node_id);

  size_t GetHashValue(const TUniqueId& fragment_id, PlanNodeId node_id);
};

}

#endif
