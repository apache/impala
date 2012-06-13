// Copyright (c) 2011 Cloudera, Inc. All rights reserved.

#ifndef IMPALA_RUNTIME_DATA_STREAM_RECVR_H
#define IMPALA_RUNTIME_DATA_STREAM_RECVR_H

#include "runtime/data-stream-mgr.h"

namespace impala {

class DataStreamMgr;

// Single receiver of an m:n data stream.
// Incoming row batches are routed to destinations based on the provided
// partitioning specification.
// Receivers are created via DataStreamMgr::CreateRecvr().
class DataStreamRecvr {
 public:
  // deregister from mgr_
  ~DataStreamRecvr() {
    // TODO: log error msg
    mgr_->DeregisterRecvr(cb_->fragment_id(), cb_->dest_node_id());
  }

  // Returns next row batch in data stream; blocks if there aren't any.
  // Returns NULL if eos (subsequent calls will not return any more batches).
  // The caller owns the batch.
  // TODO: error handling
  RowBatch* GetBatch() {
    return cb_->GetBatch();
  }

 private:
  friend class DataStreamMgr;
  DataStreamMgr* mgr_;
  DataStreamMgr::StreamControlBlock* cb_;

  DataStreamRecvr(DataStreamMgr* mgr, DataStreamMgr::StreamControlBlock* cb)
    : mgr_(mgr), cb_(cb) {}
};

}

#endif
