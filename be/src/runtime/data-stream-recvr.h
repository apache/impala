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
  ~DataStreamRecvr() {
    DCHECK(mgr_ == NULL) << "Must call Close()";
  }

  // Returns next row batch in data stream; blocks if there aren't any.
  // Returns NULL if eos (subsequent calls will not return any more batches).
  // Sets 'is_cancelled' to true if receiver fragment got cancelled, otherwise false.
  // The caller owns the batch.
  RowBatch* GetBatch(bool* is_cancelled) {
    return cb_->GetBatch(is_cancelled);
  }

  // deregister from mgr_
  void Close() {
    // TODO: log error msg
    mgr_->DeregisterRecvr(cb_->fragment_instance_id(), cb_->dest_node_id());
    mgr_ = NULL;
  }

 private:
  friend class DataStreamMgr;
  DataStreamMgr* mgr_;
  boost::shared_ptr<DataStreamMgr::StreamControlBlock> cb_;

  DataStreamRecvr(DataStreamMgr* mgr,
      boost::shared_ptr<DataStreamMgr::StreamControlBlock> cb)
    : mgr_(mgr), cb_(cb) {}
};

}

#endif
