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


#ifndef IMPALA_RUNTIME_DATA_STREAM_MGR_BASE_H
#define IMPALA_RUNTIME_DATA_STREAM_MGR_BASE_H

#include "common/status.h"
#include "runtime/descriptors.h"  // for PlanNodeId

namespace impala {

class DataStreamRecvrBase;
class RuntimeProfile;
class RuntimeState;
class TRowBatch;
class TUniqueId;

/// Interface for a singleton class which manages all incoming data streams at a backend
/// node.
/// TODO: This is a temporary pure virtual base class that defines the basic interface for
/// 2 parallel implementations of the DataStreamMgrBase, one each for Thrift and KRPC.
/// Remove this in favor of the KRPC implementation when possible.
class DataStreamMgrBase {
 public:
  DataStreamMgrBase() {}

  virtual ~DataStreamMgrBase() { }

  /// Create a receiver for a specific fragment_instance_id/node_id destination;
  virtual std::shared_ptr<DataStreamRecvrBase> CreateRecvr(RuntimeState* state,
      const RowDescriptor* row_desc, const TUniqueId& fragment_instance_id,
      PlanNodeId dest_node_id, int num_senders, int64_t buffer_size,
      RuntimeProfile* profile, bool is_merging) = 0;

  /// Notifies the recvr associated with the fragment/node id that the specified
  /// sender has closed.
  virtual Status CloseSender(const TUniqueId& fragment_instance_id,
      PlanNodeId dest_node_id, int sender_id) = 0;

  /// Closes all receivers registered for fragment_instance_id immediately.
  virtual void Cancel(const TUniqueId& fragment_instance_id) = 0;

};

}

#endif /* IMPALA_RUNTIME_DATA_STREAM_MGR_BASE_H */
