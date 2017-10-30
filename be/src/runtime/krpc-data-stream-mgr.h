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

#ifndef IMPALA_RUNTIME_KRPC_DATA_STREAM_MGR_H
#define IMPALA_RUNTIME_KRPC_DATA_STREAM_MGR_H

#include "runtime/data-stream-mgr-base.h"

#include "common/status.h"
#include "runtime/descriptors.h"  // for PlanNodeId

namespace impala {

class DataStreamRecvrBase;
class MetricGroup;
class RuntimeProfile;
class RuntimeState;
class TRowBatch;

class KrpcDataStreamMgr : public DataStreamMgrBase {
 public:
  [[noreturn]] KrpcDataStreamMgr(MetricGroup* metrics);
  virtual ~KrpcDataStreamMgr() override;

  [[noreturn]] std::shared_ptr<DataStreamRecvrBase> CreateRecvr(RuntimeState* state,
      const RowDescriptor* row_desc, const TUniqueId& fragment_instance_id,
      PlanNodeId dest_node_id, int num_senders, int64_t buffer_size,
      RuntimeProfile* profile, bool is_merging) override;

  [[noreturn]] Status CloseSender(const TUniqueId& fragment_instance_id,
      PlanNodeId dest_node_id, int sender_id) override;

  [[noreturn]] void Cancel(const TUniqueId& fragment_instance_id) override;

};

} // namespace impala
#endif /* IMPALA_RUNTIME_KRPC_DATA_STREAM_MGR_H */
