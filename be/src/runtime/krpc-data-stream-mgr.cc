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

#include "common/logging.h"

DECLARE_bool(use_krpc);

namespace impala {

[[noreturn]] static void AbortUnsupportedFeature() {
  // We should have gotten here only if the FLAGS_use_krpc is set to true.
  CHECK(FLAGS_use_krpc) << "Shouldn't reach here unless startup flag 'use_krpc' "
      "is true.";
  // KRPC isn't supported yet, so abort.
  ABORT_WITH_ERROR("KRPC is not supported yet. Please set the 'use_krpc' flag to "
      "false and restart the cluster.");
}

[[noreturn]] KrpcDataStreamMgr::KrpcDataStreamMgr(MetricGroup* metrics) {
  AbortUnsupportedFeature();
}

KrpcDataStreamMgr::~KrpcDataStreamMgr(){}

    [[noreturn]] std::shared_ptr<DataStreamRecvrBase> KrpcDataStreamMgr::CreateRecvr(
        RuntimeState* state, const RowDescriptor* row_desc,
        const TUniqueId& fragment_instance_id, PlanNodeId dest_node_id, int num_senders,
        int64_t buffer_size, RuntimeProfile* profile, bool is_merging) {
  AbortUnsupportedFeature();
}

[[noreturn]] Status KrpcDataStreamMgr::CloseSender(const TUniqueId& fragment_instance_id,
    PlanNodeId dest_node_id, int sender_id) {
  AbortUnsupportedFeature();
}

[[noreturn]] void KrpcDataStreamMgr::Cancel(const TUniqueId& fragment_instance_id) {
  AbortUnsupportedFeature();
}

} // namespace impala
