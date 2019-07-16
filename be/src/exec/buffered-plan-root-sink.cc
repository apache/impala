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

#include "exec/buffered-plan-root-sink.h"

namespace impala {

BufferedPlanRootSink::BufferedPlanRootSink(
    TDataSinkId sink_id, const RowDescriptor* row_desc, RuntimeState* state)
  : PlanRootSink(sink_id, row_desc, state) {}

Status BufferedPlanRootSink::Send(RuntimeState* state, RowBatch* batch) {
  return Status::OK();
}

Status BufferedPlanRootSink::FlushFinal(RuntimeState* state) {
  return Status::OK();
}

void BufferedPlanRootSink::Close(RuntimeState* state) {
  DataSink::Close(state);
}

void BufferedPlanRootSink::Cancel(RuntimeState* state) {}

Status BufferedPlanRootSink::GetNext(
    RuntimeState* state, QueryResultSet* results, int num_results, bool* eos) {
  *eos = true;
  return Status::OK();
}
}
