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

#include "exec/local-multi-sink.h"

#include "common/object-pool.h"
#include "runtime/runtime-state.h"

namespace impala {

DataSink* LocalMultiSinkConfig::CreateSink(RuntimeState* state) const {
  TDataSinkId sink_id = state->fragment().idx;
  return state->obj_pool()->Add(new LocalMultiSink(sink_id, *this, *tsink_, state));
}

LocalMultiSink::LocalMultiSink(TDataSinkId sink_id,
    const LocalMultiSinkConfig& sink_config, const TDataSink& dsink,
    RuntimeState* state) : DataSink(sink_id, sink_config, "LocalMultiSink", state) {
}

Status LocalMultiSink::Send(RuntimeState* state, RowBatch* batch) {
  return Status::OK();
}

Status LocalMultiSink::FlushFinal(RuntimeState* state) {
  return Status::OK();
}

} // namespace impala
