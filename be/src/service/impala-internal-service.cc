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

#include "service/impala-internal-service.h"

#include <boost/lexical_cast.hpp>

#include "common/status.h"
#include "gutil/strings/substitute.h"
#include "service/impala-server.h"
#include "runtime/query-state.h"
#include "runtime/fragment-instance-state.h"
#include "runtime/exec-env.h"
#include "testutil/fault-injection-util.h"

#include "common/names.h"

using namespace impala;

DECLARE_string(debug_actions);

ImpalaInternalService::ImpalaInternalService() {
  impala_server_ = ExecEnv::GetInstance()->impala_server();
  DCHECK(impala_server_ != nullptr);
}

template <typename T> void SetUnknownIdError(
    const string& id_type, const TUniqueId& id, T* status_container) {
  Status status(ErrorMsg(TErrorCode::INTERNAL_ERROR,
      Substitute("Unknown $0 id: $1", id_type, PrintId(id))));
  status.SetTStatus(status_container);
}

void ImpalaInternalService::UpdateFilter(TUpdateFilterResult& return_val,
    const TUpdateFilterParams& params) {
  DebugActionNoFail(FLAGS_debug_actions, "UPDATE_FILTER_DELAY");
  DCHECK(params.__isset.filter_id);
  DCHECK(params.__isset.query_id);
  DCHECK(params.__isset.bloom_filter || params.__isset.min_max_filter);
  impala_server_->UpdateFilter(return_val, params);
}

void ImpalaInternalService::PublishFilter(TPublishFilterResult& return_val,
    const TPublishFilterParams& params) {
  DebugActionNoFail(FLAGS_debug_actions, "PUBLISH_FILTER_DELAY");
  DCHECK(params.__isset.filter_id);
  DCHECK(params.__isset.dst_query_id);
  DCHECK(params.__isset.dst_fragment_idx);
  DCHECK(params.__isset.bloom_filter || params.__isset.min_max_filter);
  QueryState::ScopedRef qs(params.dst_query_id);
  if (qs.get() == nullptr) return;
  qs->PublishFilter(params);
}
