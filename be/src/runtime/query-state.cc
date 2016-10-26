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


#include "runtime/query-state.h"

#include <boost/thread/locks.hpp>
#include <boost/thread/lock_guard.hpp>

#include "runtime/exec-env.h"
#include "runtime/fragment-instance-state.h"
#include "runtime/query-exec-mgr.h"

using namespace impala;

QueryState::ScopedRef::ScopedRef(const TUniqueId& query_id) {
  DCHECK(ExecEnv::GetInstance()->query_exec_mgr() != nullptr);
  query_state_ = ExecEnv::GetInstance()->query_exec_mgr()->GetQueryState(query_id);
}

QueryState::ScopedRef::~ScopedRef() {
  if (query_state_ == nullptr) return;
  ExecEnv::GetInstance()->query_exec_mgr()->ReleaseQueryState(query_state_);
}

QueryState::QueryState(const TQueryCtx& query_ctx)
  : query_ctx_(query_ctx),
    refcnt_(0) {
  TQueryOptions& query_options = query_ctx_.client_request.query_options;
  // max_errors does not indicate how many errors in total have been recorded, but rather
  // how many are distinct. It is defined as the sum of the number of generic errors and
  // the number of distinct other errors.
  if (query_options.max_errors <= 0) {
    // TODO: fix linker error and uncomment this
    //query_options_.max_errors = FLAGS_max_errors;
    query_options.max_errors = 100;
  }
  if (query_options.batch_size <= 0) {
    query_options.__set_batch_size(DEFAULT_BATCH_SIZE);
  }
}

void QueryState::RegisterFInstance(FragmentInstanceState* fis) {
  VLOG_QUERY << "RegisterFInstance(): instance_id=" << PrintId(fis->instance_id());
  lock_guard<mutex> l(fis_map_lock_);
  DCHECK_EQ(fis_map_.count(fis->instance_id()), 0);
  fis_map_.insert(make_pair(fis->instance_id(), fis));
}

FragmentInstanceState* QueryState::GetFInstanceState(const TUniqueId& instance_id) {
  VLOG_FILE << "GetFInstanceState(): instance_id=" << PrintId(instance_id);
  lock_guard<mutex> l(fis_map_lock_);
  auto it = fis_map_.find(instance_id);
  return it != fis_map_.end() ? it->second : nullptr;
}
