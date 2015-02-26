// Copyright 2014 Cloudera Inc.
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

#include "service/fragment-mgr.h"

#include <boost/lexical_cast.hpp>
#include <google/malloc_extension.h>
#include <gutil/strings/substitute.h>

#include "service/fragment-exec-state.h"
#include "runtime/exec-env.h"
#include "util/impalad-metrics.h"
#include "util/uid-util.h"

#include "common/names.h"

using namespace impala;
using namespace strings;

// TODO: this logging should go into a per query log.
DEFINE_int32(log_mem_usage_interval, 0, "If non-zero, impalad will output memory usage "
    "every log_mem_usage_interval'th fragment completion.");

Status FragmentMgr::ExecPlanFragment(const TExecPlanFragmentParams& exec_params) {
  VLOG_QUERY << "ExecPlanFragment() instance_id="
             << exec_params.fragment_instance_ctx.fragment_instance_id
             << " coord=" << exec_params.fragment_instance_ctx.query_ctx.coord_address
             << " fragment instance#="
             << exec_params.fragment_instance_ctx.fragment_instance_idx;

  // Preparing and opening the fragment creates a thread and consumes a non-trivial
  // amount of memory. If we are already starved for memory, cancel the fragment as
  // early as possible to avoid digging the hole deeper.
  if (ExecEnv::GetInstance()->process_mem_tracker()->LimitExceeded()) {
    Status status = Status::MemLimitExceeded();
    status.AddDetail(Substitute("Instance $0 of plan fragment $1 of query $2 could not "
            "start because the backend Impala daemon is over its memory limit",
            PrintId(exec_params.fragment_instance_ctx.fragment_instance_id),
            exec_params.fragment.display_name,
            PrintId(exec_params.fragment_instance_ctx.query_ctx.query_id)));
    return status;
  }

  // Remote fragments must always have a sink. Remove when IMPALA-2905 is resolved.
  DCHECK(exec_params.fragment.__isset.output_sink);

  shared_ptr<FragmentExecState> exec_state(
      new FragmentExecState(exec_params, ExecEnv::GetInstance()));

  // Register exec_state before this RPC returns so that async Cancel() calls (which can
  // only happen after this RPC returns) can always find this fragment.
  {
    lock_guard<SpinLock> l(fragment_exec_state_map_lock_);
    fragment_exec_state_map_.insert(
        make_pair(exec_state->fragment_instance_id(), exec_state));
  }

  ImpaladMetrics::IMPALA_SERVER_NUM_FRAGMENTS_IN_FLIGHT->Increment(1L);
  ImpaladMetrics::IMPALA_SERVER_NUM_FRAGMENTS->Increment(1L);

  // Execute plan fragment in new thread.
  // TODO: manage threads via global thread pool?
  const TUniqueId& fragment_id = exec_state->fragment_instance_id();
  exec_state->set_exec_thread(new Thread("fragment-mgr",
      Substitute("exec-plan-fragment-$0", PrintId(fragment_id)),
          &FragmentMgr::FragmentThread, this, fragment_id));

  return Status::OK();
}

void FragmentMgr::FragmentThread(TUniqueId fragment_instance_id) {
  shared_ptr<FragmentExecState> exec_state = GetFragmentExecState(fragment_instance_id);
  if (exec_state.get() == NULL) return;
  Status status = exec_state->Prepare();
  if (status.ok()) exec_state->Exec();

  // We're done with this plan fragment
  {
    lock_guard<SpinLock> l(fragment_exec_state_map_lock_);
    size_t num_erased =
        fragment_exec_state_map_.erase(exec_state->fragment_instance_id());
    DCHECK_EQ(num_erased, 1);
  }
  // TODO: this might be imprecise, if another client of FragmentMgr has a reference to
  // the fragment exec state.
  ImpaladMetrics::IMPALA_SERVER_NUM_FRAGMENTS_IN_FLIGHT->Increment(-1L);

#ifndef ADDRESS_SANITIZER
  // tcmalloc and address sanitizer can not be used together
  if (FLAGS_log_mem_usage_interval > 0) {
    uint64_t num_complete = ImpaladMetrics::IMPALA_SERVER_NUM_FRAGMENTS->value();
    if (num_complete % FLAGS_log_mem_usage_interval == 0) {
      char buf[2048];
      // This outputs how much memory is currently being used by this impalad
      MallocExtension::instance()->GetStats(buf, 2048);
      LOG(INFO) << buf;
    }
  }
#endif
}

shared_ptr<FragmentMgr::FragmentExecState> FragmentMgr::GetFragmentExecState(
    const TUniqueId& fragment_instance_id) {
  lock_guard<SpinLock> l(fragment_exec_state_map_lock_);
  FragmentExecStateMap::const_iterator i =
      fragment_exec_state_map_.find(fragment_instance_id);
  if (i == fragment_exec_state_map_.end()) return shared_ptr<FragmentExecState>();

  return i->second;
}

void FragmentMgr::CancelPlanFragment(TCancelPlanFragmentResult& return_val,
    const TCancelPlanFragmentParams& params) {
  VLOG_QUERY << "CancelPlanFragment(): instance_id=" << params.fragment_instance_id;
  shared_ptr<FragmentExecState> exec_state =
      GetFragmentExecState(params.fragment_instance_id);
  if (exec_state.get() == NULL) {
    Status status(ErrorMsg(TErrorCode::INTERNAL_ERROR, Substitute("Unknown fragment id: $0",
        lexical_cast<string>(params.fragment_instance_id))));
    status.SetTStatus(&return_val);
    return;
  }
  // we only initiate cancellation here, the map entry as well as the exec state
  // are removed when fragment execution terminates (which is at present still
  // running in exec_state->exec_thread_)
  exec_state->Cancel().SetTStatus(&return_val);
}
