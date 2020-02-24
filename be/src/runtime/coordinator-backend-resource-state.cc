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

#include "runtime/coordinator-backend-state.h"

#include "common/names.h"

using namespace impala;

DEFINE_uint32_hidden(batched_release_decay_factor, 2,
    "The exponential decay factor for the 'Batched Release' of backends. The default of "
    "2 ensures that the number of times resources are released is bounded by O(log2(n)). "
    "Setting this to another value, such as 10, would bound the number of times "
    "resources are released by O(log10(n)).");
DEFINE_uint64_hidden(release_backend_states_delay_ms, 1000,
    "The timeout for the 'Timed Release' of backends. If more than this many "
    "milliseconds has passed since the last time any Backends were released, then "
    "release all pending Backends. Set to 1000 milliseconds by default.");

DECLARE_uint32(batched_release_decay_factor);
DECLARE_uint64(release_backend_states_delay_ms);

Coordinator::BackendResourceState::BackendResourceState(
    const vector<BackendState*>& backend_states)
  : num_in_use_(backend_states.size()),
    backend_states_(backend_states),
    num_backends_(backend_states.size()),
    release_backend_states_delay_ns_(FLAGS_release_backend_states_delay_ms * 1000000),
    batched_release_decay_value_(FLAGS_batched_release_decay_factor) {
  DCHECK_GT(batched_release_decay_value_, 0)
      << "Invalid value for --batched_release_decay_factor, must greater than 0";
  // Populate the backend_resource_states_ map and mark all BackendStates as
  // IN_USE.
  for (auto backend_state : backend_states_) {
    backend_resource_states_[backend_state] = ResourceState::IN_USE;
  }
  // Start the 'Timed Release' timer.
  released_timer_.Start();
}

Coordinator::BackendResourceState::~BackendResourceState() {
  // Assert that all BackendStates have been released. We use num_backends_ instead of
  // backend_states_.size() so backend_states_ does not need to be alive when the
  // destructor runs.
  DCHECK(closed_);
  DCHECK_EQ(num_released_, num_backends_);
}

void Coordinator::BackendResourceState::MarkBackendFinished(
    BackendState* backend_state, vector<BackendState*>* releasable_backend_states) {
  lock_guard<SpinLock> lock(lock_);
  if (!closed_
      && backend_resource_states_.at(backend_state) == ResourceState::IN_USE) {
    // Transition the BackendState to PENDING and update any related counters.
    backend_resource_states_.at(backend_state) = ResourceState::PENDING;
    ++num_pending_;
    --num_in_use_;

    // If the coordinator backend has not been released, but all other have, then the only
    // running Backend must be the coordinator. The Coordinator fragment should buffer
    // enough rows to allow all other fragments to be released (if result spooling is
    // enabled this is especially true, but even without spooling many queries have a
    // coordinator fragment that buffers multiple RowBatches). If the client does not
    // fetch all rows immediately, then the Coordinator Backend will be long lived
    // compared to the rest of the Backends.
    bool is_coordinator_the_last_unreleased_backend =
        !released_coordinator_ && num_in_use_ == 1;

    // True if the 'Timed Release' heuristic should be triggered.
    bool release_backends_timeout_expired =
        released_timer_.ElapsedTime() > release_backend_states_delay_ns_;

    // True if the 'Batched Release' heuristic should be triggered.
    bool unreleased_backend_threshold_reached = num_pending_
        >= std::max(floor(num_backends_ / batched_release_decay_value_), 1.0);

    // If no Backends are running or if only the Coordinator Backend is running or if both
    // the 'Timed Release' and 'Batched Release' heuristic are true, then transition all
    // PENDING BackendStates to RELEASABLE and update any state necessary for the
    // heuristics.
    if (is_coordinator_the_last_unreleased_backend
        || (release_backends_timeout_expired && unreleased_backend_threshold_reached)) {
      released_timer_.Reset();
      batched_release_decay_value_ *= FLAGS_batched_release_decay_factor;
      for (auto backend_resource_state : backend_resource_states_) {
        if (backend_resource_state.second == ResourceState::PENDING) {
          releasable_backend_states->push_back(backend_resource_state.first);
          backend_resource_states_[backend_resource_state.first] =
              ResourceState::RELEASABLE;
          --num_pending_;
        }
      }
      DCHECK_GE(num_pending_, 0);
    }
  }
}

void Coordinator::BackendResourceState::BackendsReleased(
    const vector<BackendState*>& released_backend_states) {
  lock_guard<SpinLock> lock(lock_);
  // Mark all given BackendStates as RELEASED. A BackendState must be either RELEASABLE
  // or IN_USE before it can marked as RELEASED.
  for (auto backend_state : released_backend_states) {
    DCHECK_NE(backend_resource_states_.at(backend_state), ResourceState::RELEASED);
    if (backend_resource_states_.at(backend_state) == ResourceState::IN_USE) {
      --num_in_use_;
    }
    backend_resource_states_.at(backend_state) = ResourceState::RELEASED;
    // If the Backend running the Coordinator has completed and been released, set
    // released_coordinator_ to true.
    if (backend_state->exec_params().is_coord_backend()) {
      released_coordinator_ = true;
    }
  }
  num_released_ += released_backend_states.size();
}

vector<Coordinator::BackendState*>
Coordinator::BackendResourceState::CloseAndGetUnreleasedBackends() {
  lock_guard<SpinLock> lock(lock_);
  vector<BackendState*> unreleased_backend_states;
  for (auto backend_resource_state : backend_resource_states_) {
    if (backend_resource_state.second == ResourceState::IN_USE
        || backend_resource_state.second == ResourceState::PENDING) {
      unreleased_backend_states.push_back(backend_resource_state.first);
    }
  }
  closed_ = true;
  return unreleased_backend_states;
}
