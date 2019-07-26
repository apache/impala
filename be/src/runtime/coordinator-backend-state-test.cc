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

#include "runtime/coordinator.h"

#include "common/names.h"
#include "common/object-pool.h"
#include "runtime/coordinator-backend-state.h"
#include "testutil/gtest-util.h"
#include "util/network-util.h"
#include "util/runtime-profile.h"

DECLARE_uint64(release_backend_states_delay_ms);

namespace impala {

class CoordinatorBackendStateTest : public testing::Test {
 protected:
  // Amount of time to wait, in milliseconds, to trigger the 'Timed Release' condition.
  const int64_t timeout_release_ms_ = 1.5 * FLAGS_release_backend_states_delay_ms;

  // Pool for objects to be destroyed during test teardown.
  ObjectPool pool_;

  RuntimeProfile* profile_ = nullptr;

  virtual void SetUp() { profile_ = RuntimeProfile::Create(&pool_, "pool1"); }

  /// Utility function to create a dummy QuerySchedule.
  void MakeQuerySchedule(QuerySchedule** query_schedule) {
    TUniqueId* query_id = pool_.Add(new TUniqueId());
    TQueryExecRequest* request = pool_.Add(new TQueryExecRequest());
    TQueryOptions* query_options = pool_.Add(new TQueryOptions());

    *query_schedule =
        pool_.Add(new QuerySchedule(*query_id, *request, *query_options, profile_));
  }

  /// Utility function to create a specified number of dummy BackendStates and add them
  /// to the supplied vector. 'coordinator_backend' points to the Backend running the
  /// Coordinator. The 'coordinator_backend' is added to the supplied vector as well.
  void MakeBackendStates(int num_states, QuerySchedule* query_schedule,
      Coordinator::BackendState** coordinator_backend,
      std::vector<Coordinator::BackendState*>* backend_states) {
    TQueryCtx* query_ctx = pool_.Add(new TQueryCtx());

    PerBackendExecParams* per_backend_exec_params = pool_.Add(new PerBackendExecParams());
    query_schedule->set_per_backend_exec_params(*per_backend_exec_params);

    for (int i = 0; i < num_states; ++i) {
      TNetworkAddress addr = MakeNetworkAddress(strings::Substitute("host-$0", i), 25000);
      BackendExecParams* backend_exec_params = pool_.Add(new BackendExecParams());
      per_backend_exec_params->emplace(addr, *backend_exec_params);
      Coordinator::BackendState* backend_state = pool_.Add(new Coordinator::BackendState(
          *query_schedule, *query_ctx, 0, TRuntimeFilterMode::OFF, *backend_exec_params));
      backend_states->push_back(backend_state);
      // Mark the first BackendState as the Coordinator Backend.
      if (i == 0) {
        backend_exec_params->is_coord_backend = true;
        *coordinator_backend = backend_state;
      }
    }
  }

  virtual void TearDown() { pool_.Clear(); }
};

/// Validate the state machine of BackendResourceState by testing that the following
/// conditions hold:
///     * All BackendStates are initially marked as IN_USE.
///     * Releasing all Backends transitions all Backends to the RELEASED state.
TEST_F(CoordinatorBackendStateTest, StateMachine) {
  // Initialize a BackendResourceState with three BackendStates.
  int num_backends = 3;
  QuerySchedule* query_schedule = nullptr;
  MakeQuerySchedule(&query_schedule);

  std::vector<Coordinator::BackendState*> backend_states;
  Coordinator::BackendState* coordinator_backend = nullptr;
  MakeBackendStates(num_backends, query_schedule, &coordinator_backend, &backend_states);
  Coordinator::BackendResourceState* backend_resource_state =
      pool_.Add(new Coordinator::BackendResourceState(backend_states, *query_schedule));

  // Assert that all BackendStates are initially in the IN_USE state.
  ASSERT_EQ(backend_resource_state->backend_resource_states_.size(), num_backends);
  for (auto backend_state : backend_resource_state->backend_resource_states_) {
    ASSERT_EQ(
        backend_state.second, Coordinator::BackendResourceState::ResourceState::IN_USE);
  }

  // Assert that releasing an empty vector of BackendStates does not transition any
  // BackendState to the RELEASED state.
  std::vector<Coordinator::BackendState*> empty_backend_states;
  backend_resource_state->BackendsReleased(empty_backend_states);
  ASSERT_TRUE(empty_backend_states.empty());
  ASSERT_EQ(backend_resource_state->backend_resource_states_.size(), num_backends);
  for (auto backend_state : backend_resource_state->backend_resource_states_) {
    ASSERT_EQ(
        backend_state.second, Coordinator::BackendResourceState::ResourceState::IN_USE);
  }

  // Assert that releasing all BackendStates transitions all BackendStates from
  // IN_USE to RELEASED.
  backend_resource_state->BackendsReleased(backend_states);
  std::vector<Coordinator::BackendState*> unreleased_backend_states =
      backend_resource_state->CloseAndGetUnreleasedBackends();
  ASSERT_EQ(unreleased_backend_states.size(), 0);
}

/// Validate the 'Coordinator Only' heuristic.
TEST_F(CoordinatorBackendStateTest, CoordinatorOnly) {
  // Initialize a BackendResourceState with eight BackendStates.
  int num_backends = 8;
  QuerySchedule* query_schedule = nullptr;
  MakeQuerySchedule(&query_schedule);

  std::vector<Coordinator::BackendState*> backend_states;
  Coordinator::BackendState* coordinator_backend = nullptr;
  MakeBackendStates(num_backends, query_schedule, &coordinator_backend, &backend_states);
  Coordinator::BackendResourceState* backend_resource_state =
      pool_.Add(new Coordinator::BackendResourceState(backend_states, *query_schedule));

  // Create a vector of non-Coordinator Backends.
  std::vector<Coordinator::BackendState*> non_coord_backend_states;
  for (auto backend_state : backend_states) {
    if (!backend_state->exec_params()->is_coord_backend) {
      non_coord_backend_states.push_back(backend_state);
    }
  }
  ASSERT_EQ(non_coord_backend_states.size(), num_backends - 1);

  // Release all but the last non-Coordinator Backend. Each call to MarkBackendFinished
  // should not cause any PENDING Backends to transition to RELEASABLE because the
  // 'Timeout Release' heuristic should not have be triggered.
  std::vector<Coordinator::BackendState*> releasable_backend_states;
  for (int i = 0; i < non_coord_backend_states.size() - 1; ++i) {
    backend_resource_state->MarkBackendFinished(
        non_coord_backend_states[i], &releasable_backend_states);
    ASSERT_EQ(releasable_backend_states.size(), 0);
  }

  // Release the last non-Coordinator Backend, this should trigger all PENDING Bbackends
  // to transition to the RELEASABLE state.
  backend_resource_state->MarkBackendFinished(
      non_coord_backend_states[non_coord_backend_states.size() - 1],
      &releasable_backend_states);

  // Assert that all non-Coordinator Backends are RELEASABLE and then mark them as
  // RELEASED.
  ASSERT_EQ(releasable_backend_states.size(), non_coord_backend_states.size());
  backend_resource_state->BackendsReleased(releasable_backend_states);

  // Release the remaining BackendStates.
  std::vector<Coordinator::BackendState*> unreleased_backend_states =
      backend_resource_state->CloseAndGetUnreleasedBackends();
  ASSERT_EQ(unreleased_backend_states.size(), 1);
  backend_resource_state->BackendsReleased(unreleased_backend_states);
}

/// Validate the 'Timed Release' heuristic.
TEST_F(CoordinatorBackendStateTest, TimedRelease) {
  // Initialize a BackendResourceState with eight BackendStates.
  int num_backends = 8;
  QuerySchedule* query_schedule = nullptr;
  MakeQuerySchedule(&query_schedule);

  std::vector<Coordinator::BackendState*> backend_states;
  Coordinator::BackendState* coordinator_backend = nullptr;
  MakeBackendStates(num_backends, query_schedule, &coordinator_backend, &backend_states);
  Coordinator::BackendResourceState* backend_resource_state =
      pool_.Add(new Coordinator::BackendResourceState(backend_states, *query_schedule));

  // Sleep until the 'Timed Release' timeout is hit.
  SleepForMs(timeout_release_ms_);

  // Mark half of the BackendStates as finished. Marking all Backends up to
  // num_backends / 2 - 1 should result in 0 releasable Backends, and marking the
  // num_backends / 2 Backend should result in num_backends / 2 releasable Backends.
  std::vector<Coordinator::BackendState*> releasable_backend_states;
  for (int i = 0; i < num_backends / 2 - 1; ++i) {
    backend_resource_state->MarkBackendFinished(
        backend_states.at(i), &releasable_backend_states);
    ASSERT_EQ(releasable_backend_states.size(), 0);
  }
  backend_resource_state->MarkBackendFinished(
      backend_states.at(num_backends / 2 - 1), &releasable_backend_states);

  // Assert that half of the BackendStates transitioned to RELEASABLE.
  ASSERT_EQ(releasable_backend_states.size(), num_backends / 2);

  // Release half of the BackendStates.
  backend_resource_state->BackendsReleased(releasable_backend_states);

  // Mark the remaining half of the BackendStates as finished and assert that no Backends
  // transition to RELEASABLE (the 'Timed Release' timeout should not be hit so no
  // backends should be released).
  releasable_backend_states.clear();
  for (int i = num_backends / 2; i < num_backends / 2 + num_backends / 4; ++i) {
    backend_resource_state->MarkBackendFinished(
        backend_states.at(i), &releasable_backend_states);
    ASSERT_EQ(releasable_backend_states.size(), 0);
  }

  // Release the remaining BackendStates.
  std::vector<Coordinator::BackendState*> unreleased_backend_states =
      backend_resource_state->CloseAndGetUnreleasedBackends();
  ASSERT_EQ(unreleased_backend_states.size(), num_backends / 2);
  backend_resource_state->BackendsReleased(unreleased_backend_states);
}

/// Validate the 'Batched Release' heuristic.
TEST_F(CoordinatorBackendStateTest, BatchedRelease) {
  // Initialize a BackendResouceState with 128 BackendStates.
  int num_backends = 128;
  QuerySchedule* query_schedule = nullptr;
  MakeQuerySchedule(&query_schedule);

  std::vector<Coordinator::BackendState*> backend_states;
  Coordinator::BackendState* coordinator_backend = nullptr;
  MakeBackendStates(num_backends, query_schedule, &coordinator_backend, &backend_states);
  Coordinator::BackendResourceState* backend_resource_state =
      pool_.Add(new Coordinator::BackendResourceState(backend_states, *query_schedule));

  // Sleep until the 'Timed Release' timeout is hit.
  SleepForMs(timeout_release_ms_);

  // Mark (num_backends / 2 - 1) of the BackendStates as finished. Marking all Backends
  // up to num_backends / 2 - 1 should result in 0 releasable Backends, and marking the
  // num_backends / 2 backend should result in num_backends / 2 releasable Backends.
  std::vector<Coordinator::BackendState*> releasable_backend_states;
  for (int i = 0; i < num_backends / 2 - 1; ++i) {
    backend_resource_state->MarkBackendFinished(
        backend_states.at(i), &releasable_backend_states);
    ASSERT_EQ(releasable_backend_states.size(), 0);
  }
  backend_resource_state->MarkBackendFinished(
      backend_states.at(num_backends / 2 - 1), &releasable_backend_states);

  // Assert that (num_backends / 2) of the BackendStates transitioned to RELEASABLE.
  ASSERT_EQ(releasable_backend_states.size(), num_backends / 2);

  // Release half of the BackendStates.
  backend_resource_state->BackendsReleased(releasable_backend_states);

  // Sleep until the 'Timed Release' timeout is hit.
  SleepForMs(timeout_release_ms_);

  // Mark (num_backends / 4 - 1) of the BackendStates as finished.
  releasable_backend_states.clear();
  for (int i = num_backends / 2; i < num_backends / 2 + num_backends / 4 - 1; ++i) {
    backend_resource_state->MarkBackendFinished(
        backend_states.at(i), &releasable_backend_states);
    ASSERT_EQ(releasable_backend_states.size(), 0);
  }
  backend_resource_state->MarkBackendFinished(
      backend_states.at(num_backends / 2 + num_backends / 4 - 1),
      &releasable_backend_states);

  // Assert that (num_backends / 4) of the BackendStates transitioned to RELEASABLE.
  ASSERT_EQ(releasable_backend_states.size(), num_backends / 4);

  // Release a fourth of the BackendStates.
  backend_resource_state->BackendsReleased(releasable_backend_states);

  // Release the remaining BackendStates
  std::vector<Coordinator::BackendState*> unreleased_backend_states =
      backend_resource_state->CloseAndGetUnreleasedBackends();
  ASSERT_EQ(unreleased_backend_states.size(),
      num_backends - num_backends / 2 - num_backends / 4);
  backend_resource_state->BackendsReleased(unreleased_backend_states);
}
} // namespace impala
