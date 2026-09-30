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

#include <cerrno>

#include "common/names.h"
#include "common/object-pool.h"
#include "common/status-serialization.h"
#include "gen-cpp/ExecStats_types.h"
#include "gen-cpp/control_service.pb.h"
#include "runtime/coordinator-backend-state.h"
#include "testutil/gtest-util.h"
#include "util/network-util.h"
#include "util/runtime-profile.h"
#include "util/time.h"
#include "util/uid-util.h"


DECLARE_uint64(release_backend_states_delay_ms);

namespace impala {

class CoordinatorBackendStateTest : public testing::Test {
 protected:
  // Amount of time to wait, in milliseconds, to trigger the 'Timed Release' condition.
  const int64_t timeout_release_ms_ = 1.5 * FLAGS_release_backend_states_delay_ms;

  // Pool for objects to be destroyed during test teardown.
  ObjectPool pool_;

  /// Utility function to create a specified number of dummy BackendStates and add them
  /// to the supplied vector. 'coordinator_backend' points to the Backend running the
  /// Coordinator. The 'coordinator_backend' is added to the supplied vector as well.
  void MakeBackendStates(int num_states, Coordinator::BackendState** coordinator_backend,
      std::vector<Coordinator::BackendState*>* backend_states) {
    UniqueIdPB* query_id = pool_.Add(new UniqueIdPB());
    TExecRequest* request = pool_.Add(new TExecRequest());
    QuerySchedulePB* query_schedule = pool_.Add(new QuerySchedulePB());
    QueryExecParams* exec_params =
        pool_.Add(new QueryExecParams(*query_id, *request, *query_schedule));

    for (int i = 0; i < num_states; ++i) {
      BackendExecParamsPB* backend_exec_params =
          query_schedule->add_backend_exec_params();
      Coordinator::BackendState* backend_state = pool_.Add(new Coordinator::BackendState(
          *exec_params, i, TRuntimeFilterMode::OFF, *backend_exec_params));
      backend_states->push_back(backend_state);
      // Mark the first BackendState as the Coordinator Backend.
      if (i == 0) {
        backend_exec_params->set_is_coord_backend(true);
        *coordinator_backend = backend_state;
      }
    }
  }

  /// Adds a fragment instance to backend_state. A negative completion_time_ns leaves
  /// the instance's completion time unset.
  void AddInstanceStats(Coordinator::BackendState* backend_state,
      Coordinator::FragmentStats* fragment_stats, int64_t completion_time_ns) {
    FInstanceExecParamsPB* params = pool_.Add(new FInstanceExecParamsPB());
    auto* instance_stats = pool_.Add(new Coordinator::BackendState::InstanceStats(
        *params, nullptr, NetworkAddressPB(), fragment_stats, &pool_,
        /*aggregated_profile=*/true));
    if (completion_time_ns >= 0) {
      instance_stats->completion_time_ns_ = completion_time_ns;
    }
    int idx = backend_state->instance_stats_map_.size();
    backend_state->instance_stats_map_.emplace(idx, instance_stats);
  }

  /// Returns an exec summary node with 'label' and one instance per entry in
  /// 'latencies_ns'.
  static TPlanNodeExecSummary MakeNodeSummary(
      const string& label, const vector<int64_t>& latencies_ns) {
    TPlanNodeExecSummary node;
    node.__set_label(label);
    for (int64_t latency_ns : latencies_ns) {
      TExecStats stats;
      stats.__set_latency_ns(latency_ns);
      node.exec_stats.push_back(stats);
    }
    node.__isset.exec_stats = !latencies_ns.empty();
    return node;
  }

  /// State updated by ApplyReport().
  Coordinator::ExecSummary exec_summary_;
  ProgressUpdater scan_progress_;
  ProgressUpdater query_progress_;
  DmlExecState dml_exec_state_;
  std::vector<Coordinator::FragmentStats*> report_fragment_stats_;
  UniqueIdPB report_instance_id_;

  /// Returns a BackendState with one fragment instance of fragment 0 whose exec RPC
  /// succeeded, and sets up the state that ApplyReport() updates. The exec RPC is not
  /// marked as sent, so Cancel() only marks the backend done.
  Coordinator::BackendState* MakeReportingBackendState() {
    UniqueIdPB* query_id = pool_.Add(new UniqueIdPB());
    TExecRequest* request = pool_.Add(new TExecRequest());
    // Use the aggregated profile so that reports don't need instance profiles.
    request->query_options.__set_aggregated_profile(true);
    QuerySchedulePB* query_schedule = pool_.Add(new QuerySchedulePB());
    QueryExecParams* exec_params =
        pool_.Add(new QueryExecParams(*query_id, *request, *query_schedule));
    BackendExecParamsPB* backend_exec_params = query_schedule->add_backend_exec_params();
    FInstanceExecParamsPB* instance_params = backend_exec_params->add_instance_params();
    report_instance_id_.set_hi(1);
    report_instance_id_.set_lo(1);
    *instance_params->mutable_instance_id() = report_instance_id_;
    instance_params->set_fragment_idx(0);
    instance_params->set_per_fragment_instance_idx(0);
    Coordinator::BackendState* backend_state = pool_.Add(new Coordinator::BackendState(
        *exec_params, 0, TRuntimeFilterMode::OFF, *backend_exec_params));

    Coordinator::FragmentStats* fragment_stats = pool_.Add(new Coordinator::FragmentStats(
        "Averaged Fragment F00", "Fragment F00", 1, &pool_, /*aggregated_profile=*/true));
    report_fragment_stats_ = {fragment_stats};
    auto* instance_stats = pool_.Add(new Coordinator::BackendState::InstanceStats(
        *instance_params, nullptr, NetworkAddressPB(), fragment_stats, &pool_,
        /*aggregated_profile=*/true));
    backend_state->instance_stats_map_.emplace(
        GetInstanceIdx(report_instance_id_), instance_stats);

    // Same as a successful ExecCompleteCb().
    backend_state->exec_start_time_ns_ = MonotonicStopWatch::Now();
    instance_stats->stopwatch_.Start();
    backend_state->exec_done_ = true;

    // The instance reports exec stats for plan node 0.
    TPlanNodeExecSummary node;
    node.__set_node_id(0);
    node.__set_fragment_idx(0);
    node.exec_stats.resize(1);
    exec_summary_.thrift_exec_summary.nodes.push_back(node);
    exec_summary_.thrift_exec_summary.__isset.nodes = true;
    exec_summary_.node_id_to_idx_map[0] = 0;
    scan_progress_.Init("Scan ranges", 0);
    query_progress_.Init("Fragment instances", 1);
    return backend_state;
  }

  /// Returns a report for the instance created by MakeReportingBackendState(), with
  /// 'latency_ns' as the local time of plan node 0. If 'with_aux_error' is true, the
  /// report includes a failed RPC to another node.
  ReportExecStatusRequestPB MakeReport(int64_t seq_no, bool done, int64_t latency_ns,
      bool with_aux_error = false) const {
    ReportExecStatusRequestPB report;
    report.set_backend_report_seq_no(seq_no);
    StatusToProto(Status::OK(), report.mutable_overall_status());
    FragmentInstanceExecStatusPB* instance_status = report.add_instance_exec_status();
    *instance_status->mutable_fragment_instance_id() = report_instance_id_;
    instance_status->set_report_seq_no(seq_no);
    instance_status->set_done(done);
    ExecSummaryDataPB* exec_summary_data = instance_status->add_exec_summary_data();
    exec_summary_data->set_plan_node_id(0);
    exec_summary_data->set_local_time_ns(latency_ns);
    if (with_aux_error) {
      StatefulStatusPB* stateful_report = instance_status->add_stateful_report();
      stateful_report->set_report_seq_no(seq_no);
      RPCErrorInfoPB* rpc_error_info =
          stateful_report->mutable_aux_error_info()->mutable_rpc_error_info();
      rpc_error_info->mutable_dest_node()->set_hostname("other-host");
      rpc_error_info->mutable_dest_node()->set_port(27000);
      rpc_error_info->set_posix_error_code(ECONNRESET);
    }
    return report;
  }

  /// Applies 'report' to 'backend_state' and returns the result of
  /// ApplyExecStatusReport(). Collected aux error info is appended to 'aux_error_info'
  /// if it is not null.
  bool ApplyReport(Coordinator::BackendState* backend_state,
      const ReportExecStatusRequestPB& report,
      vector<AuxErrorInfoPB>* aux_error_info = nullptr) {
    vector<AuxErrorInfoPB> ignored_aux_error_info;
    TRuntimeProfileForest thrift_profiles;
    return backend_state->ApplyExecStatusReport(report, thrift_profiles, &exec_summary_,
        &scan_progress_, &query_progress_, &dml_exec_state_,
        aux_error_info != nullptr ? aux_error_info : &ignored_aux_error_info,
        report_fragment_stats_);
  }

  /// Returns the latency_ns of plan node 0 in 'exec_summary_'.
  int64_t ReportedLatencyNs() const {
    return exec_summary_.thrift_exec_summary.nodes[0].exec_stats[0].latency_ns;
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
  std::vector<Coordinator::BackendState*> backend_states;
  Coordinator::BackendState* coordinator_backend = nullptr;
  MakeBackendStates(num_backends, &coordinator_backend, &backend_states);
  Coordinator::BackendResourceState* backend_resource_state =
      pool_.Add(new Coordinator::BackendResourceState(backend_states));

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
  std::vector<Coordinator::BackendState*> backend_states;
  Coordinator::BackendState* coordinator_backend = nullptr;
  MakeBackendStates(num_backends, &coordinator_backend, &backend_states);
  Coordinator::BackendResourceState* backend_resource_state =
      pool_.Add(new Coordinator::BackendResourceState(backend_states));

  // Create a vector of non-Coordinator Backends.
  std::vector<Coordinator::BackendState*> non_coord_backend_states;
  for (auto backend_state : backend_states) {
    if (!backend_state->exec_params().is_coord_backend()) {
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
  std::vector<Coordinator::BackendState*> backend_states;
  Coordinator::BackendState* coordinator_backend = nullptr;
  MakeBackendStates(num_backends, &coordinator_backend, &backend_states);
  Coordinator::BackendResourceState* backend_resource_state =
      pool_.Add(new Coordinator::BackendResourceState(backend_states));

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
  std::vector<Coordinator::BackendState*> backend_states;
  Coordinator::BackendState* coordinator_backend = nullptr;
  MakeBackendStates(num_backends, &coordinator_backend, &backend_states);
  Coordinator::BackendResourceState* backend_resource_state =
      pool_.Add(new Coordinator::BackendResourceState(backend_states));

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

/// Validate that GetMaxInstanceElapsedTime() returns the longest execution time across
/// the Backend's instances, using the time since the instances were started for those
/// that have not completed.
TEST_F(CoordinatorBackendStateTest, MaxInstanceElapsedTime) {
  std::vector<Coordinator::BackendState*> backend_states;
  Coordinator::BackendState* backend_state = nullptr;
  MakeBackendStates(1, &backend_state, &backend_states);
  Coordinator::FragmentStats* fragment_stats = pool_.Add(new Coordinator::FragmentStats(
      "Averaged Fragment F00", "Fragment F00", 3, &pool_, /*aggregated_profile=*/true));

  EXPECT_EQ(0, backend_state->GetMaxInstanceElapsedTime(5000));
  AddInstanceStats(backend_state, fragment_stats, -1);
  // The instances have not been started yet.
  EXPECT_EQ(0, backend_state->GetMaxInstanceElapsedTime(5000));

  backend_state->exec_start_time_ns_ = 1000;
  // Dummy backends have no instance params, so num_remaining_instances_ starts at 0 and
  // IsDone() is true. Simulate a backend that is still executing.
  backend_state->num_remaining_instances_ = 1;
  EXPECT_EQ(500, backend_state->GetMaxInstanceElapsedTime(1500));
  AddInstanceStats(backend_state, fragment_stats, 1200);
  AddInstanceStats(backend_state, fragment_stats, 300);
  EXPECT_EQ(1200, backend_state->GetMaxInstanceElapsedTime(1500));
  EXPECT_EQ(2000, backend_state->GetMaxInstanceElapsedTime(3000));
}

/// After the backend is done, unset instance completion times are frozen so
/// exec_wall_time_ns does not grow with later profile reads.
TEST_F(CoordinatorBackendStateTest, ExecWallTimeFrozenWhenBackendDone) {
  std::vector<Coordinator::BackendState*> backend_states;
  Coordinator::BackendState* backend_state = nullptr;
  MakeBackendStates(1, &backend_state, &backend_states);
  Coordinator::FragmentStats* fragment_stats = pool_.Add(new Coordinator::FragmentStats(
      "Averaged Fragment F00", "Fragment F00", 3, &pool_, /*aggregated_profile=*/true));

  backend_state->exec_start_time_ns_ = 1000;
  AddInstanceStats(backend_state, fragment_stats, -1);
  Coordinator::BackendState::InstanceStats* instance_stats =
      backend_state->instance_stats_map_.begin()->second;
  instance_stats->stopwatch_.Start();
  instance_stats->stopwatch_.Stop();

  unique_lock<mutex> l(backend_state->lock_);
  backend_state->num_remaining_instances_ = 0;
  backend_state->FreezeUnsetInstanceCompletionTimesLocked(l);
  l.unlock();

  EXPECT_GE(instance_stats->frozen_elapsed_time_ns_, 0);
  EXPECT_EQ(-1, instance_stats->completion_time_ns_);
  const int64_t elapsed = backend_state->GetMaxInstanceElapsedTime(5000);
  EXPECT_EQ(elapsed, backend_state->GetMaxInstanceElapsedTime(5000000));
  EXPECT_GT(elapsed, 0);
}

/// Nodes with preallocated exec stats but no reported latency_ns are skipped.
TEST_F(CoordinatorBackendStateTest, QueryTimeFractionsUnsetLatency) {
  TExecSummary summary;
  summary.nodes.push_back(MakeNodeSummary("00:SCAN HDFS", {500}));
  TPlanNodeExecSummary unreported;
  unreported.__set_label("01:AGGREGATE");
  unreported.exec_stats.resize(2);
  unreported.__isset.exec_stats = true;
  summary.nodes.push_back(unreported);
  summary.__isset.nodes = true;

  vector<Coordinator::BottleneckEntry> entries;
  Coordinator::SetQueryTimeFractions(1000, 0.0, 10, &summary, &entries);
  ASSERT_TRUE(summary.nodes[0].__isset.query_time_frac);
  EXPECT_FALSE(summary.nodes[1].__isset.query_time_frac);
  ASSERT_EQ(1, entries.size());
  EXPECT_EQ("00:SCAN HDFS", entries[0].label);
}

/// Nodes with only zero reported latency_ns are treated like unreported latency.
TEST_F(CoordinatorBackendStateTest, QueryTimeFractionsZeroLatency) {
  TExecSummary summary;
  summary.nodes.push_back(MakeNodeSummary("00:SCAN HDFS", {500}));
  summary.nodes.push_back(MakeNodeSummary("01:AGGREGATE", {0}));
  summary.__isset.nodes = true;

  vector<Coordinator::BottleneckEntry> entries;
  Coordinator::SetQueryTimeFractions(1000, 0.0, 10, &summary, &entries);
  ASSERT_TRUE(summary.nodes[0].__isset.query_time_frac);
  EXPECT_FALSE(summary.nodes[1].__isset.query_time_frac);
  ASSERT_EQ(1, entries.size());
}

/// Validate that SetQueryTimeFractions() uses the slowest instance of each node, clamps
/// the fraction and leaves the summary unchanged without an execution time.
TEST_F(CoordinatorBackendStateTest, QueryTimeFractions) {
  TExecSummary summary;
  summary.nodes.push_back(MakeNodeSummary("00:SCAN HDFS", {200, 600}));
  // Instance time above the wall time (e.g. clock differences) is clamped to 1.
  summary.nodes.push_back(MakeNodeSummary("01:AGGREGATE", {1500}));
  // Nodes without exec stats are left unset.
  summary.nodes.push_back(MakeNodeSummary("02:EXCHANGE", {}));
  summary.__isset.nodes = true;

  vector<Coordinator::BottleneckEntry> entries;
  TExecSummary no_wall_time = summary;
  Coordinator::SetQueryTimeFractions(0, 0.05, 3, &no_wall_time, &entries);
  EXPECT_FALSE(no_wall_time.__isset.exec_wall_time_ns);
  EXPECT_FALSE(no_wall_time.nodes[0].__isset.query_time_frac);

  Coordinator::SetQueryTimeFractions(1000, 0.05, 3, &summary, &entries);
  EXPECT_EQ(1000, summary.exec_wall_time_ns);
  ASSERT_TRUE(summary.nodes[0].__isset.query_time_frac);
  EXPECT_DOUBLE_EQ(0.6, summary.nodes[0].query_time_frac);
  ASSERT_TRUE(summary.nodes[1].__isset.query_time_frac);
  EXPECT_DOUBLE_EQ(1.0, summary.nodes[1].query_time_frac);
  EXPECT_FALSE(summary.nodes[2].__isset.query_time_frac);
}

/// Validate that SetQueryTimeFractions() ranks bottlenecks by query_time_frac, drops
/// nodes below the threshold or without exec stats, and caps the number of entries.
TEST_F(CoordinatorBackendStateTest, BottleneckSelection) {
  TExecSummary summary;
  summary.nodes.push_back(MakeNodeSummary("00:SCAN HDFS", {440}));
  summary.nodes.push_back(MakeNodeSummary("01:SCAN HDFS", {80}));
  summary.nodes.push_back(MakeNodeSummary("02:HASH JOIN", {600}));
  summary.nodes.push_back(MakeNodeSummary("03:AGGREGATE", {10}));
  summary.nodes.push_back(MakeNodeSummary("04:SCAN HDFS", {440}));
  summary.nodes.push_back(MakeNodeSummary("05:EXCHANGE", {}));
  summary.__isset.nodes = true;

  vector<Coordinator::BottleneckEntry> entries;
  Coordinator::SetQueryTimeFractions(1000, 0.05, 3, &summary, &entries);
  // Nodes with equal fractions keep their plan order.
  ASSERT_EQ(3, entries.size());
  EXPECT_EQ("02:HASH JOIN", entries[0].label);
  EXPECT_DOUBLE_EQ(0.6, entries[0].query_time_frac);
  EXPECT_EQ("00:SCAN HDFS", entries[1].label);
  EXPECT_EQ("04:SCAN HDFS", entries[2].label);

  Coordinator::SetQueryTimeFractions(1000, 0.05, 10, &summary, &entries);
  ASSERT_EQ(4, entries.size());
  EXPECT_EQ("01:SCAN HDFS", entries[3].label);

  Coordinator::SetQueryTimeFractions(1000, 0.05, 1, &summary, &entries);
  EXPECT_EQ(1, entries.size());

  // A max of 0 entries disables the summary.
  Coordinator::SetQueryTimeFractions(1000, 0.05, 0, &summary, &entries);
  EXPECT_TRUE(entries.empty());

  // A fraction equal to the threshold qualifies.
  Coordinator::SetQueryTimeFractions(1000, 0.08, 10, &summary, &entries);
  EXPECT_EQ(4, entries.size());

  Coordinator::SetQueryTimeFractions(1000, 0.9, 3, &summary, &entries);
  EXPECT_TRUE(entries.empty());

  // Entries from a previous call are cleared when there is no execution time yet.
  Coordinator::SetQueryTimeFractions(1000, 0.05, 3, &summary, &entries);
  ASSERT_FALSE(entries.empty());
  Coordinator::SetQueryTimeFractions(0, 0.05, 3, &summary, &entries);
  EXPECT_TRUE(entries.empty());
}

/// Validate that min_query_time_frac=0 lists every node with exec stats, including those
/// with a fraction below the default threshold.
TEST_F(CoordinatorBackendStateTest, MinQueryTimeFracZero) {
  TExecSummary summary;
  summary.nodes.push_back(MakeNodeSummary("00:SCAN HDFS", {440}));
  summary.nodes.push_back(MakeNodeSummary("01:SCAN HDFS", {80}));
  summary.nodes.push_back(MakeNodeSummary("02:HASH JOIN", {600}));
  summary.nodes.push_back(MakeNodeSummary("03:AGGREGATE", {10}));
  summary.nodes.push_back(MakeNodeSummary("04:SCAN HDFS", {440}));
  summary.nodes.push_back(MakeNodeSummary("05:EXCHANGE", {}));
  summary.__isset.nodes = true;

  vector<Coordinator::BottleneckEntry> entries;
  Coordinator::SetQueryTimeFractions(1000, 0.05, 10, &summary, &entries);
  EXPECT_EQ(4, entries.size());

  Coordinator::SetQueryTimeFractions(1000, 0.0, 10, &summary, &entries);
  EXPECT_EQ(5, entries.size());
  EXPECT_EQ("03:AGGREGATE", entries.back().label);
  EXPECT_DOUBLE_EQ(0.01, entries.back().query_time_frac);
}

/// A retried final report is applied, but only the report that makes the backend done
/// returns true, so the completion barrier is notified once for the backend.
TEST_F(CoordinatorBackendStateTest, DuplicateFinalReportReturnsTrueOnce) {
  Coordinator::BackendState* backend_state = MakeReportingBackendState();
  Coordinator::BackendState::InstanceStats* instance_stats =
      backend_state->instance_stats_map_.begin()->second;

  EXPECT_FALSE(ApplyReport(backend_state, MakeReport(1, /*done=*/false, 100)));
  EXPECT_FALSE(backend_state->IsDone());

  ReportExecStatusRequestPB final_report = MakeReport(2, /*done=*/true, 200);
  EXPECT_TRUE(ApplyReport(backend_state, final_report));
  EXPECT_TRUE(backend_state->IsDone());
  EXPECT_TRUE(instance_stats->done_);

  // The same report again, e.g. resent because the response to it was lost.
  EXPECT_FALSE(ApplyReport(backend_state, final_report));
  EXPECT_TRUE(backend_state->IsDone());
  EXPECT_EQ(2, instance_stats->last_report_seq_no_);
  EXPECT_EQ(1, query_progress_.num_complete());
  EXPECT_EQ(200, ReportedLatencyNs());
}

/// A backend cancelled with fire_and_forget is done before its final report arrives.
/// The final report is still applied, but returns false and collects no aux error info,
/// so it neither notifies the completion barrier again nor blacklists nodes.
TEST_F(CoordinatorBackendStateTest, ReportAfterCancelIsAppliedButNotCounted) {
  Coordinator::BackendState* backend_state = MakeReportingBackendState();
  Coordinator::BackendState::InstanceStats* instance_stats =
      backend_state->instance_stats_map_.begin()->second;

  // Aux error info is collected while the backend is running.
  vector<AuxErrorInfoPB> aux_error_info;
  EXPECT_FALSE(ApplyReport(backend_state,
      MakeReport(1, /*done=*/false, 100, /*with_aux_error=*/true), &aux_error_info));
  EXPECT_EQ(1, aux_error_info.size());

  EXPECT_TRUE(backend_state->Cancel(/*fire_and_forget=*/true).became_done);
  EXPECT_TRUE(backend_state->IsDone());

  aux_error_info.clear();
  EXPECT_FALSE(ApplyReport(backend_state,
      MakeReport(2, /*done=*/true, 300, /*with_aux_error=*/true), &aux_error_info));
  EXPECT_TRUE(aux_error_info.empty());
  EXPECT_TRUE(instance_stats->done_);
  EXPECT_EQ(2, instance_stats->last_report_seq_no_);
  EXPECT_EQ(1, query_progress_.num_complete());
  EXPECT_EQ(300, ReportedLatencyNs());
}

/// After CloseStatusReports(), reports are rejected and change nothing.
TEST_F(CoordinatorBackendStateTest, ReportsRejectedAfterClose) {
  Coordinator::BackendState* backend_state = MakeReportingBackendState();
  Coordinator::BackendState::InstanceStats* instance_stats =
      backend_state->instance_stats_map_.begin()->second;

  EXPECT_FALSE(ApplyReport(backend_state, MakeReport(1, /*done=*/false, 100)));
  backend_state->CloseStatusReports();
  EXPECT_FALSE(ApplyReport(backend_state, MakeReport(2, /*done=*/true, 200)));
  EXPECT_FALSE(backend_state->IsDone());
  EXPECT_FALSE(instance_stats->done_);
  EXPECT_EQ(1, instance_stats->last_report_seq_no_);
  EXPECT_EQ(0, query_progress_.num_complete());
  EXPECT_EQ(100, ReportedLatencyNs());
}

/// If a backend is cancelled while an instance is still running, the completion time
/// recorded when the summary is finalized equals the time frozen at cancellation, so
/// exec wall time does not change at finalization.
TEST_F(CoordinatorBackendStateTest, FinalizedCompletionTimeMatchesFrozenTime) {
  Coordinator::BackendState* backend_state = MakeReportingBackendState();
  Coordinator::BackendState::InstanceStats* instance_stats =
      backend_state->instance_stats_map_.begin()->second;

  EXPECT_FALSE(ApplyReport(backend_state, MakeReport(1, /*done=*/false, 100)));
  EXPECT_TRUE(backend_state->Cancel(/*fire_and_forget=*/true).became_done);
  const int64_t frozen_ns = instance_stats->frozen_elapsed_time_ns_;
  ASSERT_GE(frozen_ns, 0);
  EXPECT_EQ(-1, instance_stats->completion_time_ns_);
  const int64_t wall_time_ns =
      backend_state->GetMaxInstanceElapsedTime(MonotonicStopWatch::Now());
  EXPECT_EQ(frozen_ns, wall_time_ns);

  // Without stopping the stopwatch at the freeze, the completion time would include
  // this delay.
  SleepForMs(10);
  backend_state->CloseStatusReports();
  backend_state->UpdateExecStats(report_fragment_stats_, /*finalize=*/true);
  EXPECT_EQ(frozen_ns, instance_stats->completion_time_ns_);
  EXPECT_EQ(wall_time_ns,
      backend_state->GetMaxInstanceElapsedTime(MonotonicStopWatch::Now()));
}

class FinalizeCountersAfterFreezeTest : public CoordinatorBackendStateTest,
    public ::testing::WithParamInterface<bool> {};

/// Freezing elapsed time for exec wall time must not set completion_time_ns_, so a later
/// UpdateExecStats(..., finalize=true) still adds CompletionTime and ExecutionRate.
TEST_P(FinalizeCountersAfterFreezeTest, BothProfileFormats) {
  const bool aggregated_profile = GetParam();
  std::vector<Coordinator::BackendState*> backend_states;
  Coordinator::BackendState* backend_state = nullptr;
  MakeBackendStates(1, &backend_state, &backend_states);
  Coordinator::FragmentStats* fragment_stats = pool_.Add(new Coordinator::FragmentStats(
      "Averaged Fragment F00", "Fragment F00", 1, &pool_, aggregated_profile));
  FInstanceExecParamsPB* params = pool_.Add(new FInstanceExecParamsPB());
  params->set_fragment_idx(0);
  Coordinator::BackendState::InstanceStats* instance_stats = pool_.Add(
      new Coordinator::BackendState::InstanceStats(*params, nullptr, NetworkAddressPB(),
          fragment_stats, &pool_, aggregated_profile));
  instance_stats->total_split_size_ = 1024;
  instance_stats->stopwatch_.Start();
  instance_stats->stopwatch_.Stop();
  instance_stats->done_ = true;
  backend_state->instance_stats_map_.emplace(0, instance_stats);

  unique_lock<mutex> l(backend_state->lock_);
  backend_state->num_remaining_instances_ = 0;
  backend_state->FreezeUnsetInstanceCompletionTimesLocked(l);
  l.unlock();

  ASSERT_GE(instance_stats->frozen_elapsed_time_ns_, 0);
  EXPECT_EQ(-1, instance_stats->completion_time_ns_);

  vector<Coordinator::FragmentStats*> fragment_stats_vec = {fragment_stats};
  instance_stats->UpdateExecStats(fragment_stats_vec, /*finalize=*/true);

  EXPECT_EQ(instance_stats->completion_time_ns_,
      instance_stats->frozen_elapsed_time_ns_);
  RuntimeProfile::Counter* completion_time =
      instance_stats->profile_->GetCounter("CompletionTime");
  ASSERT_NE(nullptr, completion_time);
  EXPECT_EQ(instance_stats->completion_time_ns_, completion_time->value());
  RuntimeProfile::Counter* execution_rate =
      instance_stats->profile_->GetCounter("ExecutionRate");
  ASSERT_NE(nullptr, execution_rate);
  EXPECT_GT(execution_rate->value(), 0);
}

INSTANTIATE_TEST_SUITE_P(
    BothFormats, FinalizeCountersAfterFreezeTest, ::testing::Values(false, true));
} // namespace impala
