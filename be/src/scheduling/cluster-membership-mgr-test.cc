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

#include <memory>
#include <deque>

#include "common/logging.h"
#include "common/names.h"
#include "gen-cpp/StatestoreService_types.h"
#include "scheduling/cluster-membership-mgr.h"
#include "scheduling/cluster-membership-test-util.h"
#include "service/impala-server.h"
#include "testutil/gtest-util.h"
#include "testutil/rand-util.h"
#include "util/metrics.h"

using std::mt19937;
using std::uniform_int_distribution;
using std::uniform_real_distribution;
using namespace impala;
using namespace impala::test;

DECLARE_int32(statestore_max_missed_heartbeats);
DECLARE_int32(statestore_heartbeat_frequency_ms);
DECLARE_int32(num_expected_executors);
DECLARE_string(expected_executor_group_sets);

namespace impala {

/// This class and the following tests exercise the ClusterMembershipMgr core membership
/// handling code by simulating the interactions between multiple ClusterMembershipMgr
/// instances through the statestore. Updates between all cluster members are sent and
/// process sequentially and in a deterministic order.
///
/// The tests progress in 4 subsequently more sophisticated ways:
/// 1) Make sure that simple interactions between 2 backends and their
///    ClusterMembershipMgr work correctly.
/// 2) Run a cluster of backends through the regular lifecycle of a backend in lockstep.
/// 3) Make random but valid changes to the membership of a whole cluster for a given
///    number of iterations and observe that each member's state remains consistent.
/// 4) TODO: Make random, potentially invalid changes to the membership of a whole
///    cluster.
class ClusterMembershipMgrTest : public testing::Test {
 public:
  virtual void SetUp() {
    RandTestUtil::SeedRng("CLUSTER_MEMBERSHIP_MGR_TEST_SEED", &rng_);
  }

 protected:
  ClusterMembershipMgrTest() {}

  /// Returns the size of the default executor group of the current membership in 'cmm'
  /// if the default executor group exists, otherwise returns 0.
  int GetDefaultGroupSize(const ClusterMembershipMgr& cmm) const {
    const string& group_name = ImpalaServer::DEFAULT_EXECUTOR_GROUP_NAME;
    if (cmm.GetSnapshot()->executor_groups.find(group_name)
        == cmm.GetSnapshot()->executor_groups.end()) {
      return 0;
    }
    return cmm.GetSnapshot()->executor_groups.find(group_name)->second.NumExecutors();
  }

  /// A struct to hold information related to a simulated backend during the test.
  struct Backend {
    UniqueIdPB backend_id;
    std::unique_ptr<MetricGroup> metric_group;
    std::unique_ptr<ClusterMembershipMgr> cmm;
    std::shared_ptr<BackendDescriptorPB> desc;
  };
  /// The list of backends_ that owns all backends.
  vector<unique_ptr<Backend>> backends_;

  /// Various lists of pointers that point into elements of 'backends_'. These pointers
  /// will stay valid when backend_ resizes. Backends can be in one of 5 states:
  /// - "Offline": A BackendDescriptorPB has been created but has not been associated with
  ///     a ClusterMembershipMgr.
  /// - "Starting": A ClusterMembershipMgr has been created, but the associated backend
  ///     descriptor is not running yet (no callback registered with the
  ///     ClusterMembershipMgr).
  /// - "Running": The backend descriptor is available to the ClusterMembershipMgr via a
  ///     callback.
  /// - "Quiescing": The backend descriptor is marked as quiescing.
  /// - "Deleted": The backend has been deleted from 'backends_' altogether and other
  ///     backends have received statestore messages to notify them of the deletion. Note
  ///     that transition into this state does not update the affected backend itself, it
  ///     merely gets destructed.
  ///
  /// As part of the state transitions, all other backends are notified of the new state
  /// by calling their UpdateMembership() methods with a matching statestore update
  /// message.
  ///
  /// We keep backends in double ended queues to allow for efficient removal of elements
  /// on both ends.
  typedef deque<Backend*> Backends;

  bool IsInVector(const Backend* be, const Backends& backends) {
    return find(backends.begin(), backends.end(), be) != backends.end();
  }

  void RemoveFromVector(const Backend* be, Backends* backends) {
    auto it = find(backends->begin(), backends->end(), be);
    ASSERT_TRUE(it != backends->end());
    backends->erase(it);
    it = find(backends->begin(), backends->end(), be);
    ASSERT_TRUE(it == backends->end());
  }

  /// Lists that are used to track backends in a particular state. Methods that manipulate
  /// these lists maintain the invariant that a backend is only in one list at a time.

  /// Backends that are in state "Offline".
  Backends offline_;

  /// Backends that are in state "Starting".
  Backends starting_;

  /// Backends that are in state "Running".
  Backends running_;

  /// Backends that are in state "Quiescing".
  Backends quiescing_;

  typedef vector<TTopicDelta> TopicDeltas;

  /// Polls a backend for changes to its local state by sending an empty statestore update
  /// that is marked as a delta (i.e. it does not represent any changes).
  vector<TTopicDelta> Poll(Backend* be) {
    const Statestore::TopicId topic_id = Statestore::IMPALA_MEMBERSHIP_TOPIC;
    // The empty delta is used to poll subscribers for updates without sending new
    // changes.
    StatestoreSubscriber::TopicDeltaMap topic_delta_map = {{topic_id, TTopicDelta()}};
    TTopicDelta& empty_delta = topic_delta_map[topic_id];
    empty_delta.is_delta = true;
    vector<TTopicDelta> returned_topic_deltas;
    be->cmm->UpdateMembership(topic_delta_map, &returned_topic_deltas);
    return returned_topic_deltas;
  }

  /// Sends a single topic item in a delta to all backends in 'backend_'.
  void SendDelta(const TTopicItem& item) {
    const Statestore::TopicId topic_id = Statestore::IMPALA_MEMBERSHIP_TOPIC;
    StatestoreSubscriber::TopicDeltaMap topic_delta_map;
    TTopicDelta& delta = topic_delta_map[topic_id];
    delta.topic_entries.push_back(item);
    delta.is_delta = true;
    for (auto& backend : backends_) {
      vector<TTopicDelta> returned_topic_deltas;
      backend->cmm->UpdateMembership(topic_delta_map, &returned_topic_deltas);
      // We never expect backends to respond with a delta update on their own because the
      // test code explicitly polls them after making changes to their state.
      ASSERT_EQ(0, returned_topic_deltas.size())
        << "Error with backend " << backend->backend_id;
    }
  }

  /// Creates a new backend and adds it to the list of offline backends. If idx is
  /// omitted, the current number of backends will be used as the new index.
  Backend* CreateBackend(int idx = -1) {
    if (idx == -1) idx = backends_.size();
    backends_.push_back(make_unique<Backend>());
    auto& be = backends_.back();
    be->desc = make_shared<BackendDescriptorPB>(MakeBackendDescriptor(idx));
    be->backend_id = be->desc->backend_id();
    offline_.push_back(be.get());
    return be.get();
  }

  /// Creates a new ClusterMembershipMgr for a backend and moves the backend from
  /// 'offline_' to 'starting_'. Callers must handle invalidated iterators after calling
  /// this method.
  void CreateCMM(Backend* be) {
    ASSERT_TRUE(IsInVector(be, offline_));
    be->metric_group = make_unique<MetricGroup>("test");
    be->cmm = make_unique<ClusterMembershipMgr>(
        PrintId(be->backend_id), nullptr, be->metric_group.get());
    RemoveFromVector(be, &offline_);
    starting_.push_back(be);
  }

  /// Starts a backend by making its backend descriptor available to its
  /// ClusterMembershipMgr through a callback. This method also propagates the change to
  /// all other backends in 'backends_' and moves the backend from 'starting_' to
  /// 'running_'.
  void StartBackend(Backend* be) {
    ASSERT_TRUE(IsInVector(be, starting_));
    ASSERT_TRUE(be->cmm.get() != nullptr);
    ASSERT_TRUE(be->desc.get() != nullptr);
    auto be_cb = [be]() { return be->desc; };
    be->cmm->SetLocalBeDescFn(be_cb);

    // Poll to obtain topic update
    TopicDeltas topic_deltas = Poll(be);
    ASSERT_EQ(1, topic_deltas.size());
    ASSERT_EQ(1, topic_deltas[0].topic_entries.size());

    // Broadcast to all other backends
    SendDelta(topic_deltas[0].topic_entries[0]);

    RemoveFromVector(be, &starting_);
    running_.push_back(be);
  }

  /// Quiesces a backend by updating its backend descriptor and polling its
  /// ClusterMembershipMgr to make the change take effect. The resulting update from the
  /// backend's ClusterMembershipMgr is then broadcast to the rest of the cluster. Also
  /// moves the backend from 'running_' to 'quiescing_'.
  void QuiesceBackend(Backend* be) {
    ASSERT_TRUE(IsInVector(be, running_));
    be->desc->set_is_quiescing(true);
    TopicDeltas topic_deltas = Poll(be);
    ASSERT_EQ(1, topic_deltas.size());
    ASSERT_EQ(1, topic_deltas[0].topic_entries.size());

    // Broadcast to all other backends
    SendDelta(topic_deltas[0].topic_entries[0]);

    RemoveFromVector(be, &running_);
    quiescing_.push_back(be);
  }

  /// Deletes a backend from all other backends and from 'backends_'. A delta marking the
  /// backend as deleted gets sent to all nodes in 'backends_'. Note that this method does
  /// not send any updates to the backend itself, as it would - like in a real cluster -
  /// just disappear. The backend must be in 'running_' or 'quiescing_' and is removed
  /// from the respective list by this method.
  void DeleteBackend(Backend* be) {
    bool is_running = IsInVector(be, running_);
    bool is_quiescing = IsInVector(be, quiescing_);
    ASSERT_TRUE(is_running || is_quiescing);

    // Create topic item before erasing the backend.
    TTopicItem deletion_item;
    deletion_item.key = PrintId(be->backend_id);
    deletion_item.deleted = true;

    // Delete the backend
    auto new_end_it = std::remove_if(backends_.begin(), backends_.end(),
        [be](const unique_ptr<Backend>& elem) { return elem.get() == be; });
    backends_.erase(new_end_it, backends_.end());

    // Create deletion update
    SendDelta(deletion_item);

    if (is_running) RemoveFromVector(be, &running_);
    if (is_quiescing) RemoveFromVector(be, &quiescing_);
  }

  mt19937 rng_;

  int RandomInt(int max) {
    uniform_int_distribution<int> rand_int(0, max - 1);
    return rand_int(rng_);
  }

  double RandomDoubleFraction() {
    uniform_real_distribution<double> rand_double(0, 1);
    return rand_double(rng_);
  }
};

/// This test takes two instances of the ClusterMembershipMgr through a common lifecycle.
/// It also serves as an example for how to craft statestore messages and pass them to
/// UpdaUpdateMembership().
TEST_F(ClusterMembershipMgrTest, TwoInstances) {
  auto b1 = make_shared<BackendDescriptorPB>(MakeBackendDescriptor(1));
  auto b2 = make_shared<BackendDescriptorPB>(MakeBackendDescriptor(2));

  MetricGroup tmp_metrics1("test-metrics1");
  MetricGroup tmp_metrics2("test-metrics2");
  ClusterMembershipMgr cmm1(b1->address().hostname(), nullptr, &tmp_metrics1);
  ClusterMembershipMgr cmm2(b2->address().hostname(), nullptr, &tmp_metrics2);

  const Statestore::TopicId topic_id = Statestore::IMPALA_MEMBERSHIP_TOPIC;
  StatestoreSubscriber::TopicDeltaMap topic_delta_map = {{topic_id, TTopicDelta()}};
  TTopicDelta* ss_topic_delta = &topic_delta_map[topic_id];
  vector<TTopicDelta> returned_topic_deltas;
  // The empty delta is used to poll subscribers for updates without sending new changes.
  TTopicDelta empty_delta;
  empty_delta.is_delta = true;

  // Ping both managers, both should have no state to update
  cmm1.UpdateMembership(topic_delta_map, &returned_topic_deltas);
  ASSERT_EQ(0, returned_topic_deltas.size());

  cmm2.UpdateMembership(topic_delta_map, &returned_topic_deltas);
  ASSERT_EQ(0, returned_topic_deltas.size());

  // Hook up first callback and iterate again
  cmm1.SetLocalBeDescFn([b1]() { return b1; });
  cmm1.UpdateMembership(topic_delta_map, &returned_topic_deltas);
  ASSERT_EQ(1, returned_topic_deltas.size());

  // First manager now has one BE
  ASSERT_EQ(1, cmm1.GetSnapshot()->current_backends.size());

  // Hook up second callback and iterate with the result of the first manager
  cmm2.SetLocalBeDescFn([b2]() { return b2; });
  *ss_topic_delta = returned_topic_deltas[0];
  returned_topic_deltas.clear();
  cmm2.UpdateMembership(topic_delta_map, &returned_topic_deltas);
  ASSERT_EQ(1, returned_topic_deltas.size());
  ASSERT_EQ(2, cmm2.GetSnapshot()->current_backends.size());

  // Send the returned update to the first manager, this time no deltas will be returned
  *ss_topic_delta = returned_topic_deltas[0];
  returned_topic_deltas.clear();
  cmm1.UpdateMembership(topic_delta_map, &returned_topic_deltas);
  ASSERT_EQ(0, returned_topic_deltas.size());
  ASSERT_EQ(2, cmm1.GetSnapshot()->current_backends.size());

  // Both managers now have the same state. Shutdown one of them and step through
  // propagating the update.
  b1->set_is_quiescing(true);
  // Send an empty update to the 1st one to trigger propagation of the shutdown
  returned_topic_deltas.clear();
  topic_delta_map[topic_id] = empty_delta;
  cmm1.UpdateMembership(topic_delta_map, &returned_topic_deltas);
  // The mgr will return its changed BackendDescriptorPB
  ASSERT_EQ(1, returned_topic_deltas.size());
  // It will also remove itself from the executor group (but not the current backends).
  ASSERT_EQ(1, GetDefaultGroupSize(cmm1));
  ASSERT_EQ(2, cmm1.GetSnapshot()->current_backends.size());

  // Propagate the quiescing to the 2nd mgr
  *ss_topic_delta = returned_topic_deltas[0];
  returned_topic_deltas.clear();
  ASSERT_EQ(2, GetDefaultGroupSize(cmm2));
  cmm2.UpdateMembership(topic_delta_map, &returned_topic_deltas);
  ASSERT_EQ(0, returned_topic_deltas.size());
  ASSERT_EQ(2, cmm2.GetSnapshot()->current_backends.size());
  ASSERT_EQ(1, GetDefaultGroupSize(cmm2));

  // Delete the 1st backend from the 2nd one
  ASSERT_EQ(1, ss_topic_delta->topic_entries.size());
  ss_topic_delta->topic_entries[0].deleted = true;
  cmm2.UpdateMembership(topic_delta_map, &returned_topic_deltas);
  ASSERT_EQ(0, returned_topic_deltas.size());
  ASSERT_EQ(1, cmm2.GetSnapshot()->current_backends.size());
  ASSERT_EQ(1, GetDefaultGroupSize(cmm2));
}

TEST_F(ClusterMembershipMgrTest, IsBlacklisted) {
  const int NUM_BACKENDS = 2;
  for (int i = 0; i < NUM_BACKENDS; ++i) CreateBackend();
  EXPECT_EQ(NUM_BACKENDS, backends_.size());
  EXPECT_EQ(backends_.size(), offline_.size());

  while (!offline_.empty()) CreateCMM(offline_.front());
  EXPECT_EQ(0, offline_.size());
  EXPECT_EQ(NUM_BACKENDS, starting_.size());

  while (!starting_.empty()) StartBackend(starting_.front());
  EXPECT_EQ(0, starting_.size());
  EXPECT_EQ(NUM_BACKENDS, running_.size());

  backends_[0]->cmm->BlacklistExecutor(backends_[1]->desc->backend_id(), Status("error"));
  ClusterMembershipMgr::SnapshotPtr snapshot = backends_[0]->cmm->GetSnapshot();
  EXPECT_TRUE(snapshot->executor_blacklist.IsBlacklisted(*(backends_[1]->desc)));
}

// This test verifies the interaction between the ExecutorBlacklist and
// ClusterMembershipMgr.
TEST_F(ClusterMembershipMgrTest, ExecutorBlacklist) {
  // Set some flags to make the blacklist timeout fairly small (50ms);
  gflags::FlagSaver saver;
  FLAGS_statestore_max_missed_heartbeats = 5;
  FLAGS_statestore_heartbeat_frequency_ms = 10;
  const int BLACKLIST_TIMEOUT_SLEEP_US = 100000;

  const int NUM_BACKENDS = 3;
  for (int i = 0; i < NUM_BACKENDS; ++i) CreateBackend();
  EXPECT_EQ(NUM_BACKENDS, backends_.size());
  EXPECT_EQ(backends_.size(), offline_.size());

  while (!offline_.empty()) CreateCMM(offline_.front());
  EXPECT_EQ(0, offline_.size());
  EXPECT_EQ(NUM_BACKENDS, starting_.size());

  while (!starting_.empty()) StartBackend(starting_.front());
  EXPECT_EQ(0, starting_.size());
  EXPECT_EQ(NUM_BACKENDS, running_.size());

  // Assert that all backends know about each other and are all in the default executor
  // group.
  for (Backend* be : running_) {
    EXPECT_EQ(running_.size(), be->cmm->GetSnapshot()->current_backends.size());
    EXPECT_EQ(running_.size(), GetDefaultGroupSize(*be->cmm));
  }

  // Tell a BE to blacklist itself, should have no effect.
  backends_[0]->cmm->BlacklistExecutor(backends_[0]->desc->backend_id(), Status("error"));
  EXPECT_EQ(NUM_BACKENDS, backends_[0]->cmm->GetSnapshot()->current_backends.size());
  EXPECT_EQ(NUM_BACKENDS, GetDefaultGroupSize(*backends_[0]->cmm));

  // Tell a BE to blacklist another BE, should remove it from executor_groups but not
  // current_backends.
  backends_[0]->cmm->BlacklistExecutor(backends_[1]->desc->backend_id(), Status("error"));
  EXPECT_EQ(NUM_BACKENDS, backends_[0]->cmm->GetSnapshot()->current_backends.size());
  EXPECT_EQ(NUM_BACKENDS - 1, GetDefaultGroupSize(*backends_[0]->cmm));
  // Blacklist a BE that is already blacklisted. Should have no effect.
  backends_[0]->cmm->BlacklistExecutor(backends_[1]->desc->backend_id(), Status("error"));
  EXPECT_EQ(NUM_BACKENDS, backends_[0]->cmm->GetSnapshot()->current_backends.size());
  EXPECT_EQ(NUM_BACKENDS - 1, GetDefaultGroupSize(*backends_[0]->cmm));

  // Sleep and check the node has been un-blacklisted.
  usleep(BLACKLIST_TIMEOUT_SLEEP_US);
  EXPECT_EQ(Poll(backends_[0].get()).size(), 0);
  EXPECT_EQ(NUM_BACKENDS, backends_[0]->cmm->GetSnapshot()->current_backends.size());
  EXPECT_EQ(NUM_BACKENDS, GetDefaultGroupSize(*backends_[0]->cmm));

  // Blacklist the BE and sleep again.
  backends_[0]->cmm->BlacklistExecutor(backends_[1]->desc->backend_id(), Status("error"));
  EXPECT_EQ(NUM_BACKENDS, backends_[0]->cmm->GetSnapshot()->current_backends.size());
  EXPECT_EQ(NUM_BACKENDS - 1, GetDefaultGroupSize(*backends_[0]->cmm));
  usleep(BLACKLIST_TIMEOUT_SLEEP_US);
  // Quiesce the blacklisted BE. The update to quiesce it will arrive in the same call to
  // UpdateMembership() that it would have been un-blacklisted.
  QuiesceBackend(backends_[1].get());
  EXPECT_EQ(NUM_BACKENDS, backends_[0]->cmm->GetSnapshot()->current_backends.size());
  EXPECT_EQ(NUM_BACKENDS - 1, GetDefaultGroupSize(*backends_[0]->cmm));
  // Try blacklisting the quiesced BE, should have no effect.
  backends_[0]->cmm->BlacklistExecutor(backends_[1]->desc->backend_id(), Status("error"));
  EXPECT_EQ(NUM_BACKENDS, backends_[0]->cmm->GetSnapshot()->current_backends.size());
  EXPECT_EQ(NUM_BACKENDS - 1, GetDefaultGroupSize(*backends_[0]->cmm));

  // Blacklist another BE and sleep.
  backends_[0]->cmm->BlacklistExecutor(backends_[2]->desc->backend_id(), Status("error"));
  EXPECT_EQ(NUM_BACKENDS, backends_[0]->cmm->GetSnapshot()->current_backends.size());
  EXPECT_EQ(NUM_BACKENDS - 2, GetDefaultGroupSize(*backends_[0]->cmm));
  usleep(BLACKLIST_TIMEOUT_SLEEP_US);
  // Delete the blacklisted BE. The update to delete it will arrive in the same call to
  // UpdateMembership() that it would have been un-blacklisted.
  DeleteBackend(backends_[2].get());
  EXPECT_EQ(NUM_BACKENDS - 1, backends_[0]->cmm->GetSnapshot()->current_backends.size());
  EXPECT_EQ(NUM_BACKENDS - 2, GetDefaultGroupSize(*backends_[0]->cmm));
}

// This test runs a group of 20 backends through their full lifecycle, validating that
// their state is correctly propagated through the cluster after every change.
TEST_F(ClusterMembershipMgrTest, FullLifecycleMultipleBackends) {
  const int NUM_BACKENDS = 20;
  for (int i = 0; i < NUM_BACKENDS; ++i) {
    CreateBackend();
  }
  EXPECT_EQ(NUM_BACKENDS, backends_.size());
  EXPECT_EQ(backends_.size(), offline_.size());

  while (!offline_.empty()) CreateCMM(offline_.front());
  ASSERT_EQ(0, offline_.size());
  ASSERT_EQ(NUM_BACKENDS, starting_.size());

  while (!starting_.empty()) StartBackend(starting_.front());
  ASSERT_EQ(0, starting_.size());
  ASSERT_EQ(NUM_BACKENDS, running_.size());

  // Assert that all backends know about each other and are all in the default executor
  // group.
  for (Backend* be : running_) {
    EXPECT_EQ(running_.size(), be->cmm->GetSnapshot()->current_backends.size());
    EXPECT_EQ(running_.size(), GetDefaultGroupSize(*be->cmm));
  }

  // Quiesce half of the backends.
  for (int i = 0; quiescing_.size() < NUM_BACKENDS / 2; ++i) {
    Backend* be = running_.front();
    // All backends must still remain online
    EXPECT_EQ(NUM_BACKENDS, be->cmm->GetSnapshot()->current_backends.size());

    EXPECT_EQ(NUM_BACKENDS - i, GetDefaultGroupSize(*be->cmm));
    QuiesceBackend(be);
    // Make sure that the numbers drop
    EXPECT_EQ(NUM_BACKENDS - i - 1, GetDefaultGroupSize(*be->cmm));
  }
  int num_still_running = NUM_BACKENDS - quiescing_.size();
  ASSERT_EQ(num_still_running, running_.size());
  ASSERT_EQ(NUM_BACKENDS / 2, quiescing_.size());

  for (auto& be : backends_) {
    // All backends are still registered
    EXPECT_EQ(backends_.size(), be->cmm->GetSnapshot()->current_backends.size());
    // Executor groups now show half of the backends remaining
    EXPECT_EQ(num_still_running, GetDefaultGroupSize(*be->cmm));
  }

  // Delete half of the backends and make sure that the other half learned about it.
  int to_delete = backends_.size() / 2;
  int num_expected_alive = backends_.size() - to_delete;
  for (int idx = 0; idx < to_delete; ++idx) {
    // Will change backends_
    if (idx % 2 == 0) {
      DeleteBackend(running_.front());
    } else {
      DeleteBackend(quiescing_.front());
    }
  }
  ASSERT_EQ(num_expected_alive, quiescing_.size() + running_.size());

  for (Backend* be : quiescing_) {
    EXPECT_EQ(num_expected_alive, be->cmm->GetSnapshot()->current_backends.size());
  }

  // Quiesce the remaining backends to validate that executor groups can scale to 0
  // backends.
  while (!running_.empty()) QuiesceBackend(running_.front());
  for (auto& be : backends_) {
    // Executor groups now are empty
    EXPECT_EQ(0, GetDefaultGroupSize(*be->cmm));
  }
}

/// This test executes a number of random changes to cluster membership. On every
/// iteration a new backend is created, and with some probability, a backend is quiesced,
/// removed from the cluster after having been quiesced before, or removed from the
/// cluster without quiescing. The test relies on the consistency checks built into the
/// ClusterMembershipMgr to ensure that it is in a consistent state.
TEST_F(ClusterMembershipMgrTest, RandomizedMembershipUpdates) {
  // TODO: Parameterize this test and run with several parameter sets
  const int NUM_ITERATIONS = 100;
  const double P_ADD = 1;
  const double P_QUIESCE = 0.35;
  const double P_DELETE = 0.30;
  const double P_KILL = 0.2;

  // Cumulative counts of how many backends were added/shutdown/deleted/killed by the
  // tests.
  int num_added = 0;
  int num_shutdown = 0;
  int num_deleted = 0;
  // In this test "killing" a backend means deleting it without quiescing it first to
  // simulate non-graceful failures.
  int num_killed = 0;

  for (int i = 0; i < NUM_ITERATIONS; ++i) {
    double p = RandomDoubleFraction();
    if (p < P_ADD) {
      Backend* be = CreateBackend(i);
      CreateCMM(be);
      StartBackend(be);
      ++num_added;
    }
    if (p < P_QUIESCE && !running_.empty()) {
      int idx = RandomInt(running_.size());
      Backend* be = running_[idx];
      QuiesceBackend(be);
      ++num_shutdown;
    }
    if (p < P_DELETE && !quiescing_.empty()) {
      int idx = RandomInt(quiescing_.size());
      Backend* be = quiescing_[idx];
      DeleteBackend(be);
      ++num_deleted;
    }
    if (p < P_KILL && !running_.empty()) {
      int idx = RandomInt(running_.size());
      Backend* be = running_[idx];
      DeleteBackend(be);
      ++num_killed;
    }
  }
  std::cout << "Added: " << num_added << ", shutdown: " << num_shutdown << ", deleted: "
      << num_deleted << ", killed: " << num_killed << endl;
}

/// This tests various valid and invalid cases that the parsing logic in
/// PopulateExpectedExecGroupSets can encounter.
TEST(ClusterMembershipMgrUnitTest, TestPopulateExpectedExecGroupSets) {
  gflags::FlagSaver saver;
  vector<TExecutorGroupSet> expected_exec_group_sets;
  // Case 1: Empty string
  FLAGS_expected_executor_group_sets = "";
  expected_exec_group_sets.clear();
  Status status =
      ClusterMembershipMgr::PopulateExpectedExecGroupSets(expected_exec_group_sets);
  EXPECT_TRUE(status.ok());
  EXPECT_TRUE(expected_exec_group_sets.empty());

  // Case 2: Single valid group set
  FLAGS_expected_executor_group_sets = "group-prefix1:2";
  expected_exec_group_sets.clear();
  status = ClusterMembershipMgr::PopulateExpectedExecGroupSets(expected_exec_group_sets);
  EXPECT_TRUE(status.ok());
  EXPECT_EQ(expected_exec_group_sets.size(), 1);
  EXPECT_EQ(expected_exec_group_sets[0].exec_group_name_prefix, "group-prefix1");
  EXPECT_EQ(expected_exec_group_sets[0].expected_num_executors, 2);

  // Case 3: Multiple valid group sets
  FLAGS_expected_executor_group_sets = "group-prefix1:2,group-prefix2:10";
  expected_exec_group_sets.clear();
  status = ClusterMembershipMgr::PopulateExpectedExecGroupSets(expected_exec_group_sets);
  EXPECT_TRUE(status.ok());
  EXPECT_EQ(expected_exec_group_sets.size(), 2);
  EXPECT_EQ(expected_exec_group_sets[0].exec_group_name_prefix, "group-prefix1");
  EXPECT_EQ(expected_exec_group_sets[0].expected_num_executors, 2);
  EXPECT_EQ(expected_exec_group_sets[1].exec_group_name_prefix, "group-prefix2");
  EXPECT_EQ(expected_exec_group_sets[1].expected_num_executors, 10);

  // Case 4: Multiple valid group sets but out of order, output is expected to return in
  // increasing order of expected group size
  FLAGS_expected_executor_group_sets = "group-prefix1:10,group-prefix2:2";
  expected_exec_group_sets.clear();
  status = ClusterMembershipMgr::PopulateExpectedExecGroupSets(expected_exec_group_sets);
  EXPECT_TRUE(status.ok());
  EXPECT_EQ(expected_exec_group_sets.size(), 2);
  EXPECT_EQ(expected_exec_group_sets[0].exec_group_name_prefix, "group-prefix2");
  EXPECT_EQ(expected_exec_group_sets[0].expected_num_executors, 2);
  EXPECT_EQ(expected_exec_group_sets[1].exec_group_name_prefix, "group-prefix1");
  EXPECT_EQ(expected_exec_group_sets[1].expected_num_executors, 10);

  // Case 5: Invalid input for expected group size
  FLAGS_expected_executor_group_sets = "group-prefix1:2abc";
  expected_exec_group_sets.clear();
  status = ClusterMembershipMgr::PopulateExpectedExecGroupSets(expected_exec_group_sets);
  EXPECT_FALSE(status.ok());
  EXPECT_EQ(status.msg().GetFullMessageDetails(),
      "Failed to parse expected executor group set size for input: group-prefix1:2abc\n");

  // Case 6: Invalid input with no expected group size
  FLAGS_expected_executor_group_sets = "group-prefix1:";
  expected_exec_group_sets.clear();
  status = ClusterMembershipMgr::PopulateExpectedExecGroupSets(expected_exec_group_sets);
  EXPECT_FALSE(status.ok());
  EXPECT_EQ(status.msg().GetFullMessageDetails(),
      "Failed to parse expected executor group set size for input: group-prefix1:\n");

  // Case 7: Invalid input with no group prefix
  FLAGS_expected_executor_group_sets = ":1";
  expected_exec_group_sets.clear();
  status = ClusterMembershipMgr::PopulateExpectedExecGroupSets(expected_exec_group_sets);
  EXPECT_FALSE(status.ok());
  EXPECT_EQ(status.msg().GetFullMessageDetails(),
      "Executor group set prefix cannot be empty for input: :1\n");

  // Case 8: Invalid input with no colon separator
  FLAGS_expected_executor_group_sets = "group-prefix1";
  expected_exec_group_sets.clear();
  status = ClusterMembershipMgr::PopulateExpectedExecGroupSets(expected_exec_group_sets);
  EXPECT_FALSE(status.ok());
  EXPECT_EQ(status.msg().GetFullMessageDetails(),
      "Invalid executor group set format: group-prefix1\n");

  // Case 9: Invalid input with duplicated group prefix
  FLAGS_expected_executor_group_sets = "group-prefix1:2,group-prefix1:10";
  expected_exec_group_sets.clear();
  status = ClusterMembershipMgr::PopulateExpectedExecGroupSets(expected_exec_group_sets);
  EXPECT_FALSE(status.ok());
  EXPECT_EQ(status.msg().GetFullMessageDetails(),
      "Executor group set prefix specified multiple times: group-prefix1:10\n");
}

/// This ensures that all executor group configuration scenarios possible using available
/// startup flags are handled correctly when populating membership updates that are
/// sent to the frontend.
TEST(ClusterMembershipMgrUnitTest, PopulateExecutorMembershipRequest) {
  gflags::FlagSaver saver;
  FLAGS_num_expected_executors = 20;
  auto snapshot_ptr = std::make_shared<ClusterMembershipMgr::Snapshot>();
  TUpdateExecutorMembershipRequest update_req;
  vector<TExecutorGroupSet> empty_exec_group_sets;
  vector<TExecutorGroupSet> populated_exec_group_sets;
  populated_exec_group_sets.emplace_back();
  populated_exec_group_sets.back().__set_exec_group_name_prefix("foo");
  populated_exec_group_sets.back().__set_expected_num_executors(2);
  populated_exec_group_sets.emplace_back();
  populated_exec_group_sets.back().__set_exec_group_name_prefix("bar");
  populated_exec_group_sets.back().__set_expected_num_executors(10);

  // Case 1a: Not using executor groups
  {
    ExecutorGroup exec_group(ImpalaServer::DEFAULT_EXECUTOR_GROUP_NAME, 1);
    exec_group.AddExecutor(MakeBackendDescriptor(1, 0));
    snapshot_ptr->executor_groups.insert({exec_group.name(), exec_group});
    ClusterMembershipMgr::SnapshotPtr ptr = snapshot_ptr;
    PopulateExecutorMembershipRequest(ptr, empty_exec_group_sets, update_req);
    EXPECT_EQ(update_req.exec_group_sets.size(), 1);
    EXPECT_EQ(update_req.exec_group_sets[0].curr_num_executors, 1);
    EXPECT_EQ(update_req.exec_group_sets[0].expected_num_executors, 20);
    EXPECT_EQ(update_req.exec_group_sets[0].exec_group_name_prefix, "");
    snapshot_ptr->executor_groups.clear();
  }

  // Case 1b: Not using executor groups but expected_exec_group_sets is non-empty
  {
    ExecutorGroup exec_group(ImpalaServer::DEFAULT_EXECUTOR_GROUP_NAME, 1);
    exec_group.AddExecutor(MakeBackendDescriptor(1, 0));
    snapshot_ptr->executor_groups.insert({exec_group.name(), exec_group});
    ClusterMembershipMgr::SnapshotPtr ptr = snapshot_ptr;
    PopulateExecutorMembershipRequest(ptr, populated_exec_group_sets, update_req);
    EXPECT_EQ(update_req.exec_group_sets.size(), 1);
    EXPECT_EQ(update_req.exec_group_sets[0].curr_num_executors, 1);
    EXPECT_EQ(update_req.exec_group_sets[0].expected_num_executors, 20);
    EXPECT_EQ(update_req.exec_group_sets[0].exec_group_name_prefix, "");
    snapshot_ptr->executor_groups.clear();
  }

  // Case 2a: Using executor groups, expected_exec_group_sets is empty
  {
    ExecutorGroup exec_group("foo-group1", 1);
    exec_group.AddExecutor(MakeBackendDescriptor(1, exec_group, 0));
    snapshot_ptr->executor_groups.insert({exec_group.name(), exec_group});
    // Adding another exec group with more executors.
    ExecutorGroup exec_group2("foo-group2", 1);
    exec_group2.AddExecutor(MakeBackendDescriptor(1, exec_group2, 1));
    exec_group2.AddExecutor(MakeBackendDescriptor(2, exec_group2, 2));
    snapshot_ptr->executor_groups.insert({exec_group2.name(), exec_group2});
    ClusterMembershipMgr::SnapshotPtr ptr = snapshot_ptr;
    PopulateExecutorMembershipRequest(ptr, empty_exec_group_sets, update_req);
    EXPECT_EQ(update_req.exec_group_sets.size(), 1);
    EXPECT_EQ(update_req.exec_group_sets[0].curr_num_executors, 2);
    EXPECT_EQ(update_req.exec_group_sets[0].expected_num_executors, 20);
    EXPECT_EQ(update_req.exec_group_sets[0].exec_group_name_prefix, "");
    snapshot_ptr->executor_groups.clear();
  }

  // Case 2b: Using executor groups, expected_exec_group_sets is empty, executor groups
  // with different group prefixes
  {
    ExecutorGroup exec_group("foo-group1", 1);
    exec_group.AddExecutor(MakeBackendDescriptor(1, exec_group, 0));
    snapshot_ptr->executor_groups.insert({exec_group.name(), exec_group});
    // Adding another exec group with a different group prefix.
    ExecutorGroup exec_group2("bar-group1", 1);
    exec_group2.AddExecutor(MakeBackendDescriptor(1, exec_group2, 1));
    exec_group2.AddExecutor(MakeBackendDescriptor(2, exec_group2, 2));
    snapshot_ptr->executor_groups.insert({exec_group2.name(), exec_group2});
    ClusterMembershipMgr::SnapshotPtr ptr = snapshot_ptr;
    PopulateExecutorMembershipRequest(ptr, empty_exec_group_sets, update_req);
    EXPECT_EQ(update_req.exec_group_sets.size(), 1);
    EXPECT_EQ(update_req.exec_group_sets[0].curr_num_executors, 2);
    EXPECT_EQ(update_req.exec_group_sets[0].expected_num_executors, 20);
    EXPECT_EQ(update_req.exec_group_sets[0].exec_group_name_prefix, "");
    snapshot_ptr->executor_groups.clear();
  }

  // Case 2c: Using executor groups, expected_exec_group_sets is non-empty
  {
    ExecutorGroup exec_group("foo-group1", 1);
    exec_group.AddExecutor(MakeBackendDescriptor(1, exec_group, 0));
    snapshot_ptr->executor_groups.insert({exec_group.name(), exec_group});
    // Adding another exec group with a different group prefix.
    ExecutorGroup exec_group2("bar-group1", 1);
    exec_group2.AddExecutor(MakeBackendDescriptor(1, exec_group2, 1));
    exec_group2.AddExecutor(MakeBackendDescriptor(2, exec_group2, 2));
    snapshot_ptr->executor_groups.insert({exec_group2.name(), exec_group2});
    ClusterMembershipMgr::SnapshotPtr ptr = snapshot_ptr;
    PopulateExecutorMembershipRequest(ptr, populated_exec_group_sets, update_req);
    EXPECT_EQ(update_req.exec_group_sets.size(), 2);
    EXPECT_EQ(update_req.exec_group_sets[0].curr_num_executors, 1);
    EXPECT_EQ(update_req.exec_group_sets[0].expected_num_executors, 2);
    EXPECT_EQ(update_req.exec_group_sets[0].exec_group_name_prefix, "foo");
    EXPECT_EQ(update_req.exec_group_sets[1].curr_num_executors, 2);
    EXPECT_EQ(update_req.exec_group_sets[1].expected_num_executors, 10);
    EXPECT_EQ(update_req.exec_group_sets[1].exec_group_name_prefix, "bar");
    snapshot_ptr->executor_groups.clear();
  }

  // Case 2d: Using executor groups, expected_exec_group_sets is non-empty
  // and one executor group that does not match to any executor group sets having more
  // number of executor groups
  {
    ExecutorGroup exec_group("foo-group1", 1);
    exec_group.AddExecutor(MakeBackendDescriptor(1, exec_group, 0));
    snapshot_ptr->executor_groups.insert({exec_group.name(), exec_group});
    // Adding another exec group with a different group prefix.
    ExecutorGroup exec_group2("unmatch-group1", 1);
    exec_group2.AddExecutor(MakeBackendDescriptor(1, exec_group2, 1));
    exec_group2.AddExecutor(MakeBackendDescriptor(2, exec_group2, 2));
    snapshot_ptr->executor_groups.insert({exec_group2.name(), exec_group2});
    ClusterMembershipMgr::SnapshotPtr ptr = snapshot_ptr;
    PopulateExecutorMembershipRequest(ptr, populated_exec_group_sets, update_req);
    EXPECT_EQ(update_req.exec_group_sets.size(), 2);
    EXPECT_EQ(update_req.exec_group_sets[0].curr_num_executors, 1);
    EXPECT_EQ(update_req.exec_group_sets[0].expected_num_executors, 2);
    EXPECT_EQ(update_req.exec_group_sets[0].exec_group_name_prefix, "foo");
    EXPECT_EQ(update_req.exec_group_sets[1].curr_num_executors, 0);
    EXPECT_EQ(update_req.exec_group_sets[1].expected_num_executors, 10);
    EXPECT_EQ(update_req.exec_group_sets[1].exec_group_name_prefix, "bar");
    snapshot_ptr->executor_groups.clear();
  }
}

template <class T>
static bool has(const vector<T>& v, const T& m) {
  return find(v.begin(), v.end(), m) != v.end();
}

/// Test that we can get a list of coordinators.
TEST_F(ClusterMembershipMgrTest, GetCoordinatorAddresses) {
  // Initialize all backends early. Test methods handle state propagation through the
  // backends_ list, which must be fully initialized before starting backends.
  Backend* coordinator0 = CreateBackend();
  const NetworkAddressPB& addr0 = coordinator0->desc->address();
  CreateCMM(coordinator0);
  Backend* coordinator1 = CreateBackend();
  const NetworkAddressPB& addr1 = coordinator1->desc->address();
  coordinator1->desc->set_is_executor(false);
  CreateCMM(coordinator1);
  Backend* executor = CreateBackend();
  executor->desc->set_is_coordinator(false);
  CreateCMM(executor);

  EXPECT_EQ(0, coordinator0->cmm->GetSnapshot()->GetCoordinatorAddresses().size());

  StartBackend(coordinator0);
  vector<TNetworkAddress> orig_coordinators =
      coordinator0->cmm->GetSnapshot()->GetCoordinatorAddresses();
  EXPECT_EQ(1, orig_coordinators.size());
  EXPECT_EQ(FromNetworkAddressPB(addr0), orig_coordinators[0]);

  StartBackend(executor);
  EXPECT_EQ(orig_coordinators,
      coordinator0->cmm->GetSnapshot()->GetCoordinatorAddresses());
  EXPECT_EQ(orig_coordinators, executor->cmm->GetSnapshot()->GetCoordinatorAddresses());

  StartBackend(coordinator1);
  orig_coordinators = coordinator0->cmm->GetSnapshot()->GetCoordinatorAddresses();
  EXPECT_EQ(2, orig_coordinators.size());
  // List of coordinators is unsorted.
  EXPECT_TRUE(has(orig_coordinators, FromNetworkAddressPB(addr0)));
  EXPECT_TRUE(has(orig_coordinators, FromNetworkAddressPB(addr1)));

  EXPECT_EQ(orig_coordinators, executor->cmm->GetSnapshot()->GetCoordinatorAddresses());
  EXPECT_EQ(orig_coordinators,
      coordinator1->cmm->GetSnapshot()->GetCoordinatorAddresses());
}

/// TODO: Write a test that makes a number of random changes to cluster membership while
/// not maintaining the proper lifecycle steps that a backend goes through (create, start,
/// quiesce, delete).

} // end namespace impala
