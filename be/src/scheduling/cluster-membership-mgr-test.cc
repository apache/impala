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
#include "testutil/gtest-util.h"
#include "testutil/rand-util.h"

using std::mt19937;
using std::uniform_int_distribution;
using std::uniform_real_distribution;
using namespace impala;
using namespace impala::test;

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

  /// A struct to hold information related to a simulated backend during the test.
  struct Backend {
    string backend_id;
    std::unique_ptr<ClusterMembershipMgr> cmm;
    std::shared_ptr<TBackendDescriptor> desc;
  };
  /// The list of backends_ that owns all backends.
  vector<unique_ptr<Backend>> backends_;

  /// Various lists of pointers that point into elements of 'backends_'. These pointers
  /// will stay valid when backend_ resizes. Backends can be in one of 5 states:
  /// - "Offline": A TBackendDescriptor has been created but has not been associated with
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
    be->desc = make_shared<TBackendDescriptor>(MakeBackendDescriptor(idx));
    be->backend_id = be->desc->address.hostname;
    offline_.push_back(be.get());
    return be.get();
  }

  /// Creates a new ClusterMembershipMgr for a backend and moves the backend from
  /// 'offline_' to 'starting_'. Callers must handle invalidated iterators after calling
  /// this method.
  void CreateCMM(Backend* be) {
    ASSERT_TRUE(IsInVector(be, offline_));
    be->cmm = make_unique<ClusterMembershipMgr>(be->backend_id, nullptr);
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
    be->desc->__set_is_quiescing(true);
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
    deletion_item.key = be->backend_id;
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
  auto b1 = make_shared<TBackendDescriptor>(MakeBackendDescriptor(1));
  auto b2 = make_shared<TBackendDescriptor>(MakeBackendDescriptor(2));

  ClusterMembershipMgr cmm1(b1->address.hostname, nullptr);
  ClusterMembershipMgr cmm2(b2->address.hostname, nullptr);

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
  b1->is_quiescing = true;
  // Send an empty update to the 1st one to trigger propagation of the shutdown
  returned_topic_deltas.clear();
  topic_delta_map[topic_id] = empty_delta;
  cmm1.UpdateMembership(topic_delta_map, &returned_topic_deltas);
  // The mgr will return its changed TBackendDescriptor
  ASSERT_EQ(1, returned_topic_deltas.size());
  // It will also remove itself from the executor group (but not the current backends).
  ASSERT_EQ(1,
      cmm1.GetSnapshot()->executor_groups.find("default")->second.NumExecutors());
  ASSERT_EQ(2, cmm1.GetSnapshot()->current_backends.size());

  // Propagate the quiescing to the 2nd mgr
  *ss_topic_delta = returned_topic_deltas[0];
  returned_topic_deltas.clear();
  ASSERT_EQ(2,
      cmm2.GetSnapshot()->executor_groups.find("default")->second.NumExecutors());
  cmm2.UpdateMembership(topic_delta_map, &returned_topic_deltas);
  ASSERT_EQ(0, returned_topic_deltas.size());
  ASSERT_EQ(2, cmm2.GetSnapshot()->current_backends.size());
  ASSERT_EQ(1,
      cmm2.GetSnapshot()->executor_groups.find("default")->second.NumExecutors());

  // Delete the 1st backend from the 2nd one
  ASSERT_EQ(1, ss_topic_delta->topic_entries.size());
  ss_topic_delta->topic_entries[0].deleted = true;
  cmm2.UpdateMembership(topic_delta_map, &returned_topic_deltas);
  ASSERT_EQ(0, returned_topic_deltas.size());
  ASSERT_EQ(1, cmm2.GetSnapshot()->current_backends.size());
  ASSERT_EQ(1,
      cmm2.GetSnapshot()->executor_groups.find("default")->second.NumExecutors());

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
    EXPECT_EQ(running_.size(),
        be->cmm->GetSnapshot()->executor_groups.find("default")->second.NumExecutors());
  }

  // Quiesce half of the backends.
  for (int i = 0; quiescing_.size() < NUM_BACKENDS / 2; ++i) {
    Backend* be = running_.front();
    // All backends must still remain online
    EXPECT_EQ(NUM_BACKENDS, be->cmm->GetSnapshot()->current_backends.size());

    EXPECT_EQ(NUM_BACKENDS - i,
        be->cmm->GetSnapshot()->executor_groups.find("default")->second.NumExecutors());
    QuiesceBackend(be);
    // Make sure that the numbers drop
    EXPECT_EQ(NUM_BACKENDS - i - 1,
        be->cmm->GetSnapshot()->executor_groups.find("default")->second.NumExecutors());
  }
  int num_still_running = NUM_BACKENDS - quiescing_.size();
  ASSERT_EQ(num_still_running, running_.size());
  ASSERT_EQ(NUM_BACKENDS / 2, quiescing_.size());

  for (auto& be : backends_) {
    // All backends are still registered
    EXPECT_EQ(backends_.size(), be->cmm->GetSnapshot()->current_backends.size());
    // Executor groups now show half of the backends remaining
    EXPECT_EQ(num_still_running,
        be->cmm->GetSnapshot()->executor_groups.find("default")->second.NumExecutors());
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
    EXPECT_EQ(0,
        be->cmm->GetSnapshot()->executor_groups.find("default")->second.NumExecutors());
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

/// TODO: Write a test that makes a number of random changes to cluster membership while
/// not maintaining the proper lifecycle steps that a backend goes through (create, start,
/// quiesce, delete).

} // end namespace impala

IMPALA_TEST_MAIN();
