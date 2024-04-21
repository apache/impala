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

#pragma once

#include <functional>
#include <memory>
#include <mutex>
#include <string>
#include <unordered_map>
#include <vector>

#include "common/status.h"
#include "gen-cpp/Frontend_types.h"
#include "scheduling/executor-blacklist.h"
#include "scheduling/executor-group.h"
#include "statestore/statestore-subscriber.h"
#include "util/metrics-fwd.h"

namespace impala {

class TUpdateExecutorMembershipRequest;

namespace test {
class SchedulerWrapper;
}

/// The ClusterMembershipMgr manages the local backend's membership in the cluster. It has
/// two roles:
///   - Provide a consistent view on the current cluster membership, i.e. other backends
///   - Establish and maintain the membership of the local backend in the cluster.
///
/// To do this, the class subscribes to the statestore cluster-membership topic and
/// applies incoming changes to its local copy of the cluster state. If it finds that the
/// local backend is missing from the cluster membership, it will add it (contingent on
/// the local backend being available after startup).
///
/// Clients of this class can obtain a consistent, immutable snapshot of the cluster
/// membership state through GetSnapshot().
///
/// The ClusterMembershipMgr keeps the membership snapshot stable while the statestore
/// subscriber is recovering from a connection failure. This allows other backends to
/// re-register with the statestore after a statestore restart.
///
/// The class also allows the local backend (ImpalaServer), the local Frontend and the
/// AdmissionController to register callbacks to receive notifications of changes to the
/// cluster membership. Note: The notifications for blacklisted executors are not sent
/// immediately, but deferred to when statestore updates are processed (see detailed
/// comment in the BlacklistExecutor method in the cc file). These callbacks are
/// registered before the statestore subscriber is started by the impala server which
/// ensures that the very first update to the cluster membership is sent to all callbacks.
///
/// TODO: Replace the usage of shared_ptr with atomic_shared_ptr once compilers support
///       it. Alternatively consider using Kudu's rw locks.
class ClusterMembershipMgr {
 public:
  /// A immutable pointer to a backend descriptor. It is used to return a consistent,
  /// immutable copy of a backend descriptor.
  typedef std::shared_ptr<const BackendDescriptorPB> BeDescSharedPtr;

  /// Maps backend IDs to backend descriptors.
  typedef std::unordered_map<std::string, BackendDescriptorPB> BackendIdMap;

  /// Maps executor group names to executor groups.
  typedef std::unordered_map<std::string, ExecutorGroup> ExecutorGroups;

  /// Empty group name when query use coordinator only.
  static const std::string EMPTY_GROUP_NAME;

  // A snapshot of the current cluster membership. The ClusterMembershipMgr maintains a
  // consistent copy of this and updates it atomically when the membership changes.
  // Clients can obtain an immutable copy. Class instances can be created through the
  // implicitly-defined default and copy constructors.
  struct Snapshot {
    Snapshot() = default;
    Snapshot(const Snapshot&) = default;
    /// Returns an executor group of all non-quiescing coordinators in the cluster.
    ExecutorGroup GetCoordinators() const;
    /// Returns the addresses of all non-quiescing coordinators in the cluster.
    std::vector<TNetworkAddress> GetCoordinatorAddresses() const;
    /// The current backend descriptor of the local backend.
    BeDescSharedPtr local_be_desc;
    /// Map from unique backend ID to BackendDescriptorPB for all known backends,
    /// including those that are quiescing or blacklisted. The {backend ID,
    /// BackendDescriptorPB} pairs represent the IMPALA_MEMBERSHIP_TOPIC {key, value}
    /// pairs of known executors retrieved from the statestore. It's important to track
    /// both the backend ID as well as the BackendDescriptorPB so we know what is being
    /// removed in a given update.
    BackendIdMap current_backends;
    /// A map of executor groups by their names. Only contains executors that are
    /// available for scheduling queries on and not executors that are quiescing or
    /// blacklisted.
    ExecutorGroups executor_groups;
    /// The local blacklist. Backends that are added to this will be present in
    /// 'current_backends' but not in 'executor_groups'.
    ExecutorBlacklist executor_blacklist;
    /// The version of this Snapshot. It is incremented every time the cluster membership
    /// changes.
    int64_t version = 0;
  };

  /// An immutable shared membership snapshot.
  typedef std::shared_ptr<const Snapshot> SnapshotPtr;

  /// A callback to provide the local backend descriptor. Typically the ImpalaServer would
  /// provide such a backend descriptor, and it's the task of code outside this class to
  /// register the ImpalaServer with this class, e.g. after it has started up. Tests can
  /// register their own callback to provide a local backend descriptor. No locks are held
  /// when calling this callback.
  typedef std::function<BeDescSharedPtr()> BackendDescriptorPtrFn;

  /// A callback to provide the latest snapshot of cluster membership whenever there are
  /// any changes to the membership.
  typedef std::function<void(SnapshotPtr)> UpdateCallbackFn;

  ClusterMembershipMgr(std::string local_backend_id, StatestoreSubscriber* subscriber,
      MetricGroup* metrics);

  /// Initializes instances of this class. This only sets up the statestore subscription.
  /// Callbacks to the local ImpalaServer and Frontend must be registered in separate
  /// steps.
  Status Init();

  /// The following functions allow users of this class to register callbacks for certain
  /// events. They may be called at any time before or after calling Init() and are
  /// thread-safe. Each of the functions may be called at most once during the lifetime of
  /// the object, and the function 'fn' must not be empty.

  /// Registers a callback to provide the local backend descriptor.
  void SetLocalBeDescFn(BackendDescriptorPtrFn fn);

  /// Registers a callback to be notified with the latest snapshot of cluster membership
  /// whenever there are any changes to the membership.
  void RegisterUpdateCallbackFn(UpdateCallbackFn fn);

  /// Returns a read only snapshot of the current cluster membership state. May be called
  /// before or after calling Init(). The returned shared pointer will always be non-null,
  /// but it may point to an empty Snapshot, depending on the arrival of statestore
  /// updates and the status of the local backend.
  SnapshotPtr GetSnapshot() const;

  /// Handler for statestore updates, called asynchronously when an update is received
  /// from the subscription manager. This method processes incoming updates from the
  /// statestore and applies them to the current membership state. It also ensures that
  /// the local backend descriptor gets registered with the statestore and adds it to the
  /// current membership state. This method changes the 'current_membership_' atomically.
  ///
  /// This handler is registered with the statestore and must not be called directly,
  /// except in tests.
  void UpdateMembership(const StatestoreSubscriber::TopicDeltaMap& incoming_topic_deltas,
      std::vector<TTopicDelta>* subscriber_topic_updates);

  /// Adds the given backend to the local blacklist. Updates 'current_membership_' to
  /// remove the backend from 'executor_groups' so that it will not be scheduled on.
  /// 'cause' is an error status representing the reason the node was blacklisted.
  void BlacklistExecutor(const UniqueIdPB& backend_id, const Status& cause);

  /// Returns a pointer to the static empty group reserved for scheduling coord only
  /// queries.
  const ExecutorGroup* GetEmptyExecutorGroup() { return &empty_exec_group_; }

  const std::vector<TExecutorGroupSet>& GetExpectedExecGroupSets() {
    return expected_exec_group_sets_;
  }

 private:
  struct GroupSetMetrics {
    IntCounter* total_live_executor_groups_ = nullptr;
    IntCounter* total_healthy_executor_groups_ = nullptr;
    IntCounter* total_backends_ = nullptr;
  };

  /// Serializes and adds the local backend descriptor to 'subscriber_topic_updates'.
  void AddLocalBackendToStatestore(const BackendDescriptorPB& local_be_desc,
      std::vector<TTopicDelta>* subscriber_topic_updates);

  /// Returns the local backend descriptor or nullptr if no local backend has been
  /// registered.
  BeDescSharedPtr GetLocalBackendDescriptor();

  /// Notifies all registered callbacks of the latest changes to the membership by sending
  /// them the latest cluster membership snapshot.
  void NotifyListeners(SnapshotPtr snapshot);

  /// Atomically replaces a membership snapshot with a new copy.
  void SetState(const SnapshotPtr& new_state);

  /// Checks if the local backend is available and its registration in 'state' matches the
  /// given backend descriptor in 'local_be_desc'. Returns false otherwise.
  bool NeedsLocalBackendUpdate(const Snapshot& state,
      const BeDescSharedPtr& local_be_desc);

  /// Checks that the backend ID map is consistent with 'executor_groups' and
  /// 'executor_blacklist', i.e. all executors in all groups are also present in the map
  /// and are non-quiescing, non-blacklisted executors.
  /// This method should only be called in debug builds.
  bool CheckConsistency(const BackendIdMap& current_backends,
      const ExecutorGroups& executor_groups, const ExecutorBlacklist& executor_blacklist);

  /// Updates the membership metrics. Is registered as an updated callback function to
  /// receive any membership updates. The only exception is that this is called directly
  /// in BlacklistExecutor() where updates are not required to be sent to external
  /// listeners. InitMetrics() must have been called before registering this method.
  void UpdateMetrics(const SnapshotPtr& new_state);

  /// Returns true if the 'be_desc' is in any of the 'executor_groups', false otherwise.
  /// Used to check the consistency of the ClusterMembershipMgr state. Currently, only
  /// used in DCHECKs.
  bool IsBackendInExecutorGroups(
      const BackendDescriptorPB& be_desc, const ExecutorGroups& executor_groups);

  /// Parses the --expected_executor_group_sets startup flag and populates
  /// 'expected_exec_group_sets' with the group name prefix and expected group size. Also
  /// sorts it in increasing order by the expected group size.
  static Status PopulateExpectedExecGroupSets(
      std::vector<TExecutorGroupSet>& expected_exec_group_sets);

  /// Helper method to initialize metrics. Also initializes metrics for all group sets if
  /// expected group sets are specified.
  void InitMetrics(MetricGroup* metrics);

  /// An empty group used for scheduling coordinator only queries.
  const ExecutorGroup empty_exec_group_;

  /// Info about expected executor group sets parsed from num_expected_executor_groups
  /// flag in Init() and sorted by the expected group size in increasing order.
  std::vector<TExecutorGroupSet> expected_exec_group_sets_;

  /// Ensures that only one thread is processing a membership update at a time, either
  /// from a statestore update or a blacklisting decision. Must be taken before any other
  /// locks in this class.
  std::mutex update_membership_lock_;

  /// Membership metrics for all executor groups regardless of which group set they belong
  /// to. The value for total backends also includes the coordinators.
  GroupSetMetrics aggregated_group_set_metrics_;

  /// Maps from the group set prefix to its group set metrics. Is empty if group sets are
  /// not used.
  std::unordered_map<string, GroupSetMetrics> per_group_set_metrics_;

  /// The snapshot of the current cluster membership. When receiving changes to the
  /// executors configuration from the statestore we will make a copy of the stored
  /// object, apply the updates to the copy and atomically swap the contents of this
  /// pointer.
  SnapshotPtr current_membership_;

  /// Protects current_membership_. Cannot be held at the same time as
  /// 'callback_fn_lock_'.
  mutable std::mutex current_membership_lock_;

  /// A temporary membership snapshot to hold updates while the statestore is in its
  /// post-recovery grace period. Not exposed to clients. Protected by
  /// 'update_membership_lock_'.
  std::shared_ptr<Snapshot> recovering_membership_;

  /// Pointer to a subscription manager (which we do not own) which is used to register
  /// for dynamic updates to the set of available backends. May be nullptr if the set of
  /// backends is fixed (only useful for tests). Thread-safe, not protected by a lock.
  StatestoreSubscriber* statestore_subscriber_;

  /// Unique - across the cluster - identifier for this impala backend. Used to validate
  /// incoming backend descriptors and to register this backend with the statestore. Not
  /// protected by a lock - only used in the statestore thread.
  std::string local_backend_id_;

  /// Callbacks that provide external dependencies.
  BackendDescriptorPtrFn local_be_desc_fn_;
  std::vector<UpdateCallbackFn> update_callback_fns_;

  /// Protects the callbacks. Cannot be held at the same time as
  /// 'current_membership_lock_'.
  mutable std::mutex callback_fn_lock_;

  friend class impala::test::SchedulerWrapper;
  friend class ClusterMembershipMgrUnitTest_TestPopulateExpectedExecGroupSets_Test;
};

/// Helper method to populate a thrift request object 'update_req' for cluster membership
/// using a supplied 'snapshot' from the cluster membership manager and the
/// 'expected_exec_group_sets' sorted in increasing order by their expected group size.
/// The frontend uses cluster membership information to determine whether it expects the
/// scheduler to assign local or remote reads. It also uses the number of executors to
/// determine the join type (partitioned vs broadcast).
void PopulateExecutorMembershipRequest(ClusterMembershipMgr::SnapshotPtr& snapshot,
    const std::vector<TExecutorGroupSet>& expected_exec_group_sets,
    TUpdateExecutorMembershipRequest& update_req);

} // end namespace impala
