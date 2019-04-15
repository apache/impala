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

#include <gtest/gtest_prod.h> // for FRIEND_TEST
#include <mutex>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <vector>

#include "common/global-types.h"
#include "common/status.h"
#include "gen-cpp/StatestoreService_types.h"
#include "gutil/threading/thread_collision_warner.h"
#include "scheduling/executor-group.h"
#include "statestore/statestore-subscriber.h"
#include "util/container-util.h"

namespace impala {

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
/// TODO(IMPALA-8484): Allow specifying executor groups during backend startup. Currently
/// only one executor group named "default" exists. All backends are part of that group
/// and it's the only group available for scheduling.
///
/// The class also allows the local backend (ImpalaServer) and the local Frontend to
/// register callbacks to receive notifications of changes to the cluster membership.
///
/// TODO: Replace the usage of shared_ptr with atomic_shared_ptr once compilers support
///       it. Alternatively consider using Kudu's rw locks.
class ClusterMembershipMgr {
 public:
  /// A immutable pointer to a backend descriptor. It is used to return a consistent,
  /// immutable copy of a backend descriptor.
  typedef std::shared_ptr<const TBackendDescriptor> BeDescSharedPtr;

  /// Maps statestore subscriber IDs to backend descriptors.
  typedef std::unordered_map<std::string, TBackendDescriptor> BackendIdMap;

  /// Maps executor group names to executor groups. For now, only a default group exists
  /// and all executors are part of that group.
  typedef std::unordered_map<std::string, ExecutorGroup> ExecutorGroups;

  // A snapshot of the current cluster membership. The ClusterMembershipMgr maintains a
  // consistent copy of this and updates it atomically when the membership changes.
  // Clients can obtain an immutable copy. Class instances can be created through the
  // implicitly-defined default and copy constructors.
  struct Snapshot {
    Snapshot() = default;
    Snapshot(const Snapshot&) = default;
    /// The current backend descriptor of the local backend.
    BeDescSharedPtr local_be_desc;
    /// Map from unique backend ID to TBackendDescriptor. The
    /// {backend ID, TBackendDescriptor} pairs represent the IMPALA_MEMBERSHIP_TOPIC
    /// {key, value} pairs of known executors retrieved from the statestore. It's
    /// important to track both the backend ID as well as the TBackendDescriptor so we
    /// know what is being removed in a given update.
    BackendIdMap current_backends;
    /// A map of executor groups by their names.
    ExecutorGroups executor_groups;
  };

  /// An immutable shared membership snapshot.
  typedef std::shared_ptr<const Snapshot> SnapshotPtr;

  /// A callback to provide the local backend descriptor. Typically the ImpalaServer would
  /// provide such a backend descriptor, and it's the task of code outside this class to
  /// register the ImpalaServer with this class, e.g. after it has started up. Tests can
  /// register their own callback to provide a local backend descriptor. No locks are held
  /// when calling this callback.
  typedef std::function<BeDescSharedPtr()> BackendDescriptorPtrFn;

  /// A set of backend addresses.
  typedef std::unordered_set<TNetworkAddress> BackendAddressSet;

  /// A callback to notify the local ImpalaServer of changes to the cluster membership.
  /// Only called when backends are deleted from the current membership. No locks are held
  /// when calling this callback.
  typedef std::function<void(const BackendAddressSet&)> UpdateLocalServerFn;

  /// A callback to notify the local Frontend of changes to the cluster membership. No
  /// locks are held when calling this callback.
  typedef std::function<Status(const TUpdateExecutorMembershipRequest&)> UpdateFrontendFn;

  ClusterMembershipMgr(std::string local_backend_id, StatestoreSubscriber* subscriber);

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

  /// Registers a callback to notify the local ImpalaServer of changes in the cluster
  /// membership. This callback will only be called when backends are deleted from the
  /// membership.
  void SetUpdateLocalServerFn(UpdateLocalServerFn fn);

  /// Registers a callback to notify the local Frontend of changes in the cluster
  /// membership.
  void SetUpdateFrontendFn(UpdateFrontendFn fn);

  /// Returns a read only snapshot of the current cluster membership state. May be called
  /// before or after calling Init(). The returned shared pointer will always be non-null,
  /// but it may point to an empty Snapshot, depending on the arrival of statestore
  /// updates and the status of the local backend.
  SnapshotPtr GetSnapshot() const;

  /// The default executor group name.
  static const std::string DEFAULT_EXECUTOR_GROUP;

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

 private:
  /// Serializes and adds the local backend descriptor to 'subscriber_topic_updates'.
  void AddLocalBackendToStatestore(const TBackendDescriptor& local_be_desc,
      std::vector<TTopicDelta>* subscriber_topic_updates);

  /// Returns the local backend descriptor or nullptr if no local backend has been
  /// registered.
  BeDescSharedPtr GetLocalBackendDescriptor();

  /// Notifies the ImpalaServer of a deleted backend through the registered callback for
  /// cluster membership changes if the callback is non-empty.
  void NotifyLocalServerForDeletedBackend(const BackendIdMap& current_backends);

  /// Notifies the Frontend through the registered callback for cluster membership changes
  /// if the callback is non-empty.
  void UpdateFrontendExecutorMembership(const BackendIdMap& current_backends,
      const ExecutorGroups executor_groups);

  /// Atomically replaces a membership snapshot with a new copy.
  void SetState(const SnapshotPtr& new_state);

  /// Checks if the local backend is available and its registration in 'state' matches the
  /// given backend descriptor in 'local_be_desc'. Returns false otherwise.
  bool NeedsLocalBackendUpdate(const Snapshot& state,
      const BeDescSharedPtr& local_be_desc);

  /// Checks that the backend ID map is consistent with 'executor_groups', i.e. all
  /// executors in all groups are also present in the map and are non-quiescing executors.
  /// This method should only be called in debug builds.
  bool CheckConsistency(const BackendIdMap& current_backends,
      const ExecutorGroups& executor_groups);

  /// Fake mutex to DCHECK that only one thread at a time calls UpdateMembership.
  DFAKE_MUTEX(update_membership_lock_);

  /// The snapshot of the current cluster membership. When receiving changes to the
  /// executors configuration from the statestore we will make a copy of the stored
  /// object, apply the updates to the copy and atomically swap the contents of this
  /// pointer.
  SnapshotPtr current_membership_;

  /// Protects current_membership_
  mutable boost::mutex current_membership_lock_;

  /// A temporary membership snapshot to hold updates while the statestore is in its
  /// post-recovery grace period. Not exposed to clients and not protected by any locking.
  std::shared_ptr<Snapshot> recovering_membership_;

  /// Pointer to a subscription manager (which we do not own) which is used to register
  /// for dynamic updates to the set of available backends. May be nullptr if the set of
  /// backends is fixed (only useful for tests).
  StatestoreSubscriber* statestore_subscriber_;

  /// Serializes TBackendDescriptors when creating topic updates
  ThriftSerializer thrift_serializer_;

  /// Unique - across the cluster - identifier for this impala backend. Used to validate
  /// incoming backend descriptors and to register this backend with the statestore.
  std::string local_backend_id_;

  /// Callbacks that provide external dependencies.
  BackendDescriptorPtrFn local_be_desc_fn_;
  UpdateLocalServerFn update_local_server_fn_;
  UpdateFrontendFn update_frontend_fn_;

  /// Protects the callbacks.
  mutable boost::mutex callback_fn_lock_;

  friend class impala::test::SchedulerWrapper;
};

} // end namespace impala
