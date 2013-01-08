// Copyright 2012 Cloudera Inc.
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


#ifndef STATESTORE_STATE_STORE_SUBSCRIBER_SERVICE_H
#define STATESTORE_STATE_STORE_SUBSCRIBER_SERVICE_H

#include <string>

#include <boost/enable_shared_from_this.hpp>
#include <boost/scoped_ptr.hpp>
#include <boost/shared_ptr.hpp>
#include <boost/thread/mutex.hpp>
#include <boost/thread/thread.hpp>

#include "statestore/util.h"
#include "statestore/subscription-manager.h"
#include "util/thrift-util.h"
#include "util/thrift-client.h"
#include "gen-cpp/StateStoreService.h"
#include "gen-cpp/StateStoreSubscriberService.h"

namespace impala {

class TimeoutFailureDetector;
class Status;
class THostPort;
class ThriftServer;

} // namespace impala

namespace impala {

class StateStoreTest;

// The StateStoreSubscriber class implements the
// StateStoreSubscriberService interface, which allows it to receive
// updates from the central state store.  It also allows local
// services to register with and register for updates from the
// StateStore through implementing StateStoreSubcriberInternalIf.
// This class is thread-safe. It is always wrapped by a SubscriptionManager from
// the local client's perspective, which exposes only the APIs suitable for
// client use (other public methods in this class are invoked by RPC calls).
class StateStoreSubscriber
  : public StateStoreSubscriberServiceIf,
    public boost::enable_shared_from_this<StateStoreSubscriber> {
 public:
  StateStoreSubscriber(const std::string& hostname, const std::string& ipaddress,
                       int port,
                       const std::string& state_store_host, int state_store_port);

  ~StateStoreSubscriber();

  // Whether the StateStoreSubscriberService server is running.
  bool IsRunning();

  // Internal API implementation
  // Calling any of the Register or Unregister functions before starting the
  // StateStoreSubscriber service (using Start()) will cause an error.

  // Starts a thread that exports StateStoreSubscriberService. The thread will run until
  // the server terminates or Stop() is called. Before Start() is called,
  // a boost::shared_ptr<StateStoreSubscriber> to this StateStoreSubscriber must exist
  // somewhere outside of this class.
  impala::Status Start();

  // Stops exporting StateStoreSubscriberService and unregisters all subscriptions.
  void UnregisterAll();

  // Registers with the state store to receive updates for the given services.
  // Fills in the given id with an id identifying the subscription, which should be
  // used when unregistering.  The given UpdateCallback will be called with updates and
  // takes a single ServiceStateMap as a parameter, which contains a mapping of service
  // ids to the relevant state for that service. The callback may not Register() or
  // Unregister() any subscriptions.
  impala::Status RegisterSubscription(
      const boost::unordered_set<std::string>& update_services, const SubscriptionId& id,
      SubscriptionManager::UpdateCallback* update);

  // Unregisters the subscription identified by the given id with the state store. Also
  // unregisters the associated callback, so that it will no longer be called.
  impala::Status UnregisterSubscription(const SubscriptionId& id);

  // Registers an instance of the given service type at the given
  // address with the state store.
  // TODO: Make it a condition that this can be called only before Start()
  impala::Status RegisterService(const ServiceId& service_id,
      const impala::THostPort& address);

  // Unregisters an instance of the given service type with the state store.
  virtual impala::Status UnregisterService(const ServiceId& service_id);

  // StateStoreSubscriberServiceIf implementation

  // Implements the UpdateState() method of the thrift StateStateSubscriberServiceIf.
  // This method is thread-safe.
  virtual void UpdateState(TUpdateStateResponse& response,
      const TUpdateStateRequest& request);

 private:
  // Class-wide lock. Protects all subsequent members. Most private methods must
  // be called holding this lock; this is noted in the method comments.
  boost::mutex lock_;

 // Mapping of subscription ids to the associated callback. Because this mapping
 // stores a pointer to an UpdateCallback, memory errors will occur if an UpdateCallback
 // is deleted before being unregistered. The UpdateCallback destructor checks for
 // such problems, so that we will have an assertion failure rather than a memory error.
  typedef boost::unordered_map<SubscriptionId, SubscriptionManager::UpdateCallback*>
      UpdateCallbacks;

  static const char* DISCONNECTED_FROM_STATE_STORE_ERROR;

  // Whether the Thrift service is running.
  bool server_running_;

  // Address where the StateStoreSubscriberService is running.
  impala::THostPort host_port_;

  // Address of state store.
  impala::THostPort state_store_host_port_;

  // Thrift server.
  boost::scoped_ptr<impala::ThriftServer> server_;

  // Client to use to connect to the StateStore.
  boost::shared_ptr<impala::ThriftClient<StateStoreServiceClient> > client_;

  // Callback for all services that have registered for updates (indexed by the
  // associated SubscriptionId), and associated lock.
  UpdateCallbacks update_callbacks_;

  // Subscriptions registered from this subscriber. Used to properly reregister
  // if recovery mode is entered.
  typedef boost::unordered_map<SubscriptionId, boost::unordered_set<ServiceId> >
      SubscriptionRegistrations;
  SubscriptionRegistrations subscriptions_;

  // Services registered with this subscriber. Used to properly unregister if the
  // subscriber is Stop()ed, and to reregister if recovery mode is entered
  typedef boost::unordered_map<ServiceId, impala::THostPort> ServiceRegistrations;
  ServiceRegistrations services_;

  // Thread in which RecoveryModeChecker runs.
  boost::scoped_ptr<boost::thread> recovery_mode_thread_;

  // Failure detector that monitors heartbeats from the state-store.
  boost::scoped_ptr<impala::TimeoutFailureDetector> failure_detector_;

  // Initializes client_, if it hasn't been initialized already. Returns an
  // error if in recovery mode. Must be called with lock_ held.
  impala::Status InitClient();

  // Executes an RPC to unregister the given service with the state store. Must
  // be called with lock_ held.
  impala::Status UnregisterServiceInternal(const ServiceId& service_id);

  // Executes an RPC to unregister the given subscription with the state
  // store. Must be called with lock_ held.
  impala::Status UnregisterSubscriptionInternal(const SubscriptionId& id);

  // Must be called with lock_
  impala::Status RegisterServiceInternal(const ServiceId& service_id,
                                         const impala::THostPort& address);

  // Registers a subscription with the given ID to all services
  // Must be called with lock_
  impala::Status RegisterSubscriptionInternal(
      const boost::unordered_set<std::string>& update_services, const SubscriptionId& id,
      SubscriptionManager::UpdateCallback* update);

  // Run in a separate thread. In a loop, check failure_detector_ to see if the
  // state-store is still sending heartbeats. If not, enter 'recovery mode'
  // where a reconnection is repeatedly attempted. Once reconnected, all
  // existing subscriptions and services are reregistered and normal operation
  // resumes.
  // During recovery mode, any public methods that are started will block on
  // lock_, which is only released when recovery finishes. In practice, all
  // registrations are made early in the life of an impalad before the
  // state-store could be detected as failed.
  void RecoveryModeChecker();

  // Once the state-store is reconnected to after a failure, this method
  // re-registers all subscriptions and service instances. Must be called with
  // lock_ held.
  impala::Status Reregister();

  // Friend so that tests can force shutdown to simulate failure.
  friend class StateStoreTest;
};

}

#endif
