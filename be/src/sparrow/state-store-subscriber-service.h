// Copyright (c) 2012 Cloudera, Inc. All rights reserved.

#ifndef SPARROW_STATE_STORE_SUBSCRIBER_SERVICE_H
#define SPARROW_STATE_STORE_SUBSCRIBER_SERVICE_H

#include <string>

#include <boost/enable_shared_from_this.hpp>
#include <boost/scoped_ptr.hpp>
#include <boost/shared_ptr.hpp>
#include <boost/thread/mutex.hpp>
#include <boost/thread/thread.hpp>

#include "sparrow/util.h"
#include "sparrow/subscription-manager.h"
#include "util/thrift-util.h"
#include "util/thrift-client.h"
#include "gen-cpp/StateStoreService.h"
#include "gen-cpp/StateStoreSubscriberService.h"

namespace impala {

class Status;
class THostPort;
class ThriftServer;

} // namespace impala

namespace sparrow {

// The StateStoreSubscriber class implements the
// StateStoreSubscriberService interface, which allows it to receive
// updates from the central state store.  It also allows local
// services to register with and register for updates from the
// StateStore through implementing StateStoreSubcriberInternalIf.
// The class is generally not thread safe; however, the UpdateState() method
// (which is part of the StateStoreSubscriberIf) may be called concurrently
// with any other class method.
// TODO: Make this class thread safe, so it can be used by multiple in-process threads
// that all wish to register and unregister subscriptions and services.
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
      const boost::unordered_set<std::string>& update_services,
      SubscriptionManager::UpdateCallback* update, SubscriptionId* id);

  // Unregisters the subscription identified by the given id with the state store. Also
  // unregisters the associated callback, so that it will no longer be called.
  impala::Status UnregisterSubscription(const SubscriptionId& id);

  // Registers an instance of the given service type at the given
  // address with the state store.
  impala::Status RegisterService(const std::string& service_id,
      const impala::THostPort& address);

  // Unregisters an instance of the given service type with the state store.
  virtual impala::Status UnregisterService(const std::string& service_id);

  // StateStoreSubscriberServiceIf implementation

  // Implements the UpdateState() method of the thrift StateStateSubscriberServiceIf.
  // This method is thread-safe.
  virtual void UpdateState(TUpdateStateResponse& response,
      const TUpdateStateRequest& request);

 private:
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
  boost::mutex update_callbacks_lock_;

  // The thread that the callbacks are being executed in. If the callbacks are
  // not currently executing in any thread, calblack_thread_id_ will be set to the
  // default boost::thread::id value, which does not refer to any thread.
  boost::thread::id callback_thread_id_;

  // Lock for callback_thread_id_.  A lock is necessary because boost::thread::id may
  // not be an atomic value.
  boost::mutex callback_thread_id_lock_;

  // Services registered with this subscriber. Used to properly unregister if the
  // subscriber is Stop()ed.
  boost::unordered_set<std::string> services_;

  // Initializes client_, if it hasn't been intialized already.
  impala::Status InitClient();

  // Verifies that the current thread of execution is not the thread that is executing
  // callbacks. This function should be called before attempting to acquire
  // update_callbacks_lock_, and is used to ensure that UpdateCallbacks don't register
  // or unregister subscritions, which leads to deadlock.
  //
  // Since this function expects to be called before the update_callbacks_lock_ is
  // acquired, there is a possible race condition between verifiying that the current
  // thread is not the callback thread and acquiring update_callbacks_lock_. This is not
  // a problem because the race condition only occurs when this function and
  // UpdateState() are called from different threads, so it doesn't effect the case
  // we're trying to check for (when this function and UpdateState are called from the
  // same thread).
  void CheckNotInCallbackThread();

  // Executes an RPC to unregister the given service with the state store.
  impala::Status UnregisterServiceWithStateStore(const std::string& service_id);

  // Executes an RPC to unregister the given subscription with the state store.
  impala::Status UnregisterSubscriptionWithStateStore(const SubscriptionId& id);
};

}

#endif
