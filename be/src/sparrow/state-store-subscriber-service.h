// Copyright (c) 2012 Cloudera, Inc. All rights reserved.

#ifndef SPARROW_STATE_STORE_SUBSCRIBER_SERVICE_H
#define SPARROW_STATE_STORE_SUBSCRIBER_SERVICE_H

#include <string>

#include <boost/enable_shared_from_this.hpp>
#include <boost/shared_ptr.hpp>
#include <boost/thread/mutex.hpp>
#include <boost/thread/thread.hpp>
#include <server/TSimpleServer.h>

#include "sparrow/util.h"
#include "sparrow/subscription-manager.h"
#include "gen-cpp/StateStoreService.h"
#include "gen-cpp/StateStoreSubscriberService.h"

namespace impala {

class Status;
class THostPort;

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
class StateStoreSubscriber
  : public StateStoreSubscriberServiceIf,
    public boost::enable_shared_from_this<StateStoreSubscriber> {
 public:
  StateStoreSubscriber(const std::string& host, int port,
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
  void Stop();

  // Registers with the state store to receive updates for the given services.
  // Fills in the given id with an id identifying the subscription, which should be
  // used when unregistering.  The given UpdateCallback will be called with updates and
  // takes a single ServiceStateMap as a parameter, which contains a mapping of service
  // ids to the relevant state for that service.
  impala::Status RegisterSubscription(
      const SubscriptionManager::UpdateCallback& update,
      const boost::unordered_set<std::string>& update_services, SubscriptionId* id);

  // Unregisters the subscription identified by the given id with the state store. Also
  // unregisters the associated callback, so that it will no longer be called.
  impala::Status UnregisterSubscription(SubscriptionId id);

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
 // Mapping of subscription ids to the associated callback.
  typedef boost::unordered_map<SubscriptionId, SubscriptionManager::UpdateCallback>
      UpdateCallbacks;

  static const char* DISCONNECTED_FROM_STATE_STORE_ERROR;

  // Whether the Thrift service is running.
  bool server_running_;

  // Address where the StateStoreSubscriberService is running.
  impala::THostPort host_port_;

  // Address of state store.
  impala::THostPort state_store_host_port_;

  // Thread running the Thrift service.
  boost::shared_ptr<boost::thread> server_thread_;

  // Thrift server.
  boost::shared_ptr<apache::thrift::server::TSimpleServer> server_;

  // Client to use to connect to the StateStore.
  boost::shared_ptr<StateStoreServiceClient> client_;

  // Callback for all services that have registered for updates (indexed by the
  // associated SubscriptionId), and associated lock.
  UpdateCallbacks update_callbacks_;
  boost::mutex update_callbacks_lock_;

  // Services registered with this subscriber. Used to properly unregister if the
  // subscriber is Stop()ed.
  boost::unordered_set<std::string> services_;

  // Initializes client_, if it hasn't been intialized already.
  impala::Status InitClient();
};

}

#endif
