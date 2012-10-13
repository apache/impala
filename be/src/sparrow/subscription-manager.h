// Copyright (c) 2012 Cloudera, Inc. All rights reserved.

#ifndef SPARROW_SUBSCRIPTION_MANAGER_H
#define SPARROW_SUBSCRIPTION_MANAGER_H

#include <string>

#include <boost/function.hpp>
#include <boost/unordered_set.hpp>

#include "sparrow/util.h"

namespace impala {

class Status;
class THostPort;

} // namespace impala

namespace sparrow {

class StateStoreSubscriber;

// The SubscriptionManager is the local interface to Sparrow's state store. Clients of
// this class may register and unregister service instances, and subscribe to and
// unsubscribe from notifications about service membership and state changes.
class SubscriptionManager {
 public:
  // Function called to update a service with new state. Called in a separate thread to
  // the one in which it is registered.
  // TODO: Also return object updates using this callback.
  typedef boost::function<void (const ServiceStateMap& state)> UpdateCallbackFunction;

  // The UpdateCallback class is a lightweight wrappper for UpdateCallbackFunction,
  // which ensures that the callback is not registered with the SubscriptionManager
  // when it is destroyed. If the callback is still registered when it is destroyed, it
  // may be called after being destroyed, which will lead to a seg fault.
  class UpdateCallback {
   public:
    UpdateCallback(UpdateCallbackFunction callback_function)
      : callback_function_(callback_function), currently_registered_(false) {}

    // Checks to ensure that the callback function is not currently registered.
    ~UpdateCallback();

   private:
    friend class StateStoreSubscriber;

    UpdateCallbackFunction callback_function_;
    
    // Whether the callback function is currently registered with the subscription
    // manager.
    bool currently_registered_;
  };

  // Initializes a subscription manager based on flags used to describe the address of
  // the underlying StateStoreSubscriber (state_store_subscriber_host and
  // state_store_subscriber_port) and the address of the StateStore that the subscriber
  // contacts (state_store_host, state_store_port).
  SubscriptionManager();

  // Initialises a subscription manager based on explicit
  // configuration parameters, not flags.
  SubscriptionManager(const std::string& state_store_subscriber_host,
                      int state_store_subscriber_port,
                      const std::string& state_store_host,
                      int state_store_port);

  // Registers an instance of the given service type at the given
  // address with the state store.
  impala::Status RegisterService(const ServiceId& service_id,
      const impala::THostPort& address);

  // Unregisters an instance of the given service type with the state store.
  impala::Status UnregisterService(const ServiceId& service_id);

  // Registers with the state store to receive updates for the given services.
  // The given id will be the unique identifier for this subscription, and may
  // take on any non-empty value as long as it is unique within this subscription
  // manager. The given UpdateCallback will be called with updates and takes a
  // single ServiceStateMap as a parameter, which contains a mapping of service
  // ids to the relevant state for that service. update_callback is owned by the
  // caller.  The caller must not deallocate the memory until after calling
  // UnregisterSubscription().
  impala::Status RegisterSubscription(const boost::unordered_set<ServiceId>& services,
      const SubscriptionId& subscription, UpdateCallback* update_callback);

  // Unregisters the subscription identified by the given id with the state store. Also
  // unregisters the associated callback, so that it will no longer be called.
  impala::Status UnregisterSubscription(const SubscriptionId& id);

  // Starts the underlying server, which receives updates from the StateStore.
  impala::Status Start();

  // Unregisters any services or subscriptions that are currently
  // registered.
  impala::Status UnregisterAll();

 private:
  // Thrift requires a shared_ptr, which must have at least one active reference for the
  // lifetime of the state store subscriber (otherwise if the thrift server gets stopped
  // and the reference count gets decremented, the referent will be destroyed before other
  // references are done with it).
  boost::shared_ptr<StateStoreSubscriber> state_store_subscriber_;
};

}

#endif
