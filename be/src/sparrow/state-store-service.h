// Copyright (c) 2012 Cloudera, Inc. All rights reserved.

#ifndef SPARROW_STATE_STORE_SERVICE_H
#define SPARROW_STATE_STORE_SERVICE_H

#include <string>

#include <boost/enable_shared_from_this.hpp>
#include <boost/scoped_ptr.hpp>
#include <boost/shared_ptr.hpp>
#include <boost/thread/recursive_mutex.hpp>
#include <boost/thread/thread.hpp>
#include <boost/unordered_map.hpp>
#include <boost/unordered_set.hpp>
#include "util/metrics.h"
#include "util/non-primitive-metrics.h"
#include "util/thrift-client.h"
#include "util/thrift-server.h"

#include "sparrow/util.h"
#include "util/thrift-util.h"
#include "gen-cpp/SparrowTypes_types.h"
#include "gen-cpp/StateStoreService.h"
#include "gen-cpp/StateStoreSubscriberService.h"
#include "sparrow/failure-detector.h"

namespace impala {

class Status;
class THostPort;
}

namespace sparrow {

class TUpdateStateRequest;
class StateStoreTest;

// The StateStore is a single, centralized repository that stores soft state. It stores
// both membership information about the instances of each service, and generic
// versioned key-value pairs. The StateStoreServiceIf interface implementation is thread
// safe.
// TODO: Send membership changes as a delta, rather than a full update.
// TODO: Add versioned objects to the state store.
class StateStore : public StateStoreServiceIf,
                   public boost::enable_shared_from_this<StateStore> {
 public:
  // Frequency at which subscribers are updated.
  static const int DEFAULT_UPDATE_FREQUENCY_MS = 1000;

  StateStore(int subscriber_update_frequency_ms, impala::Metrics* metrics);

  // StateStoreServiceIf RPCS.
  virtual void RegisterService(TRegisterServiceResponse& response,
                               const TRegisterServiceRequest& request);
  virtual void UnregisterService(TUnregisterServiceResponse& response,
                                 const TUnregisterServiceRequest& request);
  virtual void RegisterSubscription(TRegisterSubscriptionResponse& response,
                                    const TRegisterSubscriptionRequest& request);
  virtual void UnregisterSubscription(TUnregisterSubscriptionResponse& response,
                                      const TUnregisterSubscriptionRequest& request);

  // Starts the state store by starting one new thread to perform updates and a second
  // new thread that exports StateStoreService on the given port. Before Start() is
  // called, there must be a boost::shared_ptr<StateStore> to this StateStore.
  void Start(int port);

  // Stops the server. Once the server is stopped it may not be restarted.
  void Stop();

  // Blocks until the server stops (which will occur if the server
  // returns due to an error, for example). Note that is_updating does
  // not control whether the Thrift server is running.
  void WaitForServerToStop();

  int subscriber_update_frequency_ms() { return subscriber_update_frequency_ms_; }

 private:
  typedef impala::ThriftClient<StateStoreSubscriberServiceClient> SubscriberClient;

  // Describes a subscriber connected to the StateStore. This class is not thread safe,
  // which is fine because access to subscribers_ is always protected by a lock.
  class Subscriber {
   public:
    // Count of the number of registered subscriptions for each service id.
    typedef boost::unordered_map<std::string, int> ServiceSubscriptionCounts;

    // Mapping between a subscription id, and a list of service ids for which updates
    // should be pushed.
    typedef boost::unordered_map<SubscriptionId, boost::unordered_set<std::string> >
        Subscriptions;

    Subscriber(SubscriberId id) : id_(id), next_subscription_id_(0) {};

    // Initializes the underlying thrift transport and StateStoreSubscriberServiceClient,
    // but doesn't open the transport.
    void Init(const impala::THostPort& address);

    // Adds an instance of the given service.
    void AddService(const std::string& service_id);

    // Removes the instance of the given service.
    void RemoveService(const std::string& service_id);

    // Add a subscription for the given services. Returns a identifier for the
    // subscription that's unique within this subscriber.
    SubscriptionId AddSubscription(const std::set<std::string>& services);

    // Removes the subscription with the given identifier. Returns true if the
    // subscription was removed, and false if the subscription did not exist.
    bool RemoveSubscription(SubscriptionId id);

    // Returns true if the subscriber has no more subscriptions and no registered
    // service instances (so needs to be cleaned up), and false otherwise.
    bool IsZombie();

    SubscriberId id() { return id_; };

    boost::shared_ptr<SubscriberClient> client() const {
      return client_;
    };

    const ServiceSubscriptionCounts& service_subscription_counts() const {
      return service_subscription_counts_;
    }

    const Subscriptions& subscriptions() const {
      return subscriptions_;
    }

    const boost::unordered_set<std::string>& service_ids() const {
      return service_ids_;
    }

   private:

    // Unique identifier for the subscriber.
    SubscriberId id_;

    // Next identifier to use for a subscription.
    SubscriptionId next_subscription_id_;

    // Exported services that are associated with the subscriber (needed when a subscriber
    // become unreachable, to determine which service instances should also be marked
    // as unreachable).
    boost::unordered_set<std::string> service_ids_;

    Subscriptions subscriptions_;

    ServiceSubscriptionCounts service_subscription_counts_;

    // Thrift connection information.
    boost::shared_ptr<SubscriberClient> client_;
  };

  // Information needed to update a subscriber with the latest state. Because we use
  // shared pointers to the thrift transport and client, it is fine if the corresponding
  // Subscriber gets deleted before this SubscriberUpdate is used.
  struct SubscriberUpdate {
    // Address of the subscriber to receive this update
    impala::THostPort subscriber_address;

    boost::shared_ptr<SubscriberClient> client;
    TUpdateStateRequest request;

    SubscriberUpdate(const impala::THostPort& address, Subscriber* subscriber)
      : subscriber_address(address),
        client(subscriber->client()) {}
  };

  // Mapping of service ids to the corresponding membership.
  typedef boost::unordered_map<std::string, Membership> ServiceMemberships;

  // Information about each subscriber, indexed by the address of the subscriber.
  typedef boost::unordered_map<impala::THostPort, Subscriber> Subscribers;
  Subscribers subscribers_;

  // Lock for is_updating_. This lock is necessary for visibility: it ensures that the
  // change to is_updating_ will be visible in the update loop.
  boost::mutex is_updating_lock_;

  // Whether updates are currently being performed. Must be volatile because it is
  // updated in one thread and read in a different one (making it volatile prevents
  // the compiler from caching the value in a register, for example).
  volatile bool is_updating_;

  boost::scoped_ptr<boost::thread> update_thread_;

  boost::scoped_ptr<impala::ThriftServer> server_;

  // Protects all following member variables. Recursive because all of the RPC methods
  // take this lock before modifying member variables, but many of them subsequently
  // call GetOrCreateSubscriber(), which also needs the lock.
  boost::recursive_mutex lock_;

  // A set of instances for each service.
  ServiceMemberships service_instances_;

  // Next id to use for a StateStoreSubscriber.
  SubscriberId next_subscriber_id_;

  // Frequency of updates to subscribers
  int subscriber_update_frequency_ms_;

  boost::scoped_ptr<impala::MissedHeartbeatFailureDetector> failure_detector_;

  // May not be NULL. Not owned by us.
  impala::Metrics* metrics_;

  // Metric that tracks the number of backends registered and alive.
  // Should only measure live IMPALAD backends, but because there
  // aren't any other types right now, so just tracks the total
  // services registered which is the same thing.
  impala::Metrics::IntMetric* num_backends_metric_;
  impala::SetMetric<std::string>* backend_set_metric_;
  impala::MapMetric<std::string, std::string>* backend_state_metric_;

  // Getter and setter for is_updating_, both are thread safe.
  bool is_updating();
  void set_is_updating(bool is_updating);

  // Adds a subscriber corresponding to the given THostPort to subscribers_, if it is
  // not there already, and returns a reference to the Subscriber.
  Subscriber& GetOrCreateSubscriber(const impala::THostPort& host_port);

  // Begins updating all StateStoreSubscriberServices with the new state.  Should be
  // called in its own thread, because this method blocks until is_updating_ is false.
  void UpdateLoop();

  // Fills in updates with a SubscriberUpdate (including a filled in TUpdateStateRequest)
  // for each currently registered subscriber.
  void GenerateUpdates(std::vector<StateStore::SubscriberUpdate>* updates);

  // Removes all subscriptions and registered services for the subscriber with
  // the given address.
  void UnregisterSubscriberCompletely(const impala::THostPort& address);

  // Friend class so that tests can manipulate internal data structures
  friend class StateStoreTest;
};

}

#endif
