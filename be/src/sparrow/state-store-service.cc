// Copyright (c) 2012 Cloudera, Inc. All rights reserved.

#include "sparrow/state-store-service.h"

#include <exception>
#include <utility>
#include <sstream>
#include <vector>
#include <set>

#include <boost/foreach.hpp>
#include <boost/thread/locks.hpp>
#include <boost/thread/thread_time.hpp>
#include <boost/algorithm/string/join.hpp>
#include <boost/date_time/posix_time/posix_time_types.hpp>
#include <transport/TTransportException.h>

#include "common/logging.h"
#include "common/status.h"
#include "util/metrics.h"
#include "util/thrift-server.h"
#include "util/thrift-client.h"
#include "util/container-util.h"
#include "sparrow/failure-detector.h"
#include "gen-cpp/StateStoreService_types.h"
#include "gen-cpp/StateStoreSubscriberService_types.h"
#include "gen-cpp/Types_types.h"

using namespace boost;
using namespace boost::posix_time;
using namespace std;

using namespace ::apache::thrift::server;
using namespace ::apache::thrift::transport;
using impala::Status;
using impala::THostPort;
using impala::TStatusCode;
using impala::ThriftServer;
using impala::Metrics;
using impala::MapMetric;
using impala::SetMetric;
using impala::FailureDetector;
using impala::MissedHeartbeatFailureDetector;

DEFINE_int32(state_store_num_server_worker_threads, 4,
             "number of worker threads for the thread manager underlying the "
             "State Store Thrift server");
DEFINE_int32(state_store_max_missed_heartbeats, 5, "Maximum number of consecutive "
             "heartbeats an impalad can miss before being declared failed by the "
             "state-store.");
DEFINE_int32(state_store_suspect_heartbeats, 2, "(Advanced) Number of consecutive "
             "heartbeats an impalad can miss before being suspected of failure by the "
             "state-store");

const string STATESTORE_LIVE_BACKENDS = "statestore.live.backends";
const string STATESTORE_LIVE_BACKENDS_LIST = "statestore.live.backends.list";
const string STATESTORE_BACKEND_STATE_MAP = "statestore.backend.state.map";

namespace sparrow {

StateStore::StateStore(int subscriber_update_frequency_ms, Metrics* metrics) 
    : is_updating_(false), 
      next_subscriber_id_(0),
      subscriber_update_frequency_ms_(subscriber_update_frequency_ms),
      failure_detector_(
          new MissedHeartbeatFailureDetector(FLAGS_state_store_max_missed_heartbeats, 
                                             FLAGS_state_store_suspect_heartbeats)),
      metrics_(metrics) {
  DCHECK(metrics);
}

void StateStore::RegisterService(TRegisterServiceResponse& response,
                                 const TRegisterServiceRequest& request) {
  RETURN_IF_UNSET(request, subscriber_address, response);
  RETURN_IF_UNSET(request, service_id, response);
  RETURN_IF_UNSET(request, service_address, response);

  lock_guard<recursive_mutex> lock(lock_);

  // Create a new entry in service_instances_ for the service id in the request,
  // if necessary.
  ServiceMemberships::iterator service_membership = service_instances_.find(
      request.service_id);
  if (service_membership == service_instances_.end()) {
    service_membership = service_instances_.insert(
        make_pair(request.service_id, Membership())).first;
  }
  Membership& membership = service_membership->second;

  // Add the service (and associated address) given by the request, if it's not already
  // registered.
  Subscriber& subscriber = GetOrCreateSubscriber(request.subscriber_address);
  if (membership.find(subscriber.id()) == membership.end()) {
    membership.insert(make_pair(subscriber.id(), request.service_address));
    subscriber.AddService(request.service_id);
    LOG(INFO) << "Registered service instance (id: "
              << request.service_id << ", for: "
              << request.service_address.hostname << "("
              << request.service_address.ipaddress << "):"
              << request.service_address.port
              << ")";
    num_backends_metric_->Increment(1L);
    stringstream ss;
    ss << request.service_address.ipaddress << ":" << request.service_address.port;
    backend_set_metric_->Add(ss.str());
    VLOG(2) << "Number of backends registered: " << num_backends_metric_->value();
  }
  RETURN_AND_SET_STATUS_OK(response);
}

void StateStore::UnregisterService(TUnregisterServiceResponse& response,
                                   const TUnregisterServiceRequest& request) {
  RETURN_IF_UNSET(request, subscriber_address, response);
  RETURN_IF_UNSET(request, service_id, response);

  lock_guard<recursive_mutex> lock(lock_);

  // Ensure the associated subscriber is registered (if it's not, the service is
  // definitely not registered).
  Subscribers::iterator subscriber_iterator =
      subscribers_.find(request.subscriber_address);
  if (subscriber_iterator == subscribers_.end()) {
    stringstream error_message;
    error_message << "No registered instances at subscriber "
                  << request.subscriber_address.ipaddress << ":"
                  << request.subscriber_address.port;
    RETURN_AND_SET_ERROR(error_message.str(), response);
  }
  Subscriber& subscriber = subscriber_iterator->second;

  // Check if the service is already registered.  If it isn't, return an
  // error to the client.
  ServiceMemberships::iterator service_membership = service_instances_.find(
      request.service_id);
  bool instance_unregistered = false;
  if (service_membership != service_instances_.end()) {
    Membership& membership = service_membership->second;
    Membership::iterator instance = membership.find(subscriber.id());
    if (instance != membership.end()) {
      instance_unregistered = true;
      stringstream ss;
      ss << instance->second.ipaddress << ":" << instance->second.port;
      backend_set_metric_->Remove(ss.str());

      membership.erase(instance);
      if (membership.empty()) {
        service_instances_.erase(service_membership);
      }

      subscriber.RemoveService(request.service_id);
      if (subscriber.IsZombie()) {
        subscribers_.erase(subscriber_iterator);
      }
    }
  }
  if (!instance_unregistered) {
    stringstream error_message;
    error_message << "No instance for service " << request.service_id
                  << " at subscriber " << request.subscriber_address.ipaddress << ":"
                  << request.subscriber_address.port;
    RETURN_AND_SET_ERROR(error_message.str(), response);
  }

  num_backends_metric_->Increment(-1L);
  VLOG(2) << "Number of backends registered: " << num_backends_metric_->value();

  RETURN_AND_SET_STATUS_OK(response);
}

void StateStore::RegisterSubscription(TRegisterSubscriptionResponse& response,
                                      const TRegisterSubscriptionRequest& request) {
  RETURN_IF_UNSET(request, subscriber_address, response);
  RETURN_IF_UNSET(request, services, response);

  lock_guard<recursive_mutex> lock(lock_);
  Subscriber& subscriber = GetOrCreateSubscriber(request.subscriber_address);

  SubscriptionId subscription_id = subscriber.AddSubscription(request.services);
  response.__set_subscription_id(subscription_id);
  LOG(INFO) << "Registered subscription (id: " << subscription_id << ", for: "
            << request.subscriber_address.ipaddress
            << ":" << request.subscriber_address.port
            << ") for " << request.services.size() << " topics (" 
            << join(request.services, ", ") << ")";

  RETURN_AND_SET_STATUS_OK(response);
}

void StateStore::UnregisterSubscription(TUnregisterSubscriptionResponse& response,
                                        const TUnregisterSubscriptionRequest& request) {
  RETURN_IF_UNSET(request, subscriber_address, response);
  RETURN_IF_UNSET(request, subscription_id, response);

  lock_guard<recursive_mutex> lock(lock_);

  Subscribers::iterator subscriber_iterator =
      subscribers_.find(request.subscriber_address);
  if (subscriber_iterator == subscribers_.end()) {
    stringstream error_message;
    error_message << "No registered subscriptions at subscriber "
                  << request.subscriber_address.ipaddress << ":"
                  << request.subscriber_address.port;
    RETURN_AND_SET_ERROR(error_message.str(), response);
  }

  Subscriber& subscriber = subscriber_iterator->second;
  bool subscription_existed = subscriber.RemoveSubscription(request.subscription_id);
  if (!subscription_existed) {
    stringstream error_message;
    error_message << "No subscription with ID " << request.subscription_id
                  << " at subscriber " << request.subscriber_address.ipaddress << ":"
                  << request.subscriber_address.port;
    RETURN_AND_SET_ERROR(error_message.str(), response);
  }

  if (subscriber.IsZombie()) subscribers_.erase(request.subscriber_address);
  RETURN_AND_SET_STATUS_OK(response);
}

void StateStore::UnregisterSubscriberCompletely(const THostPort& address) {
  // Protect against the subscriber getting removed or updated concurrently
  lock_guard<recursive_mutex> lock(lock_);
  Subscribers::iterator subscriber_it = subscribers_.find(address);
  if (subscriber_it == subscribers_.end()) {
    // Return quietly if the subscriber has already been removed
    return;
  }

  Subscriber& subscriber = subscriber_it->second;
  subscriber.client()->Close();
  // TODO: Make *Internal version of thrift calls so we don't have to construct
  // T* structures.
  BOOST_FOREACH(const Subscriber::Subscriptions::value_type& subscription,
      subscriber.subscriptions()) {
    TUnregisterSubscriptionRequest request;
    request.__set_subscriber_address(address);
    request.__set_subscription_id(subscription.first);
    TUnregisterSubscriptionResponse response;
    UnregisterSubscription(response, request);
  }

  BOOST_FOREACH(const string& service_id, subscriber.service_ids()) {
    TUnregisterServiceRequest request;
    request.__set_subscriber_address(address);
    request.__set_service_id(service_id);
    TUnregisterServiceResponse response;
    UnregisterService(response, request);    
  }

  // Expect that subscriber is automatically removed after last unregistration.
  DCHECK(subscribers_.find(address) == subscribers_.end());
  DCHECK(subscriber.IsZombie());
}

void StateStore::Start(int port) {
  // Create metrics
  num_backends_metric_ = 
      metrics_->CreateAndRegisterPrimitiveMetric(STATESTORE_LIVE_BACKENDS, 0L);
  backend_set_metric_ = 
      metrics_->RegisterMetric(new SetMetric<string>(STATESTORE_LIVE_BACKENDS_LIST,
              set<string>()));
  backend_state_metric_ = 
    metrics_->RegisterMetric(new MapMetric<string, string>(
            STATESTORE_BACKEND_STATE_MAP, map<string, string>()));

  set_is_updating(true);
  update_thread_.reset(new thread(&StateStore::UpdateLoop, this));

  // If there isn't already a shared_ptr to this somewhere, this call will lead
  // to a boost runtime exception.
  shared_ptr<StateStore> state_store = shared_from_this();
  shared_ptr<TProcessor> processor(new StateStoreServiceProcessor(state_store));

  server_.reset(new ThriftServer("StateStoreService", processor, port,
      FLAGS_state_store_num_server_worker_threads));
  server_->Start();

  LOG(INFO) << "StateStore listening on " << port;
}

void StateStore::Stop() {
  set_is_updating(false);
  if (update_thread_.get() != NULL) {
    update_thread_->join();
  }
}

void StateStore::WaitForServerToStop() {
  server_->Join();
}

void StateStore::Subscriber::Init(const THostPort& address) {
  client_.reset(new SubscriberClient(address.hostname, address.port));
}

void StateStore::Subscriber::AddService(const string& service_id) {
  service_ids_.insert(service_id);
}

void StateStore::Subscriber::RemoveService(const string& service_id) {
  service_ids_.erase(service_id);
}

SubscriptionId StateStore::Subscriber::AddSubscription(const set<string>& services) {
  SubscriptionId subscription_id = next_subscription_id_++;
  // Insert the new subscription with an empty set of services (to avoid copying the
  // subscribed services unnecessarily), and then add the given services.
  Subscriptions::value_type new_subscription =
      make_pair(subscription_id, unordered_set<string>());
  unordered_set<string>& subscribed_services = subscriptions_.insert(
      new_subscription).first->second;
  subscribed_services.insert(services.begin(), services.end());

  // Update the per-service subscription counts.
  BOOST_FOREACH(const string& service_id, services) {
    ServiceSubscriptionCounts::iterator service_subscription_count =
        service_subscription_counts_.find(service_id);
    if (service_subscription_count == service_subscription_counts_.end()) {
      service_subscription_count = service_subscription_counts_.insert(
          make_pair(service_id, 0)).first;
    }
    ++service_subscription_count->second;
  }
  return subscription_id;
}

bool StateStore::Subscriber::RemoveSubscription(SubscriptionId id) {
  Subscriptions::iterator subscription = subscriptions_.find(id);
  if (subscription != subscriptions_.end()) {
    LOG(INFO) << "Remove subscription " << id << " for " << id_ << " on "
              << join(subscription->second, ", ") << ".";

    // For each subscribed service, decrease the associated count, and remove the
    // service from the list of services that should be updated at this subscriber if
    // the count has reached 0.
    BOOST_FOREACH(const string& service_id, subscription->second) {
      ServiceSubscriptionCounts::iterator service_subscription_count =
          service_subscription_counts_.find(service_id);
      DCHECK(service_subscription_count != service_subscription_counts_.end());
      DCHECK_GT(service_subscription_count->second, 0);
      --service_subscription_count->second;
      if (service_subscription_count->second == 0) {
        service_subscription_counts_.erase(service_subscription_count);
      }
    }
    subscriptions_.erase(subscription);
    return true;
  }
  return false;
}

bool StateStore::Subscriber::IsZombie() {
  return subscriptions_.empty() && service_ids_.empty();
}

bool StateStore::is_updating() {
  lock_guard<mutex> lock(is_updating_lock_);
  return is_updating_;
}

void StateStore::set_is_updating(bool is_updating) {
  lock_guard<mutex> lock(is_updating_lock_);
  is_updating_ = is_updating;
}

StateStore::Subscriber& StateStore::GetOrCreateSubscriber(const THostPort& host_port) {
  lock_guard<recursive_mutex> lock(lock_);
  Subscribers::iterator subscriber = subscribers_.find(host_port);
  if (subscriber == subscribers_.end()) {
    subscriber = subscribers_.insert(
        make_pair(host_port, Subscriber(next_subscriber_id_++))).first;
    subscriber->second.Init(host_port);
  }
  return subscriber->second;
}

void StateStore::UpdateLoop() {
  LOG(INFO) << "Beginning to pull/push updates";

  system_time next_update_time =
      (get_system_time() + posix_time::milliseconds(subscriber_update_frequency_ms_));
  vector<SubscriberUpdate> subscriber_updates;
  while (is_updating()) {
    GenerateUpdates(&subscriber_updates);

    // Update each subscriber with the latest state.
    // TODO: Make this multithreaded.
    BOOST_FOREACH(SubscriberUpdate& update, subscriber_updates) {
      string address;
      THostPortToString(update.subscriber_address, &address);
      // Will be set in the following if-else block
      FailureDetector::PeerState peer_state;

      // Open the transport here so that we keep retrying if we don't succeed on the
      // first attempt to open a connection.
      Status status = update.client->Open();
      if (!status.ok()) {
        // Log failure messages only when peer state is OK; once it is FAILED or
        // SUSPECTED suppress messages to avoid log spam.
        if (failure_detector_->GetPeerState(address) == FailureDetector::OK) {
          LOG(INFO) << "Unable to update client at " << update.client->ipaddress()
                    << ":" << update.client->port() << "; received error "
                    << status.GetErrorMsg();
        }

        peer_state = failure_detector_->UpdateHeartbeat(address, false);
      } else {      
        TUpdateStateResponse response;
        try {
          update.client->iface()->UpdateState(response, update.request);
          if (response.status.status_code != TStatusCode::OK) {
            Status status(response.status);
            LOG(WARNING) << status.GetErrorMsg();
            peer_state = failure_detector_->UpdateHeartbeat(address, false);
          } else {
            peer_state = failure_detector_->UpdateHeartbeat(address, true);
          }
          
        } catch (TTransportException& e) {
          LOG(INFO) << "Unable to update client at " << update.client 
                    << "; received error " << e.what();
          peer_state = failure_detector_->UpdateHeartbeat(address, false);
        } catch (std::exception& e) {
          // Make sure Thrift isn't throwing any other exceptions.
          DCHECK(false) << e.what();
        }
      }

      backend_state_metric_->Add(address, FailureDetector::PeerStateToString(peer_state));

      if (peer_state == FailureDetector::FAILED) {
        LOG(INFO) << "Subscriber at " << address << " has failed, and will be removed.";
        UnregisterSubscriberCompletely(update.subscriber_address);
      }
    }
    
    if (get_system_time() < next_update_time && is_updating()) {
      posix_time::time_duration duration = next_update_time - get_system_time();
      usleep(duration.total_microseconds());
    }
    next_update_time = 
      get_system_time() + posix_time::milliseconds(subscriber_update_frequency_ms_);
  }
}

void StateStore::GenerateUpdates(vector<StateStore::SubscriberUpdate>* updates) {
  lock_guard<recursive_mutex> lock(lock_);
  updates->clear();

  // For each subscriber, generate the corresponding SubscriberUpdate (and fill in the
  // TUpdateRequest).
  BOOST_FOREACH(Subscribers::value_type& subscriber, subscribers_) {
    updates->push_back(SubscriberUpdate(subscriber.first, &subscriber.second));
    SubscriberUpdate& subscriber_update = updates->back();
    BOOST_FOREACH(
        const Subscriber::ServiceSubscriptionCounts::value_type& service_subscription,
        subscriber.second.service_subscription_counts()) {
      const string& service_id = service_subscription.first;
      // Check if any instances exist for the service described by service_subscription,
      // and if they do, add them to the request.
      ServiceMemberships::iterator service_membership =
          service_instances_.find(service_id);
      if (service_membership != service_instances_.end()) {
        // Add the membership information for the given service. Add an empty membership
        // and then modify it, to avoid copying all membership information twice.
        subscriber_update.request.service_memberships.push_back(TServiceMembership());
        TServiceMembership& new_membership =
            subscriber_update.request.service_memberships.back();
        MembershipToThrift(service_membership->second,
                           &new_membership.service_instances);
        new_membership.service_id = service_id;
      }
    }
    subscriber_update.request.__isset.service_memberships = true;
  }
}

}
