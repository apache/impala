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

#include "statestore/state-store.h"

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
#include <thrift/transport/TTransportException.h>

#include "common/logging.h"
#include "common/status.h"
#include "util/metrics.h"
#include "util/thrift-server.h"
#include "util/thrift-client.h"
#include "util/thrift-util.h"
#include "util/container-util.h"
#include "util/webserver.h"
#include "statestore/failure-detector.h"
#include "gen-cpp/StateStoreService_types.h"
#include "gen-cpp/StateStoreSubscriberService_types.h"
#include "gen-cpp/Types_types.h"

using namespace boost;
using namespace boost::posix_time;
using namespace std;

using namespace ::apache::thrift::server;
using namespace ::apache::thrift::transport;

DEFINE_int32(statestore_num_server_worker_threads, 4,
             "number of worker threads for the thread manager underlying the "
             "State Store Thrift server");
DEFINE_int32(statestore_max_missed_heartbeats, 5, "Maximum number of consecutive "
             "heartbeats an impalad can miss before being declared failed by the "
             "state-store.");
DEFINE_int32(statestore_suspect_heartbeats, 2, "(Advanced) Number of consecutive "
             "heartbeats an impalad can miss before being suspected of failure by the "
             "state-store");

const string STATESTORE_LIVE_BACKENDS = "statestore.live-backends";
const string STATESTORE_LIVE_BACKENDS_LIST = "statestore.live-backends.list";
const string STATESTORE_SUBSCRIBER_STATE_MAP = "statestore.backend-state-map";

namespace impala {

StateStore::StateStore(int subscriber_update_frequency_ms, Metrics* metrics)
    : is_updating_(false),
      next_subscriber_id_(0),
      subscriber_update_frequency_ms_(subscriber_update_frequency_ms),
      failure_detector_(
          new MissedHeartbeatFailureDetector(FLAGS_statestore_max_missed_heartbeats,
                                             FLAGS_statestore_suspect_heartbeats)),
      metrics_(metrics) {
  DCHECK(metrics);
}

Status StateStore::RegisterWebpages(Webserver* server) {
  Webserver::PathHandlerCallback subscriptions_callback =
      bind<void>(mem_fn(&StateStore::SubscriptionsCallback), this, _1, _2);
  server->RegisterPathHandler("/subscriptions", subscriptions_callback);
  return Status::OK;
}

void StateStore::SubscriptionsCallback(const Webserver::ArgumentMap& args,
                                       stringstream* output) {
  (*output) << "<h2>Subscriptions</h2><pre>" << endl;
  lock_guard<recursive_mutex> l(lock_);

  BOOST_FOREACH(const Subscribers::value_type& subscriber, subscribers_) {
    (*output) << "Subscriber: " << subscriber.first.ipaddress << ":" << subscriber.first.port << endl;
    BOOST_FOREACH(const Subscriber::Subscriptions::value_type& subscription,
                  subscriber.second.subscriptions()) {
      (*output) << subscription.first << " :: ";
      (*output) << join(subscription.second, ", ") << endl;
    }
  }
  (*output) << "</pre>";
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

Status StateStore::UnregisterServiceInternal(const THostPort& address,
                                             const ServiceId& service_id) {

  lock_guard<recursive_mutex> lock(lock_);

  // Ensure the associated subscriber is registered (if it's not, the service is
  // definitely not registered).
  Subscribers::iterator subscriber_iterator =
      subscribers_.find(address);
  if (subscriber_iterator == subscribers_.end()) {
    stringstream error_message;
    error_message << "No registered instances at subscriber " << address;
    return Status(error_message.str());
  }
  Subscriber& subscriber = subscriber_iterator->second;

  // Check if the service is already registered.  If it isn't, return an
  // error to the client.
  ServiceMemberships::iterator service_membership = service_instances_.find(
      service_id);
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

      subscriber.RemoveService(service_id);
      if (subscriber.IsZombie()) {
        subscribers_.erase(subscriber_iterator);
      }
    }
  }
  if (!instance_unregistered) {
    stringstream error_message;
    error_message << "No instance for service " << service_id
                  << " at subscriber " << address;
    return Status(error_message.str());
  }

  num_backends_metric_->Increment(-1L);
  VLOG(2) << "Number of backends registered: " << num_backends_metric_->value();

  return Status::OK;
}

void StateStore::UnregisterService(TUnregisterServiceResponse& response,
                                   const TUnregisterServiceRequest& request) {
  RETURN_IF_UNSET(request, subscriber_address, response);
  RETURN_IF_UNSET(request, service_id, response);
  Status status = UnregisterServiceInternal(request.subscriber_address,
                                            request.service_id);
  if (status.ok()) RETURN_AND_SET_STATUS_OK(response);

  RETURN_AND_SET_ERROR(status.GetErrorMsg(), response);
}

void StateStore::RegisterSubscription(TRegisterSubscriptionResponse& response,
                                      const TRegisterSubscriptionRequest& request) {
  RETURN_IF_UNSET(request, subscriber_address, response);
  RETURN_IF_UNSET(request, services, response);

  lock_guard<recursive_mutex> lock(lock_);
  Subscriber& subscriber = GetOrCreateSubscriber(request.subscriber_address);
  subscriber.AddSubscription(request.services, request.subscription_id);

  LOG(INFO) << "Registered subscription (id: " << request.subscription_id << ", for: "
            << request.subscriber_address.ipaddress
            << ":" << request.subscriber_address.port
            << ") for " << request.services.size() << " topics ("
            << join(request.services, ", ") << ")";

  RETURN_AND_SET_STATUS_OK(response);
}

Status StateStore::UnregisterSubscriptionInternal(const THostPort& address,
                                                  const SubscriptionId& id) {

  lock_guard<recursive_mutex> lock(lock_);

  Subscribers::iterator subscriber_iterator =
      subscribers_.find(address);
  if (subscriber_iterator == subscribers_.end()) {
    stringstream error_message;
    error_message << "No registered subscriptions at subscriber " << address;
    return Status(error_message.str());
  }

  Subscriber& subscriber = subscriber_iterator->second;
  bool subscription_existed = subscriber.RemoveSubscription(id);
  if (!subscription_existed) {
    stringstream error_message;
    error_message << "No subscription with ID " << id
                  << " at subscriber " << address;
    return Status(error_message.str());
  }

  if (subscriber.IsZombie()) {
    subscribers_.erase(address);
  }

  return Status::OK;
}

void StateStore::UnregisterSubscription(TUnregisterSubscriptionResponse& response,
                                        const TUnregisterSubscriptionRequest& request) {
  RETURN_IF_UNSET(request, subscriber_address, response);
  RETURN_IF_UNSET(request, subscription_id, response);
  Status status = UnregisterSubscriptionInternal(request.subscriber_address,
                                                 request.subscription_id);
  if (status.ok()) RETURN_AND_SET_STATUS_OK(response);

  RETURN_AND_SET_ERROR(status.GetErrorMsg(), response);
}

Status StateStore::UnregisterSubscriberCompletely(const THostPort& address) {
  // Protect against the subscriber getting removed or updated concurrently
  lock_guard<recursive_mutex> lock(lock_);
  Subscribers::iterator subscriber_it = subscribers_.find(address);
  if (subscriber_it == subscribers_.end()) {
    LOG(INFO) << "Subscriber already removed: " << address;
    // Return quietly if the subscriber has already been removed
    return Status::OK;
  }

  Subscriber& subscriber = subscriber_it->second;
  subscriber.client()->Close();

  // Copy any services or subscriptions to remove, since it's possible that
  // removing the last service or subscription will invalidate subscriber (by
  // causing it to be removed from subscribers_ and its destructor called).
  vector<SubscriptionId> subscriptions_to_unregister;
  vector<ServiceId> services_to_unregister(subscriber.service_ids().begin(),
                                           subscriber.service_ids().end());
  BOOST_FOREACH(const Subscriber::Subscriptions::value_type& subscription,
      subscriber.subscriptions()) {
    subscriptions_to_unregister.push_back(subscription.first);
  }

  BOOST_FOREACH(const SubscriptionId& subscription, subscriptions_to_unregister) {
    RETURN_IF_ERROR(UnregisterSubscriptionInternal(address, subscription));
  }

  BOOST_FOREACH(const ServiceId& service_id, services_to_unregister) {
    RETURN_IF_ERROR(UnregisterServiceInternal(address, service_id));
  }

  // Expect that subscriber is automatically removed after last unregistration.
  // Therefore at this point subscriber_ may no longer be valid and should not
  // be dereferenced hereafter.
  DCHECK(subscribers_.find(address) == subscribers_.end());
  return Status::OK;
}

void StateStore::Start(int port) {
  // Create metrics
  num_backends_metric_ =
      metrics_->CreateAndRegisterPrimitiveMetric(STATESTORE_LIVE_BACKENDS, 0L);
  backend_set_metric_ =
      metrics_->RegisterMetric(new SetMetric<string>(STATESTORE_LIVE_BACKENDS_LIST,
              set<string>()));
  subscriber_state_metric_ =
      metrics_->RegisterMetric(new MapMetric<string, string>(
            STATESTORE_SUBSCRIBER_STATE_MAP, map<string, string>()));

  set_is_updating(true);
  update_thread_.reset(new thread(&StateStore::UpdateLoop, this));

  // If there isn't already a shared_ptr to this somewhere, this call will lead
  // to a boost runtime exception.
  shared_ptr<StateStore> state_store = shared_from_this();
  shared_ptr<TProcessor> processor(new StateStoreServiceProcessor(state_store));

  server_.reset(new ThriftServer("StateStoreService", processor, port,
      FLAGS_statestore_num_server_worker_threads));
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

void StateStore::Subscriber::AddService(const ServiceId& service_id) {
  service_ids_.insert(service_id);
}

void StateStore::Subscriber::RemoveService(const ServiceId& service_id) {
  service_ids_.erase(service_id);
}

void StateStore::Subscriber::AddSubscription(const set<ServiceId>& services,
                                             const SubscriptionId& id) {
  // If there was an existing subscription, remove it and make sure
  // service_subscription_counts_ is correct.
  // TODO: Revisit whether counts are necessary, or can be updated more elegantly.
  RemoveSubscription(id);
  unordered_set<ServiceId>& subscribed_services = subscriptions_[id];
  subscribed_services.insert(services.begin(), services.end());

  // Update the per-service subscription counts.
  BOOST_FOREACH(const ServiceId& service_id, services) {
    ++service_subscription_counts_[service_id];
  }
}

bool StateStore::Subscriber::RemoveSubscription(SubscriptionId id) {
  Subscriptions::iterator subscription = subscriptions_.find(id);
  if (subscription != subscriptions_.end()) {
    LOG(INFO) << "Remove subscription " << id << " for " << id_ << " on "
              << join(subscription->second, ", ") << ".";

    // For each subscribed service, decrease the associated count, and remove the
    // service from the list of services that should be updated at this subscriber if
    // the count has reached 0.
    BOOST_FOREACH(const ServiceId& service_id, subscription->second) {
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
  LOG(INFO) << "State-store entering heartbeat / update loop";

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
      FailureDetector::PeerState peer_state = FailureDetector::FAILED;

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
          LOG(INFO) << "Unable to update client at " << update.client->ipaddress() << ":"
                    << update.client->port() << "; received error " << e.what();
          peer_state = failure_detector_->UpdateHeartbeat(address, false);
        } catch (std::exception& e) {
          // Make sure Thrift isn't throwing any other exceptions.
          DCHECK(false) << e.what();
        }
      }

      subscriber_state_metric_->Add(address,
                                    FailureDetector::PeerStateToString(peer_state));

      if (peer_state == FailureDetector::FAILED) {
        LOG(INFO) << "Subscriber at " << address << " has failed, and will be removed.";
        Status status = UnregisterSubscriberCompletely(update.subscriber_address);
        if (!status.ok()) {
          // Should never happen; if the subscriber was removed concurrently
          // status will be OK and otherwise lock_ is held during
          // UnregisterSusbcriberCompletely, so this signifies a concurrency bug.
          // We can recover from it but it might leave zombie subscriptions around.
          LOG(ERROR) << "Could not unregister subscriber on failure: "
                     << status.GetErrorMsg();
        }
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
      const ServiceId& service_id = service_subscription.first;
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
