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

#include "statestore/state-store-subscriber.h"

#include <sstream>
#include <utility>

#include <boost/algorithm/string/join.hpp>
#include <boost/foreach.hpp>
#include <boost/date_time/posix_time/posix_time.hpp>
#include <boost/thread/shared_mutex.hpp>
#include <transport/TBufferTransports.h>

#include "common/logging.h"
#include "common/status.h"
#include "statestore/failure-detector.h"
#include "gen-cpp/StateStoreService_types.h"
#include "gen-cpp/StateStoreSubscriberService_types.h"
#include "util/thrift-util.h"

using namespace std;
using namespace boost;
using namespace boost::posix_time;
using namespace ::apache::thrift;
using namespace ::apache::thrift::transport;

DECLARE_int32(rpc_cnxn_attempts);
DECLARE_int32(rpc_cnxn_retry_interval_ms);
DEFINE_int32(statestore_subscriber_timeout_seconds, 10, "The amount of time (in seconds) "
    "that may elapse before the connection with the state-store is considered lost.");

namespace impala {

const char* StateStoreSubscriber::DISCONNECTED_FROM_STATE_STORE_ERROR =
    "Client disconnected from state store";

StateStoreSubscriber::StateStoreSubscriber(const string& hostname,
                                           const string& ipaddress, int port,
                                           const string& state_store_host,
                                           int state_store_port)
  : server_running_(false),
    failure_detector_(new TimeoutFailureDetector(
        seconds(FLAGS_statestore_subscriber_timeout_seconds),
        seconds(FLAGS_statestore_subscriber_timeout_seconds / 2))) {
  client_.reset();
  host_port_.ipaddress = ipaddress;
  host_port_.port = port;
  host_port_.hostname = hostname;
  state_store_host_port_.ipaddress = state_store_host;
  state_store_host_port_.hostname = state_store_host;
  state_store_host_port_.port = state_store_port;
}

StateStoreSubscriber::~StateStoreSubscriber() {
}

Status StateStoreSubscriber::RegisterServiceInternal(const ServiceId& service_id,
                                                     const THostPort& address) {
  // Precondition: lock_ is held entering this method
  RETURN_IF_ERROR(InitClient());
  TRegisterServiceRequest request;
  request.__set_subscriber_address(host_port_);
  request.__set_service_id(service_id);
  request.__set_service_address(address);
  TRegisterServiceResponse response;
  VLOG_CONNECTION << "Attempting to register service " << request.service_id
                  << " on address " << address.ipaddress << ":" << address.port
                  << ", to subscriber at " << request.subscriber_address.ipaddress << ":"
                  << request.subscriber_address.port;

  try {
    client_->iface()->RegisterService(response, request);
  } catch (TTransportException& e) {
    // Client has gotten disconnected from the state store.
    Status status(e.what());
    status.AddErrorMsg(DISCONNECTED_FROM_STATE_STORE_ERROR);
    return status;
  }
  Status status(response.status);
  if (status.ok()) services_[service_id] = address;
  return status;
}

Status StateStoreSubscriber::RegisterService(const ServiceId& service_id,
                                             const THostPort& address) {
  lock_guard<mutex> l(lock_);
  return RegisterServiceInternal(service_id, address);
}

Status StateStoreSubscriber::UnregisterService(const ServiceId& service_id) {
  // TODO: Consider moving registration / unregistration into RecoveryChecker
  // loop and performing asynchronously.
  lock_guard<mutex> l(lock_);
  ServiceRegistrations::iterator service = services_.find(service_id);
  if (service == services_.end()) {
    stringstream error_message;
    error_message << "Service id " << service_id
                  << " not registered with this subscriber";
    return Status(error_message.str());
  }

  Status status = UnregisterServiceInternal(service_id);
  if (status.ok()) services_.erase(service);
  return status;
}

Status StateStoreSubscriber::RegisterSubscriptionInternal(
    const unordered_set<ServiceId>& update_services, const SubscriptionId& id,
    SubscriptionManager::UpdateCallback* update_callback) {
  // Precondition: lock_ is held entering this method
  RETURN_IF_ERROR(InitClient());
  TRegisterSubscriptionRequest request;
  request.__set_subscriber_address(host_port_);
  request.services.insert(update_services.begin(), update_services.end());
  request.__isset.services = true;
  request.__set_subscription_id(id);
  TRegisterSubscriptionResponse response;
  VLOG_CONNECTION << "Attempting to register subscriber for services "
                  << algorithm::join(update_services, ", ") << " at "
                  << request.subscriber_address.ipaddress << ":"
                  << request.subscriber_address.port;

  try {
    client_->iface()->RegisterSubscription(response, request);
  } catch (TTransportException& e) {
    // Client has gotten disconnected from the state store.
    Status status(e.what());
    status.AddErrorMsg(DISCONNECTED_FROM_STATE_STORE_ERROR);
    return status;
  }

  Status status(response.status);
  if (!status.ok()) {
    return status;
  }
  subscriptions_[id] = update_services;

  update_callbacks_.insert(make_pair(id, update_callback));
  update_callback->currently_registered_ = true;
  return status;
}


Status StateStoreSubscriber::RegisterSubscription(
    const unordered_set<string>& update_services, const SubscriptionId& id,
    SubscriptionManager::UpdateCallback* update_callback) {
  lock_guard<mutex> l(lock_);
  return RegisterSubscriptionInternal(update_services, id, update_callback);
}

Status StateStoreSubscriber::UnregisterSubscription(const SubscriptionId& id) {
  lock_guard<mutex> l(lock_);
  {
    UpdateCallbacks::iterator callback = update_callbacks_.find(id);
    if (callback == update_callbacks_.end()) {
      stringstream error_message;
      error_message << "Subscription id " << id
                    << " not registered with this subscriber";
      return Status(error_message.str());
    }
  }

  Status status = UnregisterSubscriptionInternal(id);

  if (status.ok()) subscriptions_.erase(id);

  // Don't save the old iterator and use it to erase, because it may have been
  // invalidated by now.
  UpdateCallbacks::iterator callback = update_callbacks_.find(id);
  if (callback != update_callbacks_.end()) {
    DCHECK(callback->second->currently_registered_);
    callback->second->currently_registered_ = false;
    update_callbacks_.erase(callback);
  }
  return status;
}

void StateStoreSubscriber::UpdateState(TUpdateStateResponse& response,
                                       const TUpdateStateRequest& request) {
  RETURN_IF_UNSET(request, service_memberships, response);

  ServiceStateMap state;
  StateFromThrift(request, &state);
  failure_detector_->UpdateHeartbeat(state_store_host_port_.ipaddress, true);

  // Log all of the new state we just got.
  stringstream new_state;
  BOOST_FOREACH(const ServiceStateMap::value_type& service_state, state) {
    new_state << "State for service " << service_state.first << ":\n" << "Membership: ";
    BOOST_FOREACH(const Membership::value_type& instance,
                  service_state.second.membership) {
      new_state << instance.second.ipaddress << ":" << instance.second.port
                << " (at subscriber " << instance.first << "),";
    }
    new_state << "\n";
    // TODO: Log object updates here too, once we include them.
  }
  VLOG(4) << "Received new state:\n" << new_state.str();

  lock_guard<mutex> l(lock_);

  BOOST_FOREACH(UpdateCallbacks::value_type& update, update_callbacks_) {
    DCHECK(update.second->currently_registered_);
    update.second->callback_function_(state);
  }

  RETURN_AND_SET_STATUS_OK(response);
}

Status StateStoreSubscriber::Start() {
  shared_ptr<TProcessor> processor(
      new StateStoreSubscriberServiceProcessor(shared_from_this()));

  server_.reset(new ThriftServer("StateStoreSubscriber", processor, host_port_.port, 1));

  DCHECK(!server_running_);
  server_running_ = true;
  server_->Start();

  // Wait for up to 2s for the server to start, polling at 50ms intervals
  RETURN_IF_ERROR(impala::WaitForServer(host_port_.ipaddress, host_port_.port, 40, 50));

  LOG(INFO) << "StateStoreSubscriber listening on " << host_port_.port;

  recovery_mode_thread_.reset(
      new thread(&StateStoreSubscriber::RecoveryModeChecker, this));

  return Status::OK;
}

bool StateStoreSubscriber::IsRunning() {
  return server_running_;
}

void StateStoreSubscriber::UnregisterAll() {
  lock_guard<mutex> l(lock_);
  Status status = InitClient();
  if (status.ok()) {
    // Unregister all services.
    LOG(INFO) << "SERVICE HAS: " << services_.size();
    BOOST_FOREACH(const ServiceRegistrations::value_type& service, services_) {
      Status unregister_status = UnregisterServiceInternal(service.first);
      if (!unregister_status.ok()) {
        string error_msg;
        unregister_status.GetErrorMsg(&error_msg);
        LOG(ERROR) << "Error when unregistering service " << service.first << ":"
                   << error_msg;
        // Add the unregistration error message to status, which contains all error
        // messages. Keep going, rather than failing here, so we make a best-effort
        // attempt to unregister everything.
        status.AddErrorMsg(error_msg);
      }
    }
    services_.clear();
    // Unregister all subscriptions.
    BOOST_FOREACH(const UpdateCallbacks::value_type& callback_pair,
                  update_callbacks_) {
      DCHECK(callback_pair.second->currently_registered_);
      SubscriptionId id = callback_pair.first;
      Status unregister_status = UnregisterSubscriptionInternal(id);
      if (!unregister_status.ok()) {
        string error_msg;
        unregister_status.GetErrorMsg(&error_msg);
        LOG(ERROR) << "Error when unregistering subscription " << id << ":" << error_msg;
        // As above, add the unregistration error message to status.
        status.AddErrorMsg(error_msg);
      }
      callback_pair.second->currently_registered_ = false;
    }
    update_callbacks_.clear();
  }
}

Status StateStoreSubscriber::InitClient() {
  DCHECK(server_running_);
  if (client_.get() == NULL) {
    client_.reset(new ThriftClient<StateStoreServiceClient>(
        state_store_host_port_.ipaddress, state_store_host_port_.port));

    Status status = client_->OpenWithRetry(FLAGS_rpc_cnxn_attempts,
        FLAGS_rpc_cnxn_retry_interval_ms);
    if (!status.ok()) {
      client_.reset();
      return status;
    }
  }
  return Status::OK;
}

Status StateStoreSubscriber::UnregisterServiceInternal(const ServiceId& service_id) {
  // Precondition: lock_ is held entering this method
  RETURN_IF_ERROR(InitClient());
  TUnregisterServiceRequest request;
  request.__set_subscriber_address(host_port_);
  request.__set_service_id(service_id);
  TUnregisterServiceResponse response;

  try {
    client_->iface()->UnregisterService(response, request);
  } catch (TTransportException& e) {
    Status status(e.what());
    status.AddErrorMsg(DISCONNECTED_FROM_STATE_STORE_ERROR);
    return status;
  }
  return Status(response.status);
}

Status StateStoreSubscriber::UnregisterSubscriptionInternal(
    const SubscriptionId& id) {
  // Precondition: lock_ is held entering this method
  RETURN_IF_ERROR(InitClient());

  TUnregisterSubscriptionRequest request;
  request.__set_subscriber_address(host_port_);
  request.__set_subscription_id(id);
  TUnregisterSubscriptionResponse response;
  try {
    client_->iface()->UnregisterSubscription(response, request);
  } catch (TTransportException& e) {
    Status status(e.what());
    status.AddErrorMsg(DISCONNECTED_FROM_STATE_STORE_ERROR);
    return status;
  }

  return Status(response.status);
}

Status StateStoreSubscriber::Reregister() {
  RETURN_IF_ERROR(InitClient());
  BOOST_FOREACH(const ServiceRegistrations::value_type& service, services_) {
    RETURN_IF_ERROR(RegisterServiceInternal(service.first, service.second));
  }
  BOOST_FOREACH(const SubscriptionRegistrations::value_type& subscription,
                subscriptions_) {
    UpdateCallbacks::iterator cb;
    cb = update_callbacks_.find(subscription.first);
    if (cb == update_callbacks_.end()) {
      continue;
    }
    RETURN_IF_ERROR(RegisterSubscriptionInternal(subscription.second,
                                                 subscription.first, cb->second));
  }
  return Status::OK;
}

void StateStoreSubscriber::RecoveryModeChecker() {
  static const int SLEEP_INTERVAL_MS = 1000;
  // TODO: Should only start this when first connection is made
  failure_detector_->UpdateHeartbeat(state_store_host_port_.ipaddress, true);
  // Every few seconds, wake up and check if the failure detector has determined
  // that the state-store has failed from our perspective. If so, enter recovery
  // mode and try to reconnect, followed by reregistering all subscriptions and
  // services.
  // When entering recovery mode, the class-wide lock_ is taken to
  // ensure mutual exclusion with any operations in flight.
  while (true) {
    FailureDetector::PeerState peer_state =
        failure_detector_->GetPeerState(state_store_host_port_.ipaddress);
    if (peer_state == FailureDetector::FAILED) {
      // Take class-wide lock so that any client operations that start after this
      // will block
      lock_guard<mutex> l(lock_);
      // TODO: Metric
      LOG(WARNING) << "Lost connection to the state-store, entering recovery mode";

      // Need an interior loop so that it's easy to hang on to the scoped
      // recovery_mode_lock_ writer
      while (true) {
        // Force to be null so that InitClient will try to reopen
        client_.reset();

        // We're recovering. Try to open a client and re-register
        // TODO: Random sleep +/- to avoid correlated reconnects
        Status status = Reregister();
        if (status.ok()) {
          // Make sure to update failure detector so that we don't
          // immediately fail on the next loop while we're waiting for
          // heartbeats to resume.
          failure_detector_->UpdateHeartbeat(state_store_host_port_.ipaddress, true);
          // Break out of enclosing while (true) to top of outer-scope loop.
          break;
        } else {
          // Don't exit recovery mode, continue
          LOG(WARNING) << "Failed to re-register with state-store: "
                       << status.GetErrorMsg();
          usleep(SLEEP_INTERVAL_MS * 1000);
        }
      }
    } else { // peer_state == OK
      // Back to sleep
      usleep(SLEEP_INTERVAL_MS * 1000);
    }
  }
}

}
