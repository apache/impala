// Copyright (c) 2012 Cloudera, Inc. All rights reserved.

#include "sparrow/state-store-subscriber-service.h"

#include <sstream>
#include <utility>

#include <boost/algorithm/string/join.hpp>
#include <boost/foreach.hpp>
#include <transport/TBufferTransports.h>

#include "common/logging.h"
#include "common/status.h"
#include "gen-cpp/StateStoreService_types.h"
#include "gen-cpp/StateStoreSubscriberService_types.h"

using namespace std;
using namespace boost;
using namespace ::apache::thrift;
using namespace ::apache::thrift::transport;
using impala::Status;
using impala::THostPort;
using impala::ThriftClient;
using impala::ThriftServer;
using impala::TStatusCode;

DECLARE_int32(rpc_cnxn_attempts);
DECLARE_int32(rpc_cnxn_retry_interval_ms);

namespace sparrow {

const char* StateStoreSubscriber::DISCONNECTED_FROM_STATE_STORE_ERROR =
    "Client disconnected from state store";

StateStoreSubscriber::StateStoreSubscriber(const string& hostname,
                                           const string& ipaddress, int port,
                                           const string& state_store_host,
                                           int state_store_port)
  : server_running_(false) {
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

Status StateStoreSubscriber::RegisterService(const string& service_id,
                                             const THostPort& address) {
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
  if (status.ok()) {
    services_.insert(service_id);
  }
  return status;
}

Status StateStoreSubscriber::UnregisterService(const string& service_id) {
  unordered_set<string>::iterator service = services_.find(service_id);
  if (service == services_.end()) {
    stringstream error_message;
    error_message << "Service id " << service_id
                  << " not registered with this subscriber";
    return Status(error_message.str());
  }

  Status status = UnregisterServiceWithStateStore(service_id);
  if (status.ok()) {
    services_.erase(service);
  }
  return status;
}

Status StateStoreSubscriber::RegisterSubscription(
    const unordered_set<string>& update_services,
    SubscriptionManager::UpdateCallback* update_callback, SubscriptionId* id) {
  RETURN_IF_ERROR(InitClient());

  TRegisterSubscriptionRequest request;
  request.__set_subscriber_address(host_port_);
  request.services.insert(update_services.begin(), update_services.end());
  request.__isset.services = true;
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
  if (!response.__isset.subscription_id) {
    status.AddErrorMsg("Invalid response: subscription_id not set.");
    return status;
  }

  *id = response.subscription_id;
  CheckNotInCallbackThread();
  lock_guard<mutex> lock(update_callbacks_lock_);
  update_callbacks_.insert(make_pair(response.subscription_id, update_callback));
  update_callback->currently_registered_ = true;
  return status;
}

Status StateStoreSubscriber::UnregisterSubscription(const SubscriptionId& id) {
  {
    CheckNotInCallbackThread();
    lock_guard<mutex> lock(update_callbacks_lock_);
    UpdateCallbacks::iterator callback = update_callbacks_.find(id);
    if (callback == update_callbacks_.end()) {
      stringstream error_message;
      error_message << "Subscription id " << id
                    << " not registered with this subscriber";
      return Status(error_message.str());
    }
  }

  Status status = UnregisterSubscriptionWithStateStore(id);

  CheckNotInCallbackThread();
  lock_guard<mutex> lock(update_callbacks_lock_);
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

  {
    lock_guard<mutex> lock(callback_thread_id_lock_);
    callback_thread_id_ = boost::this_thread::get_id();
  }

  lock_guard<mutex> lock(update_callbacks_lock_);
  // callback_thread_id_lock_ cannot be held while executing the callbacks,
  // otherwise the deadlock that it helps prevent (on update_callbacks_lock_) would
  // occur on callback_thread_id_lock_ instead.
  // TODO: This is problematic if any of the callbacks take a long time. Eventually,
  // we'll probably want to execute the callbacks asynchronously in a different thread.  
  BOOST_FOREACH(UpdateCallbacks::value_type& update, update_callbacks_) {
    DCHECK(update.second->currently_registered_);
    update.second->callback_function_(state);
  }

  {
    lock_guard<mutex> lock(callback_thread_id_lock_);
    callback_thread_id_ = thread::id();
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
  return Status::OK;
}

bool StateStoreSubscriber::IsRunning() {
  return server_running_;
}

void StateStoreSubscriber::UnregisterAll() {
  Status status = InitClient();
  if (status.ok()) {
    // Unregister all services.
    BOOST_FOREACH(const string& service_id, services_) {
      Status unregister_status = UnregisterServiceWithStateStore(service_id);
      if (!unregister_status.ok()) {
        string error_msg;
        unregister_status.GetErrorMsg(&error_msg);
        LOG(ERROR) << "Error when unregistering service " << service_id << ":"
                   << error_msg;
        // Add the unregistration error message to status, which contains all error
        // messages. Keep going, rather than failing here, so we make a best-effort
        // attempt to unregister everything.
        status.AddErrorMsg(error_msg);
      }
    }
    services_.clear();

    // Unregister all subscriptions.
    CheckNotInCallbackThread();
    lock_guard<mutex> lock(update_callbacks_lock_);
    BOOST_FOREACH(const UpdateCallbacks::value_type& callback_pair,
                  update_callbacks_) {
      DCHECK(callback_pair.second->currently_registered_);
      SubscriptionId id = callback_pair.first;
      Status unregister_status = UnregisterSubscriptionWithStateStore(id);
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
    client_.reset(new ThriftClient<StateStoreServiceClient, impala::SPARROW_SERVER>(
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

void StateStoreSubscriber::CheckNotInCallbackThread() {
  lock_guard<mutex> lock(callback_thread_id_lock_);
  DCHECK_NE(callback_thread_id_, this_thread::get_id());
}


Status StateStoreSubscriber::UnregisterServiceWithStateStore(const string& service_id) {
  RETURN_IF_ERROR(InitClient());

  TUnregisterServiceRequest request;
  request.__set_subscriber_address(host_port_);
  request.__set_service_id(service_id);
  TUnregisterServiceResponse response;

  try {
    client_->iface()->UnregisterService(response, request);
  } catch ( TTransportException& e) {
    Status status(e.what());
    status.AddErrorMsg(DISCONNECTED_FROM_STATE_STORE_ERROR);
    return status;
  }
  return Status(response.status);
}

Status StateStoreSubscriber::UnregisterSubscriptionWithStateStore(
    const SubscriptionId& id) {
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

}
