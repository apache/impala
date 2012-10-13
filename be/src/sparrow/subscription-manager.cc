// Copyright (c) 2012 Cloudera, Inc. All rights reserved.

#include "sparrow/subscription-manager.h"

#include <string>

#include "common/logging.h"
#include "common/status.h"
#include "sparrow/state-store-subscriber.h"
#include "gen-cpp/StateStoreService_types.h"

using namespace std;
using namespace boost;
using impala::Status;
using impala::THostPort;

DEFINE_string(state_store_host, "localhost",
              "hostname where StateStoreService is running");
DEFINE_int32(state_store_port, 24000, "port where StateStoreService is running");
DECLARE_string(ipaddress);
DECLARE_string(hostname);
DEFINE_int32(state_store_subscriber_port, 23000,
             "port where StateStoreSubscriberService should be exported");

namespace sparrow {

SubscriptionManager::UpdateCallback::~UpdateCallback() {
  DCHECK(!currently_registered_);
}

SubscriptionManager::SubscriptionManager()
    : state_store_subscriber_(new StateStoreSubscriber(FLAGS_hostname, FLAGS_ipaddress,
        FLAGS_state_store_subscriber_port, FLAGS_state_store_host,
        FLAGS_state_store_port)) {
}

SubscriptionManager::SubscriptionManager(const string& state_store_subscriber_host,
    int state_store_subscriber_port, const string& state_store_host, int state_store_port)
    : state_store_subscriber_(new StateStoreSubscriber(state_store_subscriber_host,
        state_store_subscriber_host,
        state_store_subscriber_port, state_store_host, state_store_port)) {
}

Status SubscriptionManager::RegisterService(const ServiceId& service_id,
                                            const THostPort& address) {
  return state_store_subscriber_->RegisterService(service_id, address);
}

Status SubscriptionManager::UnregisterService(const ServiceId& service_id) {
  return state_store_subscriber_->UnregisterService(service_id);
}

Status SubscriptionManager::RegisterSubscription(const unordered_set<ServiceId>& services, 
    const SubscriptionId& id, UpdateCallback* update) {
  return state_store_subscriber_->RegisterSubscription(services, id, update);
}

Status SubscriptionManager::UnregisterSubscription(const SubscriptionId& id) {
  return state_store_subscriber_->UnregisterSubscription(id);
}

Status SubscriptionManager::Start() {
  LOG(INFO) << "Starting subscription manager";
  return state_store_subscriber_->Start();
}

Status SubscriptionManager::UnregisterAll() {
  state_store_subscriber_->UnregisterAll();
  return Status::OK;
}

}
