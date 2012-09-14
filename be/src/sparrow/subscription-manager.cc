// Copyright (c) 2012 Cloudera, Inc. All rights reserved.

#include "sparrow/subscription-manager.h"

#include <string>

#include "common/logging.h"
#include "common/status.h"
#include "sparrow/state-store-subscriber-service.h"
#include "gen-cpp/StateStoreService_types.h"

using namespace std;
using namespace boost;
using impala::Status;
using impala::THostPort;

DEFINE_string(state_store_host, "localhost",
              "hostname where StateStoreService is running");
DEFINE_int32(state_store_port, 24000, "port where StateStoreService is running");
DECLARE_string(host);
DEFINE_int32(state_store_subscriber_port, 23000,
             "port where StateStoreSubscriberService should be exported");

namespace sparrow {

SubscriptionManager::UpdateCallback::~UpdateCallback() {
  DCHECK(!currently_registered_);
}

SubscriptionManager::SubscriptionManager()
    : state_store_(new StateStoreSubscriber(FLAGS_host,
                       FLAGS_state_store_subscriber_port, FLAGS_state_store_host,
                       FLAGS_state_store_port)) {
}

SubscriptionManager::SubscriptionManager(const string& state_store_subscriber_host,
    int state_store_subscriber_port, const string& state_store_host, int state_store_port)
    : state_store_(new StateStoreSubscriber(state_store_subscriber_host,
                       state_store_subscriber_port, state_store_host, state_store_port)) {
}

Status SubscriptionManager::RegisterService(const string& service_id,
                                            const THostPort& address) {
  return state_store_->RegisterService(service_id, address);
}

Status SubscriptionManager::UnregisterService(const string& service_id) {
  return state_store_->UnregisterService(service_id);
}

Status SubscriptionManager::RegisterSubscription(
    const unordered_set<std::string>& services, UpdateCallback* update,
    SubscriptionId* id) {
  return state_store_->RegisterSubscription(services, update, id);
}

Status SubscriptionManager::UnregisterSubscription(SubscriptionId id) {
  return state_store_->UnregisterSubscription(id);
}

Status SubscriptionManager::Start() {
  return state_store_->Start();
}

Status SubscriptionManager::UnregisterAll() {
  state_store_->UnregisterAll();
  return Status::OK;
}

}
