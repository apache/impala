// Copyright (c) 2012 Cloudera, Inc. All rights reserved.

#include "sparrow/subscription-manager.h"

#include <string>

#include <gflags/gflags.h>

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
DEFINE_string(state_store_subscriber_host, "localhost",
              "hostname where StateStoreSubscriberService is running");
DEFINE_int32(state_store_subscriber_port, 23000,
             "port where StateStoreSubscriberService is running");

namespace sparrow {

SubscriptionManager::SubscriptionManager()
    : state_store_(new StateStoreSubscriber(FLAGS_state_store_subscriber_host,
                                            FLAGS_state_store_subscriber_port,
                                            FLAGS_state_store_host,
                                            FLAGS_state_store_port)) {
}

Status SubscriptionManager::RegisterService(const string& service_id,
                                            const THostPort& address) {
  return state_store_->RegisterService(service_id, address);
}

Status SubscriptionManager::UnregisterService(const string& service_id) {
  return state_store_->UnregisterService(service_id);
}

Status SubscriptionManager::RegisterSubscription(
    const UpdateCallback& update, const unordered_set<std::string>& services,
    SubscriptionId* id) {
  return state_store_->RegisterSubscription(update, services, id);
}

Status SubscriptionManager::UnregisterSubscription(SubscriptionId id) {
  return state_store_->UnregisterSubscription(id);
}

Status SubscriptionManager::Start() {
  state_store_->Start();
  return Status::OK;
}

Status SubscriptionManager::Stop() {
  state_store_->Stop();
  return Status::OK;
}

}
