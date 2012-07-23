// Copyright (c) 2012 Cloudera, Inc. All rights reserved.

#include <vector>

#include <glog/logging.h>

#include <boost/algorithm/string.hpp>
#include <boost/bind.hpp>
#include <boost/mem_fn.hpp>
#include <boost/foreach.hpp>

#include "runtime/coordinator.h"
#include "runtime/exec-env.h"

#include "sparrow/simple-scheduler.h"
#include "sparrow/state-store-subscriber-service.h"
#include "gen-cpp/Types_types.h"

using namespace std;
using namespace boost;
using impala::Status;
using impala::THostPort;
using impala::ExecEnv;

namespace sparrow {

SimpleScheduler::SimpleScheduler(SubscriptionManager* subscription_manager,
    const ServiceId& backend_service_id)
  : subscription_manager_(subscription_manager),
    subscription_id_(INVALID_SUBSCRIPTION_ID),
    backend_service_id_(backend_service_id) {
  next_nonlocal_host_entry_ = host_map_.begin();
}

impala::Status SimpleScheduler::Init() {
  if (subscription_manager_ == NULL) return Status::OK;

  unordered_set<string> services;
  services.insert(backend_service_id_);
  SubscriptionManager::UpdateCallback callback =
      boost::bind<void>(boost::mem_fn(&SimpleScheduler::UpdateMembership), this, _1);
  RETURN_IF_ERROR(
      subscription_manager_->RegisterSubscription(callback, services, &subscription_id_));

  return Status::OK;
}

void SimpleScheduler::UpdateMembership(const ServiceStateMap& service_state) {
  lock_guard<mutex> lock(host_map_lock_);
  VLOG_QUERY << "Received update from subscription manager" << endl;
  host_map_.clear();
  ServiceStateMap::const_iterator it = service_state.find(backend_service_id_);
  if (it != service_state.end()) {
    VLOG_QUERY << "Found membership information for " << backend_service_id_;
    ServiceState service_state = it->second;
    BOOST_FOREACH(const Membership::value_type& member, service_state.membership) {
      VLOG_QUERY << "Got member: " << member.second.host << ":" << member.second.port;
      HostMap::iterator host_it = host_map_.find(member.second.host);
      if (host_it == host_map_.end()) {
        host_it = host_map_.insert(make_pair(member.second.host, list<int>())).first;
      }
      host_it->second.push_back(member.second.port);
    }
  } else {
    VLOG_QUERY << "No membership information found.";
  }

  next_nonlocal_host_entry_ = host_map_.begin();
}

SimpleScheduler::SimpleScheduler(const vector<THostPort>& backends)
    : subscription_manager_(NULL) {
  DCHECK(backends.size() > 0);
  for (int i = 0; i < backends.size(); ++i) {
    string host = backends[i].host;
    int port = backends[i].port;

    HostMap::iterator i = host_map_.find(host);
    if (i == host_map_.end()) {
      i = host_map_.insert(make_pair(host, list<int>())).first;
    }
    i->second.push_back(port);
  }
  next_nonlocal_host_entry_ = host_map_.begin();
}

Status SimpleScheduler::GetHosts(
    const vector<THostPort>& data_locations, vector<pair<string, int> >* hostports) {
  lock_guard<mutex> lock(host_map_lock_);
  if (host_map_.size() == 0) {
    return Status("No backends configured");
  }
  hostports->clear();
  for (int i = 0; i < data_locations.size(); ++i) {
    HostMap::iterator entry = host_map_.find(data_locations[i].host);
    if (entry == host_map_.end()) {
      // round robin the host
      entry = next_nonlocal_host_entry_;
      ++next_nonlocal_host_entry_;
      if (next_nonlocal_host_entry_ == host_map_.end()) {
        next_nonlocal_host_entry_ = host_map_.begin();
      }
    }
    DCHECK(!entry->second.empty());
    // Round-robin between impalads on the same host. Pick the first one, then move it
    // to the back of the queue
    int port = entry->second.front();
    hostports->push_back(make_pair(entry->first, port));
    entry->second.pop_front();
    entry->second.push_back(port);
    VLOG_QUERY << "SimpleScheduler: selecting "
            << entry->first << ":" << entry->second.front();
  }
  DCHECK_EQ(data_locations.size(), hostports->size());
  return Status::OK;
}

void SimpleScheduler::GetAllKnownHosts(vector<pair<string, int> >* hostports) {
  lock_guard<mutex> lock(host_map_lock_);
  hostports->clear();
  BOOST_FOREACH(HostMap::value_type host, host_map_) {
    BOOST_FOREACH(int port, host.second) {
      hostports->push_back(make_pair(host.first, port));
    }
  }
}

SimpleScheduler::~SimpleScheduler() {
  if (subscription_manager_ != NULL && subscription_id_ != INVALID_SUBSCRIPTION_ID) {
    VLOG_QUERY << "Unregistering simple scheduler with subscription manager";
    Status status = subscription_manager_->UnregisterSubscription(subscription_id_);
    if (!status.ok()) {
      LOG(ERROR) << "Error unsubscribing from subscription manager: "
                 << status.GetErrorMsg();
    }
  }
}

}
