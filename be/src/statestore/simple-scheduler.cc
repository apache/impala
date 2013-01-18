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

#include <vector>

#include <boost/algorithm/string.hpp>
#include <boost/algorithm/string/join.hpp>
#include <boost/bind.hpp>
#include <boost/mem_fn.hpp>
#include <boost/foreach.hpp>

#include "util/metrics.h"
#include "runtime/coordinator.h"
#include "runtime/exec-env.h"

#include "statestore/simple-scheduler.h"
#include "statestore/state-store-subscriber.h"
#include "gen-cpp/Types_types.h"

using namespace std;
using namespace boost;

namespace impala {

static const string LOCAL_ASSIGNMENTS_KEY("simple-scheduler.local-assignments.total");
static const string ASSIGNMENTS_KEY("simple-scheduler.assignments.total");
static const string SCHEDULER_INIT_KEY("simple-scheduler.initialized");

static const string SUBSCRIPTION_ID("simple.scheduler");

SimpleScheduler::SimpleScheduler(SubscriptionManager* subscription_manager,
    const ServiceId& backend_service_id, Metrics* metrics)
  : metrics_(metrics),
    subscription_manager_(subscription_manager),
    callback_(bind<void>(mem_fn(&SimpleScheduler::UpdateMembership), this, _1)),
    subscription_id_(INVALID_SUBSCRIPTION_ID),
    backend_service_id_(backend_service_id),
    total_assignments_(NULL),
    total_local_assignments_(NULL),
    initialised_(NULL) {
  next_nonlocal_host_entry_ = host_map_.begin();
}

SimpleScheduler::SimpleScheduler(const vector<THostPort>& backends, Metrics* metrics)
  : metrics_(metrics),
    subscription_manager_(NULL),
    callback_(NULL),
    subscription_id_(INVALID_SUBSCRIPTION_ID),
    total_assignments_(NULL),
    total_local_assignments_(NULL),
    initialised_(NULL) {
  DCHECK(backends.size() > 0);
  for (int i = 0; i < backends.size(); ++i) {
    string ipaddress = backends[i].ipaddress;
    int port = backends[i].port;

    HostMap::iterator it = host_map_.find(ipaddress);
    if (it == host_map_.end()) {
      it = host_map_.insert(make_pair(ipaddress, list<int>())).first;
    }
    it->second.push_back(port);
  }
  next_nonlocal_host_entry_ = host_map_.begin();
}

SimpleScheduler::~SimpleScheduler() {
  DCHECK_EQ(subscription_id_, INVALID_SUBSCRIPTION_ID) << "Did not call Close()";
}

impala::Status SimpleScheduler::Init() {
  LOG(INFO) << "Starting simple scheduler";
  if (subscription_manager_ != NULL) {
    unordered_set<string> services;
    services.insert(backend_service_id_);
    RETURN_IF_ERROR(subscription_manager_->RegisterSubscription(
        services, SUBSCRIPTION_ID, &callback_));
  }
  if (metrics_ != NULL) {
    total_assignments_ =
        metrics_->CreateAndRegisterPrimitiveMetric(ASSIGNMENTS_KEY, 0L);
    total_local_assignments_ =
        metrics_->CreateAndRegisterPrimitiveMetric(LOCAL_ASSIGNMENTS_KEY, 0L);
    initialised_ =
        metrics_->CreateAndRegisterPrimitiveMetric(SCHEDULER_INIT_KEY, true);
  }
  return Status::OK;
}

void SimpleScheduler::UpdateMembership(const ServiceStateMap& service_state) {
  lock_guard<mutex> lock(host_map_lock_);
  VLOG(4) << "Received update from subscription manager" << endl;
  host_map_.clear();
  ServiceStateMap::const_iterator it = service_state.find(backend_service_id_);
  if (it != service_state.end()) {
    VLOG(4) << "Found membership information for " << backend_service_id_;
    ServiceState service_state = it->second;
    BOOST_FOREACH(const Membership::value_type& member, service_state.membership) {
      VLOG(4) << "Got member: " << member.second.ipaddress << ":" << member.second.port;
      HostMap::iterator host_it = host_map_.find(member.second.ipaddress);
      if (host_it == host_map_.end()) {
        host_it = host_map_.insert(make_pair(member.second.ipaddress, list<int>())).first;
      }
      host_it->second.push_back(member.second.port);
    }
  } else {
    VLOG(4) << "No membership information found.";
  }

  next_nonlocal_host_entry_ = host_map_.begin();
}

Status SimpleScheduler::GetHosts(
    const vector<THostPort>& data_locations, HostList* hostports) {
  lock_guard<mutex> lock(host_map_lock_);
  if (host_map_.size() == 0) {
    return Status("No backends configured");
  }
  hostports->clear();
  int num_local_assignments = 0;
  for (int i = 0; i < data_locations.size(); ++i) {
    HostMap::iterator entry = host_map_.find(data_locations[i].ipaddress);
    if (entry == host_map_.end()) {
      // round robin the ipaddress
      entry = next_nonlocal_host_entry_;
      ++next_nonlocal_host_entry_;
      if (next_nonlocal_host_entry_ == host_map_.end()) {
        next_nonlocal_host_entry_ = host_map_.begin();
      }
    } else {
      ++num_local_assignments;
    }
    DCHECK(!entry->second.empty());
    // Round-robin between impalads on the same ipaddress.
    // Pick the first one, then move it to the back of the queue
    int port = entry->second.front();
    THostPort hostport;
    hostport.ipaddress = entry->first;
    hostport.port = port;
    hostports->push_back(hostport);
    entry->second.pop_front();
    entry->second.push_back(port);
  }

  if (metrics_ != NULL) {
    total_assignments_->Increment(data_locations.size());
    total_local_assignments_->Increment(num_local_assignments);
  }

  if (VLOG_QUERY_IS_ON) {
    vector<string> hostport_strings;
    for (int i = 0; i < hostports->size(); ++i) {
      stringstream s;
      s << "(" << data_locations[i].ipaddress << ":" << data_locations[i].port
        << " -> " << (*hostports)[i].ipaddress << ":" << (*hostports)[i].port << ")";
      hostport_strings.push_back(s.str());
    }
    VLOG_QUERY << "SimpleScheduler assignment (data->backend):  "
               << algorithm::join(hostport_strings, ", ");
    if (data_locations.size() > 0) {
      VLOG_QUERY << "SimpleScheduler locality percentage " << setprecision(4)
                 << 100.0f * (num_local_assignments / (float)data_locations.size())
                 << "% (" << num_local_assignments << " out of " << data_locations.size()
                 << ")";
    }
  }
  DCHECK_EQ(data_locations.size(), hostports->size());
  return Status::OK;
}

void SimpleScheduler::GetAllKnownHosts(HostList* hostports) {
  lock_guard<mutex> lock(host_map_lock_);
  hostports->clear();
  BOOST_FOREACH(const HostMap::value_type& ipaddress, host_map_) {
    BOOST_FOREACH(int port, ipaddress.second) {
      THostPort hostport;
      hostport.ipaddress = ipaddress.first;
      hostport.port = port;
      hostports->push_back(hostport);
    }
  }
}

void SimpleScheduler::Close() {
  if (subscription_manager_ != NULL && subscription_id_ != INVALID_SUBSCRIPTION_ID) {
    VLOG_QUERY << "Unregistering simple scheduler with subscription manager";
    Status status = subscription_manager_->UnregisterSubscription(subscription_id_);
    if (!status.ok()) {
      LOG(ERROR) << "Error unsubscribing from subscription manager: "
                 << status.GetErrorMsg();
    }
  }
  // Reset everything to make sure no one can use this anymore
  subscription_id_ = INVALID_SUBSCRIPTION_ID;
  subscription_manager_ = NULL;
  host_map_.clear();
}

}
