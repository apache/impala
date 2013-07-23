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

#include "util/network-util.h"

using namespace std;
using namespace boost;

namespace impala {

static const string LOCAL_ASSIGNMENTS_KEY("simple-scheduler.local-assignments.total");
static const string ASSIGNMENTS_KEY("simple-scheduler.assignments.total");
static const string SCHEDULER_INIT_KEY("simple-scheduler.initialized");

const string SimpleScheduler::IMPALA_MEMBERSHIP_TOPIC("impala-membership");

SimpleScheduler::SimpleScheduler(StateStoreSubscriber* subscriber,
    const string& backend_id, const TNetworkAddress& backend_address,
    Metrics* metrics, Webserver* webserver)
  : metrics_(metrics),
    webserver_(webserver),
    statestore_subscriber_(subscriber),
    backend_id_(backend_id),
    thrift_serializer_(false),
    total_assignments_(NULL),
    total_local_assignments_(NULL),
    initialised_(NULL),
    update_count_(0) {
  backend_descriptor_.address = backend_address;
  next_nonlocal_backend_entry_ = backend_map_.begin();
}

SimpleScheduler::SimpleScheduler(const vector<TNetworkAddress>& backends,
                                 Metrics* metrics, Webserver* webserver)
  : metrics_(metrics),
    webserver_(webserver),
    statestore_subscriber_(NULL),
    thrift_serializer_(false),
    total_assignments_(NULL),
    total_local_assignments_(NULL),
    initialised_(NULL),
    update_count_(0) {
  DCHECK(backends.size() > 0);

  for (int i = 0; i < backends.size(); ++i) {
    vector<string> ipaddrs;
    Status status = HostnameToIpAddrs(backends[i].hostname, &ipaddrs);
    if (!status.ok()) {
      VLOG(1) << "Failed to resolve " << backends[i].hostname << ": "
              << status.GetErrorMsg();
      continue;
    }

    // Try to find a non-localhost address, otherwise just use the
    // first IP address returned.
    string ipaddr = ipaddrs[0];
    if (!FindFirstNonLocalhost(ipaddrs, &ipaddr)) {
      VLOG(1) << "Only localhost addresses found for " << backends[i].hostname;
    }

    BackendMap::iterator it = backend_map_.find(ipaddr);
    if (it == backend_map_.end()) {
      it = backend_map_.insert(
          make_pair(ipaddr, list<TBackendDescriptor>())).first;
      backend_ip_map_[backends[i].hostname] = ipaddr;
    }

    TBackendDescriptor descriptor;
    descriptor.address = MakeNetworkAddress(ipaddr, backends[i].port);
    it->second.push_back(descriptor);
  }
  next_nonlocal_backend_entry_ = backend_map_.begin();
}

Status SimpleScheduler::Init() {
  LOG(INFO) << "Starting simple scheduler";

  if (webserver_ != NULL) {
    Webserver::PathHandlerCallback backends_callback =
        bind<void>(mem_fn(&SimpleScheduler::BackendsPathHandler), this, _1, _2);
    webserver_->RegisterPathHandler("/backends", backends_callback);
  }

  if (statestore_subscriber_ != NULL) {
    StateStoreSubscriber::UpdateCallback cb =
        bind<void>(mem_fn(&SimpleScheduler::UpdateMembership), this, _1, _2);
    Status status =
        statestore_subscriber_->AddTopic(IMPALA_MEMBERSHIP_TOPIC, true, cb);
    if (!status.ok()) {
      status.AddErrorMsg("SimpleScheduler failed to start");
      return status;
    }
  }
  if (metrics_ != NULL) {
    total_assignments_ =
        metrics_->CreateAndRegisterPrimitiveMetric(ASSIGNMENTS_KEY, 0L);
    total_local_assignments_ =
        metrics_->CreateAndRegisterPrimitiveMetric(LOCAL_ASSIGNMENTS_KEY, 0L);
    initialised_ =
        metrics_->CreateAndRegisterPrimitiveMetric(SCHEDULER_INIT_KEY, true);
  }

  if (statestore_subscriber_ != NULL) {
    // Figure out what our IP address is, so that each subscriber
    // doesn't have to resolve it on every heartbeat.
    vector<string> ipaddrs;
    const string& hostname = backend_descriptor_.address.hostname;
    Status status = HostnameToIpAddrs(hostname, &ipaddrs);
    if (!status.ok()) {
      VLOG(1) << "Failed to resolve " << hostname << ": " << status.GetErrorMsg();
      status.AddErrorMsg("SimpleScheduler failed to start");
      return status;
    }
    // Find a non-localhost address for this host; if one can't be
    // found use the first address returned by HostnameToIpAddrs
    string ipaddr = ipaddrs[0];
    if (!FindFirstNonLocalhost(ipaddrs, &ipaddr)) {
      VLOG(3) << "Only localhost addresses found for " << hostname;
    }

    backend_descriptor_.ip_address = ipaddr;
    LOG(INFO) << "Simple-scheduler using " << ipaddr << " as IP address";

    if (webserver_ != NULL) {
      const TNetworkAddress& webserver_address = webserver_->http_address();
      if (IsWildcardAddress(webserver_address.hostname)) {
        backend_descriptor_.__set_debug_http_address(
            MakeNetworkAddress(ipaddr, webserver_address.port));
      } else {
        backend_descriptor_.__set_debug_http_address(webserver_address);
      }
      backend_descriptor_.__set_secure_webserver(webserver_->IsSecure());
    }
  }
  return Status::OK;
}

// Utility method to help sort backends by ascending network address
bool TBackendDescriptorComparator(const TBackendDescriptor& a,
    const TBackendDescriptor& b) {
  return TNetworkAddressComparator(a.address, b.address);
}

void SimpleScheduler::BackendsPathHandler(const Webserver::ArgumentMap& args,
                                          stringstream* output) {
  BackendList backends;
  GetAllKnownBackends(&backends);
  sort(backends.begin(), backends.end(), TBackendDescriptorComparator);

  if (args.find("raw") == args.end()) {
    (*output) << "<h2>Known Backends "
              << "(" << backends.size() << ")"
              << "</h2>" << endl;

    (*output) << "<table class='table table-hover table-bordered'>";
    (*output) << "<tr><th>Backend</th><th></th>" << endl;
    BOOST_FOREACH(const BackendList::value_type& backend, backends) {
      (*output) << "<tr><td>" << backend.address << "</td>"
                << "<td>";
      if (backend.__isset.debug_http_address) {
        if (backend.__isset.secure_webserver && backend.secure_webserver) {
          (*output) << "<a href='https://";
        } else {
          (*output) << "<a href='http://";
        }
        (*output) << backend.debug_http_address << "'>Debug pages</a>";
      } else {
        (*output) << "N/A";
      }
      (*output) << "</td></tr>" << endl;
    }
    (*output) << "</table>";
  } else {
    (*output) << "Known Backends " << "(" << backends.size() << ")" << endl;

    BOOST_FOREACH(const BackendList::value_type& backend, backends) {
      (*output) << backend.address << endl;
    }
  }
}

void SimpleScheduler::UpdateMembership(
    const StateStoreSubscriber::TopicDeltaMap& service_state,
    vector<TTopicUpdate>* topic_updates) {
  ++update_count_;
  // TODO: Work on a copy if possible, or at least do resolution as a separate step
  // First look to see if the topic(s) we're interested in have an update
  StateStoreSubscriber::TopicDeltaMap::const_iterator topic =
      service_state.find(IMPALA_MEMBERSHIP_TOPIC);

  // Copy to work on without holding the map lock
  BackendMap backend_map_copy;
  BackendIpAddressMap backend_ip_map_copy;
  bool found_self = false;

  if (topic != service_state.end()) {
    const TTopicDelta& delta = topic->second;
    if (delta.is_delta) {
      // TODO: Handle deltas when the state-store starts sending them
      LOG(WARNING) << "Unexpected delta update from state-store, ignoring as scheduler"
                      " cannot handle deltas";
      return;
    }

    BOOST_FOREACH(const TTopicItem& item, delta.topic_entries) {
      TBackendDescriptor backend_descriptor;
      // Benchmarks have suggested that this method can deserialize
      // ~10m messages per second, so no immediate need to consider optimisation.
      uint32_t len = item.value.size();
      Status status = DeserializeThriftMsg(reinterpret_cast<const uint8_t*>(
          item.value.data()), &len, false, &backend_descriptor);
      if (!status.ok()) {
        VLOG(2) << "Error deserializing topic item with key: " << item.key;
        continue;
      }

      if (item.key == backend_id_) {
        if (backend_descriptor.address == backend_descriptor_.address) {
          found_self = true;
        } else {
          // Someone else has registered this subscriber ID with a
          // different address. We will try to re-register
          // (i.e. overwrite their subscription), but there is likely
          // a configuration problem.
          LOG_EVERY_N(WARNING, 30) << "Duplicate subscriber registration from address: "
                                   << backend_descriptor.address;
        }
      }

      backend_map_copy[backend_descriptor.ip_address].push_back(
          backend_descriptor);
      backend_ip_map_copy[backend_descriptor.address.hostname] =
          backend_descriptor.ip_address;
    }
  }

  // If this impalad is not in our view of the membership list, we
  // should add it and tell the state-store.
  if (!found_self) {
    VLOG(2) << "Registering local backend with state-store";
    topic_updates->push_back(TTopicUpdate());
    TTopicUpdate& update = topic_updates->back();
    update.topic_name = IMPALA_MEMBERSHIP_TOPIC;
    update.topic_updates.push_back(TTopicItem());

    TTopicItem& item = update.topic_updates.back();
    item.key = backend_id_;
    Status status = thrift_serializer_.Serialize(&backend_descriptor_, &item.value);
    if (!status.ok()) {
      LOG(INFO) << "Failed to serialize Impala backend address for state-store topic: "
                << status.GetErrorMsg();
      topic_updates->pop_back();
    }
  }

  {
    lock_guard<mutex> lock(backend_map_lock_);
    backend_map_.swap(backend_map_copy);
    backend_ip_map_.swap(backend_ip_map_copy);
    next_nonlocal_backend_entry_ = backend_map_.begin();
  }
}

Status SimpleScheduler::GetBackends(
    const vector<TNetworkAddress>& data_locations, BackendList* backendports) {
  backendports->clear();
  for (int i = 0; i < data_locations.size(); ++i) {
    TBackendDescriptor backend;
    GetBackend(data_locations[i], &backend);
    backendports->push_back(backend);
  }
  DCHECK_EQ(data_locations.size(), backendports->size());
  return Status::OK;
}

Status SimpleScheduler::GetBackend(const TNetworkAddress& data_location,
    TBackendDescriptor* backend) {
  lock_guard<mutex> lock(backend_map_lock_);
  if (backend_map_.size() == 0) {
    return Status("No backends configured");
  }
  bool local_assignment = false;
  BackendMap::iterator entry = backend_map_.find(data_location.hostname);

  if (entry == backend_map_.end()) {
    // backend_map_ maps ip address to backend but
    // data_location.hostname might be a hostname.
    // Find the ip address of the data_location from backend_ip_map_.
    BackendIpAddressMap::const_iterator itr =
        backend_ip_map_.find(data_location.hostname);
    if (itr != backend_ip_map_.end()) {
      entry = backend_map_.find(itr->second);
    }
  }

  if (entry == backend_map_.end()) {
    // round robin the ipaddress
    entry = next_nonlocal_backend_entry_;
    ++next_nonlocal_backend_entry_;
    if (next_nonlocal_backend_entry_ == backend_map_.end()) {
      next_nonlocal_backend_entry_ = backend_map_.begin();
    }
  } else {
    local_assignment = true;
  }
  DCHECK(!entry->second.empty());
  // Round-robin between impalads on the same ipaddress.
  // Pick the first one, then move it to the back of the queue
  *backend = entry->second.front();
  entry->second.pop_front();
  entry->second.push_back(*backend);

  if (metrics_ != NULL) {
    total_assignments_->Increment(1);
    if (local_assignment) {
      total_local_assignments_->Increment(1L);
    }
  }

  if (VLOG_FILE_IS_ON) {
    stringstream s;
    s << "(" << data_location;
    s << " -> " << backend->address << ")";
    VLOG_FILE << "SimpleScheduler assignment (data->backend):  " << s.str();
  }
  return Status::OK;
}

void SimpleScheduler::GetAllKnownBackends(BackendList* backends) {
  lock_guard<mutex> lock(backend_map_lock_);
  backends->clear();
  BOOST_FOREACH(const BackendMap::value_type& backend_list, backend_map_) {
    backends->insert(backends->end(), backend_list.second.begin(),
                     backend_list.second.end());
  }
}

}
