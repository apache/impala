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

#include "scheduling/simple-scheduler.h"

#include <vector>

#include <boost/algorithm/string.hpp>
#include <boost/algorithm/string/join.hpp>
#include <boost/bind.hpp>
#include <boost/mem_fn.hpp>
#include <boost/foreach.hpp>
#include <gutil/strings/substitute.h>

#include "common/logging.h"
#include "util/metrics.h"
#include "runtime/exec-env.h"
#include "runtime/coordinator.h"
#include "service/impala-server.h"

#include "statestore/statestore-subscriber.h"
#include "gen-cpp/Types_types.h"
#include "gen-cpp/ImpalaInternalService_constants.h"

#include "util/network-util.h"
#include "util/uid-util.h"
#include "util/container-util.h"
#include "util/debug-util.h"
#include "util/error-util.h"
#include "util/llama-util.h"
#include "util/mem-info.h"
#include "util/parse-util.h"
#include "gen-cpp/ResourceBrokerService_types.h"

#include "common/names.h"

using boost::algorithm::join;
using namespace apache::thrift;
using namespace rapidjson;
using namespace strings;

DECLARE_int32(be_port);
DECLARE_string(hostname);
DECLARE_bool(enable_rm);
DECLARE_int32(rm_default_cpu_vcores);
DECLARE_string(rm_default_memory);

DEFINE_bool(disable_admission_control, true, "Disables admission control.");

DEFINE_bool(require_username, false, "Requires that a user be provided in order to "
    "schedule requests. If enabled and a user is not provided, requests will be "
    "rejected, otherwise requests without a username will be submitted with the "
    "username 'default'.");

namespace impala {

static const string LOCAL_ASSIGNMENTS_KEY("simple-scheduler.local-assignments.total");
static const string ASSIGNMENTS_KEY("simple-scheduler.assignments.total");
static const string SCHEDULER_INIT_KEY("simple-scheduler.initialized");
static const string NUM_BACKENDS_KEY("simple-scheduler.num-backends");
static const string DEFAULT_USER("default");

static const string BACKENDS_WEB_PAGE = "/backends";
static const string BACKENDS_TEMPLATE = "backends.tmpl";

const string SimpleScheduler::IMPALA_MEMBERSHIP_TOPIC("impala-membership");

static const string ERROR_USER_TO_POOL_MAPPING_NOT_FOUND(
    "No mapping found for request from user '$0' with requested pool '$1'");
static const string ERROR_USER_NOT_ALLOWED_IN_POOL("Request from user '$0' with "
    "requested pool '$1' denied access to assigned pool '$2'");
static const string ERROR_USER_NOT_SPECIFIED("User must be specified because "
    "-require_username=true.");

SimpleScheduler::SimpleScheduler(StatestoreSubscriber* subscriber,
    const string& backend_id, const TNetworkAddress& backend_address,
    MetricGroup* metrics, Webserver* webserver, ResourceBroker* resource_broker,
    RequestPoolService* request_pool_service)
  : metrics_(metrics->GetChildGroup("scheduler")),
    webserver_(webserver),
    statestore_subscriber_(subscriber),
    backend_id_(backend_id),
    thrift_serializer_(false),
    total_assignments_(NULL),
    total_local_assignments_(NULL),
    initialised_(NULL),
    update_count_(0),
    resource_broker_(resource_broker),
    request_pool_service_(request_pool_service) {
  backend_descriptor_.address = backend_address;
  next_nonlocal_backend_entry_ = backend_map_.begin();
  if (FLAGS_disable_admission_control) LOG(INFO) << "Admission control is disabled.";
  if (!FLAGS_disable_admission_control) {
    admission_controller_.reset(
        new AdmissionController(request_pool_service_, metrics, backend_id_));
  }

  if (FLAGS_enable_rm) {
    if (FLAGS_rm_default_cpu_vcores <= 0) {
      LOG(ERROR) << "Bad value for --rm_default_cpu_vcores (must be postive): "
                 << FLAGS_rm_default_cpu_vcores;
      exit(1);
    }
    bool is_percent;
    int64_t mem_bytes =
        ParseUtil::ParseMemSpec(
            FLAGS_rm_default_memory, &is_percent, MemInfo::physical_mem());
    if (mem_bytes <= 1024 * 1024) {
      LOG(ERROR) << "Bad value for --rm_default_memory (must be larger than 1M):"
                 << FLAGS_rm_default_memory;
      exit(1);
    } else if (is_percent) {
      LOG(ERROR) << "Must use absolute value for --rm_default_memory: "
                 << FLAGS_rm_default_memory;
      exit(1);
    }
  }
}

SimpleScheduler::SimpleScheduler(const vector<TNetworkAddress>& backends,
    MetricGroup* metrics, Webserver* webserver, ResourceBroker* resource_broker,
    RequestPoolService* request_pool_service)
  : metrics_(metrics),
    webserver_(webserver),
    statestore_subscriber_(NULL),
    thrift_serializer_(false),
    total_assignments_(NULL),
    total_local_assignments_(NULL),
    initialised_(NULL),
    update_count_(0),
    resource_broker_(resource_broker),
    request_pool_service_(request_pool_service) {
  DCHECK(backends.size() > 0);
  if (FLAGS_disable_admission_control) LOG(INFO) << "Admission control is disabled.";
  // request_pool_service_ may be null in unit tests
  if (request_pool_service_ != NULL && !FLAGS_disable_admission_control) {
    admission_controller_.reset(
        new AdmissionController(request_pool_service_, metrics, backend_id_));
  }

  for (int i = 0; i < backends.size(); ++i) {
    vector<string> ipaddrs;
    Status status = HostnameToIpAddrs(backends[i].hostname, &ipaddrs);
    if (!status.ok()) {
      VLOG(1) << "Failed to resolve " << backends[i].hostname << ": "
              << status.GetDetail();
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
    Webserver::UrlCallback backends_callback =
        bind<void>(mem_fn(&SimpleScheduler::BackendsUrlCallback), this, _1, _2);
    webserver_->RegisterUrlCallback(BACKENDS_WEB_PAGE, BACKENDS_TEMPLATE,
        backends_callback);
  }

  if (statestore_subscriber_ != NULL) {
    StatestoreSubscriber::UpdateCallback cb =
        bind<void>(mem_fn(&SimpleScheduler::UpdateMembership), this, _1, _2);
    Status status = statestore_subscriber_->AddTopic(IMPALA_MEMBERSHIP_TOPIC, true, cb);
    if (!status.ok()) {
      status.AddDetail("SimpleScheduler failed to register membership topic");
      return status;
    }
    if (!FLAGS_disable_admission_control) {
      RETURN_IF_ERROR(admission_controller_->Init(statestore_subscriber_));
    }
  }
  if (metrics_ != NULL) {
    total_assignments_ = metrics_->AddCounter<int64_t>(ASSIGNMENTS_KEY, 0);
    total_local_assignments_ = metrics_->AddCounter<int64_t>(LOCAL_ASSIGNMENTS_KEY, 0);
    initialised_ = metrics_->AddProperty(SCHEDULER_INIT_KEY, true);
    num_fragment_instances_metric_ = metrics_->AddGauge<int64_t>(
        NUM_BACKENDS_KEY, backend_map_.size());
  }

  if (statestore_subscriber_ != NULL) {
    // Figure out what our IP address is, so that each subscriber
    // doesn't have to resolve it on every heartbeat.
    vector<string> ipaddrs;
    const string& hostname = backend_descriptor_.address.hostname;
    Status status = HostnameToIpAddrs(hostname, &ipaddrs);
    if (!status.ok()) {
      VLOG(1) << "Failed to resolve " << hostname << ": " << status.GetDetail();
      status.AddDetail("SimpleScheduler failed to start");
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
  return Status::OK();
}

// Utility method to help sort backends by ascending network address
bool TBackendDescriptorComparator(const TBackendDescriptor& a,
    const TBackendDescriptor& b) {
  return TNetworkAddressComparator(a.address, b.address);
}

void SimpleScheduler::BackendsUrlCallback(const Webserver::ArgumentMap& args,
    Document* document) {
  BackendList backends;
  GetAllKnownBackends(&backends);
  Value backends_list(kArrayType);
  BOOST_FOREACH(const BackendList::value_type& backend, backends) {
    Value str(TNetworkAddressToString(backend.address).c_str(), document->GetAllocator());
    backends_list.PushBack(str, document->GetAllocator());
  }

  document->AddMember("backends", backends_list, document->GetAllocator());
}

void SimpleScheduler::UpdateMembership(
    const StatestoreSubscriber::TopicDeltaMap& incoming_topic_deltas,
    vector<TTopicDelta>* subscriber_topic_updates) {
  ++update_count_;
  // TODO: Work on a copy if possible, or at least do resolution as a separate step
  // First look to see if the topic(s) we're interested in have an update
  StatestoreSubscriber::TopicDeltaMap::const_iterator topic =
      incoming_topic_deltas.find(IMPALA_MEMBERSHIP_TOPIC);

  if (topic != incoming_topic_deltas.end()) {
    const TTopicDelta& delta = topic->second;

    // This function needs to handle both delta and non-delta updates. For delta
    // updates, it is desireable to minimize the number of copies to only
    // the added/removed items. To accomplish this, all updates are processed
    // under a lock and applied to the shared backend maps (backend_map_ and
    // backend_ip_map_) in place.
    {
      lock_guard<mutex> lock(backend_map_lock_);
      if (!delta.is_delta) {
        current_membership_.clear();
        backend_map_.clear();
        backend_ip_map_.clear();
      }

      // Process new entries to the topic
      BOOST_FOREACH(const TTopicItem& item, delta.topic_entries) {
        TBackendDescriptor be_desc;
        // Benchmarks have suggested that this method can deserialize
        // ~10m messages per second, so no immediate need to consider optimisation.
        uint32_t len = item.value.size();
        Status status = DeserializeThriftMsg(reinterpret_cast<const uint8_t*>(
            item.value.data()), &len, false, &be_desc);
        if (!status.ok()) {
          VLOG(2) << "Error deserializing membership topic item with key: " << item.key;
          continue;
        }
        if (item.key == backend_id_ && be_desc.address != backend_descriptor_.address) {
          // Someone else has registered this subscriber ID with a
          // different address. We will try to re-register
          // (i.e. overwrite their subscription), but there is likely
          // a configuration problem.
          LOG_EVERY_N(WARNING, 30) << "Duplicate subscriber registration from address: "
                                   << be_desc.address;
        }

        list<TBackendDescriptor>* be_descs = &backend_map_[be_desc.ip_address];
        if (find(be_descs->begin(), be_descs->end(), be_desc) == be_descs->end()) {
          backend_map_[be_desc.ip_address].push_back(be_desc);
        }
        backend_ip_map_[be_desc.address.hostname] = be_desc.ip_address;
        current_membership_.insert(make_pair(item.key, be_desc));
      }
      // Process deletions from the topic
      BOOST_FOREACH(const string& backend_id, delta.topic_deletions) {
        if (current_membership_.find(backend_id) != current_membership_.end()) {
          const TBackendDescriptor& be_desc = current_membership_[backend_id];
          backend_ip_map_.erase(be_desc.address.hostname);
          list<TBackendDescriptor>* be_descs = &backend_map_[be_desc.ip_address];
          be_descs->erase(
              remove(be_descs->begin(), be_descs->end(), be_desc), be_descs->end());
          if (be_descs->empty()) backend_map_.erase(be_desc.ip_address);
          current_membership_.erase(backend_id);
        }
      }
      next_nonlocal_backend_entry_ = backend_map_.begin();
    }

    // If this impalad is not in our view of the membership list, we should add it and
    // tell the statestore.
    bool is_offline = ExecEnv::GetInstance()->impala_server()->IsOffline();
    if (!is_offline &&
        current_membership_.find(backend_id_) == current_membership_.end()) {
      VLOG(1) << "Registering local backend with statestore";
      subscriber_topic_updates->push_back(TTopicDelta());
      TTopicDelta& update = subscriber_topic_updates->back();
      update.topic_name = IMPALA_MEMBERSHIP_TOPIC;
      update.topic_entries.push_back(TTopicItem());

      TTopicItem& item = update.topic_entries.back();
      item.key = backend_id_;
      Status status = thrift_serializer_.Serialize(&backend_descriptor_, &item.value);
      if (!status.ok()) {
        LOG(WARNING) << "Failed to serialize Impala backend address for statestore topic: "
                     << status.GetDetail();
        subscriber_topic_updates->pop_back();
      }
    } else if (is_offline &&
        current_membership_.find(backend_id_) != current_membership_.end()) {
      LOG(WARNING) << "Removing offline ImpalaServer from statestore";
      subscriber_topic_updates->push_back(TTopicDelta());
      TTopicDelta& update = subscriber_topic_updates->back();
      update.topic_name = IMPALA_MEMBERSHIP_TOPIC;
      update.topic_deletions.push_back(backend_id_);
    }
    if (metrics_ != NULL) {
      num_fragment_instances_metric_->set_value(current_membership_.size());
    }
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
  return Status::OK();
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
      total_local_assignments_->Increment(1);
    }
  }

  if (VLOG_FILE_IS_ON) {
    stringstream s;
    s << "(" << data_location;
    s << " -> " << backend->address << ")";
    VLOG_FILE << "SimpleScheduler assignment (data->backend):  " << s.str();
  }
  return Status::OK();
}

void SimpleScheduler::GetAllKnownBackends(BackendList* backends) {
  lock_guard<mutex> lock(backend_map_lock_);
  backends->clear();
  BOOST_FOREACH(const BackendMap::value_type& backend_list, backend_map_) {
    backends->insert(backends->end(), backend_list.second.begin(),
                     backend_list.second.end());
  }
}

Status SimpleScheduler::ComputeScanRangeAssignment(const TQueryExecRequest& exec_request,
    QuerySchedule* schedule) {
  map<TPlanNodeId, vector<TScanRangeLocations> >::const_iterator entry;
  for (entry = exec_request.per_node_scan_ranges.begin();
      entry != exec_request.per_node_scan_ranges.end(); ++entry) {
    int fragment_idx = schedule->GetFragmentIdx(entry->first);
    const TPlanFragment& fragment = exec_request.fragments[fragment_idx];
    bool exec_at_coord = (fragment.partition.type == TPartitionType::UNPARTITIONED);

    FragmentScanRangeAssignment* assignment =
        &(*schedule->exec_params())[fragment_idx].scan_range_assignment;
    RETURN_IF_ERROR(ComputeScanRangeAssignment(
        entry->first, entry->second, exec_request.host_list, exec_at_coord,
        schedule->query_options(), assignment));
    schedule->AddScanRanges(entry->second.size());
  }
  return Status::OK();
}

Status SimpleScheduler::ComputeScanRangeAssignment(
    PlanNodeId node_id, const vector<TScanRangeLocations>& locations,
    const vector<TNetworkAddress>& host_list, bool exec_at_coord,
    const TQueryOptions& query_options, FragmentScanRangeAssignment* assignment) {
  // We adjust all replicas with memory distance less than base_distance to base_distance
  // and view all replicas with equal or better distance as the same. For a full list of
  // memory distance classes see TReplicaPreference in ImpalaInternalService.thrift.
  TReplicaPreference::type base_distance = query_options.replica_preference;

  // The query option to disable cached reads adjusts the memory base distance to view
  // all replicas as disk_local or worse.
  // TODO remove in CDH6
  if (query_options.disable_cached_reads &&
      base_distance == TReplicaPreference::CACHE_LOCAL) {
    base_distance = TReplicaPreference::DISK_LOCAL;
  }

  // On otherwise equivalent disk replicas we either pick the first one, or we pick a
  // random one. Picking random ones helps with preventing hot spots across several
  // queries. On cached replica we will always break ties randomly.
  bool random_non_cached_tiebreak = query_options.random_replica;

  // map from datanode host to total assigned bytes.
  unordered_map<TNetworkAddress, uint64_t> assigned_bytes_per_host;
  unordered_set<TNetworkAddress> remote_hosts;
  int64_t remote_bytes = 0L;
  int64_t local_bytes = 0L;
  int64_t cached_bytes = 0L;

  BOOST_FOREACH(const TScanRangeLocations& scan_range_locations, locations) {
    // Assign scans to replica with smallest memory distance.
    TReplicaPreference::type min_distance = TReplicaPreference::REMOTE;
    // Assign this scan range to the host w/ the fewest assigned bytes.
    uint64_t min_assigned_bytes = numeric_limits<uint64_t>::max();
    const TNetworkAddress* data_host = NULL;  // data server; not necessarily backend
    int volume_id = -1;
    bool is_cached = false;
    bool remote_read = false;

    // Equivalent replicas have the same adjusted memory distance and the same number of
    // assigned bytes.
    int num_equivalent_replicas = 0;
    BOOST_FOREACH(const TScanRangeLocation& location, scan_range_locations.locations) {
      TReplicaPreference::type memory_distance = TReplicaPreference::REMOTE;
      const TNetworkAddress& replica_host = host_list[location.host_idx];
      if (HasLocalBackend(replica_host)) {
        // Adjust whether or not this replica should count as being cached based on the
        // query option and whether it is collocated. If the DN is not collocated treat
        // the replica as not cached (network transfer dominates anyway in this case).
        // TODO: measure this in a cluster setup. Are remote reads better with caching?
        if (location.is_cached) {
          is_cached = true;
          memory_distance = TReplicaPreference::CACHE_LOCAL;
        } else {
          is_cached = false;
          memory_distance = TReplicaPreference::DISK_LOCAL;
        }
        remote_read = false;
      } else {
        is_cached = false;
        remote_read = true;
        memory_distance = TReplicaPreference::REMOTE;
      }
      memory_distance = max(memory_distance, base_distance);

      // Named variable is needed here for template parameter deduction to work.
      uint64_t initial_bytes = 0L;
      uint64_t assigned_bytes =
          *FindOrInsert(&assigned_bytes_per_host, replica_host, initial_bytes);

      bool found_new_replica = false;

      // Check if we can already accept based on memory distance.
      if (memory_distance < min_distance) {
        min_distance = memory_distance;
        num_equivalent_replicas = 1;
        found_new_replica = true;
      } else if (memory_distance == min_distance) {
        // Check the effective memory distance of the current replica to decide whether to
        // treat it as cached. If the actual distance has been increased to base_distance,
        // then cached_replica will be different from is_cached.
        bool cached_replica = memory_distance == TReplicaPreference::CACHE_LOCAL;
        // Load based scheduling
        if (assigned_bytes < min_assigned_bytes) {
          num_equivalent_replicas = 1;
          found_new_replica = true;
        } else if (assigned_bytes == min_assigned_bytes &&
            (random_non_cached_tiebreak || cached_replica)) {
          // We do reservoir sampling: assume we have k equivalent replicas and encounter
          // another equivalent one. Then we want to select the new one with probability
          // 1/(k+1). This is achieved by rand % k+1 == 0. Now, assume the probability for
          // one of the other replicas to be selected had been 1/k before. It will now be
          // 1/k * k/(k+1) = 1/(k+1). Thus we achieve the goal of picking a replica
          // uniformly at random.
          ++num_equivalent_replicas;
          const int r = rand();  // make debugging easier.
          found_new_replica = (r % num_equivalent_replicas == 0);
        }
      }

      if (found_new_replica) {
        min_assigned_bytes = assigned_bytes;
        data_host = &replica_host;
        volume_id = location.volume_id;
      }
    }  // end of BOOST_FOREACH

    int64_t scan_range_length = 0;
    if (scan_range_locations.scan_range.__isset.hdfs_file_split) {
      scan_range_length = scan_range_locations.scan_range.hdfs_file_split.length;
    }

    if (remote_read) {
      remote_bytes += scan_range_length;
      remote_hosts.insert(*data_host);
    } else {
      local_bytes += scan_range_length;
      if (is_cached) cached_bytes += scan_range_length;
    }
    assigned_bytes_per_host[*data_host] += scan_range_length;

    // translate data host to backend host
    DCHECK(data_host != NULL);

    TNetworkAddress exec_hostport;
    if (!exec_at_coord) {
      TBackendDescriptor backend;
      RETURN_IF_ERROR(GetBackend(*data_host, &backend));
      exec_hostport = backend.address;
    } else {
      exec_hostport = MakeNetworkAddress(FLAGS_hostname, FLAGS_be_port);
    }

    PerNodeScanRanges* scan_ranges =
        FindOrInsert(assignment, exec_hostport, PerNodeScanRanges());
    vector<TScanRangeParams>* scan_range_params_list =
        FindOrInsert(scan_ranges, node_id, vector<TScanRangeParams>());
    // add scan range
    TScanRangeParams scan_range_params;
    scan_range_params.scan_range = scan_range_locations.scan_range;
    // Explicitly set the optional fields.
    scan_range_params.__set_volume_id(volume_id);
    scan_range_params.__set_is_cached(is_cached);
    scan_range_params.__set_is_remote(remote_read);
    scan_range_params_list->push_back(scan_range_params);
  }

  if (VLOG_FILE_IS_ON) {
    VLOG_FILE << "Total remote scan volume = " <<
        PrettyPrinter::Print(remote_bytes, TUnit::BYTES);
    VLOG_FILE << "Total local scan volume = " <<
        PrettyPrinter::Print(local_bytes, TUnit::BYTES);
    VLOG_FILE << "Total cached scan volume = " <<
        PrettyPrinter::Print(cached_bytes, TUnit::BYTES);
    if (remote_hosts.size() > 0) {
      stringstream remote_node_log;
      remote_node_log << "Remote data node list: ";
      BOOST_FOREACH(const TNetworkAddress& remote_host, remote_hosts) {
        remote_node_log << remote_host << " ";
      }
      VLOG_FILE << remote_node_log.str();
    }

    BOOST_FOREACH(FragmentScanRangeAssignment::value_type& entry, *assignment) {
      VLOG_FILE << "ScanRangeAssignment: server=" << ThriftDebugString(entry.first);
      BOOST_FOREACH(PerNodeScanRanges::value_type& per_node_scan_ranges, entry.second) {
        stringstream str;
        BOOST_FOREACH(TScanRangeParams& params, per_node_scan_ranges.second) {
          str << ThriftDebugString(params) << " ";
        }
        VLOG_FILE << "node_id=" << per_node_scan_ranges.first << " ranges=" << str.str();
      }
    }
  }

  return Status::OK();
}

void SimpleScheduler::ComputeFragmentExecParams(const TQueryExecRequest& exec_request,
    QuerySchedule* schedule) {
  vector<FragmentExecParams>* fragment_exec_params = schedule->exec_params();
  // assign instance ids
  int64_t num_fragment_instances = 0;
  BOOST_FOREACH(FragmentExecParams& params, *fragment_exec_params) {
    for (int j = 0; j < params.hosts.size(); ++j) {
      int instance_num = num_fragment_instances + j;
      // we add instance_num to query_id.lo to create a globally-unique instance id
      TUniqueId instance_id;
      instance_id.hi = schedule->query_id().hi;
      DCHECK_LT(
          schedule->query_id().lo, numeric_limits<int64_t>::max() - instance_num - 1);
      instance_id.lo = schedule->query_id().lo + instance_num + 1;
      params.instance_ids.push_back(instance_id);
    }
    num_fragment_instances += params.hosts.size();
  }
  if (exec_request.fragments[0].partition.type == TPartitionType::UNPARTITIONED) {
    // the root fragment is executed directly by the coordinator
    --num_fragment_instances;
  }
  schedule->set_num_fragment_instances(num_fragment_instances);

  // compute destinations and # senders per exchange node
  // (the root fragment doesn't have a destination)
  for (int i = 1; i < fragment_exec_params->size(); ++i) {
    FragmentExecParams& params = (*fragment_exec_params)[i];
    int dest_fragment_idx = exec_request.dest_fragment_idx[i - 1];
    DCHECK_LT(dest_fragment_idx, fragment_exec_params->size());
    FragmentExecParams& dest_params = (*fragment_exec_params)[dest_fragment_idx];

    // set # of senders
    DCHECK(exec_request.fragments[i].output_sink.__isset.stream_sink);
    const TDataStreamSink& sink = exec_request.fragments[i].output_sink.stream_sink;
    // we can only handle unpartitioned (= broadcast), random-partitioned or
    // hash-partitioned output at the moment
    DCHECK(sink.output_partition.type == TPartitionType::UNPARTITIONED
           || sink.output_partition.type == TPartitionType::HASH_PARTITIONED
           || sink.output_partition.type == TPartitionType::RANDOM);
    PlanNodeId exch_id = sink.dest_node_id;
    // we might have multiple fragments sending to this exchange node
    // (distributed MERGE), which is why we need to add up the #senders
    params.sender_id_base = dest_params.per_exch_num_senders[exch_id];
    dest_params.per_exch_num_senders[exch_id] += params.hosts.size();

    // create one TPlanFragmentDestination per destination host
    params.destinations.resize(dest_params.hosts.size());
    for (int j = 0; j < dest_params.hosts.size(); ++j) {
      TPlanFragmentDestination& dest = params.destinations[j];
      dest.fragment_instance_id = dest_params.instance_ids[j];
      dest.server = dest_params.hosts[j];
      VLOG_RPC  << "dest for fragment " << i << ":"
                << " instance_id=" << dest.fragment_instance_id
                << " server=" << dest.server;
    }
  }
}

void SimpleScheduler::ComputeFragmentHosts(const TQueryExecRequest& exec_request,
    QuerySchedule* schedule) {
  vector<FragmentExecParams>* fragment_exec_params = schedule->exec_params();
  TNetworkAddress coord = MakeNetworkAddress(FLAGS_hostname, FLAGS_be_port);
  DCHECK_EQ(fragment_exec_params->size(), exec_request.fragments.size());
  vector<TPlanNodeType::type> scan_node_types;
  scan_node_types.push_back(TPlanNodeType::HDFS_SCAN_NODE);
  scan_node_types.push_back(TPlanNodeType::HBASE_SCAN_NODE);
  scan_node_types.push_back(TPlanNodeType::DATA_SOURCE_NODE);

  // compute hosts of producer fragment before those of consumer fragment(s),
  // the latter might inherit the set of hosts from the former
  for (int i = exec_request.fragments.size() - 1; i >= 0; --i) {
    const TPlanFragment& fragment = exec_request.fragments[i];
    FragmentExecParams& params = (*fragment_exec_params)[i];
    if (fragment.partition.type == TPartitionType::UNPARTITIONED) {
      // all single-node fragments run on the coordinator host
      params.hosts.push_back(coord);
      continue;
    }

    // UnionNodes are special because they can consume multiple partitioned inputs,
    // as well as execute multiple scans in the same fragment.
    // Fragments containing a UnionNode are executed on the union of hosts of all
    // scans in the fragment as well as the hosts of all its input fragments (s.t.
    // a UnionNode with partitioned joins or grouping aggregates as children runs on
    // at least as many hosts as the input to those children).
    if (ContainsNode(fragment.plan, TPlanNodeType::UNION_NODE)) {
      vector<TPlanNodeId> scan_nodes;
      FindNodes(fragment.plan, scan_node_types, &scan_nodes);
      vector<TPlanNodeId> exch_nodes;
      FindNodes(fragment.plan,
          vector<TPlanNodeType::type>(1, TPlanNodeType::EXCHANGE_NODE),
          &exch_nodes);

      // Add hosts of scan nodes.
      vector<TNetworkAddress> scan_hosts;
      for (int j = 0; j < scan_nodes.size(); ++j) {
        GetScanHosts(scan_nodes[j], exec_request, params, &scan_hosts);
      }
      unordered_set<TNetworkAddress> hosts(scan_hosts.begin(), scan_hosts.end());

      // Add hosts of input fragments.
      for (int j = 0; j < exch_nodes.size(); ++j) {
        int input_fragment_idx = FindSenderFragment(exch_nodes[j], i, exec_request);
        const vector<TNetworkAddress>& input_fragment_hosts =
            (*fragment_exec_params)[input_fragment_idx].hosts;
        hosts.insert(input_fragment_hosts.begin(), input_fragment_hosts.end());
      }
      DCHECK(!hosts.empty()) << "no hosts for fragment " << i << " with a UnionNode";

      params.hosts.assign(hosts.begin(), hosts.end());
      continue;
    }

    PlanNodeId leftmost_scan_id = FindLeftmostNode(fragment.plan, scan_node_types);
    if (leftmost_scan_id == g_ImpalaInternalService_constants.INVALID_PLAN_NODE_ID) {
      // there is no leftmost scan; we assign the same hosts as those of our
      // leftmost input fragment (so that a partitioned aggregation fragment
      // runs on the hosts that provide the input data)
      int input_fragment_idx = FindLeftmostInputFragment(i, exec_request);
      DCHECK_GE(input_fragment_idx, 0);
      DCHECK_LT(input_fragment_idx, fragment_exec_params->size());
      params.hosts = (*fragment_exec_params)[input_fragment_idx].hosts;
      // TODO: switch to unpartitioned/coord execution if our input fragment
      // is executed that way (could have been downgraded from distributed)
      continue;
    }

    // This fragment is executed on those hosts that have scan ranges
    // for the leftmost scan.
    GetScanHosts(leftmost_scan_id, exec_request, params, &params.hosts);
  }

  unordered_set<TNetworkAddress> unique_hosts;
  BOOST_FOREACH(const FragmentExecParams& exec_params, *fragment_exec_params) {
    unique_hosts.insert(exec_params.hosts.begin(), exec_params.hosts.end());
  }

  schedule->SetUniqueHosts(unique_hosts);
}

PlanNodeId SimpleScheduler::FindLeftmostNode(
    const TPlan& plan, const vector<TPlanNodeType::type>& types) {
  // the first node with num_children == 0 is the leftmost node
  int node_idx = 0;
  while (node_idx < plan.nodes.size() && plan.nodes[node_idx].num_children != 0) {
    ++node_idx;
  }
  if (node_idx == plan.nodes.size()) {
    return g_ImpalaInternalService_constants.INVALID_PLAN_NODE_ID;
  }
  const TPlanNode& node = plan.nodes[node_idx];

  for (int i = 0; i < types.size(); ++i) {
    if (node.node_type == types[i]) return node.node_id;
  }
  return g_ImpalaInternalService_constants.INVALID_PLAN_NODE_ID;
}

bool SimpleScheduler::ContainsNode(const TPlan& plan, TPlanNodeType::type type) {
  for (int i = 0; i < plan.nodes.size(); ++i) {
    if (plan.nodes[i].node_type == type) return true;
  }
  return false;
}

void SimpleScheduler::FindNodes(const TPlan& plan,
    const vector<TPlanNodeType::type>& types, vector<TPlanNodeId>* results) {
  for (int i = 0; i < plan.nodes.size(); ++i) {
    for (int j = 0; j < types.size(); ++j) {
      if (plan.nodes[i].node_type == types[j]) {
        results->push_back(plan.nodes[i].node_id);
        break;
      }
    }
  }
}

void SimpleScheduler::GetScanHosts(TPlanNodeId scan_id,
    const TQueryExecRequest& exec_request, const FragmentExecParams& params,
    vector<TNetworkAddress>* scan_hosts) {
  map<TPlanNodeId, vector<TScanRangeLocations> >::const_iterator entry =
      exec_request.per_node_scan_ranges.find(scan_id);
  if (entry == exec_request.per_node_scan_ranges.end() || entry->second.empty()) {
    // this scan node doesn't have any scan ranges; run it on the coordinator
    // TODO: we'll need to revisit this strategy once we can partition joins
    // (in which case this fragment might be executing a right outer join
    // with a large build table)
    scan_hosts->push_back(MakeNetworkAddress(FLAGS_hostname, FLAGS_be_port));
    return;
  }

  // Get the list of impalad host from scan_range_assignment_
  BOOST_FOREACH(const FragmentScanRangeAssignment::value_type& scan_range_assignment,
      params.scan_range_assignment) {
    scan_hosts->push_back(scan_range_assignment.first);
  }
}

int SimpleScheduler::FindLeftmostInputFragment(
    int fragment_idx, const TQueryExecRequest& exec_request) {
  // find the leftmost node, which we expect to be an exchage node
  vector<TPlanNodeType::type> exch_node_type;
  exch_node_type.push_back(TPlanNodeType::EXCHANGE_NODE);
  PlanNodeId exch_id =
      FindLeftmostNode(exec_request.fragments[fragment_idx].plan, exch_node_type);
  if (exch_id == g_ImpalaInternalService_constants.INVALID_PLAN_NODE_ID) {
    return g_ImpalaInternalService_constants.INVALID_PLAN_NODE_ID;
  }
  // find the fragment that sends to this exchange node
  return FindSenderFragment(exch_id, fragment_idx, exec_request);
}

int SimpleScheduler::FindSenderFragment(TPlanNodeId exch_id, int fragment_idx,
    const TQueryExecRequest& exec_request) {
  for (int i = 0; i < exec_request.dest_fragment_idx.size(); ++i) {
    if (exec_request.dest_fragment_idx[i] != fragment_idx) continue;
    const TPlanFragment& input_fragment = exec_request.fragments[i + 1];
    DCHECK(input_fragment.__isset.output_sink);
    DCHECK(input_fragment.output_sink.__isset.stream_sink);
    if (input_fragment.output_sink.stream_sink.dest_node_id == exch_id) return i + 1;
  }
  // this shouldn't happen
  DCHECK(false) << "no fragment sends to exch id " << exch_id;
  return g_ImpalaInternalService_constants.INVALID_PLAN_NODE_ID;
}

Status SimpleScheduler::GetRequestPool(const string& user,
    const TQueryOptions& query_options, string* pool) const {
  TResolveRequestPoolResult resolve_pool_result;
  const string& configured_pool = query_options.request_pool;
  RETURN_IF_ERROR(request_pool_service_->ResolveRequestPool(configured_pool, user,
        &resolve_pool_result));
  if (resolve_pool_result.status.status_code != TErrorCode::OK) {
    return Status(join(resolve_pool_result.status.error_msgs, "; "));
  }
  if (resolve_pool_result.resolved_pool.empty()) {
    return Status(Substitute(ERROR_USER_TO_POOL_MAPPING_NOT_FOUND, user,
          configured_pool));
  }
  if (!resolve_pool_result.has_access) {
    return Status(Substitute(ERROR_USER_NOT_ALLOWED_IN_POOL, user,
          configured_pool, resolve_pool_result.resolved_pool));
  }
  *pool = resolve_pool_result.resolved_pool;
  return Status::OK();
}

Status SimpleScheduler::Schedule(Coordinator* coord, QuerySchedule* schedule) {
  if (schedule->effective_user().empty()) {
    if (FLAGS_require_username) return Status(ERROR_USER_NOT_SPECIFIED);
    // Fall back to a 'default' user if not set so that queries can still run.
    VLOG(2) << "No user specified: using user=default";
  }
  const string& user =
    schedule->effective_user().empty() ? DEFAULT_USER : schedule->effective_user();
  VLOG(3) << "user='" << user << "'";
  string pool;
  RETURN_IF_ERROR(GetRequestPool(user, schedule->query_options(), &pool));
  schedule->set_request_pool(pool);
  // Statestore topic may not have been updated yet if this is soon after startup, but
  // there is always at least this backend.
  schedule->set_num_hosts(max<int64_t>(num_fragment_instances_metric_->value(), 1));

  if (!FLAGS_disable_admission_control) {
    RETURN_IF_ERROR(admission_controller_->AdmitQuery(schedule));
  }
  if (ExecEnv::GetInstance()->impala_server()->IsOffline()) {
    return Status("This Impala server is offline. Please retry your query later.");
  }

  RETURN_IF_ERROR(ComputeScanRangeAssignment(schedule->request(), schedule));
  ComputeFragmentHosts(schedule->request(), schedule);
  ComputeFragmentExecParams(schedule->request(), schedule);
  if (!FLAGS_enable_rm) return Status::OK();
  schedule->PrepareReservationRequest(pool, user);
  const TResourceBrokerReservationRequest& reservation_request =
      schedule->reservation_request();
  if (!reservation_request.resources.empty()) {
    Status status = resource_broker_->Reserve(
        reservation_request, schedule->reservation());
    if (!status.ok()) {
      // Warn about missing table and/or column stats if necessary.
      const TQueryCtx& query_ctx = schedule->request().query_ctx;
      if(!query_ctx.__isset.parent_query_id &&
          query_ctx.__isset.tables_missing_stats &&
          !query_ctx.tables_missing_stats.empty()) {
        status.AddDetail(GetTablesMissingStatsWarning(query_ctx.tables_missing_stats));
      }
      return status;
    }
    RETURN_IF_ERROR(schedule->ValidateReservation());
    AddToActiveResourceMaps(*schedule->reservation(), coord);
  }
  return Status::OK();
}

Status SimpleScheduler::Release(QuerySchedule* schedule) {
  if (!FLAGS_disable_admission_control) {
    RETURN_IF_ERROR(admission_controller_->ReleaseQuery(schedule));
  }
  if (FLAGS_enable_rm && schedule->NeedsRelease()) {
    DCHECK(resource_broker_ != NULL);
    Status status = resource_broker_->ReleaseReservation(
        schedule->reservation()->reservation_id);
    // Remove the reservation from the active-resource maps even if there was an error
    // releasing the reservation because the query running in the reservation is done.
    RemoveFromActiveResourceMaps(*schedule->reservation());
    RETURN_IF_ERROR(status);
  }
  return Status::OK();
}

void SimpleScheduler::AddToActiveResourceMaps(
    const TResourceBrokerReservationResponse& reservation, Coordinator* coord) {
  lock_guard<mutex> l(active_resources_lock_);
  active_reservations_[reservation.reservation_id] = coord;
  map<TNetworkAddress, llama::TAllocatedResource>::const_iterator iter;
  for (iter = reservation.allocated_resources.begin();
      iter != reservation.allocated_resources.end();
      ++iter) {
    TUniqueId client_resource_id;
    client_resource_id << iter->second.client_resource_id;
    active_client_resources_[client_resource_id] = coord;
  }
}

void SimpleScheduler::RemoveFromActiveResourceMaps(
    const TResourceBrokerReservationResponse& reservation) {
  lock_guard<mutex> l(active_resources_lock_);
  active_reservations_.erase(reservation.reservation_id);
  map<TNetworkAddress, llama::TAllocatedResource>::const_iterator iter;
  for (iter = reservation.allocated_resources.begin();
      iter != reservation.allocated_resources.end();
      ++iter) {
    TUniqueId client_resource_id;
    client_resource_id << iter->second.client_resource_id;
    active_client_resources_.erase(client_resource_id);
  }
}

// TODO: Refactor the Handle*{Reservation,Resource} functions to avoid code duplication.
void SimpleScheduler::HandlePreemptedReservation(const TUniqueId& reservation_id) {
  VLOG_QUERY << "HandlePreemptedReservation client_id=" << reservation_id;
  Coordinator* coord = NULL;
  {
    lock_guard<mutex> l(active_resources_lock_);
    ActiveReservationsMap::iterator it = active_reservations_.find(reservation_id);
    if (it != active_reservations_.end()) coord = it->second;
  }
  if (coord == NULL) {
    LOG(WARNING) << "Ignoring preempted reservation id " << reservation_id
                 << " because no active query using it was found.";
  } else {
    stringstream err_msg;
    err_msg << "Reservation " << reservation_id << " was preempted";
    Status status(err_msg.str());
    coord->Cancel(&status);
  }
}

void SimpleScheduler::HandlePreemptedResource(const TUniqueId& client_resource_id) {
  VLOG_QUERY << "HandlePreemptedResource client_id=" << client_resource_id;
  Coordinator* coord = NULL;
  {
    lock_guard<mutex> l(active_resources_lock_);
    ActiveClientResourcesMap::iterator it =
        active_client_resources_.find(client_resource_id);
    if (it != active_client_resources_.end()) coord = it->second;
  }
  if (coord == NULL) {
    LOG(WARNING) << "Ignoring preempted client resource id " << client_resource_id
                 << " because no active query using it was found.";
  } else {
    stringstream err_msg;
    err_msg << "Resource " << client_resource_id << " was preempted";
    Status status(err_msg.str());
    coord->Cancel(&status);
  }
}

void SimpleScheduler::HandleLostResource(const TUniqueId& client_resource_id) {
  VLOG_QUERY << "HandleLostResource preempting client_id=" << client_resource_id;
  Coordinator* coord = NULL;
  {
    lock_guard<mutex> l(active_resources_lock_);
    ActiveClientResourcesMap::iterator it =
        active_client_resources_.find(client_resource_id);
    if (it != active_client_resources_.end()) coord = it->second;
  }
  if (coord == NULL) {
    LOG(WARNING) << "Ignoring lost client resource id " << client_resource_id
                 << " because no active query using it was found.";
  } else {
    stringstream err_msg;
    err_msg << "Resource " << client_resource_id << " was lost";
    Status status(err_msg.str());
    coord->Cancel(&status);
  }
}

}
