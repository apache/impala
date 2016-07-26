// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#include "scheduling/simple-scheduler.h"

#include <atomic>
#include <random>
#include <vector>

#include <boost/algorithm/string.hpp>
#include <boost/algorithm/string/join.hpp>
#include <boost/bind.hpp>
#include <boost/mem_fn.hpp>
#include <gutil/strings/substitute.h>

#include "common/logging.h"
#include "util/metrics.h"
#include "resourcebroker/resource-broker.h"
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
#include "util/runtime-profile-counters.h"
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

DEFINE_bool(disable_admission_control, false, "Disables admission control.");

namespace impala {

static const string LOCAL_ASSIGNMENTS_KEY("simple-scheduler.local-assignments.total");
static const string ASSIGNMENTS_KEY("simple-scheduler.assignments.total");
static const string SCHEDULER_INIT_KEY("simple-scheduler.initialized");
static const string NUM_BACKENDS_KEY("simple-scheduler.num-backends");


static const string BACKENDS_WEB_PAGE = "/backends";
static const string BACKENDS_TEMPLATE = "backends.tmpl";

const string SimpleScheduler::IMPALA_MEMBERSHIP_TOPIC("impala-membership");

SimpleScheduler::SimpleScheduler(StatestoreSubscriber* subscriber,
    const string& backend_id, const TNetworkAddress& backend_address,
    MetricGroup* metrics, Webserver* webserver, ResourceBroker* resource_broker,
    RequestPoolService* request_pool_service)
  : backend_config_(std::make_shared<const BackendConfig>()),
    metrics_(metrics->GetChildGroup("scheduler")),
    webserver_(webserver),
    statestore_subscriber_(subscriber),
    local_backend_id_(backend_id),
    thrift_serializer_(false),
    total_assignments_(NULL),
    total_local_assignments_(NULL),
    initialized_(NULL),
    resource_broker_(resource_broker),
    request_pool_service_(request_pool_service) {
  local_backend_descriptor_.address = backend_address;

  if (FLAGS_disable_admission_control) LOG(INFO) << "Admission control is disabled.";
  if (!FLAGS_disable_admission_control) {
    admission_controller_.reset(
        new AdmissionController(request_pool_service_, metrics, backend_address));
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
  : backend_config_(std::make_shared<const BackendConfig>(backends)),
    metrics_(metrics),
    webserver_(webserver),
    statestore_subscriber_(NULL),
    thrift_serializer_(false),
    total_assignments_(NULL),
    total_local_assignments_(NULL),
    initialized_(NULL),
    resource_broker_(resource_broker),
    request_pool_service_(request_pool_service) {
  DCHECK(backends.size() > 0);
  local_backend_descriptor_.address = MakeNetworkAddress(FLAGS_hostname, FLAGS_be_port);
  if (FLAGS_disable_admission_control) LOG(INFO) << "Admission control is disabled.";
  // request_pool_service_ may be null in unit tests
  if (request_pool_service_ != NULL && !FLAGS_disable_admission_control) {
    admission_controller_.reset(
        new AdmissionController(request_pool_service_, metrics, TNetworkAddress()));
  }
}

Status SimpleScheduler::Init() {
  LOG(INFO) << "Starting simple scheduler";

  // Figure out what our IP address is, so that each subscriber
  // doesn't have to resolve it on every heartbeat.
  IpAddr ip;
  const Hostname& hostname = local_backend_descriptor_.address.hostname;
  Status status = HostnameToIpAddr(hostname, &ip);
  if (!status.ok()) {
    VLOG(1) << status.GetDetail();
    status.AddDetail("SimpleScheduler failed to start");
    return status;
  }

  local_backend_descriptor_.ip_address = ip;
  LOG(INFO) << "Simple-scheduler using " << ip << " as IP address";

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
    // This is after registering with the statestored, so we already have to synchronize
    // access to the backend_config_ shared_ptr.
    int num_backends = GetBackendConfig()->backend_map().size();
    total_assignments_ = metrics_->AddCounter<int64_t>(ASSIGNMENTS_KEY, 0);
    total_local_assignments_ = metrics_->AddCounter<int64_t>(LOCAL_ASSIGNMENTS_KEY, 0);
    initialized_ = metrics_->AddProperty(SCHEDULER_INIT_KEY, true);
    num_fragment_instances_metric_ = metrics_->AddGauge<int64_t>(
        NUM_BACKENDS_KEY, num_backends);
  }

  if (statestore_subscriber_ != NULL) {
    if (webserver_ != NULL) {
      const TNetworkAddress& webserver_address = webserver_->http_address();
      if (IsWildcardAddress(webserver_address.hostname)) {
        local_backend_descriptor_.__set_debug_http_address(
            MakeNetworkAddress(ip, webserver_address.port));
      } else {
        local_backend_descriptor_.__set_debug_http_address(webserver_address);
      }
      local_backend_descriptor_.__set_secure_webserver(webserver_->IsSecure());
    }
  }
  return Status::OK();
}

void SimpleScheduler::BackendsUrlCallback(const Webserver::ArgumentMap& args,
    Document* document) {
  BackendList backends;
  GetAllKnownBackends(&backends);
  Value backends_list(kArrayType);
  for (const BackendList::value_type& backend: backends) {
    Value str(TNetworkAddressToString(backend.address).c_str(), document->GetAllocator());
    backends_list.PushBack(str, document->GetAllocator());
  }

  document->AddMember("backends", backends_list, document->GetAllocator());
}

void SimpleScheduler::UpdateMembership(
    const StatestoreSubscriber::TopicDeltaMap& incoming_topic_deltas,
    vector<TTopicDelta>* subscriber_topic_updates) {
  // First look to see if the topic(s) we're interested in have an update
  StatestoreSubscriber::TopicDeltaMap::const_iterator topic =
      incoming_topic_deltas.find(IMPALA_MEMBERSHIP_TOPIC);

  if (topic != incoming_topic_deltas.end()) {
    const TTopicDelta& delta = topic->second;

    // This function needs to handle both delta and non-delta updates. To minimize the
    // time needed to hold locks, all updates are applied to a copy of backend_config_,
    // which is then swapped into place atomically.
    std::shared_ptr<BackendConfig> new_backend_config;

    if (!delta.is_delta) {
      current_membership_.clear();
      new_backend_config = std::make_shared<BackendConfig>();
    } else {
      // Make a copy
      lock_guard<mutex> lock(backend_config_lock_);
      new_backend_config = std::make_shared<BackendConfig>(*backend_config_);
    }

    // Process new entries to the topic
    for (const TTopicItem& item: delta.topic_entries) {
      TBackendDescriptor be_desc;
      // Benchmarks have suggested that this method can deserialize
      // ~10m messages per second, so no immediate need to consider optimization.
      uint32_t len = item.value.size();
      Status status = DeserializeThriftMsg(reinterpret_cast<const uint8_t*>(
          item.value.data()), &len, false, &be_desc);
      if (!status.ok()) {
        VLOG(2) << "Error deserializing membership topic item with key: " << item.key;
        continue;
      }
      if (item.key == local_backend_id_
          && be_desc.address != local_backend_descriptor_.address) {
        // Someone else has registered this subscriber ID with a different address. We
        // will try to re-register (i.e. overwrite their subscription), but there is
        // likely a configuration problem.
        LOG_EVERY_N(WARNING, 30) << "Duplicate subscriber registration from address: "
          << be_desc.address;
      }

      new_backend_config->AddBackend(be_desc);
      current_membership_.insert(make_pair(item.key, be_desc));
    }
    // Process deletions from the topic
    for (const string& backend_id: delta.topic_deletions) {
      if (current_membership_.find(backend_id) != current_membership_.end()) {
        new_backend_config->RemoveBackend(current_membership_[backend_id]);
        current_membership_.erase(backend_id);
      }
    }
    SetBackendConfig(new_backend_config);

    // If this impalad is not in our view of the membership list, we should add it and
    // tell the statestore.
    bool is_offline = ExecEnv::GetInstance() &&
        ExecEnv::GetInstance()->impala_server()->IsOffline();
    if (!is_offline &&
        current_membership_.find(local_backend_id_) == current_membership_.end()) {
      VLOG(1) << "Registering local backend with statestore";
      subscriber_topic_updates->push_back(TTopicDelta());
      TTopicDelta& update = subscriber_topic_updates->back();
      update.topic_name = IMPALA_MEMBERSHIP_TOPIC;
      update.topic_entries.push_back(TTopicItem());

      TTopicItem& item = update.topic_entries.back();
      item.key = local_backend_id_;
      Status status = thrift_serializer_.Serialize(
          &local_backend_descriptor_, &item.value);
      if (!status.ok()) {
        LOG(WARNING) << "Failed to serialize Impala backend address for statestore topic:"
                     << " " << status.GetDetail();
        subscriber_topic_updates->pop_back();
      }
    } else if (is_offline &&
        current_membership_.find(local_backend_id_) != current_membership_.end()) {
      LOG(WARNING) << "Removing offline ImpalaServer from statestore";
      subscriber_topic_updates->push_back(TTopicDelta());
      TTopicDelta& update = subscriber_topic_updates->back();
      update.topic_name = IMPALA_MEMBERSHIP_TOPIC;
      update.topic_deletions.push_back(local_backend_id_);
    }
    if (metrics_ != NULL) {
      num_fragment_instances_metric_->set_value(current_membership_.size());
    }
  }
}

SimpleScheduler::BackendConfigPtr SimpleScheduler::GetBackendConfig() const {
  lock_guard<mutex> l(backend_config_lock_);
  DCHECK(backend_config_.get() != NULL);
  BackendConfigPtr backend_config = backend_config_;
  return backend_config;
}

void SimpleScheduler::SetBackendConfig(const BackendConfigPtr& backend_config)
{
  lock_guard<mutex> l(backend_config_lock_);
  backend_config_ = backend_config;
}


void SimpleScheduler::GetAllKnownBackends(BackendList* backends) {
  backends->clear();
  BackendConfigPtr backend_config = GetBackendConfig();
  for (const BackendMap::value_type& backend_list: backend_config->backend_map()) {
    backends->insert(backends->end(), backend_list.second.begin(),
                     backend_list.second.end());
  }
}

Status SimpleScheduler::ComputeScanRangeAssignment(const TQueryExecRequest& exec_request,
    QuerySchedule* schedule) {
  map<TPlanNodeId, vector<TScanRangeLocations>>::const_iterator entry;
  RuntimeProfile::Counter* total_assignment_timer =
      ADD_TIMER(schedule->summary_profile(), "ComputeScanRangeAssignmentTimer");

  BackendConfigPtr backend_config = GetBackendConfig();

  for (entry = exec_request.per_node_scan_ranges.begin();
      entry != exec_request.per_node_scan_ranges.end(); ++entry) {
    const TPlanNodeId node_id = entry->first;
    int fragment_idx = schedule->GetFragmentIdx(node_id);
    const TPlanFragment& fragment = exec_request.fragments[fragment_idx];
    bool exec_at_coord = (fragment.partition.type == TPartitionType::UNPARTITIONED);

    const TPlanNode& node = fragment.plan.nodes[schedule->GetNodeIdx(node_id)];
    DCHECK_EQ(node.node_id, node_id);

    const TReplicaPreference::type* node_replica_preference = node.__isset.hdfs_scan_node
        && node.hdfs_scan_node.__isset.replica_preference
        ? &node.hdfs_scan_node.replica_preference : NULL;

    bool node_random_replica = node.__isset.hdfs_scan_node
        && node.hdfs_scan_node.__isset.random_replica
        && node.hdfs_scan_node.random_replica;

    FragmentScanRangeAssignment* assignment =
        &(*schedule->exec_params())[fragment_idx].scan_range_assignment;
    RETURN_IF_ERROR(ComputeScanRangeAssignment(*backend_config,
        node_id, node_replica_preference, node_random_replica, entry->second,
        exec_request.host_list, exec_at_coord, schedule->query_options(),
        total_assignment_timer, assignment));
    schedule->AddScanRanges(entry->second.size());
  }
  return Status::OK();
}

Status SimpleScheduler::ComputeScanRangeAssignment(
    const BackendConfig& backend_config, PlanNodeId node_id,
    const TReplicaPreference::type* node_replica_preference, bool node_random_replica,
    const vector<TScanRangeLocations>& locations,
    const vector<TNetworkAddress>& host_list, bool exec_at_coord,
    const TQueryOptions& query_options, RuntimeProfile::Counter* timer,
    FragmentScanRangeAssignment* assignment) {
  SCOPED_TIMER(timer);
  // We adjust all replicas with memory distance less than base_distance to base_distance
  // and collect all replicas with equal or better distance as candidates. For a full list
  // of memory distance classes see TReplicaPreference in PlanNodes.thrift.
  TReplicaPreference::type base_distance = query_options.replica_preference;

  // The query option to disable cached reads adjusts the memory base distance to view
  // all replicas as having a distance disk_local or worse.
  if (query_options.disable_cached_reads &&
      base_distance == TReplicaPreference::CACHE_LOCAL) {
    base_distance = TReplicaPreference::DISK_LOCAL;
  }

  // A preference attached to the plan node takes precedence.
  if (node_replica_preference) base_distance = *node_replica_preference;

  // Between otherwise equivalent backends we optionally break ties by comparing their
  // random rank.
  bool random_replica = query_options.schedule_random_replica || node_random_replica;

  AssignmentCtx assignment_ctx(backend_config, total_assignments_,
      total_local_assignments_);

  vector<const TScanRangeLocations*> remote_scan_range_locations;

  // Loop over all scan ranges, select a backend for those with local impalads and collect
  // all others for later processing.
  for (const TScanRangeLocations& scan_range_locations: locations) {
    TReplicaPreference::type min_distance = TReplicaPreference::REMOTE;

    // Select backend host for the current scan range.
    if (exec_at_coord) {
      assignment_ctx.RecordScanRangeAssignment(local_backend_descriptor_, node_id,
          host_list, scan_range_locations, assignment);
    } else {
      // Collect backend candidates with smallest memory distance.
      vector<IpAddr> backend_candidates;
      if (base_distance < TReplicaPreference::REMOTE) {
        for (const TScanRangeLocation& location: scan_range_locations.locations) {
          const TNetworkAddress& replica_host = host_list[location.host_idx];
          // Determine the adjusted memory distance to the closest backend for the replica
          // host.
          TReplicaPreference::type memory_distance = TReplicaPreference::REMOTE;
          IpAddr backend_ip;
          bool has_local_backend = assignment_ctx.backend_config().LookUpBackendIp(
              replica_host.hostname, &backend_ip);
          if (has_local_backend) {
            if (location.is_cached) {
              memory_distance = TReplicaPreference::CACHE_LOCAL;
            } else {
              memory_distance = TReplicaPreference::DISK_LOCAL;
            }
          } else {
            memory_distance = TReplicaPreference::REMOTE;
          }
          memory_distance = max(memory_distance, base_distance);

          // We only need to collect backend candidates for non-remote reads, as it is the
          // nature of remote reads that there is no backend available.
          if (memory_distance < TReplicaPreference::REMOTE) {
            DCHECK(has_local_backend);
            // Check if we found a closer replica than the previous ones.
            if (memory_distance < min_distance) {
              min_distance = memory_distance;
              backend_candidates.clear();
              backend_candidates.push_back(backend_ip);
            } else if (memory_distance == min_distance) {
              backend_candidates.push_back(backend_ip);
            }
          }
        }
      }  // End of candidate selection.
      DCHECK(!backend_candidates.empty() || min_distance == TReplicaPreference::REMOTE);

      // Check the effective memory distance of the candidates to decide whether to treat
      // the scan range as cached.
      bool cached_replica = min_distance == TReplicaPreference::CACHE_LOCAL;

      // Pick backend host based on data location.
      bool local_backend = min_distance != TReplicaPreference::REMOTE;

      if (!local_backend) {
        remote_scan_range_locations.push_back(&scan_range_locations);
        continue;
      }
      // For local reads we want to break ties by backend rank in these cases:
      // - if it is enforced via a query option.
      // - when selecting between cached replicas. In this case there is no OS buffer
      //   cache to worry about.
      // Remote reads will always break ties by backend rank.
      bool decide_local_assignment_by_rank = random_replica || cached_replica;
      const IpAddr* backend_ip = NULL;
      backend_ip = assignment_ctx.SelectLocalBackendHost(backend_candidates,
          decide_local_assignment_by_rank);
      TBackendDescriptor backend;
      assignment_ctx.SelectBackendOnHost(*backend_ip, &backend);
      assignment_ctx.RecordScanRangeAssignment(backend, node_id, host_list,
          scan_range_locations, assignment);
    }  // End of backend host selection.
  }  // End of for loop over scan ranges.

  // Assign remote scans to backends.
  for (const TScanRangeLocations* scan_range_locations: remote_scan_range_locations) {
    const IpAddr* backend_ip = assignment_ctx.SelectRemoteBackendHost();
    TBackendDescriptor backend;
    assignment_ctx.SelectBackendOnHost(*backend_ip, &backend);
    assignment_ctx.RecordScanRangeAssignment(backend, node_id, host_list,
        *scan_range_locations, assignment);
  }

  if (VLOG_FILE_IS_ON) assignment_ctx.PrintAssignment(*assignment);

  return Status::OK();
}

void SimpleScheduler::ComputeFragmentExecParams(const TQueryExecRequest& exec_request,
    QuerySchedule* schedule) {
  vector<FragmentExecParams>* fragment_exec_params = schedule->exec_params();
  // assign instance ids
  int64_t num_fragment_instances = 0;
  for (FragmentExecParams& params: *fragment_exec_params) {
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
  DCHECK_EQ(fragment_exec_params->size(), exec_request.fragments.size());
  vector<TPlanNodeType::type> scan_node_types;
  scan_node_types.push_back(TPlanNodeType::HDFS_SCAN_NODE);
  scan_node_types.push_back(TPlanNodeType::HBASE_SCAN_NODE);
  scan_node_types.push_back(TPlanNodeType::DATA_SOURCE_NODE);
  scan_node_types.push_back(TPlanNodeType::KUDU_SCAN_NODE);

  // compute hosts of producer fragment before those of consumer fragment(s),
  // the latter might inherit the set of hosts from the former
  for (int i = exec_request.fragments.size() - 1; i >= 0; --i) {
    const TPlanFragment& fragment = exec_request.fragments[i];
    FragmentExecParams& params = (*fragment_exec_params)[i];
    if (fragment.partition.type == TPartitionType::UNPARTITIONED) {
      // all single-node fragments run on the coordinator host
      params.hosts.push_back(local_backend_descriptor_.address);
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
  for (const FragmentExecParams& exec_params: *fragment_exec_params) {
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
  map<TPlanNodeId, vector<TScanRangeLocations>>::const_iterator entry =
      exec_request.per_node_scan_ranges.find(scan_id);
  if (entry == exec_request.per_node_scan_ranges.end() || entry->second.empty()) {
    // this scan node doesn't have any scan ranges; run it on the coordinator
    // TODO: we'll need to revisit this strategy once we can partition joins
    // (in which case this fragment might be executing a right outer join
    // with a large build table)
    scan_hosts->push_back(local_backend_descriptor_.address);
    return;
  }

  // Get the list of impalad host from scan_range_assignment_
  for (const FragmentScanRangeAssignment::value_type& scan_range_assignment:
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

Status SimpleScheduler::Schedule(Coordinator* coord, QuerySchedule* schedule) {
  string resolved_pool;
  RETURN_IF_ERROR(request_pool_service_->ResolveRequestPool(
      schedule->request().query_ctx, &resolved_pool));
  schedule->set_request_pool(resolved_pool);
  schedule->summary_profile()->AddInfoString("Request Pool", resolved_pool);

  if (ExecEnv::GetInstance()->impala_server()->IsOffline()) {
    return Status("This Impala server is offline. Please retry your query later.");
  }

  RETURN_IF_ERROR(ComputeScanRangeAssignment(schedule->request(), schedule));
  ComputeFragmentHosts(schedule->request(), schedule);
  ComputeFragmentExecParams(schedule->request(), schedule);
  if (!FLAGS_disable_admission_control) {
    RETURN_IF_ERROR(admission_controller_->AdmitQuery(schedule));
  }
  if (!FLAGS_enable_rm) return Status::OK();
  string user = GetEffectiveUser(schedule->request().query_ctx.session);
  if (user.empty()) user = "default";
  schedule->PrepareReservationRequest(resolved_pool, user);
  const TResourceBrokerReservationRequest& reservation_request =
      schedule->reservation_request();
  if (!reservation_request.resources.empty()) {
    Status status = resource_broker_->Reserve(
        reservation_request, schedule->reservation());
    if (!status.ok()) {
      // Warn about missing table and/or column stats if necessary.
      const TQueryCtx& query_ctx = schedule->request().query_ctx;
      if (!query_ctx.__isset.parent_query_id &&
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

Status SimpleScheduler::HostnameToIpAddr(const Hostname& hostname, IpAddr* ip) {
  // Try to resolve via the operating system.
  vector<IpAddr> ipaddrs;
  Status status = HostnameToIpAddrs(hostname, &ipaddrs);
  if (!status.ok() || ipaddrs.empty()) {
    stringstream ss;
    ss << "Failed to resolve " << hostname << ": " << status.GetDetail();
    return Status(ss.str());
  }

  // HostnameToIpAddrs() calls getaddrinfo() from glibc and will preserve the order of the
  // result. RFC 3484 only specifies a partial order so we need to sort the addresses
  // before picking the first non-localhost one.
  sort(ipaddrs.begin(), ipaddrs.end());

  // Try to find a non-localhost address, otherwise just use the first IP address
  // returned.
  *ip = ipaddrs[0];
  if (!FindFirstNonLocalhost(ipaddrs, ip)) {
    VLOG(3) << "Only localhost addresses found for " << hostname;
  }
  return Status::OK();
}

SimpleScheduler::BackendConfig::BackendConfig(
    const std::vector<TNetworkAddress>& backends) {
  // Construct backend_map and backend_ip_map.
  for (int i = 0; i < backends.size(); ++i) {
    IpAddr ip;
    Status status = HostnameToIpAddr(backends[i].hostname, &ip);
    if (!status.ok()) {
      VLOG(1) << status.GetDetail();
      continue;
    }

    BackendMap::iterator it = backend_map_.find(ip);
    if (it == backend_map_.end()) {
      it = backend_map_.insert(
          make_pair(ip, BackendList())).first;
      backend_ip_map_[backends[i].hostname] = ip;
    }

    TBackendDescriptor descriptor;
    descriptor.address = MakeNetworkAddress(ip, backends[i].port);
    descriptor.ip_address = ip;
    it->second.push_back(descriptor);
  }
}

void SimpleScheduler::BackendConfig::AddBackend(const TBackendDescriptor& be_desc) {
  BackendList* be_descs = &backend_map_[be_desc.ip_address];
  if (find(be_descs->begin(), be_descs->end(), be_desc) == be_descs->end()) {
    be_descs->push_back(be_desc);
  }
  backend_ip_map_[be_desc.address.hostname] = be_desc.ip_address;
}

void SimpleScheduler::BackendConfig::RemoveBackend(const TBackendDescriptor& be_desc) {
  backend_ip_map_.erase(be_desc.address.hostname);
  auto be_descs_it = backend_map_.find(be_desc.ip_address);
  if (be_descs_it != backend_map_.end()) {
    BackendList* be_descs = &be_descs_it->second;
    be_descs->erase(remove(be_descs->begin(), be_descs->end(), be_desc), be_descs->end());
    if (be_descs->empty()) backend_map_.erase(be_descs_it);
  }
}

bool SimpleScheduler::BackendConfig::LookUpBackendIp(const Hostname& hostname,
    IpAddr* ip) const {
  // Check if hostname is already a valid IP address.
  if (backend_map_.find(hostname) != backend_map_.end()) {
    if (ip) *ip = hostname;
    return true;
  }
  auto it = backend_ip_map_.find(hostname);
  if (it != backend_ip_map_.end()) {
    if (ip) *ip = it->second;
    return true;
  }
  return false;
}

SimpleScheduler::AssignmentCtx::AssignmentCtx(
    const BackendConfig& backend_config,
    IntCounter* total_assignments, IntCounter* total_local_assignments)
  : backend_config_(backend_config), first_unused_backend_idx_(0),
    total_assignments_(total_assignments),
    total_local_assignments_(total_local_assignments) {
  random_backend_order_.reserve(backend_map().size());
  for (auto& v: backend_map()) random_backend_order_.push_back(&v);
  std::mt19937 g(rand());
  std::shuffle(random_backend_order_.begin(), random_backend_order_.end(), g);
  // Initialize inverted map for backend rank lookups
  int i = 0;
  for (const BackendMap::value_type* v: random_backend_order_) {
    random_backend_rank_[v->first] = i++;
  }
}

const SimpleScheduler::IpAddr* SimpleScheduler::AssignmentCtx::SelectLocalBackendHost(
    const std::vector<IpAddr>& data_locations, bool break_ties_by_rank) {
  DCHECK(!data_locations.empty());
  // List of candidate indexes into 'data_locations'.
  vector<int> candidates_idxs;
  // Find locations with minimum number of assigned bytes.
  int64_t min_assigned_bytes = numeric_limits<int64_t>::max();
  for (int i = 0; i < data_locations.size(); ++i) {
    const IpAddr& backend_ip = data_locations[i];
    int64_t assigned_bytes = 0;
    auto handle_it = assignment_heap_.find(backend_ip);
    if (handle_it != assignment_heap_.end()) {
      assigned_bytes = (*handle_it->second).assigned_bytes;
    }
    if (assigned_bytes < min_assigned_bytes) {
      candidates_idxs.clear();
      min_assigned_bytes = assigned_bytes;
    }
    if (assigned_bytes == min_assigned_bytes) candidates_idxs.push_back(i);
  }

  DCHECK(!candidates_idxs.empty());
  auto min_rank_idx = candidates_idxs.begin();
  if (break_ties_by_rank) {
    min_rank_idx = min_element(candidates_idxs.begin(), candidates_idxs.end(),
        [&data_locations, this](const int& a, const int& b) {
          return GetBackendRank(data_locations[a]) < GetBackendRank(data_locations[b]);
        });
  }
  return &data_locations[*min_rank_idx];
}

const SimpleScheduler::IpAddr* SimpleScheduler::AssignmentCtx::SelectRemoteBackendHost() {
  const IpAddr* candidate_ip;
  if (HasUnusedBackends()) {
    // Pick next unused backend.
    candidate_ip = GetNextUnusedBackendAndIncrement();
  } else {
    // Pick next backend from assignment_heap. All backends must have been inserted into
    // the heap at this point.
    DCHECK(backend_config_.NumBackends() == assignment_heap_.size());
    candidate_ip = &(assignment_heap_.top().ip);
  }
  DCHECK(candidate_ip != NULL);
  return candidate_ip;
}

bool SimpleScheduler::AssignmentCtx::HasUnusedBackends() const {
  return first_unused_backend_idx_ < random_backend_order_.size();
}

const SimpleScheduler::IpAddr*
    SimpleScheduler::AssignmentCtx::GetNextUnusedBackendAndIncrement() {
  DCHECK(HasUnusedBackends());
  const IpAddr* ip = &(random_backend_order_[first_unused_backend_idx_++])->first;
  DCHECK(backend_map().find(*ip) != backend_map().end());
  return ip;
}

int SimpleScheduler::AssignmentCtx::GetBackendRank(const IpAddr& ip) const {
  auto it = random_backend_rank_.find(ip);
  DCHECK(it != random_backend_rank_.end());
  return it->second;
}

void SimpleScheduler::AssignmentCtx::SelectBackendOnHost(const IpAddr& backend_ip,
    TBackendDescriptor* backend) {
  BackendMap::const_iterator backend_it = backend_map().find(backend_ip);
  DCHECK(backend_it != backend_map().end());
  const BackendList& backends_on_host = backend_it->second;
  DCHECK(backends_on_host.size() > 0);
  if (backends_on_host.size() == 1) {
    *backend = *backends_on_host.begin();
  } else {
    BackendList::const_iterator* next_backend_on_host;
    next_backend_on_host = FindOrInsert(&next_backend_per_host_, backend_ip,
        backends_on_host.begin());
    DCHECK(find(backends_on_host.begin(), backends_on_host.end(), **next_backend_on_host)
        != backends_on_host.end());
    *backend = **next_backend_on_host;
    // Rotate
    ++(*next_backend_on_host);
    if (*next_backend_on_host == backends_on_host.end()) {
      *next_backend_on_host = backends_on_host.begin();
    }
  }
}

void SimpleScheduler::AssignmentCtx::RecordScanRangeAssignment(
    const TBackendDescriptor& backend, PlanNodeId node_id,
    const vector<TNetworkAddress>& host_list,
    const TScanRangeLocations& scan_range_locations,
    FragmentScanRangeAssignment* assignment) {
  int64_t scan_range_length = 0;
  if (scan_range_locations.scan_range.__isset.hdfs_file_split) {
    scan_range_length = scan_range_locations.scan_range.hdfs_file_split.length;
  } else if (scan_range_locations.scan_range.__isset.kudu_key_range) {
    // Hack so that kudu ranges are well distributed.
    // TODO: KUDU-1133 Use the tablet size instead.
    scan_range_length = 1000;
  }

  IpAddr backend_ip;
  backend_config_.LookUpBackendIp(backend.address.hostname, &backend_ip);
  DCHECK(backend_map().find(backend_ip) != backend_map().end());
  assignment_heap_.InsertOrUpdate(backend_ip, scan_range_length,
      GetBackendRank(backend_ip));

  // See if the read will be remote. This is not the case if the impalad runs on one of
  // the replica's datanodes.
  bool remote_read = true;
  // For local reads we can set volume_id and is_cached. For remote reads HDFS will
  // decide which replica to use so we keep those at default values.
  int volume_id = -1;
  bool is_cached = false;
  for (const TScanRangeLocation& location: scan_range_locations.locations) {
    const TNetworkAddress& replica_host = host_list[location.host_idx];
    IpAddr replica_ip;
    if (backend_config_.LookUpBackendIp(replica_host.hostname, &replica_ip)
        && backend_ip == replica_ip) {
      remote_read = false;
      volume_id = location.volume_id;
      is_cached = location.is_cached;
      break;
    }
  }

  if (remote_read) {
    assignment_byte_counters_.remote_bytes += scan_range_length;
  } else {
    assignment_byte_counters_.local_bytes += scan_range_length;
    if (is_cached) assignment_byte_counters_.cached_bytes += scan_range_length;
  }

  if (total_assignments_ != NULL) {
    DCHECK(total_local_assignments_ != NULL);
    total_assignments_->Increment(1);
    if (!remote_read) total_local_assignments_->Increment(1);
  }

  PerNodeScanRanges* scan_ranges = FindOrInsert(assignment, backend.address,
      PerNodeScanRanges());
  vector<TScanRangeParams>* scan_range_params_list = FindOrInsert(scan_ranges, node_id,
      vector<TScanRangeParams>());
  // Add scan range.
  TScanRangeParams scan_range_params;
  scan_range_params.scan_range = scan_range_locations.scan_range;
  scan_range_params.__set_volume_id(volume_id);
  scan_range_params.__set_is_cached(is_cached);
  scan_range_params.__set_is_remote(remote_read);
  scan_range_params_list->push_back(scan_range_params);

  if (VLOG_FILE_IS_ON) {
    VLOG_FILE << "SimpleScheduler assignment to backend: " << backend.address
        << "(" << (remote_read ? "remote" : "local") << " selection)";
  }
}

void SimpleScheduler::AssignmentCtx::PrintAssignment(
    const FragmentScanRangeAssignment& assignment) {
  VLOG_FILE << "Total remote scan volume = " <<
    PrettyPrinter::Print(assignment_byte_counters_.remote_bytes, TUnit::BYTES);
  VLOG_FILE << "Total local scan volume = " <<
    PrettyPrinter::Print(assignment_byte_counters_.local_bytes, TUnit::BYTES);
  VLOG_FILE << "Total cached scan volume = " <<
    PrettyPrinter::Print(assignment_byte_counters_.cached_bytes, TUnit::BYTES);

  for (const FragmentScanRangeAssignment::value_type& entry: assignment) {
    VLOG_FILE << "ScanRangeAssignment: server=" << ThriftDebugString(entry.first);
    for (const PerNodeScanRanges::value_type& per_node_scan_ranges: entry.second) {
      stringstream str;
      for (const TScanRangeParams& params: per_node_scan_ranges.second) {
        str << ThriftDebugString(params) << " ";
      }
      VLOG_FILE << "node_id=" << per_node_scan_ranges.first << " ranges=" << str.str();
    }
  }
}

void SimpleScheduler::AddressableAssignmentHeap::InsertOrUpdate(const IpAddr& ip,
    int64_t assigned_bytes, int rank) {
  auto handle_it = backend_handles_.find(ip);
  if (handle_it == backend_handles_.end()) {
    AssignmentHeap::handle_type handle = backend_heap_.push({assigned_bytes, rank, ip});
    backend_handles_.emplace(ip, handle);
  } else {
    // We need to rebuild the heap after every update operation. Calling decrease once is
    // sufficient as both assignments decrease the key.
    AssignmentHeap::handle_type handle = handle_it->second;
    (*handle).assigned_bytes += assigned_bytes;
    backend_heap_.decrease(handle);
  }
}

}
