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

#include "scheduling/executor-group.h"

namespace impala {

// Hand-testing shows that 25 replicas produces a reasonable balance between nodes
// across the hash ring. See HashRingTest::MaxMinRatio() for some empirical results
// at similar replication levels. There is nothing special about 25 (i.e. 24 or 26
// would be similar). Increasing this results in a more even distribution.
// TODO: This can be tuned further with real world tests
static const uint32_t NUM_HASH_RING_REPLICAS = 25;

ExecutorGroup::ExecutorGroup(string name) : ExecutorGroup(name, 1) {}

ExecutorGroup::ExecutorGroup(string name, int64_t min_size)
  : name_(name), min_size_(min_size), executor_ip_hash_ring_(NUM_HASH_RING_REPLICAS),
    per_executor_admit_mem_limit_(0) {
  DCHECK_GT(min_size_, 0);
}

ExecutorGroup::ExecutorGroup(const ExecutorGroupDescPB& desc)
  : ExecutorGroup(desc.name(), desc.min_size()) {}

ExecutorGroup* ExecutorGroup::GetFilteredExecutorGroup(const ExecutorGroup* group,
    const std::unordered_set<NetworkAddressPB>& blacklisted_executor_addresses) {
  ExecutorGroup* filtered_group = new ExecutorGroup(*group);
  for (const NetworkAddressPB& be_address : blacklisted_executor_addresses) {
    const BackendDescriptorPB* be_desc = filtered_group->LookUpBackendDesc(be_address);
    if (be_desc != nullptr) {
      filtered_group->RemoveExecutor(BackendDescriptorPB(*be_desc));
    }
  }
  return filtered_group;
}

const ExecutorGroup::Executors& ExecutorGroup::GetExecutorsForHost(
    const IpAddr& ip) const {
  ExecutorMap::const_iterator it = executor_map_.find(ip);
  DCHECK(it != executor_map_.end());
  return it->second;
}

ExecutorGroup::IpAddrs ExecutorGroup::GetAllExecutorIps() const {
  IpAddrs ips;
  ips.reserve(NumHosts());
  for (auto& it: executor_map_) ips.push_back(it.first);
  return ips;
}

ExecutorGroup::Executors ExecutorGroup::GetAllExecutorDescriptors() const {
  Executors executors;
  for (const auto& executor_list: executor_map_) {
    executors.insert(executors.end(), executor_list.second.begin(),
        executor_list.second.end());
  }
  return executors;
}

void ExecutorGroup::AddExecutor(const BackendDescriptorPB& be_desc) {
  // be_desc.is_executor can be false for the local backend when scheduling queries to run
  // on the coordinator host.
  DCHECK(!be_desc.ip_address().empty());
  Executors& be_descs = executor_map_[be_desc.ip_address()];
  auto eq = [&be_desc](const BackendDescriptorPB& existing) {
    // The IP addresses must already match, so it is sufficient to check the port.
    DCHECK_EQ(existing.ip_address(), be_desc.ip_address());
    return existing.address().port() == be_desc.address().port();
  };
  if (find_if(be_descs.begin(), be_descs.end(), eq) != be_descs.end()) {
    LOG(DFATAL) << "Tried to add existing backend to executor group: "
                << be_desc.krpc_address();
    return;
  }
  if (!CheckConsistencyOrWarn(be_desc)) {
    LOG(WARNING) << "Ignoring inconsistent backend for executor group: "
                 << be_desc.krpc_address();
    return;
  }
  if (be_descs.empty()) {
    executor_ip_hash_ring_.AddNode(be_desc.ip_address());
  }
  be_descs.push_back(be_desc);

  // When computing ScanRange assignment, if there are multiple backends on a host, a
  // round-robin approach is taken. That is, which backend a ScanRange assigned to the
  // host will eventually be assigned to depends on the order of these backends in the
  // vector, and the corresponding code is located in
  // Scheduler::AssignmentCtx::SelectExecutorOnHost(). Since backend's remote data cache
  // have data dump-load ability, so it is better to keep the order of backends before and
  // after restarting consistent (here using port sorting), to ensure that ScanRange
  // assignments do not change after certain backends or even entire clusters restart, to
  // improve data cache hit rate.
  auto cmp = [](const BackendDescriptorPB& a, const BackendDescriptorPB& b) {
    return a.address().port() < b.address().port();
  };
  std::sort(be_descs.begin(), be_descs.end(), cmp);

  executor_ip_map_[be_desc.address().hostname()] = be_desc.ip_address();

  DCHECK(be_desc.admit_mem_limit() > 0) << "Admit memory limit must be set for backends";
  if (per_executor_admit_mem_limit_ > 0) {
    per_executor_admit_mem_limit_ =
        std::min(be_desc.admit_mem_limit(), per_executor_admit_mem_limit_);
  } else if (per_executor_admit_mem_limit_ == 0) {
    per_executor_admit_mem_limit_ = be_desc.admit_mem_limit();
  }

  if (be_desc.ip_address() == "127.0.0.1") {
    // Include localhost as an alias for filesystems that don't translate it.
    LOG(INFO) << "Adding executor localhost alias for "
              << be_desc.address().hostname() << " -> " << be_desc.ip_address();
    executor_ip_map_["localhost"] = be_desc.ip_address();
  }
}

void ExecutorGroup::RemoveExecutor(const BackendDescriptorPB& be_desc) {
  auto be_descs_it = executor_map_.find(be_desc.ip_address());
  if (be_descs_it == executor_map_.end()) {
    LOG(DFATAL) << "Tried to remove a backend from non-existing host: "
                << be_desc.krpc_address();
    return;
  }
  auto eq = [&be_desc](const BackendDescriptorPB& existing) {
    // The IP addresses must already match, so it is sufficient to check the port.
    DCHECK_EQ(existing.ip_address(), be_desc.ip_address());
    return existing.address().port() == be_desc.address().port();
  };

  Executors& be_descs = be_descs_it->second;
  auto remove_it = find_if(be_descs.begin(), be_descs.end(), eq);
  if (remove_it == be_descs.end()) {
    LOG(DFATAL) << "Tried to remove non-existing backend from per-host list: "
                << be_desc.krpc_address();
    return;
  }
  be_descs.erase(remove_it);
  if (per_executor_admit_mem_limit_ == be_desc.admit_mem_limit()) {
    CalculatePerExecutorMemLimitForAdmission();
  }
  if (be_descs.empty()) {
    executor_map_.erase(be_descs_it);
    executor_ip_map_.erase(be_desc.address().hostname());
    executor_ip_hash_ring_.RemoveNode(be_desc.ip_address());
  }
}

bool ExecutorGroup::LookUpExecutorIp(const Hostname& hostname, IpAddr* ip) const {
  // Check if hostname is already a valid IP address.
  if (executor_map_.find(hostname) != executor_map_.end()) {
    if (ip != nullptr) *ip = hostname;
    return true;
  }
  auto it = executor_ip_map_.find(hostname);
  if (it != executor_ip_map_.end()) {
    if (ip != nullptr) *ip = it->second;
    return true;
  }
  return false;
}

const BackendDescriptorPB* ExecutorGroup::LookUpBackendDesc(
    const NetworkAddressPB& host) const {
  IpAddr ip;
  if (LookUpExecutorIp(host.hostname(), &ip)) {
    const ExecutorGroup::Executors& be_list = GetExecutorsForHost(ip);
    for (const BackendDescriptorPB& desc : be_list) {
      if (desc.address() == host) return &desc;
    }
  }
  return nullptr;
}

int ExecutorGroup::NumExecutors() const {
  int count = 0;
  for (const auto& executor_list : executor_map_) count += executor_list.second.size();
  return count;
}

bool ExecutorGroup::IsHealthy() const {
  int num_executors = NumExecutors();
  if (num_executors < min_size_) {
    LOG(WARNING) << "Executor group " << name_ << " is unhealthy: " << num_executors
                 << " out of " << min_size_ << " are available.";
    return false;
  }
  return true;
}

bool ExecutorGroup::CheckConsistencyOrWarn(const BackendDescriptorPB& be_desc) const {
  // Check if the executor's group configuration matches this group.
  for (const ExecutorGroupDescPB& desc : be_desc.executor_groups()) {
    if (desc.name() == name_) {
      if (desc.min_size() == min_size_) {
        return true;
      } else {
        LOG(WARNING) << "Backend " << be_desc.DebugString()
                     << " is configured for executor group " << desc.DebugString()
                     << " but group has minimum size " << min_size_;
        return false;
      }
    }
  }
  // If the backend does not mention the group we consider it consistent to allow backends
  // to be added to unrelated groups, e.g. for the coordinator-only scheuduling.
  return true;
}

void ExecutorGroup::CalculatePerExecutorMemLimitForAdmission() {
  per_executor_admit_mem_limit_ = numeric_limits<int64_t>::max();
  int num_executors = 0;
  for (const auto& executor_list: executor_map_) {
    const Executors& be_descs = executor_list.second;
    num_executors += be_descs.size();
    for (const auto& be_desc: be_descs) {
      per_executor_admit_mem_limit_ = std::min(per_executor_admit_mem_limit_,
          be_desc.admit_mem_limit());
    }
  }
  if (num_executors == 0) {
    per_executor_admit_mem_limit_ = 0;
  }
}

}  // end ns impala
