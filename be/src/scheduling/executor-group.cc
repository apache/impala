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
  : name_(name), min_size_(min_size), executor_ip_hash_ring_(NUM_HASH_RING_REPLICAS) {
    DCHECK_GT(min_size_, 0);
  }

ExecutorGroup::ExecutorGroup(const TExecutorGroupDesc& desc)
  : ExecutorGroup(desc.name, desc.min_size) {}

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

void ExecutorGroup::AddExecutor(const TBackendDescriptor& be_desc) {
  // be_desc.is_executor can be false for the local backend when scheduling queries to run
  // on the coordinator host.
  DCHECK(!be_desc.ip_address.empty());
  Executors& be_descs = executor_map_[be_desc.ip_address];
  auto eq = [&be_desc](const TBackendDescriptor& existing) {
    // The IP addresses must already match, so it is sufficient to check the port.
    DCHECK_EQ(existing.ip_address, be_desc.ip_address);
    return existing.address.port == be_desc.address.port;
  };
  if (find_if(be_descs.begin(), be_descs.end(), eq) != be_descs.end()) {
    LOG(DFATAL) << "Tried to add existing backend to executor group: "
        << be_desc.krpc_address;
    return;
  }
  if (!CheckConsistencyOrWarn(be_desc)) {
    LOG(WARNING) << "Ignoring inconsistent backend for executor group: "
                 << be_desc.krpc_address;
    return;
  }
  if (be_descs.empty()) {
    executor_ip_hash_ring_.AddNode(be_desc.ip_address);
  }
  be_descs.push_back(be_desc);
  executor_ip_map_[be_desc.address.hostname] = be_desc.ip_address;
}

void ExecutorGroup::RemoveExecutor(const TBackendDescriptor& be_desc) {
  auto be_descs_it = executor_map_.find(be_desc.ip_address);
  if (be_descs_it == executor_map_.end()) {
    LOG(DFATAL) << "Tried to remove a backend from non-existing host: "
        << be_desc.krpc_address;
    return;
  }
  auto eq = [&be_desc](const TBackendDescriptor& existing) {
    // The IP addresses must already match, so it is sufficient to check the port.
    DCHECK_EQ(existing.ip_address, be_desc.ip_address);
    return existing.address.port == be_desc.address.port;
  };

  Executors& be_descs = be_descs_it->second;
  auto remove_it = find_if(be_descs.begin(), be_descs.end(), eq);
  if (remove_it == be_descs.end()) {
    LOG(DFATAL) << "Tried to remove non-existing backend from per-host list: "
        << be_desc.krpc_address;
    return;
  }
  be_descs.erase(remove_it);
  if (be_descs.empty()) {
    executor_map_.erase(be_descs_it);
    executor_ip_map_.erase(be_desc.address.hostname);
    executor_ip_hash_ring_.RemoveNode(be_desc.ip_address);
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

const TBackendDescriptor* ExecutorGroup::LookUpBackendDesc(
    const TNetworkAddress& host) const {
  IpAddr ip;
  if (LookUpExecutorIp(host.hostname, &ip)) {
    const ExecutorGroup::Executors& be_list = GetExecutorsForHost(ip);
    for (const TBackendDescriptor& desc : be_list) {
      if (desc.address == host) return &desc;
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

bool ExecutorGroup::CheckConsistencyOrWarn(const TBackendDescriptor& be_desc) const {
  // Check if the executor's group configuration matches this group.
  for (const TExecutorGroupDesc& desc : be_desc.executor_groups) {
    if (desc.name == name_) {
      if (desc.min_size == min_size_) {
        return true;
      } else {
        LOG(WARNING) << "Backend " << be_desc << " is configured for executor group "
                     << desc << " but group has minimum size " << min_size_;
        return false;
      }
    }
  }
  // If the backend does not mention the group we consider it consistent to allow backends
  // to be added to unrelated groups, e.g. for the coordinator-only scheuduling.
  return true;
}

}  // end ns impala
