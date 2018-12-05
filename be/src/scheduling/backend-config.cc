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

#include "scheduling/backend-config.h"

namespace impala{

// Hand-testing shows that 25 replicas produces a reasonable balance between nodes
// across the hash ring. See HashRingTest::MaxMinRatio() for some empirical results
// at similar replication levels. There is nothing special about 25 (i.e. 24 or 26
// would be similar). Increasing this results in a more even distribution.
// TODO: This can be tuned further with real world tests
static const uint32_t NUM_HASH_RING_REPLICAS = 25;

BackendConfig::BackendConfig()
  : backend_ip_hash_ring_(NUM_HASH_RING_REPLICAS) {}

BackendConfig::BackendConfig(const std::vector<TNetworkAddress>& backends)
  : backend_ip_hash_ring_(NUM_HASH_RING_REPLICAS) {
  // Construct backend_map and backend_ip_map.
  for (const TNetworkAddress& backend: backends) {
    IpAddr ip;
    Status status = HostnameToIpAddr(backend.hostname, &ip);
    if (!status.ok()) {
      VLOG(1) << status.GetDetail();
      continue;
    }
    AddBackend(MakeBackendDescriptor(backend.hostname, ip, backend.port));
  }
}

const BackendConfig::BackendList& BackendConfig::GetBackendListForHost(
    const IpAddr& ip) const {
  BackendMap::const_iterator it = backend_map_.find(ip);
  DCHECK(it != backend_map_.end());
  return it->second;
}

void BackendConfig::GetAllBackendIps(std::vector<IpAddr>* ip_addresses) const {
  ip_addresses->reserve(NumBackends());
  for (auto& it: backend_map_) ip_addresses->push_back(it.first);
}

void BackendConfig::GetAllBackends(BackendList* backends) const {
  for (const auto& backend_list: backend_map_) {
    backends->insert(backends->end(), backend_list.second.begin(),
        backend_list.second.end());
  }
}

void BackendConfig::AddBackend(const TBackendDescriptor& be_desc) {
  DCHECK(!be_desc.ip_address.empty());
  BackendList& be_descs = backend_map_[be_desc.ip_address];
  if (be_descs.empty()) {
    backend_ip_hash_ring_.AddNode(be_desc.ip_address);
  }
  if (find(be_descs.begin(), be_descs.end(), be_desc) == be_descs.end()) {
    be_descs.push_back(be_desc);
  }
  backend_ip_map_[be_desc.address.hostname] = be_desc.ip_address;
}

void BackendConfig::RemoveBackend(const TBackendDescriptor& be_desc) {
  auto be_descs_it = backend_map_.find(be_desc.ip_address);
  if (be_descs_it != backend_map_.end()) {
    BackendList* be_descs = &be_descs_it->second;
    be_descs->erase(remove(be_descs->begin(), be_descs->end(), be_desc), be_descs->end());
    if (be_descs->empty()) {
      backend_map_.erase(be_descs_it);
      backend_ip_map_.erase(be_desc.address.hostname);
      backend_ip_hash_ring_.RemoveNode(be_desc.ip_address);
    }
  }
}

bool BackendConfig::LookUpBackendIp(const Hostname& hostname, IpAddr* ip) const {
  // Check if hostname is already a valid IP address.
  if (backend_map_.find(hostname) != backend_map_.end()) {
    if (ip != nullptr) *ip = hostname;
    return true;
  }
  auto it = backend_ip_map_.find(hostname);
  if (it != backend_ip_map_.end()) {
    if (ip != nullptr) *ip = it->second;
    return true;
  }
  return false;
}

const TBackendDescriptor* BackendConfig::LookUpBackendDesc(
    const TNetworkAddress& host) const {
  IpAddr ip;
  if (LIKELY(LookUpBackendIp(host.hostname, &ip))) {
    const BackendConfig::BackendList& be_list = GetBackendListForHost(ip);
    for (const TBackendDescriptor& desc : be_list) {
      if (desc.address == host) return &desc;
    }
  }
  return nullptr;
}

}  // end ns impala
