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

#ifndef SCHEDULING_BACKEND_CONFIG_H
#define SCHEDULING_BACKEND_CONFIG_H

#include <vector>

#include <boost/unordered_map.hpp>

#include "gen-cpp/StatestoreService_types.h"
#include "gen-cpp/Types_types.h"
#include "scheduling/hash-ring.h"
#include "util/network-util.h"

namespace impala {

/// Configuration class to store a list of backends per IP address, a mapping from
/// hostnames to IP addresses, and a hash ring containing all backends.
class BackendConfig {
 public:
  BackendConfig();

  /// Construct from list of backends.
  BackendConfig(const std::vector<TNetworkAddress>& backends);

  /// List of Backends.
  typedef std::list<TBackendDescriptor> BackendList;

  /// Return the list of backends on a particular host. The caller must make sure that the
  /// host is actually contained in backend_map_.
  const BackendList& GetBackendListForHost(const IpAddr& ip) const;

  void GetAllBackendIps(std::vector<IpAddr>* ip_addresses) const;
  void GetAllBackends(BackendList* backends) const;
  void AddBackend(const TBackendDescriptor& be_desc);
  void RemoveBackend(const TBackendDescriptor& be_desc);

  /// Look up the IP address of 'hostname' in the internal backend maps and return
  /// whether the lookup was successful. If 'hostname' itself is a valid IP address and
  /// is contained in backend_map_, then it is copied to 'ip' and true is returned. 'ip'
  /// can be nullptr if the caller only wants to check whether the lookup succeeds. Use
  /// this method to resolve datanode hostnames to IP addresses during scheduling, to
  /// prevent blocking on the OS.
  bool LookUpBackendIp(const Hostname& hostname, IpAddr* ip) const;

  /// Look up the backend descriptor for the backend with hostname 'host'.
  /// Returns nullptr if it's not found. The returned descriptor should not
  /// be retained beyond the lifetime of this BackendConfig.
  const TBackendDescriptor* LookUpBackendDesc(const TNetworkAddress& host) const;

  const HashRing* GetHashRing() const { return &backend_ip_hash_ring_; }

  int NumBackends() const { return backend_map_.size(); }

 private:
  /// Map from a host's IP address to a list of backends running on that node.
  typedef boost::unordered_map<IpAddr, BackendList> BackendMap;
  BackendMap backend_map_;

  /// Map from a hostname to its IP address to support hostname based backend lookup. It
  /// contains entries for all backends in backend_map_ and needs to be updated whenever
  /// backend_map_ changes.
  typedef boost::unordered_map<Hostname, IpAddr> BackendIpAddressMap;
  BackendIpAddressMap backend_ip_map_;

  /// All backends are kept in a hash ring to allow a consistent mapping from filenames
  /// to backends.
  HashRing backend_ip_hash_ring_;
};

}  // end ns impala
#endif
