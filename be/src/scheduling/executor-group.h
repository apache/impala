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

#pragma once

#include <unordered_map>
#include <vector>

#include "gen-cpp/Types_types.h"
#include "gen-cpp/statestore_service.pb.h"
#include "scheduling/hash-ring.h"
#include "util/container-util.h"
#include "util/network-util.h"
#include "util/unique-id-hash.h"

namespace impala {

/// Configuration class to store a list of executor hosts, a list of backend descriptors
/// per host, a mapping from hostnames to IP addresses, and a hash ring containing all
/// backends.
///
/// The intended purpose of this class is to provide a consistent view on a subset of
/// executors during scheduling. The class is not thread safe. In particular, some of the
/// getter methods return references and the membership must not be changed while client
/// code holds those references.
///
/// Executor groups can optionally have a target size assigned to them and will be
/// considered unhealthy if less than that number of executors are added.
///
/// Note that only during tests objects of this class will store more than one backend per
/// host/IP address.
class ExecutorGroup {
 public:
  explicit ExecutorGroup(std::string name);
  explicit ExecutorGroup(std::string name, int64_t min_size);
  explicit ExecutorGroup(const ExecutorGroupDescPB& desc);
  ExecutorGroup(const ExecutorGroup& other) = default;

  /// Get an ExecutorGroup with the executors from the given ExecutorGroup, excluding
  /// the executors with address in the given set of blacklisted executor addresses.
  static ExecutorGroup* GetFilteredExecutorGroup(const ExecutorGroup* group,
      const std::unordered_set<NetworkAddressPB>& blacklisted_executor_addresses);

  /// List of backends, in this case they're all executors.
  typedef std::vector<BackendDescriptorPB> Executors;
  typedef std::vector<IpAddr> IpAddrs;

  /// Returns the list of executors on a particular host. The caller must make sure that
  /// the host is actually contained in executor_map_.
  const Executors& GetExecutorsForHost(const IpAddr& ip) const;

  /// Returns all executor IP addresses in the executor group.
  IpAddrs GetAllExecutorIps() const;

  /// Returns all executor backend descriptors. Note that during tests this can include
  /// multiple executors per IP address.
  Executors GetAllExecutorDescriptors() const;

  /// Adds an executor to the group. If it already exists, it is ignored. Backend
  /// descriptors are identified by their IP address and port. Backends that fail the
  /// consistency check (CheckConsistencyOrWarn()) are ignored. Note that executors can be
  /// added to an executor group even if they don't have a matching ExecutorGroupDescPB in
  /// their 'executor_groups' list. This is required when building the coordinator-only
  /// group during scheduling.
  void AddExecutor(const BackendDescriptorPB& be_desc);

  /// Removes an executor from the group if it exists. Otherwise does nothing. Backend
  /// descriptors are identified by their IP address and port.
  void RemoveExecutor(const BackendDescriptorPB& be_desc);

  /// Look up the IP address of 'hostname' in the internal executor maps and return
  /// whether the lookup was successful. If 'hostname' itself is a valid IP address and is
  /// contained in executor_map_, then it is copied to 'ip' and true is returned. 'ip' can
  /// be nullptr if the caller only wants to check whether the lookup succeeds. Use this
  /// method to resolve datanode hostnames to IP addresses during scheduling, to prevent
  /// blocking on the OS.
  bool LookUpExecutorIp(const Hostname& hostname, IpAddr* ip) const;

  /// Looks up the backend descriptor for the executor with hostname 'host'. Returns
  /// nullptr if it's not found. The returned descriptor should not be retained beyond the
  /// lifetime of this ExecutorGroup and the caller must make sure that the group does not
  /// change while it holds the pointer.
  const BackendDescriptorPB* LookUpBackendDesc(const NetworkAddressPB& host) const;

  /// Returns the hash ring associated with this executor group. It's owned by the group
  /// and the caller must not hold a reference beyond the groups lifetime.
  const HashRing* GetHashRing() const { return &executor_ip_hash_ring_; }

  /// Returns the number of executor hosts in this group. During tests, hosts can run
  /// multiple executor backend descriptors, but will only be counted here once.
  int NumHosts() const { return executor_map_.size(); }

  /// Returns the number of executors (backend descriptors) in this group. Multiple
  /// executors running on the same host (e.g. during tests) are counted individually.
  int NumExecutors() const;

  /// Returns true if the group is healthy, i.e. contains at least 'min_size_' executors.
  /// Returns false otherwise.
  bool IsHealthy() const;

  /// Return effective memory limit for admission for executors in this group.
  int64_t GetPerExecutorMemLimitForAdmission() const {
      return per_executor_admit_mem_limit_;
  }

  const string& name() const { return name_; }
  int64_t min_size() const { return min_size_; }

 private:
  /// Finds the first executor group in 'be_desc.executor_groups' and validates its target
  /// size. Returns true if a group is found and its min size matches, or if no group is
  /// found. Returns false and logs a warning otherwise.
  bool CheckConsistencyOrWarn(const BackendDescriptorPB& be_desc) const;

  /// Calculates minimum memory limit for admission across all backends.
  void CalculatePerExecutorMemLimitForAdmission();

  const std::string name_;

  /// The minimum number of executors in this group to be considered healthy. A group must
  /// not be empty to be considered healthy and this value must be non-zero.
  int64_t min_size_;

  /// Map from a host's IP address to a list of executors running on that node.
  typedef std::unordered_map<IpAddr, Executors> ExecutorMap;
  ExecutorMap executor_map_;

  /// Map from a hostname to its IP address to support hostname based executor lookup. It
  /// contains entries for all executors in executor_map_ and needs to be updated whenever
  /// executor_map_ changes.
  typedef std::unordered_map<Hostname, IpAddr> ExecutorIpAddressMap;
  ExecutorIpAddressMap executor_ip_map_;

  /// All executors are kept in a hash ring to allow a consistent mapping from filenames
  /// to executors.
  HashRing executor_ip_hash_ring_;

  /// The minimum memory limit in bytes for admission across all backends. Used by
  /// admission controller to bound the per backend memory limit for a given query.
  int64_t per_executor_admit_mem_limit_;
};

}  // end ns impala
