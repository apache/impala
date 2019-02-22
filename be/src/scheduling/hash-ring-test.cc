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

#include <map>
#include <math.h>
#include <stdio.h>
#include <iostream>

#include "scheduling/hash-ring.h"
#include "testutil/gtest-util.h"
#include "thirdparty/pcg-cpp-0.98/include/pcg_random.hpp"
#include "util/network-util.h"
#include "gen-cpp/Types_types.h"
#include "common/names.h"

namespace impala {

class HashRingTest : public ::testing::Test {
 protected:
  void GetDistributionMap(const HashRing& hash_ring,
      std::map<IpAddr, uint64_t>* distribution_map) {
    return hash_ring.GetDistributionMap(distribution_map);
  }

  // Verify the specified hash ring has the appropriate number of nodes and replicas.
  // This assumes no collisions. If there are collisions, the total replicas will be
  // smaller than expected.
  void VerifyCounts(const HashRing& hash_ring, uint32_t num_nodes) {
    EXPECT_EQ(hash_ring.GetNumNodes(), num_nodes);
    EXPECT_EQ(hash_ring.GetTotalReplicas(), num_nodes * hash_ring.GetNumReplicas());
  }

  // Verify that the allocations that GetDistributionMap() returns for each node
  // sum to the size of the hash space (UINT_MAX + 1).
  void VerifyTotalAllocation(const vector<IpAddr>& addresses,
      const uint32_t replication) {
    HashRing hash_ring(replication);
    for (const IpAddr& addr : addresses) hash_ring.AddNode(addr);
    std::map<IpAddr, uint64_t> dist_map;
    hash_ring.GetDistributionMap(&dist_map);
    uint64_t total_allocation = 0;
    uint64_t total_uint32_range = static_cast<uint64_t>(UINT_MAX) + 1;
    for (auto dist_map_it : dist_map) {
      total_allocation += dist_map_it.second;
    }
    EXPECT_EQ(total_allocation, total_uint32_range);
  }

  // Verify that ratio of the maximum allocation to the minimum allocation is below
  // the 'expected_ratio_limit'.
  void VerifyMaxMinRatio(const vector<IpAddr>& addresses,
      const uint32_t replication, double expected_ratio_limit) {
    HashRing hash_ring(replication);
    for (const IpAddr& addr : addresses) hash_ring.AddNode(addr);
    std::map<IpAddr, uint64_t> dist_map;
    hash_ring.GetDistributionMap(&dist_map);
    uint64_t min = static_cast<uint64_t>(UINT_MAX) + 1;
    uint64_t max = 0;
    for (auto it = dist_map.begin(); it != dist_map.end(); it++) {
      uint64_t range = it->second;
      if (range < min) min = range;
      if (range > max) max = range;
    }
    double max_min_ratio = (double) max / (double) min;
    LOG(INFO) << "VerifyMaxMinRatio: num_nodes: " << addresses.size()
              << " replication: " << replication << " max/min ratio: " << max_min_ratio
              << " expected limit: " << expected_ratio_limit;
    EXPECT_LT(max_min_ratio, expected_ratio_limit);
  }

  void GetBasicNetworkAddresses(vector<IpAddr>& addresses) {
    // Collection of three addresses
    addresses.push_back("hostname1");
    addresses.push_back("hostname2");
    addresses.push_back("hostname3");
  }

  void GetMultipleNetworkAddresses(string basename, uint32_t num_nodes,
      vector<IpAddr>& addresses) {
    for (uint32_t i = 0; i < num_nodes; i++) {
      addresses.push_back(basename + std::to_string(i));
    }
  }
};

TEST_F(HashRingTest, BasicAddRemove) {
  vector<IpAddr> basic_addresses;
  GetBasicNetworkAddresses(basic_addresses);

  const uint32_t replication = 10;
  HashRing h(replication);
  VerifyCounts(h, 0);

  int total_addresses = basic_addresses.size();
  for (int i = 0; i < total_addresses; i++) {
    h.AddNode(basic_addresses[i]);
    VerifyCounts(h, i + 1);
  }

  // Remove the elements in a different order
  std::random_shuffle(basic_addresses.begin(), basic_addresses.end());
  for (int i = 0; i < total_addresses; i++) {
    h.RemoveNode(basic_addresses[i]);
    VerifyCounts(h, total_addresses - (i + 1));
  }
}

TEST_F(HashRingTest, GetNode) {
  vector<IpAddr> basic_addresses;
  GetBasicNetworkAddresses(basic_addresses);

  const uint32_t replication = 10;
  HashRing h(replication);
  // Test null return when empty
  EXPECT_EQ(h.GetNode(0), nullptr);

  for (const IpAddr& addr : basic_addresses) h.AddNode(addr);

  // Test wraparound
  // (Note that this assumes that nothing mapped to exactly UINT_MAX.)
  const IpAddr* first_node = h.GetNode(0);
  const IpAddr* wrapped_node = h.GetNode(UINT_MAX);
  EXPECT_EQ(first_node, wrapped_node);

  // Pick 100 random addresses and verify that they are all non-null and exist in
  // the input addresses. This is not deterministic.
  pcg32 prng(time(nullptr));
  for (int i = 0; i < 100; i++) {
    const IpAddr* getnode_output = h.GetNode(prng());
    DCHECK(getnode_output != nullptr);
    auto addr_it =
        std::find(basic_addresses.begin(), basic_addresses.end(), *getnode_output);
    DCHECK(addr_it != basic_addresses.end());
  }
}

TEST_F(HashRingTest, CopyConstructor) {
  vector<IpAddr> basic_addresses;
  GetBasicNetworkAddresses(basic_addresses);

  // Make a basic HashRing with a few elements
  const uint32_t replication = 10;
  unique_ptr<HashRing> old_h(new HashRing(replication));
  for (const IpAddr& addr : basic_addresses) old_h->AddNode(addr);
  std::map<IpAddr, uint64_t> old_dist_map;
  GetDistributionMap(*old_h, &old_dist_map);

  // Use copy constructor
  HashRing new_h(*old_h);

  // Destroy old HashRing
  old_h.reset();

  // Verify that the new HashRing is the same
  VerifyCounts(new_h, basic_addresses.size());
  std::map<IpAddr, uint64_t> new_dist_map;
  // Getting the distribution visits every element, so it is an effective check for
  // use-after-free, etc.
  GetDistributionMap(new_h, &new_dist_map);

  // The distribution should be identical
  EXPECT_EQ(new_dist_map, old_dist_map);
}

TEST_F(HashRingTest, TotalAllocation) {
  const uint32_t replication = 10;

  // Test total allocation for basic addresses
  vector<IpAddr> basic_addresses;
  GetBasicNetworkAddresses(basic_addresses);
  VerifyTotalAllocation(basic_addresses, replication);

  // Test total allocation for 100 addresses
  vector<IpAddr> multiple_addresses;
  GetMultipleNetworkAddresses("total_alloc_host", 100, multiple_addresses);
  VerifyTotalAllocation(multiple_addresses, replication);
}

TEST_F(HashRingTest, Collisions) {
  // Collisions should be rare, even with a relatively large number of nodes.
  // If there are collisions, the counts will not match up, because a node will not have
  // the specified replication. This verifies that that there are no collisions on this
  // case. Clearly, there are cases where there will be collisions and this could fail
  // innocuously if the algorithm changes, but this is a good canary in the coalmine to
  // catch changes that cause abnormal collisions.
  const uint32_t replication = 25;
  vector<IpAddr> multiple_addresses;
  GetMultipleNetworkAddresses("collision_host", 1000, multiple_addresses);
  HashRing h(replication);
  for (const IpAddr& addr : multiple_addresses) h.AddNode(addr);
  VerifyCounts(h, 1000);
}

TEST_F(HashRingTest, MaxMinRatio) {
  // The ratio of the maximum node to the minimum node should be bounded.
  // This is purely a functional question. It makes no assumption about the underlying
  // statistics. If this ratio regresses, it could impact scheduling.
  const uint32_t replication = 25;
  vector<uint32_t> node_counts = {10, 25, 100};
  vector<double> max_expected_ratios = {3.0, 3.0, 4.5};
  DCHECK_EQ(node_counts.size(), max_expected_ratios.size());
  // Anecdotally, the base hostname has a noticeable (but bounded) impact on the ratio.
  // Test with multiple base hostnames.
  vector<string> base_hostnames = {"minmaxhost", "qwert", "asdfjkl", "2a8b67", "91htgh"};
  for (int i = 0; i < node_counts.size(); i++) {
    for (const string& base_hostname : base_hostnames) {
      vector<IpAddr> addresses;
      GetMultipleNetworkAddresses(base_hostname, node_counts[i], addresses);
      VerifyMaxMinRatio(addresses, replication, max_expected_ratios[i]);
    }
  }
}

}
