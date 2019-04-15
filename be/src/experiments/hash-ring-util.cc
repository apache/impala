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

#include <cmath>
#include <stdlib.h>
#include <stdio.h>
#include <iostream>

#include "common/init.h"
#include "scheduling/hash-ring.h"
#include "scheduling/cluster-membership-test-util.h"
#include "scheduling/scheduler-test-util.h"
#include "testutil/gtest-util.h"
#include "util/network-util.h"
#include "util/test-info.h"
#include "util/time.h"

#include "common/names.h"

DEFINE_int32(num_hosts, 10, "Number of hosts for simulation");
DEFINE_int32(num_replicas, 10, "Replication factor for hashring");

namespace impala {

// Utility to test the HashRing's performance. It is important to understand how well the
// HashRing distributes nodes throughout the hash space. Specifically, it is useful to
// know how much of the hash space is mapped to each node and how much variation there is
// between the nodes. This builds Cluster with appropriate number of hosts, then it adds
// them into the HashRing. It extracts information about the distribution and prints it.

class HashRingDistributionCheck {
public:
  // Given a number of hosts and number of replicas, generate the hashring and calculate
  // various measurements of the distribution of the nodes across the hash space.
  static void TestDistribution(int num_hosts, int num_replicas) {
    int64_t start_nanos = MonotonicNanos();
    cout << "Running TestDistribution with num_hosts=" << std::to_string(num_hosts)
         << " num_replicas=" << std::to_string(num_replicas) << endl;
    test::Cluster cluster;
    cluster.AddHosts(num_hosts, true, false);
    HashRing hashring(num_replicas);
    for (int host_idx = 0; host_idx < num_hosts; host_idx++) {
      IpAddr node = test::HostIdxToIpAddr(host_idx);
      hashring.AddNode(node);
    }
    int64_t end_nanos = MonotonicNanos();
    map<IpAddr, uint64_t> distribution;
    hashring.GetDistributionMap(&distribution);

    uint64_t total_uint32_range = static_cast<uint64_t>(UINT_MAX) + 1;
    // By definition, the ranges add up to UINT_MAX+1, so the average is
    // UINT_MAX+1/num_hosts
    double average = ((double) total_uint32_range / (double) num_hosts);
    uint64_t min = total_uint32_range;
    uint64_t max = 0;
    uint64_t total = 0;
    double sum_of_diff_squared = 0.0;
    for (auto it = distribution.begin(); it != distribution.end(); it++) {
      uint64_t range = it->second;
      DCHECK_LE(range, total_uint32_range);
      if (range < min) min = range;
      if (range > max) max = range;
      total += range;
      double diff_squared = pow(((double)range - average), 2);
      sum_of_diff_squared += diff_squared;
      double range_pct = (100.00 * (double) range) / (double) UINT_MAX;
      cout << "Host: " << it->first << " Range: " << std::to_string(range_pct) << endl;
    }
    cout << "Min Range: " << min << endl;
    cout << "Max Range: " << max << endl;
    cout << "Max/Min Ratio: " << std::to_string((double) max / (double) min) << endl;
    // The distribution should sum to UINT_MAX+1
    DCHECK_EQ(total, total_uint32_range);
    double stddev = sqrt(sum_of_diff_squared / ((double) num_hosts - 1));
    cout << "StdDev: " << std::to_string(stddev) << endl;
    cout << "Time nanos: " << std::to_string(end_nanos - start_nanos) << endl;
  }
};

}

int main(int argc, char ** argv) {
  google::ParseCommandLineFlags(&argc, &argv, true);
  impala::InitCommonRuntime(argc, argv, true, impala::TestInfo::BE_TEST);
  impala::HashRingDistributionCheck::TestDistribution(FLAGS_num_hosts,
      FLAGS_num_replicas);
  return 0;
}
