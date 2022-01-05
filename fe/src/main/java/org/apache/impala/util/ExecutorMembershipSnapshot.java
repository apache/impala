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

package org.apache.impala.util;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.impala.thrift.TExecutorGroupSet;
import org.apache.impala.thrift.TNetworkAddress;
import org.apache.impala.thrift.TExecutorGroupSet;
import org.apache.impala.thrift.TUpdateExecutorMembershipRequest;
import org.apache.kudu.shaded.com.google.common.base.Preconditions;

import com.google.common.collect.Sets;

/**
 * Singleton class that represents a snapshot of the Impalad executor membership. Host
 * membership is determined by both IP address and hostname (to mimic the backend's
 * Scheduler). A new snapshot is created whenever the cluster membership changes
 * so that clients don't need to hold a lock while examining a snapshot.
 */
public class ExecutorMembershipSnapshot {
  // The latest instance of the ExecutorMembershipSnapshot.
  private static AtomicReference<ExecutorMembershipSnapshot> cluster_ =
      new AtomicReference<ExecutorMembershipSnapshot>(new ExecutorMembershipSnapshot());

  // The set of hosts that are members of the cluster given by hostname. Can be empty when
  // using executor groups, in which case the planner shall assume that all reads are
  // remote.
  private final Set<String> hostnames_;

  // The set of hosts that are members of the cluster given by IP address. Can be empty
  // when using executor groups, in which case the planner shall assume that all reads are
  // remote.
  private final Set<String> ipAddresses_;

  // The number of executor nodes of the cluster.
  //
  // When not using executor groups, this value reflects the number of executors in the
  // cluster. It will be equal to hostnames_.size(), except in the test minicluster where
  // multiple impalads are running on a single host.

  // When using executor groups, this value reflects the number of executors in the
  // largest healthy group. If all groups become unhealthy, it will be set to the
  // expected group size for that executor group set. This allows the planner to
  // work on the assumption that a healthy executor group of the same size will
  // eventually come online to execute queries.
  private final int numExecutors_;

  // Info about the expected executor group sets sorted by the expected executor
  // group size. When not using executor groups (using 'default' excutor group) or
  // when 'expected_executor_group_sets' startup flag is not specified, this will
  // contain a single entry with an empty group name prefix.
  private final List<TExecutorGroupSet> exec_group_sets_;

  // Used only to construct the initial ExecutorMembershipSnapshot.
  private ExecutorMembershipSnapshot() {
    hostnames_ = Sets.newHashSet();
    ipAddresses_ = Sets.newHashSet();
    exec_group_sets_ = new ArrayList<TExecutorGroupSet>();
    // We use 0 for the number of executors to indicate that no update from the
    // ClusterMembershipManager has arrived yet and we will return the value
    // '-num_expected_executors' in numExecutors().
    numExecutors_ = 0;
  }

  // Construct a new snapshot based on the TUpdateExecutorMembershipRequest.
  private ExecutorMembershipSnapshot(TUpdateExecutorMembershipRequest request) {
    hostnames_ = request.getHostnames();
    ipAddresses_ = request.getIp_addresses();
    exec_group_sets_ = request.getExec_group_sets();
    Preconditions.checkState(!exec_group_sets_.isEmpty(), "Atleast one executor group "
        + "set should have been specified in the membership update.");
    numExecutors_ = exec_group_sets_.get(0).curr_num_executors;
  }

  // Determine whether a host, given either by IP address or hostname, is a member of
  // this snapshot.  Returns true if it is, false otherwise.
  public boolean contains(TNetworkAddress address) {
    String host = address.getHostname();
    return ipAddresses_.contains(host) || hostnames_.contains(host);
  }

  // Return the number of executors that should be used for planning. If no executors have
  // been registered so far, this method will return a configurable default to allow the
  // planner to operated based on the expected number of executors.
  public int numExecutors() {
    if (numExecutors_ == 0 && !exec_group_sets_.isEmpty()) {
      return exec_group_sets_.get(0).expected_num_executors;
    }
    return numExecutors_;
  }

  // Atomically update the singleton snapshot instance.  After the update completes,
  // all calls to getCluster() will return the new snapshot.
  public static void update(TUpdateExecutorMembershipRequest request) {
    cluster_.set(new ExecutorMembershipSnapshot(request));
  }

  // Return the current singleton snapshot instance.
  public static ExecutorMembershipSnapshot getCluster() { return cluster_.get(); }

  public static List<TExecutorGroupSet> getAllExecutorGroupSets() {
    return cluster_.get().exec_group_sets_;
  }
}
