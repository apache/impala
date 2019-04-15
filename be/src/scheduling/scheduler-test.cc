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

#include "common/logging.h"
#include "scheduling/cluster-membership-mgr.h"
#include "scheduling/scheduler.h"
#include "scheduling/scheduler-test-util.h"
#include "testutil/gtest-util.h"

using namespace impala;
using namespace impala::test;

namespace impala {

class SchedulerTest : public testing::Test {
 protected:
  SchedulerTest() { srand(0); }
};

/// Smoke test to schedule a single table with a single scan range over a single host.
TEST_F(SchedulerTest, SingleHostSingleFile) {
  Cluster cluster;
  cluster.AddHost(true, true);

  Schema schema(cluster);
  schema.AddMultiBlockTable("T", 1, ReplicaPlacement::LOCAL_ONLY, 1);

  Plan plan(schema);
  plan.AddTableScan("T");

  Result result(plan);
  SchedulerWrapper scheduler(plan);
  ASSERT_OK(scheduler.Compute(&result));

  EXPECT_EQ(1, result.NumTotalAssignments());
  EXPECT_EQ(1 * Block::DEFAULT_BLOCK_SIZE, result.NumTotalAssignedBytes());
  EXPECT_EQ(1, result.NumTotalAssignments(0));
  EXPECT_EQ(1 * Block::DEFAULT_BLOCK_SIZE, result.NumTotalAssignedBytes(0));
  EXPECT_EQ(0, result.NumCachedAssignments());
}

/// Test cluster configuration with one coordinator that can't process scan ranges.
TEST_F(SchedulerTest, SingleCoordinatorNoExecutor) {
  Cluster cluster;
  cluster.AddHost(true, true, false);
  cluster.AddHost(true, true, true);
  cluster.AddHost(true, true, true);

  Schema schema(cluster);
  schema.AddMultiBlockTable("T1", 10, ReplicaPlacement::LOCAL_ONLY, 3);

  Plan plan(schema);
  plan.AddTableScan("T1");

  Result result(plan);
  SchedulerWrapper scheduler(plan);
  ASSERT_OK(scheduler.Compute(&result));

  EXPECT_EQ(2, result.NumDistinctBackends());
  EXPECT_EQ(0, result.NumDiskAssignments(0));
}

/// Test assigning all scan ranges to the coordinator.
TEST_F(SchedulerTest, ExecAtCoord) {
  Cluster cluster;
  cluster.AddHosts(3, true, true);

  Schema schema(cluster);
  schema.AddMultiBlockTable("T", 3, ReplicaPlacement::LOCAL_ONLY, 3);

  Plan plan(schema);
  plan.AddTableScan("T");

  Result result(plan);
  SchedulerWrapper scheduler(plan);
  bool exec_at_coord = true;
  ASSERT_OK(scheduler.Compute(exec_at_coord, &result));

  EXPECT_EQ(3 * Block::DEFAULT_BLOCK_SIZE, result.NumTotalAssignedBytes(0));
  EXPECT_EQ(0, result.NumTotalAssignedBytes(1));
  EXPECT_EQ(0, result.NumTotalAssignedBytes(2));
}

/// Test scanning a simple table twice.
TEST_F(SchedulerTest, ScanTableTwice) {
  Cluster cluster;
  cluster.AddHosts(3, true, true);

  Schema schema(cluster);
  schema.AddMultiBlockTable("T", 2, ReplicaPlacement::LOCAL_ONLY, 3);

  Plan plan(schema);
  plan.AddTableScan("T");
  plan.AddTableScan("T");

  Result result(plan);
  SchedulerWrapper scheduler(plan);
  ASSERT_OK(scheduler.Compute(&result));

  EXPECT_EQ(4 * Block::DEFAULT_BLOCK_SIZE, result.NumTotalAssignedBytes());
  EXPECT_EQ(4 * Block::DEFAULT_BLOCK_SIZE, result.NumDiskAssignedBytes());
  EXPECT_EQ(0, result.NumCachedAssignedBytes());
}

/// TODO: This test can be removed once we have the non-random backend round-robin by
/// rank.
/// Schedule randomly over 3 backends and ensure that each backend is at least used once.
TEST_F(SchedulerTest, LocalReadRandomReplica) {
  Cluster cluster;
  cluster.AddHosts(3, true, true);

  Schema schema(cluster);
  schema.AddSingleBlockTable("T1", {0, 1, 2});

  Plan plan(schema);
  plan.AddTableScan("T1");
  plan.SetRandomReplica(true);

  Result result(plan);
  SchedulerWrapper scheduler(plan);
  for (int i = 0; i < 100; ++i) ASSERT_OK(scheduler.Compute(&result));

  ASSERT_EQ(100, result.NumAssignments());
  EXPECT_EQ(100, result.NumTotalAssignments());
  EXPECT_EQ(100 * Block::DEFAULT_BLOCK_SIZE, result.NumTotalAssignedBytes());
  EXPECT_EQ(3, result.NumDistinctBackends());
  EXPECT_GE(result.MinNumAssignedBytesPerHost(), Block::DEFAULT_BLOCK_SIZE);
}

/// Distribute a table over the first 3 nodes in the cluster and verify that repeated
/// schedules always pick the first replica (random_replica = false).
TEST_F(SchedulerTest, LocalReadsPickFirstReplica) {
  Cluster cluster;
  for (int i = 0; i < 10; ++i) cluster.AddHost(i < 5, true);

  Schema schema(cluster);
  schema.AddSingleBlockTable("T1", {0, 1, 2});

  Plan plan(schema);
  plan.AddTableScan("T1");
  plan.SetRandomReplica(false);

  Result result(plan);
  SchedulerWrapper scheduler(plan);
  for (int i = 0; i < 3; ++i) ASSERT_OK(scheduler.Compute(&result));

  EXPECT_EQ(3, result.NumTotalAssignments());
  EXPECT_EQ(3, result.NumDiskAssignments(0));
  EXPECT_EQ(0, result.NumDiskAssignments(1));
  EXPECT_EQ(0, result.NumDiskAssignments(2));
}

/// Create a medium sized cluster with 100 nodes and compute a schedule over 3 tables.
TEST_F(SchedulerTest, TestMediumSizedCluster) {
  Cluster cluster;
  cluster.AddHosts(100, true, true);

  Schema schema(cluster);
  schema.AddMultiBlockTable("T1", 10, ReplicaPlacement::LOCAL_ONLY, 3);
  schema.AddMultiBlockTable("T2", 5, ReplicaPlacement::LOCAL_ONLY, 3);
  schema.AddMultiBlockTable("T3", 1, ReplicaPlacement::LOCAL_ONLY, 3);

  Plan plan(schema);
  plan.AddTableScan("T1");
  plan.AddTableScan("T2");
  plan.AddTableScan("T3");

  Result result(plan);
  SchedulerWrapper scheduler(plan);
  ASSERT_OK(scheduler.Compute(&result));

  EXPECT_EQ(16, result.NumTotalAssignments());
  EXPECT_EQ(16, result.NumDiskAssignments());
}

/// Verify that remote placement and scheduling work as expected when
/// num_remote_executor_candidates=0. (i.e. that it is random and covers all nodes).
TEST_F(SchedulerTest, NoRemoteExecutorCandidates) {
  Cluster cluster;
  for (int i = 0; i < 100; ++i) cluster.AddHost(i < 30, true);

  Schema schema(cluster);
  schema.AddMultiBlockTable("T1", 10, ReplicaPlacement::REMOTE_ONLY, 3);

  Plan plan(schema);
  plan.AddTableScan("T1");
  plan.SetNumRemoteExecutorCandidates(0);

  Result result(plan);
  SchedulerWrapper scheduler(plan);
  ASSERT_OK(scheduler.Compute(&result));

  EXPECT_EQ(10, result.NumTotalAssignments());
  EXPECT_EQ(10, result.NumRemoteAssignments());
  EXPECT_EQ(Block::DEFAULT_BLOCK_SIZE, result.MaxNumAssignedBytesPerHost());
}

/// Tests scheduling with num_remote_executor_candidates > 0. Specifically, it verifies
/// that repeated scheduling of a block happens on the appropriate number of distinct
/// nodes for varying values of num_remote_executor_candidates. This includes cases
/// where the num_remote_executor_candidates exceeds the number of Impala executors.
TEST_F(SchedulerTest, RemoteExecutorCandidates) {
  Cluster cluster;
  int num_data_nodes = 3;
  int num_impala_nodes = 5;
  // Set of datanodes
  cluster.AddHosts(num_data_nodes, false, true);
  // Set of Impala hosts
  cluster.AddHosts(num_impala_nodes, true, false);

  Schema schema(cluster);
  schema.AddSingleBlockTable("T1", {0, 1, 2});

  // Test a range of number of remote executor candidates, including cases where the
  // number of remote executor candidates exceeds the number of Impala nodes.
  for (int num_candidates = 1; num_candidates <= num_impala_nodes + 1; ++num_candidates) {
    Plan plan(schema);
    plan.AddTableScan("T1");
    plan.SetRandomReplica(true);
    plan.SetNumRemoteExecutorCandidates(num_candidates);

    Result result(plan);
    SchedulerWrapper scheduler(plan);
    for (int i = 0; i < 100; ++i) ASSERT_OK(scheduler.Compute(&result));

    ASSERT_EQ(100, result.NumAssignments());
    EXPECT_EQ(100, result.NumTotalAssignments());
    EXPECT_EQ(100 * Block::DEFAULT_BLOCK_SIZE, result.NumTotalAssignedBytes());
    if (num_candidates < num_impala_nodes) {
      EXPECT_EQ(num_candidates, result.NumDistinctBackends());
    } else {
      EXPECT_EQ(num_impala_nodes, result.NumDistinctBackends());
    }
    EXPECT_GE(result.MinNumAssignedBytesPerHost(), Block::DEFAULT_BLOCK_SIZE);
    // If there is only one remote executor candidate, then all scan ranges will be
    // assigned to one backend.
    if (num_candidates == 1) {
      EXPECT_EQ(result.MinNumAssignedBytesPerHost(), 100 * Block::DEFAULT_BLOCK_SIZE);
    }
  }
}

/// Verify basic consistency of remote executor candidates. Specifically, it schedules
/// a set of blocks, then removes an executor that did not have any blocks assigned to
/// it, and verifies that rerunning the scheduling results in the same assignments.
TEST_F(SchedulerTest, RemoteExecutorCandidateConsistency) {
  Cluster cluster;
  int num_data_nodes = 3;
  int num_impala_nodes = 50;

  // Set of Impala hosts
  cluster.AddHosts(num_impala_nodes, true, false);
  // Set of datanodes
  cluster.AddHosts(num_data_nodes, false, true);

  // Replica placement is unimportant for this test. All blocks will be on
  // all datanodes, but Impala is runnning remotely.
  Schema schema(cluster);
  schema.AddMultiBlockTable("T1", 25, ReplicaPlacement::RANDOM, 3);

  Plan plan(schema);
  plan.AddTableScan("T1");
  plan.SetRandomReplica(false);
  plan.SetNumRemoteExecutorCandidates(3);

  Result result_base(plan);
  SchedulerWrapper scheduler(plan);
  ASSERT_OK(scheduler.Compute(&result_base));
  EXPECT_EQ(25, result_base.NumTotalAssignments());
  EXPECT_EQ(25 * Block::DEFAULT_BLOCK_SIZE, result_base.NumTotalAssignedBytes());

  // There are 25 blocks and 50 Impala hosts. There will be Impala hosts without
  // any assigned bytes. Removing one of them should not change the outcome.
  Result result_empty_removed(plan);
  // Find an Impala host that was not assigned any bytes and remove it
  bool removed_one = false;
  for (int i = 0; i < num_impala_nodes; ++i) {
    if (result_base.NumTotalAssignedBytes(i) == 0) {
      scheduler.RemoveBackend(cluster.hosts()[i]);
      removed_one = true;
      break;
    }
  }
  ASSERT_TRUE(removed_one);
  // Rerun the scheduling with the node removed.
  ASSERT_OK(scheduler.Compute(&result_empty_removed));
  EXPECT_EQ(25, result_empty_removed.NumTotalAssignments());
  EXPECT_EQ(25 * Block::DEFAULT_BLOCK_SIZE, result_empty_removed.NumTotalAssignedBytes());

  // Verify that the outcome is identical.
  for (int i = 0; i < num_impala_nodes; ++i) {
    EXPECT_EQ(result_base.NumRemoteAssignedBytes(i),
              result_empty_removed.NumRemoteAssignedBytes(i));
  }
}

/// Add a table with 1000 scan ranges over 10 hosts and ensure that the right number of
/// assignments is computed.
TEST_F(SchedulerTest, ManyScanRanges) {
  Cluster cluster;
  cluster.AddHosts(10, true, true);

  Schema schema(cluster);
  schema.AddMultiBlockTable("T", 1000, ReplicaPlacement::LOCAL_ONLY, 3);

  Plan plan(schema);
  plan.AddTableScan("T");

  Result result(plan);
  SchedulerWrapper scheduler(plan);
  ASSERT_OK(scheduler.Compute(&result));

  EXPECT_EQ(1000, result.NumTotalAssignments());
  EXPECT_EQ(1000, result.NumDiskAssignments());
  // When distributing 1000 blocks with 1 replica over 10 hosts, the probability for the
  // most-picked host to end up with more than 140 blocks is smaller than 1E-3 (Chernoff
  // bound). Adding 2 additional replicas per block will make the probability even
  // smaller. This test is deterministic, so we expect a failure less often than every 1E3
  // changes to the test, not every 1E3 runs.
  EXPECT_LE(result.MaxNumAssignmentsPerHost(), 140);
  EXPECT_LE(result.MaxNumAssignedBytesPerHost(), 140 * Block::DEFAULT_BLOCK_SIZE);
}

/// Compute a schedule in a split cluster (disjoint set of backends and datanodes).
TEST_F(SchedulerTest, DisjointClusterWithRemoteReads) {
  Cluster cluster;
  for (int i = 0; i < 20; ++i) cluster.AddHost(i < 10, i >= 10);

  Schema schema(cluster);
  schema.AddMultiBlockTable("T1", 10, ReplicaPlacement::REMOTE_ONLY, 3);

  Plan plan(schema);
  plan.AddTableScan("T1");

  Result result(plan);
  SchedulerWrapper scheduler(plan);
  ASSERT_OK(scheduler.Compute(&result));

  EXPECT_EQ(10, result.NumTotalAssignments());
  EXPECT_EQ(10, result.NumRemoteAssignments());
  // Expect that the datanodes were not mistaken for backends.
  for (int i = 10; i < 20; ++i) EXPECT_EQ(0, result.NumTotalAssignments(i));
}

/// Verify that cached replicas take precedence.
TEST_F(SchedulerTest, TestCachedReadPreferred) {
  Cluster cluster;
  cluster.AddHosts(3, true, true);

  Schema schema(cluster);
  schema.AddSingleBlockTable("T1", {0, 2}, {1});

  Plan plan(schema);
  // 1 of the 3 replicas is cached.
  plan.AddTableScan("T1");

  Result result(plan);
  SchedulerWrapper scheduler(plan);
  ASSERT_OK(scheduler.Compute(&result));
  EXPECT_EQ(1 * Block::DEFAULT_BLOCK_SIZE, result.NumCachedAssignedBytes());
  EXPECT_EQ(1 * Block::DEFAULT_BLOCK_SIZE, result.NumCachedAssignedBytes(1));
  EXPECT_EQ(0, result.NumDiskAssignedBytes());
  EXPECT_EQ(0, result.NumRemoteAssignedBytes());

  // Compute additional assignments.
  for (int i = 0; i < 8; ++i) ASSERT_OK(scheduler.Compute(&result));
  EXPECT_EQ(9 * Block::DEFAULT_BLOCK_SIZE, result.NumCachedAssignedBytes());
  EXPECT_EQ(9 * Block::DEFAULT_BLOCK_SIZE, result.NumCachedAssignedBytes(1));
  EXPECT_EQ(0, result.NumDiskAssignedBytes());
  EXPECT_EQ(0, result.NumRemoteAssignedBytes());
}

/// Test sending updates to the scheduler.
TEST_F(SchedulerTest, TestSendUpdates) {
  Cluster cluster;
  // 3 hosts, only last two run backends. This allows us to remove one of the backends
  // from the scheduler and then verify that reads are assigned to the other backend.
  for (int i = 0; i < 3; ++i) cluster.AddHost(i > 0, true);

  Schema schema(cluster);
  schema.AddMultiBlockTable("T1", 1, ReplicaPlacement::REMOTE_ONLY, 1);

  Plan plan(schema);
  plan.AddTableScan("T1");
  // Test only applies when num_remote_executor_candidates=0.
  plan.SetNumRemoteExecutorCandidates(0);

  Result result(plan);
  SchedulerWrapper scheduler(plan);

  ASSERT_OK(scheduler.Compute(&result));
  // Two backends are registered, so the scheduler will pick a random one.
  EXPECT_EQ(0, result.NumTotalAssignedBytes(0));
  EXPECT_EQ(0, result.NumTotalAssignedBytes(1));
  EXPECT_EQ(1 * Block::DEFAULT_BLOCK_SIZE, result.NumTotalAssignedBytes(2));

  // Remove one host from scheduler.
  int test_host = 2;
  scheduler.RemoveBackend(cluster.hosts()[test_host]);
  result.Reset();

  ASSERT_OK(scheduler.Compute(&result));
  EXPECT_EQ(0, result.NumTotalAssignedBytes(0));
  EXPECT_EQ(1 * Block::DEFAULT_BLOCK_SIZE, result.NumTotalAssignedBytes(1));
  EXPECT_EQ(0, result.NumTotalAssignedBytes(2));

  // Re-add host to scheduler.
  scheduler.AddBackend(cluster.hosts()[test_host]);
  result.Reset();

  ASSERT_OK(scheduler.Compute(&result));
  // Two backends are registered, so the scheduler will pick a random one.
  EXPECT_EQ(0, result.NumTotalAssignedBytes(0));
  EXPECT_EQ(1 * Block::DEFAULT_BLOCK_SIZE, result.NumTotalAssignedBytes(1));
  EXPECT_EQ(0, result.NumTotalAssignedBytes(2));

  // Remove the other host from the scheduler.
  test_host = 1;
  scheduler.RemoveBackend(cluster.hosts()[test_host]);
  result.Reset();

  ASSERT_OK(scheduler.Compute(&result));
  // Only one backend remains so the scheduler must pick it.
  EXPECT_EQ(0, result.NumTotalAssignedBytes(0));
  EXPECT_EQ(0, result.NumTotalAssignedBytes(1));
  EXPECT_EQ(1 * Block::DEFAULT_BLOCK_SIZE, result.NumTotalAssignedBytes(2));
}

TEST_F(SchedulerTest, TestGeneratedSingleSplit) {
  Cluster cluster;

  cluster.AddHosts(3, true, true);

  Schema schema(cluster);
  schema.AddFileSplitGeneratorDefaultSpecs("T", 1);

  Plan plan(schema);
  plan.AddTableScan("T");
  plan.SetRandomReplica(true);

  Result result(plan);
  SchedulerWrapper scheduler(plan);
  ASSERT_OK(scheduler.Compute(&result));

  EXPECT_EQ(FileSplitGeneratorSpec::DEFAULT_FILE_SIZE
          / FileSplitGeneratorSpec::DEFAULT_BLOCK_SIZE,
      result.NumTotalAssignments());
  EXPECT_EQ(
      1 * FileSplitGeneratorSpec::DEFAULT_FILE_SIZE, result.NumTotalAssignedBytes());
}

TEST_F(SchedulerTest, TestGeneratedMultiSplit) {
  Cluster cluster;

  cluster.AddHosts(3, true, true);

  Schema schema(cluster);
  schema.AddFileSplitGeneratorDefaultSpecs("T", 100);

  Plan plan(schema);
  plan.AddTableScan("T");
  plan.SetRandomReplica(true);

  Result result(plan);
  SchedulerWrapper scheduler(plan);
  ASSERT_OK(scheduler.Compute(&result));

  EXPECT_EQ(100 * FileSplitGeneratorSpec::DEFAULT_FILE_SIZE
          / FileSplitGeneratorSpec::DEFAULT_BLOCK_SIZE,
      result.NumTotalAssignments());
  EXPECT_EQ(
      100 * FileSplitGeneratorSpec::DEFAULT_FILE_SIZE, result.NumTotalAssignedBytes());
}

TEST_F(SchedulerTest, TestGeneratedVariableSizeSplit) {
  Cluster cluster;

  cluster.AddHosts(3, true, true);

  Schema schema(cluster);
  schema.AddFileSplitGeneratorSpecs(
      "T", {{100, 100, true}, {100, 1, false}, {100, 10, true}});

  Plan plan(schema);
  plan.AddTableScan("T");
  plan.SetRandomReplica(true);

  Result result(plan);
  SchedulerWrapper scheduler(plan);
  ASSERT_OK(scheduler.Compute(&result));

  EXPECT_EQ(12, result.NumTotalAssignments());
  EXPECT_EQ(300, result.NumTotalAssignedBytes());
}

TEST_F(SchedulerTest, TestBlockAndGenerateSplit) {
  Cluster cluster;

  cluster.AddHosts(3, true, true);
  Schema schema(cluster);
  schema.AddMultiBlockTable("T", 2, ReplicaPlacement::LOCAL_ONLY, 3);
  schema.AddFileSplitGeneratorSpecs(
      "T", {{100, 100, true}, {100, 1, false}, {100, 10, true}});

  Plan plan(schema);
  plan.AddTableScan("T");

  Result result(plan);
  SchedulerWrapper scheduler(plan);
  ASSERT_OK(scheduler.Compute(&result));

  EXPECT_EQ(14, result.NumTotalAssignments());
  EXPECT_EQ(300 + 2 * Block::DEFAULT_BLOCK_SIZE, result.NumTotalAssignedBytes());
  EXPECT_EQ(2 * Block::DEFAULT_BLOCK_SIZE, result.NumDiskAssignedBytes());
  EXPECT_EQ(0, result.NumCachedAssignedBytes());
}

/// Test scheduling fails with no backends (the local backend gets registered with the
/// scheduler but is not marked as an executor).
TEST_F(SchedulerTest, TestEmptyBackendConfig) {
  Cluster cluster;
  cluster.AddHost(false, true);

  Schema schema(cluster);
  schema.AddMultiBlockTable("T", 1, ReplicaPlacement::REMOTE_ONLY, 1);

  Plan plan(schema);
  plan.AddTableScan("T");

  Result result(plan);
  SchedulerWrapper scheduler(plan);
  Status status = scheduler.Compute(&result);
  EXPECT_TRUE(!status.ok());
}

/// IMPALA-4494: Test scheduling with no backends but exec_at_coord.
TEST_F(SchedulerTest, TestExecAtCoordWithEmptyBackendConfig) {
  Cluster cluster;
  cluster.AddHost(false, true);

  Schema schema(cluster);
  schema.AddMultiBlockTable("T", 1, ReplicaPlacement::REMOTE_ONLY, 1);

  Plan plan(schema);
  plan.AddTableScan("T");

  Result result(plan);
  SchedulerWrapper scheduler(plan);
  bool exec_at_coord = true;
  Status status = scheduler.Compute(exec_at_coord, &result);
  EXPECT_TRUE(status.ok());
}

/// IMPALA-4494: Test exec_at_coord while local backend is not registered with itself.
TEST_F(SchedulerTest, TestExecAtCoordWithoutLocalBackend) {
  Cluster cluster;
  cluster.AddHosts(3, true, true);

  Schema schema(cluster);
  schema.AddMultiBlockTable("T", 1, ReplicaPlacement::LOCAL_ONLY, 1);

  Plan plan(schema);
  plan.AddTableScan("T");

  Result result(plan);
  SchedulerWrapper scheduler(plan);

  // Remove first host from scheduler. By convention this is the coordinator. The
  // scheduler will ignore this and successfully assign the scan.
  scheduler.RemoveBackend(cluster.hosts()[0]);

  bool exec_at_coord = true;
  Status status = scheduler.Compute(exec_at_coord, &result);
  EXPECT_TRUE(status.ok());
}

} // end namespace impala

IMPALA_TEST_MAIN();
