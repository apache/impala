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

#include <iostream>
#include <string>
#include <vector>

#include "gutil/strings/substitute.h"
#include "scheduling/scheduler-test-util.h"
#include "util/benchmark.h"
#include "util/cpu-info.h"
#include "util/debug-util.h"
#include "util/thread.h"

#include "codegen/llvm-codegen.h"
#include "common/init.h"
#include "common/names.h"
#include "service/fe-support.h"

using namespace impala;
using namespace impala::test;


// This benchmark exercises the core scheduling method 'ComputeScanRangeAssignment()' of
// the Scheduler class for various cluster and table sizes. It makes the following
// assumptions:
// - All nodes run local impalads. The benchmark includes suites for both DISK_LOCAL and
//   REMOTE scheduling preferences. Having datanodes without a local impalad would result
//   in a performance somewhere between these two.
// - The plan only scans one table. All logic in the scheduler is built around assigning
//   blocks to impalad backends, which scan them. Having multiple tables will merely
//   repeat this process. The interesting metric is varying the number of blocks per
//   table.
// - Blocks and files are treated as the same thing from the scheduler's perspective.
//   Scheduling happens on scan ranges, which are issued based on file blocks by the
//   frontend.
//
// Machine Info: Intel(R) Core(TM) i7-4790 CPU @ 3.60GHz
// Cluster Size, DISK_LOCAL:  Function  iters/ms   10%ile   50%ile   90%ile     10%ile     50%ile     90%ile
//                                                                          (relative) (relative) (relative)
// ---------------------------------------------------------------------------------------------------------
//                             3 Hosts               13.2     13.5     13.5         1X         1X         1X
//                            10 Hosts               12.6     12.6     12.8     0.951X     0.937X     0.951X
//                            50 Hosts               9.34     9.52     9.53     0.705X     0.706X     0.706X
//                           100 Hosts               8.68     8.68     8.68     0.655X     0.643X     0.643X
//                           500 Hosts               5.57     5.67      5.7      0.42X     0.421X     0.423X
//                          1000 Hosts               4.02      4.1      4.1     0.304X     0.304X     0.304X
//                          3000 Hosts               1.85     1.85     1.86      0.14X     0.137X     0.138X
//                         10000 Hosts              0.577    0.588    0.588    0.0436X    0.0436X    0.0436X
//
// Cluster Size, REMOTE:      Function  iters/ms   10%ile   50%ile   90%ile     10%ile     50%ile     90%ile
//                                                                          (relative) (relative) (relative)
// ---------------------------------------------------------------------------------------------------------
//                             3 Hosts               23.8     23.9     24.1         1X         1X         1X
//                            10 Hosts               20.4     20.9     21.1     0.854X     0.877X     0.878X
//                            50 Hosts                 15       15     15.2     0.628X     0.628X     0.632X
//                           100 Hosts               12.6     12.7     12.7      0.53X     0.532X     0.528X
//                           500 Hosts               7.38     7.55      7.6      0.31X     0.316X     0.316X
//                          1000 Hosts                4.9     4.93     4.93     0.206X     0.207X     0.205X
//                          3000 Hosts                1.9     1.96     1.96    0.0797X    0.0823X    0.0817X
//                         10000 Hosts              0.577    0.588    0.588    0.0242X    0.0246X    0.0245X
//
// Number of Blocks:          Function  iters/ms   10%ile   50%ile   90%ile     10%ile     50%ile     90%ile
//                                                                          (relative) (relative) (relative)
// ---------------------------------------------------------------------------------------------------------
//                            1 Blocks               74.5     75.2     76.1         1X         1X         1X
//                           10 Blocks               44.4     44.6     45.1     0.596X     0.593X     0.593X
//                          100 Blocks               8.46     8.46     8.49     0.114X     0.113X     0.112X
//                         1000 Blocks              0.981        1        1    0.0132X    0.0133X    0.0131X
//                        10000 Blocks                0.1    0.102    0.103   0.00134X   0.00136X   0.00136X

static const vector<int> CLUSTER_SIZES = {3, 10, 50, 100, 500, 1000, 3000, 10000};
static const int DEFAULT_CLUSTER_SIZE = 100;
static const vector<int> NUM_BLOCKS_PER_TABLE = {1, 10, 100, 1000, 10000};
static const int DEFAULT_NUM_BLOCKS_PER_TABLE = 100;

/// Members of this struct are needed to build the test fixtures and depend on each other.
/// Since their constructors take const references they must be constructed in order,
/// which is why we keep pointers to them here.
struct TestCtx {
  std::unique_ptr<Cluster> cluster;
  std::unique_ptr<Schema> schema;
  std::unique_ptr<Plan> plan;
  std::unique_ptr<Result> result;
  std::unique_ptr<SchedulerWrapper> scheduler_wrapper;
};

/// Initialize a test context for a single benchmark run.
void InitializeTestCtx(int num_hosts, int num_blocks,
    TReplicaPreference::type replica_preference, TestCtx* test_ctx) {
  test_ctx->cluster.reset(new Cluster());
  test_ctx->cluster->AddHosts(num_hosts, true, true);

  test_ctx->schema.reset(new Schema(*test_ctx->cluster));
  test_ctx->schema->AddMultiBlockTable("T0", num_blocks, ReplicaPlacement::LOCAL_ONLY, 3);

  test_ctx->plan.reset(new Plan(*test_ctx->schema));
  test_ctx->plan->SetReplicaPreference(replica_preference);
  test_ctx->plan->SetRandomReplica(true);
  test_ctx->plan->AddTableScan("T0");

  test_ctx->result.reset(new Result(*test_ctx->plan));

  test_ctx->scheduler_wrapper.reset(new SchedulerWrapper(*test_ctx->plan));
}

/// This function is passed to the test framework and executes the scheduling method
/// repeatedly.
void BenchmarkFunction(int num_iterations, void* data) {
  TestCtx* test_ctx = static_cast<TestCtx*>(data);
  for (int i = 0; i < num_iterations; ++i) {
    test_ctx->result->Reset();
    Status status = test_ctx->scheduler_wrapper->Compute(test_ctx->result.get());
    if (!status.ok()) LOG(FATAL) << status.GetDetail();
  }
}

/// Build and run a benchmark suite for various cluster sizes with the default number of
/// blocks. Scheduling will be done according to the parameter 'replica_preference'.
void RunClusterSizeBenchmark(TReplicaPreference::type replica_preference) {
  string suite_name = strings::Substitute(
      "Cluster Size, $0", PrintValue(replica_preference));
  Benchmark suite(suite_name, false /* micro_heuristics */);
  vector<TestCtx> test_ctx(CLUSTER_SIZES.size());

  for (int i = 0; i < CLUSTER_SIZES.size(); ++i) {
    int cluster_size = CLUSTER_SIZES[i];
    InitializeTestCtx(
        cluster_size, DEFAULT_NUM_BLOCKS_PER_TABLE, replica_preference, &test_ctx[i]);
    string benchmark_name = strings::Substitute("$0 Hosts", cluster_size);
    suite.AddBenchmark(benchmark_name, BenchmarkFunction, &test_ctx[i]);
  }
  cout << suite.Measure() << endl;
}

/// Build and run a benchmark suite for various table sizes with the default cluster size.
/// Scheduling will be done according to the parameter 'replica_preference'.
void RunNumBlocksBenchmark(TReplicaPreference::type replica_preference) {
  Benchmark suite("Number of Blocks", false /* micro_heuristics */);
  vector<TestCtx> test_ctx(NUM_BLOCKS_PER_TABLE.size());

  for (int i = 0; i < NUM_BLOCKS_PER_TABLE.size(); ++i) {
    int num_blocks = NUM_BLOCKS_PER_TABLE[i];
    InitializeTestCtx(DEFAULT_CLUSTER_SIZE, num_blocks, replica_preference, &test_ctx[i]);
    string benchmark_name = strings::Substitute("$0 Blocks", num_blocks);
    suite.AddBenchmark(benchmark_name, BenchmarkFunction, &test_ctx[i]);
  }
  cout << suite.Measure() << endl;
}

int main(int argc, char** argv) {
  impala::InitCommonRuntime(argc, argv, true, impala::TestInfo::BE_TEST);
  impala::InitFeSupport();
  ABORT_IF_ERROR(LlvmCodeGen::InitializeLlvm());

  cout << Benchmark::GetMachineInfo() << endl;
  RunClusterSizeBenchmark(TReplicaPreference::DISK_LOCAL);
  RunClusterSizeBenchmark(TReplicaPreference::REMOTE);
  RunNumBlocksBenchmark(TReplicaPreference::DISK_LOCAL);
}
