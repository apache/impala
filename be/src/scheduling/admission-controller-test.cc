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

#include "scheduling/admission-controller.h"

#include "common/names.h"
#include "kudu/util/logging.h"
#include "kudu/util/logging_test_util.h"
#include "runtime/exec-env.h"
#include "runtime/runtime-state.h"
#include "runtime/test-env.h"
#include "service/fe-support.h"
#include "testutil/gtest-util.h"
#include "testutil/scoped-flag-setter.h"
#include "util/metrics.h"

// Access the flags that are defined in RequestPoolService.
DECLARE_string(fair_scheduler_allocation_path);
DECLARE_string(llama_site_path);

namespace impala {

static const string IMPALA_HOME(getenv("IMPALA_HOME"));

// Queues used in the configuration files fair-scheduler-test2.xml and
// llama-site-test2.xml.
static const string QUEUE_A = "root.queueA";
static const string QUEUE_B = "root.queueB";
static const string QUEUE_C = "root.queueC";
static const string QUEUE_D = "root.queueD";

// Host names
static const string HOST_1 = "host1:25000";
static const string HOST_2 = "host2:25000";

static const int64_t MEGABYTE = 1024L * 1024L;

/// Parent class for Admission Controller tests.
/// Common code and constants should go here.
/// These are single threaded tests so we access the internal data structures of
/// the AdmissionController object, and call methods such as 'GetPoolStats' without
/// taking the admission_ctrl_lock_ lock.
class AdmissionControllerTest : public testing::Test {
 protected:
  boost::scoped_ptr<TestEnv> test_env_;

  // Pool for objects to be destroyed during test teardown.
  ObjectPool pool_;

  // Saved configuration flags for restoring the values at the end of the test.
  std::unique_ptr<google::FlagSaver> flag_saver_;

  virtual void SetUp() {
    // Establish a TestEnv so that ExecEnv works in tests.
    test_env_.reset(new TestEnv);
    flag_saver_.reset(new google::FlagSaver());
    ASSERT_OK(test_env_->Init());
  }

  virtual void TearDown() {
    flag_saver_.reset();
    pool_.Clear();
  }

  /// Make a QuerySchedule with dummy parameters that can be used to test admission and
  /// rejection in AdmissionController.
  QuerySchedule* MakeQuerySchedule(string request_pool_name, TPoolConfig& config,
      const int num_hosts, const int per_host_mem_estimate) {
    DCHECK_GT(num_hosts, 0);
    TQueryExecRequest* request = pool_.Add(new TQueryExecRequest());
    request->query_ctx.request_pool = request_pool_name;
    request->__set_per_host_mem_estimate(per_host_mem_estimate);

    RuntimeProfile* profile = RuntimeProfile::Create(&pool_, "pool1");
    TUniqueId* query_id = pool_.Add(new TUniqueId()); // always 0,0
    TQueryOptions* query_options = pool_.Add(new TQueryOptions());
    QuerySchedule* query_schedule =
        pool_.Add(new QuerySchedule(*query_id, *request, *query_options, profile));
    query_schedule->UpdateMemoryRequirements(config);

    SetHostsInQuerySchedule(*query_schedule, num_hosts);
    return query_schedule;
  }

  /// Replace the per-backend hosts in the schedule with one having 'count' hosts.
  void SetHostsInQuerySchedule(QuerySchedule& query_schedule, const int count,
      int64_t min_mem_reservation_bytes = 0, int64_t admit_mem_limit = 200L * MEGABYTE) {
    PerBackendExecParams* per_backend_exec_params = pool_.Add(new PerBackendExecParams());
    for (int i = 0; i < count; ++i) {
      BackendExecParams* backend_exec_params = pool_.Add(new BackendExecParams());
      backend_exec_params->admit_mem_limit = admit_mem_limit;
      backend_exec_params->min_mem_reservation_bytes = min_mem_reservation_bytes;
      const string host_name = Substitute("host$0", i);
      per_backend_exec_params->emplace(
          MakeNetworkAddress(host_name, 25000), *backend_exec_params);
    }
    query_schedule.set_per_backend_exec_params(*per_backend_exec_params);
  }

  /// Build a TTopicDelta object for IMPALA_REQUEST_QUEUE_TOPIC.
  static TTopicDelta MakeTopicDelta(const bool is_delta) {
    TTopicDelta delta;
    delta.topic_name = Statestore::IMPALA_REQUEST_QUEUE_TOPIC;
    delta.is_delta = is_delta;
    return delta;
  }

  /// Build a TPoolStats object.
  static TPoolStats MakePoolStats(const int backend_mem_reserved,
      const int num_admitted_running, const int num_queued) {
    TPoolStats stats;
    stats.backend_mem_reserved = backend_mem_reserved;
    stats.num_admitted_running = num_admitted_running;
    stats.num_queued = num_queued;
    return stats;
  }

  /// Add a TPoolStats to the TTopicDelta 'delta' with a key created from 'host' and
  /// 'pool_name'
  static void AddStatsToTopic(
      TTopicDelta* topic, const string host, const string pool_name, TPoolStats stats) {
    // Build topic item.
    TTopicItem item;
    item.key = AdmissionController::MakePoolTopicKey(pool_name, host);
    ThriftSerializer serializer(false);
    Status status = serializer.SerializeToString(&stats, &item.value);
    DCHECK(status.ok());

    // Add to the topic.
    topic->topic_entries.push_back(item);
  }

  /// Check that PoolConfig can be read from a RequestPoolService, and that the
  /// configured values are as expected.
  static void CheckPoolConfig(RequestPoolService& request_pool_service,
      const string pool_name, const int64_t max_requests, const int64_t max_mem_resources,
      const int64_t queue_timeout_ms, const bool clamp_mem_limit_query_option,
      const int64_t min_query_mem_limit = 0, const int64_t max_query_mem_limit = 0,
      const double max_running_queries_multiple = 0.0,
      const double max_queued_queries_multiple = 0.0,
      const int64_t max_memory_multiple = 0) {
    TPoolConfig config;
    ASSERT_OK(request_pool_service.GetPoolConfig(pool_name, &config));

    ASSERT_EQ(max_requests, config.max_requests);
    ASSERT_EQ(max_mem_resources, config.max_mem_resources);
    ASSERT_EQ(queue_timeout_ms, config.queue_timeout_ms);
    ASSERT_EQ(clamp_mem_limit_query_option, config.clamp_mem_limit_query_option);
    ASSERT_EQ(min_query_mem_limit, config.min_query_mem_limit);
    ASSERT_EQ(max_query_mem_limit, config.max_query_mem_limit);
    ASSERT_EQ(max_running_queries_multiple, config.max_running_queries_multiple);
    ASSERT_EQ(max_queued_queries_multiple, config.max_queued_queries_multiple);
    ASSERT_EQ(max_memory_multiple, config.max_memory_multiple);
  }

  /// Check that a PoolStats object has all zero values.
  static void CheckPoolStatsEmpty(AdmissionController::PoolStats* pool_stats) {
    ASSERT_EQ(0, pool_stats->agg_mem_reserved_);
    ASSERT_EQ(0, pool_stats->agg_num_queued_);
    ASSERT_EQ(0, pool_stats->agg_num_running_);
    ASSERT_EQ(0, pool_stats->local_mem_admitted_);
    ASSERT_EQ(0, pool_stats->local_stats_.num_queued);
    ASSERT_EQ(0, pool_stats->local_stats_.num_admitted_running);
    ASSERT_EQ(0, pool_stats->local_stats_.backend_mem_reserved);
    ASSERT_EQ(0, pool_stats->metrics()->agg_num_queued->GetValue());
    ASSERT_EQ(0, pool_stats->metrics()->agg_num_running->GetValue());
  }

  /// Check the calculations made by GetMaxQueuedForPool and GetMaxRequestsForPool are
  /// rounded correctly.
  static void CheckRoundingForPool(AdmissionController* admission_controller,
      const int expected_result, const double multiple, const int host_count) {
    TPoolConfig config_round;
    config_round.max_queued_queries_multiple = multiple;
    config_round.max_queued = 0;
    config_round.max_running_queries_multiple = multiple;
    config_round.max_requests = 0;

    int64_t num_queued_rounded =
        admission_controller->GetMaxQueuedForPool(config_round, host_count);
    ASSERT_EQ(expected_result, num_queued_rounded)
        << "with max_queued_queries_multiple=" << config_round.max_queued_queries_multiple
        << " host_count=" << host_count;

    int64_t num_requests_rounded =
        admission_controller->GetMaxRequestsForPool(config_round, host_count);
    ASSERT_EQ(expected_result, num_requests_rounded)
        << "with max_running_queries_multiple="
        << config_round.max_running_queries_multiple << " host_count=" << host_count;
  }

  /// Return the path of the configuration file in the test resources directory
  /// that has name 'file_name'.
  static string GetResourceFile(const string& file_name) {
    return Substitute("$0/fe/src/test/resources/$1", IMPALA_HOME, file_name);
  }

  /// Make an AdmissionController with some dummy parameters
  AdmissionController* MakeAdmissionController() {
    // Create a RequestPoolService which will read the configuration files.
    MetricGroup* metric_group = pool_.Add(new MetricGroup("impala-metrics"));
    RequestPoolService* request_pool_service =
        pool_.Add(new RequestPoolService(metric_group));
    TNetworkAddress* addr = pool_.Add(new TNetworkAddress());
    addr->__set_hostname("host0");
    addr->__set_port(25000);
    return pool_.Add(new AdmissionController(
        nullptr, nullptr, request_pool_service, metric_group, *addr));
  }

  static void checkPoolDisabled(bool expected_result, int64_t max_requests,
      double max_running_queries_multiple, int64_t max_mem_resources,
      int64_t max_memory_multiple) {
    TPoolConfig pool_config;
    pool_config.max_requests = max_requests;
    pool_config.max_running_queries_multiple = max_running_queries_multiple;
    pool_config.max_mem_resources = max_mem_resources;
    pool_config.max_memory_multiple = max_memory_multiple;
    ASSERT_EQ(expected_result, AdmissionController::PoolDisabled(pool_config));
  }
};

/// Test that AdmissionController will admit a query into a pool, then simulate other
/// work being added to the pool, and then test that the AdmissionController will no
/// longer admit the query.
TEST_F(AdmissionControllerTest, Simple) {
  // Pass the paths of the configuration files as command line flags
  FLAGS_fair_scheduler_allocation_path = GetResourceFile("fair-scheduler-test2.xml");
  FLAGS_llama_site_path = GetResourceFile("llama-site-test2.xml");

  AdmissionController* admission_controller = MakeAdmissionController();
  RequestPoolService* request_pool_service = admission_controller->request_pool_service_;

  // Get the PoolConfig for QUEUE_C ("root.queueC").
  TPoolConfig config_c;
  ASSERT_OK(request_pool_service->GetPoolConfig(QUEUE_C, &config_c));

  // Create a QuerySchedule to run on QUEUE_C.
  QuerySchedule* query_schedule = MakeQuerySchedule(QUEUE_C, config_c, 1, 64L * MEGABYTE);

  // Check that the AdmissionController initially has no data about other hosts.
  ASSERT_EQ(0, admission_controller->host_mem_reserved_.size());

  // Check that the query can be admitted.
  string not_admitted_reason;
  int64_t host_count = 1;
  ASSERT_TRUE(admission_controller->CanAdmitRequest(
      *query_schedule, config_c, host_count, true, &not_admitted_reason));

  // Create a QuerySchedule just like 'query_schedule' but to run on 3 hosts.
  QuerySchedule* query_schedule_3_hosts =
      MakeQuerySchedule(QUEUE_C, config_c, 3, 64L * MEGABYTE);
  host_count = 3;
  // This won't run as configuration using 'max_mem_resources' is not scalable.
  ASSERT_FALSE(admission_controller->CanAdmitRequest(
      *query_schedule_3_hosts, config_c, host_count, true, &not_admitted_reason));
  EXPECT_STR_CONTAINS(not_admitted_reason,
      "Not enough aggregate memory available in pool root.queueC with max mem "
      "resources 128.00 MB (configured statically). Needed 192.00 MB but only 128.00 "
      "MB was available.");

  // Make a TopicDeltaMap describing some activity on host1 and host2.
  TTopicDelta membership = MakeTopicDelta(false);
  AddStatsToTopic(&membership, HOST_1, QUEUE_B, MakePoolStats(1000, 1, 0));
  AddStatsToTopic(&membership, HOST_1, QUEUE_C, MakePoolStats(5000, 10, 0));
  AddStatsToTopic(&membership, HOST_2, QUEUE_C, MakePoolStats(5000, 1, 0));

  // Imitate the StateStore passing updates on query activity to the
  // AdmissionController.
  StatestoreSubscriber::TopicDeltaMap incoming_topic_deltas;
  incoming_topic_deltas.emplace(Statestore::IMPALA_REQUEST_QUEUE_TOPIC, membership);
  vector<TTopicDelta> outgoing_topic_updates;
  admission_controller->UpdatePoolStats(incoming_topic_deltas, &outgoing_topic_updates);

  // Check that the AdmissionController has aggregated the remote stats.
  ASSERT_EQ(3, admission_controller->host_mem_reserved_.size());
  ASSERT_EQ(6000, admission_controller->host_mem_reserved_[HOST_1]);
  ASSERT_EQ(5000, admission_controller->host_mem_reserved_[HOST_2]);

  // Check the PoolStats for QUEUE_C.
  AdmissionController::PoolStats* pool_stats =
      admission_controller->GetPoolStats(QUEUE_C);
  ASSERT_EQ(10000, pool_stats->agg_mem_reserved_);
  ASSERT_EQ(11, pool_stats->agg_num_running_);

  // Test that the query cannot be admitted now.
  ASSERT_FALSE(admission_controller->CanAdmitRequest(
      *query_schedule, config_c, host_count, true, &not_admitted_reason));
  EXPECT_STR_CONTAINS(not_admitted_reason,
      "number of running queries 11 is at or over limit 10 (configured statically).");
}

/// Test rounding of scalable configuration parameters.
TEST_F(AdmissionControllerTest, CheckRounding) {
  // Pass the paths of the configuration files as command line flags.
  FLAGS_fair_scheduler_allocation_path = GetResourceFile("fair-scheduler-test2.xml");
  FLAGS_llama_site_path = GetResourceFile("llama-site-test2.xml");

  AdmissionController* ac = MakeAdmissionController(); // Short name to make code neat.

  // The scalable configuration parameters 'max_running_queries_multiple' and
  // 'max_queued_queries_multiple' are scaled by multiplying by the number of hosts.
  // If the result is non-zero then this is rounded up.
  CheckRoundingForPool(ac, /*expected*/ 0, /*parameter*/ 0, /*num hosts*/ 100);
  CheckRoundingForPool(ac, /*expected*/ 3, /*parameter*/ 0.3, /*num hosts*/ 10);
  CheckRoundingForPool(ac, /*expected*/ 4, /*parameter*/ 0.31, /*num hosts*/ 10);
  CheckRoundingForPool(ac, /*expected*/ 1, /*parameter*/ 0.3, /*num hosts*/ 3);
  CheckRoundingForPool(ac, /*expected*/ 1, /*parameter*/ 0.1, /*num hosts*/ 3);
  CheckRoundingForPool(ac, /*expected*/ 3, /*parameter*/ 0.3, /*num hosts*/ 9);
  CheckRoundingForPool(ac, /*expected*/ 2, /*parameter*/ 0.5, /*num hosts*/ 3);
  CheckRoundingForPool(ac, /*expected*/ 10000, /*parameter*/ 100, /*num hosts*/ 100);
}

/// Test CanAdmitRequest using scalable memory parameter 'max_memory_multiple'.
TEST_F(AdmissionControllerTest, CanAdmitRequestMemory) {
  // Pass the paths of the configuration files as command line flags.
  FLAGS_fair_scheduler_allocation_path = GetResourceFile("fair-scheduler-test2.xml");
  FLAGS_llama_site_path = GetResourceFile("llama-site-test2.xml");

  AdmissionController* admission_controller = MakeAdmissionController();
  RequestPoolService* request_pool_service = admission_controller->request_pool_service_;

  // Get the PoolConfig for QUEUE_D ("root.queueD").
  TPoolConfig config_d;
  ASSERT_OK(request_pool_service->GetPoolConfig(QUEUE_D, &config_d));
  // This queue is using a scalable amount of memory.
  ASSERT_EQ(40L * MEGABYTE, config_d.max_memory_multiple);

  // Check the PoolStats for QUEUE_D.
  AdmissionController::PoolStats* pool_stats =
      admission_controller->GetPoolStats(QUEUE_D);
  CheckPoolStatsEmpty(pool_stats);

  // Create a QuerySchedule to run on QUEUE_D with per_host_mem_estimate of 30MB.
  int64_t host_count = 2;
  QuerySchedule* query_schedule =
      MakeQuerySchedule(QUEUE_D, config_d, host_count, 30L * MEGABYTE);

  // Check that the query can be admitted.
  string not_admitted_reason;
  ASSERT_TRUE(admission_controller->CanAdmitRequest(
      *query_schedule, config_d, host_count, true, &not_admitted_reason));

  // The query scales with cluster size of 1000.
  host_count = 1000;
  QuerySchedule* query_schedule1000 =
      MakeQuerySchedule(QUEUE_D, config_d, host_count, 30L * MEGABYTE);
  ASSERT_TRUE(admission_controller->CanAdmitRequest(
      *query_schedule1000, config_d, host_count, true, &not_admitted_reason));

  // Create a QuerySchedule to run on QUEUE_D with per_host_mem_estimate of 50MB.
  // which is going to be too much memory.
  host_count = 10;
  QuerySchedule* query_schedule_10_fail =
      MakeQuerySchedule(QUEUE_D, config_d, host_count, 50L * MEGABYTE);

  // Test that this query cannot be admitted.
  ASSERT_FALSE(admission_controller->CanAdmitRequest(
      *query_schedule_10_fail, config_d, host_count, true, &not_admitted_reason));
  EXPECT_STR_CONTAINS(not_admitted_reason,
      "Not enough aggregate memory available in pool root.queueD with max mem resources "
      "400.00 MB (calculated as 10 backends each with 40.00 MB). Needed 500.00 MB but "
      "only 400.00 MB was available.");
}

/// Test CanAdmitRequest using scalable parameter 'max_running_queries_multiple'.
TEST_F(AdmissionControllerTest, CanAdmitRequestCount) {
  // Pass the paths of the configuration files as command line flags.
  FLAGS_fair_scheduler_allocation_path = GetResourceFile("fair-scheduler-test2.xml");
  FLAGS_llama_site_path = GetResourceFile("llama-site-test2.xml");

  AdmissionController* admission_controller = MakeAdmissionController();
  RequestPoolService* request_pool_service = admission_controller->request_pool_service_;

  // Get the PoolConfig for QUEUE_D ("root.queueD").
  TPoolConfig config_d;
  ASSERT_OK(request_pool_service->GetPoolConfig(QUEUE_D, &config_d));

  // This queue can run a scalable number of queries.
  ASSERT_EQ(0.5, config_d.max_running_queries_multiple);
  ASSERT_EQ(2.5, config_d.max_queued_queries_multiple);

  // Check the PoolStats for QUEUE_D.
  AdmissionController::PoolStats* pool_stats =
      admission_controller->GetPoolStats(QUEUE_D);
  CheckPoolStatsEmpty(pool_stats);

  // Create a QuerySchedule to run on QUEUE_D on 12 hosts.
  int64_t host_count = 12;
  QuerySchedule* query_schedule =
      MakeQuerySchedule(QUEUE_D, config_d, host_count, 30L * MEGABYTE);
  string not_admitted_reason;

  // Simulate that there are 2 queries queued.
  pool_stats->local_stats_.num_queued = 2;

  // Query can be admitted from queue...
  ASSERT_TRUE(admission_controller->CanAdmitRequest(
      *query_schedule, config_d, host_count, true, &not_admitted_reason));
  // ... but same Query cannot be admitted directly.
  ASSERT_FALSE(admission_controller->CanAdmitRequest(
      *query_schedule, config_d, host_count, false, &not_admitted_reason));
  EXPECT_STR_CONTAINS(not_admitted_reason,
      "queue is not empty (size 2); queued queries are executed first");

  // Simulate that there are 7 queries already running.
  pool_stats->agg_num_running_ = 7;
  ASSERT_FALSE(admission_controller->CanAdmitRequest(
      *query_schedule, config_d, host_count, true, &not_admitted_reason));
  EXPECT_STR_CONTAINS(not_admitted_reason,
      "number of running queries 7 is at or over limit 6 (calculated as 12 backends each "
      "with 0.5 queries)");
}

/// Test RejectImmediately using scalable parameters.
TEST_F(AdmissionControllerTest, RejectImmediately) {
  // Pass the paths of the configuration files as command line flags.
  FLAGS_fair_scheduler_allocation_path = GetResourceFile("fair-scheduler-test2.xml");
  FLAGS_llama_site_path = GetResourceFile("llama-site-test2.xml");

  AdmissionController* admission_controller = MakeAdmissionController();
  RequestPoolService* request_pool_service = admission_controller->request_pool_service_;

  // Get the PoolConfig for QUEUE_D ("root.queueD").
  TPoolConfig config_d;
  ASSERT_OK(request_pool_service->GetPoolConfig(QUEUE_D, &config_d));

  // Check the PoolStats for QUEUE_D.
  AdmissionController::PoolStats* pool_stats =
      admission_controller->GetPoolStats(QUEUE_D);
  CheckPoolStatsEmpty(pool_stats);

  // Create a QuerySchedule to run on QUEUE_D with per_host_mem_estimate of 50MB
  // which is going to be too much memory.
  int host_count = 10;
  QuerySchedule* query_schedule =
      MakeQuerySchedule(QUEUE_D, config_d, host_count, 50L * MEGABYTE);

  // Check messages from RejectImmediately().
  string rejected_reason;
  ASSERT_TRUE(admission_controller->RejectImmediately(
      *query_schedule, config_d, host_count, &rejected_reason));
  EXPECT_STR_CONTAINS(rejected_reason,
      "request memory needed 500.00 MB is greater than pool max mem resources 400.00 MB "
      "(calculated as 10 backends each with 40.00 MB)");

  // Adjust the QuerySchedule to have minimum memory reservation of 45MB.
  // This will be rejected immediately as minimum memory reservation is too high.
  SetHostsInQuerySchedule(*query_schedule, host_count, 45L * MEGABYTE);
  string rejected_reserved_reason;
  ASSERT_TRUE(admission_controller->RejectImmediately(
      *query_schedule, config_d, host_count, &rejected_reserved_reason));
  EXPECT_STR_CONTAINS(rejected_reserved_reason,
      "minimum memory reservation needed is greater than pool max mem resources. Pool "
      "max mem resources: 400.00 MB (calculated as 10 backends each with 40.00 MB). "
      "Cluster-wide memory reservation needed: 450.00 MB. Increase the pool max mem "
      "resources.");

  // Overwrite min_query_mem_limit and max_query_mem_limit in config_d to test a message.
  // After this config_d is unusable.
  config_d.min_query_mem_limit = 600L * MEGABYTE;
  config_d.max_query_mem_limit = 700L * MEGABYTE;
  string rejected_invalid_config_reason;
  ASSERT_TRUE(admission_controller->RejectImmediately(
      *query_schedule, config_d, host_count, &rejected_invalid_config_reason));
  EXPECT_STR_CONTAINS(rejected_invalid_config_reason,
      "The min_query_mem_limit 629145600 is greater than the current max_mem_resources "
      "419430400 (calculated as 10 backends each with 40.00 MB); queries will not be "
      "admitted until more executors are available.");

  TPoolConfig config_disabled_queries;
  config_disabled_queries.max_requests = 0;
  string rejected_queries_reason;
  ASSERT_TRUE(admission_controller->RejectImmediately(
      *query_schedule, config_disabled_queries, host_count, &rejected_queries_reason));
  EXPECT_STR_CONTAINS(rejected_queries_reason, "disabled by requests limit set to 0");

  TPoolConfig config_disabled_memory;
  config_disabled_memory.max_requests = 1;
  config_disabled_memory.max_mem_resources = 0;
  config_disabled_memory.max_memory_multiple = 0;
  string rejected_mem_reason;
  ASSERT_TRUE(admission_controller->RejectImmediately(
      *query_schedule, config_disabled_memory, host_count, &rejected_mem_reason));
  EXPECT_STR_CONTAINS(rejected_mem_reason, "disabled by pool max mem resources set to 0");

  TPoolConfig config_queue_small;
  config_queue_small.max_requests = 1;
  config_queue_small.max_queued = 3;
  config_queue_small.max_mem_resources = 600 * MEGABYTE;
  pool_stats->agg_num_queued_ = 3;
  string rejected_queue_length_reason;
  ASSERT_TRUE(admission_controller->RejectImmediately(
      *query_schedule, config_queue_small, host_count, &rejected_queue_length_reason));
  EXPECT_STR_CONTAINS(rejected_queue_length_reason,
      "queue full, limit=3 (configured statically), num_queued=3.");

  // Make max_queued_queries_multiple small so that rejection is becasue of number of
  // queries that can run be queued.
  config_queue_small.max_queued_queries_multiple = 0.3;
  string rejected_queue_multiple_reason;
  ASSERT_TRUE(admission_controller->RejectImmediately(
      *query_schedule, config_queue_small, host_count, &rejected_queue_multiple_reason));
  EXPECT_STR_CONTAINS(rejected_queue_multiple_reason,
      "queue full, limit=3 (calculated as 10 backends each with 0.3 queries), "
      "num_queued=3.");
}

/// Test GetMaxToDequeue() method.
TEST_F(AdmissionControllerTest, GetMaxToDequeue) {
  // Pass the paths of the configuration files as command line flags
  FLAGS_fair_scheduler_allocation_path = GetResourceFile("fair-scheduler-test2.xml");
  FLAGS_llama_site_path = GetResourceFile("llama-site-test2.xml");

  AdmissionController* admission_controller = MakeAdmissionController();
  RequestPoolService* request_pool_service = admission_controller->request_pool_service_;

  // Get the PoolConfig for QUEUE_C and QUEUE_D
  TPoolConfig config_c;
  TPoolConfig config_d;
  TPoolConfig config;
  ASSERT_OK(request_pool_service->GetPoolConfig(QUEUE_D, &config_d));
  AdmissionController::RequestQueue& queue_c =
      admission_controller->request_queue_map_[QUEUE_C];
  ASSERT_OK(request_pool_service->GetPoolConfig(QUEUE_C, &config_c));
  AdmissionController::RequestQueue& queue_d =
      admission_controller->request_queue_map_[QUEUE_D];

  AdmissionController::PoolStats* stats_c = admission_controller->GetPoolStats(QUEUE_C);
  AdmissionController::PoolStats* stats_d = admission_controller->GetPoolStats(QUEUE_D);

  int64_t max_to_dequeue;
  int64_t host_count = 1;

  // Queue is empty, so nothing to dequeue
  max_to_dequeue =
      admission_controller->GetMaxToDequeue(queue_c, stats_c, config_c, host_count);
  ASSERT_EQ(0, max_to_dequeue);
  max_to_dequeue =
      admission_controller->GetMaxToDequeue(queue_d, stats_d, config_d, host_count);
  ASSERT_EQ(0, max_to_dequeue);

  AdmissionController::PoolStats stats(admission_controller, "test");

  // First of all test non-scalable configuration.

  // Queue holds 10 with 10 running - cannot dequeue
  config.max_requests = 10;
  stats.local_stats_.num_queued = 10;
  stats.agg_num_queued_ = 20;
  stats.agg_num_running_ = 10;
  max_to_dequeue =
      admission_controller->GetMaxToDequeue(queue_c, &stats, config, host_count);
  ASSERT_EQ(0, max_to_dequeue);

  // Can only dequeue 1.
  stats.agg_num_running_ = 9;
  max_to_dequeue =
      admission_controller->GetMaxToDequeue(queue_c, &stats, config, host_count);
  ASSERT_EQ(1, max_to_dequeue);

  // There is space for 10 but it looks like there are 2 coordinators.
  stats.agg_num_running_ = 0;
  max_to_dequeue =
      admission_controller->GetMaxToDequeue(queue_c, &stats, config, host_count);
  ASSERT_EQ(5, max_to_dequeue);

  // Now test scalable configuration.

  config.max_running_queries_multiple = 0.5;
  max_to_dequeue =
      admission_controller->GetMaxToDequeue(queue_c, &stats, config, host_count);
  ASSERT_EQ(1, max_to_dequeue);

  config.max_running_queries_multiple = 5;
  // At this point the host_count is one, so the estimate of the pool
  // size will be 1. This coordinator will take its share (1/2) of the 5 that can run
  max_to_dequeue =
      admission_controller->GetMaxToDequeue(queue_c, &stats, config, host_count);
  ASSERT_EQ(2, max_to_dequeue);

  // Add a lot of hosts, limitation will now be number queued
  host_count = 100;
  max_to_dequeue =
      admission_controller->GetMaxToDequeue(queue_c, &stats, config, host_count);
  ASSERT_EQ(stats.local_stats_.num_queued, max_to_dequeue);

  // Increase number queued.
  host_count = 200;
  stats.local_stats_.num_queued = host_count;
  stats.agg_num_queued_ = host_count;
  max_to_dequeue =
      admission_controller->GetMaxToDequeue(queue_c, &stats, config, host_count);
  ASSERT_EQ(stats.local_stats_.num_queued, max_to_dequeue);

  // Test max_running_queries_multiple less than 1.
  config.max_running_queries_multiple = 0.5;
  max_to_dequeue =
      admission_controller->GetMaxToDequeue(queue_c, &stats, config, host_count);
  ASSERT_EQ(100, max_to_dequeue);
}

/// Test that RequestPoolService correctly reads configuration files.
TEST_F(AdmissionControllerTest, Config) {
  // Pass the paths of the configuration files as command line flags
  FLAGS_fair_scheduler_allocation_path = GetResourceFile("fair-scheduler-test2.xml");
  FLAGS_llama_site_path = GetResourceFile("llama-site-test2.xml");

  // Create a RequestPoolService which will read the configuration files.
  MetricGroup metric_group("impala-metrics");
  RequestPoolService request_pool_service(&metric_group);

  // Test that the pool configurations can be read correctly.
  CheckPoolConfig(request_pool_service, "non-existent queue", 5, -1, 30000, true);
  CheckPoolConfig(request_pool_service, QUEUE_A, 1, 100000L * MEGABYTE, 50, true);
  CheckPoolConfig(request_pool_service, QUEUE_B, 5, -1, 600000, true);
  CheckPoolConfig(request_pool_service, QUEUE_C, 10, 128L * MEGABYTE, 30000, true);
  CheckPoolConfig(request_pool_service, QUEUE_D, 5, MEGABYTE * 1024L, 30000, true, 10,
      60L * MEGABYTE, 0.5, 2.5, 40L * MEGABYTE);
}

/// Unit test for PoolStats
TEST_F(AdmissionControllerTest, PoolStats) {
  AdmissionController* admission_controller = MakeAdmissionController();
  RequestPoolService* request_pool_service = admission_controller->request_pool_service_;

  // Get the PoolConfig for QUEUE_C ("root.queueC").
  TPoolConfig config_c;
  ASSERT_OK(request_pool_service->GetPoolConfig(QUEUE_C, &config_c));

  // Create a QuerySchedule to run on QUEUE_C.
  QuerySchedule* query_schedule = MakeQuerySchedule(QUEUE_C, config_c, 1, 1000);

  // Get the PoolStats for QUEUE_C.
  AdmissionController::PoolStats* pool_stats =
      admission_controller->GetPoolStats(QUEUE_C);
  CheckPoolStatsEmpty(pool_stats);

  // Show that Queue and Dequeue leave stats at zero.
  pool_stats->Queue(*query_schedule);
  ASSERT_EQ(1, pool_stats->agg_num_queued());
  ASSERT_EQ(1, pool_stats->metrics()->agg_num_queued->GetValue());
  pool_stats->Dequeue(*query_schedule, false);
  CheckPoolStatsEmpty(pool_stats);

  // Show that Admit and Release leave stats at zero.
  pool_stats->Admit(*query_schedule);
  ASSERT_EQ(1, pool_stats->agg_num_running());
  ASSERT_EQ(1, pool_stats->metrics()->agg_num_running->GetValue());
  pool_stats->Release(*query_schedule, false);
  CheckPoolStatsEmpty(pool_stats);
}

/// Test that PoolDisabled works
TEST_F(AdmissionControllerTest, PoolDisabled) {
  checkPoolDisabled(true, /* max_requests */ 0, /* max_running_queries_multiple */ 0,
      /* max_mem_resources */ 0, /* max_memory_multiple */ 0);
  checkPoolDisabled(false, /* max_requests */ 1, /* max_running_queries_multiple */ 0,
      /* max_mem_resources */ 1, /* max_memory_multiple */ 0);
  checkPoolDisabled(false, /* max_requests */ 0, /* max_running_queries_multiple */ 1.0,
      /* max_mem_resources */ 0, /* max_memory_multiple */ 1);
  checkPoolDisabled(true, /* max_requests */ 0, /* max_running_queries_multiple */ 0,
      /* max_mem_resources */ 0, /* max_memory_multiple */ 1);
  checkPoolDisabled(true, /* max_requests */ 0, /* max_running_queries_multiple */ 1.0,
      /* max_mem_resources */ 0, /* max_memory_multiple */ 0);
}

} // end namespace impala

int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);
  impala::InitCommonRuntime(argc, argv, true, impala::TestInfo::BE_TEST);
  impala::InitFeSupport();
  return RUN_ALL_TESTS();
}
