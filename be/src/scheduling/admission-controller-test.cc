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
#include "runtime/bufferpool/reservation-util.h"
#include "runtime/exec-env.h"
#include "runtime/runtime-state.h"
#include "runtime/test-env.h"
#include "scheduling/query-schedule.h"
#include "service/fe-support.h"
#include "service/impala-server.h"
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
static const int64_t GIGABYTE = 1024L * MEGABYTE;

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
  QuerySchedule* MakeQuerySchedule(string request_pool_name, int64_t mem_limit,
      TPoolConfig& config, const int num_hosts, const int per_host_mem_estimate,
      const int coord_mem_estimate, bool is_dedicated_coord,
      const string& executor_group = ImpalaServer::DEFAULT_EXECUTOR_GROUP_NAME) {
    DCHECK_GT(num_hosts, 0);
    TQueryExecRequest* request = pool_.Add(new TQueryExecRequest());
    request->query_ctx.request_pool = request_pool_name;
    request->__set_per_host_mem_estimate(per_host_mem_estimate);
    request->__set_dedicated_coord_mem_estimate(coord_mem_estimate);
    request->__set_stmt_type(TStmtType::QUERY);

    RuntimeProfile* profile = RuntimeProfile::Create(&pool_, "pool1");
    UniqueIdPB* query_id = pool_.Add(new UniqueIdPB()); // always 0,0
    TQueryOptions* query_options = pool_.Add(new TQueryOptions());
    query_options->__set_mem_limit(mem_limit);
    QuerySchedule* query_schedule = pool_.Add(new QuerySchedule(
        *query_id, *request, *query_options, profile));
    query_schedule->set_executor_group(executor_group);
    SetHostsInQuerySchedule(*query_schedule, num_hosts, is_dedicated_coord);
    query_schedule->UpdateMemoryRequirements(config);
    return query_schedule;
  }

  /// Same as previous MakeQuerySchedule with fewer input (more default params).
  QuerySchedule* MakeQuerySchedule(string request_pool_name, TPoolConfig& config,
      const int num_hosts, const int per_host_mem_estimate,
      const string& executor_group = ImpalaServer::DEFAULT_EXECUTOR_GROUP_NAME) {
    return MakeQuerySchedule(request_pool_name, 0, config, num_hosts,
        per_host_mem_estimate, per_host_mem_estimate, false, executor_group);
  }

  /// Replace the per-backend hosts in the schedule with one having 'count' hosts.
  /// Note: no FInstanceExecParams are added so
  /// QuerySchedule::UseDedicatedCoordEstimates() would consider this schedule as not
  /// having anything scheduled on the backend which would result in always returning true
  /// if a dedicated coordinator backend exists.
  void SetHostsInQuerySchedule(QuerySchedule& query_schedule, const int count,
      bool is_dedicated_coord, int64_t min_mem_reservation_bytes = 0,
      int64_t admit_mem_limit = 200L * MEGABYTE, int slots_to_use = 1,
      int slots_available = 8) {
    query_schedule.ClearBackendExecParams();
    for (int i = 0; i < count; ++i) {
      const string host_name = Substitute("host$0", i);
      NetworkAddressPB host_addr = MakeNetworkAddressPB(host_name, 25000);
      BackendExecParams& backend_exec_params =
          query_schedule.GetOrCreateBackendExecParams(host_addr);
      backend_exec_params.pb->set_min_mem_reservation_bytes(min_mem_reservation_bytes);
      backend_exec_params.pb->set_slots_to_use(slots_to_use);
      backend_exec_params.be_desc.set_admit_mem_limit(admit_mem_limit);
      backend_exec_params.be_desc.set_admission_slots(slots_available);
      backend_exec_params.be_desc.set_is_executor(true);
      *backend_exec_params.be_desc.mutable_address() = host_addr;
      if (i == 0) {
        // Add first element as the coordinator.
        backend_exec_params.pb->set_is_coord_backend(true);
        backend_exec_params.be_desc.set_is_executor(!is_dedicated_coord);
      }
    }
  }

  /// Extract the host network addresses from 'schedule'.
  vector<NetworkAddressPB> GetHostAddrs(const QuerySchedule& schedule) {
    vector<NetworkAddressPB> host_addrs;
    for (auto& backend_state : schedule.per_backend_exec_params()) {
      host_addrs.push_back(backend_state.first);
    }
    return host_addrs;
  }

  /// Set the slots in use for all the hosts in 'host_addrs'.
  void SetSlotsInUse(AdmissionController* admission_controller,
      const vector<NetworkAddressPB>& host_addrs, int slots_in_use) {
    for (NetworkAddressPB host_addr : host_addrs) {
      string host = NetworkAddressPBToString(host_addr);
      admission_controller->host_stats_[host].slots_in_use = slots_in_use;
    }
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
      const int64_t min_query_mem_limit = 0, const int64_t max_query_mem_limit = 0) {
    TPoolConfig config;
    ASSERT_OK(request_pool_service.GetPoolConfig(pool_name, &config));

    ASSERT_EQ(max_requests, config.max_requests);
    ASSERT_EQ(max_mem_resources, config.max_mem_resources);
    ASSERT_EQ(queue_timeout_ms, config.queue_timeout_ms);
    ASSERT_EQ(clamp_mem_limit_query_option, config.clamp_mem_limit_query_option);
    ASSERT_EQ(min_query_mem_limit, config.min_query_mem_limit);
    ASSERT_EQ(max_query_mem_limit, config.max_query_mem_limit);
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
    ClusterMembershipMgr* cmm =
        pool_.Add(new ClusterMembershipMgr("", nullptr, metric_group));
    return pool_.Add(new AdmissionController(
        cmm, nullptr, request_pool_service, metric_group, *addr));
  }

  static void checkPoolDisabled(
      bool expected_result, int64_t max_requests, int64_t max_mem_resources) {
    TPoolConfig pool_config;
    pool_config.max_requests = max_requests;
    pool_config.max_mem_resources = max_mem_resources;
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
  query_schedule->UpdateMemoryRequirements(config_c);

  // Check that the AdmissionController initially has no data about other hosts.
  ASSERT_EQ(0, admission_controller->host_stats_.size());

  // Check that the query can be admitted.
  string not_admitted_reason;
  ASSERT_TRUE(admission_controller->CanAdmitRequest(
      *query_schedule, config_c, true, &not_admitted_reason));

  // Create a QuerySchedule just like 'query_schedule' to run on 3 hosts which can't be
  // admitted.
  QuerySchedule* query_schedule_3_hosts =
      MakeQuerySchedule(QUEUE_C, config_c, 3, 64L * MEGABYTE);
  ASSERT_FALSE(admission_controller->CanAdmitRequest(
      *query_schedule_3_hosts, config_c, true, &not_admitted_reason));
  EXPECT_STR_CONTAINS(not_admitted_reason,
      "Not enough aggregate memory available in pool root.queueC with max mem "
      "resources 128.00 MB. Needed 192.00 MB but only 128.00 MB was available.");

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
  ASSERT_EQ(3, admission_controller->host_stats_.size());
  ASSERT_EQ(6000, admission_controller->host_stats_[HOST_1].mem_reserved);
  ASSERT_EQ(5000, admission_controller->host_stats_[HOST_2].mem_reserved);

  // Check the PoolStats for QUEUE_C.
  AdmissionController::PoolStats* pool_stats =
      admission_controller->GetPoolStats(QUEUE_C);
  ASSERT_EQ(10000, pool_stats->agg_mem_reserved_);
  ASSERT_EQ(11, pool_stats->agg_num_running_);

  // Test that the query cannot be admitted now.
  ASSERT_FALSE(admission_controller->CanAdmitRequest(
      *query_schedule, config_c, true, &not_admitted_reason));
  EXPECT_STR_CONTAINS(not_admitted_reason,
      "number of running queries 11 is at or over limit 10.");
}

/// Test CanAdmitRequest in the context of aggregated memory required to admit a query.
TEST_F(AdmissionControllerTest, CanAdmitRequestMemory) {
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

  // Create a QuerySchedule to run on QUEUE_D with per_host_mem_estimate of 30MB.
  int64_t host_count = 2;
  QuerySchedule* query_schedule =
      MakeQuerySchedule(QUEUE_D, config_d, host_count, 30L * MEGABYTE);

  // Check that the query can be admitted.
  string not_admitted_reason;
  ASSERT_TRUE(admission_controller->CanAdmitRequest(
      *query_schedule, config_d, true, &not_admitted_reason));

  // Tests that this query cannot be admitted.
  // Increasing the number of hosts pushes the aggregate memory required to admit this
  // query over the allowed limit.
  host_count = 15;
  QuerySchedule* query_schedule15 =
      MakeQuerySchedule(QUEUE_D, config_d, host_count, 30L * MEGABYTE);
  ASSERT_FALSE(admission_controller->CanAdmitRequest(
      *query_schedule15, config_d, true, &not_admitted_reason));
  EXPECT_STR_CONTAINS(not_admitted_reason,
      "Not enough aggregate memory available in pool root.queueD with max mem resources "
      "400.00 MB. Needed 480.00 MB but only 400.00 MB was available.");

  // Create a QuerySchedule to run on QUEUE_D with per_host_mem_estimate of 50MB.
  // which is going to be too much memory.
  host_count = 10;
  QuerySchedule* query_schedule_10_fail =
      MakeQuerySchedule(QUEUE_D, config_d, host_count, 50L * MEGABYTE);

  ASSERT_FALSE(admission_controller->CanAdmitRequest(
      *query_schedule_10_fail, config_d, true, &not_admitted_reason));
  EXPECT_STR_CONTAINS(not_admitted_reason,
      "Not enough aggregate memory available in pool root.queueD with max mem resources "
      "400.00 MB. Needed 500.00 MB but only 400.00 MB was available.");
}

/// Test CanAdmitRequest in the context of max running queries allowed.
TEST_F(AdmissionControllerTest, CanAdmitRequestCount) {
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

  // Create a QuerySchedule to run on QUEUE_D on 12 hosts.
  int64_t host_count = 12;
  QuerySchedule* query_schedule =
      MakeQuerySchedule(QUEUE_D, config_d, host_count, 30L * MEGABYTE);
  string not_admitted_reason;

  // Simulate that there are 2 queries queued.
  pool_stats->local_stats_.num_queued = 2;

  // Query can be admitted from queue...
  ASSERT_TRUE(admission_controller->CanAdmitRequest(
      *query_schedule, config_d, true, &not_admitted_reason));
  // ... but same Query cannot be admitted directly.
  ASSERT_FALSE(admission_controller->CanAdmitRequest(
      *query_schedule, config_d, false, &not_admitted_reason));
  EXPECT_STR_CONTAINS(not_admitted_reason,
      "queue is not empty (size 2); queued queries are executed first");

  // Simulate that there are 7 queries already running.
  pool_stats->agg_num_running_ = 7;
  ASSERT_FALSE(admission_controller->CanAdmitRequest(
      *query_schedule, config_d, true, &not_admitted_reason));
  EXPECT_STR_CONTAINS(
      not_admitted_reason, "number of running queries 7 is at or over limit 6");
}

/// Test CanAdmitRequest() using the slots mechanism that is enabled with non-default
/// executor groups.
TEST_F(AdmissionControllerTest, CanAdmitRequestSlots) {
  // Pass the paths of the configuration files as command line flags.
  FLAGS_fair_scheduler_allocation_path = GetResourceFile("fair-scheduler-test2.xml");
  FLAGS_llama_site_path = GetResourceFile("llama-site-test2.xml");

  AdmissionController* admission_controller = MakeAdmissionController();
  RequestPoolService* request_pool_service = admission_controller->request_pool_service_;

  // Get the PoolConfig for QUEUE_D ("root.queueD").
  TPoolConfig config_d;
  ASSERT_OK(request_pool_service->GetPoolConfig(QUEUE_D, &config_d));

  // Create QuerySchedules to run on QUEUE_D on 12 hosts.
  // Running in both default and non-default executor groups is simulated.
  int64_t host_count = 12;
  int64_t slots_per_host = 16;
  int64_t slots_per_query = 4;
  QuerySchedule* default_group_schedule =
      MakeQuerySchedule(QUEUE_D, config_d, host_count, 30L * MEGABYTE);
  QuerySchedule* other_group_schedule =
      MakeQuerySchedule(QUEUE_D, config_d, host_count, 30L * MEGABYTE, "other_group");
  for (QuerySchedule* schedule : {default_group_schedule, other_group_schedule}) {
    SetHostsInQuerySchedule(
        *schedule, 2, false, MEGABYTE, 200L * MEGABYTE, slots_per_query, slots_per_host);
  }
  vector<NetworkAddressPB> host_addrs = GetHostAddrs(*default_group_schedule);
  string not_admitted_reason;

  // Simulate that there are just enough slots free for the query on all hosts.
  SetSlotsInUse(admission_controller, host_addrs, slots_per_host - slots_per_query);

  // Enough slots are available so it can be admitted in both cases.
  ASSERT_TRUE(admission_controller->CanAdmitRequest(
      *default_group_schedule, config_d, true, &not_admitted_reason))
      << not_admitted_reason;
  ASSERT_TRUE(admission_controller->CanAdmitRequest(
      *other_group_schedule, config_d, true, &not_admitted_reason))
      << not_admitted_reason;

  // Simulate that almost all the slots are in use, which prevents admission in the
  // non-default group.
  SetSlotsInUse(admission_controller, host_addrs, slots_per_host - 1);

  ASSERT_TRUE(admission_controller->CanAdmitRequest(
      *default_group_schedule, config_d, true, &not_admitted_reason))
      << not_admitted_reason;
  ASSERT_FALSE(admission_controller->CanAdmitRequest(
      *other_group_schedule, config_d, true, &not_admitted_reason));
  EXPECT_STR_CONTAINS(not_admitted_reason,
      "Not enough admission control slots available on host host1:25000. Needed 4 "
      "slots but 15/16 are already in use.");
}

/// Tests that query rejection works as expected by calling RejectForSchedule() and
/// RejectForCluster() directly.
TEST_F(AdmissionControllerTest, QueryRejection) {
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

  // Check messages from RejectForSchedule().
  string rejected_reason;
  ASSERT_TRUE(admission_controller->RejectForSchedule(
      *query_schedule, config_d, &rejected_reason));
  EXPECT_STR_CONTAINS(rejected_reason,
      "request memory needed 500.00 MB is greater than pool max mem resources 400.00 MB");

  // Adjust the QuerySchedule to have minimum memory reservation of 45MB.
  // This will be rejected immediately as minimum memory reservation is too high.
  SetHostsInQuerySchedule(*query_schedule, host_count, false, 45L * MEGABYTE);
  string rejected_reserved_reason;
  ASSERT_TRUE(admission_controller->RejectForSchedule(
      *query_schedule, config_d, &rejected_reserved_reason));
  EXPECT_STR_CONTAINS(rejected_reserved_reason,
      "minimum memory reservation needed is greater than pool max mem resources. Pool "
      "max mem resources: 400.00 MB. Cluster-wide memory reservation needed: 450.00 MB. "
      "Increase the pool max mem resources.");

  // Adjust the QuerySchedule to require many slots per node.
  // This will be rejected immediately in non-default executor groups
  // as the nodes do not have that many slots. Because of IMPALA-8757, this check
  // does not yet occur for the default executor group.
  SetHostsInQuerySchedule(*query_schedule, 2, false, MEGABYTE, 200L * MEGABYTE, 16, 4);
  string rejected_slots_reason;
  // Don't reject for default executor group.
  EXPECT_FALSE(admission_controller->RejectForSchedule(
      *query_schedule, config_d, &rejected_slots_reason))
      << rejected_slots_reason;
  // Reject for non-default executor group.
  QuerySchedule* other_group_schedule = MakeQuerySchedule(
      QUEUE_D, config_d, host_count, 50L * MEGABYTE, "a_different_executor_group");
  SetHostsInQuerySchedule(
      *other_group_schedule, 2, false, MEGABYTE, 200L * MEGABYTE, 16, 4);
  EXPECT_TRUE(admission_controller->RejectForSchedule(
      *other_group_schedule, config_d, &rejected_slots_reason));
  EXPECT_STR_CONTAINS(rejected_slots_reason,
      "number of admission control slots needed "
      "(16) on backend 'host1:25000' is greater than total slots available 4. Reduce "
      "mt_dop to less than 4 to ensure that the query can execute.");
  rejected_slots_reason = "";
  // Reduce mt_dop to ensure it can execute.
  SetHostsInQuerySchedule(
      *other_group_schedule, 2, false, MEGABYTE, 200L * MEGABYTE, 4, 4);
  EXPECT_FALSE(admission_controller->RejectForSchedule(
      *other_group_schedule, config_d, &rejected_slots_reason))
      << rejected_slots_reason;

  // Overwrite min_query_mem_limit and max_query_mem_limit in config_d to test a message.
  // After this config_d is unusable.
  config_d.min_query_mem_limit = 600L * MEGABYTE;
  config_d.max_query_mem_limit = 700L * MEGABYTE;
  string rejected_invalid_config_reason;
  ASSERT_TRUE(admission_controller->RejectForCluster(QUEUE_D, config_d,
      /* admit_from_queue=*/false, &rejected_invalid_config_reason));
  EXPECT_STR_CONTAINS(rejected_invalid_config_reason,
      "Invalid pool config: the min_query_mem_limit 629145600 is greater than the "
      "max_mem_resources 419430400");

  TPoolConfig config_disabled_queries;
  config_disabled_queries.max_requests = 0;
  string rejected_queries_reason;
  ASSERT_TRUE(admission_controller->RejectForCluster(QUEUE_D, config_disabled_queries,
      /* admit_from_queue=*/false, &rejected_queries_reason));
  EXPECT_STR_CONTAINS(rejected_queries_reason, "disabled by requests limit set to 0");

  TPoolConfig config_disabled_memory;
  config_disabled_memory.max_requests = 1;
  config_disabled_memory.max_mem_resources = 0;
  string rejected_mem_reason;
  ASSERT_TRUE(admission_controller->RejectForCluster(QUEUE_D, config_disabled_memory,
      /* admit_from_queue=*/false, &rejected_mem_reason));
  EXPECT_STR_CONTAINS(rejected_mem_reason, "disabled by pool max mem resources set to 0");

  TPoolConfig config_queue_small;
  config_queue_small.max_requests = 1;
  config_queue_small.max_queued = 3;
  config_queue_small.max_mem_resources = 600 * MEGABYTE;
  pool_stats->agg_num_queued_ = 3;
  string rejected_queue_length_reason;
  ASSERT_TRUE(admission_controller->RejectForCluster(QUEUE_D, config_queue_small,
      /* admit_from_queue=*/false, &rejected_queue_length_reason));
  EXPECT_STR_CONTAINS(rejected_queue_length_reason, "queue full, limit=3, num_queued=3.");
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

  AdmissionController::PoolStats* stats_c = admission_controller->GetPoolStats(QUEUE_C);

  int64_t max_to_dequeue;
  // Queue is empty, so nothing to dequeue
  max_to_dequeue =
      admission_controller->GetMaxToDequeue(queue_c, stats_c, config_c);
  ASSERT_EQ(0, max_to_dequeue);

  AdmissionController::PoolStats stats(admission_controller, "test");

  // First of all test non-scalable configuration.

  // Queue holds 10 with 10 running - cannot dequeue
  config.max_requests = 10;
  stats.local_stats_.num_queued = 10;
  stats.agg_num_queued_ = 20;
  stats.agg_num_running_ = 10;
  max_to_dequeue =
      admission_controller->GetMaxToDequeue(queue_c, &stats, config);
  ASSERT_EQ(0, max_to_dequeue);

  // Can only dequeue 1.
  stats.agg_num_running_ = 9;
  max_to_dequeue =
      admission_controller->GetMaxToDequeue(queue_c, &stats, config);
  ASSERT_EQ(1, max_to_dequeue);

  // There is space for 10 but it looks like there are 2 coordinators.
  stats.agg_num_running_ = 0;
  max_to_dequeue =
      admission_controller->GetMaxToDequeue(queue_c, &stats, config);
  ASSERT_EQ(5, max_to_dequeue);
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
  pool_stats->Queue();
  ASSERT_EQ(1, pool_stats->agg_num_queued());
  ASSERT_EQ(1, pool_stats->metrics()->agg_num_queued->GetValue());
  pool_stats->Dequeue(false);
  CheckPoolStatsEmpty(pool_stats);

  // Show that Admit and Release leave stats at zero.
  pool_stats->AdmitQueryAndMemory(*query_schedule);
  ASSERT_EQ(1, pool_stats->agg_num_running());
  ASSERT_EQ(1, pool_stats->metrics()->agg_num_running->GetValue());
  int64_t mem_to_release = 0;
  for (auto& backend_state : query_schedule->per_backend_exec_params()) {
    mem_to_release +=
        admission_controller->GetMemToAdmit(*query_schedule, backend_state.second);
  }
  pool_stats->ReleaseMem(mem_to_release);
  pool_stats->ReleaseQuery(0);
  CheckPoolStatsEmpty(pool_stats);
}

/// Test that PoolDisabled works
TEST_F(AdmissionControllerTest, PoolDisabled) {
  checkPoolDisabled(true, /* max_requests */ 0, /* max_mem_resources */ 0);
  checkPoolDisabled(false, /* max_requests */ 1, /* max_mem_resources */ 1);
  checkPoolDisabled(true, /* max_requests */ 0, /* max_mem_resources */ 1);
  checkPoolDisabled(true, /* max_requests */ 1, /* max_mem_resources */ 0);
}

// Basic tests of the QuerySchedule object to confirm that a query with different
// coordinator and executor memory estimates calculates memory to admit correctly
// for various combinations of memory limit configurations.
TEST_F(AdmissionControllerTest, DedicatedCoordQuerySchedule) {
  AdmissionController* admission_controller = MakeAdmissionController();
  RequestPoolService* request_pool_service = admission_controller->request_pool_service_;

  const int64_t PER_EXEC_MEM_ESTIMATE = 512 * MEGABYTE;
  const int64_t COORD_MEM_ESTIMATE = 150 * MEGABYTE;
  TPoolConfig pool_config;
  ASSERT_OK(request_pool_service->GetPoolConfig("default", &pool_config));

  // For query only running on the coordinator, the per_backend_mem_to_admit should be 0.
  QuerySchedule* query_schedule = MakeQuerySchedule(
      "default", 0, pool_config, 1, PER_EXEC_MEM_ESTIMATE, COORD_MEM_ESTIMATE, true);
  query_schedule->UpdateMemoryRequirements(pool_config);
  ASSERT_EQ(0, query_schedule->per_backend_mem_to_admit());
  ASSERT_EQ(COORD_MEM_ESTIMATE, query_schedule->coord_backend_mem_to_admit());

  // Make sure executors and coordinators are assigned memory to admit appropriately and
  // that the cluster memory to admitted is calculated correctly.
  query_schedule = MakeQuerySchedule(
      "default", 0, pool_config, 2, PER_EXEC_MEM_ESTIMATE, COORD_MEM_ESTIMATE, true);
  ASSERT_EQ(COORD_MEM_ESTIMATE, query_schedule->GetDedicatedCoordMemoryEstimate());
  ASSERT_EQ(PER_EXEC_MEM_ESTIMATE, query_schedule->GetPerExecutorMemoryEstimate());
  query_schedule->UpdateMemoryRequirements(pool_config);
  ASSERT_EQ(PER_EXEC_MEM_ESTIMATE, query_schedule->per_backend_mem_to_admit());
  ASSERT_EQ(COORD_MEM_ESTIMATE, query_schedule->coord_backend_mem_to_admit());
  ASSERT_EQ(-1, query_schedule->per_backend_mem_limit());
  ASSERT_EQ(-1, query_schedule->coord_backend_mem_limit());
  ASSERT_EQ(COORD_MEM_ESTIMATE + PER_EXEC_MEM_ESTIMATE,
      query_schedule->GetClusterMemoryToAdmit());

  // Set the min_query_mem_limit in pool_config. min_query_mem_limit should
  // not be enforced on the coordinator. Also ensure mem limits are set for both.
  query_schedule = MakeQuerySchedule(
      "default", 0, pool_config, 2, PER_EXEC_MEM_ESTIMATE, COORD_MEM_ESTIMATE, true);
  ASSERT_OK(request_pool_service->GetPoolConfig("default", &pool_config));
  pool_config.__set_min_query_mem_limit(700 * MEGABYTE);
  query_schedule->UpdateMemoryRequirements(pool_config);
  ASSERT_EQ(700 * MEGABYTE, query_schedule->per_backend_mem_to_admit());
  ASSERT_EQ(COORD_MEM_ESTIMATE, query_schedule->coord_backend_mem_to_admit());
  ASSERT_EQ(700 * MEGABYTE, query_schedule->per_backend_mem_limit());
  ASSERT_EQ(COORD_MEM_ESTIMATE, query_schedule->coord_backend_mem_limit());

  // Make sure coordinator's mem to admit is adjusted based on its own minimum mem
  // reservation.
  query_schedule = MakeQuerySchedule(
      "default", 0, pool_config, 2, PER_EXEC_MEM_ESTIMATE, COORD_MEM_ESTIMATE, true);
  int64_t coord_min_reservation = 200 * MEGABYTE;
  int64_t min_coord_mem_limit_required =
      ReservationUtil::GetMinMemLimitFromReservation(coord_min_reservation);
  pool_config.__set_min_query_mem_limit(700 * MEGABYTE);
  query_schedule->set_coord_min_reservation(200 * MEGABYTE);
  query_schedule->UpdateMemoryRequirements(pool_config);
  ASSERT_EQ(700 * MEGABYTE, query_schedule->per_backend_mem_to_admit());
  ASSERT_EQ(min_coord_mem_limit_required, query_schedule->coord_backend_mem_to_admit());
  ASSERT_EQ(700 * MEGABYTE, query_schedule->per_backend_mem_limit());
  ASSERT_EQ(min_coord_mem_limit_required, query_schedule->coord_backend_mem_limit());

  // Set mem_limit query option.
  ASSERT_OK(request_pool_service->GetPoolConfig("default", &pool_config));
  query_schedule = MakeQuerySchedule("default", GIGABYTE, pool_config, 2,
      PER_EXEC_MEM_ESTIMATE, COORD_MEM_ESTIMATE, true);
  query_schedule->UpdateMemoryRequirements(pool_config);
  ASSERT_EQ(GIGABYTE, query_schedule->per_backend_mem_to_admit());
  ASSERT_EQ(GIGABYTE, query_schedule->coord_backend_mem_to_admit());
  ASSERT_EQ(GIGABYTE, query_schedule->per_backend_mem_limit());
  ASSERT_EQ(GIGABYTE, query_schedule->coord_backend_mem_limit());

  // Set mem_limit query option and max_query_mem_limit. In this case, max_query_mem_limit
  // will be enforced on both coordinator and executor.
  query_schedule = MakeQuerySchedule("default", GIGABYTE, pool_config, 2,
      PER_EXEC_MEM_ESTIMATE, COORD_MEM_ESTIMATE, true);
  ASSERT_OK(request_pool_service->GetPoolConfig("default", &pool_config));
  pool_config.__set_max_query_mem_limit(700 * MEGABYTE);
  query_schedule->UpdateMemoryRequirements(pool_config);
  ASSERT_EQ(700 * MEGABYTE, query_schedule->per_backend_mem_to_admit());
  ASSERT_EQ(700 * MEGABYTE, query_schedule->coord_backend_mem_to_admit());
  ASSERT_EQ(700 * MEGABYTE, query_schedule->per_backend_mem_limit());
  ASSERT_EQ(700 * MEGABYTE, query_schedule->coord_backend_mem_limit());
}

// Test admission decisions for clusters with dedicated coordinators, where different
// amounts of memory should be admitted on coordinators and executors.
TEST_F(AdmissionControllerTest, DedicatedCoordAdmissionChecks) {
  AdmissionController* admission_controller = MakeAdmissionController();
  RequestPoolService* request_pool_service = admission_controller->request_pool_service_;

  TPoolConfig pool_config;
  ASSERT_OK(request_pool_service->GetPoolConfig("default", &pool_config));
  pool_config.__set_max_mem_resources(2*GIGABYTE); // to enable memory based admission.

  // Set up a query schedule to test.
  const int64_t PER_EXEC_MEM_ESTIMATE = GIGABYTE;
  const int64_t COORD_MEM_ESTIMATE = 150 * MEGABYTE;
  QuerySchedule* query_schedule = MakeQuerySchedule(
      "default", 0, pool_config, 2, PER_EXEC_MEM_ESTIMATE, COORD_MEM_ESTIMATE, true);
  query_schedule->ClearBackendExecParams();
  // Add coordinator backend.
  const string coord_host_name = Substitute("host$0", 1);
  NetworkAddressPB coord_addr = MakeNetworkAddressPB(coord_host_name, 25000);
  const string coord_host = NetworkAddressPBToString(coord_addr);
  BackendExecParams& coord_exec_params =
      query_schedule->GetOrCreateBackendExecParams(coord_addr);
  coord_exec_params.pb->set_is_coord_backend(true);
  coord_exec_params.pb->set_thread_reservation(1);
  coord_exec_params.be_desc.set_admit_mem_limit(512 * MEGABYTE);
  coord_exec_params.be_desc.set_is_executor(false);
  // Add executor backend.
  const string exec_host_name = Substitute("host$0", 2);
  NetworkAddressPB exec_addr = MakeNetworkAddressPB(exec_host_name, 25000);
  const string exec_host = NetworkAddressPBToString(exec_addr);
  BackendExecParams& backend_exec_params =
      query_schedule->GetOrCreateBackendExecParams(exec_addr);
  backend_exec_params.pb->set_thread_reservation(1);
  backend_exec_params.be_desc.set_admit_mem_limit(GIGABYTE);
  backend_exec_params.be_desc.set_is_executor(true);
  string not_admitted_reason;
  // Test 1: coord's admit_mem_limit < executor's admit_mem_limit. Query should not
  // be rejected because query fits on both executor and coordinator. It should be
  // queued if there is not enough capacity.
  query_schedule->UpdateMemoryRequirements(pool_config);
  ASSERT_FALSE(admission_controller->RejectForSchedule(
      *query_schedule, pool_config, &not_admitted_reason));
  ASSERT_TRUE(admission_controller->HasAvailableMemResources(
      *query_schedule, pool_config, &not_admitted_reason));
  // Coord does not have enough available memory.
  admission_controller->host_stats_[coord_host].mem_reserved = 500 * MEGABYTE;
  ASSERT_FALSE(admission_controller->HasAvailableMemResources(
      *query_schedule, pool_config, &not_admitted_reason));
  EXPECT_STR_CONTAINS(not_admitted_reason,
      "Not enough memory available on host host1:25000. Needed 150.00 MB but only "
      "12.00 MB out of 512.00 MB was available.");
  not_admitted_reason.clear();
  // Neither coordinator or executor has enough available memory.
  admission_controller->host_stats_[exec_host].mem_reserved = 500 * MEGABYTE;
  ASSERT_FALSE(admission_controller->HasAvailableMemResources(
      *query_schedule, pool_config, &not_admitted_reason));
  EXPECT_STR_CONTAINS(not_admitted_reason,
      "Not enough memory available on host host2:25000. Needed 1.00 GB but only "
      "524.00 MB out of 1.00 GB was available.");
  not_admitted_reason.clear();
  // Executor does not have enough available memory.
  admission_controller->host_stats_[coord_host].mem_reserved = 0;
  ASSERT_FALSE(admission_controller->HasAvailableMemResources(
      *query_schedule, pool_config, &not_admitted_reason));
  EXPECT_STR_CONTAINS(not_admitted_reason,
      "Not enough memory available on host host2:25000. Needed 1.00 GB but only "
      "524.00 MB out of 1.00 GB was available.");
  not_admitted_reason.clear();
  admission_controller->host_stats_[exec_host].mem_reserved = 0;

  // Test 2: coord's admit_mem_limit < executor's admit_mem_limit. Query rejected because
  // coord's admit_mem_limit is less than mem_to_admit on the coord.
  // Re-using previous QuerySchedule object.
  coord_exec_params.be_desc.set_admit_mem_limit(100 * MEGABYTE);
  ASSERT_TRUE(admission_controller->RejectForSchedule(
      *query_schedule, pool_config, &not_admitted_reason));
  EXPECT_STR_CONTAINS(not_admitted_reason,
      "request memory needed 150.00 MB is greater than memory available for "
      "admission 100.00 MB of host1:25000");
  ASSERT_FALSE(admission_controller->HasAvailableMemResources(
      *query_schedule, pool_config, &not_admitted_reason));
  EXPECT_STR_CONTAINS(not_admitted_reason,
      "Not enough memory available on host host1:25000. Needed 150.00 MB but only "
      "100.00 MB out of 100.00 MB was available.");
  not_admitted_reason.clear();

  // Test 3: Make sure that coord and executors have separate checks on for whether their
  // mem limits can accommodate their respective initial reservations.
    query_schedule = MakeQuerySchedule(
      "default", 0, pool_config, 2, PER_EXEC_MEM_ESTIMATE, COORD_MEM_ESTIMATE, true);
    pool_config.__set_min_query_mem_limit(MEGABYTE); // to auto set mem_limit(s).
    query_schedule->UpdateMemoryRequirements(pool_config);
    query_schedule->set_largest_min_reservation(600 * MEGABYTE);
    query_schedule->set_coord_min_reservation(50 * MEGABYTE);
    ASSERT_TRUE(AdmissionController::CanAccommodateMaxInitialReservation(
        *query_schedule, pool_config, &not_admitted_reason));
    // Coordinator reservation doesn't fit.
    query_schedule->set_coord_min_reservation(200 * MEGABYTE);
    ASSERT_FALSE(AdmissionController::CanAccommodateMaxInitialReservation(
        *query_schedule, pool_config, &not_admitted_reason));
    EXPECT_STR_CONTAINS(not_admitted_reason, "minimum memory reservation is greater "
        "than memory available to the query for buffer reservations. Memory reservation "
        "needed given the current plan: 200.00 MB");
    // Neither coordinator or executor reservation fits.
    query_schedule->set_largest_min_reservation(GIGABYTE);
    ASSERT_FALSE(AdmissionController::CanAccommodateMaxInitialReservation(
        *query_schedule, pool_config, &not_admitted_reason));
    EXPECT_STR_CONTAINS(not_admitted_reason, "minimum memory reservation is greater "
        "than memory available to the query for buffer reservations. Memory reservation "
        "needed given the current plan: 1.00 GB");
    // Coordinator reservation doesn't fit.
    query_schedule->set_coord_min_reservation(50 * MEGABYTE);
    query_schedule->set_largest_min_reservation(GIGABYTE);
    ASSERT_FALSE(AdmissionController::CanAccommodateMaxInitialReservation(
        *query_schedule, pool_config, &not_admitted_reason));
    EXPECT_STR_CONTAINS(not_admitted_reason, "minimum memory reservation is greater "
        "than memory available to the query for buffer reservations. Memory reservation "
        "needed given the current plan: 1.00 GB");
}

} // end namespace impala
