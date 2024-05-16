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
#include "runtime/mem-tracker.h"
#include "runtime/test-env.h"
#include "scheduling/cluster-membership-test-util.h"
#include "scheduling/schedule-state.h"
#include "service/impala-server.h"
#include "testutil/gtest-util.h"
#include "util/metrics.h"
#include <regex>

// Access the flags that are defined in RequestPoolService.
DECLARE_string(fair_scheduler_allocation_path);
DECLARE_string(llama_site_path);
DECLARE_bool(clamp_query_mem_limit_backend_mem_limit);

namespace impala {
using namespace impala::test;

static const string IMPALA_HOME(getenv("IMPALA_HOME"));

// Queues used in the configuration files fair-scheduler-test2.xml and
// llama-site-test2.xml.
static const string QUEUE_A = "root.queueA";
static const string QUEUE_B = "root.queueB";
static const string QUEUE_C = "root.queueC";
static const string QUEUE_D = "root.queueD";

// Host names
static const string HOST_0 = "host0:25000";
static const string HOST_1 = "host1:25000";
static const string HOST_2 = "host2:25000";

// The default version of the heavy memory query list.
static std::vector<THeavyMemoryQuery> empty_heavy_memory_query_list;

// Default numbers used in few tests below.
static const int64_t DEFAULT_PER_EXEC_MEM_ESTIMATE = GIGABYTE;
static const int64_t DEFAULT_COORD_MEM_ESTIMATE = 150 * MEGABYTE;
static const int64_t ADMIT_MEM_LIMIT_BACKEND = GIGABYTE;
static const int64_t ADMIT_MEM_LIMIT_COORD = 512 * MEGABYTE;

/// Parent class for Admission Controller tests.
/// Common code and constants should go here.
/// These are single threaded tests so we access the internal data structures of
/// the AdmissionController object, and call methods such as 'GetPoolStats' without
/// taking the admission_ctrl_lock_ lock.
class AdmissionControllerTest : public testing::Test {
 protected:
  typedef std::pair<ExecutorGroup, BackendDescriptorPB> ExecutorGroupCoordinatorPair;

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

  /// Make a ScheduleState with dummy parameters that can be used to test admission and
  /// rejection in AdmissionController.
  ScheduleState* MakeScheduleState(string request_pool_name, int64_t mem_limit,
      TPoolConfig& config, const int num_hosts, const int64_t per_host_mem_estimate,
      const int64_t coord_mem_estimate, bool is_dedicated_coord,
      const string& executor_group = ImpalaServer::DEFAULT_EXECUTOR_GROUP_NAME,
      int64_t mem_limit_executors = -1, int64_t mem_limit_coordinators = -1) {
    DCHECK_GT(num_hosts, 0);
    TQueryExecRequest* request = pool_.Add(new TQueryExecRequest());
    request->query_ctx.request_pool = request_pool_name;
    request->__set_per_host_mem_estimate(per_host_mem_estimate);
    request->__set_dedicated_coord_mem_estimate(coord_mem_estimate);
    request->__set_stmt_type(TStmtType::QUERY);

    RuntimeProfile* profile = RuntimeProfile::Create(&pool_, "pool1");
    UniqueIdPB* query_id = pool_.Add(new UniqueIdPB()); // always 0,0
    TQueryOptions* query_options = pool_.Add(new TQueryOptions());
    if (mem_limit > -1) query_options->__set_mem_limit(mem_limit);
    if (mem_limit_executors > -1) {
      query_options->__set_mem_limit_executors(mem_limit_executors);
    }
    if (mem_limit_coordinators > -1) {
      query_options->__set_mem_limit_coordinators(mem_limit_coordinators);
    }
    ScheduleState* schedule_state =
        pool_.Add(new ScheduleState(*query_id, *request, *query_options, profile, true));
    schedule_state->set_executor_group(executor_group);
    SetHostsInScheduleState(*schedule_state, num_hosts, is_dedicated_coord);
    ExecutorGroupCoordinatorPair group = MakeExecutorConfig(*schedule_state);
    schedule_state->UpdateMemoryRequirements(config,
        group.second.admit_mem_limit(),
        group.first.GetPerExecutorMemLimitForAdmission());
    return schedule_state;
  }

  /// Same as previous MakeScheduleState with fewer input (more default params).
  ScheduleState* MakeScheduleState(string request_pool_name, TPoolConfig& config,
      const int num_hosts, const int per_host_mem_estimate,
      const string& executor_group = ImpalaServer::DEFAULT_EXECUTOR_GROUP_NAME) {
    return MakeScheduleState(request_pool_name, 0, config, num_hosts,
        per_host_mem_estimate, per_host_mem_estimate, false, executor_group);
  }

  /// Create ExecutorGroup and BackendDescriptor for Coordinator for the given
  /// ScheduleState.
  ExecutorGroupCoordinatorPair MakeExecutorConfig(const ScheduleState& schedule_state) {
    BackendDescriptorPB coord_desc;
    ExecutorGroup exec_group(schedule_state.executor_group());
    const PerBackendScheduleStates& per_backend_schedule_states =
        schedule_state.per_backend_schedule_states();
    bool has_coord_backend = false;
    for (const auto& itr : per_backend_schedule_states) {
      if (itr.second.exec_params->is_coord_backend()) {
        coord_desc = itr.second.be_desc;
        has_coord_backend = true;
      }
      if (itr.second.be_desc.is_executor()) {
        exec_group.AddExecutor(itr.second.be_desc);
      }
    }
    DCHECK(has_coord_backend) << "Query schedule must have a coordinator backend";
    return std::make_pair(exec_group, coord_desc);
  }

  /// Replace the per-backend hosts in the schedule with one having 'count' hosts.
  /// Note: no FInstanceExecParams are added so
  /// ScheduleState::UseDedicatedCoordEstimates() would consider this schedule as not
  /// having anything scheduled on the backend which would result in always returning true
  /// if a dedicated coordinator backend exists.
  void SetHostsInScheduleState(ScheduleState& schedule_state, const int count,
      bool is_dedicated_coord, int64_t min_mem_reservation_bytes = 0,
      int64_t admit_mem_limit = 4096L * MEGABYTE, int slots_to_use = 1,
      int slots_available = 8) {
    schedule_state.ClearBackendScheduleStates();
    for (int i = 0; i < count; ++i) {
      const string host_name = Substitute("host$0", i);
      NetworkAddressPB host_addr = MakeNetworkAddressPB(host_name, 25000);
      BackendScheduleState& backend_schedule_state =
          schedule_state.GetOrCreateBackendScheduleState(host_addr);
      backend_schedule_state.exec_params->set_min_mem_reservation_bytes(
          min_mem_reservation_bytes);
      backend_schedule_state.exec_params->set_slots_to_use(slots_to_use);
      backend_schedule_state.be_desc.set_admit_mem_limit(admit_mem_limit);
      backend_schedule_state.be_desc.set_admission_slots(slots_available);
      backend_schedule_state.be_desc.set_is_executor(true);
      backend_schedule_state.be_desc.set_ip_address(test::HostIdxToIpAddr(i));
      *backend_schedule_state.be_desc.mutable_address() = host_addr;
      if (i == 0) {
        // Add first element as the coordinator.
        backend_schedule_state.exec_params->set_is_coord_backend(true);
        backend_schedule_state.be_desc.set_is_executor(!is_dedicated_coord);
        backend_schedule_state.be_desc.set_is_coordinator(is_dedicated_coord);
      }
    }
  }

  /// Extract the host network addresses from 'schedule'.
  vector<NetworkAddressPB> GetHostAddrs(const ScheduleState& schedule_state) {
    vector<NetworkAddressPB> host_addrs;
    for (auto& backend_state : schedule_state.per_backend_schedule_states()) {
      host_addrs.push_back(backend_state.first);
    }
    return host_addrs;
  }

  /// Set the slots in use for all the hosts in 'host_addrs'.
  void SetSlotsInUse(AdmissionController* admission_controller,
      const vector<NetworkAddressPB>& host_addrs, int slots_in_use) {
    for (const NetworkAddressPB& host_addr : host_addrs) {
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

  // Form the hi part of a query Id which is an enumeration of the pool name.
  static int64_t FormQueryIdHi(const std::string& pool_name) {
    // Translate pool name to pool Id
    int pool_id = 0; // for pool QUEUE_A
    if (pool_name == QUEUE_B) {
      pool_id = 1;
    } else if (pool_name == QUEUE_C) {
      pool_id = 2;
    } else if (pool_name == QUEUE_D) {
      pool_id = 3;
    }
    return ((int64_t)pool_id);
  }

  /// Build a vector of THeavyMemoryQuery objects with "queries" queries. The id of each
  /// query is composed of the pool id and a sequence number. The memory consumed by the
  /// query is a number randomly chosen between 1MB and 20MB.
  static std::vector<THeavyMemoryQuery> MakeHeavyMemoryQueryList(
      const std::string pool, const int queries) {
    // Generate the query list
    std::vector<THeavyMemoryQuery> query_list;
    int64_t hi = FormQueryIdHi(pool);
    for (int i = 0; i < queries; i++) {
      THeavyMemoryQuery query;
      query.memory_consumed = (rand() % 20 + 1) * MEGABYTE;
      query.queryId.hi = hi;
      query.queryId.lo = i;
      query_list.emplace_back(query);
    }
    return query_list;
  }

  /// Build a TPoolStats object.
  static TPoolStats MakePoolStats(const int backend_mem_reserved,
      const int num_admitted_running, const int num_queued,
      const std::vector<THeavyMemoryQuery>& heavy_memory_queries =
          empty_heavy_memory_query_list,
      const int64_t min_memory_consumed = 0, const int64_t max_memory_consumed = 0,
      const int64_t total_memory_consumed = 0, const int64_t num_running = 0) {
    TPoolStats stats;
    stats.backend_mem_reserved = backend_mem_reserved;
    stats.num_admitted_running = num_admitted_running;
    stats.num_queued = num_queued;
    stats.heavy_memory_queries = heavy_memory_queries;
    stats.min_memory_consumed = min_memory_consumed;
    stats.max_memory_consumed = max_memory_consumed;
    stats.total_memory_consumed = total_memory_consumed;
    stats.num_running = num_running;
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
    return pool_.Add(new AdmissionController(cmm, nullptr, request_pool_service,
        metric_group, ExecEnv::GetInstance()->scheduler(),
        ExecEnv::GetInstance()->pool_mem_trackers(), *addr));
  }

  static void checkPoolDisabled(
      bool expected_result, int64_t max_requests, int64_t max_mem_resources) {
    TPoolConfig pool_config;
    pool_config.max_requests = max_requests;
    pool_config.max_mem_resources = max_mem_resources;
    ASSERT_EQ(expected_result, AdmissionController::PoolDisabled(pool_config));
  }

  void ResetMemConsumed(MemTracker* tracker) {
    tracker->consumption_->Set(0);
    for (MemTracker* child : tracker->child_trackers_) {
      ResetMemConsumed(child);
    }
  }

  ScheduleState* DedicatedCoordAdmissionSetup(TPoolConfig& test_pool_config,
      int64_t mem_limit, int64_t mem_limit_executors, int64_t mem_limit_coordinators) {
    AdmissionController* admission_controller = MakeAdmissionController();
    RequestPoolService* request_pool_service =
        admission_controller->request_pool_service_;

    Status status = request_pool_service->GetPoolConfig("default", &test_pool_config);
    if (!status.ok()) return nullptr;
    test_pool_config.__set_max_mem_resources(
        2 * GIGABYTE); // to enable memory based admission.

    // Set up a query schedule to test.
    ScheduleState* test_state = MakeScheduleState("default", mem_limit, test_pool_config,
        2, DEFAULT_PER_EXEC_MEM_ESTIMATE, DEFAULT_COORD_MEM_ESTIMATE, true,
        ImpalaServer::DEFAULT_EXECUTOR_GROUP_NAME, mem_limit_executors,
        mem_limit_coordinators);
    test_state->ClearBackendScheduleStates();
    // Add coordinator backend.
    const string coord_host_name = Substitute("host$0", 1);
    NetworkAddressPB coord_addr = MakeNetworkAddressPB(coord_host_name, 25000);
    const string coord_host = NetworkAddressPBToString(coord_addr);
    BackendScheduleState& coord_exec_params =
        test_state->GetOrCreateBackendScheduleState(coord_addr);
    coord_exec_params.exec_params->set_is_coord_backend(true);
    coord_exec_params.exec_params->set_thread_reservation(1);
    coord_exec_params.exec_params->set_slots_to_use(2);
    coord_exec_params.be_desc.set_admit_mem_limit(ADMIT_MEM_LIMIT_COORD);
    coord_exec_params.be_desc.set_admission_slots(8);
    coord_exec_params.be_desc.set_is_executor(false);
    coord_exec_params.be_desc.set_is_coordinator(true);
    coord_exec_params.be_desc.set_ip_address(test::HostIdxToIpAddr(1));
    // Add executor backend.
    const string exec_host_name = Substitute("host$0", 2);
    NetworkAddressPB exec_addr = MakeNetworkAddressPB(exec_host_name, 25000);
    const string exec_host = NetworkAddressPBToString(exec_addr);
    BackendScheduleState& backend_schedule_state =
        test_state->GetOrCreateBackendScheduleState(exec_addr);
    backend_schedule_state.exec_params->set_thread_reservation(1);
    backend_schedule_state.exec_params->set_slots_to_use(2);
    backend_schedule_state.be_desc.set_admit_mem_limit(ADMIT_MEM_LIMIT_BACKEND);
    backend_schedule_state.be_desc.set_admission_slots(8);
    backend_schedule_state.be_desc.set_is_executor(true);
    backend_schedule_state.be_desc.set_ip_address(test::HostIdxToIpAddr(2));

    ExecutorGroupCoordinatorPair group1 = MakeExecutorConfig(*test_state);
    test_state->UpdateMemoryRequirements(test_pool_config,
        group1.second.admit_mem_limit(),
        group1.first.GetPerExecutorMemLimitForAdmission());
    return test_state;
  }

  bool CanAccommodateMaxInitialReservation(const ScheduleState& state,
      const TPoolConfig& pool_cfg, string* mem_unavailable_reason) {
    return AdmissionController::CanAccommodateMaxInitialReservation(
        state, pool_cfg, mem_unavailable_reason);
  }

  void TestDedicatedCoordAdmissionRejection(TPoolConfig& test_pool_config,
      int64_t mem_limit, int64_t mem_limit_executors, int64_t mem_limit_coordinators) {
    ScheduleState* test_state = DedicatedCoordAdmissionSetup(
        test_pool_config, mem_limit, mem_limit_executors, mem_limit_coordinators);
    ASSERT_NE(nullptr, test_state);

    string not_admitted_reason = "--not set--";
    const bool mimic_old_behaviour = test_pool_config.min_query_mem_limit == 0
        && test_pool_config.max_query_mem_limit == 0;
    const bool backend_mem_unlimited = mimic_old_behaviour && mem_limit < 0
        && mem_limit_executors < 0 && mem_limit_coordinators < 0;

    if (backend_mem_unlimited) {
      ASSERT_EQ(-1, test_state->per_backend_mem_limit());
      ASSERT_EQ(-1, test_state->coord_backend_mem_limit());
    }
    // Both coordinator and executor reservation fits.
    test_state->set_largest_min_reservation(400 * MEGABYTE);
    test_state->set_coord_min_reservation(50 * MEGABYTE);
    bool can_accomodate = CanAccommodateMaxInitialReservation(
        *test_state, test_pool_config, &not_admitted_reason);
    EXPECT_STR_CONTAINS(not_admitted_reason, "--not set--");
    ASSERT_TRUE(can_accomodate);
    // Coordinator reservation doesn't fit.
    test_state->set_largest_min_reservation(400 * MEGABYTE);
    test_state->set_coord_min_reservation(700 * MEGABYTE);
    can_accomodate = CanAccommodateMaxInitialReservation(
        *test_state, test_pool_config, &not_admitted_reason);
    if (!backend_mem_unlimited) {
      EXPECT_STR_CONTAINS(not_admitted_reason,
          "minimum memory reservation is greater than memory available to the query for "
          "buffer reservations. Memory reservation needed given the current plan: 700.00 "
          "MB. Adjust the MEM_LIMIT option ");
      ASSERT_FALSE(can_accomodate);
    } else {
      ASSERT_TRUE(can_accomodate);
    }
    // Neither coordinator nor executor reservation fits.
    test_state->set_largest_min_reservation(GIGABYTE);
    test_state->set_coord_min_reservation(GIGABYTE);
    can_accomodate = CanAccommodateMaxInitialReservation(
        *test_state, test_pool_config, &not_admitted_reason);
    if (!backend_mem_unlimited) {
      EXPECT_STR_CONTAINS(not_admitted_reason,
          "minimum memory reservation is greater than memory available to the query for "
          "buffer reservations. Memory reservation needed given the current plan: 1.00 "
          "GB. Adjust the MEM_LIMIT option ");
      ASSERT_FALSE(can_accomodate);
    } else {
      ASSERT_TRUE(can_accomodate);
    }
    // Executor reservation doesn't fit.
    test_state->set_largest_min_reservation(900 * MEGABYTE);
    test_state->set_coord_min_reservation(50 * MEGABYTE);
    can_accomodate = CanAccommodateMaxInitialReservation(
        *test_state, test_pool_config, &not_admitted_reason);
    if (!backend_mem_unlimited) {
      EXPECT_STR_CONTAINS(not_admitted_reason,
          "minimum memory reservation is greater than memory available to the query for "
          "buffer reservations. Memory reservation needed given the current plan: 900.00 "
          "MB. Adjust the MEM_LIMIT option ");
      ASSERT_FALSE(can_accomodate);
    } else {
      ASSERT_TRUE(can_accomodate);
    }
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

  // Create a ScheduleState to run on QUEUE_C.
  ScheduleState* schedule_state = MakeScheduleState(QUEUE_C, config_c, 1, 64L * MEGABYTE);
  ExecutorGroupCoordinatorPair group = MakeExecutorConfig(*schedule_state);
  schedule_state->UpdateMemoryRequirements(config_c,
      group.second.admit_mem_limit(),
      group.first.GetPerExecutorMemLimitForAdmission());

  // Check that the AdmissionController initially has no data about other hosts.
  ASSERT_EQ(0, admission_controller->host_stats_.size());

  // Check that the query can be admitted.
  string not_admitted_reason;
  bool coordinator_resource_limited = false;
  ASSERT_TRUE(admission_controller->CanAdmitRequest(*schedule_state, config_c, true,
      &not_admitted_reason, nullptr, coordinator_resource_limited));
  ASSERT_FALSE(coordinator_resource_limited);

  // Create a ScheduleState just like 'schedule_state' to run on 3 hosts which can't be
  // admitted.
  ScheduleState* schedule_state_3_hosts =
      MakeScheduleState(QUEUE_C, config_c, 3, 64L * MEGABYTE);
  ASSERT_FALSE(admission_controller->CanAdmitRequest(*schedule_state_3_hosts, config_c,
      true, &not_admitted_reason, nullptr, coordinator_resource_limited));
  EXPECT_STR_CONTAINS(not_admitted_reason,
      "Not enough aggregate memory available in pool root.queueC with max mem "
      "resources 128.00 MB. Needed 192.00 MB but only 128.00 MB was available.");
  ASSERT_FALSE(coordinator_resource_limited);

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
  ASSERT_FALSE(admission_controller->CanAdmitRequest(*schedule_state, config_c, true,
      &not_admitted_reason, nullptr, coordinator_resource_limited));
  EXPECT_STR_CONTAINS(
      not_admitted_reason, "number of running queries 11 is at or over limit 10.");
  ASSERT_FALSE(coordinator_resource_limited);
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

  // Create a ScheduleState to run on QUEUE_D with per_host_mem_estimate of 30MB.
  int64_t host_count = 2;
  ScheduleState* schedule_state =
      MakeScheduleState(QUEUE_D, config_d, host_count, 30L * MEGABYTE);

  // Check that the query can be admitted.
  string not_admitted_reason;
  bool coordinator_resource_limited = false;
  ASSERT_TRUE(admission_controller->CanAdmitRequest(*schedule_state, config_d, true,
      &not_admitted_reason, nullptr, coordinator_resource_limited));
  ASSERT_FALSE(coordinator_resource_limited);

  // Tests that this query cannot be admitted.
  // Increasing the number of hosts pushes the aggregate memory required to admit this
  // query over the allowed limit.
  host_count = 15;
  ScheduleState* schedule_state15 =
      MakeScheduleState(QUEUE_D, config_d, host_count, 30L * MEGABYTE);
  ASSERT_FALSE(admission_controller->CanAdmitRequest(*schedule_state15, config_d, true,
      &not_admitted_reason, nullptr, coordinator_resource_limited));
  EXPECT_STR_CONTAINS(not_admitted_reason,
      "Not enough aggregate memory available in pool root.queueD with max mem resources "
      "400.00 MB. Needed 480.00 MB but only 400.00 MB was available.");
  ASSERT_FALSE(coordinator_resource_limited);

  // Create a ScheduleState to run on QUEUE_D with per_host_mem_estimate of 50MB.
  // which is going to be too much memory.
  host_count = 10;
  ScheduleState* schedule_state_10_fail =
      MakeScheduleState(QUEUE_D, config_d, host_count, 50L * MEGABYTE);

  ASSERT_FALSE(admission_controller->CanAdmitRequest(*schedule_state_10_fail, config_d,
      true, &not_admitted_reason, nullptr, coordinator_resource_limited));
  EXPECT_STR_CONTAINS(not_admitted_reason,
      "Not enough aggregate memory available in pool root.queueD with max mem resources "
      "400.00 MB. Needed 500.00 MB but only 400.00 MB was available.");
  ASSERT_FALSE(coordinator_resource_limited);
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

  // Create a ScheduleState to run on QUEUE_D on 12 hosts.
  int64_t host_count = 12;
  ScheduleState* schedule_state =
      MakeScheduleState(QUEUE_D, config_d, host_count, 30L * MEGABYTE);
  string not_admitted_reason;

  // Simulate that there are 2 queries queued.
  pool_stats->local_stats_.num_queued = 2;

  // Query can be admitted from queue...
  bool coordinator_resource_limited = false;
  ASSERT_TRUE(admission_controller->CanAdmitRequest(*schedule_state, config_d, true,
      &not_admitted_reason, nullptr, coordinator_resource_limited));
  ASSERT_FALSE(coordinator_resource_limited);
  // ... but same Query cannot be admitted directly.
  ASSERT_FALSE(admission_controller->CanAdmitRequest(*schedule_state, config_d, false,
      &not_admitted_reason, nullptr, coordinator_resource_limited));
  EXPECT_STR_CONTAINS(not_admitted_reason,
      "queue is not empty (size 2); queued queries are executed first");
  ASSERT_FALSE(coordinator_resource_limited);

  // Simulate that there are 7 queries already running.
  pool_stats->agg_num_running_ = 7;
  ASSERT_FALSE(admission_controller->CanAdmitRequest(*schedule_state, config_d, true,
      &not_admitted_reason, nullptr, coordinator_resource_limited));
  EXPECT_STR_CONTAINS(
      not_admitted_reason, "number of running queries 7 is at or over limit 6");
  ASSERT_FALSE(coordinator_resource_limited);
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

  // Create ScheduleStates to run on QUEUE_D on 12 hosts.
  // Running in both default and non-default executor groups is simulated.
  int64_t host_count = 12;
  int64_t slots_per_host = 16;
  int64_t slots_per_query = 4;
  ScheduleState* default_group_schedule =
      MakeScheduleState(QUEUE_D, config_d, host_count, 30L * MEGABYTE);
  ScheduleState* other_group_schedule =
      MakeScheduleState(QUEUE_D, config_d, host_count, 30L * MEGABYTE, "other_group");
  for (ScheduleState* schedule : {default_group_schedule, other_group_schedule}) {
    SetHostsInScheduleState(
        *schedule, 2, false, MEGABYTE, 200L * MEGABYTE, slots_per_query, slots_per_host);
  }

  // Coordinator only schedule at EMPTY_GROUP_NAME.
  ScheduleState* coordinator_only_schedule = MakeScheduleState(QUEUE_D, config_d,
      host_count, 30L * MEGABYTE, ClusterMembershipMgr::EMPTY_GROUP_NAME);
  SetHostsInScheduleState(*coordinator_only_schedule, 1, true, MEGABYTE, 200L * MEGABYTE,
      slots_per_query, slots_per_host);

  vector<NetworkAddressPB> host_addrs = GetHostAddrs(*default_group_schedule);
  string not_admitted_reason;
  bool coordinator_resource_limited = false;

  // Simulate that there are just enough slots free for the query on all hosts.
  SetSlotsInUse(admission_controller, host_addrs, slots_per_host - slots_per_query);

  // Enough slots are available so it can be admitted in both cases.
  ASSERT_TRUE(admission_controller->CanAdmitRequest(*default_group_schedule, config_d,
      true, &not_admitted_reason, nullptr, coordinator_resource_limited))
      << not_admitted_reason;
  ASSERT_FALSE(coordinator_resource_limited);
  ASSERT_TRUE(admission_controller->CanAdmitRequest(*other_group_schedule, config_d, true,
      &not_admitted_reason, nullptr, coordinator_resource_limited))
      << not_admitted_reason;
  ASSERT_FALSE(coordinator_resource_limited);

  // Simulate that almost all the slots are in use, which prevents admission in the
  // non-default group.
  SetSlotsInUse(admission_controller, host_addrs, slots_per_host - 1);

  ASSERT_TRUE(admission_controller->CanAdmitRequest(*default_group_schedule, config_d,
      true, &not_admitted_reason, nullptr, coordinator_resource_limited))
      << not_admitted_reason;
  ASSERT_FALSE(coordinator_resource_limited);
  ASSERT_FALSE(admission_controller->CanAdmitRequest(*other_group_schedule, config_d,
      true, &not_admitted_reason, nullptr, coordinator_resource_limited));
  EXPECT_STR_CONTAINS(not_admitted_reason,
      "Not enough admission control slots available on host host1:25000. Needed 4 "
      "slots but 15/16 are already in use.");
  ASSERT_FALSE(coordinator_resource_limited);
  // Assert that coordinator-only schedule also not admitted.
  ASSERT_FALSE(admission_controller->CanAdmitRequest(*coordinator_only_schedule, config_d,
      true, &not_admitted_reason, nullptr, coordinator_resource_limited));
  EXPECT_STR_CONTAINS(not_admitted_reason,
      "Not enough admission control slots available on host host0:25000. Needed 4 "
      "slots but 15/16 are already in use.");
  ASSERT_TRUE(coordinator_resource_limited);
}

/// Test CanAdmitRequest() ignore slots mechanism in default pool setup.
TEST_F(AdmissionControllerTest, CanAdmitRequestSlotsDefault) {
  // Pass the paths of the configuration files as command line flags.
  FLAGS_fair_scheduler_allocation_path = GetResourceFile("fair-scheduler-empty.xml");
  FLAGS_llama_site_path = GetResourceFile("llama-site-empty.xml");

  AdmissionController* admission_controller = MakeAdmissionController();
  RequestPoolService* request_pool_service = admission_controller->request_pool_service_;

  // Get the PoolConfig for "default-pool".
  TPoolConfig pool_config;
  ASSERT_OK(request_pool_service->GetPoolConfig(
      RequestPoolService::DEFAULT_POOL_NAME, &pool_config));

  // Create ScheduleStates to run on "default-pool" on 12 hosts.
  // Running both distributed and coordinator-only schedule.
  int64_t host_count = 12;
  int64_t slots_per_host = 16;
  int64_t slots_per_query = 4;
  // Distributed query schedule.
  ScheduleState* default_pool_schedule = MakeScheduleState(
      RequestPoolService::DEFAULT_POOL_NAME, pool_config, host_count, 30L * MEGABYTE);
  SetHostsInScheduleState(*default_pool_schedule, 2, false, MEGABYTE, 200L * MEGABYTE,
      slots_per_query, slots_per_host);
  // Coordinator only schedule at EMPTY_GROUP_NAME.
  ScheduleState* coordinator_only_schedule =
      MakeScheduleState(RequestPoolService::DEFAULT_POOL_NAME, pool_config, host_count,
          30L * MEGABYTE, ClusterMembershipMgr::EMPTY_GROUP_NAME);
  SetHostsInScheduleState(*coordinator_only_schedule, 1, true, MEGABYTE, 200L * MEGABYTE,
      slots_per_query, slots_per_host);

  vector<NetworkAddressPB> host_addrs = GetHostAddrs(*default_pool_schedule);
  string not_admitted_reason;
  bool coordinator_resource_limited = false;

  // Simulate that all slots are being used.
  // All schedules should be admitted.
  SetSlotsInUse(admission_controller, host_addrs, slots_per_host);
  ASSERT_TRUE(admission_controller->CanAdmitRequest(*default_pool_schedule, pool_config,
      true, &not_admitted_reason, nullptr, coordinator_resource_limited))
      << not_admitted_reason;
  ASSERT_FALSE(coordinator_resource_limited);
  ASSERT_EQ(coordinator_only_schedule->executor_group(),
      ClusterMembershipMgr::EMPTY_GROUP_NAME);
  ASSERT_TRUE(admission_controller->CanAdmitRequest(*coordinator_only_schedule,
      pool_config, true, &not_admitted_reason, nullptr, coordinator_resource_limited))
      << not_admitted_reason;
  ASSERT_FALSE(coordinator_resource_limited);
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

  // Create a ScheduleState to run on QUEUE_D with per_host_mem_estimate of 50MB
  // which is going to be too much memory.
  int host_count = 10;
  ScheduleState* schedule_state =
      MakeScheduleState(QUEUE_D, config_d, host_count, 50L * MEGABYTE);

  // Check messages from RejectForSchedule().
  string rejected_reason;
  ASSERT_TRUE(admission_controller->RejectForSchedule(
      *schedule_state, config_d, &rejected_reason));
  EXPECT_STR_CONTAINS(rejected_reason,
      "request memory needed 500.00 MB is greater than pool max mem resources 400.00 MB");

  // Adjust the ScheduleState to have minimum memory reservation of 45MB.
  // This will be rejected immediately as minimum memory reservation is too high.
  SetHostsInScheduleState(*schedule_state, host_count, false, 45L * MEGABYTE);
  string rejected_reserved_reason;
  ASSERT_TRUE(admission_controller->RejectForSchedule(
      *schedule_state, config_d, &rejected_reserved_reason));
  EXPECT_STR_CONTAINS(rejected_reserved_reason,
      "minimum memory reservation needed is greater than pool max mem resources. Pool "
      "max mem resources: 400.00 MB. Cluster-wide memory reservation needed: 450.00 MB. "
      "Increase the pool max mem resources.");

  // Adjust the ScheduleState to require many slots per node.
  // This will be rejected immediately in non-default executor groups
  // as the nodes do not have that many slots. Because of IMPALA-8757, this check
  // does not yet occur for the default executor group.
  SetHostsInScheduleState(*schedule_state, 2, false, MEGABYTE, 200L * MEGABYTE, 16, 4);
  string rejected_slots_reason;
  // Don't reject for default executor group.
  EXPECT_FALSE(admission_controller->RejectForSchedule(
      *schedule_state, config_d, &rejected_slots_reason))
      << rejected_slots_reason;
  // Reject for non-default executor group.
  ScheduleState* other_group_schedule = MakeScheduleState(
      QUEUE_D, config_d, host_count, 50L * MEGABYTE, "a_different_executor_group");
  SetHostsInScheduleState(
      *other_group_schedule, 2, false, MEGABYTE, 200L * MEGABYTE, 16, 4);
  EXPECT_TRUE(admission_controller->RejectForSchedule(
      *other_group_schedule, config_d, &rejected_slots_reason));
  EXPECT_STR_CONTAINS(rejected_slots_reason,
      "number of admission control slots needed "
      "(16) on backend 'host1:25000' is greater than total slots available 4. Reduce "
      "MT_DOP or MAX_FRAGMENT_INSTANCES_PER_NODE to less than 4 to ensure that the "
      "query can execute.");
  rejected_slots_reason = "";
  // Reduce mt_dop to ensure it can execute.
  SetHostsInScheduleState(
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
  max_to_dequeue = admission_controller->GetMaxToDequeue(queue_c, stats_c, config_c);
  ASSERT_EQ(0, max_to_dequeue);

  AdmissionController::PoolStats stats(admission_controller, "test");

  // First of all test non-scalable configuration.

  // Queue holds 10 with 10 running - cannot dequeue
  config.max_requests = 10;
  stats.local_stats_.num_queued = 10;
  stats.agg_num_queued_ = 20;
  stats.agg_num_running_ = 10;
  max_to_dequeue = admission_controller->GetMaxToDequeue(queue_c, &stats, config);
  ASSERT_EQ(0, max_to_dequeue);

  // Can only dequeue 1.
  stats.agg_num_running_ = 9;
  max_to_dequeue = admission_controller->GetMaxToDequeue(queue_c, &stats, config);
  ASSERT_EQ(1, max_to_dequeue);

  // There is space for 10 but it looks like there are 2 coordinators.
  stats.agg_num_running_ = 0;
  max_to_dequeue = admission_controller->GetMaxToDequeue(queue_c, &stats, config);
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

  // Create a ScheduleState to run on QUEUE_C.
  ScheduleState* schedule_state = MakeScheduleState(QUEUE_C, config_c, 1, 1000);

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
  pool_stats->AdmitQueryAndMemory(*schedule_state, false);
  ASSERT_EQ(1, pool_stats->agg_num_running());
  ASSERT_EQ(1, pool_stats->metrics()->agg_num_running->GetValue());
  int64_t mem_to_release = 0;
  for (auto& backend_state : schedule_state->per_backend_schedule_states()) {
    mem_to_release +=
        admission_controller->GetMemToAdmit(*schedule_state, backend_state.second);
  }
  pool_stats->ReleaseMem(mem_to_release);
  pool_stats->ReleaseQuery(0, false);
  CheckPoolStatsEmpty(pool_stats);
}

/// Test that PoolDisabled works
TEST_F(AdmissionControllerTest, PoolDisabled) {
  checkPoolDisabled(true, /* max_requests */ 0, /* max_mem_resources */ 0);
  checkPoolDisabled(false, /* max_requests */ 1, /* max_mem_resources */ 1);
  checkPoolDisabled(true, /* max_requests */ 0, /* max_mem_resources */ 1);
  checkPoolDisabled(true, /* max_requests */ 1, /* max_mem_resources */ 0);
}

// Basic tests of the ScheduleState object to confirm that a query with different
// coordinator and executor memory estimates calculates memory to admit correctly
// for various combinations of memory limit configurations.
TEST_F(AdmissionControllerTest, DedicatedCoordScheduleState) {
  AdmissionController* admission_controller = MakeAdmissionController();
  RequestPoolService* request_pool_service = admission_controller->request_pool_service_;

  const int64_t PER_EXEC_MEM_ESTIMATE = 512 * MEGABYTE;
  const int64_t COORD_MEM_ESTIMATE = 150 * MEGABYTE;
  TPoolConfig pool_config;
  ASSERT_OK(request_pool_service->GetPoolConfig("default", &pool_config));

  // For query only running on the coordinator, the per_backend_mem_to_admit should be 0.
  ScheduleState* schedule_state = MakeScheduleState(
      "default", 0, pool_config, 1, PER_EXEC_MEM_ESTIMATE, COORD_MEM_ESTIMATE, true);
  ExecutorGroupCoordinatorPair group = MakeExecutorConfig(*schedule_state);
  schedule_state->UpdateMemoryRequirements(pool_config,
      group.second.admit_mem_limit(),
      group.first.GetPerExecutorMemLimitForAdmission());
  ASSERT_EQ(MemLimitSourcePB::COORDINATOR_ONLY_OPTIMIZATION,
      schedule_state->per_backend_mem_to_admit_source());
  ASSERT_EQ(MemLimitSourcePB::QUERY_PLAN_DEDICATED_COORDINATOR_MEM_ESTIMATE,
      schedule_state->coord_backend_mem_to_admit_source());
  ASSERT_EQ(0, schedule_state->per_backend_mem_to_admit());
  ASSERT_EQ(COORD_MEM_ESTIMATE, schedule_state->coord_backend_mem_to_admit());

  // Make sure executors and coordinators are assigned memory to admit appropriately and
  // that the cluster memory to admitted is calculated correctly.
  schedule_state = MakeScheduleState(
      "default", 0, pool_config, 2, PER_EXEC_MEM_ESTIMATE, COORD_MEM_ESTIMATE, true);
  ASSERT_EQ(COORD_MEM_ESTIMATE, schedule_state->GetDedicatedCoordMemoryEstimate());
  ASSERT_EQ(PER_EXEC_MEM_ESTIMATE, schedule_state->GetPerExecutorMemoryEstimate());
  ExecutorGroupCoordinatorPair group1 = MakeExecutorConfig(*schedule_state);
  schedule_state->UpdateMemoryRequirements(pool_config,
      group1.second.admit_mem_limit(),
      group1.first.GetPerExecutorMemLimitForAdmission());
  ASSERT_EQ(MemLimitSourcePB::QUERY_PLAN_PER_HOST_MEM_ESTIMATE,
      schedule_state->per_backend_mem_to_admit_source());
  ASSERT_EQ(MemLimitSourcePB::QUERY_PLAN_DEDICATED_COORDINATOR_MEM_ESTIMATE,
      schedule_state->coord_backend_mem_to_admit_source());
  ASSERT_EQ(PER_EXEC_MEM_ESTIMATE, schedule_state->per_backend_mem_to_admit());
  ASSERT_EQ(COORD_MEM_ESTIMATE, schedule_state->coord_backend_mem_to_admit());
  ASSERT_EQ(-1, schedule_state->per_backend_mem_limit());
  ASSERT_EQ(-1, schedule_state->coord_backend_mem_limit());
  ASSERT_EQ(COORD_MEM_ESTIMATE + PER_EXEC_MEM_ESTIMATE,
      schedule_state->GetClusterMemoryToAdmit());

  // Set the min_query_mem_limit in pool_config. min_query_mem_limit should
  // not be enforced on the coordinator. Also ensure mem limits are set for both.
  schedule_state = MakeScheduleState(
      "default", 0, pool_config, 2, PER_EXEC_MEM_ESTIMATE, COORD_MEM_ESTIMATE, true);
  ASSERT_OK(request_pool_service->GetPoolConfig("default", &pool_config));
  pool_config.__set_min_query_mem_limit(700 * MEGABYTE);
  ExecutorGroupCoordinatorPair group2 = MakeExecutorConfig(*schedule_state);
  schedule_state->UpdateMemoryRequirements(pool_config,
      group2.second.admit_mem_limit(),
      group2.first.GetPerExecutorMemLimitForAdmission());
  ASSERT_EQ(MemLimitSourcePB::POOL_CONFIG_MIN_QUERY_MEM_LIMIT,
      schedule_state->per_backend_mem_to_admit_source());
  ASSERT_EQ(MemLimitSourcePB::QUERY_PLAN_DEDICATED_COORDINATOR_MEM_ESTIMATE,
      schedule_state->coord_backend_mem_to_admit_source());
  ASSERT_EQ(700 * MEGABYTE, schedule_state->per_backend_mem_to_admit());
  ASSERT_EQ(COORD_MEM_ESTIMATE, schedule_state->coord_backend_mem_to_admit());
  ASSERT_EQ(700 * MEGABYTE, schedule_state->per_backend_mem_limit());
  ASSERT_EQ(COORD_MEM_ESTIMATE, schedule_state->coord_backend_mem_limit());

  // Make sure coordinator's mem to admit is adjusted based on its own minimum mem
  // reservation.
  schedule_state = MakeScheduleState(
      "default", 0, pool_config, 2, PER_EXEC_MEM_ESTIMATE, COORD_MEM_ESTIMATE, true);
  int64_t coord_min_reservation = 200 * MEGABYTE;
  int64_t min_coord_mem_limit_required =
      ReservationUtil::GetMinMemLimitFromReservation(coord_min_reservation);
  pool_config.__set_min_query_mem_limit(700 * MEGABYTE);
  schedule_state->set_coord_min_reservation(200 * MEGABYTE);
  ExecutorGroupCoordinatorPair group3 = MakeExecutorConfig(*schedule_state);
  schedule_state->UpdateMemoryRequirements(pool_config,
      group3.second.admit_mem_limit(),
      group3.first.GetPerExecutorMemLimitForAdmission());
  ASSERT_EQ(MemLimitSourcePB::POOL_CONFIG_MIN_QUERY_MEM_LIMIT,
      schedule_state->per_backend_mem_to_admit_source());
  ASSERT_EQ(MemLimitSourcePB::ADJUSTED_DEDICATED_COORDINATOR_MEM_ESTIMATE,
      schedule_state->coord_backend_mem_to_admit_source());
  ASSERT_EQ(700 * MEGABYTE, schedule_state->per_backend_mem_to_admit());
  ASSERT_EQ(min_coord_mem_limit_required, schedule_state->coord_backend_mem_to_admit());
  ASSERT_EQ(700 * MEGABYTE, schedule_state->per_backend_mem_limit());
  ASSERT_EQ(min_coord_mem_limit_required, schedule_state->coord_backend_mem_limit());

  // Set mem_limit query option.
  ASSERT_OK(request_pool_service->GetPoolConfig("default", &pool_config));
  schedule_state = MakeScheduleState("default", GIGABYTE, pool_config, 2,
      PER_EXEC_MEM_ESTIMATE, COORD_MEM_ESTIMATE, true);
  ExecutorGroupCoordinatorPair group4 = MakeExecutorConfig(*schedule_state);
  schedule_state->UpdateMemoryRequirements(pool_config,
      group4.second.admit_mem_limit(),
      group4.first.GetPerExecutorMemLimitForAdmission());
  ASSERT_EQ(MemLimitSourcePB::QUERY_OPTION_MEM_LIMIT,
      schedule_state->per_backend_mem_to_admit_source());
  ASSERT_EQ(MemLimitSourcePB::QUERY_OPTION_MEM_LIMIT,
      schedule_state->coord_backend_mem_to_admit_source());
  ASSERT_EQ(GIGABYTE, schedule_state->per_backend_mem_to_admit());
  ASSERT_EQ(GIGABYTE, schedule_state->coord_backend_mem_to_admit());
  ASSERT_EQ(GIGABYTE, schedule_state->per_backend_mem_limit());
  ASSERT_EQ(GIGABYTE, schedule_state->coord_backend_mem_limit());

  // Set mem_limit query option and max_query_mem_limit. In this case, max_query_mem_limit
  // will be enforced on both coordinator and executor.
  schedule_state = MakeScheduleState("default", GIGABYTE, pool_config, 2,
      PER_EXEC_MEM_ESTIMATE, COORD_MEM_ESTIMATE, true);
  ASSERT_OK(request_pool_service->GetPoolConfig("default", &pool_config));
  pool_config.__set_max_query_mem_limit(700 * MEGABYTE);
  ExecutorGroupCoordinatorPair group5 = MakeExecutorConfig(*schedule_state);
  schedule_state->UpdateMemoryRequirements(pool_config,
      group5.second.admit_mem_limit(),
      group5.first.GetPerExecutorMemLimitForAdmission());
  ASSERT_EQ(MemLimitSourcePB::POOL_CONFIG_MAX_QUERY_MEM_LIMIT,
      schedule_state->per_backend_mem_to_admit_source());
  ASSERT_EQ(MemLimitSourcePB::POOL_CONFIG_MAX_QUERY_MEM_LIMIT,
      schedule_state->coord_backend_mem_to_admit_source());
  ASSERT_EQ(700 * MEGABYTE, schedule_state->per_backend_mem_to_admit());
  ASSERT_EQ(700 * MEGABYTE, schedule_state->coord_backend_mem_to_admit());
  ASSERT_EQ(700 * MEGABYTE, schedule_state->per_backend_mem_limit());
  ASSERT_EQ(700 * MEGABYTE, schedule_state->coord_backend_mem_limit());
}

// Test admission decisions for clusters with dedicated coordinators, where different
// amounts of memory should be admitted on coordinators and executors.
TEST_F(AdmissionControllerTest, DedicatedCoordAdmissionChecks) {
  AdmissionController* admission_controller = MakeAdmissionController();
  RequestPoolService* request_pool_service = admission_controller->request_pool_service_;

  TPoolConfig pool_config;
  ASSERT_OK(request_pool_service->GetPoolConfig("default", &pool_config));
  pool_config.__set_max_mem_resources(2 * GIGABYTE); // to enable memory based admission.

  // Set up a query schedule to test.
  const int64_t PER_EXEC_MEM_ESTIMATE = GIGABYTE;
  const int64_t COORD_MEM_ESTIMATE = 150 * MEGABYTE;
  ScheduleState* schedule_state = MakeScheduleState(
      "default", 0, pool_config, 2, PER_EXEC_MEM_ESTIMATE, COORD_MEM_ESTIMATE, true);
  schedule_state->ClearBackendScheduleStates();
  // Add coordinator backend.
  const string coord_host_name = Substitute("host$0", 1);
  NetworkAddressPB coord_addr = MakeNetworkAddressPB(coord_host_name, 25000);
  const string coord_host = NetworkAddressPBToString(coord_addr);
  BackendScheduleState& coord_exec_params =
      schedule_state->GetOrCreateBackendScheduleState(coord_addr);
  coord_exec_params.exec_params->set_is_coord_backend(true);
  coord_exec_params.exec_params->set_thread_reservation(1);
  coord_exec_params.exec_params->set_slots_to_use(2);
  coord_exec_params.be_desc.set_admit_mem_limit(512 * MEGABYTE);
  coord_exec_params.be_desc.set_admission_slots(8);
  coord_exec_params.be_desc.set_is_executor(false);
  coord_exec_params.be_desc.set_is_coordinator(true);
  coord_exec_params.be_desc.set_ip_address(test::HostIdxToIpAddr(1));
  // Add executor backend.
  const string exec_host_name = Substitute("host$0", 2);
  NetworkAddressPB exec_addr = MakeNetworkAddressPB(exec_host_name, 25000);
  const string exec_host = NetworkAddressPBToString(exec_addr);
  BackendScheduleState& backend_schedule_state =
      schedule_state->GetOrCreateBackendScheduleState(exec_addr);
  backend_schedule_state.exec_params->set_thread_reservation(1);
  backend_schedule_state.exec_params->set_slots_to_use(2);
  backend_schedule_state.be_desc.set_admit_mem_limit(GIGABYTE);
  backend_schedule_state.be_desc.set_admission_slots(8);
  backend_schedule_state.be_desc.set_is_executor(true);
  backend_schedule_state.be_desc.set_ip_address(test::HostIdxToIpAddr(2));
  string not_admitted_reason;
  bool coordinator_resource_limited = false;
  // Test 1: coord's admit_mem_limit < executor's admit_mem_limit. Query should not
  // be rejected because query fits on both executor and coordinator. It should be
  // queued if there is not enough capacity.
  ExecutorGroupCoordinatorPair group = MakeExecutorConfig(*schedule_state);
  schedule_state->UpdateMemoryRequirements(pool_config,
      group.second.admit_mem_limit(),
      group.first.GetPerExecutorMemLimitForAdmission());
  ASSERT_FALSE(admission_controller->RejectForSchedule(
      *schedule_state, pool_config, &not_admitted_reason));
  ASSERT_TRUE(admission_controller->HasAvailableMemResources(
      *schedule_state, pool_config, &not_admitted_reason, coordinator_resource_limited));
  ASSERT_FALSE(coordinator_resource_limited);
  // Coord does not have enough available memory.
  admission_controller->host_stats_[coord_host].mem_reserved = 500 * MEGABYTE;
  ASSERT_FALSE(admission_controller->HasAvailableMemResources(
      *schedule_state, pool_config, &not_admitted_reason, coordinator_resource_limited));
  EXPECT_STR_CONTAINS(not_admitted_reason,
      "Not enough memory available on host host1:25000. Needed 150.00 MB but only "
      "12.00 MB out of 512.00 MB was available.");
  ASSERT_TRUE(coordinator_resource_limited);
  coordinator_resource_limited = false;
  not_admitted_reason.clear();
  // Neither coordinator or executor has enough available memory.
  admission_controller->host_stats_[exec_host].mem_reserved = 500 * MEGABYTE;
  ASSERT_FALSE(admission_controller->HasAvailableMemResources(
      *schedule_state, pool_config, &not_admitted_reason, coordinator_resource_limited));
  EXPECT_STR_CONTAINS(not_admitted_reason,
      "Not enough memory available on host host2:25000. Needed 1.00 GB but only "
      "524.00 MB out of 1.00 GB was available.");
  ASSERT_FALSE(coordinator_resource_limited);
  not_admitted_reason.clear();
  // Executor does not have enough available memory.
  admission_controller->host_stats_[coord_host].mem_reserved = 0;
  ASSERT_FALSE(admission_controller->HasAvailableMemResources(
      *schedule_state, pool_config, &not_admitted_reason, coordinator_resource_limited));
  EXPECT_STR_CONTAINS(not_admitted_reason,
      "Not enough memory available on host host2:25000. Needed 1.00 GB but only "
      "524.00 MB out of 1.00 GB was available.");
  ASSERT_FALSE(coordinator_resource_limited);
  not_admitted_reason.clear();
  admission_controller->host_stats_[exec_host].mem_reserved = 0;

  // Test 2: coord's admit_mem_limit < executor's admit_mem_limit. Query rejected because
  // coord's admit_mem_limit is less than mem_to_admit on the coord.
  // Re-using previous ScheduleState object.
  FLAGS_clamp_query_mem_limit_backend_mem_limit = false;
  coord_exec_params.be_desc.set_admit_mem_limit(100 * MEGABYTE);
  ASSERT_TRUE(admission_controller->RejectForSchedule(
      *schedule_state, pool_config, &not_admitted_reason));
  EXPECT_STR_CONTAINS(not_admitted_reason,
      "request memory needed 150.00 MB is greater than memory available for "
      "admission 100.00 MB of host1:25000");
  ASSERT_FALSE(admission_controller->HasAvailableMemResources(
      *schedule_state, pool_config, &not_admitted_reason, coordinator_resource_limited));
  EXPECT_STR_CONTAINS(not_admitted_reason,
      "Not enough memory available on host host1:25000. Needed 150.00 MB but only "
      "100.00 MB out of 100.00 MB was available.");
  ASSERT_TRUE(coordinator_resource_limited);
  coordinator_resource_limited = false;
  not_admitted_reason.clear();
  FLAGS_clamp_query_mem_limit_backend_mem_limit = true;

  // Test 3: Check HasAvailableSlots by simulating slots being in use.
  ASSERT_TRUE(admission_controller->HasAvailableSlots(
      *schedule_state, pool_config, &not_admitted_reason, coordinator_resource_limited));
  ASSERT_FALSE(coordinator_resource_limited);

  // Set the number of slots in use on the coordinator high enough to prohibit further
  // queries.
  admission_controller->host_stats_["host1:25000"].slots_in_use = 7;
  ASSERT_FALSE(admission_controller->HasAvailableSlots(
      *schedule_state, pool_config, &not_admitted_reason, coordinator_resource_limited));
  EXPECT_STR_CONTAINS(not_admitted_reason,
      "Not enough admission control slots available "
      "on host host1:25000. Needed 2 slots but 7/8 are already in use.");
  ASSERT_TRUE(coordinator_resource_limited);
  coordinator_resource_limited = false;

  // Now instead set the executor to have too many slots in use
  admission_controller->host_stats_["host2:25000"].slots_in_use = 7;
  admission_controller->host_stats_["host1:25000"].slots_in_use = 0;
  ASSERT_FALSE(admission_controller->HasAvailableSlots(
      *schedule_state, pool_config, &not_admitted_reason, coordinator_resource_limited));
  EXPECT_STR_CONTAINS(not_admitted_reason,
                      "Not enough admission control slots available "
                      "on host host2:25000. Needed 2 slots but 7/8 are already in use.");
  ASSERT_FALSE(coordinator_resource_limited);
}

// Test rejection with pool's mem limit clamp set to 0 and no MEM_LIMIT set.
TEST_F(AdmissionControllerTest, DedicatedCoordAdmissionZeroPoolMemLimit) {
  TPoolConfig pool_config;
  pool_config.__set_min_query_mem_limit(0);
  pool_config.__set_max_query_mem_limit(0);
  TestDedicatedCoordAdmissionRejection(pool_config, -1, -1, -1);
}

// Test rejection with pool's mem limit clamp set to non default value.
TEST_F(AdmissionControllerTest, DedicatedCoordAdmission1MBPoolMemLimit) {
  TPoolConfig pool_config;
  pool_config.__set_min_query_mem_limit(MEGABYTE);
  pool_config.__set_max_query_mem_limit(MEGABYTE);
  TestDedicatedCoordAdmissionRejection(pool_config, -1, -1, -1);
}

// Test rejection with pool's mem limit clamp disabled and no MEM_LIMIT set.
TEST_F(AdmissionControllerTest, DedicatedCoordAdmissionDisabledPoolMemLimit) {
  TPoolConfig pool_config;
  pool_config.__set_min_query_mem_limit(MEGABYTE);
  pool_config.__set_max_query_mem_limit(GIGABYTE);
  pool_config.__set_clamp_mem_limit_query_option(false);
  TestDedicatedCoordAdmissionRejection(pool_config, -1, -1, -1);
}

// Test rejection with MEM_LIMIT set to non default value.
TEST_F(AdmissionControllerTest, DedicatedCoordAdmissionAtCoordAdmitMemLimit) {
  TPoolConfig pool_config;
  pool_config.__set_min_query_mem_limit(0);
  pool_config.__set_max_query_mem_limit(0);
  TestDedicatedCoordAdmissionRejection(pool_config, ADMIT_MEM_LIMIT_COORD, -1, -1);
}

// Test rejection with pool's mem limit clamp and MEM_LIMIT set to non default value.
TEST_F(AdmissionControllerTest, DedicatedCoordAdmissionWithPoolAndMemLimit) {
  TPoolConfig pool_config;
  pool_config.__set_min_query_mem_limit(MEGABYTE);
  pool_config.__set_max_query_mem_limit(MEGABYTE);
  TestDedicatedCoordAdmissionRejection(pool_config, ADMIT_MEM_LIMIT_COORD, -1, -1);
}

// Test that memory clamping is ignored if clamp_mem_limit_query_option is false.
TEST_F(AdmissionControllerTest, DedicatedCoordAdmissionIgnoreMemClamp) {
  TPoolConfig pool_config;
  string not_admitted_reason = "--not set--";
  pool_config.__set_min_query_mem_limit(2 * MEGABYTE);
  pool_config.__set_max_query_mem_limit(2 * MEGABYTE);
  pool_config.__set_clamp_mem_limit_query_option(false);
  ScheduleState* schedule_state = MakeScheduleState("default", MEGABYTE, pool_config, 2,
      DEFAULT_PER_EXEC_MEM_ESTIMATE, DEFAULT_COORD_MEM_ESTIMATE, true);
  schedule_state->set_largest_min_reservation(600 * MEGABYTE);
  schedule_state->set_coord_min_reservation(50 * MEGABYTE);
  ASSERT_EQ(MemLimitSourcePB::QUERY_OPTION_MEM_LIMIT,
      schedule_state->per_backend_mem_to_admit_source());
  ASSERT_EQ(MEGABYTE, schedule_state->per_backend_mem_to_admit());
  ASSERT_EQ(MEGABYTE, schedule_state->per_backend_mem_limit());
  ASSERT_EQ(MemLimitSourcePB::QUERY_OPTION_MEM_LIMIT,
      schedule_state->per_backend_mem_to_admit_source());
  ASSERT_EQ(MEGABYTE, schedule_state->coord_backend_mem_to_admit());
  ASSERT_EQ(MEGABYTE, schedule_state->coord_backend_mem_limit());
  bool can_accomodate = CanAccommodateMaxInitialReservation(
      *schedule_state, pool_config, &not_admitted_reason);
  EXPECT_STR_CONTAINS(not_admitted_reason,
      "minimum memory reservation is greater than memory available to the query for "
      "buffer reservations. Memory reservation needed given the current plan: 600.00 MB."
      " Adjust the MEM_LIMIT option ");
  ASSERT_FALSE(can_accomodate);
}

// Test rejection due to min-query-mem-limit clamping.
TEST_F(AdmissionControllerTest, DedicatedCoordAdmissionExceedMinMemClamp) {
  TPoolConfig pool_config;
  string not_admitted_reason = "--not set--";
  pool_config.__set_min_query_mem_limit(2 * MEGABYTE);
  pool_config.__set_max_query_mem_limit(2 * MEGABYTE);
  ScheduleState* schedule_state = MakeScheduleState("default", MEGABYTE, pool_config, 2,
      DEFAULT_PER_EXEC_MEM_ESTIMATE, DEFAULT_COORD_MEM_ESTIMATE, true);
  schedule_state->set_largest_min_reservation(600 * MEGABYTE);
  schedule_state->set_coord_min_reservation(50 * MEGABYTE);
  ASSERT_EQ(MemLimitSourcePB::POOL_CONFIG_MIN_QUERY_MEM_LIMIT,
      schedule_state->per_backend_mem_to_admit_source());
  ASSERT_EQ(2 * MEGABYTE, schedule_state->per_backend_mem_to_admit());
  ASSERT_EQ(2 * MEGABYTE, schedule_state->per_backend_mem_limit());
  ASSERT_EQ(MemLimitSourcePB::POOL_CONFIG_MIN_QUERY_MEM_LIMIT,
      schedule_state->per_backend_mem_to_admit_source());
  ASSERT_EQ(2 * MEGABYTE, schedule_state->coord_backend_mem_to_admit());
  ASSERT_EQ(2 * MEGABYTE, schedule_state->coord_backend_mem_limit());
  bool can_accomodate = CanAccommodateMaxInitialReservation(
      *schedule_state, pool_config, &not_admitted_reason);
  EXPECT_STR_CONTAINS(not_admitted_reason,
      "minimum memory reservation is greater than memory available to the query for "
      "buffer reservations. Memory reservation needed given the current plan: 600.00 MB."
      " Adjust the impala.admission-control.min-query-mem-limit of request pool "
      "'default' ");
  ASSERT_FALSE(can_accomodate);
}

// Test rejection due to max-query-mem-limit clamping.
TEST_F(AdmissionControllerTest, DedicatedCoordAdmissionExceedMaxMemClamp) {
  TPoolConfig pool_config;
  string not_admitted_reason = "--not set--";
  pool_config.__set_min_query_mem_limit(2 * MEGABYTE);
  pool_config.__set_max_query_mem_limit(3 * MEGABYTE);
  ScheduleState* schedule_state = MakeScheduleState("default", 4 * MEGABYTE, pool_config,
      2, DEFAULT_PER_EXEC_MEM_ESTIMATE, DEFAULT_COORD_MEM_ESTIMATE, true);
  schedule_state->set_largest_min_reservation(600 * MEGABYTE);
  schedule_state->set_coord_min_reservation(50 * MEGABYTE);
  ASSERT_EQ(MemLimitSourcePB::POOL_CONFIG_MAX_QUERY_MEM_LIMIT,
      schedule_state->per_backend_mem_to_admit_source());
  ASSERT_EQ(3 * MEGABYTE, schedule_state->per_backend_mem_to_admit());
  ASSERT_EQ(3 * MEGABYTE, schedule_state->per_backend_mem_limit());
  ASSERT_EQ(MemLimitSourcePB::POOL_CONFIG_MAX_QUERY_MEM_LIMIT,
      schedule_state->per_backend_mem_to_admit_source());
  ASSERT_EQ(3 * MEGABYTE, schedule_state->coord_backend_mem_to_admit());
  ASSERT_EQ(3 * MEGABYTE, schedule_state->coord_backend_mem_limit());
  bool can_accomodate = CanAccommodateMaxInitialReservation(
      *schedule_state, pool_config, &not_admitted_reason);
  EXPECT_STR_CONTAINS(not_admitted_reason,
      "minimum memory reservation is greater than memory available to the query for "
      "buffer reservations. Memory reservation needed given the current plan: 600.00 MB."
      " Adjust the impala.admission-control.max-query-mem-limit of request pool "
      "'default' ");
  ASSERT_FALSE(can_accomodate);
}

// Test rejection due to MEM_LIMIT_EXECUTORS exceeded.
TEST_F(AdmissionControllerTest, DedicatedCoordAdmissionExceedMemLimitExecutors) {
  FLAGS_clamp_query_mem_limit_backend_mem_limit = false;
  TPoolConfig pool_config;
  pool_config.__set_min_query_mem_limit(MEGABYTE);
  pool_config.__set_max_query_mem_limit(ADMIT_MEM_LIMIT_BACKEND);
  string not_admitted_reason = "--not set--";
  ScheduleState* schedule_state =
      DedicatedCoordAdmissionSetup(pool_config, -1, 3 * GIGABYTE, -1);
  ASSERT_NE(nullptr, schedule_state);
  schedule_state->set_largest_min_reservation(4 * GIGABYTE);
  schedule_state->set_coord_min_reservation(50 * MEGABYTE);
  ASSERT_EQ(MemLimitSourcePB::QUERY_OPTION_MEM_LIMIT_EXECUTORS,
      schedule_state->per_backend_mem_to_admit_source());
  ASSERT_EQ(3 * GIGABYTE, schedule_state->per_backend_mem_to_admit());
  ASSERT_EQ(3 * GIGABYTE, schedule_state->per_backend_mem_limit());
  ASSERT_EQ(MemLimitSourcePB::QUERY_PLAN_DEDICATED_COORDINATOR_MEM_ESTIMATE,
      schedule_state->coord_backend_mem_to_admit_source());
  ASSERT_EQ(DEFAULT_COORD_MEM_ESTIMATE, schedule_state->coord_backend_mem_to_admit());
  ASSERT_EQ(DEFAULT_COORD_MEM_ESTIMATE, schedule_state->coord_backend_mem_limit());
  bool can_accomodate = CanAccommodateMaxInitialReservation(
      *schedule_state, pool_config, &not_admitted_reason);
  EXPECT_STR_CONTAINS(not_admitted_reason,
      "minimum memory reservation is greater than memory available to the query for "
      "buffer reservations. Memory reservation needed given the current plan: 4.00 GB. "
      "Adjust the MEM_LIMIT_EXECUTORS option ");
  ASSERT_FALSE(can_accomodate);
}

// Test rejection due to MEM_LIMIT_COORDINATORS exceeded.
TEST_F(AdmissionControllerTest, DedicatedCoordAdmissionExceedMemLimitCoordinators) {
  FLAGS_clamp_query_mem_limit_backend_mem_limit = false;
  TPoolConfig pool_config;
  pool_config.__set_min_query_mem_limit(MEGABYTE);
  pool_config.__set_max_query_mem_limit(ADMIT_MEM_LIMIT_BACKEND);
  string not_admitted_reason = "--not set--";
  ScheduleState* schedule_state =
      DedicatedCoordAdmissionSetup(pool_config, -1, -1, 3 * GIGABYTE);
  ASSERT_NE(nullptr, schedule_state);
  schedule_state->set_largest_min_reservation(600 * MEGABYTE);
  schedule_state->set_coord_min_reservation(4 * GIGABYTE);
  ASSERT_EQ(MemLimitSourcePB::QUERY_PLAN_PER_HOST_MEM_ESTIMATE,
      schedule_state->per_backend_mem_to_admit_source());
  ASSERT_EQ(DEFAULT_PER_EXEC_MEM_ESTIMATE, schedule_state->per_backend_mem_to_admit());
  ASSERT_EQ(DEFAULT_PER_EXEC_MEM_ESTIMATE, schedule_state->per_backend_mem_limit());
  ASSERT_EQ(MemLimitSourcePB::QUERY_OPTION_MEM_LIMIT_COORDINATORS,
      schedule_state->coord_backend_mem_to_admit_source());
  ASSERT_EQ(3 * GIGABYTE, schedule_state->coord_backend_mem_to_admit());
  ASSERT_EQ(3 * GIGABYTE, schedule_state->coord_backend_mem_limit());
  bool can_accomodate = CanAccommodateMaxInitialReservation(
      *schedule_state, pool_config, &not_admitted_reason);
  EXPECT_STR_CONTAINS(not_admitted_reason,
      "minimum memory reservation is greater than memory available to the query for "
      "buffer reservations. Memory reservation needed given the current plan: 4.00 GB. "
      "Adjust the MEM_LIMIT_COORDINATORS option ");
  ASSERT_FALSE(can_accomodate);
}

// Test rejection due to system memory limit.
TEST_F(AdmissionControllerTest, DedicatedCoordAdmissionExceedSystemMem) {
  FLAGS_clamp_query_mem_limit_backend_mem_limit = true;
  TPoolConfig pool_config;
  pool_config.__set_min_query_mem_limit(MEGABYTE);
  pool_config.__set_max_query_mem_limit(3 * ADMIT_MEM_LIMIT_BACKEND);
  string not_admitted_reason = "--not set--";
  ScheduleState* schedule_state = MakeScheduleState("default", -1, pool_config, 2,
      2 * ADMIT_MEM_LIMIT_BACKEND, 2 * ADMIT_MEM_LIMIT_COORD, true);
  schedule_state->set_largest_min_reservation(2 * ADMIT_MEM_LIMIT_BACKEND);
  schedule_state->set_coord_min_reservation(50 * MEGABYTE);
  schedule_state->UpdateMemoryRequirements(
      pool_config, ADMIT_MEM_LIMIT_COORD, ADMIT_MEM_LIMIT_BACKEND);
  ASSERT_EQ(MemLimitSourcePB::HOST_MEM_TRACKER_LIMIT,
      schedule_state->per_backend_mem_to_admit_source());
  ASSERT_EQ(ADMIT_MEM_LIMIT_BACKEND, schedule_state->per_backend_mem_to_admit());
  ASSERT_EQ(ADMIT_MEM_LIMIT_BACKEND, schedule_state->per_backend_mem_limit());
  ASSERT_EQ(MemLimitSourcePB::HOST_MEM_TRACKER_LIMIT,
      schedule_state->coord_backend_mem_to_admit_source());
  ASSERT_EQ(ADMIT_MEM_LIMIT_COORD, schedule_state->coord_backend_mem_to_admit());
  ASSERT_EQ(ADMIT_MEM_LIMIT_COORD, schedule_state->coord_backend_mem_limit());
  bool can_accomodate = CanAccommodateMaxInitialReservation(
      *schedule_state, pool_config, &not_admitted_reason);
  EXPECT_STR_CONTAINS(not_admitted_reason,
      "minimum memory reservation is greater than memory available to the query for "
      "buffer reservations. Memory reservation needed given the current plan: 2.00 GB. "
      "Adjust the system memory or the CGroup memory limit ");
  ASSERT_FALSE(can_accomodate);
}

/// Test that AdmissionController can identify 5 queries with top memory consumption
/// from 4 pools. Each pool holds a number of queries with different memory consumptions.
/// Run the test only when undefined behavior sanitizer check is off to avoid core
/// generated from std::regex_match().
TEST_F(AdmissionControllerTest, TopNQueryCheck) {
#ifndef UNDEFINED_SANITIZER
  // Pass the paths of the configuration files as command line flags
  FLAGS_fair_scheduler_allocation_path = GetResourceFile("fair-scheduler-test2.xml");
  FLAGS_llama_site_path = GetResourceFile("llama-site-test2.xml");

  AdmissionController* admission_controller = MakeAdmissionController();
  RequestPoolService* request_pool_service = admission_controller->request_pool_service_;

  // A vector of 4 specs. Each spec defines a pool with a number of queries running
  // in the pool.
  std::vector<std::tuple<std::string, TPoolConfig, int>> resource_pools(4);

  // Establish the pool names
  std::get<0>(resource_pools[0]) = QUEUE_A;
  std::get<0>(resource_pools[1]) = QUEUE_B;
  std::get<0>(resource_pools[2]) = QUEUE_C;
  std::get<0>(resource_pools[3]) = QUEUE_D;

  // Setup the pool config
  for (int i = 0; i < 4; i++) {
    const std::string& pool_name = std::get<0>(resource_pools[i]);
    TPoolConfig& pool_config = std::get<1>(resource_pools[i]);
    ASSERT_OK(request_pool_service->GetPoolConfig(pool_name, &pool_config));
  }

  // Set the number of queries to run in each pool
  std::get<2>(resource_pools[0]) = 5;
  std::get<2>(resource_pools[1]) = 10;
  std::get<2>(resource_pools[2]) = 20;
  std::get<2>(resource_pools[3]) = 20;

  // Set the seed for the random generator used below to generate
  // memory needed for each query.
  srand(19);

  MemTracker* pool_mem_tracker = nullptr;

  // Process each of the 4 pools.
  for (int i = 0; i < 4; i++) {
    // Get the parameters of the pool.
    const std::string& pool_name = std::get<0>(resource_pools[i]);
    TPoolConfig& pool_config = std::get<1>(resource_pools[i]);
    int num_queries = std::get<2>(resource_pools[i]);

    // For all queries in the pool, fabricate the hi part of the query Id.
    TUniqueId id;
    id.hi = FormQueryIdHi(pool_name);

    // Create a number of queries to run inside the pool.
    for (int j = 0; j < num_queries; j++) {
      // For each query, fabricate the lo part of the query Id.
      id.lo = j;
      // Next create a ScheduleState that runs on this host with per host
      // memory limit as a random number between 1MB and 10MB.
      long per_host_mem_estimate = (rand() % 10 + 1) * MEGABYTE;
      ScheduleState* schedule_state =
          MakeScheduleState(pool_name, pool_config, 1, per_host_mem_estimate);
      ExecutorGroupCoordinatorPair group = MakeExecutorConfig(*schedule_state);
      schedule_state->UpdateMemoryRequirements(pool_config,
          group.second.admit_mem_limit(),
          group.first.GetPerExecutorMemLimitForAdmission());
      // Admit the query to the pool.
      string not_admitted_reason;
      bool coordinator_resource_limited = false;
      ASSERT_TRUE(admission_controller->CanAdmitRequest(*schedule_state, pool_config,
          true, &not_admitted_reason, nullptr, coordinator_resource_limited));
      ASSERT_FALSE(coordinator_resource_limited);
      // Create a query memory tracker for the query and set the memory consumption
      // as per_host_mem_estimate.
      int64_t mem_consumed = per_host_mem_estimate;
      MemTracker::CreateQueryMemTracker(id, -1 /*mem_limit*/, pool_name, &pool_)
          ->Consume(mem_consumed);
    }
    // Get the pool mem tracker.
    pool_mem_tracker =
        ExecEnv::GetInstance()->pool_mem_trackers()->GetRequestPoolMemTracker(
            pool_name, false);
    ASSERT_TRUE(pool_mem_tracker);
    // Create the pool stats.
    AdmissionController::PoolStats* pool_stats =
        admission_controller->GetPoolStats(pool_name, true);
    // Update the local stats in pool stats with up to 5 top queries.
    pool_stats->UpdateMemTrackerStats();
  }
  //
  // Next make a TopicDeltaMap describing some activity on HOST_1 and HOST_2.
  //
  TTopicDelta membership = MakeTopicDelta(false);
  AddStatsToTopic(&membership, HOST_1, QUEUE_B,
      MakePoolStats(1000, 1, 0, MakeHeavyMemoryQueryList(QUEUE_B, 5),
          5 * MEGABYTE /*min*/, 20 * MEGABYTE /*max*/, 100 * MEGABYTE /*total*/,
          4 /*running*/));
  AddStatsToTopic(&membership, HOST_1, QUEUE_C,
      MakePoolStats(5000, 10, 0, MakeHeavyMemoryQueryList(QUEUE_C, 2),
          10 * MEGABYTE /*min*/, 200 * MEGABYTE /*max*/, 500 * MEGABYTE /*total*/,
          40 /*running*/));
  AddStatsToTopic(&membership, HOST_2, QUEUE_C,
      MakePoolStats(5000, 1, 0, MakeHeavyMemoryQueryList(QUEUE_C, 5),
          10 * MEGABYTE /*min*/, 2000 * MEGABYTE /*max*/, 10000 * MEGABYTE /*total*/,
          100 /*running*/));
  // Imitate the StateStore passing updates on query activity to the
  // AdmissionController.
  StatestoreSubscriber::TopicDeltaMap incoming_topic_deltas;
  incoming_topic_deltas.emplace(Statestore::IMPALA_REQUEST_QUEUE_TOPIC, membership);
  vector<TTopicDelta> outgoing_topic_updates;
  admission_controller->UpdatePoolStats(incoming_topic_deltas, &outgoing_topic_updates);

  //
  // Find the top 5 queries from these 4 pools in HOST_0
  //
  string mem_details_for_host0 =
      admission_controller->GetLogStringForTopNQueriesOnHost(HOST_0);
  // Verify that the 5 top ones appear in the following order.
  std::regex pattern_pools_for_host0(".*"+
       QUEUE_B+".*"+"id=0000000000000001:0000000000000002, consumed=10.00 MB"+".*"+
       QUEUE_A+".*"+"id=0000000000000000:0000000000000000, consumed=10.00 MB"+".*"+
       QUEUE_D+".*"+"id=0000000000000003:0000000000000011, consumed=9.00 MB"+".*"+
                    "id=0000000000000003:000000000000000a, consumed=9.00 MB"+".*"+
                    "id=0000000000000003:0000000000000007, consumed=9.00 MB"+".*"
       ,std::regex::basic
       );
  ASSERT_TRUE(std::regex_match(mem_details_for_host0, pattern_pools_for_host0));

  //
  // Next find the top 5 queries from these 4 pools in HOST_1
  //
  string mem_details_for_host1 =
      admission_controller->GetLogStringForTopNQueriesOnHost(HOST_1);
  // Verify that the 5 top ones appear in the following order.
  std::regex pattern_pools_for_host1(".*"+
       QUEUE_B+".*"+"id=0000000000000001:0000000000000004, consumed=20.00 MB"+".*"+
                    "id=0000000000000001:0000000000000003, consumed=19.00 MB"+".*"+
                    "id=0000000000000001:0000000000000002, consumed=8.00 MB"+".*"+
       QUEUE_C+".*"+"id=0000000000000002:0000000000000000, consumed=18.00 MB"+".*"+
                    "id=0000000000000002:0000000000000001, consumed=12.00 MB"+".*"
       ,std::regex::basic
       );
  ASSERT_TRUE(std::regex_match(mem_details_for_host1, pattern_pools_for_host1));
  //
  // Next find the top 5 queries from pool QUEUE_C among 3 hosts.
  //
  string mem_details_for_this_pool =
      admission_controller->GetLogStringForTopNQueriesInPool(QUEUE_C);
  // Verify that the 5 top ones appear in the following order.
  std::regex pattern_aggregated(std::string(".*")+
       "id=0000000000000002:0000000000000001, consumed=32.00 MB"+".*"+
       "id=0000000000000002:0000000000000004, consumed=26.00 MB"+".*"+
       "id=0000000000000002:0000000000000000, consumed=21.00 MB"+".*"+
       "id=0000000000000002:0000000000000002, consumed=17.00 MB"+".*"+
       "id=0000000000000002:000000000000000e, consumed=9.00 MB"+".*"
       ,std::regex::basic
       );
  ASSERT_TRUE(std::regex_match(mem_details_for_this_pool, pattern_aggregated));

  //
  // Reset the consumption_ counter for all trackers so that TearDown() call
  // can run cleanly.
  ResetMemConsumed(pool_mem_tracker->GetRootMemTracker());
#endif
}

} // end namespace impala
