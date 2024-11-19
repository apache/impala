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

#include <regex>

#include "common/names.h"
#include "kudu/util/logging.h"
#include "kudu/util/logging_test_util.h"
#include "runtime/bufferpool/reservation-util.h"
#include "runtime/exec-env.h"
#include "runtime/mem-tracker.h"
#include "runtime/test-env.h"
#include "scheduling/cluster-membership-test-util.h"
#include "scheduling/schedule-state.h"
#include "service/frontend.h"
#include "service/impala-server.h"
#include "testutil/death-test-util.h"
#include "testutil/gtest-util.h"
#include "testutil/scoped-flag-setter.h"
#include "util/collection-metrics.h"
#include "util/debug-util.h"
#include "util/metrics.h"

// Access the flags that are defined in RequestPoolService.
DECLARE_string(fair_scheduler_allocation_path);
DECLARE_string(llama_site_path);
DECLARE_string(injected_group_members_debug_only);
DECLARE_bool(clamp_query_mem_limit_backend_mem_limit);

namespace impala {
using namespace impala::test;

static const string IMPALA_HOME(getenv("IMPALA_HOME"));

// Queues used in the configuration files fair-scheduler-test2.xml and
// llama-site-test2.xml.
static const string QUEUE_ROOT = "root";
static const string QUEUE_A = "root.queueA";
static const string QUEUE_B = "root.queueB";
static const string QUEUE_C = "root.queueC";
static const string QUEUE_D = "root.queueD";
static const string QUEUE_E = "root.queueE";
static const string QUEUE_SMALL = "root.group-set-small";
static const string QUEUE_LARGE = "root.group-set-large";

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

static const string USER1 = "user1";
static const string USER2 = "user2";
static const string USER3 = "user3";
static const string USER_A = "userA";
static const string USER_H = "userH";

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
    // Setup injected groups that will be visible to all tests.
    // This is done here as the flags are copied to Java space at Jni initiation time.
    FLAGS_injected_group_members_debug_only = "group0:userA;"
                                              "group1:user1,user3;"
                                              "dev:alice,deborah;"
                                              "it:bob,fiona,geeta;"
                                              "support:claire,geeta,howard;";
    ASSERT_OK(test_env_->Init());
  }

  virtual void TearDown() {
    flag_saver_.reset();
    pool_.Clear();
  }

  /// Make a ScheduleState with dummy parameters that can be used to test admission and
  /// rejection in AdmissionController.
  ScheduleState* MakeScheduleState(string request_pool_name, int64_t mem_limit,
      const TPoolConfig& config, const int num_hosts, const int64_t per_host_mem_estimate,
      const int64_t coord_mem_estimate, bool is_dedicated_coord,
      const string& executor_group = ImpalaServer::DEFAULT_EXECUTOR_GROUP_NAME,
      int64_t mem_limit_executors = -1, int64_t mem_limit_coordinators = -1,
      const string& delegated_user = USER1) {
    DCHECK_GT(num_hosts, 0);
    TQueryExecRequest* request = pool_.Add(new TQueryExecRequest());
    request->query_ctx.request_pool = move(request_pool_name);
    TSessionState* session = new TSessionState();
    session->__set_delegated_user(delegated_user);
    request->query_ctx.__set_session(*session);
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
    schedule_state->UpdateMemoryRequirements(config, group.second.admit_mem_limit(),
        group.first.GetPerExecutorMemLimitForAdmission());
    return schedule_state;
  }

  /// Same as previous MakeScheduleState with fewer inputs (and more default params).
  ScheduleState* MakeScheduleState(string request_pool_name, const TPoolConfig& config,
      const int num_hosts, const int per_host_mem_estimate,
      const string& executor_group = ImpalaServer::DEFAULT_EXECUTOR_GROUP_NAME,
      const string& delegated_user = USER1) {
    return MakeScheduleState(move(request_pool_name), 0, config, num_hosts,
        per_host_mem_estimate, per_host_mem_estimate, false, executor_group, -1, -1,
        delegated_user);
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
      const std::string& pool, const int queries) {
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

  /// Build a TPoolStats object with user loads.
  static TPoolStats MakePoolStats(const int backend_mem_reserved,
      const int num_admitted_running, const int num_queued,
      std::map<std::string, int64_t>& user_loads) {
    TPoolStats stats =
        MakePoolStats(backend_mem_reserved, num_admitted_running, num_queued);
    stats.__set_user_loads(user_loads);
    return stats;
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
      TTopicDelta* topic, const string& host, const string& pool_name, TPoolStats stats) {
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
      const string& pool_name, const int64_t max_requests,
      const int64_t max_mem_resources, const int64_t queue_timeout_ms,
      const bool clamp_mem_limit_query_option, const int64_t min_query_mem_limit = 0,
      const int64_t max_query_mem_limit = 0) {
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
    UniqueIdPB* coord_id = pool_.Add(new UniqueIdPB());
    return MakeAdmissionController(coord_id);
  }

  AdmissionController* MakeAdmissionController(UniqueIdPB* coord_id) {
    // Create a RequestPoolService which will read the configuration files.
    MetricGroup* metric_group = pool_.Add(new MetricGroup("impala-metrics"));
    RequestPoolService* request_pool_service =
        pool_.Add(new RequestPoolService(metric_group));
    TNetworkAddress* addr = pool_.Add(new TNetworkAddress());
    addr->__set_hostname("host0");
    addr->__set_port(25000);
    ClusterMembershipMgr* cmm =
        pool_.Add(new ClusterMembershipMgr("", nullptr, metric_group));

    ClusterMembershipMgr::Snapshot* snapshot =
        pool_.Add(new ClusterMembershipMgr::Snapshot());

    ExecutorGroup* eg = pool_.Add(new ExecutorGroup("EG1"));
    snapshot->executor_groups.emplace(eg->name(), *eg);
    snapshot->version = 1;
    BackendDescriptorPB* coordinator = pool_.Add(new BackendDescriptorPB());
    snapshot->current_backends.emplace(PrintId(*coord_id), *coordinator);

    ClusterMembershipMgr::SnapshotPtr new_state =
        std::make_shared<ClusterMembershipMgr::Snapshot>(*snapshot);
    cmm->SetState(new_state);
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

  // Print a string for each of the AdmissionOutcome values,
  static string Outcome(const AdmissionOutcome outcome) {
    if (outcome == AdmissionOutcome::REJECTED) return "REJECTED";
    if (outcome == AdmissionOutcome::ADMITTED) return "ADMITTED";
    if (outcome == AdmissionOutcome::TIMED_OUT) return "TIMED_OUT";
    if (outcome == AdmissionOutcome::CANCELLED) return "CANCELLED";
    EXPECT_FALSE(false) << "unknown outcome";
    return {};
  }

  AdmissionController::QueueNode* makeQueueNode(AdmissionController* admission_controller,
      UniqueIdPB* coord_id,
      Promise<AdmissionOutcome, PromiseMode::MULTIPLE_PRODUCER>* admit_outcome,
      TPoolConfig& config, const string& pool_name) {
    ScheduleState* schedule_state = MakeScheduleState(QUEUE_C, config, 1, 1000);
    UniqueIdPB* query_id = pool_.Add(new UniqueIdPB());
    RuntimeProfile* summary_profile = pool_.Add(new RuntimeProfile(&pool_, "foo"));
    std::unordered_set<NetworkAddressPB> blacklisted_executor_addresses;
    const TQueryExecRequest& exec_request = schedule_state->request();
    TQueryOptions query_options;
    AdmissionController::AdmissionRequest request = {*query_id, *coord_id, exec_request,
        query_options, summary_profile, blacklisted_executor_addresses};

    // Clear queue_nodes_ so we can call this method again, though this means there can
    // only ever be one queue node.
    admission_controller->queue_nodes_.clear();

    auto it = admission_controller->queue_nodes_.emplace(std::piecewise_construct,
        std::forward_as_tuple(request.query_id),
        std::forward_as_tuple(request, admit_outcome, request.summary_profile));
    EXPECT_TRUE(it.second);
    AdmissionController::QueueNode* queue_node = &it.first->second;
    queue_node->pool_name = pool_name;
    queue_node->profile = summary_profile;
    return queue_node;
  }

  void ResetMemConsumed(MemTracker* tracker) {
    tracker->consumption_->Set(0);
    for (MemTracker* child : tracker->child_trackers_) {
      ResetMemConsumed(child);
    }
  }

  // Set the per-user loads for a pool. Used only for testing.
  static void set_user_loads(AdmissionController* admission_controller, const char* user,
      const string& pool_name, int64_t load) {
    AdmissionController::PoolStats* stats = admission_controller->GetPoolStats(pool_name);
    TPoolStats pool_stats;
    pool_stats.user_loads[user] = load;
    stats->local_stats_ = pool_stats;
  }

  // Try and run a query in a 2-pool system.
  bool can_queue(const char* user, int64_t current_load_small, int64_t current_load_large,
      bool use_small_queue, string* not_admitted_reason) {
    set<string> pool_stats_removed_hosts;
    AdmissionController* admission_controller = MakeAdmissionController();
    RequestPoolService* request_pool_service =
        admission_controller->request_pool_service_;

    // Get the PoolConfig for the global "root" configuration.
    TPoolConfig config_root;
    EXPECT_OK(request_pool_service->GetPoolConfig(QUEUE_ROOT, &config_root));
    TPoolConfig config_large;
    EXPECT_OK(request_pool_service->GetPoolConfig(QUEUE_LARGE, &config_large));
    TPoolConfig config_small;
    EXPECT_OK(request_pool_service->GetPoolConfig(QUEUE_SMALL, &config_small));

    set_user_loads(admission_controller, user, QUEUE_LARGE, current_load_large);
    set_user_loads(admission_controller, user, QUEUE_SMALL, current_load_small);

    admission_controller->UpdateClusterAggregates(pool_stats_removed_hosts);

    // Validate the aggregation done in UpdateClusterAggregates().
    EXPECT_EQ(current_load_small + current_load_large,
        admission_controller->root_agg_user_loads_.get(user));
    AdmissionController::PoolStats* large_pool_stats =
        admission_controller->GetPoolStats(QUEUE_LARGE, true);
    AdmissionController::PoolStats* small_pool_stats =
        admission_controller->GetPoolStats(QUEUE_SMALL, true);
    EXPECT_EQ(current_load_small, small_pool_stats->GetUserLoad(user));
    EXPECT_EQ(current_load_large, large_pool_stats->GetUserLoad(user));

    TPoolConfig pool_to_submit = use_small_queue ? config_small : config_large;
    string pool_name_to_submit = use_small_queue ? QUEUE_SMALL : QUEUE_LARGE;

    ScheduleState* schedule_state = MakeScheduleState(pool_name_to_submit, pool_to_submit,
        12, 30L * MEGABYTE, ImpalaServer::DEFAULT_EXECUTOR_GROUP_NAME, user);

    bool coordinator_resource_limited = false;
    bool can_admit = admission_controller->CanAdmitQuota(
        *schedule_state, pool_to_submit, config_root, not_admitted_reason);
    EXPECT_FALSE(coordinator_resource_limited);
    return can_admit;
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
    bool can_accommodate = CanAccommodateMaxInitialReservation(
        *test_state, test_pool_config, &not_admitted_reason);
    EXPECT_STR_CONTAINS(not_admitted_reason, "--not set--");
    ASSERT_TRUE(can_accommodate);
    // Coordinator reservation doesn't fit.
    test_state->set_largest_min_reservation(400 * MEGABYTE);
    test_state->set_coord_min_reservation(700 * MEGABYTE);
    can_accommodate = CanAccommodateMaxInitialReservation(
        *test_state, test_pool_config, &not_admitted_reason);
    if (!backend_mem_unlimited) {
      EXPECT_STR_CONTAINS(not_admitted_reason,
          "minimum memory reservation is greater than memory available to the query for "
          "buffer reservations. Memory reservation needed given the current plan: 700.00 "
          "MB. Adjust the MEM_LIMIT option ");
      ASSERT_FALSE(can_accommodate);
    } else {
      ASSERT_TRUE(can_accommodate);
    }
    // Neither coordinator nor executor reservation fits.
    test_state->set_largest_min_reservation(GIGABYTE);
    test_state->set_coord_min_reservation(GIGABYTE);
    can_accommodate = CanAccommodateMaxInitialReservation(
        *test_state, test_pool_config, &not_admitted_reason);
    if (!backend_mem_unlimited) {
      EXPECT_STR_CONTAINS(not_admitted_reason,
          "minimum memory reservation is greater than memory available to the query for "
          "buffer reservations. Memory reservation needed given the current plan: 1.00 "
          "GB. Adjust the MEM_LIMIT option ");
      ASSERT_FALSE(can_accommodate);
    } else {
      ASSERT_TRUE(can_accommodate);
    }
    // Executor reservation doesn't fit.
    test_state->set_largest_min_reservation(900 * MEGABYTE);
    test_state->set_coord_min_reservation(50 * MEGABYTE);
    can_accommodate = CanAccommodateMaxInitialReservation(
        *test_state, test_pool_config, &not_admitted_reason);
    if (!backend_mem_unlimited) {
      EXPECT_STR_CONTAINS(not_admitted_reason,
          "minimum memory reservation is greater than memory available to the query for "
          "buffer reservations. Memory reservation needed given the current plan: 900.00 "
          "MB. Adjust the MEM_LIMIT option ");
      ASSERT_FALSE(can_accommodate);
    } else {
      ASSERT_TRUE(can_accommodate);
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

  // Get the PoolConfig for the global "root" configuration.
  TPoolConfig config_root;
  ASSERT_OK(request_pool_service->GetPoolConfig(QUEUE_ROOT, &config_root));

  // Get the PoolConfig for QUEUE_C ("root.queueC").
  TPoolConfig config_c;
  ASSERT_OK(request_pool_service->GetPoolConfig(QUEUE_C, &config_c));

  // Create a ScheduleState to run on QUEUE_C.
  ScheduleState* schedule_state = MakeScheduleState(QUEUE_C, config_c, 1, 64L * MEGABYTE);
  ExecutorGroupCoordinatorPair group = MakeExecutorConfig(*schedule_state);
  schedule_state->UpdateMemoryRequirements(config_c, group.second.admit_mem_limit(),
      group.first.GetPerExecutorMemLimitForAdmission());

  // Check that the AdmissionController initially has no data about other hosts.
  ASSERT_EQ(0, admission_controller->host_stats_.size());

  // Check that the query can be admitted.
  string not_admitted_reason;
  bool coordinator_resource_limited = false;
  ASSERT_TRUE(admission_controller->CanAdmitRequest(*schedule_state, config_c,
      config_root, true, &not_admitted_reason, nullptr, coordinator_resource_limited));
  ASSERT_FALSE(coordinator_resource_limited);
  ASSERT_EQ(0, admission_controller->root_agg_user_loads_.size());

  // Create a ScheduleState just like 'schedule_state' to run on 3 hosts which can't be
  // admitted.
  ScheduleState* schedule_state_3_hosts =
      MakeScheduleState(QUEUE_C, config_c, 3, 64L * MEGABYTE);
  ASSERT_FALSE(admission_controller->CanAdmitRequest(*schedule_state_3_hosts, config_c,
      config_root, true, &not_admitted_reason, nullptr, coordinator_resource_limited));
  EXPECT_STR_CONTAINS(not_admitted_reason,
      "Not enough aggregate memory available in pool root.queueC with max mem "
      "resources 128.00 MB. Needed 192.00 MB but only 128.00 MB was available.");
  ASSERT_FALSE(coordinator_resource_limited);

  // Make a TopicDeltaMap describing some activity on host1 and host2.
  TTopicDelta membership = MakeTopicDelta(false);

  AdmissionController::UserLoads loads1{{USER1, 1}, {USER2, 4}};
  AddStatsToTopic(&membership, HOST_1, QUEUE_B, MakePoolStats(1000, 1, 0, loads1));
  AdmissionController::UserLoads loads2{{USER2, 3}, {USER3, 2}};
  AddStatsToTopic(&membership, HOST_1, QUEUE_C, MakePoolStats(5000, 10, 0, loads2));
  AdmissionController::UserLoads loads3{{USER1, 1}, {USER3, 7}};
  AddStatsToTopic(&membership, HOST_2, QUEUE_C, MakePoolStats(5000, 1, 0, loads3));

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
  ASSERT_EQ(1, pool_stats->agg_user_loads_.get(USER1));
  ASSERT_EQ(3, pool_stats->agg_user_loads_.get(USER2));
  ASSERT_EQ(9, pool_stats->agg_user_loads_.get(USER3));

  // Check the aggregated user loads across the pools.
  ASSERT_EQ(2, admission_controller->root_agg_user_loads_.get(USER1));
  ASSERT_EQ(7, admission_controller->root_agg_user_loads_.get(USER2));
  ASSERT_EQ(9, admission_controller->root_agg_user_loads_.get(USER3));

  // Test that the query cannot be admitted now.
  ASSERT_FALSE(admission_controller->CanAdmitRequest(*schedule_state, config_c,
      config_root, true, &not_admitted_reason, nullptr, coordinator_resource_limited));
  EXPECT_STR_CONTAINS(
      not_admitted_reason, "number of running queries 11 is at or over limit 10.");
  ASSERT_FALSE(coordinator_resource_limited);
}

/// Test that removing hosts from AdmissionController, and check if the memory reserved in
/// host stats for the specific host is updated correctly after the host is removed.
TEST_F(AdmissionControllerTest, EraseHostStats) {
  FLAGS_fair_scheduler_allocation_path = GetResourceFile("fair-scheduler-test2.xml");
  FLAGS_llama_site_path = GetResourceFile("llama-site-test2.xml");

  AdmissionController* admission_controller = MakeAdmissionController();

  ASSERT_EQ(0, admission_controller->host_stats_.size());

  TTopicDelta membership = MakeTopicDelta(false);

  AddStatsToTopic(&membership, HOST_1, QUEUE_C, MakePoolStats(1000, 1, 0));
  AddStatsToTopic(&membership, HOST_2, QUEUE_C, MakePoolStats(5000, 10, 0));
  AddStatsToTopic(&membership, HOST_2, QUEUE_B, MakePoolStats(3000, 5, 0));

  StatestoreSubscriber::TopicDeltaMap initial_topic_deltas;
  initial_topic_deltas.emplace(Statestore::IMPALA_REQUEST_QUEUE_TOPIC, membership);
  vector<TTopicDelta> outgoing_topic_updates;
  admission_controller->UpdatePoolStats(initial_topic_deltas, &outgoing_topic_updates);

  // Verify that the host stats were added
  ASSERT_EQ(3, admission_controller->host_stats_.size());
  ASSERT_EQ(1, admission_controller->host_stats_.count(HOST_0));
  ASSERT_EQ(1, admission_controller->host_stats_.count(HOST_1));
  ASSERT_EQ(1, admission_controller->host_stats_.count(HOST_2));
  ASSERT_EQ(1000, admission_controller->host_stats_[HOST_1].mem_reserved);
  ASSERT_EQ(8000, admission_controller->host_stats_[HOST_2].mem_reserved);

  // Create an update that deletes HOST_1
  TTopicDelta delete_update = MakeTopicDelta(true);
  TTopicItem delete_item;
  delete_item.key = "POOL:" + QUEUE_C + "!" + HOST_1;
  delete_item.deleted = true;
  delete_update.topic_entries.push_back(delete_item);

  StatestoreSubscriber::TopicDeltaMap delete_topic_deltas;
  delete_topic_deltas.emplace(Statestore::IMPALA_REQUEST_QUEUE_TOPIC, delete_update);

  // Apply the delete update
  admission_controller->UpdatePoolStats(delete_topic_deltas, &outgoing_topic_updates);

  // Verify that HOST_1 mem_reserved was reset.
  ASSERT_EQ(3, admission_controller->host_stats_.size());
  ASSERT_EQ(8000, admission_controller->host_stats_[HOST_2].mem_reserved);
  ASSERT_EQ(0, admission_controller->host_stats_[HOST_1].mem_reserved);

  // Verify that the pool stats were updated accordingly
  AdmissionController::PoolStats* pool_stats =
      admission_controller->GetPoolStats(QUEUE_C);
  ASSERT_EQ(5000, pool_stats->agg_mem_reserved_);
  ASSERT_EQ(10, pool_stats->agg_num_running_);

  // Remove HOST_2 in Queue C.
  delete_update = MakeTopicDelta(true);
  delete_item.key = "POOL:" + QUEUE_C + "!" + HOST_2;
  delete_item.deleted = true;
  delete_update.topic_entries.push_back(delete_item);
  StatestoreSubscriber::TopicDeltaMap delete_topic_deltas2;
  delete_topic_deltas2.emplace(Statestore::IMPALA_REQUEST_QUEUE_TOPIC, delete_update);
  admission_controller->UpdatePoolStats(delete_topic_deltas2, &outgoing_topic_updates);

  ASSERT_EQ(3, admission_controller->host_stats_.size());
  ASSERT_EQ(3000, admission_controller->host_stats_[HOST_2].mem_reserved);
  ASSERT_EQ(0, admission_controller->host_stats_[HOST_1].mem_reserved);

  // Remove HOST_2 in Queue B.
  TTopicDelta delete_update3 = MakeTopicDelta(true);
  TTopicItem delete_item3;
  delete_item3.key = "POOL:" + QUEUE_B + "!" + HOST_2;
  delete_item3.deleted = true;
  delete_update3.topic_entries.push_back(delete_item3);
  StatestoreSubscriber::TopicDeltaMap delete_topic_deltas3;
  delete_topic_deltas3.emplace(Statestore::IMPALA_REQUEST_QUEUE_TOPIC, delete_update3);
  admission_controller->UpdatePoolStats(delete_topic_deltas3, &outgoing_topic_updates);

  ASSERT_EQ(3, admission_controller->host_stats_.size());
  ASSERT_EQ(0, admission_controller->host_stats_[HOST_2].mem_reserved);
  ASSERT_EQ(0, admission_controller->host_stats_[HOST_1].mem_reserved);
}

/// Test CanAdmitRequest in the context of aggregated memory required to admit a query.
TEST_F(AdmissionControllerTest, CanAdmitRequestMemory) {
  // Pass the paths of the configuration files as command line flags.
  FLAGS_fair_scheduler_allocation_path = GetResourceFile("fair-scheduler-test2.xml");
  FLAGS_llama_site_path = GetResourceFile("llama-site-test2.xml");

  AdmissionController* admission_controller = MakeAdmissionController();
  RequestPoolService* request_pool_service = admission_controller->request_pool_service_;

  // Get the PoolConfig for the global "root" configuration.
  TPoolConfig config_root;
  ASSERT_OK(request_pool_service->GetPoolConfig(QUEUE_ROOT, &config_root));

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
  ASSERT_TRUE(admission_controller->CanAdmitRequest(*schedule_state, config_d,
      config_root, true, &not_admitted_reason, nullptr, coordinator_resource_limited));
  ASSERT_FALSE(coordinator_resource_limited);

  // Tests that this query cannot be admitted.
  // Increasing the number of hosts pushes the aggregate memory required to admit this
  // query over the allowed limit.
  host_count = 15;
  ScheduleState* schedule_state15 =
      MakeScheduleState(QUEUE_D, config_d, host_count, 30L * MEGABYTE);
  ASSERT_FALSE(admission_controller->CanAdmitRequest(*schedule_state15, config_d,
      config_root, true, &not_admitted_reason, nullptr, coordinator_resource_limited));
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
      config_root, true, &not_admitted_reason, nullptr, coordinator_resource_limited));
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

  // Get the PoolConfig for the global "root" configuration.
  TPoolConfig config_root;
  ASSERT_OK(request_pool_service->GetPoolConfig(QUEUE_ROOT, &config_root));

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
  ASSERT_TRUE(admission_controller->CanAdmitRequest(*schedule_state, config_d,
      config_root, true, &not_admitted_reason, nullptr, coordinator_resource_limited));
  ASSERT_FALSE(coordinator_resource_limited);
  // ... but same Query cannot be admitted directly.
  ASSERT_FALSE(admission_controller->CanAdmitRequest(*schedule_state, config_d,
      config_root, false, &not_admitted_reason, nullptr, coordinator_resource_limited));
  EXPECT_STR_CONTAINS(not_admitted_reason,
      "queue is not empty (size 2); queued queries are executed first");
  ASSERT_FALSE(coordinator_resource_limited);

  // Simulate that there are 7 queries already running.
  pool_stats->agg_num_running_ = 7;
  ASSERT_FALSE(admission_controller->CanAdmitRequest(*schedule_state, config_d,
      config_root, true, &not_admitted_reason, nullptr, coordinator_resource_limited));
  EXPECT_STR_CONTAINS(
      not_admitted_reason, "number of running queries 7 is at or over limit 6");
  ASSERT_FALSE(coordinator_resource_limited);
}

/// Test CanAdmitQuota in the context of user and group quotas.
TEST_F(AdmissionControllerTest, UserAndGroupQuotas) {
  // Enable Kerberos so we can test kerberos names in GetEffectiveShortUser().
  auto enable_kerberos =
      ScopedFlagSetter<string>::Make(&FLAGS_principal, "service_name/_HOST@some.realm");
  // Pass the paths of the configuration files as command line flags.
  FLAGS_fair_scheduler_allocation_path = GetResourceFile("fair-scheduler-test2.xml");
  FLAGS_llama_site_path = GetResourceFile("llama-site-test2.xml");

  AdmissionController* admission_controller = MakeAdmissionController();
  RequestPoolService* request_pool_service = admission_controller->request_pool_service_;

  // Get the PoolConfig for the global "root" configuration.
  TPoolConfig config_root;
  ASSERT_OK(request_pool_service->GetPoolConfig(QUEUE_ROOT, &config_root));
  ASSERT_TRUE(AdmissionController::HasQuotaConfig(config_root));

  TPoolConfig config_e;
  ASSERT_OK(request_pool_service->GetPoolConfig(QUEUE_E, &config_e));
  ASSERT_TRUE(AdmissionController::HasQuotaConfig(config_e));

  // Check the PoolStats for QUEUE_E.
  AdmissionController::PoolStats* pool_stats =
      admission_controller->GetPoolStats(QUEUE_E);
  CheckPoolStatsEmpty(pool_stats);

  // Create a ScheduleState to run on QUEUE_E on 12 hosts.
  int host_count = 12;
  ScheduleState* schedule_state = MakeScheduleState(QUEUE_E, config_e, host_count,
      30L * MEGABYTE, ImpalaServer::DEFAULT_EXECUTOR_GROUP_NAME, USER_A);
  string not_admitted_reason;

  // Simulate that there are 2 queries queued.
  pool_stats->local_stats_.num_queued = 2;

  // Test CanAdmitRequest
  // Query can be admitted from queue...
  bool coordinator_resource_limited = false;
  ASSERT_TRUE(admission_controller->CanAdmitRequest(*schedule_state, config_e,
      config_root, true, &not_admitted_reason, nullptr, coordinator_resource_limited));
  ASSERT_FALSE(coordinator_resource_limited);
  // ... but same Query cannot be admitted directly.
  ASSERT_FALSE(admission_controller->CanAdmitRequest(*schedule_state, config_e,
      config_root, false, &not_admitted_reason, nullptr, coordinator_resource_limited));
  EXPECT_STR_CONTAINS(not_admitted_reason,
      "queue is not empty (size 2); queued queries are executed first");
  ASSERT_FALSE(coordinator_resource_limited);

  // Simulate that there are 7 queries already running.
  pool_stats->agg_num_running_ = 7;
  ASSERT_FALSE(admission_controller->CanAdmitRequest(*schedule_state, config_e,
      config_root, true, &not_admitted_reason, nullptr, coordinator_resource_limited));
  // Limit of requests is 5 from llama.am.throttling.maximum.placed.reservations.
  EXPECT_STR_CONTAINS(
      not_admitted_reason, "number of running queries 7 is at or over limit 5");
  ASSERT_FALSE(coordinator_resource_limited);

  pool_stats->agg_num_running_ = 3;
  ASSERT_TRUE(admission_controller->CanAdmitRequest(*schedule_state, config_e,
      config_root, true, &not_admitted_reason, nullptr, coordinator_resource_limited));

  // Test CanAdmitQuota
  // Make sure queue is empty as we are going to pass admit_from_queue=false.
  pool_stats->local_stats_.num_queued = 0;
  // Test with load == limit, should fail
  pool_stats->agg_user_loads_.insert(USER_A, 3);
  ASSERT_FALSE(admission_controller->CanAdmitQuota(
      *schedule_state, config_e, config_root, &not_admitted_reason));
  EXPECT_STR_CONTAINS(not_admitted_reason,
      "current per-user load 3 for user 'userA' is at or above the user limit 3");

  // If UserA's load is 2 it should be admitted because the user rule takes precedence
  // over the wildcard rule.
  pool_stats->agg_user_loads_.insert(USER_A, 2);
  ASSERT_TRUE(admission_controller->CanAdmitQuota(
      *schedule_state, config_e, config_root, &not_admitted_reason));

  // Test wildcards with User2.
  schedule_state = MakeScheduleState(QUEUE_E, config_e, host_count, 30L * MEGABYTE,
      ImpalaServer::DEFAULT_EXECUTOR_GROUP_NAME, USER2);
  ASSERT_TRUE(admission_controller->CanAdmitQuota(
      *schedule_state, config_e, config_root, &not_admitted_reason));
  pool_stats->agg_user_loads_.insert(USER2, 3);
  ASSERT_FALSE(admission_controller->CanAdmitQuota(
      *schedule_state, config_e, config_root, &not_admitted_reason));
  EXPECT_STR_CONTAINS(not_admitted_reason,
      "current per-user load 3 for user 'user2' is at or above the wildcard limit 1");

  pool_stats->agg_user_loads_.clear_key(USER2);
  ASSERT_TRUE(admission_controller->CanAdmitQuota(
      *schedule_state, config_e, config_root, &not_admitted_reason));

  // Test group quotas. Group membership is injected in AdmissionControllerTest::Setup().
  // The user USER3 is in group 'group1'.
  schedule_state = MakeScheduleState(QUEUE_E, config_e, host_count, 30L * MEGABYTE,
      ImpalaServer::DEFAULT_EXECUTOR_GROUP_NAME, USER3);
  pool_stats->agg_user_loads_.insert(USER3, 2);
  ASSERT_FALSE(admission_controller->CanAdmitQuota(
      *schedule_state, config_e, config_root, &not_admitted_reason));
  EXPECT_STR_CONTAINS(not_admitted_reason,
      "current per-group load 2 for user 'user3' in group 'group1' is at or above the "
      "group limit 2");

  // Quota set to 0 disallows entry.
  schedule_state = MakeScheduleState(QUEUE_E, config_e, host_count, 30L * MEGABYTE,
      ImpalaServer::DEFAULT_EXECUTOR_GROUP_NAME, USER_H);
  // We skip the usual setting of the load to agg_user_loads_ as it is 0 by default.
  ASSERT_FALSE(admission_controller->CanAdmitQuota(
      *schedule_state, config_e, config_root, &not_admitted_reason));
  EXPECT_STR_CONTAINS(not_admitted_reason,
      "current per-user load 0 for user 'userH' is at or above the user limit 0");

  ScheduleState* bad_user_state = MakeScheduleState(QUEUE_E, config_e, host_count,
      30L * MEGABYTE, ImpalaServer::DEFAULT_EXECUTOR_GROUP_NAME, "a@b.b.com@d.com");
  ASSERT_FALSE(admission_controller->CanAdmitQuota(
      *bad_user_state, config_e, config_root, &not_admitted_reason));
  EXPECT_STR_CONTAINS(not_admitted_reason, "cannot parse user name a@b.b.com@d.com");
}

/// Test CanAdmitRequest in the context of user and group quotas.
// Group membership is injected in AdmissionControllerTest::Setup().
// The users 'bob' and 'fiona' are in group 'it'.
// The user 'howard' is in group 'support'.
TEST_F(AdmissionControllerTest, QuotaExamples) {
  // Pass the paths of the configuration files as command line flags.
  FLAGS_fair_scheduler_allocation_path = GetResourceFile("fair-scheduler-test3.xml");
  FLAGS_llama_site_path = GetResourceFile("llama-site-test2.xml");
  string not_admitted_reason;

  // Alice can run 3 queries, because the more specific rule for 'alice' overrides
  // the less-specific wildcard rule.
  ASSERT_TRUE(can_queue("alice", 3, 2, true, &not_admitted_reason));
  ASSERT_FALSE(can_queue("alice", 4, 12, true, &not_admitted_reason));
  ASSERT_EQ(
      "current per-user load 4 for user 'alice' is at or above the user limit 4 in pool "
      "'" + QUEUE_SMALL + "'",
      not_admitted_reason);

  // Bob can run 2 queries, because the more specific group rule for 'it' overrides
  // the less-specific wildcard rule.
  ASSERT_TRUE(can_queue("bob", 1, 1, true, &not_admitted_reason));
  ASSERT_FALSE(can_queue("bob", 2, 1, true, &not_admitted_reason));
  ASSERT_EQ(
      "current per-group load 2 for user 'bob' in group 'it' is at or above the group "
      "limit 2 in pool '"
          + QUEUE_SMALL + "'",
      not_admitted_reason);

  ASSERT_FALSE(can_queue("claire", 5, 0, true, &not_admitted_reason));
  ASSERT_EQ(
      "current per-group load 5 for user 'claire' in group 'support' is at or above the "
      "group limit 5 in pool '"
          + QUEUE_SMALL + "'",
      not_admitted_reason);

  // Fiona can run 3 queries, because the more specific user rule overrides
  // the less-specific group rule.
  ASSERT_TRUE(can_queue("fiona", 2, 1, true, &not_admitted_reason));
  ASSERT_FALSE(can_queue("fiona", 3, 1, true, &not_admitted_reason));
  ASSERT_EQ(
      "current per-user load 3 for user 'fiona' is at or above the user limit 3 in pool '"
          + QUEUE_SMALL + "'",
      not_admitted_reason);

  // Geeta is in 2 groups: 'it' and 'support'.
  // Group 'it' restricts her to running 2 queries in the small pool.
  // Group 'support' restricts her to running 5 queries in the small pool.
  // The 'support' rule is the least restrictive, so she can run 5 queries in the small
  // pool.
  ASSERT_TRUE(can_queue("geeta", 4, 1, true, &not_admitted_reason));
  ASSERT_FALSE(can_queue("geeta", 5, 1, true, &not_admitted_reason));
  ASSERT_EQ("current per-group load 5 for user 'geeta' in group 'support' is at or above "
            "the group limit 5 in pool '"
          + QUEUE_SMALL + "' (Ignored Group Quotas 'it':2)",
      not_admitted_reason);

  // Howard has a limit of 4 at root level, and limit of 100 in the small pool.
  // Arguably this small pool configuration does not make sense as it will never have any
  // effect.
  ASSERT_TRUE(can_queue("howard", 2, 1, true, &not_admitted_reason));
  ASSERT_FALSE(can_queue("howard", 3, 1, true, &not_admitted_reason));
  ASSERT_EQ(
      "current per-user load 4 for user 'howard' is at or above the user limit 4 in pool"
      " '" + QUEUE_ROOT
          + "'",
      not_admitted_reason);

  // Iris is not in any groups and so hits the large pool wildcard limit.
  ASSERT_FALSE(can_queue("iris", 0, 1, false, &not_admitted_reason));
  ASSERT_EQ(
      "current per-user load 1 for user 'iris' is at or above the wildcard limit 1 in "
      "pool '"
          + QUEUE_LARGE + "'",
      not_admitted_reason);

  // Jade has a limit of 100 at root level, and limit of 8 in the small pool.
  // This is an example where there are User Quotas at both root and pool level.
  ASSERT_TRUE(can_queue("jade", 7, 0, true, &not_admitted_reason));
  ASSERT_FALSE(can_queue("jade", 8, 0, true, &not_admitted_reason));
  ASSERT_EQ(
      "current per-user load 8 for user 'jade' is at or above the user limit 8 in pool '"
          + QUEUE_SMALL + "'",
      not_admitted_reason);
}

/// Test CanAdmitRequest() using the slots mechanism that is enabled with non-default
/// executor groups.
TEST_F(AdmissionControllerTest, CanAdmitRequestSlots) {
  // Pass the paths of the configuration files as command line flags.
  FLAGS_fair_scheduler_allocation_path = GetResourceFile("fair-scheduler-test2.xml");
  FLAGS_llama_site_path = GetResourceFile("llama-site-test2.xml");

  AdmissionController* admission_controller = MakeAdmissionController();
  RequestPoolService* request_pool_service = admission_controller->request_pool_service_;

  // Get the PoolConfig for the global "root" configuration.
  TPoolConfig config_root;
  ASSERT_OK(request_pool_service->GetPoolConfig(QUEUE_ROOT, &config_root));

  // Get the PoolConfig for QUEUE_D ("root.queueD").
  TPoolConfig config_d;
  ASSERT_OK(request_pool_service->GetPoolConfig(QUEUE_D, &config_d));

  // Create ScheduleStates to run on QUEUE_D on 12 hosts.
  // Running in both default and non-default executor groups is simulated.
  int host_count = 12;
  int slots_per_host = 16;
  int slots_per_query = 4;
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

  // Enough slots are available, so it can be admitted in both cases.
  ASSERT_TRUE(admission_controller->CanAdmitRequest(*default_group_schedule, config_d,
      config_root, true, &not_admitted_reason, nullptr, coordinator_resource_limited))
      << not_admitted_reason;
  ASSERT_FALSE(coordinator_resource_limited);
  ASSERT_TRUE(admission_controller->CanAdmitRequest(*other_group_schedule, config_d,
      config_root, true, &not_admitted_reason, nullptr, coordinator_resource_limited))
      << not_admitted_reason;
  ASSERT_FALSE(coordinator_resource_limited);

  // Simulate that almost all the slots are in use, which prevents admission in the
  // non-default group.
  SetSlotsInUse(admission_controller, host_addrs, slots_per_host - 1);

  ASSERT_TRUE(admission_controller->CanAdmitRequest(*default_group_schedule, config_d,
      config_root, true, &not_admitted_reason, nullptr, coordinator_resource_limited))
      << not_admitted_reason;
  ASSERT_FALSE(coordinator_resource_limited);
  ASSERT_FALSE(admission_controller->CanAdmitRequest(*other_group_schedule, config_d,
      config_root, true, &not_admitted_reason, nullptr, coordinator_resource_limited));
  EXPECT_STR_CONTAINS(not_admitted_reason,
      "Not enough admission control slots available on host host1:25000. Needed 4 "
      "slots but 15/16 are already in use.");
  ASSERT_FALSE(coordinator_resource_limited);
  // Assert that coordinator-only schedule also not admitted.
  ASSERT_FALSE(admission_controller->CanAdmitRequest(*coordinator_only_schedule, config_d,
      config_root, true, &not_admitted_reason, nullptr, coordinator_resource_limited));
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

  TPoolConfig config_root;
  ASSERT_OK(request_pool_service->GetPoolConfig(QUEUE_ROOT, &config_root));

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
      config_root, true, &not_admitted_reason, nullptr, coordinator_resource_limited))
      << not_admitted_reason;
  ASSERT_FALSE(coordinator_resource_limited);
  ASSERT_EQ(coordinator_only_schedule->executor_group(),
      ClusterMembershipMgr::EMPTY_GROUP_NAME);
  ASSERT_TRUE(
      admission_controller->CanAdmitRequest(*coordinator_only_schedule, pool_config,
          config_root, true, &not_admitted_reason, nullptr, coordinator_resource_limited))
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
}

/// Unit test for TryDequeue showing queries being rejected or cancelled.
TEST_F(AdmissionControllerTest, DequeueLoop) {
  // Pass the paths of the configuration files as command line flags
  FLAGS_fair_scheduler_allocation_path = GetResourceFile("fair-scheduler-test2.xml");
  FLAGS_llama_site_path = GetResourceFile("llama-site-test2.xml");

  UniqueIdPB* coord_id = pool_.Add(new UniqueIdPB());
  AdmissionController* admission_controller = MakeAdmissionController(coord_id);
  RequestPoolService* request_pool_service = admission_controller->request_pool_service_;

  // Get the PoolConfig for QUEUE_C
  TPoolConfig config_c;
  AdmissionController::RequestQueue& queue_c =
      admission_controller->request_queue_map_[QUEUE_C];
  ASSERT_OK(request_pool_service->GetPoolConfig(QUEUE_C, &config_c));

  AdmissionController::PoolStats* stats_c = admission_controller->GetPoolStats(QUEUE_C);
  CheckPoolStatsEmpty(stats_c);

  int64_t max_to_dequeue;
  // Queue is empty, so nothing to dequeue
  ASSERT_TRUE(queue_c.empty());
  max_to_dequeue = admission_controller->GetMaxToDequeue(queue_c, stats_c, config_c);
  ASSERT_EQ(0, max_to_dequeue);

  // Queue a query.
  Promise<AdmissionOutcome, PromiseMode::MULTIPLE_PRODUCER> admit_outcome;
  AdmissionController::QueueNode* queue_node =
      makeQueueNode(admission_controller, coord_id, &admit_outcome, config_c, QUEUE_C);
  queue_c.Enqueue(queue_node);
  stats_c->Queue();
  ASSERT_FALSE(queue_c.empty());

  // Check we put it in the queue/
  max_to_dequeue = admission_controller->GetMaxToDequeue(queue_c, stats_c, config_c);
  ASSERT_EQ(1, max_to_dequeue);
  admission_controller->pool_config_map_[queue_node->pool_name] = queue_node->pool_cfg;

  // Try to dequeue a query which will be rejected.
  admission_controller->TryDequeue();
  ASSERT_TRUE(queue_c.empty());
  // The pool max_requests is 0 so query will be rejected.
  ASSERT_EQ(AdmissionOutcome::REJECTED, queue_node->admit_outcome->Get());
  ASSERT_EQ("disabled by requests limit set to 0", queue_node->not_admitted_reason);

  // Queue another query.
  Promise<AdmissionOutcome, PromiseMode::MULTIPLE_PRODUCER> canceled_outcome;
  // Mark the outcome as cancelled to force execution through the cancellation path in
  // AdmissionController::TryDequeue()
  canceled_outcome.Set(AdmissionOutcome::CANCELLED);
  queue_node =
      makeQueueNode(admission_controller, coord_id, &canceled_outcome, config_c, QUEUE_C);
  queue_node->pool_cfg.__set_max_requests(1);
  queue_node->pool_cfg.__set_max_mem_resources(2 * GIGABYTE);
  admission_controller->pool_config_map_[queue_node->pool_name] = queue_node->pool_cfg;
  queue_c.Enqueue(queue_node);
  stats_c->Queue();
  ASSERT_FALSE(queue_c.empty());

  // Try to dequeue a query which will be cancelled.
  admission_controller->TryDequeue();
  ASSERT_TRUE(queue_c.empty());
  ASSERT_EQ(AdmissionOutcome::CANCELLED, queue_node->admit_outcome->Get());
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

  // Get a default PoolConfig (as fair-scheduler and llama files not set)
  TPoolConfig config;
  const string QUEUE = "unused";
  ASSERT_OK(request_pool_service->GetPoolConfig(QUEUE, &config));
  ASSERT_FALSE(AdmissionController::HasQuotaConfig(config));

  // Create a ScheduleState.
  ScheduleState* schedule_state = MakeScheduleState(QUEUE, config, 1, 1000);

  // Get a PoolStats object.
  AdmissionController::PoolStats* pool_stats = admission_controller->GetPoolStats(QUEUE);
  CheckPoolStatsEmpty(pool_stats);

  // Show that Queue, IncrementPerUser, and Dequeue leave stats at zero.
  pool_stats->Queue();
  pool_stats->IncrementPerUser(USER1);
  ASSERT_EQ(1, pool_stats->agg_num_queued());
  ASSERT_EQ(1, pool_stats->metrics()->agg_num_queued->GetValue());
  ASSERT_EQ(
      "[" + USER1 + "]", pool_stats->metrics()->agg_current_users->ToHumanReadable());
  ASSERT_EQ(
      "[" + USER1 + "]", pool_stats->metrics()->local_current_users->ToHumanReadable());
  ASSERT_EQ(1, pool_stats->agg_user_loads_.get(USER1));
  ASSERT_EQ(0, pool_stats->agg_user_loads_.get(USER2));
  pool_stats->Dequeue(false);
  CheckPoolStatsEmpty(pool_stats);
  // the user load should be unchanged.
  ASSERT_EQ(1, pool_stats->agg_user_loads_.get(USER1));
  ASSERT_EQ(
      "[" + USER1 + "]", pool_stats->metrics()->agg_current_users->ToHumanReadable());
  ASSERT_EQ(
      "[" + USER1 + "]", pool_stats->metrics()->local_current_users->ToHumanReadable());

  // Show that Admit and Release leave stats at zero.
  AdmissionController::PerUserTracking per_user_tracking{USER1, true, false};
  pool_stats->AdmitQueryAndMemory(*schedule_state, false, per_user_tracking);
  ASSERT_EQ(1, pool_stats->agg_num_running());
  ASSERT_EQ(1, pool_stats->metrics()->agg_num_running->GetValue());
  ASSERT_EQ(
      "[" + USER1 + "]", pool_stats->metrics()->agg_current_users->ToHumanReadable());
  ASSERT_EQ(
      "[" + USER1 + "]", pool_stats->metrics()->local_current_users->ToHumanReadable());

  int64_t mem_to_release = 0;
  for (auto& backend_state : schedule_state->per_backend_schedule_states()) {
    mem_to_release +=
        admission_controller->GetMemToAdmit(*schedule_state, backend_state.second);
  }
  pool_stats->ReleaseMem(mem_to_release);
  ASSERT_EQ(1, pool_stats->agg_user_loads_.get(USER1));
  pool_stats->ReleaseQuery(0, false, USER1);
  ASSERT_EQ(0, pool_stats->agg_user_loads_.get(USER1));
  ASSERT_EQ("[]", pool_stats->metrics()->agg_current_users->ToHumanReadable());
  ASSERT_EQ("[]", pool_stats->metrics()->local_current_users->ToHumanReadable());
  CheckPoolStatsEmpty(pool_stats);

  // Check that IncrementPerUser and DecrementPerUser have opposite effects.
  ASSERT_EQ(0, pool_stats->GetUserLoad(USER1));
  pool_stats->IncrementPerUser(USER1);
  pool_stats->IncrementPerUser(USER1);
  ASSERT_EQ(2, pool_stats->local_stats_.user_loads[USER1]);
  ASSERT_EQ(2, pool_stats->agg_user_loads_.get(USER1));
  ASSERT_EQ(
      "[" + USER1 + "]", pool_stats->metrics()->agg_current_users->ToHumanReadable());
  ASSERT_EQ(
      "[" + USER1 + "]", pool_stats->metrics()->local_current_users->ToHumanReadable());
  pool_stats->DecrementPerUser(USER1);
  pool_stats->DecrementPerUser(USER1);
  ASSERT_EQ(0, pool_stats->local_stats_.user_loads[USER1]);
  ASSERT_EQ(0, pool_stats->agg_user_loads_.get(USER1));
  ASSERT_EQ("[]", pool_stats->metrics()->agg_current_users->ToHumanReadable());
  ASSERT_EQ("[]", pool_stats->metrics()->local_current_users->ToHumanReadable());
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
  schedule_state->UpdateMemoryRequirements(pool_config, group.second.admit_mem_limit(),
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
  schedule_state->UpdateMemoryRequirements(pool_config, group1.second.admit_mem_limit(),
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
  schedule_state->UpdateMemoryRequirements(pool_config, group2.second.admit_mem_limit(),
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
  schedule_state->UpdateMemoryRequirements(pool_config, group3.second.admit_mem_limit(),
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
  schedule_state->UpdateMemoryRequirements(pool_config, group4.second.admit_mem_limit(),
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
  schedule_state->UpdateMemoryRequirements(pool_config, group5.second.admit_mem_limit(),
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
  schedule_state->UpdateMemoryRequirements(pool_config, group.second.admit_mem_limit(),
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
  // Neither coordinator nor executor has enough available memory.
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
  bool can_accommodate = CanAccommodateMaxInitialReservation(
      *schedule_state, pool_config, &not_admitted_reason);
  EXPECT_STR_CONTAINS(not_admitted_reason,
      "minimum memory reservation is greater than memory available to the query for "
      "buffer reservations. Memory reservation needed given the current plan: 600.00 MB."
      " Adjust the MEM_LIMIT option ");
  ASSERT_FALSE(can_accommodate);
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
  bool can_accommodate = CanAccommodateMaxInitialReservation(
      *schedule_state, pool_config, &not_admitted_reason);
  EXPECT_STR_CONTAINS(not_admitted_reason,
      "minimum memory reservation is greater than memory available to the query for "
      "buffer reservations. Memory reservation needed given the current plan: 600.00 MB."
      " Adjust the impala.admission-control.min-query-mem-limit of request pool "
      "'default' ");
  ASSERT_FALSE(can_accommodate);
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
  bool can_accommodate = CanAccommodateMaxInitialReservation(
      *schedule_state, pool_config, &not_admitted_reason);
  EXPECT_STR_CONTAINS(not_admitted_reason,
      "minimum memory reservation is greater than memory available to the query for "
      "buffer reservations. Memory reservation needed given the current plan: 600.00 MB."
      " Adjust the impala.admission-control.max-query-mem-limit of request pool "
      "'default' ");
  ASSERT_FALSE(can_accommodate);
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
  bool can_accommodate = CanAccommodateMaxInitialReservation(
      *schedule_state, pool_config, &not_admitted_reason);
  EXPECT_STR_CONTAINS(not_admitted_reason,
      "minimum memory reservation is greater than memory available to the query for "
      "buffer reservations. Memory reservation needed given the current plan: 4.00 GB. "
      "Adjust the MEM_LIMIT_EXECUTORS option ");
  ASSERT_FALSE(can_accommodate);
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
  bool can_accommodate = CanAccommodateMaxInitialReservation(
      *schedule_state, pool_config, &not_admitted_reason);
  EXPECT_STR_CONTAINS(not_admitted_reason,
      "minimum memory reservation is greater than memory available to the query for "
      "buffer reservations. Memory reservation needed given the current plan: 4.00 GB. "
      "Adjust the MEM_LIMIT_COORDINATORS option ");
  ASSERT_FALSE(can_accommodate);
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
  bool can_accommodate = CanAccommodateMaxInitialReservation(
      *schedule_state, pool_config, &not_admitted_reason);
  EXPECT_STR_CONTAINS(not_admitted_reason,
      "minimum memory reservation is greater than memory available to the query for "
      "buffer reservations. Memory reservation needed given the current plan: 2.00 GB. "
      "Adjust the system memory or the CGroup memory limit ");
  ASSERT_FALSE(can_accommodate);
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

  // Get the PoolConfig for the global "root" configuration.
  TPoolConfig config_root;
  ASSERT_OK(request_pool_service->GetPoolConfig(QUEUE_ROOT, &config_root));

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
      ASSERT_TRUE(
          admission_controller->CanAdmitRequest(*schedule_state, pool_config, config_root,
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

/// Unit test for AdmissionController::AggregatedUserLoads
/// and for its associated helper methods.
TEST_F(AdmissionControllerTest, AggregatedUserLoads) {
  AdmissionController::AggregatedUserLoads user_loads;
  // Value is zero before any inserts.
  ASSERT_EQ(0, user_loads.size());
  ASSERT_EQ(0, user_loads.get(USER1));
  ASSERT_EQ(0, user_loads.size());

  ASSERT_EQ(1, user_loads.increment(USER1));
  ASSERT_EQ(1, user_loads.get(USER1));
  ASSERT_EQ(1, user_loads.size());
  ASSERT_EQ("user1:1 ", user_loads.DebugString());
  ASSERT_EQ(0, user_loads.decrement(USER1));
  ASSERT_EQ(0, user_loads.get(USER1));
  ASSERT_EQ(0, user_loads.size());
  // Show we cannot go below zero.
  IMPALA_ASSERT_DEBUG_DEATH(user_loads.decrement(USER1), /* no expected message */ "");
  ASSERT_EQ(0, user_loads.get(USER1));
  ASSERT_EQ(0, user_loads.size());

  user_loads.insert(USER2, 12);
  ASSERT_EQ(12, user_loads.get(USER2));
  ASSERT_EQ(1, user_loads.size());

  // Test that clear() works.
  user_loads.clear();
  ASSERT_EQ(0, user_loads.get(USER2));
  ASSERT_EQ(0, user_loads.size());

  AdmissionController::UserLoads loads1{{USER1, 1}, {USER2, 4}};
  ASSERT_EQ("user1:1 user2:4 ", AdmissionController::DebugString(loads1));

  user_loads.add_loads(loads1);
  ASSERT_EQ(2, user_loads.size());
  ASSERT_EQ(1, user_loads.get(USER1));
  ASSERT_EQ(4, user_loads.get(USER2));
  // check input is unchanged.
  ASSERT_EQ(1, loads1[USER1]);
  ASSERT_EQ(4, loads1[USER2]);

  AdmissionController::UserLoads loads2{{USER1, 1}, {USER3, 6}};
  user_loads.add_loads(loads2);
  ASSERT_EQ(3, user_loads.size());
  ASSERT_EQ(2, user_loads.get(USER1));
  ASSERT_EQ(4, user_loads.get(USER2));
  ASSERT_EQ(6, user_loads.get(USER3));
  ASSERT_EQ(5, user_loads.decrement(USER3));
  ASSERT_EQ(4, user_loads.decrement(USER3));
}

/// Unit test for AdmissionController::HasSufficientGroupQuota
TEST_F(AdmissionControllerTest, HasSufficientGroupQuota) {
  const vector<std::string> groups = {"group1", "group2", "group3", "group4"};
  TPoolConfig config;
  std::map<std::string, int32_t> limits = {
      // These limits are unordered so that we touch both code paths that add a group to
      // extra_groups in HasSufficientGroupQuota().
      {"group1", 3},
      {"group2", 1},
      {"group4", 4}};
  config.__set_group_query_limits(limits);

  string quota_exceeded_reason;
  bool key_matched;
  bool ok;
  ok = AdmissionController::HasSufficientGroupQuota(
      USER1, groups, config, "poolname", 100, &quota_exceeded_reason, &key_matched);
  EXPECT_FALSE(ok);
  EXPECT_STR_CONTAINS(quota_exceeded_reason,
      "current per-group load 100 for user 'user1' in group 'group4' is at or above the "
      "group limit 4 in pool 'poolname' (Ignored Group Quotas 'group2':1 'group1':3)");
}

} // end namespace impala
