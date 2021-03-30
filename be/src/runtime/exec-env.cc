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

#include "runtime/exec-env.h"

#include <vector>

#include <boost/algorithm/string.hpp>
#include <gflags/gflags.h>
#include <gutil/strings/substitute.h>
#include <kudu/client/client.h>

#include "catalog/catalog-service-client-wrapper.h"
#include "common/logging.h"
#include "common/object-pool.h"
#include "exec/kudu-util.h"
#include "kudu/rpc/service_if.h"
#include "rpc/rpc-mgr.h"
#include "runtime/bufferpool/buffer-pool.h"
#include "runtime/bufferpool/reservation-tracker.h"
#include "runtime/client-cache.h"
#include "runtime/coordinator.h"
#include "runtime/hbase-table-factory.h"
#include "runtime/hdfs-fs-cache.h"
#include "runtime/io/disk-io-mgr.h"
#include "runtime/krpc-data-stream-mgr.h"
#include "runtime/lib-cache.h"
#include "runtime/mem-tracker.h"
#include "runtime/query-exec-mgr.h"
#include "runtime/thread-resource-mgr.h"
#include "runtime/tmp-file-mgr.h"
#include "scheduling/admission-controller.h"
#include "scheduling/cluster-membership-mgr.h"
#include "scheduling/request-pool-service.h"
#include "scheduling/scheduler.h"
#include "service/control-service.h"
#include "service/data-stream-service.h"
#include "service/frontend.h"
#include "service/impala-server.h"
#include "statestore/statestore-subscriber.h"
#include "util/cgroup-util.h"
#include "util/cpu-info.h"
#include "util/debug-util.h"
#include "util/default-path-handlers.h"
#include "util/hdfs-bulk-ops.h"
#include "util/impalad-metrics.h"
#include "util/mem-info.h"
#include "util/memory-metrics.h"
#include "util/metrics.h"
#include "util/network-util.h"
#include "util/openssl-util.h"
#include "util/parse-util.h"
#include "util/periodic-counter-updater.h"
#include "util/pretty-printer.h"
#include "util/system-state-info.h"
#include "util/test-info.h"
#include "util/thread-pool.h"
#include "util/uid-util.h"
#include "util/webserver.h"

#include "common/names.h"

using boost::algorithm::join;
using kudu::rpc::GeneratedServiceIf;
using namespace strings;

DEFINE_string(catalog_service_host, "localhost",
    "hostname where CatalogService is running");
DEFINE_bool(enable_webserver, true, "If true, debug webserver is enabled");
DEFINE_string(state_store_host, "localhost",
    "hostname where StatestoreService is running");
DEFINE_int32(state_store_subscriber_port, 23000,
    "port where StatestoreSubscriberService should be exported");
DEFINE_int32(num_hdfs_worker_threads, 16,
    "(Advanced) The number of threads in the global HDFS operation pool");
DEFINE_int32(max_concurrent_queries, 0,
    "(Deprecated) This has been replaced with --admission_control_slots, which "
    "better accounts for the higher parallelism of queries with mt_dop > 1. "
    "If --admission_control_slots is not set, the value of --max_concurrent_queries "
    "is used instead for backward compatibility.");
DEFINE_int32(admission_control_slots, 0,
    "(Advanced) The maximum degree of parallelism to run queries with on this backend. "
    "This determines the number of slots available to queries in admission control for "
    "this backend. The degree of parallelism of the query determines the number of slots "
    "that it needs. Defaults to number of cores / -num_cores for executors, and 8x that "
    "value for dedicated coordinators).");

DEFINE_bool_hidden(use_local_catalog, false,
    "Use experimental implementation of a local catalog. If this is set, "
    "the catalog service is not used and does not need to be started.");
DEFINE_int32_hidden(local_catalog_cache_mb, -1,
    "If --use_local_catalog is enabled, configures the size of the catalog "
    "cache within each impalad. If this is set to -1, the cache is auto-"
    "configured to 60% of the configured Java heap size. Note that the Java "
    "heap size is distinct from and typically smaller than the overall "
    "Impala memory limit.");
DEFINE_int32_hidden(local_catalog_cache_expiration_s, 60 * 60,
    "If --use_local_catalog is enabled, configures the expiration time "
    "of the catalog cache within each impalad. Even if the configured "
    "cache capacity has not been reached, items are removed from the cache "
    "if they have not been accessed in this amount of time.");
DEFINE_int32_hidden(local_catalog_max_fetch_retries, 40,
    "If --use_local_catalog is enabled, configures the maximum number of times "
    "the frontend retries when fetching a metadata object from the impalad "
    "coordinator's local catalog cache.");

DECLARE_int32(state_store_port);
DECLARE_int32(num_threads_per_core);
DECLARE_int32(num_cores);
DECLARE_int32(be_port);
DECLARE_int32(krpc_port);
DECLARE_string(mem_limit);
DECLARE_bool(mem_limit_includes_jvm);
DECLARE_string(buffer_pool_limit);
DECLARE_string(buffer_pool_clean_pages_limit);
DECLARE_int64(min_buffer_size);
DECLARE_bool(is_coordinator);
DECLARE_bool(is_executor);
DECLARE_string(webserver_interface);
DECLARE_int32(webserver_port);
DECLARE_int64(tcmalloc_max_total_thread_cache_bytes);

DECLARE_string(ssl_client_ca_certificate);

DEFINE_int32(backend_client_connection_num_retries, 3, "Retry backend connections.");
// When network is unstable, TCP will retry and sending could take longer time.
// Choose 5 minutes as default timeout because we don't want RPC timeout be triggered
// by intermittent network issue. The timeout should not be too long either, otherwise
// query could hang for a while before it's cancelled.
DEFINE_int32(backend_client_rpc_timeout_ms, 300000, "(Advanced) The underlying "
    "TSocket send/recv timeout in milliseconds for a backend client RPC. ");

DEFINE_int32(catalog_client_connection_num_retries, 10, "The number of times connections "
    "or RPCs to the catalog should be retried.");
DEFINE_int32(catalog_client_rpc_timeout_ms, 0, "(Advanced) The underlying TSocket "
    "send/recv timeout in milliseconds for a catalog client RPC.");
DEFINE_int32(catalog_client_rpc_retry_interval_ms, 3000, "(Advanced) The time to wait "
    "before retrying when the catalog RPC client fails to connect to catalogd or when "
    "RPCs to the catalogd fail.");

DEFINE_int32(metrics_webserver_port, 0,
    "If non-zero, the port to run the metrics webserver on, which exposes the /metrics, "
    "/jsonmetrics, /metrics_prometheus, and /healthz endpoints without authentication "
    "enabled.");

DEFINE_string(metrics_webserver_interface, "",
    "Interface to start metrics webserver on. If blank, webserver binds to 0.0.0.0");

const static string DEFAULT_FS = "fs.defaultFS";

// The multiplier for how many queries a dedicated coordinator can run compared to an
// executor. This is only effective when using non-default settings for executor groups
// and the absolute value can be overridden by the '--admission_control_slots' flag.
const static int COORDINATOR_CONCURRENCY_MULTIPLIER = 8;

namespace {
using namespace impala;
/// Helper method to forward cluster membership updates to the frontend.
///
/// The frontend uses cluster membership information to determine whether it expects the
/// scheduler to assign local or remote reads. It also uses the number of executors to
/// determine the join type (partitioned vs broadcast). For the default executor group, we
/// assume that local reads are preferred and will include the hostnames and IP addresses
/// in the update to the frontend. For non-default executor groups, we assume that we will
/// read data remotely and will only send the number of executors in the largest healthy
/// group.
void SendClusterMembershipToFrontend(
    ClusterMembershipMgr::SnapshotPtr& snapshot, Frontend* frontend) {
  TUpdateExecutorMembershipRequest update_req;
  const ExecutorGroup* group = nullptr;
  bool is_default_group = false;
  auto default_it =
      snapshot->executor_groups.find(ImpalaServer::DEFAULT_EXECUTOR_GROUP_NAME);
  if (default_it != snapshot->executor_groups.end()) {
    is_default_group = true;
    group = &(default_it->second);
  } else {
    int max_num_executors = 0;
    // Find largest healthy group.
    for (const auto& it : snapshot->executor_groups) {
      if (!it.second.IsHealthy()) continue;
      int num_executors = it.second.NumExecutors();
      if (num_executors > max_num_executors) {
        max_num_executors = num_executors;
        group = &(it.second);
      }
    }
  }
  if (group) {
    for (const auto& backend : group->GetAllExecutorDescriptors()) {
      if (backend.is_executor()) {
        if (is_default_group) {
          update_req.hostnames.insert(backend.address().hostname());
          update_req.ip_addresses.insert(backend.ip_address());
        }
        update_req.num_executors++;
      }
    }
  }
  Status status = frontend->UpdateExecutorMembership(update_req);
  if (!status.ok()) {
    LOG(WARNING) << "Error updating frontend membership snapshot: " << status.GetDetail();
  }
}
}

namespace impala {

struct ExecEnv::KuduClientPtr {
  kudu::client::sp::shared_ptr<kudu::client::KuduClient> kudu_client;
};

ExecEnv* ExecEnv::exec_env_ = nullptr;

ExecEnv::ExecEnv()
  : ExecEnv(FLAGS_krpc_port, FLAGS_state_store_subscriber_port, FLAGS_webserver_port,
        FLAGS_state_store_host, FLAGS_state_store_port) {}

ExecEnv::ExecEnv(int krpc_port, int subscriber_port, int webserver_port,
    const string& statestore_host, int statestore_port)
  : obj_pool_(new ObjectPool),
    metrics_(new MetricGroup("impala-metrics")),
    // Create the CatalogServiceClientCache with num_retries = 1 and wait_ms = 0.
    // Connections are still retried, but the retry mechanism is driven by
    // DoRpcWithRetry. Clients should always use DoRpcWithRetry rather than DoRpc to
    // ensure that both RPCs and connections are retried.
    catalogd_client_cache_(new CatalogServiceClientCache(1, 0,
        FLAGS_catalog_client_rpc_timeout_ms, FLAGS_catalog_client_rpc_timeout_ms, "",
        !FLAGS_ssl_client_ca_certificate.empty())),
    htable_factory_(new HBaseTableFactory()),
    disk_io_mgr_(new io::DiskIoMgr()),
    webserver_(new Webserver(FLAGS_webserver_interface, webserver_port, metrics_.get())),
    pool_mem_trackers_(new PoolMemTrackerRegistry),
    thread_mgr_(new ThreadResourceMgr),
    tmp_file_mgr_(new TmpFileMgr),
    frontend_(new Frontend()),
    async_rpc_pool_(new CallableThreadPool("rpc-pool", "async-rpc-sender", 8, 10000)),
    query_exec_mgr_(new QueryExecMgr()),
    rpc_metrics_(metrics_->GetOrCreateChildGroup("rpc")),
    enable_webserver_(FLAGS_enable_webserver && webserver_port > 0),
    configured_backend_address_(MakeNetworkAddress(FLAGS_hostname, krpc_port)) {
  UUIDToUniqueIdPB(boost::uuids::random_generator()(), &backend_id_);

  // Resolve hostname to IP address.
  ABORT_IF_ERROR(HostnameToIpAddr(FLAGS_hostname, &ip_address_));

  // KRPC relies on resolved IP address.
  krpc_address_.__set_hostname(ip_address_);
  krpc_address_.__set_port(krpc_port);
  rpc_mgr_.reset(new RpcMgr(IsInternalTlsConfigured()));
  stream_mgr_.reset(new KrpcDataStreamMgr(metrics_.get()));

  request_pool_service_.reset(new RequestPoolService(metrics_.get()));

  TNetworkAddress subscriber_address =
      MakeNetworkAddress(FLAGS_hostname, subscriber_port);
  TNetworkAddress statestore_address =
      MakeNetworkAddress(statestore_host, statestore_port);

  // Set StatestoreSubscriber::subscriber_id as hostname + be_port.
  statestore_subscriber_.reset(
      new StatestoreSubscriber(Substitute("impalad@$0:$1", FLAGS_hostname, FLAGS_be_port),
          subscriber_address, statestore_address, metrics_.get()));

  if (FLAGS_is_coordinator) {
    hdfs_op_thread_pool_.reset(
        CreateHdfsOpThreadPool("hdfs-worker-pool", FLAGS_num_hdfs_worker_threads, 1024));
    scheduler_.reset(new Scheduler(metrics_.get(), request_pool_service_.get()));
  }

  cluster_membership_mgr_.reset(new ClusterMembershipMgr(
      PrintId(backend_id_), statestore_subscriber_.get(), metrics_.get()));

  // TODO: Consider removing AdmissionController from executor only impalads.
  admission_controller_.reset(
      new AdmissionController(cluster_membership_mgr_.get(), statestore_subscriber_.get(),
          request_pool_service_.get(), metrics_.get(), configured_backend_address_));

  if (FLAGS_metrics_webserver_port > 0) {
    metrics_webserver_.reset(new Webserver(FLAGS_metrics_webserver_interface,
        FLAGS_metrics_webserver_port, metrics_.get(), Webserver::AuthMode::NONE));
  }

  exec_env_ = this;
}

ExecEnv::~ExecEnv() {
  if (buffer_reservation_ != nullptr) buffer_reservation_->Close();
  if (rpc_mgr_ != nullptr) rpc_mgr_->Shutdown();
  disk_io_mgr_.reset(); // Need to tear down before mem_tracker_.
}

Status ExecEnv::InitForFeTests() {
  mem_tracker_.reset(new MemTracker(-1, "Process"));
  is_fe_tests_ = true;
  return Status::OK();
}

Status ExecEnv::Init() {
  LOG(INFO) << "Initializing impalad with backend uuid: " << PrintId(backend_id_);
  // Initialize thread pools
  if (FLAGS_is_coordinator) {
    RETURN_IF_ERROR(hdfs_op_thread_pool_->Init());
  }
  RETURN_IF_ERROR(async_rpc_pool_->Init());

  int64_t bytes_limit;
  RETURN_IF_ERROR(ChooseProcessMemLimit(&bytes_limit));

  // Need to register JVM metrics first so that we can use them to compute the buffer pool
  // limit.
  JvmMemoryMetric::InitMetrics(metrics_.get());
  if (!BitUtil::IsPowerOf2(FLAGS_min_buffer_size)) {
    return Status(Substitute(
        "--min_buffer_size must be a power-of-two: $0", FLAGS_min_buffer_size));
  }
  // The bytes limit we want to size everything else as a fraction of, excluding the
  // JVM.
  admit_mem_limit_ = bytes_limit;
  if (FLAGS_mem_limit_includes_jvm) {
    // The JVM max heap size is static and therefore known at this point. Other categories
    // of JVM memory consumption are much smaller and dynamic so it is simpler not to
    // include them here.
    admit_mem_limit_ -= JvmMemoryMetric::HEAP_MAX_USAGE->GetValue();
  }

  bool is_percent;
  int64_t buffer_pool_limit = ParseUtil::ParseMemSpec(FLAGS_buffer_pool_limit,
      &is_percent, admit_mem_limit_);
  if (buffer_pool_limit <= 0) {
    return Status(Substitute("Invalid --buffer_pool_limit value, must be a percentage or "
          "positive bytes value or percentage: $0", FLAGS_buffer_pool_limit));
  }
  buffer_pool_limit = BitUtil::RoundDown(buffer_pool_limit, FLAGS_min_buffer_size);
  LOG(INFO) << "Buffer pool limit: "
            << PrettyPrinter::Print(buffer_pool_limit, TUnit::BYTES);

  int64_t clean_pages_limit = ParseUtil::ParseMemSpec(FLAGS_buffer_pool_clean_pages_limit,
      &is_percent, buffer_pool_limit);
  if (clean_pages_limit <= 0) {
    return Status(Substitute("Invalid --buffer_pool_clean_pages_limit value, must be a "
        "percentage or positive bytes value or percentage: $0",
        FLAGS_buffer_pool_clean_pages_limit));
  }
  InitBufferPool(FLAGS_min_buffer_size, buffer_pool_limit, clean_pages_limit);

  admission_slots_ = CpuInfo::num_cores();
  if (FLAGS_admission_control_slots > 0) {
    if (FLAGS_max_concurrent_queries > 0) {
      LOG(WARNING) << "Ignored --max_concurrent_queries, --admission_control_slots was "
                   << "set and takes precedence.";
    }
    admission_slots_ = FLAGS_admission_control_slots;
  } else if (FLAGS_max_concurrent_queries > 0) {
    admission_slots_ = FLAGS_max_concurrent_queries;
  } else if (FLAGS_is_coordinator && !FLAGS_is_executor) {
    // By default we assume that dedicated coordinators can handle more queries than
    // executors.
    admission_slots_ *= COORDINATOR_CONCURRENCY_MULTIPLIER;
  }

  InitSystemStateInfo();

  if (enable_webserver_) {
    RETURN_IF_ERROR(metrics_->RegisterHttpHandlers(webserver_.get()));
  }
  if (FLAGS_metrics_webserver_port > 0) {
    RETURN_IF_ERROR(metrics_->RegisterHttpHandlers(metrics_webserver_.get()));
    RETURN_IF_ERROR(metrics_webserver_->Start());
  }
  catalogd_client_cache_->InitMetrics(metrics_.get(), "catalog.server");
  RETURN_IF_ERROR(RegisterMemoryMetrics(
      metrics_.get(), true, buffer_reservation_.get(), buffer_pool_.get()));
  // Initialize impalad metrics
  ImpaladMetrics::CreateMetrics(
      exec_env_->metrics()->GetOrCreateChildGroup("impala-server"));

  InitMemTracker(bytes_limit);

  // Initializes the RPCMgr, ControlServices and DataStreamServices.
  // Initialization needs to happen in the following order due to dependencies:
  // - RPC manager, DataStreamService and DataStreamManager.
  RETURN_IF_ERROR(rpc_mgr_->Init(krpc_address_));
  control_svc_.reset(new ControlService(rpc_metrics_));
  RETURN_IF_ERROR(control_svc_->Init());
  data_svc_.reset(new DataStreamService(rpc_metrics_));
  RETURN_IF_ERROR(data_svc_->Init());
  RETURN_IF_ERROR(stream_mgr_->Init(data_svc_->mem_tracker()));
  // Bump thread cache to 1GB to reduce contention for TCMalloc central
  // list's spinlock.
  if (FLAGS_tcmalloc_max_total_thread_cache_bytes == 0) {
    FLAGS_tcmalloc_max_total_thread_cache_bytes = 1 << 30;
  }

#if !defined(ADDRESS_SANITIZER) && !defined(THREAD_SANITIZER)
  // Change the total TCMalloc thread cache size if necessary.
  if (FLAGS_tcmalloc_max_total_thread_cache_bytes > 0 &&
      !MallocExtension::instance()->SetNumericProperty(
          "tcmalloc.max_total_thread_cache_bytes",
          FLAGS_tcmalloc_max_total_thread_cache_bytes)) {
    return Status("Failed to change TCMalloc total thread cache size.");
  }
  // A MemTracker for TCMalloc overhead which is the difference between the physical bytes
  // reserved (TcmallocMetric::PHYSICAL_BYTES_RESERVED) and the bytes in use
  // (TcmallocMetrics::BYTES_IN_USE). This overhead accounts for all the cached freelists
  // used by TCMalloc.
  IntGauge* negated_bytes_in_use = obj_pool_->Add(new NegatedGauge(
      MakeTMetricDef("negated_tcmalloc_bytes_in_use", TMetricKind::GAUGE, TUnit::BYTES),
      TcmallocMetric::BYTES_IN_USE));
  vector<IntGauge*> overhead_metrics;
  overhead_metrics.push_back(negated_bytes_in_use);
  overhead_metrics.push_back(TcmallocMetric::PHYSICAL_BYTES_RESERVED);
  SumGauge* tcmalloc_overhead = obj_pool_->Add(new SumGauge(
      MakeTMetricDef("tcmalloc_overhead", TMetricKind::GAUGE, TUnit::BYTES),
      overhead_metrics));
  obj_pool_->Add(
      new MemTracker(tcmalloc_overhead, -1, "TCMalloc Overhead", mem_tracker_.get()));
#endif
  mem_tracker_->RegisterMetrics(metrics_.get(), "mem-tracker.process");

  RETURN_IF_ERROR(disk_io_mgr_->Init());

  // Start services in order to ensure that dependencies between them are met
  if (enable_webserver_) {
    AddDefaultUrlCallbacks(webserver_.get(), metrics_.get(), mem_tracker_.get());
    RETURN_IF_ERROR(webserver_->Start());
  } else {
    LOG(INFO) << "Not starting webserver";
  }

  RETURN_IF_ERROR(cluster_membership_mgr_->Init());
  if (FLAGS_is_coordinator) {
    cluster_membership_mgr_->RegisterUpdateCallbackFn(
        [this](ClusterMembershipMgr::SnapshotPtr snapshot) {
          SendClusterMembershipToFrontend(snapshot, this->frontend());
        });
  }

  RETURN_IF_ERROR(admission_controller_->Init());

  // Get the fs.defaultFS value set in core-site.xml and assign it to configured_defaultFs
  TGetHadoopConfigRequest config_request;
  config_request.__set_name(DEFAULT_FS);
  TGetHadoopConfigResponse config_response;
  RETURN_IF_ERROR(frontend_->GetHadoopConfig(config_request, &config_response));
  if (config_response.__isset.value) {
    default_fs_ = config_response.value;
  } else {
    default_fs_ = "hdfs://";
  }

  return Status::OK();
}

Status ExecEnv::ChooseProcessMemLimit(int64_t* bytes_limit) {
  // Depending on the system configuration, we detect the total amount of memory
  // available to the system - either the available physical memory, or if overcommitting
  // is turned off, we use the memory commit limit from /proc/meminfo (see IMPALA-1690).
  // The 'memory' CGroup can also impose a lower limit on memory consumption,
  // so we take the minimum of the system memory and the CGroup memory limit.
  int64_t avail_mem = MemInfo::physical_mem();
  bool use_commit_limit =
      MemInfo::vm_overcommit() == 2 && MemInfo::commit_limit() < MemInfo::physical_mem();
  if (use_commit_limit) {
    avail_mem = MemInfo::commit_limit();
    // There might be the case of misconfiguration, when on a system swap is disabled
    // and overcommitting is turned off the actual usable memory is less than the
    // available physical memory.
    LOG(WARNING) << "This system shows a discrepancy between the available "
                 << "memory and the memory commit limit allowed by the "
                 << "operating system. ( Mem: " << MemInfo::physical_mem()
                 << "<=> CommitLimit: " << MemInfo::commit_limit() << "). "
                 << "Impala will adhere to the smaller value when setting the process "
                 << "memory limit. Please verify the system configuration. Specifically, "
                 << "/proc/sys/vm/overcommit_memory and "
                 << "/proc/sys/vm/overcommit_ratio.";
  }
  LOG(INFO) << "System memory available: " << PrettyPrinter::PrintBytes(avail_mem)
            << " (from " << (use_commit_limit ? "commit limit" : "physical mem") << ")";
  int64_t cgroup_mem_limit;
  Status cgroup_mem_status = CGroupUtil::FindCGroupMemLimit(&cgroup_mem_limit);
  if (cgroup_mem_status.ok()) {
    if (cgroup_mem_limit < avail_mem) {
      avail_mem = cgroup_mem_limit;
      LOG(INFO) << "CGroup memory limit for this process reduces physical memory "
                << "available to: " << PrettyPrinter::PrintBytes(avail_mem);
    }
  } else {
    LOG(WARNING) << "Could not detect CGroup memory limit, assuming unlimited: $0"
                 << cgroup_mem_status.GetDetail();
  }
  bool is_percent;
  *bytes_limit = ParseUtil::ParseMemSpec(FLAGS_mem_limit, &is_percent, avail_mem);
  // ParseMemSpec() returns -1 for invalid input and 0 to mean unlimited. From Impala
  // 2.11 onwards we do not support unlimited process memory limits.
  if (*bytes_limit <= 0) {
    return Status(Substitute("The process memory limit (--mem_limit) must be a positive "
                             "bytes value or percentage: $0", FLAGS_mem_limit));
  }
  if (is_percent) {
    LOG(INFO) << "Using process memory limit: " << PrettyPrinter::PrintBytes(*bytes_limit)
              << " (--mem_limit=" << FLAGS_mem_limit << " of "
              << PrettyPrinter::PrintBytes(avail_mem) << ")";
  } else {
    LOG(INFO) << "Using process memory limit: " << PrettyPrinter::PrintBytes(*bytes_limit)
              << " (--mem_limit=" << FLAGS_mem_limit << ")";
  }
  if (*bytes_limit > MemInfo::physical_mem()) {
    LOG(WARNING) << "Process memory limit " << PrettyPrinter::PrintBytes(*bytes_limit)
                 << " exceeds physical memory of "
                 << PrettyPrinter::PrintBytes(MemInfo::physical_mem());
  }
  if (cgroup_mem_status.ok() && *bytes_limit > cgroup_mem_limit) {
    LOG(WARNING) << "Process Memory limit " << PrettyPrinter::PrintBytes(*bytes_limit)
                 << " exceeds CGroup memory limit of "
                 << PrettyPrinter::PrintBytes(cgroup_mem_limit);
  }
  return Status::OK();
}

Status ExecEnv::StartStatestoreSubscriberService() {
  LOG(INFO) << "Starting statestore subscriber service";

  // Must happen after all topic registrations / callbacks are done
  if (statestore_subscriber_.get() != nullptr) {
    Status status = statestore_subscriber_->Start();
    if (!status.ok()) {
      status.AddDetail("Statestore subscriber did not start up.");
      return status;
    }
  }

  return Status::OK();
}

Status ExecEnv::StartKrpcService() {
  LOG(INFO) << "Starting KRPC service";
  RETURN_IF_ERROR(rpc_mgr_->StartServices());
  return Status::OK();
}

void ExecEnv::SetImpalaServer(ImpalaServer* server) {
  DCHECK(impala_server_ == nullptr) << "ImpalaServer already registered";
  DCHECK(server != nullptr);
  impala_server_ = server;
  // Register the ImpalaServer with the cluster membership manager
  cluster_membership_mgr_->SetLocalBeDescFn([server]() {
    return server->GetLocalBackendDescriptor();
  });
  if (FLAGS_is_coordinator) {
    cluster_membership_mgr_->RegisterUpdateCallbackFn(
        [server](ClusterMembershipMgr::SnapshotPtr snapshot) {
          std::unordered_set<BackendIdPB> current_backend_set;
          for (const auto& it : snapshot->current_backends) {
            current_backend_set.insert(it.second.backend_id());
          }
          server->CancelQueriesOnFailedBackends(current_backend_set);
        });
  }
  if (FLAGS_is_executor && !TestInfo::is_test()) {
    cluster_membership_mgr_->RegisterUpdateCallbackFn(
        [](ClusterMembershipMgr::SnapshotPtr snapshot) {
          std::unordered_set<BackendIdPB> current_backend_set;
          for (const auto& it : snapshot->current_backends) {
            current_backend_set.insert(it.second.backend_id());
          }
          ExecEnv::GetInstance()->query_exec_mgr()->CancelQueriesForFailedCoordinators(
              current_backend_set);
        });
  }
}

void ExecEnv::InitBufferPool(int64_t min_buffer_size, int64_t capacity,
    int64_t clean_pages_limit) {
#if !defined(ADDRESS_SANITIZER) && !defined(THREAD_SANITIZER)
  // Aggressive decommit is required so that unused pages in the TCMalloc page heap are
  // not backed by physical pages and do not contribute towards memory consumption.
  // Enable it in TCMalloc before InitBufferPool().
  MallocExtension::instance()->SetNumericProperty(
      "tcmalloc.aggressive_memory_decommit", 1);
#endif
  buffer_pool_.reset(
      new BufferPool(metrics_.get(), min_buffer_size, capacity, clean_pages_limit));
  buffer_reservation_.reset(new ReservationTracker());
  buffer_reservation_->InitRootTracker(nullptr, capacity);
}

void ExecEnv::InitMemTracker(int64_t bytes_limit) {
  DCHECK(AggregateMemoryMetrics::TOTAL_USED != nullptr) << "Memory metrics not reg'd";
  mem_tracker_.reset(
      new MemTracker(AggregateMemoryMetrics::TOTAL_USED, bytes_limit, "Process"));
  if (FLAGS_mem_limit_includes_jvm) {
    // Add JVM metrics that should count against the process memory limit.
    obj_pool_->Add(new MemTracker(
        JvmMemoryMetric::HEAP_MAX_USAGE, -1, "JVM: max heap size", mem_tracker_.get()));
    obj_pool_->Add(new MemTracker(JvmMemoryMetric::NON_HEAP_COMMITTED, -1,
        "JVM: non-heap committed", mem_tracker_.get()));
  }
  if (buffer_pool_ != nullptr) {
    // Tests can create multiple buffer pools, meaning that the metrics are not usable.
    if (!TestInfo::is_test()) {
      // Add BufferPool MemTrackers for cached memory that is not tracked against queries
      // but is included in process memory consumption.
      obj_pool_->Add(new MemTracker(BufferPoolMetric::FREE_BUFFER_BYTES, -1,
          "Buffer Pool: Free Buffers", mem_tracker_.get()));
      obj_pool_->Add(new MemTracker(BufferPoolMetric::CLEAN_PAGE_BYTES, -1,
          "Buffer Pool: Clean Pages", mem_tracker_.get()));
      // Also need a MemTracker for unused reservations as a negative value. Unused
      // reservations are counted against queries but not against the process memory
      // consumption. This accounts for that difference.
      IntGauge* negated_unused_reservation = obj_pool_->Add(new NegatedGauge(
          MakeTMetricDef("negated_unused_reservation", TMetricKind::GAUGE, TUnit::BYTES),
          BufferPoolMetric::UNUSED_RESERVATION_BYTES));
      obj_pool_->Add(new MemTracker(negated_unused_reservation, -1,
          "Buffer Pool: Unused Reservation", mem_tracker_.get()));
    }
    mem_tracker_->AddGcFunction([buffer_pool=buffer_pool_.get()] (int64_t bytes_to_free)
    {
        // Only free memory in excess of the current reservation - the buffer pool
        // does not need to give up cached memory that is offset by an unused reservation.
        int64_t reserved = BufferPoolMetric::RESERVED->GetValue();
        int64_t allocated_from_sys = BufferPoolMetric::SYSTEM_ALLOCATED->GetValue();
        if (reserved >= allocated_from_sys) return;
        buffer_pool->ReleaseMemory(min(bytes_to_free, allocated_from_sys - reserved));
    });
  }
}

void ExecEnv::InitSystemStateInfo() {
  system_state_info_.reset(new SystemStateInfo());
  PeriodicCounterUpdater::RegisterUpdateFunction([s = system_state_info_.get()]() {
    s->CaptureSystemStateSnapshot();
  });
}

Status ExecEnv::GetKuduClient(
    const vector<string>& master_addresses, kudu::client::KuduClient** client) {
  string master_addr_concat = join(master_addresses, ",");
  lock_guard<SpinLock> l(kudu_client_map_lock_);
  auto kudu_client_map_it = kudu_client_map_.find(master_addr_concat);
  if (kudu_client_map_it == kudu_client_map_.end()) {
    // KuduClient doesn't exist, create it
    KuduClientPtr* kudu_client_ptr = new KuduClientPtr;
    RETURN_IF_ERROR(CreateKuduClient(master_addresses, &kudu_client_ptr->kudu_client));
    kudu_client_map_[master_addr_concat].reset(kudu_client_ptr);
    *client = kudu_client_ptr->kudu_client.get();
  } else {
    // Return existing KuduClient
    *client = kudu_client_map_it->second->kudu_client.get();
  }
  return Status::OK();
}

} // namespace impala
