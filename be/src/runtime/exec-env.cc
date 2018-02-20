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

#include "common/logging.h"
#include "common/object-pool.h"
#include "exec/kudu-util.h"
#include "gen-cpp/ImpalaInternalService.h"
#include "kudu/rpc/service_if.h"
#include "rpc/rpc-mgr.h"
#include "runtime/backend-client.h"
#include "runtime/bufferpool/buffer-pool.h"
#include "runtime/bufferpool/reservation-tracker.h"
#include "runtime/client-cache.h"
#include "runtime/coordinator.h"
#include "runtime/data-stream-mgr.h"
#include "runtime/io/disk-io-mgr.h"
#include "runtime/hbase-table-factory.h"
#include "runtime/hdfs-fs-cache.h"
#include "runtime/krpc-data-stream-mgr.h"
#include "runtime/lib-cache.h"
#include "runtime/mem-tracker.h"
#include "runtime/query-exec-mgr.h"
#include "runtime/thread-resource-mgr.h"
#include "runtime/tmp-file-mgr.h"
#include "scheduling/admission-controller.h"
#include "scheduling/request-pool-service.h"
#include "scheduling/scheduler.h"
#include "service/data-stream-service.h"
#include "service/frontend.h"
#include "statestore/statestore-subscriber.h"
#include "util/debug-util.h"
#include "util/default-path-handlers.h"
#include "util/hdfs-bulk-ops.h"
#include "util/mem-info.h"
#include "util/memory-metrics.h"
#include "util/metrics.h"
#include "util/network-util.h"
#include "util/openssl-util.h"
#include "util/parse-util.h"
#include "util/pretty-printer.h"
#include "util/thread-pool.h"
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
DEFINE_bool(disable_admission_control, false, "Disables admission control.");
DEFINE_bool(use_krpc, true, "If true, use KRPC for the DataStream subsystem. "
    "Otherwise use Thrift RPC.");

DECLARE_int32(state_store_port);
DECLARE_int32(num_threads_per_core);
DECLARE_int32(num_cores);
DECLARE_int32(be_port);
DECLARE_int32(krpc_port);
DECLARE_string(mem_limit);
DECLARE_string(buffer_pool_limit);
DECLARE_string(buffer_pool_clean_pages_limit);
DECLARE_int64(min_buffer_size);
DECLARE_bool(is_coordinator);
DECLARE_int32(webserver_port);
DECLARE_int64(tcmalloc_max_total_thread_cache_bytes);

// TODO-MT: rename or retire
DEFINE_int32(coordinator_rpc_threads, 12, "(Advanced) Number of threads available to "
    "start fragments on remote Impala daemons.");

DECLARE_string(ssl_client_ca_certificate);

DEFINE_int32(backend_client_connection_num_retries, 3, "Retry backend connections.");
// When network is unstable, TCP will retry and sending could take longer time.
// Choose 5 minutes as default timeout because we don't want RPC timeout be triggered
// by intermittent network issue. The timeout should not be too long either, otherwise
// query could hang for a while before it's cancelled.
DEFINE_int32(backend_client_rpc_timeout_ms, 300000, "(Advanced) The underlying "
    "TSocket send/recv timeout in milliseconds for a backend client RPC. ");

DEFINE_int32(catalog_client_connection_num_retries, 3, "Retry catalog connections.");
DEFINE_int32(catalog_client_rpc_timeout_ms, 0, "(Advanced) The underlying TSocket "
    "send/recv timeout in milliseconds for a catalog client RPC.");

const static string DEFAULT_FS = "fs.defaultFS";

namespace impala {

struct ExecEnv::KuduClientPtr {
  kudu::client::sp::shared_ptr<kudu::client::KuduClient> kudu_client;
};

ExecEnv* ExecEnv::exec_env_ = nullptr;

ExecEnv::ExecEnv()
  : ExecEnv(FLAGS_hostname, FLAGS_be_port, FLAGS_krpc_port,
        FLAGS_state_store_subscriber_port, FLAGS_webserver_port,
        FLAGS_state_store_host, FLAGS_state_store_port) {}

ExecEnv::ExecEnv(const string& hostname, int backend_port, int krpc_port,
    int subscriber_port, int webserver_port, const string& statestore_host,
    int statestore_port)
  : obj_pool_(new ObjectPool),
    metrics_(new MetricGroup("impala-metrics")),
    impalad_client_cache_(
        new ImpalaBackendClientCache(FLAGS_backend_client_connection_num_retries, 0,
            FLAGS_backend_client_rpc_timeout_ms, FLAGS_backend_client_rpc_timeout_ms, "",
            !FLAGS_ssl_client_ca_certificate.empty())),
    catalogd_client_cache_(
        new CatalogServiceClientCache(FLAGS_catalog_client_connection_num_retries, 0,
            FLAGS_catalog_client_rpc_timeout_ms, FLAGS_catalog_client_rpc_timeout_ms, "",
            !FLAGS_ssl_client_ca_certificate.empty())),
    htable_factory_(new HBaseTableFactory()),
    disk_io_mgr_(new io::DiskIoMgr()),
    webserver_(new Webserver(webserver_port)),
    pool_mem_trackers_(new PoolMemTrackerRegistry),
    thread_mgr_(new ThreadResourceMgr),
    tmp_file_mgr_(new TmpFileMgr),
    frontend_(new Frontend()),
    async_rpc_pool_(new CallableThreadPool("rpc-pool", "async-rpc-sender", 8, 10000)),
    query_exec_mgr_(new QueryExecMgr()),
    rpc_metrics_(metrics_->GetOrCreateChildGroup("rpc")),
    enable_webserver_(FLAGS_enable_webserver && webserver_port > 0),
    backend_address_(MakeNetworkAddress(hostname, backend_port)) {

  if (FLAGS_use_krpc) {
    VLOG_QUERY << "Using KRPC.";
    // KRPC relies on resolved IP address. It's set in Init().
    krpc_address_.__set_port(krpc_port);
    rpc_mgr_.reset(new RpcMgr(IsInternalTlsConfigured()));
    stream_mgr_.reset(new KrpcDataStreamMgr(metrics_.get()));
  } else {
    stream_mgr_.reset(new DataStreamMgr(metrics_.get()));
  }

  request_pool_service_.reset(new RequestPoolService(metrics_.get()));

  TNetworkAddress subscriber_address = MakeNetworkAddress(hostname, subscriber_port);
  TNetworkAddress statestore_address =
      MakeNetworkAddress(statestore_host, statestore_port);

  statestore_subscriber_.reset(new StatestoreSubscriber(
      Substitute("impalad@$0", TNetworkAddressToString(backend_address_)),
      subscriber_address, statestore_address, metrics_.get()));

  if (FLAGS_is_coordinator) {
    hdfs_op_thread_pool_.reset(
        CreateHdfsOpThreadPool("hdfs-worker-pool", FLAGS_num_hdfs_worker_threads, 1024));
    exec_rpc_thread_pool_.reset(new CallableThreadPool("exec-rpc-pool", "worker",
        FLAGS_coordinator_rpc_threads, numeric_limits<int32_t>::max()));
    scheduler_.reset(new Scheduler(statestore_subscriber_.get(),
        statestore_subscriber_->id(), metrics_.get(), webserver_.get(),
        request_pool_service_.get()));
  }

  if (FLAGS_disable_admission_control) {
    LOG(INFO) << "Admission control is disabled.";
  } else {
    admission_controller_.reset(new AdmissionController(statestore_subscriber_.get(),
        request_pool_service_.get(), metrics_.get(), backend_address_));
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
  // Initialize thread pools
  if (FLAGS_is_coordinator) {
    RETURN_IF_ERROR(exec_rpc_thread_pool_->Init());
    RETURN_IF_ERROR(hdfs_op_thread_pool_->Init());
  }
  RETURN_IF_ERROR(async_rpc_pool_->Init());

  // Initialize global memory limit.
  // Depending on the system configuration, we will have to calculate the process
  // memory limit either based on the available physical memory, or if overcommitting
  // is turned off, we use the memory commit limit from /proc/meminfo (see
  // IMPALA-1690).
  int64_t bytes_limit = 0;
  bool is_percent;
  int64_t system_mem;
  if (MemInfo::vm_overcommit() == 2 &&
      MemInfo::commit_limit() < MemInfo::physical_mem()) {
    system_mem = MemInfo::commit_limit();
    bytes_limit = ParseUtil::ParseMemSpec(FLAGS_mem_limit, &is_percent, system_mem);
    // There might be the case of misconfiguration, when on a system swap is disabled
    // and overcommitting is turned off the actual usable memory is less than the
    // available physical memory.
    LOG(WARNING) << "This system shows a discrepancy between the available "
                 << "memory and the memory commit limit allowed by the "
                 << "operating system. ( Mem: " << MemInfo::physical_mem()
                 << "<=> CommitLimit: "
                 << MemInfo::commit_limit() << "). "
                 << "Impala will adhere to the smaller value by setting the "
                 << "process memory limit to " << bytes_limit << " "
                 << "Please verify the system configuration. Specifically, "
                 << "/proc/sys/vm/overcommit_memory and "
                 << "/proc/sys/vm/overcommit_ratio.";
  } else {
    system_mem = MemInfo::physical_mem();
    bytes_limit = ParseUtil::ParseMemSpec(FLAGS_mem_limit, &is_percent, system_mem);
  }
  // ParseMemSpec() returns -1 for invalid input and 0 to mean unlimited. From Impala
  // 2.11 onwards we do not support unlimited process memory limits.
  if (bytes_limit <= 0) {
    return Status(Substitute("The process memory limit (--mem_limit) must be a positive "
          "bytes value or percentage: $0", FLAGS_mem_limit));
  }

  if (!BitUtil::IsPowerOf2(FLAGS_min_buffer_size)) {
    return Status(Substitute(
        "--min_buffer_size must be a power-of-two: $0", FLAGS_min_buffer_size));
  }
  int64_t buffer_pool_limit = ParseUtil::ParseMemSpec(FLAGS_buffer_pool_limit,
      &is_percent, bytes_limit);
  if (buffer_pool_limit <= 0) {
    return Status(Substitute("Invalid --buffer_pool_limit value, must be a percentage or "
          "positive bytes value or percentage: $0", FLAGS_buffer_pool_limit));
  }
  buffer_pool_limit = BitUtil::RoundDown(buffer_pool_limit, FLAGS_min_buffer_size);

  int64_t clean_pages_limit = ParseUtil::ParseMemSpec(FLAGS_buffer_pool_clean_pages_limit,
      &is_percent, buffer_pool_limit);
  if (clean_pages_limit <= 0) {
    return Status(Substitute("Invalid --buffer_pool_clean_pages_limit value, must be a percentage or "
          "positive bytes value or percentage: $0", FLAGS_buffer_pool_clean_pages_limit));
  }
  InitBufferPool(FLAGS_min_buffer_size, buffer_pool_limit, clean_pages_limit);

  RETURN_IF_ERROR(metrics_->Init(enable_webserver_ ? webserver_.get() : nullptr));
  impalad_client_cache_->InitMetrics(metrics_.get(), "impala-server.backends");
  catalogd_client_cache_->InitMetrics(metrics_.get(), "catalog.server");
  RETURN_IF_ERROR(RegisterMemoryMetrics(
      metrics_.get(), true, buffer_reservation_.get(), buffer_pool_.get()));

  // Resolve hostname to IP address.
  RETURN_IF_ERROR(HostnameToIpAddr(backend_address_.hostname, &ip_address_));

  mem_tracker_.reset(
      new MemTracker(AggregateMemoryMetrics::TOTAL_USED, bytes_limit, "Process"));
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

  // Initializes the RPCMgr and DataStreamServices.
  if (FLAGS_use_krpc) {
    krpc_address_.__set_hostname(ip_address_);
    // Initialization needs to happen in the following order due to dependencies:
    // - RPC manager, DataStreamService and DataStreamManager.
    RETURN_IF_ERROR(rpc_mgr_->Init());
    data_svc_.reset(new DataStreamService());
    RETURN_IF_ERROR(data_svc_->Init());
    RETURN_IF_ERROR(KrpcStreamMgr()->Init(data_svc_->mem_tracker()));
    // Bump thread cache to 1GB to reduce contention for TCMalloc central
    // list's spinlock.
    if (FLAGS_tcmalloc_max_total_thread_cache_bytes == 0) {
      FLAGS_tcmalloc_max_total_thread_cache_bytes = 1 << 30;
    }
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

  if (bytes_limit > MemInfo::physical_mem()) {
    LOG(WARNING) << "Memory limit "
                 << PrettyPrinter::Print(bytes_limit, TUnit::BYTES)
                 << " exceeds physical memory of "
                 << PrettyPrinter::Print(MemInfo::physical_mem(), TUnit::BYTES);
  }
  LOG(INFO) << "Using global memory limit: "
            << PrettyPrinter::Print(bytes_limit, TUnit::BYTES);
  LOG(INFO) << "Buffer pool limit: "
            << PrettyPrinter::Print(buffer_pool_limit, TUnit::BYTES);

  RETURN_IF_ERROR(disk_io_mgr_->Init(mem_tracker_.get()));

  mem_tracker_->AddGcFunction(
      [this](int64_t bytes_to_free) { disk_io_mgr_->GcIoBuffers(bytes_to_free); });

  // Start services in order to ensure that dependencies between them are met
  if (enable_webserver_) {
    AddDefaultUrlCallbacks(webserver_.get(), mem_tracker_.get(), metrics_.get());
    RETURN_IF_ERROR(webserver_->Start());
  } else {
    LOG(INFO) << "Not starting webserver";
  }

  if (scheduler_ != nullptr) {
    RETURN_IF_ERROR(scheduler_->Init(backend_address_, krpc_address_, ip_address_));
  }
  if (admission_controller_ != nullptr) RETURN_IF_ERROR(admission_controller_->Init());

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
  if (FLAGS_use_krpc) {
    LOG(INFO) << "Starting KRPC service";
    RETURN_IF_ERROR(rpc_mgr_->StartServices(krpc_address_));
  }
  return Status::OK();
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
  buffer_pool_.reset(new BufferPool(min_buffer_size, capacity, clean_pages_limit));
  buffer_reservation_.reset(new ReservationTracker());
  buffer_reservation_->InitRootTracker(nullptr, capacity);
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

DataStreamMgr* ExecEnv::ThriftStreamMgr() {
  DCHECK(!FLAGS_use_krpc);
  return dynamic_cast<DataStreamMgr*>(stream_mgr_.get());
}

KrpcDataStreamMgr* ExecEnv::KrpcStreamMgr() {
  DCHECK(FLAGS_use_krpc);
  return dynamic_cast<KrpcDataStreamMgr*>(stream_mgr_.get());
}

} // namespace impala
