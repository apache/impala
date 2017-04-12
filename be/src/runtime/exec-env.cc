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

#include "common/logging.h"
#include "gen-cpp/CatalogService.h"
#include "gen-cpp/ImpalaInternalService.h"
#include "runtime/backend-client.h"
#include "runtime/bufferpool/buffer-pool.h"
#include "runtime/bufferpool/reservation-tracker.h"
#include "runtime/client-cache.h"
#include "runtime/coordinator.h"
#include "runtime/data-stream-mgr.h"
#include "runtime/disk-io-mgr.h"
#include "runtime/hbase-table-factory.h"
#include "runtime/hdfs-fs-cache.h"
#include "runtime/lib-cache.h"
#include "runtime/mem-tracker.h"
#include "runtime/query-exec-mgr.h"
#include "runtime/thread-resource-mgr.h"
#include "runtime/tmp-file-mgr.h"
#include "scheduling/admission-controller.h"
#include "scheduling/request-pool-service.h"
#include "scheduling/scheduler.h"
#include "service/frontend.h"
#include "statestore/statestore-subscriber.h"
#include "util/bit-util.h"
#include "util/debug-util.h"
#include "util/debug-util.h"
#include "util/default-path-handlers.h"
#include "util/hdfs-bulk-ops.h"
#include "util/mem-info.h"
#include "util/mem-info.h"
#include "util/memory-metrics.h"
#include "util/memory-metrics.h"
#include "util/metrics.h"
#include "util/network-util.h"
#include "util/parse-util.h"
#include "util/pretty-printer.h"
#include "util/thread-pool.h"
#include "util/webserver.h"

#include "common/names.h"

using boost::algorithm::is_any_of;
using boost::algorithm::split;
using boost::algorithm::to_lower;
using boost::algorithm::token_compress_on;
using namespace strings;

DEFINE_bool(use_statestore, true,
    "Use an external statestore process to manage cluster membership");
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

DECLARE_int32(state_store_port);
DECLARE_int32(num_threads_per_core);
DECLARE_int32(num_cores);
DECLARE_int32(be_port);
DECLARE_string(mem_limit);
DECLARE_bool(is_coordinator);

// TODO: Remove the following RM-related flags in Impala 3.0.
DEFINE_bool(enable_rm, false, "Deprecated");
DEFINE_int32(llama_callback_port, 28000, "Deprecated");
DEFINE_string(llama_host, "", "Deprecated");
DEFINE_int32(llama_port, 15000, "Deprecated");
DEFINE_string(llama_addresses, "", "Deprecated");
DEFINE_int64(llama_registration_timeout_secs, 30, "Deprecated");
DEFINE_int64(llama_registration_wait_secs, 3, "Deprecated");
DEFINE_int64(llama_max_request_attempts, 5, "Deprecated");
DEFINE_string(cgroup_hierarchy_path, "", "Deprecated");
DEFINE_string(staging_cgroup, "impala_staging", "Deprecated");
DEFINE_int32(resource_broker_cnxn_attempts, 1, "Deprecated");
DEFINE_int32(resource_broker_cnxn_retry_interval_ms, 3000, "Deprecated");
DEFINE_int32(resource_broker_send_timeout, 0, "Deprecated");
DEFINE_int32(resource_broker_recv_timeout, 0, "Deprecated");

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

ExecEnv* ExecEnv::exec_env_ = nullptr;

ExecEnv::ExecEnv()
  : metrics_(new MetricGroup("impala-metrics")),
    stream_mgr_(new DataStreamMgr(metrics_.get())),
    impalad_client_cache_(
        new ImpalaBackendClientCache(FLAGS_backend_client_connection_num_retries, 0,
            FLAGS_backend_client_rpc_timeout_ms, FLAGS_backend_client_rpc_timeout_ms, "",
            !FLAGS_ssl_client_ca_certificate.empty())),
    catalogd_client_cache_(
        new CatalogServiceClientCache(FLAGS_catalog_client_connection_num_retries, 0,
            FLAGS_catalog_client_rpc_timeout_ms, FLAGS_catalog_client_rpc_timeout_ms, "",
            !FLAGS_ssl_client_ca_certificate.empty())),
    htable_factory_(new HBaseTableFactory()),
    disk_io_mgr_(new DiskIoMgr()),
    webserver_(new Webserver()),
    mem_tracker_(nullptr),
    pool_mem_trackers_(new PoolMemTrackerRegistry),
    thread_mgr_(new ThreadResourceMgr),
    hdfs_op_thread_pool_(
        CreateHdfsOpThreadPool("hdfs-worker-pool", FLAGS_num_hdfs_worker_threads, 1024)),
    tmp_file_mgr_(new TmpFileMgr),
    request_pool_service_(new RequestPoolService(metrics_.get())),
    frontend_(new Frontend()),
    fragment_exec_thread_pool_(new CallableThreadPool("coordinator-fragment-rpc",
        "worker", FLAGS_coordinator_rpc_threads, numeric_limits<int32_t>::max())),
    async_rpc_pool_(new CallableThreadPool("rpc-pool", "async-rpc-sender", 8, 10000)),
    query_exec_mgr_(new QueryExecMgr()),
    buffer_reservation_(nullptr),
    buffer_pool_(nullptr),
    enable_webserver_(FLAGS_enable_webserver),
    is_fe_tests_(false),
    backend_address_(MakeNetworkAddress(FLAGS_hostname, FLAGS_be_port)) {
  // Initialize the scheduler either dynamically (with a statestore) or statically (with
  // a standalone single backend)
  if (FLAGS_use_statestore) {
    TNetworkAddress subscriber_address =
        MakeNetworkAddress(FLAGS_hostname, FLAGS_state_store_subscriber_port);
    TNetworkAddress statestore_address =
        MakeNetworkAddress(FLAGS_state_store_host, FLAGS_state_store_port);

    statestore_subscriber_.reset(new StatestoreSubscriber(
        Substitute("impalad@$0", TNetworkAddressToString(backend_address_)),
        subscriber_address, statestore_address, metrics_.get()));

    if (FLAGS_is_coordinator) {
      scheduler_.reset(new Scheduler(statestore_subscriber_.get(),
          statestore_subscriber_->id(), backend_address_, metrics_.get(),
          webserver_.get(), request_pool_service_.get()));
    }

    if (!FLAGS_disable_admission_control) {
      admission_controller_.reset(new AdmissionController(statestore_subscriber_.get(),
          request_pool_service_.get(), metrics_.get(), backend_address_));
    } else {
      LOG(INFO) << "Admission control is disabled.";
    }
  } else if (FLAGS_is_coordinator) {
    vector<TNetworkAddress> addresses;
    addresses.push_back(MakeNetworkAddress(FLAGS_hostname, FLAGS_be_port));
    scheduler_.reset(new Scheduler(
        addresses, metrics_.get(), webserver_.get(), request_pool_service_.get()));
  }
  exec_env_ = this;
}

// TODO: Need refactor to get rid of duplicated code.
ExecEnv::ExecEnv(const string& hostname, int backend_port, int subscriber_port,
    int webserver_port, const string& statestore_host, int statestore_port)
  : metrics_(new MetricGroup("impala-metrics")),
    stream_mgr_(new DataStreamMgr(metrics_.get())),
    impalad_client_cache_(
        new ImpalaBackendClientCache(FLAGS_backend_client_connection_num_retries, 0,
            FLAGS_backend_client_rpc_timeout_ms, FLAGS_backend_client_rpc_timeout_ms, "",
            !FLAGS_ssl_client_ca_certificate.empty())),
    catalogd_client_cache_(
        new CatalogServiceClientCache(FLAGS_catalog_client_connection_num_retries, 0,
            FLAGS_catalog_client_rpc_timeout_ms, FLAGS_catalog_client_rpc_timeout_ms, "",
            !FLAGS_ssl_client_ca_certificate.empty())),
    htable_factory_(new HBaseTableFactory()),
    disk_io_mgr_(new DiskIoMgr()),
    webserver_(new Webserver(webserver_port)),
    mem_tracker_(nullptr),
    pool_mem_trackers_(new PoolMemTrackerRegistry),
    thread_mgr_(new ThreadResourceMgr),
    hdfs_op_thread_pool_(
        CreateHdfsOpThreadPool("hdfs-worker-pool", FLAGS_num_hdfs_worker_threads, 1024)),
    tmp_file_mgr_(new TmpFileMgr),
    frontend_(new Frontend()),
    fragment_exec_thread_pool_(new CallableThreadPool("coordinator-fragment-rpc",
        "worker", FLAGS_coordinator_rpc_threads, numeric_limits<int32_t>::max())),
    async_rpc_pool_(new CallableThreadPool("rpc-pool", "async-rpc-sender", 8, 10000)),
    query_exec_mgr_(new QueryExecMgr()),
    buffer_reservation_(nullptr),
    buffer_pool_(nullptr),
    enable_webserver_(FLAGS_enable_webserver && webserver_port > 0),
    is_fe_tests_(false),
    backend_address_(MakeNetworkAddress(FLAGS_hostname, FLAGS_be_port)) {
  request_pool_service_.reset(new RequestPoolService(metrics_.get()));

  if (FLAGS_use_statestore && statestore_port > 0) {
    TNetworkAddress subscriber_address =
        MakeNetworkAddress(hostname, subscriber_port);
    TNetworkAddress statestore_address =
        MakeNetworkAddress(statestore_host, statestore_port);

    statestore_subscriber_.reset(new StatestoreSubscriber(
        Substitute("impalad@$0", TNetworkAddressToString(backend_address_)),
        subscriber_address, statestore_address, metrics_.get()));

    if (FLAGS_is_coordinator) {
      scheduler_.reset(new Scheduler(statestore_subscriber_.get(),
          statestore_subscriber_->id(), backend_address_, metrics_.get(),
          webserver_.get(), request_pool_service_.get()));
    }

    if (FLAGS_disable_admission_control) LOG(INFO) << "Admission control is disabled.";
    if (!FLAGS_disable_admission_control) {
      admission_controller_.reset(new AdmissionController(statestore_subscriber_.get(),
          request_pool_service_.get(), metrics_.get(), backend_address_));
    }
  } else if (FLAGS_is_coordinator) {
    vector<TNetworkAddress> addresses;
    addresses.push_back(MakeNetworkAddress(hostname, backend_port));
    scheduler_.reset(new Scheduler(
        addresses, metrics_.get(), webserver_.get(), request_pool_service_.get()));
  }
  exec_env_ = this;
}

ExecEnv::~ExecEnv() {
  if (buffer_reservation_ != nullptr) buffer_reservation_->Close();
  disk_io_mgr_.reset(); // Need to tear down before mem_tracker_.
}

Status ExecEnv::InitForFeTests() {
  mem_tracker_.reset(new MemTracker(-1, "Process"));
  is_fe_tests_ = true;
  return Status::OK();
}

Status ExecEnv::StartServices() {
  LOG(INFO) << "Starting global services";

  // Initialize global memory limit.
  // Depending on the system configuration, we will have to calculate the process
  // memory limit either based on the available physical memory, or if overcommitting
  // is turned off, we use the memory commit limit from /proc/meminfo (see
  // IMPALA-1690).
  // --mem_limit="" means no memory limit
  int64_t bytes_limit = 0;
  bool is_percent;
  if (MemInfo::vm_overcommit() == 2 &&
      MemInfo::commit_limit() < MemInfo::physical_mem()) {
    bytes_limit = ParseUtil::ParseMemSpec(FLAGS_mem_limit, &is_percent,
        MemInfo::commit_limit());
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
    bytes_limit = ParseUtil::ParseMemSpec(FLAGS_mem_limit, &is_percent,
        MemInfo::physical_mem());
  }

  if (bytes_limit < 0) {
    return Status("Failed to parse mem limit from '" + FLAGS_mem_limit + "'.");
  }

  metrics_->Init(enable_webserver_ ? webserver_.get() : nullptr);
  impalad_client_cache_->InitMetrics(metrics_.get(), "impala-server.backends");
  catalogd_client_cache_->InitMetrics(metrics_.get(), "catalog.server");
  RETURN_IF_ERROR(RegisterMemoryMetrics(
      metrics_.get(), true, buffer_reservation_.get(), buffer_pool_.get()));

#ifndef ADDRESS_SANITIZER
  // Limit of -1 means no memory limit.
  mem_tracker_.reset(new MemTracker(
      AggregateMemoryMetric::TOTAL_USED, bytes_limit > 0 ? bytes_limit : -1, "Process"));
#else
  // tcmalloc metrics aren't defined in ASAN builds, just use the default behavior to
  // track process memory usage (sum of all children trackers).
  mem_tracker_.reset(new MemTracker(bytes_limit > 0 ? bytes_limit : -1, "Process"));
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

  RETURN_IF_ERROR(disk_io_mgr_->Init(mem_tracker_.get()));

  mem_tracker_->AddGcFunction(
      [this](int64_t bytes_to_free) { disk_io_mgr_->GcIoBuffers(bytes_to_free); });

  // TODO: IMPALA-3200: register BufferPool::ReleaseMemory() as GC function.

#ifndef ADDRESS_SANITIZER
  // Since tcmalloc does not free unused memory, we may exceed the process mem limit even
  // if Impala is not actually using that much memory. Add a callback to free any unused
  // memory if we hit the process limit. TCMalloc GC must run last, because other GC
  // functions may have released memory to TCMalloc, and TCMalloc may have cached it
  // instead of releasing it to the system.
  mem_tracker_->AddGcFunction([](int64_t bytes_to_free) {
    MallocExtension::instance()->ReleaseToSystem(bytes_to_free);
  });
#endif

  // Start services in order to ensure that dependencies between them are met
  if (enable_webserver_) {
    AddDefaultUrlCallbacks(webserver_.get(), mem_tracker_.get(), metrics_.get());
    RETURN_IF_ERROR(webserver_->Start());
  } else {
    LOG(INFO) << "Not starting webserver";
  }

  if (scheduler_ != nullptr) RETURN_IF_ERROR(scheduler_->Init());
  if (admission_controller_ != nullptr) RETURN_IF_ERROR(admission_controller_->Init());

  // Get the fs.defaultFS value set in core-site.xml and assign it to
  // configured_defaultFs
  TGetHadoopConfigRequest config_request;
  config_request.__set_name(DEFAULT_FS);
  TGetHadoopConfigResponse config_response;
  frontend_->GetHadoopConfig(config_request, &config_response);
  if (config_response.__isset.value) {
    default_fs_ = config_response.value;
  } else {
    default_fs_ = "hdfs://";
  }
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

void ExecEnv::InitBufferPool(int64_t min_page_size, int64_t capacity) {
  DCHECK(buffer_pool_ == nullptr);
  buffer_pool_.reset(new BufferPool(min_page_size, capacity));
  buffer_reservation_.reset(new ReservationTracker());
  buffer_reservation_->InitRootTracker(nullptr, capacity);
}
}
