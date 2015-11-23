// Copyright 2012 Cloudera Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#include "runtime/exec-env.h"

#include <vector>

#include <boost/algorithm/string.hpp>
#include <gflags/gflags.h>
#include <gutil/strings/substitute.h>

#include "common/logging.h"
#include "resourcebroker/resource-broker.h"
#include "runtime/client-cache.h"
#include "runtime/data-stream-mgr.h"
#include "runtime/disk-io-mgr.h"
#include "runtime/hbase-table-factory.h"
#include "runtime/hdfs-fs-cache.h"
#include "runtime/lib-cache.h"
#include "runtime/mem-tracker.h"
#include "runtime/thread-resource-mgr.h"
#include "runtime/tmp-file-mgr.h"
#include "scheduling/request-pool-service.h"
#include "service/frontend.h"
#include "scheduling/simple-scheduler.h"
#include "statestore/statestore-subscriber.h"
#include "util/debug-util.h"
#include "util/default-path-handlers.h"
#include "util/mem-info.h"
#include "util/metrics.h"
#include "util/network-util.h"
#include "util/parse-util.h"
#include "util/memory-metrics.h"
#include "util/webserver.h"
#include "util/mem-info.h"
#include "util/debug-util.h"
#include "util/cgroups-mgr.h"
#include "util/memory-metrics.h"
#include "util/pretty-printer.h"
#include "gen-cpp/ImpalaInternalService.h"
#include "gen-cpp/CatalogService.h"

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

DECLARE_int32(state_store_port);
DECLARE_int32(num_threads_per_core);
DECLARE_int32(num_cores);
DECLARE_int32(be_port);
DECLARE_string(mem_limit);

DEFINE_bool(enable_rm, false, "Whether to enable resource management. If enabled, "
                              "-fair_scheduler_allocation_path is required.");
DEFINE_int32(llama_callback_port, 28000,
             "Port where Llama notification callback should be started");
// TODO: Deprecate llama_host and llama_port in favor of the new llama_hostports.
// This needs to be coordinated with CM.
DEFINE_string(llama_host, "",
              "Host of Llama service that the resource broker should connect to");
DEFINE_int32(llama_port, 15000,
             "Port of Llama service that the resource broker should connect to");
DEFINE_string(llama_addresses, "",
             "Llama availability group given as a comma-separated list of hostports.");
DEFINE_int64(llama_registration_timeout_secs, 30,
             "Maximum number of seconds that Impala will attempt to (re-)register "
             "with Llama before aborting the triggering action with an error "
             "(e.g. Impalad startup or a Llama RPC request). "
             "A setting of -1 means try indefinitely.");
DEFINE_int64(llama_registration_wait_secs, 3,
             "Number of seconds to wait between attempts during Llama registration.");
DEFINE_int64(llama_max_request_attempts, 5,
             "Maximum number of times a non-registration Llama RPC request "
             "(reserve/expand/release, etc.) is retried until the request is aborted. "
             "An attempt is counted once Impala is registered with Llama, i.e., a "
             "request survives at most llama_max_request_attempts-1 re-registrations.");
DEFINE_string(cgroup_hierarchy_path, "", "If Resource Management is enabled, this must "
    "be set to the Impala-writeable root of the cgroups hierarchy into which execution "
    "threads are assigned.");
DEFINE_string(staging_cgroup, "impala_staging", "Name of the cgroup that a query's "
    "execution threads are moved into once the query completes.");

// Use a low default value because the reconnection logic is performed manually
// for the purpose of faster Llama failover (otherwise we may try to reconnect to the
// inactive Llama for a long time).
DEFINE_int32(resource_broker_cnxn_attempts, 1, "The number of times to retry an "
    "RPC connection to Llama. A setting of 0 means retry indefinitely");
DEFINE_int32(resource_broker_cnxn_retry_interval_ms, 3000, "The interval, in ms, "
    "to wait between attempts to make an RPC connection to the Llama.");
DEFINE_int32(resource_broker_send_timeout, 0, "Time to wait, in ms, "
    "for the underlying socket of an RPC to Llama to successfully send data. "
    "A setting of 0 means the socket will wait indefinitely.");
DEFINE_int32(resource_broker_recv_timeout, 0, "Time to wait, in ms, "
    "for the underlying socket of an RPC to Llama to successfully receive data. "
    "A setting of 0 means the socket will wait indefinitely.");

DECLARE_string(ssl_client_ca_certificate);

// The key for a variable set in Impala's test environment only, to allow the
// resource-broker to correctly map node addresses into a form that Llama understand.
const static string PSEUDO_DISTRIBUTED_CONFIG_KEY =
    "yarn.scheduler.include-port-in-node-name";

namespace impala {

ExecEnv* ExecEnv::exec_env_ = NULL;

ExecEnv::ExecEnv()
  : stream_mgr_(new DataStreamMgr()),
    impalad_client_cache_(
        new ImpalaInternalServiceClientCache(
            "", !FLAGS_ssl_client_ca_certificate.empty())),
    catalogd_client_cache_(
        new CatalogServiceClientCache(
            "", !FLAGS_ssl_client_ca_certificate.empty())),
    htable_factory_(new HBaseTableFactory()),
    disk_io_mgr_(new DiskIoMgr()),
    webserver_(new Webserver()),
    metrics_(new MetricGroup("impala-metrics")),
    mem_tracker_(NULL),
    thread_mgr_(new ThreadResourceMgr),
    cgroups_mgr_(NULL),
    hdfs_op_thread_pool_(
        CreateHdfsOpThreadPool("hdfs-worker-pool", FLAGS_num_hdfs_worker_threads, 1024)),
    tmp_file_mgr_(new TmpFileMgr),
    request_pool_service_(new RequestPoolService(metrics_.get())),
    frontend_(new Frontend()),
    enable_webserver_(FLAGS_enable_webserver),
    tz_database_(TimezoneDatabase()),
    is_fe_tests_(false),
    backend_address_(MakeNetworkAddress(FLAGS_hostname, FLAGS_be_port)),
    is_pseudo_distributed_llama_(false) {
  if (FLAGS_enable_rm) InitRm();
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

    scheduler_.reset(new SimpleScheduler(statestore_subscriber_.get(),
        statestore_subscriber_->id(), backend_address_, metrics_.get(),
        webserver_.get(), resource_broker_.get(), request_pool_service_.get()));
  } else {
    vector<TNetworkAddress> addresses;
    addresses.push_back(MakeNetworkAddress(FLAGS_hostname, FLAGS_be_port));
    scheduler_.reset(new SimpleScheduler(addresses, metrics_.get(), webserver_.get(),
        resource_broker_.get(), request_pool_service_.get()));
  }
  if (exec_env_ == NULL) exec_env_ = this;
  if (FLAGS_enable_rm) resource_broker_->set_scheduler(scheduler_.get());
}

ExecEnv::ExecEnv(const string& hostname, int backend_port, int subscriber_port,
                 int webserver_port, const string& statestore_host, int statestore_port)
  : stream_mgr_(new DataStreamMgr()),
    impalad_client_cache_(
        new ImpalaInternalServiceClientCache(
            "", !FLAGS_ssl_client_ca_certificate.empty())),
    catalogd_client_cache_(
        new CatalogServiceClientCache(
            "", !FLAGS_ssl_client_ca_certificate.empty())),
    htable_factory_(new HBaseTableFactory()),
    disk_io_mgr_(new DiskIoMgr()),
    webserver_(new Webserver(webserver_port)),
    metrics_(new MetricGroup("impala-metrics")),
    mem_tracker_(NULL),
    thread_mgr_(new ThreadResourceMgr),
    hdfs_op_thread_pool_(
        CreateHdfsOpThreadPool("hdfs-worker-pool", FLAGS_num_hdfs_worker_threads, 1024)),
    tmp_file_mgr_(new TmpFileMgr),
    frontend_(new Frontend()),
    enable_webserver_(FLAGS_enable_webserver && webserver_port > 0),
    tz_database_(TimezoneDatabase()),
    is_fe_tests_(false),
    backend_address_(MakeNetworkAddress(FLAGS_hostname, FLAGS_be_port)),
    is_pseudo_distributed_llama_(false) {
  request_pool_service_.reset(new RequestPoolService(metrics_.get()));
  if (FLAGS_enable_rm) InitRm();

  if (FLAGS_use_statestore && statestore_port > 0) {
    TNetworkAddress subscriber_address =
        MakeNetworkAddress(hostname, subscriber_port);
    TNetworkAddress statestore_address =
        MakeNetworkAddress(statestore_host, statestore_port);

    statestore_subscriber_.reset(new StatestoreSubscriber(
        Substitute("impalad@$0", TNetworkAddressToString(backend_address_)),
        subscriber_address, statestore_address, metrics_.get()));

    scheduler_.reset(new SimpleScheduler(statestore_subscriber_.get(),
        statestore_subscriber_->id(), backend_address_, metrics_.get(),
        webserver_.get(), resource_broker_.get(), request_pool_service_.get()));
  } else {
    vector<TNetworkAddress> addresses;
    addresses.push_back(MakeNetworkAddress(hostname, backend_port));
    scheduler_.reset(new SimpleScheduler(addresses, metrics_.get(), webserver_.get(),
        resource_broker_.get(), request_pool_service_.get()));
  }
  if (exec_env_ == NULL) exec_env_ = this;
  if (FLAGS_enable_rm) resource_broker_->set_scheduler(scheduler_.get());
}

void ExecEnv::InitRm() {
  // Unique addresses from FLAGS_llama_addresses and FLAGS_llama_host/FLAGS_llama_port.
  vector<TNetworkAddress> llama_addresses;
  if (!FLAGS_llama_addresses.empty()) {
    vector<string> components;
    split(components, FLAGS_llama_addresses, is_any_of(","), token_compress_on);
    for (int i = 0; i < components.size(); ++i) {
      to_lower(components[i]);
      TNetworkAddress llama_address = MakeNetworkAddress(components[i]);
      if (find(llama_addresses.begin(), llama_addresses.end(), llama_address)
          == llama_addresses.end()) {
        llama_addresses.push_back(llama_address);
      }
    }
  }
  // Add Llama hostport from deprecated flags (if it does not already exist).
  if (!FLAGS_llama_host.empty()) {
    to_lower(FLAGS_llama_host);
    TNetworkAddress llama_address =
        MakeNetworkAddress(FLAGS_llama_host, FLAGS_llama_port);
    if (find(llama_addresses.begin(), llama_addresses.end(), llama_address)
        == llama_addresses.end()) {
      llama_addresses.push_back(llama_address);
    }
  }
  for (int i = 0; i < llama_addresses.size(); ++i) {
    LOG(INFO) << "Llama address " << i << ": " << llama_addresses[i];
  }

  TNetworkAddress llama_callback_address =
      MakeNetworkAddress(FLAGS_hostname, FLAGS_llama_callback_port);
  resource_broker_.reset(new ResourceBroker(llama_addresses, llama_callback_address,
      metrics_.get()));
  cgroups_mgr_.reset(new CgroupsMgr(metrics_.get()));

  TGetHadoopConfigRequest config_request;
  config_request.__set_name(PSEUDO_DISTRIBUTED_CONFIG_KEY);
  TGetHadoopConfigResponse config_response;
  frontend_->GetHadoopConfig(config_request, &config_response);
  if (config_response.__isset.value) {
    to_lower(config_response.value);
    is_pseudo_distributed_llama_ = (config_response.value == "true");
  } else {
    is_pseudo_distributed_llama_ = false;
  }
  if (is_pseudo_distributed_llama_) {
    LOG(INFO) << "Pseudo-distributed Llama cluster detected";
  }
}

ExecEnv::~ExecEnv() {
}

Status ExecEnv::InitForFeTests() {
  mem_tracker_.reset(new MemTracker(-1, -1, "Process"));
  is_fe_tests_ = true;
  return Status::OK();
}

Status ExecEnv::StartServices() {
  LOG(INFO) << "Starting global services";

  if (FLAGS_enable_rm) {
    // Initialize the resource broker to make sure the Llama is up and reachable.
    DCHECK(resource_broker_.get() != NULL);
    RETURN_IF_ERROR(resource_broker_->Init());
    DCHECK(cgroups_mgr_.get() != NULL);
    RETURN_IF_ERROR(
        cgroups_mgr_->Init(FLAGS_cgroup_hierarchy_path, FLAGS_staging_cgroup));
  }

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
  // Minimal IO Buffer requirements:
  //   IO buffer (8MB default) * number of IO buffers per thread (5) *
  //   number of threads per core * number of cores
  int64_t min_requirement = disk_io_mgr_->max_read_buffer_size() *
      DiskIoMgr::DEFAULT_QUEUE_CAPACITY *
      FLAGS_num_threads_per_core * FLAGS_num_cores;
  if (bytes_limit < min_requirement) {
    LOG(WARNING) << "Memory limit "
                 << PrettyPrinter::Print(bytes_limit, TUnit::BYTES)
                 << " does not meet minimal memory requirement of "
                 << PrettyPrinter::Print(min_requirement, TUnit::BYTES);
  }

  metrics_->Init(enable_webserver_ ? webserver_.get() : NULL);
  impalad_client_cache_->InitMetrics(metrics_.get(), "impala-server.backends");
  catalogd_client_cache_->InitMetrics(metrics_.get(), "catalog.server");
  RETURN_IF_ERROR(RegisterMemoryMetrics(metrics_.get(), true));

#ifndef ADDRESS_SANITIZER
  // Limit of -1 means no memory limit.
  mem_tracker_.reset(new MemTracker(TcmallocMetric::PHYSICAL_BYTES_RESERVED,
      bytes_limit > 0 ? bytes_limit : -1, -1, "Process"));

  // Since tcmalloc does not free unused memory, we may exceed the process mem limit even
  // if Impala is not actually using that much memory. Add a callback to free any unused
  // memory if we hit the process limit.
  mem_tracker_->AddGcFunction(boost::bind(&MallocExtension::ReleaseFreeMemory,
                                          MallocExtension::instance()));
#else
  // tcmalloc metrics aren't defined in ASAN builds, just use the default behavior to
  // track process memory usage (sum of all children trackers).
  mem_tracker_.reset(new MemTracker(bytes_limit > 0 ? bytes_limit : -1, -1, "Process"));
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

  // Start services in order to ensure that dependencies between them are met
  if (enable_webserver_) {
    AddDefaultUrlCallbacks(webserver_.get(), mem_tracker_.get());
    RETURN_IF_ERROR(webserver_->Start());
  } else {
    LOG(INFO) << "Not starting webserver";
  }

  if (scheduler_ != NULL) RETURN_IF_ERROR(scheduler_->Init());

  // Must happen after all topic registrations / callbacks are done
  if (statestore_subscriber_.get() != NULL) {
    Status status = statestore_subscriber_->Start();
    if (!status.ok()) {
      status.AddDetail("State Store Subscriber did not start up.");
      return status;
    }
  }

  return Status::OK();
}

}
