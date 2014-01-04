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
#include "runtime/client-cache.h"
#include "runtime/data-stream-mgr.h"
#include "runtime/disk-io-mgr.h"
#include "runtime/hbase-table-factory.h"
#include "runtime/hdfs-fs-cache.h"
#include "runtime/lib-cache.h"
#include "runtime/mem-tracker.h"
#include "runtime/thread-resource-mgr.h"
#include "statestore/simple-scheduler.h"
#include "statestore/state-store-subscriber.h"
#include "util/debug-util.h"
#include "util/default-path-handlers.h"
#include "util/mem-info.h"
#include "util/metrics.h"
#include "util/network-util.h"
#include "util/parse-util.h"
#include "util/tcmalloc-metric.h"
#include "util/webserver.h"
#include "gen-cpp/ImpalaInternalService.h"

using namespace std;
using namespace boost;
using namespace strings;

DEFINE_bool(use_statestore, true,
    "Use an external state-store process to manage cluster membership");
DEFINE_string(catalog_service_host, "localhost",
    "hostname where CatalogService is running");
DEFINE_bool(enable_webserver, true, "If true, debug webserver is enabled");
DEFINE_string(state_store_host, "localhost",
    "hostname where StateStoreService is running");
DEFINE_int32(state_store_subscriber_port, 23000,
    "port where StateStoreSubscriberService should be exported");
DEFINE_int32(num_hdfs_worker_threads, 16,
    "(Advanced) The number of threads in the global HDFS operation pool");

DECLARE_int32(state_store_port);
DECLARE_int32(num_threads_per_core);
DECLARE_int32(num_cores);
DECLARE_int32(be_port);
DECLARE_string(mem_limit);

namespace impala {

ExecEnv* ExecEnv::exec_env_ = NULL;

ExecEnv::ExecEnv()
  : stream_mgr_(new DataStreamMgr()),
    client_cache_(new ImpalaInternalServiceClientCache()),
    fs_cache_(new HdfsFsCache()),
    lib_cache_(new LibCache()),
    htable_factory_(new HBaseTableFactory()),
    disk_io_mgr_(new DiskIoMgr()),
    webserver_(new Webserver()),
    metrics_(new Metrics()),
    mem_tracker_(NULL),
    thread_mgr_(new ThreadResourceMgr),
    hdfs_op_thread_pool_(
        CreateHdfsOpThreadPool("hdfs-worker-pool", FLAGS_num_hdfs_worker_threads, 1024)),
    enable_webserver_(FLAGS_enable_webserver),
    tz_database_(TimezoneDatabase()),
    is_fe_tests_(false) {
  // Initialize the scheduler either dynamically (with a statestore) or statically (with
  // a standalone single backend)
  if (FLAGS_use_statestore) {
    TNetworkAddress subscriber_address =
        MakeNetworkAddress(FLAGS_hostname, FLAGS_state_store_subscriber_port);
    TNetworkAddress statestore_address =
        MakeNetworkAddress(FLAGS_state_store_host, FLAGS_state_store_port);
    TNetworkAddress backend_address = MakeNetworkAddress(FLAGS_hostname, FLAGS_be_port);

    state_store_subscriber_.reset(new StateStoreSubscriber(
        Substitute("impalad@$0", TNetworkAddressToString(backend_address)),
        subscriber_address, statestore_address, metrics_.get()));

    scheduler_.reset(new SimpleScheduler(state_store_subscriber_.get(),
        state_store_subscriber_->id(), backend_address, metrics_.get(),
        webserver_.get()));
  } else {
    vector<TNetworkAddress> addresses;
    addresses.push_back(MakeNetworkAddress(FLAGS_hostname, FLAGS_be_port));
    scheduler_.reset(new SimpleScheduler(addresses, metrics_.get(), webserver_.get()));
  }
  if (exec_env_ == NULL) exec_env_ = this;
}

ExecEnv::ExecEnv(const string& hostname, int backend_port, int subscriber_port,
                 int webserver_port, const string& statestore_host, int statestore_port)
  : stream_mgr_(new DataStreamMgr()),
    client_cache_(new ImpalaInternalServiceClientCache()),
    fs_cache_(new HdfsFsCache()),
    lib_cache_(new LibCache()),
    htable_factory_(new HBaseTableFactory()),
    disk_io_mgr_(new DiskIoMgr()),
    webserver_(new Webserver(webserver_port)),
    metrics_(new Metrics()),
    mem_tracker_(NULL),
    thread_mgr_(new ThreadResourceMgr),
    hdfs_op_thread_pool_(
        CreateHdfsOpThreadPool("hdfs-worker-pool", FLAGS_num_hdfs_worker_threads, 1024)),
    enable_webserver_(FLAGS_enable_webserver && webserver_port > 0),
    tz_database_(TimezoneDatabase()),
    is_fe_tests_(false) {
  if (FLAGS_use_statestore && statestore_port > 0) {
    TNetworkAddress subscriber_address =
        MakeNetworkAddress(hostname, subscriber_port);
    TNetworkAddress statestore_address =
        MakeNetworkAddress(statestore_host, statestore_port);
    TNetworkAddress backend_address = MakeNetworkAddress(hostname, backend_port);

    state_store_subscriber_.reset(new StateStoreSubscriber(
        Substitute("impalad@$0", TNetworkAddressToString(backend_address)),
        subscriber_address, statestore_address, metrics_.get()));

    scheduler_.reset(new SimpleScheduler(state_store_subscriber_.get(),
        state_store_subscriber_->id(), backend_address, metrics_.get(),
        webserver_.get()));

  } else {
    vector<TNetworkAddress> addresses;
    addresses.push_back(MakeNetworkAddress(hostname, backend_port));
    scheduler_.reset(new SimpleScheduler(addresses, metrics_.get(), webserver_.get()));
  }
  if (exec_env_ == NULL) exec_env_ = this;
}

ExecEnv::~ExecEnv() {
}

Status ExecEnv::StartServices() {
  LOG(INFO) << "Starting global services";

  // Initialize global memory limit.
  int64_t bytes_limit = 0;
  bool is_percent;
  // --mem_limit="" means no memory limit
  bytes_limit = ParseUtil::ParseMemSpec(FLAGS_mem_limit, &is_percent);
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
                 << PrettyPrinter::Print(bytes_limit, TCounterType::BYTES)
                 << " does not meet minimal memory requirement of "
                 << PrettyPrinter::Print(min_requirement, TCounterType::BYTES);
  }

  metrics_->Init(enable_webserver_ ? webserver_.get() : NULL);
  client_cache_->InitMetrics(metrics_.get(), "impala-server.backends");
  RegisterTcmallocMetrics(metrics_.get());

#ifndef ADDRESS_SANITIZER
  // Limit of -1 means no memory limit.
  mem_tracker_.reset(new MemTracker(TcmallocMetric::PHYSICAL_BYTES_RESERVED,
                                    bytes_limit > 0 ? bytes_limit : -1, "Process"));

  // Since tcmalloc does not free unused memory, we may exceed the process mem limit even
  // if Impala is not actually using that much memory. Add a callback to free any unused
  // memory if we hit the process limit.
  mem_tracker_->AddGcFunction(boost::bind(&MallocExtension::ReleaseFreeMemory,
                                          MallocExtension::instance()));
#else
  // tcmalloc metrics aren't defined in ASAN builds, just use the default behavior to
  // track process memory usage (sum of all children trackers).
  mem_tracker_.reset(new MemTracker(bytes_limit > 0 ? bytes_limit : -1, "Process"));
#endif

  mem_tracker_->RegisterMetrics(metrics_.get(), "mem-tracker.process");

  if (bytes_limit > MemInfo::physical_mem()) {
    LOG(WARNING) << "Memory limit "
                 << PrettyPrinter::Print(bytes_limit, TCounterType::BYTES)
                 << " exceeds physical memory of "
                 << PrettyPrinter::Print(MemInfo::physical_mem(), TCounterType::BYTES);
  }
  LOG(INFO) << "Using global memory limit: "
            << PrettyPrinter::Print(bytes_limit, TCounterType::BYTES);

  RETURN_IF_ERROR(disk_io_mgr_->Init(mem_tracker_.get()));

  // Start services in order to ensure that dependencies between them are met
  if (enable_webserver_) {
    AddDefaultPathHandlers(webserver_.get(), mem_tracker_.get());
    RETURN_IF_ERROR(webserver_->Start());
  } else {
    LOG(INFO) << "Not starting webserver";
  }

  if (scheduler_ != NULL) RETURN_IF_ERROR(scheduler_->Init());

  // Must happen after all topic registrations / callbacks are done
  if (state_store_subscriber_.get() != NULL) {
    Status status = state_store_subscriber_->Start();
    if (!status.ok()) {
      status.AddErrorMsg("State Store Subscriber did not start up.");
      return status;
    }
  }

  return Status::OK;
}

}
