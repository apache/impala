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

#include "common/logging.h"
#include "runtime/client-cache.h"
#include "runtime/data-stream-mgr.h"
#include "runtime/disk-io-mgr.h"
#include "runtime/hbase-table-factory.h"
#include "runtime/hdfs-fs-cache.h"
#include "runtime/mem-limit.h"
#include "statestore/simple-scheduler.h"
#include "statestore/state-store-subscriber.h"
#include "util/metrics.h"
#include "util/network-util.h"
#include "util/webserver.h"
#include "util/default-path-handlers.h"
#include "util/parse-util.h"
#include "util/mem-info.h"
#include "util/debug-util.h"
#include "gen-cpp/ImpalaInternalService.h"

using namespace std;
using namespace boost;

DEFINE_bool(use_statestore, true,
    "Use an external state-store process to manage cluster membership");
DEFINE_bool(enable_webserver, true, "If true, debug webserver is enabled");
DECLARE_int32(be_port);
DECLARE_string(mem_limit);

DEFINE_string(state_store_host, "localhost",
              "hostname where StateStoreService is running");
DEFINE_int32(state_store_subscriber_port, 23000,
             "port where StateStoreSubscriberService should be exported");
DECLARE_int32(state_store_port);

namespace impala {

ExecEnv::ExecEnv()
  : stream_mgr_(new DataStreamMgr()),
    client_cache_(new ImpalaInternalServiceClientCache()),
    fs_cache_(new HdfsFsCache()),
    htable_factory_(new HBaseTableFactory()),
    disk_io_mgr_(new DiskIoMgr()),
    webserver_(new Webserver()),
    metrics_(new Metrics()),
    mem_limit_(NULL),
    enable_webserver_(FLAGS_enable_webserver),
    tz_database_(TimezoneDatabase()) {
  // Initialize the scheduler either dynamically (with a statestore) or statically (with
  // a standalone single backend)
  if (FLAGS_use_statestore) {
    TNetworkAddress subscriber_address =
        MakeNetworkAddress(FLAGS_hostname, FLAGS_state_store_subscriber_port);
    TNetworkAddress statestore_address =
        MakeNetworkAddress(FLAGS_state_store_host, FLAGS_state_store_port);
    TNetworkAddress backend_address = MakeNetworkAddress(FLAGS_hostname, FLAGS_be_port);

    stringstream subscriber_id;
    subscriber_id << backend_address;

    state_store_subscriber_.reset(new StateStoreSubscriber(subscriber_id.str(),
        subscriber_address, statestore_address, metrics_.get()));

    scheduler_.reset(
        new SimpleScheduler(state_store_subscriber_.get(), subscriber_id.str(),
                            backend_address, metrics_.get()));
  } else {
    vector<TNetworkAddress> addresses;
    addresses.push_back(MakeNetworkAddress(FLAGS_hostname, FLAGS_be_port));
    scheduler_.reset(new SimpleScheduler(addresses, metrics_.get()));
  }
  
  Status status = disk_io_mgr_->Init();
  CHECK(status.ok());

  client_cache_->InitMetrics(metrics_.get(), "impala-server.backends");
}

ExecEnv::ExecEnv(const string& hostname, int backend_port, int subscriber_port,
                 int webserver_port, const string& statestore_host, int statestore_port)
    : stream_mgr_(new DataStreamMgr()),
      client_cache_(new ImpalaInternalServiceClientCache()),
      fs_cache_(new HdfsFsCache()),
      htable_factory_(new HBaseTableFactory()),
      disk_io_mgr_(new DiskIoMgr()),
      webserver_(new Webserver(webserver_port)),
      metrics_(new Metrics()),
      mem_limit_(NULL),
      enable_webserver_(FLAGS_enable_webserver && webserver_port > 0),
      tz_database_(TimezoneDatabase()) {
  if (FLAGS_use_statestore && statestore_port > 0) {
    TNetworkAddress subscriber_address =
        MakeNetworkAddress(hostname, subscriber_port);
    TNetworkAddress statestore_address =
        MakeNetworkAddress(statestore_host, statestore_port);
    TNetworkAddress backend_address = MakeNetworkAddress(hostname, backend_port);

    stringstream ss;
    ss << backend_address;

    state_store_subscriber_.reset(new StateStoreSubscriber(ss.str(), subscriber_address,
        statestore_address, metrics_.get()));

    scheduler_.reset(new SimpleScheduler(state_store_subscriber_.get(), ss.str(),
                                         backend_address, metrics_.get()));

  } else {
    vector<TNetworkAddress> addresses;
    addresses.push_back(MakeNetworkAddress(hostname, backend_port));
    scheduler_.reset(new SimpleScheduler(addresses, metrics_.get()));
  }
  
  Status status = disk_io_mgr_->Init();
  CHECK(status.ok());

  client_cache_->InitMetrics(metrics_.get(), "impala-server.backends");
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
  // Limit of 0 means no memory limit.
  if (bytes_limit > 0) {
    mem_limit_.reset(new MemLimit(bytes_limit));
  }
  if (bytes_limit > MemInfo::physical_mem()) {
    LOG(WARNING) << "Memory limit "
                 << PrettyPrinter::Print(bytes_limit, TCounterType::BYTES)
                 << " exceeds physical memory of "
                 << PrettyPrinter::Print(MemInfo::physical_mem(),
                    TCounterType::BYTES);
  }
  LOG(INFO) << "Using global memory limit: "
            << PrettyPrinter::Print(bytes_limit, TCounterType::BYTES);
  
  disk_io_mgr_->SetProcessMemLimit(mem_limit_.get());

  // Start services in order to ensure that dependencies between them are met
  if (enable_webserver_) {
    AddDefaultPathHandlers(webserver_.get());
    RETURN_IF_ERROR(webserver_->Start());
  } else {
    LOG(INFO) << "Not starting webserver";
  }

  metrics_->Init(enable_webserver_ ? webserver_.get() : NULL);

  if (scheduler_ != NULL) RETURN_IF_ERROR(scheduler_->Init());

  // Must happen after all topic registrations / callbacks are done
  if (state_store_subscriber_.get() != NULL) {
    RETURN_IF_ERROR(state_store_subscriber_->Start());
  }

  return Status::OK;
}

}
