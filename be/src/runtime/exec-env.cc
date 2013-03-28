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

#include "common/logging.h"
#include "common/service-ids.h"
#include "runtime/client-cache.h"
#include "runtime/data-stream-mgr.h"
#include "runtime/disk-io-mgr.h"
#include "runtime/hbase-table-factory.h"
#include "runtime/hdfs-fs-cache.h"
#include "runtime/mem-limit.h"
#include "statestore/simple-scheduler.h"
#include "statestore/subscription-manager.h"
#include "util/metrics.h"
#include "util/network-util.h"
#include "util/webserver.h"
#include "util/default-path-handlers.h"
#include "gen-cpp/ImpalaInternalService.h"

using namespace std;
using namespace boost;

DEFINE_bool(use_statestore, true,
    "Use an external state-store process to manage cluster membership");
DEFINE_bool(enable_webserver, true, "If true, debug webserver is enabled");
DECLARE_int32(be_port);
DECLARE_int64(mem_limit);

namespace impala {

ExecEnv::ExecEnv()
  : stream_mgr_(new DataStreamMgr()),
    client_cache_(new ImpalaInternalServiceClientCache()),
    fs_cache_(new HdfsFsCache()),
    htable_factory_(new HBaseTableFactory()),
    disk_io_mgr_(new DiskIoMgr()),
    webserver_(new Webserver()),
    metrics_(new Metrics()),
    mem_limit_(FLAGS_mem_limit > 0 ? new MemLimit(FLAGS_mem_limit) : NULL),
    enable_webserver_(FLAGS_enable_webserver),
    tz_database_(TimezoneDatabase()) {
  // Initialize the scheduler either dynamically (with a statestore) or statically (with
  // a standalone single backend)
  if (FLAGS_use_statestore) {
    subscription_mgr_.reset(new SubscriptionManager());

    scheduler_.reset(new SimpleScheduler(subscription_mgr_.get(), IMPALA_SERVICE_ID,
                                         metrics_.get()));
  } else {
    vector<TNetworkAddress> addresses;
    addresses.push_back(MakeNetworkAddress(FLAGS_hostname, FLAGS_be_port));
    scheduler_.reset(new SimpleScheduler(addresses, metrics_.get()));
    subscription_mgr_.reset(NULL);
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
      mem_limit_(FLAGS_mem_limit > 0 ? new MemLimit(FLAGS_mem_limit) : NULL),
      enable_webserver_(FLAGS_enable_webserver && webserver_port > 0),
      tz_database_(TimezoneDatabase()) {
  if (FLAGS_use_statestore && statestore_port > 0) {
    subscription_mgr_.reset(new SubscriptionManager(hostname, subscriber_port,
                                                    statestore_host, statestore_port));

    scheduler_.reset(new SimpleScheduler(subscription_mgr_.get(), IMPALA_SERVICE_ID,
                                         metrics_.get()));

  } else {
    vector<TNetworkAddress> addresses;
    addresses.push_back(MakeNetworkAddress(hostname, backend_port));
    scheduler_.reset(new SimpleScheduler(addresses, metrics_.get()));
    subscription_mgr_.reset(NULL);
  }
  Status status = disk_io_mgr_->Init();
  CHECK(status.ok());

  client_cache_->InitMetrics(metrics_.get(), "impala-server.backends");
}

ExecEnv::~ExecEnv() {
  if (scheduler_ != NULL) scheduler_->Close();
}

Status ExecEnv::StartServices() {
  LOG(INFO) << "Starting global services";
  // Start services in order to ensure that dependencies between them are met
  if (enable_webserver_) {
    AddDefaultPathHandlers(webserver_.get());
    RETURN_IF_ERROR(webserver_->Start());
  } else {
    LOG(INFO) << "Not starting webserver";
  }

  metrics_->Init(enable_webserver_ ? webserver_.get() : NULL);

  if (FLAGS_use_statestore && subscription_mgr_.get() != NULL) {
    RETURN_IF_ERROR(subscription_mgr_->Start());
  }

  if (scheduler_.get() != NULL) RETURN_IF_ERROR(scheduler_->Init());

  return Status::OK;
}

}
