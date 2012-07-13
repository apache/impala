// Copyright (c) 2012 Cloudera, Inc. All rights reserved.

#include "runtime/exec-env.h"

#include <vector>

#include <gflags/gflags.h>
#include <boost/algorithm/string.hpp>

#include "common/service-ids.h"
#include "runtime/client-cache.h"
#include "runtime/data-stream-mgr.h"
#include "runtime/hbase-table-cache.h"
#include "runtime/hdfs-fs-cache.h"
#include "sparrow/simple-scheduler.h"
#include "sparrow/subscription-manager.h"
#include "util/webserver.h"
#include "gen-cpp/ImpalaBackendService.h"

using namespace std;
using namespace boost;
using sparrow::Scheduler;
using sparrow::SimpleScheduler;
using sparrow::SubscriptionManager;

DEFINE_string(backends, "", "comma-separated list of <host:port> pairs");
DEFINE_bool(use_statestore, false,
    "Use an external state store for membership information");

namespace impala {

ExecEnv::ExecEnv()
  : stream_mgr_impl_(new DataStreamMgr()),
    subscription_manager_impl_(new SubscriptionManager()),
    client_cache_impl_(new BackendClientCache(0, 0)),
    fs_cache_impl_(new HdfsFsCache()),
    htable_cache_impl_(new HBaseTableCache()),
    webserver_(new Webserver()),
    tz_database_(TimezoneDatabase()),
    stream_mgr_(stream_mgr_impl_.get()),
    subscription_manager_(subscription_manager_impl_.get()),
    client_cache_(client_cache_impl_.get()),
    fs_cache_(fs_cache_impl_.get()),
    htable_cache_(htable_cache_impl_.get()) {
  // Initialize the scheduler either dynamically (with a statestore) or statically (with
  // configured backends)
  if (FLAGS_use_statestore) {
    scheduler_impl_.reset(new SimpleScheduler(subscription_manager_, IMPALA_SERVICE_ID));
  } else if (!FLAGS_backends.empty()) {
    vector<string> backends;
    vector<THostPort> addresses;
    split(backends, FLAGS_backends, is_any_of(","));
    for (int i = 0; i < backends.size(); ++i) {
      int pos = backends[i].find(':');
      if (pos == string::npos) {
        DCHECK(false) << "ignoring backend " << backends[i] << ": missing ':'";
        continue;
      }
      addresses.push_back(THostPort());
      THostPort& addr = addresses.back();
      addr.host = backends[i].substr(0, pos);
      addr.port = atoi(backends[i].substr(pos + 1).c_str());
    }
    scheduler_impl_.reset(new SimpleScheduler(addresses));
  } else {
    // No scheduler required if query is not distributed.
    // TODO: Check that num_nodes is correctly set?
    scheduler_impl_.reset(NULL);
  }

  scheduler_ = scheduler_impl_.get();
}

ExecEnv::ExecEnv(HdfsFsCache* fs_cache)
  : stream_mgr_impl_(new DataStreamMgr()),
    subscription_manager_impl_(new SubscriptionManager()),
    client_cache_impl_(new BackendClientCache(0, 0)),
    fs_cache_impl_(),
    htable_cache_impl_(new HBaseTableCache()),
    tz_database_(TimezoneDatabase()),
    stream_mgr_(stream_mgr_impl_.get()),
    scheduler_(NULL),
    subscription_manager_(subscription_manager_impl_.get()),
    client_cache_(client_cache_impl_.get()),
    fs_cache_(fs_cache),
    htable_cache_(htable_cache_impl_.get()) {
}

ExecEnv::~ExecEnv() {
}

Status ExecEnv::StartServices() {
  // Start services in order to ensure that dependencies between them are met
  if (FLAGS_use_statestore) RETURN_IF_ERROR(subscription_manager_->Start());
  if (scheduler_ != NULL) scheduler_impl_->Init();
  RETURN_IF_ERROR(webserver_->Start());
  return Status::OK;
}

}
