// Copyright (c) 2012 Cloudera, Inc. All rights reserved.

#include <vector>

#include <gflags/gflags.h>
#include <boost/algorithm/string.hpp>

#include "runtime/exec-env.h"

#include "runtime/client-cache.h"
#include "runtime/data-stream-mgr.h"
#include "runtime/hbase-table-cache.h"
#include "runtime/hdfs-fs-cache.h"
#include "sparrow/simple-scheduler.h"
#include "gen-cpp/ImpalaBackendService.h"

using namespace std;
using namespace boost;
using sparrow::Scheduler;
using sparrow::SimpleScheduler;

DEFINE_string(backends, "", "comma-separated list of <host:port> pairs");

namespace impala {


ExecEnv::ExecEnv()
  : stream_mgr_impl_(new DataStreamMgr()),
    client_cache_impl_(new BackendClientCache(0, 0)),
    fs_cache_impl_(new HdfsFsCache()),
    htable_cache_impl_(new HBaseTableCache()),
    tz_database_(TimezoneDatabase()),
    stream_mgr_(stream_mgr_impl_.get()),
    client_cache_(client_cache_impl_.get()),
    fs_cache_(fs_cache_impl_.get()),
    htable_cache_(htable_cache_impl_.get()) {
  // Initialize the scheduler with FLAGS_backends only if FLAGS_backends is set
  if (FLAGS_backends.empty()) {
    scheduler_ = NULL;
    return;
  }
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
  scheduler_ = scheduler_impl_.get();
}

ExecEnv::ExecEnv(HdfsFsCache* fs_cache)
  : stream_mgr_impl_(new DataStreamMgr()),
    client_cache_impl_(new BackendClientCache(0, 0)),
    fs_cache_impl_(),
    htable_cache_impl_(new HBaseTableCache()),
    tz_database_(TimezoneDatabase()),
    stream_mgr_(stream_mgr_impl_.get()),
    scheduler_(NULL),
    client_cache_(client_cache_impl_.get()),
    fs_cache_(fs_cache),
    htable_cache_(htable_cache_impl_.get()) {
}

ExecEnv::~ExecEnv() {
}

}
