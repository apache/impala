// Copyright (c) 2012 Cloudera, Inc. All rights reserved.

#include "runtime/exec-env.h"

#include "runtime/data-stream-mgr.h"
#include "runtime/hdfs-fs-cache.h"
#include "runtime/client-cache.h"
#include "runtime/simple-scheduler.h"

namespace impala {

ExecEnv::ExecEnv()
  : stream_mgr_impl_(new DataStreamMgr()),
    scheduler_impl_(new SimpleScheduler()),
    client_cache_impl_(new BackendClientCache(0, 0)),
    fs_cache_impl_(new HdfsFsCache()),
    stream_mgr_(stream_mgr_impl_.get()),
    scheduler_(scheduler_impl_.get()),
    client_cache_(client_cache_impl_.get()),
    fs_cache_(fs_cache_impl_.get()) {
}

ExecEnv::ExecEnv(HdfsFsCache* fs_cache)
  : stream_mgr_impl_(new DataStreamMgr()),
    scheduler_impl_(new SimpleScheduler()),
    client_cache_impl_(new BackendClientCache(0, 0)),
    fs_cache_impl_(),
    stream_mgr_(stream_mgr_impl_.get()),
    scheduler_(scheduler_impl_.get()),
    client_cache_(client_cache_impl_.get()),
    fs_cache_(fs_cache) {
}

ExecEnv::~ExecEnv() {
}

}
