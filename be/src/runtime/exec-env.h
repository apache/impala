// Copyright (c) 2012 Cloudera, Inc. All rights reserved.

#ifndef IMPALA_RUNTIME_EXEC_ENV_H
#define IMPALA_RUNTIME_EXEC_ENV_H

#include <boost/scoped_ptr.hpp>
#include "exprs/timestamp-functions.h"
#include "common/status.h"

namespace impala {

class DataStreamMgr;
class Scheduler;
class SimpleScheduler;
class BackendClientCache;
class HdfsFsCache;
class HBaseTableCache;
class TestExecEnv;

// Execution environment for queries/plan fragments.
// Contains all required global structures.
class ExecEnv {
 public:
  ExecEnv();
  virtual ~ExecEnv();

  // special c'tor for TestExecEnv::BackendInfo so that multiple in-process backends
  // can share a single fs cache
  ExecEnv(HdfsFsCache* fs_cache);
  
  DataStreamMgr* stream_mgr() { return stream_mgr_; }
  Scheduler* scheduler() { return scheduler_; }
  BackendClientCache* client_cache() { return client_cache_; }
  HdfsFsCache* fs_cache() { return fs_cache_; }
  HBaseTableCache* htable_cache() { return htable_cache_; }

 private:
  boost::scoped_ptr<DataStreamMgr> stream_mgr_impl_;
  boost::scoped_ptr<SimpleScheduler> scheduler_impl_;
  boost::scoped_ptr<BackendClientCache> client_cache_impl_;
  boost::scoped_ptr<HdfsFsCache> fs_cache_impl_;
  boost::scoped_ptr<HBaseTableCache> htable_cache_impl_;

  TimezoneDatabase tz_database_;

 protected:
  // leave these protected so TestExecEnv can "override" them
  // w/o having to resort to virtual getters
  DataStreamMgr* stream_mgr_;
  Scheduler* scheduler_;
  BackendClientCache* client_cache_;
  HdfsFsCache* fs_cache_;
  HBaseTableCache* htable_cache_;

};

}

#endif
