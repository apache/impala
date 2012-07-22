// Copyright (c) 2012 Cloudera, Inc. All rights reserved.

#ifndef IMPALA_RUNTIME_EXEC_ENV_H
#define IMPALA_RUNTIME_EXEC_ENV_H

#include <boost/scoped_ptr.hpp>
#include <boost/shared_ptr.hpp>
#include <boost/thread/thread.hpp>

#include "exprs/timestamp-functions.h"
#include "sparrow/state-store-subscriber-service.h"
#include "common/status.h"

namespace sparrow {

class Scheduler;

} // namespace sparrow

namespace impala {

class BackendClientCache;
class DataStreamMgr;
class HBaseTableCache;
class HdfsFsCache;
class TestExecEnv;
class Webserver;

// Execution environment for queries/plan fragments.
// Contains all required global structures, and handles to
// singleton services. Clients must call StartServices exactly
// once to properly initialise service state.
class ExecEnv {
 public:
  ExecEnv();
  virtual ~ExecEnv();

  // special c'tor for TestExecEnv::BackendInfo so that multiple in-process backends
  // can share a single fs cache
  ExecEnv(HdfsFsCache* fs_cache);

  sparrow::SubscriptionManager* subscription_mgr() {
    return subscription_mgr_.get();
  }

  DataStreamMgr* stream_mgr() { return stream_mgr_.get(); }
  BackendClientCache* client_cache() { return client_cache_.get(); }
  HdfsFsCache* fs_cache() { return fs_cache_.get(); }
  HBaseTableCache* htable_cache() { return htable_cache_.get(); }
  Webserver* webserver() { return webserver_.get(); }

  sparrow::Scheduler* scheduler() {
    DCHECK(scheduler_.get() != NULL);
    return scheduler_.get();
  }

  // Starts any dependent services in their correct order
  virtual Status StartServices();

 protected:
  // Leave protected so that subclasses can override
  boost::scoped_ptr<DataStreamMgr> stream_mgr_;
  boost::scoped_ptr<sparrow::Scheduler> scheduler_;
  boost::scoped_ptr<sparrow::SubscriptionManager> subscription_mgr_;
  boost::scoped_ptr<BackendClientCache> client_cache_;
  boost::scoped_ptr<HdfsFsCache> fs_cache_;
  boost::scoped_ptr<HBaseTableCache> htable_cache_;
  boost::scoped_ptr<Webserver> webserver_;

 private:
  TimezoneDatabase tz_database_;
};

} // namespace impala

#endif
