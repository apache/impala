// Copyright (c) 2012 Cloudera, Inc. All rights reserved.

#ifndef IMPALA_RUNTIME_EXEC_ENV_H
#define IMPALA_RUNTIME_EXEC_ENV_H

#include <boost/scoped_ptr.hpp>
#include <boost/shared_ptr.hpp>
#include <boost/thread/thread.hpp>

#include "exprs/timestamp-functions.h"
#include "common/status.h"

namespace sparrow {

class Scheduler;
class SubscriptionManager;

} // namespace sparrow

namespace impala {

class BackendClientCache;
class DataStreamMgr;
class DiskIoMgr;
class HBaseTableCache;
class HdfsFsCache;
class TestExecEnv;
class Webserver;
class Metrics;

// Execution environment for queries/plan fragments.
// Contains all required global structures, and handles to
// singleton services. Clients must call StartServices exactly
// once to properly initialise service state.
class ExecEnv {
 public:
  ExecEnv();
  virtual ~ExecEnv();

  sparrow::SubscriptionManager* subscription_mgr() {
    return subscription_mgr_.get();
  }

  DataStreamMgr* stream_mgr() { return stream_mgr_.get(); }
  BackendClientCache* client_cache() { return client_cache_.get(); }
  HdfsFsCache* fs_cache() { return fs_cache_.get(); }
  HBaseTableCache* htable_cache() { return htable_cache_.get(); }
  DiskIoMgr* disk_io_mgr() { return disk_io_mgr_.get(); }
  Webserver* webserver() { return webserver_.get(); }
  Metrics* metrics() { return metrics_.get(); }

  void set_enable_webserver(bool enable) { enable_webserver_ = enable; }

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
  boost::scoped_ptr<DiskIoMgr> disk_io_mgr_;
  boost::scoped_ptr<Webserver> webserver_;
  boost::scoped_ptr<Metrics> metrics_;

  bool enable_webserver_;

 private:
  TimezoneDatabase tz_database_;
};

} // namespace impala

#endif
