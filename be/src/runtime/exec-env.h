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


#ifndef IMPALA_RUNTIME_EXEC_ENV_H
#define IMPALA_RUNTIME_EXEC_ENV_H

#include <boost/scoped_ptr.hpp>
#include <boost/shared_ptr.hpp>
#include <boost/thread/thread.hpp>

#include "exprs/timestamp-functions.h"
#include "common/status.h"
#include "runtime/client-cache.h"

namespace impala {

class DataStreamMgr;
class DiskIoMgr;
class HBaseTableCache;
class HdfsFsCache;
class Scheduler;
class SubscriptionManager;
class TestExecEnv;
class Webserver;
class Metrics;
class MemLimit;

// Execution environment for queries/plan fragments.
// Contains all required global structures, and handles to
// singleton services. Clients must call StartServices exactly
// once to properly initialise service state.
class ExecEnv {
 public:
  ExecEnv();
  virtual ~ExecEnv();

  SubscriptionManager* subscription_mgr() {
    return subscription_mgr_.get();
  }

  DataStreamMgr* stream_mgr() { return stream_mgr_.get(); }
  ImpalaInternalServiceClientCache* client_cache() { return client_cache_.get(); }
  HdfsFsCache* fs_cache() { return fs_cache_.get(); }
  HBaseTableCache* htable_cache() { return htable_cache_.get(); }
  DiskIoMgr* disk_io_mgr() { return disk_io_mgr_.get(); }
  Webserver* webserver() { return webserver_.get(); }
  Metrics* metrics() { return metrics_.get(); }
  MemLimit* mem_limit() { return mem_limit_.get(); }

  void set_enable_webserver(bool enable) { enable_webserver_ = enable; }

  Scheduler* scheduler() { return scheduler_.get(); }

  // Starts any dependent services in their correct order
  virtual Status StartServices();

 protected:
  // Leave protected so that subclasses can override
  boost::scoped_ptr<DataStreamMgr> stream_mgr_;
  boost::scoped_ptr<Scheduler> scheduler_;
  boost::scoped_ptr<SubscriptionManager> subscription_mgr_;
  boost::scoped_ptr<ImpalaInternalServiceClientCache> client_cache_;
  boost::scoped_ptr<HdfsFsCache> fs_cache_;
  boost::scoped_ptr<HBaseTableCache> htable_cache_;
  boost::scoped_ptr<DiskIoMgr> disk_io_mgr_;
  boost::scoped_ptr<Webserver> webserver_;
  boost::scoped_ptr<Metrics> metrics_;
  boost::scoped_ptr<MemLimit> mem_limit_;

  bool enable_webserver_;

 private:
  TimezoneDatabase tz_database_;
};

} // namespace impala

#endif
