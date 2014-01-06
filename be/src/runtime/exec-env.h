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

#include "common/status.h"
#include "exprs/timestamp-functions.h"
#include "runtime/client-cache.h"
#include "util/hdfs-bulk-ops.h" // For declaration of HdfsOpThreadPool

namespace impala {

class DataStreamMgr;
class DiskIoMgr;
class HBaseTableFactory;
class HdfsFsCache;
class LibCache;
class Scheduler;
class StatestoreSubscriber;
class TestExecEnv;
class Webserver;
class Metrics;
class MemTracker;
class ThreadResourceMgr;

// Execution environment for queries/plan fragments.
// Contains all required global structures, and handles to
// singleton services. Clients must call StartServices exactly
// once to properly initialise service state.
class ExecEnv {
 public:
  ExecEnv();

  ExecEnv(const std::string& hostname, int backend_port, int subscriber_port,
          int webserver_port, const std::string& statestore_host, int statestore_port);

  // Returns the first created exec env instance. In a normal impalad, this is
  // the only instance. In test setups with multiple ExecEnv's per process,
  // we return the first instance.
  static ExecEnv* GetInstance() { return exec_env_; }

  // Empty destructor because the compiler-generated one requires full
  // declarations for classes in scoped_ptrs.
  virtual ~ExecEnv();

  StatestoreSubscriber* statestore_subscriber() {
    return statestore_subscriber_.get();
  }

  DataStreamMgr* stream_mgr() { return stream_mgr_.get(); }
  ImpalaInternalServiceClientCache* impalad_client_cache() {
    return impalad_client_cache_.get();
  }
  CatalogServiceClientCache* catalogd_client_cache() {
    return catalogd_client_cache_.get();
  }
  HdfsFsCache* fs_cache() { return fs_cache_.get(); }
  LibCache* lib_cache() { return lib_cache_.get(); }
  HBaseTableFactory* htable_factory() { return htable_factory_.get(); }
  DiskIoMgr* disk_io_mgr() { return disk_io_mgr_.get(); }
  Webserver* webserver() { return webserver_.get(); }
  Metrics* metrics() { return metrics_.get(); }
  MemTracker* process_mem_tracker() { return mem_tracker_.get(); }
  ThreadResourceMgr* thread_mgr() { return thread_mgr_.get(); }
  HdfsOpThreadPool* hdfs_op_thread_pool() { return hdfs_op_thread_pool_.get(); }

  void set_enable_webserver(bool enable) { enable_webserver_ = enable; }

  Scheduler* scheduler() { return scheduler_.get(); }
  StatestoreSubscriber* subscriber() { return statestore_subscriber_.get(); }

  // Starts any dependent services in their correct order
  virtual Status StartServices();

  // Returns true if this environment was created from the FE tests. This makes the
  // environment special since the JVM is started first and libraries are loaded
  // differently.
  bool is_fe_tests() { return is_fe_tests_; }
  void set_is_fe_tests(bool v) { is_fe_tests_ = v; }

 protected:
  // Leave protected so that subclasses can override
  boost::scoped_ptr<DataStreamMgr> stream_mgr_;
  boost::scoped_ptr<Scheduler> scheduler_;
  boost::scoped_ptr<StatestoreSubscriber> statestore_subscriber_;
  boost::scoped_ptr<ImpalaInternalServiceClientCache> impalad_client_cache_;
  boost::scoped_ptr<CatalogServiceClientCache> catalogd_client_cache_;
  boost::scoped_ptr<HdfsFsCache> fs_cache_;
  boost::scoped_ptr<LibCache> lib_cache_;
  boost::scoped_ptr<HBaseTableFactory> htable_factory_;
  boost::scoped_ptr<DiskIoMgr> disk_io_mgr_;
  boost::scoped_ptr<Webserver> webserver_;
  boost::scoped_ptr<Metrics> metrics_;
  boost::scoped_ptr<MemTracker> mem_tracker_;
  boost::scoped_ptr<ThreadResourceMgr> thread_mgr_;
  boost::scoped_ptr<HdfsOpThreadPool> hdfs_op_thread_pool_;

  bool enable_webserver_;

 private:
  static ExecEnv* exec_env_;
  TimezoneDatabase tz_database_;
  bool is_fe_tests_;
};

} // namespace impala

#endif
