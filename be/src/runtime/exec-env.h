// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.


#ifndef IMPALA_RUNTIME_EXEC_ENV_H
#define IMPALA_RUNTIME_EXEC_ENV_H

#include <boost/scoped_ptr.hpp>

// NOTE: try not to add more headers here: exec-env.h is included in many many files.
#include "common/status.h"
#include "runtime/client-cache-types.h"
#include "util/hdfs-bulk-ops-defs.h" // For declaration of HdfsOpThreadPool

namespace impala {

class AdmissionController;
class BufferPool;
class CallableThreadPool;
class DataStreamMgr;
class DiskIoMgr;
class QueryExecMgr;
class Frontend;
class HBaseTableFactory;
class HdfsFsCache;
class ImpalaServer;
class LibCache;
class MemTracker;
class PoolMemTrackerRegistry;
class MetricGroup;
class QueryResourceMgr;
class RequestPoolService;
class ReservationTracker;
class Scheduler;
class StatestoreSubscriber;
class TestExecEnv;
class ThreadResourceMgr;
class TmpFileMgr;
class Webserver;

/// Execution environment for queries/plan fragments.
/// Contains all required global structures, and handles to
/// singleton services. Clients must call StartServices exactly
/// once to properly initialise service state.
class ExecEnv {
 public:
  ExecEnv();

  ExecEnv(const std::string& hostname, int backend_port, int subscriber_port,
      int webserver_port, const std::string& statestore_host, int statestore_port);

  /// Returns the first created exec env instance. In a normal impalad, this is
  /// the only instance. In test setups with multiple ExecEnv's per process,
  /// we return the most recently created instance.
  static ExecEnv* GetInstance() { return exec_env_; }

  /// Destructor - only used in backend tests that create new environment per test.
  virtual ~ExecEnv();

  void SetImpalaServer(ImpalaServer* server) { impala_server_ = server; }

  DataStreamMgr* stream_mgr() { return stream_mgr_.get(); }
  ImpalaBackendClientCache* impalad_client_cache() {
    return impalad_client_cache_.get();
  }
  CatalogServiceClientCache* catalogd_client_cache() {
    return catalogd_client_cache_.get();
  }
  HBaseTableFactory* htable_factory() { return htable_factory_.get(); }
  DiskIoMgr* disk_io_mgr() { return disk_io_mgr_.get(); }
  Webserver* webserver() { return webserver_.get(); }
  MetricGroup* metrics() { return metrics_.get(); }
  MemTracker* process_mem_tracker() { return mem_tracker_.get(); }
  ThreadResourceMgr* thread_mgr() { return thread_mgr_.get(); }
  HdfsOpThreadPool* hdfs_op_thread_pool() { return hdfs_op_thread_pool_.get(); }
  TmpFileMgr* tmp_file_mgr() { return tmp_file_mgr_.get(); }
  CallableThreadPool* fragment_exec_thread_pool() {
    return fragment_exec_thread_pool_.get();
  }
  ImpalaServer* impala_server() { return impala_server_; }
  Frontend* frontend() { return frontend_.get(); }
  RequestPoolService* request_pool_service() { return request_pool_service_.get(); }
  CallableThreadPool* rpc_pool() { return async_rpc_pool_.get(); }
  QueryExecMgr* query_exec_mgr() { return query_exec_mgr_.get(); }
  PoolMemTrackerRegistry* pool_mem_trackers() { return pool_mem_trackers_.get(); }
  ReservationTracker* buffer_reservation() { return buffer_reservation_.get(); }
  BufferPool* buffer_pool() { return buffer_pool_.get(); }

  void set_enable_webserver(bool enable) { enable_webserver_ = enable; }

  Scheduler* scheduler() { return scheduler_.get(); }
  AdmissionController* admission_controller() { return admission_controller_.get(); }
  StatestoreSubscriber* subscriber() { return statestore_subscriber_.get(); }

  const TNetworkAddress& backend_address() const { return backend_address_; }

  /// Starts any dependent services in their correct order
  virtual Status StartServices();

  /// Initializes the exec env for running FE tests.
  Status InitForFeTests();

  /// Returns true if this environment was created from the FE tests. This makes the
  /// environment special since the JVM is started first and libraries are loaded
  /// differently.
  bool is_fe_tests() { return is_fe_tests_; }

  /// Returns the configured defaultFs set in core-site.xml
  string default_fs() { return default_fs_; }

 protected:
  /// Leave protected so that subclasses can override
  boost::scoped_ptr<MetricGroup> metrics_;
  boost::scoped_ptr<DataStreamMgr> stream_mgr_;
  boost::scoped_ptr<Scheduler> scheduler_;
  boost::scoped_ptr<AdmissionController> admission_controller_;
  boost::scoped_ptr<StatestoreSubscriber> statestore_subscriber_;
  boost::scoped_ptr<ImpalaBackendClientCache> impalad_client_cache_;
  boost::scoped_ptr<CatalogServiceClientCache> catalogd_client_cache_;
  boost::scoped_ptr<HBaseTableFactory> htable_factory_;
  boost::scoped_ptr<DiskIoMgr> disk_io_mgr_;
  boost::scoped_ptr<Webserver> webserver_;
  boost::scoped_ptr<MemTracker> mem_tracker_;
  boost::scoped_ptr<PoolMemTrackerRegistry> pool_mem_trackers_;
  boost::scoped_ptr<ThreadResourceMgr> thread_mgr_;
  boost::scoped_ptr<HdfsOpThreadPool> hdfs_op_thread_pool_;
  boost::scoped_ptr<TmpFileMgr> tmp_file_mgr_;
  boost::scoped_ptr<RequestPoolService> request_pool_service_;
  boost::scoped_ptr<Frontend> frontend_;
  boost::scoped_ptr<CallableThreadPool> fragment_exec_thread_pool_;
  boost::scoped_ptr<CallableThreadPool> async_rpc_pool_;
  boost::scoped_ptr<QueryExecMgr> query_exec_mgr_;

  /// Query-wide buffer pool and the root reservation tracker for the pool. The
  /// reservation limit is equal to the maximum capacity of the pool.
  /// For now this is only used by backend tests that create them via InitBufferPool();
  boost::scoped_ptr<ReservationTracker> buffer_reservation_;
  boost::scoped_ptr<BufferPool> buffer_pool_;

  /// Not owned by this class
  ImpalaServer* impala_server_;

  bool enable_webserver_;

 private:
  friend class TestEnv;

  static ExecEnv* exec_env_;
  bool is_fe_tests_;

  /// Address of the Impala backend server instance
  TNetworkAddress backend_address_;

  /// fs.defaultFs value set in core-site.xml
  std::string default_fs_;

  /// Initialise 'buffer_pool_' and 'buffer_reservation_' with given capacity.
  void InitBufferPool(int64_t min_page_len, int64_t capacity);
};

} // namespace impala

#endif
