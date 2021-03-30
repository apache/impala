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

#include <unordered_map>

#include <boost/scoped_ptr.hpp>

// NOTE: try not to add more headers here: exec-env.h is included in many many files.
#include "common/global-types.h"
#include "common/status.h"
#include "runtime/client-cache-types.h"
#include "util/hdfs-bulk-ops-defs.h" // For declaration of HdfsOpThreadPool
#include "util/network-util.h"
#include "util/spinlock.h"

namespace kudu {
namespace client {
class KuduClient;
} // namespace client
} // namespace kudu

namespace impala {

class AdmissionController;
class BufferPool;
class CallableThreadPool;
class ClusterMembershipMgr;
class ControlService;
class DataStreamMgr;
class DataStreamService;
class QueryExecMgr;
class Frontend;
class HBaseTableFactory;
class HdfsFsCache;
class ImpalaServer;
class KrpcDataStreamMgr;
class LibCache;
class MemTracker;
class MetricGroup;
class PoolMemTrackerRegistry;
class ObjectPool;
class QueryResourceMgr;
class RequestPoolService;
class ReservationTracker;
class RpcMgr;
class Scheduler;
class StatestoreSubscriber;
class SystemStateInfo;
class ThreadResourceMgr;
class TmpFileMgr;
class Webserver;

namespace io {
  class DiskIoMgr;
}

/// Execution environment for Impala daemon. Contains all required global structures, and
/// handles to singleton services. Clients must call StartServices() exactly once to
/// properly initialise service state.
///
/// There should only be one ExecEnv instance. It should always be accessed by calling
/// ExecEnv::GetInstance().
class ExecEnv {
 public:
  ExecEnv();

  ExecEnv(int backend_port, int krpc_port,
      int subscriber_port, int webserver_port, const std::string& statestore_host,
      int statestore_port);

  /// Returns the most recently created exec env instance. In a normal impalad, this is
  /// the only instance. In test setups with multiple ExecEnv's per process,
  /// we return the most recently created instance.
  static ExecEnv* GetInstance() { return exec_env_; }

  /// Destructor - only used in backend tests that create new environment per test.
  ~ExecEnv();

  /// Initialize the exec environment, including parsing memory limits and initializing
  /// subsystems like the webserver, scheduler etc.
  Status Init();

  /// Starts the service to subscribe to the statestore.
  Status StartStatestoreSubscriberService() WARN_UNUSED_RESULT;

  /// Starts krpc, if needed. Start this last so everything is in place before accepting
  /// the first call.
  Status StartKrpcService() WARN_UNUSED_RESULT;

  /// TODO: Should ExecEnv own the ImpalaServer as well?
  /// Registers the ImpalaServer 'server' with this ExecEnv instance. May only be called
  /// once.
  void SetImpalaServer(ImpalaServer* server);

  /// Get the address of the thrift backend service. Only valid to call if
  /// StartServices() was successful.
  TNetworkAddress GetThriftBackendAddress() const;

  const BackendIdPB& backend_id() const { return backend_id_; }

  KrpcDataStreamMgr* stream_mgr() { return stream_mgr_.get(); }

  ImpalaBackendClientCache* impalad_client_cache() {
    return impalad_client_cache_.get();
  }
  CatalogServiceClientCache* catalogd_client_cache() {
    return catalogd_client_cache_.get();
  }
  HBaseTableFactory* htable_factory() { return htable_factory_.get(); }
  io::DiskIoMgr* disk_io_mgr() { return disk_io_mgr_.get(); }
  Webserver* webserver() { return webserver_.get(); }
  MetricGroup* metrics() { return metrics_.get(); }
  MetricGroup* rpc_metrics() { return rpc_metrics_; }
  MemTracker* process_mem_tracker() { return mem_tracker_.get(); }
  ThreadResourceMgr* thread_mgr() { return thread_mgr_.get(); }
  HdfsOpThreadPool* hdfs_op_thread_pool() { return hdfs_op_thread_pool_.get(); }
  TmpFileMgr* tmp_file_mgr() { return tmp_file_mgr_.get(); }
  ImpalaServer* impala_server() { return impala_server_; }
  Frontend* frontend() { return frontend_.get(); }
  RequestPoolService* request_pool_service() { return request_pool_service_.get(); }
  CallableThreadPool* rpc_pool() { return async_rpc_pool_.get(); }
  QueryExecMgr* query_exec_mgr() { return query_exec_mgr_.get(); }
  RpcMgr* rpc_mgr() const { return rpc_mgr_.get(); }
  PoolMemTrackerRegistry* pool_mem_trackers() { return pool_mem_trackers_.get(); }
  ReservationTracker* buffer_reservation() { return buffer_reservation_.get(); }
  BufferPool* buffer_pool() { return buffer_pool_.get(); }
  SystemStateInfo* system_state_info() { return system_state_info_.get(); }

  void set_enable_webserver(bool enable) { enable_webserver_ = enable; }

  ClusterMembershipMgr* cluster_membership_mgr() { return cluster_membership_mgr_.get(); }
  Scheduler* scheduler() { return scheduler_.get(); }
  AdmissionController* admission_controller() { return admission_controller_.get(); }
  StatestoreSubscriber* subscriber() { return statestore_subscriber_.get(); }

  const IpAddr& ip_address() const { return ip_address_; }

  const TNetworkAddress& krpc_address() const { return krpc_address_; }

  /// Initializes the exec env for running FE tests.
  Status InitForFeTests() WARN_UNUSED_RESULT;

  /// Returns true if this environment was created from the FE tests. This makes the
  /// environment special since the JVM is started first and libraries are loaded
  /// differently.
  bool is_fe_tests() { return is_fe_tests_; }

  /// Returns the configured defaultFs set in core-site.xml
  const string& default_fs() { return default_fs_; }

  /// Gets a KuduClient for this list of master addresses. It will look up and share
  /// an existing KuduClient if possible. Otherwise, it will create a new KuduClient
  /// internally and return a pointer to it. All KuduClients accessed through this
  /// interface are owned by the ExecEnv. Thread safe.
  Status GetKuduClient(const std::vector<std::string>& master_addrs,
      kudu::client::KuduClient** client) WARN_UNUSED_RESULT;

  int64_t admit_mem_limit() const { return admit_mem_limit_; }
  int64_t admission_slots() const { return admission_slots_; }

 private:
  // Used to uniquely identify this impalad.
  BackendIdPB backend_id_;

  boost::scoped_ptr<ObjectPool> obj_pool_;
  boost::scoped_ptr<MetricGroup> metrics_;
  boost::scoped_ptr<KrpcDataStreamMgr> stream_mgr_;
  boost::scoped_ptr<ClusterMembershipMgr> cluster_membership_mgr_;
  boost::scoped_ptr<Scheduler> scheduler_;
  boost::scoped_ptr<AdmissionController> admission_controller_;
  boost::scoped_ptr<StatestoreSubscriber> statestore_subscriber_;
  boost::scoped_ptr<ImpalaBackendClientCache> impalad_client_cache_;
  boost::scoped_ptr<CatalogServiceClientCache> catalogd_client_cache_;
  boost::scoped_ptr<HBaseTableFactory> htable_factory_;
  boost::scoped_ptr<io::DiskIoMgr> disk_io_mgr_;
  boost::scoped_ptr<Webserver> webserver_;
  boost::scoped_ptr<MemTracker> mem_tracker_;
  boost::scoped_ptr<PoolMemTrackerRegistry> pool_mem_trackers_;
  boost::scoped_ptr<ThreadResourceMgr> thread_mgr_;

  // Thread pool for running HdfsOp operations. Only used by the coordinator, so it's
  // only started if FLAGS_is_coordinator is 'true'.
  boost::scoped_ptr<HdfsOpThreadPool> hdfs_op_thread_pool_;

  boost::scoped_ptr<TmpFileMgr> tmp_file_mgr_;
  boost::scoped_ptr<RequestPoolService> request_pool_service_;
  boost::scoped_ptr<Frontend> frontend_;

  boost::scoped_ptr<CallableThreadPool> async_rpc_pool_;
  boost::scoped_ptr<QueryExecMgr> query_exec_mgr_;
  boost::scoped_ptr<RpcMgr> rpc_mgr_;
  boost::scoped_ptr<ControlService> control_svc_;
  boost::scoped_ptr<DataStreamService> data_svc_;

  /// Query-wide buffer pool and the root reservation tracker for the pool. The
  /// reservation limit is equal to the maximum capacity of the pool. Created in
  /// InitBufferPool();
  boost::scoped_ptr<ReservationTracker> buffer_reservation_;
  boost::scoped_ptr<BufferPool> buffer_pool_;

  /// Tracks system resource usage which we then include in profiles.
  boost::scoped_ptr<SystemStateInfo> system_state_info_;

  /// Not owned by this class
  ImpalaServer* impala_server_ = nullptr;
  MetricGroup* rpc_metrics_ = nullptr;

  bool enable_webserver_;

 private:
  friend class TestEnv;
  friend class DataStreamTest;

  static ExecEnv* exec_env_;
  bool is_fe_tests_ = false;

  /// Address of the thrift based ImpalaInternalService. In backend tests we allow
  /// wildcard port 0, so this may not be the actual backend address.
  TNetworkAddress configured_backend_address_;

  /// Resolved IP address of the host name.
  IpAddr ip_address_;

  /// Address of the KRPC-based ImpalaInternalService
  TNetworkAddress krpc_address_;

  /// fs.defaultFs value set in core-site.xml
  std::string default_fs_;

  SpinLock kudu_client_map_lock_; // protects kudu_client_map_

  /// Opaque type for storing the pointer to the KuduClient. This allows us
  /// to avoid including Kudu header files.
  struct KuduClientPtr;

  /// Map from the master addresses string for a Kudu table to the KuduClientPtr for
  /// accessing that table. The master address string is constructed by joining
  /// the sorted master address list entries with a comma separator.
  typedef std::unordered_map<std::string, std::unique_ptr<KuduClientPtr>> KuduClientMap;

  /// Map for sharing KuduClients across the ExecEnv. This map requires that the master
  /// address lists be identical in order to share a KuduClient.
  KuduClientMap kudu_client_map_;

  /// Return the bytes of memory available for queries to execute with - i.e.
  /// mem_tracker()->limit() with any overhead that can't be used subtracted out,
  /// such as the JVM if --mem_limit_includes_jvm=true. Set in Init().
  int64_t admit_mem_limit_;

  /// The maximum number of admission slots that should be used on this host. This
  /// only takes effect if the admission slot functionality is enabled in admission
  /// control. Until IMPALA-8757 is fixed, the slots are only checked for non-default
  /// executor groups.
  ///
  /// By default, the number of slots is based on the number of cores in the system.
  /// The number of slots limits the number of queries that can run concurrently on
  /// this backend. Queries take up multiple slots only when mt_dop > 1.
  int64_t admission_slots_;

  /// Choose a memory limit (returned in *bytes_limit) based on the --mem_limit flag and
  /// the memory available to the daemon process. Returns an error if the memory limit is
  /// invalid or another error is encountered that should prevent starting up the daemon.
  /// Logs the memory limit chosen and any relevant diagnostics related to that choice.
  Status ChooseProcessMemLimit(int64_t* bytes_limit);

  /// Initialise 'buffer_pool_' and 'buffer_reservation_' with given capacity.
  void InitBufferPool(int64_t min_page_len, int64_t capacity, int64_t clean_pages_limit);

  /// Initialise 'mem_tracker_' with a limit of 'bytes_limit'. Must be called after
  /// InitBufferPool() and RegisterMemoryMetrics().
  void InitMemTracker(int64_t bytes_limit);

  /// Initialize 'system_state_info_' to track system resource usage.
  void InitSystemStateInfo();
};

} // namespace impala

#endif
