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

#ifndef IMPALA_RPC_RPC_MGR_H
#define IMPALA_RPC_RPC_MGR_H

#include "common/status.h"
#include "kudu/rpc/messenger.h"
#include "kudu/rpc/result_tracker.h"
#include "kudu/util/metrics.h"
#include "rpc/impala-service-pool.h"

#include "gen-cpp/Types_types.h"

namespace kudu {
namespace rpc {
class RpcController;
class GeneratedServiceIf;
} // rpc
} // kudu

namespace impala {

/// Singleton class which manages all KRPC services and proxies.
///
/// SERVICES
/// --------
///
/// An RpcMgr manages 0 or more services: RPC interfaces that are a collection of remotely
/// accessible methods. A new service is registered by calling RegisterService(). All
/// services are served on the same port; the underlying RPC layer takes care of
/// de-multiplexing RPC calls to their respective endpoints.
///
/// Services are made available to remote clients when RpcMgr::StartServices() is called;
/// before this method no service method will be called.
///
/// Services may only be registered and started after RpcMgr::Init() is called.
///
/// PROXIES
/// -------
///
/// A proxy is a client-side interface to a remote service. Remote methods exported by
/// that service may be called through a proxy as though they were local methods.
///
/// A proxy can be obtained by calling GetProxy(). Proxies implement local methods which
/// call remote service methods, e.g. proxy->Foo(request, &response) will call the Foo()
/// service method on the service that 'proxy' points to.
///
/// Proxies may only be created after RpcMgr::Init() is called.
///
/// For example usage of proxies, please see rpc-mgr-test.cc
///
/// LIFECYCLE
/// ---------
///
/// RpcMgr resides inside the singleton ExecEnv class.
///
/// Before any proxy or service interactions, RpcMgr::Init() must be called exactly once
/// to start the reactor threads that service network events. Services must be registered
/// with RpcMgr::RegisterService() before RpcMgr::StartServices() is called. When shutting
/// down, RpcMgr::Shutdown() must be called to ensure that all services are cleanly
/// terminated. RpcMgr::Init() and RpcMgr::Shutdown() are not thread safe.
///
/// KRPC INTERNALS
/// --------------
///
/// Each service and proxy interacts with the network via a shared pool of 'reactor'
/// threads which respond to incoming and outgoing RPC events. The number of 'reactor'
/// threads are configurable via FLAGS_reactor_thread. By default, it's set to the number
/// of cpu cores. Incoming events are passed immediately to one of two thread pools: new
/// connections are handled by an 'acceptor' pool, and RPC request events are handled by
/// a per-service 'service' pool. The size of a 'service' pool is specified when calling
/// RegisterService().
///
/// All incoming RPC requests are placed into a per-service pool's fixed-size queue.
/// The service threads will dequeue from this queue and process the requests. If the
/// queue becomes full, the RPC will fail at the caller. The function IsServerTooBusy()
/// below will return true for this case. The size of the queue is specified when calling
/// RegisterService().
///
/// Inbound connection set-up is handled by a small fixed-size pool of 'acceptor'
/// threads. The number of threads that accept new TCP connection requests to the service
/// port is configurable via FLAGS_acceptor_threads.
///
/// If 'use_tls' is true, then the underlying messenger is configured with the required
/// certificates, and encryption is enabled and marked as required.
class RpcMgr {
 public:
  RpcMgr(bool use_tls = false) : use_tls_(use_tls) {}

  /// Initializes the reactor threads, and prepares for sending outbound RPC requests.
  Status Init() WARN_UNUSED_RESULT;

  bool is_inited() const { return messenger_.get() != nullptr; }

  /// Start the acceptor threads which listen on 'address', making KRPC services
  /// available. 'address' has to be a resolved IP address. Before this method is called,
  /// remote clients will get a 'connection refused' error when trying to invoke an RPC
  /// on this host.
  Status StartServices(const TNetworkAddress& address) WARN_UNUSED_RESULT;

  /// Register a new service.
  ///
  /// 'num_service_threads' is the number of threads that should be started to execute RPC
  /// handlers for the new service.
  ///
  /// 'service_queue_depth' is the maximum number of requests that may be queued for this
  /// service before clients begin to see rejection errors.
  ///
  /// 'service_ptr' contains an interface implementation that will handle RPCs. Note that
  /// the service name has to be unique within an Impala instance or the registration will
  /// fail.
  ///
  /// 'service_mem_tracker' is the MemTracker for tracking the memory usage of RPC
  /// payloads in the service queue.
  ///
  /// It is an error to call this after StartServices() has been called.
  Status RegisterService(int32_t num_service_threads, int32_t service_queue_depth,
      kudu::rpc::GeneratedServiceIf* service_ptr, MemTracker* service_mem_tracker)
      WARN_UNUSED_RESULT;

  /// Creates a new proxy for a remote service of type P at location 'address', and places
  /// it in 'proxy'. 'P' must descend from kudu::rpc::ServiceIf. Note that 'address' must
  /// be a resolved IP address.
  template <typename P>
  Status GetProxy(const TNetworkAddress& address, std::unique_ptr<P>* proxy)
      WARN_UNUSED_RESULT;

  /// Shut down all previously registered services. All service pools are shut down.
  /// All acceptor and reactor threads within the messenger are also shut down.
  /// All unprocessed incoming requests will be replied with error messages.
  void Shutdown();

  /// Returns true if the last RPC of 'rpc_controller' failed because the remote
  /// service's queue filled up and couldn't accept more incoming requests.
  /// 'rpc_controller' should contain the status of the last RPC call.
  static bool IsServerTooBusy(const kudu::rpc::RpcController& rpc_controller);

  const scoped_refptr<kudu::rpc::ResultTracker> result_tracker() const {
    return tracker_;
  }

  scoped_refptr<kudu::MetricEntity> metric_entity() const {
    return messenger_->metric_entity();
  }

  std::shared_ptr<kudu::rpc::Messenger> messenger() { return messenger_; }

  /// Writes a JSON representation of the RpcMgr's metrics to a value named 'services' in
  /// 'document'. It will include the number of RPCs accepted so far, the number of calls
  /// in flight, and metrics and histograms for each service and their methods.
  void ToJson(rapidjson::Document* document);

  ~RpcMgr() {
    DCHECK_EQ(service_pools_.size(), 0)
        << "Must call Shutdown() before destroying RpcMgr";
  }

 private:
  /// One pool per registered service. scoped_refptr<> is dictated by the Kudu interface.
  std::vector<scoped_refptr<ImpalaServicePool>> service_pools_;

  /// Required Kudu boilerplate for constructing the MetricEntity passed
  /// to c'tor of ServiceIf when creating a service.
  /// TODO(KRPC): Integrate with Impala MetricGroup.
  kudu::MetricRegistry registry_;

  /// Used when creating a new service. Shared across all services which don't really
  /// track results for idempotent RPC calls.
  const scoped_refptr<kudu::rpc::ResultTracker> tracker_;

  /// Holds a reference to the acceptor pool. Shared ownership with messenger_.
  std::shared_ptr<kudu::rpc::AcceptorPool> acceptor_pool_;

  /// Container for reactor threads which run event loops for RPC services, plus acceptor
  /// threads which manage connection setup. Has to be a shared_ptr as required by
  /// MessangerBuilder::Build().
  std::shared_ptr<kudu::rpc::Messenger> messenger_;

  /// True after StartServices() completes.
  bool services_started_ = false;

  /// True if TLS is configured for communication between Impala backends. messenger_ will
  /// be configured to use TLS if this is set.
  const bool use_tls_;
};

} // namespace impala

#endif
