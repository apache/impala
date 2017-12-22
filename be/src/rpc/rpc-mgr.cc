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

#include "rpc/rpc-mgr.h"

#include "exec/kudu-util.h"
#include "kudu/rpc/acceptor_pool.h"
#include "kudu/rpc/rpc_controller.h"
#include "kudu/rpc/rpc_introspection.pb.h"
#include "kudu/rpc/service_if.h"
#include "kudu/util/monotime.h"
#include "kudu/util/net/net_util.h"
#include "util/auth-util.h"
#include "util/cpu-info.h"
#include "util/network-util.h"

#include "common/names.h"

using kudu::HostPort;
using kudu::MetricEntity;
using kudu::MonoDelta;
using kudu::rpc::AcceptorPool;
using kudu::rpc::MessengerBuilder;
using kudu::rpc::Messenger;
using kudu::rpc::RpcController;
using kudu::rpc::ServiceIf;
using kudu::Sockaddr;

DECLARE_string(hostname);
DECLARE_string(principal);
DECLARE_string(be_principal);

DEFINE_int32(num_acceptor_threads, 2,
    "Number of threads dedicated to accepting connection requests for RPC services");
DEFINE_int32(num_reactor_threads, 0,
    "Number of threads dedicated to managing network IO for RPC services. If left at "
    "default value 0, it will be set to number of CPU cores.");
DEFINE_int32(rpc_retry_interval_ms, 5,
    "Time in millisecond of waiting before retrying an RPC when remote is busy");

namespace impala {

Status RpcMgr::Init() {
  MessengerBuilder bld("impala-server");
  const scoped_refptr<MetricEntity> entity(
      METRIC_ENTITY_server.Instantiate(&registry_, "krpc-metrics"));
  int num_reactor_threads =
      FLAGS_num_reactor_threads > 0 ? FLAGS_num_reactor_threads : CpuInfo::num_cores();
  bld.set_num_reactors(num_reactor_threads).set_metric_entity(entity);

  // Disable idle connection detection by setting keepalive_time to -1. Idle connections
  // tend to be closed and re-opened around the same time, which may lead to negotiation
  // timeout. Please see IMPALA-5557 for details. Until KUDU-279 is fixed, closing idle
  // connections is also racy and leads to query failures.
  bld.set_connection_keepalive_time(MonoDelta::FromMilliseconds(-1));

  if (IsKerberosEnabled()) {
    string internal_principal;
    RETURN_IF_ERROR(GetInternalKerberosPrincipal(&internal_principal));

    string service_name, unused_hostname, unused_realm;
    RETURN_IF_ERROR(ParseKerberosPrincipal(internal_principal, &service_name,
        &unused_hostname, &unused_realm));
    bld.set_sasl_proto_name(service_name);
    // TODO: Once the Messenger can take more options pertaining to 'rpc_authentication'
    // and more, we need to explicitly set those options here. (KUDU-2228)
  }
  KUDU_RETURN_IF_ERROR(bld.Build(&messenger_), "Could not build messenger");
  return Status::OK();
}

Status RpcMgr::RegisterService(int32_t num_service_threads, int32_t service_queue_depth,
    unique_ptr<ServiceIf> service_ptr) {
  DCHECK(is_inited()) << "Must call Init() before RegisterService()";
  DCHECK(!services_started_) << "Cannot call RegisterService() after StartServices()";
  scoped_refptr<ImpalaServicePool> service_pool =
      new ImpalaServicePool(std::move(service_ptr),
          messenger_->metric_entity(), service_queue_depth);
  // Start the thread pool first before registering the service in case the startup fails.
  RETURN_IF_ERROR(
      service_pool->Init(num_service_threads));
  KUDU_RETURN_IF_ERROR(
      messenger_->RegisterService(service_pool->service_name(), service_pool),
      "Could not register service");

  VLOG_QUERY << "Registered KRPC service: " << service_pool->service_name();
  service_pools_.push_back(std::move(service_pool));

  return Status::OK();
}

Status RpcMgr::StartServices(const TNetworkAddress& address) {
  DCHECK(is_inited()) << "Must call Init() before StartServices()";
  DCHECK(!services_started_) << "May not call StartServices() twice";

  // Convert 'address' to Kudu's Sockaddr
  DCHECK(IsResolvedAddress(address));
  Sockaddr sockaddr;
  RETURN_IF_ERROR(TNetworkAddressToSockaddr(address, &sockaddr));

  // Call the messenger to create an AcceptorPool for us.
  shared_ptr<AcceptorPool> acceptor_pool;
  KUDU_RETURN_IF_ERROR(messenger_->AddAcceptorPool(sockaddr, &acceptor_pool),
      "Failed to add acceptor pool");
  KUDU_RETURN_IF_ERROR(acceptor_pool->Start(FLAGS_num_acceptor_threads),
      "Acceptor pool failed to start");
  VLOG_QUERY << "Started " << FLAGS_num_acceptor_threads << " acceptor threads";
  services_started_ = true;
  return Status::OK();
}

void RpcMgr::Shutdown() {
  if (messenger_.get() == nullptr) return;
  for (auto service_pool : service_pools_) service_pool->Shutdown();

  messenger_->UnregisterAllServices();
  messenger_->Shutdown();
  service_pools_.clear();
}

bool RpcMgr::IsServerTooBusy(const RpcController& rpc_controller) {
  const kudu::Status status = rpc_controller.status();
  const kudu::rpc::ErrorStatusPB* err = rpc_controller.error_response();
  return status.IsRemoteError() && err != nullptr && err->has_code() &&
      err->code() == kudu::rpc::ErrorStatusPB::ERROR_SERVER_TOO_BUSY;
}

} // namespace impala
