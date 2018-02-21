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
#include "util/openssl-util.h"

#include "common/names.h"

using namespace rapidjson;

using kudu::HostPort;
using kudu::MetricEntity;
using kudu::MonoDelta;
using kudu::rpc::AcceptorPool;
using kudu::rpc::DumpRunningRpcsRequestPB;
using kudu::rpc::DumpRunningRpcsResponsePB;
using kudu::rpc::MessengerBuilder;
using kudu::rpc::Messenger;
using kudu::rpc::RpcConnectionPB;
using kudu::rpc::RpcController;
using kudu::rpc::GeneratedServiceIf;
using kudu::Sockaddr;

DECLARE_string(hostname);
DECLARE_string(principal);
DECLARE_string(be_principal);
DECLARE_string(keytab_file);

// Impala's TLS flags.
DECLARE_string(ssl_client_ca_certificate);
DECLARE_string(ssl_server_certificate);
DECLARE_string(ssl_private_key);
DECLARE_string(ssl_private_key_password_cmd);
DECLARE_string(ssl_cipher_list);
DECLARE_string(ssl_minimum_version);

// Defined in kudu/rpc/rpcz_store.cc
DECLARE_int32(rpc_duration_too_long_ms);

// Defined in kudu/rpc/transfer.cc
DECLARE_int32(rpc_max_message_size);

DEFINE_int32(num_acceptor_threads, 2,
    "Number of threads dedicated to accepting connection requests for RPC services");
DEFINE_int32(num_reactor_threads, 0,
    "Number of threads dedicated to managing network IO for RPC services. If left at "
    "default value 0, it will be set to number of CPU cores.");
DEFINE_int32(rpc_retry_interval_ms, 5,
    "Time in millisecond of waiting before retrying an RPC when remote is busy");
DEFINE_int32(rpc_negotiation_timeout_ms, 300000,
    "Time in milliseconds of waiting for a negotiation to complete before timing out.");
DEFINE_int32(rpc_negotiation_thread_count, 64,
    "Maximum number of threads dedicated to handling RPC connection negotiations.");

namespace impala {

Status RpcMgr::Init() {
  // Log any RPCs which take longer than 2 minutes.
  FLAGS_rpc_duration_too_long_ms = 2 * 60 * 1000;

  // IMPALA-4874: Impala requires support for messages up to 2GB. Override KRPC's default
  //              maximum of 50MB.
  FLAGS_rpc_max_message_size = numeric_limits<int32_t>::max();

  MessengerBuilder bld("impala-server");
  const scoped_refptr<MetricEntity> entity(
      METRIC_ENTITY_server.Instantiate(&registry_, "krpc-metrics"));
  int num_reactor_threads =
      FLAGS_num_reactor_threads > 0 ? FLAGS_num_reactor_threads : CpuInfo::num_cores();
  bld.set_num_reactors(num_reactor_threads).set_metric_entity(entity);
  bld.set_rpc_negotiation_timeout_ms(FLAGS_rpc_negotiation_timeout_ms);
  bld.set_max_negotiation_threads(max(1, FLAGS_rpc_negotiation_thread_count));

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
    bld.set_rpc_authentication("required");
    bld.set_keytab_file(FLAGS_keytab_file);
  }

  if (use_tls_) {
    LOG (INFO) << "Initing RpcMgr with TLS";
    bld.set_epki_cert_key_files(FLAGS_ssl_server_certificate, FLAGS_ssl_private_key);
    bld.set_epki_certificate_authority_file(FLAGS_ssl_client_ca_certificate);
    bld.set_epki_private_password_key_cmd(FLAGS_ssl_private_key_password_cmd);
    if (!FLAGS_ssl_cipher_list.empty()) {
      bld.set_rpc_tls_ciphers(FLAGS_ssl_cipher_list);
    }
    bld.set_rpc_tls_min_protocol(FLAGS_ssl_minimum_version);
    bld.set_rpc_encryption("required");
    bld.enable_inbound_tls();
  }

  KUDU_RETURN_IF_ERROR(bld.Build(&messenger_), "Could not build messenger");
  return Status::OK();
}

Status RpcMgr::RegisterService(int32_t num_service_threads, int32_t service_queue_depth,
    GeneratedServiceIf* service_ptr, MemTracker* service_mem_tracker) {
  DCHECK(is_inited()) << "Must call Init() before RegisterService()";
  DCHECK(!services_started_) << "Cannot call RegisterService() after StartServices()";
  scoped_refptr<ImpalaServicePool> service_pool = new ImpalaServicePool(
      messenger_->metric_entity(), service_queue_depth, service_ptr, service_mem_tracker);
  // Start the thread pool first before registering the service in case the startup fails.
  RETURN_IF_ERROR(service_pool->Init(num_service_threads));
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
  KUDU_RETURN_IF_ERROR(messenger_->AddAcceptorPool(sockaddr, &acceptor_pool_),
      "Failed to add acceptor pool");
  KUDU_RETURN_IF_ERROR(acceptor_pool_->Start(FLAGS_num_acceptor_threads),
      "Acceptor pool failed to start");
  VLOG_QUERY << "Started " << FLAGS_num_acceptor_threads << " acceptor threads";
  services_started_ = true;
  return Status::OK();
}

void RpcMgr::Shutdown() {
  if (messenger_.get() == nullptr) return;
  for (auto service_pool : service_pools_) service_pool->Shutdown();
  acceptor_pool_.reset();

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

void RpcMgr::ToJson(Document* document) {
  if (messenger_.get() == nullptr) return;
  // Add acceptor metrics.
  int64_t num_accepted = 0;
  if (acceptor_pool_.get() != nullptr) {
    num_accepted = acceptor_pool_->num_rpc_connections_accepted();
  }
  document->AddMember("rpc_connections_accepted", num_accepted, document->GetAllocator());

  // Add messenger metrics.
  DumpRunningRpcsResponsePB response;
  messenger_->DumpRunningRpcs(DumpRunningRpcsRequestPB(), &response);

  int64_t num_inbound_calls_in_flight = 0;
  for (const RpcConnectionPB& conn : response.inbound_connections()) {
    num_inbound_calls_in_flight += conn.calls_in_flight().size();
  }
  document->AddMember("num_inbound_calls_in_flight", num_inbound_calls_in_flight,
      document->GetAllocator());

  // Add per connection metrics.
  Value per_conn_metrics(kArrayType);
  int64_t num_outbound_calls_in_flight = 0;
  for (const RpcConnectionPB& conn : response.outbound_connections()) {
    num_outbound_calls_in_flight += conn.calls_in_flight().size();

    // Add per connection metrics to an array.
    Value per_conn_metrics_entry(kObjectType);
    Value remote_ip_str(conn.remote_ip().c_str(), document->GetAllocator());
    per_conn_metrics_entry.AddMember(
        "remote_ip", remote_ip_str, document->GetAllocator());
    per_conn_metrics_entry.AddMember(
        "outbound_queue_size", conn.outbound_queue_size(), document->GetAllocator());
    per_conn_metrics.PushBack(per_conn_metrics_entry, document->GetAllocator());
  }
  document->AddMember("per_conn_metrics", per_conn_metrics, document->GetAllocator());
  document->AddMember("num_outbound_calls_in_flight", num_outbound_calls_in_flight,
      document->GetAllocator());

  // Add service pool metrics
  Value services(kArrayType);
  for (auto service_pool : service_pools_) {
    Value service_entry(kObjectType);
    service_pool->ToJson(&service_entry, document);
    services.PushBack(service_entry, document->GetAllocator());
  }
  document->AddMember("services", services, document->GetAllocator());
}

} // namespace impala
