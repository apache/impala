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

#include <google/protobuf/descriptor.h>
#include <google/protobuf/descriptor.pb.h>
#include <google/protobuf/message.h>

#include "exec/kudu/kudu-util.h"
#include "kudu/rpc/acceptor_pool.h"
#include "kudu/rpc/remote_user.h"
#include "kudu/rpc/rpc_context.h"
#include "kudu/rpc/rpc_controller.h"
#include "kudu/rpc/rpc_introspection.pb.h"
#include "kudu/rpc/service_if.h"
#include "kudu/security/init.h"
#include "kudu/util/monotime.h"
#include "kudu/util/net/net_util.h"
#include "runtime/mem-tracker.h"
#include "util/auth-util.h"
#include "util/cpu-info.h"
#include "util/json-util.h"
#include "util/network-util.h"
#include "util/openssl-util.h"

#include "common/names.h"

using namespace rapidjson;

using kudu::HostPort;
using kudu::MetricEntity;
using kudu::MonoDelta;
using kudu::rpc::AcceptorPool;
using kudu::rpc::DumpConnectionsRequestPB;
using kudu::rpc::DumpConnectionsResponsePB;
using kudu::rpc::GeneratedServiceIf;
using kudu::rpc::MessengerBuilder;
using kudu::rpc::Messenger;
using kudu::rpc::RemoteUser;
using kudu::rpc::RpcCallInProgressPB;
using kudu::rpc::RpcConnectionPB;
using kudu::rpc::RpcContext;
using kudu::rpc::RpcController;
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
DECLARE_string(tls_ciphersuites);
DECLARE_string(ssl_minimum_version);

DECLARE_int64(impala_slow_rpc_threshold_ms);

// Defined in kudu/rpc/rpcz_store.cc
DECLARE_int32(rpc_duration_too_long_ms);

// Defined in kudu/rpc/transfer.cc
DECLARE_int64(rpc_max_message_size);

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
DEFINE_bool(rpc_use_loopback, false,
    "Always use loopback for local connections. This requires binding to all addresses, "
    "not just the KRPC address.");
// Cannot set rpc_use_unix_domain_socket as true when rpc_use_loopback is set as true.
DEFINE_bool(rpc_use_unix_domain_socket, false,
    "Whether the KRPC client and server should use Unix domain socket. If enabled, "
    "each daemon is identified with Unix Domain Socket address in the unique name in "
    "\"Abstract Namespace\", in format @impala-krpc:<BackendId>. The KRPC server bind "
    "to a Unix domain socket. KRPC Client attempt to connect to KRPC server via a Unix "
    "domain socket.");
DEFINE_string(uds_address_unique_id, "ip_address",
    "Specify unique Id for UDS address. It could be \"ip_address\", \"backend_id\", or "
    "\"none\"");

namespace impala {

RpcMgr::RpcMgr(bool use_tls) : use_tls_(use_tls) {
  krpc_use_uds_ = FLAGS_rpc_use_unix_domain_socket;
  if (krpc_use_uds_ && FLAGS_rpc_use_loopback) {
    LOG(WARNING) << "Cannot use Unix Domain Socket when using loopback address.";
    krpc_use_uds_ = false;
  }
  if (krpc_use_uds_) {
    if (strcasecmp("backend_id", FLAGS_uds_address_unique_id.c_str()) == 0) {
      uds_addr_unique_id_ = UdsAddressUniqueIdPB::BACKEND_ID;
    } else if (strcasecmp("none", FLAGS_uds_address_unique_id.c_str()) == 0) {
      uds_addr_unique_id_ = UdsAddressUniqueIdPB::NO_UNIQUE_ID;
    } else if (strcasecmp("ip_address", FLAGS_uds_address_unique_id.c_str()) == 0) {
      uds_addr_unique_id_ = UdsAddressUniqueIdPB::IP_ADDRESS;
    } else {
      LOG(ERROR) << "Invalid unique Id for UDS address.";
      uds_addr_unique_id_ = UdsAddressUniqueIdPB::NO_UNIQUE_ID;
      invalid_uds_config_ = true;
    }
  } else {
    uds_addr_unique_id_ = UdsAddressUniqueIdPB::NO_UNIQUE_ID;
  }
}

Status RpcMgr::Init(const NetworkAddressPB& address) {
  if (krpc_use_uds_ && invalid_uds_config_) return Status("Invalid UDS configuration");
  DCHECK(IsResolvedAddress(address));
  address_ = address;

  // Log any RPCs which take longer than this threshold on the server.
  FLAGS_rpc_duration_too_long_ms = FLAGS_impala_slow_rpc_threshold_ms;

  // IMPALA-4874: Impala requires support for messages up to 2GB. Override KRPC's default
  //              maximum of 50MB.
  // Extra note: FLAGS_rpc_max_message_size is an int64_t, but values larger than INT_MAX
  //             are for testing only and are not supported.
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

  if (IsInternalKerberosEnabled()) {
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
    bld.set_rpc_tls_ciphersuites(FLAGS_tls_ciphersuites);
    // If there are no TLS 1.3 ciphersuites listed, then exclude TLS 1.3, as it
    // cannot function.
    if (FLAGS_tls_ciphersuites.empty()) {
      bld.set_rpc_tls_excluded_protocols({"TLSv1.3"});
    }
    bld.set_rpc_tls_min_protocol(FLAGS_ssl_minimum_version);
    bld.set_rpc_encryption("required");
    bld.enable_inbound_tls();
  }

  KUDU_RETURN_IF_ERROR(bld.Build(&messenger_), "Could not build messenger");
  return Status::OK();
}

Status RpcMgr::RegisterService(int32_t num_service_threads, int32_t service_queue_depth,
    GeneratedServiceIf* service_ptr, MemTracker* service_mem_tracker,
    MetricGroup* rpc_metrics) {
  DCHECK(is_inited()) << "Must call Init() before RegisterService()";
  DCHECK(!services_started_) << "Cannot call RegisterService() after StartServices()";
  scoped_refptr<ImpalaServicePool> service_pool =
      new ImpalaServicePool(messenger_->metric_entity(), service_queue_depth, service_ptr,
          service_mem_tracker, address_, rpc_metrics);
  // Start the thread pool first before registering the service in case the startup fails.
  RETURN_IF_ERROR(service_pool->Init(num_service_threads));
  KUDU_RETURN_IF_ERROR(
      messenger_->RegisterService(service_pool->service_name(), service_pool),
      "Could not register service");

  VLOG_QUERY << "Registered KRPC service: " << service_pool->service_name();
  service_pools_.push_back(std::move(service_pool));

  return Status::OK();
}

bool RpcMgr::Authorize(const string& service_name, RpcContext* context,
    MemTracker* mem_tracker) const {
  // Authorization is enforced iff Kerberos is enabled.
  if (!IsInternalKerberosEnabled()) return true;

  // Check if the mapped username matches that of the kinit'ed principal.
  const RemoteUser& remote_user = context->remote_user();
  const string& logged_in_username =
      kudu::security::GetLoggedInUsernameFromKeytab().value_or("");
  DCHECK(!logged_in_username.empty());
  bool authorized = remote_user.username() == logged_in_username &&
      remote_user.authenticated_by() == RemoteUser::Method::KERBEROS;
  if (UNLIKELY(!authorized)) {
    LOG(ERROR) << Substitute("Rejecting unauthorized access to $0 from $1. Expected "
        "user $2", service_name, context->requestor_string(), logged_in_username);
    mem_tracker->Release(context->GetTransferSize());
    context->RespondFailure(kudu::Status::NotAuthorized(
        Substitute("$0 is not allowed to access $1",
            remote_user.ToString(), service_name)));
    return false;
  }
  return true;
}

Status RpcMgr::StartServices() {
  DCHECK(is_inited()) << "Must call Init() before StartServices()";
  DCHECK(!services_started_) << "May not call StartServices() twice";

  // Convert 'address_' to Kudu's Sockaddr
  Sockaddr sockaddr = Sockaddr::Wildcard();
  if (FLAGS_rpc_use_loopback) {
    // Listen on all addresses, including loopback.
    sockaddr.set_port(address_.port());
    DCHECK(sockaddr.IsWildcard()) << sockaddr.ToString();
  } else {
    // Only listen on the canonical address for KRPC.
    RETURN_IF_ERROR(NetworkAddressPBToSockaddr(address_, krpc_use_uds_, &sockaddr));
    if (krpc_use_uds_) {
      // KRPC server bind to Unix domain socket if krpc_use_uds_ is true.
      LOG(INFO) << "KRPC server bind to Unix domain socket: " << sockaddr.ToString();
    }
  }

  // Call the messenger to create an AcceptorPool for us.
  KUDU_RETURN_IF_ERROR(messenger_->AddAcceptorPool(sockaddr, &acceptor_pool_),
      "Failed to add acceptor pool");
  KUDU_RETURN_IF_ERROR(acceptor_pool_->Start(FLAGS_num_acceptor_threads),
      "Acceptor pool failed to start");
  VLOG_QUERY << "Started " << FLAGS_num_acceptor_threads << " acceptor threads";
  services_started_ = true;
  return Status::OK();
}

void RpcMgr::Join() {
  if (services_started_) {
    if (messenger_.get() == nullptr) return;
    for (const auto& service_pool : service_pools_) service_pool->Join();
  }
}

void RpcMgr::Shutdown() {
  if (messenger_.get() == nullptr) return;
  for (const auto& service_pool : service_pools_) service_pool->Shutdown();
  acceptor_pool_.reset();

  messenger_->UnregisterAllServices();
  messenger_->Shutdown();
  service_pools_.clear();
}

bool RpcMgr::IsServerTooBusy(const RpcController& rpc_controller) {
  const kudu::Status status = rpc_controller.status();
  const kudu::rpc::ErrorStatusPB* err = rpc_controller.error_response();
  return status.IsRemoteError() && err != nullptr && err->has_code()
      && err->code() == kudu::rpc::ErrorStatusPB::ERROR_SERVER_TOO_BUSY;
}

void RpcMgr::ToJson(Document* document) {
  if (messenger_.get() == nullptr) return;
  // Add rpc_use_unix_domain_socket.
  document->AddMember(
      "rpc_use_unix_domain_socket", krpc_use_uds_, document->GetAllocator());
  // Add acceptor metrics.
  int64_t num_accepted = 0;
  if (acceptor_pool_.get() != nullptr) {
    num_accepted = acceptor_pool_->num_rpc_connections_accepted();
  }
  document->AddMember("rpc_connections_accepted", num_accepted, document->GetAllocator());

  // Add messenger metrics.
  DumpConnectionsResponsePB response;
  messenger_->DumpConnections(DumpConnectionsRequestPB(), &response);

  int64_t num_inbound_calls_in_flight = 0;
  // Add per connection metrics for inbound connections.
  Value inbound_per_conn_metrics(kArrayType);
  for (const RpcConnectionPB& conn : response.inbound_connections()) {
    Value per_conn_metrics_entry(kObjectType);
    // Remote addresses for UDS inbound connections are not available.
    Value remote_addr_str(
        (!krpc_use_uds_ ? conn.remote_ip().c_str() : "*"), document->GetAllocator());
    per_conn_metrics_entry.AddMember(
        "remote_addr", remote_addr_str, document->GetAllocator());
    per_conn_metrics_entry.AddMember(
        "num_calls_in_flight", conn.calls_in_flight().size(), document->GetAllocator());
    num_inbound_calls_in_flight += conn.calls_in_flight().size();
    Value socket_stats_entry(kObjectType);
    ProtobufToJson(conn.socket_stats(), document, &socket_stats_entry);
    per_conn_metrics_entry.AddMember(
        "socket_stats", socket_stats_entry, document->GetAllocator());

    Value calls_in_flight(kArrayType);
    for (const RpcCallInProgressPB& call : conn.calls_in_flight()) {
      Value call_in_flight(kObjectType);
      ProtobufToJson(call, document, &call_in_flight);
      calls_in_flight.PushBack(call_in_flight, document->GetAllocator());
    }
    per_conn_metrics_entry.AddMember(
      "calls_in_flight", calls_in_flight, document->GetAllocator());
    inbound_per_conn_metrics.PushBack(per_conn_metrics_entry, document->GetAllocator());
  }
  document->AddMember(
      "inbound_per_conn_metrics", inbound_per_conn_metrics, document->GetAllocator());
  document->AddMember("num_inbound_calls_in_flight", num_inbound_calls_in_flight,
      document->GetAllocator());

  // Add per connection metrics for outbound connections.
  Value outbound_per_conn_metrics(kArrayType);
  int64_t num_outbound_calls_in_flight = 0;
  for (const RpcConnectionPB& conn : response.outbound_connections()) {
    num_outbound_calls_in_flight += conn.calls_in_flight().size();

    // Add per connection metrics to an array.
    Value per_conn_metrics_entry(kObjectType);
    Value remote_addr_str(conn.remote_ip().c_str(), document->GetAllocator());
    per_conn_metrics_entry.AddMember(
        "remote_addr", remote_addr_str, document->GetAllocator());
    per_conn_metrics_entry.AddMember(
        "num_calls_in_flight", conn.calls_in_flight().size(), document->GetAllocator());
    per_conn_metrics_entry.AddMember(
        "outbound_queue_size", conn.outbound_queue_size(), document->GetAllocator());

    Value socket_stats_entry(kObjectType);
    ProtobufToJson(conn.socket_stats(), document, &socket_stats_entry);
    per_conn_metrics_entry.AddMember(
        "socket_stats", socket_stats_entry, document->GetAllocator());

    Value calls_in_flight(kArrayType);
    for (const RpcCallInProgressPB& call : conn.calls_in_flight()) {
      Value call_in_flight(kObjectType);
      ProtobufToJson(call, document, &call_in_flight);
      calls_in_flight.PushBack(call_in_flight, document->GetAllocator());
    }
    per_conn_metrics_entry.AddMember(
        "calls_in_flight", calls_in_flight, document->GetAllocator());
    outbound_per_conn_metrics.PushBack(per_conn_metrics_entry, document->GetAllocator());
  }
  document->AddMember(
      "per_conn_metrics", outbound_per_conn_metrics, document->GetAllocator());
  document->AddMember("num_outbound_calls_in_flight", num_outbound_calls_in_flight,
      document->GetAllocator());

  // Add service pool metrics
  Value services(kArrayType);
  for (const auto& service_pool : service_pools_) {
    Value service_entry(kObjectType);
    service_pool->ToJson(&service_entry, document);
    services.PushBack(service_entry, document->GetAllocator());
  }
  document->AddMember("services", services, document->GetAllocator());
}

} // namespace impala
