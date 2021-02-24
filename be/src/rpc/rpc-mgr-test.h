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

#ifndef IMPALA_RPC_RPC_MGR_TEST_H
#define IMPALA_RPC_RPC_MGR_TEST_H

#include "common/init.h"
#include "exec/kudu/kudu-util.h"
#include "kudu/rpc/remote_user.h"
#include "kudu/rpc/rpc_context.h"
#include "kudu/rpc/rpc_controller.h"
#include "kudu/rpc/rpc_header.pb.h"
#include "kudu/rpc/rpc_sidecar.h"
#include "kudu/security/security_flags.h"
#include "kudu/util/status.h"
#include "rpc/rpc-mgr.inline.h"
#include "runtime/exec-env.h"
#include "runtime/mem-tracker.h"
#include "testutil/gtest-util.h"
#include "testutil/scoped-flag-setter.h"
#include "util/auth-util.h"
#include "util/network-util.h"
#include "util/openssl-util.h"
#include "util/test-info.h"

#include "gen-cpp/rpc_test.proxy.h"
#include "gen-cpp/rpc_test.service.h"
#include "gen-cpp/rpc_test.pb.h"

#include "common/names.h"

using kudu::rpc::GeneratedServiceIf;
using kudu::rpc::RemoteUser;
using kudu::rpc::RpcController;
using kudu::rpc::RpcContext;
using kudu::rpc::RpcSidecar;
using kudu::Slice;

using namespace std;

DECLARE_bool(rpc_use_unix_domain_socket);
DECLARE_int32(num_reactor_threads);
DECLARE_int32(num_acceptor_threads);
DECLARE_string(hostname);

DECLARE_string(ssl_client_ca_certificate);
DECLARE_string(ssl_server_certificate);
DECLARE_string(ssl_private_key);
DECLARE_string(ssl_private_key_password_cmd);
DECLARE_string(ssl_cipher_list);
DECLARE_string(ssl_minimum_version);
DECLARE_string(tls_ciphersuites);

// The path of the current executable file that is required for passing into the SASL
// library as the 'application name'.
static string CURRENT_EXECUTABLE_PATH;

namespace impala {

const static string IMPALA_HOME(getenv("IMPALA_HOME"));
const string& SERVER_CERT =
    Substitute("$0/be/src/testutil/server-cert.pem", IMPALA_HOME);
const string& PRIVATE_KEY =
    Substitute("$0/be/src/testutil/server-key.pem", IMPALA_HOME);
const string& BAD_SERVER_CERT =
    Substitute("$0/be/src/testutil/bad-cert.pem", IMPALA_HOME);
const string& BAD_PRIVATE_KEY =
    Substitute("$0/be/src/testutil/bad-key.pem", IMPALA_HOME);
const string& PASSWORD_PROTECTED_PRIVATE_KEY =
    Substitute("$0/be/src/testutil/server-key-password.pem", IMPALA_HOME);

/// Use this class to set the appropriate required TLS flags for the duration of the
/// lifetime of the object.
/// It is assumed that the flags always hold empty values by default.
class ScopedSetTlsFlags {
 public:
  ScopedSetTlsFlags(const string& cert, const string& pkey, const string& ca_cert,
      const string& pkey_passwd = "", const string& ciphers = "",
      const string& tls_ciphersuites =
          kudu::security::SecurityDefaults::kDefaultTlsCipherSuites,
      const string& ssl_minimum_version = "tlsv1.2") {
    FLAGS_ssl_server_certificate = cert;
    FLAGS_ssl_private_key = pkey;
    FLAGS_ssl_client_ca_certificate = ca_cert;
    FLAGS_ssl_private_key_password_cmd = pkey_passwd;
    FLAGS_ssl_cipher_list = ciphers;
    FLAGS_tls_ciphersuites = tls_ciphersuites;
    FLAGS_ssl_minimum_version = ssl_minimum_version;
  }

  ~ScopedSetTlsFlags() {
    FLAGS_ssl_server_certificate = "";
    FLAGS_ssl_private_key = "";
    FLAGS_ssl_client_ca_certificate = "";
    FLAGS_ssl_private_key_password_cmd = "";
    FLAGS_ssl_cipher_list = "";
    FLAGS_tls_ciphersuites = kudu::security::SecurityDefaults::kDefaultTlsCipherSuites;
    FLAGS_ssl_minimum_version = "tlsv1.2";
  }
};

// Only use TLSv1.0 compatible ciphers, as tests might run on machines with only TLSv1.0
// support.
const string TLS1_0_COMPATIBLE_CIPHER = "AES128-SHA";
const string TLS1_0_COMPATIBLE_CIPHER_2 = "AES256-SHA";

const string TLS1_3_CIPHERSUITE = "TLS_AES_256_GCM_SHA384";
const string TLS1_3_CIPHERSUITE_2 = "TLS_CHACHA20_POLY1305_SHA256";

#define PAYLOAD_SIZE (4096)

class RpcMgrTest : public testing::TestWithParam<bool> {
 public:
  // Utility function to initialize the parameter for ScanMem RPC.
  // Picks a random value and fills 'payload_' with it. Adds 'payload_' as a sidecar
  // to 'controller'. Also sets up 'request' with the random value and index of the
  // sidecar.
  void SetupScanMemRequest(ScanMemRequestPB* request, RpcController* controller) {
    int32_t pattern = random();
    for (int i = 0; i < PAYLOAD_SIZE / sizeof(int32_t); ++i) payload_[i] = pattern;
    int idx;
    Slice slice(reinterpret_cast<const uint8_t*>(payload_), PAYLOAD_SIZE);
    controller->AddOutboundSidecar(RpcSidecar::FromSlice(slice), &idx);
    request->set_pattern(pattern);
    request->set_sidecar_idx(idx);
  }

  // Utility function which alternately makes requests to PingService and ScanMemService.
  Status RunMultipleServicesTest(RpcMgr* rpc_mgr, const NetworkAddressPB& krpc_address);

 protected:
  NetworkAddressPB krpc_address_;
  RpcMgr rpc_mgr_;

  virtual void SetUp() {
    FLAGS_rpc_use_unix_domain_socket = GetParam();
    IpAddr ip;
    ASSERT_OK(HostnameToIpAddr(FLAGS_hostname, &ip));
    krpc_address_ = MakeNetworkAddressPB(
        ip, FindUnusedEphemeralPort(), rpc_mgr_.GetUdsAddressUniqueId());
    exec_env_.reset(new ExecEnv());
    ASSERT_OK(rpc_mgr_.Init(krpc_address_));
  }

  virtual void TearDown() {
    rpc_mgr_.Shutdown();
  }

  // Takes over ownership of the newly created 'service' which needs to have a lifetime
  // as long as 'rpc_mgr_' as RpcMgr::Shutdown() will call Shutdown() of 'service'.
  GeneratedServiceIf* TakeOverService(std::unique_ptr<GeneratedServiceIf> service) {
    services_.emplace_back(move(service));
    return services_.back().get();
  }

 private:
  int32_t payload_[PAYLOAD_SIZE];

  // Own all the services used by the test.
  std::vector<std::unique_ptr<GeneratedServiceIf>> services_;

  // Required to set up the RPC metric groups used by the service pool.
  std::unique_ptr<ExecEnv> exec_env_;
};

typedef std::function<void(RpcContext*)> ServiceCB;

class PingServiceImpl : public PingServiceIf {
 public:
  // 'cb' is a callback used by tests to inject custom behaviour into the RPC handler.
  PingServiceImpl(RpcMgr* rpc_mgr,
      ServiceCB cb = [](RpcContext* ctx) { ctx->RespondSuccess(); })
    : PingServiceIf(rpc_mgr->metric_entity(), rpc_mgr->result_tracker()),
      rpc_mgr_(rpc_mgr),
      mem_tracker_(-1, "Ping Service"),
      cb_(cb) {}

  Status GetProxy(const NetworkAddressPB& address, const std::string& hostname,
      std::unique_ptr<PingServiceProxy>* proxy) {
    return rpc_mgr_->GetProxy(address, hostname, proxy);
  }

  virtual bool Authorize(const google::protobuf::Message* req,
      google::protobuf::Message* resp, RpcContext* context) override {
    if (!IsKerberosEnabled()) {
      const RemoteUser& remote_user = context->remote_user();
      if (remote_user.username() != "impala") {
        mem_tracker_.Release(context->GetTransferSize());
        context->RespondFailure(kudu::Status::NotAuthorized(
            Substitute("$0 is not allowed to access PingService",
                remote_user.ToString())));
        return false;
      }
      return true;
    } else {
      return rpc_mgr_->Authorize("PingService", context, mem_tracker());
    }
  }

  virtual void Ping(const PingRequestPB* request, PingResponsePB* response, RpcContext*
      context) override {
    response->set_int_response(42);
    // Incoming requests will already be tracked and we need to release the memory.
    mem_tracker_.Release(context->GetTransferSize());
    cb_(context);
  }

  MemTracker* mem_tracker() { return &mem_tracker_; }

 private:
  RpcMgr* rpc_mgr_;
  MemTracker mem_tracker_;
  ServiceCB cb_;
};

class ScanMemServiceImpl : public ScanMemServiceIf {
 public:
  ScanMemServiceImpl(RpcMgr* rpc_mgr)
    : ScanMemServiceIf(rpc_mgr->metric_entity(), rpc_mgr->result_tracker()),
      rpc_mgr_(rpc_mgr),
      mem_tracker_(-1, "ScanMem Service") {
  }

  Status GetProxy(const NetworkAddressPB& address, const std::string& hostname,
      std::unique_ptr<ScanMemServiceProxy>* proxy) {
    return rpc_mgr_->GetProxy(address, hostname, proxy);
  }

  // A no-op authorization function.
  virtual bool Authorize(const google::protobuf::Message* req,
      google::protobuf::Message* resp, RpcContext* context) override {
    return true;
  }

  // The request comes with an int 'pattern' and a payload of int array sent with
  // sidecar. Scan the array to make sure every element matches 'pattern'.
  virtual void ScanMem(const ScanMemRequestPB* request, ScanMemResponsePB* response,
      RpcContext* context) override {
    int32_t pattern = request->pattern();
    Slice payload;
    ASSERT_OK(
        FromKuduStatus(context->GetInboundSidecar(request->sidecar_idx(), &payload)));
    ASSERT_EQ(payload.size() % sizeof(int32_t), 0);

    const int32_t* v = reinterpret_cast<const int32_t*>(payload.data());
    for (int i = 0; i < payload.size() / sizeof(int32_t); ++i) {
      int32_t val = v[i];
      if (val != pattern) {
        // Incoming requests will already be tracked and we need to release the memory.
        mem_tracker_.Release(context->GetTransferSize());
        context->RespondFailure(kudu::Status::Corruption(
            Substitute("Expecting $1; Found $2", pattern, val)));
        return;
      }
    }
    // Incoming requests will already be tracked and we need to release the memory.
    mem_tracker_.Release(context->GetTransferSize());
    context->RespondSuccess();
  }

  MemTracker* mem_tracker() { return &mem_tracker_; }

 private:
  RpcMgr* rpc_mgr_;
  MemTracker mem_tracker_;

};

/// A class that behaves like a ::kudu::rpc::Proxy and keeps a count of the number of
/// times it is called. It always fails by returning an IOError.
class FailingPingServiceProxy {
 public:
  kudu::Status Ping(const class PingRequestPB& req, class PingResponsePB* resp,
      ::kudu::rpc::RpcController* controller) {
    ++number_of_calls_;
    return kudu::Status::IOError(
        Substitute("ping failing, number of calls=$0.", number_of_calls_));
  }

  /// Return the number of times Ping has been called.
  int GetNumberOfCalls() const { return number_of_calls_; }

 private:
  /// Number of times Ping has been called.
  int number_of_calls_ = 0;
};

Status RpcMgrTest::RunMultipleServicesTest(
    RpcMgr* rpc_mgr, const NetworkAddressPB& krpc_address) {
  // Test that a service can be started, and will respond to requests.
  GeneratedServiceIf* ping_impl = TakeOverService(
      make_unique<PingServiceImpl>(rpc_mgr));
  RETURN_IF_ERROR(rpc_mgr->RegisterService(10, 10, ping_impl,
      static_cast<PingServiceImpl*>(ping_impl)->mem_tracker(),
      ExecEnv::GetInstance()->rpc_metrics()));

  // Test that a second service, that verifies the RPC payload is not corrupted,
  // can be started.
  GeneratedServiceIf* scan_mem_impl =
      TakeOverService(make_unique<ScanMemServiceImpl>(rpc_mgr));

  RETURN_IF_ERROR(rpc_mgr->RegisterService(10, 10, scan_mem_impl,
      static_cast<ScanMemServiceImpl*>(scan_mem_impl)->mem_tracker(),
      ExecEnv::GetInstance()->rpc_metrics()));

  FLAGS_num_acceptor_threads = 2;
  FLAGS_num_reactor_threads = 10;
  RETURN_IF_ERROR(rpc_mgr->StartServices());

  unique_ptr<PingServiceProxy> ping_proxy;
  RETURN_IF_ERROR(static_cast<PingServiceImpl*>(ping_impl)->GetProxy(krpc_address,
      FLAGS_hostname, &ping_proxy));

  unique_ptr<ScanMemServiceProxy> scan_mem_proxy;
  RETURN_IF_ERROR(static_cast<ScanMemServiceImpl*>(scan_mem_impl)->GetProxy(krpc_address,
      FLAGS_hostname, &scan_mem_proxy));

  RpcController controller;
  srand(0);
  // Randomly invoke either services to make sure a RpcMgr can host multiple
  // services at the same time.
  for (int i = 0; i < 100; ++i) {
    controller.Reset();
    if (random() % 2 == 0) {
      PingRequestPB request;
      PingResponsePB response;
      KUDU_RETURN_IF_ERROR(ping_proxy->Ping(request, &response, &controller),
          "unable to execute Ping() RPC.");
      if (response.int_response() != 42) {
        return Status(
            Substitute("Ping() failed. Incorrect response. Expected: 42; Got: $0",
                response.int_response()));
      }
    } else {
      ScanMemRequestPB request;
      ScanMemResponsePB response;
      SetupScanMemRequest(&request, &controller);
      KUDU_RETURN_IF_ERROR(scan_mem_proxy->ScanMem(request, &response, &controller),
          "unable to execute ScanMem() RPC.");
    }
  }
  return Status::OK();
}

} // namespace impala

#endif
