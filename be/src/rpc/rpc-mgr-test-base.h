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

#include "rpc/rpc-mgr.inline.h"

#include "common/init.h"
#include "exec/kudu-util.h"
#include "kudu/rpc/rpc_context.h"
#include "kudu/rpc/rpc_controller.h"
#include "kudu/rpc/rpc_header.pb.h"
#include "kudu/rpc/rpc_sidecar.h"
#include "kudu/util/monotime.h"
#include "kudu/util/status.h"
#include "rpc/auth-provider.h"
#include "runtime/exec-env.h"
#include "runtime/mem-tracker.h"
#include "testutil/gtest-util.h"
#include "testutil/mini-kdc-wrapper.h"
#include "testutil/scoped-flag-setter.h"
#include "util/counting-barrier.h"
#include "util/network-util.h"
#include "util/openssl-util.h"
#include "util/test-info.h"

#include "gen-cpp/rpc_test.proxy.h"
#include "gen-cpp/rpc_test.service.h"

#include "common/names.h"

using kudu::rpc::GeneratedServiceIf;
using kudu::rpc::RpcController;
using kudu::rpc::RpcContext;
using kudu::rpc::RpcSidecar;
using kudu::Slice;

using namespace std;

DECLARE_int32(num_reactor_threads);
DECLARE_int32(num_acceptor_threads);
DECLARE_string(hostname);

DECLARE_string(ssl_client_ca_certificate);
DECLARE_string(ssl_server_certificate);
DECLARE_string(ssl_private_key);
DECLARE_string(ssl_private_key_password_cmd);
DECLARE_string(ssl_cipher_list);

// The path of the current executable file that is required for passing into the SASL
// library as the 'application name'.
static string CURRENT_EXECUTABLE_PATH;

namespace impala {

static int32_t SERVICE_PORT = FindUnusedEphemeralPort(nullptr);

int GetServerPort() {
  int port = FindUnusedEphemeralPort(nullptr);
  EXPECT_FALSE(port == -1);
  return port;
}

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
      const string& pkey_passwd = "", const string& ciphers = "") {
    FLAGS_ssl_server_certificate = cert;
    FLAGS_ssl_private_key = pkey;
    FLAGS_ssl_client_ca_certificate = ca_cert;
    FLAGS_ssl_private_key_password_cmd = pkey_passwd;
    FLAGS_ssl_cipher_list = ciphers;
  }

  ~ScopedSetTlsFlags() {
    FLAGS_ssl_server_certificate = "";
    FLAGS_ssl_private_key = "";
    FLAGS_ssl_client_ca_certificate = "";
    FLAGS_ssl_private_key_password_cmd = "";
    FLAGS_ssl_cipher_list = "";
  }
};

// Only use TLSv1.0 compatible ciphers, as tests might run on machines with only TLSv1.0
// support.
const string TLS1_0_COMPATIBLE_CIPHER = "RC4-SHA";
const string TLS1_0_COMPATIBLE_CIPHER_2 = "RC4-MD5";

#define PAYLOAD_SIZE (4096)

template <class T> class RpcMgrTestBase : public T {
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

  // Takes over ownership of the newly created 'service' which needs to have a lifetime
  // as long as 'rpc_mgr_' as RpcMgr::Shutdown() will call Shutdown() of 'service'.
  GeneratedServiceIf* TakeOverService(std::unique_ptr<GeneratedServiceIf> service) {
    services_.emplace_back(move(service));
    return services_.back().get();
  }

 protected:
  TNetworkAddress krpc_address_;
  RpcMgr rpc_mgr_;

  virtual void SetUp() override {
    IpAddr ip;
    ASSERT_OK(HostnameToIpAddr(FLAGS_hostname, &ip));
    krpc_address_ = MakeNetworkAddress(ip, SERVICE_PORT);
    exec_env_.reset(new ExecEnv());
    ASSERT_OK(rpc_mgr_.Init());
  }

  virtual void TearDown() override {
    rpc_mgr_.Shutdown();
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
  PingServiceImpl(const scoped_refptr<kudu::MetricEntity>& entity,
      const scoped_refptr<kudu::rpc::ResultTracker> tracker,
      ServiceCB cb = [](RpcContext* ctx) { ctx->RespondSuccess(); })
    : PingServiceIf(entity, tracker), mem_tracker_(-1, "Ping Service"), cb_(cb) {}

  virtual void Ping(const PingRequestPB* request, PingResponsePB* response, RpcContext*
      context) override {
    response->set_int_response(42);
    // Incoming requests will already be tracked and we need to release the memory.
    mem_tracker_.Release(context->GetTransferSize());
    cb_(context);
  }

  MemTracker* mem_tracker() { return &mem_tracker_; }

 private:
  MemTracker mem_tracker_;
  ServiceCB cb_;
};

class ScanMemServiceImpl : public ScanMemServiceIf {
 public:
  ScanMemServiceImpl(const scoped_refptr<kudu::MetricEntity>& entity,
      const scoped_refptr<kudu::rpc::ResultTracker> tracker)
    : ScanMemServiceIf(entity, tracker), mem_tracker_(-1, "ScanMem Service") {
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
  MemTracker mem_tracker_;

};

template <class T>
Status RunMultipleServicesTestTemplate(RpcMgrTestBase<T>* test_base,
    RpcMgr* rpc_mgr, const TNetworkAddress& krpc_address) {
  // Test that a service can be started, and will respond to requests.
  GeneratedServiceIf* ping_impl = test_base->TakeOverService(make_unique<PingServiceImpl>(
      rpc_mgr->metric_entity(), rpc_mgr->result_tracker()));
  RETURN_IF_ERROR(rpc_mgr->RegisterService(10, 10, ping_impl,
      static_cast<PingServiceImpl*>(ping_impl)->mem_tracker()));

  // Test that a second service, that verifies the RPC payload is not corrupted,
  // can be started.
  GeneratedServiceIf* scan_mem_impl = test_base->TakeOverService(
      make_unique<ScanMemServiceImpl>(rpc_mgr->metric_entity(),
      rpc_mgr->result_tracker()));
  RETURN_IF_ERROR(rpc_mgr->RegisterService(10, 10, scan_mem_impl,
      static_cast<ScanMemServiceImpl*>(scan_mem_impl)->mem_tracker()));

  FLAGS_num_acceptor_threads = 2;
  FLAGS_num_reactor_threads = 10;
  RETURN_IF_ERROR(rpc_mgr->StartServices(krpc_address));

  unique_ptr<PingServiceProxy> ping_proxy;
  RETURN_IF_ERROR(rpc_mgr->GetProxy<PingServiceProxy>(krpc_address, &ping_proxy));

  unique_ptr<ScanMemServiceProxy> scan_mem_proxy;
  RETURN_IF_ERROR(rpc_mgr->GetProxy<ScanMemServiceProxy>(krpc_address, &scan_mem_proxy));

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
          return Status(Substitute(
              "Ping() failed. Incorrect response. Expected: 42; Got: $0",
                  response.int_response()));
      }
    } else {
      ScanMemRequestPB request;
      ScanMemResponsePB response;
      test_base->SetupScanMemRequest(&request, &controller);
      KUDU_RETURN_IF_ERROR(scan_mem_proxy->ScanMem(request, &response, &controller),
          "unable to execute ScanMem() RPC.");
    }
  }

  return Status::OK();
}

} // namespace impala
