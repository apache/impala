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

using kudu::rpc::ErrorStatusPB;
using kudu::rpc::ServiceIf;
using kudu::rpc::RpcController;
using kudu::rpc::RpcContext;
using kudu::rpc::RpcSidecar;
using kudu::MonoDelta;
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

static int kdc_port = GetServerPort();

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

 protected:
  TNetworkAddress krpc_address_;
  RpcMgr rpc_mgr_;

  virtual void SetUp() {
    IpAddr ip;
    ASSERT_OK(HostnameToIpAddr(FLAGS_hostname, &ip));
    krpc_address_ = MakeNetworkAddress(ip, SERVICE_PORT);
    ASSERT_OK(rpc_mgr_.Init());
  }

  virtual void TearDown() {
    rpc_mgr_.Shutdown();
  }

 private:
  int32_t payload_[PAYLOAD_SIZE];
};

// For tests that do not require kerberized testing, we use RpcTest.
class RpcMgrTest : public RpcMgrTestBase<testing::Test> {
  virtual void SetUp() {
    RpcMgrTestBase::SetUp();
  }

  virtual void TearDown() {
    RpcMgrTestBase::TearDown();
  }
};

class RpcMgrKerberizedTest :
    public RpcMgrTestBase<testing::TestWithParam<KerberosSwitch> > {
  virtual void SetUp() {
    IpAddr ip;
    ASSERT_OK(HostnameToIpAddr(FLAGS_hostname, &ip));
    string spn = Substitute("impala-test/$0", ip);

    kdc_wrapper_.reset(new MiniKdcWrapper(
        std::move(spn), "KRBTEST.COM", "24h", "7d", kdc_port));
    DCHECK(kdc_wrapper_.get() != nullptr);

    ASSERT_OK(kdc_wrapper_->SetupAndStartMiniKDC(GetParam()));
    ASSERT_OK(InitAuth(CURRENT_EXECUTABLE_PATH));

    RpcMgrTestBase::SetUp();
  }

  virtual void TearDown() {
    ASSERT_OK(kdc_wrapper_->TearDownMiniKDC(GetParam()));
    RpcMgrTestBase::TearDown();
  }

 private:
  boost::scoped_ptr<MiniKdcWrapper> kdc_wrapper_;
};

typedef std::function<void(RpcContext*)> ServiceCB;

class PingServiceImpl : public PingServiceIf {
 public:
  // 'cb' is a callback used by tests to inject custom behaviour into the RPC handler.
  PingServiceImpl(const scoped_refptr<kudu::MetricEntity>& entity,
      const scoped_refptr<kudu::rpc::ResultTracker> tracker,
      ServiceCB cb = [](RpcContext* ctx) { ctx->RespondSuccess(); })
    : PingServiceIf(entity, tracker), cb_(cb) {}

  virtual void Ping(
      const PingRequestPB* request, PingResponsePB* response, RpcContext* context) {
    response->set_int_response(42);
    cb_(context);
  }

 private:
  ServiceCB cb_;
};

class ScanMemServiceImpl : public ScanMemServiceIf {
 public:
  ScanMemServiceImpl(const scoped_refptr<kudu::MetricEntity>& entity,
      const scoped_refptr<kudu::rpc::ResultTracker> tracker)
    : ScanMemServiceIf(entity, tracker) {
  }

  // The request comes with an int 'pattern' and a payload of int array sent with
  // sidecar. Scan the array to make sure every element matches 'pattern'.
  virtual void ScanMem(const ScanMemRequestPB* request, ScanMemResponsePB* response,
      RpcContext* context) {
    int32_t pattern = request->pattern();
    Slice payload;
    ASSERT_OK(
        FromKuduStatus(context->GetInboundSidecar(request->sidecar_idx(), &payload)));
    ASSERT_EQ(payload.size() % sizeof(int32_t), 0);

    const int32_t* v = reinterpret_cast<const int32_t*>(payload.data());
    for (int i = 0; i < payload.size() / sizeof(int32_t); ++i) {
      int32_t val = v[i];
      if (val != pattern) {
        context->RespondFailure(kudu::Status::Corruption(
            Substitute("Expecting $1; Found $2", pattern, val)));
        return;
      }
    }
    context->RespondSuccess();
  }
};

// TODO: USE_KUDU_KERBEROS and USE_IMPALA_KERBEROS are disabled due to IMPALA-6448.
// Re-enable after fixing.
INSTANTIATE_TEST_CASE_P(KerberosOnAndOff,
                        RpcMgrKerberizedTest,
                        ::testing::Values(KERBEROS_OFF));

template <class T>
Status RunMultipleServicesTestTemplate(RpcMgrTestBase<T>* test_base,
    RpcMgr* rpc_mgr, const TNetworkAddress& krpc_address) {
  // Test that a service can be started, and will respond to requests.
  unique_ptr<ServiceIf> ping_impl(
      new PingServiceImpl(rpc_mgr->metric_entity(), rpc_mgr->result_tracker()));
  RETURN_IF_ERROR(rpc_mgr->RegisterService(10, 10, move(ping_impl)));

  // Test that a second service, that verifies the RPC payload is not corrupted,
  // can be started.
  unique_ptr<ServiceIf> scan_mem_impl(
      new ScanMemServiceImpl(rpc_mgr->metric_entity(), rpc_mgr->result_tracker()));
  RETURN_IF_ERROR(rpc_mgr->RegisterService(10, 10, move(scan_mem_impl)));

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


TEST_F(RpcMgrTest, MultipleServices) {
  ASSERT_OK(RunMultipleServicesTestTemplate(this, &rpc_mgr_, krpc_address_));
}

TEST_P(RpcMgrKerberizedTest, MultipleServicesTls) {
  // TODO: We're starting a seperate RpcMgr here instead of configuring
  // RpcTestBase::rpc_mgr_ to use TLS. To use RpcTestBase::rpc_mgr_, we need to introduce
  // new gtest params to turn on TLS which needs to be a coordinated change across
  // rpc-mgr-test and thrift-server-test.
  RpcMgr tls_rpc_mgr(IsInternalTlsConfigured());
  TNetworkAddress tls_krpc_address;
  IpAddr ip;
  ASSERT_OK(HostnameToIpAddr(FLAGS_hostname, &ip));

  int32_t tls_service_port = FindUnusedEphemeralPort(nullptr);
  tls_krpc_address = MakeNetworkAddress(ip, tls_service_port);

  // Enable TLS.
  auto cert_flag =
      ScopedFlagSetter<string>::Make(&FLAGS_ssl_server_certificate, SERVER_CERT);
  auto pkey_flag =
      ScopedFlagSetter<string>::Make(&FLAGS_ssl_private_key, PRIVATE_KEY);
  auto ca_flag =
      ScopedFlagSetter<string>::Make(&FLAGS_ssl_client_ca_certificate, SERVER_CERT);
  ASSERT_OK(tls_rpc_mgr.Init());

  ASSERT_OK(RunMultipleServicesTestTemplate(this, &tls_rpc_mgr, tls_krpc_address));
  tls_rpc_mgr.Shutdown();
}

// Test with a misconfigured TLS certificate and verify that an error is thrown.
TEST_F(RpcMgrTest, BadCertificateTls) {

  auto cert_flag =
      ScopedFlagSetter<string>::Make(&FLAGS_ssl_server_certificate, SERVER_CERT);
  auto pkey_flag =
      ScopedFlagSetter<string>::Make(&FLAGS_ssl_private_key, PRIVATE_KEY);
  auto ca_flag =
      ScopedFlagSetter<string>::Make(&FLAGS_ssl_client_ca_certificate, "unknown");

  RpcMgr tls_rpc_mgr(IsInternalTlsConfigured());
  TNetworkAddress tls_krpc_address;
  IpAddr ip;
  ASSERT_OK(HostnameToIpAddr(FLAGS_hostname, &ip));

  int32_t tls_service_port = FindUnusedEphemeralPort(nullptr);
  tls_krpc_address = MakeNetworkAddress(ip, tls_service_port);

  ASSERT_FALSE(tls_rpc_mgr.Init().ok());
  tls_rpc_mgr.Shutdown();
}

// Test with a bad password command for the password protected private key.
TEST_F(RpcMgrTest, BadPasswordTls) {
  auto cert_flag =
      ScopedFlagSetter<string>::Make(&FLAGS_ssl_server_certificate, SERVER_CERT);
  auto pkey_flag =
      ScopedFlagSetter<string>::Make(
          &FLAGS_ssl_private_key, PASSWORD_PROTECTED_PRIVATE_KEY);
  auto ca_flag =
      ScopedFlagSetter<string>::Make(&FLAGS_ssl_client_ca_certificate, SERVER_CERT);
  auto password_cmd =
      ScopedFlagSetter<string>::Make(
          &FLAGS_ssl_private_key_password_cmd, "echo badpassword");

  RpcMgr tls_rpc_mgr(IsInternalTlsConfigured());
  TNetworkAddress tls_krpc_address;
  IpAddr ip;
  ASSERT_OK(HostnameToIpAddr(FLAGS_hostname, &ip));

  int32_t tls_service_port = FindUnusedEphemeralPort(nullptr);
  tls_krpc_address = MakeNetworkAddress(ip, tls_service_port);

  ASSERT_FALSE(tls_rpc_mgr.Init().ok());
  tls_rpc_mgr.Shutdown();
}

// Test with a correct password command for the password protected private key.
TEST_F(RpcMgrTest, CorrectPasswordTls) {
  auto cert_flag =
      ScopedFlagSetter<string>::Make(&FLAGS_ssl_server_certificate, SERVER_CERT);
  auto pkey_flag =
      ScopedFlagSetter<string>::Make(
          &FLAGS_ssl_private_key, PASSWORD_PROTECTED_PRIVATE_KEY);
  auto ca_flag =
      ScopedFlagSetter<string>::Make(&FLAGS_ssl_client_ca_certificate, SERVER_CERT);
  auto password_cmd =
      ScopedFlagSetter<string>::Make(
          &FLAGS_ssl_private_key_password_cmd, "echo password");

  RpcMgr tls_rpc_mgr(IsInternalTlsConfigured());
  TNetworkAddress tls_krpc_address;
  IpAddr ip;
  ASSERT_OK(HostnameToIpAddr(FLAGS_hostname, &ip));

  int32_t tls_service_port = FindUnusedEphemeralPort(nullptr);
  tls_krpc_address = MakeNetworkAddress(ip, tls_service_port);

  ASSERT_OK(tls_rpc_mgr.Init());
  ASSERT_OK(RunMultipleServicesTestTemplate(this, &tls_rpc_mgr, tls_krpc_address));
  tls_rpc_mgr.Shutdown();
}

// Test with a bad TLS cipher and verify that an error is thrown.
TEST_F(RpcMgrTest, BadCiphersTls) {
  auto cert_flag =
      ScopedFlagSetter<string>::Make(&FLAGS_ssl_server_certificate, SERVER_CERT);
  auto pkey_flag =
      ScopedFlagSetter<string>::Make(&FLAGS_ssl_private_key, PRIVATE_KEY);
  auto ca_flag =
      ScopedFlagSetter<string>::Make(&FLAGS_ssl_client_ca_certificate, SERVER_CERT);
  auto cipher =
      ScopedFlagSetter<string>::Make(&FLAGS_ssl_cipher_list, "not_a_cipher");

  RpcMgr tls_rpc_mgr(IsInternalTlsConfigured());
  TNetworkAddress tls_krpc_address;
  IpAddr ip;
  ASSERT_OK(HostnameToIpAddr(FLAGS_hostname, &ip));

  int32_t tls_service_port = FindUnusedEphemeralPort(nullptr);
  tls_krpc_address = MakeNetworkAddress(ip, tls_service_port);

  ASSERT_FALSE(tls_rpc_mgr.Init().ok());
  tls_rpc_mgr.Shutdown();
}

// Test with a valid TLS cipher.
TEST_F(RpcMgrTest, ValidCiphersTls) {
  auto cert_flag =
      ScopedFlagSetter<string>::Make(&FLAGS_ssl_server_certificate, SERVER_CERT);
  auto pkey_flag =
      ScopedFlagSetter<string>::Make(&FLAGS_ssl_private_key, PRIVATE_KEY);
  auto ca_flag =
      ScopedFlagSetter<string>::Make(&FLAGS_ssl_client_ca_certificate, SERVER_CERT);
  auto cipher =
      ScopedFlagSetter<string>::Make(&FLAGS_ssl_cipher_list, TLS1_0_COMPATIBLE_CIPHER);

  RpcMgr tls_rpc_mgr(IsInternalTlsConfigured());
  TNetworkAddress tls_krpc_address;
  IpAddr ip;
  ASSERT_OK(HostnameToIpAddr(FLAGS_hostname, &ip));

  int32_t tls_service_port = FindUnusedEphemeralPort(nullptr);
  tls_krpc_address = MakeNetworkAddress(ip, tls_service_port);

  ASSERT_OK(tls_rpc_mgr.Init());
  ASSERT_OK(RunMultipleServicesTestTemplate(this, &tls_rpc_mgr, tls_krpc_address));
  tls_rpc_mgr.Shutdown();
}

// Test with multiple valid TLS ciphers.
TEST_F(RpcMgrTest, ValidMultiCiphersTls) {
  const string cipher_list = Substitute("$0,$1", TLS1_0_COMPATIBLE_CIPHER,
      TLS1_0_COMPATIBLE_CIPHER_2);
  auto cert_flag =
      ScopedFlagSetter<string>::Make(&FLAGS_ssl_server_certificate, SERVER_CERT);
  auto pkey_flag =
      ScopedFlagSetter<string>::Make(&FLAGS_ssl_private_key, PRIVATE_KEY);
  auto ca_flag =
      ScopedFlagSetter<string>::Make(&FLAGS_ssl_client_ca_certificate, SERVER_CERT);
  auto cipher =
      ScopedFlagSetter<string>::Make(&FLAGS_ssl_cipher_list, cipher_list);

  RpcMgr tls_rpc_mgr(IsInternalTlsConfigured());
  TNetworkAddress tls_krpc_address;
  IpAddr ip;
  ASSERT_OK(HostnameToIpAddr(FLAGS_hostname, &ip));

  int32_t tls_service_port = FindUnusedEphemeralPort(nullptr);
  tls_krpc_address = MakeNetworkAddress(ip, tls_service_port);

  ASSERT_OK(tls_rpc_mgr.Init());
  ASSERT_OK(RunMultipleServicesTestTemplate(this, &tls_rpc_mgr, tls_krpc_address));
  tls_rpc_mgr.Shutdown();
}

TEST_F(RpcMgrTest, SlowCallback) {

  // Use a callback which is slow to respond.
  auto slow_cb = [](RpcContext* ctx) {
    SleepForMs(300);
    ctx->RespondSuccess();
  };

  // Test a service which is slow to respond and has a short queue.
  // Set a timeout on the client side. Expect either a client timeout
  // or the service queue filling up.
  unique_ptr<ServiceIf> impl(
      new PingServiceImpl(rpc_mgr_.metric_entity(), rpc_mgr_.result_tracker(), slow_cb));
  const int num_service_threads = 1;
  const int queue_size = 3;
  ASSERT_OK(rpc_mgr_.RegisterService(num_service_threads, queue_size, move(impl)));

  FLAGS_num_acceptor_threads = 2;
  FLAGS_num_reactor_threads = 10;
  ASSERT_OK(rpc_mgr_.StartServices(krpc_address_));

  unique_ptr<PingServiceProxy> proxy;
  ASSERT_OK(rpc_mgr_.GetProxy<PingServiceProxy>(krpc_address_, &proxy));

  PingRequestPB request;
  PingResponsePB response;
  RpcController controller;
  for (int i = 0; i < 100; ++i) {
    controller.Reset();
    controller.set_timeout(MonoDelta::FromMilliseconds(50));
    kudu::Status status = proxy->Ping(request, &response, &controller);
    ASSERT_TRUE(status.IsTimedOut() || RpcMgr::IsServerTooBusy(controller));
  }
}

TEST_F(RpcMgrTest, AsyncCall) {
  unique_ptr<ServiceIf> scan_mem_impl(
      new ScanMemServiceImpl(rpc_mgr_.metric_entity(), rpc_mgr_.result_tracker()));
  ASSERT_OK(rpc_mgr_.RegisterService(10, 10, move(scan_mem_impl)));

  unique_ptr<ScanMemServiceProxy> scan_mem_proxy;
  ASSERT_OK(rpc_mgr_.GetProxy<ScanMemServiceProxy>(krpc_address_, &scan_mem_proxy));

  FLAGS_num_acceptor_threads = 2;
  FLAGS_num_reactor_threads = 10;
  ASSERT_OK(rpc_mgr_.StartServices(krpc_address_));

  RpcController controller;
  srand(0);
  for (int i = 0; i < 10; ++i) {
    controller.Reset();
    ScanMemRequestPB request;
    ScanMemResponsePB response;
    SetupScanMemRequest(&request, &controller);
    CountingBarrier barrier(1);
    scan_mem_proxy->ScanMemAsync(request, &response, &controller,
        [barrier_ptr = &barrier]() { barrier_ptr->Notify(); });
    // TODO: Inject random cancellation here.
    barrier.Wait();
    ASSERT_TRUE(controller.status().ok()) << controller.status().ToString();
  }
}

} // namespace impala

int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);
  impala::InitCommonRuntime(argc, argv, false, impala::TestInfo::BE_TEST);

  // Fill in the path of the current binary for use by the tests.
  CURRENT_EXECUTABLE_PATH = argv[0];
  return RUN_ALL_TESTS();
}
