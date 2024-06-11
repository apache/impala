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

#include "rpc/rpc-mgr-test.h"

#include "kudu/util/monotime.h"
#include "service/fe-support.h"
#include "testutil/mini-kdc-wrapper.h"
#include "util/counting-barrier.h"

using kudu::rpc::GeneratedServiceIf;
using kudu::rpc::RpcController;
using kudu::rpc::RpcContext;
using kudu::MonoDelta;

DECLARE_int32(num_reactor_threads);
DECLARE_int32(num_acceptor_threads);
DECLARE_int32(rpc_negotiation_timeout_ms);
DECLARE_string(hostname);
DECLARE_string(debug_actions);

// For tests that do not require kerberized testing, we use RpcTest.
namespace impala {

// Test multiple services managed by an Rpc Manager using TLS.
TEST_P(RpcMgrTest, MultipleServicesTls) {
  // TODO: We're starting a separate RpcMgr here instead of configuring
  // RpcTestBase::rpc_mgr_ to use TLS. To use RpcTestBase::rpc_mgr_, we need to introduce
  // new gtest params to turn on TLS which needs to be a coordinated change across
  // rpc-mgr-test and thrift-server-test.
  RpcMgr tls_rpc_mgr(IsInternalTlsConfigured());
  NetworkAddressPB tls_krpc_address;
  IpAddr ip;
  ASSERT_OK(HostnameToIpAddr(FLAGS_hostname, &ip));

  int32_t tls_service_port = FindUnusedEphemeralPort();
  tls_krpc_address =
      MakeNetworkAddressPB(ip, tls_service_port, tls_rpc_mgr.GetUdsAddressUniqueId());

  ScopedSetTlsFlags s(SERVER_CERT, PRIVATE_KEY, SERVER_CERT);
  ASSERT_OK(tls_rpc_mgr.Init(tls_krpc_address));

  ASSERT_OK(RunMultipleServicesTest(&tls_rpc_mgr, tls_krpc_address));
  tls_rpc_mgr.Shutdown();
}

// Test multiple services managed by an Rpc Manager.
TEST_P(RpcMgrTest, MultipleServices) {
  ASSERT_OK(RunMultipleServicesTest(&rpc_mgr_, krpc_address_));
}

// Test with a misconfigured TLS certificate and verify that an error is thrown.
TEST_P(RpcMgrTest, BadCertificateTls) {
  ScopedSetTlsFlags s(SERVER_CERT, PRIVATE_KEY, "unknown");

  RpcMgr tls_rpc_mgr(IsInternalTlsConfigured());
  NetworkAddressPB tls_krpc_address;
  IpAddr ip;
  ASSERT_OK(HostnameToIpAddr(FLAGS_hostname, &ip));

  int32_t tls_service_port = FindUnusedEphemeralPort();
  tls_krpc_address =
      MakeNetworkAddressPB(ip, tls_service_port, tls_rpc_mgr.GetUdsAddressUniqueId());

  ASSERT_FALSE(tls_rpc_mgr.Init(tls_krpc_address).ok());
  tls_rpc_mgr.Shutdown();
}

// Test with a bad password command for the password protected private key.
TEST_P(RpcMgrTest, BadPasswordTls) {
  ScopedSetTlsFlags s(SERVER_CERT, PASSWORD_PROTECTED_PRIVATE_KEY, SERVER_CERT,
      "echo badpassword");

  RpcMgr tls_rpc_mgr(IsInternalTlsConfigured());
  NetworkAddressPB tls_krpc_address;
  IpAddr ip;
  ASSERT_OK(HostnameToIpAddr(FLAGS_hostname, &ip));

  int32_t tls_service_port = FindUnusedEphemeralPort();
  tls_krpc_address =
      MakeNetworkAddressPB(ip, tls_service_port, tls_rpc_mgr.GetUdsAddressUniqueId());

  ASSERT_FALSE(tls_rpc_mgr.Init(tls_krpc_address).ok());
  tls_rpc_mgr.Shutdown();
}

// Test with a correct password command for the password protected private key.
TEST_P(RpcMgrTest, CorrectPasswordTls) {
  ScopedSetTlsFlags s(SERVER_CERT, PASSWORD_PROTECTED_PRIVATE_KEY, SERVER_CERT,
      "echo password");

  RpcMgr tls_rpc_mgr(IsInternalTlsConfigured());
  NetworkAddressPB tls_krpc_address;
  IpAddr ip;
  ASSERT_OK(HostnameToIpAddr(FLAGS_hostname, &ip));

  int32_t tls_service_port = FindUnusedEphemeralPort();
  tls_krpc_address =
      MakeNetworkAddressPB(ip, tls_service_port, tls_rpc_mgr.GetUdsAddressUniqueId());

  ASSERT_OK(tls_rpc_mgr.Init(tls_krpc_address));
  ASSERT_OK(RunMultipleServicesTest(&tls_rpc_mgr, tls_krpc_address));
  tls_rpc_mgr.Shutdown();
}

// Test with a bad TLS cipher and verify that an error is thrown.
TEST_P(RpcMgrTest, BadCiphersTls) {
  ScopedSetTlsFlags s(SERVER_CERT, PRIVATE_KEY, SERVER_CERT, "", "not_a_cipher", "");

  RpcMgr tls_rpc_mgr(IsInternalTlsConfigured());
  NetworkAddressPB tls_krpc_address;
  IpAddr ip;
  ASSERT_OK(HostnameToIpAddr(FLAGS_hostname, &ip));

  int32_t tls_service_port = FindUnusedEphemeralPort();
  tls_krpc_address =
      MakeNetworkAddressPB(ip, tls_service_port, tls_rpc_mgr.GetUdsAddressUniqueId());

  ASSERT_FALSE(tls_rpc_mgr.Init(tls_krpc_address).ok());
  tls_rpc_mgr.Shutdown();
}

// Test with a valid TLS cipher.
TEST_P(RpcMgrTest, ValidCiphersTls) {
  ScopedSetTlsFlags s(SERVER_CERT, PRIVATE_KEY, SERVER_CERT, "",
      TLS1_0_COMPATIBLE_CIPHER, "");

  RpcMgr tls_rpc_mgr(IsInternalTlsConfigured());
  NetworkAddressPB tls_krpc_address;
  IpAddr ip;
  ASSERT_OK(HostnameToIpAddr(FLAGS_hostname, &ip));

  int32_t tls_service_port = FindUnusedEphemeralPort();
  tls_krpc_address =
      MakeNetworkAddressPB(ip, tls_service_port, tls_rpc_mgr.GetUdsAddressUniqueId());

  ASSERT_OK(tls_rpc_mgr.Init(tls_krpc_address));
  ASSERT_OK(RunMultipleServicesTest(&tls_rpc_mgr, tls_krpc_address));
  tls_rpc_mgr.Shutdown();
}

// Test with multiple valid TLS ciphers.
TEST_P(RpcMgrTest, ValidMultiCiphersTls) {
  const string cipher_list = Substitute("$0,$1", TLS1_0_COMPATIBLE_CIPHER,
      TLS1_0_COMPATIBLE_CIPHER_2);
  ScopedSetTlsFlags s(SERVER_CERT, PRIVATE_KEY, SERVER_CERT, "", cipher_list, "");

  RpcMgr tls_rpc_mgr(IsInternalTlsConfigured());
  NetworkAddressPB tls_krpc_address;
  IpAddr ip;
  ASSERT_OK(HostnameToIpAddr(FLAGS_hostname, &ip));

  int32_t tls_service_port = FindUnusedEphemeralPort();
  tls_krpc_address =
      MakeNetworkAddressPB(ip, tls_service_port, tls_rpc_mgr.GetUdsAddressUniqueId());

  ASSERT_OK(tls_rpc_mgr.Init(tls_krpc_address));
  ASSERT_OK(RunMultipleServicesTest(&tls_rpc_mgr, tls_krpc_address));
  tls_rpc_mgr.Shutdown();
}

// TLS 1.3 tests only make sense on OpenSSL 1.1.1 and above
// These tests set ssl_minimum_version="tlsv1.3". This is safe for these tests,
// because KRPC supports a minimum version of tlsv1.3. It is not supported
// in general.
#if OPENSSL_VERSION_NUMBER >= 0x10101000L

// Test with a bad TLS cipher and verify that an error is thrown.
TEST_P(RpcMgrTest, BadTlsCiphersuites) {
  ScopedSetTlsFlags s(SERVER_CERT, PRIVATE_KEY, SERVER_CERT, "", "",
      "not_a_ciphersuite", "tlsv1.3");

  RpcMgr tls_rpc_mgr(IsInternalTlsConfigured());
  NetworkAddressPB tls_krpc_address;
  IpAddr ip;
  ASSERT_OK(HostnameToIpAddr(FLAGS_hostname, &ip));

  int32_t tls_service_port = FindUnusedEphemeralPort();
  tls_krpc_address = MakeNetworkAddressPB(ip, tls_service_port,
      tls_rpc_mgr.GetUdsAddressUniqueId());

  ASSERT_FALSE(tls_rpc_mgr.Init(tls_krpc_address).ok());
  tls_rpc_mgr.Shutdown();
}

// Test with a valid TLS 1.3 ciphersuite.
TEST_P(RpcMgrTest, ValidTlsCiphersuites) {
  ScopedSetTlsFlags s(SERVER_CERT, PRIVATE_KEY, SERVER_CERT, "",
      "", TLS1_3_CIPHERSUITE, "tlsv1.3");

  RpcMgr tls_rpc_mgr(IsInternalTlsConfigured());
  NetworkAddressPB tls_krpc_address;
  IpAddr ip;
  ASSERT_OK(HostnameToIpAddr(FLAGS_hostname, &ip));

  int32_t tls_service_port = FindUnusedEphemeralPort();
  tls_krpc_address = MakeNetworkAddressPB(ip, tls_service_port,
      tls_rpc_mgr.GetUdsAddressUniqueId());

  ASSERT_OK(tls_rpc_mgr.Init(tls_krpc_address));
  ASSERT_OK(RunMultipleServicesTest(&tls_rpc_mgr, tls_krpc_address));
  tls_rpc_mgr.Shutdown();
}

// Test with multiple valid TLS 1.3 ciphersuites
TEST_P(RpcMgrTest, ValidMultiTlsCiphersuite) {
  const string ciphersuite_list = Substitute("$0:$1", TLS1_3_CIPHERSUITE,
      TLS1_3_CIPHERSUITE_2);
  ScopedSetTlsFlags s(SERVER_CERT, PRIVATE_KEY, SERVER_CERT, "", "",
      ciphersuite_list, "tlsv1.3");

  RpcMgr tls_rpc_mgr(IsInternalTlsConfigured());
  NetworkAddressPB tls_krpc_address;
  IpAddr ip;
  ASSERT_OK(HostnameToIpAddr(FLAGS_hostname, &ip));

  int32_t tls_service_port = FindUnusedEphemeralPort();
  tls_krpc_address = MakeNetworkAddressPB(ip, tls_service_port,
      tls_rpc_mgr.GetUdsAddressUniqueId());

  ASSERT_OK(tls_rpc_mgr.Init(tls_krpc_address));
  ASSERT_OK(RunMultipleServicesTest(&tls_rpc_mgr, tls_krpc_address));
  tls_rpc_mgr.Shutdown();
}

#endif // OPENSSL_VERSION_NUMBER

// Test behavior with a slow service.
TEST_P(RpcMgrTest, SlowCallback) {
  // Use a callback which is slow to respond.
  auto slow_cb = [](RpcContext* ctx) {
    SleepForMs(300);
    ctx->RespondSuccess();
  };

  // Test a service which is slow to respond and has a short queue.
  // Set a timeout on the client side. Expect either a client timeout
  // or the service queue filling up.
  GeneratedServiceIf* ping_impl =
      TakeOverService(make_unique<PingServiceImpl>(&rpc_mgr_, slow_cb));
  const int num_service_threads = 1;
  const int queue_size = 3;
  ASSERT_OK(rpc_mgr_.RegisterService(num_service_threads, queue_size, ping_impl,
      static_cast<PingServiceImpl*>(ping_impl)->mem_tracker(),
      ExecEnv::GetInstance()->rpc_metrics()));

  FLAGS_num_acceptor_threads = 2;
  FLAGS_num_reactor_threads = 10;
  ASSERT_OK(rpc_mgr_.StartServices());

  unique_ptr<PingServiceProxy> proxy;
  ASSERT_OK(static_cast<PingServiceImpl*>(ping_impl)->GetProxy(krpc_address_,
      FLAGS_hostname, &proxy));

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

// Test async calls.
TEST_P(RpcMgrTest, AsyncCall) {
  GeneratedServiceIf* scan_mem_impl =
      TakeOverService(make_unique<ScanMemServiceImpl>(&rpc_mgr_));
  ASSERT_OK(rpc_mgr_.RegisterService(10, 10, scan_mem_impl,
      static_cast<ScanMemServiceImpl*>(scan_mem_impl)->mem_tracker(),
      ExecEnv::GetInstance()->rpc_metrics()));

  unique_ptr<ScanMemServiceProxy> scan_mem_proxy;
  ASSERT_OK(static_cast<ScanMemServiceImpl*>(scan_mem_impl)->GetProxy(krpc_address_,
      FLAGS_hostname, &scan_mem_proxy));

  FLAGS_num_acceptor_threads = 2;
  FLAGS_num_reactor_threads = 10;
  ASSERT_OK(rpc_mgr_.StartServices());

  RpcController controller;
  srand(0);
  for (int i = 0; i < 10; ++i) {
    controller.Reset();
    ScanMemRequestPB request;
    ScanMemResponsePB response;
    SetupScanMemRequest(&request, &controller);
    CountingBarrier barrier(1);
    scan_mem_proxy->ScanMemAsync(
        request, &response, &controller, [barrier_ptr = &barrier]() {
          discard_result(barrier_ptr->Notify());
        });
    // TODO: Inject random cancellation here.
    barrier.Wait();
    ASSERT_TRUE(controller.status().ok()) << controller.status().ToString();
  }
}

// Run a test with the negotiation timeout as 0 ms and ensure that connection
// establishment fails.
// This is to verify that FLAGS_rpc_negotiation_timeout_ms is actually effective.
TEST_P(RpcMgrTest, NegotiationTimeout) {
  // Set negotiation timeout to 0 milliseconds.
  auto s = ScopedFlagSetter<int32_t>::Make(&FLAGS_rpc_negotiation_timeout_ms, 0);

  RpcMgr secondary_rpc_mgr(IsInternalTlsConfigured());
  NetworkAddressPB secondary_krpc_address;
  IpAddr ip;
  ASSERT_OK(HostnameToIpAddr(FLAGS_hostname, &ip));

  int32_t secondary_service_port = FindUnusedEphemeralPort();
  secondary_krpc_address = MakeNetworkAddressPB(
      ip, secondary_service_port, secondary_rpc_mgr.GetUdsAddressUniqueId());

  ASSERT_OK(secondary_rpc_mgr.Init(secondary_krpc_address));
  ASSERT_FALSE(RunMultipleServicesTest(&secondary_rpc_mgr, secondary_krpc_address).ok());
  secondary_rpc_mgr.Shutdown();
}

// Test RpcMgr::DoRpcWithRetry using a fake proxy.
TEST_P(RpcMgrTest, DoRpcWithRetry) {
  TQueryCtx query_ctx;
  const int num_retries = 10;
  const int64_t timeout_ms = 10 * MILLIS_PER_SEC;

  // Test how DoRpcWithRetry retries by using a proxy that always fails.
  unique_ptr<FailingPingServiceProxy> failing_proxy =
      make_unique<FailingPingServiceProxy>();
  // A call that fails is not retried as the server is not busy.
  PingRequestPB request1;
  PingResponsePB response1;
  Status rpc_status_fail =
      RpcMgr::DoRpcWithRetry(failing_proxy, &FailingPingServiceProxy::Ping, request1,
          &response1, query_ctx, "ping failed", num_retries, timeout_ms);
  ASSERT_FALSE(rpc_status_fail.ok());
  // Check that proxy was only called once.
  ASSERT_EQ(1, failing_proxy->GetNumberOfCalls());

  // Test injection of DebugAction into DoRpcWithRetry.
  query_ctx.client_request.query_options.__set_debug_action("DoRpcWithRetry:FAIL");
  PingRequestPB request2;
  PingResponsePB response2;
  Status inject_status = RpcMgr::DoRpcWithRetry(failing_proxy,
      &FailingPingServiceProxy::Ping, request2, &response2, query_ctx, "ping failed",
      num_retries, timeout_ms, 0, "DoRpcWithRetry");
  ASSERT_FALSE(inject_status.ok());
  EXPECT_ERROR(inject_status, TErrorCode::INTERNAL_ERROR);
  ASSERT_EQ("Debug Action: DoRpcWithRetry:FAIL", inject_status.msg().msg());
}

// Test RpcMgr::DoRpcWithRetry by injecting service-too-busy failures.
TEST_P(RpcMgrTest, BusyService) {
  TQueryCtx query_ctx;
  auto cb = [](RpcContext* ctx) { ctx->RespondSuccess(); };
  GeneratedServiceIf* ping_impl =
      TakeOverService(make_unique<PingServiceImpl>(&rpc_mgr_, cb));
  const int num_service_threads = 4;
  const int queue_size = 25;
  ASSERT_OK(rpc_mgr_.RegisterService(num_service_threads, queue_size, ping_impl,
      static_cast<PingServiceImpl*>(ping_impl)->mem_tracker(),
      ExecEnv::GetInstance()->rpc_metrics()));
  FLAGS_num_acceptor_threads = 2;
  FLAGS_num_reactor_threads = 10;
  ASSERT_OK(rpc_mgr_.StartServices());

  // Find the counter which tracks the number of times the service queue is too full.
  const string& overflow_count = Substitute(
      ImpalaServicePool::RPC_QUEUE_OVERFLOW_METRIC_KEY, ping_impl->service_name());
  IntCounter* overflow_metric =
      ExecEnv::GetInstance()->rpc_metrics()->FindMetricForTesting<IntCounter>(
          overflow_count);
  ASSERT_TRUE(overflow_metric != nullptr);

  unique_ptr<PingServiceProxy> proxy;
  ASSERT_OK(static_cast<PingServiceImpl*>(ping_impl)->GetProxy(
      krpc_address_, FLAGS_hostname, &proxy));

  // There have been no overflows yet.
  EXPECT_EQ(overflow_metric->GetValue(), 0L);

  // Use DebugAction to make the Impala Service Pool reject 50% of Krpc calls as if the
  // service is too busy.
  auto s = ScopedFlagSetter<string>::Make(&FLAGS_debug_actions,
      Substitute("IMPALA_SERVICE_POOL:$0:$1:Ping:FAIL@0.5@REJECT_TOO_BUSY",
          krpc_address_.hostname(), krpc_address_.port()));
  PingRequestPB request;
  PingResponsePB response;
  const int64_t timeout_ms = 10 * MILLIS_PER_SEC;
  int num_retries = 40; // How many times DoRpcWithRetry can retry.
  int num_rpc_retry_calls = 40; // How many times to call DoRpcWithRetry
  for (int i = 0; i < num_rpc_retry_calls; ++i) {
    Status status = RpcMgr::DoRpcWithRetry(proxy, &PingServiceProxy::Ping, request,
        &response, query_ctx, "ping failed", num_retries, timeout_ms);
    // DoRpcWithRetry will fail with probability (1/2)^num_rpc_retry_calls.
    ASSERT_TRUE(status.ok());
  }
  // There will be no overflows (i.e. service too busy) with probability
  // (1/2)^num_retries.
  ASSERT_GT(overflow_metric->GetValue(), 0);
}

// Run tests with Unix domain socket and TCP socket by setting
// FLAGS_rpc_use_unix_domain_socket as true and false.
INSTANTIATE_TEST_SUITE_P(UdsOnAndOff, RpcMgrTest, ::testing::Values(true, false));

} // namespace impala

int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);
  impala::InitCommonRuntime(argc, argv, true, impala::TestInfo::BE_TEST);
  impala::InitFeSupport();

  // Fill in the path of the current binary for use by the tests.
  CURRENT_EXECUTABLE_PATH = argv[0];
  return RUN_ALL_TESTS();
}
