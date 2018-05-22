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

#include "exec/kudu-util.h"
#include "rpc/auth-provider.h"
#include "service/fe-support.h"
#include "testutil/mini-kdc-wrapper.h"

DECLARE_string(be_principal);
DECLARE_string(hostname);
DECLARE_string(keytab_file);
DECLARE_string(krb5_ccname);
DECLARE_string(principal);
DECLARE_string(ssl_client_ca_certificate);
DECLARE_string(ssl_server_certificate);
DECLARE_string(ssl_private_key);

// The principal name and the realm used for creating the mini-KDC.
// To be initialized at main().
static string kdc_ccname;
static string principal;
static string principal_kt_path;
static string realm;

namespace impala {

class RpcMgrKerberizedTest : public RpcMgrTest {
  virtual void SetUp() override {
    FLAGS_principal = "dummy/host@realm";
    FLAGS_be_principal = strings::Substitute("$0@$1", principal, realm);
    FLAGS_keytab_file = principal_kt_path;
    FLAGS_krb5_ccname = "/tmp/krb5cc_impala_internal";
    ASSERT_OK(InitAuth(CURRENT_EXECUTABLE_PATH));
    RpcMgrTest::SetUp();
  }

  virtual void TearDown() override {
    FLAGS_principal.clear();
    FLAGS_be_principal.clear();
    FLAGS_keytab_file.clear();
    FLAGS_krb5_ccname.clear();
    RpcMgrTest::TearDown();
  }
};

TEST_F(RpcMgrKerberizedTest, MultipleServicesTls) {
  // TODO: We're starting a seperate RpcMgr here instead of configuring
  // RpcTestBase::rpc_mgr_ to use TLS. To use RpcTestBase::rpc_mgr_, we need to introduce
  // new gtest params to turn on TLS which needs to be a coordinated change across
  // rpc-mgr-test and thrift-server-test.
  RpcMgr tls_rpc_mgr(IsInternalTlsConfigured());
  TNetworkAddress tls_krpc_address;
  IpAddr ip;
  ASSERT_OK(HostnameToIpAddr(FLAGS_hostname, &ip));

  int32_t tls_service_port = FindUnusedEphemeralPort();
  tls_krpc_address = MakeNetworkAddress(ip, tls_service_port);

  // Enable TLS.
  ScopedSetTlsFlags s(SERVER_CERT, PRIVATE_KEY, SERVER_CERT);
  ASSERT_OK(tls_rpc_mgr.Init());

  ASSERT_OK(RunMultipleServicesTest(&tls_rpc_mgr, tls_krpc_address));
  tls_rpc_mgr.Shutdown();
}

// This test aims to exercise the authorization function in RpcMgr by accessing
// services with a principal different from FLAGS_be_principal.
TEST_F(RpcMgrKerberizedTest, AuthorizationFail) {
  GeneratedServiceIf* ping_impl =
      TakeOverService(make_unique<PingServiceImpl>(&rpc_mgr_));
  GeneratedServiceIf* scan_mem_impl =
      TakeOverService(make_unique<ScanMemServiceImpl>(&rpc_mgr_));
  const int num_service_threads = 10;
  const int queue_size = 10;
  ASSERT_OK(rpc_mgr_.RegisterService(num_service_threads, queue_size, ping_impl,
      static_cast<PingServiceImpl*>(ping_impl)->mem_tracker()));
  ASSERT_OK(rpc_mgr_.RegisterService(num_service_threads, queue_size, scan_mem_impl,
      static_cast<ScanMemServiceImpl*>(scan_mem_impl)->mem_tracker()));
  FLAGS_num_acceptor_threads = 2;
  FLAGS_num_reactor_threads = 10;
  ASSERT_OK(rpc_mgr_.StartServices(krpc_address_));

  // Switch over to a credentials cache which only contains the dummy credential.
  // Kinit done in InitAuth() uses a different credentials cache.
  DCHECK_NE(FLAGS_krb5_ccname, kdc_ccname);
  discard_result(setenv("KRB5CCNAME", kdc_ccname.c_str(), 1));

  RpcController controller;
  Status rpc_status;

  // ScanMemService's authorization function always returns true so we should be able
  // to access with dummy credentials.
  unique_ptr<ScanMemServiceProxy> scan_proxy;
  ASSERT_OK(static_cast<ScanMemServiceImpl*>(scan_mem_impl)->GetProxy(krpc_address_,
      FLAGS_hostname, &scan_proxy));
  ScanMemRequestPB scan_request;
  ScanMemResponsePB scan_response;
  SetupScanMemRequest(&scan_request, &controller);
  controller.Reset();
  rpc_status =
      FromKuduStatus(scan_proxy->ScanMem(scan_request, &scan_response, &controller));
  EXPECT_TRUE(rpc_status.ok());

  // Fail to access PingService as it's expecting FLAGS_be_principal as principal name.
  unique_ptr<PingServiceProxy> ping_proxy;
  ASSERT_OK(static_cast<PingServiceImpl*>(ping_impl)->GetProxy(krpc_address_,
      FLAGS_hostname, &ping_proxy));
  PingRequestPB ping_request;
  PingResponsePB ping_response;
  controller.Reset();
  rpc_status =
      FromKuduStatus(ping_proxy->Ping(ping_request, &ping_response, &controller));
  EXPECT_TRUE(!rpc_status.ok());
  const string& err_string =
      "Not authorized: {username='dummy', principal='dummy/host@KRBTEST.COM'}";
  EXPECT_NE(rpc_status.GetDetail().find(err_string), string::npos);
}

// Test cases in which bad Kerberos credentials cache path is specified.
TEST_F(RpcMgrKerberizedTest, BadCredentialsCachePath) {
  FLAGS_krb5_ccname = "MEMORY:foo";
  Status status = InitAuth(CURRENT_EXECUTABLE_PATH);
  ASSERT_TRUE(!status.ok());
  EXPECT_EQ(status.GetDetail(),
      "Bad --krb5_ccname value: MEMORY:foo is not an absolute file path\n");

  FLAGS_krb5_ccname = "~/foo";
  status = InitAuth(CURRENT_EXECUTABLE_PATH);
  ASSERT_TRUE(!status.ok());
  EXPECT_EQ(status.GetDetail(),
      "Bad --krb5_ccname value: ~/foo is not an absolute file path\n");
}

} // namespace impala

using impala::Status;

int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);
  impala::InitCommonRuntime(argc, argv, true, impala::TestInfo::BE_TEST);
  impala::InitFeSupport();

  // Set up and start KDC.
  impala::IpAddr ip;
  impala::Status status = impala::HostnameToIpAddr(FLAGS_hostname, &ip);
  DCHECK(status.ok());
  principal = Substitute("impala-test/$0", FLAGS_hostname);
  realm = "KRBTEST.COM";

  int port = impala::FindUnusedEphemeralPort();
  std::unique_ptr<impala::MiniKdcWrapper> kdc;
  status = impala::MiniKdcWrapper::SetupAndStartMiniKDC(realm, "24h", "7d", port, &kdc);
  DCHECK(status.ok());

  // Create a valid service principal and the associated keytab used for this test.
  status = kdc->CreateServiceKeytab(principal, &principal_kt_path);
  DCHECK(status.ok());

  // Create a dummy service principal which is not authorized to access PingService.
  const string& dummy_principal = "dummy/host";
  status = kdc->CreateUserPrincipal(dummy_principal);
  DCHECK(status.ok());
  status = kdc->Kinit(dummy_principal);
  DCHECK(status.ok());

  // Get "KRB5CCNAME" set up by mini-kdc. It's the credentials cache which contains
  // the dummy service's key
  kdc_ccname = kdc->GetKrb5CCname();

  // Fill in the path of the current binary for use by the tests.
  CURRENT_EXECUTABLE_PATH = argv[0];
  int retval = RUN_ALL_TESTS();

  // Shutdown KDC.
  status = kdc->TearDownMiniKDC();
  DCHECK(status.ok());

  return retval;

}
