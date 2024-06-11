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

#include <boost/filesystem/operations.hpp>

#include "exec/kudu/kudu-util.h"
#include "rpc/auth-provider.h"
#include "service/fe-support.h"
#include "testutil/mini-kdc-wrapper.h"
#include "testutil/scoped-flag-setter.h"
#include "util/filesystem-util.h"
#include "util/scope-exit-trigger.h"

DECLARE_bool(skip_internal_kerberos_auth);
DECLARE_bool(skip_external_kerberos_auth);
DECLARE_string(be_principal);
DECLARE_string(hostname);
DECLARE_string(keytab_file);
DECLARE_string(krb5_ccname);
DECLARE_string(krb5_conf);
DECLARE_string(krb5_debug_file);
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

TEST_P(RpcMgrKerberizedTest, MultipleServicesTls) {
  // TODO: We're starting a seperate RpcMgr here instead of configuring
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

  // Enable TLS.
  ScopedSetTlsFlags s(SERVER_CERT, PRIVATE_KEY, SERVER_CERT);
  ASSERT_OK(tls_rpc_mgr.Init(tls_krpc_address));

  ASSERT_OK(RunMultipleServicesTest(&tls_rpc_mgr, tls_krpc_address));
  tls_rpc_mgr.Shutdown();
}

// This test aims to exercise the authorization function in RpcMgr by accessing
// services with a principal different from FLAGS_be_principal.
TEST_P(RpcMgrKerberizedTest, AuthorizationFail) {
  GeneratedServiceIf* ping_impl =
      TakeOverService(make_unique<PingServiceImpl>(&rpc_mgr_));
  GeneratedServiceIf* scan_mem_impl =
      TakeOverService(make_unique<ScanMemServiceImpl>(&rpc_mgr_));
  const int num_service_threads = 10;
  const int queue_size = 10;
  ASSERT_OK(rpc_mgr_.RegisterService(num_service_threads, queue_size, ping_impl,
      static_cast<PingServiceImpl*>(ping_impl)->mem_tracker(),
      ExecEnv::GetInstance()->rpc_metrics()));
  ASSERT_OK(rpc_mgr_.RegisterService(num_service_threads, queue_size, scan_mem_impl,
      static_cast<ScanMemServiceImpl*>(scan_mem_impl)->mem_tracker(),
      ExecEnv::GetInstance()->rpc_metrics()));
  FLAGS_num_acceptor_threads = 2;
  FLAGS_num_reactor_threads = 10;
  ASSERT_OK(rpc_mgr_.StartServices());

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
TEST_P(RpcMgrKerberizedTest, BadCredentialsCachePath) {
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

  FLAGS_krb5_ccname = "";
  status = InitAuth(CURRENT_EXECUTABLE_PATH);
  ASSERT_TRUE(!status.ok());
  EXPECT_EQ(
      status.GetDetail(), "--krb5_ccname must be configured if kerberos is enabled\n");
}

// Test cases in which bad keytab path is specified.
TEST_P(RpcMgrKerberizedTest, BadKeytabPath) {
  FLAGS_keytab_file = "non_existent_file_for_testing";
  Status status = InitAuth(CURRENT_EXECUTABLE_PATH);
  ASSERT_TRUE(!status.ok());
  EXPECT_EQ(status.GetDetail(),
      "Bad --keytab_file value: The file "
      "non_existent_file_for_testing is not a regular file\n");

  FLAGS_keytab_file = "";
  status = InitAuth(CURRENT_EXECUTABLE_PATH);
  ASSERT_TRUE(!status.ok());
  EXPECT_EQ(
      status.GetDetail(), "--keytab_file must be configured if kerberos is enabled\n");
}

// Test that configurations are passed through via env variables even if kerberos
// is disabled for internal auth (i.e. --principal is not set).
TEST_P(RpcMgrKerberizedTest, DisabledKerberosConfigs) {
  // These flags are reset in Setup, so just overwrite them.
  FLAGS_principal = FLAGS_be_principal = "";
  FLAGS_keytab_file = "/tmp/DisabledKerberosConfigsKeytab";
  FLAGS_krb5_ccname = "/tmp/DisabledKerberosConfigsCC";
  // These flags are not reset in Setup, so used ScopedFlagSetter.
  auto k5c = ScopedFlagSetter<string>::Make(
      &FLAGS_krb5_conf, "/tmp/DisabledKerberosConfigsConf");
  auto k5dbg = ScopedFlagSetter<string>::Make(
      &FLAGS_krb5_debug_file, "/tmp/DisabledKerberosConfigsDebug");

  // Unset JAVA_TOOL_OPTIONS before more gets appended to it.
  EXPECT_EQ(0, setenv("JAVA_TOOL_OPTIONS", "", 1));

  // Create dummy files to satisfy validations.
  auto file_cleanup = MakeScopeExitTrigger([&]() {
    boost::filesystem::remove(FLAGS_keytab_file);
    boost::filesystem::remove(FLAGS_krb5_conf);
  });
  EXPECT_OK(FileSystemUtil::CreateFile(FLAGS_keytab_file));
  EXPECT_OK(FileSystemUtil::CreateFile(FLAGS_krb5_conf));

  EXPECT_OK(InitAuth(CURRENT_EXECUTABLE_PATH));

  // Check that the above changes went into the appropriate env variables where
  // they can be picked up by libkrb5 and the JVM Kerberos libraries.
  EXPECT_EQ("/tmp/DisabledKerberosConfigsKeytab", string(getenv("KRB5_KTNAME")));
  EXPECT_EQ("/tmp/DisabledKerberosConfigsCC", string(getenv("KRB5CCNAME")));
  EXPECT_EQ("/tmp/DisabledKerberosConfigsConf", string(getenv("KRB5_CONFIG")));
  EXPECT_EQ("/tmp/DisabledKerberosConfigsDebug", string(getenv("KRB5_TRACE")));
  string jvm_flags = getenv("JAVA_TOOL_OPTIONS");
  EXPECT_STR_CONTAINS(jvm_flags, "-Dsun.security.krb5.debug=true");
  EXPECT_STR_CONTAINS(
      jvm_flags, "-Djava.security.krb5.conf=/tmp/DisabledKerberosConfigsConf");
}

// Test that we kinit even with --skip_internal_kerberos_auth and
// --skip_external_kerberos_auth set. We do this indirectly by checking for
// kinit success/failure.
TEST_P(RpcMgrKerberizedTest, KinitWhenIncomingAuthDisabled) {
  auto ia =
      ScopedFlagSetter<bool>::Make(&FLAGS_skip_internal_kerberos_auth, true);
  auto ea =
      ScopedFlagSetter<bool>::Make(&FLAGS_skip_external_kerberos_auth, true);
  EXPECT_OK(InitAuth(CURRENT_EXECUTABLE_PATH));

  FLAGS_principal = FLAGS_be_principal = "non-existent-principal/host@realm";
  // Kinit should fail because of principal not in keytab.
  Status status = InitAuth(CURRENT_EXECUTABLE_PATH);
  EXPECT_FALSE(status.ok());
  EXPECT_STR_CONTAINS(status.GetDetail(), "Could not init kerberos: Runtime error: "
      "unable to kinit: unable to login from keytab: Keytab contains no suitable keys "
      "for non-existent-principal/host@realm");

  FLAGS_principal = FLAGS_be_principal = "MALFORMEDPRINCIPAL//@";
  // We should fail before attempting kinit because of malformed principal.
  status = InitAuth(CURRENT_EXECUTABLE_PATH);
  EXPECT_FALSE(status.ok());
  EXPECT_STR_CONTAINS(status.GetDetail(), "Could not init kerberos: Runtime error: "
      "unable to kinit: unable to login from keytab:");
}

// This test confirms that auth is bypassed on KRPC services when
// --skip_external_kerberos_auth=true
TEST_P(RpcMgrKerberizedTest, InternalAuthorizationSkip) {
  auto ia =
      ScopedFlagSetter<bool>::Make(&FLAGS_skip_internal_kerberos_auth, true);
  GeneratedServiceIf* ping_impl =
      TakeOverService(make_unique<PingServiceImpl>(&rpc_mgr_));
  const int num_service_threads = 10;
  const int queue_size = 10;
  ASSERT_OK(rpc_mgr_.RegisterService(num_service_threads, queue_size, ping_impl,
      static_cast<PingServiceImpl*>(ping_impl)->mem_tracker(),
      ExecEnv::GetInstance()->rpc_metrics()));
  FLAGS_num_acceptor_threads = 2;
  FLAGS_num_reactor_threads = 10;
  ASSERT_OK(rpc_mgr_.StartServices());

  // Switch over to a credentials cache which only contains the dummy credential.
  // Kinit done in InitAuth() uses a different credentials cache.
  DCHECK_NE(FLAGS_krb5_ccname, kdc_ccname);
  discard_result(setenv("KRB5CCNAME", kdc_ccname.c_str(), 1));

  RpcController controller;
  Status rpc_status;

  // PingService would expect FLAGS_be_principal as principal name, which
  // we don't have in our dummy credential cache. So this only succeeds if
  // auth is disabled.
  unique_ptr<PingServiceProxy> ping_proxy;
  ASSERT_OK(static_cast<PingServiceImpl*>(ping_impl)->GetProxy(krpc_address_,
      FLAGS_hostname, &ping_proxy));
  PingRequestPB ping_request;
  PingResponsePB ping_response;
  controller.Reset();
  EXPECT_OK(FromKuduStatus(ping_proxy->Ping(ping_request, &ping_response, &controller)));
}

// Run tests with Unix domain socket and TCP socket by setting
// FLAGS_rpc_use_unix_domain_socket as true and false.
INSTANTIATE_TEST_SUITE_P(
    UdsOnAndOff, RpcMgrKerberizedTest, ::testing::Values(true, false));

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
