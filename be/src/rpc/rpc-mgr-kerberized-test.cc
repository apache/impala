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

#include "rpc/rpc-mgr-test-base.h"
#include "service/fe-support.h"

DECLARE_bool(use_kudu_kinit);
DECLARE_bool(use_krpc);

DECLARE_string(be_principal);
DECLARE_string(hostname);
DECLARE_string(principal);
DECLARE_string(ssl_client_ca_certificate);
DECLARE_string(ssl_server_certificate);
DECLARE_string(ssl_private_key);

// The principal name and the realm used for creating the mini-KDC.
// To be initialized at main().
static string kdc_principal;
static string kdc_realm;

namespace impala {

class RpcMgrKerberizedTest :
    public RpcMgrTestBase<testing::TestWithParam<KerberosSwitch> > {

  virtual void SetUp() override {
    KerberosSwitch k = GetParam();
    FLAGS_use_krpc = true;
    FLAGS_use_kudu_kinit = k == USE_KRPC_KUDU_KERBEROS;
    FLAGS_principal = "dummy-service/host@realm";
    FLAGS_be_principal = strings::Substitute("$0@$1", kdc_principal, kdc_realm);
    ASSERT_OK(InitAuth(CURRENT_EXECUTABLE_PATH));
    RpcMgrTestBase::SetUp();
  }

  virtual void TearDown() override {
    FLAGS_principal.clear();
    FLAGS_be_principal.clear();
  }
};

INSTANTIATE_TEST_CASE_P(KerberosOnAndOff,
                        RpcMgrKerberizedTest,
                        ::testing::Values(USE_KRPC_IMPALA_KERBEROS,
                                          USE_KRPC_KUDU_KERBEROS));

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
  ScopedSetTlsFlags s(SERVER_CERT, PRIVATE_KEY, SERVER_CERT);
  ASSERT_OK(tls_rpc_mgr.Init());

  ASSERT_OK(RunMultipleServicesTestTemplate(this, &tls_rpc_mgr, tls_krpc_address));
  tls_rpc_mgr.Shutdown();
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
  kdc_principal = Substitute("impala-test/$0", ip);
  kdc_realm = "KRBTEST.COM";

  int port = impala::FindUnusedEphemeralPort(nullptr);
  std::unique_ptr<impala::MiniKdcWrapper> kdc;
  status = impala::MiniKdcWrapper::SetupAndStartMiniKDC(
      kdc_principal, kdc_realm, "24h", "7d", port, &kdc);
  DCHECK(status.ok());

  // Fill in the path of the current binary for use by the tests.
  CURRENT_EXECUTABLE_PATH = argv[0];
  int retval = RUN_ALL_TESTS();

  // Shutdown KDC.
  status = kdc->TearDownMiniKDC();
  DCHECK(status.ok());

  return retval;

}
