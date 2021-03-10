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

#include "testutil/gtest-util.h"

#include <boost/filesystem.hpp>

#include "gen-cpp/ImpalaHiveServer2Service.h"
#include "rpc/authentication.h"
#include "util/kudu-status-util.h"
#include "util/network-util.h"
#include "util/os-util.h"

#include "kudu/security/test/mini_kdc.h"

DECLARE_string(principal);
DECLARE_string(keytab_file);

static string IMPALA_HOME(getenv("IMPALA_HOME"));

namespace filesystem = boost::filesystem;

using namespace apache::hive::service::cli::thrift;
using namespace apache::thrift;
using strings::Substitute;

namespace impala {

class TestHS2Service : public ImpalaHiveServer2ServiceIf {
 public:
  virtual ~TestHS2Service() {}
  virtual void OpenSession(TOpenSessionResp& _return, const TOpenSessionReq& req) {}
  virtual void CloseSession(TCloseSessionResp& _return, const TCloseSessionReq& req) {}
  virtual void GetInfo(TGetInfoResp& _return, const TGetInfoReq& req) {}
  virtual void ExecuteStatement(
      TExecuteStatementResp& _return, const TExecuteStatementReq& req) {}
  virtual void ExecutePlannedStatement(
      TExecuteStatementResp& _return, const TExecutePlannedStatementReq& req) {}
  virtual void InitQueryContext(TInitQueryContextResp& return_val) {}
  virtual void GetBackendConfig(TGetBackendConfigResp& return_val,
      const TGetBackendConfigReq& req) {}
  virtual void GetExecutorMembership(
      TGetExecutorMembershipResp& _return, const TGetExecutorMembershipReq& req) {}
  virtual void GetTypeInfo(TGetTypeInfoResp& _return, const TGetTypeInfoReq& req) {}
  virtual void GetCatalogs(TGetCatalogsResp& _return, const TGetCatalogsReq& req) {}
  virtual void GetSchemas(TGetSchemasResp& _return, const TGetSchemasReq& req) {}
  virtual void GetTables(TGetTablesResp& _return, const TGetTablesReq& req) {}
  virtual void GetTableTypes(TGetTableTypesResp& _return, const TGetTableTypesReq& req) {}
  virtual void GetColumns(TGetColumnsResp& _return, const TGetColumnsReq& req) {}
  virtual void GetFunctions(TGetFunctionsResp& _return, const TGetFunctionsReq& req) {}
  virtual void GetPrimaryKeys(
      TGetPrimaryKeysResp& _return, const TGetPrimaryKeysReq& req) {}
  virtual void GetCrossReference(
      TGetCrossReferenceResp& _return, const TGetCrossReferenceReq& req) {}
  virtual void GetOperationStatus(
      TGetOperationStatusResp& _return, const TGetOperationStatusReq& req) {}
  virtual void CancelOperation(
      TCancelOperationResp& _return, const TCancelOperationReq& req) {}
  virtual void CloseOperation(
      TCloseOperationResp& _return, const TCloseOperationReq& req) {}
  virtual void GetResultSetMetadata(
      TGetResultSetMetadataResp& _return, const TGetResultSetMetadataReq& req) {}
  virtual void FetchResults(TFetchResultsResp& _return, const TFetchResultsReq& req) {}
  virtual void GetDelegationToken(
      TGetDelegationTokenResp& _return, const TGetDelegationTokenReq& req) {}
  virtual void CancelDelegationToken(
      TCancelDelegationTokenResp& _return, const TCancelDelegationTokenReq& req) {}
  virtual void RenewDelegationToken(
      TRenewDelegationTokenResp& _return, const TRenewDelegationTokenReq& req) {}
  virtual void GetLog(TGetLogResp& _return, const TGetLogReq& req) {}
  virtual void GetExecSummary(
      TGetExecSummaryResp& _return, const TGetExecSummaryReq& req) {}
  virtual void GetRuntimeProfile(
      TGetRuntimeProfileResp& _return, const TGetRuntimeProfileReq& req) {}
  virtual void PingImpalaHS2Service(
      TPingImpalaHS2ServiceResp& _return, const TPingImpalaHS2ServiceReq& req) {}
  virtual void CloseImpalaOperation(
      TCloseImpalaOperationResp& _return, const TCloseImpalaOperationReq& req) {}
};

// Test that the HTTP server can successfuly read chunked requests.
// None of our usual clients are capable of generating chunked requests directly, but we
// may still have to process chunked requests, eg. if the requests are being proxied
// through something like Apache Knox, so we use curl to test it. Unfortunately, its
// difficult to craft a request that Thrift will actually recognize as an rpc, so mostly
// what we're really testing here is just that the server doesn't hang or crash.
TEST(ThriftHttpTest, TestChunkedRequests) {
  std::shared_ptr<TestHS2Service> service(new TestHS2Service());
  std::shared_ptr<TProcessor> hs2_http_processor(
      new ImpalaHiveServer2ServiceProcessor(service));
  int port = FindUnusedEphemeralPort();
  ThriftServer* http_server;
  ThriftServerBuilder http_builder("test-http-server", hs2_http_processor, port);
  ASSERT_OK(
      http_builder.transport_type(ThriftServer::TransportType::HTTP).Build(&http_server));
  ASSERT_OK(http_server->Start());

  string curl_output;
  // Only run this if curl is available.
  if (RunShellProcess("curl --version", &curl_output)) {
    string host = Substitute("http://127.0.0.1:$0", port);
    // Send a plain, non-chunked request.
    system(Substitute("curl -X POST -v '$0'", host).c_str());
    // Send a chunked request with a small amount of data.
    system(Substitute("curl -d somedata -H 'Transfer-Encoding: chunked' -v '$0'", host)
               .c_str());
    string filename = Substitute("$0/testdata/data/decimal_rtf_tbl.txt", IMPALA_HOME);
    EXPECT_TRUE(filesystem::exists(filename));
    // Send a chunked request with a large amount of data.
    system(
        Substitute("curl -d @$0 -H 'Transfer-Encoding: chunked' -v '$1'", filename, host)
            .c_str());
  } else {
    LOG(INFO) << "Skipping test, curl was not present: " << curl_output;
  }

  http_server->StopForTesting();
}

// Test that the HTTP server can be connected to successfully with Kerberos.
TEST(Hs2HttpTest, TestSpnego) {
  // Initialize the mini kdc.
  kudu::MiniKdc kdc(kudu::MiniKdcOptions{});
  KUDU_ASSERT_OK(kdc.Start());
  kdc.SetKrb5Environment();
  string kt_path;
  KUDU_ASSERT_OK(kdc.CreateServiceKeytab("HTTP/127.0.0.1", &kt_path));
  CHECK_ERR(setenv("KRB5_KTNAME", kt_path.c_str(), 1));
  KUDU_ASSERT_OK(kdc.CreateUserPrincipal("alice"));
  KUDU_ASSERT_OK(kdc.Kinit("alice"));

  // Set up a fake impala server with Kerberos enabled.
  gflags::FlagSaver saver;
  FLAGS_principal = "HTTP/127.0.0.1@KRBTEST.COM";
  FLAGS_keytab_file = kt_path;
  AuthManager auth_manager;
  ASSERT_OK(auth_manager.Init());
  std::shared_ptr<TestHS2Service> service(new TestHS2Service());
  std::shared_ptr<TProcessor> hs2_http_processor(
      new ImpalaHiveServer2ServiceProcessor(service));
  int port = FindUnusedEphemeralPort();
  ThriftServer* http_server;
  ThriftServerBuilder http_builder("test-http-server", hs2_http_processor, port);
  ASSERT_OK(http_builder.auth_provider(auth_manager.GetExternalHttpAuthProvider())
                .transport_type(ThriftServer::TransportType::HTTP)
                .Build(&http_server));
  ASSERT_OK(http_server->Start());

  string curl_output;
  // Only run this if a version of curl with the necessary features is available.
  if (RunShellProcess("curl --version", &curl_output)
      && curl_output.find("GSS-API") != string::npos
      && curl_output.find("SPNEGO") != string::npos) {
    system(Substitute("curl -X POST -v --negotiate -u : 'http://127.0.0.1:$0'", port)
               .c_str());
  } else {
    LOG(INFO) << "Skipping test, curl was not present or did not have the required "
              << "features: " << curl_output;
  }

  http_server->StopForTesting();
}
}

int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);
  impala::InitCommonRuntime(argc, argv, false, impala::TestInfo::BE_TEST);
  return RUN_ALL_TESTS();
}
