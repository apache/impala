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

#include "gen-cpp/ImpalaHiveServer2Service.h"
#include "rpc/authentication.h"
#include "util/kudu-status-util.h"

#include "kudu/security/test/mini_kdc.h"

DECLARE_string(principal);
DECLARE_string(keytab_file);

using namespace impala;
using namespace apache::hive::service::cli::thrift;
using namespace apache::thrift;

class TestHS2Service : public ImpalaHiveServer2ServiceIf {
 public:
  virtual ~TestHS2Service() {}
  virtual void OpenSession(TOpenSessionResp& _return, const TOpenSessionReq& req) {}
  virtual void CloseSession(TCloseSessionResp& _return, const TCloseSessionReq& req) {}
  virtual void GetInfo(TGetInfoResp& _return, const TGetInfoReq& req) {}
  virtual void ExecuteStatement(
      TExecuteStatementResp& _return, const TExecuteStatementReq& req) {}
  virtual void GetTypeInfo(TGetTypeInfoResp& _return, const TGetTypeInfoReq& req) {}
  virtual void GetCatalogs(TGetCatalogsResp& _return, const TGetCatalogsReq& req) {}
  virtual void GetSchemas(TGetSchemasResp& _return, const TGetSchemasReq& req) {}
  virtual void GetTables(TGetTablesResp& _return, const TGetTablesReq& req) {}
  virtual void GetTableTypes(TGetTableTypesResp& _return, const TGetTableTypesReq& req) {}
  virtual void GetColumns(TGetColumnsResp& _return, const TGetColumnsReq& req) {}
  virtual void GetFunctions(TGetFunctionsResp& _return, const TGetFunctionsReq& req) {}
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

// Test that the HTTP server can be connected to successfully with Kerberos.
TEST(ThriftKerberosTest, TestSpnego) {
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
  boost::shared_ptr<TestHS2Service> service(new TestHS2Service());
  boost::shared_ptr<TProcessor> hs2_http_processor(
      new ImpalaHiveServer2ServiceProcessor(service));
  int port = 28005;
  ThriftServer* http_server;
  ThriftServerBuilder http_builder("test-http-server", hs2_http_processor, port);
  ASSERT_OK(http_builder.auth_provider(auth_manager.GetExternalAuthProvider())
                .transport_type(ThriftServer::TransportType::HTTP)
                .Build(&http_server));
  ASSERT_OK(http_server->Start());

  // TODO: enable this when curl is available in the toolchain
  //system("curl -X POST -v --negotiate -u : 'http://127.0.0.1:28005'");
}

int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);
  InitCommonRuntime(argc, argv, false, TestInfo::BE_TEST);
  return RUN_ALL_TESTS();
}
