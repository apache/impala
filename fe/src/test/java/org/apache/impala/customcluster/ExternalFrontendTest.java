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

package org.apache.impala.customcluster;

import java.util.List;

import org.apache.hive.service.rpc.thrift.TCloseSessionReq;
import org.apache.hive.service.rpc.thrift.TCloseSessionResp;
import org.apache.hive.service.rpc.thrift.TColumn;
import org.apache.hive.service.rpc.thrift.TExecuteStatementReq;
import org.apache.hive.service.rpc.thrift.TExecuteStatementResp;
import org.apache.hive.service.rpc.thrift.TFetchOrientation;
import org.apache.hive.service.rpc.thrift.TFetchResultsReq;
import org.apache.hive.service.rpc.thrift.TFetchResultsResp;
import org.apache.hive.service.rpc.thrift.TOpenSessionReq;
import org.apache.hive.service.rpc.thrift.TOpenSessionResp;
import org.apache.hive.service.rpc.thrift.TStatus;
import org.apache.hive.service.rpc.thrift.TStatusCode;
import org.apache.impala.catalog.Catalog;
import org.apache.impala.common.FrontendFixture;
import org.apache.impala.common.ImpalaException;
import org.apache.impala.service.Frontend.PlanCtx;
import org.apache.impala.service.Frontend;
import org.apache.impala.testutil.TestUtils;
import org.apache.impala.thrift.ImpalaHiveServer2Service;
import org.apache.impala.thrift.TExecRequest;
import org.apache.impala.thrift.TExecutePlannedStatementReq;
import org.apache.impala.thrift.TQueryCtx;
import org.apache.impala.thrift.TQueryOptions;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.transport.THttpClient;
import org.apache.thrift.transport.TSocket;

import org.junit.After;
import org.junit.Assert;
import org.junit.Test;

public class ExternalFrontendTest {
  private final FrontendFixture feFixture_ = FrontendFixture.instance();
  private final Frontend frontend_ = feFixture_.frontend();
  private final int externalFePort = 21159;
  private final int hs2BinaryPort = 21050;
  private final int hs2HttpPort = 28000;

  void setup(int port, boolean isHttp) throws Exception {
    String impaladFlags = "--external_fe_port=" + port;
    if (isHttp) {
      impaladFlags += " --enable_external_fe_http";
    }
    // Start the impala cluster with the first impalad configured with external frontend
    // arguments
    int ret = CustomClusterRunner.StartImpalaCluster(
        "", "", "", "--per_impalad_args=" + impaladFlags);
    Assert.assertEquals(
        "custom cluster failed to start with args: " + impaladFlags, ret, 0);
  }

  void setupExternalFe() throws Exception { setup(externalFePort, false); }

  void setupExternalFeHttp() throws Exception { setup(externalFePort, true); }

  ImpalaHiveServer2Service.Client createBinaryClient(int port) throws Exception {
    // Create a binary connection against the hs2 port
    TSocket sock = new TSocket("localhost", port);
    sock.open();
    return new ImpalaHiveServer2Service.Client(new TBinaryProtocol(sock));
  }

  ImpalaHiveServer2Service.Client createHttpClient(int port) throws Exception {
    String host_url = "http://localhost:" + port + "/cliservice";
    THttpClient client = new THttpClient(host_url);
    return new ImpalaHiveServer2Service.Client(new TBinaryProtocol(client));
  }

  static TStatus verifySuccess(TStatus status) throws Exception {
    if (status.getStatusCode() == TStatusCode.SUCCESS_STATUS
        || status.getStatusCode() == TStatusCode.SUCCESS_WITH_INFO_STATUS) {
      return status;
    }
    throw new Exception(status.toString());
  }

  void executeTestQuery(ImpalaHiveServer2Service.Client client) throws Exception {
    executeTestQuery(client, false);
  }

  TStatus executeTestQueryExpectFailure(ImpalaHiveServer2Service.Client client)
      throws Exception {
    return executeTestQuery(client, true);
  }

  TStatus executeTestQuery(ImpalaHiveServer2Service.Client client,
      boolean shouldFailExecute) throws Exception {
    String testStmt = "SELECT 'this is a test, this is only a test'";
    String expectedValue = "this is a test, this is only a test";

    // Create the TExecRequest
    TQueryOptions options = new TQueryOptions();
    options.setExec_single_node_rows_threshold(0);

    TQueryCtx queryCtx =
        TestUtils.createQueryContext(Catalog.DEFAULT_DB, System.getProperty("user.name"));
    queryCtx.client_request.setStmt(testStmt);
    queryCtx.client_request.query_options = options;

    TExecRequest request = null;
    try {
      request = frontend_.createExecRequest(new PlanCtx(queryCtx));
    } catch (ImpalaException e) {
      Assert.fail(
          "Failed to create exec request for '" + testStmt + "': " + e.getMessage());
    }

    // Open Session
    TOpenSessionResp openResp = client.OpenSession(new TOpenSessionReq());
    verifySuccess(openResp.getStatus());

    // Create TExecutePlannedStatementReq
    TExecuteStatementReq executeReq = new TExecuteStatementReq();
    executeReq.setSessionHandle(openResp.getSessionHandle());
    executeReq.setStatement(testStmt);
    TExecutePlannedStatementReq executePlannedReq = new TExecutePlannedStatementReq();
    executePlannedReq.setStatementReq(executeReq);
    executePlannedReq.setPlan(request);

    // Execute and Fetch
    TExecuteStatementResp execResp = client.ExecutePlannedStatement(executePlannedReq);
    if (shouldFailExecute) {
      return execResp.getStatus();
    }
    verifySuccess(execResp.getStatus());

    TFetchResultsReq fetchReq = new TFetchResultsReq(
        execResp.getOperationHandle(), TFetchOrientation.FETCH_NEXT, 1000);
    TFetchResultsResp fetchResp = client.FetchResults(fetchReq);
    verifySuccess(fetchResp.getStatus());

    // Verify Results
    List<TColumn> columns = fetchResp.getResults().getColumns();
    Assert.assertEquals(1, columns.size());
    Assert.assertEquals(expectedValue, columns.get(0).getStringVal().getValues().get(0));

    // Close Session
    TCloseSessionResp closeResp =
        client.CloseSession(new TCloseSessionReq(openResp.getSessionHandle()));
    return verifySuccess(closeResp.getStatus());
  }

  @Test
  public void testExternalFrontendBinary() throws Exception {
    setupExternalFe();
    executeTestQuery(createBinaryClient(externalFePort));
  }

  @Test
  public void testExternalFrontendHttp() throws Exception {
    setupExternalFeHttp();
    executeTestQuery(createHttpClient(externalFePort));
  }

  @Test
  public void testExecutePlannedStatementDisallowedNonExternalFe() throws Exception {
    setupExternalFe();
    // Try to execute a planned query against the hs2 service (it should fail)
    TStatus status = executeTestQueryExpectFailure(createBinaryClient(hs2BinaryPort));
    Assert.assertEquals(status.getStatusCode(), TStatusCode.ERROR_STATUS);
    Assert.assertTrue(status.toString().contains("Unsupported operation"));

    // Now try to execute against the hs2 http service (it should also fail)
    status = executeTestQueryExpectFailure(createHttpClient(hs2HttpPort));
    Assert.assertEquals(status.getStatusCode(), TStatusCode.ERROR_STATUS);
    Assert.assertTrue(status.toString().contains("Unsupported operation"));
  }

  @After
  public void cleanUp() throws Exception {
    // Restore cluster to state before the test
    CustomClusterRunner.StartImpalaCluster();
  }
}
