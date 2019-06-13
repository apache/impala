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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.directory.server.core.annotations.CreateDS;
import org.apache.directory.server.core.annotations.CreatePartition;
import org.apache.directory.server.annotations.CreateLdapServer;
import org.apache.directory.server.annotations.CreateTransport;
import org.apache.directory.server.core.annotations.ApplyLdifFiles;
import org.apache.directory.server.core.integ.CreateLdapServerRule;
import org.apache.hive.service.rpc.thrift.*;
import org.apache.impala.util.Metrics;
import org.apache.thrift.transport.THttpClient;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;

@CreateDS(name = "myDS",
    partitions = { @CreatePartition(name = "test", suffix = "dc=myorg,dc=com") })
@CreateLdapServer(
    transports = { @CreateTransport(protocol = "LDAP", address = "localhost") })
@ApplyLdifFiles({"users.ldif"})
/**
 * Tests that hiveserver2 operations over the http interface work as expected when
 * ldap authentication is being used.
 */
public class LdapHS2Test {
  @ClassRule
  public static CreateLdapServerRule serverRule = new CreateLdapServerRule();

  Metrics metrics = new Metrics();

  @Before
  public void setUp() throws Exception {
    String uri =
        String.format("ldap://localhost:%s", serverRule.getLdapServer().getPort());
    String dn = "cn=#UID,ou=Users,dc=myorg,dc=com";
    String ldapArgs = String.format("--enable_ldap_auth --ldap_uri='%s' "
        + "--ldap_bind_pattern='%s' --ldap_passwords_in_clear_ok", uri, dn);
    int ret = CustomClusterRunner.StartImpalaCluster(ldapArgs);
    assertEquals(ret, 0);
  }

  static void verifySuccess(TStatus status) throws Exception {
    if (status.getStatusCode() == TStatusCode.SUCCESS_STATUS
        || status.getStatusCode() == TStatusCode.SUCCESS_WITH_INFO_STATUS) {
      return;
    }
    throw new Exception(status.toString());
  }

  /**
   * Executes 'query' and fetches the results. Expects there to be exactly one string
   * returned, which be be equal to 'expectedResult'.
   */
  static TOperationHandle execAndFetch(TCLIService.Iface client,
      TSessionHandle sessionHandle, String query, String expectedResult)
      throws Exception {
    TExecuteStatementReq execReq = new TExecuteStatementReq(sessionHandle, query);
    TExecuteStatementResp execResp = client.ExecuteStatement(execReq);
    verifySuccess(execResp.getStatus());

    TFetchResultsReq fetchReq = new TFetchResultsReq(
        execResp.getOperationHandle(), TFetchOrientation.FETCH_NEXT, 1000);
    TFetchResultsResp fetchResp = client.FetchResults(fetchReq);
    verifySuccess(fetchResp.getStatus());
    List<TColumn> columns = fetchResp.getResults().getColumns();
    assertEquals(columns.size(), 1);
    assertEquals(columns.get(0).getStringVal().getValues().get(0), expectedResult);

    return execResp.getOperationHandle();
  }

  private void verifyMetrics(long expectedBasicAuthSuccess, long expectedBasicAuthFailure)
      throws Exception {
    long actualBasicAuthSuccess = (long) metrics.getMetric(
        "impala.thrift-server.hiveserver2-http-frontend.total-basic-auth-success");
    assertEquals(expectedBasicAuthSuccess, actualBasicAuthSuccess);
    long actualBasicAuthFailure = (long) metrics.getMetric(
        "impala.thrift-server.hiveserver2-http-frontend.total-basic-auth-failure");
    assertEquals(expectedBasicAuthFailure, actualBasicAuthFailure);
  }

  /**
   * Tests LDAP authentication to the HTTP hiveserver2 endpoint.
   */
  @Test
  public void testHiveserver2() throws Exception {
    verifyMetrics(0, 0);
    THttpClient transport = new THttpClient("http://localhost:28000");
    Map<String, String> headers = new HashMap<String, String>();
    // Authenticate as 'Test1Ldap' with password '12345'
    headers.put("Authorization", "Basic VGVzdDFMZGFwOjEyMzQ1");
    transport.setCustomHeaders(headers);
    transport.open();
    TCLIService.Iface client = new TCLIService.Client(new TBinaryProtocol(transport));

    // Open a session which will get username 'Test1Ldap'.
    TOpenSessionReq openReq = new TOpenSessionReq();
    TOpenSessionResp openResp = client.OpenSession(openReq);
    // One successful authentication.
    verifyMetrics(1, 0);
    // Running a query should succeed.
    TOperationHandle operationHandle = execAndFetch(
        client, openResp.getSessionHandle(), "select logged_in_user()", "Test1Ldap");
    // Two more successful authentications - for the Exec() and the Fetch().
    verifyMetrics(3, 0);

    // Authenticate as 'Test2Ldap' with password 'abcde'
    headers.put("Authorization", "Basic VGVzdDJMZGFwOmFiY2Rl");
    transport.setCustomHeaders(headers);
    String expectedError = "The user authorized on the connection 'Test2Ldap' does not "
        + "match the session username 'Test1Ldap'\n";
    try {
      // Run a query which should fail.
      execAndFetch(client, openResp.getSessionHandle(), "select 1", "1");
      fail("Expected error: " + expectedError);
    } catch (Exception e) {
      assertTrue(e.getMessage().contains(expectedError));
      // The connection for the Exec() will be successful.
      verifyMetrics(4, 0);
    }

    // Try to cancel the first query, which should fail.
    TCancelOperationReq cancelReq = new TCancelOperationReq(operationHandle);
    TCancelOperationResp cancelResp = client.CancelOperation(cancelReq);
    verifyMetrics(5, 0);
    assertEquals(cancelResp.getStatus().getStatusCode(), TStatusCode.ERROR_STATUS);
    assertEquals(cancelResp.getStatus().getErrorMessage(), expectedError);

    // Open another session which will get username 'Test2Ldap'.
    TOpenSessionReq openReq2 = new TOpenSessionReq();
    TOpenSessionResp openResp2 = client.OpenSession(openReq);
    verifyMetrics(6, 0);
    // Running a query with the new session should succeed.
    execAndFetch(
        client, openResp2.getSessionHandle(), "select logged_in_user()", "Test2Ldap");
    verifyMetrics(8, 0);

    // Attempt to authenticate with some bad headers:
    // - invalid username/password combination
    // - invalid base64 encoded value
    // - Invalid mechanism
    int numFailures = 0;
    for (String authStr : new String[] {"Basic VGVzdDJMZGFwOjEyMzQ1",
             "Basic invalid-base64", "Negotiate VGVzdDFMZGFwOjEyMzQ1"}) {
      // Attempt to authenticate with an invalid password.
      headers.put("Authorization", authStr);
      transport.setCustomHeaders(headers);
      try {
        TOpenSessionReq openReq3 = new TOpenSessionReq();
        TOpenSessionResp openResp3 = client.OpenSession(openReq);
        ++numFailures;
        verifyMetrics(8, numFailures);
        fail("Exception exception.");
      } catch (Exception e) {
        assertEquals(e.getMessage(), "HTTP Response code: 401");
      }
    }
  }
}
