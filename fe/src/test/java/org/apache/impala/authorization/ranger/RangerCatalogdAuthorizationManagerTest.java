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

package org.apache.impala.authorization.ranger;

import org.apache.impala.analysis.GrantRevokeRoleStmt;
import org.apache.impala.authorization.AuthorizationProvider;
import org.apache.impala.authorization.AuthorizationTestBase;
import org.apache.impala.authorization.ranger.RangerBufferAuditHandler.AutoFlush;
import org.apache.impala.common.ImpalaException;
import org.apache.impala.common.InternalException;
import org.apache.impala.thrift.TCatalogServiceRequestHeader;
import org.apache.impala.thrift.TCatalogUpdateResult;
import org.apache.impala.thrift.TDdlExecResponse;
import org.apache.impala.thrift.TGrantRevokeRoleParams;
import org.apache.ranger.audit.model.AuthzAuditEvent;
import org.apache.ranger.plugin.model.RangerRole;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;

import static org.junit.Assert.assertEquals;

public class RangerCatalogdAuthorizationManagerTest extends AuthorizationTestBase {

  private static List<AutoFlush> capturedAuditHandlers;
  private static RangerCatalogdAuthorizationManager authzManager;
  private static String CLIENT_IP_ADDRESS = "127.0.0.1";
  private static String TEST_ROLE = "test_role";
  private static String GRANTEE_GROUP = "non_owner";
  private static String GRANTEE_USER = "non_owner";
  private static String REVOKEE_GROUP = "non_owner";
  private static String REVOKEE_USER = "non_owner";
  private static boolean isSetupDone = false;
  private static String ALTER_ACTION = "alter";
  private static String ALTER_ACCESS_TYPE = "ALTER";
  private static RangerImpalaPlugin staticPluginRef;

  public RangerCatalogdAuthorizationManagerTest() throws ImpalaException {
    super(AuthorizationProvider.RANGER);
    capturedAuditHandlers = new ArrayList<>();
    staticPluginRef = rangerImpalaPlugin_;
    authzManager = new RangerCatalogdAuthorizationManager(
        () -> rangerImpalaPlugin_, authzCatalog_.getSrcCatalog()) {
      @Override
      protected AutoFlush createAuditHandler(String sqlStmt, String clusterName,
          String clientIp) {
        // Create the real handler but capture it before returning.
        AutoFlush handler = RangerBufferAuditHandler.autoFlush(sqlStmt, clusterName,
            clientIp);
        capturedAuditHandlers.add(handler);
        return handler;
      }
    };
  }

  @Before
  public void setUpTest() throws Exception {
    // The role 'TEST_ROLE' only has to be created once for the tests to run.
    // We do not annotate this method as @BeforeClass and call
    // 'staticPluginRef.createRole()' because 'staticPluginRef' would have been null
    // when this method is called.
    if (!isSetupDone) {
      RangerRole role = new RangerRole();
      role.setName(TEST_ROLE);
      role.setCreatedByUser(RANGER_ADMIN.getName());
      rangerImpalaPlugin_.createRole(role, /* resultProcessor */ null);
      isSetupDone = true;
    }
  }

  @AfterClass
  public static void teardown() throws Exception {
    // We do not call rangerImpalaPlugin_.createRole() because 'rangerImpalaPlugin_' is
    // not static.
    staticPluginRef.dropRole(RANGER_ADMIN.getName(), TEST_ROLE,
        /* resultProcessor */ null);
  }

  @Test
  public void testGrantRoleAuditEvents() throws Exception {
    grantRole(handlers -> {
          assertEquals(1, handlers.size());
          List<AuthzAuditEvent> events = handlers.get(0).getAuthzEvents();
          assertEventEquals(events.get(0), ALTER_ACTION, ALTER_ACCESS_TYPE,
              RANGER_ADMIN.getName(), /* expectSuccess */ true, /* policyId */ -1L,
              CLIENT_IP_ADDRESS, "GRANT ROLE test_role TO USER non_owner",
              /* resultReason */ null, /* resultPath */ null, /* resultType */ null);
        }, RANGER_ADMIN.getName(), TEST_ROLE, null, GRANTEE_USER,
        /* expectSuccess */ true);
    grantRole(handlers -> {
          assertEquals(1, handlers.size());
          List<AuthzAuditEvent> events = handlers.get(0).getAuthzEvents();
          assertEventEquals(events.get(0), ALTER_ACTION, ALTER_ACCESS_TYPE,
              OWNER_USER.getName(), /* expectSuccess */ false, /* policyId */ -1L,
              CLIENT_IP_ADDRESS, "GRANT ROLE test_role TO USER non_owner",
              /* resultReason */ null, /* resultPath */ null, /* resultType */ null);
        }, OWNER_USER.getName(), TEST_ROLE, null, GRANTEE_USER,
        /* expectSuccess */ false);
    grantRole(handlers -> {
          assertEquals(1, handlers.size());
          List<AuthzAuditEvent> events = handlers.get(0).getAuthzEvents();
          assertEventEquals(events.get(0), ALTER_ACTION, ALTER_ACCESS_TYPE,
              RANGER_ADMIN.getName(), /* expectSuccess */ true, /* policyId */ -1L,
              CLIENT_IP_ADDRESS, "GRANT ROLE test_role TO GROUP non_owner",
              /* resultReason */ null, /* resultPath */ null, /* resultType */ null);
        }, RANGER_ADMIN.getName(), TEST_ROLE, GRANTEE_GROUP, null,
        /* expectSuccess */ true);
    grantRole(handlers -> {
          assertEquals(1, handlers.size());
          List<AuthzAuditEvent> events = handlers.get(0).getAuthzEvents();
          assertEventEquals(events.get(0), ALTER_ACTION, ALTER_ACCESS_TYPE,
              OWNER_USER.getName(), /* expectSuccess */ false, /* policyId */ -1L,
              CLIENT_IP_ADDRESS, "GRANT ROLE test_role TO GROUP non_owner",
              /* resultReason */ null, /* resultPath */ null, /* resultType */ null);
        }, OWNER_USER.getName(), TEST_ROLE, GRANTEE_GROUP, null,
        /* expectSuccess */ false);
  }

  @Test
  public void testRevokeRoleAuditEvents() throws Exception {
    revokeRole(handlers -> {
          assertEquals(1, handlers.size());
          List<AuthzAuditEvent> events = handlers.get(0).getAuthzEvents();
          assertEventEquals(events.get(0), ALTER_ACTION, ALTER_ACCESS_TYPE,
              RANGER_ADMIN.getName(), /* expectSuccess */ true, /* policyId */ -1L,
              CLIENT_IP_ADDRESS, "REVOKE ROLE test_role FROM USER non_owner",
              /* resultReason */ null, /* resultPath */ null, /* resultType */ null);
        }, RANGER_ADMIN.getName(), TEST_ROLE, null, REVOKEE_USER,
        /* expectSuccess */ true);
    revokeRole(handlers -> {
          assertEquals(1, handlers.size());
          List<AuthzAuditEvent> events = handlers.get(0).getAuthzEvents();
          assertEventEquals(events.get(0), ALTER_ACTION, ALTER_ACCESS_TYPE,
              OWNER_USER.getName(), /* expectSuccess */ false, /* policyId */ -1L,
              CLIENT_IP_ADDRESS, "REVOKE ROLE test_role FROM USER non_owner",
              /* resultReason */ null, /* resultPath */ null, /* resultType */ null);
        }, OWNER_USER.getName(), TEST_ROLE, null, REVOKEE_USER,
        /* expectSuccess */ false);
    revokeRole(handlers -> {
          assertEquals(1, handlers.size());
          List<AuthzAuditEvent> events = handlers.get(0).getAuthzEvents();
          assertEventEquals(events.get(0), ALTER_ACTION, ALTER_ACCESS_TYPE,
              RANGER_ADMIN.getName(), /* expectSuccess */ true, /* policyId */ -1L,
              CLIENT_IP_ADDRESS, "REVOKE ROLE test_role FROM GROUP non_owner",
              /* resultReason */ null, /* resultPath */ null, /* resultType */ null);
        }, RANGER_ADMIN.getName(), TEST_ROLE, REVOKEE_GROUP, null,
        /* expectSuccess */ true);
    revokeRole(handlers -> {
          assertEquals(1, handlers.size());
          List<AuthzAuditEvent> events = handlers.get(0).getAuthzEvents();
          assertEventEquals(events.get(0), ALTER_ACTION, ALTER_ACCESS_TYPE,
              OWNER_USER.getName(), /* expectSuccess */ false, /* policyId */ -1L,
              CLIENT_IP_ADDRESS, "REVOKE ROLE test_role FROM GROUP non_owner",
              /* resultReason */ null, /* resultPath */ null, /* resultType */ null);
        }, OWNER_USER.getName(), TEST_ROLE, REVOKEE_GROUP, null,
        /* expectSuccess */ false);
  }

  private static void grantRole(Consumer<List<AutoFlush>> resultChecker,
      String grantorUser, String role, String granteeGroup, String granteeUser,
      boolean expectSuccess) throws Exception {
    GrantRevokeRoleStmt stmt = new GrantRevokeRoleStmt(role, granteeGroup, granteeUser,
        /* isGrantStmt */ true);
    String grantSqlStmt = stmt.toSql();
    TCatalogServiceRequestHeader header = createHeader(grantorUser, grantSqlStmt,
        CLIENT_IP_ADDRESS);
    TGrantRevokeRoleParams params = stmt.toThrift();
    try {
      String errorString = null;
      try {
        authzManager.grantRoleToGroupOrUser(header, params,
            new TDdlExecResponse(new TCatalogUpdateResult()));
      } catch (ImpalaException e) {
        errorString = e.getMessage();
      }
      if (expectSuccess && errorString != null) {
        throw new InternalException(errorString);
      } else if (!expectSuccess && errorString == null) {
        throw new Exception("We expected the test to throw but it did not.");
      }

      resultChecker.accept(capturedAuditHandlers);
    } finally {
      TCatalogServiceRequestHeader adminHeader = createHeader(RANGER_ADMIN.getName(),
          "revoke role " + role + " from " + granteeUser, CLIENT_IP_ADDRESS);
      authzManager.revokeRoleFromGroupOrUser(adminHeader, params,
          new TDdlExecResponse(new TCatalogUpdateResult()));
      capturedAuditHandlers.clear();
    }
  }

  private void revokeRole(Consumer<List<AutoFlush>> resultChecker, String revokerUser,
      String role, String revokeeGroup, String revokeeUser, boolean expectSuccess)
      throws Exception {
    GrantRevokeRoleStmt stmt = new GrantRevokeRoleStmt(role, revokeeGroup, revokeeUser,
        /* isGrantStmt */ false);
    String revokeSqlStmt = stmt.toSql();
    TCatalogServiceRequestHeader header = createHeader(revokerUser, revokeSqlStmt,
        CLIENT_IP_ADDRESS);
    TGrantRevokeRoleParams revokeParams = stmt.toThrift();
    try {
      String errorString = null;
      try {
        authzManager.revokeRoleFromGroupOrUser(header, revokeParams,
            new TDdlExecResponse(new TCatalogUpdateResult()));
      } catch (ImpalaException e) {
        errorString = e.getMessage();
      }
      if (expectSuccess && errorString != null) {
        throw new InternalException(errorString);
      } else if (!expectSuccess && errorString == null) {
        throw new Exception("We expected the test to throw but it did not.");
      }

      resultChecker.accept(capturedAuditHandlers);
    } finally {
      capturedAuditHandlers.clear();
    }
  }

  private static TCatalogServiceRequestHeader createHeader(String user, String sqlStmt,
      String ipAddress) {
    TCatalogServiceRequestHeader header = new TCatalogServiceRequestHeader();
    header.setRequesting_user(user);
    header.setRedacted_sql_stmt(sqlStmt);
    header.setClient_ip(ipAddress);
    return header;
  }

  private static void assertEventEquals(AuthzAuditEvent event,
      String action, String accessType, String user, boolean expectSuccess,
      long policyId, String clientIp, String sqlStmt, String resultReason,
      String resultPath, String resultType) {
    assertEquals(action, event.getAction());
    assertEquals(accessType, event.getAccessType());
    assertEquals(user, event.getUser());
    assertEquals(expectSuccess ? 1 : 0, event.getAccessResult());
    assertEquals(policyId, event.getPolicyId());
    assertEquals(clientIp, event.getClientIP());
    assertEquals(sqlStmt, event.getRequestData());
    assertEquals(resultReason, event.getResultReason());
    assertEquals(resultPath, event.getResourcePath());
    assertEquals(resultType, event.getResourceType());
  }
}
