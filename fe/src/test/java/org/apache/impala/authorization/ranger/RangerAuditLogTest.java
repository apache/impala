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

import org.apache.impala.authorization.AuthorizationChecker;
import org.apache.impala.authorization.AuthorizationConfig;
import org.apache.impala.authorization.AuthorizationContext;
import org.apache.impala.authorization.AuthorizationFactory;
import org.apache.impala.authorization.AuthorizationPolicy;
import org.apache.impala.authorization.AuthorizationProvider;
import org.apache.impala.authorization.AuthorizationTestBase;
import org.apache.impala.common.ImpalaException;
import org.apache.impala.thrift.TPrivilege;
import org.apache.impala.thrift.TPrivilegeLevel;
import org.apache.ranger.audit.model.AuthzAuditEvent;
import org.junit.Test;

import java.util.Collections;
import java.util.List;
import java.util.function.Consumer;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class RangerAuditLogTest extends AuthorizationTestBase {
  private RangerAuthorizationCheckerSpy authzChecker_;

  private static class RangerAuthorizationCheckerSpy extends RangerAuthorizationChecker {
    private AuthorizationContext authzCtx_;

    public RangerAuthorizationCheckerSpy(AuthorizationConfig authzConfig) {
      super(authzConfig);
    }

    @Override
    public void postAuthorize(AuthorizationContext authzCtx) {
      super.postAuthorize(authzCtx);
      authzCtx_ = authzCtx;
    }
  }

  public RangerAuditLogTest() throws ImpalaException {
    super(AuthorizationProvider.RANGER);
  }

  @Test
  public void testAuditLogSuccess() throws ImpalaException {
    authzOk(events -> {
      assertEquals(1, events.size());
      assertEventEquals("@database", "create", "test_db", 1, events.get(0));
      assertEquals("create database test_db", events.get(0).getRequestData());
    }, "create database test_db", onServer(TPrivilegeLevel.CREATE));

    authzOk(events -> {
      assertEquals(1, events.size());
      assertEventEquals("@table", "create", "functional/test_tbl", 1, events.get(0));
      assertEquals("create table functional.test_tbl(i int)",
          events.get(0).getRequestData());
    }, "create table functional.test_tbl(i int)", onDatabase("functional",
        TPrivilegeLevel.CREATE));

    authzOk(events -> {
      assertEquals(2, events.size());
      assertEventEquals("@udf", "create", "functional/f()", 1, events.get(0));
      assertEventEquals("@url", "all",
          "hdfs://localhost:20500/test-warehouse/libTestUdfs.so", 1, events.get(1));
      assertEquals("create function functional.f() returns int location " +
          "'hdfs://localhost:20500/test-warehouse/libTestUdfs.so' symbol='NoArgs'",
          events.get(0).getRequestData());
    }, "create function functional.f() returns int location " +
        "'hdfs://localhost:20500/test-warehouse/libTestUdfs.so' symbol='NoArgs'",
        onDatabase("functional", TPrivilegeLevel.CREATE),
        onUri("hdfs://localhost:20500/test-warehouse/libTestUdfs.so",
            TPrivilegeLevel.ALL));

    authzOk(events -> {
      assertEquals(2, events.size());
      assertEventEquals("@table", "create", "functional/new_table", 1, events.get(0));
      assertEventEquals("@url", "all", "hdfs://localhost:20500/test-warehouse/new_table",
          1, events.get(1));
      assertEquals("create table functional.new_table(i int) location " +
          "'hdfs://localhost:20500/test-warehouse/new_table'",
          events.get(0).getRequestData());
    }, "create table functional.new_table(i int) location " +
        "'hdfs://localhost:20500/test-warehouse/new_table'",
        onDatabase("functional", TPrivilegeLevel.CREATE),
        onUri("hdfs://localhost:20500/test-warehouse/new_table", TPrivilegeLevel.ALL));

    authzOk(events -> {
      // Only the table event.
      assertEquals(1, events.size());
      assertEventEquals("@table", "select", "functional/alltypes", 1, events.get(0));
      assertEquals("select id, string_col from functional.alltypes",
          events.get(0).getRequestData());
    }, "select id, string_col from functional.alltypes",
        onTable("functional", "alltypes", TPrivilegeLevel.SELECT));

    authzOk(events -> {
      // Only the column events. We don't want to log the failing table event used for
      // short circuiting.
      assertEquals(2, events.size());
      assertEventEquals("@column", "select", "functional/alltypes/id", 1, events.get(0));
      assertEquals("select id, string_col from functional.alltypes",
          events.get(0).getRequestData());
      assertEventEquals("@column", "select", "functional/alltypes/string_col", 1,
          events.get(1));
      assertEquals("select id, string_col from functional.alltypes",
          events.get(1).getRequestData());
    }, "select id, string_col from functional.alltypes",
        onColumn("functional", "alltypes", "id", TPrivilegeLevel.SELECT),
        onColumn("functional", "alltypes", "string_col", TPrivilegeLevel.SELECT));

    authzOk(events -> {
      assertEquals(3, events.size());
      assertEventEquals("@column", "refresh", "*/*/*", 1, events.get(0));
      assertEquals("invalidate metadata", events.get(0).getRequestData());
      assertEventEquals("@udf", "refresh", "*/*", 1, events.get(1));
      assertEquals("invalidate metadata", events.get(1).getRequestData());
      assertEventEquals("@url", "refresh", "*", 1, events.get(2));
      assertEquals("invalidate metadata", events.get(2).getRequestData());
    }, "invalidate metadata", onServer(TPrivilegeLevel.REFRESH));
  }

  @Test
  public void testAuditLogFailure() throws ImpalaException {
    authzError(events -> {
      assertEquals(1, events.size());
      assertEventEquals("@database", "create", "test_db", 0, events.get(0));
      assertEquals("create database test_db", events.get(0).getRequestData());
    }, "create database test_db");

    authzError(events -> {
      assertEquals(1, events.size());
      assertEventEquals("@table", "create", "functional/test_tbl", 0, events.get(0));
      assertEquals("create table functional.test_tbl(i int)",
          events.get(0).getRequestData());
    }, "create table functional.test_tbl(i int)");

    authzError(events -> {
      // Only log first the first failure.
      assertEquals(1, events.size());
      assertEventEquals("@udf", "create", "functional/f()", 0, events.get(0));
      assertEquals("create function functional.f() returns int location " +
          "'hdfs://localhost:20500/test-warehouse/libTestUdfs.so' symbol='NoArgs'",
          events.get(0).getRequestData());
    }, "create function functional.f() returns int location " +
        "'hdfs://localhost:20500/test-warehouse/libTestUdfs.so' symbol='NoArgs'");

    authzError(events -> {
      assertEquals(1, events.size());
      assertEventEquals("@url", "all", "hdfs://localhost:20500/test-warehouse/new_table",
          0, events.get(0));
      assertEquals("create table functional.new_table(i int) location " +
              "'hdfs://localhost:20500/test-warehouse/new_table'",
          events.get(0).getRequestData());
    }, "create table functional.new_table(i int) location " +
        "'hdfs://localhost:20500/test-warehouse/new_table'",
        onDatabase("functional", TPrivilegeLevel.CREATE));

    authzError(events -> {
      // Only log the first column event. We do not log the table access used for
      // short-circuiting.
      assertEquals(1, events.size());
      assertEventEquals("@column", "select", "functional/alltypes/id", 0, events.get(0));
      assertEquals("select id, string_col from functional.alltypes",
          events.get(0).getRequestData());
    }, "select id, string_col from functional.alltypes");
  }

  private void authzOk(Consumer<List<AuthzAuditEvent>> resultChecker, String stmt,
      TPrivilege[]... privileges) throws ImpalaException {
    authorize(stmt).ok(privileges);
    RangerAuthorizationContext rangerCtx =
        (RangerAuthorizationContext) authzChecker_.authzCtx_;
    resultChecker.accept(rangerCtx.getAuditHandler().getAuthzEvents());
  }

  private void authzError(Consumer<List<AuthzAuditEvent>> resultChecker, String stmt,
      TPrivilege[]... privileges) throws ImpalaException {
    authorize(stmt).error("", privileges);
    RangerAuthorizationContext rangerCtx =
        (RangerAuthorizationContext) authzChecker_.authzCtx_;
    resultChecker.accept(rangerCtx.getAuditHandler().getAuthzEvents());
  }

  private static void assertEventEquals(String resourceType, String accessType,
      String resourcePath, int accessResult, AuthzAuditEvent event) {
    assertEquals(resourceType, event.getResourceType());
    assertEquals(accessType.toUpperCase(), event.getAccessType());
    assertEquals(resourcePath, event.getResourcePath());
    assertEquals(accessResult, event.getAccessResult());
    assertEquals("test-cluster", event.getClusterName());
    assertTrue(!event.getClientIP().isEmpty());
  }

  @Override
  protected List<WithPrincipal> buildWithPrincipals() {
    return Collections.singletonList(new WithRangerUser());
  }

  @Override
  protected AuthorizationFactory createAuthorizationFactory(
      AuthorizationProvider authzProvider) {
    return new RangerAuthorizationFactory(authzConfig_) {
      @Override
      public AuthorizationChecker newAuthorizationChecker(
          AuthorizationPolicy authzPolicy) {
        authzChecker_ = new RangerAuthorizationCheckerSpy(authzConfig_);
        return authzChecker_;
      }
    };
  }
}
