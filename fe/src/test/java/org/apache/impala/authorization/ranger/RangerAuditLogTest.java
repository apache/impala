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

import com.google.common.base.Preconditions;
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
import org.junit.Ignore;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.function.Consumer;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class RangerAuditLogTest extends AuthorizationTestBase {
  private static RangerAuthorizationCheckerSpy authzChecker_ = null;

  private static class RangerAuthorizationCheckerSpy extends RangerAuthorizationChecker {
    private AuthorizationContext authzCtx_;

    public RangerAuthorizationCheckerSpy(AuthorizationConfig authzConfig) {
      super(authzConfig);
    }

    @Override
    public void postAuthorize(AuthorizationContext authzCtx, boolean authzOk) {
      super.postAuthorize(authzCtx, authzOk);
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
      // Table event and 2 column events
      assertEquals(3, events.size());
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

    authzOk(events -> {
      // Show the actual view_metadata audit log.
      assertEquals(1, events.size());
      assertEventEquals("@table", "view_metadata", "functional/alltypes", 1,
          events.get(0));
      assertEquals("show partitions functional.alltypes", events.get(0).getRequestData());
    }, "show partitions functional.alltypes",
        onTable("functional", "alltypes", TPrivilegeLevel.SELECT));

    // COMPUTE STATS results in two registrations of the PrivilegeRequest for ALTER.
    // Check we do not have duplicate ALTER events when the fully-qualified table name is
    // not in lowercase.
    authzOk(events -> {
      assertEquals(2, events.size());
      assertEventEquals("@table", "alter", "functional/alltypes", 1,
          events.get(0));
      assertEventEquals("@table", "select", "functional/alltypes", 1,
          events.get(1));
      assertEquals("compute stats FUNCTIONAL.ALLTYPES", events.get(0).getRequestData());
    }, "compute stats FUNCTIONAL.ALLTYPES",
        onTable("functional", "alltypes", TPrivilegeLevel.ALTER,
            TPrivilegeLevel.SELECT));

    // Recall that the view 'functional.complex_view' is created based on two underlying
    // tables, i.e., functional.alltypesagg and functional.alltypestiny. The following 3
    // test cases make sure the same audit log entry indicating a successful
    // authorization event is produced whether or not the requesting user is granted the
    // privileges on any of the underlying tables.
    authzOk(events -> {
      assertEquals(1, events.size());
      assertEventEquals("@table", "select", "functional/complex_view", 1,
          events.get(0));
      assertEquals("select count(*) from functional.complex_view",
          events.get(0).getRequestData());
    }, "select count(*) from functional.complex_view",
        onTable("functional", "complex_view", TPrivilegeLevel.SELECT));

    // The same audit log entry is produced if the requesting user is granted the
    // privilege on one of the underlying tables.
    authzOk(events -> {
      assertEquals(1, events.size());
      assertEventEquals("@table", "select", "functional/complex_view", 1,
          events.get(0));
      assertEquals("select count(*) from functional.complex_view",
          events.get(0).getRequestData());
    }, "select count(*) from functional.complex_view",
        onTable("functional", "complex_view", TPrivilegeLevel.SELECT),
        onTable("functional", "alltypesagg", TPrivilegeLevel.SELECT));

    // The same audit log entry is produced if the requesting user is granted the
    // privileges on all of the underlying tables.
    authzOk(events -> {
      assertEquals(1, events.size());
      assertEventEquals("@table", "select", "functional/complex_view", 1,
          events.get(0));
      assertEquals("select count(*) from functional.complex_view",
          events.get(0).getRequestData());
    }, "select count(*) from functional.complex_view",
        onTable("functional", "complex_view", TPrivilegeLevel.SELECT),
        onTable("functional", "alltypesagg", TPrivilegeLevel.SELECT),
        onTable("functional", "alltypestiny", TPrivilegeLevel.SELECT));
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

    authzError(events -> {
      // Show the actual view_metadata audit log.
      assertEquals(1, events.size());
      assertEventEquals("@table", "view_metadata", "functional/alltypes", 0,
          events.get(0));
      assertEquals("show partitions functional.alltypes", events.get(0).getRequestData());
    }, "show partitions functional.alltypes");
  }

  @Test
  public void testAuditsForColumnMasking() throws ImpalaException {
    String databaseName = "functional";
    String tableName = "alltypestiny";
    String policyNames[] = {"col_mask_custom", "col_mask_null", "col_mask_none",
        "col_mask_redact"};
    String columnNames[] = {"string_col", "date_string_col", "id", "year"};
    String users[] = {user_.getShortName(), user_.getShortName(), user_.getShortName(),
        "non_owner_2"};
    String masks[] = {
        "  {\n" +
        "    \"dataMaskType\": \"CUSTOM\",\n" +
        "    \"valueExpr\": \"concat({col}, 'xyz')\"\n" +
        "  }\n",
        "  {\n" +
        "    \"dataMaskType\": \"MASK_NULL\"\n" +
        "  }\n",
        // We add a mask of type "MASK_NONE" that corresponds to an "Unmasked" policy in
        // the Ranger UI to verify the respective AuthzAuditEvent is removed.
        "  {\n" +
        "    \"dataMaskType\": \"MASK_NONE\"\n" +
        "  }\n",
        // We add a mask of type "MASK" that corresponds to a "Redact" policy in the
        // Ranger UI for the user "non_owner_2" to verify the respective AuthzAuditEvent
        // is removed.
        "  {\n" +
        "    \"dataMaskType\": \"MASK\"\n" +
        "  }\n"
    };

    List<String> policies = new ArrayList<>();
    for (int i = 0; i < masks.length; ++i) {
      String json = String.format("{\n" +
          "  \"name\": \"%s\",\n" +
          "  \"policyType\": 1,\n" +
          "  \"serviceType\": \"%s\",\n" +
          "  \"service\": \"%s\",\n" +
          "  \"resources\": {\n" +
          "    \"database\": {\n" +
          "      \"values\": [\"%s\"],\n" +
          "      \"isExcludes\": false,\n" +
          "      \"isRecursive\": false\n" +
          "    },\n" +
          "    \"table\": {\n" +
          "      \"values\": [\"%s\"],\n" +
          "      \"isExcludes\": false,\n" +
          "      \"isRecursive\": false\n" +
          "    },\n" +
          "    \"column\": {\n" +
          "      \"values\": [\"%s\"],\n" +
          "      \"isExcludes\": false,\n" +
          "      \"isRecursive\": false\n" +
          "    }\n" +
          "  },\n" +
          "  \"dataMaskPolicyItems\": [\n" +
          "    {\n" +
          "      \"accesses\": [\n" +
          "        {\n" +
          "          \"type\": \"select\",\n" +
          "          \"isAllowed\": true\n" +
          "        }\n" +
          "      ],\n" +
          "      \"users\": [\"%s\"],\n" +
          "      \"dataMaskInfo\":\n" +
              "%s" +
          "    }\n" +
          "  ]\n" +
          "}", policyNames[i], RANGER_SERVICE_TYPE, RANGER_SERVICE_NAME, databaseName,
          tableName, columnNames[i], users[i], masks[i]);
      policies.add(json);
    }

    try {
      for (int i = 0; i < masks.length; ++i) {
        String policyName = policyNames[i];
        String json = policies.get(i);
        createRangerPolicy(policyName, json);
      }

      authzOk(events -> {
        assertEquals(15, events.size());
        assertEquals("select * from functional.alltypestiny",
            events.get(0).getRequestData());
        assertEventEquals("@table", "select", "functional/alltypestiny", 1,
            events.get(0));
        assertEventEquals("@column", "select", "functional/alltypestiny/id", 1,
            events.get(1));
        assertEventEquals("@column", "select", "functional/alltypestiny/bool_col", 1,
            events.get(2));
        assertEventEquals("@column", "select", "functional/alltypestiny/tinyint_col", 1,
            events.get(3));
        assertEventEquals("@column", "select", "functional/alltypestiny/smallint_col", 1,
            events.get(4));
        assertEventEquals("@column", "select", "functional/alltypestiny/int_col", 1,
            events.get(5));
        assertEventEquals("@column", "select", "functional/alltypestiny/bigint_col", 1,
            events.get(6));
        assertEventEquals("@column", "select", "functional/alltypestiny/float_col", 1,
            events.get(7));
        assertEventEquals("@column", "select", "functional/alltypestiny/double_col", 1,
            events.get(8));
        assertEventEquals("@column", "select", "functional/alltypestiny/string_col", 1,
            events.get(9));
        assertEventEquals("@column", "select", "functional/alltypestiny/timestamp_col", 1,
            events.get(10));
        assertEventEquals("@column", "select", "functional/alltypestiny/year", 1,
            events.get(11));
        assertEventEquals("@column", "select", "functional/alltypestiny/month", 1,
            events.get(12));
        assertEventEquals("@column", "mask_null",
            "functional/alltypestiny/date_string_col", 1, events.get(13));
        assertEventEquals("@column", "custom", "functional/alltypestiny/string_col", 1,
            events.get(14));
      }, "select * from functional.alltypestiny", onTable("functional", "alltypestiny",
          TPrivilegeLevel.SELECT));

      // This query results in 2 calls to RangerImpalaPlugin#evalDataMaskPolicies() for
      // the column of 'string_col' during the execution of SelectStmt#analyze(). One is
      // due to the analysis of the WithClause and the other is due to the analysis of
      // the enclosing SelectStmt. Two duplicate audit log entries are thus generated for
      // this column. This test verifies that the duplicate event is indeed removed,
      // i.e., only one entry for the column of 'string_col' having accessType as
      // 'custom'.
      // TODO (IMPALA-9661): Note that the audits of unused columns, e.g., 'tinyint_col'
      // and 'date_string_col' are also logged, indicating that the privilege check for
      // the column of 'tinyint_col' and the evaluation of the column masking policy for
      // the column of 'date_string_col' are still performed. We should remove those
      // unnecessary checks.
      authzOk(events -> {assertEquals(15, events.size());
        assertEquals("with iv as (select id, bool_col, string_col from " +
                "functional.alltypestiny) select * from iv",
            events.get(0).getRequestData());
        assertEventEquals("@table", "select", "functional/alltypestiny", 1,
            events.get(0));
        assertEventEquals("@column", "select", "functional/alltypestiny/id", 1,
            events.get(1));
        assertEventEquals("@column", "select", "functional/alltypestiny/bool_col", 1,
            events.get(2));
        assertEventEquals("@column", "select", "functional/alltypestiny/tinyint_col", 1,
            events.get(3));
        assertEventEquals("@column", "select", "functional/alltypestiny/smallint_col", 1,
            events.get(4));
        assertEventEquals("@column", "select", "functional/alltypestiny/int_col", 1,
            events.get(5));
        assertEventEquals("@column", "select", "functional/alltypestiny/bigint_col", 1,
            events.get(6));
        assertEventEquals("@column", "select", "functional/alltypestiny/float_col", 1,
            events.get(7));
        assertEventEquals("@column", "select", "functional/alltypestiny/double_col", 1,
            events.get(8));
        assertEventEquals("@column", "select", "functional/alltypestiny/string_col", 1,
            events.get(9));
        assertEventEquals("@column", "select", "functional/alltypestiny/timestamp_col", 1,
            events.get(10));
        assertEventEquals("@column", "select", "functional/alltypestiny/year", 1,
            events.get(11));
        assertEventEquals("@column", "select", "functional/alltypestiny/month", 1,
            events.get(12));
        assertEventEquals("@column", "mask_null",
            "functional/alltypestiny/date_string_col", 1, events.get(13));
        assertEventEquals("@column", "custom", "functional/alltypestiny/string_col", 1,
            events.get(14));
      }, "with iv as (select id, bool_col, string_col from functional.alltypestiny) " +
          "select * from iv", onTable("functional", "alltypestiny",
          TPrivilegeLevel.SELECT));

      // This query results in 6 calls to RangerImpalaPlugin#evalDataMaskPolicies() for
      // the column of 'string_col'. Two of them result from the first call to
      // SelectStmt#analyze() as described above. Two of them stem from the rewrite
      // operation of the subquery at StmtRewriter#SubqueryRewriter()#rewrite() and the
      // last two are due to the re-analysis of the query by SelectStmt#analyze().
      // TODO (IMPALA-9661): Remove checks for unused columns.
      authzOk(events -> {assertEquals(15, events.size());
        assertEquals("select id, string_col from functional.alltypestiny a " +
                "where exists (select id from functional.alltypestiny where id = a.id) " +
                "order by id;",
            events.get(0).getRequestData());
        assertEventEquals("@table", "select", "functional/alltypestiny", 1,
            events.get(0));
        assertEventEquals("@column", "select", "functional/alltypestiny/id", 1,
            events.get(1));
        assertEventEquals("@column", "select", "functional/alltypestiny/bool_col", 1,
            events.get(2));
        assertEventEquals("@column", "select", "functional/alltypestiny/tinyint_col", 1,
            events.get(3));
        assertEventEquals("@column", "select", "functional/alltypestiny/smallint_col", 1,
            events.get(4));
        assertEventEquals("@column", "select", "functional/alltypestiny/int_col", 1,
            events.get(5));
        assertEventEquals("@column", "select", "functional/alltypestiny/bigint_col", 1,
            events.get(6));
        assertEventEquals("@column", "select", "functional/alltypestiny/float_col", 1,
            events.get(7));
        assertEventEquals("@column", "select", "functional/alltypestiny/double_col", 1,
            events.get(8));
        assertEventEquals("@column", "select", "functional/alltypestiny/string_col", 1,
            events.get(9));
        assertEventEquals("@column", "select", "functional/alltypestiny/timestamp_col", 1,
            events.get(10));
        assertEventEquals("@column", "select", "functional/alltypestiny/year", 1,
            events.get(11));
        assertEventEquals("@column", "select", "functional/alltypestiny/month", 1,
            events.get(12));
        assertEventEquals("@column", "mask_null",
            "functional/alltypestiny/date_string_col", 1, events.get(13));
        assertEventEquals("@column", "custom", "functional/alltypestiny/string_col", 1,
            events.get(14));
      }, "select id, string_col from functional.alltypestiny a where exists " +
          "(select id from functional.alltypestiny where id = a.id) order by id;",
          onTable("functional", "alltypestiny", TPrivilegeLevel.SELECT));

      // When the requesting user is not granted the necessary privileges, the audit log
      // entries corresponding to column masking are not generated. Notice that in the
      // case when the requesting user is not granted the necessary privileges, only the
      // event of the first column that failed the authorization will be logged, i.e.,
      // 'id'. Refer to RangerAuthorizationChecker#authorizeTableAccess() for further
      // details.
      authzError(events -> {assertEquals(1, events.size());
            assertEquals("with iv as (select id, bool_col, string_col from " +
                    "functional.alltypestiny) select * from iv",
                events.get(0).getRequestData());
            assertEventEquals("@column", "select", "functional/alltypestiny/id", 0,
                events.get(0));
          },"with iv as (select id, bool_col, string_col from functional.alltypestiny) " +
          "select * from iv", onTable("functional", "alltypestiny"));
    } finally {
      for (int i = 0; i < masks.length; ++i) {
        String policyName = policyNames[i];
        deleteRangerPolicy(policyName);
      }
    }
  }

  @Test
  public void testAuditsForRowFiltering() throws ImpalaException {
    // Two row filter policies will be added. The first one affects 'user_' and keeps rows
    // of "functional.alltypestiny" satisfied "id=0". The second one affects user
    // "non_owner_2" and keeps rows of "functional.alltypes" satisfied "id=1".
    String databaseName = "functional";
    String tableNames[] = {"alltypestiny", "alltypes"};
    String policyNames[] = {"tiny_filter", "all_filter"};
    String users[] = {user_.getShortName(), "non_owner_2"};
    String filters[] = {"id=0", "id=1"};

    List<String> policies = new ArrayList<>();
    for (int i = 0; i < filters.length; ++i) {
      String json = String.format("{\n" +
          "  \"name\": \"%s\",\n" +
          "  \"policyType\": 2,\n" +
          "  \"serviceType\": \"%s\",\n" +
          "  \"service\": \"%s\",\n" +
          "  \"resources\": {\n" +
          "    \"database\": {\n" +
          "      \"values\": [\"%s\"],\n" +
          "      \"isExcludes\": false,\n" +
          "      \"isRecursive\": false\n" +
          "    },\n" +
          "    \"table\": {\n" +
          "      \"values\": [\"%s\"],\n" +
          "      \"isExcludes\": false,\n" +
          "      \"isRecursive\": false\n" +
          "    }\n" +
          "  },\n" +
          "  \"rowFilterPolicyItems\": [\n" +
          "    {\n" +
          "      \"accesses\": [\n" +
          "        {\n" +
          "          \"type\": \"select\",\n" +
          "          \"isAllowed\": true\n" +
          "        }\n" +
          "      ],\n" +
          "      \"users\": [\"%s\"],\n" +
          "      \"rowFilterInfo\": {\"filterExpr\": \"%s\"}\n" +
          "    }\n" +
          "  ]\n" +
          "}", policyNames[i], RANGER_SERVICE_TYPE, RANGER_SERVICE_NAME, databaseName,
          tableNames[i], users[i], filters[i]);
      policies.add(json);
    }
    try {
      for (int i = 0; i < filters.length; ++i) {
        String policyName = policyNames[i];
        String json = policies.get(i);
        createRangerPolicy(policyName, json);
      }

      authzOk(events -> {
        assertEquals(15, events.size());
        assertEquals("select * from functional.alltypestiny",
            events.get(0).getRequestData());
        assertEventEquals("@table", "select", "functional/alltypestiny", 1,
            events.get(0));
        assertEventEquals("@column", "select", "functional/alltypestiny/id", 1,
            events.get(1));
        assertEventEquals("@column", "select", "functional/alltypestiny/bool_col", 1,
            events.get(2));
        assertEventEquals("@column", "select", "functional/alltypestiny/tinyint_col", 1,
            events.get(3));
        assertEventEquals("@column", "select", "functional/alltypestiny/smallint_col", 1,
            events.get(4));
        assertEventEquals("@column", "select", "functional/alltypestiny/int_col", 1,
            events.get(5));
        assertEventEquals("@column", "select", "functional/alltypestiny/bigint_col", 1,
            events.get(6));
        assertEventEquals("@column", "select", "functional/alltypestiny/float_col", 1,
            events.get(7));
        assertEventEquals("@column", "select", "functional/alltypestiny/double_col", 1,
            events.get(8));
        assertEventEquals("@column", "select",
            "functional/alltypestiny/date_string_col", 1, events.get(9));
        assertEventEquals("@column", "select", "functional/alltypestiny/string_col", 1,
            events.get(10));
        assertEventEquals("@column", "select", "functional/alltypestiny/timestamp_col", 1,
            events.get(11));
        assertEventEquals("@column", "select", "functional/alltypestiny/year", 1,
            events.get(12));
        assertEventEquals("@column", "select", "functional/alltypestiny/month", 1,
            events.get(13));
        assertEventEquals("@table", "row_filter", "functional/alltypestiny", 1,
            events.get(14));
      }, "select * from functional.alltypestiny", onTable("functional", "alltypestiny",
          TPrivilegeLevel.SELECT));

      authzOk(events -> {
        assertEquals(14, events.size());
        assertEquals("select * from functional.alltypes",
            events.get(0).getRequestData());
        assertEventEquals("@table", "select", "functional/alltypes", 1,
            events.get(0));
        assertEventEquals("@column", "select", "functional/alltypes/id", 1,
            events.get(1));
        assertEventEquals("@column", "select", "functional/alltypes/bool_col", 1,
            events.get(2));
        assertEventEquals("@column", "select", "functional/alltypes/tinyint_col", 1,
            events.get(3));
        assertEventEquals("@column", "select", "functional/alltypes/smallint_col", 1,
            events.get(4));
        assertEventEquals("@column", "select", "functional/alltypes/int_col", 1,
            events.get(5));
        assertEventEquals("@column", "select", "functional/alltypes/bigint_col", 1,
            events.get(6));
        assertEventEquals("@column", "select", "functional/alltypes/float_col", 1,
            events.get(7));
        assertEventEquals("@column", "select", "functional/alltypes/double_col", 1,
            events.get(8));
        assertEventEquals("@column", "select", "functional/alltypes/date_string_col", 1,
            events.get(9));
        assertEventEquals("@column", "select", "functional/alltypes/string_col", 1,
            events.get(10));
        assertEventEquals("@column", "select", "functional/alltypes/timestamp_col", 1,
            events.get(11));
        assertEventEquals("@column", "select", "functional/alltypes/year", 1,
            events.get(12));
        assertEventEquals("@column", "select", "functional/alltypes/month", 1,
            events.get(13));
      }, "select * from functional.alltypes", onTable("functional", "alltypes",
          TPrivilegeLevel.SELECT));

      // When fails with not enough privileges, no audit logs for row filtering is
      // generated. Only the event of the first column (i.e. id) that failed the
      // authorization will be logged.
      authzError(events -> {assertEquals(1, events.size());
        assertEquals("select * from functional.alltypestiny",
            events.get(0).getRequestData());
        assertEventEquals("@column", "select", "functional/alltypestiny/id", 0,
            events.get(0));
      },"select * from functional.alltypestiny", onTable("functional", "alltypestiny"));
    } finally {
      for (int i = 0; i < filters.length; ++i) {
        String policyName = policyNames[i];
        deleteRangerPolicy(policyName);
      }
    }
  }

  private void authzOk(Consumer<List<AuthzAuditEvent>> resultChecker, String stmt,
      TPrivilege[]... privileges) throws ImpalaException {
    authorize(stmt).ok(privileges);
    RangerAuthorizationContext rangerCtx =
        (RangerAuthorizationContext) authzChecker_.authzCtx_;
    Preconditions.checkNotNull(rangerCtx);
    Preconditions.checkNotNull(rangerCtx.getAuditHandler());
    resultChecker.accept(rangerCtx.getAuditHandler().getAuthzEvents());
  }

  private void authzError(Consumer<List<AuthzAuditEvent>> resultChecker, String stmt,
      TPrivilege[]... privileges) throws ImpalaException {
    authorize(stmt).error("", privileges);
    RangerAuthorizationContext rangerCtx =
        (RangerAuthorizationContext) authzChecker_.authzCtx_;
    Preconditions.checkNotNull(rangerCtx);
    Preconditions.checkNotNull(rangerCtx.getAuditHandler());
    resultChecker.accept(rangerCtx.getAuditHandler().getAuthzEvents());
  }

  private static void assertEventEquals(String resourceType, String accessType,
      String resourcePath, int accessResult, AuthzAuditEvent event) {
    assertEquals(resourceType, event.getResourceType());
    assertEquals(accessType, event.getAccessType());
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
        // Do not create a new instance if we already have one. This is consistent with
        // RangerAuthorizationFactory#newAuthorizationChecker().
        if (authzChecker_ == null) {
          authzChecker_ = new RangerAuthorizationCheckerSpy(authzConfig_);
        }
        return authzChecker_;
      }
    };
  }
}
