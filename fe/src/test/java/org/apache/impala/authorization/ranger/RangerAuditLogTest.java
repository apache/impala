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
import org.junit.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.function.Consumer;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;

public class RangerAuditLogTest extends AuthorizationTestBase {
  private static RangerAuthorizationCheckerSpy authzChecker_ = null;

  private static class RangerAuthorizationCheckerSpy extends RangerAuthorizationChecker {
    private AuthorizationContext authzCtx_;

    public RangerAuthorizationCheckerSpy(AuthorizationConfig authzConfig) {
      super(authzConfig);
    }

    @Override
    public void postAuthorize(AuthorizationContext authzCtx, boolean authzOk,
        boolean analysisOk) {
      super.postAuthorize(authzCtx, authzOk, analysisOk);
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
      assertEquals(2, events.size());
      assertEventEquals("@table", "select", "functional/alltypes", 1, events.get(0));
      assertEventEquals("@column", "select","functional/alltypes/id,string_col", 1,
          events.get(1));
      assertEquals("select id, string_col from functional.alltypes",
          events.get(0).getRequestData());
    }, "select id, string_col from functional.alltypes",
        onTable("functional", "alltypes", TPrivilegeLevel.SELECT));

    // Audit log entries are not consolidated since each of them corresponds to a
    // different policy.
    authzOk(events -> {
      assertEquals(13, events.size());
      assertEventEquals("@column", "select",
          "functional/alltypes/id", 1, events.get(0));
      assertEventEquals("@column", "select",
          "functional/alltypes/bool_col", 1, events.get(1));
      assertEventEquals("@column", "select",
          "functional/alltypes/tinyint_col", 1, events.get(2));
      assertEventEquals("@column", "select",
          "functional/alltypes/smallint_col", 1, events.get(3));
      assertEventEquals("@column", "select",
          "functional/alltypes/int_col", 1, events.get(4));
      assertEventEquals("@column", "select",
          "functional/alltypes/bigint_col", 1, events.get(5));
      assertEventEquals("@column", "select",
          "functional/alltypes/float_col", 1, events.get(6));
      assertEventEquals("@column", "select",
          "functional/alltypes/double_col", 1, events.get(7));
      assertEventEquals("@column", "select",
          "functional/alltypes/date_string_col", 1, events.get(8));
      assertEventEquals("@column", "select",
          "functional/alltypes/string_col", 1, events.get(9));
      assertEventEquals("@column", "select",
          "functional/alltypes/timestamp_col", 1, events.get(10));
      assertEventEquals("@column", "select",
          "functional/alltypes/year", 1, events.get(11));
      assertEventEquals("@column", "select",
          "functional/alltypes/month", 1, events.get(12));
      assertEquals("select * from functional.alltypes",
          events.get(0).getRequestData());
    }, "select * from functional.alltypes",
        onColumn("functional", "alltypes", "id", TPrivilegeLevel.SELECT),
        onColumn("functional", "alltypes", "bool_col", TPrivilegeLevel.SELECT),
        onColumn("functional", "alltypes", "tinyint_col", TPrivilegeLevel.SELECT),
        onColumn("functional", "alltypes", "smallint_col", TPrivilegeLevel.SELECT),
        onColumn("functional", "alltypes", "int_col", TPrivilegeLevel.SELECT),
        onColumn("functional", "alltypes", "bigint_col", TPrivilegeLevel.SELECT),
        onColumn("functional", "alltypes", "float_col", TPrivilegeLevel.SELECT),
        onColumn("functional", "alltypes", "double_col", TPrivilegeLevel.SELECT),
        onColumn("functional", "alltypes", "date_string_col", TPrivilegeLevel.SELECT),
        onColumn("functional", "alltypes", "string_col", TPrivilegeLevel.SELECT),
        onColumn("functional", "alltypes", "timestamp_col", TPrivilegeLevel.SELECT),
        onColumn("functional", "alltypes", "year", TPrivilegeLevel.SELECT),
        onColumn("functional", "alltypes", "month", TPrivilegeLevel.SELECT));

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
      assertEventEquals("@udf", "refresh", "*/*", 1, events.get(0));
      assertEquals("invalidate metadata", events.get(0).getRequestData());
      assertEventEquals("@url", "refresh", "*", 1, events.get(1));
      assertEquals("invalidate metadata", events.get(1).getRequestData());
      assertEventEquals("@column", "refresh", "*/*/*", 1, events.get(2));
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

    // We won't have duplicate events for a column in multiple FunctionCallExpr's since
    // Analyzer#registerSlotRef() would return if the SlotDescriptor associated with the
    // fully-qualified path to the column has already been added to 'slotPathMap_'. Hence
    // Analyzer#createAndRegisterSlotDesc(), the method that registers the
    // PrivilegeRequest for the column, would not be called again to produce another
    // AuthzAuditEvent for the same column.
    authzOk(events -> {
      assertEquals(2, events.size());
      assertEventEquals("@table", "select", "functional/alltypes", 1,
          events.get(0));
      assertEventEquals("@column", "select", "functional/alltypes/id", 1,
          events.get(1));
    }, "select min(id), max(id), avg(id) from functional.alltypes",
        onTable("functional", "alltypes", TPrivilegeLevel.SELECT));

    // We won't have duplicate events for a column even if the column appears in multiple
    // SelectStmt's in a query since we are using a Set to register the
    // PrivilegeRequest's in the query.
    authzOk(events -> {
      assertEquals(2, events.size());
      assertEventEquals("@table", "select", "functional/alltypes", 1,
          events.get(0));
      assertEventEquals("@column", "select", "functional/alltypes/id", 1,
          events.get(1));
    }, "select min(id) from functional.alltypes union all " +
        "select max(id) from functional.alltypes",
        onTable("functional", "alltypes", TPrivilegeLevel.SELECT));

    // No audit log entry should be produced for an authorized query against a
    // non-existing table.
    authzOk(events -> {
      assertEquals(0, events.size());
    }, "select * from functional.non_existing_tbl",
        /* expectAnalysisOk */ false, onDatabase("functional", TPrivilegeLevel.SELECT));
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
      // Only log the table event.
      assertEquals(1, events.size());
      assertEventEquals("@table", "select", "functional/alltypes", 0, events.get(0));
      assertEquals("select id, string_col from functional.alltypes",
          events.get(0).getRequestData());
    }, "select id, string_col from functional.alltypes");

    authzError(events -> {
      // Log the table event even when the table does not exist.
      assertEquals(1, events.size());
      assertEventEquals("@table", "select", "functional/non_existing_tbl", 0,
          events.get(0));
      assertEquals("select * from functional.non_existing_tbl",
          events.get(0).getRequestData());
    }, "select * from functional.non_existing_tbl");

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
    long policyIds[] = {-1, -1, -1, -1};
    Set<Long> columnMaskingPolicyIds = new HashSet<>();

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
        policyIds[i] = createRangerPolicy(policyName, json);
        assertNotEquals("Illegal policy id", -1, policyIds[i]);
        // Only the first 3 policies apply on current user.
        if (i < 3) columnMaskingPolicyIds.add(policyIds[i]);
      }

      authzOk(events -> {
        assertEquals(3, events.size());
        assertEquals("select id, bool_col, string_col from functional.alltypestiny",
        events.get(0).getRequestData());
        assertEventEquals("@table", "select", "functional/alltypestiny", 1,
            events.get(0));
        assertEventEquals("@column", "select",
            "functional/alltypestiny/id,bool_col,string_col", 1,
            events.get(1));
        assertEventEquals("@column", "custom", "functional/alltypestiny/string_col", 1,
            events.get(2));
        assertEquals(events.get(2).getPolicyId(), policyIds[0]);
      }, "select id, bool_col, string_col from functional.alltypestiny",
          onTable("functional", "alltypestiny", TPrivilegeLevel.SELECT));

      authzOk(events -> {
        assertEquals(4, events.size());
        assertEquals("select * from functional.alltypestiny",
            events.get(0).getRequestData());
        assertEventEquals("@table", "select", "functional/alltypestiny", 1,
            events.get(0));
        assertEventEquals("@column", "select",
            "functional/alltypestiny/id,bool_col,tinyint_col,smallint_col,int_col," +
            "bigint_col,float_col,double_col,date_string_col,string_col,timestamp_col," +
            "year,month", 1, events.get(1));
        assertEventEquals("@column", "mask_null",
            "functional/alltypestiny/date_string_col", 1, events.get(2));
        assertEquals(events.get(2).getPolicyId(), policyIds[1]);
        assertEventEquals("@column", "custom", "functional/alltypestiny/string_col", 1,
            events.get(3));
        assertEquals(events.get(3).getPolicyId(), policyIds[0]);
      }, "select * from functional.alltypestiny", onTable("functional", "alltypestiny",
          TPrivilegeLevel.SELECT));

      // This query results in 2 calls to RangerImpalaPlugin#evalDataMaskPolicies() for
      // the column of 'string_col' during the execution of SelectStmt#analyze(). One is
      // due to the analysis of the WithClause and the other is due to the analysis of
      // the enclosing SelectStmt. Two duplicate audit log entries are thus generated for
      // this column. This test verifies that the duplicate event is indeed removed,
      // i.e., only one entry for the column of 'string_col' having accessType as
      // 'custom'.
      authzOk(events -> {assertEquals(3, events.size());
        assertEquals("with iv as (select id, bool_col, string_col from " +
                "functional.alltypestiny) select * from iv",
            events.get(0).getRequestData());
        assertEventEquals("@table", "select", "functional/alltypestiny", 1,
            events.get(0));
        assertEventEquals("@column", "select",
            "functional/alltypestiny/id,bool_col,string_col", 1,
            events.get(1));
        assertEventEquals("@column", "custom", "functional/alltypestiny/string_col", 1,
            events.get(2));
        assertEquals(events.get(2).getPolicyId(), policyIds[0]);
      }, "with iv as (select id, bool_col, string_col from functional.alltypestiny) " +
          "select * from iv", onTable("functional", "alltypestiny",
          TPrivilegeLevel.SELECT));

      // Test on masking subquery. No redundant audit events.
      authzOk(events -> {assertEquals(3, events.size());
        assertEquals("select id, string_col from functional.alltypestiny a " +
                "where exists (select id from functional.alltypestiny where id = a.id) " +
                "order by id;",
            events.get(0).getRequestData());
        assertEventEquals("@table", "select", "functional/alltypestiny", 1,
            events.get(0));
        assertEventEquals("@column", "select",
            "functional/alltypestiny/id,string_col", 1,
            events.get(1));
        assertEventEquals("@column", "custom", "functional/alltypestiny/string_col", 1,
            events.get(2));
        assertEquals(events.get(2).getPolicyId(), policyIds[0]);
      }, "select id, string_col from functional.alltypestiny a where exists " +
          "(select id from functional.alltypestiny where id = a.id) order by id;",
          onTable("functional", "alltypestiny", TPrivilegeLevel.SELECT));

      // When the requesting user is not granted the necessary privileges, the audit log
      // entries corresponding to column masking are not generated. Notice that in the
      // case when the requesting user is not granted the necessary privileges, only the
      // event of the table that failed the authorization will be logged. Refer to
      // RangerAuthorizationChecker#authorizeTableAccess() for further details.
      authzError(events -> {
        assertEquals(1, events.size());
        assertEquals("with iv as (select id, bool_col, string_col from " +
                "functional.alltypestiny) select * from iv",
            events.get(0).getRequestData());
        assertEventEquals("@table", "select", "functional/alltypestiny", 0,
            events.get(0));
      },"with iv as (select id, bool_col, string_col from functional.alltypestiny) " +
          "select * from iv", onTable("functional", "alltypestiny"));

      // Updates on metadata fails by column-masking policies.
      authzError(events -> {
        assertEquals(1, events.size());
        assertEquals("invalidate metadata functional.alltypestiny",
            events.get(0).getRequestData());
        assertEventEquals("@table", "refresh", "functional/alltypestiny", 0,
            events.get(0));
        // Make sure it's denied by a column masking policy.
        assertTrue(columnMaskingPolicyIds.contains(events.get(0).getPolicyId()));
      }, "invalidate metadata functional.alltypestiny", onServer(TPrivilegeLevel.ALL));

      // Updates on metadata fails by column-masking policies.
      authzError(events -> {
        assertEquals(1, events.size());
        assertEquals("compute stats functional.alltypestiny",
            events.get(0).getRequestData());
        assertEventEquals("@table", "alter", "functional/alltypestiny", 0,
            events.get(0));
        // Make sure it's denied by a column masking policy.
        assertTrue(columnMaskingPolicyIds.contains(events.get(0).getPolicyId()));
      }, "compute stats functional.alltypestiny", onServer(TPrivilegeLevel.ALL));

      // Updates on metadata fails by column-masking policies.
      authzError(events -> {
        assertEquals(1, events.size());
        assertEquals("alter table functional.alltypestiny change column id id string",
            events.get(0).getRequestData());
        assertEventEquals("@table", "alter", "functional/alltypestiny", 0,
            events.get(0));
        // Make sure it's denied by a column masking policy.
        assertTrue(columnMaskingPolicyIds.contains(events.get(0).getPolicyId()));
      }, "alter table functional.alltypestiny change column id id string",
          onServer(TPrivilegeLevel.ALL));

      // Updates on data fails by column-masking policies.
      authzError(events -> {
        assertEquals(1, events.size());
        assertEquals("truncate table functional.alltypestiny",
            events.get(0).getRequestData());
        assertEventEquals("@table", "insert", "functional/alltypestiny", 0,
            events.get(0));
        // Make sure it's denied by a column masking policy.
        assertTrue(columnMaskingPolicyIds.contains(events.get(0).getPolicyId()));
      }, "truncate table functional.alltypestiny", onServer(TPrivilegeLevel.ALL));

      // Updates on data fails by column-masking policies.
      authzError(events -> {
        assertEquals(1, events.size());
        assertEquals("insert into functional.alltypestiny partition(year, month) " +
                "select * from functional.alltypes",
            events.get(0).getRequestData());
        assertEventEquals("@table", "insert", "functional/alltypestiny", 0,
            events.get(0));
        // Make sure it's denied by a column masking policy.
        assertTrue(columnMaskingPolicyIds.contains(events.get(0).getPolicyId()));
      }, "insert into functional.alltypestiny partition(year, month) " +
          "select * from functional.alltypes", onServer(TPrivilegeLevel.ALL));
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
    long policyIds[] = {-1, -1};

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
        policyIds[i] = createRangerPolicy(policyName, json);
        assertNotEquals("Illegal policy id", -1, policyIds[i]);
      }

      // Verify row filter audits. Note that columns used in the row filter also creates
      // column access audits.
      authzOk(events -> {
        assertEquals(3, events.size());
        assertEquals("select bool_col from functional.alltypestiny",
            events.get(0).getRequestData());
        assertEventEquals("@table", "select", "functional/alltypestiny", 1,
            events.get(0));
        assertEventEquals("@column", "select", "functional/alltypestiny/bool_col,id", 1,
            events.get(1));
        assertEventEquals("@table", "row_filter", "functional/alltypestiny", 1,
            events.get(2));
        assertEquals(events.get(2).getPolicyId(), policyIds[0]);
      }, "select bool_col from functional.alltypestiny",
          onTable("functional", "alltypestiny", TPrivilegeLevel.SELECT));

      authzOk(events -> {
        assertEquals(3, events.size());
        assertEquals("select 1 from functional.alltypestiny",
            events.get(0).getRequestData());
        assertEventEquals("@table", "select", "functional/alltypestiny", 1,
            events.get(0));
        assertEventEquals("@column", "select", "functional/alltypestiny/id", 1,
            events.get(1));
        assertEventEquals("@table", "row_filter", "functional/alltypestiny", 1,
            events.get(2));
        assertEquals(events.get(2).getPolicyId(), policyIds[0]);
      }, "select 1 from functional.alltypestiny",
          onTable("functional", "alltypestiny", TPrivilegeLevel.SELECT));

      authzOk(events -> {
        assertEquals(3, events.size());
        assertEquals("select count(*) from functional.alltypestiny",
            events.get(0).getRequestData());
        assertEventEquals("@table", "select", "functional/alltypestiny", 1,
            events.get(0));
        assertEventEquals("@column", "select", "functional/alltypestiny/id", 1,
            events.get(1));
        assertEventEquals("@table", "row_filter", "functional/alltypestiny", 1,
            events.get(2));
        assertEquals(events.get(2).getPolicyId(), policyIds[0]);
      }, "select count(*) from functional.alltypestiny",
          onTable("functional", "alltypestiny", TPrivilegeLevel.SELECT));

      // No row filters applied to current user. So no audits of row_filter.
      authzOk(events -> {
        assertEquals(1, events.size());
        assertEquals("select count(*) from functional.alltypes",
            events.get(0).getRequestData());
        assertEventEquals("@table", "select", "functional/alltypes", 1,
            events.get(0));
      }, "select count(*) from functional.alltypes",
          onTable("functional", "alltypes", TPrivilegeLevel.SELECT));

      authzOk(events -> {
        assertEquals(3, events.size());
        assertEquals("select * from functional.alltypestiny",
            events.get(0).getRequestData());
        assertEventEquals("@table", "select", "functional/alltypestiny", 1,
            events.get(0));
        assertEventEquals("@column", "select",
            "functional/alltypestiny/id,bool_col,tinyint_col,smallint_col,int_col," +
            "bigint_col,float_col,double_col,date_string_col,string_col,timestamp_col," +
            "year,month", 1, events.get(1));
        assertEventEquals("@table", "row_filter", "functional/alltypestiny", 1,
            events.get(2));
        assertEquals(events.get(2).getPolicyId(), policyIds[0]);
      }, "select * from functional.alltypestiny", onTable("functional", "alltypestiny",
          TPrivilegeLevel.SELECT));

      // No row filters applied to current user. So no audits of row_filter.
      authzOk(events -> {
        assertEquals(2, events.size());
        assertEquals("select * from functional.alltypes",
            events.get(0).getRequestData());
        assertEventEquals("@table", "select", "functional/alltypes", 1,
            events.get(0));
        assertEventEquals("@column", "select",
            "functional/alltypes/id,bool_col,tinyint_col,smallint_col,int_col," +
            "bigint_col,float_col,double_col,date_string_col,string_col,timestamp_col," +
            "year,month", 1, events.get(1));
      }, "select * from functional.alltypes", onTable("functional", "alltypes",
          TPrivilegeLevel.SELECT));

      // When fails with not enough privileges, no audit logs for row filtering is
      // generated. Only the event of the table that failed the authorization will be
      // logged.
      authzError(events -> {
        assertEquals(1, events.size());
        assertEquals("select * from functional.alltypestiny",
            events.get(0).getRequestData());
        assertEventEquals("@table", "select", "functional/alltypestiny", 0,
            events.get(0));
      },"select * from functional.alltypestiny", onTable("functional", "alltypestiny"));

      // Updates on metadata fails by row-filtering policies.
      authzError(events -> {
        assertEquals(1, events.size());
        assertEquals("invalidate metadata functional.alltypestiny",
            events.get(0).getRequestData());
        assertEventEquals("@table", "refresh", "functional/alltypestiny", 0,
            events.get(0));
        // Make sure it's denied by the row filtering policy.
        assertEquals(events.get(0).getPolicyId(), policyIds[0]);
      }, "invalidate metadata functional.alltypestiny", onServer(TPrivilegeLevel.ALL));

      // Updates on metadata fails by row-filtering policies.
      authzError(events -> {
        assertEquals(1, events.size());
        assertEquals("compute stats functional.alltypestiny",
            events.get(0).getRequestData());
        assertEventEquals("@table", "alter", "functional/alltypestiny", 0,
            events.get(0));
        // Make sure it's denied by the row filtering policy.
        assertEquals(events.get(0).getPolicyId(), policyIds[0]);
      }, "compute stats functional.alltypestiny", onServer(TPrivilegeLevel.ALL));

      // Updates on metadata fails by row-filtering policies.
      authzError(events -> {
        assertEquals(1, events.size());
        assertEquals("alter table functional.alltypestiny change column id id string",
            events.get(0).getRequestData());
        assertEventEquals("@table", "alter", "functional/alltypestiny", 0,
            events.get(0));
        // Make sure it's denied by the row filtering policy.
        assertEquals(events.get(0).getPolicyId(), policyIds[0]);
      }, "alter table functional.alltypestiny change column id id string",
          onServer(TPrivilegeLevel.ALL));

      // Updates on data fails by row-filtering policies.
      authzError(events -> {
        assertEquals(1, events.size());
        assertEquals("truncate table functional.alltypestiny",
            events.get(0).getRequestData());
        assertEventEquals("@table", "insert", "functional/alltypestiny", 0,
            events.get(0));
        // Make sure it's denied by the row filtering policy.
        assertEquals(events.get(0).getPolicyId(), policyIds[0]);
      }, "truncate table functional.alltypestiny", onServer(TPrivilegeLevel.ALL));

      // Updates on data fails by row-filtering policies.
      authzError(events -> {
        assertEquals(1, events.size());
        assertEquals("insert into functional.alltypestiny partition(year, month) " +
                "select * from functional.alltypes",
            events.get(0).getRequestData());
        assertEventEquals("@table", "insert", "functional/alltypestiny", 0,
            events.get(0));
        // Make sure it's denied by the row filtering policy.
        assertEquals(events.get(0).getPolicyId(), policyIds[0]);
      }, "insert into functional.alltypestiny partition(year, month) " +
          "select * from functional.alltypes", onServer(TPrivilegeLevel.ALL));
    } finally {
      for (int i = 0; i < filters.length; ++i) {
        String policyName = policyNames[i];
        try {
          deleteRangerPolicy(policyName);
        } catch (RuntimeException e) {
          // ignore this to expose the original error.
        }
      }
    }
  }

  private void authzOk(Consumer<List<AuthzAuditEvent>> resultChecker, String stmt,
      TPrivilege[]... privileges) throws ImpalaException {
    authzOk(resultChecker, stmt, /* expectAnalysisOk */ true, privileges);
  }

  private void authzOk(Consumer<List<AuthzAuditEvent>> resultChecker, String stmt,
      boolean expectAnalysisOk, TPrivilege[]... privileges) throws ImpalaException {
      authorize(stmt).ok(expectAnalysisOk, privileges);
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
