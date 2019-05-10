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

package org.apache.impala.authorization;

import com.google.common.collect.Sets;
import org.apache.commons.lang.ArrayUtils;
import org.apache.impala.analysis.AnalysisContext;
import org.apache.impala.analysis.AnalysisContext.AnalysisResult;
import org.apache.impala.catalog.ScalarFunction;
import org.apache.impala.catalog.Type;
import org.apache.impala.common.AnalysisException;
import org.apache.impala.common.ImpalaException;
import org.apache.impala.common.RuntimeEnv;
import org.apache.impala.thrift.TDescribeOutputStyle;
import org.apache.impala.thrift.TPrivilegeLevel;
import org.apache.impala.thrift.TQueryOptions;
import org.apache.impala.thrift.TTableName;
import org.apache.sentry.api.service.thrift.TSentryRole;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * This class contains authorization tests for SQL statements.
 */
@RunWith(Parameterized.class)
public class AuthorizationStmtTest extends AuthorizationTestBase {
  public AuthorizationStmtTest(AuthorizationProvider authzProvider)
      throws ImpalaException {
    super(authzProvider);
  }

  @BeforeClass
  public static void setUp() {
    RuntimeEnv.INSTANCE.setTestEnv(true);
  }

  @AfterClass
  public static void cleanUp() {
    RuntimeEnv.INSTANCE.reset();
  }

  @Before
  public void before() throws ImpalaException {
    if (authzProvider_ == AuthorizationProvider.SENTRY) {
      // Remove existing roles in order to not interfere with these tests.
      for (TSentryRole role : sentryService_.listAllRoles(USER)) {
        authzCatalog_.removeRole(role.getRoleName());
      }
    }
  }

  @Parameters
  public static Collection<AuthorizationProvider> data() {
    return Arrays.asList(AuthorizationProvider.SENTRY, AuthorizationProvider.RANGER);
  }

  private static final String[] ALLTYPES_COLUMNS_WITHOUT_ID = new String[]{"bool_col",
      "tinyint_col", "smallint_col", "int_col", "bigint_col", "float_col", "double_col",
      "date_string_col", "string_col", "timestamp_col", "year", "month"};

  private static final String[] ALLTYPES_COLUMNS = (String[]) ArrayUtils.addAll(
      new String[]{"id"}, ALLTYPES_COLUMNS_WITHOUT_ID);

  @Test
  public void testPrivilegeRequests() throws ImpalaException {
    // Used for select *, with, and union
    Set<String> expectedAuthorizables = Sets.newHashSet(
        "functional.alltypes",
        "functional.alltypes.id",
        "functional.alltypes.bool_col",
        "functional.alltypes.tinyint_col",
        "functional.alltypes.smallint_col",
        "functional.alltypes.int_col",
        "functional.alltypes.bigint_col",
        "functional.alltypes.float_col",
        "functional.alltypes.double_col",
        "functional.alltypes.date_string_col",
        "functional.alltypes.string_col",
        "functional.alltypes.timestamp_col",
        "functional.alltypes.year",
        "functional.alltypes.month"
    );
    // Select *
    verifyPrivilegeReqs("select * from functional.alltypes", expectedAuthorizables);
    verifyPrivilegeReqs("select alltypes.* from functional.alltypes",
        expectedAuthorizables);
    verifyPrivilegeReqs(createAnalysisCtx("functional"), "select * from alltypes",
        expectedAuthorizables);
    verifyPrivilegeReqs(createAnalysisCtx("functional"),
        "select alltypes.* from alltypes", expectedAuthorizables);
    verifyPrivilegeReqs("select a.* from functional.alltypes a", expectedAuthorizables);

    // With clause.
    verifyPrivilegeReqs("with t as (select * from functional.alltypes) select * from t",
        expectedAuthorizables);
    verifyPrivilegeReqs(createAnalysisCtx("functional"),
        "with t as (select * from alltypes) select * from t", expectedAuthorizables);

    // Union.
    verifyPrivilegeReqs("select * from functional.alltypes union all " +
        "select * from functional.alltypes", expectedAuthorizables);
    verifyPrivilegeReqs(createAnalysisCtx("functional"),
        "select * from alltypes union all select * from alltypes",
        expectedAuthorizables);

    // Describe
    expectedAuthorizables = Sets.newHashSet("functional.alltypes.*");
    verifyPrivilegeReqs("describe functional.alltypes", expectedAuthorizables);
    verifyPrivilegeReqs(createAnalysisCtx("functional"), "describe alltypes",
        expectedAuthorizables);

    // Select a specific column.
    expectedAuthorizables = Sets.newHashSet(
        "functional.alltypes",
        "functional.alltypes.id"
    );
    verifyPrivilegeReqs("select id from functional.alltypes", expectedAuthorizables);
    verifyPrivilegeReqs("select alltypes.id from functional.alltypes",
        expectedAuthorizables);
    verifyPrivilegeReqs(createAnalysisCtx("functional"),
        "select alltypes.id from alltypes", expectedAuthorizables);
    verifyPrivilegeReqs(createAnalysisCtx("functional"),
        "select id from alltypes", expectedAuthorizables);
    verifyPrivilegeReqs("select alltypes.id from functional.alltypes",
        expectedAuthorizables);
    verifyPrivilegeReqs("select a.id from functional.alltypes a", expectedAuthorizables);

    // Insert.
    expectedAuthorizables = Sets.newHashSet("functional.alltypes");
    verifyPrivilegeReqs("insert into functional.alltypes(id) partition(month, year) " +
        "values(1, 1, 2018)", expectedAuthorizables);
    verifyPrivilegeReqs(createAnalysisCtx("functional"),
        "insert into alltypes(id) partition(month, year) values(1, 1, 2018)",
        expectedAuthorizables);

    // Insert with constant select.
    expectedAuthorizables = Sets.newHashSet("functional.zipcode_incomes");
    verifyPrivilegeReqs("insert into functional.zipcode_incomes(id) select '123'",
        expectedAuthorizables);
    verifyPrivilegeReqs(createAnalysisCtx("functional"),
        "insert into zipcode_incomes(id) select '123'", expectedAuthorizables);

    // Truncate.
    expectedAuthorizables = Sets.newHashSet("functional.alltypes");
    verifyPrivilegeReqs("truncate table functional.alltypes", expectedAuthorizables);
    verifyPrivilegeReqs(createAnalysisCtx("functional"),
        "truncate table alltypes", expectedAuthorizables);

    // Load
    expectedAuthorizables = Sets.newHashSet(
        "functional.alltypes",
        "hdfs://localhost:20500/test-warehouse/tpch.lineitem"
    );
    verifyPrivilegeReqs("load data inpath " +
        "'hdfs://localhost:20500/test-warehouse/tpch.lineitem' " +
        "into table functional.alltypes partition(month=10, year=2009)",
        expectedAuthorizables);
    verifyPrivilegeReqs(createAnalysisCtx("functional"), "load data inpath " +
        "'hdfs://localhost:20500/test-warehouse/tpch.lineitem' " +
        "into table alltypes partition(month=10, year=2009)",
        expectedAuthorizables);

    // Reset metadata.
    expectedAuthorizables = Sets.newHashSet("functional.alltypes");
    verifyPrivilegeReqs("invalidate metadata functional.alltypes", expectedAuthorizables);
    verifyPrivilegeReqs(createAnalysisCtx("functional"),
        "invalidate metadata alltypes", expectedAuthorizables);
    verifyPrivilegeReqs("refresh functional.alltypes", expectedAuthorizables);
    verifyPrivilegeReqs(createAnalysisCtx("functional"), "refresh alltypes",
        expectedAuthorizables);

    // Show tables.
    expectedAuthorizables = Sets.newHashSet("functional.*.*");
    verifyPrivilegeReqs("show tables in functional", expectedAuthorizables);
    verifyPrivilegeReqs(createAnalysisCtx("functional"), "show tables",
        expectedAuthorizables);

    // Show partitions.
    expectedAuthorizables = Sets.newHashSet("functional.alltypes");
    verifyPrivilegeReqs("show partitions functional.alltypes", expectedAuthorizables);
    verifyPrivilegeReqs(createAnalysisCtx("functional"),
        "show partitions alltypes", expectedAuthorizables);

    // Show range partitions.
    expectedAuthorizables = Sets.newHashSet("functional_kudu.dimtbl");
    verifyPrivilegeReqs("show range partitions functional_kudu.dimtbl",
        expectedAuthorizables);
    verifyPrivilegeReqs(createAnalysisCtx("functional_kudu"),
        "show range partitions dimtbl", expectedAuthorizables);

    // Show table stats.
    expectedAuthorizables = Sets.newHashSet("functional.alltypes");
    verifyPrivilegeReqs("show table stats functional.alltypes",
        expectedAuthorizables);
    verifyPrivilegeReqs(createAnalysisCtx("functional"),
        "show table stats alltypes", expectedAuthorizables);

    // Show column stats.
    expectedAuthorizables = Sets.newHashSet("functional.alltypes");
    verifyPrivilegeReqs("show column stats functional.alltypes", expectedAuthorizables);
    verifyPrivilegeReqs(createAnalysisCtx("functional"),
        "show column stats alltypes", expectedAuthorizables);

    // Show create table.
    expectedAuthorizables = Sets.newHashSet("functional.alltypes");
    verifyPrivilegeReqs("show create table functional.alltypes", expectedAuthorizables);
    verifyPrivilegeReqs(createAnalysisCtx("functional"),
        "show create table functional.alltypes", expectedAuthorizables);

    // Show create view.
    expectedAuthorizables = Sets.newHashSet("functional.alltypes_view");
    verifyPrivilegeReqs("show create view functional.alltypes_view",
        expectedAuthorizables);
    verifyPrivilegeReqs(createAnalysisCtx("functional"),
        "show create view functional.alltypes_view", expectedAuthorizables);

    // Compute stats.
    expectedAuthorizables = Sets.newHashSet("functional.alltypes");
    verifyPrivilegeReqs("compute stats functional.alltypes", expectedAuthorizables);
    verifyPrivilegeReqs(createAnalysisCtx("functional"), "compute stats alltypes",
        expectedAuthorizables);

    // Drop stats.
    expectedAuthorizables = Sets.newHashSet("functional.alltypes");
    verifyPrivilegeReqs("drop stats functional.alltypes", expectedAuthorizables);
    verifyPrivilegeReqs(createAnalysisCtx("functional"), "drop stats alltypes",
        expectedAuthorizables);

    // Create table.
    expectedAuthorizables = Sets.newHashSet("functional.new_table");
    verifyPrivilegeReqs("create table functional.new_table(i int)",
        expectedAuthorizables);
    verifyPrivilegeReqs(createAnalysisCtx("functional"),
        "create table new_table(i int)", expectedAuthorizables);

    // Create view.
    expectedAuthorizables = Sets.newHashSet("functional.new_view");
    verifyPrivilegeReqs("create view functional.new_view as select 1",
        expectedAuthorizables);
    verifyPrivilegeReqs(createAnalysisCtx("functional"),
        "create view new_view as select 1", expectedAuthorizables);

    // Drop table.
    expectedAuthorizables = Sets.newHashSet("functional.alltypes");
    verifyPrivilegeReqs("drop table functional.alltypes", expectedAuthorizables);
    verifyPrivilegeReqs(createAnalysisCtx("functional"), "drop table alltypes",
        expectedAuthorizables);

    // Drop view.
    expectedAuthorizables = Sets.newHashSet("functional.alltypes_view");
    verifyPrivilegeReqs("drop view functional.alltypes_view", expectedAuthorizables);
    verifyPrivilegeReqs(createAnalysisCtx("functional"),
        "drop view alltypes_view",
        expectedAuthorizables);

    // Update table.
    expectedAuthorizables = Sets.newHashSet(
        "functional_kudu.alltypes",
        "functional_kudu.alltypes.id",
        "functional_kudu.alltypes.int_col");
    verifyPrivilegeReqs("update functional_kudu.alltypes set int_col = 1",
        expectedAuthorizables);
    verifyPrivilegeReqs(createAnalysisCtx("functional_kudu"),
        "update alltypes set int_col = 1", expectedAuthorizables);

    // Upsert table.
    expectedAuthorizables = Sets.newHashSet("functional_kudu.alltypes");
    verifyPrivilegeReqs("upsert into table functional_kudu.alltypes(id, int_col) " +
        "values(1, 1)", expectedAuthorizables);
    verifyPrivilegeReqs(createAnalysisCtx("functional_kudu"),
        "upsert into table alltypes(id, int_col) values(1, 1)", expectedAuthorizables);

    // Delete table.
    expectedAuthorizables = Sets.newHashSet(
        "functional_kudu.alltypes",
        "functional_kudu.alltypes.id");
    verifyPrivilegeReqs("delete from functional_kudu.alltypes", expectedAuthorizables);
    verifyPrivilegeReqs(createAnalysisCtx("functional_kudu"),
        "delete from alltypes",
        expectedAuthorizables);

    // Alter table.
    expectedAuthorizables = Sets.newHashSet("functional.alltypes");
    verifyPrivilegeReqs("alter table functional.alltypes add columns(c1 int)",
        expectedAuthorizables);
    verifyPrivilegeReqs(createAnalysisCtx("functional"),
        "alter table alltypes add columns(c1 int)", expectedAuthorizables);

    // Alter view.
    expectedAuthorizables = Sets.newHashSet("functional.alltypes_view");
    verifyPrivilegeReqs("alter view functional.alltypes_view as select 1",
        expectedAuthorizables);
    verifyPrivilegeReqs(createAnalysisCtx("functional"),
        "alter view alltypes_view as select 1", expectedAuthorizables);
  }

  @Test
  public void testCopyTestCasePrivileges() throws ImpalaException {
    // Used for select *, with, and union
    Set<String> expectedAuthorizables = Sets.newHashSet(
        "functional", // For including the DB related metadata in the testcase file.
        "functional.alltypes",
        "functional.alltypes.id",
        "functional.alltypes.bool_col",
        "functional.alltypes.tinyint_col",
        "functional.alltypes.smallint_col",
        "functional.alltypes.int_col",
        "functional.alltypes.bigint_col",
        "functional.alltypes.float_col",
        "functional.alltypes.double_col",
        "functional.alltypes.date_string_col",
        "functional.alltypes.string_col",
        "functional.alltypes.timestamp_col",
        "functional.alltypes.year",
        "functional.alltypes.month",
        "hdfs://localhost:20500/tmp" // For the testcase output URI
    );

    // Select *
    verifyPrivilegeReqs("copy testcase to '/tmp' select * from functional" +
        ".alltypes", expectedAuthorizables);
    verifyPrivilegeReqs("copy testcase to '/tmp' select alltypes.* from " +
        "functional.alltypes", expectedAuthorizables);
    verifyPrivilegeReqs(createAnalysisCtx("functional"), "copy testcase to " +
        "'/tmp'  select * from alltypes", expectedAuthorizables);
    verifyPrivilegeReqs(createAnalysisCtx("functional"),
        "copy testcase to '/tmp' select alltypes.* from alltypes",
        expectedAuthorizables);
    verifyPrivilegeReqs("copy testcase to '/tmp' select a.* from functional" +
        ".alltypes a", expectedAuthorizables);

    // With clause.
    verifyPrivilegeReqs("copy testcase to '/tmp' with t as (select * from " +
        "functional.alltypes) select * from t", expectedAuthorizables);
    verifyPrivilegeReqs(createAnalysisCtx("functional"),
        "copy testcase to '/tmp' with t as (select * from alltypes) select * " +
            "from t", expectedAuthorizables);

    // Union.
    verifyPrivilegeReqs("copy testcase to '/tmp' select * from functional" +
        ".alltypes union all select * from functional.alltypes", expectedAuthorizables);
    verifyPrivilegeReqs(createAnalysisCtx("functional"), "copy testcase to '/tmp'"
            + "select * from alltypes union all select * from" + " alltypes",
        expectedAuthorizables);

    // Select a specific column.
    expectedAuthorizables = Sets.newHashSet(
        "functional",
        "functional.alltypes",
        "functional.alltypes.id",
        "hdfs://localhost:20500/tmp"
    );
    verifyPrivilegeReqs("copy testcase to '/tmp' select id from " +
        "functional.alltypes", expectedAuthorizables);
    verifyPrivilegeReqs("copy testcase to '/tmp' select alltypes.id from " +
            "functional.alltypes", expectedAuthorizables);
    verifyPrivilegeReqs(createAnalysisCtx("functional"),
        "copy testcase to '/tmp' select alltypes.id from alltypes",
        expectedAuthorizables);
    verifyPrivilegeReqs(createAnalysisCtx("functional"), "copy testcase to " +
            "'/tmp' select id from alltypes", expectedAuthorizables);
    verifyPrivilegeReqs("copy testcase to '/tmp' select alltypes.id from " +
            "functional.alltypes", expectedAuthorizables);
    verifyPrivilegeReqs("copy testcase to '/tmp' select a.id from functional" +
        ".alltypes a", expectedAuthorizables);

    // Verify VIEW_METADATA privileges on authorizables.
    final String copyTestCasePrefix = "copy testcase to '/tmp' ";
    for (AuthzTest authzTest: new AuthzTest[] {
        authorize(copyTestCasePrefix + "with t as (select id from functional.alltypes) " +
            "select * from t"),
        authorize(copyTestCasePrefix + "select id from functional.alltypes")}) {
      authzTest
          // Ideal case, when all the privileges are in place.
          .ok(onUri(false, "/tmp", TPrivilegeLevel.ALL),
              onDatabase("functional", viewMetadataPrivileges()),
              onTable("functional", "alltypes", viewMetadataPrivileges()))
          // DB does not have the metadata access privileges
          .error(accessError("functional"),
              onUri(false, "/tmp", TPrivilegeLevel.ALL),
              onDatabase("functional", allExcept(viewMetadataPrivileges())),
              onTable("functional", "alltypes", viewMetadataPrivileges()))
          // URI does not have ALL privilege.
          .error(accessError("hdfs://localhost:20500/tmp"),
              onDatabase("functional", viewMetadataPrivileges()),
              onTable("functional", "alltypes", viewMetadataPrivileges()));
    }
  }

  @Test
  public void testSelect() throws ImpalaException {
    for (AuthzTest authzTest: new AuthzTest[]{
        // Select a specific column on a table.
        authorize("select id from functional.alltypes"),
        // With clause with select.
        authorize("with t as (select id from functional.alltypes) select * from t")}) {
      authzTest
          .ok(onServer(TPrivilegeLevel.ALL))
          .ok(onServer(TPrivilegeLevel.OWNER))
          .ok(onServer(TPrivilegeLevel.SELECT))
          .ok(onDatabase("functional", TPrivilegeLevel.ALL))
          .ok(onDatabase("functional", TPrivilegeLevel.OWNER))
          .ok(onDatabase("functional", TPrivilegeLevel.SELECT))
          .ok(onTable("functional", "alltypes", TPrivilegeLevel.ALL))
          .ok(onTable("functional", "alltypes", TPrivilegeLevel.OWNER))
          .ok(onTable("functional", "alltypes", TPrivilegeLevel.SELECT))
          .ok(onColumn("functional", "alltypes", "id", TPrivilegeLevel.SELECT))
          .error(selectError("functional.alltypes"))
          .error(selectError("functional.alltypes"), onServer(allExcept(
              TPrivilegeLevel.ALL, TPrivilegeLevel.OWNER, TPrivilegeLevel.SELECT)))
          .error(selectError("functional.alltypes"), onDatabase("functional",
              allExcept(TPrivilegeLevel.ALL, TPrivilegeLevel.OWNER,
              TPrivilegeLevel.SELECT)))
          .error(selectError("functional.alltypes"), onTable("functional",
              "alltypes", allExcept(TPrivilegeLevel.ALL, TPrivilegeLevel.OWNER,
              TPrivilegeLevel.SELECT)));
    }

    // Select without referencing a column.
    authorize("select 1 from functional.alltypes")
        .ok(onServer(TPrivilegeLevel.ALL))
        .ok(onServer(TPrivilegeLevel.OWNER))
        .ok(onServer(TPrivilegeLevel.SELECT))
        .ok(onDatabase("functional", TPrivilegeLevel.ALL))
        .ok(onDatabase("functional", TPrivilegeLevel.OWNER))
        .ok(onDatabase("functional", TPrivilegeLevel.SELECT))
        .ok(onTable("functional", "alltypes", TPrivilegeLevel.ALL))
        .ok(onTable("functional", "alltypes", TPrivilegeLevel.OWNER))
        .ok(onTable("functional", "alltypes", TPrivilegeLevel.SELECT))
        .error(selectError("functional.alltypes"))
        .error(selectError("functional.alltypes"), onServer(allExcept(
            TPrivilegeLevel.ALL, TPrivilegeLevel.OWNER, TPrivilegeLevel.SELECT)))
        .error(selectError("functional.alltypes"), onDatabase("functional",
            allExcept(TPrivilegeLevel.ALL, TPrivilegeLevel.OWNER,
            TPrivilegeLevel.SELECT)))
        .error(selectError("functional.alltypes"), onTable("functional", "alltypes",
            allExcept(TPrivilegeLevel.ALL, TPrivilegeLevel.OWNER,
            TPrivilegeLevel.SELECT)));


    // Select a specific column on a view.
    for (AuthzTest test: new AuthzTest[]{
        authorize("select id from functional.alltypes_view"),
        authorize("select 2 * v.id from functional.alltypes_view v"),
        authorize("select cast(id as bigint) from functional.alltypes_view")}) {
      test.ok(onServer(TPrivilegeLevel.ALL))
          .ok(onServer(TPrivilegeLevel.OWNER))
          .ok(onServer(TPrivilegeLevel.SELECT))
          .ok(onDatabase("functional", TPrivilegeLevel.ALL))
          .ok(onDatabase("functional", TPrivilegeLevel.OWNER))
          .ok(onDatabase("functional", TPrivilegeLevel.SELECT))
          .ok(onTable("functional", "alltypes_view", TPrivilegeLevel.ALL))
          .ok(onTable("functional", "alltypes_view", TPrivilegeLevel.OWNER))
          .ok(onTable("functional", "alltypes_view", TPrivilegeLevel.SELECT))
          .ok(onColumn("functional", "alltypes_view", "id", TPrivilegeLevel.SELECT))
          .error(selectError("functional.alltypes_view"))
          .error(selectError("functional.alltypes_view"), onServer(allExcept(
              TPrivilegeLevel.ALL, TPrivilegeLevel.OWNER, TPrivilegeLevel.SELECT)))
          .error(selectError("functional.alltypes_view"), onDatabase("functional",
              allExcept(TPrivilegeLevel.ALL, TPrivilegeLevel.OWNER,
                  TPrivilegeLevel.SELECT)))
          .error(selectError("functional.alltypes_view"), onTable("functional",
              "alltypes_view", allExcept(TPrivilegeLevel.ALL, TPrivilegeLevel.OWNER,
                  TPrivilegeLevel.SELECT)));
    }

    // Constant select.
    authorize("select 1").ok();

    // Select on view and join table.
    authorize("select a.id from functional.view_view a " +
        "join functional.alltypesagg b ON (a.id = b.id)")
        .ok(onServer(TPrivilegeLevel.ALL))
        .ok(onServer(TPrivilegeLevel.OWNER))
        .ok(onServer(TPrivilegeLevel.SELECT))
        .ok(onDatabase("functional", TPrivilegeLevel.ALL))
        .ok(onDatabase("functional", TPrivilegeLevel.OWNER))
        .ok(onDatabase("functional", TPrivilegeLevel.SELECT))
        .ok(onTable("functional", "view_view", TPrivilegeLevel.ALL),
            onTable("functional", "alltypesagg", TPrivilegeLevel.ALL))
        .ok(onTable("functional", "view_view", TPrivilegeLevel.OWNER),
            onTable("functional", "alltypesagg", TPrivilegeLevel.OWNER))
        .ok(onTable("functional", "view_view", TPrivilegeLevel.ALL),
            onTable("functional", "alltypesagg", TPrivilegeLevel.SELECT))
        .ok(onTable("functional", "view_view", TPrivilegeLevel.OWNER),
            onTable("functional", "alltypesagg", TPrivilegeLevel.SELECT))
        .ok(onTable("functional", "view_view", TPrivilegeLevel.SELECT),
            onTable("functional", "alltypesagg", TPrivilegeLevel.ALL))
        .ok(onTable("functional", "view_view", TPrivilegeLevel.SELECT),
            onTable("functional", "alltypesagg", TPrivilegeLevel.OWNER))
        .ok(onTable("functional", "view_view", TPrivilegeLevel.SELECT),
            onTable("functional", "alltypesagg", TPrivilegeLevel.SELECT))
        .error(selectError("functional.view_view"))
        .error(selectError("functional.view_view"), onServer(allExcept(
            TPrivilegeLevel.ALL, TPrivilegeLevel.OWNER, TPrivilegeLevel.SELECT)))
        .error(selectError("functional.view_view"), onDatabase("functional",
            allExcept(TPrivilegeLevel.ALL, TPrivilegeLevel.OWNER,
            TPrivilegeLevel.SELECT)))
        .error(selectError("functional.view_view"), onTable("functional", "view_view",
            allExcept(TPrivilegeLevel.ALL, TPrivilegeLevel.OWNER,
            TPrivilegeLevel.SELECT)), onTable("functional", "alltypesagg",
            allExcept(TPrivilegeLevel.ALL, TPrivilegeLevel.OWNER,
            TPrivilegeLevel.SELECT)));

    // Tests authorization after a statement has been rewritten (IMPALA-3915).
    authorize("select * from functional_seq_snap.subquery_view")
        .ok(onServer(TPrivilegeLevel.ALL))
        .ok(onServer(TPrivilegeLevel.OWNER))
        .ok(onServer(TPrivilegeLevel.SELECT))
        .ok(onDatabase("functional_seq_snap", TPrivilegeLevel.ALL))
        .ok(onDatabase("functional_seq_snap", TPrivilegeLevel.OWNER))
        .ok(onDatabase("functional_seq_snap", TPrivilegeLevel.SELECT))
        .ok(onTable("functional_seq_snap", "subquery_view", TPrivilegeLevel.ALL))
        .ok(onTable("functional_seq_snap", "subquery_view", TPrivilegeLevel.OWNER))
        .ok(onTable("functional_seq_snap", "subquery_view", TPrivilegeLevel.SELECT))
        .error(selectError("functional_seq_snap.subquery_view"))
        .error(selectError("functional_seq_snap.subquery_view"), onServer(
            allExcept(TPrivilegeLevel.ALL, TPrivilegeLevel.OWNER,
            TPrivilegeLevel.SELECT)))
        .error(selectError("functional_seq_snap.subquery_view"),
            onDatabase("functional_seq_snap", allExcept(TPrivilegeLevel.ALL,
            TPrivilegeLevel.OWNER, TPrivilegeLevel.SELECT)))
        .error(selectError("functional_seq_snap.subquery_view"),
            onTable("functional_seq_snap", "subquery_view", allExcept(
            TPrivilegeLevel.ALL, TPrivilegeLevel.OWNER, TPrivilegeLevel.SELECT)));

    // Select a UDF.
    ScalarFunction fn = addFunction("functional", "f");
    try {
      authorize("select functional.f()")
          .ok(onServer(TPrivilegeLevel.ALL))
          .ok(onServer(TPrivilegeLevel.OWNER))
          .ok(onServer(viewMetadataPrivileges()))
          .ok(onDatabase("functional", TPrivilegeLevel.ALL))
          .ok(onDatabase("functional", TPrivilegeLevel.OWNER))
          .ok(onDatabase("functional", viewMetadataPrivileges()))
          .error(accessError("functional"))
          .error(accessError("functional"), onServer(allExcept(
              viewMetadataPrivileges())))
          .error(accessError("functional"), onDatabase("functional", allExcept(
              viewMetadataPrivileges())));
    } finally {
      removeFunction(fn);
    }

    // Select from non-existent database.
    authorize("select 1 from nodb.alltypes")
        .error(selectError("nodb.alltypes"));

    // Select from non-existent table.
    authorize("select 1 from functional.notbl")
        .error(selectError("functional.notbl"));

    // Select with inline view.
    authorize("select a.* from (select * from functional.alltypes) a")
        .ok(onServer(TPrivilegeLevel.ALL))
        .ok(onServer(TPrivilegeLevel.OWNER))
        .ok(onServer(TPrivilegeLevel.SELECT))
        .ok(onDatabase("functional", TPrivilegeLevel.ALL))
        .ok(onDatabase("functional", TPrivilegeLevel.OWNER))
        .ok(onDatabase("functional", TPrivilegeLevel.SELECT))
        .ok(onTable("functional", "alltypes", TPrivilegeLevel.ALL))
        .ok(onTable("functional", "alltypes", TPrivilegeLevel.OWNER))
        .ok(onTable("functional", "alltypes", TPrivilegeLevel.SELECT))
        .ok(onColumn("functional", "alltypes", ALLTYPES_COLUMNS, TPrivilegeLevel.SELECT))
        .error(selectError("functional.alltypes"))
        .error(selectError("functional.alltypes"), onServer(allExcept(
            TPrivilegeLevel.ALL, TPrivilegeLevel.OWNER, TPrivilegeLevel.SELECT)))
        .error(selectError("functional.alltypes"), onDatabase("functional",
            allExcept(TPrivilegeLevel.ALL, TPrivilegeLevel.OWNER,
            TPrivilegeLevel.SELECT)))
        .error(selectError("functional.alltypes"), onTable("functional", "alltypes",
            allExcept(TPrivilegeLevel.ALL, TPrivilegeLevel.OWNER,
            TPrivilegeLevel.SELECT)));

    // Select with columns referenced in function, where clause and group by.
    authorize("select count(id), int_col from functional.alltypes where id = 10 " +
        "group by id, int_col")
        .ok(onServer(TPrivilegeLevel.ALL))
        .ok(onServer(TPrivilegeLevel.OWNER))
        .ok(onServer(TPrivilegeLevel.SELECT))
        .ok(onDatabase("functional", TPrivilegeLevel.ALL))
        .ok(onDatabase("functional", TPrivilegeLevel.OWNER))
        .ok(onDatabase("functional", TPrivilegeLevel.SELECT))
        .ok(onTable("functional", "alltypes", TPrivilegeLevel.ALL))
        .ok(onTable("functional", "alltypes", TPrivilegeLevel.OWNER))
        .ok(onTable("functional", "alltypes", TPrivilegeLevel.SELECT))
        .ok(onColumn("functional", "alltypes", new String[]{"id", "int_col"},
            TPrivilegeLevel.SELECT))
        .error(selectError("functional.alltypes"))
        .error(selectError("functional.alltypes"), onServer(allExcept(
            TPrivilegeLevel.ALL, TPrivilegeLevel.OWNER, TPrivilegeLevel.SELECT)))
        .error(selectError("functional.alltypes"), onDatabase("functional",
            allExcept(TPrivilegeLevel.ALL, TPrivilegeLevel.OWNER,
            TPrivilegeLevel.SELECT)))
        .error(selectError("functional.alltypes"), onTable("functional", "alltypes",
            allExcept(TPrivilegeLevel.ALL, TPrivilegeLevel.OWNER,
            TPrivilegeLevel.SELECT)));

    // Select on tables with complex types.
    authorize("select a.int_struct_col.f1 from functional.allcomplextypes a " +
        "where a.id = 1")
        .ok(onServer(TPrivilegeLevel.ALL))
        .ok(onServer(TPrivilegeLevel.OWNER))
        .ok(onServer(TPrivilegeLevel.SELECT))
        .ok(onDatabase("functional", TPrivilegeLevel.ALL))
        .ok(onDatabase("functional", TPrivilegeLevel.OWNER))
        .ok(onDatabase("functional", TPrivilegeLevel.SELECT))
        .ok(onTable("functional", "allcomplextypes", TPrivilegeLevel.ALL))
        .ok(onTable("functional", "allcomplextypes", TPrivilegeLevel.OWNER))
        .ok(onTable("functional", "allcomplextypes", TPrivilegeLevel.SELECT))
        .ok(onColumn("functional", "allcomplextypes",
            new String[]{"id", "int_struct_col"}, TPrivilegeLevel.SELECT))
        .error(selectError("functional.allcomplextypes"))
        .error(selectError("functional.allcomplextypes"), onServer(
            allExcept(TPrivilegeLevel.ALL, TPrivilegeLevel.OWNER,
            TPrivilegeLevel.SELECT)))
        .error(selectError("functional.allcomplextypes"), onDatabase("functional",
            allExcept(TPrivilegeLevel.ALL, TPrivilegeLevel.OWNER,
            TPrivilegeLevel.SELECT)))
        .error(selectError("functional.allcomplextypes"), onTable("functional",
            "allcomplextypes", allExcept(TPrivilegeLevel.ALL, TPrivilegeLevel.OWNER,
            TPrivilegeLevel.SELECT)));

    authorize("select key, pos, item.f1, f2 from functional.allcomplextypes t, " +
        "t.struct_array_col, functional.allcomplextypes.int_map_col")
        .ok(onServer(TPrivilegeLevel.ALL))
        .ok(onServer(TPrivilegeLevel.OWNER))
        .ok(onServer(TPrivilegeLevel.SELECT))
        .ok(onDatabase("functional", TPrivilegeLevel.ALL))
        .ok(onDatabase("functional", TPrivilegeLevel.OWNER))
        .ok(onDatabase("functional", TPrivilegeLevel.SELECT))
        .ok(onTable("functional", "allcomplextypes", TPrivilegeLevel.ALL))
        .ok(onTable("functional", "allcomplextypes", TPrivilegeLevel.OWNER))
        .ok(onTable("functional", "allcomplextypes", TPrivilegeLevel.SELECT))
        .ok(onColumn("functional", "allcomplextypes",
            new String[]{"struct_array_col", "int_map_col"}, TPrivilegeLevel.SELECT))
        .error(selectError("functional.allcomplextypes"))
        .error(selectError("functional.allcomplextypes"), onServer(allExcept(
            TPrivilegeLevel.ALL, TPrivilegeLevel.OWNER, TPrivilegeLevel.SELECT)))
        .error(selectError("functional.allcomplextypes"), onDatabase("functional",
            allExcept(TPrivilegeLevel.ALL, TPrivilegeLevel.OWNER,
            TPrivilegeLevel.SELECT)))
        .error(selectError("functional.allcomplextypes"), onTable("functional",
            "allcomplextypes", allExcept(TPrivilegeLevel.ALL, TPrivilegeLevel.OWNER,
            TPrivilegeLevel.SELECT)));

    for (AuthzTest authzTest: new AuthzTest[]{
        // Select with cross join.
        authorize("select * from functional.alltypes union all " +
            "select * from functional.alltypessmall"),
        // Union on tables.
        authorize("select * from functional.alltypes a cross join " +
            "functional.alltypessmall b")}) {
      authzTest.ok(onServer(TPrivilegeLevel.ALL))
          .ok(onServer(TPrivilegeLevel.OWNER))
          .ok(onServer(TPrivilegeLevel.SELECT))
          .ok(onDatabase("functional", TPrivilegeLevel.ALL))
          .ok(onDatabase("functional", TPrivilegeLevel.OWNER))
          .ok(onDatabase("functional", TPrivilegeLevel.SELECT))
          .ok(onTable("functional", "alltypes", TPrivilegeLevel.ALL),
              onTable("functional", "alltypessmall", TPrivilegeLevel.ALL))
          .ok(onTable("functional", "alltypes", TPrivilegeLevel.OWNER),
              onTable("functional", "alltypessmall", TPrivilegeLevel.OWNER))
          .ok(onTable("functional", "alltypes", TPrivilegeLevel.SELECT),
              onTable("functional", "alltypessmall", TPrivilegeLevel.ALL))
          .ok(onTable("functional", "alltypes", TPrivilegeLevel.SELECT),
              onTable("functional", "alltypessmall", TPrivilegeLevel.OWNER))
          .ok(onTable("functional", "alltypes", TPrivilegeLevel.ALL),
              onTable("functional", "alltypessmall", TPrivilegeLevel.SELECT))
          .ok(onTable("functional", "alltypes", TPrivilegeLevel.OWNER),
              onTable("functional", "alltypessmall", TPrivilegeLevel.SELECT))
          .ok(onTable("functional", "alltypes", TPrivilegeLevel.SELECT),
              onTable("functional", "alltypessmall", TPrivilegeLevel.SELECT))
          .ok(onColumn("functional", "alltypes", ALLTYPES_COLUMNS,
              TPrivilegeLevel.SELECT), onColumn("functional", "alltypessmall",
              ALLTYPES_COLUMNS, TPrivilegeLevel.SELECT))
          .error(selectError("functional.alltypes"))
          .error(selectError("functional.alltypes"), onServer(
              allExcept(TPrivilegeLevel.ALL, TPrivilegeLevel.OWNER,
              TPrivilegeLevel.SELECT)))
          .error(selectError("functional.alltypes"), onDatabase("functional",
              allExcept(TPrivilegeLevel.ALL, TPrivilegeLevel.OWNER,
              TPrivilegeLevel.SELECT)))
          .error(selectError("functional.alltypes"), onTable("functional", "alltypes",
              allExcept(TPrivilegeLevel.ALL, TPrivilegeLevel.OWNER,
              TPrivilegeLevel.SELECT)), onTable("functional", "alltypessmall",
              allExcept(TPrivilegeLevel.ALL, TPrivilegeLevel.OWNER,
              TPrivilegeLevel.SELECT)))
          .error(selectError("functional.alltypessmall"), onColumn("functional",
              "alltypes", ALLTYPES_COLUMNS, TPrivilegeLevel.SELECT))
          .error(selectError("functional.alltypes"), onColumn("functional",
              "alltypessmall", ALLTYPES_COLUMNS, TPrivilegeLevel.SELECT));
    }

    // Union on views.
    authorize("select id from functional.alltypes_view union all " +
        "select x from functional.alltypes_view_sub")
        .ok(onServer(TPrivilegeLevel.ALL))
        .ok(onServer(TPrivilegeLevel.OWNER))
        .ok(onServer(TPrivilegeLevel.SELECT))
        .ok(onDatabase("functional", TPrivilegeLevel.ALL))
        .ok(onDatabase("functional", TPrivilegeLevel.OWNER))
        .ok(onDatabase("functional", TPrivilegeLevel.SELECT))
        .ok(onTable("functional", "alltypes_view", TPrivilegeLevel.ALL),
            onTable("functional", "alltypes_view_sub", TPrivilegeLevel.ALL))
        .ok(onTable("functional", "alltypes_view", TPrivilegeLevel.OWNER),
            onTable("functional", "alltypes_view_sub", TPrivilegeLevel.OWNER))
        .ok(onTable("functional", "alltypes_view", TPrivilegeLevel.SELECT),
            onTable("functional", "alltypes_view_sub", TPrivilegeLevel.SELECT))
        .ok(onColumn("functional", "alltypes_view", "id", TPrivilegeLevel.SELECT),
            onColumn("functional", "alltypes_view_sub", "x", TPrivilegeLevel.SELECT))
        .error(selectError("functional.alltypes_view"))
        .error(selectError("functional.alltypes_view"), onServer(allExcept(
            TPrivilegeLevel.ALL, TPrivilegeLevel.OWNER, TPrivilegeLevel.SELECT)))
        .error(selectError("functional.alltypes_view"), onDatabase("functional",
            allExcept(TPrivilegeLevel.ALL, TPrivilegeLevel.OWNER,
            TPrivilegeLevel.SELECT)))
        .error(selectError("functional.alltypes_view"), onTable("functional",
            "alltypes_view", allExcept(TPrivilegeLevel.ALL, TPrivilegeLevel.OWNER,
            TPrivilegeLevel.SELECT)),
            onTable("functional", "alltypes_view_sub", TPrivilegeLevel.SELECT))
        .error(selectError("functional.alltypes_view_sub"), onTable("functional",
            "alltypes_view_sub", allExcept(TPrivilegeLevel.ALL, TPrivilegeLevel.OWNER,
            TPrivilegeLevel.SELECT)),
            onTable("functional", "alltypes_view", TPrivilegeLevel.SELECT));

    // Union from non-existent databases.
    authorize("select id from nodb.alltypes union all " +
        "select id from functional.alltypesagg").error(selectError("nodb.alltypes"));

    // Union from non-existent tables.
    authorize("select id from functional.notbl union all " +
        "select id from functional.alltypesagg").error(selectError("functional.notbl"));
  }

  @Test
  public void testInsert() throws ImpalaException {
    // Basic insert into a table.
    for (AuthzTest test: new AuthzTest[]{
        authorize("insert into functional.zipcode_incomes(id) values('123')"),
        // Explain insert.
        authorize("explain insert into functional.zipcode_incomes(id) values('123')")}) {
      test.ok(onServer(TPrivilegeLevel.ALL))
          .ok(onServer(TPrivilegeLevel.OWNER))
          .ok(onServer(TPrivilegeLevel.INSERT))
          .ok(onDatabase("functional", TPrivilegeLevel.ALL))
          .ok(onDatabase("functional", TPrivilegeLevel.OWNER))
          .ok(onDatabase("functional", TPrivilegeLevel.INSERT))
          .ok(onTable("functional", "zipcode_incomes", TPrivilegeLevel.ALL))
          .ok(onTable("functional", "zipcode_incomes", TPrivilegeLevel.OWNER))
          .ok(onTable("functional", "zipcode_incomes", TPrivilegeLevel.INSERT))
          .error(insertError("functional.zipcode_incomes"))
          .error(insertError("functional.zipcode_incomes"), onServer(allExcept(
              TPrivilegeLevel.ALL, TPrivilegeLevel.OWNER, TPrivilegeLevel.INSERT)))
          .error(insertError("functional.zipcode_incomes"), onDatabase("functional",
              allExcept(TPrivilegeLevel.ALL, TPrivilegeLevel.OWNER,
              TPrivilegeLevel.INSERT)))
          .error(insertError("functional.zipcode_incomes"), onTable("functional",
              "zipcode_incomes", allExcept(TPrivilegeLevel.ALL, TPrivilegeLevel.OWNER,
              TPrivilegeLevel.INSERT)));
    }

    for (AuthzTest test: new AuthzTest[]{
        // With clause with insert.
        authorize("with t as (select * from functional.alltypestiny) " +
            "insert into functional.alltypes partition(month, year) " +
            "select * from t"),
        // Insert with select on a target table.
        authorize("insert into functional.alltypes partition(month, year) " +
            "select * from functional.alltypestiny where id < 100")}) {
      test.ok(onServer(TPrivilegeLevel.ALL))
          .ok(onServer(TPrivilegeLevel.OWNER))
          .ok(onServer(TPrivilegeLevel.INSERT, TPrivilegeLevel.SELECT))
          .ok(onDatabase("functional", TPrivilegeLevel.ALL))
          .ok(onDatabase("functional", TPrivilegeLevel.OWNER))
          .ok(onDatabase("functional", TPrivilegeLevel.INSERT, TPrivilegeLevel.SELECT))
          .ok(onTable("functional", "alltypes", TPrivilegeLevel.ALL),
              onTable("functional", "alltypestiny", TPrivilegeLevel.ALL))
          .ok(onTable("functional", "alltypes", TPrivilegeLevel.OWNER),
              onTable("functional", "alltypestiny", TPrivilegeLevel.OWNER))
          .ok(onTable("functional", "alltypes", TPrivilegeLevel.INSERT),
              onTable("functional", "alltypestiny", TPrivilegeLevel.SELECT))
          .ok(onTable("functional", "alltypes", TPrivilegeLevel.INSERT),
              onColumn("functional", "alltypestiny", ALLTYPES_COLUMNS,
              TPrivilegeLevel.SELECT))
          .error(selectError("functional.alltypestiny"))
          .error(selectError("functional.alltypestiny"), onServer(allExcept(
              TPrivilegeLevel.ALL, TPrivilegeLevel.OWNER, TPrivilegeLevel.INSERT,
              TPrivilegeLevel.SELECT)))
          .error(selectError("functional.alltypestiny"), onDatabase("functional",
              allExcept(TPrivilegeLevel.ALL, TPrivilegeLevel.OWNER,
              TPrivilegeLevel.INSERT, TPrivilegeLevel.SELECT)))
          .error(insertError("functional.alltypes"), onTable("functional",
              "alltypestiny", TPrivilegeLevel.SELECT), onTable("functional",
              "alltypes", allExcept(TPrivilegeLevel.ALL, TPrivilegeLevel.OWNER,
              TPrivilegeLevel.INSERT)))
          .error(selectError("functional.alltypestiny"), onTable("functional",
              "alltypestiny", allExcept(TPrivilegeLevel.ALL, TPrivilegeLevel.OWNER,
              TPrivilegeLevel.SELECT)), onTable("functional", "alltypes",
              TPrivilegeLevel.INSERT));
    }

    // Insert with select on a target view.
    authorize("insert into functional.alltypes partition(month, year) " +
        "select * from functional.alltypes_view where id < 100")
        .ok(onServer(TPrivilegeLevel.ALL))
        .ok(onServer(TPrivilegeLevel.OWNER))
        .ok(onServer(TPrivilegeLevel.INSERT, TPrivilegeLevel.SELECT))
        .ok(onDatabase("functional", TPrivilegeLevel.ALL))
        .ok(onDatabase("functional", TPrivilegeLevel.OWNER))
        .ok(onDatabase("functional", TPrivilegeLevel.INSERT, TPrivilegeLevel.SELECT))
        .ok(onTable("functional", "alltypes", TPrivilegeLevel.ALL),
            onTable("functional", "alltypes_view", TPrivilegeLevel.ALL))
        .ok(onTable("functional", "alltypes", TPrivilegeLevel.OWNER),
            onTable("functional", "alltypes_view", TPrivilegeLevel.OWNER))
        .ok(onTable("functional", "alltypes", TPrivilegeLevel.INSERT),
            onTable("functional", "alltypes_view", TPrivilegeLevel.SELECT))
        .ok(onTable("functional", "alltypes", TPrivilegeLevel.INSERT),
            onColumn("functional", "alltypes_view", ALLTYPES_COLUMNS,
                TPrivilegeLevel.SELECT))
        .error(selectError("functional.alltypes_view"))
        .error(selectError("functional.alltypes_view"), onServer(allExcept(
            TPrivilegeLevel.ALL, TPrivilegeLevel.OWNER, TPrivilegeLevel.INSERT,
            TPrivilegeLevel.SELECT)))
        .error(selectError("functional.alltypes_view"), onDatabase("functional",
            allExcept(TPrivilegeLevel.ALL, TPrivilegeLevel.OWNER, TPrivilegeLevel.INSERT,
            TPrivilegeLevel.SELECT)))
        .error(insertError("functional.alltypes"), onTable("functional",
            "alltypes_view", TPrivilegeLevel.SELECT), onTable("functional",
            "alltypes", allExcept(TPrivilegeLevel.ALL, TPrivilegeLevel.OWNER,
            TPrivilegeLevel.INSERT)))
        .error(selectError("functional.alltypes_view"), onTable("functional",
            "alltypes_view", allExcept(TPrivilegeLevel.ALL, TPrivilegeLevel.OWNER,
            TPrivilegeLevel.SELECT)),
            onTable("functional", "alltypes", TPrivilegeLevel.INSERT));

    // Insert with inline view.
    authorize("insert into functional.alltypes partition(month, year) " +
        "select b.* from functional.alltypesagg a join (select * from " +
        "functional.alltypestiny) b on (a.int_col = b.int_col)")
        .ok(onServer(TPrivilegeLevel.ALL))
        .ok(onServer(TPrivilegeLevel.OWNER))
        .ok(onServer(TPrivilegeLevel.INSERT, TPrivilegeLevel.SELECT))
        .ok(onDatabase("functional", TPrivilegeLevel.ALL))
        .ok(onDatabase("functional", TPrivilegeLevel.OWNER))
        .ok(onDatabase("functional", TPrivilegeLevel.INSERT, TPrivilegeLevel.SELECT))
        .ok(onTable("functional", "alltypes", TPrivilegeLevel.ALL),
            onTable("functional", "alltypesagg", TPrivilegeLevel.ALL),
            onTable("functional", "alltypestiny", TPrivilegeLevel.ALL))
        .ok(onTable("functional", "alltypes", TPrivilegeLevel.OWNER),
            onTable("functional", "alltypesagg", TPrivilegeLevel.OWNER),
            onTable("functional", "alltypestiny", TPrivilegeLevel.OWNER))
        .ok(onTable("functional", "alltypes", TPrivilegeLevel.INSERT),
            onTable("functional", "alltypesagg", TPrivilegeLevel.SELECT),
            onTable("functional", "alltypestiny", TPrivilegeLevel.SELECT))
        .error(selectError("functional.alltypesagg"))
        .error(selectError("functional.alltypesagg"), onServer(allExcept(
            TPrivilegeLevel.ALL, TPrivilegeLevel.OWNER, TPrivilegeLevel.INSERT,
            TPrivilegeLevel.SELECT)))
        .error(selectError("functional.alltypesagg"), onDatabase("functional", allExcept(
            TPrivilegeLevel.ALL, TPrivilegeLevel.OWNER, TPrivilegeLevel.INSERT,
            TPrivilegeLevel.SELECT)))
        .error(insertError("functional.alltypes"), onTable("functional",
            "alltypesagg", TPrivilegeLevel.SELECT), onTable("functional",
            "alltypestiny", TPrivilegeLevel.SELECT), onTable("functional",
            "alltypes", allExcept(TPrivilegeLevel.ALL, TPrivilegeLevel.OWNER,
            TPrivilegeLevel.INSERT)))
        .error(selectError("functional.alltypesagg"), onTable("functional",
            "alltypesagg", allExcept(TPrivilegeLevel.ALL, TPrivilegeLevel.OWNER,
            TPrivilegeLevel.SELECT)),
            onTable("functional", "alltypestiny", TPrivilegeLevel.SELECT),
            onTable("functional", "alltypes", TPrivilegeLevel.INSERT))
        .error(selectError("functional.alltypestiny"), onTable("functional",
            "alltypesagg", TPrivilegeLevel.SELECT), onTable("functional",
            "alltypestiny", allExcept(TPrivilegeLevel.ALL, TPrivilegeLevel.OWNER,
            TPrivilegeLevel.SELECT)),
            onTable("functional", "alltypes", TPrivilegeLevel.INSERT));

    // Inserting into a view is not allowed.
    authorize("insert into functional.alltypes_view(id) values(123)")
        .error(insertError("functional.alltypes_view"));

    // Inserting into a non-existent database.
    authorize("insert into nodb.alltypes(id) values(1)")
        .error(insertError("nodb.alltypes"));

    // Inserting into a non-existent table.
    authorize("insert into functional.notbl(id) values(1)")
        .error(insertError("functional.notbl"));
  }

  @Test
  public void testUseDb() throws ImpalaException {
    AuthzTest test = authorize("use functional");
    for (TPrivilegeLevel privilege: TPrivilegeLevel.values()) {
      test.ok(onServer(privilege))
          .ok(onDatabase("functional", privilege))
          .ok(onTable("functional", "alltypes", privilege))
          .ok(onColumn("functional", "alltypes", "id", privilege));
    }
    test.error(accessError("functional.*.*"));

    // Accessing default database should always be allowed.
    authorize("use default").ok();

    // Accessing system database should always be allowed.
    authorize("use _impala_builtins").ok();

    // Use a non-existent database.
    authorize("use nodb").error(accessError("nodb.*.*"));
  }

  @Test
  public void testTruncate() throws ImpalaException {
    // Truncate a table.
    authorize("truncate table functional.alltypes")
        .ok(onServer(TPrivilegeLevel.ALL))
        .ok(onServer(TPrivilegeLevel.OWNER))
        .ok(onServer(TPrivilegeLevel.INSERT))
        .ok(onDatabase("functional", TPrivilegeLevel.ALL))
        .ok(onDatabase("functional", TPrivilegeLevel.OWNER))
        .ok(onDatabase("functional", TPrivilegeLevel.INSERT))
        .ok(onTable("functional", "alltypes", TPrivilegeLevel.ALL))
        .ok(onTable("functional", "alltypes", TPrivilegeLevel.OWNER))
        .ok(onTable("functional", "alltypes", TPrivilegeLevel.INSERT))
        .error(insertError("functional.alltypes"))
        .error(insertError("functional.alltypes"), onServer(allExcept(
            TPrivilegeLevel.ALL, TPrivilegeLevel.OWNER, TPrivilegeLevel.INSERT)))
        .error(insertError("functional.alltypes"), onDatabase("functional", allExcept(
            TPrivilegeLevel.ALL, TPrivilegeLevel.OWNER, TPrivilegeLevel.INSERT)))
        .error(insertError("functional.alltypes"), onTable("functional", "alltypes",
            allExcept(TPrivilegeLevel.ALL, TPrivilegeLevel.OWNER,
            TPrivilegeLevel.INSERT)));

    // Truncate a non-existent database.
    authorize("truncate table nodb.alltypes")
        .error(insertError("nodb.alltypes"));

    // Truncate a non-existent table.
    authorize("truncate table functional.notbl")
        .error(insertError("functional.notbl"));

    // Truncating a view is not supported.
    authorize("truncate table functional.alltypes_view")
        .error(insertError("functional.alltypes_view"));
  }

  @Test
  public void testLoad() throws ImpalaException {
    // Load into a table.
    authorize("load data inpath 'hdfs://localhost:20500/test-warehouse/tpch.lineitem' " +
        "into table functional.alltypes partition(month=10, year=2009)")
      .ok(onServer(TPrivilegeLevel.ALL))
      .ok(onServer(TPrivilegeLevel.OWNER))
      .ok(onDatabase("functional", TPrivilegeLevel.ALL),
          onUri("hdfs://localhost:20500/test-warehouse/tpch.lineitem",
          TPrivilegeLevel.ALL))
      .ok(onDatabase("functional", TPrivilegeLevel.OWNER),
          onUri("hdfs://localhost:20500/test-warehouse/tpch.lineitem",
          TPrivilegeLevel.OWNER))
      .ok(onDatabase("functional", TPrivilegeLevel.INSERT),
          onUri("hdfs://localhost:20500/test-warehouse/tpch.lineitem",
          TPrivilegeLevel.ALL))
      .ok(onDatabase("functional", TPrivilegeLevel.INSERT),
          onUri("hdfs://localhost:20500/test-warehouse/tpch.lineitem",
          TPrivilegeLevel.OWNER))
      .ok(onTable("functional", "alltypes", TPrivilegeLevel.ALL),
          onUri("hdfs://localhost:20500/test-warehouse/tpch.lineitem",
          TPrivilegeLevel.ALL))
      .ok(onTable("functional", "alltypes", TPrivilegeLevel.OWNER),
          onUri("hdfs://localhost:20500/test-warehouse/tpch.lineitem",
          TPrivilegeLevel.OWNER))
      .ok(onTable("functional", "alltypes", TPrivilegeLevel.INSERT),
          onUri("hdfs://localhost:20500/test-warehouse/tpch.lineitem",
          TPrivilegeLevel.ALL))
      .ok(onTable("functional", "alltypes", TPrivilegeLevel.INSERT),
          onUri("hdfs://localhost:20500/test-warehouse/tpch.lineitem",
          TPrivilegeLevel.OWNER))
      .error(insertError("functional.alltypes"))
      .error(accessError("hdfs://localhost:20500/test-warehouse/tpch.lineitem"),
          onDatabase("functional", TPrivilegeLevel.INSERT))
      .error(accessError("hdfs://localhost:20500/test-warehouse/tpch.lineitem"),
          onTable("functional", "alltypes", TPrivilegeLevel.INSERT))
      .error(insertError("functional.alltypes"),
          onUri("hdfs://localhost:20500/test-warehouse/tpch.lineitem",
          TPrivilegeLevel.ALL))
      .error(insertError("functional.alltypes"),
          onUri("hdfs://localhost:20500/test-warehouse/tpch.lineitem",
          TPrivilegeLevel.OWNER));

    // Load from non-existent URI.
    authorize("load data inpath 'hdfs://localhost:20500/test-warehouse/nouri' " +
        "into table functional.alltypes partition(month=10, year=2009)")
        .error(insertError("functional.alltypes"))
        .error(accessError("hdfs://localhost:20500/test-warehouse/nouri"),
            onDatabase("functional", TPrivilegeLevel.INSERT))
        .error(accessError("hdfs://localhost:20500/test-warehouse/nouri"),
            onTable("functional", "alltypes", TPrivilegeLevel.INSERT));

    // Load into non-existent database.
    authorize("load data inpath 'hdfs://localhost:20500/test-warehouse/tpch.lineitem' " +
        "into table nodb.alltypes partition(month=10, year=2009)")
        .error(insertError("nodb.alltypes"))
        .error(insertError("nodb.alltypes"), onUri(
            "hdfs://localhost:20500/test-warehouse/tpch.nouri", TPrivilegeLevel.ALL))
        .error(insertError("nodb.alltypes"), onUri(
            "hdfs://localhost:20500/test-warehouse/tpch.nouri", TPrivilegeLevel.OWNER));

    // Load into non-existent table.
    authorize("load data inpath 'hdfs://localhost:20500/test-warehouse/tpch.lineitem' " +
        "into table functional.notbl partition(month=10, year=2009)")
        .error(insertError("functional.notbl"))
        .error(insertError("functional.notbl"), onUri(
            "hdfs://localhost:20500/test-warehouse/tpch.nouri", TPrivilegeLevel.ALL))
        .error(insertError("functional.notbl"), onUri(
            "hdfs://localhost:20500/test-warehouse/tpch.nouri", TPrivilegeLevel.OWNER));

    // Load into a view is not supported.
    authorize("load data inpath 'hdfs://localhost:20500/test-warehouse/tpch.lineitem' " +
        "into table functional.alltypes_view")
        .error(insertError("functional.alltypes_view"));
  }

  @Test
  public void testResetMetadata() throws ImpalaException {
    // Invalidate metadata/refresh authorization on server.
    for (AuthzTest test: new AuthzTest[]{
        authorize("invalidate metadata"),
        authorize("refresh authorization")}) {
      test.ok(onServer(TPrivilegeLevel.ALL))
          .ok(onServer(TPrivilegeLevel.OWNER))
          .ok(onServer(TPrivilegeLevel.REFRESH))
          .error(refreshError("server"));
    }

    // Invalidate metadata/refresh on a table / view
    for(String name: new String[] {"alltypes", "alltypes_view"}) {
      for (AuthzTest test: new AuthzTest[]{
          authorize("invalidate metadata functional." + name),
          authorize("refresh functional." + name)}) {
        test.ok(onServer(TPrivilegeLevel.ALL))
            .ok(onServer(TPrivilegeLevel.OWNER))
            .ok(onServer(TPrivilegeLevel.REFRESH))
            .ok(onDatabase("functional", TPrivilegeLevel.ALL))
            .ok(onDatabase("functional", TPrivilegeLevel.OWNER))
            .ok(onDatabase("functional", TPrivilegeLevel.REFRESH))
            .ok(onTable("functional", name, TPrivilegeLevel.ALL))
            .ok(onTable("functional", name, TPrivilegeLevel.OWNER))
            .ok(onTable("functional", name, TPrivilegeLevel.REFRESH))
            .error(refreshError("functional." + name))
            .error(refreshError("functional." + name), onDatabase("functional", allExcept(
                TPrivilegeLevel.ALL, TPrivilegeLevel.OWNER, TPrivilegeLevel.REFRESH)))
            .error(refreshError("functional." + name), onTable("functional", name,
                allExcept(TPrivilegeLevel.ALL, TPrivilegeLevel.OWNER,
                TPrivilegeLevel.REFRESH)));
      }
    }

    authorize("refresh functions functional")
        .ok(onServer(TPrivilegeLevel.REFRESH))
        .ok(onDatabase("functional", TPrivilegeLevel.ALL))
        .ok(onDatabase("functional", TPrivilegeLevel.OWNER))
        .ok(onDatabase("functional", TPrivilegeLevel.REFRESH))
        .error(refreshError("functional"))
        .error(refreshError("functional"), onServer(allExcept(TPrivilegeLevel.ALL,
            TPrivilegeLevel.OWNER, TPrivilegeLevel.REFRESH)))
        .error(refreshError("functional"), onDatabase("functional", allExcept(
            TPrivilegeLevel.ALL, TPrivilegeLevel.OWNER, TPrivilegeLevel.REFRESH)));

    // Reset metadata in non-existent database.
    authorize("invalidate metadata nodb").error(refreshError("default.nodb"));
    authorize("refresh nodb").error(refreshError("default.nodb"));
    authorize("refresh functions nodb").error(refreshError("nodb"));
  }

  @Test
  public void testShow() throws ImpalaException {
    // Show databases should always be allowed.
    authorize("show databases").ok();

    // Show tables.
    AuthzTest test = authorize("show tables in functional");
    for (TPrivilegeLevel privilege: TPrivilegeLevel.values()) {
      test.ok(onServer(privilege))
          .ok(onDatabase("functional", privilege))
          .ok(onTable("functional", "alltypes", privilege));
    }
    test.error(accessError("functional.*.*"));

    // Show functions.
    test = authorize("show functions in functional");
    for (TPrivilegeLevel privilege: viewMetadataPrivileges()) {
      test.ok(onServer(privilege))
          .ok(onDatabase("functional", privilege));
    }
    test.error(accessError("functional"));

    // Show tables in system database should always be allowed.
    authorize("show tables in _impala_builtins").ok();

    // Show tables for non-existent database.
    authorize("show tables in nodb").error(accessError("nodb"));

    // Show partitions, table stats, and column stats
    for (AuthzTest authzTest: new AuthzTest[]{
        authorize("show partitions functional.alltypes"),
        authorize("show table stats functional.alltypes"),
        authorize("show column stats functional.alltypes")}) {
      for (TPrivilegeLevel privilege: viewMetadataPrivileges()) {
        authzTest.ok(onServer(privilege))
            .ok(onDatabase("functional", privilege))
            .ok(onTable("functional", "alltypes", privilege))
            .error(accessError("functional.alltypes"), onColumn("functional", "alltypes",
                "id", TPrivilegeLevel.SELECT));
      }
      authzTest.error(accessError("functional"));
    }

    // Show range partitions.dimtbl
    for (TPrivilegeLevel privilege: viewMetadataPrivileges()) {
      authorize("show range partitions functional_kudu.dimtbl")
          .ok(onServer(privilege))
          .ok(onDatabase("functional_kudu", privilege))
          .ok(onTable("functional_kudu", "dimtbl", privilege))
          .error(accessError("functional_kudu.dimtbl"), onColumn("functional_kudu",
              "dimtbl", "id", TPrivilegeLevel.SELECT))
          .error(accessError("functional_kudu"));
    }

    // Show files.
    for (AuthzTest authzTest: new AuthzTest[]{
        authorize("show files in functional.alltypes"),
        authorize("show files in functional.alltypes partition(month=10, year=2010)")}) {
      for (TPrivilegeLevel privilege: viewMetadataPrivileges()) {
        authzTest.ok(onServer(privilege))
            .ok(onDatabase("functional", privilege))
            .ok(onTable("functional", "alltypes", privilege));
      }
      authzTest.error(accessError("functional"));
    }

    // Show current roles should always be allowed.
    authorize("show current roles").ok();

    // Show roles should always be allowed.
    authorize("show roles").ok();

    // Show role grant group should always be allowed.
    authorize(String.format("show role grant group `%s`", USER.getName())).ok();

    // Show grant role/user should always be allowed.
    try {
      authzCatalog_.addRole("test_role");
      authzCatalog_.addUser("test_user");
      for (String type : new String[]{"role test_role", "user test_user"}) {
        authorize(String.format("show grant %s", type)).ok();
        authorize(String.format("show grant %s on server", type)).ok();
        authorize(String.format("show grant %s on database functional", type)).ok();
        authorize(String.format("show grant %s on table functional.alltypes", type)).ok();
        authorize(String.format("show grant %s on uri '/test-warehouse'", type)).ok();
      }
    } finally {
      authzCatalog_.removeRole("test_role");
      authzCatalog_.removeUser("test_user");
    }

    // Show create table.
    test = authorize("show create table functional.alltypes");
    for (TPrivilegeLevel privilege: viewMetadataPrivileges()) {
      test.ok(onServer(privilege))
          .ok(onDatabase("functional", privilege))
          .ok(onTable("functional", "alltypes", privilege));
    }
    test.error(accessError("functional"));
    // Show create table on non-existent database.
    authorize("show create table nodb.alltypes").error(accessError("nodb.alltypes"));
    // Show create table on non-existent table.
    authorize("show create table functional.notbl")
        .error(accessError("functional.notbl"));

    // Show create view.
    test = authorize("show create view functional.alltypes_view");
    for (TPrivilegeLevel privilege: viewMetadataPrivileges()) {
      test.ok(onServer(privilege, TPrivilegeLevel.SELECT))
          .ok(onDatabase("functional", privilege, TPrivilegeLevel.SELECT))
          .ok(onTable("functional", "alltypes_view", privilege),
              onTable("functional", "alltypes", TPrivilegeLevel.SELECT));
    }
    test.error(accessError("functional"));
    // Show create view on non-existent database.
    authorize("show create view nodb.alltypes").error(accessError("nodb.alltypes"));
    // Show create view on non-existent table.
    authorize("show create view functional.notbl").error(accessError("functional.notbl"));

    // IMPALA-7325: show create view that references built-in function(s) should not
    // require access to the _impala_builtins database this is because reading metadata
    // in system database should always be allowed.
    addTestView(authzCatalog_, "create view functional.test_view as " +
        "select count(*) from functional.alltypes");
    test = authorize("show create view functional.test_view");
    for (TPrivilegeLevel privilege: viewMetadataPrivileges()) {
      test.ok(onServer(privilege, TPrivilegeLevel.SELECT))
          .ok(onDatabase("functional", privilege, TPrivilegeLevel.SELECT))
          .ok(onTable("functional", "test_view", privilege),
              onTable("functional", "alltypes", privilege, TPrivilegeLevel.SELECT));
    }
    test.error(accessError("functional.test_view"))
        .error(accessError("functional.test_view"), onServer(
            allExcept(viewMetadataPrivileges())))
        .error(accessError("functional.test_view"), onDatabase("functional",
            allExcept(viewMetadataPrivileges())))
        .error(accessError("functional.test_view"), onTable("functional", "test_view",
            allExcept(viewMetadataPrivileges())), onTable("functional", "alltypes",
                TPrivilegeLevel.SELECT))
        .error(viewDefError("functional.test_view"), onTable("functional", "test_view",
            TPrivilegeLevel.SELECT), onTable("functional", "alltypes", allExcept(
                viewMetadataPrivileges())));

    // Show create function.
    ScalarFunction fn = addFunction("functional", "f");
    try {
      test = authorize("show create function functional.f");
      for (TPrivilegeLevel privilege: viewMetadataPrivileges()) {
        test.ok(onServer(privilege))
            .ok(onDatabase("functional", privilege));
      }
      test.error(accessError("functional"));
      // Show create function on non-existent database.
      authorize("show create function nodb.f").error(accessError("nodb"));
      // Show create function on non-existent function.
      authorize("show create function functional.nofn").error(accessError("functional"));
    } finally {
      removeFunction(fn);
    }
    // Show create function in system database should always be allowed.
    authorize("show create function _impala_builtins.pi").ok();

    // Show data sources should always be allowed.
    authorize("show data sources").ok();
  }

  /**
   * Test describe output of Databases and tables.
   * From https://issues.apache.org/jira/browse/IMPALA-6479
   * Column level select privileges should limit output.
   */
  @Test
  public void testDescribe() throws ImpalaException {
    // Describe database.
    AuthzTest authzTest = authorize("describe database functional");
    for (TPrivilegeLevel privilege: viewMetadataPrivileges()) {
      authzTest.ok(onServer(privilege))
          .ok(onDatabase("functional", privilege));
    }
    authzTest.error(accessError("functional"))
        .error(accessError("functional"), onServer(allExcept(viewMetadataPrivileges())))
        .error(accessError("functional"), onDatabase("functional",
            allExcept(viewMetadataPrivileges())));

    // Describe on non-existent database.
    authorize("describe database nodb").error(accessError("nodb"));

    // Describe table.
    TTableName tableName = new TTableName("functional", "alltypes");
    TDescribeOutputStyle style = TDescribeOutputStyle.MINIMAL;
    authzTest = authorize("describe functional.alltypes");
    for (TPrivilegeLevel privilege: viewMetadataPrivileges()) {
      authzTest
          .okDescribe(tableName, describeOutput(style).includeStrings(ALLTYPES_COLUMNS),
              onServer(privilege))
          .okDescribe(tableName, describeOutput(style).includeStrings(ALLTYPES_COLUMNS),
              onDatabase("functional", privilege))
          .okDescribe(tableName, describeOutput(style).includeStrings(ALLTYPES_COLUMNS),
              onTable("functional", "alltypes", privilege));
    }
    authzTest
        // In this test, since we only have column level privileges on "id", then
        // only the "id" column should show and the others should not.
        .okDescribe(tableName, describeOutput(style).includeStrings(new String[]{"id"})
            .excludeStrings(ALLTYPES_COLUMNS_WITHOUT_ID), onColumn("functional",
            "alltypes", "id", TPrivilegeLevel.SELECT))
        // If there exists a privilege in the given table/column with incorrect
        // privilege type, we return an empty result instead of an error.
        .okDescribe(tableName, describeOutput(style).excludeStrings(ALLTYPES_COLUMNS),
            onServer(allExcept(viewMetadataPrivileges())))
        .okDescribe(tableName, describeOutput(style).excludeStrings(ALLTYPES_COLUMNS),
            onDatabase("functional", allExcept(viewMetadataPrivileges())))
        .okDescribe(tableName, describeOutput(style).excludeStrings(ALLTYPES_COLUMNS),
            onTable("functional", "alltypes", allExcept(viewMetadataPrivileges())))
        .error(accessError("functional.alltypes"));

    // Describe table extended.
    tableName = new TTableName("functional", "alltypes");
    style = TDescribeOutputStyle.EXTENDED;
    String[] checkStrings = (String[]) ArrayUtils.addAll(ALLTYPES_COLUMNS,
        new String[]{"Location:"});
    authzTest = authorize("describe functional.alltypes");
    for (TPrivilegeLevel privilege: viewMetadataPrivileges()) {
      authzTest
          .okDescribe(tableName, describeOutput(style).includeStrings(checkStrings),
              onServer(privilege))
          .okDescribe(tableName, describeOutput(style).includeStrings(checkStrings),
              onDatabase("functional", privilege))
          .okDescribe(tableName, describeOutput(style).includeStrings(checkStrings),
              onTable("functional", "alltypes", privilege));
    }
    authzTest
        // Location should not appear with only column level auth.
        .okDescribe(tableName, describeOutput(style).includeStrings(new String[]{"id"})
            .excludeStrings((String[]) ArrayUtils.addAll(ALLTYPES_COLUMNS_WITHOUT_ID,
                new String[]{"Location:"})), onColumn("functional", "alltypes", "id",
            TPrivilegeLevel.SELECT))
        // If there exists a privilege in the given table/column with incorrect
        // privilege type, we return an empty result instead of an error.
        .okDescribe(tableName, describeOutput(style).excludeStrings(ALLTYPES_COLUMNS),
            onServer(allExcept(viewMetadataPrivileges())))
        .okDescribe(tableName, describeOutput(style).excludeStrings(ALLTYPES_COLUMNS),
            onDatabase("functional", allExcept(viewMetadataPrivileges())))
        .okDescribe(tableName, describeOutput(style).excludeStrings(ALLTYPES_COLUMNS),
            onTable("functional", "alltypes", allExcept(viewMetadataPrivileges())))
        .error(accessError("functional.alltypes"));

    // Describe view.
    tableName = new TTableName("functional", "alltypes_view");
    style = TDescribeOutputStyle.MINIMAL;
    authzTest = authorize("describe functional.alltypes_view");
    for (TPrivilegeLevel privilege: viewMetadataPrivileges()) {
      authzTest
          .okDescribe(tableName, describeOutput(style).includeStrings(ALLTYPES_COLUMNS),
              onServer(privilege))
          .okDescribe(tableName, describeOutput(style).includeStrings(ALLTYPES_COLUMNS),
              onDatabase("functional", privilege))
          .okDescribe(tableName, describeOutput(style).includeStrings(ALLTYPES_COLUMNS),
              onTable("functional", "alltypes_view", privilege));
    }
    authzTest
        // If there exists a privilege in the given table/column with incorrect
        // privilege type, we return an empty result instead of an error.
        .okDescribe(tableName, describeOutput(style).excludeStrings(ALLTYPES_COLUMNS),
            onServer(allExcept(viewMetadataPrivileges())))
        .okDescribe(tableName, describeOutput(style).excludeStrings(ALLTYPES_COLUMNS),
            onDatabase("functional", allExcept(viewMetadataPrivileges())))
        .okDescribe(tableName, describeOutput(style).excludeStrings(ALLTYPES_COLUMNS),
            onTable("functional", "alltypes_view", allExcept(viewMetadataPrivileges())))
        .error(accessError("functional.alltypes_view"));

    // Describe view extended.
    tableName = new TTableName("functional", "alltypes_view");
    style = TDescribeOutputStyle.EXTENDED;
    // Views have extra output to explicitly check
    String[] viewStrings = new String[]{"View Original Text:", "View Expanded Text:"};
    checkStrings = (String[]) ArrayUtils.addAll(ALLTYPES_COLUMNS, viewStrings);
    authzTest = authorize("describe functional.alltypes_view");
    for (TPrivilegeLevel privilege: viewMetadataPrivileges()) {
      authzTest
          .okDescribe(tableName, describeOutput(style).includeStrings(checkStrings),
              onServer(privilege))
          .okDescribe(tableName, describeOutput(style).includeStrings(checkStrings),
              onDatabase("functional", privilege))
          .okDescribe(tableName, describeOutput(style).includeStrings(checkStrings),
              onTable("functional", "alltypes_view", privilege));
    }
    authzTest
        // If there exists a privilege in the given table/column with incorrect
        // privilege type, we return an empty result instead of an error.
        .okDescribe(tableName, describeOutput(style).excludeStrings(ALLTYPES_COLUMNS),
            onServer(allExcept(viewMetadataPrivileges())))
        .okDescribe(tableName, describeOutput(style).excludeStrings(ALLTYPES_COLUMNS),
            onDatabase("functional", allExcept(viewMetadataPrivileges())))
        .okDescribe(tableName, describeOutput(style).excludeStrings(ALLTYPES_COLUMNS),
            onTable("functional", "alltypes_view", allExcept(viewMetadataPrivileges())))
        .error(accessError("functional.alltypes_view"));

    // Describe specific column on a table.
    authzTest = authorize("describe functional.allcomplextypes.int_struct_col");
    for (TPrivilegeLevel privilege: viewMetadataPrivileges()) {
      authzTest.ok(onServer(privilege))
          .ok(onDatabase("functional", privilege))
          .ok(onTable("functional", "allcomplextypes", privilege));
    }
    authzTest.ok(onColumn("functional", "allcomplextypes", "int_struct_col",
        TPrivilegeLevel.SELECT))
        .error(accessError("functional.allcomplextypes"));

    for (AuthzTest test: new AuthzTest[]{
        // User has access to a different column.
        authorize("describe functional.allcomplextypes.int_struct_col"),
        // Insufficient privileges on complex type column, accessing member
        authorize("describe functional.allcomplextypes.complex_struct_col.f2"),
        // Insufficient privileges on non-complex type column, accessing member
        authorize("describe functional.allcomplextypes.nested_struct_col.f1")}) {
      test.error(accessError("functional.allcomplextypes"), onColumn("functional",
          "allcomplextypes", "id", TPrivilegeLevel.SELECT));
    }
  }

  @Test
  public void testStats() throws ImpalaException {
    // Compute stats.
    authorize("compute stats functional.alltypes")
        .ok(onServer(TPrivilegeLevel.ALL))
        .ok(onServer(TPrivilegeLevel.OWNER))
        .ok(onServer(TPrivilegeLevel.ALTER, TPrivilegeLevel.SELECT))
        .ok(onDatabase("functional", TPrivilegeLevel.ALL))
        .ok(onDatabase("functional", TPrivilegeLevel.OWNER))
        .ok(onDatabase("functional", TPrivilegeLevel.ALTER, TPrivilegeLevel.SELECT))
        .ok(onTable("functional", "alltypes", TPrivilegeLevel.ALL))
        .ok(onTable("functional", "alltypes", TPrivilegeLevel.OWNER))
        .ok(onTable("functional", "alltypes", TPrivilegeLevel.ALTER,
            TPrivilegeLevel.SELECT))
        .error(alterError("functional.alltypes"))
        .error(alterError("functional.alltypes"), onServer(allExcept(
            TPrivilegeLevel.ALL, TPrivilegeLevel.OWNER, TPrivilegeLevel.ALTER,
            TPrivilegeLevel.SELECT)))
        .error(alterError("functional.alltypes"), onDatabase("functional",
            allExcept(TPrivilegeLevel.ALL, TPrivilegeLevel.OWNER, TPrivilegeLevel.ALTER,
                TPrivilegeLevel.SELECT)))
        .error(alterError("functional.alltypes"), onTable("functional", "alltypes",
            allExcept(TPrivilegeLevel.ALL, TPrivilegeLevel.OWNER, TPrivilegeLevel.ALTER,
                TPrivilegeLevel.SELECT)));

    // Compute stats on database that does not exist.
    authorize("compute stats nodb.notbl")
        .error(alterError("nodb.notbl"))
        .error(alterError("nodb.notbl"), onServer(allExcept(
            TPrivilegeLevel.ALL, TPrivilegeLevel.OWNER, TPrivilegeLevel.ALTER,
            TPrivilegeLevel.SELECT)))
        .error(alterError("nodb.notbl"), onDatabase("nodb", allExcept(
            TPrivilegeLevel.ALL, TPrivilegeLevel.OWNER, TPrivilegeLevel.ALTER,
            TPrivilegeLevel.SELECT)));

    // Compute stats on table that does not exist.
    authorize("compute stats functional.notbl")
        .error(alterError("functional.notbl"))
        .error(alterError("functional.notbl"), onServer(allExcept(
            TPrivilegeLevel.ALL, TPrivilegeLevel.OWNER, TPrivilegeLevel.ALTER,
            TPrivilegeLevel.SELECT)))
        .error(alterError("functional.notbl"), onDatabase("functional", allExcept(
            TPrivilegeLevel.ALL, TPrivilegeLevel.OWNER, TPrivilegeLevel.ALTER,
            TPrivilegeLevel.SELECT)));

    // Drop stats.
    authorize("drop stats functional.alltypes")
        .ok(onServer(TPrivilegeLevel.ALL))
        .ok(onServer(TPrivilegeLevel.OWNER))
        .ok(onServer(TPrivilegeLevel.ALTER))
        .ok(onDatabase("functional", TPrivilegeLevel.ALL))
        .ok(onDatabase("functional", TPrivilegeLevel.OWNER))
        .ok(onDatabase("functional", TPrivilegeLevel.ALTER))
        .ok(onTable("functional", "alltypes", TPrivilegeLevel.ALL))
        .ok(onTable("functional", "alltypes", TPrivilegeLevel.OWNER))
        .ok(onTable("functional", "alltypes", TPrivilegeLevel.ALTER))
        .error(alterError("functional.alltypes"))
        .error(alterError("functional.alltypes"), onServer(allExcept(
            TPrivilegeLevel.ALL, TPrivilegeLevel.OWNER, TPrivilegeLevel.ALTER)))
        .error(alterError("functional.alltypes"), onDatabase("functional",
            allExcept(TPrivilegeLevel.ALL, TPrivilegeLevel.OWNER, TPrivilegeLevel.ALTER)))
        .error(alterError("functional.alltypes"), onTable("functional", "alltypes",
            allExcept(TPrivilegeLevel.ALL, TPrivilegeLevel.OWNER,
            TPrivilegeLevel.ALTER)));

    // Drop stats on database that does not exist.
    authorize("drop stats nodb.notbl")
        .error(alterError("nodb.notbl"))
        .error(alterError("nodb.notbl"), onServer(allExcept(
            TPrivilegeLevel.ALL, TPrivilegeLevel.OWNER, TPrivilegeLevel.ALTER)))
        .error(alterError("nodb.notbl"), onDatabase("nodb", allExcept(
            TPrivilegeLevel.ALL, TPrivilegeLevel.OWNER, TPrivilegeLevel.ALTER)));

    // Drop stats on table that does not exist.
    authorize("drop stats functional.notbl")
        .error(alterError("functional.notbl"))
        .error(alterError("functional.notbl"), onServer(allExcept(
            TPrivilegeLevel.ALL, TPrivilegeLevel.OWNER, TPrivilegeLevel.ALTER)))
        .error(alterError("functional.notbl"), onDatabase("functional", allExcept(
            TPrivilegeLevel.ALL, TPrivilegeLevel.OWNER, TPrivilegeLevel.ALTER)));
  }

  @Test
  public void testCreateDatabase() throws ImpalaException {
    for (AuthzTest test: new AuthzTest[]{
        authorize("create database newdb"),
        authorize("create database if not exists newdb")}) {
      test.ok(onServer(TPrivilegeLevel.ALL))
          .ok(onServer(TPrivilegeLevel.OWNER))
          .ok(onServer(TPrivilegeLevel.CREATE))
          .error(createError("newdb"))
          .error(createError("newdb"), onServer(allExcept(TPrivilegeLevel.ALL,
              TPrivilegeLevel.OWNER, TPrivilegeLevel.CREATE)));
    }

    // Create a database with a specific location.
    authorize("create database newdb location " +
        "'hdfs://localhost:20500/test-warehouse/new_location'")
        .ok(onServer(TPrivilegeLevel.ALL))
        .ok(onServer(TPrivilegeLevel.OWNER))
        .ok(onServer(TPrivilegeLevel.CREATE), onUri(
            "hdfs://localhost:20500/test-warehouse/new_location", TPrivilegeLevel.ALL))
        .ok(onServer(TPrivilegeLevel.CREATE), onUri(
            "hdfs://localhost:20500/test-warehouse/new_location", TPrivilegeLevel.OWNER))
        .error(createError("newdb"))
        .error(createError("newdb"), onServer(allExcept(TPrivilegeLevel.ALL,
            TPrivilegeLevel.OWNER, TPrivilegeLevel.CREATE)), onUri(
            "hdfs://localhost:20500/test-warehouse/new_location", TPrivilegeLevel.ALL))
        .error(createError("newdb"), onServer(allExcept(TPrivilegeLevel.ALL,
            TPrivilegeLevel.OWNER, TPrivilegeLevel.CREATE)), onUri(
            "hdfs://localhost:20500/test-warehouse/new_location", TPrivilegeLevel.OWNER))
        .error(accessError("hdfs://localhost:20500/test-warehouse/new_location"),
            onServer(TPrivilegeLevel.CREATE));

    // Database already exists.
    for (AuthzTest test: new AuthzTest[]{
        authorize("create database functional"),
        authorize("create database if not exists functional")}) {
      test.error(createError("functional"))
          .error(createError("functional"), onServer(allExcept(
              TPrivilegeLevel.ALL, TPrivilegeLevel.OWNER, TPrivilegeLevel.CREATE)));
    }

    authorize("create database if not exists _impala_builtins")
        .error(systemDbError(), onServer(TPrivilegeLevel.ALL))
        .error(systemDbError(), onServer(TPrivilegeLevel.OWNER));
  }

  @Test
  public void testCreateTable() throws ImpalaException {
    for (AuthzTest test: new AuthzTest[]{
        authorize("create table functional.new_table(i int)"),
        authorize("create external table functional.new_table(i int)")}) {
      test.ok(onServer(TPrivilegeLevel.ALL))
          .ok(onServer(TPrivilegeLevel.OWNER))
          .ok(onServer(TPrivilegeLevel.CREATE))
          .ok(onDatabase("functional", TPrivilegeLevel.ALL))
          .ok(onDatabase("functional", TPrivilegeLevel.OWNER))
          .ok(onDatabase("functional", TPrivilegeLevel.CREATE))
          .error(createError("functional"), onServer(allExcept(
              TPrivilegeLevel.ALL, TPrivilegeLevel.OWNER, TPrivilegeLevel.CREATE)))
          .error(createError("functional"), onDatabase("functional", allExcept(
              TPrivilegeLevel.ALL, TPrivilegeLevel.OWNER, TPrivilegeLevel.CREATE)));
    }

    // Create table like.
    authorize("create table functional.new_table like functional.alltypes")
        .ok(onServer(TPrivilegeLevel.ALL))
        .ok(onServer(TPrivilegeLevel.OWNER))
        .ok(onServer(join(viewMetadataPrivileges(), TPrivilegeLevel.CREATE)))
        .ok(onDatabase("functional", TPrivilegeLevel.ALL))
        .ok(onDatabase("functional", TPrivilegeLevel.OWNER))
        .ok(onDatabase("functional", join(viewMetadataPrivileges(),
            TPrivilegeLevel.CREATE)))
        .ok(onDatabase("functional"), onDatabase("functional", TPrivilegeLevel.CREATE),
            onTable("functional", "alltypes", viewMetadataPrivileges()))
        .error(accessError("functional.alltypes"))
        .error(accessError("functional.alltypes"), onServer(allExcept(
            join(viewMetadataPrivileges(), TPrivilegeLevel.CREATE))))
        .error(createError("functional"), onDatabase("functional", allExcept(
            TPrivilegeLevel.ALL, TPrivilegeLevel.OWNER, TPrivilegeLevel.CREATE,
            TPrivilegeLevel.SELECT)))
        .error(createError("functional"), onDatabase("functional", allExcept(
            TPrivilegeLevel.ALL, TPrivilegeLevel.OWNER, TPrivilegeLevel.CREATE)), onTable(
                "functional", "alltypes", viewMetadataPrivileges()))
        .error(accessError("functional.alltypes"), onDatabase("functional",
            TPrivilegeLevel.CREATE), onTable("functional", "alltypes", allExcept(
                viewMetadataPrivileges())));

    // Table already exists.
    for (AuthzTest test : new AuthzTest[]{
        authorize("create table functional.alltypes(i int)"),
        authorize("create table if not exists functional.alltypes(i int)")}) {
      test.error(createError("functional"))
          .error(createError("functional"), onServer(allExcept(
          TPrivilegeLevel.ALL, TPrivilegeLevel.OWNER, TPrivilegeLevel.CREATE)))
          .error(createError("functional"), onDatabase("functional", allExcept(
              TPrivilegeLevel.ALL, TPrivilegeLevel.OWNER, TPrivilegeLevel.CREATE)));
    }

    // CTAS.
    for (AuthzTest test: new AuthzTest[]{
        authorize("create table functional.new_table as " +
            "select int_col from functional.alltypes"),
        // Explain CTAS.
        authorize("explain create table functional.new_table as " +
            "select int_col from functional.alltypes")}) {
      test.ok(onServer(TPrivilegeLevel.ALL))
          .ok(onServer(TPrivilegeLevel.OWNER))
          .ok(onServer(TPrivilegeLevel.CREATE, TPrivilegeLevel.INSERT,
              TPrivilegeLevel.SELECT))
          .ok(onDatabase("functional", TPrivilegeLevel.ALL))
          .ok(onDatabase("functional", TPrivilegeLevel.OWNER))
          .ok(onDatabase("functional", TPrivilegeLevel.CREATE, TPrivilegeLevel.INSERT,
              TPrivilegeLevel.SELECT))
          .ok(onDatabase("functional"), onDatabase("functional", TPrivilegeLevel.CREATE,
              TPrivilegeLevel.INSERT), onTable("functional", "alltypes",
              TPrivilegeLevel.SELECT))
          .ok(onDatabase("functional"), onDatabase("functional", TPrivilegeLevel.CREATE,
              TPrivilegeLevel.INSERT), onColumn("functional", "alltypes", "int_col",
              TPrivilegeLevel.ALL))
          .ok(onDatabase("functional"), onDatabase("functional", TPrivilegeLevel.CREATE,
              TPrivilegeLevel.INSERT), onColumn("functional", "alltypes", "int_col",
              TPrivilegeLevel.OWNER))
          .error(createError("functional"))
          .error(createError("functional"), onServer(allExcept(
              TPrivilegeLevel.ALL, TPrivilegeLevel.OWNER, TPrivilegeLevel.CREATE,
              TPrivilegeLevel.INSERT, TPrivilegeLevel.SELECT)))
          .error(createError("functional"), onDatabase("functional", allExcept(
              TPrivilegeLevel.ALL, TPrivilegeLevel.OWNER, TPrivilegeLevel.CREATE,
              TPrivilegeLevel.INSERT, TPrivilegeLevel.SELECT)))
          .error(createError("functional"), onDatabase("functional", allExcept(
              TPrivilegeLevel.ALL, TPrivilegeLevel.OWNER, TPrivilegeLevel.CREATE,
              TPrivilegeLevel.INSERT)),
              onTable("functional", "alltypes", TPrivilegeLevel.SELECT))
          .error(selectError("functional"), onDatabase("functional",
              TPrivilegeLevel.CREATE, TPrivilegeLevel.INSERT), onTable("functional",
              "alltypes", allExcept(TPrivilegeLevel.ALL, TPrivilegeLevel.OWNER,
              TPrivilegeLevel.SELECT)));
    }

    // Table with a specific location.
    authorize("create table functional.new_table(i int) location " +
        "'hdfs://localhost:20500/test-warehouse/new_table'")
        .ok(onServer(TPrivilegeLevel.ALL))
        .ok(onServer(TPrivilegeLevel.OWNER))
        .ok(onServer(TPrivilegeLevel.CREATE),
            onUri("hdfs://localhost:20500/test-warehouse/new_table", TPrivilegeLevel.ALL))
        .ok(onServer(TPrivilegeLevel.CREATE),
            onUri("hdfs://localhost:20500/test-warehouse/new_table",
            TPrivilegeLevel.OWNER))
        .error(createError("functional"), onServer(allExcept(
            TPrivilegeLevel.ALL, TPrivilegeLevel.OWNER, TPrivilegeLevel.CREATE)))
        .error(createError("functional"), onDatabase("functional", allExcept(
            TPrivilegeLevel.ALL, TPrivilegeLevel.OWNER, TPrivilegeLevel.CREATE)), onUri(
                "hdfs://localhost:20500/test-warehouse/new_table", TPrivilegeLevel.ALL))
        .error(createError("functional"), onDatabase("functional", allExcept(
            TPrivilegeLevel.ALL, TPrivilegeLevel.OWNER, TPrivilegeLevel.CREATE)), onUri(
                "hdfs://localhost:20500/test-warehouse/new_table", TPrivilegeLevel.OWNER))
        .error(accessError("hdfs://localhost:20500/test-warehouse/new_table"),
            onDatabase("functional", TPrivilegeLevel.CREATE));

    // External table with URI location.
    authorize("create external table functional.new_table(i int) location " +
        "'hdfs://localhost:20500/test-warehouse/UPPER_CASE/test'")
        .ok(onServer(TPrivilegeLevel.ALL))
        .ok(onServer(TPrivilegeLevel.OWNER))
        .ok(onServer(TPrivilegeLevel.CREATE),
            onUri("hdfs://localhost:20500/test-warehouse/UPPER_CASE/test",
                TPrivilegeLevel.ALL))
        .ok(onServer(TPrivilegeLevel.CREATE),
            onUri("hdfs://localhost:20500/test-warehouse/UPPER_CASE/test",
                TPrivilegeLevel.OWNER))
        // Wrong case letters on URI.
        .error(accessError("hdfs://localhost:20500/test-warehouse/UPPER_CASE/test"),
            onServer(TPrivilegeLevel.CREATE),
            onUri("hdfs://localhost:20500/test-warehouse/upper_case/test",
                TPrivilegeLevel.ALL))
        .error(accessError("hdfs://localhost:20500/test-warehouse/UPPER_CASE/test"),
            onServer(TPrivilegeLevel.CREATE),
            onUri("hdfs://localhost:20500/test-warehouse/upper_case/test",
                TPrivilegeLevel.OWNER))
        .error(createError("functional"), onServer(allExcept(
            TPrivilegeLevel.ALL, TPrivilegeLevel.OWNER, TPrivilegeLevel.CREATE)))
        .error(createError("functional"), onDatabase("functional", allExcept(
            TPrivilegeLevel.ALL, TPrivilegeLevel.OWNER, TPrivilegeLevel.CREATE)), onUri(
            "hdfs://localhost:20500/test-warehouse/UPPER_CASE/test", TPrivilegeLevel.ALL))
        .error(createError("functional"), onDatabase("functional", allExcept(
            TPrivilegeLevel.ALL, TPrivilegeLevel.OWNER, TPrivilegeLevel.CREATE)), onUri(
            "hdfs://localhost:20500/test-warehouse/UPPER_CASE/test",
            TPrivilegeLevel.OWNER))
        .error(accessError("hdfs://localhost:20500/test-warehouse/UPPER_CASE/test"),
            onDatabase("functional", TPrivilegeLevel.CREATE));

    authorize("create table functional.new_table like parquet "
        + "'hdfs://localhost:20500/test-warehouse/schemas/alltypestiny.parquet'")
        .ok(onServer(TPrivilegeLevel.ALL))
        .ok(onServer(TPrivilegeLevel.OWNER))
        .ok(onServer(TPrivilegeLevel.CREATE),
            onUri("hdfs://localhost:20500/test-warehouse/schemas/alltypestiny.parquet",
                TPrivilegeLevel.ALL))
        .ok(onServer(TPrivilegeLevel.CREATE),
            onUri("hdfs://localhost:20500/test-warehouse/schemas/alltypestiny.parquet",
                TPrivilegeLevel.OWNER))
        .error(accessError(
            "hdfs://localhost:20500/test-warehouse/schemas/alltypestiny.parquet"),
            onServer(allExcept(TPrivilegeLevel.ALL, TPrivilegeLevel.OWNER,
                TPrivilegeLevel.CREATE)))
        .error(createError("functional"), onDatabase("functional", allExcept(
            TPrivilegeLevel.ALL, TPrivilegeLevel.OWNER, TPrivilegeLevel.CREATE)), onUri(
            "hdfs://localhost:20500/test-warehouse/schemas/alltypestiny.parquet",
            TPrivilegeLevel.ALL))
        .error(createError("functional"), onDatabase("functional", allExcept(
            TPrivilegeLevel.ALL, TPrivilegeLevel.OWNER, TPrivilegeLevel.CREATE)), onUri(
            "hdfs://localhost:20500/test-warehouse/schemas/alltypestiny.parquet",
            TPrivilegeLevel.OWNER))
        .error(accessError(
            "hdfs://localhost:20500/test-warehouse/schemas/alltypestiny.parquet"),
            onDatabase("functional", TPrivilegeLevel.CREATE));

    authorize("create table if not exists _impala_builtins.new_table(i int)")
        .error(systemDbError(), onServer(TPrivilegeLevel.ALL))
        .error(systemDbError(), onServer(TPrivilegeLevel.OWNER));

    // IMPALA-4000: Only users with ALL/OWNER privileges on SERVER may create external
    // Kudu tables.
    authorize("create external table functional.kudu_tbl stored as kudu " +
        "tblproperties ('kudu.master_addresses'='127.0.0.1', 'kudu.table_name'='tbl')")
        .ok(onServer(TPrivilegeLevel.ALL))
        .ok(onServer(TPrivilegeLevel.OWNER))
        .error(createError("functional"))
        .error(accessError("server1"), onServer(allExcept(TPrivilegeLevel.ALL,
            TPrivilegeLevel.OWNER)))
        .error(accessError("server1"), onDatabase("functional", TPrivilegeLevel.ALL))
        .error(accessError("server1"), onDatabase("functional", TPrivilegeLevel.OWNER));

    // IMPALA-4000: ALL/OWNER privileges on SERVER are not required to create managed
    // tables.
    authorize("create table functional.kudu_tbl (i int, j int, primary key (i))" +
        " partition by hash (i) partitions 9 stored as kudu")
        .ok(onServer(TPrivilegeLevel.ALL))
        .ok(onServer(TPrivilegeLevel.OWNER))
        .ok(onDatabase("functional", TPrivilegeLevel.ALL))
        .ok(onDatabase("functional", TPrivilegeLevel.OWNER))
        .ok(onDatabase("functional", TPrivilegeLevel.CREATE))
        .error(createError("functional"))
        .error(createError("functional"), onServer(allExcept(
            TPrivilegeLevel.ALL, TPrivilegeLevel.OWNER, TPrivilegeLevel.CREATE)))
        .error(createError("functional"), onDatabase("functional", allExcept(
            TPrivilegeLevel.ALL, TPrivilegeLevel.OWNER, TPrivilegeLevel.CREATE)));

    // IMPALA-6451: CTAS for Kudu tables on non-external tables and without
    // TBLPROPERTIES ('kudu.master_addresses') should not require ALL/OWNER privileges
    // on SERVER.
    // The statement below causes the SQL statement to be rewritten.
    authorize("create table functional.kudu_tbl primary key (bigint_col) " +
        "stored as kudu as " +
        "select bigint_col, string_col, current_timestamp() as ins_date " +
        "from functional.alltypes " +
        "where exists (select 1 from functional.alltypes)")
      .ok(onServer(TPrivilegeLevel.ALL))
      .ok(onServer(TPrivilegeLevel.OWNER))
      .ok(onDatabase("functional", TPrivilegeLevel.CREATE, TPrivilegeLevel.INSERT,
          TPrivilegeLevel.SELECT))
      .error(createError("functional"))
      .error(createError("functional"), onServer(allExcept(TPrivilegeLevel.ALL,
          TPrivilegeLevel.OWNER, TPrivilegeLevel.CREATE, TPrivilegeLevel.INSERT,
          TPrivilegeLevel.SELECT)))
      .error(createError("functional"), onDatabase("functional", allExcept(
          TPrivilegeLevel.ALL, TPrivilegeLevel.OWNER, TPrivilegeLevel.CREATE,
          TPrivilegeLevel.INSERT, TPrivilegeLevel.SELECT)));

    // Database does not exist.
    authorize("create table nodb.new_table(i int)")
        .error(createError("nodb"))
        .error(createError("nodb"), onServer(allExcept(
            TPrivilegeLevel.ALL, TPrivilegeLevel.OWNER, TPrivilegeLevel.CREATE)))
        .error(createError("nodb"), onDatabase("functional", allExcept(
            TPrivilegeLevel.ALL, TPrivilegeLevel.OWNER, TPrivilegeLevel.CREATE)));
  }

  @Test
  public void testCreateView() throws ImpalaException {
    for (AuthzTest test: new AuthzTest[]{
        authorize("create view functional.new_view as " +
            "select int_col from functional.alltypes"),
        authorize("create view functional.new_view(a) as " +
            "select int_col from functional.alltypes")}) {
      test.ok(onServer(TPrivilegeLevel.ALL))
          .ok(onServer(TPrivilegeLevel.OWNER))
          .ok(onServer(TPrivilegeLevel.CREATE, TPrivilegeLevel.SELECT))
          .ok(onDatabase("functional", TPrivilegeLevel.ALL))
          .ok(onDatabase("functional", TPrivilegeLevel.OWNER))
          .ok(onDatabase("functional", TPrivilegeLevel.CREATE, TPrivilegeLevel.SELECT))
          .ok(onDatabase("functional", TPrivilegeLevel.CREATE), onTable(
              "functional", "alltypes", TPrivilegeLevel.SELECT))
          .ok(onDatabase("functional", TPrivilegeLevel.CREATE), onColumn(
              "functional", "alltypes", "int_col", TPrivilegeLevel.ALL))
          .ok(onDatabase("functional", TPrivilegeLevel.CREATE), onColumn(
              "functional", "alltypes", "int_col", TPrivilegeLevel.OWNER))
          .error(selectError("functional.alltypes"))
          .error(selectError("functional.alltypes"), onServer(allExcept(
              TPrivilegeLevel.ALL, TPrivilegeLevel.OWNER, TPrivilegeLevel.CREATE,
              TPrivilegeLevel.SELECT)))
          .error(createError("functional"), onDatabase("functional", allExcept(
              TPrivilegeLevel.ALL, TPrivilegeLevel.OWNER, TPrivilegeLevel.CREATE)))
          .error(selectError("functional.alltypes"), onDatabase("functional", allExcept(
              TPrivilegeLevel.ALL, TPrivilegeLevel.OWNER, TPrivilegeLevel.SELECT)))
          .error(selectError("functional.alltypes"), onTable("functional", "alltypes",
              allExcept(TPrivilegeLevel.ALL, TPrivilegeLevel.OWNER,
              TPrivilegeLevel.SELECT)));
    }

    // View with constant select.
    authorize("create view functional.new_view as select 1")
        .ok(onServer(TPrivilegeLevel.ALL))
        .ok(onServer(TPrivilegeLevel.OWNER))
        .ok(onServer(TPrivilegeLevel.CREATE))
        .ok(onDatabase("functional", TPrivilegeLevel.ALL))
        .ok(onDatabase("functional", TPrivilegeLevel.OWNER))
        .ok(onDatabase("functional", TPrivilegeLevel.CREATE))
        .error(createError("functional"))
        .error(createError("functional"), onServer(allExcept(
            TPrivilegeLevel.ALL, TPrivilegeLevel.OWNER, TPrivilegeLevel.CREATE,
            TPrivilegeLevel.SELECT)))
        .error(createError("functional"), onDatabase("functional", allExcept(
            TPrivilegeLevel.ALL, TPrivilegeLevel.OWNER, TPrivilegeLevel.CREATE)));

    // View already exists.
    for (AuthzTest test: new AuthzTest[]{
        authorize("create view functional.alltypes_view as " +
            "select int_col from functional.alltypes"),
        authorize("create view if not exists functional.alltypes_view as " +
            "select int_col from functional.alltypes")}) {
      test.error(selectError("functional.alltypes"))
          .error(selectError("functional.alltypes"), onServer(allExcept(
              TPrivilegeLevel.ALL, TPrivilegeLevel.OWNER, TPrivilegeLevel.CREATE,
              TPrivilegeLevel.SELECT)))
          .error(createError("functional"), onDatabase("functional", allExcept(
              TPrivilegeLevel.ALL, TPrivilegeLevel.OWNER, TPrivilegeLevel.CREATE)))
          .error(selectError("functional.alltypes"), onDatabase("functional", allExcept(
              TPrivilegeLevel.ALL, TPrivilegeLevel.OWNER, TPrivilegeLevel.SELECT)))
          .error(selectError("functional.alltypes"), onTable("functional", "alltypes",
              allExcept(TPrivilegeLevel.ALL, TPrivilegeLevel.OWNER,
              TPrivilegeLevel.SELECT)));
    }

    authorize("create view if not exists _impala_builtins.new_view as select 1")
        .error(systemDbError(), onServer(TPrivilegeLevel.ALL))
        .error(systemDbError(), onServer(TPrivilegeLevel.OWNER));

    // Database does not exist.
    authorize("create view nodb.new_view as select 1")
        .error(createError("nodb"))
        .error(createError("nodb"), onServer(allExcept(
            TPrivilegeLevel.ALL, TPrivilegeLevel.OWNER, TPrivilegeLevel.CREATE)))
        .error(createError("nodb"), onDatabase("functional", allExcept(
            TPrivilegeLevel.ALL, TPrivilegeLevel.OWNER, TPrivilegeLevel.CREATE)));
  }

  @Test
  public void testDropDatabase() throws ImpalaException {
    for (AuthzTest test: new AuthzTest[]{
        authorize("drop database functional"),
        authorize("drop database functional cascade"),
        authorize("drop database functional restrict")}) {
      test.ok(onServer(TPrivilegeLevel.ALL))
          .ok(onServer(TPrivilegeLevel.OWNER))
          .ok(onServer(TPrivilegeLevel.DROP))
          .ok(onDatabase("functional", TPrivilegeLevel.ALL))
          .ok(onDatabase("functional", TPrivilegeLevel.OWNER))
          .ok(onDatabase("functional", TPrivilegeLevel.DROP))
          .error(dropError("functional"))
          .error(dropError("functional"), onServer(allExcept(
              TPrivilegeLevel.ALL, TPrivilegeLevel.OWNER, TPrivilegeLevel.DROP)))
          .error(dropError("functional"), onDatabase("functional",
              allExcept(TPrivilegeLevel.ALL, TPrivilegeLevel.OWNER,
              TPrivilegeLevel.DROP)));
    }

    // Database does not exist.
    for (AuthzTest test: new AuthzTest[]{
        authorize("drop database nodb"),
        authorize("drop database nodb cascade"),
        authorize("drop database nodb restrict")}) {
      test.error(dropError("nodb"))
          .error(dropError("nodb"), onServer(
              allExcept(TPrivilegeLevel.ALL, TPrivilegeLevel.OWNER,
              TPrivilegeLevel.DROP)));
    }

    // Database does not exist but with if exists clause.
    for (AuthzTest test: new AuthzTest[]{
        authorize("drop database if exists nodb"),
        authorize("drop database if exists nodb cascade"),
        authorize("drop database if exists nodb restrict")}) {
      test.ok(onServer(TPrivilegeLevel.ALL))
          .ok(onServer(TPrivilegeLevel.OWNER))
          .ok(onServer(TPrivilegeLevel.DROP))
          .error(dropError("nodb"))
          .error(dropError("nodb"), onServer(allExcept(
              TPrivilegeLevel.ALL, TPrivilegeLevel.OWNER, TPrivilegeLevel.DROP)));
    }

    // Dropping system database is not allowed even if with ALL/OWNER privilege on server.
    authorize("drop database _impala_builtins")
        .error(systemDbError(), onServer(TPrivilegeLevel.ALL))
        .error(systemDbError(), onServer(TPrivilegeLevel.OWNER));
  }

  @Test
  public void testDropTable() throws ImpalaException {
    authorize("drop table functional.alltypes")
        .ok(onServer(TPrivilegeLevel.ALL))
        .ok(onServer(TPrivilegeLevel.OWNER))
        .ok(onServer(TPrivilegeLevel.DROP))
        .ok(onDatabase("functional", TPrivilegeLevel.ALL))
        .ok(onDatabase("functional", TPrivilegeLevel.OWNER))
        .ok(onDatabase("functional", TPrivilegeLevel.DROP))
        .ok(onTable("functional", "alltypes", TPrivilegeLevel.ALL))
        .ok(onTable("functional", "alltypes", TPrivilegeLevel.OWNER))
        .ok(onTable("functional", "alltypes", TPrivilegeLevel.DROP))
        .error(dropError("functional.alltypes"))
        .error(dropError("functional.alltypes"), onServer(allExcept(
            TPrivilegeLevel.ALL, TPrivilegeLevel.OWNER, TPrivilegeLevel.DROP)))
        .error(dropError("functional.alltypes"), onDatabase("functional",
            allExcept(TPrivilegeLevel.ALL, TPrivilegeLevel.OWNER, TPrivilegeLevel.DROP)))
        .error(dropError("functional.alltypes"), onTable("functional", "alltypes",
            allExcept(TPrivilegeLevel.ALL, TPrivilegeLevel.OWNER, TPrivilegeLevel.DROP)));

    // Database/Table does not exist.
    authorize("drop table nodb.notbl")
        .error(dropError("nodb.notbl"))
        .error(dropError("nodb.notbl"), onServer(allExcept(
            TPrivilegeLevel.ALL, TPrivilegeLevel.OWNER, TPrivilegeLevel.DROP)))
        .error(dropError("nodb.notbl"), onDatabase("functional",
            allExcept(TPrivilegeLevel.ALL, TPrivilegeLevel.OWNER, TPrivilegeLevel.DROP)));

    // Table does not exist.
    authorize("drop table functional.notbl")
        .error(dropError("functional.notbl"))
        .error(dropError("functional.notbl"), onServer(allExcept(
            TPrivilegeLevel.ALL, TPrivilegeLevel.OWNER, TPrivilegeLevel.DROP)))
        .error(dropError("functional.notbl"), onDatabase("functional",
            allExcept(TPrivilegeLevel.ALL, TPrivilegeLevel.OWNER, TPrivilegeLevel.DROP)));

    // Table does not exist but with if exists clause.
    authorize("drop table if exists functional.notbl")
        .ok(onServer(TPrivilegeLevel.ALL))
        .ok(onServer(TPrivilegeLevel.OWNER))
        .ok(onServer(TPrivilegeLevel.DROP))
        .ok(onDatabase("functional", TPrivilegeLevel.ALL))
        .ok(onDatabase("functional", TPrivilegeLevel.OWNER))
        .ok(onDatabase("functional", TPrivilegeLevel.DROP))
        .error(dropError("functional.notbl"))
        .error(dropError("functional.notbl"), onServer(allExcept(
            TPrivilegeLevel.ALL, TPrivilegeLevel.OWNER, TPrivilegeLevel.DROP)))
        .error(dropError("functional.notbl"), onDatabase("functional",
            allExcept(TPrivilegeLevel.ALL, TPrivilegeLevel.OWNER, TPrivilegeLevel.DROP)));

    // Dropping any tables in the system database is not allowed even with ALL/OWNER
    // privilege on server.
    authorize("drop table _impala_builtins.tbl")
        .error(systemDbError(), onServer(TPrivilegeLevel.ALL))
        .error(systemDbError(), onServer(TPrivilegeLevel.OWNER));
  }

  @Test
  public void testDropView() throws ImpalaException {
    authorize("drop view functional.alltypes_view")
        .ok(onServer(TPrivilegeLevel.ALL))
        .ok(onServer(TPrivilegeLevel.OWNER))
        .ok(onServer(TPrivilegeLevel.DROP))
        .ok(onDatabase("functional", TPrivilegeLevel.ALL))
        .ok(onDatabase("functional", TPrivilegeLevel.OWNER))
        .ok(onDatabase("functional", TPrivilegeLevel.DROP))
        .ok(onTable("functional", "alltypes_view", TPrivilegeLevel.ALL))
        .ok(onTable("functional", "alltypes_view", TPrivilegeLevel.OWNER))
        .ok(onTable("functional", "alltypes_view", TPrivilegeLevel.DROP))
        .error(dropError("functional.alltypes_view"))
        .error(dropError("functional.alltypes_view"), onServer(allExcept(
            TPrivilegeLevel.ALL, TPrivilegeLevel.OWNER, TPrivilegeLevel.DROP)))
        .error(dropError("functional.alltypes_view"), onDatabase("functional",
            allExcept(TPrivilegeLevel.ALL, TPrivilegeLevel.OWNER, TPrivilegeLevel.DROP)))
        .error(dropError("functional.alltypes_view"), onTable("functional",
            "alltypes_view", allExcept(TPrivilegeLevel.ALL, TPrivilegeLevel.OWNER,
            TPrivilegeLevel.DROP)));

    // Database does not exist.
    authorize("drop view nodb.noview")
        .error(dropError("nodb.noview"))
        .error(dropError("nodb.noview"), onServer(allExcept(
            TPrivilegeLevel.ALL, TPrivilegeLevel.OWNER, TPrivilegeLevel.DROP)))
        .error(dropError("nodb.noview"), onDatabase("functional",
            allExcept(TPrivilegeLevel.ALL, TPrivilegeLevel.OWNER, TPrivilegeLevel.DROP)));

    // View does not exist.
    authorize("drop table functional.noview")
        .error(dropError("functional.noview"))
        .error(dropError("functional.noview"), onServer(allExcept(
            TPrivilegeLevel.ALL, TPrivilegeLevel.OWNER, TPrivilegeLevel.DROP)))
        .error(dropError("functional.noview"), onDatabase("functional",
            allExcept(TPrivilegeLevel.ALL, TPrivilegeLevel.OWNER, TPrivilegeLevel.DROP)));

    // View does not exist but with if exists clause.
    authorize("drop table if exists functional.noview")
        .ok(onServer(TPrivilegeLevel.ALL))
        .ok(onServer(TPrivilegeLevel.OWNER))
        .ok(onServer(TPrivilegeLevel.DROP))
        .ok(onDatabase("functional", TPrivilegeLevel.ALL))
        .ok(onDatabase("functional", TPrivilegeLevel.OWNER))
        .ok(onDatabase("functional", TPrivilegeLevel.DROP))
        .error(dropError("functional.noview"))
        .error(dropError("functional.noview"), onServer(allExcept(
            TPrivilegeLevel.ALL, TPrivilegeLevel.OWNER, TPrivilegeLevel.DROP)))
        .error(dropError("functional.noview"), onDatabase("functional",
            allExcept(TPrivilegeLevel.ALL, TPrivilegeLevel.OWNER, TPrivilegeLevel.DROP)));

    // Dropping any views in the system database is not allowed even with ALL/OWNER
    // privilege on server.
    authorize("drop table _impala_builtins.v")
        .error(systemDbError(), onServer(TPrivilegeLevel.ALL))
        .error(systemDbError(), onServer(TPrivilegeLevel.OWNER));
  }

  @Test
  public void testAlterTable() throws ImpalaException {
    for (AuthzTest test: new AuthzTest[]{
        authorize("alter table functional.alltypes add column c1 int"),
        authorize("alter table functional.alltypes add columns(c1 int)"),
        authorize("alter table functional.alltypes replace columns(c1 int)"),
        authorize("alter table functional.alltypes change int_col c1 int"),
        authorize("alter table functional.alltypes drop int_col"),
        authorize("alter table functional.alltypes set fileformat parquet"),
        authorize("alter table functional.alltypes set tblproperties('a'='b')"),
        authorize("alter table functional.alltypes partition(year=2009) " +
            "set tblproperties('a'='b')"),
        authorize("alter table functional.alltypes set cached in 'testPool'"),
        authorize("alter table functional.alltypes partition(year=2009) set cached " +
            "in 'testPool'"),
        authorize("alter table functional.alltypes sort by (id)"),
        authorize("alter table functional.alltypes set column stats int_col " +
            "('numNulls'='1')"),
        authorize("alter table functional.alltypes recover partitions"),
        authorize("alter table functional.alltypes set row format delimited fields " +
            "terminated by ' '"),
        authorize("alter table functional.alltypes partition(year=2009) set row format " +
            "delimited fields terminated by ' '"),
        authorize("alter table functional.alltypes add partition(year=1, month=1)"),
        authorize("alter table functional.alltypes drop partition(" +
            "year=2009, month=1)")}) {
      test.ok(onServer(TPrivilegeLevel.ALL))
          .ok(onServer(TPrivilegeLevel.OWNER))
          .ok(onServer(TPrivilegeLevel.ALTER))
          .ok(onDatabase("functional", TPrivilegeLevel.ALL))
          .ok(onDatabase("functional", TPrivilegeLevel.OWNER))
          .ok(onDatabase("functional", TPrivilegeLevel.ALTER))
          .ok(onTable("functional", "alltypes", TPrivilegeLevel.ALL))
          .ok(onTable("functional", "alltypes", TPrivilegeLevel.OWNER))
          .ok(onTable("functional", "alltypes", TPrivilegeLevel.ALTER))
          .error(alterError("functional.alltypes"))
          .error(alterError("functional.alltypes"), onServer(allExcept(
              TPrivilegeLevel.ALL, TPrivilegeLevel.OWNER, TPrivilegeLevel.ALTER)))
          .error(alterError("functional.alltypes"), onDatabase("functional", allExcept(
              TPrivilegeLevel.ALL, TPrivilegeLevel.OWNER, TPrivilegeLevel.ALTER)))
          .error(alterError("functional.alltypes"), onTable("functional", "alltypes",
              allExcept(TPrivilegeLevel.ALL, TPrivilegeLevel.OWNER,
              TPrivilegeLevel.ALTER)));
    }

    try {
      // We cannot set an owner to a role that doesn't exist
      authzCatalog_.addRole("foo_owner");
      // Alter table set owner.
      for (AuthzTest test : new AuthzTest[]{
          authorize("alter table functional.alltypes set owner user foo_owner"),
          authorize("alter table functional.alltypes set owner role foo_owner")}) {
        test.ok(onServer(true, TPrivilegeLevel.ALL))
            .ok(onServer(true, TPrivilegeLevel.OWNER))
            .ok(onDatabase(true, "functional", TPrivilegeLevel.ALL))
            .ok(onDatabase(true, "functional", TPrivilegeLevel.OWNER))
            .ok(onTable(true, "functional", "alltypes", TPrivilegeLevel.ALL))
            .ok(onTable(true, "functional", "alltypes", TPrivilegeLevel.OWNER))
            .error(accessError(true, "functional.alltypes"))
            .error(accessError(true, "functional.alltypes"), onServer(true, allExcept(
                TPrivilegeLevel.ALL, TPrivilegeLevel.OWNER)))
            .error(accessError(true, "functional.alltypes"), onDatabase(true,
                "functional", allExcept(TPrivilegeLevel.ALL, TPrivilegeLevel.OWNER)))
            .error(accessError(true, "functional.alltypes"),
                onTable(true, "functional", "alltypes", allExcept(
                    TPrivilegeLevel.ALL, TPrivilegeLevel.OWNER)));
        // TODO: Checking if a request is allowed by checking if grant option flag is set
        // is to Sentry.
        if (authzProvider_ == AuthorizationProvider.SENTRY) {
          test.error(accessError(true, "functional.alltypes"), onServer(
              TPrivilegeLevel.values()))
              .error(accessError(true, "functional.alltypes"), onDatabase("functional",
                  TPrivilegeLevel.values()))
              .error(accessError(true, "functional.alltypes"), onTable("functional",
                  "alltypes", TPrivilegeLevel.values()));
        }
      }
    } finally {
      authzCatalog_.removeRole("foo_owner");
    }

    boolean exceptionThrown = false;
    try {
      parseAndAnalyze("alter table functional.alltypes set owner role foo_owner",
          authzCtx_, frontend_);
    } catch (AnalysisException e) {
      exceptionThrown = true;
      assertEquals("Role 'foo_owner' does not exist.", e.getLocalizedMessage());
    }
    assertTrue(exceptionThrown);

    // Alter table rename.
    authorize("alter table functional.alltypes rename to functional_parquet.new_table")
        .ok(onServer(TPrivilegeLevel.ALL))
        .ok(onServer(TPrivilegeLevel.OWNER))
        .ok(onDatabase("functional", TPrivilegeLevel.ALL),
            onDatabase("functional_parquet", TPrivilegeLevel.CREATE))
        .ok(onDatabase("functional_parquet", TPrivilegeLevel.CREATE),
            onTable("functional", "alltypes", TPrivilegeLevel.ALL))
        .ok(onDatabase("functional", TPrivilegeLevel.OWNER),
            onDatabase("functional_parquet", TPrivilegeLevel.CREATE))
        .ok(onDatabase("functional_parquet", TPrivilegeLevel.CREATE),
            onTable("functional", "alltypes", TPrivilegeLevel.OWNER))
        .error(accessError("functional.alltypes"))
        .error(accessError("functional.alltypes"), onServer(allExcept(
            TPrivilegeLevel.ALL, TPrivilegeLevel.OWNER, TPrivilegeLevel.CREATE)))
        .error(accessError("functional.alltypes"), onDatabase("functional", allExcept(
            TPrivilegeLevel.ALL, TPrivilegeLevel.OWNER)), onDatabase("functional_parquet",
            TPrivilegeLevel.CREATE))
        .error(createError("functional_parquet"), onDatabase("functional",
            TPrivilegeLevel.ALL), onDatabase("functional_parquet", allExcept(
            TPrivilegeLevel.ALL, TPrivilegeLevel.OWNER, TPrivilegeLevel.CREATE)))
        .error(accessError("functional.alltypes"), onDatabase("functional",
            TPrivilegeLevel.CREATE), onTable("functional", "alltypes", allExcept(
            TPrivilegeLevel.ALL, TPrivilegeLevel.OWNER)))
        .error(createError("functional_parquet"), onDatabase("functional", allExcept(
            TPrivilegeLevel.ALL, TPrivilegeLevel.OWNER, TPrivilegeLevel.CREATE)),
            onTable("functional", "alltypes", TPrivilegeLevel.ALL));

    // Only for Kudu tables.
    for (AuthzTest test: new AuthzTest[]{
        authorize("alter table functional_kudu.testtbl alter column " +
            "name drop default"),
        authorize("alter table functional_kudu.testtbl alter column name " +
            "set default null"),
        authorize("alter table functional_kudu.testtbl add range partition " +
            "1 < values < 2"),
        authorize("alter table functional_kudu.testtbl drop range partition " +
            "1 < values < 2")}) {
      test.ok(onServer(TPrivilegeLevel.ALL))
          .ok(onServer(TPrivilegeLevel.OWNER))
          .ok(onServer(TPrivilegeLevel.ALTER))
          .ok(onDatabase("functional_kudu", TPrivilegeLevel.ALL))
          .ok(onDatabase("functional_kudu", TPrivilegeLevel.OWNER))
          .ok(onDatabase("functional_kudu", TPrivilegeLevel.ALTER))
          .ok(onTable("functional_kudu", "testtbl", TPrivilegeLevel.ALL))
          .ok(onTable("functional_kudu", "testtbl", TPrivilegeLevel.OWNER))
          .ok(onTable("functional_kudu", "testtbl", TPrivilegeLevel.ALTER))
          .error(alterError("functional_kudu.testtbl"))
          .error(alterError("functional_kudu.testtbl"), onServer(allExcept(
              TPrivilegeLevel.ALL, TPrivilegeLevel.OWNER, TPrivilegeLevel.ALTER)))
          .error(alterError("functional_kudu.testtbl"), onDatabase("functional_kudu",
              allExcept(TPrivilegeLevel.ALL, TPrivilegeLevel.OWNER,
              TPrivilegeLevel.ALTER)))
          .error(alterError("functional_kudu.testtbl"), onTable("functional_kudu",
              "testtbl", allExcept(TPrivilegeLevel.ALL, TPrivilegeLevel.OWNER,
              TPrivilegeLevel.ALTER)));
    }

    // Alter table set location.
    for (AuthzTest test: new AuthzTest[] {
        authorize("alter table functional.alltypes set location " +
            "'hdfs://localhost:20500/test-warehouse/new_table'"),
        authorize("alter table functional.alltypes partition(year=2009, month=1) " +
            "set location 'hdfs://localhost:20500/test-warehouse/new_table'")}) {
      test.ok(onServer(TPrivilegeLevel.ALL))
          .ok(onServer(TPrivilegeLevel.OWNER))
          .ok(onServer(TPrivilegeLevel.ALTER), onUri(
              "hdfs://localhost:20500/test-warehouse/new_table", TPrivilegeLevel.ALL))
          .ok(onServer(TPrivilegeLevel.ALTER), onUri(
              "hdfs://localhost:20500/test-warehouse/new_table", TPrivilegeLevel.OWNER))
          .ok(onDatabase("functional", TPrivilegeLevel.ALL), onUri(
              "hdfs://localhost:20500/test-warehouse/new_table", TPrivilegeLevel.ALL))
          .ok(onDatabase("functional", TPrivilegeLevel.OWNER), onUri(
              "hdfs://localhost:20500/test-warehouse/new_table", TPrivilegeLevel.OWNER))
          .ok(onDatabase("functional", TPrivilegeLevel.ALTER), onUri(
              "hdfs://localhost:20500/test-warehouse/new_table", TPrivilegeLevel.ALL))
          .ok(onDatabase("functional", TPrivilegeLevel.ALTER), onUri(
              "hdfs://localhost:20500/test-warehouse/new_table", TPrivilegeLevel.OWNER))
          .ok(onTable("functional", "alltypes", TPrivilegeLevel.ALL), onUri(
              "hdfs://localhost:20500/test-warehouse/new_table", TPrivilegeLevel.ALL))
          .ok(onTable("functional", "alltypes", TPrivilegeLevel.OWNER), onUri(
              "hdfs://localhost:20500/test-warehouse/new_table", TPrivilegeLevel.OWNER))
          .ok(onTable("functional", "alltypes", TPrivilegeLevel.ALTER), onUri(
              "hdfs://localhost:20500/test-warehouse/new_table", TPrivilegeLevel.ALL))
          .ok(onTable("functional", "alltypes", TPrivilegeLevel.ALTER), onUri(
              "hdfs://localhost:20500/test-warehouse/new_table", TPrivilegeLevel.OWNER))
          .error(alterError("functional.alltypes"))
          .error(alterError("functional.alltypes"),
              onServer(allExcept(TPrivilegeLevel.ALL, TPrivilegeLevel.OWNER,
              TPrivilegeLevel.ALTER)),
              onUri("hdfs://localhost:20500/test-warehouse/new_table"))
          .error(alterError("functional.alltypes"), onDatabase("functional", allExcept(
              TPrivilegeLevel.ALL, TPrivilegeLevel.OWNER, TPrivilegeLevel.ALTER)),
              onUri("hdfs://localhost:20500/test-warehouse/new_table"))
          .error(alterError("functional.alltypes"), onTable("functional", "alltypes",
              allExcept(TPrivilegeLevel.ALL, TPrivilegeLevel.OWNER,
              TPrivilegeLevel.ALTER)),
              onUri("hdfs://localhost:20500/test-warehouse/new_table"))
          .error(accessError("hdfs://localhost:20500/test-warehouse/new_table"),
              onDatabase("functional", TPrivilegeLevel.ALTER))
          .error(accessError("hdfs://localhost:20500/test-warehouse/new_table"),
              onTable("functional", "alltypes", TPrivilegeLevel.ALTER));
    }

    // Database does not exist.
    authorize("alter table nodb.alltypes add columns(c1 int)")
        .error(alterError("nodb"))
        .error(alterError("nodb"), onServer(allExcept(
            TPrivilegeLevel.ALL, TPrivilegeLevel.OWNER, TPrivilegeLevel.ALTER)));

    // Table does not exist.
    authorize("alter table functional.notbl add columns(c1 int)")
        .error(alterError("functional.notbl"))
        .error(alterError("functional.notbl"), onServer(allExcept(
            TPrivilegeLevel.ALL, TPrivilegeLevel.OWNER, TPrivilegeLevel.ALTER)))
        .error(alterError("functional.notbl"), onDatabase("functional", allExcept(
            TPrivilegeLevel.ALL, TPrivilegeLevel.OWNER, TPrivilegeLevel.ALTER)));
  }

  @Test
  public void testAlterView() throws ImpalaException {
    for (AuthzTest test: new AuthzTest[] {
        authorize("alter view functional.alltypes_view as " +
            "select int_col from functional.alltypes"),
        authorize("alter view functional.alltypes_view(a) as " +
            "select int_col from functional.alltypes")}) {
      test.ok(onServer(TPrivilegeLevel.ALL))
          .ok(onServer(TPrivilegeLevel.OWNER))
          .ok(onServer(TPrivilegeLevel.ALTER), onTable("functional", "alltypes",
              TPrivilegeLevel.SELECT))
          .ok(onDatabase("functional", TPrivilegeLevel.ALL))
          .ok(onDatabase("functional", TPrivilegeLevel.OWNER))
          .ok(onDatabase("functional", TPrivilegeLevel.ALL, TPrivilegeLevel.ALTER,
              TPrivilegeLevel.SELECT))
          .ok(onDatabase("functional", TPrivilegeLevel.OWNER, TPrivilegeLevel.ALTER,
              TPrivilegeLevel.SELECT))
          .ok(onTable("functional", "alltypes_view", TPrivilegeLevel.ALTER),
              onTable("functional", "alltypes", TPrivilegeLevel.SELECT))
          .error(selectError("functional.alltypes"))
          .error(selectError("functional.alltypes"), onServer(allExcept(
              TPrivilegeLevel.ALL, TPrivilegeLevel.OWNER, TPrivilegeLevel.ALTER,
              TPrivilegeLevel.SELECT)))
          .error(selectError("functional.alltypes"), onDatabase("functional",
              allExcept(TPrivilegeLevel.ALL, TPrivilegeLevel.OWNER, TPrivilegeLevel.ALTER,
                  TPrivilegeLevel.SELECT)))
          .error(alterError("functional.alltypes_view"), onTable("functional", "alltypes",
              TPrivilegeLevel.SELECT), onTable("functional", "alltypes_view", allExcept(
              TPrivilegeLevel.ALL, TPrivilegeLevel.OWNER, TPrivilegeLevel.ALTER)))
          .error(selectError("functional.alltypes"), onTable("functional", "alltypes",
              allExcept(TPrivilegeLevel.ALL, TPrivilegeLevel.OWNER,
              TPrivilegeLevel.SELECT)),
              onTable("functional", "alltypes_view", TPrivilegeLevel.ALTER));
    }

    // Alter view rename.
    authorize("alter view functional.alltypes_view rename to functional_parquet.new_view")
        .ok(onServer(TPrivilegeLevel.ALL))
        .ok(onServer(TPrivilegeLevel.OWNER))
        .ok(onDatabase("functional", TPrivilegeLevel.ALL),
            onDatabase("functional_parquet", TPrivilegeLevel.CREATE))
        .ok(onDatabase("functional_parquet", TPrivilegeLevel.CREATE),
            onTable("functional", "alltypes_view", TPrivilegeLevel.ALL))
        .ok(onDatabase("functional", TPrivilegeLevel.OWNER),
            onDatabase("functional_parquet", TPrivilegeLevel.CREATE))
        .ok(onDatabase("functional_parquet", TPrivilegeLevel.CREATE),
            onTable("functional", "alltypes_view", TPrivilegeLevel.OWNER))
        .error(accessError("functional.alltypes_view"))
        .error(accessError("functional.alltypes_view"), onServer(allExcept(
            TPrivilegeLevel.ALL, TPrivilegeLevel.OWNER, TPrivilegeLevel.CREATE)))
        .error(accessError("functional.alltypes_view"), onDatabase("functional",
            allExcept(TPrivilegeLevel.ALL, TPrivilegeLevel.OWNER)),
            onDatabase("functional_parquet", TPrivilegeLevel.CREATE))
        .error(createError("functional_parquet"), onDatabase("functional",
            TPrivilegeLevel.ALL), onDatabase("functional_parquet", allExcept(
            TPrivilegeLevel.ALL, TPrivilegeLevel.OWNER, TPrivilegeLevel.CREATE)))
        .error(accessError("functional.alltypes_view"), onDatabase("functional",
            TPrivilegeLevel.CREATE), onTable("functional", "alltypes_view", allExcept(
            TPrivilegeLevel.ALL, TPrivilegeLevel.OWNER)))
        .error(createError("functional_parquet"), onDatabase("functional", allExcept(
            TPrivilegeLevel.ALL, TPrivilegeLevel.OWNER, TPrivilegeLevel.CREATE)),
            onTable("functional", "alltypes_view", TPrivilegeLevel.ALL));

    // Alter view with constant select.
    authorize("alter view functional.alltypes_view as select 1")
        .ok(onServer(TPrivilegeLevel.ALL))
        .ok(onServer(TPrivilegeLevel.OWNER))
        .ok(onServer(TPrivilegeLevel.ALTER))
        .ok(onDatabase("functional", TPrivilegeLevel.ALL))
        .ok(onDatabase("functional", TPrivilegeLevel.OWNER))
        .ok(onDatabase("functional", TPrivilegeLevel.ALTER))
        .ok(onTable("functional", "alltypes_view", TPrivilegeLevel.ALL))
        .ok(onTable("functional", "alltypes_view", TPrivilegeLevel.OWNER))
        .ok(onTable("functional", "alltypes_view", TPrivilegeLevel.ALTER))
        .error(alterError("functional.alltypes_view"))
        .error(alterError("functional.alltypes_view"), onServer(allExcept(
            TPrivilegeLevel.ALL, TPrivilegeLevel.OWNER, TPrivilegeLevel.ALTER)))
        .error(alterError("functional.alltypes_view"), onDatabase("functional", allExcept(
            TPrivilegeLevel.ALL, TPrivilegeLevel.OWNER, TPrivilegeLevel.ALTER)))
        .error(alterError("functional.alltypes_view"), onTable("functional",
            "alltypes_view", allExcept(TPrivilegeLevel.ALL, TPrivilegeLevel.OWNER,
            TPrivilegeLevel.ALTER)));

    try {
      // We cannot set an owner to a role that doesn't exist
      authzCatalog_.addRole("foo_owner");
      // Alter view set owner.
      for (AuthzTest test : new AuthzTest[]{
          authorize("alter view functional.alltypes_view set owner user foo_owner"),
          authorize("alter view functional.alltypes_view set owner role foo_owner")}) {
        test.ok(onServer(true, TPrivilegeLevel.ALL))
            .ok(onServer(true, TPrivilegeLevel.OWNER))
            .ok(onDatabase(true, "functional", TPrivilegeLevel.ALL))
            .ok(onDatabase(true, "functional", TPrivilegeLevel.OWNER))
            .ok(onTable(true, "functional", "alltypes_view", TPrivilegeLevel.ALL))
            .ok(onTable(true, "functional", "alltypes_view", TPrivilegeLevel.OWNER))
            .error(accessError(true, "functional.alltypes_view"))
            .error(accessError(true, "functional.alltypes_view"), onServer(allExcept(
                TPrivilegeLevel.ALL, TPrivilegeLevel.OWNER)))
            .error(accessError(true, "functional.alltypes_view"), onDatabase(
                "functional", allExcept(TPrivilegeLevel.ALL, TPrivilegeLevel.OWNER)))
            .error(accessError(true, "functional.alltypes_view"), onTable("functional",
                "alltypes_view", allExcept(TPrivilegeLevel.ALL,
                    TPrivilegeLevel.OWNER)));
        // TODO: Checking if a request is allowed by checking if grant option flag is set
        // is to Sentry.
        if (authzProvider_ == AuthorizationProvider.SENTRY) {
          test.error(accessError(true, "functional.alltypes_view"), onServer(
              TPrivilegeLevel.values()))
              .error(accessError(true, "functional.alltypes_view"), onDatabase(
                  "functional", TPrivilegeLevel.values()))
              .error(accessError(true, "functional.alltypes_view"), onTable("functional",
                  "alltypes_view", TPrivilegeLevel.values()));
        }
      }
    } finally {
      authzCatalog_.removeRole("foo_owner");
    }

    // Database does not exist.
    authorize("alter view nodb.alltypes_view as select 1")
        .error(alterError("nodb"))
        .error(alterError("nodb"), onServer(allExcept(
            TPrivilegeLevel.ALL, TPrivilegeLevel.OWNER, TPrivilegeLevel.ALTER)));

    // View does not exist.
    authorize("alter view functional.noview as select 1")
        .error(alterError("functional.noview"))
        .error(alterError("functional.noview"), onServer(allExcept(
            TPrivilegeLevel.ALL, TPrivilegeLevel.OWNER, TPrivilegeLevel.ALTER)))
        .error(alterError("functional.noview"), onDatabase("functional", allExcept(
            TPrivilegeLevel.ALL, TPrivilegeLevel.OWNER, TPrivilegeLevel.ALTER)));
  }

  @Test
  public void testAlterDatabase() throws ImpalaException {
    try {
      // We cannot set an owner to a role that doesn't exist
      authzCatalog_.addRole("foo");
      // Alter database set owner.
      for (String ownerType : new String[]{"user", "role"}) {
        authorize(String.format("alter database functional set owner %s foo",
            ownerType))
            .ok(onServer(true, TPrivilegeLevel.ALL))
            .ok(onServer(true, TPrivilegeLevel.OWNER))
            .ok(onDatabase(true, "functional", TPrivilegeLevel.ALL))
            .ok(onDatabase(true, "functional", TPrivilegeLevel.OWNER))

            .error(accessError(true, "functional"))
            .error(accessError(true, "functional"), onServer(true, allExcept(
                TPrivilegeLevel.ALL, TPrivilegeLevel.OWNER)))
            .error(accessError(true, "functional"), onDatabase(true, "functional",
                allExcept(TPrivilegeLevel.ALL, TPrivilegeLevel.OWNER)));
        // TODO: Checking if a request is allowed by checking if grant option flag is set
        // is to Sentry.
        if (authzProvider_ == AuthorizationProvider.SENTRY) {
          authorize(String.format("alter database functional set owner %s foo",
              ownerType))
              .error(accessError(true, "functional"), onServer(TPrivilegeLevel.values()))
              .error(accessError(true, "functional"), onDatabase("functional",
                  TPrivilegeLevel.values()));
        }

        // Database does not exist.
        authorize(String.format("alter database nodb set owner %s foo", ownerType))
            .error(accessError(true, "nodb"))
            .error(accessError(true, "nodb"), onServer(true, allExcept(
                TPrivilegeLevel.ALL, TPrivilegeLevel.OWNER)));
      }
    } finally {
      authzCatalog_.removeRole("foo");
    }
    boolean exceptionThrown = false;
    try {
      parseAndAnalyze("alter database functional set owner role foo",
          authzCtx_, frontend_);
    } catch (AnalysisException e) {
      exceptionThrown = true;
      assertEquals("Role 'foo' does not exist.", e.getLocalizedMessage());
    }
    assertTrue(exceptionThrown);
  }

  @Test
  public void testUpdate() throws ImpalaException {
    // Update is only supported on Kudu tables.
    for (AuthzTest test: new AuthzTest[]{
        authorize("update functional_kudu.alltypes set int_col = 1"),
        // Explain update.
        authorize("explain update functional_kudu.alltypes set int_col = 1")}) {
      test.ok(onServer(TPrivilegeLevel.ALL))
          .ok(onServer(TPrivilegeLevel.OWNER))
          .error(accessError("functional_kudu.alltypes"))
          .error(accessError("functional_kudu.alltypes"), onServer(allExcept(
              TPrivilegeLevel.ALL, TPrivilegeLevel.OWNER)));
    }

    // Database does not exist.
    authorize("update nodb.alltypes set int_col = 1")
        .error(selectError("nodb"))
        .error(selectError("nodb"), onServer(allExcept(
            TPrivilegeLevel.ALL, TPrivilegeLevel.OWNER, TPrivilegeLevel.SELECT)));

    // Table does not exist.
    authorize("update functional_kudu.notbl set int_col = 1")
        .error(selectError("functional_kudu.notbl"))
        .error(selectError("functional_kudu.notbl"), onServer(allExcept(
            TPrivilegeLevel.ALL, TPrivilegeLevel.OWNER, TPrivilegeLevel.SELECT)))
        .error(selectError("functional_kudu.notbl"), onDatabase("functional_kudu",
            allExcept(TPrivilegeLevel.ALL, TPrivilegeLevel.OWNER,
            TPrivilegeLevel.SELECT)));
  }

  @Test
  public void testUpsert() throws ImpalaException {
    // Upsert is only supported on Kudu tables.
    for (AuthzTest test: new AuthzTest[]{
        authorize("upsert into table functional_kudu.testtbl(id, name) values(1, 'a')"),
        // Upsert with clause.
        authorize("with t1 as (select 1, 'a', 2) upsert into functional_kudu.testtbl " +
            "select * from t1"),
        // Explain upsert.
        authorize("explain upsert into table functional_kudu.testtbl(id, name) " +
            "values(1, 'a')")}) {
      test.ok(onServer(TPrivilegeLevel.ALL))
          .ok(onServer(TPrivilegeLevel.OWNER))
          .error(accessError("functional_kudu.testtbl"))
          .error(accessError("functional_kudu.testtbl"), onServer(allExcept(
              TPrivilegeLevel.ALL, TPrivilegeLevel.OWNER)));
    }

    // Upsert select.
    authorize("upsert into table functional_kudu.testtbl(id) " +
        "select int_col from functional.alltypes")
        .ok(onServer(TPrivilegeLevel.ALL))
        .ok(onServer(TPrivilegeLevel.OWNER))
        .error(selectError("functional.alltypes"))
        .error(accessError("functional_kudu.testtbl"), onServer(allExcept(
            TPrivilegeLevel.ALL, TPrivilegeLevel.OWNER)));

    // Database does not exist.
    authorize("upsert into table nodb.testtbl(id, name) values(1, 'a')")
        .error(accessError("nodb.testtbl"))
        .error(accessError("nodb.testtbl"), onServer(allExcept(TPrivilegeLevel.ALL,
            TPrivilegeLevel.OWNER)));

    // Table does not exist.
    authorize("upsert into table functional_kudu.notbl(id, name) values(1, 'a')")
        .error(accessError("functional_kudu.notbl"))
        .error(accessError("functional_kudu.notbl"), onServer(allExcept(
            TPrivilegeLevel.ALL, TPrivilegeLevel.OWNER)))
        .error(accessError("functional_kudu.notbl"), onDatabase("functional_kudu",
            allExcept(TPrivilegeLevel.ALL, TPrivilegeLevel.OWNER)));
  }

  @Test
  public void testDelete() throws ImpalaException {
    // Delete is only supported on Kudu tables.
    for (AuthzTest test: new AuthzTest[]{
        authorize("delete from functional_kudu.alltypes"),
        authorize("explain delete from functional_kudu.alltypes")}) {
      test.ok(onServer(TPrivilegeLevel.ALL))
          .ok(onServer(TPrivilegeLevel.OWNER))
          .error(accessError("functional_kudu.alltypes"))
          .error(accessError("functional_kudu.alltypes"), onServer(allExcept(
              TPrivilegeLevel.ALL, TPrivilegeLevel.OWNER)));
    }

    // Database does not exist.
    authorize("delete from nodb.alltypes")
        .error(selectError("nodb"))
        .error(selectError("nodb"), onServer(allExcept(
            TPrivilegeLevel.ALL, TPrivilegeLevel.OWNER, TPrivilegeLevel.SELECT)));

    // Table does not exist.
    authorize("delete from functional_kudu.notbl")
        .error(selectError("functional_kudu.notbl"))
        .error(selectError("functional_kudu.notbl"), onServer(allExcept(
            TPrivilegeLevel.ALL, TPrivilegeLevel.OWNER, TPrivilegeLevel.SELECT)))
        .error(selectError("functional_kudu.notbl"), onDatabase("functional_kudu",
            allExcept(TPrivilegeLevel.ALL, TPrivilegeLevel.OWNER,
            TPrivilegeLevel.SELECT)));
  }

  @Test
  public void testCommentOn() throws ImpalaException {
    // Comment on database.
    authorize("comment on database functional is 'comment'")
        .ok(onServer(TPrivilegeLevel.ALL))
        .ok(onServer(TPrivilegeLevel.OWNER))
        .ok(onServer(TPrivilegeLevel.ALTER))
        .ok(onDatabase("functional", TPrivilegeLevel.ALL))
        .ok(onDatabase("functional", TPrivilegeLevel.OWNER))
        .ok(onDatabase("functional", TPrivilegeLevel.ALTER))
        .error(alterError("functional"))
        .error(alterError("functional"), onServer(allExcept(
            TPrivilegeLevel.ALL, TPrivilegeLevel.OWNER, TPrivilegeLevel.ALTER)))
        .error(alterError("functional"), onDatabase("functional", allExcept(
            TPrivilegeLevel.ALL, TPrivilegeLevel.OWNER, TPrivilegeLevel.ALTER)));

    // Comment on table.
    authorize("comment on table functional.alltypes is 'comment'")
        .ok(onServer(TPrivilegeLevel.ALL))
        .ok(onServer(TPrivilegeLevel.OWNER))
        .ok(onServer(TPrivilegeLevel.ALTER))
        .ok(onDatabase("functional", TPrivilegeLevel.ALL))
        .ok(onDatabase("functional", TPrivilegeLevel.OWNER))
        .ok(onDatabase("functional", TPrivilegeLevel.ALTER))
        .ok(onTable("functional", "alltypes", TPrivilegeLevel.ALL))
        .ok(onTable("functional", "alltypes", TPrivilegeLevel.OWNER))
        .ok(onTable("functional", "alltypes", TPrivilegeLevel.ALTER))
        .error(alterError("functional.alltypes"))
        .error(alterError("functional.alltypes"), onServer(allExcept(
            TPrivilegeLevel.ALL, TPrivilegeLevel.OWNER, TPrivilegeLevel.ALTER)))
        .error(alterError("functional.alltypes"), onDatabase("functional", allExcept(
            TPrivilegeLevel.ALL, TPrivilegeLevel.OWNER, TPrivilegeLevel.ALTER)))
        .error(alterError("functional.alltypes"), onTable("functional", "alltypes",
            allExcept(TPrivilegeLevel.ALL, TPrivilegeLevel.OWNER,
            TPrivilegeLevel.ALTER)));

    // Comment on view.
    authorize("comment on view functional.alltypes_view is 'comment'")
        .ok(onServer(TPrivilegeLevel.ALL))
        .ok(onServer(TPrivilegeLevel.OWNER))
        .ok(onServer(TPrivilegeLevel.ALTER))
        .ok(onDatabase("functional", TPrivilegeLevel.ALL))
        .ok(onDatabase("functional", TPrivilegeLevel.OWNER))
        .ok(onDatabase("functional", TPrivilegeLevel.ALTER))
        .ok(onTable("functional", "alltypes_view", TPrivilegeLevel.ALL))
        .ok(onTable("functional", "alltypes_view", TPrivilegeLevel.OWNER))
        .ok(onTable("functional", "alltypes_view", TPrivilegeLevel.ALTER))
        .error(alterError("functional.alltypes_view"))
        .error(alterError("functional.alltypes_view"), onServer(allExcept(
            TPrivilegeLevel.ALL, TPrivilegeLevel.OWNER, TPrivilegeLevel.ALTER)))
        .error(alterError("functional.alltypes_view"), onDatabase("functional", allExcept(
            TPrivilegeLevel.ALL, TPrivilegeLevel.OWNER, TPrivilegeLevel.ALTER)))
        .error(alterError("functional.alltypes_view"), onTable("functional",
            "alltypes_view", allExcept(TPrivilegeLevel.ALL, TPrivilegeLevel.OWNER,
            TPrivilegeLevel.ALTER)));

    // Comment on table column.
    authorize("comment on column functional.alltypes.id is 'comment'")
        .ok(onServer(TPrivilegeLevel.ALL))
        .ok(onServer(TPrivilegeLevel.OWNER))
        .ok(onServer(TPrivilegeLevel.ALTER))
        .ok(onDatabase("functional", TPrivilegeLevel.ALL))
        .ok(onDatabase("functional", TPrivilegeLevel.OWNER))
        .ok(onDatabase("functional", TPrivilegeLevel.ALTER))
        .ok(onTable("functional", "alltypes", TPrivilegeLevel.ALL))
        .ok(onTable("functional", "alltypes", TPrivilegeLevel.OWNER))
        .ok(onTable("functional", "alltypes", TPrivilegeLevel.ALTER))
        .error(alterError("functional.alltypes"))
        .error(alterError("functional.alltypes"), onServer(allExcept(
            TPrivilegeLevel.ALL, TPrivilegeLevel.OWNER, TPrivilegeLevel.ALTER)))
        .error(alterError("functional.alltypes"), onDatabase("functional", allExcept(
            TPrivilegeLevel.ALL, TPrivilegeLevel.OWNER, TPrivilegeLevel.ALTER)))
        .error(alterError("functional.alltypes"), onTable("functional", "alltypes",
            allExcept(TPrivilegeLevel.ALL, TPrivilegeLevel.OWNER,
            TPrivilegeLevel.ALTER)));

    // Comment on view column.
    authorize("comment on column functional.alltypes_view.id is 'comment'")
        .ok(onServer(TPrivilegeLevel.ALL))
        .ok(onServer(TPrivilegeLevel.OWNER))
        .ok(onServer(TPrivilegeLevel.ALTER))
        .ok(onDatabase("functional", TPrivilegeLevel.ALL))
        .ok(onDatabase("functional", TPrivilegeLevel.OWNER))
        .ok(onDatabase("functional", TPrivilegeLevel.ALTER))
        .ok(onTable("functional", "alltypes_view", TPrivilegeLevel.ALL))
        .ok(onTable("functional", "alltypes_view", TPrivilegeLevel.OWNER))
        .ok(onTable("functional", "alltypes_view", TPrivilegeLevel.ALTER))
        .error(alterError("functional.alltypes_view"))
        .error(alterError("functional.alltypes_view"), onServer(allExcept(
            TPrivilegeLevel.ALL, TPrivilegeLevel.OWNER, TPrivilegeLevel.ALTER)))
        .error(alterError("functional.alltypes_view"), onDatabase("functional", allExcept(
            TPrivilegeLevel.ALL, TPrivilegeLevel.OWNER, TPrivilegeLevel.ALTER)))
        .error(alterError("functional.alltypes_view"), onTable("functional",
            "alltypes_view", allExcept(TPrivilegeLevel.ALL, TPrivilegeLevel.OWNER,
            TPrivilegeLevel.ALTER)));
  }

  @Test
  public void testFunction() throws ImpalaException {
    // Create function.
    authorize("create function functional.f() returns int location " +
        "'/test-warehouse/libTestUdfs.so' symbol='NoArgs'")
        .ok(onServer(TPrivilegeLevel.ALL))
        .ok(onServer(TPrivilegeLevel.OWNER))
        .ok(onServer(TPrivilegeLevel.CREATE), onUri("/test-warehouse/libTestUdfs.so",
            TPrivilegeLevel.ALL))
        .ok(onServer(TPrivilegeLevel.CREATE), onUri("/test-warehouse/libTestUdfs.so",
            TPrivilegeLevel.OWNER))
        .ok(onDatabase("functional", TPrivilegeLevel.ALL),
            onUri("/test-warehouse/libTestUdfs.so", TPrivilegeLevel.ALL))
        .ok(onDatabase("functional", TPrivilegeLevel.OWNER),
            onUri("/test-warehouse/libTestUdfs.so", TPrivilegeLevel.OWNER))
        .ok(onDatabase("functional", TPrivilegeLevel.CREATE),
            onUri("/test-warehouse/libTestUdfs.so", TPrivilegeLevel.ALL))
        .ok(onDatabase("functional", TPrivilegeLevel.CREATE),
            onUri("/test-warehouse/libTestUdfs.so", TPrivilegeLevel.OWNER))
        .error(createFunctionError("functional.f()"))
        .error(accessError("hdfs://localhost:20500/test-warehouse/libTestUdfs.so"),
            onServer(allExcept(TPrivilegeLevel.ALL, TPrivilegeLevel.OWNER)))
        .error(createFunctionError("functional.f()"),
            onUri("/test-warehouse/libTestUdfs.so", TPrivilegeLevel.ALL))
        .error(createFunctionError("functional.f()"),
            onUri("/test-warehouse/libTestUdfs.so", TPrivilegeLevel.OWNER));

    // Create a function name that has the same name as built-in function is OK.
    authorize("create function functional.sin() returns int location " +
        "'/test-warehouse/libTestUdfs.so' symbol='NoArgs'")
        .ok(onServer(TPrivilegeLevel.ALL))
        .ok(onServer(TPrivilegeLevel.OWNER))
        .ok(onServer(TPrivilegeLevel.CREATE), onUri("/test-warehouse/libTestUdfs.so",
            TPrivilegeLevel.ALL))
        .ok(onServer(TPrivilegeLevel.CREATE), onUri("/test-warehouse/libTestUdfs.so",
            TPrivilegeLevel.OWNER))
        .ok(onDatabase("functional", TPrivilegeLevel.ALL),
            onUri("/test-warehouse/libTestUdfs.so", TPrivilegeLevel.ALL))
        .ok(onDatabase("functional", TPrivilegeLevel.OWNER),
            onUri("/test-warehouse/libTestUdfs.so", TPrivilegeLevel.OWNER))
        .ok(onDatabase("functional", TPrivilegeLevel.CREATE),
            onUri("/test-warehouse/libTestUdfs.so", TPrivilegeLevel.ALL))
        .ok(onDatabase("functional", TPrivilegeLevel.CREATE),
            onUri("/test-warehouse/libTestUdfs.so", TPrivilegeLevel.OWNER))
        .error(createFunctionError("functional.sin()"))
        .error(accessError("hdfs://localhost:20500/test-warehouse/libTestUdfs.so"),
            onServer(allExcept(TPrivilegeLevel.ALL, TPrivilegeLevel.OWNER)))
        .error(createFunctionError("functional.sin()"),
            onUri("/test-warehouse/libTestUdfs.so", TPrivilegeLevel.ALL))
        .error(createFunctionError("functional.sin()"),
            onUri("/test-warehouse/libTestUdfs.so", TPrivilegeLevel.OWNER));

    // Creating a function in the system database even with ALL/OWNER privilege on SERVER
    // is not allowed.
    for (AuthzTest test: new AuthzTest[] {
        authorize("create function _impala_builtins.sin() returns int location " +
            "'/test-warehouse/libTestUdfs.so' symbol='NoArgs'"),
        authorize("create function _impala_builtins.f() returns int location " +
            "'/test-warehouse/libTestUdfs.so' symbol='NoArgs'")}) {
      test.error(systemDbError(), onServer(TPrivilegeLevel.ALL, TPrivilegeLevel.OWNER));
    }

    ScalarFunction fn = addFunction("functional", "f");
    try {
      authorize("drop function functional.f()")
          .ok(onServer(TPrivilegeLevel.ALL))
          .ok(onServer(TPrivilegeLevel.OWNER))
          .ok(onServer(TPrivilegeLevel.DROP))
          .ok(onDatabase("functional", TPrivilegeLevel.ALL))
          .ok(onDatabase("functional", TPrivilegeLevel.OWNER))
          .ok(onDatabase("functional", TPrivilegeLevel.DROP))
          .error(dropFunctionError("functional.f()"))
          .error(dropFunctionError("functional.f()"), onServer(allExcept(
              TPrivilegeLevel.ALL, TPrivilegeLevel.OWNER, TPrivilegeLevel.DROP)));

      // Function does not exist but with if exists clause.
      authorize("drop function if exists functional.g()")
          .error(dropFunctionError("functional.g()"))
          .error(dropFunctionError("functional.g()"), onServer(allExcept(
              TPrivilegeLevel.ALL, TPrivilegeLevel.OWNER, TPrivilegeLevel.DROP)));
    } finally {
      removeFunction(fn);
    }

    // IMPALA-6086: Make sure use of a permanent function requires SELECT (or higher)
    // privilege on the database, and expr rewrite/constant-folding preserves
    // privilege requests for functions.
    List<Type> argTypes = new ArrayList<Type>();
    argTypes.add(Type.STRING);
    fn = addFunction("functional", "to_lower", argTypes, Type.STRING,
        "/test-warehouse/libTestUdf.so",
        "_Z7ToLowerPN10impala_udf15FunctionContextERKNS_9StringValE");
    try {
      TQueryOptions options = new TQueryOptions();
      options.setEnable_expr_rewrites(true);
      for (AuthzTest test: new AuthzTest[] {
          authorize("select functional.to_lower('ABCDEF')"),
          // Also test with expression rewrite enabled.
          authorize(createAnalysisCtx(options, authzFactory_),
              "select functional.to_lower('ABCDEF')")}) {
        test.ok(onServer(TPrivilegeLevel.SELECT))
            .ok(onDatabase("functional", TPrivilegeLevel.ALL))
            .ok(onDatabase("functional", TPrivilegeLevel.OWNER))
            .ok(onDatabase("functional", viewMetadataPrivileges()))
            .error(accessError("functional"))
            .error(accessError("functional"), onDatabase("functional",
                allExcept(viewMetadataPrivileges())));
      }
    } finally {
      removeFunction(fn);
    }
  }

  @Test
  public void testShutdown() throws ImpalaException {
    // Requires ALL privilege on server.
    authorize(": shutdown()")
        .ok(onServer(TPrivilegeLevel.ALL))
        .error(accessError("server"))
        .error(accessError("server"), onServer(TPrivilegeLevel.REFRESH))
        .error(accessError("server"), onServer(TPrivilegeLevel.SELECT))
        .error(accessError("server"), onDatabase("functional", TPrivilegeLevel.ALL))
        .error(accessError("server"),
            onTable("functional", "alltypes", TPrivilegeLevel.ALL));
  }

  @Test
  public void testColumnMaskEnabled() throws ImpalaException {
    if (authzProvider_ == AuthorizationProvider.SENTRY) return;

    String policyName = "col_mask";
    for (String tableName: new String[]{"alltypes", "alltypes_view"}) {
      String json = String.format("{\n" +
          "  \"name\": \"%s\",\n" +
          "  \"policyType\": 1,\n" +
          "  \"serviceType\": \"%s\",\n" +
          "  \"service\": \"%s\",\n" +
          "  \"resources\": {\n" +
          "    \"database\": {\n" +
          "      \"values\": [\"functional\"],\n" +
          "      \"isExcludes\": false,\n" +
          "      \"isRecursive\": false\n" +
          "    },\n" +
          "    \"table\": {\n" +
          "      \"values\": [\"%s\"],\n" +
          "      \"isExcludes\": false,\n" +
          "      \"isRecursive\": false\n" +
          "    },\n" +
          "    \"column\": {\n" +
          "      \"values\": [\"string_col\"],\n" +
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
          "      \"dataMaskInfo\": {\"dataMaskType\": \"MASK\"}\n" +
          "    }\n" +
          "  ]\n" +
          "}", policyName, RANGER_SERVICE_TYPE, RANGER_SERVICE_NAME, tableName,
          USER.getShortName());

      try {
        createRangerPolicy(policyName, json);
        rangerImpalaPlugin_.refreshPoliciesAndTags();

        // Queries on columns that are not masked should be allowed.
        authorize("select id from functional.alltypes")
            .ok(onServer(TPrivilegeLevel.ALL));
        authorize("select x from functional.alltypes_view_sub")
            .ok(onServer(TPrivilegeLevel.ALL));
        authorize("select string_col from functional_kudu.alltypes")
            .ok(onServer(TPrivilegeLevel.ALL));

        // Normal select.
        authorize(String.format("select string_col from functional.%s", tableName))
            .error(columnMaskError(String.format("functional.%s.string_col", tableName)),
                onServer(TPrivilegeLevel.ALL));
        // Column within a function.
        authorize(String.format(
            "select substr(string_col, 0, 1) from functional.%s", tableName))
            .error(columnMaskError(String.format("functional.%s.string_col", tableName)),
                onServer(TPrivilegeLevel.ALL));
        // Select with *.
        authorize(String.format("select * from functional.%s", tableName))
            .error(columnMaskError(String.format("functional.%s.string_col", tableName)),
                onServer(TPrivilegeLevel.ALL));
        // Sub-query.
        authorize(String.format(
            "select t.string_col from (select * from functional.%s) t", tableName))
            .error(columnMaskError(String.format("functional.%s.string_col", tableName)),
                onServer(TPrivilegeLevel.ALL));
        // CTE.
        authorize(String.format("with t as (select * from functional.%s) " +
            "select string_col from t", tableName))
            .error(columnMaskError(String.format("functional.%s.string_col", tableName)),
                onServer(TPrivilegeLevel.ALL));
        // CTAS.
        authorize(String.format(
            "create table t as select * from functional.%s", tableName))
            .error(columnMaskError(String.format("functional.%s.string_col", tableName)),
                onServer(TPrivilegeLevel.ALL));
        // Create view.
        authorize(String.format(
            "create view v as select * from functional.%s", tableName))
            .error(columnMaskError(String.format("functional.%s.string_col", tableName)),
                onServer(TPrivilegeLevel.ALL));
        // Alter view.
        authorize(String.format("alter view functional.alltypes_view_sub as " +
            "select * from functional.%s", tableName))
            .error(columnMaskError(String.format("functional.%s.string_col", tableName)),
                onServer(TPrivilegeLevel.ALL));
        // Union.
        authorize(String.format(
            "select string_col from functional.%s union select 'hello'", tableName))
            .error(columnMaskError(String.format("functional.%s.string_col", tableName)),
                onServer(TPrivilegeLevel.ALL));
        // Update.
        authorize(String.format(
            "update functional_kudu.alltypes set int_col = 1 where string_col in " +
            "(select string_col from functional.%s)", tableName))
            .error(columnMaskError(String.format("functional.%s.string_col", tableName)),
                onServer(TPrivilegeLevel.ALL));
        // Delete.
        authorize(String.format("delete functional_kudu.alltypes where string_col in " +
            "(select string_col from functional.%s)", tableName))
            .error(columnMaskError(String.format("functional.%s.string_col", tableName)),
                onServer(TPrivilegeLevel.ALL));
        // Copy testcase.
        authorize(String.format(
            "copy testcase to '/tmp' select * from functional.%s", tableName))
            .error(columnMaskError(String.format("functional.%s.string_col", tableName)),
                onServer(TPrivilegeLevel.ALL));
      } finally {
        deleteRangerPolicy(policyName);
      }
    }
  }

  @Test
  public void testRowFilterEnabled() throws ImpalaException {
    if (authzProvider_ == AuthorizationProvider.SENTRY) return;

    String policyName = "row_filter";
    for (String tableName: new String[]{"alltypes", "alltypes_view"}) {
      String json = String.format("{\n" +
          "  \"name\": \"%s\",\n" +
          "  \"policyType\": 2,\n" +
          "  \"serviceType\": \"%s\",\n" +
          "  \"service\": \"%s\",\n" +
          "  \"resources\": {\n" +
          "    \"database\": {\n" +
          "      \"values\": [\"functional\"],\n" +
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
          "      \"rowFilterInfo\": {\"filterExpr\": \"id = 0\"}\n" +
          "    }\n" +
          "  ]\n" +
          "}", policyName, RANGER_SERVICE_TYPE, RANGER_SERVICE_NAME, tableName,
          USER.getShortName());

      try {
        createRangerPolicy(policyName, json);
        rangerImpalaPlugin_.refreshPoliciesAndTags();

        // Queries on tables that are not filtered should be allowed.
        authorize("select string_col from functional_kudu.alltypes")
            .ok(onServer(TPrivilegeLevel.ALL));
        authorize("select x from functional.alltypes_view_sub")
            .ok(onServer(TPrivilegeLevel.ALL));

        // Normal select.
        authorize(String.format("select string_col from functional.%s", tableName))
            .error(rowFilterError(String.format("functional.%s", tableName)),
                onServer(TPrivilegeLevel.ALL));
        // Select with *.
        authorize(String.format("select * from functional.%s", tableName))
            .error(rowFilterError(String.format("functional.%s", tableName)),
                onServer(TPrivilegeLevel.ALL));
        // Sub-query.
        authorize(String.format(
            "select t.string_col from (select * from functional.%s) t", tableName))
            .error(rowFilterError(String.format("functional.%s", tableName)),
                onServer(TPrivilegeLevel.ALL));
        // CTE.
        authorize(String.format("with t as (select * from functional.%s) " +
            "select string_col from t", tableName))
            .error(rowFilterError(String.format("functional.%s", tableName)),
                onServer(TPrivilegeLevel.ALL));
        // CTAS.
        authorize(String.format(
            "create table t as select * from functional.%s", tableName))
            .error(rowFilterError(String.format("functional.%s", tableName)),
                onServer(TPrivilegeLevel.ALL));
        // Create view.
        authorize(String.format(
            "create view v as select * from functional.%s", tableName))
            .error(rowFilterError(String.format("functional.%s", tableName)),
                onServer(TPrivilegeLevel.ALL));
        // Alter view.
        authorize(String.format("alter view functional.alltypes_view_sub as " +
            "select * from functional.%s", tableName))
            .error(rowFilterError(String.format("functional.%s", tableName)),
                onServer(TPrivilegeLevel.ALL));
        // Union.
        authorize(String.format(
            "select string_col from functional.%s union select 'hello'", tableName))
            .error(rowFilterError(String.format("functional.%s", tableName)),
                onServer(TPrivilegeLevel.ALL));
        // Update.
        authorize(String.format(
            "update functional_kudu.alltypes set int_col = 1 where string_col in " +
                "(select string_col from functional.%s)", tableName))
            .error(rowFilterError(String.format("functional.%s", tableName)),
                onServer(TPrivilegeLevel.ALL));
        // Delete.
        authorize(String.format("delete functional_kudu.alltypes where string_col in " +
            "(select string_col from functional.%s)", tableName))
            .error(rowFilterError(String.format("functional.%s", tableName)),
                onServer(TPrivilegeLevel.ALL));
        // Copy testcase.
        authorize(String.format(
            "copy testcase to '/tmp' select * from functional.%s", tableName))
            .error(rowFilterError(String.format("functional.%s", tableName)),
                onServer(TPrivilegeLevel.ALL));
      } finally {
        deleteRangerPolicy(policyName);
      }
    }
  }

  private void verifyPrivilegeReqs(String stmt, Set<String> expectedPrivilegeNames)
      throws ImpalaException {
    verifyPrivilegeReqs(createAnalysisCtx(authzFactory_), stmt, expectedPrivilegeNames);
  }

  private void verifyPrivilegeReqs(AnalysisContext ctx, String stmt,
      Set<String> expectedPrivilegeNames) throws ImpalaException {
    AnalysisResult analysisResult = parseAndAnalyze(stmt, ctx, frontend_);
    Set<String> actualPrivilegeNames = new HashSet<>();
    for (PrivilegeRequest privReq: analysisResult.getAnalyzer().getPrivilegeReqs()) {
      actualPrivilegeNames.add(privReq.getName());
    }
    assertEquals(expectedPrivilegeNames, actualPrivilegeNames);
  }
}
