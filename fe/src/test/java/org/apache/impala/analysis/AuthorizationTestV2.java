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

package org.apache.impala.analysis;

import com.google.common.base.Preconditions;
import com.google.common.collect.Sets;
import org.apache.commons.lang.ArrayUtils;
import org.apache.impala.analysis.AnalysisContext.AnalysisResult;
import org.apache.impala.authorization.AuthorizationConfig;
import org.apache.impala.authorization.PrivilegeRequest;
import org.apache.impala.authorization.User;
import org.apache.impala.catalog.AuthorizationException;
import org.apache.impala.catalog.Role;
import org.apache.impala.catalog.RolePrivilege;
import org.apache.impala.catalog.ScalarFunction;
import org.apache.impala.catalog.Type;
import org.apache.impala.common.FrontendTestBase;
import org.apache.impala.common.ImpalaException;
import org.apache.impala.common.RuntimeEnv;
import org.apache.impala.service.Frontend;
import org.apache.impala.testutil.ImpaladTestCatalog;
import org.apache.impala.thrift.TColumnValue;
import org.apache.impala.thrift.TDescribeOutputStyle;
import org.apache.impala.thrift.TDescribeResult;
import org.apache.impala.thrift.TFunctionBinaryType;
import org.apache.impala.thrift.TPrivilege;
import org.apache.impala.thrift.TPrivilegeLevel;
import org.apache.impala.thrift.TPrivilegeScope;
import org.apache.impala.thrift.TResultRow;
import org.apache.impala.thrift.TTableName;
import org.apache.impala.util.SentryPolicyService;
import org.apache.sentry.provider.db.service.thrift.TSentryRole;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class AuthorizationTestV2 extends FrontendTestBase {
  private static final String SENTRY_SERVER = "server1";
  private final static User USER = new User(System.getProperty("user.name"));
  private final AnalysisContext analysisContext_;
  private final SentryPolicyService sentryService_;
  private final ImpaladTestCatalog authzCatalog_;
  private final Frontend authzFrontend_;

  public AuthorizationTestV2() {
    AuthorizationConfig authzConfig = AuthorizationConfig.createHadoopGroupAuthConfig(
        SENTRY_SERVER, null, System.getenv("IMPALA_HOME") +
        "/fe/src/test/resources/sentry-site.xml");
    authzConfig.validateConfig();
    analysisContext_ = createAnalysisCtx(authzConfig, USER.getName());
    authzCatalog_ = new ImpaladTestCatalog(authzConfig);
    authzFrontend_ = new Frontend(authzConfig, authzCatalog_);
    sentryService_ = new SentryPolicyService(authzConfig.getSentryConfig());
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
    // Remove existing roles in order to not interfere with these tests.
    for (TSentryRole role: sentryService_.listAllRoles(USER)) {
      authzCatalog_.removeRole(role.getRoleName());
    }
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
    verifyPrivilegeReqs(createAnalysisCtx("functional"), "select id from alltypes",
        expectedAuthorizables);
    verifyPrivilegeReqs("select alltypes.id from functional.alltypes",
        expectedAuthorizables);
    verifyPrivilegeReqs("select a.id from functional.alltypes a", expectedAuthorizables);

    // Insert.
    expectedAuthorizables = Sets.newHashSet("functional.alltypes");
    verifyPrivilegeReqs("insert into functional.alltypes(id) partition(month, year) " +
        "values(1, 1, 2018)", expectedAuthorizables);
    verifyPrivilegeReqs(createAnalysisCtx("functional"), "insert into alltypes(id) " +
        "partition(month, year) values(1, 1, 2018)", expectedAuthorizables);

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
    verifyPrivilegeReqs(createAnalysisCtx("functional"), "invalidate metadata alltypes",
        expectedAuthorizables);
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
    verifyPrivilegeReqs(createAnalysisCtx("functional"), "show partitions alltypes",
        expectedAuthorizables);

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
    verifyPrivilegeReqs(createAnalysisCtx("functional"), "show table stats alltypes",
        expectedAuthorizables);

    // Show column stats.
    expectedAuthorizables = Sets.newHashSet("functional.alltypes");
    verifyPrivilegeReqs("show column stats functional.alltypes", expectedAuthorizables);
    verifyPrivilegeReqs(createAnalysisCtx("functional"), "show column stats alltypes",
        expectedAuthorizables);

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
  }

  @Test
  public void testSelect() throws ImpalaException {
    for (AuthzTest authzTest: new AuthzTest[]{
        // Select a specific column on a table.
        authorize("select id from functional.alltypes"),
        // With clause with select.
        authorize("with t as (select id from functional.alltypes) select * from t")}) {
      authzTest.ok(onServer(TPrivilegeLevel.ALL))
          .ok(onServer(TPrivilegeLevel.ALL))
          .ok(onServer(TPrivilegeLevel.SELECT))
          .ok(onDatabase("functional", TPrivilegeLevel.ALL))
          .ok(onDatabase("functional", TPrivilegeLevel.SELECT))
          .ok(onTable("functional", "alltypes", TPrivilegeLevel.ALL))
          .ok(onTable("functional", "alltypes", TPrivilegeLevel.SELECT))
          .ok(onColumn("functional", "alltypes", "id", TPrivilegeLevel.SELECT))
          .error(selectError("functional.alltypes"))
          .error(selectError("functional.alltypes"), onServer(allExcept(
              TPrivilegeLevel.ALL, TPrivilegeLevel.SELECT)))
          .error(selectError("functional.alltypes"), onDatabase("functional",
              allExcept(TPrivilegeLevel.ALL, TPrivilegeLevel.SELECT)))
          .error(selectError("functional.alltypes"), onTable("functional",
              "alltypes", allExcept(TPrivilegeLevel.ALL, TPrivilegeLevel.SELECT)));
    }


    // Select without referencing a column.
    authorize("select 1 from functional.alltypes")
        .ok(onServer(TPrivilegeLevel.ALL))
        .ok(onServer(TPrivilegeLevel.SELECT))
        .ok(onDatabase("functional", TPrivilegeLevel.ALL))
        .ok(onDatabase("functional", TPrivilegeLevel.SELECT))
        .ok(onTable("functional", "alltypes", TPrivilegeLevel.ALL))
        .ok(onTable("functional", "alltypes", TPrivilegeLevel.SELECT))
        .error(selectError("functional.alltypes"))
        .error(selectError("functional.alltypes"), onServer(allExcept(
            TPrivilegeLevel.ALL, TPrivilegeLevel.SELECT)))
        .error(selectError("functional.alltypes"), onDatabase("functional",
            allExcept(TPrivilegeLevel.ALL, TPrivilegeLevel.SELECT)))
        .error(selectError("functional.alltypes"), onTable("functional", "alltypes",
            allExcept(TPrivilegeLevel.ALL, TPrivilegeLevel.SELECT)));


    // Select a specific column on a view.
    // Column-level privileges on views are not currently supported.
    authorize("select id from functional.alltypes_view")
        .ok(onServer(TPrivilegeLevel.ALL))
        .ok(onServer(TPrivilegeLevel.SELECT))
        .ok(onDatabase("functional", TPrivilegeLevel.ALL))
        .ok(onDatabase("functional", TPrivilegeLevel.SELECT))
        .ok(onTable("functional", "alltypes_view", TPrivilegeLevel.ALL))
        .ok(onTable("functional", "alltypes_view", TPrivilegeLevel.SELECT))
        .error(selectError("functional.alltypes_view"))
        .error(selectError("functional.alltypes_view"), onServer(allExcept(
            TPrivilegeLevel.ALL, TPrivilegeLevel.SELECT)))
        .error(selectError("functional.alltypes_view"), onDatabase("functional",
            allExcept(TPrivilegeLevel.ALL, TPrivilegeLevel.SELECT)))
        .error(selectError("functional.alltypes_view"), onTable("functional",
            "alltypes_view", allExcept(TPrivilegeLevel.ALL, TPrivilegeLevel.SELECT)));

    // Constant select.
    authorize("select 1").ok();

    // Select on view and join table.
    authorize("select a.id from functional.view_view a " +
        "join functional.alltypesagg b ON (a.id = b.id)")
        .ok(onServer(TPrivilegeLevel.ALL))
        .ok(onServer(TPrivilegeLevel.SELECT))
        .ok(onDatabase("functional", TPrivilegeLevel.ALL))
        .ok(onDatabase("functional", TPrivilegeLevel.SELECT))
        .ok(onTable("functional", "view_view", TPrivilegeLevel.ALL),
            onTable("functional", "alltypesagg", TPrivilegeLevel.ALL))
        .ok(onTable("functional", "view_view", TPrivilegeLevel.ALL),
            onTable("functional", "alltypesagg", TPrivilegeLevel.SELECT))
        .ok(onTable("functional", "view_view", TPrivilegeLevel.SELECT),
            onTable("functional", "alltypesagg", TPrivilegeLevel.ALL))
        .ok(onTable("functional", "view_view", TPrivilegeLevel.SELECT),
            onTable("functional", "alltypesagg", TPrivilegeLevel.SELECT))
        .error(selectError("functional.view_view"))
        .error(selectError("functional.view_view"), onServer(allExcept(
            TPrivilegeLevel.ALL, TPrivilegeLevel.SELECT)))
        .error(selectError("functional.view_view"), onDatabase("functional",
            allExcept(TPrivilegeLevel.ALL, TPrivilegeLevel.SELECT)))
        .error(selectError("functional.view_view"), onTable("functional", "view_view",
            allExcept(TPrivilegeLevel.ALL, TPrivilegeLevel.SELECT)), onTable("functional",
            "alltypesagg", allExcept(TPrivilegeLevel.ALL, TPrivilegeLevel.SELECT)));

    // Tests authorization after a statement has been rewritten (IMPALA-3915).
    authorize("select * from functional_seq_snap.subquery_view")
        .ok(onServer(TPrivilegeLevel.ALL))
        .ok(onServer(TPrivilegeLevel.SELECT))
        .ok(onDatabase("functional_seq_snap", TPrivilegeLevel.ALL))
        .ok(onDatabase("functional_seq_snap", TPrivilegeLevel.SELECT))
        .ok(onTable("functional_seq_snap", "subquery_view", TPrivilegeLevel.ALL))
        .ok(onTable("functional_seq_snap", "subquery_view", TPrivilegeLevel.SELECT))
        .error(selectError("functional_seq_snap.subquery_view"))
        .error(selectError("functional_seq_snap.subquery_view"), onServer(
            allExcept(TPrivilegeLevel.ALL, TPrivilegeLevel.SELECT)))
        .error(selectError("functional_seq_snap.subquery_view"),
            onDatabase("functional_seq_snap", allExcept(TPrivilegeLevel.ALL,
            TPrivilegeLevel.SELECT)))
        .error(selectError("functional_seq_snap.subquery_view"),
            onTable("functional_seq_snap", "subquery_view", allExcept(
            TPrivilegeLevel.ALL, TPrivilegeLevel.SELECT)));

    // Select from non-existent database.
    authorize("select 1 from nodb.alltypes")
        .error(selectError("nodb.alltypes"));

    // Select from non-existent table.
    authorize("select 1 from functional.notbl")
        .error(selectError("functional.notbl"));

    // Select with inline view.
    authorize("select a.* from (select * from functional.alltypes) a")
        .ok(onServer(TPrivilegeLevel.ALL))
        .ok(onServer(TPrivilegeLevel.SELECT))
        .ok(onDatabase("functional", TPrivilegeLevel.ALL))
        .ok(onDatabase("functional", TPrivilegeLevel.SELECT))
        .ok(onTable("functional", "alltypes", TPrivilegeLevel.ALL))
        .ok(onTable("functional", "alltypes", TPrivilegeLevel.SELECT))
        .ok(onColumn("functional", "alltypes", ALLTYPES_COLUMNS, TPrivilegeLevel.SELECT))
        .error(selectError("functional.alltypes"))
        .error(selectError("functional.alltypes"), onServer(allExcept(
            TPrivilegeLevel.ALL, TPrivilegeLevel.SELECT)))
        .error(selectError("functional.alltypes"), onDatabase("functional",
            allExcept(TPrivilegeLevel.ALL, TPrivilegeLevel.SELECT)))
        .error(selectError("functional.alltypes"), onTable("functional", "alltypes",
            allExcept(TPrivilegeLevel.ALL, TPrivilegeLevel.SELECT)));

    // Select with columns referenced in function, where clause and group by.
    authorize("select count(id), int_col from functional.alltypes where id = 10 " +
        "group by id, int_col")
        .ok(onServer(TPrivilegeLevel.ALL))
        .ok(onServer(TPrivilegeLevel.SELECT))
        .ok(onDatabase("functional", TPrivilegeLevel.ALL))
        .ok(onDatabase("functional", TPrivilegeLevel.SELECT))
        .ok(onTable("functional", "alltypes", TPrivilegeLevel.ALL))
        .ok(onTable("functional", "alltypes", TPrivilegeLevel.SELECT))
        .ok(onColumn("functional", "alltypes", new String[]{"id", "int_col"},
            TPrivilegeLevel.SELECT))
        .error(selectError("functional.alltypes"))
        .error(selectError("functional.alltypes"), onServer(allExcept(
            TPrivilegeLevel.ALL, TPrivilegeLevel.SELECT)))
        .error(selectError("functional.alltypes"), onDatabase("functional",
            allExcept(TPrivilegeLevel.ALL, TPrivilegeLevel.SELECT)))
        .error(selectError("functional.alltypes"), onTable("functional", "alltypes",
            allExcept(TPrivilegeLevel.ALL, TPrivilegeLevel.SELECT)));

    // Select on tables with complex types.
    authorize("select a.int_struct_col.f1 from functional.allcomplextypes a " +
        "where a.id = 1")
        .ok(onServer(TPrivilegeLevel.ALL))
        .ok(onServer(TPrivilegeLevel.SELECT))
        .ok(onDatabase("functional", TPrivilegeLevel.ALL))
        .ok(onDatabase("functional", TPrivilegeLevel.SELECT))
        .ok(onTable("functional", "allcomplextypes", TPrivilegeLevel.ALL))
        .ok(onTable("functional", "allcomplextypes", TPrivilegeLevel.SELECT))
        .ok(onColumn("functional", "allcomplextypes",
            new String[]{"id", "int_struct_col"}, TPrivilegeLevel.SELECT))
        .error(selectError("functional.allcomplextypes"))
        .error(selectError("functional.allcomplextypes"), onServer(
            allExcept(TPrivilegeLevel.ALL, TPrivilegeLevel.SELECT)))
        .error(selectError("functional.allcomplextypes"), onDatabase("functional",
            allExcept(TPrivilegeLevel.ALL, TPrivilegeLevel.SELECT)))
        .error(selectError("functional.allcomplextypes"), onTable("functional",
            "allcomplextypes", allExcept(TPrivilegeLevel.ALL, TPrivilegeLevel.SELECT)));

    authorize("select key, pos, item.f1, f2 from functional.allcomplextypes t, " +
        "t.struct_array_col, functional.allcomplextypes.int_map_col")
        .ok(onServer(TPrivilegeLevel.ALL))
        .ok(onServer(TPrivilegeLevel.SELECT))
        .ok(onDatabase("functional", TPrivilegeLevel.ALL))
        .ok(onDatabase("functional", TPrivilegeLevel.SELECT))
        .ok(onTable("functional", "allcomplextypes", TPrivilegeLevel.ALL))
        .ok(onTable("functional", "allcomplextypes", TPrivilegeLevel.SELECT))
        .ok(onColumn("functional", "allcomplextypes",
            new String[]{"struct_array_col", "int_map_col"}, TPrivilegeLevel.SELECT))
        .error(selectError("functional.allcomplextypes"))
        .error(selectError("functional.allcomplextypes"), onServer(allExcept(
            TPrivilegeLevel.ALL, TPrivilegeLevel.SELECT)))
        .error(selectError("functional.allcomplextypes"), onDatabase("functional",
            allExcept(TPrivilegeLevel.ALL, TPrivilegeLevel.SELECT)))
        .error(selectError("functional.allcomplextypes"), onTable("functional",
            "allcomplextypes", allExcept(TPrivilegeLevel.ALL, TPrivilegeLevel.SELECT)));

    for (AuthzTest authzTest: new AuthzTest[]{
        // Select with cross join.
        authorize("select * from functional.alltypes union all " +
            "select * from functional.alltypessmall"),
        // Union on tables.
        authorize("select * from functional.alltypes a cross join " +
            "functional.alltypessmall b")}) {
      authzTest.ok(onServer(TPrivilegeLevel.ALL))
          .ok(onServer(TPrivilegeLevel.SELECT))
          .ok(onDatabase("functional", TPrivilegeLevel.ALL))
          .ok(onDatabase("functional", TPrivilegeLevel.SELECT))
          .ok(onTable("functional", "alltypes", TPrivilegeLevel.ALL),
              onTable("functional", "alltypessmall", TPrivilegeLevel.ALL))
          .ok(onTable("functional", "alltypes", TPrivilegeLevel.SELECT),
              onTable("functional", "alltypessmall", TPrivilegeLevel.ALL))
          .ok(onTable("functional", "alltypes", TPrivilegeLevel.ALL),
              onTable("functional", "alltypessmall", TPrivilegeLevel.SELECT))
          .ok(onTable("functional", "alltypes", TPrivilegeLevel.SELECT),
              onTable("functional", "alltypessmall", TPrivilegeLevel.SELECT))
          .ok(onColumn("functional", "alltypes", ALLTYPES_COLUMNS,
              TPrivilegeLevel.SELECT), onColumn("functional", "alltypessmall",
              ALLTYPES_COLUMNS, TPrivilegeLevel.SELECT))
          .error(selectError("functional.alltypes"))
          .error(selectError("functional.alltypes"), onServer(
              allExcept(TPrivilegeLevel.ALL, TPrivilegeLevel.SELECT)))
          .error(selectError("functional.alltypes"), onDatabase("functional",
              allExcept(TPrivilegeLevel.ALL, TPrivilegeLevel.SELECT)))
          .error(selectError("functional.alltypes"), onTable("functional", "alltypes",
              allExcept(TPrivilegeLevel.ALL, TPrivilegeLevel.SELECT)),
              onTable("functional", "alltypessmall", allExcept(TPrivilegeLevel.ALL,
              TPrivilegeLevel.SELECT)))
          .error(selectError("functional.alltypessmall"), onColumn("functional",
              "alltypes", ALLTYPES_COLUMNS, TPrivilegeLevel.SELECT))
          .error(selectError("functional.alltypes"), onColumn("functional",
              "alltypessmall", ALLTYPES_COLUMNS, TPrivilegeLevel.SELECT));
    }

    // Union on views.
    // Column-level privileges on views are not currently supported.
    authorize("select id from functional.alltypes_view union all " +
        "select x from functional.alltypes_view_sub")
        .ok(onServer(TPrivilegeLevel.ALL))
        .ok(onServer(TPrivilegeLevel.SELECT))
        .ok(onDatabase("functional", TPrivilegeLevel.ALL))
        .ok(onDatabase("functional", TPrivilegeLevel.SELECT))
        .ok(onTable("functional", "alltypes_view", TPrivilegeLevel.ALL),
            onTable("functional", "alltypes_view_sub", TPrivilegeLevel.ALL))
        .ok(onTable("functional", "alltypes_view", TPrivilegeLevel.SELECT),
            onTable("functional", "alltypes_view_sub", TPrivilegeLevel.SELECT))
        .error(selectError("functional.alltypes_view"))
        .error(selectError("functional.alltypes_view"), onServer(allExcept(
            TPrivilegeLevel.ALL, TPrivilegeLevel.SELECT)))
        .error(selectError("functional.alltypes_view"), onDatabase("functional",
            allExcept(TPrivilegeLevel.ALL, TPrivilegeLevel.SELECT)))
        .error(selectError("functional.alltypes_view"), onTable("functional",
            "alltypes_view", allExcept(TPrivilegeLevel.ALL, TPrivilegeLevel.SELECT)),
            onTable("functional", "alltypes_view_sub", TPrivilegeLevel.SELECT))
        .error(selectError("functional.alltypes_view_sub"), onTable("functional",
            "alltypes_view_sub", allExcept(TPrivilegeLevel.ALL, TPrivilegeLevel.SELECT)),
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
    authorize("insert into functional.zipcode_incomes(id) values('123')")
        .ok(onServer(TPrivilegeLevel.ALL))
        .ok(onServer(TPrivilegeLevel.INSERT))
        .ok(onDatabase("functional", TPrivilegeLevel.ALL))
        .ok(onDatabase("functional", TPrivilegeLevel.INSERT))
        .ok(onTable("functional", "zipcode_incomes", TPrivilegeLevel.ALL))
        .ok(onTable("functional", "zipcode_incomes", TPrivilegeLevel.INSERT))
        .error(insertError("functional.zipcode_incomes"))
        .error(insertError("functional.zipcode_incomes"), onServer(allExcept(
            TPrivilegeLevel.ALL, TPrivilegeLevel.INSERT)))
        .error(insertError("functional.zipcode_incomes"), onDatabase("functional",
            allExcept(TPrivilegeLevel.ALL, TPrivilegeLevel.INSERT)))
        .error(insertError("functional.zipcode_incomes"), onTable("functional",
            "zipcode_incomes", allExcept(TPrivilegeLevel.ALL, TPrivilegeLevel.INSERT)));

    for (AuthzTest test: new AuthzTest[]{
        // With clause with insert.
        authorize("with t as (select * from functional.alltypestiny) " +
            "insert into functional.alltypes partition(month, year) " +
            "select * from t"),
        // Insert with select on a target table.
        authorize("insert into functional.alltypes partition(month, year) " +
            "select * from functional.alltypestiny where id < 100")}) {
      test.ok(onServer(TPrivilegeLevel.ALL))
          .ok(onServer(TPrivilegeLevel.INSERT, TPrivilegeLevel.SELECT))
          .ok(onDatabase("functional", TPrivilegeLevel.ALL))
          .ok(onDatabase("functional", TPrivilegeLevel.INSERT, TPrivilegeLevel.SELECT))
          .ok(onTable("functional", "alltypes", TPrivilegeLevel.ALL),
              onTable("functional", "alltypestiny", TPrivilegeLevel.ALL))
          .ok(onTable("functional", "alltypes", TPrivilegeLevel.INSERT),
              onTable("functional", "alltypestiny", TPrivilegeLevel.SELECT))
          .ok(onTable("functional", "alltypes", TPrivilegeLevel.INSERT),
              onColumn("functional", "alltypestiny", ALLTYPES_COLUMNS,
              TPrivilegeLevel.SELECT))
          .error(selectError("functional.alltypestiny"))
          .error(selectError("functional.alltypestiny"), onServer(allExcept(
              TPrivilegeLevel.ALL, TPrivilegeLevel.INSERT, TPrivilegeLevel.SELECT)))
          .error(selectError("functional.alltypestiny"), onDatabase("functional",
              allExcept(TPrivilegeLevel.ALL, TPrivilegeLevel.INSERT,
              TPrivilegeLevel.SELECT)))
          .error(insertError("functional.alltypes"), onTable("functional",
              "alltypestiny", TPrivilegeLevel.SELECT), onTable("functional",
              "alltypes", allExcept(TPrivilegeLevel.ALL, TPrivilegeLevel.INSERT)))
          .error(selectError("functional.alltypestiny"), onTable("functional",
              "alltypestiny", allExcept(TPrivilegeLevel.ALL, TPrivilegeLevel.SELECT)),
              onTable("functional", "alltypes", TPrivilegeLevel.INSERT));
    }

    // Insert with select on a target view.
    // Column-level privileges on views are not currently supported.
    authorize("insert into functional.alltypes partition(month, year) " +
        "select * from functional.alltypes_view where id < 100")
        .ok(onServer(TPrivilegeLevel.ALL))
        .ok(onServer(TPrivilegeLevel.INSERT, TPrivilegeLevel.SELECT))
        .ok(onDatabase("functional", TPrivilegeLevel.ALL))
        .ok(onDatabase("functional", TPrivilegeLevel.INSERT, TPrivilegeLevel.SELECT))
        .ok(onTable("functional", "alltypes", TPrivilegeLevel.ALL),
            onTable("functional", "alltypes_view", TPrivilegeLevel.ALL))
        .ok(onTable("functional", "alltypes", TPrivilegeLevel.INSERT),
            onTable("functional", "alltypes_view", TPrivilegeLevel.SELECT))
        .error(selectError("functional.alltypes_view"))
        .error(selectError("functional.alltypes_view"), onServer(allExcept(
            TPrivilegeLevel.ALL, TPrivilegeLevel.INSERT, TPrivilegeLevel.SELECT)))
        .error(selectError("functional.alltypes_view"), onDatabase("functional",
            allExcept(TPrivilegeLevel.ALL, TPrivilegeLevel.INSERT,
            TPrivilegeLevel.SELECT)))
        .error(insertError("functional.alltypes"), onTable("functional",
            "alltypes_view", TPrivilegeLevel.SELECT), onTable("functional",
            "alltypes", allExcept(TPrivilegeLevel.ALL, TPrivilegeLevel.INSERT)))
        .error(selectError("functional.alltypes_view"), onTable("functional",
            "alltypes_view", allExcept(TPrivilegeLevel.ALL, TPrivilegeLevel.SELECT)),
            onTable("functional", "alltypes", TPrivilegeLevel.INSERT));

    // Insert with inline view.
    authorize("insert into functional.alltypes partition(month, year) " +
        "select b.* from functional.alltypesagg a join (select * from " +
        "functional.alltypestiny) b on (a.int_col = b.int_col)")
        .ok(onServer(TPrivilegeLevel.ALL))
        .ok(onServer(TPrivilegeLevel.INSERT, TPrivilegeLevel.SELECT))
        .ok(onDatabase("functional", TPrivilegeLevel.ALL))
        .ok(onDatabase("functional", TPrivilegeLevel.INSERT, TPrivilegeLevel.SELECT))
        .ok(onTable("functional", "alltypes", TPrivilegeLevel.ALL),
            onTable("functional", "alltypesagg", TPrivilegeLevel.ALL),
            onTable("functional", "alltypestiny", TPrivilegeLevel.ALL))
        .ok(onTable("functional", "alltypes", TPrivilegeLevel.INSERT),
            onTable("functional", "alltypesagg", TPrivilegeLevel.SELECT),
            onTable("functional", "alltypestiny", TPrivilegeLevel.SELECT))
        .error(selectError("functional.alltypesagg"))
        .error(selectError("functional.alltypesagg"), onServer(allExcept(
            TPrivilegeLevel.ALL, TPrivilegeLevel.INSERT, TPrivilegeLevel.SELECT)))
        .error(selectError("functional.alltypesagg"), onDatabase("functional", allExcept(
            TPrivilegeLevel.ALL, TPrivilegeLevel.INSERT, TPrivilegeLevel.SELECT)))
        .error(insertError("functional.alltypes"), onTable("functional",
            "alltypesagg", TPrivilegeLevel.SELECT), onTable("functional",
            "alltypestiny", TPrivilegeLevel.SELECT), onTable("functional",
            "alltypes", allExcept(TPrivilegeLevel.ALL, TPrivilegeLevel.INSERT)))
        .error(selectError("functional.alltypesagg"), onTable("functional",
            "alltypesagg", allExcept(TPrivilegeLevel.ALL, TPrivilegeLevel.SELECT)),
            onTable("functional", "alltypestiny", TPrivilegeLevel.SELECT),
            onTable("functional", "alltypes", TPrivilegeLevel.INSERT))
        .error(selectError("functional.alltypestiny"), onTable("functional",
            "alltypesagg", TPrivilegeLevel.SELECT), onTable("functional",
            "alltypestiny", allExcept(TPrivilegeLevel.ALL, TPrivilegeLevel.SELECT)),
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
        .ok(onServer(TPrivilegeLevel.INSERT))
        .ok(onDatabase("functional", TPrivilegeLevel.ALL))
        .ok(onDatabase("functional", TPrivilegeLevel.INSERT))
        .ok(onTable("functional", "alltypes", TPrivilegeLevel.ALL))
        .ok(onTable("functional", "alltypes", TPrivilegeLevel.INSERT))
        .error(insertError("functional.alltypes"))
        .error(insertError("functional.alltypes"), onServer(allExcept(
            TPrivilegeLevel.ALL, TPrivilegeLevel.INSERT)))
        .error(insertError("functional.alltypes"), onDatabase("functional", allExcept(
            TPrivilegeLevel.ALL, TPrivilegeLevel.INSERT)))
        .error(insertError("functional.alltypes"), onTable("functional", "alltypes",
            allExcept(TPrivilegeLevel.ALL, TPrivilegeLevel.INSERT)));

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
      .ok(onDatabase("functional", TPrivilegeLevel.ALL),
          onUri("hdfs://localhost:20500/test-warehouse/tpch.lineitem",
          TPrivilegeLevel.ALL))
      .ok(onDatabase("functional", TPrivilegeLevel.INSERT),
          onUri("hdfs://localhost:20500/test-warehouse/tpch.lineitem",
          TPrivilegeLevel.ALL))
      .ok(onTable("functional", "alltypes", TPrivilegeLevel.ALL),
          onUri("hdfs://localhost:20500/test-warehouse/tpch.lineitem",
          TPrivilegeLevel.ALL))
      .ok(onTable("functional", "alltypes", TPrivilegeLevel.INSERT),
          onUri("hdfs://localhost:20500/test-warehouse/tpch.lineitem",
          TPrivilegeLevel.ALL))
      .error(insertError("functional.alltypes"))
      .error(accessError("hdfs://localhost:20500/test-warehouse/tpch.lineitem"),
          onDatabase("functional", TPrivilegeLevel.INSERT))
      .error(accessError("hdfs://localhost:20500/test-warehouse/tpch.lineitem"),
          onTable("functional", "alltypes", TPrivilegeLevel.INSERT))
      .error(insertError("functional.alltypes"),
          onUri("hdfs://localhost:20500/test-warehouse/tpch.lineitem",
          TPrivilegeLevel.ALL));

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
            "hdfs://localhost:20500/test-warehouse/tpch.nouri", TPrivilegeLevel.ALL));

    // Load into non-existent table.
    authorize("load data inpath 'hdfs://localhost:20500/test-warehouse/tpch.lineitem' " +
        "into table functional.notbl partition(month=10, year=2009)")
        .error(insertError("functional.notbl"))
        .error(insertError("functional.notbl"), onUri(
            "hdfs://localhost:20500/test-warehouse/tpch.nouri", TPrivilegeLevel.ALL));

    // Load into a view is not supported.
    authorize("load data inpath 'hdfs://localhost:20500/test-warehouse/tpch.lineitem' " +
        "into table functional.alltypes_view")
        .error(insertError("functional.alltypes_view"));
  }

  @Test
  public void testResetMetadata() throws ImpalaException {
    // Invalidate metadata on server.
    authorize("invalidate metadata")
        .ok(onServer(TPrivilegeLevel.ALL))
        .ok(onServer(TPrivilegeLevel.REFRESH))
        .error(refreshError("server"));

    // Invalidate metadata/refresh on a table / view
    for(String name: new String[] {"alltypes", "alltypes_view"}) {
      for (AuthzTest test: new AuthzTest[]{
          authorize("invalidate metadata functional." + name),
          authorize("refresh functional." + name)}) {
        test.ok(onServer(TPrivilegeLevel.ALL))
            .ok(onServer(TPrivilegeLevel.REFRESH))
            .ok(onDatabase("functional", TPrivilegeLevel.ALL))
            .ok(onDatabase("functional", TPrivilegeLevel.REFRESH))
            .ok(onTable("functional", name, TPrivilegeLevel.ALL))
            .ok(onTable("functional", name, TPrivilegeLevel.REFRESH))
            .error(refreshError("functional." + name))
            .error(refreshError("functional." + name), onDatabase("functional", allExcept(
                TPrivilegeLevel.ALL, TPrivilegeLevel.REFRESH)))
            .error(refreshError("functional." + name), onTable("functional", name,
                allExcept(TPrivilegeLevel.ALL, TPrivilegeLevel.REFRESH)));
      }
    }

    authorize("refresh functions functional")
        .ok(onServer(TPrivilegeLevel.REFRESH))
        .ok(onDatabase("functional", TPrivilegeLevel.ALL))
        .ok(onDatabase("functional", TPrivilegeLevel.REFRESH))
        .error(refreshError("functional"))
        .error(refreshError("functional"), onServer(allExcept(TPrivilegeLevel.ALL,
            TPrivilegeLevel.REFRESH)))
        .error(refreshError("functional"), onDatabase("functional", allExcept(
            TPrivilegeLevel.ALL, TPrivilegeLevel.REFRESH)));

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
    authorize(String.format("show role grant group %s", USER.getName())).ok();

    // Show grant role should always be allowed.
    authorize(String.format("show grant role authz_test_role")).ok();

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

  @Test
  /**
   * Test describe output of Databases and tables.
   * From https://issues.apache.org/jira/browse/IMPALA-6479
   * Column level select privileges should limit output.
   */
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
    for (TPrivilegeLevel privilege: new TPrivilegeLevel[]{
        TPrivilegeLevel.ALL, TPrivilegeLevel.SELECT}) {
      authzTest.okDescribe(tableName, style, ALLTYPES_COLUMNS, null, onServer(privilege))
          .okDescribe(tableName, style, ALLTYPES_COLUMNS, null, onDatabase("functional",
              privilege))
          .okDescribe(tableName, style, ALLTYPES_COLUMNS, null, onTable("functional",
              "alltypes", privilege));
    }
    authzTest.okDescribe(tableName, style, null, ALLTYPES_COLUMNS, onServer(allExcept(
        TPrivilegeLevel.ALL, TPrivilegeLevel.SELECT)))
        .okDescribe(tableName, style, null, ALLTYPES_COLUMNS, onDatabase("functional",
            allExcept(TPrivilegeLevel.ALL, TPrivilegeLevel.SELECT)))
        .okDescribe(tableName, style, null, ALLTYPES_COLUMNS, onTable("functional",
            "alltypes", allExcept(TPrivilegeLevel.ALL, TPrivilegeLevel.SELECT)))
        // In this test, since we only have column level privileges on "id", then
        // only the "id" column should show and the others should not.
        .okDescribe(tableName, style, new String[]{"id"}, ALLTYPES_COLUMNS_WITHOUT_ID,
            onColumn("functional", "alltypes", "id", TPrivilegeLevel.SELECT))
        .error(accessError("functional.alltypes"));

    // Describe table extended.
    tableName = new TTableName("functional", "alltypes");
    style = TDescribeOutputStyle.EXTENDED;
    String[] locationString = new String[]{"Location:"};
    String[] checkStrings = (String[]) ArrayUtils.addAll(ALLTYPES_COLUMNS,
        locationString);
    authzTest = authorize("describe functional.alltypes");
    for (TPrivilegeLevel privilege: new TPrivilegeLevel[]{
        TPrivilegeLevel.ALL, TPrivilegeLevel.SELECT}) {
      authzTest.okDescribe(tableName, style, checkStrings, null, onServer(privilege))
          .okDescribe(tableName, style, checkStrings, null, onDatabase("functional",
              privilege))
          .okDescribe(tableName, style, checkStrings, null, onTable("functional",
              "alltypes", privilege));
    }
    authzTest.okDescribe(tableName, style, locationString, ALLTYPES_COLUMNS,
        onServer(allExcept(TPrivilegeLevel.ALL, TPrivilegeLevel.SELECT)))
        .okDescribe(tableName, style, locationString, ALLTYPES_COLUMNS,
            onDatabase("functional", allExcept(TPrivilegeLevel.ALL,
            TPrivilegeLevel.SELECT)))
        .okDescribe(tableName, style, locationString, ALLTYPES_COLUMNS,
            onTable("functional", "alltypes", allExcept(TPrivilegeLevel.ALL,
            TPrivilegeLevel.SELECT)))
        // Location should not appear with only column level auth.
        .okDescribe(tableName, style, new String[]{"id"},
            (String[]) ArrayUtils.addAll(ALLTYPES_COLUMNS_WITHOUT_ID,
            new String[]{"Location:"}), onColumn("functional", "alltypes", "id",
            TPrivilegeLevel.SELECT))
        .error(accessError("functional.alltypes"));

    // Describe view.
    tableName = new TTableName("functional", "alltypes_view");
    style = TDescribeOutputStyle.MINIMAL;
    authzTest = authorize("describe functional.alltypes_view");
    for (TPrivilegeLevel privilege: new TPrivilegeLevel[]{
        TPrivilegeLevel.ALL, TPrivilegeLevel.SELECT}) {
      authzTest.okDescribe(tableName, style, ALLTYPES_COLUMNS, null, onServer(privilege))
          .okDescribe(tableName, style, ALLTYPES_COLUMNS, null, onDatabase("functional",
              privilege))
          .okDescribe(tableName, style, ALLTYPES_COLUMNS, null, onTable("functional",
              "alltypes_view", privilege));
    }
    authzTest.okDescribe(tableName, style, null, ALLTYPES_COLUMNS, onServer(allExcept(
        TPrivilegeLevel.ALL, TPrivilegeLevel.SELECT)))
        .okDescribe(tableName, style, null, ALLTYPES_COLUMNS, onDatabase("functional",
            allExcept(TPrivilegeLevel.ALL, TPrivilegeLevel.SELECT)))
        .okDescribe(tableName, style, null, ALLTYPES_COLUMNS, onTable("functional",
            "alltypes_view", TPrivilegeLevel.INSERT))
        .error(accessError("functional.alltypes_view"));

    // Describe view extended.
    tableName = new TTableName("functional", "alltypes_view");
    style = TDescribeOutputStyle.EXTENDED;
    // Views have extra output to explicitly check
    String[] viewStrings = new String[]{"View Original Text:", "View Expanded Text:"};
    checkStrings = (String[]) ArrayUtils.addAll(ALLTYPES_COLUMNS, viewStrings);
    authzTest = authorize("describe functional.alltypes_view");
    for (TPrivilegeLevel privilege: new TPrivilegeLevel[]{
        TPrivilegeLevel.ALL, TPrivilegeLevel.SELECT}) {
      authzTest.okDescribe(tableName, style, checkStrings, null, onServer(privilege))
          .okDescribe(tableName, style, checkStrings, null, onDatabase("functional",
              privilege))
          .okDescribe(tableName, style, checkStrings, null, onTable("functional",
              "alltypes_view", privilege));
    }
    authzTest.okDescribe(tableName, style, null, ALLTYPES_COLUMNS, onServer(allExcept(
        TPrivilegeLevel.ALL, TPrivilegeLevel.SELECT)))
        .okDescribe(tableName, style, null, ALLTYPES_COLUMNS, onDatabase("functional",
            allExcept(TPrivilegeLevel.ALL, TPrivilegeLevel.SELECT)))
        .okDescribe(tableName, style, viewStrings, ALLTYPES_COLUMNS, onTable("functional",
            "alltypes_view", TPrivilegeLevel.INSERT))
        .error(accessError("functional.alltypes_view"));

    // Describe specific column on a table.
    authzTest = authorize("describe functional.allcomplextypes.int_struct_col");
    for (TPrivilegeLevel privilege: TPrivilegeLevel.values()) {
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

  // Convert TDescribeResult to list of strings.
  private static List<String> resultToStringList(TDescribeResult result) {
    List<String> list = new ArrayList<>();
    for (TResultRow row: result.getResults()) {
      for (TColumnValue col: row.getColVals()) {
        list.add(col.getString_val() == null ? "NULL": col.getString_val().trim());
      }
    }
    return list;
  }

  private static String selectError(String object) {
    return "User '%s' does not have privileges to execute 'SELECT' on: " + object;
  }

  private static String insertError(String object) {
    return "User '%s' does not have privileges to execute 'INSERT' on: " + object;
  }

  private static String accessError(String object) {
    return "User '%s' does not have privileges to access: " + object;
  }

  private static String refreshError(String object) {
    return "User '%s' does not have privileges to execute " +
        "'INVALIDATE METADATA/REFRESH' on: " + object;
  }

  private ScalarFunction addFunction(String db, String fnName) {
    ScalarFunction fn = ScalarFunction.createForTesting(db, fnName,
        new ArrayList<Type>(), Type.INT, "/dummy", "dummy.class", null,
        null, TFunctionBinaryType.NATIVE);
    authzCatalog_.addFunction(fn);
    return fn;
  }

  private void removeFunction(ScalarFunction fn) {
    authzCatalog_.removeFunction(fn);
  }

  private TPrivilegeLevel[] viewMetadataPrivileges() {
    return new TPrivilegeLevel[]{TPrivilegeLevel.ALL, TPrivilegeLevel.SELECT,
        TPrivilegeLevel.INSERT, TPrivilegeLevel.REFRESH};
  }

  private static TPrivilegeLevel[] allExcept(TPrivilegeLevel... excludedPrivLevels) {
    HashSet<TPrivilegeLevel> excludedSet = Sets.newHashSet(excludedPrivLevels);
    List<TPrivilegeLevel> privLevels = new ArrayList<>();
    for (TPrivilegeLevel level: TPrivilegeLevel.values()) {
      if (!excludedSet.contains(level)) {
        privLevels.add(level);
      }
    }
    return privLevels.toArray(new TPrivilegeLevel[0]);
  }

  private class AuthzTest {
    private final AnalysisContext context_;
    private final String stmt_;
    private final String role_ = "authz_test_role";

    public AuthzTest(String stmt) {
      this(null, stmt);
    }

    public AuthzTest(AnalysisContext context, String stmt) {
      Preconditions.checkNotNull(stmt);
      context_ = context;
      stmt_ = stmt;
    }

    private void createRole(TPrivilege[]... privileges) throws ImpalaException {
      Role role = authzCatalog_.addRole(role_);
      authzCatalog_.addRoleGrantGroup(role_, USER.getName());
      for (TPrivilege[] privs: privileges) {
        for (TPrivilege privilege: privs) {
          privilege.setRole_id(role.getId());
          authzCatalog_.addRolePrivilege(role_, privilege);
        }
      }
    }

    private void dropRole() throws ImpalaException {
      authzCatalog_.removeRole(role_);
    }

    /**
     * This method runs with the specified privileges.
     *
     * A new temporary role will be created and assigned to the specified privileges
     * into the new role. The new role will be dropped once this method finishes.
     */
    public AuthzTest ok(TPrivilege[]... privileges) throws ImpalaException {
      try {
        createRole(privileges);
        if (context_ != null) {
          authzOk(context_, stmt_);
        } else {
          authzOk(stmt_);
        }
      } catch (AuthorizationException ae) {
        // Because the same test can be called from multiple statements
        // it is useful to know which statement caused the exception.
        throw new AuthorizationException(stmt_ + ": " + ae.getMessage(), ae);
      } finally {
        dropRole();
      }
      return this;
    }

    /**
     * This method runs with the specified privileges and checks describe output.
     *
     * A new temporary role will be created and assigned to the specified privileges
     * into the new role. The new role will be dropped once this method finishes.
     */
    public AuthzTest okDescribe(TTableName table, TDescribeOutputStyle style,
        String[] requiredStrings, String[] excludedStrings, TPrivilege[]... privileges)
        throws ImpalaException {
      try {
        createRole(privileges);
        if (context_ != null) {
          authzOk(context_, stmt_);
        } else {
          authzOk(stmt_);
        }
        List<String> result = resultToStringList(authzFrontend_.describeTable(table,
            style, USER));
        if (requiredStrings != null) {
          for (String str: requiredStrings) {
            assertTrue(String.format("\"%s\" is not in the describe output.\n" +
                "Expected : %s\n" +
                "Actual   : %s", str, Arrays.toString(requiredStrings), result),
                result.contains(str));
          }
        }
        if (excludedStrings != null) {
          for (String str: excludedStrings) {
            assertTrue(String.format("\"%s\" should not be in the describe output.", str),
                !result.contains(str));
          }
        }
      } finally {
        dropRole();
      }
      return this;
    }

    /**
     * This method runs with the specified privileges.
     *
     * A new temporary role will be created and assigned to the specified privileges
     * into the new role. The new role will be dropped once this method finishes.
     */
    public AuthzTest error(String expectedError, TPrivilege[]... privileges)
        throws ImpalaException {
      try {
        createRole(privileges);
        if (context_ != null) {
          authzError(context_, stmt_, expectedError);
        } else {
          authzError(stmt_, expectedError);
        }
      } finally {
        dropRole();
      }
      return this;
    }
  }

  private AuthzTest authorize(String stmt) {
    return new AuthzTest(stmt);
  }

  private AuthzTest authorize(AnalysisContext ctx, String stmt) {
    return new AuthzTest(ctx, stmt);
  }

  private TPrivilege[] onServer(TPrivilegeLevel... levels) {
    TPrivilege[] privileges = new TPrivilege[levels.length];
    for (int i = 0; i < levels.length; i++) {
      privileges[i] = new TPrivilege("", levels[i], TPrivilegeScope.SERVER, false);
      privileges[i].setServer_name(SENTRY_SERVER);
      privileges[i].setPrivilege_name(RolePrivilege.buildRolePrivilegeName(
          privileges[i]));
    }
    return privileges;
  }

  private TPrivilege[] onDatabase(String db, TPrivilegeLevel... levels) {
    TPrivilege[] privileges = new TPrivilege[levels.length];
    for (int i = 0; i < levels.length; i++) {
      privileges[i] = new TPrivilege("", levels[i], TPrivilegeScope.DATABASE, false);
      privileges[i].setServer_name(SENTRY_SERVER);
      privileges[i].setDb_name(db);
      privileges[i].setPrivilege_name(RolePrivilege.buildRolePrivilegeName(
          privileges[i]));
    }
    return privileges;
  }

  private TPrivilege[] onTable(String db, String table, TPrivilegeLevel... levels) {
    TPrivilege[] privileges = new TPrivilege[levels.length];
    for (int i = 0; i < levels.length; i++) {
      privileges[i] = new TPrivilege("", levels[i], TPrivilegeScope.TABLE, false);
      privileges[i].setServer_name(SENTRY_SERVER);
      privileges[i].setDb_name(db);
      privileges[i].setTable_name(table);
      privileges[i].setPrivilege_name(RolePrivilege.buildRolePrivilegeName(
          privileges[i]));
    }
    return privileges;
  }

  private TPrivilege[] onColumn(String db, String table, String column,
      TPrivilegeLevel... levels) {
    return onColumn(db, table, new String[]{column}, levels);
  }

  private TPrivilege[] onColumn(String db, String table, String[] columns,
      TPrivilegeLevel... levels) {
    int size = columns.length * levels.length;
    TPrivilege[] privileges = new TPrivilege[size];
    int idx = 0;
    for (int i = 0; i < levels.length; i++) {
      for (String column: columns) {
        privileges[idx] = new TPrivilege("", levels[i], TPrivilegeScope.COLUMN, false);
        privileges[idx].setServer_name(SENTRY_SERVER);
        privileges[idx].setDb_name(db);
        privileges[idx].setTable_name(table);
        privileges[idx].setColumn_name(column);
        privileges[idx].setPrivilege_name(RolePrivilege.buildRolePrivilegeName(
            privileges[idx]));
        idx++;
      }
    }
    return privileges;
  }

  private TPrivilege[] onUri(String uri, TPrivilegeLevel... levels) {
    TPrivilege[] privileges = new TPrivilege[levels.length];
    for (int i = 0; i < levels.length; i++) {
      privileges[i] = new TPrivilege("", levels[i], TPrivilegeScope.URI, false);
      privileges[i].setServer_name(SENTRY_SERVER);
      privileges[i].setUri(uri);
      privileges[i].setPrivilege_name(RolePrivilege.buildRolePrivilegeName(
          privileges[i]));
    }
    return privileges;
  }

  private void authzOk(String stmt) throws ImpalaException {
    authzOk(analysisContext_, stmt);
  }

  private void authzOk(AnalysisContext context, String stmt) throws ImpalaException {
    authzOk(authzFrontend_, context, stmt);
  }

  private void authzOk(Frontend fe, AnalysisContext context, String stmt)
      throws ImpalaException {
    parseAndAnalyze(stmt, context, fe);
  }

  /**
   * Verifies that a given statement fails authorization and the expected error
   * string matches.
   */
  private void authzError(String stmt, String expectedError, Matcher matcher)
      throws ImpalaException {
    authzError(analysisContext_, stmt, expectedError, matcher);
  }

  private void authzError(String stmt, String expectedError)
      throws ImpalaException {
    authzError(analysisContext_, stmt, expectedError, startsWith());
  }

  private void authzError(AnalysisContext ctx, String stmt, String expectedError,
      Matcher matcher) throws ImpalaException {
    authzError(authzFrontend_, ctx, stmt, expectedError, matcher);
  }

  private void authzError(AnalysisContext ctx, String stmt, String expectedError)
      throws ImpalaException {
    authzError(authzFrontend_, ctx, stmt, expectedError, startsWith());
  }

  private interface Matcher {
    boolean match(String actual, String expected);
  }

  private static Matcher exact() {
    return new Matcher() {
      @Override
      public boolean match(String actual, String expected) {
        return actual.equals(expected);
      }
    };
  }

  private static Matcher startsWith() {
    return new Matcher() {
      @Override
      public boolean match(String actual, String expected) {
        return actual.startsWith(expected);
      }
    };
  }

  private void authzError(Frontend fe, AnalysisContext ctx,
      String stmt, String expectedErrorString, Matcher matcher)
      throws ImpalaException {
    Preconditions.checkNotNull(expectedErrorString);
    try {
      parseAndAnalyze(stmt, ctx, fe);
    } catch (AuthorizationException e) {
      // Insert the username into the error.
      expectedErrorString = String.format(expectedErrorString, ctx.getUser());
      String errorString = e.getMessage();
      assertTrue(
          "got error:\n" + errorString + "\nexpected:\n" + expectedErrorString,
          matcher.match(errorString, expectedErrorString));
      return;
    }
    fail("Stmt didn't result in authorization error: " + stmt);
  }

  private void verifyPrivilegeReqs(String stmt, Set<String> expectedPrivilegeNames)
      throws ImpalaException {
    verifyPrivilegeReqs(createAnalysisCtx(), stmt, expectedPrivilegeNames);
  }

  private void verifyPrivilegeReqs(AnalysisContext ctx, String stmt,
      Set<String> expectedPrivilegeNames) throws ImpalaException {
    AnalysisResult analysisResult = parseAndAnalyze(stmt, ctx, frontend_);
    Set<String> actualPrivilegeNames = Sets.newHashSet();
    for (PrivilegeRequest privReq: analysisResult.getAnalyzer().getPrivilegeReqs()) {
      actualPrivilegeNames.add(privReq.getName());
    }
    assertEquals(expectedPrivilegeNames, actualPrivilegeNames);
  }
}
