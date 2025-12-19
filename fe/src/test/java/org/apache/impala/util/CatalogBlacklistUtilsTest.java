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

package org.apache.impala.util;

import static org.apache.impala.util.CatalogBlacklistUtils.getBlacklistedDbsCount;
import static org.apache.impala.util.CatalogBlacklistUtils.getBlacklistedTablesCount;
import static org.apache.impala.util.CatalogBlacklistUtils.getBlacklistedTablesDbs;
import static org.apache.impala.util.CatalogBlacklistUtils.isDbBlacklisted;
import static org.apache.impala.util.CatalogBlacklistUtils.isTableBlacklisted;
import static org.apache.impala.util.CatalogBlacklistUtils.reload;
import static org.apache.impala.util.CatalogBlacklistUtils.verifyDbName;
import static org.apache.impala.util.CatalogBlacklistUtils.verifyTableName;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.collection.IsIterableContainingInAnyOrder.containsInAnyOrder;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;

import org.apache.impala.analysis.TableName;
import org.apache.impala.catalog.Catalog;
import org.apache.impala.common.AnalysisException;
import org.apache.impala.service.BackendConfig;
import org.apache.impala.thrift.TBackendGflags;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

public class CatalogBlacklistUtilsTest {

  private static TBackendGflags origFlags;

  @BeforeClass
  public static void setup() {
    // The original BackendConfig need to be saved so they can be restored and not break
    // other tests.
    if (BackendConfig.INSTANCE == null) {
      BackendConfig.create(new TBackendGflags());
    }
    origFlags = BackendConfig.INSTANCE.getBackendCfg();
  }

  @AfterClass
  public static void teardown() {
    BackendConfig.create(origFlags);
  }

  @Test
  public void testParsingBlacklistedDbsHappyPath() throws AnalysisException {
    setBlacklist("db1,db2", "");

    assertThat(getBlacklistedDbsCount(), equalTo(2));
    assertThat(isDbBlacklisted("db1"), is(true));
    assertThat(isDbBlacklisted("db2"), is(true));
    assertThat(isDbBlacklisted("db3"), is(false));

    verifyDbName("db3");
    try {
      verifyDbName("db1");
      fail("Expected AnalysisException for blacklisted db");
    } catch (AnalysisException e) {
      assertThat(e.getMessage(), equalTo("Invalid db name: db1. It has been blacklisted "
          + "using --blacklisted_dbs"));
    }
  }

  @Test
  public void testParsingBlacklistedDbsNamesWithSpaces() {
    setBlacklist(" db1 , db2 ", "");

    assertThat(getBlacklistedDbsCount(), equalTo(2));
    assertThat(isDbBlacklisted("db1"), is(true));
    assertThat(isDbBlacklisted("db2"), is(true));
    assertThat(isDbBlacklisted("db3"), is(false));
  }

  @Test
  public void testParsingBlacklistedDbsCaseInsensitiveNames() {
    setBlacklist("DB1,Db2", "");

    assertThat(getBlacklistedDbsCount(), equalTo(2));
    assertThat(isDbBlacklisted("db1"), is(true));
    assertThat(isDbBlacklisted("db2"), is(true));
    assertThat(isDbBlacklisted("db3"), is(false));
  }

  @Test
  public void testParsingBlacklistedDbsInvalidNames() {
    setBlacklist("db1,", "");

    assertThat(getBlacklistedDbsCount(), equalTo(1));
    assertThat(isDbBlacklisted("db1"), is(true));
    assertThat(isDbBlacklisted("db2"), is(false));
    assertThat(isDbBlacklisted("db3"), is(false));
  }

  @Test
  public void testParsingBlacklistedDbsNone() {
    assertThat(getBlacklistedDbsCount(), equalTo(0));
  }

  @Test
  public void testParsingBlacklistedTablesHappyPath() throws AnalysisException {
    TableName foo = new TableName("db3", "foo");
    TableName baz = new TableName("db3", "baz");
    setBlacklist("", "db3.foo,db3.bar,db4.tbl1");

    assertThat(getBlacklistedTablesCount(), equalTo(3));
    assertThat(isTableBlacklisted(foo.getDb(), foo.getTbl()), is(true));
    assertThat(isTableBlacklisted(foo), is(true));
    assertThat(isTableBlacklisted("db3", "bar"), is(true));
    assertThat(isTableBlacklisted(new TableName("db3", "bar")), is(true));
    assertThat(isTableBlacklisted(baz.getDb(), baz.getTbl()), is(false));
    assertThat(isTableBlacklisted(baz), is(false));
    assertThat(isTableBlacklisted("db4", "tbl1"), is(true));
    assertThat(getBlacklistedTablesDbs(), containsInAnyOrder("db3", "db4"));

    verifyTableName(baz);
    try {
      verifyTableName(foo);
      fail("Expected AnalysisException for blacklisted table");
    } catch (AnalysisException e) {
      assertThat(e.getMessage(), equalTo("Invalid table/view name: " + foo
          + ". It has been blacklisted using --blacklisted_tables"));
    }
  }

  @Test
  public void testParsingBlacklistedTablesNamesWithInputSpaces() {
    setBlacklist("", " db3 . foo , db3 . bar  ");

    assertThat(getBlacklistedTablesCount(), equalTo(2));
    assertThat(isTableBlacklisted("db3", "foo"), is(true));
    assertThat(isTableBlacklisted("db3", "bar"), is(true));
    assertThat(isTableBlacklisted("db3", "baz"), is(false));
    assertThat(getBlacklistedTablesDbs(), containsInAnyOrder("db3"));
  }

  @Test
  public void testParsingBlacklistedTablesNamesWithoutDb() {
    setBlacklist("", "foo");

    assertThat(getBlacklistedTablesCount(), equalTo(1));
    assertThat(isTableBlacklisted(Catalog.DEFAULT_DB, "foo"), is(true));
    assertThat(getBlacklistedTablesDbs(), containsInAnyOrder("default"));
  }

  @Test
  public void testParsingBlacklistedTablesCaseInsensitiveNames() {
    setBlacklist("", "DB3.Foo,db3.Bar");

    assertThat(getBlacklistedTablesCount(), equalTo(2));
    assertThat(isTableBlacklisted("db3", "foo"), is(true));
    assertThat(isTableBlacklisted("db3", "bar"), is(true));
    assertThat(getBlacklistedTablesDbs(), containsInAnyOrder("db3"));
  }

  @Test
  public void testParsingBlacklistedTablesInvalidNames() {
    // Test abnormal inputs
    setBlacklist("", "db3.,.bar,,");

    assertThat(getBlacklistedTablesCount(), equalTo(1));
    assertThat(isTableBlacklisted(Catalog.DEFAULT_DB, "bar"), is(true));
    assertThat(getBlacklistedTablesDbs(), containsInAnyOrder("default"));
  }

  @Test
  public void testParsingBlacklistedDbsAndTables() {
    setBlacklist("db1,db2", "db3.foo,db3.bar");

    assertThat(getBlacklistedDbsCount(), equalTo(2));
    assertThat(isDbBlacklisted("db1"), is(true));
    assertThat(isDbBlacklisted("db2"), is(true));
    assertThat(isDbBlacklisted("db3"), is(false));

    assertThat(getBlacklistedTablesCount(), equalTo(2));
    assertThat(isTableBlacklisted("db1", "foo"), is(false));
    assertThat(isTableBlacklisted("db2", "bar"), is(false));
    assertThat(isTableBlacklisted("db3", "foo"), is(true));
    assertThat(isTableBlacklisted("db3", "bar"), is(true));
    assertThat(isTableBlacklisted("db3", "baz"), is(false));
    assertThat(getBlacklistedTablesDbs(), containsInAnyOrder("db3"));
  }

  @Test
  public void testWorkloadManagementEnabled() {
    setBlacklist("sys", "", true);

    assertThat(isDbBlacklisted("sys"), is(false));
    assertThat(isTableBlacklisted("sys", "impala_query_log"), is(false));
    assertThat(isTableBlacklisted("sys", "impala_query_live"), is(false));
    assertThat(isTableBlacklisted("sys", "other_tbl"), is(true));
  }

  @Test
  public void testWorkloadManagementDisabled() {
    setBlacklist("sys", "", false);

    assertThat(isDbBlacklisted("sys"), is(true));
  }

  public static void setBlacklist(String blacklistedDbs, String blacklistedTables,
      boolean enableWorkloadMgmt) {
    TBackendGflags backendGflags = new TBackendGflags();
    backendGflags.setBlacklisted_dbs(blacklistedDbs);
    backendGflags.setBlacklisted_tables(blacklistedTables);
    backendGflags.setEnable_workload_mgmt(enableWorkloadMgmt);
    backendGflags.setQuery_log_table_name("impala_query_log");
    BackendConfig.create(backendGflags, false);
    reload();
  }

  public static void setBlacklist(String blacklistedDbs, String blacklistedTables) {
    setBlacklist(blacklistedDbs, blacklistedTables, false);
  }

}
