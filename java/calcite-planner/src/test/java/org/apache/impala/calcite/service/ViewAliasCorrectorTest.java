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

package org.apache.impala.calcite.service;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.Collections;
import java.util.Locale;

import org.apache.calcite.sql.SqlBasicCall;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlSelect;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.impala.catalog.Column;
import org.apache.impala.catalog.Type;
import org.apache.impala.common.AnalysisException;
import org.apache.impala.common.FrontendTestBase;
import org.junit.Before;
import org.junit.Test;

public class ViewAliasCorrectorTest extends FrontendTestBase {
  private static final String TEST_DB = "view_alias_corrector_test";
  private static final String TEST_TABLE = TEST_DB + ".t";

  @Before
  public void addSyntheticTable() {
    addTestDb(TEST_DB, "Synthetic view validation tests");
    addTestTable("create table " + TEST_TABLE
        + " (flag boolean, ts timestamp) stored as parquet");
  }

  @Test
  public void testUnaliasedItemsAreNotAliasIssues() throws Exception {
    for (String item : new String[] {"q.n", "n", "1", "sum(q.n)"}) {
      SqlSelect select = parseSelect("select " + item
          + " from (select sum(x) as n from t) q");
      assertTrue(gatherAliases(select).getItemsWithAliasIssue().isEmpty());
    }
  }

  @Test
  public void testAliasedExpressionsAreNotAliasIssues() throws Exception {
    for (String item : new String[] {"q.n + 1 as n", "sum(q.n) as n",
        "1 as n", "n as n"}) {
      SqlSelect select = parseSelect("select " + item
          + " from (select sum(x) as n from t) q");
      assertTrue(gatherAliases(select).getItemsWithAliasIssue().isEmpty());
    }
  }

  @Test
  public void testHiveGeneratedColumnAliasIsCorrected() throws Exception {
    SqlSelect select = parseSelect("select q.\"_c0\" as n"
        + " from (select sum(x) from t) q");
    ViewAliasCorrector.ViewGatherAliases gathered = gatherAliases(select);
    assertEquals(Collections.singleton(0), gathered.getItemsWithAliasIssue());
    ViewAliasCorrector.ViewAttemptAliasCorrection correction =
        new ViewAliasCorrector.ViewAttemptAliasCorrection(gathered);
    correction.enterSelect(select);
    SqlIdentifier identifier = (SqlIdentifier) ((SqlBasicCall)
        select.getSelectList().get(0)).operand(0);
    assertEquals("EXPR$0", identifier.names.get(1));
    SqlSelect inner = innerSelect(select);
    correction.enterSelect(inner);
    SqlBasicCall corrected = (SqlBasicCall)
        correction.processSelectItem(inner.getSelectList().get(0));
    assertEquals("EXPR$0", ((SqlIdentifier) corrected.operand(1)).getSimple());
  }

  @Test
  public void testMixedProjectionPreservesUnchangedItems() throws Exception {
    SqlSelect select = parseSelect("select q.\"_c0\" as n, q.m"
        + " from (select sum(x), y as m from t) q");
    SqlNode unchanged = select.getSelectList().get(1);
    ViewAliasCorrector.ViewGatherAliases gathered = gatherAliases(select);
    assertEquals(Collections.singleton(0), gathered.getItemsWithAliasIssue());
    ViewAliasCorrector.ViewAttemptAliasCorrection correction =
        new ViewAliasCorrector.ViewAttemptAliasCorrection(gathered);
    correction.enterSelect(select);
    assertSame(unchanged, select.getSelectList().get(1));
    assertEquals("EXPR$0", ((SqlIdentifier) ((SqlBasicCall)
        select.getSelectList().get(0)).operand(0)).names.get(1));
  }

  @Test
  public void testSumBooleanViewPreservesValidationError() throws Exception {
    assertViewValidationError("sum(flag)", "sum(cast(flag as int))",
        "SUM", Type.BIGINT);
  }

  private void assertViewValidationError(String expression, String control,
      String function, Type type) throws Exception {
    String view = TEST_DB + ".invalid_view";
    String controlView = TEST_DB + ".control_view";
    addTestView("create view " + view + " as select q.n from (select "
        + expression + " as n from " + TEST_TABLE + ") q")
        .addColumn(new Column("n", type, 0));
    addTestView("create view " + controlView + " as select q.n from (select "
        + control + " as n from " + TEST_TABLE + ") q")
        .addColumn(new Column("n", type, 0));
    parseAndAnalyze("select n from " + view, feFixture_.createAnalysisCtx());
    assertFunctionValidationError("select " + expression + " from " + TEST_TABLE,
        function);
    assertFunctionValidationError("select n from " + view, function);
    parseAndAnalyze("select n from " + controlView, feFixture_.createAnalysisCtx(),
        new CalciteCompilerFactory());
  }

  private void assertFunctionValidationError(String sql, String function)
      throws Exception {
    try {
      parseAndAnalyze(sql, feFixture_.createAnalysisCtx(),
          new CalciteCompilerFactory());
      fail("Expected the Calcite function validation error for " + function);
    } catch (AnalysisException e) {
      assertTrue(e.getMessage(),
          e.getMessage().toUpperCase(Locale.ROOT).contains(function));
      for (Throwable cause = e; cause != null; cause = cause.getCause()) {
        assertTrue("Alias correction must preserve the validation failure",
            !(cause instanceof ClassCastException));
      }
    }
  }

  private static SqlSelect parseSelect(String sql) throws Exception {
    return (SqlSelect) SqlParser.create(sql).parseQuery();
  }

  private static SqlSelect innerSelect(SqlSelect select) {
    return (SqlSelect) ((SqlBasicCall) select.getFrom()).operand(0);
  }

  private static ViewAliasCorrector.ViewGatherAliases gatherAliases(SqlSelect select) {
    ViewAliasCorrector.ViewGatherAliases phase =
        new ViewAliasCorrector.ViewGatherAliases();
    phase.enterSelect(select);
    phase.enterSelect(innerSelect(select));
    phase.exitSelect();
    phase.exitSelect();
    phase.validateFinished();
    return phase;
  }
}
