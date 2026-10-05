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
import static org.junit.Assert.fail;

import org.apache.impala.analysis.AnalysisContext;
import org.apache.impala.analysis.AnalysisContext.AnalysisResult;
import org.apache.impala.catalog.Column;
import org.apache.impala.catalog.Type;
import org.apache.impala.common.AnalysisException;
import org.apache.impala.common.FrontendTestBase;
import org.apache.impala.planner.PlannerContext;
import org.apache.impala.planner.SingleNodePlannerIntf;
import org.apache.impala.service.CompilerFactory;
import org.apache.impala.service.CompilerFactoryImpl;
import org.apache.impala.thrift.TResultSetMetadata;
import org.apache.impala.util.EventSequence;
import org.junit.Test;

/** Temporal TRUNC must resolve native overloads without changing numeric calls. */
public class CalciteTruncTest extends FrontendTestBase {
  private TResultSetMetadata metadata(String sql, boolean calcite) throws Exception {
    CompilerFactory factory = calcite
        ? new CalciteCompilerFactory() : new CompilerFactoryImpl();
    AnalysisContext ctx = feFixture_.createAnalysisCtx();
    ctx.getQueryCtx().client_request.setStmt(sql);
    AnalysisResult result = parseAndAnalyze(sql, ctx, factory);
    SingleNodePlannerIntf planner = factory.createSingleNodePlanner(new PlannerContext(
        result, ctx.getQueryCtx(), new EventSequence("TRUNC parity")));
    planner.createSingleNodePlan();
    return planner.getTResultSetMetadata(result.getParsedStmt());
  }

  private void assertParity(String expression) throws Exception {
    String sql = "select " + expression + " as value";
    assertEquals(sql, metadata(sql, false), metadata(sql, true));
  }

  @Test
  public void testTimestampUnits() throws Exception {
    for (String unit : new String[] {"DD", "MM", "YEAR", "Q", "DAY", "HH", "MI"}) {
      assertParity("trunc(cast('2026-01-15 12:34:56' as timestamp),'" + unit + "')");
    }
  }

  @Test
  public void testDateUnits() throws Exception {
    for (String unit : new String[] {"DD", "MM", "YEAR", "Q", "DAY"}) {
      assertParity("trunc(cast('2026-01-15' as date),'" + unit + "')");
    }
  }

  @Test
  public void testNullAndStringOperands() throws Exception {
    assertParity("trunc(cast(null as timestamp),'DD')");
    assertParity("trunc(cast(null as date),'MM')");
    assertParity("trunc(null,'DD')");
    assertParity("trunc(null,null)");
    assertParity("trunc(cast('2026-01-15' as date),null)");
    assertParity("trunc('2014-11-11','DD')");
    assertEquals(metadata("select trunc('2014-11-11','DD')", false),
        metadata("select trunc('2014-11-11','DD')", true));
  }

  @Test
  public void testNumericAliases() throws Exception {
    for (String name : new String[] {"trunc", "truncate", "dtrunc", "round", "dround"}) {
      String decimalType = name.equals("round") || name.equals("dround")
          ? "decimal(38,3)" : "decimal(15,3)";
      assertParity(name + "(cast(1.234 as " + decimalType + "),3)");
      assertParity(name + "(cast(1.234 as double))");
    }
  }

  @Test
  public void testColumnsWithViewAndDynamicUnit() throws Exception {
    addTestDb("trunc_test", "Synthetic TRUNC metadata");
    addTestTable("create table trunc_test.t (ts timestamp, d date, unit string) "
        + "stored as parquet location '/trunc-test/t'");
    addTestView("create view trunc_test.v as select trunc(ts,'DD') value "
        + "from trunc_test.t").addColumn(new Column("value", Type.TIMESTAMP, 0));
    for (String sql : new String[] {
        "select trunc(ts,'DD') value, trunc(d,'MM') d from trunc_test.t",
        "with t as (select * from trunc_test.t) select trunc(ts,'DAY') value from t",
        "select value from trunc_test.v",
        "select trunc(ts,unit) value from trunc_test.t",
        "select dayofyear(trunc('2014-11-11',unit)) value from trunc_test.t limit 1"}) {
      assertEquals(sql, metadata(sql, false), metadata(sql, true));
    }
  }

  @Test
  public void testNestedTimestampView() throws Exception {
    addTestDb("trunc_nested_test", "Synthetic nested TRUNC view");
    addTestTable("create table trunc_nested_test.t (ts timestamp) "
        + "stored as parquet location '/trunc-nested-test/t'");
    addTestView("create view trunc_nested_test.v as select q.value from "
        + "(select trunc(ts,'DD') value from trunc_nested_test.t) q")
        .addColumn(new Column("value", Type.TIMESTAMP, 0));
    String sql = "select value from trunc_nested_test.v";
    assertEquals(sql, metadata(sql, false), metadata(sql, true));
  }

  @Test
  public void testWrongArityAndUnitType() throws Exception {
    for (String sql : new String[] {
        "select trunc(cast('2026-01-15' as timestamp))",
        "select trunc(cast('2026-01-15' as timestamp),1)",
        "select trunc(cast('2026-01-15' as date),1)",
        "select truncate(cast('2026-01-15' as timestamp),'DD')",
        "select dtrunc(cast('2026-01-15' as date),'DD')"}) {
      for (boolean calcite : new boolean[] {false, true}) {
        try {
          metadata(sql, calcite);
          fail("invalid TRUNC overload must fail analysis: " + sql);
        } catch (AnalysisException expected) {
          // Temporal aliases and numeric unit arguments are not native overloads.
        }
      }
    }
  }
}
