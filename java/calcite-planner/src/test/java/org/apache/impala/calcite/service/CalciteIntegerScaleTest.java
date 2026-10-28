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

import org.apache.impala.analysis.AnalysisContext;
import org.apache.impala.analysis.AnalysisContext.AnalysisResult;
import org.apache.impala.common.FrontendTestBase;
import org.apache.impala.planner.PlannerContext;
import org.apache.impala.planner.SingleNodePlannerIntf;
import org.apache.impala.service.CompilerFactory;
import org.apache.impala.service.CompilerFactoryImpl;
import org.apache.impala.thrift.TResultSetMetadata;
import org.apache.impala.util.EventSequence;
import org.junit.Test;

/** Numeric scale functions must preserve the native decimal result type rules. */
public class CalciteIntegerScaleTest extends FrontendTestBase {
  private static final String[] FUNCTIONS =
      {"trunc", "truncate", "dtrunc", "round", "dround"};

  private TResultSetMetadata metadata(String sql, boolean calcite)
      throws Exception {
    CompilerFactory factory = calcite
        ? new CalciteCompilerFactory() : new CompilerFactoryImpl();
    AnalysisContext ctx = feFixture_.createAnalysisCtx();
    ctx.getQueryCtx().client_request.query_options.setDecimal_v2(true);
    ctx.getQueryCtx().client_request.setStmt(sql);
    AnalysisResult result = parseAndAnalyze(sql, ctx, factory);
    SingleNodePlannerIntf planner = factory.createSingleNodePlanner(new PlannerContext(
        result, ctx.getQueryCtx(), new EventSequence("Integer scale parity")));
    planner.createSingleNodePlan();
    return planner.getTResultSetMetadata(result.getParsedStmt());
  }

  private void parity(String sql) throws Exception {
    assertEquals(sql, metadata(sql, false), metadata(sql, true));
  }

  @Test
  public void oneArgument() throws Exception {
    for (String name : FUNCTIONS) {
      for (String type : new String[] {"tinyint", "smallint", "int", "bigint"}) {
        parity("select " + name + "(cast(123 as " + type + ")) value");
      }
    }
  }

  @Test
  public void explicitScale() throws Exception {
    for (String name : FUNCTIONS) {
      for (String type : new String[] {"tinyint", "smallint", "int", "bigint"}) {
        for (int scale : new int[] {0, 2, -1}) {
          parity("select " + name + "(cast(123 as " + type + ")," + scale + ") value");
        }
      }
    }
  }

  @Test
  public void columns() throws Exception {
    for (String name : FUNCTIONS) {
      for (String type : new String[] {"tinyint", "smallint", "int", "bigint"}) {
        for (String scale : new String[] {"", ",0", ",2", ",-1"}) {
          parity("select " + name + "(" + type + "_col" + scale
              + ") value from functional.alltypes");
        }
      }
    }
  }

  @Test
  public void nullAndBoundaryValues() throws Exception {
    for (String name : FUNCTIONS) {
      for (String type : new String[] {"tinyint", "smallint", "int", "bigint"}) {
        for (String value : new String[] {"NULL", "0", "-123"}) {
          for (String scale : new String[] {"", ",2", ",-1"}) {
            parity("select " + name + "(cast(" + value + " as " + type + ")"
                + scale + ") value");
          }
        }
      }
    }
  }
}
