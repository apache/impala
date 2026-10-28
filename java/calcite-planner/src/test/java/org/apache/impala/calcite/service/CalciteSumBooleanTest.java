/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.impala.calcite.service;

import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.impala.analysis.AnalysisContext;
import org.apache.impala.common.FrontendTestBase;
import org.apache.impala.common.ImpalaException;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

public class CalciteSumBooleanTest extends FrontendTestBase {
  private void assertType(String expression, SqlTypeName expected)
      throws ImpalaException {
    assertQueryType("select " + expression + " from (select 1 as x) t", expected);
  }

  private void assertQueryType(String sql, SqlTypeName expected) throws ImpalaException {
    AnalysisContext ctx = feFixture_.createAnalysisCtx();
    ctx.getQueryCtx().client_request.query_options.setDecimal_v2(true);
    ctx.getQueryCtx().client_request.query_options.setUse_hbo_stats(false);
    ctx.getQueryCtx().client_request.query_options.setStore_hbo_stats(false);
    CalciteAnalysisResult result = (CalciteAnalysisResult)
        parseAndAnalyze(sql, ctx, new CalciteCompilerFactory());
    assertEquals(expected, result.getSqlValidator()
        .getValidatedNodeType(result.getValidatedNode())
        .getFieldList().get(0).getType().getSqlTypeName());
  }

  @Test
  public void testTrue() throws ImpalaException {
    assertType("sum(true)", SqlTypeName.BIGINT);
  }

  @Test
  public void testFalse() throws ImpalaException {
    assertType("sum(false)", SqlTypeName.BIGINT);
  }

  @Test
  public void testNull() throws ImpalaException {
    assertType("sum(cast(null as boolean))", SqlTypeName.BIGINT);
  }

  @Test
  public void testComparison() throws ImpalaException {
    assertType("sum(1 = 1)", SqlTypeName.BIGINT);
  }

  @Test
  public void testCase() throws ImpalaException {
    assertType("sum(case when 1 = 1 then true else false end)", SqlTypeName.BIGINT);
  }

  @Test
  public void testDistinct() throws ImpalaException {
    assertType("sum(distinct true)", SqlTypeName.BIGINT);
  }

  @Test
  public void testAnalytic() throws ImpalaException {
    assertType("sum(true) over ()", SqlTypeName.BIGINT);
  }

  @Test
  public void testExplicitCast() throws ImpalaException {
    assertType("sum(cast(true as bigint))", SqlTypeName.BIGINT);
  }

  @Test
  public void testInteger() throws ImpalaException {
    assertType("sum(1)", SqlTypeName.BIGINT);
  }

  @Test
  public void testDouble() throws ImpalaException {
    assertType("sum(cast(1 as double))", SqlTypeName.DOUBLE);
  }

  @Test
  public void testDecimal() throws ImpalaException {
    assertType("sum(cast(1 as decimal(8,2)))", SqlTypeName.DECIMAL);
  }

  @Test
  public void testCount() throws ImpalaException {
    assertType("count(true)", SqlTypeName.BIGINT);
  }

  @Test
  public void testNullableColumn() throws ImpalaException {
    assertQueryType("select sum(b) from (select true as b union all "
        + "select cast(null as boolean)) t", SqlTypeName.BIGINT);
  }

  @Test
  public void testInvalidArity() throws ImpalaException {
    try {
      assertType("sum(true, false)", SqlTypeName.BIGINT);
      fail("SUM must still require one argument");
    } catch (org.apache.impala.common.AnalysisException expected) {
      // Argument coercion must not accept an invalid SUM signature.
    }
  }
}
