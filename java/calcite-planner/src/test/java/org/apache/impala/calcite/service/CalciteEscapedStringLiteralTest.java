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

import com.google.common.collect.ImmutableList;

import org.apache.calcite.plan.RelOptUtil;
import org.apache.impala.analysis.AnalysisContext.AnalysisResult;
import org.apache.impala.analysis.Expr;
import org.apache.impala.analysis.SelectStmt;
import org.apache.impala.calcite.rel.node.ImpalaPlanRel;
import org.apache.impala.common.FrontendTestBase;
import org.apache.impala.common.ImpalaException;
import org.apache.impala.service.FeSupport;
import org.apache.impala.thrift.TQueryOptions;
import org.apache.impala.util.NoOpEventSequence;
import org.junit.Test;

import java.util.List;

/**
 * IMPALA-15774: string literals have to be compared after Impala decodes
 * their SQL escapes. IMPALA-15753 is that same decoding for a LIKE pattern,
 * so the folded LIKE results must stay the same as the original planner.
 */
public class CalciteEscapedStringLiteralTest extends FrontendTestBase {

  private static final String SAME_STRING =
      "select 'a\\q' = 'aq', '\\061' = '1', \"it's\" = 'it\\'s', "
      + "'a\\q' < 'aq', 'a\\q' in ('aq', 'x')";

  // Matches the LIKE cases in QueryTest/calcite.test.
  private static final String LIKE_PATTERNS =
      "select 'a\\\\' like 'a\\\\%', 'a\\\\' like 'a\\\\%%', "
      + "'a\\%' like 'a\\\\%', 'a\\%' like 'a\\\\%%', "
      + "'a%' like 'a\\\\%', 'a%' like 'a\\\\%%'";

  @Test
  public void escapedSpellingsOfOneStringFoldTogether() throws ImpalaException {
    assertFoldsLikeOriginal(SAME_STRING);
  }

  @Test
  public void likePatternsFoldLikeTheOriginalPlanner() throws ImpalaException {
    assertFoldsLikeOriginal(LIKE_PATTERNS);
    assertFoldsLikeOriginal(LIKE_PATTERNS.replace(" like ", " not like "));
  }

  private void assertFoldsLikeOriginal(String sql) throws ImpalaException {
    assertEquals(originalBooleans(sql), calciteBooleans(sql));
  }

  private List<Boolean> originalBooleans(String sql) throws ImpalaException {
    AnalysisResult original = parseAndAnalyze(sql, createAnalysisCtx());
    ImmutableList.Builder<Boolean> values = ImmutableList.builder();
    for (Expr expr : ((SelectStmt) original.getStmt()).getResultExprs()) {
      values.add(FeSupport.EvalExprWithoutRow(expr,
          original.getAnalyzer().getQueryCtx()).bool_val);
    }
    return values.build();
  }

  private List<Boolean> calciteBooleans(String sql) throws ImpalaException {
    String plan = RelOptUtil.toString(rel(sql));
    int start = plan.indexOf("{ ");
    int end = plan.indexOf(" }");
    ImmutableList.Builder<Boolean> values = ImmutableList.builder();
    for (String token : plan.substring(start + 2, end).split(", ")) {
      values.add(Boolean.valueOf(token));
    }
    return values.build();
  }

  private ImpalaPlanRel rel(String sql) throws ImpalaException {
    CalciteAnalysisResult analysisResult = (CalciteAnalysisResult) parseAndAnalyze(
        sql, createAnalysisCtx(), new CalciteCompilerFactory());
    CalciteRelNodeConverter relNodeConverter =
        new CalciteRelNodeConverter(analysisResult);
    return new CalciteOptimizer(analysisResult,
        NoOpEventSequence.INSTANCE, new TQueryOptions())
        .optimize(relNodeConverter.convert(analysisResult.getValidatedNode()));
  }
}
