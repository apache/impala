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

package org.apache.impala.calcite.planner;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.calcite.jdbc.JavaTypeFactoryImpl;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.metadata.DefaultRelMetadataProvider;
import org.apache.calcite.rel.metadata.JaninoRelMetadataProvider;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexExecutor;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlNode;
import org.apache.impala.analysis.Analyzer;
import org.apache.impala.analysis.Expr;
import org.apache.impala.calcite.operators.ImpalaOperatorTable;
import org.apache.impala.calcite.rules.ImpalaRexExecutor;
import org.apache.impala.calcite.service.CalciteAnalysisResult;
import org.apache.impala.calcite.service.CalciteCompilerFactory;
import org.apache.impala.calcite.service.CalciteRelNodeConverter;
import org.apache.impala.calcite.type.ImpalaTypeSystemImpl;
import org.apache.impala.catalog.BuiltinsDb;
import org.apache.impala.common.ImpalaException;
import org.apache.impala.common.RuntimeEnv;
import org.apache.impala.planner.PlannerTestBase;
import org.apache.impala.thrift.TQueryCtx;
import org.apache.impala.thrift.TQueryOptions;
import org.apache.impala.thrift.TSessionState;
import org.apache.impala.thrift.TColumnValue;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;

import static org.junit.Assert.*;

public class TestReduceExprShuttle extends PlannerTestBase {

  // Query option to run each planner test.
  private static TQueryOptions options =
      tpcdsParquetQueryOptions();

  @BeforeClass
  public static void setUpClass() throws Exception {
    RuntimeEnv.INSTANCE.setTestEnv(true);
    ImpalaOperatorTable.create(BuiltinsDb.getInstance());
    RelMetadataQuery.THREAD_PROVIDERS.set(
        JaninoRelMetadataProvider.of(DefaultRelMetadataProvider.INSTANCE));
  }

  @AfterClass
  public static void cleanUpClass() {
    RuntimeEnv.INSTANCE.reset();
  }

  @Test
  public void testFoldAddTinyInt() {
    try {
      ReduceShuttleObjects queryObj = createReduceShuttleObjects("SELECT 1 + 1");
      TColumnValue reducedValue = new TColumnValue();
      reducedValue.setShort_val((short)2);

      TestReducerTmp testReducer = new TestReducerTmp("add(1, 1)", reducedValue);
      List<RexNode> reducedExprs = new ArrayList<>();
      RexExecutor executor = new ImpalaRexExecutor(
          queryObj.analyzer_, queryObj.queryCtx_, testReducer);
      executor.reduce(queryObj.rexBuilder_, queryObj.project_.getProjects(),
          reducedExprs);

      assertEquals(1, reducedExprs.size(), 1);
      assertEquals("2:SMALLINT", reducedExprs.get(0).toString());
    } catch (ImpalaException e) {
      throw new RuntimeException(e);
    }
  }

  @Test
  public void testFoldAddSmallInt() {
    try {
      String expr = "CAST(1 AS SMALLINT) + CAST(2 AS SMALLINT)";
      ReduceShuttleObjects queryObj =
          createReduceShuttleObjects("SELECT " + expr);
      TColumnValue reducedCast1 = new TColumnValue();
      reducedCast1.setShort_val((short)1);
      TColumnValue reducedCast2 = new TColumnValue();
      reducedCast2.setShort_val((short)2);
      TColumnValue reducedAdd = new TColumnValue();
      reducedAdd.setInt_val((int)3);

      Map<String, TColumnValue> map = ImmutableMap.of
          ("1", reducedCast1,
          "2", reducedCast2,
          "add(1, 2)", reducedAdd);

      TestReducerTmp testReducer = new TestReducerTmp(map);
      List<RexNode> reducedExprs = new ArrayList<>();
      RexExecutor executor = new ImpalaRexExecutor(
          queryObj.analyzer_, queryObj.queryCtx_, testReducer);
      executor.reduce(queryObj.rexBuilder_, queryObj.project_.getProjects(),
          reducedExprs);


      assertEquals(1, reducedExprs.size(), 1);
      assertEquals("CAST(3):INTEGER", reducedExprs.get(0).toString());
    } catch (ImpalaException e) {
      throw new RuntimeException(e);
    }
  }

  @Test
  public void testFoldAddInt() {
    try {
      String expr = "CAST(1 AS INT) + CAST(2 AS INT)";
      ReduceShuttleObjects queryObj = createReduceShuttleObjects("SELECT " + expr);
      TColumnValue reducedCast1 = new TColumnValue();
      reducedCast1.setInt_val(1);
      TColumnValue reducedCast2 = new TColumnValue();
      reducedCast2.setInt_val(2);
      TColumnValue reducedAdd = new TColumnValue();
      reducedAdd.setLong_val(3);

      Map<String, TColumnValue> map = ImmutableMap.of
          ("1", reducedCast1,
          "2", reducedCast2,
          "add(1, 2)", reducedAdd);

      TestReducerTmp testReducer = new TestReducerTmp(map);
      List<RexNode> reducedExprs = new ArrayList<>();
      RexExecutor executor = new ImpalaRexExecutor(
          queryObj.analyzer_, queryObj.queryCtx_, testReducer);
      executor.reduce(queryObj.rexBuilder_, queryObj.project_.getProjects(),
          reducedExprs);

      assertEquals(1, reducedExprs.size(), 1);
      assertEquals("CAST(3:BIGINT):BIGINT", reducedExprs.get(0).toString());
    } catch (ImpalaException e) {
      throw new RuntimeException(e);
    }
  }

  @Test
  public void testFoldAddDecimal() {
    try {
      String expr = "1.1 + 2.2";
      ReduceShuttleObjects queryObj = createReduceShuttleObjects("SELECT " + expr);
      TColumnValue reducedCast1 = new TColumnValue();
      reducedCast1.setString_val("3.3");

      Map<String, TColumnValue> map = ImmutableMap.of
          ("add(1.1, 2.2)", reducedCast1);

      TestReducerTmp testReducer = new TestReducerTmp(map);
      List<RexNode> reducedExprs = new ArrayList<>();
      RexExecutor executor = new ImpalaRexExecutor(
          queryObj.analyzer_, queryObj.queryCtx_, testReducer);
      executor.reduce(queryObj.rexBuilder_, queryObj.project_.getProjects(),
          reducedExprs);

      assertEquals(1, reducedExprs.size(), 1);
      assertEquals("3.3:DECIMAL(3, 1)", reducedExprs.get(0).toString());
    } catch (ImpalaException e) {
      throw new RuntimeException(e);
    }
  }

  @Test
  public void testFoldConcatString() {
    try {
      String expr = "CONCAT(cast('a' as string), cast('b' as string))";
      ReduceShuttleObjects queryObj = createReduceShuttleObjects("SELECT " + expr);
      TColumnValue reducedCast1 = new TColumnValue();
      reducedCast1.setBinary_val("a".getBytes());
      TColumnValue reducedCast2 = new TColumnValue();
      reducedCast2.setBinary_val("b".getBytes());
      TColumnValue reducedAdd = new TColumnValue();
      reducedAdd.setBinary_val("ab".getBytes());

      Map<String, TColumnValue> map = ImmutableMap.of
          ("'a'", reducedCast1,
          "'b'", reducedCast2,
          "concat('a', 'b')", reducedAdd);

      TestReducerTmp testReducer = new TestReducerTmp(map);
      List<RexNode> reducedExprs = new ArrayList<>();
      RexExecutor executor = new ImpalaRexExecutor(
          queryObj.analyzer_, queryObj.queryCtx_, testReducer);
      executor.reduce(queryObj.rexBuilder_, queryObj.project_.getProjects(),
          reducedExprs);

      assertEquals(1, reducedExprs.size(), 1);
      assertEquals("CAST(_UTF-8'ab':VARCHAR(2147483647) CHARACTER SET \"UTF-8\"):" +
          "VARCHAR(2147483647) CHARACTER SET \"UTF-8\"", reducedExprs.get(0).toString());
    } catch (ImpalaException e) {
      throw new RuntimeException(e);
    }
  }

  @Test
  public void testBoolean() {
    try {
      String expr = "istrue(false)";
      ReduceShuttleObjects queryObj = createReduceShuttleObjects("SELECT " + expr);
      TColumnValue reducedValue = new TColumnValue();
      reducedValue.setBool_val(false);

      TestReducerTmp testReducer = new TestReducerTmp("istrue(FALSE)", reducedValue);
      List<RexNode> reducedExprs = new ArrayList<>();
      RexExecutor executor = new ImpalaRexExecutor(
          queryObj.analyzer_, queryObj.queryCtx_, testReducer);
      executor.reduce(queryObj.rexBuilder_, queryObj.project_.getProjects(),
          reducedExprs);

      assertEquals(1, reducedExprs.size(), 1);
      assertEquals("CAST(false):BOOLEAN", reducedExprs.get(0).toString());
    } catch (ImpalaException e) {
      throw new RuntimeException(e);
    }
  }

  @Test
  public void testPartialExpr() {
    try {
      ReduceShuttleObjects queryObj = createReduceShuttleObjects(
          "SELECT 1 + 1 + tinyint_col from functional.alltypestiny");
      TColumnValue reducedValue = new TColumnValue();
      reducedValue.setShort_val((short)2);

      TestReducerTmp testReducer = new TestReducerTmp("add(1, 1)", reducedValue);
      List<RexNode> reducedExprs = new ArrayList<>();
      RexExecutor executor = new ImpalaRexExecutor(
          queryObj.analyzer_, queryObj.queryCtx_, testReducer);
      executor.reduce(queryObj.rexBuilder_, queryObj.project_.getProjects(),
          reducedExprs);

      assertEquals(1, reducedExprs.size(), 1);
      assertEquals("+(2:SMALLINT, $2)", reducedExprs.get(0).toString());
    } catch (ImpalaException e) {
      throw new RuntimeException(e);
    }
  }

  @Test
  public void testNonDeterministic() {
    try {
      String expr = "rand()";
      ReduceShuttleObjects queryObj = createReduceShuttleObjects("SELECT " + expr);
      TestReducerTmp testReducer = new TestReducerTmp();
      List<RexNode> reducedExprs = new ArrayList<>();
      RexExecutor executor = new ImpalaRexExecutor(
          queryObj.analyzer_, queryObj.queryCtx_, testReducer);
      executor.reduce(queryObj.rexBuilder_, queryObj.project_.getProjects(),
          reducedExprs);

      assertEquals(1, reducedExprs.size(), 1);
      assertEquals("RAND()", reducedExprs.get(0).toString());
    } catch (ImpalaException e) {
      throw new RuntimeException(e);
    }
  }

  @Test
  public void testFoldMultipleFields() {
    try {
      String expr = "1 + 2, 3 + 4";
      ReduceShuttleObjects queryObj = createReduceShuttleObjects("SELECT " + expr);
      TColumnValue reducedAdd1 = new TColumnValue();
      reducedAdd1.setShort_val((short)3);
      TColumnValue reducedAdd2 = new TColumnValue();
      reducedAdd2.setShort_val((short)7);

      Map<String, TColumnValue> map = ImmutableMap.of
          ("add(1, 2)", reducedAdd1,
          "add(3, 4)", reducedAdd2);

      TestReducerTmp testReducer = new TestReducerTmp(map);
      List<RexNode> reducedExprs = new ArrayList<>();
      RexExecutor executor = new ImpalaRexExecutor(
          queryObj.analyzer_, queryObj.queryCtx_, testReducer);
      executor.reduce(queryObj.rexBuilder_, queryObj.project_.getProjects(),
          reducedExprs);

      assertEquals(2, reducedExprs.size());
      assertEquals("3:SMALLINT", reducedExprs.get(0).toString());
      assertEquals("7:SMALLINT", reducedExprs.get(1).toString());
    } catch (ImpalaException e) {
      throw new RuntimeException(e);
    }
  }

  @Test
  public void testFoldTimestamp() {
    try {
      String expr = "add_months(cast('2012-07-01 00:00:00' as timestamp), " +
          "cast(2 as integer))";
      ReduceShuttleObjects queryObj = createReduceShuttleObjects("SELECT " + expr);
      TColumnValue reducedTime1 = new TColumnValue();
      reducedTime1.setString_val("2012-07-01 00:00:00");
      TColumnValue reducedTime2 = new TColumnValue();
      reducedTime2.setString_val("2012-09-01 00:00:00");
      TColumnValue reducedInt = new TColumnValue();
      reducedInt.setInt_val(2);

      Map<String, TColumnValue> map = ImmutableMap.of
          ("'2012-07-01 00:00:00'", reducedTime1,
          "add_months(casttotimestamp('2012-07-01 00:00:00'), 2)", reducedTime2,
          "2", reducedInt);

      TestReducerTmp testReducer = new TestReducerTmp(map);
      List<RexNode> reducedExprs = new ArrayList<>();
      RexExecutor executor = new ImpalaRexExecutor(
          queryObj.analyzer_, queryObj.queryCtx_, testReducer);
      executor.reduce(queryObj.rexBuilder_, queryObj.project_.getProjects(),
          reducedExprs);

      assertEquals(1, reducedExprs.size());
      assertEquals("CAST(2012-09-01 00:00:00:TIMESTAMP(15)):TIMESTAMP(15)",
          reducedExprs.get(0).toString());
    } catch (ImpalaException e) {
      throw new RuntimeException(e);
    }

  }

  private ReduceShuttleObjects createReduceShuttleObjects(String query)
        throws ImpalaException {
    CalciteAnalysisResult analysisResult =
        (CalciteAnalysisResult) parseAndAnalyze(query,
        feFixture_.createAnalysisCtx(), new CalciteCompilerFactory());
    Analyzer analyzer = analysisResult.getAnalyzer();
    TQueryCtx queryCtx = analyzer.getQueryCtx();
    CalciteRelNodeConverter relNodeConverter =
        new CalciteRelNodeConverter(analysisResult);
    RelNode rootNode = relNodeConverter.convert(analysisResult.getValidatedNode());

    Preconditions.checkState(rootNode instanceof Project);
    Project project = (Project) rootNode;

    RelDataTypeFactory typeFactory =
        new JavaTypeFactoryImpl(new ImpalaTypeSystemImpl());
    RexBuilder rexBuilder = new RexBuilder(typeFactory);
    return new ReduceShuttleObjects(analyzer, queryCtx, project, rexBuilder);
  }
  private static class ReduceShuttleObjects {
    public final Analyzer analyzer_;
    public final TQueryCtx queryCtx_;
    public final Project project_;
    public final RexBuilder rexBuilder_;

    public ReduceShuttleObjects(Analyzer analyzer, TQueryCtx queryCtx, Project project,
        RexBuilder rexBuilder) {
      analyzer_ = analyzer;
      queryCtx_ = queryCtx;
      project_ = project;
      rexBuilder_ = rexBuilder;;
    }
  }

  private static class TestReducerTmp implements ImpalaRexExecutor.Reducer {
    private Map<String, TColumnValue> valueMap_ = new HashMap<>();

    public TestReducerTmp() {
    }

    public TestReducerTmp(String expr, TColumnValue reducedValue) {
      valueMap_.put(expr, reducedValue);
    }

    public TestReducerTmp(Map<String, TColumnValue> valueMap) {
      valueMap_.putAll(valueMap);
    }

    @Override
    public TColumnValue reduce(Expr expr, TQueryCtx queryCtx) throws ImpalaException {
      assertTrue(valueMap_.containsKey(expr.toSql()));
      return valueMap_.get(expr.toSql());
    }
  }
}

