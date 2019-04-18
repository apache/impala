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

package org.apache.impala.common;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import java.io.StringReader;
import java.util.List;

import org.apache.impala.analysis.AnalysisContext;
import org.apache.impala.analysis.AnalysisContext.AnalysisResult;
import org.apache.impala.analysis.Analyzer;
import org.apache.impala.analysis.Expr;
import org.apache.impala.analysis.SelectStmt;
import org.apache.impala.analysis.SqlParser;
import org.apache.impala.analysis.SqlScanner;
import org.apache.impala.analysis.StatementBase;
import org.apache.impala.analysis.StmtMetadataLoader;
import org.apache.impala.analysis.StmtMetadataLoader.StmtTableCache;
import org.apache.impala.authorization.NoopAuthorizationFactory;
import org.apache.impala.testutil.TestUtils;
import org.apache.impala.thrift.TQueryCtx;
import org.apache.impala.thrift.TQueryOptions;
import org.apache.impala.util.EventSequence;

import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;

/**
 * Base class for per-query processing. This base class encapsulates all the inputs
 * to a query: the session, context, options, db and user, as well as the input
 * SQL. All inputs, except for the SQL, "inherit" from the session fixture, but can
 * be overriden here. For example, if most tests use the "functional" DB, set that
 * in the session fixture. But, if one particular test needs a different DB, you can
 * set that here.
 *
 * Provides the parse step. Use this class directory for parse-only tests.
 * Subclasses implement various kinds of analysis operations.
 */
public class QueryFixture {
  /**
   * Full query analysis, including rewrites. Use this for most tests. The
   * {@link #analyze()} method provides the decorated AST after analysis.
   * Use the available methods to access supporting objects (analysis
   * context, analysis result, analyzer) if needed for a test.
   */
  public static class AnalysisFixture extends QueryFixture {
    protected AnalysisContext analysisCtx_;
    protected StatementBase stmt_;
    private AnalysisResult analysisResult_;

    public AnalysisFixture(AnalysisSessionFixture analysisFixture, String stmtSql) {
      super(analysisFixture, stmtSql);
    }

    public StatementBase analyze() throws AnalysisException {
      Preconditions.checkState(analysisCtx_ == null, "Already analyzed");
      try {
        stmt_ = parse();
        analysisCtx_ = makeAnalysisContext();
        analysisResult_ = analysisCtx_.analyzeAndAuthorize(stmt_,
            makeTableCache(stmt_), session_.frontend().getAuthzChecker());
        Preconditions.checkNotNull(analysisResult_.getStmt());
        return stmt_;
      } catch (AnalysisException e) {
        // Tests may want to test analysis errors, else this exception will
        // fail the tests; no need to call fail() to accomplish that result.
        throw e;
      } catch (ImpalaException e) {
        // Should not occur during testing; indicates a setup error, so
        // fail with an unchecked exception.
        throw new IllegalStateException(e);
      }
    }

    /**
     * Build an analysis context using the query context created earlier.
     */
    protected AnalysisContext makeAnalysisContext() {
      EventSequence timeline = new EventSequence("Frontend Test Timeline");
      return new AnalysisContext(queryCtx_, new NoopAuthorizationFactory(), timeline);
    }

    /**
     * Create a table cache for the target database, loading tables
     * needed for the given statement.
     */
    protected StmtTableCache makeTableCache(StatementBase stmt) {
      StmtMetadataLoader mdLoader =
         new StmtMetadataLoader(session_.frontend(), db_, null);
      try {
        return mdLoader.loadTables(stmt);
      } catch (InternalException e) {
        fail(e.getMessage());
        // To keep the Java parser happy.
        throw new IllegalStateException(e);
      }
    }

    public StatementBase statement() { return stmt_; }
    public Analyzer analyzer() {
      Preconditions.checkState(analysisResult_ != null, "Not yet analyzed");
      return analysisResult_.getAnalyzer();
    }

    /**
     * Asserts that a warning is produced.
     */
    public void expectWarning(String expectedWarning) {
      List<String> actualWarnings = analyzer().getWarnings();
      boolean matchedWarning = false;
      for (String actualWarning: actualWarnings) {
        if (actualWarning.startsWith(expectedWarning)) {
          matchedWarning = true;
          break;
        }
      }
      if (!matchedWarning) {
        fail(String.format("Did not produce expected warning.\n" +
            "Expected warning:\n%s.\nActual warnings:\n%s",
            expectedWarning, Joiner.on("\n").join(actualWarnings)));
      }
    }
  }

  /**
   * Query fixture specialized for the most common statement: the SELECT
   * statement. Allows working with the statement as a whole, or with
   * specific parts of the statement. For example, when testing expressions, it is
   * common to provide just an expression which is placed into the select clause
   * using an given table: SELECT <expr> FROM <table>.
   *
   * Designed to be used in fluent style:
   *
   * Expr = new SelectFixture(sessionFixture)
   *   .table("functional.alltypestiny")
   *   .analyzeExpr("id + 3");
   *
   * The table is optional and defaults to the most common case:
   * functional.alltypes.
   */
  public static class SelectFixture extends AnalysisFixture {
    public String table_ = "functional.alltypes";
    public String exprSql_;

    public SelectFixture(AnalysisSessionFixture analysisFixture) {
      super(analysisFixture, null);
    }

    public SelectFixture table(String table) {
      table_ = table;
      return this;
    }

    public SelectFixture exprSql(String exprSql) {
      exprSql_ = exprSql;
      stmtSql_ = "select " + exprSql + " from " + table_;
      return this;
    }

    public SelectFixture whereSql(String exprSql) {
      exprSql_ = exprSql;
      stmtSql_ = "select count(1)  from " + table_ + " where " + exprSql_;
      return this;
    }

    public SelectStmt analyzeSelect() throws AnalysisException {
      analyze();
      return selectStmt();
    }

    public Expr analyzeExpr() throws AnalysisException {
      analyze();
      return selectExpr();
    }

    public SelectStmt selectStmt() {
      Preconditions.checkState(stmt_ != null, "Not yet analyzed");
      return (SelectStmt) stmt_;
    }

    /**
     * Return the parsed, analyzed expression resulting from a
     * {@link #select(String)} query.
     */
    public Expr selectExpr() {
      return selectStmt().getSelectList().getItems().get(0).getExpr();
    }

    public Expr whereExpr() {
      return selectStmt().getWhereClause();
    }

    /**
     * Verify that the {@link #select(String)} query produced the
     * expected result. Input is either null, meaning the expression
     * is unchanged, or a string that represents the toSql() form of
     * the rewritten SELECT expression.
     */
    public Expr verifySelect(String expectedExprStr) {
      Expr rewrittenExpr = selectExpr();
      String rewrittenSql = rewrittenExpr.toSql();
      assertEquals(expectedExprStr == null ? exprSql_ : expectedExprStr, rewrittenSql);
      return rewrittenExpr;
    }
  }

  protected final AnalysisSessionFixture session_;
  protected final TQueryCtx queryCtx_;
  protected final TQueryOptions queryOptions_;
  protected String stmtSql_;
  protected String db_;
  protected String user_;

  public QueryFixture(AnalysisSessionFixture session, String stmtSql) {
    session_ = session;
    stmtSql_ = stmtSql;
    queryCtx_ = session_.queryContext();
    queryOptions_ = session_.cloneOptions();
    db_ = session_.db();
    user_ = session_.user();
  }

  public void setDb(String db) { db_ = db; }
  public void setUser(String user) { user_ = user; }
  public TQueryCtx context() { return queryCtx_; }
  public String stmtSql() { return stmtSql_; }
  public TQueryOptions options() { return queryOptions_; }

  protected TQueryCtx queryContext() {
    return TestUtils.createQueryContext(db_, user_, queryOptions_);
  }

  public StatementBase parse() {
    // TODO: Use the parser class when available
    SqlScanner input = new SqlScanner(new StringReader(stmtSql_));
    SqlParser parser = new SqlParser(input);
    parser.setQueryOptions(queryOptions_);
    try {
      return (StatementBase) parser.parse().value;
    } catch (Exception e) {
      throw new IllegalStateException(e);
    }
  }
}