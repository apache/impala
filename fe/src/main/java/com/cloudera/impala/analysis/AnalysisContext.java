// Copyright (c) 2011 Cloudera, Inc. All rights reserved.

package com.cloudera.impala.analysis;

import java.io.StringReader;

import com.cloudera.impala.catalog.Catalog;
import com.cloudera.impala.common.AnalysisException;

/**
 * Wrapper class for parser and analyzer.
 *
 */
public class AnalysisContext {
  private final Catalog catalog;

  public AnalysisContext(Catalog catalog) {
    this.catalog = catalog;
  }

  static public class AnalysisResult {
    // SelectStmt, InsertStmt or UnionStmt.
    private ParseNode stmt;
    private Analyzer analyzer;

    public boolean isQueryStmt() {
      return stmt instanceof QueryStmt;
    }

    public boolean isInsertStmt() {
      return stmt instanceof InsertStmt;
    }

    public boolean isUseStmt() {
      return stmt instanceof UseStmt;
    }

    public QueryStmt getQueryStmt() {
      return (QueryStmt) stmt;
    }

    public InsertStmt getInsertStmt() {
      return (InsertStmt) stmt;
    }

    public ParseNode getStmt() {
      return stmt;
    }

    public Analyzer getAnalyzer() {
      return analyzer;
    }
  }

  /**
   * Parse and analyze 'stmt'.
   *
   * @param stmt
   * @return AnalysisResult
   *         containing the analyzer and the analyzed insert or select statement.
   * @throws AnalysisException
   *           on any kind of error, including parsing error.
   */
  public AnalysisResult analyze(String stmt) throws AnalysisException {
    SqlScanner input = new SqlScanner(new StringReader(stmt));
    SqlParser parser = new SqlParser(input);
    try {
      AnalysisResult result = new AnalysisResult();
      result.stmt = (ParseNode) parser.parse().value;
      if (result.stmt == null) {
        return null;
      }
      result.analyzer = new Analyzer(catalog);
      result.stmt.analyze(result.analyzer);
      return result;
    } catch (AnalysisException e) {
      throw new AnalysisException(e.getMessage() + " (in " + stmt + ")");
    } catch (Exception e) {
      e.printStackTrace(System.err);
      throw new AnalysisException(parser.getErrorMsg(stmt));
    }
  }
}
