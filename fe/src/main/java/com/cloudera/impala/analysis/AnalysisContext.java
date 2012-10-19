// Copyright (c) 2012 Cloudera, Inc. All rights reserved.

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

  // The name of the database to use if one is not explicitly specified by a query. 
  private final String defaultDatabase;

  public AnalysisContext(Catalog catalog, String defaultDb) {
    this.catalog = catalog;
    defaultDatabase = defaultDb;
  }

  public AnalysisContext(Catalog catalog) {
    this(catalog, Catalog.DEFAULT_DB);
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

    public boolean isShowTablesStmt() {
      return stmt instanceof ShowTablesStmt;
    }

    public boolean isShowDbsStmt() {
      return stmt instanceof ShowDbsStmt;
    }

    public boolean isDescribeStmt() {
      return stmt instanceof DescribeStmt;
    }

    public boolean isDdlStmt() {
      return isUseStmt() || isShowTablesStmt() || isShowDbsStmt() || isDescribeStmt();
    }

    public boolean isDmlStmt() {
      return isInsertStmt();
    }

    public QueryStmt getQueryStmt() {
      return (QueryStmt) stmt;
    }

    public InsertStmt getInsertStmt() {
      return (InsertStmt) stmt;
    }

    public UseStmt getUseStmt() {
      return (UseStmt) stmt;
    }

    public ShowTablesStmt getShowTablesStmt() {
      return (ShowTablesStmt) stmt;
    }

    public ShowDbsStmt getShowDbsStmt() {
      return (ShowDbsStmt) stmt;
    }

    public DescribeStmt getDescribeStmt() {
      return (DescribeStmt) stmt;
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
      result.analyzer = new Analyzer(catalog, defaultDatabase);
      result.stmt.analyze(result.analyzer);
      return result;
    } catch (AnalysisException e) {
      throw new AnalysisException(e.getMessage() + " (in " + stmt + ")", e);
    } catch (Exception e) {
      e.printStackTrace(System.err);
      throw new AnalysisException(parser.getErrorMsg(stmt));
    }
  }
}
