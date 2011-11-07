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
    public SelectStmt selectStmt;
    public Analyzer analyzer;
  }

  /**
   * Parse and analyze 'stmt'.
   *
   * @param stmt
   * @return SelectStmt corresponding to 'stmt'
   * @throws AnalysisException
   *           on any kind of error, including parsing error.
   */
  public AnalysisResult analyze(String stmt) throws AnalysisException {
    SqlScanner input = new SqlScanner(new StringReader(stmt));
    SqlParser parser = new SqlParser(input);
    try {
      AnalysisResult result = new AnalysisResult();
      result.selectStmt = (SelectStmt) parser.parse().value;
      if (result.selectStmt == null) {
        return null;
      }
      result.analyzer = new Analyzer(catalog);
      result.selectStmt.analyze(result.analyzer);
      return result;
    } catch (AnalysisException e) {
      throw new AnalysisException(e.getMessage() + " (in " + stmt + ")");
    } catch (Exception e) {
      e.printStackTrace(System.err);
      throw new AnalysisException(parser.getErrorMsg(stmt));
    }
  }
}
