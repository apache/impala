// Copyright (c) 2011 Cloudera, Inc. All rights reserved.

package com.cloudera.impala.parser;

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

  /**
   * Parse and analyze 'stmt'.
   *
   * @param stmt
   * @return SelectStmt corresponding to 'stmt'
   * @throws AnalysisException
   *           on any kind of error, including parsing error.
   */
  public SelectStmt analyze(String stmt) throws AnalysisException {
    SqlScanner input = new SqlScanner(new StringReader(stmt));
    SqlParser parser = new SqlParser(input);
    try {
      ParseNode node = (ParseNode) parser.parse().value;
      if (node == null) {
        return null;
      }
      Analyzer analyzer = new Analyzer(catalog);
      node.analyze(analyzer);
      return (SelectStmt) node;
    } catch (Exception e) {
      throw new AnalysisException(parser.getErrorMsg(stmt));
    }
  }
}
