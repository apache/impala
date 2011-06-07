// Copyright (c) 2011 Cloudera, Inc. All rights reserved.

package com.cloudera.impala.parser;

interface ParseNode {
  /**
   * Perform semantic analysis of node and all of its children.
   * Throws exception if any errors found.
   * @param analyzer
   * @throws Analyzer.Exception
   */
  public void analyze(Analyzer analyzer) throws Analyzer.Exception;

  /**
   * @return SQL syntax corresponding to this node.
   */
  public String toSql();

  public String debugString();
}
