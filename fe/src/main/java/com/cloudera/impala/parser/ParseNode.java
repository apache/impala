// Copyright (c) 2011 Cloudera, Inc. All rights reserved.

package com.cloudera.impala.parser;

import com.cloudera.impala.common.AnalysisException;

public interface ParseNode {
  /**
   * Perform semantic analysis of node and all of its children.
   * Throws exception if any errors found.
   * @param analyzer
   * @throws AnalysisException
   */
  public void analyze(Analyzer analyzer) throws AnalysisException;

  /**
   * @return SQL syntax corresponding to this node.
   */
  public String toSql();

  public String debugString();
}
