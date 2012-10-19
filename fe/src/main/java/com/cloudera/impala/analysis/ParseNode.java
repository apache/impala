// Copyright (c) 2012 Cloudera, Inc. All rights reserved.

package com.cloudera.impala.analysis;

import com.cloudera.impala.common.AnalysisException;
import com.cloudera.impala.common.InternalException;

public interface ParseNode {

  /**
   * Perform semantic analysis of node and all of its children.
   * Throws exception if any errors found.
   * @param analyzer
   * @throws AnalysisException, InternalException
   */
  public void analyze(Analyzer analyzer) throws AnalysisException, InternalException;

  /**
   * @return SQL syntax corresponding to this node.
   */
  public String toSql();

  public String debugString();
}
