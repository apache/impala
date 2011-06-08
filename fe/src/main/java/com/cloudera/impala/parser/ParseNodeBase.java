// Copyright (c) 2011 Cloudera, Inc. All rights reserved.

package com.cloudera.impala.parser;

import com.cloudera.impala.common.AnalysisException;

public class ParseNodeBase implements ParseNode {
  public void analyze(Analyzer analyzer) throws AnalysisException {
    throw new AnalysisException("not implemented");
  }

  // Print SQL syntax corresponding to this node.
  public String toSql() {
    return "";
  }

  // Print debug string.
  public String debugString() {
    return "";
  }
}
