// Copyright (c) 2011 Cloudera, Inc. All rights reserved.

package com.cloudera.impala.analysis;

import com.cloudera.impala.common.AnalysisException;
import com.cloudera.impala.common.InternalException;

abstract class ParseNodeBase implements ParseNode {
  public void analyze(Analyzer analyzer) throws AnalysisException, InternalException {
    throw new AnalysisException("not implemented");
  }

  /* Print SQL syntax corresponding to this node.
   * @see com.cloudera.impala.parser.ParseNode#toSql()
   */
  public String toSql() {
    return "";
  }

  /* Print debug string.
   * @see com.cloudera.impala.parser.ParseNode#debugString()
   */
  public String debugString() {
    return "";
  }
}
