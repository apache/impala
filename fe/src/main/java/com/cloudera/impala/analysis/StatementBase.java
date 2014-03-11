// Copyright 2012 Cloudera Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package com.cloudera.impala.analysis;

import com.cloudera.impala.catalog.AuthorizationException;
import com.cloudera.impala.common.AnalysisException;

/**
 * Base class for all Impala SQL statements.
 */
abstract class StatementBase implements ParseNode {
  // True if this Stmt is top level of an explain stmt.
  protected boolean isExplain_ = false;

  /**
   * Analyzes the statement and throws an AnalysisException if analysis fails. A failure
   * could be due to a problem with the statement or because one or more tables/views
   * were missing from the catalog.
   * It is up to the analysis() implementation to ensure the maximum number of missing
   * tables/views get collected in the Analyzer before failing analyze().
   */
  public void analyze(Analyzer analyzer) throws AnalysisException,
      AuthorizationException {
    analyzer.setIsExplain(isExplain_);
  }

  /**
   * Print SQL syntax corresponding to this node.
   * @see com.cloudera.impala.parser.ParseNode#toSql()
   */
  public String toSql() { return ""; }
  public void setIsExplain(boolean isExplain) { this.isExplain_ = isExplain; }
  public boolean isExplain() { return isExplain_; }
}
