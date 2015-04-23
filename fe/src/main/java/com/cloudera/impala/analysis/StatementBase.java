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

import org.apache.commons.lang.NotImplementedException;

import com.cloudera.impala.common.AnalysisException;

/**
 * Base class for all Impala SQL statements.
 */
abstract class StatementBase implements ParseNode {
  // True if this Stmt is the top level of an explain stmt.
  protected boolean isExplain_ = false;

  /////////////////////////////////////////
  // BEGIN: Members that need to be reset()

  // Analyzer that was used to analyze this statement.
  protected Analyzer analyzer_;

  // END: Members that need to be reset()
  /////////////////////////////////////////

  protected StatementBase() { }

  /**
   * C'tor for cloning.
   */
  protected StatementBase(StatementBase other) {
    analyzer_ = other.analyzer_;
    isExplain_ = other.isExplain_;
  }

  /**
   * Analyzes the statement and throws an AnalysisException if analysis fails. A failure
   * could be due to a problem with the statement or because one or more tables/views
   * were missing from the catalog.
   * It is up to the analysis() implementation to ensure the maximum number of missing
   * tables/views get collected in the Analyzer before failing analyze().
   */
  public void analyze(Analyzer analyzer) throws AnalysisException {
    if (isAnalyzed()) return;
    if (isExplain_) analyzer.setIsExplain();
    analyzer_ = analyzer;
  }

  public Analyzer getAnalyzer() { return analyzer_; }
  public boolean isAnalyzed() { return analyzer_ != null; }

  public String toSql() { return ""; }
  public void setIsExplain() { isExplain_ = true; }
  public boolean isExplain() { return isExplain_; }

  /**
   * Returns a deep copy of this node including its analysis state. Some members such as
   * tuple and slot descriptors are generally not deep copied to avoid potential
   * confusion of having multiple descriptor instances with the same id, although
   * they should be unique in the descriptor table.
   * TODO for 2.3: Consider also cloning table and slot descriptors for clarity,
   * or otherwise make changes to more provide clearly defined clone() semantics.
   */
  @Override
  public StatementBase clone() {
    throw new NotImplementedException(
        "Clone() not implemented for " + getClass().getSimpleName());
  }

  /**
   * Resets the internal analysis state of this node.
   * For easier maintenance, class members that need to be reset are grouped into
   * a 'section' clearly indicated by comments as follows:
   *
   * class SomeStmt extends StatementBase {
   *   ...
   *   /////////////////////////////////////////
   *   // BEGIN: Members that need to be reset()
   *
   *   <member declarations>
   *
   *   // END: Members that need to be reset()
   *   /////////////////////////////////////////
   *   ...
   * }
   *
   * In general, members that are set or modified during analyze() must be reset().
   * TODO: Introduce this same convention for Exprs, possibly by moving clone()/reset()
   * into the ParseNode interface for clarity.
   */
  public void reset() { analyzer_ = null; }
}
