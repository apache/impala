// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package org.apache.impala.analysis;

import static org.apache.impala.analysis.ToSqlOptions.DEFAULT;

import java.util.ArrayList;
import java.util.List;

import org.apache.impala.authorization.PrivilegeRequest;
import org.apache.impala.catalog.FeView;
import org.apache.impala.catalog.View;
import org.apache.impala.common.AnalysisException;

import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;

/**
 * Representation of the WITH clause that may appear before a query statement or insert
 * statement. A WITH clause contains a list of named view definitions that may be
 * referenced in the query statement that follows it.
 *
 * Scoping rules:
 * A WITH-clause view is visible inside the query statement that it belongs to.
 * This includes inline views and nested WITH clauses inside the query statement.
 *
 * Each WITH clause establishes a new analysis scope. A WITH-clause view definition
 * may refer to views from the same WITH-clause appearing to its left, and to all
 * WITH-clause views from outer scopes.
 *
 * References to WITH-clause views are resolved inside out, i.e., a match is found by
 * first looking in the current scope and then in the enclosing scope(s).
 *
 * Views defined within the same WITH-clause may not use the same alias.
 */
public class WithClause extends StmtNode {
  /////////////////////////////////////////
  // BEGIN: Members that need to be reset()

  private final List<View> views_;

  // END: Members that need to be reset()
  /////////////////////////////////////////

  public WithClause(List<View> views) {
    Preconditions.checkNotNull(views);
    Preconditions.checkState(!views.isEmpty());
    views_ = views;
  }

  /**
   * Analyzes all views and registers them with the analyzer. Enforces scoping rules.
   * All local views registered with the analyzer are have QueryStmts with resolved
   * TableRefs to simplify the analysis of view references.
   */
  @Override
  public void analyze(Analyzer analyzer) throws AnalysisException {
    // Create a new analyzer for the WITH clause with a new global state (IMPALA-1357)
    // but a child of 'analyzer' so that the global state for 'analyzer' is not polluted
    // during analysis of the WITH clause. withClauseAnalyzer is a child of 'analyzer' so
    // that local views registered in parent blocks are visible here.
    Analyzer withClauseAnalyzer = Analyzer.createWithNewGlobalState(analyzer);
    withClauseAnalyzer.setHasWithClause();
    if (analyzer.isExplain()) withClauseAnalyzer.setIsExplain();
    try {
      for (View view: views_) {
        Analyzer viewAnalyzer = new Analyzer(withClauseAnalyzer);
        view.getQueryStmt().analyze(viewAnalyzer);
        // Register this view so that the next view can reference it.
        withClauseAnalyzer.registerLocalView(view);
      }
      // Register all local views with the analyzer.
      for (FeView localView: withClauseAnalyzer.getLocalViews().values()) {
        analyzer.registerLocalView(localView);
      }
      // Record audit events because the resolved table references won't generate any
      // when a view is referenced.
      analyzer.getAccessEvents().addAll(withClauseAnalyzer.getAccessEvents());
    }
    finally {
      // Register all privilege requests made from the root analyzer to the input
      // analyzer so that caller could do authorization for all the requests collected
      // during analysis and report an authorization error over an analysis error.
      // We should not accidentally reveal the non-existence of a database/table if
      // the user is not authorized.
      for (PrivilegeRequest req : withClauseAnalyzer.getPrivilegeReqs()) {
        analyzer.registerPrivReq(req);
      }
    }
  }

  /**
   * C'tor for cloning.
   */
  private WithClause(WithClause other) {
    Preconditions.checkNotNull(other);
    views_ = new ArrayList<>();
    for (View view: other.views_) {
      views_.add(new View(view.getName(), view.getQueryStmt().clone(),
          view.getOriginalColLabels()));
    }
  }

  public void reset() {
    for (View view: views_) view.getQueryStmt().reset();
  }

  @Override
  public WithClause clone() { return new WithClause(this); }

  @Override
  public final String toSql() {
    return toSql(DEFAULT);
  }

  @Override
  public String toSql(ToSqlOptions options) {
    List<String> viewStrings = new ArrayList<>();
    for (View view: views_) {
      // Enclose the view alias and explicit labels in quotes if Hive cannot parse it
      // without quotes. This is needed for view compatibility between Impala and Hive.
      String aliasSql = ToSqlUtils.getIdentSql(view.getName());
      if (view.getColLabels() != null) {
        aliasSql += "(" + Joiner.on(", ").join(
            ToSqlUtils.getIdentSqlList(view.getOriginalColLabels())) + ")";
      }
      viewStrings.add(aliasSql + " AS (" + view.getQueryStmt().toSql(options) + ")");
    }
    return "WITH " + Joiner.on(",").join(viewStrings);
  }

  public List<View> getViews() { return views_; }

  @Override
  public boolean resolveTableMask(Analyzer analyzer) throws AnalysisException {
    boolean hasChanges = false;
    for (View v : views_) {
      hasChanges |= v.getQueryStmt().resolveTableMask(analyzer);
    }
    return hasChanges;
  }
}
