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

import java.util.ArrayList;
import java.util.List;

import com.cloudera.impala.analysis.UnionStmt.UnionOperand;
import com.cloudera.impala.catalog.AuthorizationException;
import com.cloudera.impala.common.AnalysisException;
import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;

/**
 * Representation of the WITH clause that may appear before a query statement or insert
 * statement. A WITH clause contains a list of named views that may be referenced in
 * the query statement that follows it.
 *
 * Scoping rules:
 * A WITH-clause view is visible anywhere inside the query statement that it belongs to.
 * This includes subqueries and nested WITH clauses inside the query statement.
 *
 * A WITH-clause view definition may refer to views from the same
 * WITH-clause appearing to its left (and WITH-clause views from outer scopes).
 *
 * Each WITH clause establishes a new scope. Within this scope all WITH-clause views
 * from the current scope and all parents' scopes are visible.
 * References to WITH-clause views are resolved inside out, i.e., a matching is found
 * by first looking in the current scope, then the parents' scope, etc.
 *
 * Views defined within the same WITH-clause may not use the same alias.
 */
public class WithClause implements ParseNode {
  private final ArrayList<ViewRef> views_;

  // Analyzer used for this WithClause. Set during analysis.
  private Analyzer analyzer_;

  // List of table references from the WITH-clause views' query statements that do not
  // refer to nested WITH-clause views. Used to detect recursive references by
  // propagating the unresolved references to outer scopes.
  private final ArrayList<BaseTableRef> unresolvedTableRefs_ = Lists.newArrayList();

  public WithClause(ArrayList<ViewRef> views) {
    Preconditions.checkNotNull(views);
    Preconditions.checkState(!views.isEmpty());
    this.views_ = views;
  }

  /**
   * Analysis of the top-level WITH-clause. Calls analyzeWithClause() to do the actual
   * analysis work. We distinguish the analysis of the top-level WITH-clause
   * from nested WITH-clauses so that we can fully-qualify the true base table refs.
   */
  @Override
  public void analyze(Analyzer analyzer)
      throws AnalysisException, AuthorizationException {
    analyzeWithClause(analyzer);
    // Fully qualify the names of true base tables to produce correct toSql().
    for (BaseTableRef baseTblRef: unresolvedTableRefs_) {
      baseTblRef.disableWithViewReplacement();
      baseTblRef.setFullyQualifiedTableName(analyzer);
    }
  }

  /**
   * Enforces scoping rules, and ensures that there are no recursive table references.
   * Nested WITH clauses should call this method instead of analyze().
   */
  private void analyzeWithClause(Analyzer analyzer) throws AnalysisException,
      AuthorizationException {
    unresolvedTableRefs_.clear();
    analyzeWithClause(analyzer, this, unresolvedTableRefs_);

    // The remaining unresolved tables must refer to base tables in the catalog.
    // We explicitly disable view matching for them to simplify later view substitution.
    for (BaseTableRef baseTblRef: unresolvedTableRefs_) {
      baseTblRef.disableWithViewReplacement();
    }

    // Register all views in analyzer. After this step we will "blindly" replace
    // BaseTableRefs with view definitions from this WITH clause in
    // Analyser.substituteBaseTablesWithMatchingViews().
    for (ViewRef view: views_) {
      analyzer.registerWithClauseView(view);
    }
    this.analyzer_ = analyzer;
  }

  /**
   * Analyzes stmt's WITH clause if it has one, and returns the WITH clause's
   * new analyzer. Otherwise, returns analyzer.
   */
  private Analyzer pushScope(Analyzer analyzer, QueryStmt stmt)
      throws AnalysisException, AuthorizationException {
    if (stmt.hasWithClause()) {
      // Create a new analyzer to establish a new scope.
      Analyzer newAnalyzer = new Analyzer(analyzer, analyzer.getUser());
      stmt.getWithClause().analyzeWithClause(newAnalyzer);
      return newAnalyzer;
    }
    // Since withClause was not set we remain in the same scope.
    return analyzer;
  }

  /**
   * Adds all unresolved table references from stmt's WITH-clause
   * to unresolvedTableRefs, enabling view replacement for them so they can be
   * resolved in parent scopes. Does nothing if stmt has no WITH clause.
   */
  private void popScope(Analyzer analyzer, QueryStmt stmt,
      ArrayList<BaseTableRef> unresolvedTableRefs) {
    if (stmt.hasWithClause()) {
      // Since we are in a nested WITH clause (scope) re-enable view replacement to
      // allow the references to be resolved in the parent scope.
      ArrayList<BaseTableRef> nestedUnresolvedTableRefs =
          stmt.getWithClause().getUnresolvedTableRefs();
      for (BaseTableRef baseTblRef: nestedUnresolvedTableRefs) {
        baseTblRef.enableWithViewReplacement();
      }
      unresolvedTableRefs.addAll(nestedUnresolvedTableRefs);
    }
  }

  private void analyzeQueryStmt(Analyzer analyzer, QueryStmt stmt,
      ArrayList<BaseTableRef> unresolvedTableRefs)
      throws AnalysisException, AuthorizationException {
    Analyzer tmpAnalyzer = pushScope(analyzer, stmt);

    if (stmt instanceof UnionStmt) {
      // Analyze the individual operands that may have a WITH clause themselves.
      UnionStmt unionStmt = (UnionStmt) stmt;
      for(UnionOperand operand: unionStmt.getOperands()) {
        analyzeQueryStmt(tmpAnalyzer, operand.getQueryStmt(), unresolvedTableRefs);
      }
    } else {
      Preconditions.checkState(stmt instanceof SelectStmt);
      SelectStmt selectStmt = (SelectStmt) stmt;
      for (TableRef tblRef: selectStmt.getTableRefs()) {
        if (tblRef instanceof BaseTableRef) {
          BaseTableRef baseTblRef = (BaseTableRef) tblRef;
          // If there are no matching WITH-clause views then propagate the base table
          // to the parent scope as an unresolved table.
          if (tmpAnalyzer.findViewDefinition(baseTblRef, false) == null) {
            unresolvedTableRefs.add(baseTblRef);
          }
          continue;
        }

        // Recurse into the inline view's query statement to analyze nested WITH clauses.
        Preconditions.checkState(tblRef instanceof InlineViewRef);
        InlineViewRef v = (InlineViewRef) tblRef;
        analyzeQueryStmt(tmpAnalyzer, v.getViewStmt(), unresolvedTableRefs);
      }
    }

    popScope(tmpAnalyzer, stmt, unresolvedTableRefs);
  }

  /**
   * Analyzes WITH-clause views at the same level from left to right.
   * Nested WITH clauses are analyzed bottom up as follows. Each nested WITH clause
   * establishes a new scope. In each nested scope we gather base-table references and
   * attempt to resolve them against visible views according to the scoping rules.
   * Base tables that cannot be matched against a visible view are
   * propagated upwards to the parent scope to detect recursive references
   * at the parent's level.
   *
   * The visible WITH-clause views are propagated top down by creating a child analyzer
   * in every new scope. The unresolved table references are propagated bottom up.
   */
  private void analyzeWithClause(Analyzer analyzer, WithClause withClause,
      ArrayList<BaseTableRef> unresolvedTableRefs)
      throws AnalysisException, AuthorizationException {

    // Create a new child analyzer to register views into.
    Analyzer tmpAnalyzer = new Analyzer(analyzer, analyzer.getUser());
    for (ViewRef view: withClause.views_) {

      // Gather all unresolved table references from all child scopes
      // of the view's query statement.
      ArrayList<BaseTableRef> localUnresolvedTableRefs = Lists.newArrayList();
      analyzeQueryStmt(tmpAnalyzer, view.getViewStmt(), localUnresolvedTableRefs);

      // Check for recursive references to current view.
      for (BaseTableRef baseTblRef: localUnresolvedTableRefs) {
        // Fully-qualified tables cannot be references to WITH-clause views.
        if (baseTblRef.getName().isFullyQualified()) continue;
        if (baseTblRef.getName().getTbl().equalsIgnoreCase(view.getAlias())) {
          throw new AnalysisException(
              String.format("Unsupported recursive reference to table '%s' in WITH " +
                  "clause.", view.getAlias()));
        }

        // If there are no matching WITH-clause views then propagate the base table
        // to the parent scope as an unresolved table.
        if (tmpAnalyzer.findViewDefinition(baseTblRef, false) == null) {
          unresolvedTableRefs.add(baseTblRef);
        }
      }

      // Register view so that the next view definition at this level may reference it.
      tmpAnalyzer.registerWithClauseView(view);
    }
  }

  public ArrayList<ViewRef> getViews() { return views_; }
  public Analyzer getAnalyzer() { return analyzer_; }
  public ArrayList<BaseTableRef> getUnresolvedTableRefs() {
    return unresolvedTableRefs_;
  }

  @Override
  public WithClause clone() {
    ArrayList<ViewRef> viewClones = Lists.newArrayList();
    for (ViewRef view: views_) {
      viewClones.add((ViewRef) view.clone());
    }
    return new WithClause(viewClones);
  }

  @Override
  public String toSql() {
    List<String> viewStrings = Lists.newArrayList();
    for (InlineViewRef view: views_) {
      // Enclose the view alias in quotes if Hive cannot parse it without quotes.
      // This is needed for view compatibility between Impala and Hive.
      String aliasSql = ToSqlUtils.getIdentSql(view.getAlias());
      viewStrings.add(aliasSql + " AS (" + view.getViewStmt().toSql() + ")");
    }
    return "WITH " + Joiner.on(",").join(viewStrings);
  }
}
