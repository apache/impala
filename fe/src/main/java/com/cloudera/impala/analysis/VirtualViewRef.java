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
import com.google.common.base.Preconditions;

/**
 * A virtual view is a named table whose contents are defined by a query statement.
 * Such a view is "virtual" because the results of its defining query statement
 * are not physically stored anywhere. References to virtual views are replaced
 * by their corresponding query statements (with appropriate expression substitutions).
 *
 * Class used to represent virtual-view definitions as well as
 * virtual-view instantiations. A virtual view may be defined in the catalog
 * or as part of a WITH clause. A virtual view may only be defined once in
 * the same scope, but it can be referenced multiple times by statements
 * within that scope. We call each such reference an "instantiation" of the view.
 *
 * WITH-clause view definitions are registered in the analyzer. A view instantiation
 * replaces a BaseTableRef whose table name matches the view alias (see @WithClause for
 * scoping rules). Such view instantiations must be created via the instantiate() method
 * to properly "inherit" the context-dependent attributes of the BaseTableRef it is
 * replacing (e.g., the join op, join hints, on clause, etc.).
 */
public class VirtualViewRef extends InlineViewRef {

  // NULL for virtual-view definitions registered in the analyzer.
  // Set for virtual-view instantiations.
  protected BaseTableRef origTblRef;

  /**
   * C'tor used for creating a virtual view definition.
   */
  public VirtualViewRef(String alias, QueryStmt queryStmt) {
    super(alias, queryStmt);
    Preconditions.checkNotNull(alias);
  }

  /**
   * C'tor for virtual-view instantiations that replace the given BaseTableRef.
   * Only accessible via instantiate().
   */
  private VirtualViewRef(BaseTableRef origTblRef, QueryStmt queryStmt) {
    // Use tblRef's explicit alias if it has one, otherwise the table name.
    super(origTblRef.getAlias(), queryStmt);
    this.origTblRef = origTblRef;
    // Set context-dependent attributes from the original table.
    // Do not use originalTblRef.getJoinOp() because we want to record the fact that the
    // joinOp may be NULL, such that toSql() can work correctly.
    this.joinOp = origTblRef.joinOp;
    this.joinHints = origTblRef.getJoinHints();
    this.onClause = origTblRef.getOnClause();
    this.usingColNames = origTblRef.getUsingClause();
  }

  /**
   * Create an instantiation of this virtual view for replacing the given BaseTableRef.
   * A view instantiation is a copy of this view (including a clone of queryStmt)
   * with the context-dependent attributes set from the BaseTableRef to be replaced.
   * Can only be called on a virtual-view definition, and must not be called on
   * a virtual-view instantiation.
   */
  public VirtualViewRef instantiate(BaseTableRef ref) {
    // We should only instantiate a virtual-view definition.
    Preconditions.checkState(origTblRef == null);
    return new VirtualViewRef(ref, queryStmt.clone());
  }

  @Override
  public void analyze(Analyzer analyzer) throws AnalysisException,
      AuthorizationException {
    // Virtual view definitions cannot be analyzed.
    // We can only analyze an instantiation thereof.
    Preconditions.checkNotNull(origTblRef);
    super.analyze(analyzer);
  }

  @Override
  protected String tableRefToSql() {
    if (origTblRef != null) return origTblRef.tableRefToSql();
    return "(" + queryStmt.toSql() + ") " + alias;
  }

  @Override
  public TableRef clone() {
    return new VirtualViewRef(alias, queryStmt.clone());
  }

}
