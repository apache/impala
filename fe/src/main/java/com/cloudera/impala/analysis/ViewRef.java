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

import com.cloudera.impala.authorization.ImpalaInternalAdminUser;
import com.cloudera.impala.authorization.Privilege;
import com.cloudera.impala.catalog.AuthorizationException;
import com.cloudera.impala.catalog.View;
import com.cloudera.impala.common.AnalysisException;
import com.cloudera.impala.thrift.TAccessEvent;
import com.google.common.base.Preconditions;

/**
 * A view is a named table whose contents are defined by a query statement.
 * Such a view is "virtual" because the results of its defining query statement are not
 * physically stored anywhere. References to views are replaced by their corresponding
 * query statements (with appropriate expression substitutions).
 *
 * Class used to represent view definitions as well as view references. A view may
 * be defined in the catalog or as part of a WITH clause. A view may only be defined
 * once in the same scope, but it can be referenced multiple times by statements
 * within that scope.
 *
 * WITH-clause view definitions are registered in the analyzer. A view reference
 * replaces a BaseTableRef whose table name matches the view alias (see @WithClause for
 * scoping rules). Such view references must be created via the instantiate() method
 * to properly "inherit" the context-dependent attributes of the BaseTableRef it is
 * replacing (e.g., the join op, join hints, on clause, etc.).
 *
 * TODO: Separate the view definition from view references more clearly, e.g.,
 * using different classes.
 */
public class ViewRef extends InlineViewRef {
  // Set for views originating from the catalog.
  // NULL for views originating from a WITH clause.
  private final View table_;

  // NULL for view definitions registered in the analyzer. Set for view instantiations.
  private final BaseTableRef origTblRef_;

  /**
   * C'tor used for creating a view definition in a WITH clause.
   */
  public ViewRef(String alias, QueryStmt queryStmt) {
    super(alias, queryStmt);
    Preconditions.checkNotNull(alias);
    table_ = null;
    origTblRef_ = null;
  }

  /**
   * C'tor used for creating a view definition from a view registered in the catalog.
   */
  public ViewRef(String alias, QueryStmt queryStmt, View table) {
    super(alias, queryStmt);
    Preconditions.checkNotNull(alias);
    this.table_ = table;
    origTblRef_ = null;
  }

  /**
   * C'tor for view instantiations that replace the given BaseTableRef.
   * Only accessible via instantiate().
   */
  private ViewRef(BaseTableRef origTblRef, QueryStmt queryStmt, View table) {
    // Use tblRef's explicit alias if it has one, otherwise the table name.
    super(origTblRef.getAlias(), queryStmt);
    this.table_ = table;
    this.origTblRef_ = origTblRef;
    // Set context-dependent attributes from the original table.
    // Do not use originalTblRef.getJoinOp() because we want to record the fact that the
    // joinOp may be NULL, such that toSql() can work correctly.
    this.joinOp_ = origTblRef.joinOp_;
    this.joinHints_ = origTblRef.getJoinHints();
    this.onClause_ = origTblRef.getOnClause();
    this.usingColNames_ = origTblRef.getUsingClause();
  }

  /**
   * C'tor for cloning.
   */
  protected ViewRef(ViewRef other) {
    super(other);
    this.origTblRef_ =
        (other.origTblRef_ != null) ? (BaseTableRef) other.origTblRef_.clone() : null;
    this.table_ = other.table_;
  }

  /**
   * Create an instantiation of this view for replacing the given BaseTableRef.
   * A view instantiation is a copy of this view (including a clone of queryStmt)
   * with the context-dependent attributes set from the BaseTableRef to be replaced.
   * Can only be called on a view definition, and must not be called on
   * a view instantiation.
   */
  public ViewRef instantiate(BaseTableRef ref) {
    // We should only instantiate a view definition.
    Preconditions.checkState(origTblRef_ == null);
    return new ViewRef(ref, queryStmt_.clone(), table_);
  }

  @Override
  public void analyze(Analyzer analyzer) throws AnalysisException,
      AuthorizationException {
    // View definitions cannot be analyzed, only view instantiations.
    Preconditions.checkNotNull(origTblRef_);

    if (!isCatalogView()) {
      // Analyze WITH-clause view instantiations the same way as inline views.
      super.analyze(analyzer);
      return;
    }

    // At this point we have established that the analyzer's user has privileges to
    // access this view. If the user does not have privileges on the view's definition
    // then we report a generic authorization exception to not reveal
    // privileged information (e.g., the existence of a table).
    if (analyzer.isExplain()) {
      try {
        analyzeAsUser(analyzer, analyzer.getUser(), true);
      } catch (AuthorizationException e) {
        throw new AuthorizationException(
            String.format("User '%s' does not have privileges to " +
            "EXPLAIN this statement.", analyzer.getUser().getName()));
      }
    } else {
      // Use the super user to circumvent authorization checking during the
      // analysis of this view's defining queryStmt.
      analyzeAsUser(analyzer, ImpalaInternalAdminUser.getInstance(), true);
    }
    analyzer.addAccessEvent(new TAccessEvent(table_.getFullName(),
        table_.getCatalogObjectType(), Privilege.SELECT.toString()));
  }

  @Override
  public TupleDescriptor createTupleDescriptor(DescriptorTable descTbl)
      throws AnalysisException {
    // Analyze WITH-clause view instantiations the same way as inline views.
    if (!isCatalogView()) return super.createTupleDescriptor(descTbl);
    TupleDescriptor result = descTbl.createTupleDescriptor();
    result.setIsMaterialized(false);
    // This tuple is backed by the original view table from the catalog.
    result.setTable(table_);
    return result;
  }

  @Override
  protected String tableRefToSql() {
    if (origTblRef_ != null) return origTblRef_.tableRefToSql();
    // Enclose the alias in quotes if Hive cannot parse it without quotes.
    // This is needed for view compatibility between Impala and Hive.
    String aliasSql = ToSqlUtils.getHiveIdentSql(alias_);
    return "(" + queryStmt_.toSql() + ") " + aliasSql;
  }

  @Override
  public TableRef clone() { return new ViewRef(this); }

  @Override
  public String getAlias() {
    if (origTblRef_ == null) return alias_;
    return origTblRef_.getAlias().toLowerCase();
  }

  @Override
  public TableName getAliasAsName() { return new TableName(null, alias_); }

  /**
   * Returns true if this refers to a view registered in the catalog.
   * Returns false for views from a WITH clause.
   */
  public boolean isCatalogView() { return table_ != null; }
}
