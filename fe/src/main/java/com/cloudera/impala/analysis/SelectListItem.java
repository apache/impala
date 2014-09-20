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

import com.google.common.base.Preconditions;

class SelectListItem {
  private final Expr expr_;
  private String alias_;

  // for "[name.]*"
  private final TableName tblName_;
  private final boolean isStar_;

  public SelectListItem(Expr expr, String alias) {
    super();
    Preconditions.checkNotNull(expr);
    this.expr_ = expr;
    this.alias_ = alias;
    this.tblName_ = null;
    this.isStar_ = false;
  }

  // select list item corresponding to "[[db.]tbl.]*"
  static public SelectListItem createStarItem(TableName tblName) {
    return new SelectListItem(tblName);
  }

  private SelectListItem(TableName tblName) {
    super();
    this.expr_ = null;
    this.tblName_ = tblName;
    this.isStar_ = true;
  }

  public boolean isStar() { return isStar_; }
  public TableName getTblName() { return tblName_; }
  public Expr getExpr() { return expr_; }
  public String getAlias() { return alias_; }
  public void setAlias(String alias) { alias_ = alias; }

  public String toSql() {
    if (!isStar_) {
      Preconditions.checkNotNull(expr_);
      // Enclose aliases in quotes if Hive cannot parse them without quotes.
      // This is needed for view compatibility between Impala and Hive.
      String aliasSql = null;
      if (alias_ != null) aliasSql = ToSqlUtils.getIdentSql(alias_);
      return expr_.toSql() + ((aliasSql != null) ? " " + aliasSql : "");
    } else if (tblName_ != null) {
      return tblName_.toString() + ".*" + ((alias_ != null) ? " " + alias_ : "");
    } else {
      return "*";
    }
  }

  /**
   * Returns a column label for this select list item.
   * If an alias was given, then the column label is the lower case alias.
   * If expr is a SlotRef then directly use its lower case column name.
   * Otherwise, the label is the lower case toSql() of expr or a Hive auto-generated
   * column name (depending on useHiveColLabels).
   * Hive's auto-generated column labels have a "_c" prefix and a select-list pos suffix,
   * e.g., "_c0", "_c1", "_c2", etc.
   *
   * Using auto-generated columns that are consistent with Hive is important
   * for view compatibility between Impala and Hive.
   */
  public String toColumnLabel(int selectListPos, boolean useHiveColLabels) {
    if (alias_ != null) return alias_.toLowerCase();
    if (expr_ instanceof SlotRef) {
      SlotRef slotRef = (SlotRef) expr_;
      return slotRef.getColumnName().toLowerCase();
    }
    // Optionally return auto-generated column label.
    if (useHiveColLabels) return "_c" + selectListPos;
    // Abbreviate the toSql() for analytic exprs.
    if (expr_ instanceof AnalyticExpr) {
      AnalyticExpr expr = (AnalyticExpr) expr_;
      return expr.getFnCall().toSql() + " OVER(...)";
    }
    return expr_.toSql().toLowerCase();
  }

  @Override
  public SelectListItem clone() {
    if (isStar_) return createStarItem(tblName_);
    return new SelectListItem(expr_.clone().reset(), alias_);
  }

}
