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
  private final Expr expr;
  private String alias;

  // for "[name.]*"
  private final TableName tblName;
  private final boolean isStar;

  public SelectListItem(Expr expr, String alias) {
    super();
    Preconditions.checkNotNull(expr);
    this.expr = expr;
    this.alias = alias;
    this.tblName = null;
    this.isStar = false;
  }

  // select list item corresponding to "[[db.]tbl.]*"
  static public SelectListItem createStarItem(TableName tblName) {
    return new SelectListItem(tblName);
  }

  private SelectListItem(TableName tblName) {
    super();
    this.expr = null;
    this.tblName = tblName;
    this.isStar = true;
  }

  public boolean isStar() { return isStar; }
  public TableName getTblName() { return tblName; }
  public Expr getExpr() { return expr; }
  public String getAlias() { return alias; }

  public String toSql() {
    if (!isStar) {
      Preconditions.checkNotNull(expr);
      // Enclose aliases in quotes if Hive cannot parse them without quotes.
      // This is needed for view compatibility between Impala and Hive.
      String aliasSql = null;
      if (alias != null) aliasSql = ToSqlUtils.getHiveIdentSql(alias);
      return expr.toSql() + ((aliasSql != null) ? " " + aliasSql : "");
    } else if (tblName != null) {
      return tblName.toString() + ".*" + ((alias != null) ? " " + alias : "");
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
    if (alias != null) return alias.toLowerCase();
    if (expr instanceof SlotRef) {
      SlotRef slotRef = (SlotRef) expr;
      return slotRef.getColumnName().toLowerCase();
    }
    // Optionally return auto-generated column label.
    if (useHiveColLabels) return "_c" + selectListPos;
    return expr.toSql().toLowerCase();
  }

  @Override
  public SelectListItem clone() {
    if (isStar) return createStarItem(tblName);
    return new SelectListItem(expr.clone(null), alias);
  }

}
