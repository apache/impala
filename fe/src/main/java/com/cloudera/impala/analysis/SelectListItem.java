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

  public boolean isStar() {
    return isStar;
  }

  public TableName getTblName() {
    return tblName;
  }

  public Expr getExpr() {
    return expr;
  }

  public String getAlias() {
    return alias;
  }

  public String toSql() {
    if (!isStar) {
      Preconditions.checkNotNull(expr);
      return expr.toSql() + ((alias != null) ? " " + alias : "");
    } else if (tblName != null) {
      return tblName.toString() + ".*" + ((alias != null) ? " " + alias : "");
    } else {
      return "*";
    }
  }

  /**
   * Return a column label for the select list item.
   */
  public String toColumnLabel() {
    Preconditions.checkState(!isStar());
    if (alias != null) {
      return alias.toLowerCase();
    }
    return expr.toColumnLabel().toLowerCase();
  }

  @Override
  public SelectListItem clone() {
    if (isStar) {
      return createStarItem(tblName);
    }
    return new SelectListItem(expr.clone(), alias);
  }

}
