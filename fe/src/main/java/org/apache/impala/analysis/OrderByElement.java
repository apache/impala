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

import java.util.List;
import java.util.Objects;

import com.google.common.collect.Lists;

import static org.apache.impala.analysis.ToSqlOptions.DEFAULT;

/**
 * Combination of expr, ASC/DESC, and nulls ordering.
 */
public class OrderByElement {
  private Expr expr_;
  private final boolean isAsc_;
  // Represents the NULLs ordering specified: true when "NULLS FIRST", false when
  // "NULLS LAST", and null if not specified.
  private final Boolean nullsFirstParam_;

  /**
   * Constructs the OrderByElement.
   *
   * 'nullsFirstParam' should be true if "NULLS FIRST", false if "NULLS LAST", or null if
   * the NULLs order was not specified.
   */
  public OrderByElement(Expr expr, boolean isAsc, Boolean nullsFirstParam) {
    super();
    expr_ = expr;
    isAsc_ = isAsc;
    nullsFirstParam_ = nullsFirstParam;
  }

  /**
   * C'tor for cloning.
   */
  private OrderByElement(OrderByElement other) {
    expr_ = other.expr_.clone();
    isAsc_ = other.isAsc_;
    if (other.nullsFirstParam_ != null) {
      nullsFirstParam_ = Boolean.valueOf(other.nullsFirstParam_.booleanValue());
    } else {
      nullsFirstParam_ = null;
    }
  }

  public Expr getExpr() { return expr_; }
  public void setExpr(Expr e) { expr_ = e; }
  public boolean isAsc() { return isAsc_; }
  public Boolean getNullsFirstParam() { return nullsFirstParam_; }
  public boolean nullsFirst() { return nullsFirst(nullsFirstParam_, isAsc_); }

  public final String toSql() { return toSql(DEFAULT); }

  public String toSql(ToSqlOptions options) {
    StringBuilder strBuilder = new StringBuilder();
    strBuilder.append(expr_.toSql(options));
    strBuilder.append(isAsc_ ? " ASC" : " DESC");
    // When ASC and NULLS LAST or DESC and NULLS FIRST, we do not print NULLS FIRST/LAST
    // because it is the default behavior and we want to avoid printing NULLS FIRST/LAST
    // whenever possible as it is incompatible with Hive (SQL compatibility with Hive is
    // important for views).
    if (nullsFirstParam_ != null) {
      if (isAsc_ && nullsFirstParam_) {
        // If ascending, nulls are last by default, so only add if nulls first.
        strBuilder.append(" NULLS FIRST");
      } else if (!isAsc_ && !nullsFirstParam_) {
        // If descending, nulls are first by default, so only add if nulls last.
        strBuilder.append(" NULLS LAST");
      }
    }
    return strBuilder.toString();
  }

  @Override
  public boolean equals(Object obj) {
    if (obj == null) return false;
    if (obj.getClass() != this.getClass()) return false;
    OrderByElement o = (OrderByElement)obj;
    boolean nullsFirstEqual =
      (nullsFirstParam_ == null) == (o.nullsFirstParam_ == null);
    if (nullsFirstParam_ != null && nullsFirstEqual) {
      nullsFirstEqual = nullsFirstParam_.equals(o.nullsFirstParam_);
    }
    return expr_.equals(o.expr_) && isAsc_ == o.isAsc_ && nullsFirstEqual;
  }

  @Override
  public int hashCode() {
    return Objects.hash(getClass(), nullsFirstParam_, expr_, isAsc_);
  }

  @Override
  public OrderByElement clone() { return new OrderByElement(this); }

  /**
   * Compute nullsFirst.
   *
   * @param nullsFirstParam True if "NULLS FIRST", false if "NULLS LAST", or null if
   *                        the NULLs order was not specified.
   * @param isAsc
   * @return Returns true if nulls are ordered first or false if nulls are ordered last.
   *         Independent of isAsc.
   */
  public static boolean nullsFirst(Boolean nullsFirstParam, boolean isAsc) {
    return nullsFirstParam == null ? !isAsc : nullsFirstParam;
  }

  /**
   * Returns a new list of order-by elements with the order by exprs of src substituted
   * according to smap. Preserves the other sort params from src.
   */
  public static List<OrderByElement> substitute(List<OrderByElement> src,
      ExprSubstitutionMap smap, Analyzer analyzer) {
    List<OrderByElement> result = Lists.newArrayListWithCapacity(src.size());
    for (OrderByElement element: src) {
      result.add(new OrderByElement(element.getExpr().substitute(smap, analyzer, false),
          element.isAsc_, element.nullsFirstParam_));
    }
    return result;
  }

  /**
   * Extracts the order-by exprs from the list of order-by elements and returns them.
   */
  public static List<Expr> getOrderByExprs(List<OrderByElement> src) {
    List<Expr> result = Lists.newArrayListWithCapacity(src.size());
    for (OrderByElement element: src) {
      result.add(element.getExpr());
    }
    return result;
  }

  /**
   * Returns a new list of OrderByElements with the same (cloned) expressions but the
   * ordering direction reversed (asc becomes desc, nulls first becomes nulls last, etc.)
   */
  public static List<OrderByElement> reverse(List<OrderByElement> src) {
    List<OrderByElement> result = Lists.newArrayListWithCapacity(src.size());
    for (int i = 0; i < src.size(); ++i) {
      OrderByElement element = src.get(i);
      OrderByElement reverseElement =
          new OrderByElement(element.getExpr().clone(), !element.isAsc_,
              Boolean.valueOf(!nullsFirst(element.nullsFirstParam_, element.isAsc_)));
      result.add(reverseElement);
    }
    return result;
  }
}
