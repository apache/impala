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

import org.apache.impala.catalog.Type;
import org.apache.impala.catalog.TypeCompatibility;
import org.apache.impala.common.AnalysisException;
import org.apache.impala.common.InternalException;
import org.apache.impala.service.FeSupport;
import org.apache.impala.thrift.TExprNode;
import org.apache.impala.thrift.TExprNodeType;
import org.apache.impala.thrift.TDateLiteral;
import org.apache.impala.thrift.TParseDateStringResult;

/**
 * Represents a literal date as a string formatted as yyyy-MM-dd.
 * Impala supports the same DATE literal syntax as Hive. For example: DATE '2019-02-15'.
 */
public class DateLiteral extends LiteralExpr {
  // Days since 1970-01-01. Class DateValue in the backend uses the same representation.
  private final int daysSinceEpoch_;
  // String representation of date
  private final String strDate_;

  public DateLiteral(int daysSinceEpoch, String strDate) {
    daysSinceEpoch_ = daysSinceEpoch;
    strDate_ = strDate;
    type_ = Type.DATE;
  }

  public DateLiteral(String strDate) throws AnalysisException {
    type_ = Type.DATE;

    // Parse date string.
    // Accept different variations. E.g.: '2011-01-01', '2011-1-01', '2011-01-1',
    // '2011-1-1'.
    TParseDateStringResult result;
    try {
      result = FeSupport.parseDateString(strDate);
    } catch (InternalException e) {
      throw new AnalysisException("Error parsing date literal: " + e.getMessage(), e);
    }

    if (!result.valid || !result.isSetDays_since_epoch()) {
      throw new AnalysisException("Invalid date literal: '" + strDate + "'");
    }

    daysSinceEpoch_ = result.getDays_since_epoch();
    if (result.isSetCanonical_date_string()) {
      strDate_ = result.getCanonical_date_string();
    } else {
      strDate_ = strDate;
    }
  }

  /**
   * Copy c'tor used in clone.
   */
  protected DateLiteral(DateLiteral other) {
    super(other);
    daysSinceEpoch_ = other.daysSinceEpoch_;
    strDate_ = other.strDate_;
  }

  @Override
  protected boolean localEquals(Expr that) {
    return super.localEquals(that)
        && daysSinceEpoch_ == ((DateLiteral) that).daysSinceEpoch_;
  }

  @Override
  public int hashCode() { return daysSinceEpoch_; }

  @Override
  public String toSqlImpl(ToSqlOptions options) {
    // ANSI Date Literal format.
    return "DATE '" + getStringValue() + "'";
  }

  @Override
  public String getStringValue() { return strDate_; }

  public int getValue() { return daysSinceEpoch_; }

  @Override
  protected void toThrift(TExprNode msg) {
    msg.node_type = TExprNodeType.DATE_LITERAL;
    msg.date_literal = new TDateLiteral();
    msg.date_literal.setDays_since_epoch(daysSinceEpoch_);
    msg.date_literal.setDate_string(strDate_);
  }

  @Override
  protected Expr uncheckedCastTo(Type targetType, TypeCompatibility compatibility)
      throws AnalysisException {
    if (targetType.equals(type_)) {
      return this;
    } else {
      return new CastExpr(targetType, this, compatibility);
    }
  }

  @Override
  public int compareTo(LiteralExpr o) {
    int ret = super.compareTo(o);
    if (ret != 0) return ret;
    DateLiteral other = (DateLiteral) o;
    return daysSinceEpoch_ - other.daysSinceEpoch_;
  }

  @Override
  public Expr clone() { return new DateLiteral(this); }
}
