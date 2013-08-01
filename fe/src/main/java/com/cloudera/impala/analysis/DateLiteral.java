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

import java.sql.Timestamp;
import java.text.ParsePosition;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import com.cloudera.impala.catalog.PrimitiveType;
import com.cloudera.impala.common.AnalysisException;
import com.cloudera.impala.thrift.TDateLiteral;
import com.cloudera.impala.thrift.TExprNode;
import com.cloudera.impala.thrift.TExprNodeType;
import com.google.common.base.Preconditions;

class DateLiteral extends LiteralExpr {
  private static final List<SimpleDateFormat> formats =
      new ArrayList<SimpleDateFormat>();
  static {
    formats.add(new SimpleDateFormat("yyyy-MM-dd"));
    formats.add(new SimpleDateFormat("yyyy-MM-dd hh:mm:ss"));
    formats.add(new SimpleDateFormat("yyyy-MM-dd hh:mm:ss.SSS"));
  }
  private SimpleDateFormat acceptedFormat = null;
  private final Timestamp value;

  /**
   * C'tor takes string because a DateLiteral
   * can only be constructed by an implicit cast
   * Parsing will only succeed if all characters of
   * s are accepted.
   * @param s
   *          string representation of date
   * @param type
   *          desired type of date literal
   */
  public DateLiteral(String s, PrimitiveType type) throws AnalysisException {
    Preconditions.checkArgument(type.isDateType());
    Date date = null;
    ParsePosition pos = new ParsePosition(0);
    for (SimpleDateFormat format : formats) {
      pos.setIndex(0);
      date = format.parse(s, pos);
      if (pos.getIndex() == s.length()) {
        acceptedFormat = format;
        break;
      }
    }
    if (acceptedFormat == null) {
      throw new AnalysisException("Unable to parse string '" + s + "' to date.");
    }
    this.value = new Timestamp(date.getTime());
    this.type = type;
  }

  @Override
  public boolean equals(Object obj) {
    if (!super.equals(obj)) {
      return false;
    }
    // dateFormat does not need to be compared
    // because dates originating from strings of different formats
    // are still comparable
    return ((DateLiteral) obj).value == value;
  }

  @Override
  public String toSqlImpl() {
    return getStringValue();
  }

  @Override
  public String getStringValue() {
    return acceptedFormat.format(value);
  }

  @Override
  protected void toThrift(TExprNode msg) {
    msg.node_type = TExprNodeType.DATE_LITERAL;
    msg.date_literal = new TDateLiteral(value.getTime());
  }

  public Timestamp getValue() {
    return value;
  }

  @Override
  protected Expr uncheckedCastTo(PrimitiveType targetType) throws AnalysisException {
    // programmer error, we should never reach this state
    Preconditions.checkState(false);
    return this;
  }
}
