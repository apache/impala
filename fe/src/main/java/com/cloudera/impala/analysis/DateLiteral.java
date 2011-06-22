// Copyright (c) 2011 Cloudera, Inc. All rights reserved.

package com.cloudera.impala.analysis;

import java.sql.Timestamp;
import java.text.ParsePosition;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import com.cloudera.impala.catalog.PrimitiveType;
import com.cloudera.impala.common.AnalysisException;
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
  public String toSql() {
    return acceptedFormat.format(value);
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
