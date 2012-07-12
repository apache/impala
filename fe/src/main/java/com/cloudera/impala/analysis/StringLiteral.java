// Copyright (c) 2011 Cloudera, Inc. All rights reserved.

package com.cloudera.impala.analysis;

import java.io.IOException;
import java.io.StringReader;

import java_cup.runtime.Symbol;

import com.cloudera.impala.catalog.PrimitiveType;
import com.cloudera.impala.common.AnalysisException;
import com.cloudera.impala.thrift.TExprNode;
import com.cloudera.impala.thrift.TExprNodeType;
import com.cloudera.impala.thrift.TStringLiteral;
import com.google.common.base.Preconditions;

public class StringLiteral extends LiteralExpr {
  private final String value;

  public StringLiteral(String value) {
    this.value = value;
    type = PrimitiveType.STRING;
  }

  @Override
  public boolean equals(Object obj) {
    if (!super.equals(obj)) {
      return false;
    }
    return ((StringLiteral) obj).value.equals(value);
  }

  @Override
  public String toSql() {
    return "'" + value + "'";
  }

  @Override
  protected void toThrift(TExprNode msg) {
    msg.node_type = TExprNodeType.STRING_LITERAL;
    msg.string_literal = new TStringLiteral(value);
  }

  public String getValue() {
    return value;
  }

  @Override
  protected Expr uncheckedCastTo(PrimitiveType targetType) throws AnalysisException {
    Preconditions.checkState(targetType.isNumericType() || targetType.isDateType());
    if (targetType.isNumericType()) {
      return convertToNumber();
    } else if (targetType.isDateType()) {
      // Let the BE do the cast so it is in Boost format
      return new CastExpr(targetType, this, true);
    }
    return this;
  }

  /**
   * Convert this string literal to numeric literal.
   *
   * @return new converted literal (not null)
   *         the type of the literal is determined by the lexical scanner
   * @throws AnalysisException
   *           if NumberFormatException occurs,
   *           or if floating point value is NaN or infinite
   */
  public LiteralExpr convertToNumber()
      throws AnalysisException {
    StringReader reader = new StringReader(value);
    SqlScanner scanner = new SqlScanner(reader);
    // For distinguishing positive and negative numbers.
    double multiplier = 1;
    Symbol sym;
    try {
      // We allow simple chaining of MINUS to recognize negative numbers.
      // Currently we can't handle string literals containing full fledged expressions
      // which are implicitly cast to a numeric literal. This would require invoking the parser.
      sym = scanner.next_token();
      while (sym.sym == SqlParserSymbols.SUBTRACT) {
        multiplier *= -1;
        sym = scanner.next_token();
      }
    } catch (IOException e) {
      throw new AnalysisException("Failed to convert string literal to number.", e);
    }
    if (sym.sym == SqlParserSymbols.NUMERIC_OVERFLOW) {
      throw new AnalysisException("Number too large: " + value);
    }
    if (sym.sym == SqlParserSymbols.INTEGER_LITERAL) {
      Long val = (Long) sym.value;
      return new IntLiteral(new Long(val * (long)multiplier));
    }
    if (sym.sym == SqlParserSymbols.FLOATINGPOINT_LITERAL) {
      return new FloatLiteral((Double) sym.value * multiplier);
    }
    // Symbol is not an integer or floating point literal.
    throw new AnalysisException(
        "Failed to convert string literal '" + value + "' to number.");
  }


  /**
   * Convert a string literal to a date literal
   *
   * @param targetType
   *          is the desired type
   * @return new converted literal (not null)
   * @throws AnalysisException
   *           when entire given string cannot be transformed into a date
   */
  private LiteralExpr convertToDate(PrimitiveType targetType)
      throws AnalysisException {
    LiteralExpr newLiteral = null;
    newLiteral = new DateLiteral(value, targetType);
    return newLiteral;
  }
}
