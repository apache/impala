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

import java.io.IOException;
import java.io.StringReader;
import java.math.BigInteger;

import java_cup.runtime.Symbol;

import org.apache.hadoop.hive.ql.parse.BaseSemanticAnalyzer;

import com.cloudera.impala.catalog.PrimitiveType;
import com.cloudera.impala.common.AnalysisException;
import com.cloudera.impala.thrift.TExprNode;
import com.cloudera.impala.thrift.TExprNodeType;
import com.cloudera.impala.thrift.TStringLiteral;
import com.google.common.base.Objects;
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
  public String toSqlImpl() {
    return "'" + value + "'";
  }

  @Override
  protected void toThrift(TExprNode msg) {
    msg.node_type = TExprNodeType.STRING_LITERAL;
    msg.string_literal = new TStringLiteral(getUnescapedValue());
  }

  public String getValue() {
    return value;
  }

  public String getUnescapedValue() {
    // Unescape string exactly like Hive does. Hive's method assumes
    // quotes so we add them here to reuse Hive's code.
    return BaseSemanticAnalyzer.unescapeSQLString("'" + value + "'");
  }

  @Override
  public String getStringValue() {
    return value;
  }

  @Override
  public String debugString() {
    return Objects.toStringHelper(this)
        .add("value", value)
        .toString();
  }

  @Override
  protected Expr uncheckedCastTo(PrimitiveType targetType) throws AnalysisException {
    Preconditions.checkState(targetType.isNumericType() || targetType.isDateType()
        || targetType == this.type);
    if (targetType == this.type) {
      return this;
    } else if (targetType.isNumericType()) {
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
      // which are implicitly cast to a numeric literal.
      // This would require invoking the parser.
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
      return new IntLiteral(BigInteger.valueOf(val * (long)multiplier));
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

  @Override
  public int compareTo(LiteralExpr o) {
    if (!(o instanceof StringLiteral)) return -1;
    StringLiteral other = (StringLiteral) o;
    return value.compareTo(other.getStringValue());
  }
}
