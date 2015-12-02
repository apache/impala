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
import java.math.BigDecimal;

import java_cup.runtime.Symbol;

import org.apache.hadoop.hive.ql.parse.BaseSemanticAnalyzer;

import com.cloudera.impala.catalog.ScalarType;
import com.cloudera.impala.catalog.Type;
import com.cloudera.impala.common.AnalysisException;
import com.cloudera.impala.thrift.TExprNode;
import com.cloudera.impala.thrift.TExprNodeType;
import com.cloudera.impala.thrift.TStringLiteral;
import com.google.common.base.Objects;
import com.google.common.base.Preconditions;

public class StringLiteral extends LiteralExpr {
  private final String value_;

  public StringLiteral(String value) {
    this.value_ = value;
    type_ = ScalarType.STRING;
  }

  public StringLiteral(String value, Type type) {
    this.value_ = value;
    type_ = type;
  }

  /**
   * Copy c'tor used in clone().
   */
  protected StringLiteral(StringLiteral other) {
    super(other);
    value_ = other.value_;
  }

  @Override
  public boolean equals(Object obj) {
    if (!super.equals(obj)) return false;
    return ((StringLiteral) obj).value_.equals(value_);
  }

  @Override
  public int hashCode() { return value_.hashCode(); }

  @Override
  public String toSqlImpl() {
    return "'" + value_ + "'";
  }

  @Override
  protected void toThrift(TExprNode msg) {
    msg.node_type = TExprNodeType.STRING_LITERAL;
    msg.string_literal = new TStringLiteral(getUnescapedValue());
  }

  public String getValue() { return value_; }

  public String getUnescapedValue() {
    // Unescape string exactly like Hive does. Hive's method assumes
    // quotes so we add them here to reuse Hive's code.
    return BaseSemanticAnalyzer.unescapeSQLString("'" + value_ + "'");
  }

  @Override
  public String getStringValue() {
    return value_;
  }

  @Override
  public String debugString() {
    return Objects.toStringHelper(this)
        .add("value", value_)
        .toString();
  }

  @Override
  protected Expr uncheckedCastTo(Type targetType) throws AnalysisException {
    Preconditions.checkState(targetType.isNumericType() || targetType.isDateType()
        || targetType.equals(this.type_) || targetType.isStringType());
    if (targetType.equals(this.type_)) {
      return this;
    } else if (targetType.isStringType()) {
      type_ = targetType;
    } else if (targetType.isNumericType()) {
      return convertToNumber();
    } else if (targetType.isDateType()) {
      // Let the BE do the cast so it is in Boost format
      return new CastExpr(targetType, this);
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
    StringReader reader = new StringReader(value_);
    SqlScanner scanner = new SqlScanner(reader);
    // For distinguishing positive and negative numbers.
    boolean negative = false;
    Symbol sym;
    try {
      // We allow simple chaining of MINUS to recognize negative numbers.
      // Currently we can't handle string literals containing full fledged expressions
      // which are implicitly cast to a numeric literal.
      // This would require invoking the parser.
      sym = scanner.next_token();
      while (sym.sym == SqlParserSymbols.SUBTRACT) {
        negative = !negative;
        sym = scanner.next_token();
      }
    } catch (IOException e) {
      throw new AnalysisException("Failed to convert string literal to number.", e);
    }
    if (sym.sym == SqlParserSymbols.NUMERIC_OVERFLOW) {
      throw new AnalysisException("Number too large: " + value_);
    }
    if (sym.sym == SqlParserSymbols.INTEGER_LITERAL) {
      BigDecimal val = (BigDecimal) sym.value;
      if (negative) val = val.negate();
      return new NumericLiteral(val);
    }
    if (sym.sym == SqlParserSymbols.DECIMAL_LITERAL) {
      BigDecimal val = (BigDecimal) sym.value;
      if (negative) val = val.negate();
      return new NumericLiteral(val);
    }
    // Symbol is not an integer or floating point literal.
    throw new AnalysisException(
        "Failed to convert string literal '" + value_ + "' to number.");
  }

  @Override
  public int compareTo(LiteralExpr o) {
    int ret = super.compareTo(o);
    if (ret != 0) return ret;
    StringLiteral other = (StringLiteral) o;
    return value_.compareTo(other.getStringValue());
  }

  @Override
  public Expr clone() { return new StringLiteral(this); }
}
