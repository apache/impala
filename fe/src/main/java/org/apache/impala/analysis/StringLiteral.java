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

import java.io.IOException;
import java.io.StringReader;
import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.util.Arrays;

import org.apache.commons.codec.binary.Hex;
import org.apache.impala.catalog.ScalarType;
import org.apache.impala.catalog.Type;
import org.apache.impala.catalog.TypeCompatibility;
import org.apache.impala.common.AnalysisException;
import org.apache.impala.compat.MetastoreShim;
import org.apache.impala.thrift.TExprNode;
import org.apache.impala.thrift.TExprNodeType;
import org.apache.impala.thrift.TStringLiteral;
import org.apache.impala.util.StringUtils;

import com.google.common.base.MoreObjects;
import com.google.common.base.Preconditions;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java_cup.runtime.Symbol;

public class StringLiteral extends LiteralExpr {
  private final static Logger LOG = LoggerFactory.getLogger(StringLiteral.class);
  // From the strValue_ and binValue_ always only one is null while the other is non-null.
  // strValue_ == null means that the value is not decodable as utf8.
  private final String strValue_;
  private final byte[] binValue_;
  public static int MAX_STRING_LEN = Integer.MAX_VALUE;

  // Indicates whether this value needs to be unescaped in toThrift() or comparison.
  // TODO: Add enum to distinguish the sources, e.g. double/single quoted SQL, HMS view,
  //  or the result of an evaluated const expression. So we know whether we need
  //  unescaping more clearly.
  private final boolean needsUnescaping_;

  public StringLiteral(String value) {
    this(value, ScalarType.STRING, true);
  }

  public StringLiteral(String value, Type type, boolean needsUnescaping) {
    strValue_ = value;
    binValue_ = null;
    type_ = type;
    needsUnescaping_ = needsUnescaping;
  }

  public StringLiteral(byte[] value, Type type) {
    // It is ok to fail as StringLiteral can contain binary data. strValue_ is null in
    // this case.
    strValue_ = StringUtils.fromUtf8Buffer(ByteBuffer.wrap(value), true);
    if (strValue_ == null) {
      // Save binValue_ only if it cannot be represented as String.
      binValue_ = value;
    } else {
      binValue_ = null;
    }
    type_ = type;
    needsUnescaping_ = false;
  }

  /**
   * Copy c'tor used in clone().
   */
  protected StringLiteral(StringLiteral other) {
    super(other);
    strValue_ = other.strValue_;
    binValue_ = other.binValue_;
    needsUnescaping_ = other.needsUnescaping_;
  }

  @Override
  protected boolean localEquals(Expr that) {
    if (!super.localEquals(that)) return false;
    StringLiteral other = (StringLiteral) that;
    if (!type_.equals(other.type_)) return false;
    if (needsUnescaping_ != other.needsUnescaping_) return false;
    if (binValue_ != null) {
      Preconditions.checkState(strValue_ == null);
      if (other.binValue_ == null) return false;
      return Arrays.equals(binValue_, other.binValue_);
    }
    Preconditions.checkState(strValue_ != null);
    return strValue_.equals(other.strValue_);
  }

  // TODO: shouldn't this also check needsUnescaping?
  @Override
  public int hashCode() {
    return binValue_ != null ? Arrays.hashCode(binValue_) : strValue_.hashCode();
  }

  @Override
  public String toSqlImpl(ToSqlOptions options) {
    return "'" + getNormalizedValue() + "'";
  }

  @Override
  protected void toThrift(TExprNode msg) {
    msg.node_type = TExprNodeType.STRING_LITERAL;
    byte[] val;
    if (needsUnescaping_) {
      val = StringUtils.toUtf8Array(getUnescapedValue());
    } else {
      val = getBinValue();
    }
    msg.string_literal = new TStringLiteral(ByteBuffer.wrap(val));
  }

  /**
   * Returns the original value that the string literal was constructed with,
   * without escaping or unescaping it.
   * Can be only used when the StringLiteral can be represented as java String.
   */
  public String getValueWithOriginalEscapes() {
    checkHasValidString();
    return strValue_;
  }

  public String getUnescapedValue() {
    checkHasValidString();
    // Unescape string exactly like Hive does. Hive's method assumes
    // quotes so we add them here to reuse Hive's code.
    return MetastoreShim.unescapeSQLString("'" + getNormalizedValue() + "'");
  }

  private String unescapeIfNeeded() {
    checkHasValidString();
    return needsUnescaping_ ? getUnescapedValue() : strValue_;
  }

  /**
   *  String literals can come directly from the SQL of a query or from rewrites like
   *  constant folding. So this value normalization to a single-quoted string is necessary
   *  because we do not know whether single or double quotes are appropriate.
   *
   *  @return a normalized representation of the string value suitable for embedding in
   *          SQL as a single-quoted string literal.
   */
  private String getNormalizedValue() {
    if (strValue_ == null) {
      // express binary using unhex()
      String hex = Hex.encodeHexString(binValue_);
      return "unhex(\"" + hex.toUpperCase() +"\")";
    }
    final int len = strValue_.length();
    final StringBuilder sb = new StringBuilder(len);
    for (int i = 0; i < len; ++i) {
      final char currentChar = strValue_.charAt(i);
      if (currentChar == '\\' && (i + 1) < len) {
        final char nextChar = strValue_.charAt(i + 1);
        // unescape an escaped double quote: remove back-slash in front of the quote.
        if (nextChar == '"' || nextChar == '\'' || nextChar == '\\') {
          if (nextChar != '"') {
            sb.append(currentChar);
          }
          sb.append(nextChar);
          ++i;
          continue;
        }

        sb.append(currentChar);
      } else if (currentChar == '\'') {
        // escape a single quote: add back-slash in front of the quote.
        sb.append("\\\'");
      } else {
        sb.append(currentChar);
      }
    }
    return sb.toString();
  }

  @Override
  public String getStringValue() {
    return getValueWithOriginalEscapes();
  }

  @Override
  public String debugString() {
    // TODO: hex encode the binary case if someone needs this
    String str = strValue_ != null ? strValue_ : "<can't encode as String>";
    return MoreObjects.toStringHelper(this)
        .add("strValue", str)
        .toString();
  }

  @Override
  protected Expr uncheckedCastTo(Type targetType, TypeCompatibility compatibility)
      throws AnalysisException {
    Preconditions.checkState(targetType.isNumericType() || targetType.isDateOrTimeType()
        || targetType.equals(this.type_) || targetType.isStringType());
    if (targetType.equals(this.type_)) {
      return this;
    } else if (targetType.isStringType()) {
      type_ = targetType;
    } else if (targetType.isNumericType()) {
      return convertToNumber(targetType);
    } else if (targetType.isDateOrTimeType()) {
      // Let the BE do the cast
      // - it is in Boost format in case target type is TIMESTAMP
      // - CCTZ is used for conversion in case target type is DATE.
      return new CastExpr(targetType, this, compatibility);
    }
    return this;
  }

  /**
   * Convert this string literal to numeric literal.
   *
   * @param targetType sets the target type of the newly created literal
   * @return new converted literal (not null)
   *         the type of the literal is determined by the lexical scanner
   * @throws AnalysisException
   *           if NumberFormatException occurs,
   *           or if floating point value is NaN or infinite
   */
  public LiteralExpr convertToNumber(Type targetType) throws AnalysisException {
    checkHasValidString();
    StringReader reader = new StringReader(strValue_);
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
      throw new AnalysisException("Number too large: " + strValue_);
    }
    if (sym.sym == SqlParserSymbols.INTEGER_LITERAL) {
      BigDecimal val = (BigDecimal) sym.value;
      if (negative) val = val.negate();
      return new NumericLiteral(val, targetType);
    }
    if (sym.sym == SqlParserSymbols.DECIMAL_LITERAL) {
      BigDecimal val = (BigDecimal) sym.value;
      if (negative) val = val.negate();
      return new NumericLiteral(val, targetType);
    }
    // Symbol is not an integer or floating point literal.
    throw new AnalysisException("Failed to convert string literal '"
        + strValue_ + "' to number.");
  }

  @Override
  public int compareTo(LiteralExpr o) {
    int ret = super.compareTo(o);
    if (ret != 0) return ret; // TODO: this doesn't seem to handle the NULL case well
    StringLiteral other = (StringLiteral) o;
    // Do byte wise comparisin on UTF8 format to be consistant with BE.
    // TODO: it would be clearer to use backand's comparision functions in literals.
    byte[] arr1 = getBinValue();
    byte[] arr2 = other.getBinValue();
    // TODO: rewrite once Java 8 support is dropped
    //return Arrays.compare(binValue_, other.binValue_); // added in java 9
    for (int i = 0; i < arr1.length && i < arr2.length; i++) {
      if (arr1[i] < arr2[i]) return -1;
      if (arr1[i] > arr2[i]) return 1;
    }
    if (arr1.length < arr2.length) return -1;
    if (arr1.length > arr2.length) return 1;
    return 0;
  }

  @Override
  public Expr clone() { return new StringLiteral(this); }

  public int utf8ArrayLength() { return getBinValue().length; }

  public boolean isValidUtf8() { return binValue_ == null; }

  public byte[] getBinValue() {
    if (binValue_ != null) return binValue_;
    return StringUtils.toUtf8Array(unescapeIfNeeded());
  }

  private void checkHasValidString() {
    Preconditions.checkState(strValue_ != null,
        "non-utf8 string: %s", getNormalizedValue());
  }
}
