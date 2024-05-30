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

import java.util.Arrays;

import com.google.common.base.Preconditions;
import org.apache.impala.catalog.Type;
import org.apache.impala.catalog.TypeCompatibility;
import org.apache.impala.common.AnalysisException;
import org.apache.impala.thrift.TExprNode;
import org.apache.impala.thrift.TExprNodeType;
import org.apache.impala.thrift.TTimestampLiteral;

/**
 * Represents a literal timestamp. Its value is a 16-byte array that corresponds to a
 * raw BE TimestampValue, e.g., in a slot. In addition, it stores the string
 * representation of the timestamp value to avoid converting the raw bytes on the Java
 * side. Such a conversion could potentially be inconsistent with what the BE would
 * produce, so it's better to defer to a single source of truth (the BE implementation).
 *
 * Literal timestamps can currently only be created via constant folding. There is no
 * way to directly specify a literal timestamp from SQL.
 */
public class TimestampLiteral extends LiteralExpr {
  private final byte[] value_;
  private final String strValue_;

  public TimestampLiteral(byte[] value, String strValue) {
    Preconditions.checkState(value.length == Type.TIMESTAMP.getSlotSize());
    value_ = value;
    strValue_ = strValue;
    type_ = Type.TIMESTAMP;
  }

  /**
   * Copy c'tor used in clone.
   */
  protected TimestampLiteral(TimestampLiteral other) {
    super(other);
    value_ = Arrays.copyOf(other.value_, other.value_.length);
    strValue_ = other.strValue_;
  }

  @Override
  protected boolean localEquals(Expr that) {
    return super.localEquals(that) &&
        Arrays.equals(value_, ((TimestampLiteral) that).value_);
  }

  @Override
  public int hashCode() { return Arrays.hashCode(value_); }

  @Override
  public String toSqlImpl(ToSqlOptions options) {
    // ANSI Timestamp Literal format.
    return "TIMESTAMP '" + getStringValue() + "'";
  }

  @Override
  public String getStringValue() { return strValue_; }

  public byte[] getValue() { return value_; }

  @Override
  protected void toThrift(TExprNode msg) {
    msg.node_type = TExprNodeType.TIMESTAMP_LITERAL;
    msg.timestamp_literal = new TTimestampLiteral();
    msg.timestamp_literal.setValue(value_);
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
    TimestampLiteral other = (TimestampLiteral) o;
    return strValue_.compareTo(other.strValue_);
  }

  @Override
  public Expr clone() { return new TimestampLiteral(this); }
}
