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

import com.cloudera.impala.catalog.ColumnType;
import com.cloudera.impala.common.AnalysisException;
import com.cloudera.impala.common.NotImplementedException;
import com.cloudera.impala.thrift.TExprNode;
import com.cloudera.impala.thrift.TExprNodeType;
import com.cloudera.impala.thrift.TFloatLiteral;
import com.google.common.base.Objects;
import com.google.common.base.Preconditions;

public class FloatLiteral extends LiteralExpr {
  private double value_;

  private void init(Double value) {
    this.value_ = value.doubleValue();
    // Figure out if this will fit in a FLOAT without loosing precision.
    float fvalue;
    fvalue = value.floatValue();
    if (fvalue == value.doubleValue()) {
      type_ = ColumnType.FLOAT;
    } else {
      type_ = ColumnType.DOUBLE;
    }
  }

  public FloatLiteral(Double value) {
    init(value);
  }

  /**
   * C'tor forcing type, e.g., due to implicit cast
   */
  public FloatLiteral(Double value, ColumnType type) {
    this.value_ = value.doubleValue();
    this.type_ = type;
  }

  public FloatLiteral(String value) throws AnalysisException {
    Double floatValue = null;
    try {
      floatValue = new Double(value);
    } catch (NumberFormatException e) {
      throw new AnalysisException("invalid floating-point literal: " + value, e);
    }
    init(floatValue);
  }

  @Override
  public String debugString() {
    return Objects.toStringHelper(this)
        .add("value", value_)
        .toString();
  }

  @Override
  public boolean equals(Object obj) {
    if (!super.equals(obj)) return false;
    return ((FloatLiteral) obj).value_ == value_;
  }

  @Override
  public String toSqlImpl() {
    return getStringValue();
  }

  @Override
  public String getStringValue() {
    return Double.toString(value_);
  }

  @Override
  protected void toThrift(TExprNode msg) {
    msg.node_type = TExprNodeType.FLOAT_LITERAL;
    msg.float_literal = new TFloatLiteral(value_);
  }

  public double getValue() {
    return value_;
  }

  @Override
  protected Expr uncheckedCastTo(ColumnType targetType) throws AnalysisException {
    Preconditions.checkState(targetType.isFloatingPointType() || targetType.isDecimal());
    if (targetType.isFloatingPointType()) {
      type_ = targetType;
    } else {
      return new CastExpr(targetType, this, true);
    }
    return this;
  }

  @Override
  public void swapSign() throws NotImplementedException {
    // swapping sign does not change the type
    value_ = -value_;
  }

  @Override
  public int compareTo(LiteralExpr o) {
    if (!(o instanceof FloatLiteral)) return -1;
    FloatLiteral other = (FloatLiteral) o;
    if (value_ > other.getValue()) return 1;
    if (value_ < other.getValue()) return -1;
    return 0;
  }
}
