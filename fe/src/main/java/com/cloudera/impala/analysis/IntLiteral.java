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

import com.cloudera.impala.catalog.PrimitiveType;
import com.cloudera.impala.common.AnalysisException;
import com.cloudera.impala.common.NotImplementedException;
import com.cloudera.impala.thrift.TExprNode;
import com.cloudera.impala.thrift.TExprNodeType;
import com.cloudera.impala.thrift.TIntLiteral;
import com.google.common.base.Preconditions;

public class IntLiteral extends LiteralExpr {
  private long value;

  private void init(Long value) {
    this.value = value.longValue();
    if (this.value <= Byte.MAX_VALUE && this.value >= Byte.MIN_VALUE) {
      type = PrimitiveType.TINYINT;
    } else if (this.value <= Short.MAX_VALUE && this.value >= Short.MIN_VALUE) {
      type = PrimitiveType.SMALLINT;
    } else if (this.value <= Integer.MAX_VALUE && this.value >= Integer.MIN_VALUE) {
      type = PrimitiveType.INT;
    } else {
      Preconditions.checkState(this.value <= Long.MAX_VALUE
          && this.value >= Long.MIN_VALUE);
      type = PrimitiveType.BIGINT;
    }
  }

  public IntLiteral(Long value) {
    init(value);
  }

  /** C'tor forcing type, e.g., due to implicit cast */
  public IntLiteral(Long value, PrimitiveType type) {
    this.value = value.longValue();
    this.type = type;
  }

  public IntLiteral(String value) throws AnalysisException {
    Long intValue = null;
    try {
      intValue = new Long(value);
    } catch (NumberFormatException e) {
      throw new AnalysisException("invalid integer literal: " + value, e);
    }
    init(intValue);
  }

  public long getValue() {
    return value;
  }

  @Override
  public String getStringValue() {
    return Long.toString(value);
  }

  @Override
  public boolean equals(Object obj) {
    if (!super.equals(obj)) {
      return false;
    }
    return value == ((IntLiteral) obj).value;
  }

  @Override
  public String toSql() {
    return getStringValue();
  }

  @Override
  protected void toThrift(TExprNode msg) {
    msg.node_type = TExprNodeType.INT_LITERAL;
    msg.int_literal = new TIntLiteral(value);
  }

  @Override
  protected Expr uncheckedCastTo(PrimitiveType targetType) throws AnalysisException {
    Preconditions.checkState(targetType.isNumericType());
    if (targetType.isFixedPointType()) {
      this.type = targetType;
      return this;
    } else if (targetType.isFloatingPointType()) {
      return new FloatLiteral(new Double(value), targetType);
    }
    return this;
  }

  @Override
  public void swapSign() throws NotImplementedException {
    // swapping sign does not change the type
    value = -value;
  }
}
