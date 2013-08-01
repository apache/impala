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
import com.cloudera.impala.thrift.TBoolLiteral;
import com.cloudera.impala.thrift.TExprNode;
import com.cloudera.impala.thrift.TExprNodeType;

public class BoolLiteral extends LiteralExpr {
  private final boolean value;

  public BoolLiteral(boolean value) {
    this.value = value;
    type = PrimitiveType.BOOLEAN;
  }

  public BoolLiteral(String value) throws AnalysisException {
    this.type = PrimitiveType.BOOLEAN;
    if (value.toLowerCase().equals("true")) {
      this.value = true;
    } else if (value.toLowerCase().equals("false")) {
      this.value = false;
    } else {
      throw new AnalysisException("invalid BOOLEAN literal: " + value);
    }
  }

  @Override
  public boolean equals(Object obj) {
    if (!super.equals(obj)) {
      return false;
    }
    return ((BoolLiteral) obj).value == value;
  }

  public boolean getValue() {
    return value;
  }

  @Override
  public String toSqlImpl() {
    return getStringValue();
  }

  @Override
  public String getStringValue() {
    return value ? "TRUE" : "FALSE";
  }

  @Override
  protected void toThrift(TExprNode msg) {
    msg.node_type = TExprNodeType.BOOL_LITERAL;
    msg.bool_literal = new TBoolLiteral(value);
  }
}
