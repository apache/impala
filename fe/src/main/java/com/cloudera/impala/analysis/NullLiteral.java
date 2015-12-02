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

import com.cloudera.impala.catalog.Type;
import com.cloudera.impala.thrift.TExprNode;
import com.cloudera.impala.thrift.TExprNodeType;
import com.google.common.base.Objects;
import com.google.common.base.Preconditions;

public class NullLiteral extends LiteralExpr {

  public NullLiteral() {
    type_ = Type.NULL;
  }

  /**
   * Copy c'tor used in clone().
   */
  protected NullLiteral(NullLiteral other) {
    super(other);
  }

  /**
   * Returns an analyzed NullLiteral of the specified type.
   */
  public static NullLiteral create(Type type) {
    NullLiteral l = new NullLiteral();
    l.analyzeNoThrow(null);
    l.uncheckedCastTo(type);
    return l;
  }

  @Override
  public boolean equals(Object obj) {
    if (!super.equals(obj)) return false;
    return obj instanceof NullLiteral;
  }

  @Override
  public int hashCode() { return 0; }

  @Override
  public String toSqlImpl() { return getStringValue(); }

  @Override
  public String debugString() {
    return Objects.toStringHelper(this).addValue(super.debugString()).toString();
  }

  @Override
  public String getStringValue() { return "NULL"; }

  @Override
  protected Expr uncheckedCastTo(Type targetType) {
    Preconditions.checkState(targetType.isValid());
    type_ = targetType;
    return this;
  }

  @Override
  protected void toThrift(TExprNode msg) {
    msg.node_type = TExprNodeType.NULL_LITERAL;
  }

  @Override
  public Expr clone() { return new NullLiteral(this); }

  @Override
  protected void resetAnalysisState() {
    super.resetAnalysisState();
    type_ = Type.NULL;
  }
}
