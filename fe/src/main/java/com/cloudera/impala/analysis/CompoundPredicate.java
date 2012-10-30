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

import com.cloudera.impala.thrift.TExprNode;
import com.cloudera.impala.thrift.TExprNodeType;
import com.cloudera.impala.thrift.TExprOpcode;
import com.google.common.base.Preconditions;

/**
 * &&, ||, ! predicates.
 *
 */
public class CompoundPredicate extends Predicate {
  public enum Operator {
    AND("AND", TExprOpcode.COMPOUND_AND),
    OR("OR", TExprOpcode.COMPOUND_OR),
    NOT("NOT", TExprOpcode.COMPOUND_NOT);

    private final String description;
    private final TExprOpcode thriftOp;

    private Operator(String description, TExprOpcode thriftOp) {
      this.description = description;
      this.thriftOp = thriftOp;
    }

    @Override
    public String toString() {
      return description;
    }

    public TExprOpcode toThrift() {
      return thriftOp;
    }
  }
  private final Operator op;

  public CompoundPredicate(Operator op, Predicate p1, Predicate p2) {
    super();
    this.op = op;
    Preconditions.checkNotNull(p1);
    children.add(p1);
    Preconditions.checkArgument(op == Operator.NOT && p2 == null
        || op != Operator.NOT && p2 != null);
    if (p2 != null) {
      children.add(p2);
    }
  }

  public Operator getOp() {
    return op;
  }

  @Override
  public boolean equals(Object obj) {
    if (!super.equals(obj)) {
      return false;
    }
    return ((CompoundPredicate) obj).op == op;
  }

  @Override
  public String toSql() {
    if (children.size() == 1) {
      Preconditions.checkState(op == Operator.NOT);
      return "NOT " + getChild(0).toSql();
    } else {
      return getChild(0).toSql() + " " + op.toString() + " " + getChild(1).toSql();
    }
  }

  @Override
  protected void toThrift(TExprNode msg) {
    msg.node_type = TExprNodeType.COMPOUND_PRED;
    msg.setOpcode(op.toThrift());
  }
}
