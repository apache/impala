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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.cloudera.impala.catalog.AuthorizationException;
import com.cloudera.impala.common.AnalysisException;
import com.cloudera.impala.thrift.TExprNode;
import com.cloudera.impala.thrift.TExprNodeType;
import com.cloudera.impala.thrift.TExprOpcode;
import com.google.common.base.Preconditions;

/**
 * &&, ||, ! predicates.
 *
 */
public class CompoundPredicate extends Predicate {
  private final static Logger LOG = LoggerFactory.getLogger(CompoundPredicate.class);

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
  private final Operator op_;

  public CompoundPredicate(Operator op, Expr e1, Expr e2) {
    super();
    this.op_ = op;
    Preconditions.checkNotNull(e1);
    children_.add(e1);
    Preconditions.checkArgument(op == Operator.NOT && e2 == null
        || op != Operator.NOT && e2 != null);
    if (e2 != null) children_.add(e2);
  }

  public Operator getOp() { return op_; }

  @Override
  public boolean equals(Object obj) {
    if (!super.equals(obj)) return false;
    return ((CompoundPredicate) obj).op_ == op_;
  }

  @Override
  public String toSqlImpl() {
    if (children_.size() == 1) {
      Preconditions.checkState(op_ == Operator.NOT);
      return "NOT " + getChild(0).toSql();
    } else {
      return getChild(0).toSql() + " " + op_.toString() + " " + getChild(1).toSql();
    }
  }

  @Override
  protected void toThrift(TExprNode msg) {
    msg.node_type = TExprNodeType.COMPOUND_PRED;
    msg.setOpcode(op_.toThrift());
  }

  @Override
  public void analyze(Analyzer analyzer) throws AnalysisException,
      AuthorizationException {
    if (isAnalyzed_) return;
    super.analyze(analyzer);

    // Check that children are predicates.
    for (Expr e : children_) {
      if (!e.getType().isBoolean() && !e.getType().isNull()) {
        throw new AnalysisException(String.format("Operand '%s' part of predicate " +
            "'%s' should return type 'BOOLEAN' but returns type '%s'.",
            e.toSql(), toSql(), e.getType()));
      }
    }

    if (getChild(0).selectivity_ == -1
        || children_.size() == 2 && getChild(1).selectivity_ == -1) {
      // give up if we're missing an input
      selectivity_ = -1;
      return;
    }

    switch (op_) {
      case AND:
        selectivity_ = getChild(0).selectivity_ * getChild(1).selectivity_;
        break;
      case OR:
        selectivity_ = getChild(0).selectivity_ + getChild(1).selectivity_
            - getChild(0).selectivity_ * getChild(1).selectivity_;
        break;
      case NOT:
        selectivity_ = 1.0 - getChild(0).selectivity_;
        break;
    }
    selectivity_ = Math.max(0.0, Math.min(1.0, selectivity_));
  }
}
