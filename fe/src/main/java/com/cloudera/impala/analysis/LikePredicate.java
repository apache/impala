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

import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;

import com.cloudera.impala.catalog.AuthorizationException;
import com.cloudera.impala.catalog.PrimitiveType;
import com.cloudera.impala.common.AnalysisException;
import com.cloudera.impala.thrift.TExprNode;
import com.cloudera.impala.thrift.TExprNodeType;
import com.cloudera.impala.thrift.TExprOpcode;
import com.cloudera.impala.thrift.TLikePredicate;
import com.google.common.base.Preconditions;

public class LikePredicate extends Predicate {
  enum Operator {
    LIKE("LIKE", TExprOpcode.LIKE),
    RLIKE("RLIKE", TExprOpcode.REGEX),
    REGEXP("REGEXP", TExprOpcode.REGEX);

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

  public LikePredicate(Operator op, Expr e1, Expr e2) {
    super();
    this.op = op;
    Preconditions.checkNotNull(e1);
    children.add(e1);
    Preconditions.checkNotNull(e2);
    children.add(e2);
    // TODO: improve with histograms?
    selectivity = 0.1;
  }

  @Override
  public boolean equals(Object obj) {
    if (!super.equals(obj)) {
      return false;
    }
    return ((LikePredicate) obj).op == op;
  }

  @Override
  public String toSqlImpl() {
    return getChild(0).toSql() + " " + op.toString() + " " + getChild(1).toSql();
  }

  @Override
  protected void toThrift(TExprNode msg) {
    msg.node_type = TExprNodeType.LIKE_PRED;
    msg.setOpcode(op.toThrift());
    msg.like_pred = new TLikePredicate("\\");
  }

  @Override
  public void analyze(Analyzer analyzer) throws AnalysisException,
      AuthorizationException {
    if (isAnalyzed) return;
    super.analyze(analyzer);
    if (getChild(0).getType() != PrimitiveType.STRING
        && !getChild(0).getType().isNull()) {
      throw new AnalysisException(
          "left operand of " + op.toString() + " must be of type STRING: " + toSql());
    }
    if (getChild(1).getType() != PrimitiveType.STRING
        && !getChild(1).getType().isNull()) {
      throw new AnalysisException(
          "right operand of " + op.toString() + " must be of type STRING: " + toSql());
    }

    if (!getChild(1).getType().isNull() && getChild(1).isLiteral()
        && (op == Operator.RLIKE || op == Operator.REGEXP)) {
      // let's make sure the pattern works
      // TODO: this checks that it's a Java-supported regex, but the syntax supported
      // by the backend is Posix; add a call to the backend to check the re syntax
      try {
        Pattern.compile(((StringLiteral) getChild(1)).getValue());
      } catch (PatternSyntaxException e) {
        throw new AnalysisException(
            "invalid regular expression in '" + this.toSql() + "'");
      }
    }
  }
}
