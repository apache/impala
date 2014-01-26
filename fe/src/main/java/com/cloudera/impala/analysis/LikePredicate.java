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
import com.cloudera.impala.catalog.Db;
import com.cloudera.impala.catalog.Function.CompareMode;
import com.cloudera.impala.catalog.ScalarFunction;
import com.cloudera.impala.common.AnalysisException;
import com.cloudera.impala.thrift.TExprNode;
import com.cloudera.impala.thrift.TExprNodeType;
import com.cloudera.impala.thrift.TLikePredicate;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;

public class LikePredicate extends Predicate {
  enum Operator {
    LIKE("LIKE"),
    RLIKE("RLIKE"),
    REGEXP("REGEXP");

    private final String description_;

    private Operator(String description) {
      this.description_ = description;
    }

    @Override
    public String toString() {
      return description_;
    }
  }

  public static void initBuiltins(Db db) {
    db.addBuiltin(ScalarFunction.createBuiltinOperator(
        Operator.LIKE.name(), "LikePredicate", "LikeFn",
        Lists.newArrayList(ColumnType.STRING, ColumnType.STRING), ColumnType.BOOLEAN));
    db.addBuiltin(ScalarFunction.createBuiltinOperator(
        Operator.RLIKE.name(), "LikePredicate", "RegexFn",
        Lists.newArrayList(ColumnType.STRING, ColumnType.STRING), ColumnType.BOOLEAN));
    db.addBuiltin(ScalarFunction.createBuiltinOperator(
        Operator.REGEXP.name(), "LikePredicate", "RegexFn",
        Lists.newArrayList(ColumnType.STRING, ColumnType.STRING), ColumnType.BOOLEAN));
  }

  private final Operator op_;

  public LikePredicate(Operator op, Expr e1, Expr e2) {
    super();
    this.op_ = op;
    Preconditions.checkNotNull(e1);
    children_.add(e1);
    Preconditions.checkNotNull(e2);
    children_.add(e2);
    // TODO: improve with histograms?
    selectivity_ = 0.1;
  }

  @Override
  public boolean equals(Object obj) {
    if (!super.equals(obj)) {
      return false;
    }
    return ((LikePredicate) obj).op_ == op_;
  }

  @Override
  public String toSqlImpl() {
    return getChild(0).toSql() + " " + op_.toString() + " " + getChild(1).toSql();
  }

  @Override
  protected void toThrift(TExprNode msg) {
    msg.node_type = TExprNodeType.LIKE_PRED;
    msg.like_pred = new TLikePredicate("\\");
  }

  @Override
  public void analyze(Analyzer analyzer) throws AnalysisException,
      AuthorizationException {
    if (isAnalyzed_) return;
    super.analyze(analyzer);
    if (!getChild(0).getType().isStringType() && !getChild(0).getType().isNull()) {
      throw new AnalysisException(
          "left operand of " + op_.toString() + " must be of type STRING: " + toSql());
    }
    if (!getChild(1).getType().isStringType() && !getChild(1).getType().isNull()) {
      throw new AnalysisException(
          "right operand of " + op_.toString() + " must be of type STRING: " + toSql());
    }

    fn_ = getBuiltinFunction(analyzer, op_.toString(), collectChildReturnTypes(),
        CompareMode.IS_SUBTYPE);
    Preconditions.checkState(fn_ != null);
    Preconditions.checkState(fn_.getReturnType().isBoolean());

    if (!getChild(1).getType().isNull() && getChild(1).isLiteral()
        && (op_ == Operator.RLIKE || op_ == Operator.REGEXP)) {
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
