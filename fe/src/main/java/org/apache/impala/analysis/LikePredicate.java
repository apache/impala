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

import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;

import org.apache.impala.catalog.Db;
import org.apache.impala.catalog.Function.CompareMode;
import org.apache.impala.catalog.ScalarFunction;
import org.apache.impala.catalog.Type;
import org.apache.impala.common.AnalysisException;
import org.apache.impala.thrift.TExprNode;
import org.apache.impala.thrift.TExprNodeType;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;

public class LikePredicate extends Predicate {
  enum Operator {
    LIKE("LIKE"),
    ILIKE("ILIKE"),
    RLIKE("RLIKE"),
    REGEXP("REGEXP"),
    IREGEXP("IREGEXP");

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
    db.addBuiltin(ScalarFunction.createBuiltin(
        Operator.LIKE.name(), Lists.<Type>newArrayList(Type.STRING, Type.STRING),
        false, Type.BOOLEAN, "_ZN6impala13LikePredicate4LikeEPN10impala_udf15FunctionContextERKNS1_9StringValES6_",
        "_ZN6impala13LikePredicate11LikePrepareEPN10impala_udf15FunctionContextENS2_18FunctionStateScopeE",
        "_ZN6impala13LikePredicate9LikeCloseEPN10impala_udf15FunctionContextENS2_18FunctionStateScopeE", true));
    db.addBuiltin(ScalarFunction.createBuiltin(
        Operator.ILIKE.name(), Lists.<Type>newArrayList(Type.STRING, Type.STRING),
        false, Type.BOOLEAN, "_ZN6impala13LikePredicate4LikeEPN10impala_udf15FunctionContextERKNS1_9StringValES6_",
        "_ZN6impala13LikePredicate12ILikePrepareEPN10impala_udf15FunctionContextENS2_18FunctionStateScopeE",
        "_ZN6impala13LikePredicate9LikeCloseEPN10impala_udf15FunctionContextENS2_18FunctionStateScopeE", true));
    db.addBuiltin(ScalarFunction.createBuiltin(
        Operator.RLIKE.name(), Lists.<Type>newArrayList(Type.STRING, Type.STRING),
        false, Type.BOOLEAN, "_ZN6impala13LikePredicate5RegexEPN10impala_udf15FunctionContextERKNS1_9StringValES6_",
        "_ZN6impala13LikePredicate12RegexPrepareEPN10impala_udf15FunctionContextENS2_18FunctionStateScopeE",
        "_ZN6impala13LikePredicate10RegexCloseEPN10impala_udf15FunctionContextENS2_18FunctionStateScopeE", true));
    db.addBuiltin(ScalarFunction.createBuiltin(
        Operator.REGEXP.name(), Lists.<Type>newArrayList(Type.STRING, Type.STRING),
        false, Type.BOOLEAN, "_ZN6impala13LikePredicate5RegexEPN10impala_udf15FunctionContextERKNS1_9StringValES6_",
        "_ZN6impala13LikePredicate12RegexPrepareEPN10impala_udf15FunctionContextENS2_18FunctionStateScopeE",
        "_ZN6impala13LikePredicate10RegexCloseEPN10impala_udf15FunctionContextENS2_18FunctionStateScopeE", true));
    db.addBuiltin(ScalarFunction.createBuiltin(
        Operator.IREGEXP.name(), Lists.<Type>newArrayList(Type.STRING, Type.STRING),
        false, Type.BOOLEAN, "_ZN6impala13LikePredicate5RegexEPN10impala_udf15FunctionContextERKNS1_9StringValES6_",
        "_ZN6impala13LikePredicate13IRegexPrepareEPN10impala_udf15FunctionContextENS2_18FunctionStateScopeE",
        "_ZN6impala13LikePredicate10RegexCloseEPN10impala_udf15FunctionContextENS2_18FunctionStateScopeE", true));
  }

  private final Operator op_;

  public LikePredicate(Operator op, Expr e1, Expr e2) {
    super();
    this.op_ = op;
    Preconditions.checkNotNull(e1);
    children_.add(e1);
    Preconditions.checkNotNull(e2);
    children_.add(e2);
  }

  /**
   * Copy c'tor used in clone().
   */
  public LikePredicate(LikePredicate other) {
    super(other);
    op_ = other.op_;
  }

  @Override
  public boolean equals(Object obj) {
    if (!super.equals(obj)) return false;
    return ((LikePredicate) obj).op_ == op_;
  }

  @Override
  public String toSqlImpl() {
    return getChild(0).toSql() + " " + op_.toString() + " " + getChild(1).toSql();
  }

  @Override
  protected void toThrift(TExprNode msg) {
    msg.node_type = TExprNodeType.FUNCTION_CALL;
  }

  @Override
  protected void analyzeImpl(Analyzer analyzer) throws AnalysisException {
    super.analyzeImpl(analyzer);
    if (!getChild(0).getType().isStringType() && !getChild(0).getType().isNull()) {
      throw new AnalysisException(
          "left operand of " + op_.toString() + " must be of type STRING: " + toSql());
    }
    if (!getChild(1).getType().isStringType() && !getChild(1).getType().isNull()) {
      throw new AnalysisException(
          "right operand of " + op_.toString() + " must be of type STRING: " + toSql());
    }

    fn_ = getBuiltinFunction(analyzer, op_.toString(), collectChildReturnTypes(),
        CompareMode.IS_NONSTRICT_SUPERTYPE_OF);
    Preconditions.checkState(fn_ != null);
    Preconditions.checkState(fn_.getReturnType().isBoolean());

    if (getChild(1).isLiteral() && !getChild(1).isNullLiteral()
        && (op_ == Operator.RLIKE || op_ == Operator.REGEXP || op_ == Operator.IREGEXP)) {
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
    castForFunctionCall(false);
  }

  @Override
  protected float computeEvalCost() {
    if (!hasChildCosts()) return UNKNOWN_COST;
    if (getChild(1).isLiteral() && !getChild(1).isNullLiteral() &&
      Pattern.matches("[%_]*[^%_]*[%_]*", ((StringLiteral) getChild(1)).getValue())) {
      // This pattern only has wildcards as leading or trailing character,
      // so it is linear.
      return getChildCosts() +
          (float) (getAvgStringLength(getChild(0)) + getAvgStringLength(getChild(1)) *
              BINARY_PREDICATE_COST) + LIKE_COST;
    } else {
      // This pattern is more expensive, so calculate its cost as quadratic.
      return getChildCosts() +
          (float) (getAvgStringLength(getChild(0)) * getAvgStringLength(getChild(1)) *
              BINARY_PREDICATE_COST) + LIKE_COST;
    }
  }

  @Override
  public Expr clone() { return new LikePredicate(this); }

  public Operator getOp() { return op_; }
}
