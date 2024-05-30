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

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Set;

import org.apache.impala.common.AnalysisException;
import org.apache.impala.common.InternalException;
import org.apache.impala.common.ThriftSerializationCtx;
import org.apache.impala.thrift.TExprNode;
import org.apache.impala.thrift.TExprNodeType;
import org.apache.impala.thrift.TTupleIsNullPredicate;

import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

/**
 * Internal expr that returns true if all of the given tuples are NULL, otherwise false.
 * Used to make exprs originating from an inline view nullable in an outer join.
 * The given tupleIds must be materialized but not necessarily nullable at the
 * appropriate PlanNode. It is important not to require nullability of the tuples
 * because some exprs may be wrapped in a TupleIsNullPredicate that contain
 * SlotRefs on non-nullable tuples, e.g., an expr in the On-clause of an outer join
 * that refers to an outer-joined inline view (see IMPALA-904).
 */
public class TupleIsNullPredicate extends Predicate {
  private final Set<TupleId> tupleIds_;
  private Analyzer analyzer_;

  public TupleIsNullPredicate(List<TupleId> tupleIds) {
    Preconditions.checkState(tupleIds != null && !tupleIds.isEmpty());
    this.tupleIds_ = Sets.newHashSet(tupleIds);
  }

  public TupleIsNullPredicate(TupleId tupleId) {
    this(Collections.singletonList(tupleId));
  }

  /**
   * Copy c'tor used in clone().
   */
  protected TupleIsNullPredicate(TupleIsNullPredicate other) {
    super(other);
    tupleIds_ = Sets.newHashSet(other.tupleIds_);
    analyzer_ = other.analyzer_;
  }

  @Override
  protected void analyzeImpl(Analyzer analyzer) throws AnalysisException {
    super.analyzeImpl(analyzer);
    analyzer_ = analyzer;
  }

  @Override
  protected float computeEvalCost() {
    return tupleIds_.size() * IS_NULL_COST;
  }

  @Override
  protected void toThrift(TExprNode msg) {
    Preconditions.checkState(false, "Unexpected use of old toThrift() signature");
  }

  @Override
  protected void toThrift(TExprNode msg, ThriftSerializationCtx serialCtx) {
    msg.node_type = TExprNodeType.TUPLE_IS_NULL_PRED;
    msg.tuple_is_null_pred = new TTupleIsNullPredicate();
    Preconditions.checkNotNull(analyzer_);
    for (TupleId tid: tupleIds_) {
      // Check that all referenced tuples are materialized.
      TupleDescriptor tupleDesc = analyzer_.getTupleDesc(tid);
      Preconditions.checkNotNull(tupleDesc, "Unknown tuple id: " + tid.toString());
      Preconditions.checkState(tupleDesc.isMaterialized(),
          String.format("Illegal reference to non-materialized tuple: tid=%s", tid));
      msg.tuple_is_null_pred.addToTuple_ids(serialCtx.translateTupleId(tid).asInt());
    }
  }

  @Override
  protected boolean localEquals(Expr that) {
    if (!super.localEquals(that)) return false;
    TupleIsNullPredicate other = (TupleIsNullPredicate) that;
    return other.tupleIds_.containsAll(tupleIds_) &&
        tupleIds_.containsAll(other.tupleIds_);
  }

  @Override
  protected int localHash() {
    return Objects.hash(super.localHash(), tupleIds_);
  }

  @Override
  protected String toSqlImpl(ToSqlOptions options) {
    return "TupleIsNull(" + Joiner.on(",").join(tupleIds_) + ")";
  }

  public Set<TupleId> getTupleIds() { return tupleIds_; }

  @Override
  public boolean isBoundByTupleIds(List<TupleId> tids) {
    return tids.containsAll(tupleIds_);
  }

  @Override
  protected boolean isConstantImpl() { return false; }

  /**
   * Makes each input expr nullable, if necessary, by wrapping it as follows:
   * IF(TupleIsNull(tids), NULL, expr)
   *
   * The given tids must be materialized. The given inputExprs are expected to be bound
   * by tids once fully substituted against base tables. However, inputExprs may not yet
   * be fully substituted at this point.
   *
   * Returns a new list with the nullable exprs.
   */
  public static List<Expr> wrapExprs(List<Expr> inputExprs,
      List<TupleId> tids, Analyzer analyzer) throws InternalException {
    // Assert that all tids are materialized.
    for (TupleId tid: tids) {
      TupleDescriptor tupleDesc = analyzer.getTupleDesc(tid);
      Preconditions.checkState(tupleDesc.isMaterialized());
    }
    // Perform the wrapping.
    List<Expr> result = Lists.newArrayListWithCapacity(inputExprs.size());
    for (Expr e: inputExprs) {
      result.add(wrapExpr(e, tids, analyzer));
    }
    return result;
  }

  /**
   * Returns a new analyzed conditional expr 'IF(TupleIsNull(tids), NULL, expr)',
   * if required to make expr nullable. Otherwise, returns expr.
   */
  public static Expr wrapExpr(Expr expr, List<TupleId> tids, Analyzer analyzer)
      throws InternalException {
    if (!requiresNullWrapping(expr, analyzer)) return expr;
    List<Expr> params = new ArrayList<>();
    params.add(new TupleIsNullPredicate(tids));
    params.add(new NullLiteral());
    params.add(expr);
    Expr ifExpr = new FunctionCallExpr("if", params);
    ifExpr.analyzeNoThrow(analyzer);
    return ifExpr;
  }

  /**
   * Returns true if the given expr evaluates to a non-NULL value if all its contained
   * SlotRefs evaluate to NULL, false otherwise.
   * Throws an InternalException if expr evaluation in the BE failed.
   */
  public static boolean requiresNullWrapping(Expr expr, Analyzer analyzer)
      throws InternalException {
    Preconditions.checkNotNull(expr);

    if (expr.getType().isComplexType()) {
      // Currently the only Expr supported for complex types is SlotRef, which does not
      // need NULL wrapping.
      Preconditions.checkState(expr instanceof SlotRef);
      return false;
    }

    // If the expr is already wrapped in an IF(TupleIsNull(), NULL, expr)
    // then it must definitely be wrapped again at this level.
    // Do not try to execute expr because a TupleIsNullPredicate is not constant.
    if (expr.contains(TupleIsNullPredicate.class)) return true;
    // Wrap expr with an IS NOT NULL predicate.
    Expr isNotNullLiteralPred = new IsNullPredicate(expr, true);
    // analyze to insert casts, etc.
    isNotNullLiteralPred.analyzeNoThrow(analyzer);
    return analyzer.isTrueWithNullSlots(isNotNullLiteralPred);
  }

  /**
   * Recursive function that replaces all 'IF(TupleIsNull(), NULL, e)' exprs in
   * 'expr' with e and returns the modified expr.
   */
  public static Expr unwrapExpr(Expr expr)  {
    if (expr instanceof FunctionCallExpr) {
      FunctionCallExpr fnCallExpr = (FunctionCallExpr) expr;
      List<Expr> params = fnCallExpr.getParams().exprs();
      if (fnCallExpr.getFnName().getFunction().equals("if") &&
          params.get(0) instanceof TupleIsNullPredicate &&
          Expr.IS_NULL_LITERAL.apply(params.get(1))) {
        return unwrapExpr(params.get(2));
      }
    }
    for (int i = 0; i < expr.getChildren().size(); ++i) {
      expr.setChild(i, unwrapExpr(expr.getChild(i)));
    }
    return expr;
  }

  @Override
  public Expr clone() { return new TupleIsNullPredicate(this); }

  // Return true since only tuples are involved during evaluation.
  @Override
  public boolean shouldConvertToCNF() { return true; }
}
