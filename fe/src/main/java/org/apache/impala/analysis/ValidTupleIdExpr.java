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

import java.util.List;
import java.util.Objects;
import java.util.Set;

import org.apache.impala.catalog.Type;
import org.apache.impala.common.AnalysisException;
import org.apache.impala.thrift.TExprNode;
import org.apache.impala.thrift.TExprNodeType;

import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.collect.Sets;

/**
 * Internal expr that returns the id of the single non-NULL tuple in the input row.
 * Valid input rows must have exactly one non-NULL tuple.
 */
public class ValidTupleIdExpr extends Expr {
  private final Set<TupleId> tupleIds_;
  private Analyzer analyzer_;

  public ValidTupleIdExpr(List<TupleId> tupleIds) {
    Preconditions.checkState(tupleIds != null && !tupleIds.isEmpty());
    tupleIds_ = Sets.newHashSet(tupleIds);
  }

  /**
   * Copy c'tor used in clone().
   */
  protected ValidTupleIdExpr(ValidTupleIdExpr other) {
    super(other);
    tupleIds_ = Sets.newHashSet(other.tupleIds_);
    analyzer_ = other.analyzer_;
  }

  @Override
  protected void analyzeImpl(Analyzer analyzer) throws AnalysisException {
    analyzer_ = analyzer;
    type_ = Type.INT;
  }

  @Override
  protected float computeEvalCost() { return tupleIds_.size() * IS_NULL_COST; }

  @Override
  protected void toThrift(TExprNode msg) {
    msg.node_type = TExprNodeType.VALID_TUPLE_ID_EXPR;
    Preconditions.checkNotNull(analyzer_);
    for (TupleId tid : tupleIds_) {
      // Check that all referenced tuples are materialized.
      TupleDescriptor tupleDesc = analyzer_.getTupleDesc(tid);
      Preconditions.checkNotNull(tupleDesc, "Unknown tuple id: " + tid.toString());
      Preconditions.checkState(tupleDesc.isMaterialized(),
          String.format("Illegal reference to non-materialized tuple: tid=%s", tid));
    }
  }

  @Override
  protected boolean localEquals(Expr that) {
    if (!super.localEquals(that)) return false;
    ValidTupleIdExpr other = (ValidTupleIdExpr) that;
    return other.tupleIds_.containsAll(tupleIds_)
        && tupleIds_.containsAll(other.tupleIds_);
  }

  @Override
  protected int localHash() {
    return Objects.hash(super.localHash(), tupleIds_);
  }

  @Override
  protected String toSqlImpl(ToSqlOptions options) {
    return "valid_tid(" + Joiner.on(",").join(tupleIds_) + ")";
  }

  @Override
  public boolean isBoundByTupleIds(List<TupleId> tids) {
    return tids.containsAll(tupleIds_);
  }

  @Override
  protected boolean isConstantImpl() { return false; }

  @Override
  public Expr clone() { return new ValidTupleIdExpr(this); }
}
