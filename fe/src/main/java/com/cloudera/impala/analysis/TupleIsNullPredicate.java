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

import java.util.List;
import java.util.Set;

import com.cloudera.impala.common.AnalysisException;
import com.cloudera.impala.thrift.TExprNode;
import com.cloudera.impala.thrift.TExprNodeType;
import com.cloudera.impala.thrift.TTupleIsNullPredicate;
import com.google.common.base.Preconditions;
import com.google.common.collect.Sets;

/**
 * Internal expr that returns true if all of the given tuples are NULL, otherwise false.
 * Used to make exprs originating from an inline view nullable in an outer join.
 * The given tupleIds must be materialized but not necessarily nullable at the
 * appropriate PlanNode. It is important not to require nullability of the tuples
 * because some exprs may be wrapped in a TupleIsNullPredicate that contain
 * SlotRefs on non-nullable tuples, e.g., an expr in the On-clause of an outer join
 * that refers to an outer-joined inline view (see IMPALA-904).
 *
 */
public class TupleIsNullPredicate extends Predicate {
  private final Set<TupleId> tupleIds_;
  private Analyzer analyzer_;

  public TupleIsNullPredicate(List<TupleId> tupleIds) {
    Preconditions.checkState(tupleIds != null && !tupleIds.isEmpty());
    this.tupleIds_ = Sets.newHashSet(tupleIds);
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
  public void analyze(Analyzer analyzer) throws AnalysisException {
    if (isAnalyzed_) return;
    super.analyze(analyzer);
    analyzer_ = analyzer;
  }

  @Override
  protected void toThrift(TExprNode msg) {
    msg.node_type = TExprNodeType.TUPLE_IS_NULL_PRED;
    msg.tuple_is_null_pred = new TTupleIsNullPredicate();
    Preconditions.checkNotNull(analyzer_);
    for (TupleId tid: tupleIds_) {
      // Check that all referenced tuples are materialized.
      TupleDescriptor tupleDesc = analyzer_.getTupleDesc(tid);
      Preconditions.checkNotNull(tupleDesc, "Unknown tuple id: " + tid.toString());
      Preconditions.checkState(tupleDesc.isMaterialized(),
          String.format("Illegal reference to non-materialized tuple: tid=%s", tid));
      msg.tuple_is_null_pred.addToTuple_ids(tid.asInt());
    }
  }

  @Override
  public boolean equals(Object o) {
    if (!super.equals(o)) return false;
    if (!(o instanceof TupleIsNullPredicate)) return false;
    TupleIsNullPredicate other = (TupleIsNullPredicate) o;
    return other.tupleIds_.containsAll(tupleIds_) &&
        tupleIds_.containsAll(other.tupleIds_);
  }

  @Override
  protected String toSqlImpl() { return "TupleIsNull()"; }
  public Set<TupleId> getTupleIds() { return tupleIds_; }

  @Override
  public Expr clone() { return new TupleIsNullPredicate(this); }
}
