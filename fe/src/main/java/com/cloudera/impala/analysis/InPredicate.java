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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.cloudera.impala.catalog.AuthorizationException;
import com.cloudera.impala.common.AnalysisException;
import com.cloudera.impala.common.Reference;
import com.cloudera.impala.thrift.TExprNode;
import com.cloudera.impala.thrift.TExprNodeType;
import com.cloudera.impala.thrift.TInPredicate;

public class InPredicate extends Predicate {
  private final static Logger LOG = LoggerFactory.getLogger(InPredicate.class);
  private final boolean isNotIn_;

  public boolean isNotIn() { return isNotIn_; }

  // First child is the comparison expr for which we
  // should check membership in the inList (the remaining children).
  public InPredicate(Expr compareExpr, List<Expr> inList, boolean isNotIn) {
    children_.add(compareExpr);
    children_.addAll(inList);
    this.isNotIn_ = isNotIn;
  }

  @Override
  public void analyze(Analyzer analyzer) throws AnalysisException,
      AuthorizationException {
    if (isAnalyzed_) return;
    super.analyze(analyzer);
    analyzer.castAllToCompatibleType(children_);

    Reference<SlotRef> slotRefRef = new Reference<SlotRef>();
    Reference<Integer> idxRef = new Reference<Integer>();
    if (isSingleColumnPredicate(slotRefRef, idxRef)
        && idxRef.getRef() == 0
        && slotRefRef.getRef().getNumDistinctValues() > 0) {
      selectivity_ = (double) (getChildren().size() - 1)
          / (double) slotRefRef.getRef().getNumDistinctValues();
      selectivity_ = Math.max(0.0, Math.min(1.0, selectivity_));
    } else {
      selectivity_ = Expr.DEFAULT_SELECTIVITY;
    }
  }

  @Override
  protected void toThrift(TExprNode msg) {
    msg.in_predicate = new TInPredicate(isNotIn_);
    msg.node_type = TExprNodeType.IN_PRED;
  }

  @Override
  public String toSqlImpl() {
    StringBuilder strBuilder = new StringBuilder();
    String notStr = (isNotIn_) ? "NOT " : "";
    strBuilder.append(getChild(0).toSql() + " " + notStr + "IN (");
    for (int i = 1; i < children_.size(); ++i) {
      strBuilder.append(getChild(i).toSql());
      strBuilder.append((i+1 != children_.size()) ? ", " : "");
    }
    strBuilder.append(")");
    return strBuilder.toString();
  }

  /*
   * If predicate is of the form "<SlotRef> [NOT] IN", returns the
   * SlotRef.
   */
  @Override
  public SlotRef getBoundSlot() {
    return getChild(0).unwrapSlotRef(true);
  }

  /**
   * Negates an InPredicate.
   */
  @Override
  public Expr negate() {
    return new InPredicate(getChild(0), children_.subList(1, children_.size()),
        !isNotIn_);
  }
}
