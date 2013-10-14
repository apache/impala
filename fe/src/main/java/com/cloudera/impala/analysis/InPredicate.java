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
  private final boolean isNotIn;

  // First child is the comparison expr for which we
  // should check membership in the inList (the remaining children).
  public InPredicate(Expr compareExpr, List<Expr> inList, boolean isNotIn) {
    children.add(compareExpr);
    children.addAll(inList);
    this.isNotIn = isNotIn;
  }

  @Override
  public void analyze(Analyzer analyzer) throws AnalysisException,
      AuthorizationException {
    if (isAnalyzed) return;
    super.analyze(analyzer);
    analyzer.castAllToCompatibleType(children);

    Reference<SlotRef> slotRefRef = new Reference<SlotRef>();
    Reference<Integer> idxRef = new Reference<Integer>();
    if (isSingleColumnPredicate(slotRefRef, idxRef)
        && idxRef.getRef() == 0
        && slotRefRef.getRef().getNumDistinctValues() > 0) {
      selectivity = (double) (getChildren().size() - 1)
          / (double) slotRefRef.getRef().getNumDistinctValues();
      selectivity = Math.max(0.0, Math.min(1.0, selectivity));
    } else {
      selectivity = Expr.defaultSelectivity;
    }
  }

  @Override
  protected void toThrift(TExprNode msg) {
    msg.in_predicate = new TInPredicate(isNotIn);
    msg.node_type = TExprNodeType.IN_PRED;
    msg.setOpcode(opcode);
  }

  @Override
  public String toSqlImpl() {
    StringBuilder strBuilder = new StringBuilder();
    String notStr = (isNotIn) ? "NOT " : "";
    strBuilder.append(getChild(0).toSql() + " " + notStr + "IN (");
    for (int i = 1; i < children.size(); ++i) {
      strBuilder.append(getChild(i).toSql());
      strBuilder.append((i+1 != children.size()) ? ", " : "");
    }
    strBuilder.append(")");
    return strBuilder.toString();
  }
}
