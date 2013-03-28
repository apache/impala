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

import com.cloudera.impala.catalog.Table;
import com.cloudera.impala.common.AnalysisException;
import com.cloudera.impala.common.Reference;
import com.cloudera.impala.thrift.TExprNode;
import com.cloudera.impala.thrift.TExprNodeType;
import com.cloudera.impala.thrift.TIsNullPredicate;
import com.google.common.base.Preconditions;

public class IsNullPredicate extends Predicate {
  private final boolean isNotNull;

  public IsNullPredicate(Expr e, boolean isNotNull) {
    super();
    this.isNotNull = isNotNull;
    Preconditions.checkNotNull(e);
    children.add(e);
  }

  @Override
  public boolean equals(Object obj) {
    if (!super.equals(obj)) {
      return false;
    }
    return ((IsNullPredicate) obj).isNotNull == isNotNull;
  }

  @Override
  public String toSql() {
    return getChild(0).toSql() + (isNotNull ? " IS NOT NULL" : " IS NULL");
  }

  @Override
  public void analyze(Analyzer analyzer) throws AnalysisException {
    super.analyze(analyzer);

    // determine selectivity
    Reference<SlotRef> slotRefRef = new Reference<SlotRef>();
    if (isSingleColumnPredicate(slotRefRef, null)) {
      SlotDescriptor slotDesc = slotRefRef.getRef().getDesc();
      Table table = slotDesc.getParent().getTable();
      if (table != null && table.getNumRows() > 0) {
        long numRows = table.getNumRows();
        if (isNotNull) {
          selectivity =
              (double) (numRows - slotDesc.getStats().getNumNulls()) / (double) numRows;
        } else {
          selectivity = (double) slotDesc.getStats().getNumNulls() / (double) numRows;
        }
        selectivity = Math.max(0.0, Math.min(1.0, selectivity));
      }
    } else {
      // TODO: increase this to make sure we don't end up favoring broadcast joins
      // due to underestimated cardinalities?
      selectivity = 0.1;
    }
  }

  @Override
  protected void toThrift(TExprNode msg) {
    msg.node_type = TExprNodeType.IS_NULL_PRED;
    msg.is_null_pred = new TIsNullPredicate(isNotNull);
  }

}
