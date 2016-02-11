// Copyright 2016 Cloudera Inc.
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

import com.cloudera.impala.common.AnalysisException;
import com.cloudera.impala.thrift.TExprNode;
import com.cloudera.impala.thrift.TExprNodeType;
import com.google.common.base.Preconditions;

/**
 * Predicate that checks whether a collection is empty or not.
 * This predicate is not user-accessible from SQL, and may be
 * generated as a performance optimization for certain queries.
 * TODO: Pass this Predicate as a TExprNodeType.FUNCTION_CALL
 * to the BE just like the rest of our Predicates. This is not yet
 * done to avoid invasive changes required in FE/BE to deal with
 * resolution of functions with complex-types arguments,
 */
public class IsNotEmptyPredicate extends Predicate {

  public IsNotEmptyPredicate(Expr collectionExpr) {
    super();
    Preconditions.checkNotNull(collectionExpr);
    children_.add(collectionExpr);
  }

  @Override
  public void analyze(Analyzer analyzer) throws AnalysisException {
    if (isAnalyzed_) return;
    super.analyze(analyzer);
    if (!getChild(0).getType().isCollectionType()) {
      throw new AnalysisException("Operand must be a collection type: "
          + getChild(0).toSql() + " is of type " + getChild(0).getType());
    }
    // Avoid influencing cardinality estimates.
    selectivity_ = 1.0;
  }

  @Override
  public String toSqlImpl() { return "!empty(" + getChild(0).toSql() + ")"; }

  @Override
  protected void toThrift(TExprNode msg) {
    msg.node_type = TExprNodeType.IS_NOT_EMPTY_PRED;
  }

  @Override
  public Expr clone() { return new IsNotEmptyPredicate(getChild(0).clone()); }
}
