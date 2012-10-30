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

import com.cloudera.impala.catalog.PrimitiveType;
import com.cloudera.impala.common.AnalysisException;
import com.google.common.collect.Lists;

public abstract class Predicate extends Expr {
  protected boolean isEqJoinConjunct;

  public Predicate() {
    super();
    this.isEqJoinConjunct = false;
  }

  public boolean isEqJoinConjunct() {
    return isEqJoinConjunct;
  }

  public void setIsEqJoinConjunct(boolean v) {
    isEqJoinConjunct = v;
  }

  @Override
  public void analyze(Analyzer analyzer) throws AnalysisException {
    super.analyze(analyzer);
    type = PrimitiveType.BOOLEAN;
  }

  public List<Predicate> getConjuncts() {
    List<Predicate> list = Lists.newArrayList();
    if (this instanceof CompoundPredicate
        && ((CompoundPredicate) this).getOp() == CompoundPredicate.Operator.AND) {
      // TODO: we have to convert CompoundPredicate.AND to two expr trees for
      // conjuncts because NULLs are handled differently for CompoundPredicate.AND
      // and conjunct evaluation.  This is not optimal for jitted exprs because it
      // will result in two functions instead of one. Create a new CompoundPredicate
      // Operator (i.e. CONJUNCT_AND) with the right NULL semantics and use that
      // instead
      list.addAll(((Predicate) getChild(0)).getConjuncts());
      list.addAll(((Predicate) getChild(1)).getConjuncts());
    } else {
      list.add(this);
    }
    return list;
  }
}
