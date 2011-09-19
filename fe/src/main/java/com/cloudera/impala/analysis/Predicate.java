// Copyright (c) 2011 Cloudera, Inc. All rights reserved.

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
      list.addAll(((Predicate) getChild(0)).getConjuncts());
      list.addAll(((Predicate) getChild(1)).getConjuncts());
    } else {
      list.add(this);
    }
    return list;
  }
}
