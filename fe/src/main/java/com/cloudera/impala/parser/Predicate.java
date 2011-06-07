// Copyright (c) 2011 Cloudera, Inc. All rights reserved.

package com.cloudera.impala.parser;

import com.cloudera.impala.catalog.PrimitiveType;

class Predicate extends Expr {
  public Predicate() {
    super();
  }

  @Override
  public void analyze(Analyzer analyzer) throws Analyzer.Exception {
    super.analyze(analyzer);
    type = PrimitiveType.BOOLEAN;
  }
}
