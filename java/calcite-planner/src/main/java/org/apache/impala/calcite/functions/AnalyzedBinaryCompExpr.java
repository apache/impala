/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.impala.calcite.functions;

import org.apache.impala.analysis.Analyzer;
import org.apache.impala.analysis.Expr;
import org.apache.impala.analysis.BinaryPredicate;
import org.apache.impala.catalog.Function;
import org.apache.impala.catalog.Type;
import org.apache.impala.common.AnalysisException;

/**
 * A BinaryPredicate that is always in analyzed state
 */
public class AnalyzedBinaryCompExpr extends BinaryPredicate {

  // Need to save the function because it is known at constructor time. The
  // resetAnalyzeState() method can be called at various points which could
  // set the fn_ member to null. So we save the function in the savedFunction_
  // variable so it can be properly set in analyzeImpl()
  private final Function savedFunction_;

  public AnalyzedBinaryCompExpr(Function fn, Operator op, Expr e1, Expr e2) {
    super(op, e1, e2);
    this.savedFunction_ = fn;
    this.type_ = Type.BOOLEAN;
  }

  public AnalyzedBinaryCompExpr(AnalyzedBinaryCompExpr other) {
    super(other);
    this.savedFunction_ = other.savedFunction_;
  }

  @Override
  public Expr clone() {
    return new AnalyzedBinaryCompExpr(this);
  }

  @Override
  protected void analyzeImpl(Analyzer analyzer) throws AnalysisException {
    this.computeSelectivity();
    this.fn_ = savedFunction_;
    this.type_ = Type.BOOLEAN;
  }

  @Override
  protected float computeEvalCost() {
    // TODO: IMPALA-13098: need to implement
    return 1;
  }
}
