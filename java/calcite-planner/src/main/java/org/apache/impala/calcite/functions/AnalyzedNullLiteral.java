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

package org.apache.impala.calcite.functions;

import org.apache.impala.analysis.Analyzer;
import org.apache.impala.analysis.Expr;
import org.apache.impala.analysis.NullLiteral;
import org.apache.impala.catalog.Type;
import org.apache.impala.common.AnalysisException;
import org.apache.impala.common.ImpalaException;

/**
 * A NulLLiteral specialized for Calcite
 *
 * The analysisImpl is overriden for Calcite so that the NullLiteral
 * can be cast to the type that was determined by Calcite.
 */
public class AnalyzedNullLiteral extends NullLiteral {

  private final Type savedType_;

  public AnalyzedNullLiteral(Type type) {
    savedType_ = type;
  }

  public AnalyzedNullLiteral(AnalyzedNullLiteral other) {
    super(other);
    this.savedType_ = other.savedType_;
  }

  @Override
  public Expr clone() {
    return new AnalyzedNullLiteral(this);
  }

  @Override
  protected void analyzeImpl(Analyzer analyzer) throws AnalysisException {
    // resetAnalyzedState will remove the type so we use the savedType
    // from the constructor
    uncheckedCastTo(savedType_);
  }
}
