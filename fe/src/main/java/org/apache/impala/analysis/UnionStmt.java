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

package org.apache.impala.analysis;

import java.util.List;
import org.apache.impala.common.AnalysisException;

/**
 * Representation of a union.
 */
public class UnionStmt extends SetOperationStmt {
  /////////////////////////////////////////
  // BEGIN: Members that need to be reset()


  // END: Members that need to be reset()
  /////////////////////////////////////////

  public UnionStmt(List<SetOperand> operands,
      List<OrderByElement> orderByElements, LimitElement limitElement) {
    super(operands, orderByElements, limitElement);
  }

  /**
   * C'tor for cloning.
   */
  protected UnionStmt(SetOperationStmt other) {
    super(other);
  }

  @Override
  public void analyze(Analyzer analyzer) throws AnalysisException {
    if (isAnalyzed()) return;
    super.analyze(analyzer);
  }

  @Override
  public SetOperationStmt clone() {
    return new UnionStmt(this);
  }
}
