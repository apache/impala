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

import org.apache.impala.common.AnalysisException;

/**
 * Base interface for statements and statement-like nodes such as clauses.
 */
public abstract class StmtNode implements ParseNode {

  /**
   * Perform semantic analysis of node and all of its children.
   * @throws AnalysisException if any semantic errors were found
   */
  public abstract void analyze(Analyzer analyzer) throws AnalysisException;

  /**
   * By default, table masking is not performed. When authorization is enabled and
   * tbe base table/view has column-masking / row-filtering policies, the table ref
   * will be masked by a table masking view. Called after analyze().
   */
  public boolean resolveTableMask(Analyzer analyzer) throws AnalysisException {
    // Don't apply table mask by default.
    return false;
  }
}
