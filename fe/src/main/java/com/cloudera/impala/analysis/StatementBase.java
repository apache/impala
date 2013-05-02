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

import com.cloudera.impala.catalog.AuthorizationException;
import com.cloudera.impala.common.AnalysisException;

/*
 * Base class for all Impala SQL statements.
 */
abstract class StatementBase implements ParseNode {
  public void analyze(Analyzer analyzer) throws AnalysisException,
      AuthorizationException {
    throw new AnalysisException("not implemented");
  }

  /* Print SQL syntax corresponding to this node.
   * @see com.cloudera.impala.parser.ParseNode#toSql()
   */
  public String toSql() {
    return "";
  }

  /* Print debug string.
   * @see com.cloudera.impala.parser.ParseNode#debugString()
   */
  public String debugString() {
    return "";
  }
}