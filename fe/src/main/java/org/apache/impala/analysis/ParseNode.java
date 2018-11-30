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

/**
 * Parse nodes divide into two broad categories: statement-like nodes (derived
 * from {@link StmtNode}) and expression nodes (which derive from @{link Expr}.)
 *
 * Statement-like nodes form the structure of a query and mostly retain their
 * structure during analysis. That is, they are "analyzed in place." By contrast,
 * expressions are often rewritten so that the final expression out of the analyzer
 * may be different than the expression received from the parser. As a result, the
 * analyze interface differs between the two categories.
 *
 * Error reporting often wants to emit the user's original SQL expression before
 * rewrites. Statements hold onto both the original and rewritten expressions.
 * Expressions, by contrast don't know if they are original or rewritten.
 *
 * Operations that affect all statement-like nodes are defined here; those that
 * affect all expressions are defined in Expr.
 */
public interface ParseNode {

  /**
   * Returns the SQL string corresponding to this node and its descendants.
   */
  String toSql(ToSqlOptions options);

  /**
   * Returns the SQL string corresponding to this node and its descendants.
   * This should return the same result as calling toSql(ToSqlOptions.DEFAULT).
   * TODO use an interface default method to implement this when we fully move to Java8.
   */
  String toSql();
}
