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
import org.apache.impala.common.ImpalaException;
import org.apache.impala.planner.DataSink;
import org.apache.impala.planner.PlanNode;
import org.apache.impala.planner.PlannerContext;
import org.apache.impala.thrift.TSortingOrder;

import java.util.List;

/**
 * Interface for merge implementations.
 */
public interface MergeImpl {
  /**
   * Analyzes the implementation specific parts of the MERGE statement.
   * @param analyzer analyzer of the MERGE statement
   * @throws AnalysisException if analysis fails
   */
  void analyze(Analyzer analyzer) throws AnalysisException;

  /**
   * Creates a data sink for the merge statement; for example, the Iceberg implementation
   * requires a specialized multi data sink to write data and delete files simultaneously.
   * @return DataSink
   */
  DataSink createDataSink();

  /**
   * Substitutes result expressions, partition key expression. Preserves the original
   * type of the expressions.
   * @param smap substitution map
   * @param analyzer analyzer of the MERGE statement
   */
  void substituteResultExprs(ExprSubstitutionMap smap, Analyzer analyzer);

  /**
   * Returns all affected target columns.
   * @return list of expressions
   */
  List<Expr> getResultExprs();

  /**
   * Returns partitioning expressions originated from the target table.
   * @return list of partition expressions
   */
  List<Expr> getPartitionKeyExprs();

  /**
   * Returns sort expressions originated from SORT BY property of the target table.
   * @return list of sort expressions
   */
  List<Expr> getSortExprs();

  /**
   * Returns the sorting order of the target table
   * @return sorting order
   */
  TSortingOrder getSortingOrder();

  /**
   * Creates the query statement that produces the result set that will be evaluated by
   * the merge cases, and later gets persisted by a data sink.
   * @return query statement
   */
  QueryStmt getQueryStmt();

  /**
   * Creates an implementation specific plan node that handles the case evaluation over
   * the result set returned by 'getQueryStmt'.
   * @param ctx planner context
   * @param child child node that supplies the data for this node, usually a join node
   * @param analyzer analyzer of the MERGE statement
   * @return implementation-specific PlanNode
   * @throws ImpalaException if the initialization of the plan node fails.
   */
  PlanNode getPlanNode(PlannerContext ctx, PlanNode child, Analyzer analyzer)
      throws ImpalaException;

  void reset();
}
