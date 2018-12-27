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

import org.apache.impala.catalog.FeKuduTable;
import org.apache.impala.catalog.Type;
import org.apache.impala.common.AnalysisException;
import org.apache.impala.thrift.TExprNode;
import org.apache.impala.thrift.TExprNodeType;
import org.apache.impala.thrift.TKuduPartitionExpr;

import com.google.common.base.Preconditions;

/**
 * Internal expr that calls into the Kudu client to determine the partition index for
 * a given row. Returns -1 for rows that do not correspond to a partition. The children of
 * this Expr produce the values for the partition columns.
 */
public class KuduPartitionExpr extends Expr {
  // The table to use the partitioning scheme from.
  private final int targetTableId_;
  private final FeKuduTable targetTable_;
  // Maps from this Epxrs children to column positions in the table, i.e. children_[i]
  // produces the value for column partitionColPos_[i].
  private List<Integer> partitionColPos_;

  public KuduPartitionExpr(int targetTableId, FeKuduTable targetTable,
      List<Expr> partitionKeyExprs, List<Integer> partitionKeyIdxs) {
    Preconditions.checkState(partitionKeyExprs.size() == partitionKeyIdxs.size());
    targetTableId_ = targetTableId;
    targetTable_ = targetTable;
    partitionColPos_ = partitionKeyIdxs;
    children_.addAll(Expr.cloneList(partitionKeyExprs));
  }

  /**
   * Copy c'tor used in clone().
   */
  protected KuduPartitionExpr(KuduPartitionExpr other) {
    super(other);
    targetTableId_ = other.targetTableId_;
    targetTable_ = other.targetTable_;
    partitionColPos_ = other.partitionColPos_;
  }

  @Override
  protected void analyzeImpl(Analyzer analyzer) throws AnalysisException {
    type_ = Type.INT;
    // IMPALA-5294: If one of the children is a NullLiteral, it has to be cast to a type
    // to be passed to the BE.
    for (int i = 0; i < children_.size(); ++i) {
      children_.set(i, children_.get(i).castTo(
          targetTable_.getColumns().get(partitionColPos_.get(i)).getType()));
    }
  }

  @Override
  protected float computeEvalCost() { return UNKNOWN_COST; }

  @Override
  protected String toSqlImpl(ToSqlOptions options) {
    StringBuilder sb = new StringBuilder("KuduPartition(");
    for (int i = 0; i < children_.size(); ++i) {
      if (i != 0) sb.append(", ");
      sb.append(children_.get(i).toSql(options));
    }
    sb.append(")");
    return sb.toString();
  }

  @Override
  protected void toThrift(TExprNode msg) {
    msg.node_type = TExprNodeType.KUDU_PARTITION_EXPR;
    msg.kudu_partition_expr = new TKuduPartitionExpr();
    for (int i = 0; i < children_.size(); ++i) {
      msg.kudu_partition_expr.addToReferenced_columns(partitionColPos_.get(i));
    }
    msg.kudu_partition_expr.setTarget_table_id(targetTableId_);
  }

  @Override
  public Expr clone() { return new KuduPartitionExpr(this); }
}
