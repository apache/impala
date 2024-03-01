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

package org.apache.impala.planner;

import java.util.List;

import org.apache.impala.analysis.Expr;
import org.apache.impala.catalog.FeFsTable;
import org.apache.impala.catalog.FeHBaseTable;
import org.apache.impala.catalog.FeIcebergTable;
import org.apache.impala.catalog.FeKuduTable;
import org.apache.impala.catalog.FeTable;
import org.apache.impala.common.Pair;
import org.apache.impala.thrift.TSinkAction;
import org.apache.impala.thrift.TSortingOrder;
import org.apache.impala.util.ExprUtil;

import com.google.common.base.Preconditions;

/**
 * A DataSink that writes into a table.
 *
 */
public abstract class TableSink extends DataSink {

  /**
   * Enum to specify the sink operation type.
   */
  public enum Op {
    INSERT {
      @Override
      public String toExplainString() { return "INSERT INTO"; }

      @Override
      public TSinkAction toThrift() { return TSinkAction.INSERT; }
    },
    UPDATE {
      @Override
      public String toExplainString() { return "UPDATE"; }

      @Override
      public TSinkAction toThrift() { return TSinkAction.UPDATE; }
    },
    UPSERT {
      @Override
      public String toExplainString() { return "UPSERT INTO"; }

      @Override
      public TSinkAction toThrift() { return TSinkAction.UPSERT; }
    },
    DELETE {
      @Override
      public String toExplainString() { return "DELETE FROM"; }

      @Override
      public TSinkAction toThrift() { return TSinkAction.DELETE; }
    };

    public abstract String toExplainString();

    public abstract TSinkAction toThrift();
  }

  // Table which is to be populated by this sink.
  protected final FeTable targetTable_;
  // The type of operation to be performed by this sink.
  protected final Op sinkOp_;
  // One expression per result column for the query. Always non-null.
  protected final List<Expr> outputExprs_;

  public TableSink(FeTable targetTable, Op sinkAction, List<Expr> outputExprs) {
    Preconditions.checkState(outputExprs != null);
    targetTable_ = targetTable;
    sinkOp_ = sinkAction;
    outputExprs_ = outputExprs;
  }

  /**
   * Returns an output sink appropriate for writing to the given table.
   * Not all Ops are supported for all tables.
   * All parameters must be non-null, the lists in particular need to be empty if they
   * don't make sense for a certain table type.
   * For HDFS tables 'sortProperties' specifies two things, the indices into the list of
   * non-clustering columns of the target table that are stored in the 'sort.columns'
   * table property, and the sorting order.
   */
  public static TableSink create(FeTable table, Op sinkAction,
      List<Expr> partitionKeyExprs, List<Expr> outputExprs,
      List<Integer> referencedColumns, boolean overwrite,
      boolean inputIsClustered, Pair<List<Integer>, TSortingOrder> sortProperties) {
    return create(table, sinkAction, partitionKeyExprs, outputExprs, referencedColumns,
        overwrite, inputIsClustered, sortProperties, -1, null, 0, false);
  }

  /**
   * Same as above, plus it takes an ACID write id 'writeId' and Kudu transaction token
   * 'kuduTxnToken' in parameter.
   */
  public static TableSink create(FeTable table, Op sinkAction,
      List<Expr> partitionKeyExprs, List<Expr> outputExprs,
      List<Integer> referencedColumns, boolean overwrite, boolean inputIsClustered,
      Pair<List<Integer>, TSortingOrder> sortProperties, long writeId,
      java.nio.ByteBuffer kuduTxnToken, int maxTableSinks) {
    return create(table, sinkAction, partitionKeyExprs, outputExprs, referencedColumns,
        overwrite, inputIsClustered, sortProperties, writeId, kuduTxnToken, maxTableSinks,
        false);
  }

  public static TableSink create(FeTable table, Op sinkAction,
      List<Expr> partitionKeyExprs, List<Expr> outputExprs,
      List<Integer> referencedColumns, boolean overwrite, boolean inputIsClustered,
      Pair<List<Integer>, TSortingOrder> sortProperties, long writeId,
      java.nio.ByteBuffer kuduTxnToken, int maxTableSinks, boolean isResultSink) {
    Preconditions.checkNotNull(partitionKeyExprs);
    Preconditions.checkNotNull(referencedColumns);
    Preconditions.checkNotNull(sortProperties.first);
    if (table instanceof FeIcebergTable) {
      if (sinkAction == Op.INSERT) {
        return new HdfsTableSink(table, partitionKeyExprs,outputExprs, overwrite,
            inputIsClustered, sortProperties, writeId, maxTableSinks, isResultSink);
      } else {
        // Other SINK actions are either not supported or created directly.
        Preconditions.checkState(false);
      }
    }
    if (table instanceof FeFsTable) {
      // Hdfs only supports inserts.
      Preconditions.checkState(sinkAction == Op.INSERT);
      // Referenced columns don't make sense for an Hdfs table.
      Preconditions.checkState(referencedColumns.isEmpty());
      return new HdfsTableSink(table, partitionKeyExprs,outputExprs, overwrite,
          inputIsClustered, sortProperties, writeId, maxTableSinks, isResultSink);
    } else if (table instanceof FeHBaseTable) {
      // HBase only supports inserts.
      Preconditions.checkState(sinkAction == Op.INSERT);
      // Partition clause doesn't make sense for an HBase table.
      Preconditions.checkState(partitionKeyExprs.isEmpty());
      // HBase doesn't have a way to perform INSERT OVERWRITE
      Preconditions.checkState(overwrite == false);
      // Referenced columns don't make sense for an HBase table.
      Preconditions.checkState(referencedColumns.isEmpty());
      // Sort columns are not supported for HBase tables.
      Preconditions.checkState(sortProperties.first.isEmpty());
      // Create the HBaseTableSink and return it.
      return new HBaseTableSink(table, outputExprs);
    } else if (table instanceof FeKuduTable) {
      // Kudu doesn't have a way to perform INSERT OVERWRITE.
      Preconditions.checkState(overwrite == false);
      // Sort columns are not supported for Kudu tables.
      Preconditions.checkState(sortProperties.first.isEmpty());
      return new KuduTableSink(
          table, sinkAction, referencedColumns, outputExprs, kuduTxnToken);
    } else {
      throw new UnsupportedOperationException(
          "Cannot create data sink into table of type: " + table.getClass().getName());
    }
  }

  protected ProcessingCost computeDefaultProcessingCost() {
    // TODO: consider including materialization cost into the returned cost.
    return ProcessingCost.basicCost(getLabel(), fragment_.getPlanRoot().getCardinality(),
        ExprUtil.computeExprsTotalCost(outputExprs_));
  }
}
