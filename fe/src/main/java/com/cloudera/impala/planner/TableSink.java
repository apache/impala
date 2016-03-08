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

package com.cloudera.impala.planner;

import com.cloudera.impala.analysis.Expr;
import com.cloudera.impala.catalog.HBaseTable;
import com.cloudera.impala.catalog.HdfsTable;
import com.cloudera.impala.catalog.KuduTable;
import com.cloudera.impala.catalog.Table;
import com.cloudera.impala.thrift.TSinkAction;
import com.google.common.base.Preconditions;

import java.util.List;

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
  protected final Table targetTable_;
  // The type of operation to be performed by this sink.
  protected final Op sinkOp_;

  public TableSink(Table targetTable, Op sinkAction) {
    targetTable_ = targetTable;
    sinkOp_ = sinkAction;
  }

  /**
   * Returns an output sink appropriate for writing to the given table.
   * Not all Ops are supported for all tables.
   * All parameters must be non-null, the lists in particular need to be empty if they
   * don't make sense for a certain table type.
   */
  public static TableSink create(Table table, Op sinkAction,
      List<Expr> partitionKeyExprs,  List<Integer> referencedColumns,
      boolean overwrite, boolean ignoreDuplicates) {
    if (table instanceof HdfsTable) {
      // Hdfs only supports inserts.
      Preconditions.checkState(sinkAction == Op.INSERT);
      // Referenced columns don't make sense for an Hdfs table.
      Preconditions.checkState(referencedColumns.isEmpty());
      return new HdfsTableSink(table, partitionKeyExprs, overwrite);
    } else if (table instanceof HBaseTable) {
      // HBase only supports inserts.
      Preconditions.checkState(sinkAction == Op.INSERT);
      // Partition clause doesn't make sense for an HBase table.
      Preconditions.checkState(partitionKeyExprs.isEmpty());
      // HBase doesn't have a way to perform INSERT OVERWRITE
      Preconditions.checkState(overwrite == false);
      // Referenced columns don't make sense for an HBase table.
      Preconditions.checkState(referencedColumns.isEmpty());
      // Create the HBaseTableSink and return it.
      return new HBaseTableSink(table);
    } else if (table instanceof KuduTable) {
      // Kudu doesn't have a way to perform INSERT OVERWRITE.
      Preconditions.checkState(overwrite == false);
      // Partition clauses don't make sense for Kudu inserts.
      Preconditions.checkState(partitionKeyExprs.isEmpty());
      return new KuduTableSink(table, sinkAction, referencedColumns, ignoreDuplicates);
    } else {
      throw new UnsupportedOperationException(
          "Cannot create data sink into table of type: " + table.getClass().getName());
    }
  }
}
