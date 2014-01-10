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

import java.util.List;

import com.cloudera.impala.analysis.Expr;
import com.cloudera.impala.catalog.HBaseTable;
import com.cloudera.impala.catalog.HdfsTable;
import com.cloudera.impala.catalog.Table;
import com.cloudera.impala.thrift.TDataSink;
import com.cloudera.impala.thrift.TExplainLevel;
import com.google.common.base.Preconditions;

/**
 * A DataSink describes the destination of a plan fragment's output rows.
 * The destination could be another plan fragment on a remote machine,
 * or a table into which the rows are to be inserted
 * (i.e., the destination of the last fragment of an INSERT statement).
 *
 */
public abstract class DataSink {

  // estimated per-host memory requirement for sink;
  // set in computeCosts(); invalid: -1
  protected long perHostMemCost_ = -1;

  // Fragment that this DataSink belongs to. Set by the PlanFragment enclosing this sink.
  protected PlanFragment fragment_;

  /**
   * Return an explain string for the DataSink. Each line of the explain will be prefixed
   * by "prefix".
   */
  public abstract String getExplainString(String prefix, String detailPrefix,
      TExplainLevel explainLevel);

  protected abstract TDataSink toThrift();

  public void setFragment(PlanFragment fragment) { fragment_ = fragment; }
  public PlanFragment getFragment() { return fragment_; }
  public long getPerHostMemCost() { return perHostMemCost_; }

  /**
   * Returns an output sink appropriate for writing to the given table.
   */
  public static DataSink createDataSink(Table table, List<Expr> partitionKeyExprs,
      boolean overwrite) {
    if (table instanceof HdfsTable) {
      return new HdfsTableSink(table, partitionKeyExprs, overwrite);
    } else if (table instanceof HBaseTable) {
      // Partition clause doesn't make sense for an HBase table.
      Preconditions.checkState(partitionKeyExprs.isEmpty());
      // HBase doesn't have a way to perform INSERT OVERWRITE
      Preconditions.checkState(overwrite == false);
      // Create the HBaseTableSink and return it.
      return new HBaseTableSink(table);
    } else {
      throw new UnsupportedOperationException(
          "Cannot create data sink into table of type: " + table.getClass().getName());
    }
  }

  /**
   * Estimates the cost of executing this DataSink. Currently only sets perHostMemCost.
   */
  public void computeCosts() {
    perHostMemCost_ = 0;
  }
}
