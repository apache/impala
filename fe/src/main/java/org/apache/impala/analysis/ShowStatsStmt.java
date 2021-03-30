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

import org.apache.impala.authorization.Privilege;
import org.apache.impala.catalog.FeFsTable;
import org.apache.impala.catalog.FeIcebergTable;
import org.apache.impala.catalog.FeKuduTable;
import org.apache.impala.catalog.FeTable;
import org.apache.impala.catalog.FeView;
import org.apache.impala.common.AnalysisException;
import org.apache.impala.thrift.TShowStatsOp;
import org.apache.impala.thrift.TShowStatsParams;

import com.google.common.base.Preconditions;

/**
 * Representation of a SHOW TABLE/COLUMN STATS statement for
 * displaying column and table/partition statistics for a given table.
 */
public class ShowStatsStmt extends StatementBase {
  protected final TShowStatsOp op_;
  protected final TableName tableName_;

  // Set during analysis.
  protected FeTable table_;

  public ShowStatsStmt(TableName tableName, TShowStatsOp op) {
    op_ = Preconditions.checkNotNull(op);
    tableName_ = Preconditions.checkNotNull(tableName);
  }

  @Override
  public String toSql(ToSqlOptions options) {
    return getSqlPrefix() + " " + tableName_.toString();
  }

  protected String getSqlPrefix() {
    if (op_ == TShowStatsOp.TABLE_STATS) {
      return "SHOW TABLE STATS";
    } else if (op_ == TShowStatsOp.COLUMN_STATS) {
      return "SHOW COLUMN STATS";
    } else if (op_ == TShowStatsOp.PARTITIONS) {
      return "SHOW PARTITIONS";
    } else if (op_ == TShowStatsOp.RANGE_PARTITIONS) {
      return "SHOW RANGE PARTITIONS";
    } else {
      Preconditions.checkState(false);
      return "";
    }
  }

  @Override
  public void collectTableRefs(List<TableRef> tblRefs) {
    tblRefs.add(new TableRef(tableName_.toPath(), null));
  }

  @Override
  public void analyze(Analyzer analyzer) throws AnalysisException {
    table_ = analyzer.getTable(tableName_, Privilege.VIEW_METADATA);
    Preconditions.checkNotNull(table_);
    if (table_ instanceof FeView) {
      throw new AnalysisException(String.format(
          "%s not applicable to a view: %s", getSqlPrefix(), table_.getFullName()));
    }
    if (table_ instanceof FeFsTable) {
      if (table_.getNumClusteringCols() == 0 && op_ == TShowStatsOp.PARTITIONS) {
        throw new AnalysisException("Table is not partitioned: " + table_.getFullName());
      }
      if (op_ == TShowStatsOp.RANGE_PARTITIONS) {
        throw new AnalysisException(getSqlPrefix() + " must target a Kudu table: " +
            table_.getFullName());
      }
    } else if (table_ instanceof FeKuduTable) {
      FeKuduTable kuduTable = (FeKuduTable) table_;
      if (op_ == TShowStatsOp.RANGE_PARTITIONS &&
          FeKuduTable.Utils.getRangePartitioningColNames(kuduTable).isEmpty()) {
        throw new AnalysisException(getSqlPrefix() + " requested but table does not " +
            "have range partitions: " + table_.getFullName());
      }
    } else if (table_ instanceof FeIcebergTable) {
      FeIcebergTable icebergTable = (FeIcebergTable) table_;
      if (op_ == TShowStatsOp.PARTITIONS) {
        //Non-partition iceberg table only has an empty PartitionField set
        Preconditions.checkArgument(!icebergTable.getPartitionSpec().isEmpty());
        IcebergPartitionSpec spec = icebergTable.getPartitionSpec().get(0);
        boolean emptySpec = (spec.getIcebergPartitionFields_() == null ||
            spec.getIcebergPartitionFields_().size() == 0);
        if (icebergTable.getPartitionSpec().size() == 1 && emptySpec) {
          throw new AnalysisException("Iceberg table does not have PartitionSpec: "
              + table_.getFullName());
        }
      } else {
        throw new AnalysisException(getSqlPrefix() + " not supported for Iceberg table.");
      }
    } else {
      if (op_ == TShowStatsOp.RANGE_PARTITIONS) {
        throw new AnalysisException(getSqlPrefix() + " must target a Kudu table: " +
            table_.getFullName());
      } else if (op_ == TShowStatsOp.PARTITIONS) {
        throw new AnalysisException(getSqlPrefix() +
            " must target an HDFS or Kudu table: " + table_.getFullName());
      }
    }
  }

  public TShowStatsParams toThrift() {
    // Ensure the DB is set in the table_name field by using table and not tableName.
    return new TShowStatsParams(op_,
        new TableName(table_.getDb().getName(), table_.getName()).toThrift());
  }
}
