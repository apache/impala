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

package org.apache.impala.catalog.paimon;

import com.google.common.base.Preconditions;

import org.apache.hadoop.hive.metastore.api.ColumnStatisticsData;
import org.apache.hadoop.mapred.JobConf;
import org.apache.impala.catalog.Column;
import org.apache.impala.catalog.ColumnStats;
import org.apache.impala.catalog.FeTable;
import org.apache.impala.catalog.HdfsFileFormat;
import org.apache.impala.thrift.TResultSet;
import org.apache.impala.thrift.TShowFilesParams;
import org.apache.impala.thrift.TShowStatsOp;
import org.apache.paimon.stats.ColStats;
import org.apache.paimon.stats.Statistics;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.Table;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.RowType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.Optional;

/**
 * Frontend interface for interacting with an Paimon-backed table.
 */
public interface FePaimonTable extends FeTable, FeShowFileStmtSupport {
  final static Logger LOG = LoggerFactory.getLogger(FePaimonTable.class);

  public static JobConf jobConf = new JobConf();

  /**
   * Returns the cached paimon Table object that stores the metadata loaded by Paimon.
   */
  Table getPaimonApiTable();

  default boolean hasSnapshotMetaTable() {
    Table table = getPaimonApiTable();
    return table instanceof FileStoreTable;
  }

  /**
   * @return the Paimon schema.
   */
  default RowType getPaimonSchema() { return getPaimonApiTable().rowType(); }

  default void applyPaimonTableStatsIfPresent() {
    Preconditions.checkState(getTTableStats() != null);
    Table table = getPaimonApiTable();
    Optional<Statistics> stats = table.statistics();
    if (!stats.isPresent()) return;
    if (getTTableStats().getNum_rows() < 0) {
      getTTableStats().setNum_rows(stats.get().mergedRecordCount().orElse(-1));
    }
    getTTableStats().setTotal_file_bytes(stats.get().mergedRecordSize().orElse(-1));
  }

  default void applyPaimonColumnStatsIfPresent() {
    Preconditions.checkState(getTTableStats() != null);
    Table table = getPaimonApiTable();
    Optional<Statistics> stats = table.statistics();
    if (!stats.isPresent()) return;
    Map<String, ColStats<?>> colStatsMap = stats.get().colStats();
    for (String colName : colStatsMap.keySet()) {
      Column col = this.getColumn(colName.toLowerCase());
      if (null == col) { continue; }

      if (!ColumnStats.isSupportedColType(col.getType())) {
        LOG.warn(String.format("Statistics for %s, column %s are not supported as " +
                        "column has type %s",
            getFullName(), col.getName(), col.getType()));
        continue;
      }

      ColStats<?> colStats = colStatsMap.get(colName);
      DataField dataField = table.rowType().getField(colName);
      Optional<ColumnStatisticsData> colStatsData =
          PaimonUtil.convertColStats(colStats, dataField);
      if (colStatsData.isPresent()) {
        if (!col.updateStats(colStatsData.get())) {
          LOG.warn(String.format(
              "Failed to load column stats for %s, column %s. Stats may be " +
              "incompatible with column type %s. Consider regenerating " +
                      "statistics for %s.",
              getFullName(), col.getName(), col.getType(), getFullName()));
        }
      }
    }
  }

  default TResultSet getTableStats(TShowStatsOp op) {
    if (TShowStatsOp.TABLE_STATS == op) {
      return PaimonUtil.doGetTableStats(this);
    } else if (TShowStatsOp.PARTITIONS == op) {
      return PaimonUtil.doGetPartitionStats(this);
    } else {
      throw new UnsupportedOperationException(
          "paimon table doesn't support Show Stats Op" + op.name());
    }
  }

  default TResultSet doGetTableFiles(TShowFilesParams request) {
      return PaimonUtil.doGetTableFiles(this, request);
  }

  default HdfsFileFormat getTableFormat() {
    return HdfsFileFormat.PAIMON;
  }

  default boolean supportPartitionFilter() {
    return false;
  }
}
