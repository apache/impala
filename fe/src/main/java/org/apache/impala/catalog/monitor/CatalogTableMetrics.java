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

package org.apache.impala.catalog.monitor;

import com.google.common.annotations.VisibleForTesting;
import org.apache.impala.catalog.Table;
import org.apache.impala.common.Pair;
import org.apache.impala.thrift.TTableName;
import org.apache.impala.util.TopNCache;

import java.util.List;

import static org.apache.impala.catalog.monitor.TableLoadingTimeHistogram.Quantile.P100;
import static org.apache.impala.catalog.monitor.TableLoadingTimeHistogram.Quantile.P50;
import static org.apache.impala.catalog.monitor.TableLoadingTimeHistogram.Quantile.P75;
import static org.apache.impala.catalog.monitor.TableLoadingTimeHistogram.Quantile.P95;
import static org.apache.impala.catalog.monitor.TableLoadingTimeHistogram.Quantile.P99;

/**
 * Class that monitors catalog table usage. Currently, it tracks,
 * - the most frequently accessed tables (in terms of number of metadata operations)
 * - the tables with the highest (estimated) memory requirements
 * - the tables with the highest number of files
 * - the tables with the longest table metadata loading time
 * <p>
 * This class is thread-safe.
 */
public final class CatalogTableMetrics {
  public final static CatalogTableMetrics INSTANCE = new CatalogTableMetrics();

  private final TopNCache<TableMetric<Long>, Long> frequentlyAccessedTables_;

  private final TopNCache<TableMetric<Long>, Long> largestTables_;

  private final TopNCache<TableMetric<Long>, Long> highFileCountTables_;

  private final TopNCache<TableMetric<TableLoadingTimeHistogram>, Long>
      longMetadataLoadingTables_;

  private CatalogTableMetrics() {
    final int num_tables_tracked = Integer.getInteger(
        "org.apache.impala.catalog.CatalogUsageMonitor.NUM_TABLES_TRACKED", 25);
    final int num_loading_time_tables_tracked = Integer.getInteger(
        "org.apache.impala.catalog.CatalogUsageMonitor.NUM_LOADING_TIME_TABLES_TRACKED",
        100);
    frequentlyAccessedTables_ =
        new TopNCache<>(TableMetric::getSecond, num_tables_tracked, true);

    largestTables_ = new TopNCache<>(TableMetric::getSecond, num_tables_tracked, false);

    highFileCountTables_ =
        new TopNCache<>(TableMetric::getSecond, num_tables_tracked, false);

    // sort by maximum loading time by default
    longMetadataLoadingTables_ = new TopNCache<>(metric
        -> metric.getSecond().getQuantile(P100),
        num_loading_time_tables_tracked, false);
  }

  public void updateFrequentlyAccessedTables(Table tbl) {
    TableMetric<Long> metric = TableMetric.of(tbl, tbl.getMetadataOpsCount());
    frequentlyAccessedTables_.putOrUpdate(metric);
  }

  public void updateLargestTables(Table tbl) {
    TableMetric<Long> metric = TableMetric.of(tbl, tbl.getEstimatedMetadataSize());
    largestTables_.putOrUpdate(metric);
  }

  public void updateHighFileCountTables(Table tbl) {
    TableMetric<Long> metric = TableMetric.of(tbl, tbl.getNumFiles());
    highFileCountTables_.putOrUpdate(metric);
  }

  public void updateLongMetadataLoadingTables(Table tbl) {
    TableLoadingTimeHistogram histogram = new TableLoadingTimeHistogram();
    histogram.setQuantile(P50, tbl.getMedianTableLoadingTime());
    histogram.setQuantile(P75, tbl.get75TableLoadingTime());
    histogram.setQuantile(P95, tbl.get95TableLoadingTime());
    histogram.setQuantile(P99, tbl.get99TableLoadingTime());
    histogram.setQuantile(P100, tbl.getMaxTableLoadingTime());
    histogram.setCount(tbl.getTableLoadingCounts());
    TableMetric<TableLoadingTimeHistogram> metric = TableMetric.of(tbl, histogram);
    longMetadataLoadingTables_.putOrUpdate(metric);
  }

  @SuppressWarnings({"unchecked", "rawtypes"})
  public void removeTable(Table tbl) {
    TableMetric metric = TableMetric.emptyOf(tbl);
    frequentlyAccessedTables_.remove(metric);
    largestTables_.remove(metric);
    highFileCountTables_.remove(metric);
    longMetadataLoadingTables_.remove(metric);
  }

  /**
   * Removes all tables from the underlying TopNCache.
   */
  @VisibleForTesting
  synchronized void removeAllTables() {
    frequentlyAccessedTables_.removeAll();
    largestTables_.removeAll();
    highFileCountTables_.removeAll();
    longMetadataLoadingTables_.removeAll();
  }

  public List<? extends Pair<TTableName, Long>> getFrequentlyAccessedTables() {
    return frequentlyAccessedTables_.listEntries();
  }

  public List<? extends Pair<TTableName, Long>> getLargestTables() {
    return largestTables_.listEntries();
  }

  public List<? extends Pair<TTableName, Long>> getHighFileCountTables() {
    return highFileCountTables_.listEntries();
  }

  public List<? extends Pair<TTableName, TableLoadingTimeHistogram>>
      getLongMetadataLoadingTables() {
    return longMetadataLoadingTables_.listEntries();
  }

  /**
   * a data class to isolate the implicit refernce to
   * {@link org.apache.impala.catalog.Db}, see IMPALA-6876.
   */
  static class TableMetric<T> extends Pair<TTableName, T> {
    TableMetric(TTableName tableName, T value) { super(tableName, value); }

    static <T> TableMetric<T> of(Table tbl, T value) {
      TTableName tTableName = tbl.getTableName().toThrift();
      return new TableMetric<>(tTableName, value);
    }

    static TableMetric<?> emptyOf(Table tbl) { return of(tbl, null); }

    @Override
    public int hashCode() {
      return first.hashCode();
    }

    @Override
    public boolean equals(Object obj) {
      if (obj == null) return false;
      if (!(obj instanceof TableMetric)) return false;
      return first.equals(((TableMetric<?>) obj).first);
    }
  }
}
