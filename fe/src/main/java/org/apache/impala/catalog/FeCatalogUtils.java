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

package org.apache.impala.catalog;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.hadoop.hive.common.FileUtils;
import org.apache.hadoop.hive.common.StatsSetupConst;
import org.apache.hadoop.hive.metastore.api.ColumnStatisticsData;
import org.apache.hadoop.hive.metastore.api.ColumnStatisticsObj;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.impala.analysis.Expr;
import org.apache.impala.analysis.LiteralExpr;
import org.apache.impala.analysis.NullLiteral;
import org.apache.impala.analysis.PartitionKeyValue;
import org.apache.impala.catalog.CatalogObject.ThriftObjectType;
import org.apache.impala.catalog.local.CatalogdMetaProvider;
import org.apache.impala.catalog.local.LocalCatalog;
import org.apache.impala.catalog.local.LocalFsTable;
import org.apache.impala.catalog.local.LocalHbaseTable;
import org.apache.impala.catalog.local.LocalIcebergTable;
import org.apache.impala.catalog.local.LocalKuduTable;
import org.apache.impala.catalog.local.LocalView;
import org.apache.impala.catalog.local.MetaProvider;
import org.apache.impala.catalog.local.MultiMetaProvider;
import org.apache.impala.common.ImpalaException;
import org.apache.impala.common.ImpalaRuntimeException;
import org.apache.impala.common.NotImplementedException;
import org.apache.impala.service.BackendConfig;
import org.apache.impala.thrift.TCatalogObject;
import org.apache.impala.thrift.TGetCatalogMetricsResult;
import org.apache.impala.thrift.THdfsPartition;
import org.apache.impala.thrift.TPartitionKeyValue;
import org.apache.impala.thrift.TTable;
import org.apache.impala.thrift.TTableStats;
import org.apache.impala.thrift.TTableType;
import org.apache.impala.util.AcidUtils;
import org.apache.impala.util.MetaStoreUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.codahale.metrics.Snapshot;
import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.cache.CacheStats;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.collect.Maps;

/**
 * Static utility functions shared between FeCatalog implementations.
 */
public abstract class FeCatalogUtils {
  private final static Logger LOG = LoggerFactory.getLogger(FeCatalogUtils.class);

  /**
   * Gets the ColumnType from the given FieldSchema by using Impala's SqlParser.
   *
   * The type can either be:
   *   - Supported by Impala, in which case the type is returned.
   *   - A type Impala understands but is not yet implemented (e.g. date), the type is
   *     returned but type.IsSupported() returns false.
   *   - A supported type that exceeds an Impala limit, e.g., on the nesting depth.
   *   - A type Impala can't understand at all, and a TableLoadingException is thrown.
   *
   * Throws a TableLoadingException if the FieldSchema could not be parsed. In this
   * case, 'tableName' is included in the error message.
   */
  public static Type parseColumnType(FieldSchema fs, String tableName)
     throws TableLoadingException {
    Type type = Type.parseColumnType(fs.getType());
    if (type == null) {
      throw new TableLoadingException(String.format(
          "Unsupported type '%s' in column '%s' of table '%s'",
          fs.getType(), fs.getName(), tableName));
    }
    if (type.exceedsMaxNestingDepth()) {
      throw new TableLoadingException(String.format(
          "Type exceeds the maximum nesting depth of %s:\n%s",
          Type.MAX_NESTING_DEPTH, type.toSql()));
    }
    return type;
  }

  /**
   * Convert a list of HMS FieldSchemas to internal Column types.
   * @throws TableLoadingException if any type is invalid
   */
  public static ImmutableList<Column> fieldSchemasToColumns(
      org.apache.hadoop.hive.metastore.api.Table msTbl) throws TableLoadingException {
    boolean isFullAcidTable = AcidUtils.isFullAcidTable(msTbl.getParameters());
    int pos = 0;
    ImmutableList.Builder<Column> ret = ImmutableList.builder();
    for (FieldSchema s : Iterables.concat(msTbl.getPartitionKeys(),
                                          msTbl.getSd().getCols())) {
      if (isFullAcidTable && pos == msTbl.getPartitionKeys().size()) {
        ret.add(AcidUtils.getRowIdColumnType(pos++));
      }
      Type type = parseColumnType(s, msTbl.getTableName());
      ret.add(new Column(s.getName(), type, s.getComment(), pos++));
    }
    return ret.build();
  }

  /**
   * Validate that the clustering columns are valid for a table
   *
   * TODO(todd): consider refactoring to combine with
   * HdfsTable.addColumnsFromFieldSchema
   *
   * @throws TableLoadingException if the columns are invalid
   */
  public static void validateClusteringColumns(
      Iterable<Column> cols, String tableName)
      throws TableLoadingException {
    // Check if we support partitioning on columns of such a type.
    for (Column c : cols) {
      Type type = c.getType();
      if (!type.supportsTablePartitioning()) {
        throw new TableLoadingException(
            String.format("Failed to load metadata for table '%s' because of " +
                "unsupported partition-column type '%s' in partition column '%s'",
                tableName, type.toString(), c.getName()));
      }
    }
  }

  /**
   * Given the list of column stats returned from the metastore, inject those
   * stats into matching columns in 'table'.
   */
  public static void injectColumnStats(
      List<ColumnStatisticsObj> colStats, FeTable table, SideloadTableStats testStats) {
    for (ColumnStatisticsObj stats: colStats) {
      Column col = table.getColumn(stats.getColName());
      Preconditions.checkNotNull(col, "Unable to find column %s in table %s",
          stats.getColName(), table.getFullName());
      if (!ColumnStats.isSupportedColType(col.getType())) {
        LOG.warn(String.format(
            "Statistics for %s, column %s are not supported as column " +
            "has type %s", table.getFullName(), col.getName(), col.getType()));
        continue;
      }
      ColumnStatisticsData colStatsData = stats.getStatsData();
      if (testStats != null && testStats.hasColumn(stats.getColName())) {
        colStatsData = testStats.getColumnStats(stats.getColName());
        Preconditions.checkNotNull(colStatsData);
        LOG.info("Sideload stats for " + table.getFullName() + "." + stats.getColName()
            + ". " + colStatsData);
      }

      if (!col.updateStats(colStatsData)) {
        LOG.warn(String.format(
            "Failed to load column stats for %s, column %s. Stats may be " +
            "incompatible with column type %s. Consider regenerating statistics " +
            "for %s.", table.getFullName(), col.getName(), col.getType(),
            table.getFullName()));
      }
    }
  }

  /**
   * Returns the value of the ROW_COUNT constant, or -1 if not found.
   */
  public static long getRowCount(Map<String, String> parameters) {
    return getLongParam(StatsSetupConst.ROW_COUNT, parameters);
  }

  public static long getTotalSize(Map<String, String> parameters) {
    return getLongParam(StatsSetupConst.TOTAL_SIZE, parameters);
  }

  private static long getLongParam(String key, Map<String, String> parameters) {
    if (parameters == null) return -1;
    String value = parameters.get(key);
    if (value == null) return -1;
    try {
      return Long.valueOf(value);
    } catch (NumberFormatException exc) {
      // ignore
    }
    return -1;
  }

  /**
   * Parse the partition key values out of their stringified format used by HMS.
   */
  public static List<LiteralExpr> parsePartitionKeyValues(FeFsTable table,
      List<String> hmsPartitionValues) throws CatalogException {
    Preconditions.checkArgument(
        hmsPartitionValues.size() == table.getNumClusteringCols(),
        "Cannot parse partition values '%s' for table %s: " +
        "expected %s values but got %s",
        hmsPartitionValues, table.getFullName(),
        table.getNumClusteringCols(), hmsPartitionValues.size());
    List<LiteralExpr> keyValues = new ArrayList<>();
    for (String partitionKey : hmsPartitionValues) {
      Type type = table.getColumns().get(keyValues.size()).getType();
      // Deal with Hive's special NULL partition key.
      if (partitionKey.equals(table.getNullPartitionKeyValue())) {
        keyValues.add(NullLiteral.create(type));
      } else {
        try {
          keyValues.add(LiteralExpr.createFromUnescapedStr(partitionKey, type));
        } catch (Exception ex) {
          LOG.warn(String.format(
              "Failed to create literal expression: type: %s, value: '%s'",
              type.toSql(), partitionKey), ex);
          throw new CatalogException("Invalid partition key value of type: " + type,
              ex);
        }
      }
    }
    for (Expr v: keyValues) v.analyzeNoThrow(null);
    return keyValues;
  }

  /**
   * Return a partition name formed by concatenating partition keys and their values,
   * compatible with the way Hive names partitions. Reuses Hive's
   * org.apache.hadoop.hive.common.FileUtils.makePartName() function to build the name
   * string because there are a number of special cases for how partition names are URL
   * escaped.
   *
   * TODO: this could be a default method in FeFsPartition in Java 8.
   */
  public static String getPartitionName(FeFsPartition partition) {
    return getPartitionName(partition.getTable(),
        partition.getPartitionValuesAsStrings(true));
  }

  public static String getPartitionName(HdfsPartition.Builder partBuilder) {
    HdfsTable table = partBuilder.getTable();
    List<String> partitionValues = new ArrayList<>();
    for (LiteralExpr partValue : partBuilder.getPartitionValues()) {
      partitionValues.add(PartitionKeyValue.getPartitionKeyValueString(
          partValue, table.getNullPartitionKeyValue()));
    }
    return getPartitionName(table, partitionValues);
  }

  public static String getPartitionName(FeFsTable table, List<String> partitionValues) {
    List<String> partitionKeys = table.getClusteringColumns().stream()
        .map(Column::getName)
        .collect(Collectors.toList());
    return FileUtils.makePartName(partitionKeys, partitionValues);
  }

  public static String getPartitionName(List<PartitionKeyValue> partitionKeyValues) {
    List<String> partitionKeys = partitionKeyValues.stream()
        .map(PartitionKeyValue::getColName)
        .collect(Collectors.toList());
    List<String> partitionValues = partitionKeyValues.stream()
        .map(PartitionKeyValue::getLiteralValue)
        .map(l -> PartitionKeyValue.getPartitionKeyValueString(
             l, MetaStoreUtil.DEFAULT_NULL_PARTITION_KEY_VALUE))
        .collect(Collectors.toList());
    return FileUtils.makePartName(partitionKeys, partitionValues);
  }

  public static String getPartitionName(List<String> partitionKeys,
      Map<String, String> partitionColNameToValue) throws CatalogException {
    List<String> partitionValues = new ArrayList<>();
    for (String partitionColName : partitionKeys) {
      String partitionValue = partitionColNameToValue.get(partitionColName);
      if (partitionValue == null) {
        String errorMessage = String.format("Can't find column %s in %s",
            partitionColName, partitionColNameToValue);
        throw new CatalogException(errorMessage);
      }
      partitionValues.add(partitionValue);
    }
    return FileUtils.makePartName(partitionKeys, partitionValues);
  }

  /**
   * Parse a partition name string (e.g., "year=2010/month=3") into a list of
   * TPartitionKeyValue objects.
   *
   * Note on URL encoding: If partition values contain special characters like "/",
   * they must be double-encoded in the HTTP request URL because:
   * 1. Hive stores such partitions on HDFS with '/' pre-encoded as '%2F'
   *    (e.g., HDFS directory: ds=2024%2F12%2F25)
   * 2. HTTP URL encoding must encode the '%' as '%25'
   *    (e.g., HTTP request: ds=2024%252F12%252F25)
   * After URL decoding, this method receives "ds=2024%2F12%2F25" (single-encoded),
   * which is then decoded by FileUtils.unescapePathName() to get "2024/12/25".
   *
   * This method validates that:
   * - The number of partition keys matches the table's clustering columns
   * - Each partition key name matches the expected column name at that position
   * - Each partition key-value pair is in the format "key=value"
   *
   * @param partitionName The partition name string after one level of URL decoding
   *                      (e.g., "year=2010/month=3" or "ds=2024%2F12%2F25")
   * @param table The HdfsTable containing the partition
   * @return A list of TPartitionKeyValue objects representing the parsed partition
   * @throws CatalogException if the partition name is invalid or doesn't match the
   *     table's partition schema
   */
  public static List<TPartitionKeyValue> parsePartitionName(
      String partitionName, HdfsTable table) throws CatalogException {
    List<TPartitionKeyValue> partitionSpec = new ArrayList<>();
    if (partitionName == null || partitionName.isEmpty()) {
      throw new CatalogException("Invalid partition name: " + partitionName);
    }

    // Split the partition name by "/" to get individual key=value pairs.
    // Note: If partition values contain "/", they should be URL-encoded as "%2F"
    // before passing to this method, so the split will not break them apart.
    String[] parts = partitionName.split("/");
    int numClusteringCols = table.getNumClusteringCols();

    if (parts.length != numClusteringCols) {
      throw new CatalogException(
          String.format("Invalid partition name '%s': expected %d partition keys, got %d",
              partitionName, numClusteringCols, parts.length));
    }

    List<Column> clusteringCols = table.getClusteringColumns();
    for (int i = 0; i < parts.length; i++) {
      String part = parts[i];
      int eqPos = part.indexOf('=');
      if (eqPos <= 0 || eqPos >= part.length() - 1) {
        throw new CatalogException(
            "Invalid partition key-value format: " + part);
      }

      String key = part.substring(0, eqPos);
      String encodedValue = part.substring(eqPos + 1);

      // URL-decode the value to handle special characters like "/"
      // that are encoded as "%2F".
      String value = FileUtils.unescapePathName(encodedValue);

      // Verify that the key matches the expected partition column name
      String expectedKey = clusteringCols.get(i).getName();
      if (!key.equals(expectedKey)) {
        throw new CatalogException(
            String.format("Invalid partition key '%s': expected '%s'",
                key, expectedKey));
      }

      partitionSpec.add(new TPartitionKeyValue(key, value));
    }

    return partitionSpec;
  }

  /**
   * Return the set of all file formats used in the collection of partitions.
   */
  public static Set<HdfsFileFormat> getFileFormats(
      Iterable<? extends FeFsPartition> partitions) {
    Set<HdfsFileFormat> fileFormats = new HashSet<>();
    for (FeFsPartition partition : partitions) {
      fileFormats.add(partition.getFileFormat());
    }
    return fileFormats;
  }

  public static THdfsPartition fsPartitionToThrift(FeFsPartition part,
      ThriftObjectType type) {
    HdfsStorageDescriptor sd = part.getInputFormatDescriptor();
    THdfsPartition thriftHdfsPart = new THdfsPartition();
    thriftHdfsPart.setHdfs_storage_descriptor(sd.toThrift());
    thriftHdfsPart.setPartitionKeyExprs(Expr.treesToThrift(part.getPartitionValues()));
    thriftHdfsPart.setId(part.getId());
    thriftHdfsPart.setLocation(part.getLocationAsThrift());
    if (part.getWriteId() >= 0)
      thriftHdfsPart.setWrite_id(part.getWriteId());
    if (type == ThriftObjectType.FULL) {
      thriftHdfsPart.setPartition_name(part.getPartitionName());
      thriftHdfsPart.setStats(new TTableStats(part.getNumRows()));
      thriftHdfsPart.setAccess_level(part.getAccessLevel());
      thriftHdfsPart.setIs_marked_cached(part.isMarkedCached());
      // IMPALA-4902: Shallow-clone the map to avoid concurrent modifications. One thread
      // may try to serialize the returned THdfsPartition after releasing the table's lock,
      // and another thread doing DDL may modify the map.
      thriftHdfsPart.setHms_parameters(Maps.newHashMap(part.getParameters()));
      thriftHdfsPart.setHas_incremental_stats(part.hasIncrementalStats());

      // Add block location information
      long numBlocks = 0;
      long totalFileBytes = 0;
      for (FileDescriptor fd: part.getFileDescriptors()) {
        numBlocks += fd.getNumFileBlocks();
        totalFileBytes += fd.getFileLength();
      }
      if (!part.getInsertFileDescriptors().isEmpty()) {
        for (FileDescriptor fd : part.getInsertFileDescriptors()) {
          thriftHdfsPart.addToInsert_file_desc(fd.toThrift());
        }
        for (FileDescriptor fd : part.getDeleteFileDescriptors()) {
          thriftHdfsPart.addToDelete_file_desc(fd.toThrift());
        }
      } else {
        for (FileDescriptor fd: part.getFileDescriptors()) {
          thriftHdfsPart.addToFile_desc(fd.toThrift());
        }
      }
      thriftHdfsPart.setNum_blocks(numBlocks);
      thriftHdfsPart.setTotal_file_size_bytes(totalFileBytes);
    }
    return thriftHdfsPart;
  }

  /**
   * Returns the FULL thrift object for a FeTable. The result can be directly loaded into
   * the catalog cache of catalogd. See CatalogOpExecutor#copyTestCaseData().
   */
  public static TTable feTableToThrift(FeTable table) throws ImpalaException {
    if (table instanceof Table) return ((Table) table).toThrift();
    // In local-catalog mode, coordinator caches the metadata in finer grained manner.
    // Construct the thrift table using fine-grained APIs.
    TTable res = new TTable(table.getDb().getName(), table.getName());
    res.setTable_stats(table.getTTableStats());
    res.setMetastore_table(table.getMetaStoreTable());
    res.setClustering_columns(new ArrayList<>());
    for (Column c : table.getClusteringColumns()) {
      res.addToClustering_columns(c.toThrift());
    }
    res.setColumns(new ArrayList<>());
    for (Column c : table.getNonClusteringColumns()) {
      res.addToColumns(c.toThrift());
    }
    res.setVirtual_columns(new ArrayList<>());
    for (VirtualColumn c : table.getVirtualColumns()) {
      res.addToVirtual_columns(c.toThrift());
    }
    if (table instanceof LocalFsTable) {
      res.setTable_type(TTableType.HDFS_TABLE);
      res.setHdfs_table(((LocalFsTable) table).toTHdfsTable(
          CatalogObject.ThriftObjectType.FULL));
    } else if (table instanceof LocalKuduTable) {
      res.setTable_type(TTableType.KUDU_TABLE);
      res.setKudu_table(((LocalKuduTable) table).toTKuduTable());
    } else if (table instanceof LocalHbaseTable) {
      res.setTable_type(TTableType.HBASE_TABLE);
      res.setHbase_table(FeHBaseTable.Util.getTHBaseTable((FeHBaseTable) table));
    } else if (table instanceof LocalIcebergTable) {
      res.setTable_type(TTableType.ICEBERG_TABLE);
      LocalIcebergTable iceTable = (LocalIcebergTable) table;
      res.setIceberg_table(FeIcebergTable.Utils.getTIcebergTable(iceTable));
      res.setHdfs_table(iceTable.transformToTHdfsTable(/*unused*/true,
          ThriftObjectType.FULL));
    } else if (table instanceof LocalView) {
      res.setTable_type(TTableType.VIEW);
      // Metadata of the view are stored in msTable. Nothing else need to add here.
    } else {
      throw new NotImplementedException("Unsupported type to export: " +
          table.getClass());
    }
    return res;
  }

  /**
   * Populates cache metrics in the input TGetCatalogMetricsResult object.
   * No-op if CatalogdMetaProvider is not the configured metadata provider.
   */
  public static void populateCacheMetrics(
      FeCatalog catalog, TGetCatalogMetricsResult metrics) {
    Preconditions.checkNotNull(catalog);
    Preconditions.checkNotNull(metrics);
    // Populate cache stats only if configured in local mode.
    if (!BackendConfig.INSTANCE.getBackendCfg().use_local_catalog) return;
    Preconditions.checkState(catalog instanceof LocalCatalog);
    MetaProvider provider = ((LocalCatalog) catalog).getMetaProvider();
    if (provider instanceof MultiMetaProvider) {
      provider = ((MultiMetaProvider) provider).getPrimaryProvider();
    }
    if (!(provider instanceof CatalogdMetaProvider)) return;

    CacheStats stats = ((CatalogdMetaProvider) provider).getCacheStats();
    metrics.setCache_eviction_count(stats.evictionCount());
    metrics.setCache_hit_count(stats.hitCount());
    metrics.setCache_load_count(stats.loadCount());
    metrics.setCache_load_exception_count(stats.loadExceptionCount());
    metrics.setCache_load_success_count(stats.loadSuccessCount());
    metrics.setCache_miss_count(stats.missCount());
    metrics.setCache_request_count(stats.requestCount());
    metrics.setCache_total_load_time(stats.totalLoadTime());
    metrics.setCache_avg_load_time(stats.averageLoadPenalty());
    metrics.setCache_hit_rate(stats.hitRate());
    metrics.setCache_load_exception_rate(stats.loadExceptionRate());
    metrics.setCache_miss_rate(stats.missRate());

    Snapshot cacheEntrySize = ((CatalogdMetaProvider) provider).getCacheEntrySize();
    metrics.setCache_entry_median_size(cacheEntrySize.getMedian());
    metrics.setCache_entry_99th_size(cacheEntrySize.get99thPercentile());
  }


  /**
   * Returns a debug string for a given list of TCatalogObjects. Includes the unique key
   * and version number for each object.
   */
  public static String debugString(List<TCatalogObject> objects) {
    if (objects == null || objects.size() == 0) return "[]";
    List<String> catalogObjs = new ArrayList<>();
    for (TCatalogObject object: objects) {
      catalogObjs.add(String.format("%s version: %d",
          Catalog.toCatalogObjectKey(object), object.catalog_version));
    }
    return "[" + Joiner.on(",").join(catalogObjs) + "]";
  }

}
