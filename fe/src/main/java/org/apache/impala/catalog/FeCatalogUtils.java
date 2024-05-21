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
import org.apache.impala.analysis.ToSqlUtils;
import org.apache.impala.catalog.CatalogObject.ThriftObjectType;
import org.apache.impala.catalog.HdfsPartition.FileDescriptor;
import org.apache.impala.catalog.local.CatalogdMetaProvider;
import org.apache.impala.catalog.local.LocalCatalog;
import org.apache.impala.catalog.local.LocalFsTable;
import org.apache.impala.catalog.local.LocalHbaseTable;
import org.apache.impala.catalog.local.LocalIcebergTable;
import org.apache.impala.catalog.local.LocalKuduTable;
import org.apache.impala.catalog.local.LocalView;
import org.apache.impala.catalog.local.MetaProvider;
import org.apache.impala.common.ImpalaException;
import org.apache.impala.common.NotImplementedException;
import org.apache.impala.service.BackendConfig;
import org.apache.impala.thrift.TCatalogObject;
import org.apache.impala.thrift.TColumnDescriptor;
import org.apache.impala.thrift.TGetCatalogMetricsResult;
import org.apache.impala.thrift.THdfsPartition;
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

  // TODO(todd): move to a default method in FeTable in Java8
  public static List<TColumnDescriptor> getTColumnDescriptors(FeTable table) {
    List<TColumnDescriptor> colDescs = new ArrayList<>();
    for (Column col: table.getColumns()) {
      colDescs.add(col.toDescriptor());
    }
    return colDescs;
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
   * Convenience method to load exactly one partition from a table.
   *
   * TODO(todd): upon moving to Java8 this could be a default method
   * in FeFsTable.
   */
  public static FeFsPartition loadPartition(FeFsTable table,
      long partitionId) {
    Collection<? extends FeFsPartition> partCol = table.loadPartitions(
        Collections.singleton(partitionId));
    if (partCol.size() != 1) {
      throw new AssertionError(String.format(
          "expected exactly one result fetching partition ID %s from table %s " +
          "(got %s)", partitionId, table.getFullName(), partCol.size()));
    }
    return Iterables.getOnlyElement(partCol);
  }

  /**
   * Load all partitions from the given table.
   */
  public static Collection<? extends FeFsPartition> loadAllPartitions(
      FeFsTable table) {
    return table.loadPartitions(table.getPartitionIds());
  }

  /**
   * Parse the partition key values out of their stringified format used by HMS.
   */
  public static List<LiteralExpr> parsePartitionKeyValues(FeFsTable table,
      List<String> hmsPartitionValues) throws CatalogException {
    Preconditions.checkArgument(
        hmsPartitionValues.size() == table.getNumClusteringCols(),
        "Cannot parse partition values '%s' for table %s: " +
        "expected %d values but got %d",
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
        getPartitionValuesAsStrings(partition, true));
  }

  // TODO: this could be a default method in FeFsPartition in Java 8.
  public static List<String> getPartitionValuesAsStrings(
      FeFsPartition partition, boolean mapNullsToHiveKey) {
    List<String> ret = new ArrayList<>();
    for (LiteralExpr partValue: partition.getPartitionValues()) {
      if (mapNullsToHiveKey) {
        ret.add(PartitionKeyValue.getPartitionKeyValueString(
                partValue, partition.getTable().getNullPartitionKeyValue()));
      } else {
        ret.add(partValue.getStringValue());
      }
    }
    return ret;
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

  // TODO: this could be a default method in FeFsPartition in Java 8.
  public static String getConjunctSqlForPartition(FeFsPartition part) {
    List<String> partColSql = new ArrayList<>();
    for (Column partCol: part.getTable().getClusteringColumns()) {
      partColSql.add(ToSqlUtils.getIdentSql(partCol.getName()));
    }

    List<String> conjuncts = new ArrayList<>();
    for (int i = 0; i < partColSql.size(); ++i) {
      LiteralExpr partVal = part.getPartitionValues().get(i);
      String partValSql = partVal.toSql();
      if (Expr.IS_NULL_LITERAL.apply(partVal) || partValSql.isEmpty()) {
        conjuncts.add(partColSql.get(i) + " IS NULL");
      } else {
        conjuncts.add(partColSql.get(i) + "=" + partValSql);
      }
    }
    return "(" + Joiner.on(" AND " ).join(conjuncts) + ")";
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
