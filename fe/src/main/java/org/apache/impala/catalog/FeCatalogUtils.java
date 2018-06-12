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

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.hive.common.FileUtils;
import org.apache.hadoop.hive.common.StatsSetupConst;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.impala.analysis.Expr;
import org.apache.impala.analysis.LiteralExpr;
import org.apache.impala.analysis.NullLiteral;
import org.apache.impala.analysis.PartitionKeyValue;
import org.apache.impala.analysis.ToSqlUtils;
import org.apache.impala.catalog.HdfsPartition.FileDescriptor;
import org.apache.impala.thrift.TColumnDescriptor;
import org.apache.impala.thrift.THdfsPartition;
import org.apache.impala.thrift.TTableStats;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
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
      Iterable<FieldSchema> fieldSchemas,
      String tableName) throws TableLoadingException {
    int pos = 0;
    ImmutableList.Builder<Column> ret = ImmutableList.builder();
    for (FieldSchema s : fieldSchemas) {
      Type type = parseColumnType(s, tableName);
      ret.add(new Column(s.getName(), type, s.getComment(), pos));
      ++pos;
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
    List<TColumnDescriptor> colDescs = Lists.<TColumnDescriptor>newArrayList();
    for (Column col: table.getColumns()) {
      colDescs.add(new TColumnDescriptor(col.getName(), col.getType().toThrift()));
    }
    return colDescs;
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
        "Cannot parse partition values %s for table %s: " +
        "expected %s values but got %s",
        hmsPartitionValues, table.getFullName(),
        table.getNumClusteringCols(), hmsPartitionValues.size());
    List<LiteralExpr> keyValues = Lists.newArrayList();
    for (String partitionKey: hmsPartitionValues) {
      Type type = table.getColumns().get(keyValues.size()).getType();
      // Deal with Hive's special NULL partition key.
      if (partitionKey.equals(table.getNullPartitionKeyValue())) {
        keyValues.add(NullLiteral.create(type));
      } else {
        try {
          keyValues.add(LiteralExpr.create(partitionKey, type));
        } catch (Exception ex) {
          LOG.warn("Failed to create literal expression of type: " + type, ex);
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
    FeFsTable table = partition.getTable();
    List<String> partitionCols = Lists.newArrayList();
    for (int i = 0; i < table.getNumClusteringCols(); ++i) {
      partitionCols.add(table.getColumns().get(i).getName());
    }

    return FileUtils.makePartName(
        partitionCols, getPartitionValuesAsStrings(partition, true));
  }

  // TODO: this could be a default method in FeFsPartition in Java 8.
  public static List<String> getPartitionValuesAsStrings(
      FeFsPartition partition, boolean mapNullsToHiveKey) {
    List<String> ret = Lists.newArrayList();
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

  // TODO: this could be a default method in FeFsPartition in Java 8.
  public static String getConjunctSqlForPartition(FeFsPartition part) {
    List<String> partColSql = Lists.newArrayList();
    for (Column partCol: part.getTable().getClusteringColumns()) {
      partColSql.add(ToSqlUtils.getIdentSql(partCol.getName()));
    }

    List<String> conjuncts = Lists.newArrayList();
    for (int i = 0; i < partColSql.size(); ++i) {
      LiteralExpr partVal = part.getPartitionValues().get(i);
      String partValSql = partVal.toSql();
      if (partVal instanceof NullLiteral || partValSql.isEmpty()) {
        conjuncts.add(partColSql.get(i) + " IS NULL");
      } else {
        conjuncts.add(partColSql.get(i) + "=" + partValSql);
      }
    }
    return "(" + Joiner.on(" AND " ).join(conjuncts) + ")";
  }

  /**
   * Return the most commonly-used file format within a set of partitions.
   */
  public static HdfsFileFormat getMajorityFormat(
      Iterable<? extends FeFsPartition> partitions) {
    Map<HdfsFileFormat, Integer> numPartitionsByFormat = Maps.newTreeMap();
    for (FeFsPartition partition: partitions) {
      HdfsFileFormat format = partition.getInputFormatDescriptor().getFileFormat();
      Integer numPartitions = numPartitionsByFormat.get(format);
      if (numPartitions == null) {
        numPartitions = Integer.valueOf(1);
      } else {
        numPartitions = Integer.valueOf(numPartitions.intValue() + 1);
      }
      numPartitionsByFormat.put(format, numPartitions);
    }

    int maxNumPartitions = Integer.MIN_VALUE;
    HdfsFileFormat majorityFormat = null;
    for (Map.Entry<HdfsFileFormat, Integer> entry: numPartitionsByFormat.entrySet()) {
      if (entry.getValue().intValue() > maxNumPartitions) {
        majorityFormat = entry.getKey();
        maxNumPartitions = entry.getValue().intValue();
      }
    }
    Preconditions.checkNotNull(majorityFormat);
    return majorityFormat;
  }

  public static THdfsPartition fsPartitionToThrift(FeFsPartition part,
      boolean includeFileDesc, boolean includeIncrementalStats) {
    HdfsStorageDescriptor sd = part.getInputFormatDescriptor();
    THdfsPartition thriftHdfsPart = new THdfsPartition(
        sd.getLineDelim(),
        sd.getFieldDelim(),
        sd.getCollectionDelim(),
        sd.getMapKeyDelim(),
        sd.getEscapeChar(),
        sd.getFileFormat().toThrift(),
        Expr.treesToThrift(part.getPartitionValues()),
        sd.getBlockSize());
    thriftHdfsPart.setLocation(part.getLocationAsThrift());
    thriftHdfsPart.setStats(new TTableStats(part.getNumRows()));
    thriftHdfsPart.setAccess_level(part.getAccessLevel());
    thriftHdfsPart.setIs_marked_cached(part.isMarkedCached());
    thriftHdfsPart.setId(part.getId());
    thriftHdfsPart.setHas_incremental_stats(part.hasIncrementalStats());
    // IMPALA-4902: Shallow-clone the map to avoid concurrent modifications. One thread
    // may try to serialize the returned THdfsPartition after releasing the table's lock,
    // and another thread doing DDL may modify the map.
    thriftHdfsPart.setHms_parameters(Maps.newHashMap(
        includeIncrementalStats ? part.getParameters() :
          part.getFilteredHmsParameters()));
    if (includeFileDesc) {
      // Add block location information
      long numBlocks = 0;
      long totalFileBytes = 0;
      for (FileDescriptor fd: part.getFileDescriptors()) {
        thriftHdfsPart.addToFile_desc(fd.toThrift());
        numBlocks += fd.getNumFileBlocks();
        totalFileBytes += fd.getFileLength();
      }
      thriftHdfsPart.setNum_blocks(numBlocks);
      thriftHdfsPart.setTotal_file_size_bytes(totalFileBytes);
    }
    return thriftHdfsPart;
  }
}
