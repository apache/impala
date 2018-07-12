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
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

import org.apache.impala.analysis.LiteralExpr;
import org.apache.impala.catalog.HdfsPartition.FileDescriptor;
import org.apache.impala.service.BackendConfig;
import org.apache.impala.thrift.TNetworkAddress;
import org.apache.impala.thrift.TPartitionKeyValue;
import org.apache.impala.thrift.TResultSet;
import org.apache.impala.thrift.TTableStats;
import org.apache.impala.util.ListMap;

/**
 * Frontend interface for interacting with a filesystem-backed table.
 *
 * TODO(vercegovac): various method names and comments in this interface refer
 * to HDFS where they should be more generically "Fs".
 */
public interface FeFsTable extends FeTable {
  /** hive's default value for table property 'serialization.null.format' */
  public static final String DEFAULT_NULL_COLUMN_VALUE = "\\N";

  /**
   * @return true if the table and all its partitions reside at locations which
   * support caching (e.g. HDFS).
   */
  public boolean isCacheable();

  /**
   * @return true if the table resides at a location which supports caching
   * (e.g. HDFS).
   */
  public boolean isLocationCacheable();

  /**
   * @return true if this table is marked as cached
   */
  boolean isMarkedCached();

  /*
   * Returns the storage location (HDFS path) of this table.
   */
  public String getLocation();

  /**
   * @return the value Hive is configured to use for NULL partition key values.
   *
   * TODO(todd): this is an HMS-wide setting, rather than a per-table setting.
   * Perhaps this should move to the FeCatalog interface?
   */
  public String getNullPartitionKeyValue();

  /**
   * Get file info for the given set of partitions, or all partitions if
   * partitionSet is null.
   *
   * @return partition file info, ordered by partition
   */
  TResultSet getFiles(List<List<TPartitionKeyValue>> partitionSet)
      throws CatalogException;

  /**
   * @return the base HDFS directory where files of this table are stored.
   */
  public String getHdfsBaseDir();

  /**
   * @return the total number of bytes stored for this table.
   */
  long getTotalHdfsBytes();

  /**
   * @return true if this table's schema as stored in the HMS has been overridden
   * by an Avro schema.
   */
  boolean usesAvroSchemaOverride();

  /**
   * @return the set of file formats that the partitions in this table use.
   * This API is only used by the TableSink to write out partitions. It
   * should not be used for scanning.
   */
  public Set<HdfsFileFormat> getFileFormats();

  /**
   * Return true if the table may be written to.
   */
  public boolean hasWriteAccess();

  /**
   * Return some location found without write access for this table, useful
   * in error messages about insufficient permissions to insert into a table.
   *
   * In case multiple locations are missing write access, the particular
   * location returned is implementation-defined.
   *
   * Returns null if all partitions have write access.
   */
  public String getFirstLocationWithoutWriteAccess();

  /**
   * @return statistics on this table as a tabular result set. Used for the
   * SHOW TABLE STATS statement. The schema of the returned TResultSet is set
   * inside this method.
   */
  public TResultSet getTableStats();

  /**
   * @return all partitions of this table
   */
  Collection<? extends PrunablePartition> getPartitions();

  /**
   * @return identifiers for all partitions in this table
   */
  public Set<Long> getPartitionIds();

  /**
   * Returns the map from partition identifier to prunable partition.
   */
  Map<Long, ? extends PrunablePartition> getPartitionMap();

  /**
   * @param the index of the target partitioning column
   * @return a map from value to a set of partitions for which column 'col'
   * has that value.
   */
  TreeMap<LiteralExpr, HashSet<Long>> getPartitionValueMap(int col);

  /**
   * @return the set of partitions which have a null value for column
   * index 'colIdx'.
   */
  Set<Long> getNullPartitionIds(int colIdx);

  /**
   * Returns the full partition objects for the given partition IDs, which must
   * have been obtained by prior calls to the above methods.
   * @throws IllegalArgumentException if any partition ID does not exist
   */
  List<? extends FeFsPartition> loadPartitions(Collection<Long> ids);

  /**
   * Parses and returns the value of the 'skip.header.line.count' table property. If the
   * value is not set for the table, returns 0. If parsing fails or a value < 0 is found,
   * the error parameter is updated to contain an error message.
   */
  int parseSkipHeaderLineCount(StringBuilder error);

  /**
   * Selects a random sample of files from the given list of partitions such that the sum
   * of file sizes is at least 'percentBytes' percent of the total number of bytes in
   * those partitions and at least 'minSampleBytes'. The sample is returned as a map from
   * partition id to a list of file descriptors selected from that partition.
   * This function allocates memory proportional to the number of files in 'inputParts'.
   * Its implementation tries to minimize the constant factor and object generation.
   * The given 'randomSeed' is used for random number generation.
   * The 'percentBytes' parameter must be between 0 and 100.
   */
  Map<Long, List<FileDescriptor>> getFilesSample(
      Collection<? extends FeFsPartition> inputParts,
      long percentBytes, long minSampleBytes,
      long randomSeed);

  /**
   * @return the index of hosts that store replicas of blocks of this table.
   */
  ListMap<TNetworkAddress> getHostIndex();

  /**
   * Utility functions for operating on FeFsTable. When we move fully to Java 8,
   * these can become default methods of the interface.
   */
  abstract class Utils {
    /**
     * Returns true if stats extrapolation is enabled for this table, false otherwise.
     * Reconciles the Impalad-wide --enable_stats_extrapolation flag and the
     * TBL_PROP_ENABLE_STATS_EXTRAPOLATION table property
     */
    public static boolean isStatsExtrapolationEnabled(FeFsTable table) {
      org.apache.hadoop.hive.metastore.api.Table msTbl = table.getMetaStoreTable();
      String propVal = msTbl.getParameters().get(
          HdfsTable.TBL_PROP_ENABLE_STATS_EXTRAPOLATION);
      if (propVal == null) return BackendConfig.INSTANCE.isStatsExtrapolationEnabled();
      return Boolean.parseBoolean(propVal);
    }

    /**
     * Returns an estimated row count for the given number of file bytes. The row count is
     * extrapolated using the table-level row count and file bytes statistics.
     * Returns zero only if the given file bytes is zero.
     * Returns -1 if:
     * - stats extrapolation has been disabled
     * - the given file bytes statistic is negative
     * - the row count or the file byte statistic is missing
     * - the file bytes statistic is zero or negative
     * - the row count statistic is zero and the file bytes is non-zero
     * Otherwise, returns a value >= 1.
     */
    public static long getExtrapolatedNumRows(FeFsTable table, long fileBytes) {
      if (!isStatsExtrapolationEnabled(table)) return -1;
      if (fileBytes == 0) return 0;
      if (fileBytes < 0) return -1;

      TTableStats tableStats = table.getTTableStats();
      if (tableStats.num_rows < 0 || tableStats.total_file_bytes <= 0) return -1;
      if (tableStats.num_rows == 0 && tableStats.total_file_bytes != 0) return -1;
      double rowsPerByte = tableStats.num_rows / (double) tableStats.total_file_bytes;
      double extrapolatedNumRows = fileBytes * rowsPerByte;
      return (long) Math.max(1, Math.round(extrapolatedNumRows));
    }
  }
}
