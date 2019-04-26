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
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.TreeMap;

import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.impala.analysis.Expr;
import org.apache.impala.analysis.LiteralExpr;
import org.apache.impala.analysis.PartitionKeyValue;
import org.apache.impala.catalog.HdfsPartition.FileDescriptor;
import org.apache.impala.common.AnalysisException;
import org.apache.impala.common.FileSystemUtil;
import org.apache.impala.common.PrintUtils;
import org.apache.impala.planner.HdfsScanNode;
import org.apache.impala.service.BackendConfig;
import org.apache.impala.thrift.TColumn;
import org.apache.impala.thrift.TNetworkAddress;
import org.apache.impala.thrift.TPartitionKeyValue;
import org.apache.impala.thrift.TResultSet;
import org.apache.impala.thrift.TResultSetMetadata;
import org.apache.impala.thrift.TTableStats;
import org.apache.impala.util.ListMap;
import org.apache.impala.util.TAccessLevelUtil;
import org.apache.impala.util.TResultRowBuilder;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;

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
   * @return the base HDFS directory where files of this table are stored.
   */
  public String getHdfsBaseDir();

  /**
   * @return the FsType where files of this table are stored.
   */
  public FileSystemUtil.FsType getFsType();

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
   * Return true if the table's base directory may be written to, in order to
   * create new partitions, or insert into the default partition in the case of
   * an unpartitioned table.
   */
  public boolean hasWriteAccessToBaseDir();

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
  TreeMap<LiteralExpr, Set<Long>> getPartitionValueMap(int col);

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
  default int parseSkipHeaderLineCount(StringBuilder error) {
    org.apache.hadoop.hive.metastore.api.Table msTbl = getMetaStoreTable();
    if (msTbl == null ||
        !msTbl.getParameters().containsKey(
          FeFsTable.Utils.TBL_PROP_SKIP_HEADER_LINE_COUNT)) {
      return 0;
    }
    return Utils.parseSkipHeaderLineCount(msTbl.getParameters(), error);
  }

  /**
   * @return the index of hosts that store replicas of blocks of this table.
   */
  ListMap<TNetworkAddress> getHostIndex();

  /**
   * Utility functions for operating on FeFsTable. When we move fully to Java 8,
   * these can become default methods of the interface.
   */
  abstract class Utils {
    // Table property key for skip.header.line.count
    public static final String TBL_PROP_SKIP_HEADER_LINE_COUNT = "skip.header.line.count";

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
     * Returns true if the file contents within a partition should be recursively listed.
     * Reconciles the Impalad-wide --recursively_list_partitions and the
     * TBL_PROP_DISABLE_RECURSIVE_LISTING table property
     */
    public static boolean shouldRecursivelyListPartitions(FeFsTable table) {
      org.apache.hadoop.hive.metastore.api.Table msTbl = table.getMetaStoreTable();
      String propVal = msTbl.getParameters().get(
          HdfsTable.TBL_PROP_DISABLE_RECURSIVE_LISTING);
      if (propVal == null) return BackendConfig.INSTANCE.recursivelyListPartitions();
      // TODO(todd): we should detect if this flag is set on an ACID table and
      // give some kind of error (we _must_ recursively list such tables)
      return !Boolean.parseBoolean(propVal);
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

    /**
     * Get file info for the given set of partitions, or all partitions if
     * partitionSet is null.
     *
     * @return partition file info, ordered by partition
     */
    public static TResultSet getFiles(FeFsTable table,
        List<List<TPartitionKeyValue>> partitionSet) {
      TResultSet result = new TResultSet();
      TResultSetMetadata resultSchema = new TResultSetMetadata();
      result.setSchema(resultSchema);
      resultSchema.addToColumns(new TColumn("Path", Type.STRING.toThrift()));
      resultSchema.addToColumns(new TColumn("Size", Type.STRING.toThrift()));
      resultSchema.addToColumns(new TColumn("Partition", Type.STRING.toThrift()));
      result.setRows(new ArrayList<>());

      List<? extends FeFsPartition> orderedPartitions;
      if (partitionSet == null) {
        orderedPartitions = Lists.newArrayList(FeCatalogUtils.loadAllPartitions(table));
      } else {
        // Get a list of HdfsPartition objects for the given partition set.
        orderedPartitions = getPartitionsFromPartitionSet(table, partitionSet);
      }
      Collections.sort(orderedPartitions, HdfsPartition.KV_COMPARATOR);

      for (FeFsPartition p: orderedPartitions) {
        List<FileDescriptor> orderedFds = Lists.newArrayList(p.getFileDescriptors());
        Collections.sort(orderedFds);
        for (FileDescriptor fd: orderedFds) {
          TResultRowBuilder rowBuilder = new TResultRowBuilder();
          rowBuilder.add(p.getLocation() + "/" + fd.getRelativePath());
          rowBuilder.add(PrintUtils.printBytes(fd.getFileLength()));
          rowBuilder.add(p.getPartitionName());
          result.addToRows(rowBuilder.get());
        }
      }
      return result;
    }

    /**
     * Selects a random sample of files from the given list of partitions such that the
     * sum of file sizes is at least 'percentBytes' percent of the total number of bytes
     * in those partitions and at least 'minSampleBytes'. The sample is returned as a map
     * from partition id to a list of file descriptors selected from that partition.
     *
     * This function allocates memory proportional to the number of files in 'inputParts'.
     * Its implementation tries to minimize the constant factor and object generation.
     * The given 'randomSeed' is used for random number generation.
     * The 'percentBytes' parameter must be between 0 and 100.
     */
    public static Map<HdfsScanNode.SampledPartitionMetadata, List<FileDescriptor>>
        getFilesSample(FeFsTable table, Collection<? extends FeFsPartition> inputParts,
            long percentBytes, long minSampleBytes, long randomSeed) {
      Preconditions.checkState(percentBytes >= 0 && percentBytes <= 100);
      Preconditions.checkState(minSampleBytes >= 0);

      long totalNumFiles = 0;
      for (FeFsPartition part : inputParts) {
        totalNumFiles += part.getNumFileDescriptors();
      }

      // Conservative max size for Java arrays. The actual maximum varies
      // from JVM version and sometimes between configurations.
      final long JVM_MAX_ARRAY_SIZE = Integer.MAX_VALUE - 10;
      if (totalNumFiles > JVM_MAX_ARRAY_SIZE) {
        throw new IllegalStateException(String.format(
            "Too many files to generate a table sample of table %s. " +
            "Sample requested over %s files, but a maximum of %s files are supported.",
            table.getTableName().toString(), totalNumFiles, JVM_MAX_ARRAY_SIZE));
      }

      // Ensure a consistent ordering of files for repeatable runs. The files within a
      // partition are already ordered based on how they are loaded in the catalog.
      List<FeFsPartition> orderedParts = Lists.newArrayList(inputParts);
      Collections.sort(orderedParts, HdfsPartition.KV_COMPARATOR);

      // fileIdxs contains indexes into the file descriptor lists of all inputParts
      // parts[i] contains the partition corresponding to fileIdxs[i]
      // fileIdxs[i] is an index into the file descriptor list of the partition parts[i]
      // The purpose of these arrays is to efficiently avoid selecting the same file
      // multiple times during the sampling, regardless of the sample percent.
      // We purposely avoid generating objects proportional to the number of files.
      int[] fileIdxs = new int[(int)totalNumFiles];
      FeFsPartition[] parts = new FeFsPartition[(int)totalNumFiles];
      int idx = 0;
      long totalBytes = 0;
      for (FeFsPartition part: orderedParts) {
        totalBytes += part.getSize();
        int numFds = part.getNumFileDescriptors();
        for (int fileIdx = 0; fileIdx < numFds; ++fileIdx) {
          fileIdxs[idx] = fileIdx;
          parts[idx] = part;
          ++idx;
        }
      }
      if (idx != totalNumFiles) {
        throw new AssertionError("partition file counts changed during iteration");
      }

      int numFilesRemaining = idx;
      double fracPercentBytes = (double) percentBytes / 100;
      long targetBytes = (long) Math.round(totalBytes * fracPercentBytes);
      targetBytes = Math.max(targetBytes, minSampleBytes);

      // Randomly select files until targetBytes has been reached or all files have been
      // selected.
      Random rnd = new Random(randomSeed);
      long selectedBytes = 0;
      Map<HdfsScanNode.SampledPartitionMetadata, List<FileDescriptor>> result =
          new HashMap<>();
      while (selectedBytes < targetBytes && numFilesRemaining > 0) {
        int selectedIdx = Math.abs(rnd.nextInt()) % numFilesRemaining;
        FeFsPartition part = parts[selectedIdx];
        HdfsScanNode.SampledPartitionMetadata sampledPartitionMetadata =
            new HdfsScanNode.SampledPartitionMetadata(part.getId(), part.getFsType());
        List<FileDescriptor> sampleFileIdxs = result.computeIfAbsent(
            sampledPartitionMetadata, partMetadata -> Lists.newArrayList());
        FileDescriptor fd = part.getFileDescriptors().get(fileIdxs[selectedIdx]);
        sampleFileIdxs.add(fd);
        selectedBytes += fd.getFileLength();
        // Avoid selecting the same file multiple times.
        fileIdxs[selectedIdx] = fileIdxs[numFilesRemaining - 1];
        parts[selectedIdx] = parts[numFilesRemaining - 1];
        --numFilesRemaining;
      }
      return result;
    }

    /**
     * Get and load the specified partitions from the table.
     */
    public static List<? extends FeFsPartition> getPartitionsFromPartitionSet(
        FeFsTable table, List<List<TPartitionKeyValue>> partitionSet) {
      List<Long> partitionIds = new ArrayList<>();
      for (List<TPartitionKeyValue> kv : partitionSet) {
        PrunablePartition partition = getPartitionFromThriftPartitionSpec(table, kv);
        if (partition != null) partitionIds.add(partition.getId());
      }
      return table.loadPartitions(partitionIds);
    }

    /**
     * Get the specified partition from the table, or null if no such partition
     * exists.
     */
    public static PrunablePartition getPartitionFromThriftPartitionSpec(
        FeFsTable table,
        List<TPartitionKeyValue> partitionSpec) {
      // First, build a list of the partition values to search for in the same order they
      // are defined in the table.
      List<String> targetValues = new ArrayList<>();
      Set<String> keys = new HashSet<>();
      for (FieldSchema fs: table.getMetaStoreTable().getPartitionKeys()) {
        for (TPartitionKeyValue kv: partitionSpec) {
          if (fs.getName().toLowerCase().equals(kv.getName().toLowerCase())) {
            targetValues.add(kv.getValue());
            // Same key was specified twice
            if (!keys.add(kv.getName().toLowerCase())) {
              return null;
            }
          }
        }
      }

      // Make sure the number of values match up and that some values were found.
      if (targetValues.size() == 0 ||
          (targetValues.size() != table.getMetaStoreTable().getPartitionKeysSize())) {
        return null;
      }

      // Search through all the partitions and check if their partition key values
      // match the values being searched for.
      for (PrunablePartition partition: table.getPartitions()) {
        List<LiteralExpr> partitionValues = partition.getPartitionValues();
        Preconditions.checkState(partitionValues.size() == targetValues.size());
        boolean matchFound = true;
        for (int i = 0; i < targetValues.size(); ++i) {
          String value;
          if (Expr.IS_NULL_LITERAL.apply(partitionValues.get(i))) {
            value = table.getNullPartitionKeyValue();
          } else {
            value = partitionValues.get(i).getStringValue();
            Preconditions.checkNotNull(value);
            // See IMPALA-252: we deliberately map empty strings on to
            // NULL when they're in partition columns. This is for
            // backwards compatibility with Hive, and is clearly broken.
            if (value.isEmpty()) value = table.getNullPartitionKeyValue();
          }
          if (!targetValues.get(i).equals(value)) {
            matchFound = false;
            break;
          }
        }
        if (matchFound) return partition;
      }
      return null;
    }

    /**
     * Check that the Impala user has write access to the given target table.
     * If 'partitionKeyValues' is null, the user should have write access to all
     * partitions (or to the table directory itself in the case of unpartitioned
     * tables). Otherwise, the user only needs write access to the specific partition.
     *
     * @throws AnalysisException if write access is not available
     */
    public static void checkWriteAccess(FeFsTable table,
        List<PartitionKeyValue> partitionKeyValues,
        String operationType) throws AnalysisException {
      String noWriteAccessErrorMsg = String.format("Unable to %s into " +
          "target table (%s) because Impala does not have WRITE access to HDFS " +
          "location: ", operationType, table.getFullName());

      PrunablePartition existingTargetPartition = null;
      if (partitionKeyValues != null) {
        existingTargetPartition = HdfsTable.getPartition(table, partitionKeyValues);
        // This could be null in the case that we are writing to a specific partition that
        // has not been created yet.
      }

      if (existingTargetPartition != null) {
        FeFsPartition partition = FeCatalogUtils.loadPartition(table,
            existingTargetPartition.getId());
        String location = partition.getLocation();
        if (!TAccessLevelUtil.impliesWriteAccess(partition.getAccessLevel())) {
          throw new AnalysisException(noWriteAccessErrorMsg + location);
        }
      } else if (partitionKeyValues != null) {
        // Writing into a table with a specific partition specified which doesn't
        // exist yet. In this case, we need write access to the top-level
        // table location in order to create a new partition.
        if (!table.hasWriteAccessToBaseDir()) {
          throw new AnalysisException(noWriteAccessErrorMsg + table.getHdfsBaseDir());
        }
      } else {
        // No explicit partition was specified. Need to ensure that write access is
        // available to all partitions as well as the base dir.
        String badPath = table.getFirstLocationWithoutWriteAccess();
        if (badPath != null) {
          throw new AnalysisException(noWriteAccessErrorMsg + badPath);
        }
      }
    }

    /**
     * Parses and returns the value of the 'skip.header.line.count' table property. The
     * caller must ensure that the property is contained in the 'tblProperties' map. If
     * parsing fails or a value < 0 is found, the error parameter is updated to contain an
     * error message.
     */
    public static int parseSkipHeaderLineCount(Map<String, String> tblProperties,
        StringBuilder error) {
      Preconditions.checkState(tblProperties != null);
      Preconditions.checkState(
          tblProperties.containsKey(TBL_PROP_SKIP_HEADER_LINE_COUNT));
      // Try to parse.
      String string_value = tblProperties.get(TBL_PROP_SKIP_HEADER_LINE_COUNT);
      int skipHeaderLineCount = 0;
      String error_msg = String.format("Invalid value for table property %s: %s (value " +
          "must be an integer >= 0)", TBL_PROP_SKIP_HEADER_LINE_COUNT, string_value);
      try {
        skipHeaderLineCount = Integer.parseInt(string_value);
      } catch (NumberFormatException exc) {
        error.append(error_msg);
      }
      if (skipHeaderLineCount < 0) error.append(error_msg);
      return skipHeaderLineCount;
    }
  }
}
