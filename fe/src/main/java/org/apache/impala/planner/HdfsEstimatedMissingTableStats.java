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

package org.apache.impala.planner;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableSet;
import org.apache.impala.catalog.Column;
import org.apache.impala.catalog.FeFsPartition;
import org.apache.impala.catalog.FeFsTable;
import org.apache.impala.catalog.FileDescriptor;
import org.apache.impala.catalog.HdfsCompression;
import org.apache.impala.catalog.HdfsFileFormat;
import org.apache.impala.catalog.ScalarType;
import org.apache.impala.catalog.Type;
import org.apache.impala.util.MathUtil;
import org.apache.impala.thrift.TQueryOptions;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Set;

/**
 * Computes and returns the number of rows scanned based on the per-partition row count
 * stats and/or the table-level row count stats, depending on which of those are
 * available. Partition stats are used as long as they are neither missing nor
 * corrupted. Otherwise, we fall back to table-level stats even for partitioned tables.
 * We further estimate the row count if the table-level stats is missing or corrupted,
 * or some partitions are with corrupt stats. The estimation is done only for those
 * partitions with corrupt stats.
 *
 * Currently, we provide a table hint 'TABLE_NUM_ROWS' to set table rows manually if
 * table has no stats or has corrupt stats. If the table has valid stats, this hint
 * will be ignored. This is passed into the constructor.
 */
public class HdfsEstimatedMissingTableStats {

  // Number of partitions that have the row count statistic
  public final int numPartitionsWithNumRows_;

  // Indicates corrupt table stats based on the number of non-empty scan ranges and
  // numRows set to 0. Set in computeStats().
  public final boolean hasCorruptTableStats_;

  // Input cardinality based on the partition row counts or extrapolation. -1 if invalid.
  public final long partitionNumRows_;

  // the number of rows scanned based on the per-partition row count
  // stats and/or the table-level row count stats, depending on which of those are
  // available.
  public final long statsNumRows_;

  // An estimate of the width of a row when the information is not available.
  private double DEFAULT_ROW_WIDTH_ESTIMATE = 1.0;

  // When the information of cardinality is not available for the underlying hdfs table,
  // i.e., the field of cardinality_ is equal to -1, we will attempt to compute an
  // estimate for the number of rows in getStatsNumbers().
  // Specifically, we divide the files into 3 categories - uncompressed,
  // legacy compressed (e.g., text, avro, rc, seq), and
  // columnar (e.g., parquet and orc).
  // Depending on the category of a file, we multiply the size of the file by
  // its corresponding compression factor to derive an estimated original size
  // of the file before compression.
  // These estimates were computed based on the empirical compression ratios
  // that we have observed for 3 tables in our tpch datasets:
  // customer, lineitem, and orders.
  // The max compression ratio we have seen for legacy formats is 3.58, whereas
  // the max compression ratio we have seen for columnar formats is 4.97.
  private static double ESTIMATED_COMPRESSION_FACTOR_UNCOMPRESSED = 1.0;
  private static double ESTIMATED_COMPRESSION_FACTOR_LEGACY = 3.58;
  private static double ESTIMATED_COMPRESSION_FACTOR_COLUMNAR = 4.97;

  private static Set<HdfsFileFormat> VALID_LEGACY_FORMATS =
      ImmutableSet.<HdfsFileFormat>builder()
          .add(HdfsFileFormat.RC_FILE)
          .add(HdfsFileFormat.TEXT)
          .add(HdfsFileFormat.SEQUENCE_FILE)
          .add(HdfsFileFormat.AVRO)
          .add(HdfsFileFormat.JSON)
          .build();

  public HdfsEstimatedMissingTableStats() {
    numPartitionsWithNumRows_ = 0;
    hasCorruptTableStats_ = false;
    partitionNumRows_ = -1;
    statsNumRows_ = -1;
  }

  public HdfsEstimatedMissingTableStats(TQueryOptions queryOptions, FeFsTable tbl,
      Collection<? extends FeFsPartition> partitions, long tableNumRowsHint) {
    int numPartitionsWithNumRows = 0;
    boolean hasCorruptTableStats = false;
    long partitionNumRows = -1;

    List<FeFsPartition> partitionsWithCorruptOrMissingStats = new ArrayList<>();
    for (FeFsPartition p : partitions) {
      long partNumRows = p.getNumRows();
      // Check for corrupt stats
      if (partNumRows < -1 || (partNumRows == 0 && p.getSize() > 0)) {
        hasCorruptTableStats = true;
        partitionsWithCorruptOrMissingStats.add(p);
      } else if (partNumRows == -1) { // Check for missing stats
        partitionsWithCorruptOrMissingStats.add(p);
      } else if (partNumRows > -1) {
        // Consider partition with good stats.
        if (partitionNumRows == -1) partitionNumRows = 0;
        partitionNumRows = MathUtil.addCardinalities(partitionNumRows, partNumRows);
        ++numPartitionsWithNumRows;
      }
    }
    // If all partitions have good stats, return the total row count contributed
    // by each of the partitions, as the row count for the table.
    if (partitionsWithCorruptOrMissingStats.size() == 0
        && numPartitionsWithNumRows > 0) {
      partitionNumRows_ = partitionNumRows;
      hasCorruptTableStats_ = hasCorruptTableStats;
      numPartitionsWithNumRows_ = numPartitionsWithNumRows;
      statsNumRows_ = partitionNumRows_;
      return;
    }

    // Set cardinality based on table-level stats.
    long numRows = tbl.getNumRows();
    // Depending on the query option of disable_hdfs_num_rows_est, if numRows
    // is still not available (-1), or partition stats is corrupted, we provide
    // a crude estimation by computing sumAvgRowSizes, the sum of the slot
    // size of each column of scalar type, and then generate the estimate using
    // sumValues(totalBytesPerFs_), the size of the hdfs table.
    if (!queryOptions.disable_hdfs_num_rows_estimate
        && (numRows == -1L || hasCorruptTableStats) && tableNumRowsHint == -1L) {
      // Compute the estimated table size from those partitions with missing or corrupt
      // row count, when taking compression into consideration
      long estimatedTableSize =
          computeEstimatedTableSize(partitionsWithCorruptOrMissingStats);

      double sumAvgRowSizes = 0.0;
      for (Column col : tbl.getColumns()) {
        Type currentType = col.getType();
        if (currentType instanceof ScalarType) {
          if (col.getStats().hasAvgSize()) {
            sumAvgRowSizes = sumAvgRowSizes + col.getStats().getAvgSerializedSize();
          } else {
            sumAvgRowSizes = sumAvgRowSizes + col.getType().getSlotSize();
          }
        }
      }

      long estNumRows = 0;
      if (sumAvgRowSizes == 0.0) {
        // When the type of each Column is of ArrayType or MapType,
        // sumAvgRowSizes would be equal to 0. In this case, we use a ultimate
        // fallback row width if sumAvgRowSizes == 0.0.
        estNumRows = Math.round(estimatedTableSize / DEFAULT_ROW_WIDTH_ESTIMATE);
      } else {
        estNumRows = Math.round(estimatedTableSize / sumAvgRowSizes);
      }

      // Include the row count contributed by partitions with good stats (if any).
      numRows = partitionNumRows =
          (partitionNumRows > 0) ? partitionNumRows + estNumRows : estNumRows;
    }

    if (numRows < -1 || (numRows == 0 && tbl.getTotalHdfsBytes() > 0)) {
      hasCorruptTableStats = true;
    }

    partitionNumRows_ = partitionNumRows;
    hasCorruptTableStats_ = hasCorruptTableStats;
    numPartitionsWithNumRows_ = numPartitionsWithNumRows;

    // Use hint value if table no stats or stats is corrupt and hint is set
    statsNumRows_ = (numRows >= 0 && !hasCorruptTableStats) || tableNumRowsHint == -1L ?
        numRows : tableNumRowsHint;
  }

  /**
   * Compute the estimated table size for the partitions contained in
   * the partitions argument when taking compression into consideration
   */
  private long computeEstimatedTableSize(List<FeFsPartition> partitions) {
    long estimatedTableSize = 0;
    for (FeFsPartition p : partitions) {
      HdfsFileFormat format = p.getFileFormat();
      long estimatedPartitionSize = 0;
      if (format == HdfsFileFormat.TEXT || format == HdfsFileFormat.JSON) {
        for (FileDescriptor desc : p.getFileDescriptors()) {
          HdfsCompression compression = HdfsCompression.fromFileName(desc.getPath());
          if (HdfsCompression.SUFFIX_MAP.containsValue(compression)) {
            estimatedPartitionSize += Math.round(desc.getFileLength()
                * ESTIMATED_COMPRESSION_FACTOR_LEGACY);
          } else {
            // When the text file is not compressed.
            estimatedPartitionSize += Math.round(desc.getFileLength()
                * ESTIMATED_COMPRESSION_FACTOR_UNCOMPRESSED);
          }
        }
      } else {
        // When the current partition is not a text file.
        if (VALID_LEGACY_FORMATS.contains(format)) {
          estimatedPartitionSize += Math.round(p.getSize()
              * ESTIMATED_COMPRESSION_FACTOR_LEGACY);
        } else {
         Preconditions.checkState(HdfsScanNode.VALID_COLUMNAR_FORMATS.contains(format),
             "Unknown HDFS compressed format: %s", format, this);
         estimatedPartitionSize += Math.round(p.getSize()
             * ESTIMATED_COMPRESSION_FACTOR_COLUMNAR);
        }
      }
      estimatedTableSize += estimatedPartitionSize;
    }
    return estimatedTableSize;
  }
}
