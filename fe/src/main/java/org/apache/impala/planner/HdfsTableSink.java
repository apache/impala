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

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import org.apache.impala.analysis.DescriptorTable;
import org.apache.impala.analysis.Expr;
import org.apache.impala.catalog.FeFsTable;
import org.apache.impala.catalog.FeTable;
import org.apache.impala.catalog.HdfsFileFormat;
import org.apache.impala.thrift.TDataSink;
import org.apache.impala.thrift.TDataSinkType;
import org.apache.impala.thrift.TExplainLevel;
import org.apache.impala.thrift.THdfsTableSink;
import org.apache.impala.thrift.TQueryOptions;
import org.apache.impala.thrift.TTableSink;
import org.apache.impala.thrift.TTableSinkType;

import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;

/**
 * Sink for inserting into filesystem-backed tables.
 *
 * TODO(vercegovac): rename to FsTableSink
 */
public class HdfsTableSink extends TableSink {
  // Default number of partitions used for computeResourceProfile() in the absence of
  // column stats.
  protected final long DEFAULT_NUM_PARTITIONS = 10;

  // Exprs for computing the output partition(s).
  protected final List<Expr> partitionKeyExprs_;
  // Whether to overwrite the existing partition(s).
  protected final boolean overwrite_;

  // Indicates whether the input is ordered by the partition keys, meaning partitions can
  // be opened, written, and closed one by one.
  protected final boolean inputIsClustered_;

  private static final Set<HdfsFileFormat> SUPPORTED_FILE_FORMATS = ImmutableSet.of(
      HdfsFileFormat.PARQUET, HdfsFileFormat.TEXT, HdfsFileFormat.LZO_TEXT,
      HdfsFileFormat.RC_FILE, HdfsFileFormat.SEQUENCE_FILE, HdfsFileFormat.AVRO);

  // Stores the indices into the list of non-clustering columns of the target table that
  // are stored in the 'sort.columns' table property. This is sent to the backend to
  // populate the RowGroup::sorting_columns list in parquet files.
  private List<Integer> sortColumns_ = new ArrayList<>();

  public HdfsTableSink(FeTable targetTable, List<Expr> partitionKeyExprs,
      boolean overwrite, boolean inputIsClustered, List<Integer> sortColumns) {
    super(targetTable, Op.INSERT);
    Preconditions.checkState(targetTable instanceof FeFsTable);
    partitionKeyExprs_ = partitionKeyExprs;
    overwrite_ = overwrite;
    inputIsClustered_ = inputIsClustered;
    sortColumns_ = sortColumns;
  }

  @Override
  public void computeResourceProfile(TQueryOptions queryOptions) {
    PlanNode inputNode = fragment_.getPlanRoot();
    int numInstances = fragment_.getNumInstances(queryOptions.getMt_dop());
    // Compute the number of partitions buffered in memory at the same time, taking into
    // account the number of nodes and the data partition of the fragment executing this
    // sink.
    long numBufferedPartitionsPerInstance;
    if (inputIsClustered_) {
      // If the insert is clustered, it produces a single partition at a time.
      numBufferedPartitionsPerInstance = 1;
    } else {
      numBufferedPartitionsPerInstance =
          fragment_.getPerInstanceNdv(queryOptions.getMt_dop(), partitionKeyExprs_);
      if (numBufferedPartitionsPerInstance == -1) {
        numBufferedPartitionsPerInstance = DEFAULT_NUM_PARTITIONS;
      }
    }

    FeFsTable table = (FeFsTable) targetTable_;
    // TODO: Estimate the memory requirements more accurately by partition type.
    Set<HdfsFileFormat> formats = table.getFileFormats();
    long perPartitionMemReq = getPerPartitionMemReq(formats);

    long perInstanceMemEstimate;
    // The estimate is based purely on the per-partition mem req if the input cardinality_
    // or the avg row size is unknown.
    if (inputNode.getCardinality() == -1 || inputNode.getAvgRowSize() == -1) {
      perInstanceMemEstimate = numBufferedPartitionsPerInstance * perPartitionMemReq;
    } else {
      // The per-partition estimate may be higher than the memory required to buffer
      // the entire input data.
      long perInstanceInputCardinality =
          Math.max(1L, inputNode.getCardinality() / numInstances);
      long perInstanceInputBytes =
          (long) Math.ceil(perInstanceInputCardinality * inputNode.getAvgRowSize());
      long perInstanceMemReq =
          PlanNode.checkedMultiply(numBufferedPartitionsPerInstance, perPartitionMemReq);
      perInstanceMemEstimate = Math.min(perInstanceInputBytes, perInstanceMemReq);
    }
    resourceProfile_ = ResourceProfile.noReservation(perInstanceMemEstimate);
  }

  /**
   * Returns the per-partition memory requirement for inserting into the given
   * set of file formats.
   */
  private long getPerPartitionMemReq(Set<HdfsFileFormat> formats) {
    Set<HdfsFileFormat> unsupportedFormats =
        Sets.difference(formats, SUPPORTED_FILE_FORMATS);
    if (!unsupportedFormats.isEmpty()) {
      Preconditions.checkState(false,
          "Unsupported TableSink format(s): " + Joiner.on(',').join(unsupportedFormats));
    }
    if (formats.contains(HdfsFileFormat.PARQUET)) {
      // Writing to a Parquet partition requires up to 1GB of buffer. From a resource
      // management purview, even if there are non-Parquet partitions, we want to be
      // conservative and make a high memory estimate.
      return 1024L * 1024L * 1024L;
    }

    // For all other supported formats (TEXT, LZO_TEXT, RC_FILE, SEQUENCE_FILE & AVRO)
    // 100KB is a very approximate estimate of the amount of data buffered.
    return 100L * 1024L;
  }

  @Override
  public void appendSinkExplainString(String prefix, String detailPrefix,
      TQueryOptions queryOptions, TExplainLevel explainLevel, StringBuilder output) {
    String overwriteStr = ", OVERWRITE=" + (overwrite_ ? "true" : "false");
    String partitionKeyStr = "";
    if (!partitionKeyExprs_.isEmpty()) {
      StringBuilder tmpBuilder = new StringBuilder(", PARTITION-KEYS=(");
      for (Expr expr: partitionKeyExprs_) {
        tmpBuilder.append(expr.toSql() + ",");
      }
      tmpBuilder.deleteCharAt(tmpBuilder.length() - 1);
      tmpBuilder.append(")");
      partitionKeyStr = tmpBuilder.toString();
    }
    output.append(String.format("%sWRITE TO HDFS [%s%s%s]\n", prefix,
        targetTable_.getFullName(), overwriteStr, partitionKeyStr));
    // Report the total number of partitions, independent of the number of nodes
    // and the data partition of the fragment executing this sink.
    if (explainLevel.ordinal() > TExplainLevel.MINIMAL.ordinal()) {
      long totalNumPartitions = Expr.getNumDistinctValues(partitionKeyExprs_);
      if (totalNumPartitions == -1) {
        output.append(detailPrefix + "partitions=unavailable");
      } else {
        output.append(detailPrefix + "partitions="
            + (totalNumPartitions == 0 ? 1 : totalNumPartitions));
      }
      output.append("\n");
    }
  }

  @Override
  protected String getLabel() {
    return "HDFS WRITER";
  }

  @Override
  protected void toThriftImpl(TDataSink tsink) {
    THdfsTableSink hdfsTableSink = new THdfsTableSink(
        Expr.treesToThrift(partitionKeyExprs_), overwrite_, inputIsClustered_);
    FeFsTable table = (FeFsTable) targetTable_;
    StringBuilder error = new StringBuilder();
    int skipHeaderLineCount = table.parseSkipHeaderLineCount(error);
    // Errors will be caught during analysis.
    Preconditions.checkState(error.length() == 0);
    if (skipHeaderLineCount > 0) {
      hdfsTableSink.setSkip_header_line_count(skipHeaderLineCount);
    }
    hdfsTableSink.setSort_columns(sortColumns_);
    TTableSink tTableSink = new TTableSink(DescriptorTable.TABLE_SINK_ID,
        TTableSinkType.HDFS, sinkOp_.toThrift());
    tTableSink.hdfs_table_sink = hdfsTableSink;
    tsink.table_sink = tTableSink;
  }

  @Override
  protected TDataSinkType getSinkType() {
    return TDataSinkType.TABLE_SINK;
  }
}
