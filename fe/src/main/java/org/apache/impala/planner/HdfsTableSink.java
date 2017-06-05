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

import java.util.List;

import org.apache.impala.analysis.DescriptorTable;
import org.apache.impala.analysis.Expr;
import org.apache.impala.catalog.HdfsFileFormat;
import org.apache.impala.catalog.HdfsTable;
import org.apache.impala.catalog.Table;
import org.apache.impala.thrift.TDataSink;
import org.apache.impala.thrift.TDataSinkType;
import org.apache.impala.thrift.TExplainLevel;
import org.apache.impala.thrift.THdfsTableSink;
import org.apache.impala.thrift.TQueryOptions;
import org.apache.impala.thrift.TTableSink;
import org.apache.impala.thrift.TTableSinkType;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;

/**
 * Base class for Hdfs data sinks such as HdfsTextTableSink.
 *
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

  // Stores the indices into the list of non-clustering columns of the target table that
  // are stored in the 'sort.columns' table property. This is sent to the backend to
  // populate the RowGroup::sorting_columns list in parquet files.
  private List<Integer> sortColumns_ = Lists.newArrayList();

  public HdfsTableSink(Table targetTable, List<Expr> partitionKeyExprs,
      boolean overwrite, boolean inputIsClustered, List<Integer> sortColumns) {
    super(targetTable, Op.INSERT);
    Preconditions.checkState(targetTable instanceof HdfsTable);
    partitionKeyExprs_ = partitionKeyExprs;
    overwrite_ = overwrite;
    inputIsClustered_ = inputIsClustered;
    sortColumns_ = sortColumns;
  }

  @Override
  public void computeResourceProfile(TQueryOptions queryOptions) {
    HdfsTable table = (HdfsTable) targetTable_;
    // TODO: Estimate the memory requirements more accurately by partition type.
    HdfsFileFormat format = table.getMajorityFormat();
    PlanNode inputNode = fragment_.getPlanRoot();
    int numInstances = fragment_.getNumInstances(queryOptions.getMt_dop());
    // Compute the per-instance number of partitions, taking the number of nodes
    // and the data partition of the fragment executing this sink into account.
    long numPartitionsPerInstance =
        fragment_.getPerInstanceNdv(queryOptions.getMt_dop(), partitionKeyExprs_);
    if (numPartitionsPerInstance == -1) {
      numPartitionsPerInstance = DEFAULT_NUM_PARTITIONS;
    }
    long perPartitionMemReq = getPerPartitionMemReq(format);

    long perInstanceMemEstimate;
    // The estimate is based purely on the per-partition mem req if the input cardinality_
    // or the avg row size is unknown.
    if (inputNode.getCardinality() == -1 || inputNode.getAvgRowSize() == -1) {
      perInstanceMemEstimate = numPartitionsPerInstance * perPartitionMemReq;
    } else {
      // The per-partition estimate may be higher than the memory required to buffer
      // the entire input data.
      long perInstanceInputCardinality =
          Math.max(1L, inputNode.getCardinality() / numInstances);
      long perInstanceInputBytes =
          (long) Math.ceil(perInstanceInputCardinality * inputNode.getAvgRowSize());
      long perInstanceMemReq =
          PlanNode.checkedMultiply(numPartitionsPerInstance, perPartitionMemReq);
      perInstanceMemEstimate = Math.min(perInstanceInputBytes, perInstanceMemReq);
    }
    resourceProfile_ = new ResourceProfile(perInstanceMemEstimate, 0);
  }

  /**
   * Returns the per-partition memory requirement for inserting into the given
   * file format.
   */
  private long getPerPartitionMemReq(HdfsFileFormat format) {
    switch (format) {
      case PARQUET:
        // Writing to a Parquet table requires up to 1GB of buffer per partition.
        // TODO: The per-partition memory requirement is configurable in the QueryOptions.
        return 1024L * 1024L * 1024L;
      case TEXT:
      case LZO_TEXT:
        // Very approximate estimate of amount of data buffered.
        return 100L * 1024L;
      case RC_FILE:
      case SEQUENCE_FILE:
      case AVRO:
        // Very approximate estimate of amount of data buffered.
        return 100L * 1024L;
      default:
        Preconditions.checkState(false, "Unsupported TableSink format " +
            format.toString());
    }
    return 0;
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
  protected TDataSink toThrift() {
    TDataSink result = new TDataSink(TDataSinkType.TABLE_SINK);
    THdfsTableSink hdfsTableSink = new THdfsTableSink(
        Expr.treesToThrift(partitionKeyExprs_), overwrite_, inputIsClustered_);
    HdfsTable table = (HdfsTable) targetTable_;
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
    result.table_sink = tTableSink;
    return result;
  }
}
