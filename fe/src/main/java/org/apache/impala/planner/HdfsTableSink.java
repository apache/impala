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
import java.util.Map;
import java.util.HashMap;
import java.util.List;
import java.util.Set;

import org.apache.impala.analysis.DescriptorTable;
import org.apache.impala.analysis.Expr;
import org.apache.impala.catalog.FeFsTable;
import org.apache.impala.catalog.FeIcebergTable;
import org.apache.impala.catalog.FeTable;
import org.apache.impala.catalog.HdfsFileFormat;
import org.apache.impala.common.Pair;
import org.apache.impala.thrift.TDataSink;
import org.apache.impala.thrift.TDataSinkType;
import org.apache.impala.thrift.TExplainLevel;
import org.apache.impala.thrift.THdfsTableSink;
import org.apache.impala.thrift.TQueryOptions;
import org.apache.impala.thrift.TSortingOrder;
import org.apache.impala.thrift.TTableSink;
import org.apache.impala.thrift.TTableSinkType;
import org.apache.impala.util.BitUtil;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableSet;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Sink for inserting into filesystem-backed tables.
 *
 * TODO(vercegovac): rename to FsTableSink
 */
public class HdfsTableSink extends TableSink {
  private final static Logger LOG = LoggerFactory.getLogger(HdfsTableSink.class);

  // The name of the table property that sets the parameters of writing Parquet Bloom
  // filters.
  public static final String PARQUET_BLOOM_FILTER_WRITING_TBL_PROPERTY =
    "parquet.bloom.filter.columns";

  // These constants are the maximal and minimal size of the bitset of a
  // ParquetBloomFilter object (in the BE). These should be kept in sync with the values
  // in be/src/util/parquet-bloom-filter.h.
  public static final long PARQUET_BLOOM_FILTER_MAX_BYTES = 128 * 1024 * 1024;
  public static final long PARQUET_BLOOM_FILTER_MIN_BYTES = 64;

  // Minimum total writes in bytes that individual HdfsTableSink should aim.
  // Used to estimate parallelism of writer fragment in DistributedPlanner.java.
  // This is set to match with HDFS_BLOCK_SIZE in hdfs-parquet-table-writer.h.
  public static final int MIN_WRITE_BYTES = 256 * 1024 * 1024;

  // Default number of partitions used for computeResourceProfile() in the absence of
  // column stats.
  protected final long DEFAULT_NUM_PARTITIONS = 10;

  // Coefficiencts for estimating insert CPU processing cost. Derived from benchmarking.
  // Cost per byte for Parquet inserts
  private static final double COST_COEFFICIENT_PARQUET_BYTES_INSERTED = 0.1170;
  // Fixed cost for Parquet inserts
  private static final double PARQUET_FIXED_INSERT_COST = 3954235.0;
  // Cost per byte for non-Parquet inserts (benchmarks used TEXT format)
  private static final double COST_COEFFICIENT_DEFAULT_BYTES_INSERTED = 0.2916;
  // Fixed cost for non-Parquet inserts (benchmarks used TEXT format)
  private static final double DEFAULT_FIXED_INSERT_COST = 3621898.0;

  // Exprs for computing the output partition(s).
  protected final List<Expr> partitionKeyExprs_;

  // Whether to overwrite the existing partition(s).
  protected final boolean overwrite_;

  // Indicates whether the input is ordered by the partition keys, meaning partitions can
  // be opened, written, and closed one by one.
  protected final boolean inputIsClustered_;

  // Indicates that this sink is being used to write query results
  protected final boolean isResultSink_;

  public static final Set<HdfsFileFormat> SUPPORTED_FILE_FORMATS = ImmutableSet.of(
      HdfsFileFormat.PARQUET, HdfsFileFormat.TEXT, HdfsFileFormat.RC_FILE,
      HdfsFileFormat.SEQUENCE_FILE, HdfsFileFormat.AVRO, HdfsFileFormat.ICEBERG);

  // Stores the indices into the list of non-clustering columns of the target table that
  // are stored in the 'sort.columns' table property. This is sent to the backend to
  // populate the RowGroup::sorting_columns list in parquet files.
  // If sortingOrder_ is not lexicographical, the backend will skip this process.
  private List<Integer> sortColumns_ = new ArrayList<>();
  private TSortingOrder sortingOrder_;

  // Stores the allocated write id if the target table is transactional, otherwise -1.
  private long writeId_;

  // Set the limit on the maximum number of hdfs table sink instances.
  // A value of 0 means no limit.
  private int maxHdfsSinks_;

  // Stores the desired location for the sink output. Primarily intended to be used by
  // external FEs that expect the results at a certain location for the purposes of
  // post processing and finalization.
  protected String externalOutputDir_;

  // This specifies the position in the partition specifition in which to start
  // creating partition directories.
  // Example:
  // Partition specification: year, month, day
  // The external frontend has provided a path that has year, month created
  // by them and thus provides a depth of 2 which hints to the TableSink to skip the
  // first two partition keys and create the day directory for new results. This is
  // usually used by tables with dynamic partition columns.
  private int externalOutputPartitionDepth_;

  public HdfsTableSink(FeTable targetTable, List<Expr> partitionKeyExprs,
      List<Expr> outputExprs, boolean overwrite, boolean inputIsClustered,
      Pair<List<Integer>, TSortingOrder> sortProperties, long writeId,
      int maxTableSinks, boolean isResultSink) {
    super(targetTable, Op.INSERT, outputExprs);
    Preconditions.checkState(targetTable instanceof FeFsTable);
    partitionKeyExprs_ = partitionKeyExprs;
    overwrite_ = overwrite;
    inputIsClustered_ = inputIsClustered;
    sortColumns_ = sortProperties.first;
    sortingOrder_ = sortProperties.second;
    writeId_ = writeId;
    maxHdfsSinks_ = maxTableSinks;
    isResultSink_ = isResultSink;
  }

  public void setExternalOutputDir(String externalOutputDir) {
    externalOutputDir_ = externalOutputDir;
  }

  public void setExternalOutputPartitionDepth(int partitionDepth) {
    externalOutputPartitionDepth_ = partitionDepth;
  }

  @Override
  public void computeProcessingCost(TQueryOptions queryOptions) {
    PlanNode inputNode = fragment_.getPlanRoot();
    long cardinality = Math.max(0, inputNode.getCardinality());
    float avgRowDataSize = inputNode.getAvgRowSizeWithoutPad();
    long estBytesInserted = (long) Math.ceil(avgRowDataSize * (double) cardinality);
    double totalCost = 0.0F;
    FeFsTable table = (FeFsTable) targetTable_;
    String fileFormat;
    Set<HdfsFileFormat> formats = table.getFileFormats();
    if (formats.contains(HdfsFileFormat.PARQUET)
        || formats.contains(HdfsFileFormat.ICEBERG)) {
      fileFormat = "PARQUET";
      // Use cost coefficients measured for Parquet format.
      totalCost = (estBytesInserted * COST_COEFFICIENT_PARQUET_BYTES_INSERTED)
          + PARQUET_FIXED_INSERT_COST;
    } else {
      fileFormat = "NON-PARQUET";
      // Use default cost coefficients measured with TEXT format.
      // TODO: Improve estimates for other formats
      totalCost = (estBytesInserted * COST_COEFFICIENT_DEFAULT_BYTES_INSERTED)
          + DEFAULT_FIXED_INSERT_COST;
    }
    if (LOG.isTraceEnabled()) {
      LOG.trace("HdfsTableSink insert CPU cost estimate: " + totalCost
          + ", File Format: " + fileFormat + ", Cardinality: " + cardinality
          + ", Estimated Bytes Inserted: " + estBytesInserted);
    }
    processingCost_ = ProcessingCost.basicCost(getLabel(), totalCost);
  }

  @Override
  public void computeResourceProfile(TQueryOptions queryOptions) {
    PlanNode inputNode = fragment_.getPlanRoot();
    int numInstances = fragment_.getNumInstances();
    // Compute the number of partitions buffered in memory at the same time, taking into
    // account the number of nodes and the data partition of the fragment executing this
    // sink.
    long numBufferedPartitionsPerInstance;
    if (inputIsClustered_) {
      // If the insert is clustered, it produces a single partition at a time.
      numBufferedPartitionsPerInstance = 1;
    } else {
      numBufferedPartitionsPerInstance =
          fragment_.getPerInstanceNdv(partitionKeyExprs_, false);
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
    if (formats.contains(HdfsFileFormat.PARQUET)) {
      // Writing to a Parquet partition requires up to 1GB of buffer. From a resource
      // management purview, even if there are non-Parquet partitions, we want to be
      // conservative and make a high memory estimate.
      return 1024L * 1024L * 1024L;
    }

    // For all other supported formats (TEXT, RC_FILE, SEQUENCE_FILE & AVRO)
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
    if (!(targetTable_ instanceof FeIcebergTable)) {
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
    if (explainLevel.ordinal() >= TExplainLevel.EXTENDED.ordinal()) {
      output.append(detailPrefix + "output exprs: ")
          .append(Expr.getExplainString(outputExprs_, explainLevel) + "\n");
    }
  }

  @Override
  protected String getLabel() {
    return "HDFS WRITER";
  }

  // The table property has the following format: a comma separated list of
  // 'col_name:bitset_size' pairs. The 'bitset_size' part means the size of the bitset of
  // the Bloom filter, and is optional. Values will be rounded up to the smallest power of
  // 2 not less than the given number. If the size is not given, it will be the maximal
  // Bloom filter size (PARQUET_BLOOM_FILTER_MAX_BYTES). No Bloom filter will be written
  // for columns not listed here.
  // Example: "col1:1024,col2,col4:100'.
  @VisibleForTesting
  static Map<String, Long> parseParquetBloomFilterWritingTblProp(final String tbl_prop) {
    Map<String, Long> result = new HashMap<>();
    String[] colSizePairs = tbl_prop.split(",");
    for (String colSizePair : colSizePairs) {
      String[] tokens = colSizePair.split(":");

      if (tokens.length == 0 || tokens.length > 2) {
        String err = "Invalid token in table property "
          + PARQUET_BLOOM_FILTER_WRITING_TBL_PROPERTY + ": "
          + colSizePair.trim()
          + ". Expected either a column name or a column name and a size "
          + "separated by a colon (';').";
        LOG.warn(err);
        return null;
      }

      long size;
      if (tokens.length == 1) {
        size = PARQUET_BLOOM_FILTER_MAX_BYTES;
      } else {
        assert tokens.length == 2;
        try {
          size = Long.parseLong(tokens[1].trim());
        } catch (NumberFormatException e) {
          String err =
                "Invalid bitset size in table property "
                + PARQUET_BLOOM_FILTER_WRITING_TBL_PROPERTY + ": "
                + tokens[1].trim();
          LOG.warn(err);
          return null;
        }

        size = Long.max(PARQUET_BLOOM_FILTER_MIN_BYTES, size);
        size = Long.min(PARQUET_BLOOM_FILTER_MAX_BYTES, size);
        size = BitUtil.roundUpToPowerOf2(size);
      }
      result.put(tokens[0].trim(), size);
    }
    return result;
  }

  @Override
  protected void toThriftImpl(TDataSink tsink) {
    THdfsTableSink hdfsTableSink = new THdfsTableSink(
        Expr.treesToThrift(partitionKeyExprs_), overwrite_, inputIsClustered_,
        sortingOrder_);
    FeFsTable table = (FeFsTable) targetTable_;
    StringBuilder error = new StringBuilder();
    int skipHeaderLineCount = table.parseSkipHeaderLineCount(error);
    // Errors will be caught during analysis.
    Preconditions.checkState(error.length() == 0);
    if (skipHeaderLineCount > 0) {
      hdfsTableSink.setSkip_header_line_count(skipHeaderLineCount);
    }

    org.apache.hadoop.hive.metastore.api.Table msTbl = table.getMetaStoreTable();
    Map<String, String> params = msTbl.getParameters();
    String parquetBloomTblProp = params.get(PARQUET_BLOOM_FILTER_WRITING_TBL_PROPERTY);
    if (parquetBloomTblProp != null) {
      Map<String, Long> parsedProperties = parseParquetBloomFilterWritingTblProp(
          parquetBloomTblProp);
      hdfsTableSink.setParquet_bloom_filter_col_info(parsedProperties);
    }

    hdfsTableSink.setSort_columns(sortColumns_);
    hdfsTableSink.setSorting_order(sortingOrder_);
    hdfsTableSink.setIs_result_sink(isResultSink_);
    if (externalOutputDir_ != null) {
      hdfsTableSink.setExternal_output_dir(externalOutputDir_);
      hdfsTableSink.setExternal_output_partition_depth(externalOutputPartitionDepth_);
    }
    if (writeId_ != -1) hdfsTableSink.setWrite_id(writeId_);
    TTableSink tTableSink = new TTableSink(DescriptorTable.TABLE_SINK_ID,
        TTableSinkType.HDFS, sinkOp_.toThrift());
    tTableSink.hdfs_table_sink = hdfsTableSink;
    tsink.table_sink = tTableSink;
    tsink.output_exprs = Expr.treesToThrift(outputExprs_);
  }

  @Override
  protected TDataSinkType getSinkType() {
    return TDataSinkType.TABLE_SINK;
  }

  @Override
  public void collectExprs(List<Expr> exprs) {
    if (!(targetTable_ instanceof FeIcebergTable)) exprs.addAll(partitionKeyExprs_);
    if (isResultSink_) {
      // It is expected outputExprs_ is fully populated with the expected result schema
      // when writing query results
      exprs.addAll(outputExprs_);
    } else {
      // Avoid adding any partition exprs redundantly.
      exprs.addAll(outputExprs_.subList(0,
          targetTable_.getNonClusteringColumns().size()));
    }
  }

  /**
   * Return an estimate of the number of nodes the fragment with this sink will
   * run on. This is based on the number of nodes set for the plan root and has an
   * upper limit set by the MAX_FS_WRITERS query option.
   */
  public int getNumNodes() {
    int num_nodes = getFragment().getPlanRoot().getNumNodes();
    if (maxHdfsSinks_ > 0) {
      // If there are more nodes than instances where the fragment was initially
      // planned to run then, then the instances will be distributed evenly across them.
      num_nodes = Math.min(num_nodes, getNumInstances());
    }
    return num_nodes;
  }

  /**
   * Return an estimate of the number of instances the fragment with this sink
   * will run on. This is based on the number of instances set for the plan root
   * and has an upper limit set by the MAX_FS_WRITERS query option.
   */
  public int getNumInstances() {
    int num_instances = getFragment().getPlanRoot().getNumInstances();
    if (maxHdfsSinks_ > 0) {
      num_instances =  Math.min(num_instances, maxHdfsSinks_);
    }
    return num_instances;
  }

  @Override
  public void computeRowConsumptionAndProductionToCost() {
    super.computeRowConsumptionAndProductionToCost();
    fragment_.setFixedInstanceCount(fragment_.getNumInstances());
  }
}
