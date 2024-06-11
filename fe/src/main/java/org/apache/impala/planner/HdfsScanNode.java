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

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.TreeMap;
import java.util.stream.Collectors;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.impala.analysis.Analyzer;
import org.apache.impala.analysis.BinaryPredicate;
import org.apache.impala.analysis.DescriptorTable;
import org.apache.impala.analysis.Expr;
import org.apache.impala.analysis.InPredicate;
import org.apache.impala.analysis.IsNotEmptyPredicate;
import org.apache.impala.analysis.IsNullPredicate;
import org.apache.impala.analysis.LiteralExpr;
import org.apache.impala.analysis.MultiAggregateInfo;
import org.apache.impala.analysis.SlotDescriptor;
import org.apache.impala.analysis.SlotId;
import org.apache.impala.analysis.SlotRef;
import org.apache.impala.analysis.TableRef;
import org.apache.impala.analysis.TableSampleClause;
import org.apache.impala.analysis.TupleDescriptor;
import org.apache.impala.analysis.TupleId;
import org.apache.impala.catalog.Column;
import org.apache.impala.catalog.ColumnStats;
import org.apache.impala.catalog.FeFsPartition;
import org.apache.impala.catalog.FeFsTable;
import org.apache.impala.catalog.FeTable;
import org.apache.impala.catalog.HdfsCompression;
import org.apache.impala.catalog.HdfsFileFormat;
import org.apache.impala.catalog.HdfsPartition.FileBlock;
import org.apache.impala.catalog.HdfsPartition.FileDescriptor;
import org.apache.impala.catalog.PrimitiveType;
import org.apache.impala.catalog.ScalarType;
import org.apache.impala.catalog.Type;
import org.apache.impala.common.FileSystemUtil;
import org.apache.impala.common.ImpalaException;
import org.apache.impala.common.ImpalaRuntimeException;
import org.apache.impala.common.InternalException;
import org.apache.impala.common.NotImplementedException;
import org.apache.impala.common.Pair;
import org.apache.impala.common.PrintUtils;
import org.apache.impala.common.RuntimeEnv;
import org.apache.impala.common.ThriftSerializationCtx;
import org.apache.impala.fb.FbFileBlock;
import org.apache.impala.planner.RuntimeFilterGenerator.RuntimeFilter;
import org.apache.impala.service.BackendConfig;
import org.apache.impala.thrift.TExplainLevel;
import org.apache.impala.thrift.TExpr;
import org.apache.impala.thrift.TFileSplitGeneratorSpec;
import org.apache.impala.thrift.THdfsFileSplit;
import org.apache.impala.thrift.THdfsScanNode;
import org.apache.impala.thrift.TNetworkAddress;
import org.apache.impala.thrift.TOverlapPredicateDesc;
import org.apache.impala.thrift.TPlanNode;
import org.apache.impala.thrift.TPlanNodeType;
import org.apache.impala.thrift.TQueryOptions;
import org.apache.impala.thrift.TReplicaPreference;
import org.apache.impala.thrift.TRuntimeFilterType;
import org.apache.impala.thrift.TScanRange;
import org.apache.impala.thrift.TScanRangeLocation;
import org.apache.impala.thrift.TScanRangeLocationList;
import org.apache.impala.thrift.TScanRangeSpec;
import org.apache.impala.thrift.TSortingOrder;
import org.apache.impala.thrift.TTableStats;
import org.apache.impala.util.AcidUtils;
import org.apache.impala.util.BitUtil;
import org.apache.impala.util.ExecutorMembershipSnapshot;
import org.apache.impala.util.MathUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Joiner;
import com.google.common.base.MoreObjects;
import com.google.common.base.MoreObjects.ToStringHelper;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

/**
 * Scan of a single table.
 *
 * It's expected that the creator of this object has already done any necessary
 * partition pruning before creating this object. In other words, the 'conjuncts'
 * passed to the constructors are conjuncts not fully evaluated by partition pruning
 * and 'partitions' are the remaining partitions after pruning.
 *
 * Supports scanning a random sample of files based on the parameters from a
 * TABLESAMPLE clause. Scan predicates and the sampling are independent, so we first
 * prune partitions and then randomly select files from those partitions.
 *
 * For scans of tables with Parquet files the class sends over additional information
 * to the backend to enable more aggressive runtime pruning. Two types of pruning are
 * supported:
 *
 * 1. Min-max pruning: the class creates an additional list of conjuncts from applicable
 * scan-node conjuncts and collection conjuncts. The additional conjuncts are
 * used to prune a row group if any fail the row group's min-max parquet::Statistics.
 *
 * 2. Dictionary pruning: the class identifies which scan-node conjuncts and collection
 * conjuncts can be used to prune a row group by evaluating conjuncts on the
 * column dictionaries.
 *
 * TODO: pass in range restrictions.
 */
public class HdfsScanNode extends ScanNode {
  private final static Logger LOG = LoggerFactory.getLogger(HdfsScanNode.class);

  private static final Configuration CONF = new Configuration();

  // Maximum number of I/O buffers per thread executing this scan.
  // TODO: it's unclear how this was chosen - this seems like a very high number
  private static final long MAX_IO_BUFFERS_PER_THREAD = 10;

  // Maximum number of thread tokens per core that may be used to spin up extra scanner
  // threads. Corresponds to the default value of --num_threads_per_core in the backend.
  private static final int MAX_THREAD_TOKENS_PER_CORE = 3;

  // The minimum amount of memory we estimate a scan will use. The number is
  // derived experimentally: running metadata-only Parquet count(*) scans on TPC-H
  // lineitem and TPC-DS store_sales of different sizes resulted in memory consumption
  // between 128kb and 1.1mb.
  private static final long MIN_MEMORY_ESTIMATE = 1L * 1024L * 1024L;

  // Default reservation in bytes for a IoMgr scan range for a column in columnar
  // formats like Parquet. Chosen to allow reasonably efficient I/O for all columns
  // even with only the minimum reservation, but not to use excessive memory for columns
  // where we overestimate the size.
  // TODO: is it worth making this a tunable query option?
  private static final long DEFAULT_COLUMN_SCAN_RANGE_RESERVATION = 4L * 1024L * 1024L;

  // Read size for Parquet and ORC footers.
  // Matches HdfsParquetScanner::PARQUET_FOOTER_SIZE in backend.
  private static final long PARQUET_FOOTER_SIZE = 100L * 1024L;

  // Read size for ORC footers.
  // Matches HdfsOrcScanner::ORC_FOOTER_SIZE in backend.
  private static final long ORC_FOOTER_SIZE = 16L * 1024L;

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

  // Adjustment factor for inputCardinality_ calculation used when doing a partition
  // key scan to reflect that opening a file to read a single row imposes significant
  // overhead per row.
  private static long PARTITION_KEY_SCAN_INPUT_CARDINALITY_ADJUSTMENT_FACTOR = 100;

  private static Set<HdfsFileFormat> VALID_LEGACY_FORMATS =
      ImmutableSet.<HdfsFileFormat>builder()
          .add(HdfsFileFormat.RC_FILE)
          .add(HdfsFileFormat.TEXT)
          .add(HdfsFileFormat.SEQUENCE_FILE)
          .add(HdfsFileFormat.AVRO)
          .add(HdfsFileFormat.JSON)
          .build();

  private static Set<HdfsFileFormat> VALID_COLUMNAR_FORMATS =
      ImmutableSet.<HdfsFileFormat>builder()
          .add(HdfsFileFormat.PARQUET)
          .add(HdfsFileFormat.HUDI_PARQUET)
          .add(HdfsFileFormat.ORC)
          .add(HdfsFileFormat.ICEBERG)
          .build();

  // Coefficients for estimating scan CPU processing cost. Derived from benchmarking.
  // Cost per byte materialized for columnar or per byte scanned for non-columnar.
  private static final double COST_COEFFICIENT_COLUMNAR_BYTES_MATERIALIZED = 0.0144;
  private static final double COST_COEFFICIENT_NONCOLUMNAR_BYTES_SCANNED = 0.0354;
  // Cost per predicate per row
  private static final double COST_COEFFICIENT_COLUMNAR_PREDICATE_EVAL = 0.0281;
  private static final double COST_COEFFICIENT_NONCOLUMNAR_PREDICATE_EVAL = 0.0549;

  //An estimate of the width of a row when the information is not available.
  private double DEFAULT_ROW_WIDTH_ESTIMATE = 1.0;

  private final FeFsTable tbl_;

  // List of partitions to be scanned. Partitions have been pruned.
  protected final List<FeFsPartition> partitions_;

  // List of paritions that has been reduced through sampling.
  // Only initialized at checkSamplingAndCountStar() if sampling is True.
  // Accessors must fallback to partitions_ if sampledPartitions_ stays null after
  // checkSamplingAndCountStar().
  protected List<FeFsPartition> sampledPartitions_ = null;

  // Parameters for table sampling. Null if not sampling.
  private final TableSampleClause sampleParams_;

  private final TReplicaPreference replicaPreference_;
  private final boolean randomReplica_;

  // Number of partitions, files and bytes scanned. Set in computeScanRangeLocations().
  // Might not match 'partitions_' due to table sampling. Grouped by the FsType, so
  // each key value pair maps how many partitions / files / bytes are stored on each fs.
  // Stored as a TreeMap so that iteration order is defined by the order of enums in
  // FsType.
  private Map<FileSystemUtil.FsType, Long> numPartitionsPerFs_ = new TreeMap<>();
  private Map<FileSystemUtil.FsType, Long> totalFilesPerFs_ = new TreeMap<>();
  private Map<FileSystemUtil.FsType, Long> totalBytesPerFs_ = new TreeMap<>();

  // Number of erasure coded files and bytes scanned, groupped by the FsType.
  private Map<FileSystemUtil.FsType, Long> totalFilesPerFsEC_ = new TreeMap<>();
  private Map<FileSystemUtil.FsType, Long> totalBytesPerFsEC_ = new TreeMap<>();

  // File formats scanned. Set in checkSamplingAndCountStar().
  // HdfsFileFormat.ICEBERG is always excluded from this set.
  // Populated in checkSamplingAndCountStar().
  protected Set<HdfsFileFormat> fileFormats_ = new HashSet<>();

  // Whether all formats scanned are Parquet. Set in computeScanRangeLocations().
  private boolean allParquet_ = false;

  // Whether all formats scanned are columnar format. Set in computeScanRangeLocations().
  private boolean allColumnarFormat_ = false;

  // Number of bytes in the largest scan range (i.e. hdfs split). Set in
  // computeScanRangeLocations().
  private long largestScanRangeBytes_ = 0;

  // Input cardinality based on the partition row counts or extrapolation. -1 if invalid.
  // Both values can be valid to report them in the explain plan, but only one of them is
  // used for determining the scan cardinality.
  private long partitionNumRows_ = -1;
  private long extrapolatedNumRows_ = -1;

  // Number of scan ranges that will be generated for all TFileSplitGeneratorSpec's.
  private long generatedScanRangeCount_ = 0;

  // Estimated row count of the largest scan range. -1 if no stats are available.
  // Set in computeScanRangeLocations()
  private long maxScanRangeNumRows_ = -1;

  // True if this scan node should use the MT implementation in the backend.
  // Set in computeNodeResourceProfile().
  private boolean useMtScanNode_;

  // True if this is a scan that only returns partition keys and is only required to
  // return at least one of each of the distinct values of the partition keys.
  private final boolean isPartitionKeyScan_;

  // Conjuncts that can be evaluated while materializing the items (tuples) of
  // collection-typed slots. Maps from tuple descriptor to the conjuncts bound by that
  // tuple. Uses a linked hash map for consistent display in explain.
  private final Map<TupleDescriptor, List<Expr>> collectionConjuncts_ =
      new LinkedHashMap<>();

  // TupleDescriptors of collection slots that have an IsNotEmptyPredicate. See
  // SelectStmt#registerIsNotEmptyPredicates.
  // Correctness for applying min-max and dictionary filters requires that the nested
  // collection is tested to be not empty (via the IsNotEmptyPredicate).
  // These filters are added by analysis (see: SelectStmt#registerIsNotEmptyPredicates).
  // While correct, they may be conservative. See the tests for parquet collection
  // filtering for examples that could benefit from being more aggressive
  // (yet still correct).
  private final Set<TupleDescriptor> notEmptyCollections_ = new HashSet<>();

  // Map from SlotDescriptor to indices in PlanNodes.conjuncts_ and
  // collectionConjuncts_ that are eligible for dictionary filtering. Slots in the
  // the TupleDescriptor of this scan node map to indices into PlanNodes.conjuncts_ and
  // slots in the TupleDescriptors of nested types map to indices into
  // collectionConjuncts_.
  private final Map<SlotDescriptor, List<Integer>> dictionaryFilterConjuncts_ =
      new LinkedHashMap<>();

  // Number of partitions that have the row count statistic.
  private int numPartitionsWithNumRows_ = 0;

  // Indicates corrupt table stats based on the number of non-empty scan ranges and
  // numRows set to 0. Set in computeStats().
  private boolean hasCorruptTableStats_;

  // Number of header lines to skip at the beginning of each file of this table. Only set
  // to values > 0 for hdfs text files.
  private int skipHeaderLineCount_ = 0;

  // Number of scan-ranges/files/partitions that have missing disk ids. Reported in the
  // explain plan.
  private int numScanRangesNoDiskIds_ = 0;
  private int numFilesNoDiskIds_ = 0;
  private int numPartitionsNoDiskIds_ = 0;

  // List of conjuncts for min/max values of parquet/orc statistics, that are used to skip
  // data when scanning Parquet/ORC files.
  private final List<Expr> statsConjuncts_ = new ArrayList<>();

  // Map from TupleDescriptor to list of PlanNode conjuncts that have been transformed
  // into conjuncts in 'statsConjuncts_'.
  private final Map<TupleDescriptor, List<Expr>> statsOriginalConjuncts_ =
      new LinkedHashMap<>();

  // Tuple that is used to materialize statistics when scanning Parquet or ORC files.
  // For each column it can contain 0, 1, or 2 slots, depending on whether the column
  // needs to be evaluated against the min and/or the max value of the corresponding
  // file statistics.
  private TupleDescriptor statsTuple_;

  // The list of overlap predicate descs. See TOverlapPredicateDesc in PlanNodes.thrift.
  private ArrayList<TOverlapPredicateDesc> overlapPredicateDescs_ = new ArrayList<>();

  // Index of the first slot in statsTuple_ for overlap predicates.
  private int overlap_first_slot_idx_ = -1;

  // Slot that is used to record the Parquet metadata for the count(*) aggregation if
  // this scan node has the count(*) optimization enabled.
  protected SlotDescriptor countStarSlot_ = null;

  // Sampled file descriptors if table sampling is used. Grouped by partition id.
  // Initialized in checkSamplingAndCountStar();
  Map<Long, List<FileDescriptor>> sampledFiles_ = null;

  // Conjuncts used to trim the set of partitions passed to this node.
  // Used only to display EXPLAIN information.
  private final List<Expr> partitionConjuncts_;

  private boolean isFullAcidTable_ = false;

  /**
   * Construct a node to scan given data files into tuples described by 'desc',
   * with 'conjuncts' being the unevaluated conjuncts bound by the tuple and
   * 'partitions' being the partitions which need to be included. Please see
   * class comments above for details.
   */
  public HdfsScanNode(PlanNodeId id, TupleDescriptor desc, List<Expr> conjuncts,
      List<? extends FeFsPartition> partitions, TableRef hdfsTblRef,
      MultiAggregateInfo aggInfo, List<Expr> partConjuncts, boolean isPartitionKeyScan) {
    super(id, desc, createDisplayName(hdfsTblRef.getTable()));
    tbl_ = (FeFsTable)desc.getTable();
    conjuncts_ = conjuncts;
    partitions_ = new ArrayList<>(partitions);
    partitionConjuncts_ = partConjuncts;
    sampleParams_ = hdfsTblRef.getSampleParams();
    replicaPreference_ = hdfsTblRef.getReplicaPreference();
    randomReplica_ = hdfsTblRef.getRandomReplica();
    tableNumRowsHint_ = hdfsTblRef.getTableNumRowsHint();
    FeFsTable hdfsTable = (FeFsTable)hdfsTblRef.getTable();
    Preconditions.checkState(tbl_ == hdfsTable);
    isFullAcidTable_ =
        AcidUtils.isFullAcidTable(hdfsTable.getMetaStoreTable().getParameters());
    StringBuilder error = new StringBuilder();
    aggInfo_ = aggInfo;
    skipHeaderLineCount_ = tbl_.parseSkipHeaderLineCount(error);
    if (error.length() > 0) {
      // Any errors should already have been caught during analysis.
      throw new IllegalStateException(error.toString());
    }
    isPartitionKeyScan_ = isPartitionKeyScan;
  }

  /**
   * Returns the display name for this scan node. Of the form "SCAN [storage-layer-name]"
   */
  private static String createDisplayName(FeTable table) {
    Preconditions.checkState(table instanceof FeFsTable);
    return "SCAN " + ((FeFsTable) table).getFsType();
  }

  @Override
  protected String debugString() {
    ToStringHelper helper = MoreObjects.toStringHelper(this);
    for (FeFsPartition partition: partitions_) {
      helper.add("Partition " + partition.getId() + ":", partition.toString());
    }
    return helper.addValue(super.debugString()).toString();
  }

  /**
   * Returns true if this scan node contains PARQUET or HUDI_PARQUET
   */
  private boolean hasParquet(Set<HdfsFileFormat> fileFormats) {
    return fileFormats.contains(HdfsFileFormat.PARQUET)
        || fileFormats.contains(HdfsFileFormat.HUDI_PARQUET);
  }

  /**
   * Returns true if this scan node contains ORC.
   */
  private boolean hasOrc(Set<HdfsFileFormat> fileFormats) {
    return fileFormats.contains(HdfsFileFormat.ORC);
  }

  /**
   * Returns true if the count(*) optimization can be applied to the query block
   * of this scan node.
   */
  protected boolean canApplyCountStarOptimization(Analyzer analyzer,
      Set<HdfsFileFormat> fileFormats) {
    if (fileFormats.size() != 1) return false;
    if (isFullAcidTable_) return false;
    if (!hasParquet(fileFormats) && !hasOrc(fileFormats)) return false;
    return canApplyCountStarOptimization(analyzer);
  }

  // Return sampledPartitions_ if not null. Otherwise, return partitions_.
  private List<FeFsPartition> getSampledOrRawPartitions() {
    return sampledPartitions_ == null ? partitions_ : sampledPartitions_;
  }

  /**
   * Populate collectionConjuncts_ and scanRanges_.
   */
  @Override
  public void init(Analyzer analyzer) throws ImpalaException {
    conjuncts_ = orderConjunctsByCost(conjuncts_);
    checkSamplingAndCountStar(analyzer);
    checkForSupportedFileFormats();

    assignCollectionConjuncts(analyzer);

    // compute scan range locations with optional sampling
    computeScanRangeLocations(analyzer);

    if (hasParquet(fileFormats_)) {
      // Compute min-max conjuncts only if the PARQUET_READ_STATISTICS query option is
      // set to true.
      if (analyzer.getQueryOptions().parquet_read_statistics) {
        computeStatsTupleAndConjuncts(analyzer);
      }
      // Compute dictionary conjuncts only if the PARQUET_DICTIONARY_FILTERING query
      // option is set to true.
      if (analyzer.getQueryOptions().parquet_dictionary_filtering) {
        computeDictionaryFilterConjuncts(analyzer);
      }
    }

    if (hasOrc(fileFormats_)) {
      // Compute min-max conjuncts only if the ORC_READ_STATISTICS query option is
      // set to true.
      if (analyzer.getQueryOptions().orc_read_statistics) {
        computeStatsTupleAndConjuncts(analyzer);
      }
    }

    computeMemLayout(analyzer);

    // This is towards the end, so that it can take all conjuncts, scan ranges and mem
    // layout into account.
    computeStats(analyzer);

    // TODO: do we need this?
    assignedConjuncts_ = analyzer.getAssignedConjuncts();
  }

  /**
   * Initialize sampledFiles_, sampledPartitions_, fileFormats_, and countStarSlot_.
   * @param analyzer Analyzer object used to init this class.
   */
  private void checkSamplingAndCountStar(Analyzer analyzer) {
    if (sampleParams_ != null) {
      long percentBytes = sampleParams_.getPercentBytes();
      long randomSeed;
      if (sampleParams_.hasRandomSeed()) {
        randomSeed = sampleParams_.getRandomSeed();
      } else {
        randomSeed = System.currentTimeMillis();
      }
      // Pass a minimum sample size of 0 because users cannot set a minimum sample size
      // for scans directly. For compute stats, a minimum sample size can be set, and
      // the sampling percent is adjusted to reflect it.
      sampledFiles_ = getFilesSample(percentBytes, 0, randomSeed);
    }

    if (sampledFiles_ != null) {
      // Initialize sampledPartitions_.
      sampledPartitions_ = new ArrayList<>();
      for (FeFsPartition partition : partitions_) {
        Preconditions.checkState(partition.getId() >= 0);
        if (sampledFiles_.get(partition.getId()) != null) {
          sampledPartitions_.add(partition);
        }
      }
    }

    // Populate fileFormats_.
    for (FeFsPartition partition : getSampledOrRawPartitions()) {
      if (partition.getFileFormat() != HdfsFileFormat.ICEBERG) {
        fileFormats_.add(partition.getFileFormat());
      }
    }

    // Initialize countStarSlot_.
    if (canApplyCountStarOptimization(analyzer, fileFormats_)) {
      Preconditions.checkState(desc_.getPath().destTable() != null);
      Preconditions.checkState(collectionConjuncts_.isEmpty());
      countStarSlot_ = applyCountStarOptimization(analyzer);
    }
  }

  /**
   * Throws NotImplementedException if we do not support scanning the partition.
   * Specifically:
   * 1) if the table schema contains a complex type and we need to scan
   * a partition that has a format for which we do not support complex types,
   * regardless of whether a complex-typed column is actually referenced
   * in the query.
   * 2) if we are scanning compressed json file or the json scanner is disabled.
   */
  @Override
  protected void checkForSupportedFileFormats() throws NotImplementedException {
    Preconditions.checkNotNull(desc_);
    Preconditions.checkNotNull(desc_.getTable());

    for (FeFsPartition part: partitions_) {
      if (!part.getFileFormat().equals(HdfsFileFormat.JSON)) continue;
      if (!BackendConfig.INSTANCE.isJsonScannerEnabled()) {
        throw new NotImplementedException(
            "JSON scans are disabled by --enable_json_scanner flag.");
      }
    }

    Column firstComplexTypedCol = null;
    for (Column col: desc_.getTable().getColumns()) {
      if (col.getType().isComplexType()) {
        firstComplexTypedCol = col;
        break;
      }
    }
    if (firstComplexTypedCol == null) return;

    boolean referencesComplexTypedCol = false;
    for (SlotDescriptor slotDesc: desc_.getSlots()) {
      if (!slotDesc.isMaterialized()) continue;
      if (slotDesc.getType().isComplexType() || slotDesc.getColumn() == null) {
        referencesComplexTypedCol = true;
        break;
      }
    }

    for (FeFsPartition part: partitions_) {
      HdfsFileFormat format = part.getFileFormat();
      if (format.isComplexTypesSupported()) continue;
      // If the file format allows querying just scalar typed columns and the query
      // doesn't materialize any complex typed columns, it is allowed.
      if (format.canSkipComplexTypes() && !referencesComplexTypedCol) {
        continue;
      }
      String errSuffix = String.format(
          "Complex types are supported for these file formats: %s",
          Joiner.on(", ").join(HdfsFileFormat.complexTypesFormats()));
      if (desc_.getTable().getNumClusteringCols() == 0) {
        throw new NotImplementedException(String.format(
            "Scan of table '%s' in format '%s' is not supported because the table " +
            "has a column '%s' with a complex type '%s'.\n%s.",
            desc_.getAlias(), format, firstComplexTypedCol.getName(),
            firstComplexTypedCol.getType().toSql(), errSuffix));
      }
      throw new NotImplementedException(String.format(
          "Scan of partition '%s' in format '%s' of table '%s' is not supported " +
          "because the table has a column '%s' with a complex type '%s'.\n%s.",
          part.getPartitionName(), format, desc_.getAlias(),
          firstComplexTypedCol.getName(), firstComplexTypedCol.getType().toSql(),
          errSuffix));
    }
  }

  /**
   * Populates the collection conjuncts, materializes their required slots, and marks
   * the conjuncts as assigned, if it is correct to do so. Some conjuncts may have to
   * also be evaluated at a subsequent semi or outer join.
   */
  private void assignCollectionConjuncts(Analyzer analyzer) {
    collectionConjuncts_.clear();
    addNotEmptyCollections(conjuncts_);
    assignCollectionConjuncts(desc_, analyzer);
  }

  /**
   * Builds a predicate to evaluate against parquet::Statistics by copying 'inputSlot'
   * into 'statsTuple_', combining 'inputSlot', 'inputPred' and 'op' into a new
   * predicate, and adding it to 'statsConjuncts_'.
   */
  private void buildBinaryStatsPredicate(Analyzer analyzer, SlotRef inputSlot,
      BinaryPredicate inputPred, BinaryPredicate.Operator op) {
    // Obtain the rhs expr of the input predicate
    Expr constExpr = inputPred.getChild(1).clone();
    Preconditions.checkState(constExpr.isConstant());

    // Make a new slot descriptor, which adds it to the tuple descriptor.
    SlotDescriptor slotDesc = analyzer.getDescTbl().copySlotDescriptor(statsTuple_,
        inputSlot.getDesc());
    SlotRef slot = new SlotRef(slotDesc);
    BinaryPredicate statsPred = new BinaryPredicate(op, slot, constExpr);
    statsPred.analyzeNoThrow(analyzer);
    statsConjuncts_.add(statsPred);
  }

  /**
   * Similar to the above method but builds an IN-list predicate which can be pushed down
   * to the ORC reader.
   */
  private void buildInListStatsPredicate(Analyzer analyzer, SlotRef inputSlot,
      InPredicate inputPred) {
    Preconditions.checkState(!inputPred.isNotIn());
    List<Expr> children = inputPred.getChildren();
    Preconditions.checkState(inputSlot == children.get(0).unwrapSlotRef(true));
    List<Expr> inList = Lists.newArrayListWithCapacity(children.size() - 1);
    for (int i = 1; i < children.size(); ++i) {
      Expr child = children.get(i);
      // If any child is not a literal, then nothing can be done
      if (!Expr.IS_LITERAL.apply(child)) return;
      if (isUnsupportedStatsType(child.getType())) return;
      inList.add(child);
    }
    // Make a new slot descriptor, which adds it to the tuple descriptor.
    SlotDescriptor slotDesc = analyzer.getDescTbl().copySlotDescriptor(statsTuple_,
        inputSlot.getDesc());
    SlotRef slot = new SlotRef(slotDesc);
    InPredicate inPred = new InPredicate(slot, inList, inputPred.isNotIn());
    inPred.analyzeNoThrow(analyzer);
    statsConjuncts_.add(inPred);
  }

  private boolean isUnsupportedStatsType(Type type) {
    // TODO(IMPALA-10882): Push down Min-Max predicates of CHAR/VARCHAR to ORC reader
    // TODO(IMPALA-10915): Push down Min-Max predicates of TIMESTAMP to ORC reader
    return hasOrc(fileFormats_)
        && (type.getPrimitiveType() == PrimitiveType.CHAR
               || type.getPrimitiveType() == PrimitiveType.VARCHAR
               || type.getPrimitiveType() == PrimitiveType.TIMESTAMP);
  }

  private void tryComputeBinaryStatsPredicate(Analyzer analyzer,
      BinaryPredicate binaryPred) {
    // We only support slot refs on the left hand side of the predicate, a rewriting
    // rule makes sure that all compatible exprs are rewritten into this form. Only
    // implicit casts are supported.
    SlotRef slotRef = binaryPred.getChild(0).unwrapSlotRef(true);
    if (slotRef == null) return;

    SlotDescriptor slotDesc = slotRef.getDesc();
    // This node is a table scan, so this must be a scanning slot.
    Preconditions.checkState(slotDesc.isScanSlot());
    // Skip the slot ref if it refers to an array's "pos" field.
    if (slotDesc.isArrayPosRef()) return;

    Expr constExpr = binaryPred.getChild(1);
    // Only constant exprs can be evaluated against parquet::Statistics. This includes
    // LiteralExpr, but can also be an expr like "1 + 2".
    if (!constExpr.isConstant()) return;
    if (Expr.IS_NULL_VALUE.apply(constExpr)) return;
    if (slotDesc.isVirtualColumn()) return;
    if (isUnsupportedStatsType(slotDesc.getType())) return;
    if (isUnsupportedStatsType(constExpr.getType())) return;

    if (BinaryPredicate.IS_RANGE_PREDICATE.apply(binaryPred)) {
      addStatsOriginalConjunct(slotDesc.getParent(), binaryPred);
      buildBinaryStatsPredicate(analyzer, slotRef, binaryPred, binaryPred.getOp());
    } else if (BinaryPredicate.IS_EQ_PREDICATE.apply(binaryPred)) {
      addStatsOriginalConjunct(slotDesc.getParent(), binaryPred);
      if (hasParquet(fileFormats_)) {
        // TODO: this could be optimized for boolean columns.
        buildBinaryStatsPredicate(analyzer, slotRef, binaryPred,
            BinaryPredicate.Operator.LE);
        buildBinaryStatsPredicate(analyzer, slotRef, binaryPred,
            BinaryPredicate.Operator.GE);
      }
      if (hasOrc(fileFormats_)) {
        // We can push down EQ predicates to the ORC reader directly.
        buildBinaryStatsPredicate(analyzer, slotRef, binaryPred, binaryPred.getOp());
      }
    }
  }

  private void tryComputeInListStatsPredicate(Analyzer analyzer, InPredicate inPred) {
    // Retrieve the left side of the IN predicate. It must be a simple slot to proceed.
    SlotRef slotRef = inPred.getBoundSlot();
    if (slotRef == null) return;
    SlotDescriptor slotDesc = slotRef.getDesc();
    // This node is a table scan, so this must be a scanning slot.
    Preconditions.checkState(slotDesc.isScanSlot());
    // Skip the slot ref if it refers to an array's "pos" field.
    if (slotDesc.isArrayPosRef()) return;
    if (inPred.isNotIn()) return;
    if (hasOrc(fileFormats_)) {
      if (isUnsupportedStatsType(slotDesc.getType())) return;
      addStatsOriginalConjunct(slotDesc.getParent(), inPred);
      buildInListStatsPredicate(analyzer, slotRef, inPred);
    }

    if (!hasParquet(fileFormats_)) return;
    List<Expr> children = inPred.getChildren();
    LiteralExpr min = null;
    LiteralExpr max = null;
    for (int i = 1; i < children.size(); ++i) {
      Expr child = children.get(i);

      // If any child is not a literal, then nothing can be done
      if (!Expr.IS_LITERAL.apply(child)) return;
      LiteralExpr literalChild = (LiteralExpr) child;
      // If any child is NULL, then there is not a valid min/max. Nothing can be done.
      if (Expr.IS_NULL_LITERAL.apply(literalChild)) return;

      if (min == null || literalChild.compareTo(min) < 0) min = literalChild;
      if (max == null || literalChild.compareTo(max) > 0) max = literalChild;
    }
    Preconditions.checkState(min != null);
    Preconditions.checkState(max != null);

    BinaryPredicate minBound = new BinaryPredicate(BinaryPredicate.Operator.GE,
        children.get(0).clone(), min.clone());
    BinaryPredicate maxBound = new BinaryPredicate(BinaryPredicate.Operator.LE,
        children.get(0).clone(), max.clone());

    addStatsOriginalConjunct(slotDesc.getParent(), inPred);
    buildBinaryStatsPredicate(analyzer, slotRef, minBound, minBound.getOp());
    buildBinaryStatsPredicate(analyzer, slotRef, maxBound, maxBound.getOp());
  }

  private void tryComputeIsNullStatsPredicate(Analyzer analyzer,
      IsNullPredicate isNullPred) {
    // Currently, only ORC table can push down IS-NULL predicates.
    if (!hasOrc(fileFormats_)) return;
    // Retrieve the left side of the IS-NULL predicate. Skip if it's not a simple slot.
    SlotRef slotRef = isNullPred.getBoundSlot();
    if (slotRef == null) return;
    // This node is a table scan, so this must be a scanning slot.
    Preconditions.checkState(slotRef.getDesc().isScanSlot());
    // Skip the slot ref if it refers to an array's "pos" field.
    if (slotRef.getDesc().isArrayPosRef()) return;
    addStatsOriginalConjunct(slotRef.getDesc().getParent(), isNullPred);
    SlotDescriptor slotDesc = analyzer.getDescTbl().copySlotDescriptor(statsTuple_,
        slotRef.getDesc());
    SlotRef slot = new SlotRef(slotDesc);
    IsNullPredicate statsPred = new IsNullPredicate(slot, isNullPred.isNotNull());
    statsPred.analyzeNoThrow(analyzer);
    statsConjuncts_.add(statsPred);
  }

  private void addStatsOriginalConjunct(TupleDescriptor tupleDesc, Expr expr) {
    List<Expr> exprs = statsOriginalConjuncts_.computeIfAbsent(
        tupleDesc, k -> new ArrayList<>());
    exprs.add(expr);
  }

  private void tryComputeStatsPredicate(Analyzer analyzer, Expr pred) {
    if (pred instanceof BinaryPredicate) {
      tryComputeBinaryStatsPredicate(analyzer, (BinaryPredicate) pred);
    } else if (pred instanceof InPredicate) {
      tryComputeInListStatsPredicate(analyzer, (InPredicate) pred);
    } else if (pred instanceof IsNullPredicate) {
      tryComputeIsNullStatsPredicate(analyzer, (IsNullPredicate) pred);
    }
  }

  /**
   * Populates notEmptyCollections_ based on IsNotEmptyPredicates in the given conjuncts.
   */
  private void addNotEmptyCollections(List<Expr> conjuncts) {
    for (Expr expr : conjuncts) {
      if (expr instanceof IsNotEmptyPredicate) {
        SlotRef ref = (SlotRef)((IsNotEmptyPredicate)expr).getChild(0);
        Preconditions.checkState(ref.getDesc().getType().isComplexType());
        Preconditions.checkState(ref.getDesc().getItemTupleDesc() != null);
        notEmptyCollections_.add(ref.getDesc().getItemTupleDesc());
      }
    }
  }

  /**
   * Analyzes 'conjuncts_' and 'collectionConjuncts_', populates 'statsTuple_' with slots
   * for statistics values, and populates 'statsConjuncts_' with conjuncts pointing into
   * the 'statsTuple_'. Binary conjuncts of the form <slot> <op> <constant> are supported,
   * and <op> must be one of LT, LE, GE, GT, or EQ. IN-list and IS-NULL conjuncts are also
   * supported.
   */
  private void computeStatsTupleAndConjuncts(Analyzer analyzer) throws ImpalaException{
    Preconditions.checkNotNull(desc_.getPath());
    if (statsTuple_ != null) return;
    String tupleName = desc_.getPath().toString() + " statistics";
    DescriptorTable descTbl = analyzer.getDescTbl();
    statsTuple_ = descTbl.createTupleDescriptor(tupleName);
    statsTuple_.setPath(desc_.getPath());

    // Adds predicates for scalar, top-level columns.
    for (Expr pred: conjuncts_) tryComputeStatsPredicate(analyzer, pred);

    // Adds predicates for collections.
    for (Map.Entry<TupleDescriptor, List<Expr>> entry: collectionConjuncts_.entrySet()) {
      if (notEmptyCollections_.contains(entry.getKey())) {
        for (Expr pred: entry.getValue()) tryComputeStatsPredicate(analyzer, pred);
      }
    }
    statsTuple_.computeMemLayout();
  }

  /**
   * Init the necessary data structures prior to the detection of overlap predicates.
   */
  public void initOverlapPredicate(Analyzer analyzer) {
    if (!allParquet_) return;
    Preconditions.checkNotNull(statsTuple_);
    // Allow the tuple to accept new slots.
    statsTuple_.resetHasMemoryLayout();

    overlap_first_slot_idx_ = statsTuple_.getSlots().size();
  }

  /**
   * Data type check on the slot type and the join column type.
   */
  private boolean checkTypeForOverlapPredicate(Type slotType, Type joinType) {
    // Both slotType and joinType must be Boolean at the same time.
    if (slotType.isBoolean() && joinType.isBoolean()) {
      return true;
    }

    // Both slotType and joinType must be one of the integer types (tinyint, smallint,
    // int, or bigint) at the same time.
    if (slotType.isIntegerType() && joinType.isIntegerType()) {
      return true;
    }

    // Both slotType and joinType must be strings at the same time, as CHAR or VARCHAR
    // are not supported by min/max filters.
    if (slotType.isScalarType(PrimitiveType.STRING)
        && joinType.isScalarType(PrimitiveType.STRING)) {
      return true;
    }

    // Both slotType and joinType must be timestamp at the same time.
    if (slotType.isTimestamp() && joinType.isTimestamp()) {
      return true;
    }

    // Both slotType and joinType must be date at the same time.
    if (slotType.isDate() && joinType.isDate()) {
      return true;
    }

    // Both slotType and joinType must be approximate numeric at the same time.
    if (slotType.isFloatingPointType() && joinType.isFloatingPointType()) {
      return true;
    }

    // If slotType and joinType are both decimals, make sure both are of the same
    // precision and scale. This is because the backend representation of a decimal
    // value (DecimalValue) does not carry precision and scale with it. Without a
    // cast, it is difficult to compare two decimals with different precisions and
    // scales.
    if (slotType.isDecimal() && joinType.isDecimal()) {
      ScalarType slotScalarType = (ScalarType)slotType;
      ScalarType hashScalarType = (ScalarType)joinType;
      return (slotScalarType.decimalPrecision() == hashScalarType.decimalPrecision()
          && slotScalarType.decimalScale() == hashScalarType.decimalScale());
    }

    return false;
  }

  /**
   * Determine if a runtime filter should be allowed given the relevant query options.
   */
  private boolean allowMinMaxFilter(FeTable table, Column column,
      TQueryOptions queryOptions, boolean isBoundByPartitionColumns) {
    if (column == null || table == null || !(table instanceof FeFsTable)) return false;
    FeFsTable feFsTable = (FeFsTable) table;

    boolean minmaxOnPartitionColumns = queryOptions.isMinmax_filter_partition_columns();
    boolean minmaxOnSortedColumns = queryOptions.isMinmax_filter_sorted_columns();

    TSortingOrder sortOrder = feFsTable.getSortOrderForSortByColumn();
    if (sortOrder != null) {
      // The table is sorted.
      if (sortOrder == TSortingOrder.LEXICAL) {
        // If the table is sorted in lexical order, allow it if the column is a
        // leading sort-by column and filtering on sorted column is enabled.
        return feFsTable.isLeadingSortByColumn(column.getName()) && minmaxOnSortedColumns;
      } else {
        // Must be Z-order. Allow it if it is one of the sort-by columns and filtering
        // on sorted column is enabled.
        Preconditions.checkState(sortOrder == TSortingOrder.ZORDER);
        return feFsTable.isSortByColumn(column.getName()) && minmaxOnSortedColumns;
      }
    }

    // Allow min/max filters on partition columns only when enabled.
    if (isBoundByPartitionColumns) {
      return minmaxOnPartitionColumns;
    }

    // Allow min/max filters if the threshold value > 0.0.
    return queryOptions.getMinmax_filter_threshold() > 0.0;
  }

  // Try to compute the overlap predicate for the filter. Return true if an overlap
  // predicate can be formed utilizing the min/max filter 'filter' against the
  // target expr 'targetExpr'. Return false otherwise.
  public Boolean tryToComputeOverlapPredicate(Analyzer analyzer, RuntimeFilter filter,
      Expr targetExpr, boolean isBoundByPartitionColumns) {
    // This optimization is only valid for min/max filters and Parquet tables.
    if (filter.getType() != TRuntimeFilterType.MIN_MAX) return false;
    if (!allParquet_) return false;

    // The unwrapped targetExpr should refer to a column in the scan node.
    SlotRef slotRefInScan = targetExpr.unwrapSlotRef(true);
    if (slotRefInScan == null) return false;

    // Check if targetExpr refers to some column in one of the min/max conjuncts
    // already formed. If so, do not add an overlap predicate as it may not be
    // as effective as the conjunct.
    List<SlotDescriptor> slotDescs = statsTuple_.getSlots();
    for (int i=0; i<overlap_first_slot_idx_; i++) {
      if (slotDescs.get(i).getPath() == slotRefInScan.getDesc().getPath())
        return false;
    }

    Column column = slotRefInScan.getDesc().getColumn();
    FeTable table = slotRefInScan.getDesc().getParent().getTable();
    if (!allowMinMaxFilter(
            table, column, analyzer.getQueryOptions(), isBoundByPartitionColumns)) {
      return false;
    }

    Expr srcExpr = filter.getSrcExpr();

    // When the target is not an implicit cast, make a type check between the type
    // of the min/max stats that overlap predicate will see from the row groups or
    // pages, and the type of the min/max from the other side of the join.
    if (!targetExpr.isImplicitCast()) {
      if (!checkTypeForOverlapPredicate(slotRefInScan.getType(), srcExpr.getType())) {
        return false;
      }
    } else {
      // The target is an implicit cast, make the following two type checks to
      // assure the overlap filter can work with the row group or page stats
      // (without any cast) and the min/max from the other side of the join.
      // 1. The type of the target column and the casted-to type;
      // 2. The the casted-to type and the type of the column from the
      //    other side of the join.
      //
      // To do: cast the row group or page stats before overlap predicate
      //        evaluation.
      if (!checkTypeForOverlapPredicate(slotRefInScan.getType(), targetExpr.getType())
          || !checkTypeForOverlapPredicate(targetExpr.getType(), srcExpr.getType())) {
        return false;
      }
    }

    int firstSlotIdx = statsTuple_.getSlots().size();
    // Make two new slot descriptors to hold data min/max values (such as from
    // row groups or pages in Parquet)) and append them to the tuple descriptor.
    SlotDescriptor slotDescDataMin =
        analyzer.getDescTbl().copySlotDescriptor(statsTuple_, slotRefInScan.getDesc());
    SlotDescriptor slotDescDataMax =
        analyzer.getDescTbl().copySlotDescriptor(statsTuple_, slotRefInScan.getDesc());

    overlapPredicateDescs_.add(
        new TOverlapPredicateDesc(filter.getFilterId().asInt(), firstSlotIdx));
    return true;
  }

  // Finalize the necessary data structures.
  public void finalizeOverlapPredicate() {
    if (!allParquet_) return;
    // Recompute the memory layout for the min/max tuple.
    statsTuple_.computeMemLayout();
  }

  /**
   * Recursively collects and assigns conjuncts bound by tuples materialized in a
   * collection-typed slot. As conjuncts are seen, collect non-empty nested collections.
   *
   * Limitation: Conjuncts that must first be migrated into inline views and that cannot
   * be captured by slot binding will not be assigned here, but in an UnnestNode.
   * This limitation applies to conjuncts bound by inline-view slots that are backed by
   * non-SlotRef exprs in the inline-view's select list. We only capture value transfers
   * between slots, and not between arbitrary exprs.
   *
   * TODO: The logic for gathering conjuncts and deciding which ones should be
   * marked as assigned needs to be clarified and consolidated in one place. The code
   * below is rather different from the code for assigning the top-level conjuncts in
   * init() although the performed tasks is conceptually identical. Refactoring the
   * assignment code is tricky/risky for now.
   */
  private void assignCollectionConjuncts(TupleDescriptor tupleDesc, Analyzer analyzer) {
    for (SlotDescriptor slotDesc: tupleDesc.getSlots()) {
      if (!slotDesc.getType().isCollectionType()) continue;
      Preconditions.checkNotNull(slotDesc.getItemTupleDesc());
      TupleDescriptor itemTupleDesc = slotDesc.getItemTupleDesc();
      TupleId itemTid = itemTupleDesc.getId();

      // If the slot is part of a collection that is given as a zipping unnest in the
      // FROM clause then avoid pushing down conjunct for this slot to the scanner as it
      // would result incorrect results on that slot after performing the unnest.
      // One exception is when there is only one such table reference in the FROM clause.
      if (analyzer.getNumZippingUnnests() > 1 &&
          analyzer.getZippingUnnestTupleIds().contains(itemTid)) {
        continue;
      }

      // First collect unassigned and binding predicates. Then remove redundant
      // predicates based on slot equivalences and enforce slot equivalences by
      // generating new predicates.
      List<Expr> collectionConjuncts =
          analyzer.getUnassignedConjuncts(Lists.newArrayList(itemTid));
      List<Expr> bindingPredicates = analyzer.getBoundPredicates(itemTid);
      for (Expr boundPred: bindingPredicates) {
        if (!collectionConjuncts.contains(boundPred)) collectionConjuncts.add(boundPred);
      }
      analyzer.createEquivConjuncts(itemTid, collectionConjuncts);
      // Mark those conjuncts as assigned that do not also need to be evaluated by a
      // subsequent semi or outer join.
      for (Expr conjunct: collectionConjuncts) {
        if (!analyzer.evalAfterJoin(conjunct)) analyzer.markConjunctAssigned(conjunct);
      }

      if (analyzer.getQueryCtx().client_request.getQuery_options().enable_expr_rewrites) {
        Expr.optimizeConjuncts(collectionConjuncts, analyzer);
      }

      if (!collectionConjuncts.isEmpty()) {
        analyzer.materializeSlots(collectionConjuncts);
        collectionConjuncts_.put(itemTupleDesc, collectionConjuncts);
        addNotEmptyCollections(collectionConjuncts);
      }
      // Recursively look for collection-typed slots in nested tuple descriptors.
      assignCollectionConjuncts(itemTupleDesc, analyzer);
    }
  }

  /**
   * Adds an entry to dictionaryFilterConjuncts_ if dictionary filtering is applicable
   * for conjunct. The dictionaryFilterConjuncts_ entry maps the conjunct's tupleId and
   * slotId to conjunctIdx. The conjunctIdx is the offset into a list of conjuncts;
   * either conjuncts_ (for scan node's tupleId) or collectionConjuncts_ (for nested
   * collections).
   */
  private void addDictionaryFilter(Analyzer analyzer, Expr conjunct, int conjunctIdx) {
    List<TupleId> tupleIds = new ArrayList<>();
    List<SlotId> slotIds = new ArrayList<>();
    conjunct.getIds(tupleIds, slotIds);
    // Only single-slot conjuncts are eligible for dictionary filtering. When pruning
    // a row-group, the conjunct must be evaluated only against a single row-group
    // at-a-time. Expect a single slot conjunct to be associated with a single tuple-id.
    if (slotIds.size() != 1) return;

    SlotDescriptor firstSlotDesc = analyzer.getSlotDesc(slotIds.get(0));
    // Check to see if this slot is a collection type. Dictionary pruning is applicable
    // to scalar values nested in collection types, not enclosing collection types.
    if (firstSlotDesc.getType().isCollectionType()) return;

    // Dictionary filtering is not applicable on virtual columns.
    if (firstSlotDesc.isVirtualColumn()) return;

    // If any of the slot descriptors affected by 'conjunct' happens to be a scalar member
    // of a struct, where the struct is also given in the select list then skip dictionary
    // filtering as the slot/tuple IDs in the conjunct would mismatch with the ones in the
    // select list and would result a Precondition check failure later.
    for (SlotId slotId : slotIds) {
      SlotDescriptor slotDesc = analyzer.getSlotDesc(slotId);
      if (IsMemberOfStructInSelectList(slotDesc)) return;
    }

    // Check to see if this conjunct contains any known randomized function
    if (conjunct.contains(Expr.IS_NONDETERMINISTIC_BUILTIN_FN_PREDICATE)) return;

    // Check to see if the conjunct evaluates to true when the slot is NULL
    // This is important for dictionary filtering. Dictionaries do not
    // contain an entry for NULL and do not provide an indication about
    // whether NULLs are present. A conjunct that evaluates to true on NULL
    // cannot be evaluated purely on the dictionary.
    try {
      if (analyzer.isTrueWithNullSlots(conjunct)) return;
    } catch (InternalException e) {
      // Expr evaluation failed in the backend. Skip this conjunct since we cannot
      // determine whether it is safe to apply it against a dictionary.
      LOG.warn("Skipping dictionary filter because backend evaluation failed: "
          + conjunct.toSql(), e);
      return;
    }

    // TODO: Should there be a limit on the cost/structure of the conjunct?
    SlotId slotId = slotIds.get(0);
    SlotDescriptor slotKey = analyzer.getSlotDesc(slotId);
    List<Integer> slotList = dictionaryFilterConjuncts_.get(slotKey);
    if (slotList == null) {
      slotList = new ArrayList<>();
      dictionaryFilterConjuncts_.put(slotKey, slotList);
    }
    slotList.add(conjunctIdx);
  }

  /**
   * Checks if 'slotDesc' is a member of a struct slot where the struct slot is given in
   * the select list.
   */
  private boolean IsMemberOfStructInSelectList(SlotDescriptor slotDesc) {
    SlotDescriptor parentStructSlot = slotDesc.getParent().getParentSlotDesc();
    // Check if 'slotDesc' is a member of a struct slot descriptor.
    if (slotDesc.getType().isScalarType() && parentStructSlot == null) return false;

    if (slotDesc.getType().isStructType()) {
      // Check if the struct is in the select list.
      for (SlotDescriptor scannedSlots : desc_.getSlots()) {
        if (scannedSlots.getId() == slotDesc.getId()) return true;
      }
    }

    // Recursively check the parent struct if it's given in the select list.
    if (parentStructSlot != null) return IsMemberOfStructInSelectList(parentStructSlot);
    return false;
  }

  /**
   * Walks through conjuncts_ and collectionConjuncts_ and populates
   * dictionaryFilterConjuncts_.
   */
  private void computeDictionaryFilterConjuncts(Analyzer analyzer) {
    for (int conjunctIdx = 0; conjunctIdx < conjuncts_.size(); ++conjunctIdx) {
      addDictionaryFilter(analyzer, conjuncts_.get(conjunctIdx), conjunctIdx);
    }
    for (Map.Entry<TupleDescriptor, List<Expr>> entry: collectionConjuncts_.entrySet()) {
      if (notEmptyCollections_.contains(entry.getKey())) {
        List<Expr> conjuncts = entry.getValue();
        for (int conjunctIdx = 0; conjunctIdx < conjuncts.size(); ++conjunctIdx) {
          addDictionaryFilter(analyzer, conjuncts.get(conjunctIdx), conjunctIdx);
        }
      }
    }
  }

  /**
   * Computes scan ranges (i.e. hdfs splits) plus their storage locations, including
   * volume ids, based on the given maximum number of bytes each scan range should scan.
   * If 'sampledPartitions_' is not null, computes the scan ranges based on the sample
   * partitions.
   *
   * Initializes members with information about files and scan ranges, e.g.
   * scanRangeSpecs_, generatedScanRangeCount_, totalFilesPerFs_, etc.
   */
  protected void computeScanRangeLocations(Analyzer analyzer)
      throws ImpalaRuntimeException {
    long scanRangeBytesLimit = analyzer.getQueryCtx().client_request.getQuery_options()
        .getMax_scan_range_length();
    if (RuntimeEnv.INSTANCE.hasTableScanRangeLimit() && desc_.getTableName() != null) {
      long testLimit = RuntimeEnv.INSTANCE.getTableScanRangeLimit(
          desc_.getTableName().getDb(), desc_.getTableName().getTbl());
      if (testLimit > 0
          && (scanRangeBytesLimit == 0 || scanRangeBytesLimit > testLimit)) {
        scanRangeBytesLimit = testLimit;
      }
    }

    // Initialize class fields that related to scan ranges.
    scanRangeSpecs_ = new TScanRangeSpec();
    generatedScanRangeCount_ = 0;
    largestScanRangeBytes_ = 0;
    maxScanRangeNumRows_ = -1;
    numScanRangesNoDiskIds_ = 0;
    numFilesNoDiskIds_ = 0;
    numPartitionsNoDiskIds_ = 0;
    numPartitionsPerFs_ = new TreeMap<>();
    totalFilesPerFs_ = new TreeMap<>();
    totalBytesPerFs_ = new TreeMap<>();
    totalFilesPerFsEC_ = new TreeMap<>();
    totalBytesPerFsEC_ = new TreeMap<>();

    Preconditions.checkState((sampleParams_ == null) == (sampledPartitions_ == null));
    int partitionsSize = getSampledOrRawPartitions().size();
    boolean allParquet = (partitionsSize > 0) ? true : false;
    boolean allColumnarFormat = (partitionsSize > 0) ? true : false;
    long simpleLimitNumRows = 0; // only used for the simple limit case
    boolean isSimpleLimit = sampleParams_ == null &&
        (analyzer.getQueryCtx().client_request.getQuery_options()
            .isOptimize_simple_limit()
        && analyzer.getSimpleLimitStatus() != null
        && analyzer.getSimpleLimitStatus().first);

    // Save the last looked up FileSystem object. It is enough for the scheme and
    // authority part of the URI to match to ensure that getFileSystem() would return the
    // same file system. See Hadoop's filesystem caching implementation at
    // https://github.com/apache/hadoop/blob/1046f9cf9888155c27923f3f56efa107d908ad5b/hadoop-common-project/hadoop-common/src/main/java/org/apache/hadoop/fs/FileSystem.java#L3867
    // Note that in the Hadoop code the slow part is UserGroupInformation.getCurrentUser()
    // which is not important here as the user is always the same in Impala.
    String lastFsScheme = null;
    String lastFsAuthority = null;
    FileSystem lastFileSytem = null;
    for (FeFsPartition partition : getSampledOrRawPartitions()) {
      // Save location to local variable beacuse getLocation() can be slow as it needs to
      // decompress the partition's location.
      String partitionLocation = partition.getLocation();
      Path partitionPath = new Path(partitionLocation);
      String fsScheme = partitionPath.toUri().getScheme();
      String fsAuthority = partitionPath.toUri().getAuthority();
      FileSystemUtil.FsType fsType = FileSystemUtil.FsType.getFsType(fsScheme);

      FileSystem partitionFs;
      if (lastFileSytem != null &&
         Objects.equals(lastFsScheme, fsScheme) &&
         Objects.equals(lastFsAuthority, fsAuthority)) {
        partitionFs = lastFileSytem;
      } else {
        try {
          partitionFs = partitionPath.getFileSystem(CONF);
        } catch (IOException e) {
          throw new ImpalaRuntimeException("Error determining partition fs type", e);
        }
        lastFsScheme = fsScheme;
        lastFsAuthority = fsAuthority;
        lastFileSytem = partitionFs;
      }

      // Missing disk id accounting is only done for file systems that support the notion
      // of disk/storage ids.
      boolean fsHasBlocks = FileSystemUtil.supportsStorageIds(partitionFs);
      List<FileDescriptor> fileDescs = getFileDescriptorsWithLimit(partition, fsHasBlocks,
          isSimpleLimit ? analyzer.getSimpleLimitStatus().second - simpleLimitNumRows
                        : -1);
      // conservatively estimate 1 row per file
      simpleLimitNumRows += fileDescs.size();
      if (sampledFiles_ != null) {
        // If we are sampling, get files in the sample.
        fileDescs = sampledFiles_.get(partition.getId());
      }

      long partitionNumRows = partition.getNumRows();
      analyzer.getDescTbl().addReferencedPartition(tbl_, partition.getId());
      if (!partition.getFileFormat().isParquetBased()) {
        allParquet = false;
      }
      allColumnarFormat =
          allColumnarFormat && VALID_COLUMNAR_FORMATS.contains(partition.getFileFormat());

      if (!fsHasBlocks) {
        // Limit the scan range length if generating scan ranges (and we're not
        // short-circuiting the scan for a partition key scan).
        long defaultBlockSize = (partition.getFileFormat().isParquetBased()) ?
            analyzer.getQueryOptions().parquet_object_store_split_size :
            partitionFs.getDefaultBlockSize(partitionPath);
        long maxBlockSize =
            Math.max(defaultBlockSize, FileDescriptor.MIN_SYNTHETIC_BLOCK_SIZE);
        if (scanRangeBytesLimit > 0) {
          scanRangeBytesLimit = Math.min(scanRangeBytesLimit, maxBlockSize);
        } else {
          scanRangeBytesLimit = maxBlockSize;
        }
      }
      final long partitionBytes = FileDescriptor.computeTotalFileLength(fileDescs);
      long partitionMaxScanRangeBytes = 0;
      boolean partitionMissingDiskIds = false;
      totalBytesPerFs_.merge(fsType, partitionBytes, Long::sum);
      totalFilesPerFs_.merge(fsType, (long) fileDescs.size(), Long::sum);
      numPartitionsPerFs_.merge(fsType, 1L, Long::sum);

      for (FileDescriptor fileDesc: fileDescs) {
        if (!analyzer.getQueryOptions().isAllow_erasure_coded_files() &&
            fileDesc.getIsEc()) {
          throw new ImpalaRuntimeException(String
              .format("Scanning of HDFS erasure-coded file (%s) is not supported",
                  fileDesc.getAbsolutePath(partitionLocation)));
        }

        // Accumulate on the number of EC files and the total size of such files.
        if (fileDesc.getIsEc()) {
          totalFilesPerFsEC_.merge(fsType, 1L, Long::sum);
          totalBytesPerFsEC_.merge(fsType, fileDesc.getFileLength(), Long::sum);
        }

        // If parquet count star optimization is enabled, we only need the
        // 'RowGroup.num_rows' in file metadata, thus only the scan range that contains
        // a file footer is required.
        // IMPALA-8834 introduced the optimization for partition key scan by generating
        // one scan range for each HDFS file. With Parquet and ORC, we only need to get
        // the scan range that contains a file footer for short-circuiting.
        boolean isFooterOnly = countStarSlot_ != null
            || (isPartitionKeyScan_
                && (partition.getFileFormat().isParquetBased()
                    || partition.getFileFormat() == HdfsFileFormat.ORC));

        if (!fsHasBlocks) {
          Preconditions.checkState(fileDesc.getNumFileBlocks() == 0);
          generateScanRangeSpecs(
              partition, partitionLocation, fileDesc, scanRangeBytesLimit, isFooterOnly);
        } else {
          // Skips files that have no associated blocks.
          if (fileDesc.getNumFileBlocks() == 0) continue;
          Pair<Boolean, Long> result =
              transformBlocksToScanRanges(partition, partitionLocation, fsType, fileDesc,
                  fsHasBlocks, scanRangeBytesLimit, analyzer, isFooterOnly);
          partitionMaxScanRangeBytes =
              Math.max(partitionMaxScanRangeBytes, result.second);
          if (result.first) partitionMissingDiskIds = true;
        }
      }
      if (partitionMissingDiskIds) ++numPartitionsNoDiskIds_;
      if (partitionMaxScanRangeBytes > 0 && partitionNumRows >= 0) {
        updateMaxScanRangeNumRows(
            partitionNumRows, partitionBytes, partitionMaxScanRangeBytes);
      }
      if (isSimpleLimit && simpleLimitNumRows ==
          analyzer.getSimpleLimitStatus().second) {
        // for the simple limit case if the estimated rows has already reached the limit
        // there's no need to process more partitions
        break;
      }
    }
    allParquet_ = allParquet;
    allColumnarFormat_ = allColumnarFormat;
    if (totalFilesPerFs_.isEmpty() || sumValues(totalFilesPerFs_) == 0) {
      maxScanRangeNumRows_ = 0;
    } else {
      // Also estimate max rows per scan range based on table-level stats, in case some
      // or all partition-level stats were missing.
      long tableNumRows = tbl_.getNumRows();
      if (tableNumRows >= 0) {
        updateMaxScanRangeNumRows(
            tableNumRows, sumValues(totalBytesPerFs_), largestScanRangeBytes_);
      }
    }
  }

  protected List<FileDescriptor> getFileDescriptorsWithLimit(
      FeFsPartition partition, boolean fsHasBlocks, long limit) {
    List<FileDescriptor> fileDescs;
    if (limit != -1) {
      int fileCount = 0;
      fileDescs = new ArrayList<>();
      for (FileDescriptor fd : partition.getFileDescriptors()) {
        // skip empty files
        if ((fsHasBlocks && fd.getNumFileBlocks() == 0)
            || (!fsHasBlocks && fd.getFileLength() <= 0)) {
          continue;
        }
        fileCount++;
        fileDescs.add(fd);
        if (fileCount == limit) {
          break;
        }
      }
    } else {
      fileDescs = partition.getFileDescriptors();
    }
    return fileDescs;
  }

  /**
   * Returns a sample of file descriptors associated to this scan node.
   * @param percentBytes must be between 0 and 100.
   * @param minSampleBytes minimum number of bytes to read.
   * @param randomSeed used for random number generation.
   */
  protected Map<Long, List<FileDescriptor>> getFilesSample(
      long percentBytes, long minSampleBytes, long randomSeed) {
    return FeFsTable.Utils.getFilesSample(tbl_, partitions_, percentBytes, minSampleBytes,
        randomSeed);
  }

  /**
   * Update the estimate of maximum number of rows per scan range based on the fraction
   * of bytes of the scan range relative to the total bytes per partition or table.
   */
  private void updateMaxScanRangeNumRows(long totalRows, long totalBytes,
      long maxScanRangeBytes) {
    Preconditions.checkState(totalRows >= 0);
    Preconditions.checkState(totalBytes >= 0);
    Preconditions.checkState(maxScanRangeBytes >= 0);
    // Check for zeros first to avoid possibility of divide-by-zero below.
    long estimate;
    if (maxScanRangeBytes == 0 || totalBytes == 0 || totalRows == 0) {
      estimate = 0;
    } else {
      double divisor = totalBytes / (double) maxScanRangeBytes;
      estimate = (long)(totalRows / divisor);
    }
    maxScanRangeNumRows_ =  Math.max(maxScanRangeNumRows_, estimate);
  }

  /**
   * Given a fileDesc of partition, generates TScanRanges that are specifications rather
   * than actual ranges. Defers generating the TScanRanges to the backend.
   * Used for file systems that do not have any physical attributes associated with
   * blocks (e.g., replica locations, caching, etc.). 'maxBlock' size determines how large
   * the scan ranges can be (may be ignored if the file is not splittable).
   * Expects partition's location string in partitionLocation as getting it from
   * FeFsPartition can be expensive.
   */
  private void generateScanRangeSpecs(FeFsPartition partition, String partitionLocation,
      FileDescriptor fileDesc, long maxBlockSize, boolean isFooterOnly) {
    Preconditions.checkArgument(fileDesc.getNumFileBlocks() == 0);
    Preconditions.checkArgument(maxBlockSize > 0);
    if (fileDesc.getFileLength() <= 0) return;
    boolean splittable = partition.getFileFormat().isSplittable(
        HdfsCompression.fromFileName(fileDesc.getPath()));
    isFooterOnly &= splittable;
    // Hashing must use String.hashCode() for consistency.
    int partitionHash = partitionLocation.hashCode();
    TFileSplitGeneratorSpec splitSpec = new TFileSplitGeneratorSpec(fileDesc.toThrift(),
        maxBlockSize, splittable, partition.getId(), partitionHash, isFooterOnly);
    scanRangeSpecs_.addToSplit_specs(splitSpec);
    long scanRangeBytes = Math.min(maxBlockSize, fileDesc.getFileLength());
    if (splittable && !isPartitionKeyScan_) {
      generatedScanRangeCount_ +=
          Math.ceil((double) fileDesc.getFileLength() / (double) maxBlockSize);
    } else {
      ++generatedScanRangeCount_;
      scanRangeBytes = fileDesc.getFileLength();
    }
    largestScanRangeBytes_ = Math.max(largestScanRangeBytes_, scanRangeBytes);
  }

  /**
   * Given a fileDesc of partition, transforms the blocks into TScanRanges. Each range
   * is paired with information about where the block is located so that the backend
   * coordinator can assign ranges to workers to avoid remote reads. These
   * TScanRangeLocationLists are added to scanRanges_. A pair is returned that indicates
   * whether the file has a missing disk id and the maximum scan range (in bytes) found.
   * Expects partition's location string in partitionLocation and filesystem type in
   * fsType as getting these from FeFsPartition can be expensive.
   */
  private Pair<Boolean, Long> transformBlocksToScanRanges(FeFsPartition partition,
      String partitionLocation, FileSystemUtil.FsType fsType, FileDescriptor fileDesc,
      boolean fsHasBlocks, long scanRangeBytesLimit, Analyzer analyzer,
      boolean isFooterOnly) {
    Preconditions.checkArgument(fileDesc.getNumFileBlocks() > 0);
    boolean fileDescMissingDiskIds = false;
    long fileMaxScanRangeBytes = 0;
    int i = 0;
    if (isFooterOnly) { i = fileDesc.getNumFileBlocks() - 1; }
    for (; i < fileDesc.getNumFileBlocks(); ++i) {
      FbFileBlock block = fileDesc.getFbFileBlock(i);
      int replicaHostCount = FileBlock.getNumReplicaHosts(block);
      if (replicaHostCount == 0) {
        // we didn't get locations for this block; for now, just ignore the block
        // TODO: do something meaningful with that
        continue;
      }
      // Collect the network address and volume ID of all replicas of this block.
      List<TScanRangeLocation> locations = new ArrayList<>();
      for (int j = 0; j < replicaHostCount; ++j) {
        TScanRangeLocation location = new TScanRangeLocation();
        // Translate from the host index (local to the HdfsTable) to network address.
        int replicaHostIdx = FileBlock.getReplicaHostIdx(block, j);
        TNetworkAddress networkAddress =
            partition.getHostIndex().getEntry(replicaHostIdx);
        Preconditions.checkNotNull(networkAddress);
        // Translate from network address to the global (to this request) host index.
        Integer globalHostIdx = analyzer.getHostIndex().getOrAddIndex(networkAddress);
        location.setHost_idx(globalHostIdx);
        if (fsHasBlocks && !fileDesc.getIsEc() && FileBlock.getDiskId(block, j) == -1) {
          // Skip Ozone; it returns NULL storageIds and users can't do anything about it.
          if (fsType != FileSystemUtil.FsType.OZONE) {
            ++numScanRangesNoDiskIds_;
          }
          fileDescMissingDiskIds = true;
        }
        location.setVolume_id(FileBlock.getDiskId(block, j));
        location.setIs_cached(FileBlock.isReplicaCached(block, j));
        locations.add(location);
      }
      // create scan ranges, taking into account maxScanRangeLength
      long currentOffset = FileBlock.getOffset(block);
      long remainingLength = FileBlock.getLength(block);
      while (remainingLength > 0) {
        long currentLength = remainingLength;
        if (scanRangeBytesLimit > 0 && remainingLength > scanRangeBytesLimit) {
          currentLength = scanRangeBytesLimit;
          if (isFooterOnly) {
            // Only generate one scan range for footer only scans.
            currentOffset += remainingLength - currentLength;
            remainingLength = currentLength;
          }
        }
        TScanRange scanRange = new TScanRange();
        THdfsFileSplit hdfsFileSplit = new THdfsFileSplit(fileDesc.getRelativePath(),
            currentOffset, currentLength, partition.getId(), fileDesc.getFileLength(),
            fileDesc.getFileCompression().toThrift(), fileDesc.getModificationTime(),
            partitionLocation.hashCode());
        hdfsFileSplit.setAbsolute_path(fileDesc.getAbsolutePath());
        hdfsFileSplit.setIs_encrypted(fileDesc.getIsEncrypted());
        hdfsFileSplit.setIs_erasure_coded(fileDesc.getIsEc());
        scanRange.setHdfs_file_split(hdfsFileSplit);
        if (fileDesc.getFbFileMetadata() != null) {
          scanRange.setFile_metadata(fileDesc.getFbFileMetadata().getByteBuffer());
        }
        TScanRangeLocationList scanRangeLocations = new TScanRangeLocationList();
        scanRangeLocations.scan_range = scanRange;
        scanRangeLocations.locations = locations;
        scanRangeSpecs_.addToConcrete_ranges(scanRangeLocations);
        largestScanRangeBytes_ = Math.max(largestScanRangeBytes_, currentLength);
        fileMaxScanRangeBytes = Math.max(fileMaxScanRangeBytes, currentLength);
        remainingLength -= currentLength;
        currentOffset += currentLength;
      }
      // Only generate one scan range for partition key scans.
      if (isPartitionKeyScan_) break;
    }
    if (fileDescMissingDiskIds) {
      ++numFilesNoDiskIds_;
      if (LOG.isTraceEnabled()) {
        LOG.trace("File blocks mapping to unknown disk ids. Dir: "
            + partitionLocation + " File:" + fileDesc.toString());
      }
    }

    return new Pair<Boolean, Long>(fileDescMissingDiskIds, fileMaxScanRangeBytes);
  }

  /**
   * Computes the average row size, input and output cardinalities, and estimates the
   * number of nodes.
   * Requires that computeScanRangeLocations() has been called.
   */
  @Override
  public void computeStats(Analyzer analyzer) {
    Preconditions.checkNotNull(scanRangeSpecs_);
    super.computeStats(analyzer);
    computeCardinalities(analyzer);
    computeNumNodes(analyzer, cardinality_);
  }

  /**
   * Computes and sets the input and output cardinalities.
   *
   * If available, table-level row count and file bytes statistics are used for
   * extrapolating the input cardinality (before conjuncts). The extrapolation is based
   * on the total number of bytes to be scanned and is intended to address the following
   * scenarios: (1) new partitions that have no stats, and (2) existing partitions which
   * have changed since the last stats collection. When extrapolating, the per-partition
   * row counts are ignored because we cannot determine whether the partition has changed
   * since the last stats collection.
   * Otherwise, the input cardinality is based on the per-partition row count stats
   * and/or the table-level row count stats, depending on which of those are available.
   * Adjusts the output cardinality based on the scan conjuncts and table sampling.
   *
   * Sets these members:
   * extrapolatedNumRows_, inputCardinality_, cardinality_
   */
  protected void computeCardinalities(Analyzer analyzer) {
    // Choose between the extrapolated row count and the one based on stored stats.
    extrapolatedNumRows_ = FeFsTable.Utils.getExtrapolatedNumRows(tbl_,
            sumValues(totalBytesPerFs_));
    long statsNumRows = getStatsNumRows(analyzer.getQueryOptions());
    if (extrapolatedNumRows_ != -1) {
      // The extrapolated row count is based on the 'totalBytesPerFs_' which already
      // accounts for table sampling, so no additional adjustment for sampling is
      // necessary.
      cardinality_ = extrapolatedNumRows_;
    } else {
      // Set the cardinality based on table or partition stats.
      cardinality_ = statsNumRows;
      // Adjust the cardinality based on table sampling.
      if (sampleParams_ != null && cardinality_ != -1) {
        double fracPercBytes = (double) sampleParams_.getPercentBytes() / 100;
        cardinality_ = Math.round(cardinality_ * fracPercBytes);
        cardinality_ = Math.max(cardinality_, 1);
      }
    }

    // Checked after the block above to first collect information for the explain output.
    if (sumValues(totalBytesPerFs_) == 0) {
      // Nothing to scan. Definitely a cardinality of 0.
      inputCardinality_ = 0;
      cardinality_ = 0;
      return;
    }

    // Adjust cardinality for all collections referenced along the tuple's path.
    if (cardinality_ != -1) {
      for (Type t: desc_.getPath().getMatchedTypes()) {
        if (t.isCollectionType()) cardinality_ *= PlannerContext.AVG_COLLECTION_SIZE;
      }
    }
    inputCardinality_ = cardinality_;

    // Sanity check scan node cardinality.
    if (cardinality_ < -1) {
      hasCorruptTableStats_ = true;
      cardinality_ = -1;
    }

    if (cardinality_ > 0) {
      if (LOG.isTraceEnabled()) {
        LOG.trace("cardinality_=" + Long.toString(cardinality_) +
                  " sel=" + Double.toString(computeSelectivity()));
      }
      cardinality_ = applyConjunctsSelectivity(cardinality_);
    }
    cardinality_ = capCardinalityAtLimit(cardinality_);

    // Backend only returns one row per partition
    if (isPartitionKeyScan_) {
      // The short-circuiting means we generate at most one row per scan range.
      long numRanges = getNumScanRanges();
      cardinality_ = cardinality_ < 0 ? numRanges : Math.min(cardinality_, numRanges);
      // inputCardinality_ is used as a rough cost measure to determine whether to run on
      // a single node. Adjust it upwards to reflect that producing the one row per scan
      // range requires more work than a typical rows, so that we won't process large
      // numbers of scan ranges on the coordinator.
      long numRangesAdjusted = MathUtil.saturatingMultiply(
          numRanges, PARTITION_KEY_SCAN_INPUT_CARDINALITY_ADJUSTMENT_FACTOR);
      inputCardinality_ = inputCardinality_ < 0 ?
          numRangesAdjusted :
          Math.min(inputCardinality_, numRangesAdjusted);
    }

    if (countStarSlot_ != null) {
      // We are doing optimized count star. Override cardinality with total num files.
      long totalFiles = sumValues(totalFilesPerFs_);
      inputCardinality_ = totalFiles;
      cardinality_ = totalFiles;
    }
    if (LOG.isTraceEnabled()) {
      LOG.trace("HdfsScan: cardinality_=" + Long.toString(cardinality_));
    }
  }

  /**
   * Computes and returns the number of rows scanned based on the per-partition row count
   * stats and/or the table-level row count stats, depending on which of those are
   * available. Partition stats are used as long as they are neither missing nor
   * corrupted. Otherwise, we fall back to table-level stats even for partitioned tables.
   * We further estimate the row count if the table-level stats is missing or corrupted,
   * or some partitions are with corrupt stats. The estimation is done only for those
   * partitions with corrupt stats.
   *
   * Sets these members:
   * numPartitionsWithNumRows_, partitionNumRows_, hasCorruptTableStats_.
   *
   * Currently, we provide a table hint 'TABLE_NUM_ROWS' to set table rows manually if
   * table has no stats or has corrupt stats. If the table has valid stats, this hint
   * will be ignored.
   */
  private long getStatsNumRows(TQueryOptions queryOptions) {
    numPartitionsWithNumRows_ = 0;
    partitionNumRows_ = -1;
    hasCorruptTableStats_ = false;

    List<FeFsPartition> partitionsWithCorruptOrMissingStats = new ArrayList<>();
    for (FeFsPartition p : partitions_) {
      long partNumRows = p.getNumRows();
      // Check for corrupt stats
      if (partNumRows < -1 || (partNumRows == 0 && p.getSize() > 0)) {
        hasCorruptTableStats_ = true;
        partitionsWithCorruptOrMissingStats.add(p);
      } else if (partNumRows == -1) { // Check for missing stats
        partitionsWithCorruptOrMissingStats.add(p);
      } else if (partNumRows > -1) {
        // Consider partition with good stats.
        if (partitionNumRows_ == -1) partitionNumRows_ = 0;
        partitionNumRows_ = checkedAdd(partitionNumRows_, partNumRows);
        ++numPartitionsWithNumRows_;
      }
    }
    // If all partitions have good stats, return the total row count contributed
    // by each of the partitions, as the row count for the table.
    if (partitionsWithCorruptOrMissingStats.size() == 0
        && numPartitionsWithNumRows_ > 0) {
      return partitionNumRows_;
    }

    // Set cardinality based on table-level stats.
    long numRows = tbl_.getNumRows();
    // Depending on the query option of disable_hdfs_num_rows_est, if numRows
    // is still not available (-1), or partition stats is corrupted, we provide
    // a crude estimation by computing sumAvgRowSizes, the sum of the slot
    // size of each column of scalar type, and then generate the estimate using
    // sumValues(totalBytesPerFs_), the size of the hdfs table.
    if (!queryOptions.disable_hdfs_num_rows_estimate
        && (numRows == -1L || hasCorruptTableStats_) && tableNumRowsHint_ == -1L) {
      // Compute the estimated table size from those partitions with missing or corrupt
      // row count, when taking compression into consideration
      long estimatedTableSize =
          computeEstimatedTableSize(partitionsWithCorruptOrMissingStats);

      double sumAvgRowSizes = 0.0;
      for (Column col : tbl_.getColumns()) {
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
      numRows = partitionNumRows_ =
          (partitionNumRows_ > 0) ? partitionNumRows_ + estNumRows : estNumRows;
    }

    if (numRows < -1 || (numRows == 0 && tbl_.getTotalHdfsBytes() > 0)) {
      hasCorruptTableStats_ = true;
    }

    // Use hint value if table no stats or stats is corrupt and hint is set
    return (numRows >= 0 && !hasCorruptTableStats_) || tableNumRowsHint_ == -1L ?
        numRows :tableNumRowsHint_;
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
         Preconditions.checkState(VALID_COLUMNAR_FORMATS.contains(format),
             "Unknown HDFS compressed format: %s", format, this);
         estimatedPartitionSize += Math.round(p.getSize()
             * ESTIMATED_COMPRESSION_FACTOR_COLUMNAR);
        }
      }
      estimatedTableSize += estimatedPartitionSize;
    }
    return estimatedTableSize;
  }

  /**
   * Estimate the number of impalad nodes that this scan node will execute on (which is
   * ultimately determined by the scheduling done by the backend's Scheduler).
   * Assume that scan ranges that can be scheduled locally will be, and that scan
   * ranges that cannot will be round-robined across the cluster.
   *
   * When the planner runs in the debug mode (SET PLANNER_TESTCASE_MODE=true), the
   * estimation does not take into account the local cluster topology and instead
   * assumes that every scan range location is local to some datanode. This should only
   * be set when replaying a testcase from some other cluster.
   */
  protected void computeNumNodes(Analyzer analyzer, long cardinality) {
    Preconditions.checkNotNull(scanRangeSpecs_);
    ExecutorMembershipSnapshot cluster = ExecutorMembershipSnapshot.getCluster();
    final int maxInstancesPerNode = getMaxInstancesPerNode(analyzer);
    final int maxPossibleInstances =
        analyzer.numExecutorsForPlanning() * maxInstancesPerNode;
    int totalNodes = 0;
    int totalInstances = 0;
    int numLocalRanges = 0;
    int numRemoteRanges = 0;
    // Counts the number of local ranges, capped at maxInstancesPerNode.
    Map<TNetworkAddress, Integer> localRangeCounts = new HashMap<>();
    // Sum of the counter values in localRangeCounts.
    int totalLocalParallelism = 0;
    if (scanRangeSpecs_.isSetConcrete_ranges()) {
      if (analyzer.getQueryOptions().planner_testcase_mode) {
        // TODO: Have a separate scan node implementation that mocks an HDFS scan
        // node rather than including the logic here.

        // Track the number of unique host indexes across all scan ranges. Assume for
        // the sake of simplicity that every scan is served from a local datanode.
        Set<Integer> dummyHostIndex = Sets.newHashSet();
        for (TScanRangeLocationList range : scanRangeSpecs_.concrete_ranges) {
          for (TScanRangeLocation loc: range.locations) {
            dummyHostIndex.add(loc.getHost_idx());
            ++numLocalRanges;
          }
        }
        totalNodes = Math.min(scanRangeSpecs_.concrete_ranges.size(),
            analyzer.getQueryOptions().getReplica_preference().equals(
                TReplicaPreference.REMOTE) ?
                analyzer.numExecutorsForPlanning() :
                dummyHostIndex.size());
        totalInstances = Math.min(scanRangeSpecs_.concrete_ranges.size(),
            totalNodes * maxInstancesPerNode);
        LOG.info(String.format("Planner running in DEBUG mode. ScanNode: %s, "
                + "TotalNodes %d, TotalInstances %d Local Ranges %d",
            tbl_.getFullName(), totalNodes, totalInstances, numLocalRanges));
      } else {
        for (TScanRangeLocationList range : scanRangeSpecs_.concrete_ranges) {
          boolean anyLocal = false;
          if (range.isSetLocations()) {
            for (TScanRangeLocation loc : range.locations) {
              TNetworkAddress dataNode =
                  analyzer.getHostIndex().getEntry(loc.getHost_idx());
              if (cluster.contains(dataNode)) {
                anyLocal = true;
                // Use the full datanode address (including port) to account for the test
                // minicluster where there are multiple datanodes and impalads on a single
                // host.  This assumes that when an impalad is colocated with a datanode,
                // there are the same number of impalads as datanodes on this host in this
                // cluster.
                int count = localRangeCounts.getOrDefault(dataNode, 0);
                if (count < maxInstancesPerNode) {
                  ++totalLocalParallelism;
                  localRangeCounts.put(dataNode, count + 1);
                }
              }
            }
          }
          // This range has at least one replica with a colocated impalad, so assume it
          // will be scheduled on one of those nodes.
          if (anyLocal) {
            ++numLocalRanges;
          } else {
            ++numRemoteRanges;
          }
          // Approximate the number of nodes that will execute locally assigned ranges to
          // be the smaller of the number of locally assigned ranges and the number of
          // hosts that hold block replica for those ranges.
          int numLocalNodes = Math.min(numLocalRanges, localRangeCounts.size());
          // The remote ranges are round-robined across all the impalads.
          int numRemoteNodes =
              Math.min(numRemoteRanges, analyzer.numExecutorsForPlanning());
          // The local and remote assignments may overlap, but we don't know by how much
          // so conservatively assume no overlap.
          totalNodes = Math.min(
              numLocalNodes + numRemoteNodes, analyzer.numExecutorsForPlanning());
          totalInstances = computeNumInstances(numLocalRanges, numRemoteRanges,
              totalNodes, maxInstancesPerNode, totalLocalParallelism);
          // Exit early if we have maxed out our estimate of hosts/instances, to avoid
          // extraneous work in case the number of scan ranges dominates the number of
          // nodes.
          if (totalInstances == maxPossibleInstances) break;
        }
      }
    }
    // Handle the generated range specifications, which may increase our estimates of
    // number of nodes.
    if (totalInstances < maxPossibleInstances && scanRangeSpecs_.isSetSplit_specs()) {
      Preconditions.checkState(
          generatedScanRangeCount_ >= scanRangeSpecs_.getSplit_specsSize());
      numRemoteRanges += generatedScanRangeCount_;
      int numLocalNodes = Math.min(numLocalRanges, localRangeCounts.size());
      totalNodes =
          Math.min(numLocalNodes + numRemoteRanges, analyzer.numExecutorsForPlanning());
      totalInstances = computeNumInstances(numLocalRanges, numRemoteRanges,
          totalNodes, maxInstancesPerNode, totalLocalParallelism);
    }
    // Tables can reside on 0 nodes (empty table), but a plan node must always be
    // executed on at least one node.
    numNodes_ = (cardinality == 0 || totalNodes == 0) ? 1 : totalNodes;
    numInstances_ = (cardinality == 0 || totalInstances == 0) ? 1 : totalInstances;
    if (LOG.isTraceEnabled()) {
      LOG.trace("computeNumNodes totalRanges=" + getEffectiveNumScanRanges()
          + " localRanges=" + numLocalRanges + " remoteRanges=" + numRemoteRanges
          + " localRangeCounts.size=" + localRangeCounts.size()
          + " totalLocalParallelism=" + totalLocalParallelism
          + " executorNodes=" + analyzer.numExecutorsForPlanning() + " "
          + " numNodes=" + numNodes_ + " numInstances=" + numInstances_);
    }
  }

  /**
   * Compute an estimate of the number of instances based on the total number of local
   * and remote ranges, the estimated number of nodes, the maximum possible number
   * of instances we can schedule on a node, and the total number of local instances
   * across nodes.
   */
  private int computeNumInstances(int numLocalRanges, int numRemoteRanges, int numNodes,
      int maxInstancesPerNode, int totalLocalParallelism) {
    // Estimate the total number of instances, based on two upper bounds:
    // * The number of scan ranges to process, excluding local ranges in excess of
    //   maxInstancesPerNode.
    // * The maximum parallelism allowed across all the nodes that will participate.
    int numLocalInstances = Math.min(numLocalRanges, totalLocalParallelism);
    return Math.min(numLocalInstances + numRemoteRanges, numNodes * maxInstancesPerNode);
  }

  @Override
  protected void toThrift(TPlanNode msg) {
    Preconditions.checkState(false, "Unexpected use of old toThrift() signature.");
  }

  @Override
  protected void toThrift(TPlanNode msg, ThriftSerializationCtx serialCtx) {
    msg.hdfs_scan_node = new THdfsScanNode(serialCtx.translateTupleId(
        desc_.getId()).asInt(), new HashSet<>());
    // Register the table for this scan node so tuple caching knows about it.
    serialCtx.registerTable(desc_.getTable());
    if (replicaPreference_ != null) {
      msg.hdfs_scan_node.setReplica_preference(replicaPreference_);
    }
    msg.hdfs_scan_node.setRandom_replica(randomReplica_);
    msg.node_type = TPlanNodeType.HDFS_SCAN_NODE;
    if (!collectionConjuncts_.isEmpty()) {
      Map<Integer, List<TExpr>> tcollectionConjuncts = new LinkedHashMap<>();
      for (Map.Entry<TupleDescriptor, List<Expr>> entry:
        collectionConjuncts_.entrySet()) {
        tcollectionConjuncts.put(
            serialCtx.translateTupleId(entry.getKey().getId()).asInt(),
            Expr.treesToThrift(entry.getValue(), serialCtx));
      }
      msg.hdfs_scan_node.setCollection_conjuncts(tcollectionConjuncts);
    }
    if (skipHeaderLineCount_ > 0) {
      msg.hdfs_scan_node.setSkip_header_line_count(skipHeaderLineCount_);
    }
    msg.hdfs_scan_node.setUse_mt_scan_node(useMtScanNode_);
    Preconditions.checkState((optimizedAggSmap_ == null) == (countStarSlot_ == null));
    if (countStarSlot_ != null) {
      msg.hdfs_scan_node.setCount_star_slot_offset(countStarSlot_.getByteOffset());
    }
    // Stats/dictionary filter conjuncts do not change the logical set that would be
    // returned, so they can be skipped for tuple caching.
    if (!serialCtx.isTupleCache()) {
      if (!statsConjuncts_.isEmpty()) {
        for (Expr e: statsConjuncts_) {
          msg.hdfs_scan_node.addToStats_conjuncts(e.treeToThrift());
        }
      }

      if (statsTuple_ != null) {
        msg.hdfs_scan_node.setStats_tuple_id(statsTuple_.getId().asInt());
      }

      Map<Integer, List<Integer>> dictMap = new LinkedHashMap<>();
      for (Map.Entry<SlotDescriptor, List<Integer>> entry :
        dictionaryFilterConjuncts_.entrySet()) {
        dictMap.put(entry.getKey().getId().asInt(), entry.getValue());
      }
      msg.hdfs_scan_node.setDictionary_filter_conjuncts(dictMap);
    }
    msg.hdfs_scan_node.setIs_partition_key_scan(isPartitionKeyScan_);

    for (HdfsFileFormat format : fileFormats_) {
      msg.hdfs_scan_node.addToFile_formats(format.toThrift());
    }

    if (!overlapPredicateDescs_.isEmpty()) {
      for (TOverlapPredicateDesc desc: overlapPredicateDescs_) {
        msg.hdfs_scan_node.addToOverlap_predicate_descs(desc);
      }
    }
  }

  @Override
  protected String getNodeExplainString(String prefix, String detailPrefix,
      TExplainLevel detailLevel) {
    StringBuilder output = new StringBuilder();
    FeFsTable table = (FeFsTable) desc_.getTable();
    output.append(String.format("%s%s [%s", prefix, getDisplayLabel(),
        getDisplayLabelDetail()));
    if (detailLevel.ordinal() >= TExplainLevel.EXTENDED.ordinal() &&
        fragment_.isPartitioned()) {
      output.append(", " + fragment_.getDataPartition().getExplainString());
    }
    output.append("]\n");
    if (detailLevel.ordinal() >= TExplainLevel.STANDARD.ordinal()) {
      if (partitionConjuncts_ != null && !partitionConjuncts_.isEmpty()) {
        output.append(detailPrefix)
          .append(String.format("partition predicates: %s\n",
              Expr.getExplainString(partitionConjuncts_, detailLevel)));
      }
      String partMetaTemplate = "partitions=%d/%d files=%d size=%s\n";
      String erasureCodeTemplate = "erasure coded: files=%d size=%s\n";
      if (!numPartitionsPerFs_.isEmpty()) {
        // The table is partitioned; print a line for each filesystem we are reading
        // partitions from
        for (Map.Entry<FileSystemUtil.FsType, Long> partsPerFs :
            numPartitionsPerFs_.entrySet()) {
          FileSystemUtil.FsType fsType = partsPerFs.getKey();
          output.append(detailPrefix);
          output.append(fsType).append(" ");
          output.append(String.format(partMetaTemplate, partsPerFs.getValue(),
              table.getPartitions().size(), totalFilesPerFs_.get(fsType),
              PrintUtils.printBytes(totalBytesPerFs_.get(fsType))));

          // Report the total number of erasure coded files and total bytes, if any.
          if (totalFilesPerFsEC_.containsKey(fsType)) {
            long totalNumECFiles = totalFilesPerFsEC_.get(fsType);
            long totalECSize = totalBytesPerFsEC_.get(fsType);
            output.append(String.format("%s", detailPrefix))
                .append(String.format(erasureCodeTemplate, totalNumECFiles,
                    PrintUtils.printBytes(totalECSize)));
          }
        }
      } else if (tbl_.getNumClusteringCols() == 0) {
        // There are no partitions so we use the FsType of the base table. No report
        // on EC related info.
        output.append(detailPrefix);
        output.append(table.getFsType()).append(" ");
        output.append(String.format(partMetaTemplate, 1, table.getPartitions().size(),
            0, PrintUtils.printBytes(0)));
      } else {
        // The table is partitioned, but no partitions are selected; in this case we
        // exclude the FsType completely. No report on EC related info.
        output.append(detailPrefix);
        output.append(String.format(partMetaTemplate, 0, table.getPartitions().size(),
            0, PrintUtils.printBytes(0)));
      }

      if (!conjuncts_.isEmpty()) {
        output.append(detailPrefix)
          .append(String.format("predicates: %s\n",
              Expr.getExplainString(conjuncts_, detailLevel)));
      }
      if (!collectionConjuncts_.isEmpty()) {
        for (Map.Entry<TupleDescriptor, List<Expr>> entry:
          collectionConjuncts_.entrySet()) {
          String alias = entry.getKey().getAlias();
          output.append(detailPrefix)
            .append(String.format("predicates on %s: %s\n", alias,
                Expr.getExplainString(entry.getValue(), detailLevel)));
        }
      }
      if (!runtimeFilters_.isEmpty()) {
        output.append(detailPrefix + "runtime filters: ");
        output.append(getRuntimeFilterExplainString(false, detailLevel));
      }
      if (isPartitionKeyScan_) {
        output.append(detailPrefix + "partition key scan\n");
      }

      String derivedExplain = getDerivedExplainString(detailPrefix, detailLevel);
      if (!derivedExplain.isEmpty()) output.append(derivedExplain);
    }
    if (detailLevel.ordinal() >= TExplainLevel.EXTENDED.ordinal()) {
      output.append(getStatsExplainString(detailPrefix)).append("\n");
      String extrapRows;
      if (FeFsTable.Utils.isStatsExtrapolationEnabled(tbl_)) {
        extrapRows = PrintUtils.printEstCardinality(extrapolatedNumRows_);
      } else {
        extrapRows = "disabled";
      }
      output.append(detailPrefix)
          .append("extrapolated-rows=")
          .append(extrapRows)
          .append(" max-scan-range-rows=")
          .append(PrintUtils.printEstCardinality(maxScanRangeNumRows_));
      if (filteredCardinality_ > -1 && scanRangeSelectivity_ < 1.0) {
        output.append(String.format(" est-scan-range=%d(filtered from %d)",
            estScanRangeAfterRuntimeFilter(), getEffectiveNumScanRanges()));
      }
      output.append("\n");
      if (numScanRangesNoDiskIds_ > 0) {
        output.append(detailPrefix)
            .append(String.format("missing disk ids: "
                    + "partitions=%s/%s files=%s/%s scan ranges %s/%s\n",
                numPartitionsNoDiskIds_, sumValues(numPartitionsPerFs_),
                numFilesNoDiskIds_, sumValues(totalFilesPerFs_), numScanRangesNoDiskIds_,
                getEffectiveNumScanRanges()));
      }
      // Groups the min max original conjuncts by tuple descriptor.
      output.append(getMinMaxOriginalConjunctsExplainString(detailPrefix, detailLevel));
      // Groups the dictionary filterable conjuncts by tuple descriptor.
      output.append(getDictionaryConjunctsExplainString(detailPrefix, detailLevel));

    }
    if (detailLevel.ordinal() >= TExplainLevel.VERBOSE.ordinal()) {
      // Add file formats after sorting so their order is deterministic in the explain
      // string.
      List<String> formats = fileFormats_.stream()
          .map(Object::toString).collect(Collectors.toList());
      Collections.sort(formats);
      output.append(detailPrefix)
          .append("file formats: ")
          .append(formats.toString())
          .append("\n");
    }
    return output.toString();
  }

  // Overriding this function can be used to add extra information to the explain string
  // of the HDFS Scan node from the derived classes (e.g. IcebergScanNode). Each new line
  // in the output should be appended to 'explainLevel' to have the correct indentation.
  protected String getDerivedExplainString(
      String indentPrefix, TExplainLevel detailLevel) {
    return "";
  }

  // Helper method that prints min max original conjuncts by tuple descriptor.
  private String getMinMaxOriginalConjunctsExplainString(
      String prefix, TExplainLevel detailLevel) {
    StringBuilder output = new StringBuilder();
    for (Map.Entry<TupleDescriptor, List<Expr>> entry :
        statsOriginalConjuncts_.entrySet()) {
      TupleDescriptor tupleDesc = entry.getKey();
      List<Expr> exprs = entry.getValue();
      String fileFormatStr;
      if (hasParquet(fileFormats_) && hasOrc(fileFormats_)) {
        fileFormatStr = "parquet/orc";
      } else {
        fileFormatStr = hasParquet(fileFormats_) ? "parquet" : "orc";
      }
      if (tupleDesc == getTupleDesc()) {
        output.append(prefix)
        .append(String.format("%s statistics predicates: %s\n", fileFormatStr,
            Expr.getExplainString(exprs, detailLevel)));
      } else {
        output.append(prefix)
        .append(String.format("%s statistics predicates on %s: %s\n", fileFormatStr,
            tupleDesc.getAlias(), Expr.getExplainString(exprs, detailLevel)));
      }
    }
    return output.toString();
  }

  // Helper method that prints the dictionary filterable conjuncts by tuple descriptor.
  private String getDictionaryConjunctsExplainString(
      String prefix, TExplainLevel detailLevel) {
    StringBuilder output = new StringBuilder();
    Map<TupleDescriptor, List<Integer>> perTupleConjuncts = new LinkedHashMap<>();
    for (Map.Entry<SlotDescriptor, List<Integer>> entry :
      dictionaryFilterConjuncts_.entrySet()) {
      SlotDescriptor slotDescriptor = entry.getKey();
      TupleDescriptor tupleDescriptor = slotDescriptor.getParent();
      List<Integer> indexes = perTupleConjuncts.get(tupleDescriptor);
      if (indexes == null) {
        indexes = new ArrayList<>();
        perTupleConjuncts.put(tupleDescriptor, indexes);
      }
      indexes.addAll(entry.getValue());
    }
    for (Map.Entry<TupleDescriptor, List<Integer>> entry :
      perTupleConjuncts.entrySet()) {
      List<Integer> totalIdxList = entry.getValue();
      // Since the conjuncts are stored by the slot id, they are not necessarily
      // in the same order as the normal conjuncts. Sort the indices so that the
      // order matches the normal conjuncts.
      Collections.sort(totalIdxList);
      List<Expr> conjuncts;
      TupleDescriptor tupleDescriptor = entry.getKey();
      String tupleName = "";

      if (tupleDescriptor == getTupleDesc()) {
        conjuncts = conjuncts_;
      } else {
        conjuncts = collectionConjuncts_.get(tupleDescriptor);
        tupleName = " on " + tupleDescriptor.getAlias();
      }
      Preconditions.checkNotNull(conjuncts);
      List<Expr> exprList = new ArrayList<>();
      for (Integer idx : totalIdxList) {
        Preconditions.checkState(idx.intValue() < conjuncts.size());
        exprList.add(conjuncts.get(idx));
      }
      output.append(String.format("%sparquet dictionary predicates%s: %s\n", prefix,
          tupleName, Expr.getExplainString(exprList, detailLevel)));
    }
    return output.toString();
  }

  @Override
  protected String getTableStatsExplainString(String prefix) {
    StringBuilder output = new StringBuilder();
    TTableStats tblStats = desc_.getTable().getTTableStats();
    String totalBytes = PrintUtils.printBytes(tblStats.total_file_bytes);
    if (tblStats.total_file_bytes == -1) totalBytes = "unavailable";
    output.append(String.format("%stable: rows=%s size=%s",
        prefix,
        PrintUtils.printEstCardinality(tblStats.num_rows),
        totalBytes));
    if (tbl_.getNumClusteringCols() > 0) {
      output.append("\n");
      output.append(String.format("%spartitions: %s/%s rows=%s",
          prefix, numPartitionsWithNumRows_, partitions_.size(),
          PrintUtils.printEstCardinality(partitionNumRows_)));
    }
    return output.toString();
  }

  @Override
  public void computeProcessingCost(TQueryOptions queryOptions) {
    Preconditions.checkNotNull(scanRangeSpecs_);
    processingCost_ = computeScanProcessingCost(queryOptions);
  }

  /**
   *  Estimate CPU processing cost for the scan.  Based on benchmarking, the cost can be
   *  estimated as a function of the number of bytes materialized (for columnar scans) or
   *  bytes read (for non-columnar) plus an additional per row cost for each predicate
   * evaluated.
   */
  @Override
  protected ProcessingCost computeScanProcessingCost(TQueryOptions queryOptions) {
    long inputCardinality = getFilteredInputCardinality();
    long estBytes = 0L;
    double bytesCostCoefficient = 0.0;
    double predicateCostCoefficient = 0.0;

    if (inputCardinality >= 0) {
      if (allColumnarFormat_) {
        float avgRowDataSize = getAvgRowSizeWithoutPad();
        estBytes = (long) Math.ceil(avgRowDataSize * (double) inputCardinality);
        bytesCostCoefficient = COST_COEFFICIENT_COLUMNAR_BYTES_MATERIALIZED;
        predicateCostCoefficient = COST_COEFFICIENT_COLUMNAR_PREDICATE_EVAL;
      } else {
        estBytes = sumValues(totalBytesPerFs_);
        bytesCostCoefficient = COST_COEFFICIENT_NONCOLUMNAR_BYTES_SCANNED;
        predicateCostCoefficient = COST_COEFFICIENT_NONCOLUMNAR_PREDICATE_EVAL;
      }
      double totalCost = (estBytes * bytesCostCoefficient)
          + (inputCardinality * getConjuncts().size() * predicateCostCoefficient);
      if (LOG.isTraceEnabled()) {
        LOG.trace("Scan CPU cost estimate: " + totalCost + ", All Columnar: "
            + allColumnarFormat_ + ", Input Cardinality: " + inputCardinality
            + ", Bytes: " + estBytes + ", Pred Count: " + getConjuncts().size());
      }
      return ProcessingCost.basicCost(getDisplayLabel(), totalCost);
    } else {
      // Input cardinality is 0 or unknown. Fallback to superclass.
      return super.computeScanProcessingCost(queryOptions);
    }
  }

  @Override
  public void computeNodeResourceProfile(TQueryOptions queryOptions) {
    // Update 'useMtScanNode_' before any return cases. It's used in BE.
    useMtScanNode_ = Planner.useMTFragment(queryOptions);
    Preconditions.checkNotNull(scanRangeSpecs_, "Cost estimation requires scan ranges.");
    long scanRangeSize = getEffectiveNumScanRanges();
    if (scanRangeSize == 0) {
      nodeResourceProfile_ = ResourceProfile.noReservation(0);
      return;
    }

    Preconditions.checkState(0 < numNodes_);
    Preconditions.checkState(numNodes_ <= scanRangeSize);
    Preconditions.checkNotNull(desc_);
    Preconditions.checkState(desc_.getTable() instanceof FeFsTable);
    List<Long> columnReservations = null;
    if (hasParquet(fileFormats_) || hasOrc(fileFormats_)) {
      boolean orcAsyncRead = hasOrc(fileFormats_) && queryOptions.orc_async_read;
      columnReservations = computeMinColumnMemReservations(orcAsyncRead);
    }

    int perHostScanRanges = 0;
    for (HdfsFileFormat format : fileFormats_) {
      int partitionScanRange = 0;
      if (format.isParquetBased() || format == HdfsFileFormat.ORC) {
        Preconditions.checkNotNull(columnReservations);
        // For the purpose of this estimation, the number of per-host scan ranges for
        // Parquet/HUDI_PARQUET/ORC files are equal to the number of columns read from the
        // file.
        // I.e. excluding partition columns and columns that are populated from file
        // metadata.
        partitionScanRange = columnReservations.size();
      } else {
        partitionScanRange = estimatePerHostScanRanges(scanRangeSize);
      }
      // From the resource management purview, we want to conservatively estimate memory
      // consumption based on the partition with the highest memory requirements.
      if (partitionScanRange > perHostScanRanges) {
        perHostScanRanges = partitionScanRange;
      }
    }

    // The non-MT scan node requires at least one scanner thread.
    int requiredThreads = useMtScanNode_ ? 0 : 1;
    int maxScannerThreads = computeMaxNumberOfScannerThreads(queryOptions,
        perHostScanRanges);

    long avgScanRangeBytes =
        (long) Math.ceil(sumValues(totalBytesPerFs_) / (double) scanRangeSize);
    // The +1 accounts for an extra I/O buffer to read past the scan range due to a
    // trailing record spanning Hdfs blocks.
    long maxIoBufferSize =
        BitUtil.roundUpToPowerOf2(BackendConfig.INSTANCE.getReadSize());
    long perThreadIoBuffers =
        Math.min((long) Math.ceil(avgScanRangeBytes / (double) maxIoBufferSize),
            MAX_IO_BUFFERS_PER_THREAD) + 1;
    if (queryOptions.compute_processing_cost) {
      // In scan-node.cc, we fix max_row_batches to a constant of 2.
      Preconditions.checkState(maxScannerThreads == 1);
      perThreadIoBuffers = 2;
    }
    long perInstanceMemEstimate = checkedMultiply(
        checkedMultiply(maxScannerThreads, perThreadIoBuffers), maxIoBufferSize);

    // Sanity check: the tighter estimation should not exceed the per-host maximum.
    long perHostUpperBound = getPerHostMemUpperBound();
    if (perInstanceMemEstimate > perHostUpperBound) {
      LOG.warn(String.format("Per-instance mem cost %s exceeded per-host upper bound %s.",
          PrintUtils.printBytes(perInstanceMemEstimate),
          PrintUtils.printBytes(perHostUpperBound)));
      perInstanceMemEstimate = perHostUpperBound;
    }
    perInstanceMemEstimate = Math.max(perInstanceMemEstimate, MIN_MEMORY_ESTIMATE);

    nodeResourceProfile_ = new ResourceProfileBuilder()
        .setMemEstimateBytes(perInstanceMemEstimate)
        .setMinMemReservationBytes(computeMinMemReservation(
            columnReservations, queryOptions))
        .setThreadReservation(requiredThreads).build();
  }

  /**
   *  Compute the minimum memory reservation to process a single scan range
   *  (i.e. hdfs split). We aim to choose a reservation that is as low as possible while
   *  still giving OK performance when running with only the minimum reservation. The
   *  lower bound is one minimum-sized buffer per IoMgr scan range - the absolute minimum
   *  required to scan the data. The upper bounds are:
   * - One max-sized I/O buffer per IoMgr scan range. One max-sized I/O buffer avoids
   *   issuing small I/O unnecessarily. The backend can try to increase the reservation
   *   further if more memory would speed up processing.
   * - File format-specific calculations, e.g. based on estimated column sizes for
   *   Parquet.
   * - The hdfs split size, to avoid reserving excessive memory for small files or ranges,
   *   e.g. small dimension tables with very few rows.
   */
  private long computeMinMemReservation(
      List<Long> columnReservations, TQueryOptions queryOptions) {
    Preconditions.checkState(largestScanRangeBytes_ >= 0);
    long maxIoBufferSize =
        BitUtil.roundUpToPowerOf2(BackendConfig.INSTANCE.getReadSize());
    long reservationBytes = 0;
    for (HdfsFileFormat format: fileFormats_) {
      long formatReservationBytes = 0;
      // TODO: IMPALA-6875 - ORC should compute total reservation across columns once the
      // ORC scanner supports reservations. For now it is treated the same as a
      // row-oriented format because there is no per-column reservation.
      if (format.isParquetBased()
          || (format == HdfsFileFormat.ORC && queryOptions.orc_async_read)) {
        // With Parquet and ORC, we first read the footer then all of the materialized
        // columns in parallel.
        for (long columnReservation : columnReservations) {
          formatReservationBytes += columnReservation;
        }
        long footerSize =
            format == HdfsFileFormat.ORC ? ORC_FOOTER_SIZE : PARQUET_FOOTER_SIZE;
        formatReservationBytes = Math.max(footerSize, formatReservationBytes);
      } else {
        // Scanners for row-oriented formats issue only one IoMgr scan range at a time.
        // Minimum reservation is based on using I/O buffer per IoMgr scan range to get
        // efficient large I/Os.
        formatReservationBytes = maxIoBufferSize;
      }
      reservationBytes = Math.max(reservationBytes, formatReservationBytes);
    }
    reservationBytes = roundUpToIoBuffer(reservationBytes, maxIoBufferSize);

    // Clamp the reservation we computed above to range:
    // * minimum: <# concurrent io mgr ranges> * <min buffer size>, the absolute minimum
    //   needed to execute the scan.
    // * maximum: the maximum scan range (i.e. HDFS split size), rounded up to
    //   the amount of buffers required to read it all at once.
    int iomgrScanRangesPerSplit = columnReservations != null ?
        Math.max(1, columnReservations.size()) : 1;
    long maxReservationBytes =
        roundUpToIoBuffer(largestScanRangeBytes_, maxIoBufferSize);
    return Math.max(iomgrScanRangesPerSplit * BackendConfig.INSTANCE.getMinBufferSize(),
        Math.min(reservationBytes, maxReservationBytes));
  }

  /**
   * Compute minimum memory reservations in bytes per column per scan range for each of
   * the columns read from disk for a columnar format. Returns the raw estimate for
   * each column, not quantized to a buffer size.
   *
   * If there are nested collections, returns a size for each of the leaf scalar slots
   * per collection. This matches Parquet's "shredded" approach to nested collections,
   * where each nested field is stored as a separate column.
   *
   * If table is in ORC format and orcAsyncRead is true, we split per column reservation
   * evenly for number of stream representing that column. For example, an ORC string
   * column with dictionary encoding has four streams (PRESENT, DATA, DICTIONARY_DATA, and
   * LENGTH stream). A computeMinColumnMemReservations over ORC table having one string
   * column will return list:
   *   [1048576, 1048576, 1048576, 1048576]
   * Meanwhile, computeMinColumnMemReservations over Parquet table having one string
   * column will return list:
   *   [4194304]
   */
  private List<Long> computeMinColumnMemReservations(boolean orcAsyncRead) {
    List<Long> columnByteSizes = new ArrayList<>();
    FeFsTable table = (FeFsTable) desc_.getTable();
    boolean havePosSlot = false;
    for (SlotDescriptor slot: desc_.getSlots()) {
      if (!slot.isMaterialized() || slot == countStarSlot_) continue;
      if (slot.getColumn() == null ||
          slot.getColumn().getPosition() >= table.getNumClusteringCols()) {
        Type type = slot.getType();
        if (slot.isArrayPosRef()) {
          // Position virtual slots can be materialized by piggybacking on another slot.
          havePosSlot = true;
        } else if (type.isScalarType()) {
          Column column = slot.getColumn();
          long estReservation;
          if (column == null) {
            // Not a top-level column, e.g. a value from a nested collection that is
            // being unnested by the scanner. No stats are available for nested
            // collections.
            estReservation = DEFAULT_COLUMN_SCAN_RANGE_RESERVATION;
          } else {
            estReservation = computeMinScalarColumnMemReservation(column);
          }

          if (orcAsyncRead) {
            // estReservation need to be spread for each stream. Estimate how many stream
            // that will be available for this column based on column type.
            int estNumStream = 2; // DATA and PRESENT stream.
            if (type.isTimestamp() || type.isStringType() || type.isDecimal()) {
              estNumStream++; // SECONDARY/LENGTH stream.
              if (type.isStringType()) {
                estNumStream++; // DICTIONARY_DATA stream
              }
            }
            long reservationPerStream = estReservation / estNumStream;
            for (int i = 0; i < estNumStream; i++) {
              columnByteSizes.add(reservationPerStream);
            }
          } else {
            columnByteSizes.add(estReservation);
          }
        } else {
          appendMinColumnMemReservationsForComplexType(
              slot, columnByteSizes, orcAsyncRead);
        }
      }
    }

    if (columnByteSizes.isEmpty()) {
      if (havePosSlot || (orcAsyncRead && desc_.getSlots().isEmpty())) {
        // We must scan something to materialize a position slot (hasPosSlot=true).
        // For the case of orcAsyncRead=true and empty descriptor slot, we probably need
        // to materialize something too if the query can not be served by file metadata
        // (ie., select count(*) against subtype of a complex column).
        // We do not know anything about the column we are scanning in either case, so use
        // the default reservation.
        appendDefaultColumnReservation(columnByteSizes, orcAsyncRead);
      }
    }
    return columnByteSizes;
  }

  /**
   * Helper for computeMinColumnMemReservations() - compute minimum memory reservations
   * for all of the scalar columns read from disk when materializing complexSlot.
   * Appends one number per scalar column to columnMemReservations for Parquet format.
   * For Orc, up to 4 number per scalar column can be added to columnMemReservations.
   */
  private void appendMinColumnMemReservationsForComplexType(SlotDescriptor complexSlot,
      List<Long> columnMemReservations, boolean orcAsyncRead) {
    Preconditions.checkState(complexSlot.getType().isComplexType());
    boolean addedColumn = false;
    for (SlotDescriptor nestedSlot: complexSlot.getItemTupleDesc().getSlots()) {
      // Position virtual slots can be materialized by piggybacking on another slot.
      if (!nestedSlot.isMaterialized() || nestedSlot.isArrayPosRef()) continue;
      if (nestedSlot.getType().isScalarType()) {
        // No column stats are available for nested collections so use the default
        // reservation.
        appendDefaultColumnReservation(columnMemReservations, orcAsyncRead);
        addedColumn = true;
      } else if (nestedSlot.getType().isComplexType()) {
        appendMinColumnMemReservationsForComplexType(
            nestedSlot, columnMemReservations, orcAsyncRead);
      }
    }
    // Need to scan at least one column to materialize the pos virtual slot and/or
    // determine the size of the nested array. Assume it is the size of a single I/O
    // buffer.
    if (!addedColumn) {
      appendDefaultColumnReservation(columnMemReservations, orcAsyncRead);
    }
  }

  /**
   * Compute minimum memory reservation for an unknown column.
   *
   * If orcAsyncRead is true, we allocate DEFAULT_COLUMN_SCAN_RANGE_RESERVATION and split
   * it for 4 streams. This is because an ORC column can have 4 streams at most (ie.,
   * dictionary encoded string/char/varchar column). If orcAsyncRead is false, just append
   * single default reservation.
   */
  private void appendDefaultColumnReservation(
      List<Long> columnMemReservations, boolean orcAsyncRead) {
    if (orcAsyncRead) {
      int estNumStream = 4;
      Long reservationPerStream = DEFAULT_COLUMN_SCAN_RANGE_RESERVATION / estNumStream;
      for (int i = 0; i < estNumStream; i++) {
        columnMemReservations.add(reservationPerStream);
      }
    } else {
      columnMemReservations.add(DEFAULT_COLUMN_SCAN_RANGE_RESERVATION);
    }
  }

  /**
   * Choose the min bytes to reserve for this scalar column for a scan range. Returns the
   * raw estimate without quantizing to buffer sizes - the caller should do so if needed.
   *
   * Starts with DEFAULT_COLUMN_SCAN_RANGE_RESERVATION and tries different strategies to
   * infer that the column data is smaller than this starting value (and therefore the
   * extra memory would not be useful). These estimates are quite conservative so this
   * will still often overestimate the column size. An overestimate does not necessarily
   * result in memory being wasted because the Parquet scanner distributes the total
   * reservation between columns based on actual column size, so if multiple columns are
   * scanned, memory over-reserved for one column can be used to help scan a different
   * larger column.
   */
  private long computeMinScalarColumnMemReservation(Column column) {
    Preconditions.checkNotNull(column);
    long reservationBytes = DEFAULT_COLUMN_SCAN_RANGE_RESERVATION;
    ColumnStats stats = column.getStats();
    if (stats.hasAvgSize() && maxScanRangeNumRows_ != -1) {
      // Estimate the column's uncompressed data size based on row count and average
      // size.
      // TODO: Size of strings seems to be underestimated, as avg size returns the
      //       average length of the strings and does not include the 4 byte length
      //       field used in Parquet plain encoding. (IMPALA-8431)
      reservationBytes =
          (long) Math.min(reservationBytes, stats.getAvgSize() * maxScanRangeNumRows_);
      if (stats.hasNumDistinctValues()) {
        // Estimate the data size with dictionary compression, assuming all distinct
        // values occur in the scan range with the largest number of rows and that each
        // value can be represented with approximately log2(ndv) bits. Even if Parquet
        // dictionary compression does not kick in, general-purpose compression
        // algorithms like Snappy can often find redundancy when there are repeated
        // values.
        long dictBytes = (long)(stats.getAvgSize() * stats.getNumDistinctValues());
        long bitsPerVal = BitUtil.log2Ceiling(stats.getNumDistinctValues());
        long encodedDataBytes = bitsPerVal * maxScanRangeNumRows_ / 8;
        reservationBytes = Math.min(reservationBytes, dictBytes + encodedDataBytes);
      }
    }
    return reservationBytes;
  }

  /**
   * Calculate the total bytes of I/O buffers that would be allocated to hold bytes,
   * given that buffers must be a power-of-two size <= maxIoBufferSize bytes.
   */
  private static long roundUpToIoBuffer(long bytes, long maxIoBufferSize) {
    return bytes < maxIoBufferSize ?
        BitUtil.roundUpToPowerOf2(bytes) :
        BitUtil.roundUpToPowerOf2Factor(bytes, maxIoBufferSize);
  }

  /**
   * Hdfs scans use a shared pool of buffers managed by the I/O manager. Intuitively,
   * the maximum number of I/O buffers is limited by the total disk bandwidth of a node.
   * Therefore, this upper bound is independent of the number of concurrent scans and
   * queries and helps to derive a tighter per-host memory estimate for queries with
   * multiple concurrent scans.
   * TODO: this doesn't accurately describe how the backend works, but it is useful to
   * have an upper bound. We should rethink and replace this with a different upper bound.
   */
  public static long getPerHostMemUpperBound() {
    // THREADS_PER_CORE each using a default of
    // MAX_IO_BUFFERS_PER_THREAD * read_size bytes.
    return (long) RuntimeEnv.INSTANCE.getNumCores() * (long) MAX_THREAD_TOKENS_PER_CORE *
        MAX_IO_BUFFERS_PER_THREAD * BackendConfig.INSTANCE.getReadSize();
  }

  @Override
  public boolean isTableMissingTableStats() {
    if (extrapolatedNumRows_ >= 0) return false;
    if (tbl_.getNumClusteringCols() > 0
        && numPartitionsWithNumRows_ != partitions_.size()) {
      return true;
    }
    return super.isTableMissingTableStats();
  }

  @Override
  public boolean hasCorruptTableStats() { return hasCorruptTableStats_; }

  public boolean hasMissingDiskIds() { return numScanRangesNoDiskIds_ > 0; }

  protected Map<TupleDescriptor, List<Expr>> getCollectionConjuncts() {
    return collectionConjuncts_;
  }

  protected Set<HdfsFileFormat> getFileFormats() { return fileFormats_; }

  /**
   * Returns of all the values in the given {@link Map}.
   */
  private static long sumValues(Map<?, Long> input) {
    return input.values().stream().mapToLong(Long::longValue).sum();
  }

  /**
   * Return the number of scan ranges. computeScanRangeLocations() must be called
   * before calling this.
   */
  public long getNumScanRanges() {
    Preconditions.checkNotNull(scanRangeSpecs_);
    return scanRangeSpecs_.getConcrete_rangesSize()
        + scanRangeSpecs_.getSplit_specsSize();
  }

  /**
   * Return the number of scan ranges when considering MAX_SCAN_RANGE_LENGTH option.
   * computeScanRangeLocations() must be called before calling this.
   */
  @Override
  public long getEffectiveNumScanRanges() {
    Preconditions.checkNotNull(scanRangeSpecs_);
    Preconditions.checkState(
        generatedScanRangeCount_ >= scanRangeSpecs_.getSplit_specsSize());
    return scanRangeSpecs_.getConcrete_rangesSize() + generatedScanRangeCount_;
  }

  /**
   * Sort filters in runtimeFilters_: min/max first followed by bloom.
   */
  public void arrangeRuntimefiltersForParquet() {
    if (allParquet_) {
      Collections.sort(runtimeFilters_, new Comparator<RuntimeFilter>() {
        @Override
        public int compare(RuntimeFilter a, RuntimeFilter b) {
          if (a.getType() == b.getType()) return 0;
          if (a.getType() == TRuntimeFilterType.MIN_MAX
              && b.getType() == TRuntimeFilterType.BLOOM)
            return -1;
          return 1;
        }
      });
    }
  }

  protected boolean isAllColumnarScanner() { return allColumnarFormat_; }

  @Override
  public boolean isTupleCachingImplemented() { return true; }
}
