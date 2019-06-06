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
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.stream.Collectors;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.impala.analysis.AggregateInfo;
import org.apache.impala.analysis.Analyzer;
import org.apache.impala.analysis.BinaryPredicate;
import org.apache.impala.analysis.DescriptorTable;
import org.apache.impala.analysis.Expr;
import org.apache.impala.analysis.ExprSubstitutionMap;
import org.apache.impala.analysis.FunctionCallExpr;
import org.apache.impala.analysis.FunctionName;
import org.apache.impala.analysis.FunctionParams;
import org.apache.impala.analysis.InPredicate;
import org.apache.impala.analysis.IsNotEmptyPredicate;
import org.apache.impala.analysis.LiteralExpr;
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
import org.apache.impala.fb.FbFileBlock;
import org.apache.impala.service.BackendConfig;
import org.apache.impala.thrift.TExplainLevel;
import org.apache.impala.thrift.TExpr;
import org.apache.impala.thrift.TFileSplitGeneratorSpec;
import org.apache.impala.thrift.THdfsFileSplit;
import org.apache.impala.thrift.THdfsScanNode;
import org.apache.impala.thrift.TNetworkAddress;
import org.apache.impala.thrift.TPlanNode;
import org.apache.impala.thrift.TPlanNodeType;
import org.apache.impala.thrift.TQueryOptions;
import org.apache.impala.thrift.TReplicaPreference;
import org.apache.impala.thrift.TScanRange;
import org.apache.impala.thrift.TScanRangeLocation;
import org.apache.impala.thrift.TScanRangeLocationList;
import org.apache.impala.thrift.TScanRangeSpec;
import org.apache.impala.thrift.TTableStats;
import org.apache.impala.util.BitUtil;
import org.apache.impala.util.ExecutorMembershipSnapshot;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Joiner;
import com.google.common.base.Objects;
import com.google.common.base.Objects.ToStringHelper;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
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
 * Count(*) aggregation optimization flow:
 * The caller passes in an AggregateInfo to the constructor that this scan node uses to
 * determine whether to apply the optimization or not. The produced smap must then be
 * applied to the AggregateInfo in this query block. We do not apply the smap in this
 * class directly to avoid side effects and make it easier to reason about.
 * See HdfsScanNode.applyParquetCountStartOptimization().
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

  // Read size for Parquet and ORC footers. Matches HdfsScanner::FOOTER_SIZE in backend.
  private static final long FOOTER_SIZE = 100L * 1024L;

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
      .add(HdfsFileFormat.LZO_TEXT)
      .add(HdfsFileFormat.SEQUENCE_FILE)
      .add(HdfsFileFormat.AVRO)
      .build();

  private static Set<HdfsFileFormat> VALID_COLUMNAR_FORMATS =
      ImmutableSet.<HdfsFileFormat>builder()
      .add(HdfsFileFormat.PARQUET)
      .add(HdfsFileFormat.ORC)
      .build();

  //An estimate of the width of a row when the information is not available.
  private double DEFAULT_ROW_WIDTH_ESTIMATE = 1.0;

  private final FeFsTable tbl_;

  // List of partitions to be scanned. Partitions have been pruned.
  private final List<? extends FeFsPartition> partitions_;

  // Parameters for table sampling. Null if not sampling.
  private final TableSampleClause sampleParams_;

  private final TReplicaPreference replicaPreference_;
  private final boolean randomReplica_;

  // The AggregationInfo from the query block of this scan node. Used for determining if
  // the Parquet count(*) optimization can be applied.
  private final AggregateInfo aggInfo_;

  // Number of partitions, files and bytes scanned. Set in computeScanRangeLocations().
  // Might not match 'partitions_' due to table sampling. Grouped by the FsType, so
  // each key value pair maps how many partitions / files / bytes are stored on each fs.
  // Stored as a TreeMap so that iteration order is defined by the order of enums in
  // FsType.
  private Map<FileSystemUtil.FsType, Long> numPartitionsPerFs_ = new TreeMap<>();
  private Map<FileSystemUtil.FsType, Long> totalFilesPerFs_ = new TreeMap<>();
  private Map<FileSystemUtil.FsType, Long> totalBytesPerFs_ = new TreeMap<>();

  // File formats scanned. Set in computeScanRangeLocations().
  private Set<HdfsFileFormat> fileFormats_;

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
  private boolean useMtScanNode_;

  // Should be applied to the AggregateInfo from the same query block. We cannot use the
  // PlanNode.outputSmap_ for this purpose because we don't want the smap entries to be
  // propagated outside the query block.
  protected ExprSubstitutionMap optimizedAggSmap_;

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

  // List of conjuncts for min/max values of parquet::Statistics, that are used to skip
  // data when scanning Parquet files.
  private final List<Expr> minMaxConjuncts_ = new ArrayList<>();

  // Map from TupleDescriptor to list of PlanNode conjuncts that have been transformed
  // into conjuncts in 'minMaxConjuncts_'.
  private final Map<TupleDescriptor, List<Expr>> minMaxOriginalConjuncts_ =
      new LinkedHashMap<>();

  // Tuple that is used to materialize statistics when scanning Parquet files. For each
  // column it can contain 0, 1, or 2 slots, depending on whether the column needs to be
  // evaluated against the min and/or the max value of the corresponding
  // parquet::Statistics.
  private TupleDescriptor minMaxTuple_;

  // Slot that is used to record the Parquet metadata for the count(*) aggregation if
  // this scan node has the count(*) optimization enabled.
  private SlotDescriptor countStarSlot_ = null;

  // Conjuncts used to trim the set of partitions passed to this node.
  // Used only to display EXPLAIN information.
  private final List<Expr> partitionConjuncts_;

  /**
   * Construct a node to scan given data files into tuples described by 'desc',
   * with 'conjuncts' being the unevaluated conjuncts bound by the tuple and
   * 'partitions' being the partitions which need to be included. Please see
   * class comments above for details.
   */
  public HdfsScanNode(PlanNodeId id, TupleDescriptor desc, List<Expr> conjuncts,
      List<? extends FeFsPartition> partitions, TableRef hdfsTblRef,
      AggregateInfo aggInfo, List<Expr> partConjuncts) {
    super(id, desc, createDisplayName(hdfsTblRef.getTable()));
    tbl_ = (FeFsTable)desc.getTable();
    conjuncts_ = conjuncts;
    partitions_ = partitions;
    partitionConjuncts_ = partConjuncts;
    sampleParams_ = hdfsTblRef.getSampleParams();
    replicaPreference_ = hdfsTblRef.getReplicaPreference();
    randomReplica_ = hdfsTblRef.getRandomReplica();
    FeFsTable hdfsTable = (FeFsTable)hdfsTblRef.getTable();
    Preconditions.checkState(tbl_ == hdfsTable);
    StringBuilder error = new StringBuilder();
    aggInfo_ = aggInfo;
    skipHeaderLineCount_ = tbl_.parseSkipHeaderLineCount(error);
    if (error.length() > 0) {
      // Any errors should already have been caught during analysis.
      throw new IllegalStateException(error.toString());
    }
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
    ToStringHelper helper = Objects.toStringHelper(this);
    for (FeFsPartition partition: partitions_) {
      helper.add("Partition " + partition.getId() + ":", partition.toString());
    }
    return helper.addValue(super.debugString()).toString();
  }

  /**
   * Adds a new slot descriptor to the tuple descriptor of this scan. The new slot will be
   * used for storing the data extracted from the Parquet num rows statistic. Also adds an
   * entry to 'optimizedAggSmap_' that substitutes count(*) with
   * sum_init_zero(<new-slotref>). Returns the new slot descriptor.
   */
  private SlotDescriptor applyParquetCountStartOptimization(Analyzer analyzer) {
    FunctionCallExpr countFn = new FunctionCallExpr(new FunctionName("count"),
        FunctionParams.createStarParam());
    countFn.analyzeNoThrow(analyzer);

    // Create the sum function.
    SlotDescriptor sd = analyzer.addSlotDescriptor(getTupleDesc());
    sd.setType(Type.BIGINT);
    sd.setIsMaterialized(true);
    sd.setIsNullable(false);
    sd.setLabel("parquet-stats: num_rows");
    List<Expr> args = new ArrayList<>();
    args.add(new SlotRef(sd));
    FunctionCallExpr sumFn = new FunctionCallExpr("sum_init_zero", args);
    sumFn.analyzeNoThrow(analyzer);

    optimizedAggSmap_ = new ExprSubstitutionMap();
    optimizedAggSmap_.put(countFn, sumFn);
    return sd;
  }

  /**
   * Returns true if the Parquet count(*) optimization can be applied to the query block
   * of this scan node.
   */
  private boolean canApplyParquetCountStarOptimization(Analyzer analyzer,
      Set<HdfsFileFormat> fileFormats) {
    if (analyzer.getNumTableRefs() != 1) return false;
    if (aggInfo_ == null || !aggInfo_.hasCountStarOnly()) return false;
    if (fileFormats.size() != 1) return false;
    if (!fileFormats.contains(HdfsFileFormat.PARQUET)) return false;
    if (!conjuncts_.isEmpty()) return false;
    return desc_.getMaterializedSlots().isEmpty() || desc_.hasClusteringColsOnly();
  }

  /**
   * Populate collectionConjuncts_ and scanRanges_.
   */
  @Override
  public void init(Analyzer analyzer) throws ImpalaException {
    conjuncts_ = orderConjunctsByCost(conjuncts_);
    checkForSupportedFileFormats();

    assignCollectionConjuncts(analyzer);

    // compute scan range locations with optional sampling
    computeScanRangeLocations(analyzer);

    // Determine backend scan node implementation to use. The optimized MT implementation
    // is currently supported for Parquet, ORC and Text.
    if (analyzer.getQueryOptions().isSetMt_dop() &&
        analyzer.getQueryOptions().mt_dop > 0 &&
        fileFormats_.size() == 1 &&
        (fileFormats_.contains(HdfsFileFormat.PARQUET)
          || fileFormats_.contains(HdfsFileFormat.ORC)
          || fileFormats_.contains(HdfsFileFormat.TEXT))) {
      useMtScanNode_ = true;
    } else {
      useMtScanNode_ = false;
    }

    if (fileFormats_.contains(HdfsFileFormat.PARQUET)) {
      // Compute min-max conjuncts only if the PARQUET_READ_STATISTICS query option is
      // set to true.
      if (analyzer.getQueryOptions().parquet_read_statistics) {
        computeMinMaxTupleAndConjuncts(analyzer);
      }
      // Compute dictionary conjuncts only if the PARQUET_DICTIONARY_FILTERING query
      // option is set to true.
      if (analyzer.getQueryOptions().parquet_dictionary_filtering) {
        computeDictionaryFilterConjuncts(analyzer);
      }
    }

    if (canApplyParquetCountStarOptimization(analyzer, fileFormats_)) {
      Preconditions.checkState(desc_.getPath().destTable() != null);
      Preconditions.checkState(collectionConjuncts_.isEmpty());
      countStarSlot_ = applyParquetCountStartOptimization(analyzer);
    }

    computeMemLayout(analyzer);

    // This is towards the end, so that it can take all conjuncts, scan ranges and mem
    // layout into account.
    computeStats(analyzer);

    // TODO: do we need this?
    assignedConjuncts_ = analyzer.getAssignedConjuncts();
  }

  /**
   * Throws NotImplementedException if we do not support scanning the partition.
   * Specifically:
   * 1) if the table schema contains a complex type and we need to scan
   * a partition that has a format for which we do not support complex types,
   * regardless of whether a complex-typed column is actually referenced
   * in the query.
   * 2) if we are scanning ORC partitions and the ORC scanner is disabled.
   */
  @Override
  protected void checkForSupportedFileFormats() throws NotImplementedException {
    Preconditions.checkNotNull(desc_);
    Preconditions.checkNotNull(desc_.getTable());

    if (!BackendConfig.INSTANCE.isOrcScannerEnabled()) {
      for (FeFsPartition part: partitions_) {
        if (part.getInputFormatDescriptor().getFileFormat() == HdfsFileFormat.ORC) {
          throw new NotImplementedException(
              "ORC scans are disabled by --enable_orc_scanner flag");
        }
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
      HdfsFileFormat format = part.getInputFormatDescriptor().getFileFormat();
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
   * into 'minMaxTuple_', combining 'inputSlot', 'inputPred' and 'op' into a new
   * predicate, and adding it to 'minMaxConjuncts_'.
   */
  private void buildStatsPredicate(Analyzer analyzer, SlotRef inputSlot,
      BinaryPredicate inputPred, BinaryPredicate.Operator op) {
    // Obtain the rhs expr of the input predicate
    Expr constExpr = inputPred.getChild(1);
    Preconditions.checkState(constExpr.isConstant());

    // Make a new slot descriptor, which adds it to the tuple descriptor.
    SlotDescriptor slotDesc = analyzer.getDescTbl().copySlotDescriptor(minMaxTuple_,
        inputSlot.getDesc());
    SlotRef slot = new SlotRef(slotDesc);
    BinaryPredicate statsPred = new BinaryPredicate(op, slot, constExpr);
    statsPred.analyzeNoThrow(analyzer);
    minMaxConjuncts_.add(statsPred);
  }

  private void tryComputeBinaryMinMaxPredicate(Analyzer analyzer,
      BinaryPredicate binaryPred) {
    // We only support slot refs on the left hand side of the predicate, a rewriting
    // rule makes sure that all compatible exprs are rewritten into this form. Only
    // implicit casts are supported.
    SlotRef slotRef = binaryPred.getChild(0).unwrapSlotRef(true);
    if (slotRef == null) return;

    // This node is a table scan, so this must be a scanning slot.
    Preconditions.checkState(slotRef.getDesc().isScanSlot());
    // Skip the slot ref if it refers to an array's "pos" field.
    if (slotRef.getDesc().isArrayPosRef()) return;

    Expr constExpr = binaryPred.getChild(1);
    // Only constant exprs can be evaluated against parquet::Statistics. This includes
    // LiteralExpr, but can also be an expr like "1 + 2".
    if (!constExpr.isConstant()) return;
    if (Expr.IS_NULL_VALUE.apply(constExpr)) return;

    BinaryPredicate.Operator op = binaryPred.getOp();
    if (op == BinaryPredicate.Operator.LT || op == BinaryPredicate.Operator.LE ||
        op == BinaryPredicate.Operator.GE || op == BinaryPredicate.Operator.GT) {
      addMinMaxOriginalConjunct(slotRef.getDesc().getParent(), binaryPred);
      buildStatsPredicate(analyzer, slotRef, binaryPred, op);
    } else if (op == BinaryPredicate.Operator.EQ) {
      addMinMaxOriginalConjunct(slotRef.getDesc().getParent(), binaryPred);
      // TODO: this could be optimized for boolean columns.
      buildStatsPredicate(analyzer, slotRef, binaryPred, BinaryPredicate.Operator.LE);
      buildStatsPredicate(analyzer, slotRef, binaryPred, BinaryPredicate.Operator.GE);
    }
  }

  private void tryComputeInListMinMaxPredicate(Analyzer analyzer, InPredicate inPred) {
    // Retrieve the left side of the IN predicate. It must be a simple slot to proceed.
    SlotRef slotRef = inPred.getBoundSlot();
    if (slotRef == null) return;
    // This node is a table scan, so this must be a scanning slot.
    Preconditions.checkState(slotRef.getDesc().isScanSlot());
    // Skip the slot ref if it refers to an array's "pos" field.
    if (slotRef.getDesc().isArrayPosRef()) return;
    if (inPred.isNotIn()) return;

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

    addMinMaxOriginalConjunct(slotRef.getDesc().getParent(), inPred);
    buildStatsPredicate(analyzer, slotRef, minBound, minBound.getOp());
    buildStatsPredicate(analyzer, slotRef, maxBound, maxBound.getOp());
  }

  private void addMinMaxOriginalConjunct(TupleDescriptor tupleDesc, Expr expr) {
    List<Expr> exprs = minMaxOriginalConjuncts_.get(tupleDesc);
    if (exprs == null) {
      exprs = new ArrayList<>();
      minMaxOriginalConjuncts_.put(tupleDesc, exprs);
    }
    exprs.add(expr);
  }

  private void tryComputeMinMaxPredicate(Analyzer analyzer, Expr pred) {
    if (pred instanceof BinaryPredicate) {
      tryComputeBinaryMinMaxPredicate(analyzer, (BinaryPredicate) pred);
    } else if (pred instanceof InPredicate) {
      tryComputeInListMinMaxPredicate(analyzer, (InPredicate) pred);
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
   * Analyzes 'conjuncts_' and 'collectionConjuncts_', populates 'minMaxTuple_' with slots
   * for statistics values, and populates 'minMaxConjuncts_' with conjuncts pointing into
   * the 'minMaxTuple_'. Only conjuncts of the form <slot> <op> <constant> are supported,
   * and <op> must be one of LT, LE, GE, GT, or EQ.
   */
  private void computeMinMaxTupleAndConjuncts(Analyzer analyzer) throws ImpalaException{
    Preconditions.checkNotNull(desc_.getPath());
    String tupleName = desc_.getPath().toString() + " statistics";
    DescriptorTable descTbl = analyzer.getDescTbl();
    minMaxTuple_ = descTbl.createTupleDescriptor(tupleName);
    minMaxTuple_.setPath(desc_.getPath());

    // Adds predicates for scalar, top-level columns.
    for (Expr pred: conjuncts_) tryComputeMinMaxPredicate(analyzer, pred);

    // Adds predicates for collections.
    for (Map.Entry<TupleDescriptor, List<Expr>> entry: collectionConjuncts_.entrySet()) {
      if (notEmptyCollections_.contains(entry.getKey())) {
        for (Expr pred: entry.getValue()) tryComputeMinMaxPredicate(analyzer, pred);
      }
    }
    minMaxTuple_.computeMemLayout();
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

    // Check to see if this slot is a collection type. Dictionary pruning is applicable
    // to scalar values nested in collection types, not enclosing collection types.
    if (analyzer.getSlotDesc(slotIds.get(0)).getType().isCollectionType()) return;

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
   * A collection of metadata associated with a sampled partition. Unlike
   * {@link FeFsPartition} this class is safe to use in hash-based data structures.
   */
  public static final class SampledPartitionMetadata {

    private final long partitionId;
    private final FileSystemUtil.FsType partitionFsType;

    public SampledPartitionMetadata(
        long partitionId, FileSystemUtil.FsType partitionFsType) {
      this.partitionId = partitionId;
      this.partitionFsType = partitionFsType;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;
      SampledPartitionMetadata that = (SampledPartitionMetadata) o;
      return partitionId == that.partitionId && partitionFsType == that.partitionFsType;
    }

    @Override
    public int hashCode() {
      return java.util.Objects.hash(partitionId, partitionFsType);
    }

    private FileSystemUtil.FsType getPartitionFsType() { return partitionFsType; }
  }

  /**
   * Computes scan ranges (i.e. hdfs splits) plus their storage locations, including
   * volume ids, based on the given maximum number of bytes each scan range should scan.
   * If 'sampleParams_' is not null, generates a sample and computes the scan ranges
   * based on the sample.
   *
   * Initializes members with information about files and scan ranges, e.g.
   * totalFilesPerFs_, fileFormats_, etc.
   */
  private void computeScanRangeLocations(Analyzer analyzer)
      throws ImpalaRuntimeException {
    Map<SampledPartitionMetadata, List<FileDescriptor>> sampledFiles = null;
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
      sampledFiles = FeFsTable.Utils.getFilesSample(tbl_, partitions_, percentBytes, 0,
          randomSeed);
    }

    long scanRangeBytesLimit = analyzer.getQueryCtx().client_request.getQuery_options()
        .getMax_scan_range_length();
    scanRangeSpecs_ = new TScanRangeSpec();

    if (sampledFiles != null) {
      numPartitionsPerFs_ = sampledFiles.keySet().stream().collect(Collectors.groupingBy(
          SampledPartitionMetadata::getPartitionFsType, Collectors.counting()));
    } else {
      numPartitionsPerFs_.putAll(partitions_.stream().collect(
          Collectors.groupingBy(FeFsPartition::getFsType, Collectors.counting())));
    }

    totalFilesPerFs_ = new TreeMap<>();
    totalBytesPerFs_ = new TreeMap<>();
    largestScanRangeBytes_ = 0;
    maxScanRangeNumRows_ = -1;
    fileFormats_ = new HashSet<>();
    for (FeFsPartition partition: partitions_) {
      List<FileDescriptor> fileDescs = partition.getFileDescriptors();
      if (sampledFiles != null) {
        // If we are sampling, check whether this partition is included in the sample.
        fileDescs = sampledFiles.get(
            new SampledPartitionMetadata(partition.getId(), partition.getFsType()));
        if (fileDescs == null) continue;
      }
      long partitionNumRows = partition.getNumRows();

      analyzer.getDescTbl().addReferencedPartition(tbl_, partition.getId());
      fileFormats_.add(partition.getFileFormat());
      Preconditions.checkState(partition.getId() >= 0);
      // Missing disk id accounting is only done for file systems that support the notion
      // of disk/storage ids.
      FileSystem partitionFs;
      try {
        partitionFs = partition.getLocationPath().getFileSystem(CONF);
      } catch (IOException e) {
        throw new ImpalaRuntimeException("Error determining partition fs type", e);
      }
      boolean fsHasBlocks = FileSystemUtil.supportsStorageIds(partitionFs);
      if (!fsHasBlocks) {
        // Limit the scan range length if generating scan ranges.
        long maxBlockSize =
            Math.max(partitionFs.getDefaultBlockSize(partition.getLocationPath()),
                FileDescriptor.MIN_SYNTHETIC_BLOCK_SIZE);
        if (scanRangeBytesLimit > 0) {
          scanRangeBytesLimit = Math.min(scanRangeBytesLimit, maxBlockSize);
        } else {
          scanRangeBytesLimit = maxBlockSize;
        }
      }
      final long partitionBytes = FileDescriptor.computeTotalFileLength(fileDescs);
      long partitionMaxScanRangeBytes = 0;
      boolean partitionMissingDiskIds = false;
      totalBytesPerFs_.merge(partition.getFsType(), partitionBytes, Long::sum);
      totalFilesPerFs_.merge(partition.getFsType(), (long) fileDescs.size(), Long::sum);
      for (FileDescriptor fileDesc: fileDescs) {
        if (!analyzer.getQueryOptions().isAllow_erasure_coded_files() &&
            fileDesc.getIsEc()) {
          throw new ImpalaRuntimeException(String.format(
              "Scanning of HDFS erasure-coded file (%s/%s) is not supported",
              partition.getLocation(), fileDesc.getRelativePath()));
        }
        if (!fsHasBlocks) {
          Preconditions.checkState(fileDesc.getNumFileBlocks() == 0);
          generateScanRangeSpecs(partition, fileDesc, scanRangeBytesLimit);
        } else {
          // Skips files that have no associated blocks.
          if (fileDesc.getNumFileBlocks() == 0) continue;
          Pair<Boolean, Long> result = transformBlocksToScanRanges(
              partition, fileDesc, fsHasBlocks, scanRangeBytesLimit, analyzer);
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
    }
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
   */
  private void generateScanRangeSpecs(
      FeFsPartition partition, FileDescriptor fileDesc, long maxBlockSize) {
    Preconditions.checkArgument(fileDesc.getNumFileBlocks() == 0);
    Preconditions.checkArgument(maxBlockSize > 0);
    if (fileDesc.getFileLength() <= 0) return;
    boolean splittable = partition.getFileFormat().isSplittable(
        HdfsCompression.fromFileName(fileDesc.getRelativePath()));
    TFileSplitGeneratorSpec splitSpec = new TFileSplitGeneratorSpec(
        fileDesc.toThrift(), maxBlockSize, splittable, partition.getId(),
        partition.getLocation().hashCode());
    scanRangeSpecs_.addToSplit_specs(splitSpec);
    long scanRangeBytes = Math.min(maxBlockSize, fileDesc.getFileLength());
    if (splittable) {
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
   */
  private Pair<Boolean, Long> transformBlocksToScanRanges(FeFsPartition partition,
      FileDescriptor fileDesc, boolean fsHasBlocks,
      long scanRangeBytesLimit, Analyzer analyzer) {
    Preconditions.checkArgument(fileDesc.getNumFileBlocks() > 0);
    boolean fileDescMissingDiskIds = false;
    long fileMaxScanRangeBytes = 0;
    for (int i = 0; i < fileDesc.getNumFileBlocks(); ++i) {
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
            partition.getTable().getHostIndex().getEntry(replicaHostIdx);
        Preconditions.checkNotNull(networkAddress);
        // Translate from network address to the global (to this request) host index.
        Integer globalHostIdx = analyzer.getHostIndex().getIndex(networkAddress);
        location.setHost_idx(globalHostIdx);
        if (fsHasBlocks && !fileDesc.getIsEc() && FileBlock.getDiskId(block, j) == -1) {
          ++numScanRangesNoDiskIds_;
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
        }
        TScanRange scanRange = new TScanRange();
        scanRange.setHdfs_file_split(new THdfsFileSplit(fileDesc.getRelativePath(),
            currentOffset, currentLength, partition.getId(), fileDesc.getFileLength(),
            fileDesc.getFileCompression().toThrift(), fileDesc.getModificationTime(),
            fileDesc.getIsEc(), partition.getLocation().hashCode()));
        TScanRangeLocationList scanRangeLocations = new TScanRangeLocationList();
        scanRangeLocations.scan_range = scanRange;
        scanRangeLocations.locations = locations;
        scanRangeSpecs_.addToConcrete_ranges(scanRangeLocations);
        largestScanRangeBytes_ = Math.max(largestScanRangeBytes_, currentLength);
        fileMaxScanRangeBytes = Math.max(fileMaxScanRangeBytes, currentLength);
        remainingLength -= currentLength;
        currentOffset += currentLength;
      }
    }
    if (fileDescMissingDiskIds) {
      ++numFilesNoDiskIds_;
      if (LOG.isTraceEnabled()) {
        LOG.trace("File blocks mapping to unknown disk ids. Dir: "
            + partition.getLocation() + " File:" + fileDesc.toString());
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
  private void computeCardinalities(Analyzer analyzer) {
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
      cardinality_ = Math.round(cardinality_ * computeSelectivity());
      // IMPALA-2165: Avoid setting the cardinality to 0 after rounding.
      cardinality_ = Math.max(cardinality_, 1);
    }
    cardinality_ = capCardinalityAtLimit(cardinality_);
    if (LOG.isTraceEnabled()) {
      LOG.trace("HdfsScan: cardinality_=" + Long.toString(cardinality_));
    }
  }

  /**
   * Computes and returns the number of rows scanned based on the per-partition row count
   * stats and/or the table-level row count stats, depending on which of those are
   * available, and whether the table is partitioned. Partitions without stats are
   * ignored as long as there is at least one partition with stats. Otherwise,
   * we fall back to table-level stats even for partitioned tables.
   *
   * Sets these members:
   * numPartitionsWithNumRows_, partitionNumRows_, hasCorruptTableStats_.
   */
  private long getStatsNumRows(TQueryOptions queryOptions) {
    numPartitionsWithNumRows_ = 0;
    partitionNumRows_ = -1;
    hasCorruptTableStats_ = false;
    if (tbl_.getNumClusteringCols() > 0) {
      for (FeFsPartition p: partitions_) {
        // Check for corrupt partition stats
        long partNumRows = p.getNumRows();
        if (partNumRows < -1  || (partNumRows == 0 && p.getSize() > 0))  {
          hasCorruptTableStats_ = true;
        }
        // Ignore partitions with missing stats in the hope they don't matter
        // enough to change the planning outcome.
        if (partNumRows > -1) {
          if (partitionNumRows_ == -1) partitionNumRows_ = 0;
          partitionNumRows_ = checkedAdd(partitionNumRows_, partNumRows);
          ++numPartitionsWithNumRows_;
        }
      }
      if (numPartitionsWithNumRows_ > 0) return partitionNumRows_;
    }
    // Table is unpartitioned or the table is partitioned but no partitions have stats.
    // Set cardinality based on table-level stats.
    long numRows = tbl_.getNumRows();
    // Depending on the query option of disable_hdfs_num_rows_est, if numRows
    // is still not available, we provide a crude estimation by computing
    // sumAvgRowSizes, the sum of the slot size of each column of scalar type,
    // and then generate the estimate using sumValues(totalBytesPerFs_), the size of
    // the hdfs table.
    if (!queryOptions.disable_hdfs_num_rows_estimate && numRows == -1L) {
      // Compute the estimated table size when taking compression into consideration
      long estimatedTableSize = computeEstimatedTableSize();

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

      if (sumAvgRowSizes == 0.0) {
        // When the type of each Column is of ArrayType or MapType,
        // sumAvgRowSizes would be equal to 0. In this case, we use a ultimate
        // fallback row width if sumAvgRowSizes == 0.0.
        numRows = Math.round(estimatedTableSize / DEFAULT_ROW_WIDTH_ESTIMATE);
      } else {
        numRows = Math.round(estimatedTableSize / sumAvgRowSizes);
      }
    }
    if (numRows < -1 || (numRows == 0 && tbl_.getTotalHdfsBytes() > 0)) {
      hasCorruptTableStats_ = true;
    }
    return numRows;
  }

  /** Compute the estimated table size when taking compression into consideration */
  private long computeEstimatedTableSize() {
    long estimatedTableSize = 0;
    for (FeFsPartition p: partitions_) {
      HdfsFileFormat format = p.getFileFormat();
      long estimatedPartitionSize = 0;
      if (format == HdfsFileFormat.TEXT) {
        for (FileDescriptor desc : p.getFileDescriptors()) {
          HdfsCompression compression
            = HdfsCompression.fromFileName(desc.getRelativePath().toString());
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
             "Unknown HDFS compressed format: %s", this);
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
    Set<TNetworkAddress> localHostSet = new HashSet<>();
    int totalNodes = 0;
    int numLocalRanges = 0;
    int numRemoteRanges = 0;
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
        totalNodes = Math.min(
            scanRangeSpecs_.concrete_ranges.size(), dummyHostIndex.size());
        LOG.info(String.format("Planner running in DEBUG mode. ScanNode: %s, " +
            "TotalNodes %d, Local Ranges %d", tbl_.getFullName(), totalNodes,
            numLocalRanges));
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
                localHostSet.add(dataNode);
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
          int numLocalNodes = Math.min(numLocalRanges, localHostSet.size());
          // The remote ranges are round-robined across all the impalads.
          int numRemoteNodes = Math.min(numRemoteRanges, cluster.numExecutors());
          // The local and remote assignments may overlap, but we don't know by how much
          // so conservatively assume no overlap.
          totalNodes = Math.min(numLocalNodes + numRemoteNodes, cluster.numExecutors());
          // Exit early if all hosts have a scan range assignment, to avoid extraneous
          // work in case the number of scan ranges dominates the number of nodes.
          if (totalNodes == cluster.numExecutors()) break;
        }
      }
    }
    // Handle the generated range specifications.
    if (totalNodes < cluster.numExecutors() && scanRangeSpecs_.isSetSplit_specs()) {
      Preconditions.checkState(
          generatedScanRangeCount_ >= scanRangeSpecs_.getSplit_specsSize());
      numRemoteRanges += generatedScanRangeCount_;
      totalNodes = Math.min(numRemoteRanges, cluster.numExecutors());
    }
    // Tables can reside on 0 nodes (empty table), but a plan node must always be
    // executed on at least one node.
    numNodes_ = (cardinality == 0 || totalNodes == 0) ? 1 : totalNodes;
    if (LOG.isTraceEnabled()) {
      LOG.trace("computeNumNodes totalRanges="
          + (scanRangeSpecs_.getConcrete_rangesSize() + generatedScanRangeCount_)
          + " localRanges=" + numLocalRanges + " remoteRanges=" + numRemoteRanges
          + " localHostSet.size=" + localHostSet.size()
          + " executorNodes=" + cluster.numExecutors());
    }
  }

  @Override
  protected void toThrift(TPlanNode msg) {
    msg.hdfs_scan_node = new THdfsScanNode(desc_.getId().asInt());
    if (replicaPreference_ != null) {
      msg.hdfs_scan_node.setReplica_preference(replicaPreference_);
    }
    msg.hdfs_scan_node.setRandom_replica(randomReplica_);
    msg.node_type = TPlanNodeType.HDFS_SCAN_NODE;
    if (!collectionConjuncts_.isEmpty()) {
      Map<Integer, List<TExpr>> tcollectionConjuncts = new LinkedHashMap<>();
      for (Map.Entry<TupleDescriptor, List<Expr>> entry:
        collectionConjuncts_.entrySet()) {
        tcollectionConjuncts.put(entry.getKey().getId().asInt(),
            Expr.treesToThrift(entry.getValue()));
      }
      msg.hdfs_scan_node.setCollection_conjuncts(tcollectionConjuncts);
    }
    if (skipHeaderLineCount_ > 0) {
      msg.hdfs_scan_node.setSkip_header_line_count(skipHeaderLineCount_);
    }
    msg.hdfs_scan_node.setUse_mt_scan_node(useMtScanNode_);
    Preconditions.checkState((optimizedAggSmap_ == null) == (countStarSlot_ == null));
    if (countStarSlot_ != null) {
      msg.hdfs_scan_node.setParquet_count_star_slot_offset(
          countStarSlot_.getByteOffset());
    }
    if (!minMaxConjuncts_.isEmpty()) {
      for (Expr e: minMaxConjuncts_) {
        msg.hdfs_scan_node.addToMin_max_conjuncts(e.treeToThrift());
      }
      msg.hdfs_scan_node.setMin_max_tuple_id(minMaxTuple_.getId().asInt());
    }
    Map<Integer, List<Integer>> dictMap = new LinkedHashMap<>();
    for (Map.Entry<SlotDescriptor, List<Integer>> entry :
      dictionaryFilterConjuncts_.entrySet()) {
      dictMap.put(entry.getKey().getId().asInt(), entry.getValue());
    }
    msg.hdfs_scan_node.setDictionary_filter_conjuncts(dictMap);
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
              getExplainString(partitionConjuncts_, detailLevel)));
      }
      String partMetaTemplate = "partitions=%d/%d files=%d size=%s\n";
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
        }
      } else if (tbl_.getNumClusteringCols() == 0) {
        // There are no partitions so we use the FsType of the base table
        output.append(detailPrefix);
        output.append(table.getFsType()).append(" ");
        output.append(String.format(partMetaTemplate, 1, table.getPartitions().size(),
            0, PrintUtils.printBytes(0)));
      } else {
        // The table is partitioned, but no partitions are selected; in this case we
        // exclude the FsType completely
        output.append(detailPrefix);
        output.append(String.format(partMetaTemplate, 0, table.getPartitions().size(),
            0, PrintUtils.printBytes(0)));
      }

      if (!conjuncts_.isEmpty()) {
        output.append(detailPrefix)
          .append(String.format("predicates: %s\n",
            getExplainString(conjuncts_, detailLevel)));
      }
      if (!collectionConjuncts_.isEmpty()) {
        for (Map.Entry<TupleDescriptor, List<Expr>> entry:
          collectionConjuncts_.entrySet()) {
          String alias = entry.getKey().getAlias();
          output.append(detailPrefix)
            .append(String.format("predicates on %s: %s\n", alias,
              getExplainString(entry.getValue(), detailLevel)));
        }
      }
      if (!runtimeFilters_.isEmpty()) {
        output.append(detailPrefix + "runtime filters: ");
        output.append(getRuntimeFilterExplainString(false, detailLevel));
      }
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
            .append(PrintUtils.printEstCardinality(maxScanRangeNumRows_))
            .append("\n");
      if (numScanRangesNoDiskIds_ > 0) {
        output.append(detailPrefix)
          .append(String.format("missing disk ids: "
                + "partitions=%s/%s files=%s/%s scan ranges %s/%s\n",
            numPartitionsNoDiskIds_, sumValues(numPartitionsPerFs_),
            numFilesNoDiskIds_, sumValues(totalFilesPerFs_), numScanRangesNoDiskIds_,
            scanRangeSpecs_.getConcrete_rangesSize() + generatedScanRangeCount_));
      }
      // Groups the min max original conjuncts by tuple descriptor.
      output.append(getMinMaxOriginalConjunctsExplainString(detailPrefix, detailLevel));
      // Groups the dictionary filterable conjuncts by tuple descriptor.
      output.append(getDictionaryConjunctsExplainString(detailPrefix, detailLevel));
    }
    return output.toString();
  }

  // Helper method that prints min max original conjuncts by tuple descriptor.
  private String getMinMaxOriginalConjunctsExplainString(
      String prefix, TExplainLevel detailLevel) {
    StringBuilder output = new StringBuilder();
    for (Map.Entry<TupleDescriptor, List<Expr>> entry :
        minMaxOriginalConjuncts_.entrySet()) {
      TupleDescriptor tupleDesc = entry.getKey();
      List<Expr> exprs = entry.getValue();
      if (tupleDesc == getTupleDesc()) {
        output.append(prefix)
        .append(String.format("parquet statistics predicates: %s\n",
            getExplainString(exprs, detailLevel)));
      } else {
        output.append(prefix)
        .append(String.format("parquet statistics predicates on %s: %s\n",
            tupleDesc.getAlias(), getExplainString(exprs, detailLevel)));
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
          tupleName, getExplainString(exprList, detailLevel)));
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
  public void computeNodeResourceProfile(TQueryOptions queryOptions) {
    Preconditions.checkNotNull(scanRangeSpecs_, "Cost estimation requires scan ranges.");
    long scanRangeSize =
        scanRangeSpecs_.getConcrete_rangesSize() + generatedScanRangeCount_;
    if (scanRangeSize == 0) {
      nodeResourceProfile_ = ResourceProfile.noReservation(0);
      return;
    }

    Preconditions.checkState(0 < numNodes_ && numNodes_ <= scanRangeSize);
    Preconditions.checkNotNull(desc_);
    Preconditions.checkState(desc_.getTable() instanceof FeFsTable);
    List<Long> columnReservations = null;
    if (fileFormats_.contains(HdfsFileFormat.PARQUET)
        || fileFormats_.contains(HdfsFileFormat.ORC)) {
      columnReservations = computeMinColumnMemReservations();
    }

    int perHostScanRanges = 0;
    for (HdfsFileFormat format : fileFormats_) {
      int partitionScanRange = 0;
      if ((format == HdfsFileFormat.PARQUET) || (format == HdfsFileFormat.ORC)) {
        Preconditions.checkNotNull(columnReservations);
        // For the purpose of this estimation, the number of per-host scan ranges for
        // Parquet/ORC files are equal to the number of columns read from the file. I.e.
        // excluding partition columns and columns that are populated from file metadata.
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
        .setMinMemReservationBytes(computeMinMemReservation(columnReservations))
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
  private long computeMinMemReservation(List<Long> columnReservations) {
    Preconditions.checkState(largestScanRangeBytes_ >= 0);
    long maxIoBufferSize =
        BitUtil.roundUpToPowerOf2(BackendConfig.INSTANCE.getReadSize());
    long reservationBytes = 0;
    for (HdfsFileFormat format: fileFormats_) {
      long formatReservationBytes = 0;
      // TODO: IMPALA-6875 - ORC should compute total reservation across columns once the
      // ORC scanner supports reservations. For now it is treated the same as a
      // row-oriented format because there is no per-column reservation.
      if (format == HdfsFileFormat.PARQUET) {
        // With Parquet, we first read the footer then all of the materialized columns in
        // parallel.
        for (long columnReservation : columnReservations) {
          formatReservationBytes += columnReservation;
        }
        formatReservationBytes = Math.max(FOOTER_SIZE, formatReservationBytes);
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

   * If there are nested collections, returns a size for each of the leaf scalar slots
   * per collection. This matches Parquet's "shredded" approach to nested collections,
   * where each nested field is stored as a separate column. We may need to adjust this
   * logic for nested types in non-shredded columnar formats (e.g. IMPALA-6503 - ORC)
   * if/when that is added.
   */
  private List<Long> computeMinColumnMemReservations() {
    List<Long> columnByteSizes = new ArrayList<>();
    FeFsTable table = (FeFsTable) desc_.getTable();
    boolean havePosSlot = false;
    for (SlotDescriptor slot: desc_.getSlots()) {
      if (!slot.isMaterialized() || slot == countStarSlot_) continue;
      if (slot.getColumn() == null ||
          slot.getColumn().getPosition() >= table.getNumClusteringCols()) {
        if (slot.isArrayPosRef()) {
          // Position virtual slots can be materialized by piggybacking on another slot.
          havePosSlot = true;
        } else if (slot.getType().isScalarType()) {
          Column column = slot.getColumn();
          if (column == null) {
            // Not a top-level column, e.g. a value from a nested collection that is
            // being unnested by the scanner. No stats are available for nested
            // collections.
            columnByteSizes.add(DEFAULT_COLUMN_SCAN_RANGE_RESERVATION);
          } else {
            columnByteSizes.add(computeMinScalarColumnMemReservation(column));
          }
        } else {
          appendMinColumnMemReservationsForCollection(slot, columnByteSizes);
        }
      }
    }
    if (havePosSlot && columnByteSizes.isEmpty()) {
      // Must scan something to materialize a position slot. We don't know anything about
      // the column that we're scanning so use the default reservation.
      columnByteSizes.add(DEFAULT_COLUMN_SCAN_RANGE_RESERVATION);
    }
    return columnByteSizes;
  }

  /**
   * Helper for computeMinColumnMemReservations() - compute minimum memory reservations
   * for all of the scalar columns read from disk when materializing collectionSlot.
   * Appends one number per scalar column to columnMemReservations.
   */
  private void appendMinColumnMemReservationsForCollection(SlotDescriptor collectionSlot,
      List<Long> columnMemReservations) {
    Preconditions.checkState(collectionSlot.getType().isCollectionType());
    boolean addedColumn = false;
    for (SlotDescriptor nestedSlot: collectionSlot.getItemTupleDesc().getSlots()) {
      // Position virtual slots can be materialized by piggybacking on another slot.
      if (!nestedSlot.isMaterialized() || nestedSlot.isArrayPosRef()) continue;
      if (nestedSlot.getType().isScalarType()) {
        // No column stats are available for nested collections so use the default
        // reservation.
        columnMemReservations.add(DEFAULT_COLUMN_SCAN_RANGE_RESERVATION);
        addedColumn = true;
      } else {
        appendMinColumnMemReservationsForCollection(nestedSlot, columnMemReservations);
      }
    }
    // Need to scan at least one column to materialize the pos virtual slot and/or
    // determine the size of the nested array. Assume it is the size of a single I/O
    // buffer.
    if (!addedColumn) columnMemReservations.add(DEFAULT_COLUMN_SCAN_RANGE_RESERVATION);
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

  public ExprSubstitutionMap getOptimizedAggSmap() { return optimizedAggSmap_; }

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

  /**
   * Returns of all the values in the given {@link Map}.
   */
  private static long sumValues(Map<?, Long> input) {
    return input.values().stream().mapToLong(Long::longValue).sum();
  }
}
