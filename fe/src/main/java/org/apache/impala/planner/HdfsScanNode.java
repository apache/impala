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
import java.util.List;
import java.util.Map;
import java.util.Set;

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
import org.apache.impala.analysis.NullLiteral;
import org.apache.impala.analysis.SlotDescriptor;
import org.apache.impala.analysis.SlotId;
import org.apache.impala.analysis.SlotRef;
import org.apache.impala.analysis.TableRef;
import org.apache.impala.analysis.TableSampleClause;
import org.apache.impala.analysis.TupleDescriptor;
import org.apache.impala.analysis.TupleId;
import org.apache.impala.catalog.Column;
import org.apache.impala.catalog.HdfsFileFormat;
import org.apache.impala.catalog.HdfsPartition;
import org.apache.impala.catalog.HdfsPartition.FileBlock;
import org.apache.impala.catalog.HdfsPartition.FileDescriptor;
import org.apache.impala.catalog.HdfsTable;
import org.apache.impala.catalog.Type;
import org.apache.impala.common.FileSystemUtil;
import org.apache.impala.common.ImpalaException;
import org.apache.impala.common.ImpalaRuntimeException;
import org.apache.impala.common.InternalException;
import org.apache.impala.common.NotImplementedException;
import org.apache.impala.common.PrintUtils;
import org.apache.impala.common.RuntimeEnv;
import org.apache.impala.fb.FbFileBlock;
import org.apache.impala.service.BackendConfig;
import org.apache.impala.thrift.TExplainLevel;
import org.apache.impala.thrift.TExpr;
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
import org.apache.impala.thrift.TTableStats;
import org.apache.impala.util.MembershipSnapshot;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Joiner;
import com.google.common.base.Objects;
import com.google.common.base.Objects.ToStringHelper;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
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

  // Factor capturing the worst-case deviation from a uniform distribution of scan ranges
  // among nodes. The factor of 1.2 means that a particular node may have 20% more
  // scan ranges than would have been estimated assuming a uniform distribution.
  private static final double SCAN_RANGE_SKEW_FACTOR = 1.2;

  // The minimum amount of memory we estimate a scan will use. The number is
  // derived experimentally: running metadata-only Parquet count(*) scans on TPC-H
  // lineitem and TPC-DS store_sales of different sizes resulted in memory consumption
  // between 128kb and 1.1mb.
  private final static long MIN_MEMORY_ESTIMATE = 1 * 1024 * 1024;

  private final HdfsTable tbl_;

  // List of partitions to be scanned. Partitions have been pruned.
  private final List<HdfsPartition> partitions_;

  // Parameters for table sampling. Null if not sampling.
  private final TableSampleClause sampleParams_;

  private final TReplicaPreference replicaPreference_;
  private final boolean randomReplica_;

  // The AggregationInfo from the query block of this scan node. Used for determining if
  // the Parquet count(*) optimization can be applied.
  private final AggregateInfo aggInfo_;

  // Number of partitions, files and bytes scanned. Set in computeScanRangeLocations().
  // Might not match 'partitions_' due to table sampling.
  private int numPartitions_ = 0;
  private long totalFiles_ = 0;
  private long totalBytes_ = 0;

  // Input cardinality based on the partition row counts or extrapolation. -1 if invalid.
  // Both values can be valid to report them in the explain plan, but only one of them is
  // used for determining the scan cardinality.
  private long partitionNumRows_ = -1;
  private long extrapolatedNumRows_ = -1;

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
      Maps.newLinkedHashMap();

  // TupleDescriptors of collection slots that have an IsNotEmptyPredicate. See
  // SelectStmt#registerIsNotEmptyPredicates.
  // Correctness for applying min-max and dictionary filters requires that the nested
  // collection is tested to be not empty (via the IsNotEmptyPredicate).
  // These filters are added by analysis (see: SelectStmt#registerIsNotEmptyPredicates).
  // While correct, they may be conservative. See the tests for parquet collection
  // filtering for examples that could benefit from being more aggressive
  // (yet still correct).
  private final Set<TupleDescriptor> notEmptyCollections_ = Sets.newHashSet();

  // Map from SlotDescriptor to indices in PlanNodes.conjuncts_ and
  // collectionConjuncts_ that are eligible for dictionary filtering. Slots in the
  // the TupleDescriptor of this scan node map to indices into PlanNodes.conjuncts_ and
  // slots in the TupleDescriptors of nested types map to indices into
  // collectionConjuncts_.
  private final Map<SlotDescriptor, List<Integer>> dictionaryFilterConjuncts_ =
      Maps.newLinkedHashMap();

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
  private final List<Expr> minMaxConjuncts_ = Lists.newArrayList();

  // Map from TupleDescriptor to list of PlanNode conjuncts that have been transformed
  // into conjuncts in 'minMaxConjuncts_'.
  private final Map<TupleDescriptor, List<Expr>> minMaxOriginalConjuncts_ =
      Maps.newLinkedHashMap();

  // Tuple that is used to materialize statistics when scanning Parquet files. For each
  // column it can contain 0, 1, or 2 slots, depending on whether the column needs to be
  // evaluated against the min and/or the max value of the corresponding
  // parquet::Statistics.
  private TupleDescriptor minMaxTuple_;

  // Slot that is used to record the Parquet metatdata for the count(*) aggregation if
  // this scan node has the count(*) optimization enabled.
  private SlotDescriptor countStarSlot_ = null;

  /**
   * Construct a node to scan given data files into tuples described by 'desc',
   * with 'conjuncts' being the unevaluated conjuncts bound by the tuple and
   * 'partitions' being the partitions which need to be included. Please see
   * class comments above for details.
   */
  public HdfsScanNode(PlanNodeId id, TupleDescriptor desc, List<Expr> conjuncts,
      List<HdfsPartition> partitions, TableRef hdfsTblRef, AggregateInfo aggInfo) {
    super(id, desc, "SCAN HDFS");
    Preconditions.checkState(desc.getTable() instanceof HdfsTable);
    tbl_ = (HdfsTable)desc.getTable();
    conjuncts_ = conjuncts;
    partitions_ = partitions;
    sampleParams_ = hdfsTblRef.getSampleParams();
    replicaPreference_ = hdfsTblRef.getReplicaPreference();
    randomReplica_ = hdfsTblRef.getRandomReplica();
    HdfsTable hdfsTable = (HdfsTable)hdfsTblRef.getTable();
    Preconditions.checkState(tbl_ == hdfsTable);
    StringBuilder error = new StringBuilder();
    aggInfo_ = aggInfo;
    skipHeaderLineCount_ = tbl_.parseSkipHeaderLineCount(error);
    if (error.length() > 0) {
      // Any errors should already have been caught during analysis.
      throw new IllegalStateException(error.toString());
    }
  }

  @Override
  protected String debugString() {
    ToStringHelper helper = Objects.toStringHelper(this);
    for (HdfsPartition partition: partitions_) {
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
    ArrayList<Expr> args = Lists.newArrayList();
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
    computeDictionaryFilterConjuncts(analyzer);

    // compute scan range locations with optional sampling
    Set<HdfsFileFormat> fileFormats = computeScanRangeLocations(analyzer);

    // Determine backend scan node implementation to use. The optimized MT implementation
    // is currently only supported for Parquet.
    if (analyzer.getQueryOptions().isSetMt_dop() &&
        analyzer.getQueryOptions().mt_dop > 0 &&
        fileFormats.size() == 1 &&
        (fileFormats.contains(HdfsFileFormat.PARQUET)
          || fileFormats.contains(HdfsFileFormat.TEXT))) {
      useMtScanNode_ = true;
    } else {
      useMtScanNode_ = false;
    }

    if (fileFormats.contains(HdfsFileFormat.PARQUET)) {
      computeMinMaxTupleAndConjuncts(analyzer);
    }

    if (canApplyParquetCountStarOptimization(analyzer, fileFormats)) {
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
   * Throws if the table schema contains a complex type and we need to scan
   * a partition that has a format for which we do not support complex types,
   * regardless of whether a complex-typed column is actually referenced
   * in the query.
   */
  @Override
  protected void checkForSupportedFileFormats() throws NotImplementedException {
    Preconditions.checkNotNull(desc_);
    Preconditions.checkNotNull(desc_.getTable());
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

    for (HdfsPartition part: partitions_) {
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
    if (slotRef.isArrayPosRef()) return;

    Expr constExpr = binaryPred.getChild(1);
    // Only constant exprs can be evaluated against parquet::Statistics. This includes
    // LiteralExpr, but can also be an expr like "1 + 2".
    if (!constExpr.isConstant()) return;
    if (constExpr.isNullLiteral()) return;

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
    if (slotRef.isArrayPosRef()) return;
    if (inPred.isNotIn()) return;

    ArrayList<Expr> children = inPred.getChildren();
    LiteralExpr min = null;
    LiteralExpr max = null;
    for (int i = 1; i < children.size(); ++i) {
      Expr child = children.get(i);

      // If any child is not a literal, then nothing can be done
      if (!child.isLiteral()) return;
      LiteralExpr literalChild = (LiteralExpr) child;
      // If any child is NULL, then there is not a valid min/max. Nothing can be done.
      if (literalChild instanceof NullLiteral) return;

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
      exprs = new ArrayList<Expr>();
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
      ArrayList<Expr> bindingPredicates = analyzer.getBoundPredicates(itemTid);
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
    List<TupleId> tupleIds = Lists.newArrayList();
    List<SlotId> slotIds = Lists.newArrayList();
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
      slotList = Lists.newArrayList();
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
   * Computes scan ranges (hdfs splits) plus their storage locations, including volume
   * ids, based on the given maximum number of bytes each scan range should scan.
   * If 'sampleParams_' is not null, generates a sample and computes the scan ranges
   * based on the sample.
   * Returns the set of file formats being scanned.
   */
  private Set<HdfsFileFormat> computeScanRangeLocations(Analyzer analyzer)
      throws ImpalaRuntimeException {
    Map<Long, List<FileDescriptor>> sampledFiles = null;
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
      sampledFiles = tbl_.getFilesSample(partitions_, percentBytes, 0, randomSeed);
    }

    long maxScanRangeLength = analyzer.getQueryCtx().client_request.getQuery_options()
        .getMax_scan_range_length();
    scanRanges_ = Lists.newArrayList();
    numPartitions_ = (sampledFiles != null) ? sampledFiles.size() : partitions_.size();
    totalFiles_ = 0;
    totalBytes_ = 0;
    Set<HdfsFileFormat> fileFormats = Sets.newHashSet();
    for (HdfsPartition partition: partitions_) {
      List<FileDescriptor> fileDescs = partition.getFileDescriptors();
      if (sampledFiles != null) {
        // If we are sampling, check whether this partition is included in the sample.
        fileDescs = sampledFiles.get(Long.valueOf(partition.getId()));
        if (fileDescs == null) continue;
      }

      analyzer.getDescTbl().addReferencedPartition(tbl_, partition.getId());
      fileFormats.add(partition.getFileFormat());
      Preconditions.checkState(partition.getId() >= 0);
      // Missing disk id accounting is only done for file systems that support the notion
      // of disk/storage ids.
      FileSystem partitionFs;
      try {
        partitionFs = partition.getLocationPath().getFileSystem(CONF);
      } catch (IOException e) {
        throw new ImpalaRuntimeException("Error determining partition fs type", e);
      }
      boolean checkMissingDiskIds = FileSystemUtil.supportsStorageIds(partitionFs);
      boolean partitionMissingDiskIds = false;

      totalFiles_ += fileDescs.size();
      for (FileDescriptor fileDesc: fileDescs) {
        totalBytes_ += fileDesc.getFileLength();
        boolean fileDescMissingDiskIds = false;
        for (int j = 0; j < fileDesc.getNumFileBlocks(); ++j) {
          FbFileBlock block = fileDesc.getFbFileBlock(j);
          int replicaHostCount = FileBlock.getNumReplicaHosts(block);
          if (replicaHostCount == 0) {
            // we didn't get locations for this block; for now, just ignore the block
            // TODO: do something meaningful with that
            continue;
          }
          // Collect the network address and volume ID of all replicas of this block.
          List<TScanRangeLocation> locations = Lists.newArrayList();
          for (int i = 0; i < replicaHostCount; ++i) {
            TScanRangeLocation location = new TScanRangeLocation();
            // Translate from the host index (local to the HdfsTable) to network address.
            int replicaHostIdx = FileBlock.getReplicaHostIdx(block, i);
            TNetworkAddress networkAddress =
                partition.getTable().getHostIndex().getEntry(replicaHostIdx);
            Preconditions.checkNotNull(networkAddress);
            // Translate from network address to the global (to this request) host index.
            Integer globalHostIdx = analyzer.getHostIndex().getIndex(networkAddress);
            location.setHost_idx(globalHostIdx);
            if (checkMissingDiskIds && FileBlock.getDiskId(block, i) == -1) {
              ++numScanRangesNoDiskIds_;
              partitionMissingDiskIds = true;
              fileDescMissingDiskIds = true;
            }
            location.setVolume_id(FileBlock.getDiskId(block, i));
            location.setIs_cached(FileBlock.isReplicaCached(block, i));
            locations.add(location);
          }
          // create scan ranges, taking into account maxScanRangeLength
          long currentOffset = FileBlock.getOffset(block);
          long remainingLength = FileBlock.getLength(block);
          while (remainingLength > 0) {
            long currentLength = remainingLength;
            if (maxScanRangeLength > 0 && remainingLength > maxScanRangeLength) {
              currentLength = maxScanRangeLength;
            }
            TScanRange scanRange = new TScanRange();
            scanRange.setHdfs_file_split(new THdfsFileSplit(fileDesc.getFileName(),
                currentOffset, currentLength, partition.getId(), fileDesc.getFileLength(),
                fileDesc.getFileCompression().toThrift(),
                fileDesc.getModificationTime()));
            TScanRangeLocationList scanRangeLocations = new TScanRangeLocationList();
            scanRangeLocations.scan_range = scanRange;
            scanRangeLocations.locations = locations;
            scanRanges_.add(scanRangeLocations);
            remainingLength -= currentLength;
            currentOffset += currentLength;
          }
        }
        if (fileDescMissingDiskIds) {
          ++numFilesNoDiskIds_;
          if (LOG.isTraceEnabled()) {
            LOG.trace("File blocks mapping to unknown disk ids. Dir: " +
                partition.getLocation() + " File:" + fileDesc.toString());
          }
        }
      }
      if (partitionMissingDiskIds) ++numPartitionsNoDiskIds_;
    }
    return fileFormats;
  }

  /**
   * Computes the average row size, input and output cardinalities, and estimates the
   * number of nodes.
   * Requires that computeScanRangeLocations() has been called.
   */
  @Override
  public void computeStats(Analyzer analyzer) {
    Preconditions.checkNotNull(scanRanges_);
    super.computeStats(analyzer);
    computeCardinalities();
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
  private void computeCardinalities() {
    // Choose between the extrapolated row count and the one based on stored stats.
    extrapolatedNumRows_ = tbl_.getExtrapolatedNumRows(totalBytes_);
    long statsNumRows = getStatsNumRows();
    if (extrapolatedNumRows_ != -1) {
      // The extrapolated row count is based on the 'totalBytes_' which already accounts
      // for table sampling, so no additional adjustment for sampling is necessary.
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
    if (totalBytes_ == 0) {
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
    cardinality_ = capAtLimit(cardinality_);
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
  private long getStatsNumRows() {
    numPartitionsWithNumRows_ = 0;
    partitionNumRows_ = -1;
    hasCorruptTableStats_ = false;
    if (tbl_.getNumClusteringCols() > 0) {
      for (HdfsPartition p: partitions_) {
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
    if (numRows < -1 || (numRows == 0 && tbl_.getTotalHdfsBytes() > 0)) {
      hasCorruptTableStats_ = true;
    }
    return numRows;
  }

  /**
   * Estimate the number of impalad nodes that this scan node will execute on (which is
   * ultimately determined by the scheduling done by the backend's Scheduler).
   * Assume that scan ranges that can be scheduled locally will be, and that scan
   * ranges that cannot will be round-robined across the cluster.
   */
  protected void computeNumNodes(Analyzer analyzer, long cardinality) {
    Preconditions.checkNotNull(scanRanges_);
    MembershipSnapshot cluster = MembershipSnapshot.getCluster();
    HashSet<TNetworkAddress> localHostSet = Sets.newHashSet();
    int totalNodes = 0;
    int numLocalRanges = 0;
    int numRemoteRanges = 0;
    for (TScanRangeLocationList range: scanRanges_) {
      boolean anyLocal = false;
      for (TScanRangeLocation loc: range.locations) {
        TNetworkAddress dataNode = analyzer.getHostIndex().getEntry(loc.getHost_idx());
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
      int numRemoteNodes = Math.min(numRemoteRanges, cluster.numNodes());
      // The local and remote assignments may overlap, but we don't know by how much so
      // conservatively assume no overlap.
      totalNodes = Math.min(numLocalNodes + numRemoteNodes, cluster.numNodes());
      // Exit early if all hosts have a scan range assignment, to avoid extraneous work
      // in case the number of scan ranges dominates the number of nodes.
      if (totalNodes == cluster.numNodes()) break;
    }
    // Tables can reside on 0 nodes (empty table), but a plan node must always be
    // executed on at least one node.
    numNodes_ = (cardinality == 0 || totalNodes == 0) ? 1 : totalNodes;
    if (LOG.isTraceEnabled()) {
      LOG.trace("computeNumNodes totalRanges=" + scanRanges_.size() +
          " localRanges=" + numLocalRanges + " remoteRanges=" + numRemoteRanges +
          " localHostSet.size=" + localHostSet.size() +
          " clusterNodes=" + cluster.numNodes());
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
      Map<Integer, List<TExpr>> tcollectionConjuncts = Maps.newLinkedHashMap();
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
    Map<Integer, List<Integer>> dictMap = Maps.newLinkedHashMap();
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
    HdfsTable table = (HdfsTable) desc_.getTable();
    output.append(String.format("%s%s [%s", prefix, getDisplayLabel(),
        getDisplayLabelDetail()));
    if (detailLevel.ordinal() >= TExplainLevel.EXTENDED.ordinal() &&
        fragment_.isPartitioned()) {
      output.append(", " + fragment_.getDataPartition().getExplainString());
    }
    output.append("]\n");
    if (detailLevel.ordinal() >= TExplainLevel.STANDARD.ordinal()) {
      if (tbl_.getNumClusteringCols() == 0) numPartitions_ = 1;
      output.append(String.format("%spartitions=%s/%s files=%s size=%s", detailPrefix,
          numPartitions_, table.getPartitions().size() - 1, totalFiles_,
          PrintUtils.printBytes(totalBytes_)));
      output.append("\n");
      if (!conjuncts_.isEmpty()) {
        output.append(String.format("%spredicates: %s\n", detailPrefix,
            getExplainString(conjuncts_)));
      }
      if (!collectionConjuncts_.isEmpty()) {
        for (Map.Entry<TupleDescriptor, List<Expr>> entry:
          collectionConjuncts_.entrySet()) {
          String alias = entry.getKey().getAlias();
          output.append(String.format("%spredicates on %s: %s\n",
              detailPrefix, alias, getExplainString(entry.getValue())));
        }
      }
      if (!runtimeFilters_.isEmpty()) {
        output.append(detailPrefix + "runtime filters: ");
        output.append(getRuntimeFilterExplainString(false, detailLevel));
      }
    }
    if (detailLevel.ordinal() >= TExplainLevel.EXTENDED.ordinal()) {
      output.append(getStatsExplainString(detailPrefix));
      output.append("\n");
      String extrapRows = String.valueOf(extrapolatedNumRows_);
      if (!tbl_.isStatsExtrapolationEnabled()) {
        extrapRows = "disabled";
      } else if (extrapolatedNumRows_ == -1) {
        extrapRows = "unavailable";
      }
      output.append(String.format("%sextrapolated-rows=%s", detailPrefix, extrapRows));
      output.append("\n");
      if (numScanRangesNoDiskIds_ > 0) {
        output.append(String.format("%smissing disk ids: " +
            "partitions=%s/%s files=%s/%s scan ranges %s/%s\n", detailPrefix,
            numPartitionsNoDiskIds_, numPartitions_, numFilesNoDiskIds_,
            totalFiles_, numScanRangesNoDiskIds_, scanRanges_.size()));
      }
      // Groups the min max original conjuncts by tuple descriptor.
      output.append(getMinMaxOriginalConjunctsExplainString(detailPrefix));
      // Groups the dictionary filterable conjuncts by tuple descriptor.
      output.append(getDictionaryConjunctsExplainString(detailPrefix));
    }
    return output.toString();
  }

  // Helper method that prints min max original conjuncts by tuple descriptor.
  private String getMinMaxOriginalConjunctsExplainString(String prefix) {
    StringBuilder output = new StringBuilder();
    for (Map.Entry<TupleDescriptor, List<Expr>> entry :
        minMaxOriginalConjuncts_.entrySet()) {
      TupleDescriptor tupleDesc = entry.getKey();
      List<Expr> exprs = entry.getValue();
      if (tupleDesc == getTupleDesc()) {
        output.append(String.format("%sparquet statistics predicates: %s\n", prefix,
            getExplainString(exprs)));
      } else {
        output.append(String.format("%sparquet statistics predicates on %s: %s\n",
            prefix, tupleDesc.getAlias(), getExplainString(exprs)));
      }
    }
    return output.toString();
  }

  // Helper method that prints the dictionary filterable conjuncts by tuple descriptor.
  private String getDictionaryConjunctsExplainString(String prefix) {
    StringBuilder output = new StringBuilder();
    Map<TupleDescriptor, List<Integer>> perTupleConjuncts = Maps.newLinkedHashMap();
    for (Map.Entry<SlotDescriptor, List<Integer>> entry :
      dictionaryFilterConjuncts_.entrySet()) {
      SlotDescriptor slotDescriptor = entry.getKey();
      TupleDescriptor tupleDescriptor = slotDescriptor.getParent();
      List<Integer> indexes = perTupleConjuncts.get(tupleDescriptor);
      if (indexes == null) {
        indexes = Lists.newArrayList();
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
      List<Expr> exprList = Lists.newArrayList();
      for (Integer idx : totalIdxList) {
        Preconditions.checkState(idx.intValue() < conjuncts.size());
        exprList.add(conjuncts.get(idx));
      }
      output.append(String.format("%sparquet dictionary predicates%s: %s\n",
          prefix, tupleName, getExplainString(exprList)));
    }
    return output.toString();
  }

  @Override
  protected String getTableStatsExplainString(String prefix) {
    StringBuilder output = new StringBuilder();
    TTableStats tblStats = desc_.getTable().getTTableStats();
    String numRows = String.valueOf(tblStats.num_rows);
    if (tblStats.num_rows == -1) numRows = "unavailable";
    String totalBytes = PrintUtils.printBytes(tblStats.total_file_bytes);
    if (tblStats.total_file_bytes == -1) totalBytes = "unavailable";
    output.append(String.format("%stable: rows=%s size=%s",
        prefix, numRows, totalBytes));
    if (tbl_.getNumClusteringCols() > 0) {
      output.append("\n");
      String partNumRows = String.valueOf(partitionNumRows_);
      if (partitionNumRows_ == -1) partNumRows = "unavailable";
      output.append(String.format("%spartitions: %s/%s rows=%s",
          prefix, numPartitionsWithNumRows_, partitions_.size(), partNumRows));
    }
    return output.toString();
  }

  @Override
  public void computeNodeResourceProfile(TQueryOptions queryOptions) {
    Preconditions.checkNotNull(scanRanges_, "Cost estimation requires scan ranges.");
    if (scanRanges_.isEmpty()) {
      nodeResourceProfile_ = ResourceProfile.noReservation(0);
      return;
    }
    Preconditions.checkState(0 < numNodes_ && numNodes_ <= scanRanges_.size());
    Preconditions.checkNotNull(desc_);
    Preconditions.checkNotNull(desc_.getTable() instanceof HdfsTable);
    HdfsTable table = (HdfsTable) desc_.getTable();
    int perHostScanRanges;
    if (table.getMajorityFormat() == HdfsFileFormat.PARQUET) {
      // For the purpose of this estimation, the number of per-host scan ranges for
      // Parquet files are equal to the number of columns read from the file. I.e.
      // excluding partition columns and columns that are populated from file metadata.
      perHostScanRanges = 0;
      for (SlotDescriptor slot: desc_.getSlots()) {
        if (!slot.isMaterialized() || slot == countStarSlot_) continue;
        if (slot.getColumn() == null ||
            slot.getColumn().getPosition() >= table.getNumClusteringCols()) {
          ++perHostScanRanges;
        }
      }
    } else {
      perHostScanRanges = (int) Math.ceil((
          (double) scanRanges_.size() / (double) numNodes_) * SCAN_RANGE_SKEW_FACTOR);
    }

    int maxScannerThreads;
    if (queryOptions.getMt_dop() >= 1) {
      maxScannerThreads = 1;
    } else {
      maxScannerThreads = Math.min(perHostScanRanges, RuntimeEnv.INSTANCE.getNumCores());
      // Account for the max scanner threads query option.
      if (queryOptions.isSetNum_scanner_threads() &&
          queryOptions.getNum_scanner_threads() > 0) {
        maxScannerThreads =
            Math.min(maxScannerThreads, queryOptions.getNum_scanner_threads());
      }
    }

    long avgScanRangeBytes = (long) Math.ceil(totalBytes_ / (double) scanRanges_.size());
    // The +1 accounts for an extra I/O buffer to read past the scan range due to a
    // trailing record spanning Hdfs blocks.
    long readSize = BackendConfig.INSTANCE.getReadSize();
    long perThreadIoBuffers =
        Math.min((long) Math.ceil(avgScanRangeBytes / (double) readSize),
            MAX_IO_BUFFERS_PER_THREAD) + 1;
    long perInstanceMemEstimate = checkedMultiply(
        checkedMultiply(maxScannerThreads, perThreadIoBuffers), readSize);

    // Sanity check: the tighter estimation should not exceed the per-host maximum.
    long perHostUpperBound = getPerHostMemUpperBound();
    if (perInstanceMemEstimate > perHostUpperBound) {
      LOG.warn(String.format("Per-instance mem cost %s exceeded per-host upper bound %s.",
          PrintUtils.printBytes(perInstanceMemEstimate),
          PrintUtils.printBytes(perHostUpperBound)));
      perInstanceMemEstimate = perHostUpperBound;
    }
    perInstanceMemEstimate = Math.max(perInstanceMemEstimate, MIN_MEMORY_ESTIMATE);
    nodeResourceProfile_ = ResourceProfile.noReservation(perInstanceMemEstimate);
  }

  /**
   * Hdfs scans use a shared pool of buffers managed by the I/O manager. Intuitively,
   * the maximum number of I/O buffers is limited by the total disk bandwidth of a node.
   * Therefore, this upper bound is independent of the number of concurrent scans and
   * queries and helps to derive a tighter per-host memory estimate for queries with
   * multiple concurrent scans.
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
}
