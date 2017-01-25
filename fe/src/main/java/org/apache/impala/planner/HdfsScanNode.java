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

import org.apache.impala.analysis.Analyzer;
import org.apache.impala.analysis.BinaryPredicate;
import org.apache.impala.analysis.DescriptorTable;
import org.apache.impala.analysis.Expr;
import org.apache.impala.analysis.SlotDescriptor;
import org.apache.impala.analysis.SlotId;
import org.apache.impala.analysis.SlotRef;
import org.apache.impala.analysis.TableRef;
import org.apache.impala.analysis.TupleDescriptor;
import org.apache.impala.analysis.TupleId;
import org.apache.impala.catalog.Column;
import org.apache.impala.catalog.HdfsFileFormat;
import org.apache.impala.catalog.HdfsPartition;
import org.apache.impala.catalog.HdfsPartition.FileBlock;
import org.apache.impala.catalog.HdfsTable;
import org.apache.impala.catalog.Type;
import org.apache.impala.common.FileSystemUtil;
import org.apache.impala.common.ImpalaRuntimeException;
import org.apache.impala.common.ImpalaException;
import org.apache.impala.common.NotImplementedException;
import org.apache.impala.common.PrintUtils;
import org.apache.impala.common.RuntimeEnv;
import org.apache.impala.service.BackendConfig;
import org.apache.impala.thrift.TExplainLevel;
import org.apache.impala.thrift.TExpr;
import org.apache.impala.thrift.THdfsFileBlock;
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
 * Scan of a single table. Currently limited to full-table scans.
 *
 * It's expected that the creator of this object has already done any necessary
 * partition pruning before creating this object. In other words, the 'conjuncts'
 * passed to the constructors are conjuncts not fully evaluated by partition pruning
 * and 'partitions' are the remaining partitions after pruning.
 *
 * For scans of tables with Parquet files the class creates an additional list of
 * conjuncts that are passed to the backend and will be evaluated against the
 * parquet::Statistics of row groups. If the conjuncts don't match, then whole row groups
 * will be skipped.
 *
 * TODO: pass in range restrictions.
 */
public class HdfsScanNode extends ScanNode {
  private final static Logger LOG = LoggerFactory.getLogger(HdfsScanNode.class);

  // Maximum number of I/O buffers per thread executing this scan.
  // TODO: it's unclear how this was chosen - this seems like a very high number
  private final static long MAX_IO_BUFFERS_PER_THREAD = 10;

  // Maximum number of thread tokens per core that may be used to spin up extra scanner
  // threads. Corresponds to the default value of --num_threads_per_core in the backend.
  private final static int MAX_THREAD_TOKENS_PER_CORE = 3;

  // Factor capturing the worst-case deviation from a uniform distribution of scan ranges
  // among nodes. The factor of 1.2 means that a particular node may have 20% more
  // scan ranges than would have been estimated assuming a uniform distribution.
  private final static double SCAN_RANGE_SKEW_FACTOR = 1.2;

  private final HdfsTable tbl_;

  // Partitions that are filtered in for scanning by the key ranges
  private final List<HdfsPartition> partitions_;

  private final TReplicaPreference replicaPreference_;
  private final boolean randomReplica_;

  // Total number of files from partitions_
  private long totalFiles_ = 0;

  // Total number of bytes from partitions_
  private long totalBytes_ = 0;

  // True if this scan node should use the MT implementation in the backend.
  private boolean useMtScanNode_;

  // Conjuncts that can be evaluated while materializing the items (tuples) of
  // collection-typed slots. Maps from tuple descriptor to the conjuncts bound by that
  // tuple. Uses a linked hash map for consistent display in explain.
  private final Map<TupleDescriptor, List<Expr>> collectionConjuncts_ =
      Maps.newLinkedHashMap();

  // Map from SlotIds to indices in PlanNodes.conjuncts_ that are eligible for
  // dictionary filtering
  private Map<Integer, List<Integer>> dictionaryFilterConjuncts_ =
      Maps.newLinkedHashMap();

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

  private static final Configuration CONF = new Configuration();


  // List of conjuncts for min/max values of parquet::Statistics, that are used to skip
  // data when scanning Parquet files.
  private List<Expr> minMaxConjuncts_ = Lists.newArrayList();

  // List of PlanNode conjuncts that have been transformed into conjuncts in
  // 'minMaxConjuncts_'.
  private List<Expr> minMaxOriginalConjuncts_ = Lists.newArrayList();

  // Tuple that is used to materialize statistics when scanning Parquet files. For each
  // column it can contain 0, 1, or 2 slots, depending on whether the column needs to be
  // evaluated against the min and/or the max value of the corresponding
  // parquet::Statistics.
  private TupleDescriptor minMaxTuple_;

  /**
   * Construct a node to scan given data files into tuples described by 'desc',
   * with 'conjuncts' being the unevaluated conjuncts bound by the tuple and
   * 'partitions' being the partitions which need to be included. Please see
   * class comments above for details.
   */
  public HdfsScanNode(PlanNodeId id, TupleDescriptor desc, List<Expr> conjuncts,
      List<HdfsPartition> partitions, TableRef hdfsTblRef) {
    super(id, desc, "SCAN HDFS");
    Preconditions.checkState(desc.getTable() instanceof HdfsTable);
    tbl_ = (HdfsTable)desc.getTable();
    conjuncts_ = conjuncts;
    partitions_ = partitions;
    replicaPreference_ = hdfsTblRef.getReplicaPreference();
    randomReplica_ = hdfsTblRef.getRandomReplica();
    HdfsTable hdfsTable = (HdfsTable)hdfsTblRef.getTable();
    Preconditions.checkState(tbl_ == hdfsTable);
    StringBuilder error = new StringBuilder();
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
   * Populate collectionConjuncts_ and scanRanges_.
   */
  @Override
  public void init(Analyzer analyzer) throws ImpalaException {
    conjuncts_ = orderConjunctsByCost(conjuncts_);
    checkForSupportedFileFormats();

    assignCollectionConjuncts(analyzer);
    computeDictionaryFilterConjuncts(analyzer);
    computeMemLayout(analyzer);

    // compute scan range locations
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

    // do this at the end so it can take all conjuncts and scan ranges into account
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

  public boolean isPartitionedTable() {
    return desc_.getTable().getNumClusteringCols() > 0;
  }

  /**
   * Populates the collection conjuncts, materializes their required slots, and marks
   * the conjuncts as assigned, if it is correct to do so. Some conjuncts may have to
   * also be evaluated at a subsequent semi or outer join.
   */
  private void assignCollectionConjuncts(Analyzer analyzer) {
    collectionConjuncts_.clear();
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

  /**
   * Analyzes 'conjuncts_', populates 'minMaxTuple_' with slots for statistics values, and
   * populates 'minMaxConjuncts_' with conjuncts pointing into the 'minMaxTuple_'. Only
   * conjuncts of the form <slot> <op> <constant> are supported, and <op> must be one of
   * LT, LE, GE, GT, or EQ.
   */
  private void computeMinMaxTupleAndConjuncts(Analyzer analyzer) throws ImpalaException{
    Preconditions.checkNotNull(desc_.getPath());
    String tupleName = desc_.getPath().toString() + " statistics";
    DescriptorTable descTbl = analyzer.getDescTbl();
    minMaxTuple_ = descTbl.createTupleDescriptor(tupleName);
    minMaxTuple_.setPath(desc_.getPath());

    for (Expr pred: conjuncts_) {
      if (!(pred instanceof BinaryPredicate)) continue;
      BinaryPredicate binaryPred = (BinaryPredicate) pred;

      // We only support slot refs on the left hand side of the predicate, a rewriting
      // rule makes sure that all compatible exprs are rewritten into this form. Only
      // implicit casts are supported.
      SlotRef slot = binaryPred.getChild(0).unwrapSlotRef(true);
      if (slot == null) continue;

      // This node is a table scan, so this must be a scanning slot.
      Preconditions.checkState(slot.getDesc().isScanSlot());
      // If the column is null, then this can be a 'pos' scanning slot of a nested type.
      if (slot.getDesc().getColumn() == null) continue;

      Expr constExpr = binaryPred.getChild(1);
      // Only constant exprs can be evaluated against parquet::Statistics. This includes
      // LiteralExpr, but can also be an expr like "1 + 2".
      if (!constExpr.isConstant()) continue;
      if (constExpr.isNullLiteral()) continue;

      BinaryPredicate.Operator op = binaryPred.getOp();
      if (op == BinaryPredicate.Operator.LT || op == BinaryPredicate.Operator.LE ||
          op == BinaryPredicate.Operator.GE || op == BinaryPredicate.Operator.GT) {
        minMaxOriginalConjuncts_.add(pred);
        buildStatsPredicate(analyzer, slot, binaryPred, op);
      } else if (op == BinaryPredicate.Operator.EQ) {
        minMaxOriginalConjuncts_.add(pred);
        // TODO: this could be optimized for boolean columns.
        buildStatsPredicate(analyzer, slot, binaryPred, BinaryPredicate.Operator.LE);
        buildStatsPredicate(analyzer, slot, binaryPred, BinaryPredicate.Operator.GE);
      }

    }
    minMaxTuple_.computeMemLayout();
  }

  /**
   * Recursively collects and assigns conjuncts bound by tuples materialized in a
   * collection-typed slot.
   *
   * Limitation: Conjuncts that must first be migrated into inline views and that cannot
   * be captured by slot binding will not be assigned here, but in an UnnestNode.
   * This limitation applies to conjuncts bound by inline-view slots that are backed by
   * non-SlotRef exprs in the inline-view's select list. We only capture value transfers
   * between slots, and not between arbitrary exprs.
   *
   * TODO for 2.3: The logic for gathering conjuncts and deciding which ones should be
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
      if (!collectionConjuncts.isEmpty()) {
        analyzer.materializeSlots(collectionConjuncts);
        collectionConjuncts_.put(itemTupleDesc, collectionConjuncts);
      }
      // Recursively look for collection-typed slots in nested tuple descriptors.
      assignCollectionConjuncts(itemTupleDesc, analyzer);
    }
  }

  /**
   * Walks through conjuncts and populates dictionaryFilterConjuncts_.
   */
  private void computeDictionaryFilterConjuncts(Analyzer analyzer) {
    for (int conjunct_idx = 0; conjunct_idx < conjuncts_.size(); ++conjunct_idx) {
      Expr conjunct = conjuncts_.get(conjunct_idx);
      List<TupleId> tupleIds = Lists.newArrayList();
      List<SlotId> slotIds = Lists.newArrayList();

      conjunct.getIds(tupleIds, slotIds);
      Preconditions.checkState(tupleIds.size() == 1);
      if (slotIds.size() != 1) continue;

      // Check to see if this slot is a collection type. Nested types are
      // currently not supported. For example, an IsNotEmptyPredicate cannot
      // be evaluated at the dictionary level.
      if (analyzer.getSlotDesc(slotIds.get(0)).getType().isCollectionType()) continue;

      // Check to see if this conjunct contains any known randomized function
      if (conjunct.contains(Expr.IS_NONDETERMINISTIC_BUILTIN_FN_PREDICATE)) continue;

      // Check to see if the conjunct evaluates to true when the slot is NULL
      // This is important for dictionary filtering. Dictionaries do not
      // contain an entry for NULL and do not provide an indication about
      // whether NULLs are present. A conjunct that evaluates to true on NULL
      // cannot be evaluated purely on the dictionary.
      if (analyzer.isTrueWithNullSlots(conjunct)) continue;

      // TODO: Should there be a limit on the cost/structure of the conjunct?
      Integer slotIdInt = slotIds.get(0).asInt();
      if (dictionaryFilterConjuncts_.containsKey(slotIdInt)) {
        dictionaryFilterConjuncts_.get(slotIdInt).add(conjunct_idx);
      } else {
        List<Integer> slotList = Lists.newArrayList(conjunct_idx);
        dictionaryFilterConjuncts_.put(slotIdInt, slotList);
      }
    }
  }

  /**
   * Computes scan ranges (hdfs splits) plus their storage locations, including volume
   * ids, based on the given maximum number of bytes each scan range should scan.
   * Returns the set of file formats being scanned.
   */
  private Set<HdfsFileFormat> computeScanRangeLocations(Analyzer analyzer)
      throws ImpalaRuntimeException {
    long maxScanRangeLength = analyzer.getQueryCtx().client_request.getQuery_options()
        .getMax_scan_range_length();
    scanRanges_ = Lists.newArrayList();
    Set<HdfsFileFormat> fileFormats = Sets.newHashSet();
    for (HdfsPartition partition: partitions_) {
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
      for (HdfsPartition.FileDescriptor fileDesc: partition.getFileDescriptors()) {
        boolean fileDescMissingDiskIds = false;
        for (THdfsFileBlock thriftBlock: fileDesc.getFileBlocks()) {
          HdfsPartition.FileBlock block = FileBlock.fromThrift(thriftBlock);
          List<Integer> replicaHostIdxs = block.getReplicaHostIdxs();
          if (replicaHostIdxs.size() == 0) {
            // we didn't get locations for this block; for now, just ignore the block
            // TODO: do something meaningful with that
            continue;
          }
          // Collect the network address and volume ID of all replicas of this block.
          List<TScanRangeLocation> locations = Lists.newArrayList();
          for (int i = 0; i < replicaHostIdxs.size(); ++i) {
            TScanRangeLocation location = new TScanRangeLocation();
            // Translate from the host index (local to the HdfsTable) to network address.
            Integer tableHostIdx = replicaHostIdxs.get(i);
            TNetworkAddress networkAddress =
                partition.getTable().getHostIndex().getEntry(tableHostIdx);
            Preconditions.checkNotNull(networkAddress);
            // Translate from network address to the global (to this request) host index.
            Integer globalHostIdx = analyzer.getHostIndex().getIndex(networkAddress);
            location.setHost_idx(globalHostIdx);
            if (checkMissingDiskIds && block.getDiskId(i) == -1) {
              ++numScanRangesNoDiskIds_;
              partitionMissingDiskIds = true;
              fileDescMissingDiskIds = true;
            }
            location.setVolume_id(block.getDiskId(i));
            location.setIs_cached(block.isCached(i));
            locations.add(location);
          }
          // create scan ranges, taking into account maxScanRangeLength
          long currentOffset = block.getOffset();
          long remainingLength = block.getLength();
          while (remainingLength > 0) {
            long currentLength = remainingLength;
            if (maxScanRangeLength > 0 && remainingLength > maxScanRangeLength) {
              currentLength = maxScanRangeLength;
            }
            TScanRange scanRange = new TScanRange();
            scanRange.setHdfs_file_split(new THdfsFileSplit(
                fileDesc.getFileName(), currentOffset, currentLength, partition.getId(),
                fileDesc.getFileLength(), fileDesc.getFileCompression(),
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
   * Also computes totalBytes_, totalFiles_, numPartitionsMissingStats_,
   * and sets hasCorruptTableStats_.
   */
  @Override
  public void computeStats(Analyzer analyzer) {
    super.computeStats(analyzer);
    if (LOG.isTraceEnabled()) {
      LOG.trace("collecting partitions for table " + tbl_.getName());
    }
    numPartitionsMissingStats_ = 0;
    totalFiles_ = 0;
    totalBytes_ = 0;
    if (tbl_.getNumClusteringCols() == 0) {
      cardinality_ = tbl_.getNumRows();
      if (cardinality_ < -1 || (cardinality_ == 0 && tbl_.getTotalHdfsBytes() > 0)) {
        hasCorruptTableStats_ = true;
      }
      if (partitions_.isEmpty()) {
        // Nothing to scan. Definitely a cardinality of 0 even if we have no stats.
        cardinality_ = 0;
      } else {
        Preconditions.checkState(partitions_.size() == 1);
        totalFiles_ += partitions_.get(0).getFileDescriptors().size();
        totalBytes_ += partitions_.get(0).getSize();
      }
    } else {
      cardinality_ = 0;
      boolean hasValidPartitionCardinality = false;
      for (HdfsPartition p: partitions_) {
        // Check for corrupt table stats
        if (p.getNumRows() < -1  || (p.getNumRows() == 0 && p.getSize() > 0))  {
          hasCorruptTableStats_ = true;
        }
        // ignore partitions with missing stats in the hope they don't matter
        // enough to change the planning outcome
        if (p.getNumRows() > -1) {
          cardinality_ = addCardinalities(cardinality_, p.getNumRows());
          hasValidPartitionCardinality = true;
        } else {
          ++numPartitionsMissingStats_;
        }
        totalFiles_ += p.getFileDescriptors().size();
        totalBytes_ += p.getSize();
      }
      if (!partitions_.isEmpty() && !hasValidPartitionCardinality) {
        // if none of the partitions knew its number of rows, we fall back on
        // the table stats
        cardinality_ = tbl_.getNumRows();
      }
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
      LOG.trace("computeStats HdfsScan: cardinality_=" + Long.toString(cardinality_));
    }

    computeNumNodes(analyzer, cardinality_);
    if (LOG.isTraceEnabled()) {
      LOG.trace("computeStats HdfsScan: #nodes=" + Integer.toString(numNodes_));
    }
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
    if (!minMaxConjuncts_.isEmpty()) {
      for (Expr e: minMaxConjuncts_) {
        msg.hdfs_scan_node.addToMin_max_conjuncts(e.treeToThrift());
      }
      msg.hdfs_scan_node.setMin_max_tuple_id(minMaxTuple_.getId().asInt());
    }
    msg.hdfs_scan_node.setDictionary_filter_conjuncts(dictionaryFilterConjuncts_);
  }

  @Override
  protected String getDisplayLabelDetail() {
    HdfsTable table = (HdfsTable) desc_.getTable();
    List<String> path = Lists.newArrayList();
    path.add(table.getDb().getName());
    path.add(table.getName());
    Preconditions.checkNotNull(desc_.getPath());
    if (desc_.hasExplicitAlias()) {
      return desc_.getPath().toString() + " " + desc_.getAlias();
    } else {
      return desc_.getPath().toString();
    }
  }

  @Override
  protected String getNodeExplainString(String prefix, String detailPrefix,
      TExplainLevel detailLevel) {
    StringBuilder output = new StringBuilder();
    HdfsTable table = (HdfsTable) desc_.getTable();
    output.append(String.format("%s%s [%s", prefix, getDisplayLabel(),
        getDisplayLabelDetail()));
    int numPartitions = partitions_.size();
    if (detailLevel.ordinal() >= TExplainLevel.EXTENDED.ordinal() &&
        fragment_.isPartitioned()) {
      output.append(", " + fragment_.getDataPartition().getExplainString());
    }
    output.append("]\n");
    if (detailLevel.ordinal() >= TExplainLevel.STANDARD.ordinal()) {
      if (tbl_.getNumClusteringCols() == 0) numPartitions = 1;
      output.append(String.format("%spartitions=%s/%s files=%s size=%s", detailPrefix,
          numPartitions, table.getPartitions().size() - 1, totalFiles_,
          PrintUtils.printBytes(totalBytes_)));
      output.append("\n");
      if (!conjuncts_.isEmpty()) {
        output.append(
            detailPrefix + "predicates: " + getExplainString(conjuncts_) + "\n");
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
        output.append(getRuntimeFilterExplainString(false));
      }
    }
    if (detailLevel.ordinal() >= TExplainLevel.EXTENDED.ordinal()) {
      output.append(getStatsExplainString(detailPrefix, detailLevel));
      output.append("\n");
      if (numScanRangesNoDiskIds_ > 0) {
        output.append(String.format("%smissing disk ids: " +
            "partitions=%s/%s files=%s/%s scan ranges %s/%s\n", detailPrefix,
            numPartitionsNoDiskIds_, numPartitions, numFilesNoDiskIds_,
            totalFiles_, numScanRangesNoDiskIds_, scanRanges_.size()));
      }
      if (!minMaxOriginalConjuncts_.isEmpty()) {
        output.append(detailPrefix + "parquet statistics predicates: " +
            getExplainString(minMaxOriginalConjuncts_) + "\n");
      }
      if (!dictionaryFilterConjuncts_.isEmpty()) {
        List<Integer> totalIdxList = Lists.newArrayList();
        for (List<Integer> idxList : dictionaryFilterConjuncts_.values()) {
          totalIdxList.addAll(idxList);
        }
        // Since the conjuncts are stored by the slot id, they are not necessarily
        // in the same order as the normal conjuncts. Sort the indices so that the
        // order matches the normal conjuncts.
        Collections.sort(totalIdxList);
        List<Expr> exprList = Lists.newArrayList();
        for (Integer idx : totalIdxList) exprList.add(conjuncts_.get(idx));
        output.append(String.format("%sparquet dictionary predicates: %s\n",
            detailPrefix, getExplainString(exprList)));
      }
    }
    return output.toString();
  }

  @Override
  public void computeResourceProfile(TQueryOptions queryOptions) {
    Preconditions.checkNotNull(scanRanges_, "Cost estimation requires scan ranges.");
    if (scanRanges_.isEmpty()) {
      resourceProfile_ = new ResourceProfile(0, 0);
      return;
    }
    Preconditions.checkState(0 < numNodes_ && numNodes_ <= scanRanges_.size());
    Preconditions.checkNotNull(desc_);
    Preconditions.checkNotNull(desc_.getTable() instanceof HdfsTable);
    HdfsTable table = (HdfsTable) desc_.getTable();
    int perHostScanRanges;
    if (table.getMajorityFormat() == HdfsFileFormat.PARQUET) {
      // For the purpose of this estimation, the number of per-host scan ranges for
      // Parquet files are equal to the number of non-partition columns scanned.
      perHostScanRanges = 0;
      for (SlotDescriptor slot: desc_.getSlots()) {
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
    long perInstanceMemEstimate = maxScannerThreads * perThreadIoBuffers * readSize;

    // Sanity check: the tighter estimation should not exceed the per-host maximum.
    long perHostUpperBound = getPerHostMemUpperBound();
    if (perInstanceMemEstimate > perHostUpperBound) {
      LOG.warn(String.format("Per-instance mem cost %s exceeded per-host upper bound %s.",
          PrintUtils.printBytes(perInstanceMemEstimate),
          PrintUtils.printBytes(perHostUpperBound)));
      perInstanceMemEstimate = perHostUpperBound;
    }
    resourceProfile_ = new ResourceProfile(perInstanceMemEstimate, 0);
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

  @Override
  public boolean hasCorruptTableStats() { return hasCorruptTableStats_; }

  public boolean hasMissingDiskIds() { return numScanRangesNoDiskIds_ > 0; }
}
