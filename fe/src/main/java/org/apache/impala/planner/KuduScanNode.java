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
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.ListIterator;
import java.util.Map;
import java.util.Set;

import org.apache.impala.analysis.Analyzer;
import org.apache.impala.analysis.BinaryPredicate;
import org.apache.impala.analysis.BoolLiteral;
import org.apache.impala.analysis.DateLiteral;
import org.apache.impala.analysis.Expr;
import org.apache.impala.analysis.InPredicate;
import org.apache.impala.analysis.IsNullPredicate;
import org.apache.impala.analysis.LiteralExpr;
import org.apache.impala.analysis.MultiAggregateInfo;
import org.apache.impala.analysis.NumericLiteral;
import org.apache.impala.analysis.SlotDescriptor;
import org.apache.impala.analysis.SlotRef;
import org.apache.impala.analysis.StringLiteral;
import org.apache.impala.analysis.TableRef;
import org.apache.impala.analysis.TupleDescriptor;
import org.apache.impala.catalog.FeKuduTable;
import org.apache.impala.catalog.KuduColumn;
import org.apache.impala.catalog.Type;
import org.apache.impala.common.AnalysisException;
import org.apache.impala.common.ImpalaRuntimeException;
import org.apache.impala.common.InternalException;
import org.apache.impala.service.BackendConfig;
import org.apache.impala.thrift.TExplainLevel;
import org.apache.impala.thrift.TKuduReplicaSelection;
import org.apache.impala.thrift.TKuduScanNode;
import org.apache.impala.thrift.TNetworkAddress;
import org.apache.impala.thrift.TPlanNode;
import org.apache.impala.thrift.TPlanNodeType;
import org.apache.impala.thrift.TQueryOptions;
import org.apache.impala.thrift.TScanRange;
import org.apache.impala.thrift.TScanRangeLocation;
import org.apache.impala.thrift.TScanRangeLocationList;
import org.apache.impala.thrift.TScanRangeSpec;
import org.apache.impala.util.ExprUtil;
import org.apache.impala.util.KuduUtil;
import org.apache.impala.util.ExecutorMembershipSnapshot;
import org.apache.kudu.ColumnSchema;
import org.apache.kudu.Schema;
import org.apache.kudu.client.KuduClient;
import org.apache.kudu.client.KuduPredicate;
import org.apache.kudu.client.KuduPredicate.ComparisonOp;
import org.apache.kudu.client.KuduScanToken;
import org.apache.kudu.client.KuduScanToken.KuduScanTokenBuilder;
import org.apache.kudu.client.LocatedTablet;
import org.apache.kudu.consensus.Metadata.RaftPeerPB.Role;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Charsets;
import com.google.common.base.Preconditions;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;

/**
 * Scan of a single Kudu table.
 *
 * Extracts predicates that can be pushed down to Kudu. Currently only binary predicates
 * that have a constant expression on one side and a slot ref on the other can be
 * evaluated by Kudu.
 *
 * Uses the Kudu ScanToken API to generate a set of Kudu "scan tokens" which are used for
 * scheduling and initializing the scanners. Scan tokens are opaque objects that represent
 * a scan for some Kudu data on a tablet (currently one token represents one tablet), and
 * it contains the tablet locations and all information needed to produce a Kudu scanner,
 * including the projected columns and predicates that are pushed down.
 *
 * After KUDU-1065 is resolved, Kudu will also prune the tablets that don't need to be
 * scanned, and only the tokens for those tablets will be returned.
 */
public class KuduScanNode extends ScanNode {
  private final static Logger LOG = LoggerFactory.getLogger(KuduScanNode.class);

  private final FeKuduTable kuduTable_;

  // True if this scan node should use the MT implementation in the backend.
  // Set in computeNodeResourceProfile().
  private boolean useMtScanNode_;

  // True if the query option of replica selection is set as leader-only.
  private boolean replicaSelectionLeaderOnly_ = false;

  // Indexes for the set of hosts that will be used for the query.
  // From analyzer.getHostIndex().getIndex(address)
  private final Set<Integer> hostIndexSet_ = new HashSet<>();

  // List of conjuncts that can be pushed down to Kudu. Used for computing stats and
  // explain strings.
  private final List<Expr> kuduConjuncts_ = new ArrayList<>();

  // Exprs in kuduConjuncts_ converted to KuduPredicates.
  private final List<KuduPredicate> kuduPredicates_ = new ArrayList<>();

  // Slot that is used to record the Kudu metadata for the count(*) aggregation if
  // this scan node has the count(*) optimization enabled.
  private SlotDescriptor countStarSlot_ = null;

  // It is used to indicate if the input query returns not more than one row
  // from Kudu. It is set as TRUE in extractKuduConjuncts() if the number of
  // primary key columns in equivalence predicates pushed down to Kudu equals
  // the total number of primary key columns of the Kudu table.
  // It is used to adjust the cardinality estimation to speed up point lookup
  // for Kudu primary keys by enabling small query optimization.
  boolean isPointLookupQuery_ = false;

  // It is used to indicate current kudu predicate should not be removed from conjuncts.
  // If the current predicate is a comparison predicate with ambiguous timestamp, it may
  // need to check again after actually scanning. For example, if we have two rows with
  // column 'ts' 01:40:00(Local, UTC is 05:40:00) and 01:20:00(Local, UTC is 06:20:00),
  // and the predicate 'ts' < 01:30:00(Local, convert to UTC is 05:30:00 or 06:30:00), we
  // should push ts < 06:30:00(UTC) to Kudu to avoid missing the row with ts
  // 01:20:00(Local, UTC is 06:20:00) and also need to filter out the row with ts
  // 01:40:00(Local, UTC is 05:40:00) after actually scanning.
  boolean currentPredicateNeedCheckAgain_ = false;

  public KuduScanNode(PlanNodeId id, TupleDescriptor desc, List<Expr> conjuncts,
      MultiAggregateInfo aggInfo, TableRef kuduTblRef) {
    super(id, desc, "SCAN KUDU");
    kuduTable_ = (FeKuduTable) desc_.getTable();
    conjuncts_ = conjuncts;
    aggInfo_ = aggInfo;
    tableNumRowsHint_ = kuduTblRef.getTableNumRowsHint();
  }

  @Override
  public void init(Analyzer analyzer) throws ImpalaRuntimeException {
    conjuncts_ = orderConjunctsByCost(conjuncts_);

    KuduClient client = KuduUtil.getKuduClient(kuduTable_.getKuduMasterHosts());
    try {
      // Get the KuduTable from the analyzer to retrieve the cached KuduTable
      // for this query and prevent multiple openTable calls for a single query.
      org.apache.kudu.client.KuduTable rpcTable =
          analyzer.getKuduTable(kuduTable_);
      validateSchema(rpcTable);

      if (canApplyCountStarOptimization(analyzer)) {
        Preconditions.checkState(desc_.getPath().destTable() != null);
        Preconditions.checkState(kuduConjuncts_.isEmpty());
        countStarSlot_ = applyCountStarOptimization(analyzer);
      }

      // Extract predicates that can be evaluated by Kudu.
      extractKuduConjuncts(analyzer, client, rpcTable);

      // Materialize the slots of the remaining conjuncts (i.e. those not pushed to Kudu)
      analyzer.materializeSlots(conjuncts_);

      // Compute mem layout before the scan range locations because creation of the Kudu
      // scan tokens depends on having a mem layout.
      computeMemLayout(analyzer);

      // Creates Kudu scan tokens and sets the scan range locations.
      computeScanRangeLocations(analyzer, client, rpcTable);
    } catch (Exception e) {
      throw new ImpalaRuntimeException("Unable to initialize the Kudu scan node", e);
    }
    computeStats(analyzer);
  }

  /**
   * Validate the columns Impala expects are actually in the Kudu table.
   */
  private void validateSchema(org.apache.kudu.client.KuduTable rpcTable)
      throws ImpalaRuntimeException {
    Schema tableSchema = rpcTable.getSchema();
    for (SlotDescriptor desc: getTupleDesc().getSlots()) {
      if (!desc.isScanSlot()) continue;
      String colName = ((KuduColumn) desc.getColumn()).getKuduName();
      Type colType = desc.getColumn().getType();
      ColumnSchema kuduCol = null;
      try {
        kuduCol = tableSchema.getColumn(colName);
      } catch (Exception e) {
        throw new ImpalaRuntimeException("Column '" + colName + "' not found in kudu " +
            "table " + rpcTable.getName() + ". The table metadata in Impala may be " +
            "outdated and need to be refreshed.");
      }

      Type kuduColType =
          KuduUtil.toImpalaType(kuduCol.getType(), kuduCol.getTypeAttributes());
      if (!colType.equals(kuduColType)) {
        throw new ImpalaRuntimeException("Column '" + colName + "' is type " +
            kuduColType.toSql() + " but Impala expected " + colType.toSql() +
            ". The table metadata in Impala may be outdated and need to be refreshed.");
      }

      if (desc.getIsNullable() != kuduCol.isNullable()) {
        String expected;
        String actual;
        if (desc.getIsNullable()) {
          expected = "nullable";
          actual = "not nullable";
        } else {
          expected = "not nullable";
          actual = "nullable";
        }
        throw new ImpalaRuntimeException("Column '" + colName + "' is " + actual +
            " but Impala expected it to be " + expected +
            ". The table metadata in Impala may be outdated and need to be refreshed.");
      }
    }
  }

  /**
   * Compute the scan range locations for the given table using the scan tokens.
   */
  private void computeScanRangeLocations(Analyzer analyzer,
      KuduClient client, org.apache.kudu.client.KuduTable rpcTable)
      throws ImpalaRuntimeException {
    scanRangeSpecs_ = new TScanRangeSpec();

    replicaSelectionLeaderOnly_ = (analyzer.getQueryOptions().getKudu_replica_selection()
        == TKuduReplicaSelection.LEADER_ONLY);
    List<KuduScanToken> scanTokens = createScanTokens(analyzer, client, rpcTable);
    for (KuduScanToken token: scanTokens) {
      LocatedTablet tablet = token.getTablet();
      List<TScanRangeLocation> locations = new ArrayList<>();
      if (tablet.getReplicas().isEmpty()) {
        throw new ImpalaRuntimeException(String.format(
            "At least one tablet does not have any replicas. Tablet ID: %s",
            new String(tablet.getTabletId(), Charsets.UTF_8)));
      }

      for (LocatedTablet.Replica replica: tablet.getReplicas()) {
        // Skip non-leader replicas if query option KUDU_REPLICA_SELECTION is set as
        // LEADER_ONLY.
        if (replicaSelectionLeaderOnly_
            && !replica.getRole().equals(Role.LEADER.toString())) {
          continue;
        }

        TNetworkAddress address =
            new TNetworkAddress(replica.getRpcHost(), replica.getRpcPort());
        // Use the network address to look up the host in the global list
        Integer hostIndex = analyzer.getHostIndex().getOrAddIndex(address);
        locations.add(new TScanRangeLocation(hostIndex));
        hostIndexSet_.add(hostIndex);
      }

      TScanRange scanRange = new TScanRange();
      try {
        scanRange.setKudu_scan_token(token.serialize());
      } catch (IOException e) {
        throw new ImpalaRuntimeException("Unable to serialize Kudu scan token=" +
            token.toString(), e);
      }

      TScanRangeLocationList locs = new TScanRangeLocationList();
      locs.setScan_range(scanRange);
      locs.setLocations(locations);
      scanRangeSpecs_.addToConcrete_ranges(locs);
    }
  }

  /**
   * Returns KuduScanTokens for this scan given the projected columns and predicates that
   * will be pushed to Kudu. The projected Kudu columns are ordered by offset in an
   * Impala tuple to make the Impala and Kudu tuple layouts identical.
   */
  private List<KuduScanToken> createScanTokens(Analyzer analyzer, KuduClient client,
      org.apache.kudu.client.KuduTable rpcTable) {
    List<String> projectedCols = new ArrayList<>();
    for (SlotDescriptor desc: getTupleDesc().getSlotsOrderedByOffset()) {
      if (!isCountStarOptimizationDescriptor(desc)) {
        projectedCols.add(((KuduColumn) desc.getColumn()).getKuduName());
      }
    }
    KuduScanTokenBuilder tokenBuilder = client.newScanTokenBuilder(rpcTable);
    tokenBuilder.setProjectedColumnNames(projectedCols);
    long split_size_hint = analyzer.getQueryOptions()
        .getTargeted_kudu_scan_range_length();
    if (split_size_hint > 0) tokenBuilder.setSplitSizeBytes(split_size_hint);
    for (KuduPredicate predicate: kuduPredicates_) tokenBuilder.addPredicate(predicate);
    return tokenBuilder.build();
  }

  @Override
  protected double computeSelectivity() {
    List<Expr> allConjuncts = Lists.newArrayList(
        Iterables.concat(conjuncts_, kuduConjuncts_));
    return computeCombinedSelectivity(allConjuncts);
  }

  /**
   * Estimate the number of impalad nodes that this scan node will execute on (which is
   * ultimately determined by the scheduling done by the backend's Scheduler).
   * Assume that scan ranges that can be scheduled locally will be, and that scan
   * ranges that cannot will be round-robined across the cluster.
   */
  protected void computeNumNodes(Analyzer analyzer) {
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
      for (TScanRangeLocationList range : scanRangeSpecs_.concrete_ranges) {
        boolean anyLocal = false;
        if (range.isSetLocations()) {
          for (TScanRangeLocation loc : range.locations) {
            TNetworkAddress address =
                analyzer.getHostIndex().getEntry(loc.getHost_idx());
            if (cluster.contains(address)) {
              anyLocal = true;
              // Use the full tserver address (including port) to account for the test
              // minicluster where there are multiple tservers and impalads on a single
              // host.  This assumes that when an impalad is colocated with a tserver,
              // there are the same number of impalads as tservers on this host in this
              // cluster.
              int count = localRangeCounts.getOrDefault(address, 0);
              if (count < maxInstancesPerNode) {
                ++totalLocalParallelism;
                localRangeCounts.put(address, count + 1);
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
        // hosts that hold replica for those ranges.
        int numLocalNodes = Math.min(numLocalRanges, localRangeCounts.size());
        // The remote ranges are round-robined across all the impalads.
        int numRemoteNodes =
            Math.min(numRemoteRanges, analyzer.numExecutorsForPlanning());
        // The local and remote assignments may overlap, but we don't know by how much
        // so conservatively assume no overlap.
        totalNodes =
            Math.min(numLocalNodes + numRemoteNodes, analyzer.numExecutorsForPlanning());

        int numLocalInstances = Math.min(numLocalRanges, totalLocalParallelism);
        totalInstances = Math.min(numLocalInstances + numRemoteRanges,
            totalNodes * maxInstancesPerNode);

        // Exit early if we have maxed out our estimate of hosts/instances, to avoid
        // extraneous work in case the number of scan ranges dominates the number of
        // nodes.
        if (totalInstances == maxPossibleInstances) break;
      }
    }
    numNodes_ = Math.max(totalNodes, 1);
    numInstances_ = Math.max(totalInstances, 1);
  }

  @Override
  public void computeStats(Analyzer analyzer) {
    super.computeStats(analyzer);
    computeNumNodes(analyzer);

    // Update the cardinality, hint value will be used when table has no stats.
    inputCardinality_ = cardinality_ =
        kuduTable_.getNumRows() == -1 ? tableNumRowsHint_ : kuduTable_.getNumRows();
    if (isPointLookupQuery_) {
      // Adjust input and output cardinality for point lookup.
      // Planner don't create KuduScanNode for query with closure "limit 0" so
      // we can assume "limit" is not less than 1 here and don't need to call
      // capCardinalityAtLimit().
      if (cardinality_ != 0) cardinality_ = 1;
      inputCardinality_ = cardinality_;
    } else {
      cardinality_ = applyConjunctsSelectivity(cardinality_);
      cardinality_ = capCardinalityAtLimit(cardinality_);
    }
    if (LOG.isTraceEnabled()) {
      LOG.trace("computeStats KuduScan: cardinality=" + Long.toString(cardinality_));
    }
  }

  @Override
  public void computeProcessingCost(TQueryOptions queryOptions) {
    processingCost_ = computeScanProcessingCost(queryOptions);
  }

  @Override
  public void computeNodeResourceProfile(TQueryOptions queryOptions) {
    // The bulk of memory used by Kudu scan node is generally utilized by the
    // RowbatchQueue plus the row batches filled in by the scanner threads and
    // waiting to be queued into the RowbatchQueue. Due to a number of factors
    // like variable length string columns, mem pool usage pattern,
    // variability of the number of scanner threads being spawned and the
    // variability of the average RowbatchQueue size, it is increasingly
    // difficult to precisely estimate the memory usage. Therefore, we fall back
    // to a more simpler approach of using empirically derived estimates.
    int numOfScanRanges = scanRangeSpecs_.getConcrete_rangesSize();
    int perHostScanRanges = estimatePerHostScanRanges(numOfScanRanges);
    int maxScannerThreads = computeMaxNumberOfScannerThreads(queryOptions,
        perHostScanRanges);
    long estimated_bytes_per_column_per_thread = BackendConfig.INSTANCE.getBackendCfg().
        kudu_scanner_thread_estimated_bytes_per_column;
    long max_estimated_bytes_per_thread = BackendConfig.INSTANCE.getBackendCfg().
        kudu_scanner_thread_max_estimated_bytes;
    long mem_estimate_per_thread = Math.min(getNumMaterializedSlots(desc_) *
        estimated_bytes_per_column_per_thread, max_estimated_bytes_per_thread);
    useMtScanNode_ = Planner.useMTFragment(queryOptions);
    nodeResourceProfile_ = new ResourceProfileBuilder()
        .setMemEstimateBytes(mem_estimate_per_thread * maxScannerThreads)
        .setThreadReservation(useMtScanNode_ ? 0 : 1).build();
  }

  @Override
  protected String getNodeExplainString(String prefix, String detailPrefix,
      TExplainLevel detailLevel) {
    StringBuilder result = new StringBuilder();

    String aliasStr = desc_.hasExplicitAlias() ? " " + desc_.getAlias() : "";
    result.append(
        String.format(replicaSelectionLeaderOnly_ ? "%s%s:%s [%s%s, LEADER-only]\n" :
                                                    "%s%s:%s [%s%s]\n",
            prefix, id_.toString(), displayName_, kuduTable_.getFullName(), aliasStr));

    switch (detailLevel) {
      case MINIMAL: break;
      case STANDARD: // Fallthrough intended.
      case EXTENDED: // Fallthrough intended.
      case VERBOSE: {
        if (!conjuncts_.isEmpty()) {
          result.append(detailPrefix
              + "predicates: " + Expr.getExplainString(conjuncts_, detailLevel) + "\n");
        }
        if (!kuduConjuncts_.isEmpty()) {
          result.append(detailPrefix + "kudu predicates: "
              + Expr.getExplainString(kuduConjuncts_, detailLevel) + "\n");
        }
        if (!runtimeFilters_.isEmpty()) {
          result.append(detailPrefix + "runtime filters: ");
          result.append(getRuntimeFilterExplainString(false, detailLevel));
        }
      }
    }
    return result.toString();
  }

  @Override
  protected void toThrift(TPlanNode node) {
    node.node_type = TPlanNodeType.KUDU_SCAN_NODE;
    node.kudu_scan_node = new TKuduScanNode(desc_.getId().asInt());
    node.kudu_scan_node.setUse_mt_scan_node(useMtScanNode_);

    Preconditions.checkState((optimizedAggSmap_ == null) == (countStarSlot_ == null));
    if (countStarSlot_ != null) {
      node.kudu_scan_node.setCount_star_slot_offset(countStarSlot_.getByteOffset());
    }
  }

  /**
   * Extracts predicates from conjuncts_ that can be pushed down to Kudu. Currently only
   * binary predicates that have a constant expression on one side and a slot ref on the
   * other can be evaluated by Kudu. Only looks at comparisons of constants (i.e., the
   * bounds of the result can be evaluated with Expr::GetValue(NULL)). If a conjunct can
   * be converted into this form, the normalized expr is added to kuduConjuncts_, a
   * KuduPredicate is added to kuduPredicates_, and the original expr from conjuncts_ is
   * removed.
   */
  private void extractKuduConjuncts(Analyzer analyzer,
      KuduClient client, org.apache.kudu.client.KuduTable rpcTable) {
    // The set of primary key column index which are in equality predicates where
    // it's compared to a constant, and will be pushed down to Kudu.
    Set<Integer> primaryKeyColsInEqualPred = new HashSet<>();
    ListIterator<Expr> it = conjuncts_.listIterator();
    while (it.hasNext()) {
      Expr predicate = it.next();
      if (tryConvertBinaryKuduPredicate(analyzer, rpcTable, predicate,
              primaryKeyColsInEqualPred) ||
          tryConvertInListKuduPredicate(analyzer, rpcTable, predicate) ||
          tryConvertIsNullKuduPredicate(analyzer, rpcTable, predicate)) {
        if (currentPredicateNeedCheckAgain_) {
          currentPredicateNeedCheckAgain_ = false;
        } else {
          it.remove();
        }
      }
    }
    if (primaryKeyColsInEqualPred.size() >= 1 &&
        primaryKeyColsInEqualPred.size() ==
            rpcTable.getSchema().getPrimaryKeyColumnCount()) {
      isPointLookupQuery_ = true;
    }
  }

  /**
   * If 'expr' can be converted to a KuduPredicate, returns true and updates
   * kuduPredicates_ and kuduConjuncts_.
   */
  private boolean tryConvertBinaryKuduPredicate(Analyzer analyzer,
      org.apache.kudu.client.KuduTable table, Expr expr,
      Set<Integer> primaryKeyColsInEqualPred) {
    if (!(expr instanceof BinaryPredicate)) return false;
    BinaryPredicate predicate = (BinaryPredicate) expr;

    // TODO KUDU-931 look into handling implicit/explicit casts on the SlotRef.

    ComparisonOp op = getKuduOperator(predicate.getOp());
    if (op == null) return false;

    if (!(predicate.getChild(0) instanceof SlotRef)) return false;
    SlotRef ref = (SlotRef) predicate.getChild(0);

    if (!(predicate.getChild(1) instanceof LiteralExpr)) return false;
    LiteralExpr literal = (LiteralExpr) predicate.getChild(1);

    // Cannot push predicates with null literal values (KUDU-1595).
    if (Expr.IS_NULL_LITERAL.apply(literal)) return false;

    String colName = ((KuduColumn) ref.getDesc().getColumn()).getKuduName();
    ColumnSchema column = table.getSchema().getColumn(colName);
    KuduPredicate kuduPredicate = null;
    switch (literal.getType().getPrimitiveType()) {
      case BOOLEAN: {
        kuduPredicate = KuduPredicate.newComparisonPredicate(column, op,
            ((BoolLiteral)literal).getValue());
        break;
      }
      case TINYINT:
      case SMALLINT:
      case INT: {
        kuduPredicate = KuduPredicate.newComparisonPredicate(column, op,
            ((NumericLiteral)literal).getIntValue());
        break;
      }
      case BIGINT: {
        kuduPredicate = KuduPredicate.newComparisonPredicate(column, op,
            ((NumericLiteral)literal).getLongValue());
        break;
      }
      case FLOAT: {
        kuduPredicate = KuduPredicate.newComparisonPredicate(column, op,
            (float)((NumericLiteral)literal).getDoubleValue());
        break;
      }
      case DOUBLE: {
        kuduPredicate = KuduPredicate.newComparisonPredicate(column, op,
            ((NumericLiteral)literal).getDoubleValue());
        break;
      }
      case STRING:
      case VARCHAR:
      case CHAR: {
        kuduPredicate = KuduPredicate.newComparisonPredicate(column, op,
            ((StringLiteral)literal).getUnescapedValue());
        break;
      }
      case BINARY: {
        kuduPredicate = KuduPredicate.newComparisonPredicate(column, op,
            ((StringLiteral)literal).getUnescapedValue().getBytes());
        break;
      }
      case TIMESTAMP: {
        try {
          // TODO: Simplify when Impala supports a 64-bit TIMESTAMP type.
          kuduPredicate = analyzer.getQueryOptions().isConvert_kudu_utc_timestamps() ?
              convertLocalTimestampBinaryKuduPredicate(analyzer, column, op, literal) :
              KuduPredicate.newComparisonPredicate(column, op,
                  ExprUtil.utcTimestampToUnixTimeMicros(analyzer, literal));
        } catch (Exception e) {
          LOG.info("Exception converting Kudu timestamp predicate: " + expr.toSql(), e);
          return false;
        }
        break;
      }
      case DATE:
        kuduPredicate = KuduPredicate.newComparisonPredicate(column, op,
            ((DateLiteral)literal).getValue());
        break;
      case DECIMAL: {
        kuduPredicate = KuduPredicate.newComparisonPredicate(column, op,
            ((NumericLiteral)literal).getValue());
        break;
      }
      default:
        //All supported types are covered, should not reach default case
        Preconditions.checkState(false);
    }
    Preconditions.checkState(kuduPredicate != null);

    kuduConjuncts_.add(predicate);
    kuduPredicates_.add(kuduPredicate);

    if (predicate.getOp().isEquivalence() && column.isKey()) {
      Integer colIndex = table.getSchema().getColumnIndex(colName);
      primaryKeyColsInEqualPred.add(colIndex);
    }

    return true;
  }

  private KuduPredicate convertLocalTimestampBinaryKuduPredicate(Analyzer analyzer,
      ColumnSchema column, ComparisonOp op, LiteralExpr literal)
      throws AnalysisException, InternalException {
    Long preUnixTimeMicros =
        ExprUtil.localTimestampToUnixTimeMicros(analyzer, literal, true);
    Long postUnixTimeMicros =
        ExprUtil.localTimestampToUnixTimeMicros(analyzer, literal, false);
    // If the timestamp is not a valid local timestamp, EQUAL predicate should be always
    // false. For other comparison predicates, we could use the transition point time as
    // a common value for comparison.
    if (preUnixTimeMicros == null || postUnixTimeMicros == null) {
      if (preUnixTimeMicros == null) return null; // should not happen
      if (op == ComparisonOp.EQUAL) {
        // An empty IN LIST predicate is always false.
        return KuduPredicate.newInListPredicate(column, Lists.newArrayList());
      } else {
        postUnixTimeMicros = preUnixTimeMicros;
      }
    }
    // If the timestamp is unique, create the predicate normally.
    if (preUnixTimeMicros.equals(postUnixTimeMicros)) {
      return KuduPredicate.newComparisonPredicate(column, op, preUnixTimeMicros);
    }
    // If the timestamp is ambiguous, we should convert EQUAL predicate to an IN LIST
    // predicate that include all ambiguous values. For comparison predicates, we need to
    // use a larger range of possible values for comparison to avoid missing rows.
    // Additionally, set currentPredicateNeedCheckAgain_ to true to indicate that the
    // predicate should not removed from the conjuncts_ list.
    switch (op) {
      case EQUAL: return KuduPredicate.newInListPredicate(column,
          Lists.newArrayList(preUnixTimeMicros, postUnixTimeMicros));
      case LESS:
      case LESS_EQUAL: {
        currentPredicateNeedCheckAgain_ = true;
        return KuduPredicate.newComparisonPredicate(column, op,
            postUnixTimeMicros);
      }
      case GREATER:
      case GREATER_EQUAL: {
        currentPredicateNeedCheckAgain_ = true;
        return KuduPredicate.newComparisonPredicate(column, op,
            preUnixTimeMicros);
      }
      default:
        throw new InternalException("Unexpected operator: " + op);
    }
  }

  /**
   * If the InList 'expr' can be converted to a KuduPredicate, returns true and updates
   * kuduPredicates_ and kuduConjuncts_.
   */
  private boolean tryConvertInListKuduPredicate(Analyzer analyzer,
      org.apache.kudu.client.KuduTable table, Expr expr) {
    if (!(expr instanceof InPredicate)) return false;
    InPredicate predicate = (InPredicate) expr;

    // Only convert IN predicates, i.e. cannot convert NOT IN.
    if (predicate.isNotIn()) return false;

    // Do not convert if there is an implicit cast.
    if (!(predicate.getChild(0) instanceof SlotRef)) return false;
    SlotRef ref = (SlotRef) predicate.getChild(0);

    // KuduPredicate takes a list of values as Objects.
    List<Object> values = new ArrayList<>();
    for (int i = 1; i < predicate.getChildren().size(); ++i) {
      if (!Expr.IS_LITERAL.apply(predicate.getChild(i))) return false;
      LiteralExpr literal = (LiteralExpr) predicate.getChild(i);

      // Cannot push predicates with null literal values (KUDU-1595).
      if (Expr.IS_NULL_LITERAL.apply(literal)) return false;

      Object value = getKuduInListValue(analyzer, literal);
      if (value == null) return false;
      if (value instanceof List) {
        values.addAll((List<?>) value);
      } else {
        values.add(value);
      }
    }

    String colName = ((KuduColumn) ref.getDesc().getColumn()).getKuduName();
    ColumnSchema column = table.getSchema().getColumn(colName);
    kuduPredicates_.add(KuduPredicate.newInListPredicate(column, values));
    kuduConjuncts_.add(predicate);
    return true;
  }

  /**
   * If IS NULL/IS NOT NULL 'expr' can be converted to a KuduPredicate,
   * returns true and updates kuduPredicates_ and kuduConjuncts_.
   */
  private boolean tryConvertIsNullKuduPredicate(Analyzer analyzer,
      org.apache.kudu.client.KuduTable table, Expr expr) {
    if (!(expr instanceof IsNullPredicate)) return false;
    IsNullPredicate predicate = (IsNullPredicate) expr;

    // Do not convert if expression is more than a SlotRef
    // This is true even for casts, as certain casts can take a non-NULL
    // value and produce a NULL. For example, CAST('test' as tinyint)
    // is NULL.
    if (!(predicate.getChild(0) instanceof SlotRef)) return false;
    SlotRef ref = (SlotRef) predicate.getChild(0);

    String colName = ((KuduColumn) ref.getDesc().getColumn()).getKuduName();
    ColumnSchema column = table.getSchema().getColumn(colName);
    KuduPredicate kuduPredicate = null;
    if (predicate.isNotNull()) {
      kuduPredicate = KuduPredicate.newIsNotNullPredicate(column);
    } else {
      kuduPredicate = KuduPredicate.newIsNullPredicate(column);
    }
    kuduConjuncts_.add(predicate);
    kuduPredicates_.add(kuduPredicate);
    return true;
  }

  /**
   * Return the value of the InList child expression 'e' as an Object that can be
   * added to a KuduPredicate. If the Expr is not supported by Kudu or the type doesn't
   * match the expected PrimitiveType 'type', null is returned.
   * Additionally, if the query option 'convert_kudu_utc_timestamps' is enabled and when
   * the expression 'e' is converted from a local timestamp to a UTC timestamp, it is
   * invalid or ambiguous, the method will return either an empty list or a list
   * containing two ambiguous values.
   */
  private static Object getKuduInListValue(Analyzer analyzer, LiteralExpr e) {
    switch (e.getType().getPrimitiveType()) {
      case BOOLEAN: return ((BoolLiteral) e).getValue();
      case TINYINT: return (byte) ((NumericLiteral) e).getLongValue();
      case SMALLINT: return (short) ((NumericLiteral) e).getLongValue();
      case INT: return (int) ((NumericLiteral) e).getLongValue();
      case BIGINT: return ((NumericLiteral) e).getLongValue();
      case FLOAT: return (float) ((NumericLiteral) e).getDoubleValue();
      case DOUBLE: return ((NumericLiteral) e).getDoubleValue();
      case STRING: return ((StringLiteral) e).getUnescapedValue();
      case TIMESTAMP: {
        try {
          // TODO: Simplify when Impala supports a 64-bit TIMESTAMP type.
          if (analyzer.getQueryOptions().isConvert_kudu_utc_timestamps()) {
            Long preUnixTimeMicros =
                ExprUtil.localTimestampToUnixTimeMicros(analyzer, e, true);
            Long postUnixTimeMicros =
                ExprUtil.localTimestampToUnixTimeMicros(analyzer, e, false);
            // If the timestamp is invalid in local time, return empty list.
            if (preUnixTimeMicros == null || postUnixTimeMicros == null) {
              if (preUnixTimeMicros == null) return null; // should not happen
              return Lists.newArrayList();
            }
            // If the timestamp is unique, return the unique value.
            if (preUnixTimeMicros.equals(postUnixTimeMicros)) return preUnixTimeMicros;
            // If the timestamp is ambiguous, return a list of the two possible values.
            return Lists.newArrayList(preUnixTimeMicros, postUnixTimeMicros);
          }
          return ExprUtil.utcTimestampToUnixTimeMicros(analyzer, e);
        } catch (Exception ex) {
          LOG.info("Exception converting Kudu timestamp expr: " + e.toSql(), ex);
        }
        break;
      }
      case DECIMAL: return ((NumericLiteral) e).getValue();
      default:
        Preconditions.checkState(false,
            "Unsupported Kudu type considered for predicate: %s", e.getType().toSql());
    }
    return null;
  }

  /**
   * Returns a Kudu comparison operator for the BinaryPredicate operator, or null if
   * the operation is not supported by Kudu.
   */
  private static KuduPredicate.ComparisonOp getKuduOperator(BinaryPredicate.Operator op) {
    switch (op) {
      case GT: return ComparisonOp.GREATER;
      case LT: return ComparisonOp.LESS;
      case GE: return ComparisonOp.GREATER_EQUAL;
      case LE: return ComparisonOp.LESS_EQUAL;
      case EQ: return ComparisonOp.EQUAL;
      default: return null;
    }
  }

  @Override
  public boolean hasStorageLayerConjuncts() { return !kuduConjuncts_.isEmpty(); }
}
