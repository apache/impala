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

import com.google.common.base.MoreObjects;
import com.google.common.base.Preconditions;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import org.apache.commons.lang3.SerializationUtils;
import org.apache.impala.analysis.Analyzer;
import org.apache.impala.analysis.Expr;
import org.apache.impala.analysis.MultiAggregateInfo;
import org.apache.impala.analysis.SlotDescriptor;
import org.apache.impala.analysis.TupleDescriptor;
import org.apache.impala.catalog.paimon.FePaimonTable;
import org.apache.impala.catalog.paimon.PaimonColumn;
import org.apache.impala.common.AnalysisException;
import org.apache.impala.common.ImpalaException;
import org.apache.impala.common.ImpalaRuntimeException;
import org.apache.impala.common.ThriftSerializationCtx;
import org.apache.impala.planner.paimon.PaimonSplit;
import org.apache.impala.thrift.TExplainLevel;
import org.apache.impala.thrift.TNetworkAddress;
import org.apache.impala.thrift.TPaimonScanNode;
import org.apache.impala.thrift.TPlanNode;
import org.apache.impala.thrift.TPlanNodeType;
import org.apache.impala.thrift.TQueryOptions;
import org.apache.impala.thrift.TScanRange;
import org.apache.impala.thrift.TScanRangeLocation;
import org.apache.impala.thrift.TScanRangeLocationList;
import org.apache.impala.thrift.TScanRangeSpec;
import org.apache.impala.util.ExecutorMembershipSnapshot;
import org.apache.paimon.table.Table;
import org.apache.paimon.table.source.DataSplit;
import org.apache.paimon.table.source.ReadBuilder;
import org.apache.paimon.table.source.Split;
import org.apache.paimon.types.DataField;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * Jni-based scan node of a single paimon table.
 */
public class PaimonScanNode extends ScanNode {
  private final static Logger LOG = LoggerFactory.getLogger(PaimonScanNode.class);
  private final static long PAIMON_ROW_AVG_SIZE_OVERHEAD = 4L;
  // FeTable object
  private final FePaimonTable table_;

  // paimon table object
  private final Table paimonApiTable_;

  // Indexes for the set of hosts that will be used for the query.
  // From analyzer.getHostIndex().getIndex(address)
  private final Set<Integer> hostIndexSet_ = new HashSet<>();
  // Array of top level field ids used for top level column scan pruning.
  private int[] projection_;
  // top level field id map
  private Set<Integer> fieldIdMap_ = Sets.newHashSet();
  // Splits generated for paimon scanning during planning stage.
  private List<Split> splits_;

  public PaimonScanNode(PlanNodeId id, TupleDescriptor desc, List<Expr> conjuncts,
      MultiAggregateInfo aggInfo, FePaimonTable table) {
    super(id, desc, "SCAN PAIMON");
    conjuncts_ = conjuncts;
    aggInfo_ = aggInfo;
    table_ = table;
    paimonApiTable_ = table.getPaimonApiTable();
  }

  @Override
  public void init(Analyzer analyzer) throws ImpalaException {
    super.init(analyzer);
    conjuncts_ = orderConjunctsByCost(conjuncts_);
    // TODO: implement predicate push down later here.

    // materialize slots in remaining conjuncts_
    analyzer.materializeSlots(conjuncts_);
    collectProjectionId();
    computeMemLayout(analyzer);
    computeScanRangeLocations(analyzer);
    computePaimonStats(analyzer);
  }

  public void computePaimonStats(Analyzer analyzer) {
    computeNumNodes(analyzer);
    // Update the cardinality, hint value will be used when table has no stats.
    inputCardinality_ = cardinality_ = estimateTableRowCount();
    cardinality_ = applyConjunctsSelectivity(cardinality_);
    cardinality_ = capCardinalityAtLimit(cardinality_);
    avgRowSize_ = estimateAvgRowSize();
    if (LOG.isTraceEnabled()) {
      LOG.trace("computeStats paimonScan: cardinality=" + Long.toString(cardinality_));
    }
  }

  /**
   * Collect and analyze top-level columns.
   */
  public void collectProjectionId() throws AnalysisException {
    projection_ = new int[desc_.getSlots().size()];
    for (int i = 0; i < desc_.getSlots().size(); i++) {
      SlotDescriptor sd = desc_.getSlots().get(i);
      if (sd.isVirtualColumn()) {
        throw new AnalysisException("Paimon Scanner doesn't support virtual columns.");
      }
      if (sd.getPath().getRawPath() != null && sd.getPath().getRawPath().size() > 1) {
        throw new AnalysisException("Paimon Scanner doesn't support nested columns.");
      }
      PaimonColumn paimonColumn = (PaimonColumn) desc_.getSlots().get(i).getColumn();
      projection_[i] = paimonColumn.getFieldId();
      fieldIdMap_.add(paimonColumn.getFieldId());
    }
    Preconditions.checkArgument(projection_.length == desc_.getSlots().size());
    LOG.info(String.format("table %s projection fields: %s", table_.getFullName(),
        Arrays.toString(projection_)));
  }

  protected long estimateSplitRowCount(Split s) {
    if (s instanceof DataSplit) {
      DataSplit dataSplit = (DataSplit) s;
      if (dataSplit.mergedRowCountAvailable()) { return dataSplit.mergedRowCount(); }
    }
    return s.rowCount();
  }

  protected long estimateTableRowCount() {
    return splits_.stream()
        .map(this ::estimateSplitRowCount)
        .reduce(Long::sum)
        .orElse(-1L);
  }

  protected long estimateAvgRowSize() {
    List<DataField> dataColumns = paimonApiTable_.rowType().getFields();
    return dataColumns.stream()
               .filter(df -> fieldIdMap_.contains(df.id()))
               .mapToInt(column -> column.type().defaultSize())
               .sum()
        + PAIMON_ROW_AVG_SIZE_OVERHEAD;
  }
  /**
   * Compute the scan range locations for the given table using the scan tokens.
   */
  private void computeScanRangeLocations(Analyzer analyzer)
      throws ImpalaRuntimeException {
    scanRangeSpecs_ = new TScanRangeSpec();
    ReadBuilder readBuilder = paimonApiTable_.newReadBuilder();

    // 2. Plan splits in 'Coordinator'.
    splits_ = readBuilder.newScan().plan().splits();

    if (splits_.size() <= 0) {
      LOG.info("no paimon data available");
      return;
    }

    for (Split split : splits_) {
      List<TScanRangeLocation> locations = new ArrayList<>();
      // TODO: Currently, set to dummy network address for random executor scheduling,
      // don't forget to get actual location for data locality after native table scan
      // is supported.
      //
      {
        TNetworkAddress address = new TNetworkAddress("localhost", 12345);
        // Use the network address to look up the host in the global list
        Integer hostIndex = analyzer.getHostIndex().getOrAddIndex(address);
        locations.add(new TScanRangeLocation(hostIndex));
        hostIndexSet_.add(hostIndex);
      }

      TScanRange scanRange = new TScanRange();
      // TODO: apply predicate push down later.
      PaimonSplit paimonSplit = new PaimonSplit(split, null);
      byte[] split_data_serialized = SerializationUtils.serialize(paimonSplit);
      scanRange.setFile_metadata(split_data_serialized);
      TScanRangeLocationList locs = new TScanRangeLocationList();
      locs.setScan_range(scanRange);
      locs.setLocations(locations);
      scanRangeSpecs_.addToConcrete_ranges(locs);
    }
  }

  @Override
  protected double computeSelectivity() {
    List<Expr> allConjuncts = Lists.newArrayList(Iterables.concat(conjuncts_));
    return computeCombinedSelectivity(allConjuncts);
  }

  /**
   * Estimate the number of impalad nodes that this scan node will execute on (which is
   * ultimately determined by the scheduling done by the backend's Scheduler).
   * As of now, scan ranges are scheduled round-robin, since they have no location
   * information. .
   */
  protected void computeNumNodes(Analyzer analyzer) {
    ExecutorMembershipSnapshot cluster = ExecutorMembershipSnapshot.getCluster();
    final int maxInstancesPerNode = getMaxInstancesPerNode(analyzer);
    final int maxPossibleInstances =
        analyzer.numExecutorsForPlanning() * maxInstancesPerNode;
    int totalNodes = 0;
    int totalInstances = 0;
    int numRemoteRanges = splits_.size();

    // The remote ranges are round-robined across all the impalads.
    int numRemoteNodes = Math.min(numRemoteRanges, analyzer.numExecutorsForPlanning());
    // The remote assignments may overlap, but we don't know by how much
    // so conservatively assume no overlap.
    totalNodes = Math.min(numRemoteNodes, analyzer.numExecutorsForPlanning());

    totalInstances = Math.min(numRemoteRanges, totalNodes * maxInstancesPerNode);

    numNodes_ = Math.max(totalNodes, 1);
    numInstances_ = Math.max(totalInstances, 1);
  }

  @Override
  public void computeNodeResourceProfile(TQueryOptions queryOptions) {
    // current batch size is from query options, so estimated bytes
    // is calculated as BATCH_SIZE * average row size * 2
    long batchSize = getRowBatchSize(queryOptions);
    long memSize = batchSize * (long) getAvgRowSize() * 2;
    nodeResourceProfile_ =
        new ResourceProfileBuilder().setMemEstimateBytes(memSize).build();
  }

  @Override
  protected void toThrift(TPlanNode msg, ThriftSerializationCtx serialCtx) {
    toThrift(msg);
  }

  @Override
  protected void toThrift(TPlanNode node) {
    node.node_type = TPlanNodeType.PAIMON_SCAN_NODE;
    node.paimon_table_scan_node = new TPaimonScanNode(desc_.getId().asInt(),
        ByteBuffer.wrap(SerializationUtils.serialize(paimonApiTable_)),
        table_.getFullName());
  }

  @Override
  public void computeProcessingCost(TQueryOptions queryOptions) {
    processingCost_ = computeScanProcessingCost(queryOptions);
  }

  @Override
  protected String getNodeExplainString(
      String prefix, String detailPrefix, TExplainLevel detailLevel) {
    StringBuilder result = new StringBuilder();

    String aliasStr = desc_.hasExplicitAlias() ? " " + desc_.getAlias() : "";
    result.append(String.format("%s%s:%s [%s%s]\n", prefix, id_.toString(), displayName_,
        table_.getFullName(), aliasStr));

    switch (detailLevel) {
      case MINIMAL: break;
      case STANDARD: // Fallthrough intended.
      case EXTENDED: // Fallthrough intended.
      case VERBOSE: {
        if (!conjuncts_.isEmpty()) {
          result.append(detailPrefix
              + "predicates: " + Expr.getExplainString(conjuncts_, detailLevel) + "\n");
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
  protected String debugString() {
    MoreObjects.ToStringHelper helper = MoreObjects.toStringHelper(this);
    helper.addValue(super.debugString());
    helper.addValue("paimonTable=" + table_.getFullName());
    return helper.toString();
  }
}
