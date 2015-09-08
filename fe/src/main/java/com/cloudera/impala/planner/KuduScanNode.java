// Copyright 2015 Cloudera Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package com.cloudera.impala.planner;

import java.util.List;
import java.util.ListIterator;
import java.util.Set;

import com.cloudera.impala.analysis.BinaryPredicate;
import com.cloudera.impala.analysis.BinaryPredicate.Operator;
import com.cloudera.impala.analysis.Expr;
import com.cloudera.impala.analysis.SlotRef;
import com.cloudera.impala.common.AnalysisException;
import com.cloudera.impala.common.ImpalaRuntimeException;
import com.google.common.base.Charsets;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import org.kududb.client.KuduClient;
import org.kududb.client.LocatedTablet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.cloudera.impala.analysis.Analyzer;
import com.cloudera.impala.analysis.TupleDescriptor;
import com.cloudera.impala.catalog.KuduTable;
import com.cloudera.impala.common.InternalException;
import com.cloudera.impala.thrift.TExplainLevel;
import com.cloudera.impala.thrift.TKuduKeyRange;
import com.cloudera.impala.thrift.TKuduScanNode;
import com.cloudera.impala.thrift.TNetworkAddress;
import com.cloudera.impala.thrift.TPlanNode;
import com.cloudera.impala.thrift.TPlanNodeType;
import com.cloudera.impala.thrift.TScanRange;
import com.cloudera.impala.thrift.TScanRangeLocation;
import com.cloudera.impala.thrift.TScanRangeLocations;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

import static org.kududb.client.KuduClient.KuduClientBuilder;

/**
 * Scan of a single Kudu table.
 * Extracts predicates that can be pushed down to Kudu and sends them to the backend
 * as part of a TKuduScanNode.
 * Currently only binary predicates (<=, >=, ==) that have a constant expression on one
 * side and a slot ref on the other can be evaluated by Kudu.
 */
public class KuduScanNode extends ScanNode {

  private final static Logger LOG = LoggerFactory.getLogger(KuduScanNode.class);

  private final KuduTable kuduTable_;

  private final Set<Integer> hostIndexSet_ = Sets.newHashSet();

  // List of conjuncts that can be pushed down to Kudu
  // TODO use the portion of this list that pertains to keys to do partition pruning.
  private final List<Expr> kuduConjuncts_ = Lists.newArrayList();

  public KuduScanNode(PlanNodeId id, TupleDescriptor desc) {
    super(id, desc, "SCAN KUDU");
    kuduTable_ = (KuduTable) desc_.getTable();
  }

  @Override
  public void init(Analyzer analyzer) throws InternalException {
    assignConjuncts(analyzer);
    analyzer.createEquivConjuncts(tupleIds_.get(0), conjuncts_);

    // Extract predicates that can evaluated by Kudu.
    try {
      kuduConjuncts_.addAll(extractKuduConjuncts(conjuncts_, analyzer));
      // Mark these slots as materialized, otherwise the toThrift() of SlotRefs
      // referencing them will fail. These slots will never be filled with data though as
      // Kudu won't return these columns.
      // TODO KUDU-935 Don't require that slots be materialized in order to serialize
      // SlotRefs.
      analyzer.materializeSlots(kuduConjuncts_);
    } catch (AnalysisException e) {
      throw new InternalException("Error while extracting Kudu conjuncts.", e);
    }

    computeScanRangeLocations(analyzer);
    markSlotsMaterialized(analyzer, conjuncts_);
    computeMemLayout(analyzer);
    computeStats(analyzer);
  }

  /**
   * Compute the scan range locations for the given table. Does not look at predicates.
   * To get the locations, we look at the table and load its tablets, for each tablet
   * we get the key-range and for each tablet we get the replicated hosts as well.
   */
  private void computeScanRangeLocations(Analyzer analyzer) {
    KuduClientBuilder builder =
        new KuduClientBuilder(kuduTable_.getKuduMasterAddresses());
    KuduClient client = builder.build();
    scanRanges_ = Lists.newArrayList();
    // TODO: The metadata that is queried from Kudu should be cached in the Catalog
    try {
      org.kududb.client.KuduTable rpcTable =
          client.openTable(kuduTable_.getKuduTableName());
      List<LocatedTablet> tabletLocations =
          rpcTable.getTabletsLocations(KuduTable.KUDU_RPC_TIMEOUT_MS);

      for (LocatedTablet tablet : tabletLocations) {
        List<TScanRangeLocation> locations = Lists.newArrayList();
        if (tablet.getReplicas().isEmpty()) {
          throw new ImpalaRuntimeException(String.format(
              "At least one tablet does not have any replicas. Tablet ID: %s",
              new String(tablet.getTabletId(), Charsets.UTF_8)));
        }
        for (LocatedTablet.Replica replica : tablet.getReplicas()) {
          TNetworkAddress address = new TNetworkAddress(replica.getRpcHost(),
              replica.getRpcPort());
          // Use the network address to look up the host in the global list
          Integer hostIndex = analyzer.getHostIndex().getIndex(address);
          locations.add(new TScanRangeLocation(hostIndex));
          hostIndexSet_.add(hostIndex);
        }

        TScanRangeLocations locs = new TScanRangeLocations();

        // Now set the scan range of this tablet
        TKuduKeyRange keyRange = new TKuduKeyRange();
        keyRange.setStartKey(tablet.getStartKey());
        keyRange.setStopKey(tablet.getEndKey());
        TScanRange scanRange = new TScanRange();
        scanRange.setKudu_key_range(keyRange);

        // Set the scan range for this set of locations
        locs.setScan_range(scanRange);
        locs.locations = locations;

        scanRanges_.add(locs);
      }
    } catch (Exception e) {
      throw new RuntimeException("Loading Kudu Table failed", e);
    } finally {
      try {
        client.shutdown();
      } catch (Exception e) {
        LOG.error("Error during shutdown of Kudu client.", e);
      }
    }
  }

  @Override
  protected void computeStats(Analyzer analyzer) {
    super.computeStats(analyzer);
    // Update the number of nodes to reflect the hosts that have relevant data.
    numNodes_ = hostIndexSet_.size();

    // Update the cardinality
    inputCardinality_ = cardinality_ = kuduTable_.getNumRows();
    cardinality_ *= computeSelectivity();
    cardinality_ = Math.max(1, cardinality_);
    cardinality_ = capAtLimit(cardinality_);
    LOG.debug("computeStats KuduScan: cardinality=" + Long.toString(cardinality_));
  }

  @Override
  protected String getNodeExplainString(String prefix, String detailPrefix,
      TExplainLevel detailLevel) {
    StringBuilder result = new StringBuilder();

    String aliasStr = desc_.hasExplicitAlias() ? " " + desc_.getAlias() : "";
    result.append(String.format("%s%s:%s [%s%s]\n", prefix, id_.toString(), displayName_,
        kuduTable_.getFullName(), aliasStr));

    switch (detailLevel) {
      case MINIMAL: break;
      case STANDARD: // Fallthrough intended.
      case EXTENDED: // Fallthrough intended.
      case VERBOSE: {
        if (!conjuncts_.isEmpty()) {
          result.append(detailPrefix + "predicates: " + getExplainString(conjuncts_)
              + "\n");
        }
        if (!kuduConjuncts_.isEmpty()) {
          result.append(detailPrefix + "kudu predicates: " + getExplainString(
              kuduConjuncts_));
        }
      }
    }
    return result.toString();
  }

  @Override
  protected void toThrift(TPlanNode node) {
    node.node_type = TPlanNodeType.KUDU_SCAN_NODE;
    node.kudu_scan_node = new TKuduScanNode(desc_.getId().asInt());

    // Thriftify the pushable predicates and set them on the scan node.
    for (Expr predicate : kuduConjuncts_) {
      node.kudu_scan_node.addToPushable_conjuncts(predicate.treeToThrift());
    }
  }

  /**
   * Extracts predicates that can be pushed down to Kudu. Currently only binary predicates
   * (<=, >=, ==) that have a constant expression on one side and a slot ref on the other
   * can be evaluated by Kudu. Only looks at comparisons of constants (i.e., the bounds
   * of the result can be evaluated with Expr::GetValue(NULL)).
   */
  private static List<Expr> extractKuduConjuncts(List<Expr> conjuncts, Analyzer analyzer)
      throws InternalException, AnalysisException {
    ImmutableList.Builder<Expr> pushableConjunctsBuilder = ImmutableList.builder();
    ListIterator<Expr> i = conjuncts.listIterator();
    while (i.hasNext()) {
      Expr e = i.next();
      if (!(e instanceof BinaryPredicate)) continue;
      BinaryPredicate comparisonPred = (BinaryPredicate) e;
      comparisonPred = normalizeBinaryPredicate(comparisonPred, analyzer);

      // Make sure the expression on the left is a bare SlotRef.
      // TODO KUDU-931 look into handling implicit/explicit casts on the SlotRef.
      Expr leftExpr = comparisonPred.getChild(0);
      if (!(leftExpr instanceof SlotRef)) continue;

      // Needs to have a literal on the right.
      if (!comparisonPred.getChild(1).isLiteral()) continue;

      Operator op = comparisonPred.getOp();
      switch (comparisonPred.getOp()) {
        case NE: continue;
        case GT: continue; // TODO Exclusive predicates are not supported in Kudu yet.
        case LT: continue; // TODO Exclusive predicates are not supported in Kudu yet.
        case GE: // Fallthrough intended.
        case LE: // Fallthrough intended.
        case EQ: {
          i.remove();
          pushableConjunctsBuilder.add(comparisonPred);
          break;
        }
        default:
          Preconditions.checkState(false, "Unexpected BinaryPredicate type: "
              + op.getName());
      }
    }
    return pushableConjunctsBuilder.build();
  }

  /**
   * Returns a version of 'predicate' that always has the SlotRef, if there is one, on the
   * left and the other expression on the right. This also folds constant children of the
   * predicate.
   *
   * TODO KUDU-930 move this to BinaryPredicate as a static helper
   */
  private static BinaryPredicate normalizeBinaryPredicate(BinaryPredicate predicate,
      Analyzer analyzer)
      throws AnalysisException {
    SlotRef ref = predicate.getBoundSlot();
    if (ref == predicate.getChild(0).unwrapSlotRef(true)) return predicate;
    Preconditions.checkState(ref == predicate.getChild(1).unwrapSlotRef(true));
    BinaryPredicate pred = new BinaryPredicate(predicate.getOp().converse(), ref,
        predicate.getChild(0));
    pred.analyzeNoThrow(analyzer);
    pred.foldConstantChildren(analyzer);
    pred.analyzeNoThrow(analyzer);
    return pred;
  }
}
