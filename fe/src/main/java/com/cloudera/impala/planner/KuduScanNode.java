package com.cloudera.impala.planner;

import java.util.List;
import java.util.Set;

import org.kududb.Common.HostPortPB;
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
 * Implements the frontend representation of a Kudu scan node.
 */
public class KuduScanNode extends ScanNode {

  private final static Logger LOG = LoggerFactory.getLogger(KuduScanNode.class);

  private final KuduTable kuduTable_;

  private final Set<Integer> hostIndexSet_ = Sets.newHashSet();

  public KuduScanNode(PlanNodeId id, TupleDescriptor desc) {
    super(id, desc, "SCAN KUDU");
    kuduTable_ = (KuduTable) desc_.getTable();
  }

    @Override
  public void init(Analyzer analyzer) throws InternalException {
    // also add remaining unassigned conjuncts
    assignConjuncts(analyzer);

    analyzer.createEquivConjuncts(tupleIds_.get(0), conjuncts_);

    computeScanRangeLocations(analyzer);

    // mark all slots referenced by the remaining conjuncts as materialized
    markSlotsMaterialized(analyzer, conjuncts_);
    computeMemLayout(analyzer);

    computeStats(analyzer);
  }

  @Override
  protected void computeStats(Analyzer analyzer) {
    super.computeStats(analyzer);

    // Update the number of nodes to reflect the hosts that have data
    numNodes_ = hostIndexSet_.size();
  }

  @Override
  protected String getNodeExplainString(String prefix, String detailPrefix,
      TExplainLevel detailLevel) {
    StringBuilder b = new StringBuilder();

    String aliasStr = desc_.hasExplicitAlias() ? desc_.getAlias() : "";
    b.append(String.format("%s%s:%s [%s%s]\n", prefix, id_.toString(),
        displayName_, kuduTable_.getFullName(), aliasStr));

    // Print the conjuncts on the scan node
    if (!conjuncts_.isEmpty()) {
      b.append(detailPrefix + "predicates: " + getExplainString(conjuncts_) + "\n");
    }
    return b.toString();
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
        TScanRangeLocations locs = new TScanRangeLocations();
        // We're using the leader to avoid querying a lagging replica. This hinders
        // scheduling a scan with different replicas.
        LocatedTablet.Replica replica = tablet.getLeaderReplica();
        HostPortPB host = replica.getRpcHostPort();
        TNetworkAddress address = new TNetworkAddress(host.getHost(), host.getPort());

        // Use the network address to look up the host in the global list
        Integer hostIndex = analyzer.getHostIndex().getIndex(address);
        locs.addToLocations(new TScanRangeLocation(hostIndex));
        hostIndexSet_.add(hostIndex);

        // Now set the scan range of this tablet
        TKuduKeyRange keyRange = new TKuduKeyRange();
        keyRange.setStartKey(tablet.getStartKey());
        keyRange.setStopKey(tablet.getEndKey());
        TScanRange scanRange = new TScanRange();
        scanRange.setKudu_key_range(keyRange);

        // Set the scan range for this set of locations
        locs.setScan_range(scanRange);
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
  protected void toThrift(TPlanNode node) {
    node.node_type = TPlanNodeType.KUDU_SCAN_NODE;
    node.kudu_scan_node = new TKuduScanNode(desc_.getId().asInt());
  }
}
