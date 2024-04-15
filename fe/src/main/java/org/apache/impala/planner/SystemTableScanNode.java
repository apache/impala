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

import org.apache.impala.analysis.Analyzer;
import org.apache.impala.analysis.Expr;
import org.apache.impala.analysis.TupleDescriptor;
import org.apache.impala.catalog.FeSystemTable;
import org.apache.impala.common.ImpalaException;
import org.apache.impala.common.InternalException;
import org.apache.impala.service.FeSupport;
import org.apache.impala.thrift.TAddressesList;
import org.apache.impala.thrift.TExplainLevel;
import org.apache.impala.thrift.TNetworkAddress;
import org.apache.impala.thrift.TPlanNode;
import org.apache.impala.thrift.TPlanNodeType;
import org.apache.impala.thrift.TQueryOptions;
import org.apache.impala.thrift.TScanRange;
import org.apache.impala.thrift.TScanRangeLocation;
import org.apache.impala.thrift.TScanRangeLocationList;
import org.apache.impala.thrift.TScanRangeSpec;
import org.apache.impala.thrift.TSystemTableScanNode;
import com.google.common.base.MoreObjects;
import com.google.common.collect.Lists;

public class SystemTableScanNode extends ScanNode {
  public SystemTableScanNode(PlanNodeId id, TupleDescriptor desc) {
    super(id, desc, "SCAN SYSTEM_TABLE");
    table_ = (FeSystemTable) desc_.getTable();
  }

  private final FeSystemTable table_;

  @Override
  public void init(Analyzer analyzer) throws ImpalaException {
    assignConjuncts(analyzer);
    analyzer.createEquivConjuncts(tupleIds_.get(0), conjuncts_);
    conjuncts_ = orderConjunctsByCost(conjuncts_);
    // materialize slots in remaining conjuncts_
    analyzer.materializeSlots(conjuncts_);
    computeMemLayout(analyzer);
    computeScanRangeLocations(analyzer);
    computeStats(analyzer);
  }

  /**
   * Create a single scan range for each coordinator in cluster.
   */
  private void computeScanRangeLocations(Analyzer analyzer) throws InternalException {
    TAddressesList coordinators_container = FeSupport.GetCoordinators();
    List<TNetworkAddress> coordinators = coordinators_container.getAddresses();

    scanRangeSpecs_ = new TScanRangeSpec();
    for (TNetworkAddress networkAddress: coordinators) {
      // Translate from network address to the global (to this request) host index.
      int globalHostIdx = analyzer.getHostIndex().getOrAddIndex(networkAddress);
      TScanRange range = new TScanRange();
      // Enable scheduling scan ranges to coordinators independent of the executor group
      // or whether they're provisioned as executors. Currently only addresses
      // coordinators, but could be expanded to addressing all impalad backends for other
      // system tables.
      range.setIs_system_scan(true);
      scanRangeSpecs_.addToConcrete_ranges(new TScanRangeLocationList(
        range, Lists.newArrayList(new TScanRangeLocation(globalHostIdx))));
    }
    analyzer.setIncludeAllCoordinatorsInScheduling(true);
  }

  @Override
  public void computeProcessingCost(TQueryOptions queryOptions) {
    // Only cost is serialization as we're grabbing existing data from memory.
    processingCost_ = ProcessingCost.zero();
  }

  @Override
  public void computeStats(Analyzer analyzer) {
    super.computeStats(analyzer);
    inputCardinality_ = FeSupport.NumLiveQueries();
    cardinality_ = inputCardinality_;
    cardinality_ = applyConjunctsSelectivity(cardinality_);
    cardinality_ = Math.max(1, cardinality_);
    cardinality_ = capCardinalityAtLimit(cardinality_);
    numInstances_ = numNodes_ = scanRangeSpecs_.getConcrete_rangesSize();
  }

  @Override
  protected String debugString() {
    return MoreObjects.toStringHelper(this)
        .add("tid", desc_.getId().asInt())
        .add("TblName", desc_.getTable().getFullName())
        .addValue(super.debugString())
        .toString();
  }

  @Override
  protected void toThrift(TPlanNode msg) {
    msg.node_type = TPlanNodeType.SYSTEM_TABLE_SCAN_NODE;
    msg.system_table_scan_node =
        new TSystemTableScanNode(desc_.getId().asInt(), table_.getSystemTableName());
  }

  @Override
  public void computeNodeResourceProfile(TQueryOptions queryOptions) {
    // Resource requirements are low since we're grabbing existing data from memory.
    nodeResourceProfile_ = ResourceProfile.noReservation(1024L * 1024L);
  }

  @Override
  protected String getNodeExplainString(
      String prefix, String detailPrefix, TExplainLevel detailLevel) {
    StringBuilder output = new StringBuilder();
    String aliasStr = "";
    if (!table_.getFullName().equalsIgnoreCase(desc_.getAlias())
        && !table_.getName().equalsIgnoreCase(desc_.getAlias())) {
      aliasStr = " " + desc_.getAlias();
    }

    output.append(String.format("%s%s:%s [%s%s]\n", prefix, id_.toString(), displayName_,
        table_.getFullName(), aliasStr));

    if (!conjuncts_.isEmpty()) {
      output.append(prefix + "predicates: " +
          Expr.getExplainString(conjuncts_, detailLevel) + "\n");
    }

    // Add table and column stats in verbose mode.
    if (detailLevel == TExplainLevel.VERBOSE) {
      output.append(getStatsExplainString(prefix));
      output.append("\n");
    }
    return output.toString();
  }
}
