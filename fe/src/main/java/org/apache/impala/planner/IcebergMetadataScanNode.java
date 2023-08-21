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
import org.apache.impala.analysis.TableRef;
import org.apache.impala.catalog.iceberg.IcebergMetadataTable;
import org.apache.impala.common.ImpalaException;
import org.apache.impala.thrift.TExplainLevel;
import org.apache.impala.thrift.TIcebergMetadataScanNode;
import org.apache.impala.thrift.TPlanNode;
import org.apache.impala.thrift.TPlanNodeType;
import org.apache.impala.thrift.TQueryOptions;
import org.apache.impala.thrift.TScanRangeSpec;
import org.apache.impala.thrift.TTableName;

import com.google.common.base.Preconditions;

public class IcebergMetadataScanNode extends ScanNode {

  // Metadata table name, it is stored here so it can be passed to the backend.
  protected final String metadataTableName_;

  protected IcebergMetadataScanNode(PlanNodeId id, List<Expr> conjuncts,
      TableRef tblRef) {
    super(id, tblRef.getDesc(), "SCAN ICEBERG METADATA");
    conjuncts_ = conjuncts;
    metadataTableName_ = ((IcebergMetadataTable)tblRef.getTable()).getMetadataTableName();
  }

  @Override
  public void init(Analyzer analyzer) throws ImpalaException {
    scanRangeSpecs_ = new TScanRangeSpec();
    assignConjuncts(analyzer);
    conjuncts_ = orderConjunctsByCost(conjuncts_);
    analyzer.materializeSlots(conjuncts_);
    computeMemLayout(analyzer);
    computeStats(analyzer);
    numInstances_ = 1;
    numNodes_ = 1;
  }

  @Override
  protected String getDisplayLabelDetail() {
    Preconditions.checkNotNull(desc_.getPath());
    if (desc_.hasExplicitAlias()) {
      return desc_.getPath().toString() + " " + desc_.getAlias();
    } else {
      return desc_.getPath().toString();
    }
  }

  @Override
  protected String getNodeExplainString(String rootPrefix, String detailPrefix,
      TExplainLevel detailLevel) {
    StringBuilder output = new StringBuilder();
    output.append(String.format("%s%s [%s]\n", rootPrefix, getDisplayLabel(),
        getDisplayLabelDetail()));
    return output.toString();
  }

  @Override
  protected void toThrift(TPlanNode msg) {
    msg.iceberg_scan_metadata_node = new TIcebergMetadataScanNode();
    msg.node_type = TPlanNodeType.ICEBERG_METADATA_SCAN_NODE;
    msg.iceberg_scan_metadata_node.table_name =
        new TTableName(desc_.getTableName().getDb(), desc_.getTableName().getTbl());
    msg.iceberg_scan_metadata_node.metadata_table_name = metadataTableName_;
    msg.iceberg_scan_metadata_node.tuple_id = desc_.getId().asInt();
  }

  @Override
  public void computeNodeResourceProfile(TQueryOptions queryOptions) {
    nodeResourceProfile_ = ResourceProfile.noReservation(0);
  }

  @Override
  public void computeProcessingCost(TQueryOptions queryOptions) {
    processingCost_ = computeDefaultProcessingCost();
  }

}
