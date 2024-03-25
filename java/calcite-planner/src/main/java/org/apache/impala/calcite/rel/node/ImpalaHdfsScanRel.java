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

package org.apache.impala.calcite.rel.node;

import com.google.common.base.Preconditions;

import org.apache.calcite.rel.core.TableScan;
import org.apache.impala.analysis.BaseTableRef;
import org.apache.impala.analysis.Expr;
import org.apache.impala.analysis.SlotDescriptor;
import org.apache.impala.analysis.SlotRef;
import org.apache.impala.analysis.TupleDescriptor;
import org.apache.impala.calcite.rel.phys.ImpalaHdfsScanNode;
import org.apache.impala.calcite.rel.util.ExprConjunctsConverter;
import org.apache.impala.calcite.schema.CalciteTable;
import org.apache.impala.catalog.FeFsPartition;
import org.apache.impala.catalog.HdfsTable;
import org.apache.impala.common.ImpalaException;
import org.apache.impala.planner.PlanNode;
import org.apache.impala.planner.PlanNodeId;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * ImpalaHdfsScanRel. Calcite RelNode which maps to an Impala TableScan node.
 */
public class ImpalaHdfsScanRel extends TableScan
    implements ImpalaPlanRel {

  public ImpalaHdfsScanRel(TableScan scan) {
    super(scan.getCluster(), scan.getTraitSet(), scan.getHints(), scan.getTable());
  }

  @Override
  public NodeWithExprs getPlanNode(ParentPlanRelContext context) throws ImpalaException {

    CalciteTable table = (CalciteTable) getTable();

    BaseTableRef baseTblRef =
        table.createBaseTableRef(context.ctx_.getRootAnalyzer());

    // Create the Tuple Descriptor which will contain only the relevant columns
    // from the table needed for the query.
    TupleDescriptor tupleDesc = table.createTupleAndSlotDesc(baseTblRef,
        getInputRefFieldNames(context), context.ctx_.getRootAnalyzer());

    // outputExprs will contain all the needed columns from the table
    List<Expr> outputExprs = createScanOutputExprs(tupleDesc.getSlots());

    // break up the filter condition (if given) to ones that can be used for
    // partition pruning and ones that cannot.
    ExprConjunctsConverter converter = new ExprConjunctsConverter(
        context.filterCondition_, outputExprs, getCluster().getRexBuilder(),
        context.ctx_.getRootAnalyzer());
    List<? extends FeFsPartition> impalaPartitions = table.getPrunedPartitions(
        context.ctx_.getRootAnalyzer(), tupleDesc);

    // TODO: All conjuncts will be nonpartitioned conjuncts until the partition
    // pruning feature is committed.
    List<Expr> filterConjuncts = converter.getImpalaConjuncts();
    List<Expr> partitionConjuncts = new ArrayList<>();

    PlanNodeId nodeId = context.ctx_.getNextNodeId();

    PlanNode physicalNode = new ImpalaHdfsScanNode(nodeId, tupleDesc, impalaPartitions,
        baseTblRef, null, partitionConjuncts, filterConjuncts);
    physicalNode.init(context.ctx_.getRootAnalyzer());

    return new NodeWithExprs(physicalNode, outputExprs);
  }

  /**
   * Return a list of (SlotRef) expressions for the scan node.  The list will
   * be the size of the # of columns in the table. We initialize the list to
   * contain null values for all elements.
   *
   * If a column isn't projected out by the parent of the scan node, the array
   * location for the column will remain null.
   */
  private List<Expr> createScanOutputExprs(List<SlotDescriptor> slotDescs) {
    int totalCols = getRowType().getFieldNames().size();

    // Initialize all fields to null.
    // TODO: Should this be a map instead of a list?  See IMPALA-12961 for details.
    List<Expr> scanOutputExprs = new ArrayList<>(Collections.nCopies(totalCols, null));

    HdfsTable table = ((CalciteTable) getTable()).getHdfsTable();
    Preconditions.checkState(totalCols == table.getColumns().size());
    int nonPartitionedCols = totalCols - table.getNumClusteringCols();
    for (SlotDescriptor slotDesc : slotDescs) {
      // On a "select *" with partitioned columns, the partition columns occur after the
      // nonpartitioned columns.  But Impala displays the partition columns first. The
      // modular arithmetic provides the correct position number for Impala.
      int position =
          (slotDesc.getColumn().getPosition() + nonPartitionedCols) % totalCols;
      scanOutputExprs.set(position, new SlotRef(slotDesc));
    }
    return scanOutputExprs;
  }


  private List<String> getInputRefFieldNames(ParentPlanRelContext context) {
    // If the parent context didn't pass in input refs, we will select all the
    // columns from the table.
    if (context.inputRefs_ == null) {
      return getRowType().getFieldNames();
    }

    List<String> inputRefFieldNames = new ArrayList<>();
    for (Integer i : context.inputRefs_) {
      inputRefFieldNames.add(getRowType().getFieldNames().get(i));
    }
    return inputRefFieldNames;
  }
}
