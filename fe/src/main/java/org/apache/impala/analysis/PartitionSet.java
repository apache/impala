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

package org.apache.impala.analysis;

import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.apache.impala.analysis.BinaryPredicate.Operator;
import org.apache.impala.catalog.Column;
import org.apache.impala.catalog.FeFsPartition;
import org.apache.impala.catalog.FeIcebergTable;
import org.apache.impala.catalog.FeTable;
import org.apache.impala.common.AnalysisException;
import org.apache.impala.common.ImpalaException;
import org.apache.impala.common.Reference;
import org.apache.impala.planner.HdfsPartitionPruner;
import org.apache.impala.thrift.TPartitionKeyValue;

/**
 * Represents a set of partitions resulting from evaluating a list of partition conjuncts
 * against a table's partition list.
 */
public class PartitionSet extends PartitionSpecBase {
  private final List<Expr> partitionExprs_;

  // Result of analysis, null until analysis is complete.
  private List<? extends FeFsPartition> partitions_;
  public PartitionSet(List<Expr> partitionExprs) {
    this.partitionExprs_ = ImmutableList.copyOf(partitionExprs);
  }
  public List<? extends FeFsPartition> getPartitions() { return partitions_; }
  public List<Expr> getPartitionExprs() { return partitionExprs_; }

  @Override
  public void analyze(Analyzer analyzer) throws AnalysisException {
    super.analyze(analyzer);
    if(table_ instanceof FeIcebergTable) return;
    List<Expr> conjuncts = new ArrayList<>();
    // Do not register column-authorization requests.
    analyzer.setEnablePrivChecks(false);
    for (Expr e : partitionExprs_) {
      e.analyze(analyzer);
      e.checkReturnsBool("Partition expr", false);
      conjuncts.addAll(e.getConjuncts());
    }

    TupleDescriptor desc = analyzer.getDescriptor(tableName_.toString());
    List<SlotId> partitionSlots = desc.getPartitionSlots();
    for (Expr e : conjuncts) {
      analyzer.getConstantFolder().rewrite(e, analyzer);
      // Make sure there are no constant predicates in the partition exprs.
      if (e.isConstant()) {
        throw new AnalysisException(String.format("Invalid partition expr %s. A " +
            "partition spec may not contain constant predicates.", e.toSql()));
      }

      // Make sure every conjunct only contains partition slot refs.
      if (!e.isBoundBySlotIds(partitionSlots)) {
        throw new AnalysisException("Partition exprs cannot contain non-partition " +
            "column(s): " + e.toSql() + ".");
      }
    }

    List<Expr> transformedConjuncts = transformPartitionConjuncts(analyzer, conjuncts);
    addIfExists(analyzer, table_, transformedConjuncts);

    try {
      HdfsPartitionPruner pruner = new HdfsPartitionPruner(desc);
      partitions_ = pruner.prunePartitions(analyzer, transformedConjuncts, true,
          null).first;
    } catch (ImpalaException e) {
      if (e instanceof AnalysisException) throw (AnalysisException)e;
      throw new AnalysisException("Partition expr evaluation failed in the backend.", e);
    }

    if (partitionShouldExist_ != null) {
      if (partitionShouldExist_ && partitions_.isEmpty()) {
        throw new AnalysisException("No matching partition(s) found.");
      } else if (!partitionShouldExist_ && !partitions_.isEmpty()) {
        throw new AnalysisException("Partition already exists.");
      }
    }
    analyzer.setEnablePrivChecks(true);
  }

  // Check if we should add IF EXISTS. Fully-specified partition specs don't add it for
  // backwards compatibility, while more general partition expressions or partially
  // specified partition specs add IF EXISTS by setting partitionShouldExists_ to null.
  // The given conjuncts are assumed to only reference partition columns.
  private void addIfExists(
      Analyzer analyzer, FeTable table, List<Expr> conjuncts) {
    boolean add = false;
    Set<String> partColNames = new HashSet<>();
    Reference<SlotRef> slotRef = new Reference<>();
    for (Expr e : conjuncts) {
      if (e instanceof BinaryPredicate) {
        BinaryPredicate bp = (BinaryPredicate) e;
        if (bp.getOp() != Operator.EQ || !bp.isSingleColumnPredicate(slotRef, null)) {
          add = true;
          break;
        }
        Column partColumn = slotRef.getRef().getDesc().getColumn();
        Preconditions.checkState(table.isClusteringColumn(partColumn));
        partColNames.add(partColumn.getName());
      } else if (e instanceof IsNullPredicate) {
        IsNullPredicate nullPredicate = (IsNullPredicate) e;
        Column partColumn = nullPredicate.getBoundSlot().getDesc().getColumn();
        Preconditions.checkState(table.isClusteringColumn(partColumn));
        partColNames.add(partColumn.getName());
      } else {
        add = true;
        break;
      }
    }

    if (add || partColNames.size() < table.getNumClusteringCols()) {
      partitionShouldExist_ = null;
    }
  }

  // Transform <COL> = NULL into IsNull expr; <String COL> = '' into IsNull expr.
  // The reason is that COL = NULL is allowed for selecting the NULL
  // partition, but a COL = NULL predicate can never be true, so we
  // need to transform such predicates before feeding them into the
  // partition pruner.
  private List<Expr> transformPartitionConjuncts(Analyzer analyzer, List<Expr> conjuncts)
      throws AnalysisException {
    List<Expr> transformedConjuncts = new ArrayList<>();
    for (Expr e : conjuncts) {
      Expr result = e;
      if (e instanceof BinaryPredicate) {
        BinaryPredicate bp = ((BinaryPredicate) e);
        if (bp.getOp() == Operator.EQ) {
          SlotRef leftChild =
              bp.getChild(0) instanceof SlotRef ? ((SlotRef) bp.getChild(0)) : null;
          NullLiteral nullChild = Expr.IS_NULL_LITERAL.apply(bp.getChild(1)) ?
              ((NullLiteral) bp.getChild(1)) : null;
          StringLiteral stringChild = bp.getChild(1) instanceof StringLiteral ?
              ((StringLiteral) bp.getChild(1)) : null;
          if (leftChild != null && nullChild != null) {
            result = new IsNullPredicate(leftChild, false);
          } else if (leftChild != null && stringChild != null) {
            if (stringChild.getStringValue().isEmpty()) {
              result = new IsNullPredicate(leftChild, false);
            }
          }
        }
      }
      result.analyze(analyzer);
      transformedConjuncts.add(result);
    }
    return transformedConjuncts;
  }

  public List<List<TPartitionKeyValue>> toThrift() {
    List<List<TPartitionKeyValue>> thriftPartitionSet = new ArrayList<>();
    for (FeFsPartition hdfsPartition : partitions_) {
      List<TPartitionKeyValue> thriftPartitionSpec = new ArrayList<>();
      for (int i = 0; i < table_.getNumClusteringCols(); ++i) {
        String key = table_.getColumns().get(i).getName();
        String value = PartitionKeyValue.getPartitionKeyValueString(
            hdfsPartition.getPartitionValue(i), nullPartitionKeyValue_);
        thriftPartitionSpec.add(new TPartitionKeyValue(key, value));
      }
      thriftPartitionSet.add(thriftPartitionSpec);
    }
    return thriftPartitionSet;
  }

  @Override
  public String toSql(ToSqlOptions options) {
    List<String> partitionExprStr = new ArrayList<>();
    for (Expr e : partitionExprs_) {
      partitionExprStr.add(e.toSql(options));
    }
    return String.format("PARTITION (%s)", Joiner.on(", ").join(partitionExprStr));
  }
}
