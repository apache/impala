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

import org.apache.impala.catalog.FeIcebergTable;
import org.apache.impala.catalog.IcebergPositionDeleteTable;
import org.apache.impala.common.AnalysisException;
import org.apache.impala.thrift.TIcebergFileFormat;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import com.google.common.collect.Lists;

abstract class IcebergModifyImpl extends ModifyImpl {
  FeIcebergTable originalTargetTable_;
  IcebergPositionDeleteTable icePosDelTable_;

  /////////////////////////////////////////
  // START: Members that are set in buildAndValidateSelectExprs().

  // All Iceberg modify statements (DELETE, UPDATE) need delete exprs.
  protected List<Expr> deleteResultExprs_ = new ArrayList<>();
  protected List<Expr> deletePartitionKeyExprs_ = new ArrayList<>();

  // For every column of the target table that is referenced in the optional
  // 'sort.columns' table property, this list will contain the corresponding result expr
  // from 'resultExprs_'. Before insertion, all rows will be sorted by these exprs. If the
  // list is empty, no additional sorting by non-partitioning columns will be performed.
  // The column list must not contain partition columns and must be empty for non-Hdfs
  // tables.
  protected List<Expr> sortExprs_ = new ArrayList<>();
  // END: Members that are set in buildAndValidateSelectExprs().
  /////////////////////////////////////////

  public IcebergModifyImpl(ModifyStmt modifyStmt) {
    super(modifyStmt);
    originalTargetTable_ = (FeIcebergTable)modifyStmt_.getTargetTable();
    icePosDelTable_ = new IcebergPositionDeleteTable(originalTargetTable_);
  }

  @Override
  public void analyze(Analyzer analyzer) throws AnalysisException {
    modifyStmt_.setMaxTableSinks(analyzer.getQueryOptions().getMax_fs_writers());

    if (icePosDelTable_.getFormatVersion() == 1) {
      throw new AnalysisException("Iceberg V1 table do not support DELETE/UPDATE " +
          "operations: " + originalTargetTable_.getFullName());
    }

    if (originalTargetTable_.getDeleteFileFormat() != TIcebergFileFormat.PARQUET) {
      throw new AnalysisException("Impala can only write delete files in PARQUET, " +
          "but the given table uses a different file format: " +
          originalTargetTable_.getFullName());
    }
  }

  @Override
  public List<Expr> getSortExprs() {
    return sortExprs_;
  }

  @Override
  public void substituteResultExprs(ExprSubstitutionMap smap, Analyzer analyzer) {
    super.substituteResultExprs(smap, analyzer);
    sortExprs_ = Expr.substituteList(sortExprs_, smap, analyzer, true);
    deleteResultExprs_ = Expr.substituteList(deleteResultExprs_, smap, analyzer, true);
    deletePartitionKeyExprs_ = Expr.substituteList(
        deletePartitionKeyExprs_, smap, analyzer, true);
  }

  public List<Expr> getDeletePartitionExprs(Analyzer analyzer) throws AnalysisException {
    if (!originalTargetTable_.isPartitioned()) return Collections.emptyList();
    return getSlotRefs(analyzer, Lists.newArrayList(
        "PARTITION__SPEC__ID", "ICEBERG__PARTITION__SERIALIZED"));
  }

  public List<Expr> getDeleteResultExprs(Analyzer analyzer) throws AnalysisException {
    return getSlotRefs(analyzer, Lists.newArrayList(
        "INPUT__FILE__NAME", "FILE__POSITION"));
  }

  private List<Expr> getSlotRefs(Analyzer analyzer, List<String> cols)
      throws AnalysisException {
    List<Expr> ret = new ArrayList<>();
    for (String col : cols) {
      ret.add(createSlotRef(analyzer, col));
    }
    return ret;
  }

  protected SlotRef createSlotRef(Analyzer analyzer, String colName)
      throws AnalysisException {
    List<String> path = Path.createRawPath(modifyStmt_.targetTableRef_.getUniqueAlias(),
        colName);
    SlotRef ref = new SlotRef(path);
    ref.analyze(analyzer);
    return ref;
  }
}
