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

import java.util.List;
import java.util.Map;
import java.util.Set;

abstract class IcebergModifyImpl extends ModifyImpl {
  FeIcebergTable originalTargetTable_;
  IcebergPositionDeleteTable icePosDelTable_;

  public IcebergModifyImpl(ModifyStmt modifyStmt) {
    super(modifyStmt);
    originalTargetTable_ = (FeIcebergTable)modifyStmt_.getTargetTable();
    icePosDelTable_ = new IcebergPositionDeleteTable(originalTargetTable_);
  }

  @Override
  public void analyze(Analyzer analyzer) throws AnalysisException {
    // Make the virtual position delete table the new target table.
    modifyStmt_.setTargetTable(icePosDelTable_);
    modifyStmt_.setMaxTableSinks(analyzer.getQueryOptions().getMax_fs_writers());
    if (modifyStmt_ instanceof UpdateStmt) {
      throw new AnalysisException("UPDATE is not supported for Iceberg table " +
          originalTargetTable_.getFullName());
    }

    if (icePosDelTable_.getFormatVersion() == 1) {
      throw new AnalysisException("Iceberg V1 table do not support DELETE/UPDATE " +
          "operations: " + originalTargetTable_.getFullName());
    }

    String deleteMode = originalTargetTable_.getIcebergApiTable().properties().get(
        org.apache.iceberg.TableProperties.DELETE_MODE);
    if (deleteMode != null && !deleteMode.equals("merge-on-read")) {
      throw new AnalysisException(String.format("Unsupported delete mode: '%s' for " +
          "Iceberg table: %s", deleteMode, originalTargetTable_.getFullName()));
    }

    if (originalTargetTable_.getDeleteFileFormat() != TIcebergFileFormat.PARQUET) {
      throw new AnalysisException("Impala can only write delete files in PARQUET, " +
          "but the given table uses a different file format: " +
          originalTargetTable_.getFullName());
    }

    Expr wherePredicate = modifyStmt_.getWherePredicate();
    if (wherePredicate == null ||
        org.apache.impala.analysis.Expr.IS_TRUE_LITERAL.apply(wherePredicate)) {
      // TODO (IMPALA-12136): rewrite DELETE FROM t; statements to TRUNCATE TABLE t;
      throw new AnalysisException("For deleting every row, please use TRUNCATE.");
    }
  }

  @Override
  public void addCastsToAssignmentsInSourceStmt(Analyzer analyzer)
      throws AnalysisException {
  }

  @Override
  public void addKeyColumns(Analyzer analyzer, List<SelectListItem> selectList,
      List<Integer> referencedColumns, Set<SlotId> uniqueSlots, Set<SlotId> keySlots,
      Map<String, Integer> colIndexMap) throws AnalysisException {
    if (originalTargetTable_.isPartitioned()) {
      String[] partitionCols;
      partitionCols = new String[] {"PARTITION__SPEC__ID",
          "ICEBERG__PARTITION__SERIALIZED"};
      for (String k : partitionCols) {
        addPartitioningColumn(analyzer, selectList, referencedColumns, uniqueSlots,
            keySlots, colIndexMap, k);
      }
    }
    String[] deleteCols;
    deleteCols = new String[] {"INPUT__FILE__NAME", "FILE__POSITION"};
    // Add the key columns as slot refs
    for (String k : deleteCols) {
      addKeyColumn(analyzer, selectList, referencedColumns, uniqueSlots, keySlots,
          colIndexMap, k, true);
    }
  }
}
