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

import java.util.List;

import org.apache.impala.catalog.FeIcebergTable;
import org.apache.impala.common.AnalysisException;
import org.apache.impala.common.Pair;
import org.apache.impala.planner.DataSink;
import org.apache.impala.planner.IcebergBufferedDeleteSink;
import org.apache.impala.planner.TableSink;
import org.apache.impala.thrift.TSortingOrder;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import org.apache.impala.util.ExprUtil;

public class IcebergDeleteImpl extends IcebergModifyImpl {
  public IcebergDeleteImpl(ModifyStmt modifyStmt) {
    super(modifyStmt);
  }

  @Override
  public void analyze(Analyzer analyzer) throws AnalysisException {
    super.analyze(analyzer);
    // Make the virtual position delete table the new target table.
    modifyStmt_.setTargetTable(icePosDelTable_);

    String deleteMode = originalTargetTable_.getIcebergApiTable().properties().get(
      org.apache.iceberg.TableProperties.DELETE_MODE);
    if (deleteMode != null && !deleteMode.equals("merge-on-read")) {
      throw new AnalysisException(String.format("Unsupported delete mode: '%s' for " +
          "Iceberg table: %s", deleteMode, originalTargetTable_.getFullName()));
    }

    Expr wherePredicate = modifyStmt_.getWherePredicate();
    if (wherePredicate == null ||
        org.apache.impala.analysis.Expr.IS_TRUE_LITERAL.apply(wherePredicate)) {
      // TODO (IMPALA-12136): rewrite DELETE FROM t; statements to TRUNCATE TABLE t;
      throw new AnalysisException("For deleting every row, please use TRUNCATE.");
    }
  }

  @Override
  protected void buildAndValidateSelectExprs(Analyzer analyzer,
      List<SelectListItem> selectList)
      throws AnalysisException {
    deletePartitionKeyExprs_ = getDeletePartitionExprs(analyzer);
    deleteResultExprs_ = getDeleteResultExprs(analyzer);
    selectList.addAll(ExprUtil.exprsAsSelectList(deletePartitionKeyExprs_));
    selectList.addAll(ExprUtil.exprsAsSelectList(deleteResultExprs_));
  }

  @Override
  public List<Expr> getPartitionKeyExprs() { return deletePartitionKeyExprs_; }

  @Override
  public void addCastsToAssignmentsInSourceStmt(Analyzer analyzer)
      throws AnalysisException {
    // No-op
  }

  @Override
  public DataSink createDataSink() {
    Preconditions.checkState(modifyStmt_.table_ instanceof FeIcebergTable);
    return new IcebergBufferedDeleteSink(icePosDelTable_, deletePartitionKeyExprs_,
        deleteResultExprs_);
  }
}