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

import org.apache.impala.catalog.FeKuduTable;
import org.apache.impala.common.AnalysisException;
import org.apache.impala.common.Pair;

import java.util.List;
import java.util.Map;
import java.util.Set;

abstract class KuduModifyImpl extends ModifyImpl {
  // Target Kudu table.
  FeKuduTable kuduTable_;

  public KuduModifyImpl(ModifyStmt modifyStmt) {
    super(modifyStmt);
    kuduTable_ = (FeKuduTable)modifyStmt.getTargetTable();
  }

  @Override
  public void analyze(Analyzer analyzer) throws AnalysisException {}

  @Override
  public void addCastsToAssignmentsInSourceStmt(Analyzer analyzer)
      throws AnalysisException {
    // cast result expressions to the correct type of the referenced slot of the
    // target table
    List<Pair<SlotRef, Expr>> assignments = modifyStmt_.getAssignments();
    int keyColumnsOffset = kuduTable_.getPrimaryKeyColumnNames().size();
    for (int i = keyColumnsOffset; i < sourceStmt_.resultExprs_.size(); ++i) {
      sourceStmt_.resultExprs_.set(i, sourceStmt_.resultExprs_.get(i).castTo(
          assignments.get(i - keyColumnsOffset).first.getType()));
    }
  }

  @Override
  public void addKeyColumns(Analyzer analyzer, List<SelectListItem> selectList,
      List<Integer> referencedColumns, Set<SlotId> uniqueSlots, Set<SlotId> keySlots,
      Map<String, Integer> colIndexMap) throws AnalysisException {
    // Add the key columns as slot refs
    for (String k : kuduTable_.getPrimaryKeyColumnNames()) {
      addKeyColumn(analyzer, selectList, referencedColumns, uniqueSlots, keySlots,
          colIndexMap, k, false);
    }
  }
}
