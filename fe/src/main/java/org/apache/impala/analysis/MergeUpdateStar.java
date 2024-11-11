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

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import java.util.Collections;
import java.util.List;
import java.util.StringJoiner;
import org.apache.impala.common.AnalysisException;
import org.apache.impala.common.Pair;

/**
 * Special UPDATE clause for MERGE statements, tries to match every source expression
 * brought by source table or source subquery against the target table's column list.
 */
public class MergeUpdateStar extends MergeUpdate {

  public MergeUpdateStar() {
    super(Collections.emptyList());
  }

  @Override
  public void analyze(Analyzer analyzer) throws AnalysisException {
    assignmentExprs_ = Lists.newArrayList();
    Preconditions.checkNotNull(targetTableColumns_);
    Preconditions.checkNotNull(sourceTableRef_);
    List<String> sourceColumnLabels = getSourceColumnLabels();
    String tableName = sourceTableRef_.getUniqueAlias();

    checkSize(sourceColumnLabels);

    for (int i = 0; i < targetTableColumns_.size(); i++) {
      SlotRef sourceExpr = DmlStatementBase.createSlotRef(analyzer, tableName,
          sourceColumnLabels.get(i));
      SlotRef targetColumn = DmlStatementBase.createSlotRef(analyzer,
          targetTableRef_.getUniqueAlias(),
          targetTableColumns_.get(i).getName());
      assignmentExprs_.add(Pair.create(targetColumn, sourceExpr));
    }
    super.analyze(analyzer);
  }

  private void checkSize(List<String> sourceColumnLabels) throws AnalysisException {
    int sourceColSize = sourceColumnLabels.size();
    int targetColSize = targetTableColumns_.size();
    if (targetColSize < sourceColSize) {
      throw new AnalysisException(String.format(
          "Target table has fewer columns (%d) than the source expression (%d): %s",
          targetColSize, sourceColSize, toSql()));
    }
    if (targetColSize > sourceColSize) {
      throw new AnalysisException(String.format(
          "Target table has more columns (%d) than the source expression (%d): %s",
          targetColSize, sourceColSize, toSql()));
    }
  }

  @Override
  protected String listAssignments(ToSqlOptions options) {
    List<String> sourceColumnLabels = getSourceColumnLabels();
    int minAvailableItems = Math.min(sourceColumnLabels.size(),
        targetTableColumns_.size());
    StringJoiner assignmentJoiner = new StringJoiner(", ");
    for (int i = 0; i < minAvailableItems; ++i) {
      assignmentJoiner.add(String.format(
          "%s = %s", targetTableColumns_.get(i).getName(), sourceColumnLabels.get(i)));
    }
    return assignmentJoiner.toString();
  }
}
