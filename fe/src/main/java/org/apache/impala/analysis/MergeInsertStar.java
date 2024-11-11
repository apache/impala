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
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.impala.catalog.Column;
import org.apache.impala.common.AnalysisException;

/**
 * Special INSERT clause for MERGE statements, it tries to match the source
 * table/subquery's result expression list against the columns of the target table.
 */
public class MergeInsertStar extends MergeInsert {

  public MergeInsertStar() {
    super(Collections.emptyList(), new SelectList());
  }

  @Override
  public void analyze(Analyzer analyzer) throws AnalysisException {
    Preconditions.checkNotNull(targetTableColumns_);
    Preconditions.checkNotNull(sourceTableRef_);
    List<String> sourceColumnLabels = getSourceColumnLabels();
    String sourceAlias = sourceTableRef_.getUniqueAlias();

    columnPermutation_ = targetTableColumns_.stream().map(Column::getName).collect(
        Collectors.toList());

    for (String column : sourceColumnLabels) {
      SlotRef slotRef = DmlStatementBase.createSlotRef(analyzer, sourceAlias, column);
      selectList_.getItems().add(new SelectListItem(slotRef, column));
    }
    super.analyze(analyzer);
  }

  @Override
  protected String moreColumnsMessageTemplate() {
    return "%s more columns (%d) than the source expression (%d): %s";
  }

  @Override
  protected String fewerColumnsMessageTemplate() {
    return "%s fewer columns (%d) than the source expression (%d): %s";
  }
}
