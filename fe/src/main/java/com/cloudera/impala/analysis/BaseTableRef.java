// Copyright 2012 Cloudera Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package com.cloudera.impala.analysis;

import com.cloudera.impala.catalog.Table;
import com.cloudera.impala.common.AnalysisException;
import com.google.common.base.Preconditions;

/**
 * Represents a reference to an actual table, such as an Hdfs or HBase table.
 * BaseTableRefs are instantiated as a result of table resolution during analysis
 * of a SelectStmt.
 */
public class BaseTableRef extends TableRef {
  private final Table table_;

  public BaseTableRef(TableRef tableRef, Table table) {
    super(tableRef);
    table_ = table;
  }

  /**
   * C'tor for cloning.
   */
  private BaseTableRef(BaseTableRef other) {
    super(other);
    table_ = other.table_;
  }

  /**
   * Register this table ref and then analyze the Join clause.
   */
  @Override
  public void analyze(Analyzer analyzer) throws AnalysisException {
    Preconditions.checkNotNull(getPrivilegeRequirement());
    setFullyQualifiedTableName(analyzer);
    desc_ = analyzer.registerTableRef(this);
    isAnalyzed_ = true;  // true that we have assigned desc
    analyzeJoin(analyzer);
  }

  @Override
  public TupleDescriptor createTupleDescriptor(Analyzer analyzer)
      throws AnalysisException {
    TupleDescriptor result = analyzer.getDescTbl().createTupleDescriptor("basetbl");
    result.setTable(table_);
    return result;
  }

  @Override
  public TableRef clone() { return new BaseTableRef(this); }
  public String debugString() { return tableRefToSql(); }
}
