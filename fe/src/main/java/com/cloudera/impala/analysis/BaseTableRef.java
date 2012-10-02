// Copyright (c) 2012 Cloudera, Inc. All rights reserved.

package com.cloudera.impala.analysis;

import java.util.List;

import com.cloudera.impala.common.AnalysisException;
import com.google.common.base.Preconditions;

/**
 * An actual table, such as HBase table or a Hive table.
 * BaseTableRef.
 */
public class BaseTableRef extends TableRef {
  private TableName name;

  public BaseTableRef(TableName name, String alias) {
    super(alias);
    Preconditions.checkArgument(!name.toString().isEmpty());
    Preconditions.checkArgument(alias == null || !alias.isEmpty());
    this.name = name;
  }


  /**
   * Returns the name of the table referred to. Before analysis, the table name
   * may not be fully qualified; afterwards it is guaranteed to be fully
   * qualified.
   */
  public TableName getName() {
    return name;
  }

  /**
   * Register this table ref and then analyze the Join clause.
   */
  @Override
  public void analyze(Analyzer analyzer) throws AnalysisException {
    desc = analyzer.registerBaseTableRef(this);
    analyzeJoin(analyzer);
    isAnalyzed = true;
  }

  @Override
  public List<TupleId> getMaterializedTupleIds() {
    // This function should only be called after analyze().
    Preconditions.checkState(isAnalyzed);
    Preconditions.checkState(desc != null);
    return desc.getId().asList();
  }

  /**
   * Return alias by which this table is referenced in select block.
   */
  @Override
  public String getAlias() {
    if (alias == null) {
      return name.toString().toLowerCase();
    } else {
      return alias;
    }
  }

  @Override
  public TableName getAliasAsName() {
    if (alias != null) {
      return new TableName(null, alias);
    } else {
      return name;
    }
  }

  @Override
  protected String tableRefToSql() {
    return name.toString() + (alias != null ? " " + alias : "");
  }
}
