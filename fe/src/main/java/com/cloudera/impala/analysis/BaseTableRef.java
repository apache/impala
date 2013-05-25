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

import java.util.List;

import com.cloudera.impala.catalog.AuthorizationException;
import com.cloudera.impala.common.AnalysisException;
import com.cloudera.impala.common.InternalException;
import com.google.common.base.Preconditions;

/**
 * An actual table, such as HBase table or a Hive table.
 * BaseTableRef.
 */
public class BaseTableRef extends TableRef {
  private final TableName name;

  // Indicates whether this table should be considered for view replacement.
  // Used to disambiguate non-fully-qualified references to base tables
  // from WITH-clause views.
  private boolean allowViewReplacement = true;

  public BaseTableRef(TableName name, String alias) {
    super(alias);
    Preconditions.checkArgument(!name.toString().isEmpty());
    Preconditions.checkArgument(alias == null || !alias.isEmpty());
    this.name = name;
  }

  /**
   * C'tor for cloning.
   */
  public BaseTableRef(BaseTableRef other) {
    super(other);
    this.name = other.name;
    this.allowViewReplacement = other.allowViewReplacement;
  }

  /**
   * Returns the name of the table referred to. Before analysis, the table name
   * may not be fully qualified. If the table name is unqualified, the current
   * default database from the analyzer will be used as the db name.
   */
  public TableName getName() {
    return name;
  }

  /**
   * Register this table ref and then analyze the Join clause.
   */
  @Override
  public void analyze(Analyzer analyzer) throws AnalysisException,
      AuthorizationException {
    Preconditions.checkNotNull(getPrivilegeRequirement());
    desc = analyzer.registerBaseTableRef(this);
    isAnalyzed = true;  // true that we have assigned desc
    try {
      analyzeJoin(analyzer);
    } catch (InternalException e) {
      throw new AnalysisException(e.getMessage(), e);
    }
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

  public String debugString() {
    return tableRefToSql();
  }

  @Override
  public TableRef clone() {
    return new BaseTableRef(this);
  }

  /**
   * Disable view replacement for this table.
   * See comment on allowViewReplacement.
   */
  public void disableViewReplacement() {
    allowViewReplacement = false;
  }

  /**
   * Enable view replacement for this table.
   * See comment on allowViewReplacement.
   */
  public void enableViewReplacement() {
    allowViewReplacement = true;
  }

  public boolean isReplacableByView() {
    return allowViewReplacement;
  }
}
