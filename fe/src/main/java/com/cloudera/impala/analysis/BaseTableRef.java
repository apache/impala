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
 * Represents an actual table, such as an HBase table or a Hive table,
 * or an unresolved reference to a view in the catalog or from a WITH clause.
 *
 * TODO: Parsing can no longer determine whether an identifier in a FROM clause
 * should represent a base table or a view. Clean up parsing/analysis of TableRefs
 * and the replacement of views, e.g., analysis should go through the list
 * of abstract TableRefs in a SelectStmt and replace them with Views or BaseTableRefs.
 */
public class BaseTableRef extends TableRef {
  private final TableName name_;

  // Indicates whether this table should be considered for view replacement
  // from WITH-clause views. Used to distinguish non-fully-qualified references
  // to catalog entries (base table or view) from WITH-clause views.
  private boolean allowWithViewReplacement_ = true;

  public BaseTableRef(TableName name, String alias) {
    super(alias);
    Preconditions.checkArgument(!name.toString().isEmpty());
    Preconditions.checkArgument(alias == null || !alias.isEmpty());
    this.name_ = name;
  }

  /**
   * C'tor for cloning.
   */
  public BaseTableRef(BaseTableRef other) {
    super(other);
    this.name_ = other.name_;
    this.allowWithViewReplacement_ = other.allowWithViewReplacement_;
  }

  /**
   * Returns the name of the table referred to. Before analysis, the table name
   * may not be fully qualified. If the table name is unqualified, the current
   * default database from the analyzer will be used as the db name.
   */
  public TableName getName() { return name_; }

  /**
   * Register this table ref and then analyze the Join clause.
   */
  @Override
  public void analyze(Analyzer analyzer) throws AnalysisException,
      AuthorizationException {
    Preconditions.checkNotNull(getPrivilegeRequirement());
    desc_ = analyzer.registerBaseTableRef(this);
    isAnalyzed_ = true;  // true that we have assigned desc
    try {
      analyzeJoin(analyzer);
    } catch (InternalException e) {
      throw new AnalysisException(e.getMessage(), e);
    }
  }

  @Override
  public List<TupleId> getMaterializedTupleIds() {
    // This function should only be called after analyze().
    Preconditions.checkState(isAnalyzed_);
    Preconditions.checkState(desc_ != null);
    return desc_.getId().asList();
  }

  /**
   * Return alias by which this table is referenced in select block.
   */
  @Override
  public String getAlias() {
    if (alias_ == null) {
      return name_.toString().toLowerCase();
    } else {
      return alias_;
    }
  }

  @Override
  public TableName getAliasAsName() {
    if (alias_ != null) {
      return new TableName(null, alias_);
    } else {
      return name_;
    }
  }

  @Override
  protected String tableRefToSql() {
    // Enclose the alias in quotes if Hive cannot parse it without quotes.
    // This is needed for view compatibility between Impala and Hive.
    String aliasSql = null;
    if (alias_ != null) aliasSql = ToSqlUtils.getIdentSql(alias_);
    return name_.toSql() + ((aliasSql != null) ? " " + aliasSql : "");
  }

  public String debugString() {
    return tableRefToSql();
  }

  @Override
  public TableRef clone() {
    return new BaseTableRef(this);
  }

  /**
   * Disable/enable WITH-clause view replacement for this table.
   * See comment on allowWithViewReplacement.
   */
  public void disableWithViewReplacement() { allowWithViewReplacement_ = false; }
  public void enableWithViewReplacement() { allowWithViewReplacement_ = true; }
  public boolean isReplaceableByWithView() { return allowWithViewReplacement_; }
}
