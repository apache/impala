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

package org.apache.impala.catalog;

import java.util.List;
import java.util.Set;

import org.apache.hadoop.hive.metastore.IMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.impala.analysis.Parser;
import org.apache.impala.analysis.QueryStmt;
import org.apache.impala.analysis.StatementBase;
import org.apache.impala.common.AnalysisException;
import org.apache.impala.thrift.TCatalogObjectType;
import org.apache.impala.thrift.TImpalaTableType;
import org.apache.impala.thrift.TTable;
import org.apache.impala.thrift.TTableDescriptor;
import org.apache.impala.thrift.TTableStats;
import org.apache.impala.thrift.TTableType;
import org.apache.impala.util.EventSequence;

import com.google.common.collect.Lists;

/**
 * Table metadata representing a catalog view or a local view from a WITH clause.
 * Most methods inherited from Table are not supposed to be called on this class because
 * views are substituted with their underlying definition during analysis of a statement.
 *
 * Refreshing or invalidating a view will reload the view's definition but will not
 * affect the metadata of the underlying tables (if any).
 */
public class View extends Table implements FeView {

  // View definition created by parsing inlineViewDef_ into a QueryStmt.
  private QueryStmt queryStmt_;

  // Set if this View is from a WITH clause and not persisted in the catalog.
  private final boolean isLocalView_;

  // Set if this View is from a WITH clause with column labels.
  private List<String> colLabels_;

  public View(org.apache.hadoop.hive.metastore.api.Table msTable,
      Db db, String name, String owner) {
    super(msTable, db, name, owner);
    isLocalView_ = false;
  }

  /**
   * C'tor for WITH-clause views that already have a parsed QueryStmt and an optional
   * list of column labels.
   */
  public View(String alias, QueryStmt queryStmt, List<String> colLabels) {
    super(null, null, alias, null);
    isLocalView_ = true;
    queryStmt_ = queryStmt;
    colLabels_ = colLabels;
  }

  /**
   * Creates a view for testing purposes.
   */
  private View(Db db, String name, QueryStmt queryStmt) {
    super(null, db, name, null);
    isLocalView_ = false;
    queryStmt_ = queryStmt;
    colLabels_ = null;
  }

  @Override
  public void load(boolean reuseMetadata, IMetaStoreClient client,
      org.apache.hadoop.hive.metastore.api.Table msTbl, String reason,
      EventSequence catalogTimeline) throws TableLoadingException {
    try {
      Table.LOADING_TABLES.incrementAndGet();
      clearColumns();
      msTable_ = msTbl;
      // Load columns.
      List<FieldSchema> fieldSchemas = client.getFields(db_.getName(), name_);
      for (int i = 0; i < fieldSchemas.size(); ++i) {
        FieldSchema s = fieldSchemas.get(i);
        Type type = parseColumnType(s);
        Column col = new Column(s.getName(), type, s.getComment(), i);
        addColumn(col);
      }
      // These fields are irrelevant for views.
      numClusteringCols_ = 0;
      tableStats_ = new TTableStats(-1);
      tableStats_.setTotal_file_bytes(-1);
      queryStmt_ = parseViewDef(this);
      refreshLastUsedTime();
    } catch (TableLoadingException e) {
      throw e;
    } catch (Exception e) {
      throw new TableLoadingException("Failed to load metadata for view: " + name_, e);
    } finally {
      Table.LOADING_TABLES.decrementAndGet();
    }
  }

  @Override
  protected void loadFromThrift(TTable t) throws TableLoadingException {
    super.loadFromThrift(t);
    queryStmt_ = parseViewDef(this);
  }

  /**
   * Parse the expanded view definition SQL-string.
   * Throws a TableLoadingException if there was any error parsing the
   * the SQL or if the view definition did not parse into a QueryStmt.
   */
  public static QueryStmt parseViewDef(FeView view) throws TableLoadingException {
    // Query statement (as SQL string) that defines the View for view substitution.
    // It is a transformation of the original view definition, e.g., to enforce the
    // explicit column definitions even if the original view definition has explicit
    // column aliases.
    // If column definitions were given, then this "expanded" view definition
    // wraps the original view definition in a select stmt as follows.
    //
    // SELECT viewName.origCol1 AS colDesc1, viewName.origCol2 AS colDesc2, ...
    // FROM (originalViewDef) AS viewName
    //
    // Corresponds to Hive's viewExpandedText, but is not identical to the SQL
    // Hive would produce in view creation.
    String inlineViewDef = view.getMetaStoreTable().getViewExpandedText();

    // Parse the expanded view definition SQL-string into a QueryStmt and
    // populate a view definition.
    StatementBase node;
    try {
      node = Parser.parse(inlineViewDef);
    } catch (AnalysisException e) {
      // Do not pass e as the exception cause because it might reveal the existence
      // of tables that the user triggering this load may not have privileges on.
      throw new TableLoadingException(
          String.format("Failed to parse view-definition statement of view: " +
              "%s", view.getFullName()));
    }
    // Make sure the view definition parses to a query statement.
    if (!(node instanceof QueryStmt)) {
      throw new TableLoadingException(String.format("View definition of %s " +
          "is not a query statement", view.getFullName()));
    }
    return (QueryStmt) node;
  }

  @Override
  public TCatalogObjectType getCatalogObjectType() { return TCatalogObjectType.VIEW; }

  @Override // FeView
  public QueryStmt getQueryStmt() { return queryStmt_; }

  @Override // FeView
  public boolean isLocalView() { return isLocalView_; }

  @Override
  public TImpalaTableType getTableType() {
    return TImpalaTableType.VIEW;
  }

  /**
   * Returns the column labels the user specified in the WITH-clause.
   */
  public List<String> getOriginalColLabels() { return colLabels_; }

  @Override
  public List<String> getColLabels() {
    if (colLabels_ == null) return null;
    if (colLabels_.size() >= queryStmt_.getColLabels().size()) return colLabels_;
    List<String> explicitColLabels = Lists.newArrayList(colLabels_);
    explicitColLabels.addAll(queryStmt_.getColLabels().subList(
        colLabels_.size(), queryStmt_.getColLabels().size()));
    return explicitColLabels;
  }

  @Override
  public TTableDescriptor toThriftDescriptor(int tableId, Set<Long> referencedPartitions) {
    throw new IllegalStateException("Cannot call toThriftDescriptor() on a view.");
  }

  @Override
  public TTable toThrift() {
    TTable view = super.toThrift();
    view.setTable_type(TTableType.VIEW);
    return view;
  }

  public static View createTestView(Db db, String name, QueryStmt viewDefStmt) {
    return new View(db, name, viewDefStmt);
  }
}
