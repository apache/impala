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

package com.cloudera.impala.catalog;

import java.io.StringReader;
import java.util.List;

import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.serde.serdeConstants;

import com.cloudera.impala.analysis.ParseNode;
import com.cloudera.impala.analysis.QueryStmt;
import com.cloudera.impala.analysis.SqlParser;
import com.cloudera.impala.analysis.SqlScanner;
import com.cloudera.impala.analysis.ViewRef;
import com.cloudera.impala.thrift.TCatalogObjectType;
import com.cloudera.impala.thrift.TTable;
import com.cloudera.impala.thrift.TTableDescriptor;

/**
 * Table metadata representing a view.
 * Most methods inherited from Table are not supposed to be called on this class because
 * views are substituted with their underlying definition during analysis of a statement.
 *
 * Refreshing or invalidating a view will reload the view's definition but will not
 * affect the metadata of the underlying tables (if any).
 */
public class View extends Table {

  // The original SQL-string given as view definition.
  private String originalViewDef;

  // The extended SQL-string used for view substitution.
  private String inlineViewDef;

  // View definition created by parsing expandedViewDef into a QueryStmt.
  private ViewRef viewDef;

  public View(TableId id, org.apache.hadoop.hive.metastore.api.Table msTable,
      Db db, String name, String owner) {
    super(id, msTable, db, name, owner);
  }

  @Override
  public void load(Table oldValue, HiveMetaStoreClient client,
      org.apache.hadoop.hive.metastore.api.Table msTbl) throws TableLoadingException {
    try {
      // Load columns.
      List<FieldSchema> fieldSchemas = client.getFields(db.getName(), name);
      for (int i = 0; i < fieldSchemas.size(); ++i) {
        FieldSchema s = fieldSchemas.get(i);
        // catch currently unsupported hive schema elements
        if (!serdeConstants.PrimitiveTypes.contains(s.getType())) {
          throw new TableLoadingException(
              String.format("Failed to load metadata for table '%s' due to unsupported " +
              "column type '%s' in column '%s'", getName(), s.getType(), s.getName()));
        }
        PrimitiveType type = getPrimitiveType(s.getType());
        Column col = new Column(s.getName(), type, s.getComment(), i);
        colsByPos.add(col);
        colsByName.put(s.getName(), col);
      }
      // These fields are irrelevant for views.
      numClusteringCols = 0;
      numRows = -1;
      initViewDef();
    } catch (TableLoadingException e) {
      throw e;
    } catch (Exception e) {
      throw new TableLoadingException("Failed to load metadata for view: " + name, e);
    }
  }

  @Override
  public void loadFromTTable(TTable t) throws TableLoadingException {
    super.loadFromTTable(t);
    initViewDef();
  }

  /**
   * Initializes the originalViewDef, inlineViewDef, and viewDef members
   * by parsing the expanded view definition SQL-string.
   * Throws a TableLoadingException if there was any error parsing the
   * the SQL or if the view definition did not parse into a QueryStmt.
   */
  private void initViewDef() throws TableLoadingException {
    // Set view-definition SQL strings.
    originalViewDef = getMetaStoreTable().getViewOriginalText();
    inlineViewDef = getMetaStoreTable().getViewExpandedText();
    // Parse the expanded view definition SQL-string into a QueryStmt and
    // populate a ViewRef to provide as view definition.
    SqlScanner input = new SqlScanner(new StringReader(inlineViewDef));
    SqlParser parser = new SqlParser(input);
    ParseNode node = null;
    try {
      node = (ParseNode) parser.parse().value;
    } catch (Exception e) {
      // Do not pass e as the exception cause because it might reveal the existence
      // of tables that the user triggering this load may not have privileges on.
      throw new TableLoadingException(
          String.format("Failed to parse view-definition statement of view: " +
              "%s.%s", db.getName(), name));
    }
    // Make sure the view definition parses to a query statement.
    if (!(node instanceof QueryStmt)) {
      throw new TableLoadingException(String.format("View definition of %s.%s " +
          "is not a query statement", db.getName(), name));
    }
    viewDef = new ViewRef(name, (QueryStmt) node, this);
  }

  @Override
  public TCatalogObjectType getCatalogObjectType() { return TCatalogObjectType.VIEW; }
  public ViewRef getViewDef() { return viewDef; }
  public String getOriginalViewDef() { return originalViewDef; }
  public String getInlineViewDef() { return inlineViewDef; }

  @Override
  public int getNumNodes() {
    throw new IllegalStateException("Cannot call getNumNodes() on a view.");
  }

  @Override
  public boolean isVirtualTable() { return true; }

  @Override
  public TTableDescriptor toThriftDescriptor() {
    throw new IllegalStateException("Cannot call toThrift() on a view.");
  }
}
