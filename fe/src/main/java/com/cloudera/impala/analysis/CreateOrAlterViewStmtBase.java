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

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import com.cloudera.impala.common.AnalysisException;
import com.cloudera.impala.thrift.TCreateOrAlterViewParams;
import com.cloudera.impala.thrift.TTableName;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

/**
 * Base class for CREATE VIEW and ALTER VIEW AS SELECT statements.
 */
public abstract class CreateOrAlterViewStmtBase extends StatementBase {
  protected final boolean ifNotExists;
  protected final TableName tableName;
  protected final ArrayList<ColumnDef> columnDefs;
  protected final String comment;
  protected final QueryStmt viewDefStmt;

  // Set during analysis
  protected String dbName;
  protected String owner;

  // The original SQL-string given as view definition. Set during analysis.
  // Corresponds to Hive's viewOriginalText.
  protected String originalViewDef;

  // Query statement (as SQL string) that defines the ViewRef for view substitution.
  // It is a transformation of the original view definition, e.g., to enforce the
  // columnDefs even if the original view definition has explicit column aliases.
  // If column definitions were given, then this "expanded" view definition
  // wraps the original view definition in a select stmt as follows.
  //
  // SELECT viewName.origCol1 AS colDef1, viewName.origCol2 AS colDef2, ...
  // FROM (originalViewDef) AS viewName
  //
  // Corresponds to Hive's viewExpandedText, but is not identical to the SQL
  // Hive would produce in view creation.
  protected String inlineViewDef;

  // Columns to use in the select list of the expanded SQL string and when registering
  // this view in the metastore. Set in analysis.
  protected ArrayList<ColumnDef> finalColDefs;

  public CreateOrAlterViewStmtBase(boolean ifNotExists, TableName tableName,
      ArrayList<ColumnDef> columnDefs, String comment, QueryStmt viewDefStmt) {
    Preconditions.checkNotNull(tableName);
    Preconditions.checkNotNull(viewDefStmt);
    this.ifNotExists = ifNotExists;
    this.tableName = tableName;
    this.columnDefs = columnDefs;
    this.comment = comment;
    this.viewDefStmt = viewDefStmt;
  }

  /**
   * Sets the originalViewDef and the expanded inlineViewDef based on viewDefStmt.
   * If columnDefs were given, checks that they do not contain duplicate column names
   * and throws an exception if they do.
   */
  protected void createColumnAndViewDefs(Analyzer analyzer) throws AnalysisException {
    Preconditions.checkNotNull(dbName);
    Preconditions.checkNotNull(owner);

    // Check that all the column names are unique. Also, set the finalColDefs
    // to reflect the given column definitions.
    if (columnDefs != null) {
      Preconditions.checkState(!columnDefs.isEmpty());
      if (columnDefs.size() != viewDefStmt.getColLabels().size()) {
        String cmp =
            (columnDefs.size() > viewDefStmt.getColLabels().size()) ? "more" : "fewer";
        throw new AnalysisException(String.format("Column-definition list has " +
            "%s columns (%s) than the view-definition query statement returns (%s).",
            cmp, columnDefs.size(), viewDefStmt.getColLabels().size()));
      }

      finalColDefs = columnDefs;
      Preconditions.checkState(columnDefs.size() == viewDefStmt.getResultExprs().size());
      Set<String> colNames = Sets.newHashSet();
      for (int i = 0; i < columnDefs.size(); ++i) {
        ColumnDef colDef = columnDefs.get(i);
        if (!colNames.add(colDef.getColName().toLowerCase())) {
          throw new AnalysisException("Duplicate column name: " + colDef.getColName());
        }
        // Set type in the column definition from the view-definition statement.
        colDef.setColType(viewDefStmt.getResultExprs().get(i).getType());
      }
    } else {
      // Create list of column definitions from the view-definition statement.
      finalColDefs = Lists.newArrayList();
      List<Expr> exprs = viewDefStmt.getResultExprs();
      List<String> labels = viewDefStmt.getColLabels();
      Preconditions.checkState(exprs.size() == labels.size());
      for (int i = 0; i < viewDefStmt.getColLabels().size(); ++i) {
        finalColDefs.add(new ColumnDef(labels.get(i), exprs.get(i).getType(), null));
      }
    }

    // Check that the column definitions have valid names.
    for (ColumnDef colDef: finalColDefs) {
      colDef.analyze();
    }

    // Set original and expanded view-definition SQL strings.
    originalViewDef = viewDefStmt.toSql();

    // If no column definitions were given, then the expanded view SQL is the same
    // as the original one.
    if (columnDefs == null) {
      inlineViewDef = originalViewDef;
      return;
    }

    // Wrap the original view-definition statement into a SELECT to enforce the
    // given column definitions.
    StringBuilder sb = new StringBuilder();
    sb.append("SELECT ");
    for (int i = 0; i < finalColDefs.size(); ++i) {
      String colRef = ToSqlUtils.getHiveIdentSql(viewDefStmt.getColLabels().get(i));
      String colAlias = ToSqlUtils.getHiveIdentSql(finalColDefs.get(i).getColName());
      sb.append(String.format("%s.%s AS %s", tableName.getTbl(), colRef, colAlias));
      sb.append((i+1 != finalColDefs.size()) ? ", " : "");
    }
    // Do not use 'AS' for table aliases because Hive only accepts them without 'AS'.
    sb.append(String.format(" FROM (%s) %s", originalViewDef, tableName.getTbl()));
    inlineViewDef = sb.toString();
  }

  public TCreateOrAlterViewParams toThrift() {
    TCreateOrAlterViewParams params = new TCreateOrAlterViewParams();
    params.setView_name(new TTableName(getDb(), getTbl()));
    for (ColumnDef col: finalColDefs) {
      params.addToColumns(col.toThrift());
    }
    params.setOwner(getOwner());
    params.setIf_not_exists(getIfNotExists());
    params.setOriginal_view_def(originalViewDef);
    params.setExpanded_view_def(inlineViewDef);
    if (comment != null) params.setComment(comment);
    return params;
  }

  /**
   * Can only be called after analysis, returns the name of the database the table will
   * be created within.
   */
  public String getDb() {
    Preconditions.checkNotNull(dbName);
    return dbName;
  }

  /**
   * Can only be called after analysis, returns the owner of the view to be created.
   */
  public String getOwner() {
    Preconditions.checkNotNull(owner);
    return owner;
  }

  @Override
  public String debugString() { return toSql(); }

  public List<ColumnDef> getColumnDefs() {return columnDefs; }
  public String getComment() { return comment; }
  public boolean getIfNotExists() { return ifNotExists; }
  public String getOriginalViewDef() { return originalViewDef; }
  public String getInlineViewDef() { return inlineViewDef; }
  public String getTbl() { return tableName.getTbl(); }
}
