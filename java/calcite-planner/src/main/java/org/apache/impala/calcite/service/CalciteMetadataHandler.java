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

package org.apache.impala.calcite.service;

import com.google.common.base.Splitter;
import org.apache.calcite.config.CalciteConnectionConfig;
import org.apache.calcite.config.CalciteConnectionConfigImpl;
import org.apache.calcite.config.CalciteConnectionProperty;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.jdbc.JavaTypeFactoryImpl;
import org.apache.calcite.prepare.CalciteCatalogReader;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.sql.SqlBasicCall;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlJoin;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlSelect;
import org.apache.calcite.sql.SqlWith;
import org.apache.calcite.sql.SqlWithItem;
import org.apache.calcite.sql.util.SqlBasicVisitor;
import org.apache.impala.analysis.Analyzer;
import org.apache.impala.analysis.StmtMetadataLoader;
import org.apache.impala.analysis.TableName;
import org.apache.impala.authorization.NoopAuthorizationFactory;
import org.apache.impala.calcite.schema.CalciteDb;
import org.apache.impala.calcite.schema.ImpalaCalciteCatalogReader;
import org.apache.impala.calcite.type.ImpalaTypeSystemImpl;
import org.apache.impala.calcite.util.SimplifiedAnalyzer;
import org.apache.impala.catalog.FeCatalog;
import org.apache.impala.catalog.FeDb;
import org.apache.impala.catalog.FeTable;
import org.apache.impala.common.ImpalaException;
import org.apache.impala.common.UnsupportedFeatureException;
import org.apache.impala.thrift.TQueryCtx;

import com.google.common.collect.Sets;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.Stack;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * CalciteMetadataHandler. Responsible for loading the tables for a query
 * from catalogd into the coordinator and populating the Calcite schema with
 * these tables.
 */
public class CalciteMetadataHandler implements CompilerStep {

  protected static final Logger LOG =
      LoggerFactory.getLogger(CalciteMetadataHandler.class.getName());

  // StmtTableCache needed by Impala's Analyzer class at planning time.
  private final StmtMetadataLoader.StmtTableCache stmtTableCache_;

  // CalciteCatalogReader is a context class that holds global information that
  // may be needed by the CalciteTable object
  private final CalciteCatalogReader reader_;

  private final Analyzer analyzer_;

  public CalciteMetadataHandler(SqlNode parsedNode,
      CalciteJniFrontend.QueryContext queryCtx) throws ImpalaException {

    StmtMetadataLoader stmtMetadataLoader = new StmtMetadataLoader(
        queryCtx.getFrontend(), queryCtx.getCurrentDb(), queryCtx.getTimeline());

    // retrieve all the tablenames in the query, will be in tableVisitor.tableNames
    TableVisitor tableVisitor = new TableVisitor(queryCtx.getCurrentDb());
    parsedNode.accept(tableVisitor);

    // load the relevant tables in the query from catalogd
    this.stmtTableCache_ = stmtMetadataLoader.loadTables(tableVisitor.tableNames_);

    this.reader_ = createCalciteCatalogReader(stmtTableCache_,
        queryCtx.getTQueryCtx(), queryCtx.getCurrentDb());

    this.analyzer_ = createAnalyzer(stmtTableCache_, queryCtx);

    // populate calcite schema.  This step needs to be done after the loader because the
    // schema needs to contain the columns in the table for validation, which cannot
    // be done when it's an IncompleteTable
    List<String> errorTables = populateCalciteSchema(reader_,
        queryCtx.getFrontend().getCatalog(), stmtTableCache_, analyzer_);

    tableVisitor.checkForComplexTable(stmtTableCache_, errorTables, queryCtx);
  }

  /**
   * Creates CalciteCatalogReader object which will contain information about the schema.
   * Since the individual Table objects have reference to the Schema, this also serves
   * as a way to give the tables Context information about the general query.
   */
  public static CalciteCatalogReader createCalciteCatalogReader(
      StmtMetadataLoader.StmtTableCache stmtTableCache, TQueryCtx queryCtx,
      String database) {
    RelDataTypeFactory typeFactory = new JavaTypeFactoryImpl(new ImpalaTypeSystemImpl());
    Properties props = new Properties();
    props.setProperty(CalciteConnectionProperty.CASE_SENSITIVE.camelName(), "false");
    CalciteConnectionConfig config = new CalciteConnectionConfigImpl(props);
    CalciteSchema rootSchema = CalciteSchema.createRootSchema(true);
    return new ImpalaCalciteCatalogReader(rootSchema,
        Collections.singletonList(database),
        typeFactory, config, queryCtx, stmtTableCache);
  }

  /**
   * Populate the CalciteSchema with tables being used by this query. Returns a
   * list of tables in the query that are not found in the database.
   */
  public static List<String> populateCalciteSchema(CalciteCatalogReader reader,
      FeCatalog catalog, StmtMetadataLoader.StmtTableCache stmtTableCache,
      Analyzer analyzer) throws ImpalaException {
    List<String> notFoundTables = new ArrayList<>();
    CalciteSchema rootSchema = reader.getRootSchema();
    Map<String, CalciteDb.Builder> dbSchemas = new HashMap<>();
    for (TableName tableName : stmtTableCache.tables.keySet()) {
      FeDb db = catalog.getDb(tableName.getDb());
      // db is not found, this will probably fail in the validation step
      if (db == null) {
        notFoundTables.add(tableName.toString());
        continue;
      }

      // table is not found, this will probably fail in the validation step
      FeTable feTable = db.getTable(tableName.getTbl());
      if (feTable == null) {
        notFoundTables.add(tableName.toString());
        continue;
      }

      // populate the dbschema with its table, creating the dbschema if it's the
      // first instance seen in the query.
      CalciteDb.Builder dbBuilder =
          dbSchemas.getOrDefault(tableName.getDb(), new CalciteDb.Builder(reader));
      dbBuilder.addTable(tableName.getTbl().toLowerCase(), feTable, analyzer);
      dbSchemas.put(tableName.getDb().toLowerCase(), dbBuilder);
    }

    // add all databases to the root schema
    for (String dbName : dbSchemas.keySet()) {
      rootSchema.add(dbName, dbSchemas.get(dbName.toLowerCase()).build());
    }
    return notFoundTables;
  }

  public StmtMetadataLoader.StmtTableCache getStmtTableCache() {
    return stmtTableCache_;
  }

  public CalciteCatalogReader getCalciteCatalogReader() {
    return reader_;
  }

  public Analyzer getAnalyzer() {
    return analyzer_;
  }

  private Analyzer createAnalyzer(StmtMetadataLoader.StmtTableCache stmtTableCache,
      CalciteJniFrontend.QueryContext queryCtx) throws ImpalaException {
    // XXX: using NoopAuthorizationFactory, but this part of the code will
    // eventually either be deprecated or only used in the test environment.
    return new SimplifiedAnalyzer(stmtTableCache, queryCtx.getTQueryCtx(),
        new NoopAuthorizationFactory(), null);
  }

  /**
   * TableVisitor walks through the AST and places all the tables into
   * tableNames
   */
  public static class TableVisitor extends SqlBasicVisitor<Void> {
    private final String currentDb_;
    public final Set<TableName> tableNames_ = new HashSet<>();

    // Error condition for now. Complex tables are not yet supported
    // so if we see a table name that has more than 2 parts, this variable
    // will contain that table.
    public final List<String> errorTables_ = new ArrayList<>();

    // This stack contains the sets of TableName's of the currently visited SqlWith
    // nodes, with the set of TableName's of the most recently visited SqlWith node being
    // the top of the stack.
    public final Stack<Set<TableName>> withItemTableNames_ = new Stack<>();

    public TableVisitor(String currentDb) {
      this.currentDb_ = currentDb.toLowerCase();
    }

    @Override
    public Void visit(SqlCall call) {
      if (call instanceof SqlWith) {
        withItemTableNames_.push(new HashSet<>());
      }

      if (call.getKind() == SqlKind.SELECT) {
        SqlSelect select = (SqlSelect) call;
        if (select.getFrom() != null) {
          tableNames_.addAll(getTableNames(select.getFrom()));
        }
      }

      if (call.getKind() == SqlKind.WITH_ITEM) {
        TableName tableName = new TableName(this.currentDb_.toLowerCase(),
            ((SqlWithItem) call).name.names.get(0).toLowerCase());
        // Since a SqlWithItem node cannot exist without a SqlWith node, we can be sure
        // the top stack element was added by the respective SqlWith node of this
        // SqlWithItem. Adding 'tableName' to this stack element would allow us to
        // determine in getTableNames() if a given TableName derived from a SqlIdentifier
        // was registered via a SqlWithItem node of which the corresponding SqlWith node
        // is an ancestor of the SqlIdentifier.
        withItemTableNames_.peek().add(tableName);
      }

      Void v = super.visit(call);

      if (call instanceof SqlWith) {
        withItemTableNames_.pop();
      }
      return v;
    }

    private List<TableName> getTableNames(SqlNode fromNode) {
      List<TableName> localTableNames = new ArrayList<>();
      if (fromNode instanceof SqlIdentifier) {
        String tableName = fromNode.toString();
        List<String> parts = Splitter.on('.').splitToList(tableName);
        if (parts.size() == 1) {
          TableName tableNameToAdd = new TableName(
              currentDb_.toLowerCase(), parts.get(0).toLowerCase());
          // Do not collect this table if 'tableNameToAdd' was already registered via
          // a SqlWithItem node since in this case 'tableNameToAdd' is not an actual
          // table.
          if (!isRegisteredBySqlWithItem(tableNameToAdd)) {
            localTableNames.add(tableNameToAdd);
          }
        } else if (parts.size() == 2) {
          localTableNames.add(
              new TableName(parts.get(0).toLowerCase(), parts.get(1).toLowerCase()));
        } else {
          errorTables_.add(tableName);
          return localTableNames;
        }
      }

      // Join node has the tables in the left and right node.
      if (fromNode instanceof SqlJoin) {
        localTableNames.addAll(getTableNames(((SqlJoin) fromNode).getLeft()));
        localTableNames.addAll(getTableNames(((SqlJoin) fromNode).getRight()));
      }

      // Put references in the schema too
      if (fromNode instanceof SqlBasicCall) {
        SqlBasicCall basicCall = (SqlBasicCall) fromNode;
        if (basicCall.getKind().equals(SqlKind.AS)) {
          localTableNames.addAll(getTableNames(basicCall.operand(0)));
        }
      }
      return localTableNames;
    }

    private boolean isRegisteredBySqlWithItem(TableName tableName) {
      for (Set<TableName> tableNames : withItemTableNames_) {
        if (tableNames.contains(tableName)) return true;
      }
      return false;
    }

    /**
     * Check if the error table is actually a table with a complex column. There is Impala
     * syntax where a complex column uses the same syntax in the FROM clause as a table.
     * This method is passed in all the tables that are not found and checks to see if
     * the table turned out to be a complex column rather than an actual table. If so,
     * this throws an unsupported feature exception (for the time being). If it's not
     * a table with a complex column, a table not found error will eventually be thrown
     * in a different place.
     */
    public void checkForComplexTable(StmtMetadataLoader.StmtTableCache stmtTableCache,
        List<String> errorTables, CalciteJniFrontend.QueryContext queryCtx)
        throws ImpalaException {
      List<String> allErrorTables = new ArrayList<>();
      allErrorTables.addAll(errorTables_);
      allErrorTables.addAll(errorTables);
      for (String errorTable : allErrorTables) {
        List<String> parts = Splitter.on('.').splitToList(errorTable);
        // if there are 3 parts, then it has to be db.table.column and must be a
        // complex column.
        if (parts.size() > 2) {
          throw new UnsupportedFeatureException("Complex column " +
              errorTable + " not supported.");
        }
        // if there are 2 parts, then it is either a missing db.table or a
        // table.column.  We look to see if the column can be found in any
        // of the tables, in which case, it is a complex column being referenced.
        if (parts.size() == 2) {
          // first check the already existing cache for the error table.
          if (anyTableContainsColumn(stmtTableCache, parts.get(1))) {
            throw new UnsupportedFeatureException("Complex column " +
                errorTable + " not supported.");
          }
          // it's possible that the table wasn't loaded yet because this method is
          // only called when there is an error finding a table. Try loading the table
          // from catalogd just in case, and check to see if it's a complex column.
          TableName potentialComplexTable = new TableName(
              currentDb_.toLowerCase(), parts.get(0).toLowerCase());
          StmtMetadataLoader errorLoader = new StmtMetadataLoader(
              queryCtx.getFrontend(), queryCtx.getCurrentDb(), queryCtx.getTimeline());
          StmtMetadataLoader.StmtTableCache errorCache =
              errorLoader.loadTables(Sets.newHashSet(potentialComplexTable));
          if (anyTableContainsColumn(errorCache, parts.get(1))) {
            throw new UnsupportedFeatureException("Complex column " +
                errorTable + " not supported.");
          }
        }
      }
    }
  }

  public static boolean anyTableContainsColumn(
      StmtMetadataLoader.StmtTableCache stmtTableCache, String columnName) {
    String onlyColumnPart = columnName.split("\\.")[0];
    for (FeTable table : stmtTableCache.tables.values()) {
      if (table.getColumn(onlyColumnPart) != null) {
        return true;
      }
    }
    return false;
  }

  @Override
  public void logDebug(Object resultObject) {
    LOG.debug("Loaded tables: " + stmtTableCache_.tables.values().stream()
        .map(feTable -> feTable.getName().toString())
        .collect(Collectors.joining( ", " )));
  }
}

