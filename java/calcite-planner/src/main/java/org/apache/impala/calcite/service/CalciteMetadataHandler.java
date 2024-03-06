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
import org.apache.calcite.sql.util.SqlBasicVisitor;
import org.apache.impala.analysis.StmtMetadataLoader;
import org.apache.impala.analysis.TableName;
import org.apache.impala.calcite.schema.CalciteDb;
import org.apache.impala.calcite.schema.CalciteTable;
import org.apache.impala.calcite.schema.ImpalaCalciteCatalogReader;
import org.apache.impala.calcite.type.ImpalaTypeSystemImpl;
import org.apache.impala.catalog.Column;
import org.apache.impala.catalog.FeCatalog;
import org.apache.impala.catalog.FeDb;
import org.apache.impala.catalog.FeTable;
import org.apache.impala.catalog.FeView;
import org.apache.impala.catalog.HdfsTable;
import org.apache.impala.common.ImpalaException;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
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

  public CalciteMetadataHandler(SqlNode parsedNode,
      CalciteJniFrontend.QueryContext queryCtx) throws ImpalaException {

    StmtMetadataLoader stmtMetadataLoader = new StmtMetadataLoader(
        queryCtx.getFrontend(), queryCtx.getCurrentDb(), queryCtx.getTimeline());

    // retrieve all the tablenames in the query, will be in tableVisitor.tableNames
    TableVisitor tableVisitor = new TableVisitor(queryCtx.getCurrentDb());
    parsedNode.accept(tableVisitor);

    // load the relevant tables in the query from catalogd
    this.stmtTableCache_ = stmtMetadataLoader.loadTables(tableVisitor.tableNames_);

    this.reader_ = createCalciteCatalogReader(queryCtx, stmtTableCache_);

    // populate calcite schema.  This step needs to be done after the loader because the
    // schema needs to contain the columns in the table for validation, which cannot
    // be done when it's an IncompleteTable
    populateCalciteSchema(reader_, queryCtx.getFrontend().getCatalog(),
        tableVisitor.tableNames_);
  }

  /**
   * Creates CalciteCatalogReader object which will contain information about the schema.
   * Since the individual Table objects have reference to the Schema, this also serves
   * as a way to give the tables Context information about the general query.
   */
  private CalciteCatalogReader createCalciteCatalogReader(
      CalciteJniFrontend.QueryContext queryCtx,
      StmtMetadataLoader.StmtTableCache stmtTableCache) {
    RelDataTypeFactory typeFactory = new JavaTypeFactoryImpl(new ImpalaTypeSystemImpl());
    Properties props = new Properties();
    props.setProperty(CalciteConnectionProperty.CASE_SENSITIVE.camelName(), "false");
    CalciteConnectionConfig config = new CalciteConnectionConfigImpl(props);
    CalciteSchema rootSchema = CalciteSchema.createRootSchema(true);
    return new ImpalaCalciteCatalogReader(rootSchema,
        Collections.singletonList(queryCtx.getCurrentDb()),
        typeFactory, config, queryCtx.getTQueryCtx(), stmtTableCache);
  }

  /**
   * Populate the CalciteSchema with tables being used by this query.
   */
  private void populateCalciteSchema(CalciteCatalogReader reader,
      FeCatalog catalog, Set<TableName> tableNames) {
    CalciteSchema rootSchema = reader.getRootSchema();
    Map<String, CalciteDb.Builder> dbSchemas = new HashMap<>();
    for (TableName tableName : tableNames) {
      FeDb db = catalog.getDb(tableName.getDb());
      // db is not found, this will probably fail in the validation step
      if (db == null) {
        continue;
      }

      // table is not found, this will probably fail in the validation step
      FeTable feTable = db.getTable(tableName.getTbl());
      if (feTable == null) {
        continue;
      }

      // populate the dbschema with its table, creating the dbschema if it's the
      // first instance seen in the query.
      CalciteDb.Builder dbBuilder =
          dbSchemas.getOrDefault(tableName.getDb(), new CalciteDb.Builder(reader));
      dbBuilder.addTable(tableName.getTbl().toLowerCase(), feTable);
      dbSchemas.put(tableName.getDb().toLowerCase(), dbBuilder);
    }

    // add all databases to the root schema
    for (String dbName : dbSchemas.keySet()) {
      rootSchema.add(dbName, dbSchemas.get(dbName.toLowerCase()).build());
    }
  }

  public StmtMetadataLoader.StmtTableCache getStmtTableCache() {
    return stmtTableCache_;
  }

  public CalciteCatalogReader getCalciteCatalogReader() {
    return reader_;
  }

  /**
   * TableVisitor walks through the AST and places all the tables into
   * tableNames
   */
  private static class TableVisitor extends SqlBasicVisitor<Void> {
    private final String currentDb_;
    public final Set<TableName> tableNames_ = new HashSet<>();

    public TableVisitor(String currentDb) {
      this.currentDb_ = currentDb.toLowerCase();
    }

    @Override
    public Void visit(SqlCall call) {
      if (call.getKind() == SqlKind.SELECT) {
        SqlSelect select = (SqlSelect) call;
        if (select.getFrom() != null) {
          tableNames_.addAll(getTableNames(select.getFrom()));
        }
      }
      return super.visit(call);
    }

    private List<TableName> getTableNames(SqlNode fromNode) {
      List<TableName> localTableNames = new ArrayList<>();
      if (fromNode instanceof SqlIdentifier) {
        String tableName = fromNode.toString();
        List<String> parts = Splitter.on('.').splitToList(tableName);
        // TODO: 'complex' tables ignored for now
        if (parts.size() == 1) {
          localTableNames.add(new TableName(
              currentDb_.toLowerCase(), parts.get(0).toLowerCase()));
        } else if (parts.size() == 2) {
          localTableNames.add(
              new TableName(parts.get(0).toLowerCase(), parts.get(1).toLowerCase()));
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
  }

  @Override
  public void logDebug(Object resultObject) {
    LOG.debug("Loaded tables: " + stmtTableCache_.tables.values().stream()
        .map(feTable -> feTable.getName().toString())
        .collect(Collectors.joining( ", " )));
  }
}

