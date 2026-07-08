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

import org.apache.calcite.config.CalciteConnectionConfig;
import org.apache.calcite.config.CalciteConnectionConfigImpl;
import org.apache.calcite.config.CalciteConnectionProperty;
import org.apache.calcite.jdbc.CalciteSchema;
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
import org.apache.impala.analysis.TimeTravelSpec;
import org.apache.impala.calcite.schema.CalciteDb;
import org.apache.impala.calcite.schema.ImpalaCalciteCatalogReader;
import org.apache.impala.calcite.type.ImpalaTypeFactoryImpl;
import org.apache.impala.calcite.validate.ImpalaSnapshotSqlNode;
import org.apache.impala.catalog.FeCatalog;
import org.apache.impala.catalog.FeDb;
import org.apache.impala.catalog.FeIcebergTable;
import org.apache.impala.catalog.FeTable;
import org.apache.impala.common.AnalysisException;
import org.apache.impala.common.ImpalaException;
import org.apache.impala.thrift.TQueryCtx;

import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableSet;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.Stack;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * CalciteMetadataHandler. Responsible for loading the tables for a query
 * from catalogd into the coordinator and populating the Calcite schema with
 * these tables.
 */
public class CalciteMetadataHandler {

  protected static final Logger LOG =
      LoggerFactory.getLogger(CalciteMetadataHandler.class.getName());

  /**
   * Creates CalciteCatalogReader object which will contain information about the schema.
   * Since the individual Table objects have reference to the Schema, this also serves
   * as a way to give the tables Context information about the general query.
   */
  public static CalciteCatalogReader createCalciteCatalogReader(
      StmtMetadataLoader.StmtTableCache stmtTableCache, TQueryCtx queryCtx,
      String database) {
    RelDataTypeFactory typeFactory = ImpalaTypeFactoryImpl.INSTANCE;
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
      Map<TableName, List<TimeTravelSpec>> timeTravelSpecMap,
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
      String lowerCaseTableName = tableName.getTbl().toLowerCase();
      List<TimeTravelSpec> timeTravelSpecs = timeTravelSpecMap.get(tableName);
      // Special case: The tables in the for loop come from the tables loaded in the
      // Impala stmtTableCache. For Kudu, this table can be different from the table
      // name in the SQL query. In the e2e tests, this occurred on the
      // functional_kudu.alltypesagg table which loaded in the
      // functional_kudu.alltypesagg_idx table. The timeTravelSpecMap will not contain
      // a value in this case, which is loaded in based on the user query. Since Kudu
      // does not use time travel, we can safely load the table into the catalog without
      // a time travel extension.
      // If ever we do have a database that has time travel and manipulates the table
      // name, this logic will have to change.
      if (timeTravelSpecs == null) {
        dbBuilder.addTable(lowerCaseTableName, feTable, analyzer);
      } else {
        for (TimeTravelSpec tts : timeTravelSpecs) {
          // Actually, this is The normal case. Only Iceberg time travel tables will
          // have a TimeTravelSpec
          if (tts == null) {
            dbBuilder.addTable(lowerCaseTableName, feTable, analyzer);
          } else {
            if (!(feTable instanceof FeIcebergTable)) {
              throw new AnalysisException("Table '" + lowerCaseTableName +
                  "' is not a temporal table");
            }
            tts.analyze(analyzer);
            String timeTravelTableKey = ImpalaSnapshotSqlNode.getIdentifierName(
                lowerCaseTableName, tts);
            dbBuilder.addTimeTravelTable(timeTravelTableKey, tts, feTable, analyzer);
          }
        }
      }
      dbSchemas.put(tableName.getDb().toLowerCase(), dbBuilder);
    }

    // add all databases to the root schema
    for (String dbName : dbSchemas.keySet()) {
      rootSchema.add(dbName, dbSchemas.get(dbName.toLowerCase()).build());
    }
    return notFoundTables;
  }

  /**
   * TableVisitor walks through the AST and places all the tables into
   * tableNames
   */
  public static class TableVisitor extends SqlBasicVisitor<Void> {
    private final String currentDb_;
    private final Map<TableName, List<TimeTravelSpec>> tableNames_ = new HashMap<>();

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

    public Set<TableName> getTableNames() {
      return ImmutableSet.copyOf(tableNames_.keySet());
    }

    public Map<TableName, List<TimeTravelSpec>> getTableNameMap() {
      return tableNames_;
    }

    @Override
    public Void visit(SqlCall call) {
      if (call instanceof SqlWith) {
        withItemTableNames_.push(new HashSet<>());
      }

      if (call.getKind() == SqlKind.SELECT) {
        SqlSelect select = (SqlSelect) call;
        if (select.getFrom() != null) {
          visitTableNameNode(select.getFrom());
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

    private void extractTableName(SqlIdentifier identifer,
        TimeTravelSpec timeTravelSpec) {
      String tableNameString = identifer.toString();
      List<String> parts = Splitter.on('.').splitToList(tableNameString);
      if (parts.size() > 2) {
        errorTables_.add(tableNameString);
        return;
      }
      TableName tableName = parts.size() == 1
          ? new TableName(currentDb_.toLowerCase(), parts.get(0).toLowerCase())
          : new TableName(parts.get(0).toLowerCase(), parts.get(1).toLowerCase());

      // Do not collect this table if 'tableNameToAdd' was already registered via
      // a SqlWithItem node since in this case 'tableNameToAdd' is not an actual
      // table.
      if (parts.size() == 1 && isRegisteredBySqlWithItem(tableName)) {
        return;
      }

      List<TimeTravelSpec> timeTravelSpecs =
          tableNames_.getOrDefault(tableName, new ArrayList<>());
      timeTravelSpecs.add(timeTravelSpec);
      tableNames_.put(tableName, timeTravelSpecs);
    }

    private void visitTableNameNode(SqlNode fromNode) {
      if (fromNode instanceof SqlIdentifier) {
        extractTableName((SqlIdentifier) fromNode, null);
      }

      if (fromNode instanceof ImpalaSnapshotSqlNode) {
        ImpalaSnapshotSqlNode snapshot = (ImpalaSnapshotSqlNode) fromNode;
        extractTableName(snapshot.tableRefOriginal_, snapshot.timeTravelSpec_);
        return;
      }

      // Join node has the tables in the left and right node.
      if (fromNode instanceof SqlJoin) {
        visitTableNameNode(((SqlJoin) fromNode).getLeft());
        visitTableNameNode(((SqlJoin) fromNode).getRight());
      }

      // Put references in the schema too
      if (fromNode instanceof SqlBasicCall) {
        SqlBasicCall basicCall = (SqlBasicCall) fromNode;
        if (basicCall.getKind().equals(SqlKind.AS)) {
          visitTableNameNode(basicCall.operand(0));
        }
      }
    }

    private boolean isRegisteredBySqlWithItem(TableName tableName) {
      for (Set<TableName> tableNames : withItemTableNames_) {
        if (tableNames.contains(tableName)) return true;
      }
      return false;
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
}
