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

package org.apache.impala.service;

import java.sql.DatabaseMetaData;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.hadoop.hive.metastore.api.SQLForeignKey;
import org.apache.hadoop.hive.metastore.api.SQLPrimaryKey;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hive.service.rpc.thrift.TGetCrossReferenceReq;
import org.apache.hive.service.rpc.thrift.TGetPrimaryKeysReq;
import org.apache.impala.analysis.StmtMetadataLoader;
import org.apache.impala.analysis.TableName;
import org.apache.impala.authorization.User;
import org.apache.impala.catalog.ArrayType;
import org.apache.impala.catalog.Catalog;
import org.apache.impala.catalog.Column;
import org.apache.impala.catalog.FeCatalog;
import org.apache.impala.catalog.FeDb;
import org.apache.impala.catalog.FeIncompleteTable;
import org.apache.impala.catalog.FeTable;
import org.apache.impala.catalog.Function;
import org.apache.impala.catalog.MapType;
import org.apache.impala.catalog.PrimitiveType;
import org.apache.impala.catalog.ScalarType;
import org.apache.impala.catalog.StructType;
import org.apache.impala.catalog.Type;
import org.apache.impala.catalog.local.InconsistentMetadataFetchException;
import org.apache.impala.common.ImpalaException;
import org.apache.impala.compat.MetastoreShim;
import org.apache.impala.thrift.TColumn;
import org.apache.impala.thrift.TColumnValue;
import org.apache.impala.thrift.TImpalaTableType;
import org.apache.impala.thrift.TMetadataOpRequest;
import org.apache.impala.thrift.TResultRow;
import org.apache.impala.thrift.TResultSet;
import org.apache.impala.thrift.TResultSetMetadata;
import org.apache.impala.thrift.TTypeNodeType;
import org.apache.impala.util.PatternMatcher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Strings;
import com.google.common.collect.Lists;

import javax.annotation.Nullable;

/**
 * Metadata operation. It contains static methods to execute HiveServer2 metadata
 * operations and return the results, result schema and an unique request id in
 * TResultSet.
 */
public class MetadataOp {
  private static final Logger LOG = LoggerFactory.getLogger(MetadataOp.class);

  // Static column values
  private static final TColumnValue NULL_COL_VAL = new TColumnValue();
  private static final TColumnValue EMPTY_COL_VAL = createTColumnValue("");
  private static final String TABLE_COMMENT_KEY = "comment";

  // Result set schema for each of the metadata operations.
  private final static TResultSetMetadata GET_CATALOGS_MD = new TResultSetMetadata();
  private final static TResultSetMetadata GET_COLUMNS_MD = new TResultSetMetadata();
  private final static TResultSetMetadata GET_SCHEMAS_MD = new TResultSetMetadata();
  private final static TResultSetMetadata GET_TABLES_MD = new TResultSetMetadata();
  private static final TResultSetMetadata GET_TYPEINFO_MD = new TResultSetMetadata();
  private static final TResultSetMetadata GET_TABLE_TYPES_MD = new TResultSetMetadata();
  private static final TResultSetMetadata GET_FUNCTIONS_MD = new TResultSetMetadata();
  private static final TResultSetMetadata GET_PRIMARY_KEYS_MD = new TResultSetMetadata();
  private static final TResultSetMetadata GET_CROSS_REFERENCE_MD =
      new TResultSetMetadata();

  // GetTypeInfo contains all primitive types supported by Impala.
  private static final List<TResultRow> GET_TYPEINFO_RESULTS = Lists.newArrayList();

  // GetTableTypes returns values: "TABLE", "VIEW"
  private static final List<TResultRow> GET_TABLE_TYPES_RESULTS = Lists.newArrayList();

  // Initialize result set schemas and static result set
  static {
    initialzeResultSetSchemas();
    createGetTypeInfoResults();
    createGetTableTypesResults();
  }

  /**
   * Initialize result set schema for each of the HiveServer2 operations
   */
  private static void initialzeResultSetSchemas() {
    GET_CATALOGS_MD.addToColumns(new TColumn("TABLE_CAT", Type.STRING.toThrift()));

    GET_COLUMNS_MD.addToColumns(
        new TColumn("TABLE_CAT", Type.STRING.toThrift()));
    GET_COLUMNS_MD.addToColumns(
        new TColumn("TABLE_MD", Type.STRING.toThrift()));
    GET_COLUMNS_MD.addToColumns(
        new TColumn("TABLE_NAME", Type.STRING.toThrift()));
    GET_COLUMNS_MD.addToColumns(
        new TColumn("COLUMN_NAME", Type.STRING.toThrift()));
    GET_COLUMNS_MD.addToColumns(
        new TColumn("DATA_TYPE", Type.INT.toThrift()));
    GET_COLUMNS_MD.addToColumns(
        new TColumn("TYPE_NAME", Type.STRING.toThrift()));
    GET_COLUMNS_MD.addToColumns(
        new TColumn("COLUMN_SIZE", Type.INT.toThrift()));
    GET_COLUMNS_MD.addToColumns(
        new TColumn("BUFFER_LENGTH", Type.INT.toThrift()));
    GET_COLUMNS_MD.addToColumns(
        new TColumn("DECIMAL_DIGITS", Type.INT.toThrift()));
    GET_COLUMNS_MD.addToColumns(
        new TColumn("NUM_PREC_RADIX", Type.INT.toThrift()));
    GET_COLUMNS_MD.addToColumns(
        new TColumn("NULLABLE", Type.INT.toThrift()));
    GET_COLUMNS_MD.addToColumns(
        new TColumn("REMARKS", Type.STRING.toThrift()));
    GET_COLUMNS_MD.addToColumns(
        new TColumn("COLUMN_DEF", Type.STRING.toThrift()));
    GET_COLUMNS_MD.addToColumns(
        new TColumn("SQL_DATA_TYPE", Type.INT.toThrift()));
    GET_COLUMNS_MD.addToColumns(
        new TColumn("SQL_DATETIME_SUB", Type.INT.toThrift()));
    GET_COLUMNS_MD.addToColumns(
        new TColumn("CHAR_OCTET_LENGTH", Type.INT.toThrift()));
    GET_COLUMNS_MD.addToColumns(
        new TColumn("ORDINAL_POSITION", Type.INT.toThrift()));
    GET_COLUMNS_MD.addToColumns(
        new TColumn("IS_NULLABLE", Type.STRING.toThrift()));
    GET_COLUMNS_MD.addToColumns(
        new TColumn("SCOPE_CATALOG", Type.STRING.toThrift()));
    GET_COLUMNS_MD.addToColumns(
        new TColumn("SCOPE_SCHEMA", Type.STRING.toThrift()));
    GET_COLUMNS_MD.addToColumns(
        new TColumn("SCOPE_TABLE", Type.STRING.toThrift()));
    GET_COLUMNS_MD.addToColumns(
        new TColumn("SOURCE_DATA_TYPE", Type.SMALLINT.toThrift()));
    GET_COLUMNS_MD.addToColumns(
        new TColumn("IS_AUTO_INCREMENT", Type.STRING.toThrift()));

    GET_SCHEMAS_MD.addToColumns(
        new TColumn("TABLE_SCHEM", Type.STRING.toThrift()));
    GET_SCHEMAS_MD.addToColumns(
        new TColumn("TABLE_CATALOG", Type.STRING.toThrift()));

    GET_TABLES_MD.addToColumns(
        new TColumn("TABLE_CAT", Type.STRING.toThrift()));
    GET_TABLES_MD.addToColumns(
        new TColumn("TABLE_SCHEM", Type.STRING.toThrift()));
    GET_TABLES_MD.addToColumns(
        new TColumn("TABLE_NAME", Type.STRING.toThrift()));
    GET_TABLES_MD.addToColumns(
        new TColumn("TABLE_TYPE", Type.STRING.toThrift()));
    GET_TABLES_MD.addToColumns(
        new TColumn("REMARKS", Type.STRING.toThrift()));

    GET_TYPEINFO_MD.addToColumns(
        new TColumn("TYPE_NAME", Type.STRING.toThrift()));
    GET_TYPEINFO_MD.addToColumns(
        new TColumn("DATA_TYPE", Type.INT.toThrift()));
    GET_TYPEINFO_MD.addToColumns(
        new TColumn("PRECISION", Type.INT.toThrift()));
    GET_TYPEINFO_MD.addToColumns(
        new TColumn("LITERAL_PREFIX", Type.STRING.toThrift()));
    GET_TYPEINFO_MD.addToColumns(
        new TColumn("LITERAL_SUFFIX", Type.STRING.toThrift()));
    GET_TYPEINFO_MD.addToColumns(
        new TColumn("CREATE_PARAMS", Type.STRING.toThrift()));
    GET_TYPEINFO_MD.addToColumns(
        new TColumn("NULLABLE", Type.INT.toThrift()));
    GET_TYPEINFO_MD.addToColumns(
        new TColumn("CASE_SENSITIVE", Type.BOOLEAN.toThrift()));
    GET_TYPEINFO_MD.addToColumns(
        new TColumn("SEARCHABLE", Type.SMALLINT.toThrift()));
    GET_TYPEINFO_MD.addToColumns(
        new TColumn("UNSIGNED_ATTRIBUTE", Type.BOOLEAN.toThrift()));
    GET_TYPEINFO_MD.addToColumns(
        new TColumn("FIXED_PREC_SCALE", Type.BOOLEAN.toThrift()));
    GET_TYPEINFO_MD.addToColumns(
        new TColumn("AUTO_INCREMENT", Type.BOOLEAN.toThrift()));
    GET_TYPEINFO_MD.addToColumns(
        new TColumn("LOCAL_TYPE_NAME", Type.STRING.toThrift()));
    GET_TYPEINFO_MD.addToColumns(
        new TColumn("MINIMUM_SCALE", Type.SMALLINT.toThrift()));
    GET_TYPEINFO_MD.addToColumns(
        new TColumn("MAXIMUM_SCALE", Type.SMALLINT.toThrift()));
    GET_TYPEINFO_MD.addToColumns(
        new TColumn("SQL_DATA_TYPE", Type.INT.toThrift()));
    GET_TYPEINFO_MD.addToColumns(
        new TColumn("SQL_DATETIME_SUB", Type.INT.toThrift()));
    GET_TYPEINFO_MD.addToColumns(
        new TColumn("NUM_PREC_RADIX", Type.INT.toThrift()));

    GET_TABLE_TYPES_MD.addToColumns(
        new TColumn("TABLE_TYPE", Type.STRING.toThrift()));

    GET_FUNCTIONS_MD.addToColumns(
        new TColumn("FUNCTION_CAT", Type.STRING.toThrift()));
    GET_FUNCTIONS_MD.addToColumns(
        new TColumn("FUNCTION_SCHEM", Type.STRING.toThrift()));
    GET_FUNCTIONS_MD.addToColumns(
        new TColumn("FUNCTION_NAME", Type.STRING.toThrift()));
    GET_FUNCTIONS_MD.addToColumns(
        new TColumn("REMARKS", Type.STRING.toThrift()));
    GET_FUNCTIONS_MD.addToColumns(
        new TColumn("FUNCTION_TYPE", Type.INT.toThrift()));
    GET_FUNCTIONS_MD.addToColumns(
        new TColumn("SPECIFIC_NAME", Type.STRING.toThrift()));

    GET_PRIMARY_KEYS_MD.addToColumns(
        new TColumn("TABLE_CAT", Type.STRING.toThrift()));
    GET_PRIMARY_KEYS_MD.addToColumns(
        new TColumn("TABLE_SCHEM", Type.STRING.toThrift()));
    GET_PRIMARY_KEYS_MD.addToColumns(
        new TColumn("TABLE_NAME", Type.STRING.toThrift()));
    GET_PRIMARY_KEYS_MD.addToColumns(
        new TColumn("COLUMN_NAME", Type.STRING.toThrift()));
    GET_PRIMARY_KEYS_MD.addToColumns(
        new TColumn("KEQ_SEQ", Type.INT.toThrift()));
    GET_PRIMARY_KEYS_MD.addToColumns(
        new TColumn("PK_NAME", Type.STRING.toThrift()));

    GET_CROSS_REFERENCE_MD.addToColumns(
        new TColumn("PKTABLE_CAT", Type.STRING.toThrift()));
    GET_CROSS_REFERENCE_MD.addToColumns(
        new TColumn("PKTABLE_SCHEM", Type.STRING.toThrift()));
    GET_CROSS_REFERENCE_MD.addToColumns(
        new TColumn("PKTABLE_NAME", Type.STRING.toThrift()));
    GET_CROSS_REFERENCE_MD.addToColumns(
        new TColumn("PKCOLUMN_NAME", Type.STRING.toThrift()));
    GET_CROSS_REFERENCE_MD.addToColumns(
        new TColumn("FKTABLE_CAT", Type.STRING.toThrift()));
    GET_CROSS_REFERENCE_MD.addToColumns(
        new TColumn("FKTABLE_SCHEM", Type.STRING.toThrift()));
    GET_CROSS_REFERENCE_MD.addToColumns(
        new TColumn("FKTABLE_NAME", Type.STRING.toThrift()));
    GET_CROSS_REFERENCE_MD.addToColumns(
        new TColumn("FKCOLUMN_NAME", Type.STRING.toThrift()));
    GET_CROSS_REFERENCE_MD.addToColumns(
        new TColumn("KEQ_SEQ", Type.INT.toThrift()));
    GET_CROSS_REFERENCE_MD.addToColumns(
        new TColumn("UPDATE_RULE", Type.INT.toThrift()));
    GET_CROSS_REFERENCE_MD.addToColumns(
        new TColumn("DELETE_RULE", Type.INT.toThrift()));
    GET_CROSS_REFERENCE_MD.addToColumns(
        new TColumn("FK_NAME", Type.STRING.toThrift()));
    GET_CROSS_REFERENCE_MD.addToColumns(
        new TColumn("PK_NAME", Type.STRING.toThrift()));
    GET_CROSS_REFERENCE_MD.addToColumns(
        new TColumn("DEFERRABILITY", Type.INT.toThrift()));
  }

  /**
   * Contains lists of databases, lists of table belonging to the dbs, list of columns
   * belonging to the tables, primary keys and foreign keys belonging to the tables,
   * and list of user functions.
   */
  private static class DbsMetadata {
     // the list of database
    public List<String> dbs = Lists.newArrayList();

    // tableNames[i] are the tables within dbs[i]
    public List<List<String>> tableNames = Lists.newArrayList();

    // tableTypes[i] are the type of tables within dbs[i]
    public List<List<String>> tableTypes = Lists.newArrayList();

    // comments[i][j] is the comment of tableNames[j] in dbs[i].
    public List<List<String>> comments = Lists.newArrayList();

    // columns[i][j] are the columns of tableNames[j] in dbs[i].
    // If the table is missing (not yet loaded) its column list will be empty.
    public List<List<List<Column>>> columns = Lists.newArrayList();

    // functions[i] are the functions within dbs[i]
    public List<List<Function>> functions = Lists.newArrayList();

    // primaryKeys[i][j] are primary keys of tableNames[j] in dbs[i]
    public List<List<List<SQLPrimaryKey>>> primaryKeys = Lists.newArrayList();

    // foreignKeys[i][j] are primary keys of tableNames[j] in dbs[i]
    public List<List<List<SQLForeignKey>>> foreignKeys = Lists.newArrayList();

    // Set of tables that are missing (not yet loaded).
    public Set<TableName> missingTbls = new HashSet<TableName>();
  }

  /**
   * Returns the list of schemas, tables, columns and user functions that match the
   * corresponding matchers.
   *
   * The return value 'result.dbs' contains the list of databases that match
   * 'schemaPatternMatcher'.
   * 'result.tableNames[i]' contains the list of tables inside dbs[i] that match
   * 'tablePatternMatcher'.
   * 'result.columns[i][j]' contains the list of columns of table[j] in dbs[i]
   * that match 'columnPatternMatcher'.
   * result.functions[i] contains the list of functions inside dbs[i] that
   * match 'fnPatternMatcher'.
   *
   * If 'fnPatternMatcher' is not PatternMatcher.MATCHER_MATCH_NONE, then only function
   * metadata will be returned.
   * If 'tablePatternMatcher' is PatternMatcher.MATCHER_MATCH_NONE, then
   * 'result.tableNames' and 'result.columns' will not be populated.
   * If columns is null, then 'result.columns' will not be populated.
   */
  private static DbsMetadata getDbsMetadata(Frontend fe, String catalogName,
      PatternMatcher schemaPatternMatcher, PatternMatcher tablePatternMatcher,
      PatternMatcher columnPatternMatcher, PatternMatcher fnPatternMatcher, User user)
      throws ImpalaException {
    Frontend.RetryTracker retries = new Frontend.RetryTracker(
        String.format("fetching metadata for user %s", user.getName()));
    while (true) {
      try {
        return doGetDbsMetadata(fe, catalogName,
            schemaPatternMatcher, tablePatternMatcher,
            columnPatternMatcher, fnPatternMatcher, user);
      } catch (InconsistentMetadataFetchException e) {
        retries.handleRetryOrThrow(e);
      }
    }
  }

  private static DbsMetadata doGetDbsMetadata(Frontend fe, String catalogName,
      PatternMatcher schemaPatternMatcher, PatternMatcher tablePatternMatcher,
      PatternMatcher columnPatternMatcher, PatternMatcher fnPatternMatcher, User user)
      throws ImpalaException {
    DbsMetadata result = new DbsMetadata();

    // Hive does not have a catalog concept. Returns nothing if the request specifies an
    // non-empty catalog pattern.
    if (!isEmptyPattern(catalogName)) {
      return result;
    }

    FeCatalog catalog = fe.getCatalog();
    for (FeDb db: fe.getDbs(schemaPatternMatcher, user)) {
      if (fnPatternMatcher != PatternMatcher.MATCHER_MATCH_NONE) {
        // Get function metadata
        List<Function> fns = db.getFunctions(null, fnPatternMatcher);
        result.functions.add(fns);
      } else if (tablePatternMatcher == PatternMatcher.MATCHER_MATCH_NONE
          && columnPatternMatcher == PatternMatcher.MATCHER_MATCH_NONE) {
        // Get db list. No need to list table names.
        result.dbs.add(db.getName());
      } else {
        // Get table metadata
        List<String> tableList = Lists.newArrayList();
        List<List<Column>> tablesColumnsList = Lists.newArrayList();
        List<String> tableComments = Lists.newArrayList();
        List<String> tableTypes = Lists.newArrayList();
        List<List<SQLPrimaryKey>> primaryKeysList = Lists.newArrayList();
        List<List<SQLForeignKey>> foreignKeysList = Lists.newArrayList();
        for (String tabName: fe.getTableNames(db.getName(), tablePatternMatcher, user)) {
          FeTable table;
          if (columnPatternMatcher == PatternMatcher.MATCHER_MATCH_NONE) {
            // Don't need to completely load the table meta if columns are not required.
            table = catalog.getTableIfCachedNoThrow(db.getName(), tabName);
          } else {
            table = catalog.getTableNoThrow(db.getName(), tabName);
          }
          if (table == null) {
            result.missingTbls.add(new TableName(db.getName(), tabName));
            continue;
          }

          String comment = table.getTableComment();
          String tableType = getTableTypeString(table);
          List<Column> columns = Lists.newArrayList();
          List<SQLPrimaryKey> primaryKeys = Lists.newArrayList();
          List<SQLForeignKey> foreignKeys = Lists.newArrayList();
          // If the table is not yet loaded, the columns will be unknown. Add it
          // to the set of missing tables.
          if (!table.isLoaded() || table instanceof FeIncompleteTable) {
            result.missingTbls.add(new TableName(db.getName(), tabName));
          } else {
            columns.addAll(fe.getColumns(table, columnPatternMatcher, user));
            if (columnPatternMatcher != PatternMatcher.MATCHER_MATCH_NONE) {
              // It is unnecessary to populate pk/fk information if the request does not
              // want to match any columns.
              primaryKeys.addAll(fe.getPrimaryKeys(table, user));
              foreignKeys.addAll(fe.getForeignKeys(table, user));
            }
          }
          tableList.add(tabName);
          tablesColumnsList.add(columns);
          primaryKeysList.add(primaryKeys);
          foreignKeysList.add(foreignKeys);
          tableComments.add(Strings.nullToEmpty(comment));
          tableTypes.add(tableType);
        }
        result.dbs.add(db.getName());
        result.tableNames.add(tableList);
        result.comments.add(tableComments);
        result.columns.add(tablesColumnsList);
        result.primaryKeys.add(primaryKeysList);
        result.foreignKeys.add(foreignKeysList);
        result.tableTypes.add(tableTypes);
      }
    }
    return result;
  }

  public static String getTableTypeString(FeTable table) {
    String msTableType;
    if (table instanceof FeIncompleteTable) {
      // FeIncompleteTable doesn't have a msTable object but it contains the Impala table
      // type if the table is loaded in catalogd.
      return table.getTableType().name();
    }
    Table msTbl = table.getMetaStoreTable();
    msTableType = msTbl == null ? null : msTbl.getTableType();
    return getImpalaTableType(msTableType).name();
  }

  public static TImpalaTableType getImpalaTableType(@Nullable String msTableType) {
    if (msTableType != null) msTableType = msTableType.toUpperCase();
    return MetastoreShim.HMS_TO_IMPALA_TYPE.getOrDefault(msTableType,
        TImpalaTableType.TABLE);
  }

  @Nullable
  public static String getTableComment(Table msTbl) {
    return msTbl == null ? null : msTbl.getParameters().get(TABLE_COMMENT_KEY);
  }

  /**
   * Executes the GetCatalogs HiveServer2 operation and returns TResultSet.
   * Hive does not have a catalog concept. It always returns an empty result set.
   */
  public static TResultSet getCatalogs() {
    return createEmptyResultSet(GET_CATALOGS_MD);
  }

  /**
   * Executes the GetColumns HiveServer2 operation and returns TResultSet.
   * Queries the Impala catalog to return the list of table columns that fit the
   * search patterns. Matching columns requires loading the table metadata, so if
   * any missing tables are found an RPC to the CatalogServer will be executed
   * to request loading these tables. The matching process will be restarted
   * once the required tables have been loaded in the local Impalad Catalog or
   * the wait timeout has been reached.
   *
   * The parameters catalogName, schemaName, tableName and columnName are JDBC search
   * patterns.
   */
  public static TResultSet getColumns(Frontend fe, String catalogName, String schemaName,
      String tableName, String columnName, User user)
      throws ImpalaException {
    // Get the list of schemas, tables, and columns that satisfy the search conditions.
    PatternMatcher schemaMatcher = PatternMatcher.createJdbcPatternMatcher(schemaName);
    PatternMatcher tableMatcher = PatternMatcher.createJdbcPatternMatcher(tableName);
    PatternMatcher columnMatcher = PatternMatcher.createJdbcPatternMatcher(columnName);
    DbsMetadata dbsMetadata = getDbsMetadata(fe, catalogName, schemaMatcher,
        tableMatcher, columnMatcher, PatternMatcher.MATCHER_MATCH_NONE, user);
    if (!dbsMetadata.missingTbls.isEmpty()) {
      // Need to load tables for column metadata.
      StmtMetadataLoader mdLoader = new StmtMetadataLoader(fe, Catalog.DEFAULT_DB, null);
      mdLoader.loadTables(dbsMetadata.missingTbls);
      dbsMetadata = getDbsMetadata(fe, catalogName, schemaMatcher,
          tableMatcher, columnMatcher, PatternMatcher.MATCHER_MATCH_NONE, user);
    }

    TResultSet result = createEmptyResultSet(GET_COLUMNS_MD);
    for (int i = 0; i < dbsMetadata.dbs.size(); ++i) {
      String dbName = dbsMetadata.dbs.get(i);
      for (int j = 0; j < dbsMetadata.tableNames.get(i).size(); ++j) {
        String tabName = dbsMetadata.tableNames.get(i).get(j);
        for (int k = 0; k < dbsMetadata.columns.get(i).get(j).size(); ++k) {
          Column column = dbsMetadata.columns.get(i).get(j).get(k);
          Type colType = column.getType();
          String colTypeName = getHs2MetadataTypeName(colType);

          TResultRow row = new TResultRow();
          row.colVals = Lists.newArrayList();
          row.colVals.add(NULL_COL_VAL); // TABLE_CAT
          row.colVals.add(createTColumnValue(dbName)); // TABLE_SCHEM
          row.colVals.add(createTColumnValue(tabName)); // TABLE_NAME
          row.colVals.add(createTColumnValue(column.getName())); // COLUMN_NAME
          row.colVals.add(createTColumnValue(colType.getJavaSqlType())); // DATA_TYPE
          row.colVals.add(createTColumnValue(colTypeName)); // TYPE_NAME
          row.colVals.add(createTColumnValue(colType.getColumnSize())); // COLUMN_SIZE
          row.colVals.add(NULL_COL_VAL); // BUFFER_LENGTH, unused
          // DECIMAL_DIGITS
          row.colVals.add(createTColumnValue(colType.getDecimalDigits()));
          // NUM_PREC_RADIX
          row.colVals.add(createTColumnValue(colType.getNumPrecRadix()));
          // NULLABLE
          row.colVals.add(createTColumnValue(DatabaseMetaData.columnNullable));
          row.colVals.add(createTColumnValue(column.getComment())); // REMARKS
          row.colVals.add(NULL_COL_VAL); // COLUMN_DEF
          row.colVals.add(NULL_COL_VAL); // SQL_DATA_TYPE
          row.colVals.add(NULL_COL_VAL); // SQL_DATETIME_SUB
          row.colVals.add(NULL_COL_VAL); // CHAR_OCTET_LENGTH
          // ORDINAL_POSITION starts from 1
          row.colVals.add(createTColumnValue(column.getPosition() + 1));
          row.colVals.add(createTColumnValue("YES")); // IS_NULLABLE
          row.colVals.add(NULL_COL_VAL); // SCOPE_CATALOG
          row.colVals.add(NULL_COL_VAL); // SCOPE_SCHEMA
          row.colVals.add(NULL_COL_VAL); // SCOPE_TABLE
          row.colVals.add(NULL_COL_VAL); // SOURCE_DATA_TYPE
          row.colVals.add(createTColumnValue("NO")); // IS_AUTO_INCREMENT
          result.rows.add(row);
        }
      }
    }
    if (LOG.isTraceEnabled()) {
      LOG.trace("Returning " + result.rows.size() + " table columns");
    }
    return result;
  }

  /**
   * Returns the string representation of the given Impala column type to populate the
   * TYPE_NAME column of the result set returned by a HiveServer2 GetColumns() request.
   *
   * To be consistent with Hive's behavior, the TYPE_NAME field is populated with the
   * primitive type name for scalar types, and with the full toSql() for complex types.
   * The resulting type names are somewhat inconsistent, because nested types are printed
   * differently than top-level types, e.g.:
   * toSql()                     TYPE_NAME
   * DECIMAL(10,10)         -->  DECIMAL
   * CHAR(10)               -->  CHAR
   * VARCHAR(10)            -->  VARCHAR
   * ARRAY<DECIMAL(10,10)>  -->  ARRAY<DECIMAL(10,10)>
   * ARRAY<CHAR(10)>        -->  ARRAY<CHAR(10)>
   * ARRAY<VARCHAR(10)>     -->  ARRAY<VARCHAR(10)>
   */
  private static String getHs2MetadataTypeName(Type colType) {
    if (colType.isScalarType()) return colType.getPrimitiveType().toString();
    return colType.toSql();
  }

  /**
   * Executes the GetSchemas HiveServer2 operation and returns TResultSet.
   * It queries the Impala catalog to return the list of schemas that fit the search
   * pattern.
   * catalogName and schemaName are JDBC search patterns.
   */
  public static TResultSet getSchemas(Frontend fe,
      String catalogName, String schemaName, User user) throws ImpalaException {
    TResultSet result = createEmptyResultSet(GET_SCHEMAS_MD);

    // Get the list of schemas that satisfy the search condition.
    DbsMetadata dbsMetadata = getDbsMetadata(fe, catalogName,
        PatternMatcher.createJdbcPatternMatcher(schemaName),
        PatternMatcher.MATCHER_MATCH_NONE,
        PatternMatcher.MATCHER_MATCH_NONE,
        PatternMatcher.MATCHER_MATCH_NONE, user);

    for (int i = 0; i < dbsMetadata.dbs.size(); ++i) {
      String dbName = dbsMetadata.dbs.get(i);
      TResultRow row = new TResultRow();
      row.colVals = Lists.newArrayList();
      row.colVals.add(createTColumnValue(dbName)); // TABLE_SCHEM
      row.colVals.add(EMPTY_COL_VAL); // default Hive catalog is an empty string.
      result.rows.add(row);
    }

    if (LOG.isTraceEnabled()) {
      LOG.trace("Returning " + result.rows.size() + " schemas");
    }
    return result;
  }

  /**
   * Executes the GetTables HiveServer2 operation and returns TResultSet.
   * It queries the Impala catalog to return the list of tables that fit the search
   * patterns.
   * catalogName, schemaName and tableName are JDBC search patterns.
   * tableTypes specifies which table types to search for (TABLE, VIEW, etc).
   */
  public static TResultSet getTables(Frontend fe, String catalogName,
      String schemaName, String tableName, List<String> tableTypes, User user)
          throws ImpalaException{
    TResultSet result = createEmptyResultSet(GET_TABLES_MD);

    List<String> upperCaseTableTypes = null;
    if (tableTypes != null && !tableTypes.isEmpty()) {
      boolean hasValidTableType = false;
      upperCaseTableTypes = Lists.newArrayList();
      for (String tableType : tableTypes) {
        tableType = tableType.toUpperCase();
        upperCaseTableTypes.add(tableType);
        if (tableType.equals(TImpalaTableType.TABLE.name())) hasValidTableType = true;
        if (tableType.equals(TImpalaTableType.VIEW.name())) hasValidTableType = true;
      }
      if (!hasValidTableType) return result;
    }

    // Get the list of schemas, tables that satisfy the search conditions.
    DbsMetadata dbsMetadata = getDbsMetadata(fe, catalogName,
        PatternMatcher.createJdbcPatternMatcher(schemaName),
        PatternMatcher.createJdbcPatternMatcher(tableName),
        PatternMatcher.MATCHER_MATCH_NONE,
        PatternMatcher.MATCHER_MATCH_NONE, user);

    for (int i = 0; i < dbsMetadata.dbs.size(); ++i) {
      String dbName = dbsMetadata.dbs.get(i);
      for (int j = 0; j < dbsMetadata.tableNames.get(i).size(); ++j) {
        String tabName = dbsMetadata.tableNames.get(i).get(j);
        String tableType = dbsMetadata.tableTypes.get(i).get(j);
        if (upperCaseTableTypes != null && !upperCaseTableTypes.contains(tableType)) continue;

        TResultRow row = new TResultRow();
        row.colVals = Lists.newArrayList();
        row.colVals.add(EMPTY_COL_VAL);
        row.colVals.add(createTColumnValue(dbName));
        row.colVals.add(createTColumnValue(tabName));
        row.colVals.add(createTColumnValue(tableType));
        row.colVals.add(createTColumnValue(dbsMetadata.comments.get(i).get(j)));
        result.rows.add(row);
      }
    }
    if (LOG.isTraceEnabled()) LOG.trace("Returning " + result.rows.size() + " tables");
    return result;
  }

  /**
   * Executes the GetPrimaryKeys HiveServer2 operation and returns TResultSet.
   * This queries the Impala catalog to get the primary keys of a given table.
   * Similar to getColumns, matching primary key columns requires loading the
   * table metadata, so if any missing tables are found, an RPC to the CatalogServer
   * will be executed to request loading these tables. The matching process will be
   * restarted once the required tables have been loaded in the local Impalad Catalog or
   * the wait timeout has been reached.
   */
  public static TResultSet getPrimaryKeys(Frontend fe, TMetadataOpRequest request,
      User user) throws ImpalaException {
    TGetPrimaryKeysReq req = request.getGet_primary_keys_req();
    String catalogName = req.getCatalogName();
    String schemaName = req.getSchemaName();
    String tableName = req.getTableName();
    TResultSet result = createEmptyResultSet(GET_PRIMARY_KEYS_MD);
    // Get the list of schemas, tables that satisfy the search conditions.
    PatternMatcher schemaMatcher = PatternMatcher.createJdbcPatternMatcher(schemaName);
    PatternMatcher tableMatcher = PatternMatcher.createJdbcPatternMatcher(tableName);
    DbsMetadata dbsMetadata = getDbsMetadata(fe, catalogName, schemaMatcher,
        tableMatcher, PatternMatcher.MATCHER_MATCH_ALL,
        PatternMatcher.MATCHER_MATCH_NONE, user);

    if (!dbsMetadata.missingTbls.isEmpty()) {
      // Need to load tables for column metadata.
      StmtMetadataLoader mdLoader = new StmtMetadataLoader(fe, Catalog.DEFAULT_DB, null);
      mdLoader.loadTables(dbsMetadata.missingTbls);
      dbsMetadata = getDbsMetadata(fe, catalogName, schemaMatcher,
          tableMatcher, PatternMatcher.MATCHER_MATCH_ALL,
          PatternMatcher.MATCHER_MATCH_NONE, user);
    }

    for (int i = 0; i < dbsMetadata.dbs.size(); ++i) {
      for (int j = 0; j < dbsMetadata.tableNames.get(i).size(); ++j) {
        for (SQLPrimaryKey pk : dbsMetadata.primaryKeys.get(i).get(j)) {
          TResultRow row = new TResultRow();
          row.colVals = Lists.newArrayList();
          row.colVals.add(EMPTY_COL_VAL);
          row.colVals.add(createTColumnValue(pk.getTable_db()));
          row.colVals.add(createTColumnValue(pk.getTable_name()));
          row.colVals.add(createTColumnValue(pk.getColumn_name()));
          row.colVals.add(createTColumnValue(pk.getKey_seq()));
          row.colVals.add(createTColumnValue(pk.getPk_name()));
          result.rows.add(row);
        }
      }
    }
    if (LOG.isTraceEnabled()) {
      LOG.trace("Returning {} primary keys for table {}.", result.rows.size(), tableName);
    }
    return result;
  }

  /**
   * Executes the GetCrossReference HiveServer2 operation and returns TResultSet.
   * This queries the Impala catalog to get the foreign keys of a given table.
   * Similar to getColumns, matching foreign key columns requires loading the
   * table metadata, so if any missing tables are found, an RPC to the CatalogServer
   * will be executed to request loading these tables. The matching process will be
   * restarted once the required tables have been loaded in the local Impalad Catalog or
   * the wait timeout has been reached. If parent schema and parent table are specified
   * in the request, we return only the foreign keys related to the parent table. If
   * not, we return all foreign keys associated with the foreign table.
   */
  public static TResultSet getCrossReference(Frontend fe, TMetadataOpRequest request,
      User user) throws ImpalaException {
    TGetCrossReferenceReq req = request.getGet_cross_reference_req();
    String foreignCatalogName = req.getForeignCatalogName();
    String foreignSchemaName = req.getForeignSchemaName();
    String foreignTableName = req.getForeignTableName();
    String parentSchemaName = req.getParentSchemaName();
    String parentTableName = req.getParentTableName();
    TResultSet result = createEmptyResultSet(GET_CROSS_REFERENCE_MD);
    // Get the list of schemas, tables that satisfy the search conditions.
    PatternMatcher schemaMatcher =
        PatternMatcher.createJdbcPatternMatcher(foreignSchemaName);
    PatternMatcher tableMatcher =
        PatternMatcher.createJdbcPatternMatcher(foreignTableName);
    DbsMetadata dbsMetadata = getDbsMetadata(fe, foreignCatalogName, schemaMatcher,
        tableMatcher, PatternMatcher.MATCHER_MATCH_ALL,
        PatternMatcher.MATCHER_MATCH_NONE, user);

    if (!dbsMetadata.missingTbls.isEmpty()) {
      // Need to load tables for column metadata.
      StmtMetadataLoader mdLoader = new StmtMetadataLoader(fe, Catalog.DEFAULT_DB, null);
      mdLoader.loadTables(dbsMetadata.missingTbls);
      dbsMetadata = getDbsMetadata(fe, foreignCatalogName, schemaMatcher,
          tableMatcher, PatternMatcher.MATCHER_MATCH_ALL,
          PatternMatcher.MATCHER_MATCH_NONE, user);
    }

    for (int i = 0; i < dbsMetadata.dbs.size(); ++i) {
      for (int j = 0; j < dbsMetadata.tableNames.get(i).size(); ++j) {
        // HMS API allows querying FK information for specific pk/fk columns. In Impala,
        // we store all foreign keys associated with at table together in the Table
        // object. For this metadata we filter out the foreignKeys which are not matching
        // given parent and foreign schema information.
        List<SQLForeignKey> filteredForeignKeys =
            filterForeignKeys(dbsMetadata.foreignKeys.get(i).get(j), parentSchemaName,
                parentTableName);
        for (SQLForeignKey fk : filteredForeignKeys) {
          TResultRow row = new TResultRow();
          row.colVals = Lists.newArrayList();
          row.colVals.add(EMPTY_COL_VAL); // PKTABLE_CAT
          row.colVals.add(createTColumnValue(fk.getPktable_db()));
          row.colVals.add(createTColumnValue(fk.getPktable_name()));
          row.colVals.add(createTColumnValue(fk.getPkcolumn_name()));
          row.colVals.add(EMPTY_COL_VAL); // FKTABLE_CAT
          row.colVals.add(createTColumnValue(fk.getFktable_db()));
          row.colVals.add(createTColumnValue(fk.getFktable_name()));
          row.colVals.add(createTColumnValue(fk.getFkcolumn_name()));
          row.colVals.add(createTColumnValue(fk.getKey_seq()));
          row.colVals.add(createTColumnValue(fk.getUpdate_rule()));
          row.colVals.add(createTColumnValue(fk.getDelete_rule()));
          row.colVals.add(createTColumnValue(fk.getFk_name()));
          row.colVals.add(createTColumnValue(fk.getPk_name()));
          // DEFERRABILITY is currently not supported.
          row.colVals.add(EMPTY_COL_VAL); // DEFERRABILITY
          result.rows.add(row);
        }
      }
    }
    if (LOG.isTraceEnabled()) {
      LOG.trace("Returning {} foreign keys for table {}.", result.rows.size(),
          foreignTableName);
    }
    return result;
  }

  /**
   * Helper to filter foreign keys based on given parent table. If both parent schema
   * name and parent table name are specified, we return only the foreign keys that are
   * associated with the parent table. Otherwise, we return all the foreign keys for
   * this table.
   */
  private static List<SQLForeignKey> filterForeignKeys(List<SQLForeignKey> dbForeignKeys,
      String pkSchemaName, String pkTableName) {
    // If pkSchema or pkTable are not specified (For example: We want to retrieve all
    // the foreignKeys from the foreign key side.) we return all the foreign keys.
    if (pkSchemaName == null || pkTableName == null) {
      return new ArrayList<>(dbForeignKeys);
    }
    List<SQLForeignKey> foreignKeys = new ArrayList<>();
    for (SQLForeignKey fk : dbForeignKeys) {
      if (fk.getPktable_db().equals(pkSchemaName) &&
          fk.getPktable_name().equals(pkTableName)) {
        foreignKeys.add(fk);
      }
    }
    return foreignKeys;
  }

  /**
   * Executes the GetTypeInfo HiveServer2 operation and returns Impala supported types.
   */
  public static TResultSet getTypeInfo() {
    TResultSet result = createEmptyResultSet(GET_TYPEINFO_MD);
    result.rows = GET_TYPEINFO_RESULTS;
    return result;
  }

  /**
   * Executes the GetTableTypes HiveServer2 operation.
   */
  public static TResultSet getTableTypes() {
    TResultSet result = createEmptyResultSet(GET_TABLE_TYPES_MD);
    result.rows = GET_TABLE_TYPES_RESULTS;
    return result;
  }

  /**
   * Create a function result row in the JDBC format.
   */
  private static TResultRow createFunctionResultRow(Function fn) {
    TResultRow row = new TResultRow();
    row.colVals = Lists.newArrayList();
    row.colVals.add(NULL_COL_VAL); // FUNCTION_CAT
    row.colVals.add(createTColumnValue(fn.dbName())); // FUNCTION_SCHEM
    row.colVals.add(createTColumnValue(fn.functionName())); // FUNCTION_NAME
    row.colVals.add(EMPTY_COL_VAL); // REMARKS
    // FUNCTION_TYPE
    row.colVals.add(createTColumnValue(DatabaseMetaData.functionNoTable));
    row.colVals.add(createTColumnValue(fn.signatureString())); // SPECIFIC_NAME
    return row;
  }

  /**
   * Executes the GetFunctions HiveServer2 operation and returns TResultSet.
   * Returns the list of functions that fit the search patterns.
   * catalogName, schemaName and functionName are JDBC search patterns.
   * @throws ImpalaException
   */
  public static TResultSet getFunctions(Frontend fe,
      String catalogName, String schemaName, String functionName,
      User user) throws ImpalaException {
    TResultSet result = createEmptyResultSet(GET_FUNCTIONS_MD);

    // Impala's built-in functions do not have a catalog name or schema name.
    if (!isEmptyPattern(catalogName) || !isEmptyPattern(schemaName)) {
      return result;
    }

    DbsMetadata dbsMetadata = getDbsMetadata(fe, catalogName,
        PatternMatcher.createJdbcPatternMatcher(schemaName),
        PatternMatcher.MATCHER_MATCH_NONE,
        PatternMatcher.MATCHER_MATCH_NONE,
        PatternMatcher.createJdbcPatternMatcher(functionName), user);
    for (List<Function> fns: dbsMetadata.functions) {
      for (Function fn: fns) {
        result.rows.add(createFunctionResultRow(fn));
      }
    }

    return result;
  }

  /**
   * Return result row corresponding to the input type.
   */
  private static TResultRow createGetTypeInfoResult(String typeName, Type type) {
    TResultRow row = new TResultRow();
    row.colVals = Lists.newArrayList();
    row.colVals.add(createTColumnValue(typeName)); // TYPE_NAME
    row.colVals.add(createTColumnValue(type.getJavaSqlType())); // DATA_TYPE
    row.colVals.add(createTColumnValue(type.getPrecision())); // PRECISION
    row.colVals.add(NULL_COL_VAL); // LITERAL_PREFIX
    row.colVals.add(NULL_COL_VAL); // LITERAL_SUFFIX
    row.colVals.add(NULL_COL_VAL); // CREATE_PARAMS
    row.colVals.add(createTColumnValue(DatabaseMetaData.typeNullable)); // NULLABLE
    row.colVals.add(createTColumnValue(type.isStringType())); // CASE_SENSITIVE
    row.colVals.add(createTColumnValue(DatabaseMetaData.typeSearchable)); // SEARCHABLE
    row.colVals.add(createTColumnValue(!type.isNumericType())); // UNSIGNED_ATTRIBUTE
    row.colVals.add(createTColumnValue(false)); // FIXED_PREC_SCALE
    row.colVals.add(createTColumnValue(false)); // AUTO_INCREMENT
    row.colVals.add(NULL_COL_VAL); // LOCAL_TYPE_NAME
    row.colVals.add(createTColumnValue(0)); // MINIMUM_SCALE
    row.colVals.add(createTColumnValue(0)); // MAXIMUM_SCALE
    row.colVals.add(NULL_COL_VAL); // SQL_DATA_TYPE
    row.colVals.add(NULL_COL_VAL); // SQL_DATETIME_SUB
    row.colVals.add(createTColumnValue(type.getNumPrecRadix())); // NUM_PREC_RADIX
    return row;
  }

  /**
   * Fills the GET_TYPEINFO_RESULTS with externally supported primitive types.
   */
  private static void createGetPrimitiveTypeInfoResults() {
    for (PrimitiveType ptype: PrimitiveType.values()) {
      ScalarType type = Type.getDefaultScalarType(ptype);
      if (type.isInternalType() || !type.isSupported()) continue;
      TResultRow row = createGetTypeInfoResult(ptype.name(), type);
      GET_TYPEINFO_RESULTS.add(row);
    }
  }

  /**
   * Fills the GET_TYPEINFO_RESULTS with externally supported types.
   */
  private static void createGetTypeInfoResults() {
    // Loop through the types included in the TTypeNodeType enum so that all the types
    // (both existing and any new ones which get added) are accounted for.
    for (TTypeNodeType nodeType : TTypeNodeType.values()) {
      Type type = null;
      if (nodeType == TTypeNodeType.SCALAR) {
        createGetPrimitiveTypeInfoResults();
        continue;
      } else if (nodeType == TTypeNodeType.ARRAY) {
        type = new ArrayType(ScalarType.createType(PrimitiveType.INT));
      } else if (nodeType == TTypeNodeType.MAP) {
        type = new MapType(ScalarType.createType(PrimitiveType.INT),
            ScalarType.createType(PrimitiveType.INT));
      } else if (nodeType == TTypeNodeType.STRUCT) {
        type = new StructType();
      }

      if (!type.isSupported()) continue;
      TResultRow row = createGetTypeInfoResult(nodeType.name(), type);
      GET_TYPEINFO_RESULTS.add(row);
    }
  }

  /**
   * Fills the GET_TYPEINFO_RESULTS with "TABLE", "VIEW".
   */
  private static void createGetTableTypesResults() {
    TResultRow row = new TResultRow();
    row.colVals = Lists.newArrayList();
    row.colVals.add(createTColumnValue(TImpalaTableType.TABLE.name()));
    GET_TABLE_TYPES_RESULTS.add(row);
    row = new TResultRow();
    row.colVals = Lists.newArrayList();
    row.colVals.add(createTColumnValue(TImpalaTableType.VIEW.name()));
    GET_TABLE_TYPES_RESULTS.add(row);
  }

  /**
   * Returns an TResultSet with the specified schema. The
   * result set will be empty.
   */
  private static TResultSet createEmptyResultSet(TResultSetMetadata metadata) {
    TResultSet result = new TResultSet();
    result.rows = Lists.newArrayList();
    result.schema = metadata;
    return result;
  }

  // Helper methods to create TColumnValue
  public static TColumnValue createTColumnValue(String val) {
    TColumnValue colVal = new TColumnValue();
    if (val != null) {
      colVal.setString_val(val);
    }
    return colVal;
  }

  public static TColumnValue createTColumnValue(Integer val) {
    TColumnValue colVal = new TColumnValue();
    if (val != null) {
      colVal.setInt_val(val.intValue());
    }
    return colVal;
  }

  public static TColumnValue createTColumnValue(Boolean val) {
    TColumnValue colVal = new TColumnValue();
    if (val != null) {
      colVal.setBool_val(val);
    }
    return colVal;
  }

  /**
   * Returns true if the JDBC search pattern is empty: either null, empty string or "%".
   */
  public static boolean isEmptyPattern(final String pattern) {
    return (pattern == null) || pattern.isEmpty() ||
           (pattern.length() == 1 && pattern.equals("%"));
  }
}
