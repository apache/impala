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
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.hadoop.hive.metastore.TableType;
import org.apache.impala.analysis.StmtMetadataLoader;
import org.apache.impala.analysis.TableName;
import org.apache.impala.authorization.User;
import org.apache.impala.catalog.Catalog;
import org.apache.impala.catalog.Column;
import org.apache.impala.catalog.Db;
import org.apache.impala.catalog.Function;
import org.apache.impala.catalog.ImpaladCatalog;
import org.apache.impala.catalog.PrimitiveType;
import org.apache.impala.catalog.ScalarType;
import org.apache.impala.catalog.Table;
import org.apache.impala.catalog.Type;
import org.apache.impala.common.ImpalaException;
import org.apache.impala.thrift.TColumn;
import org.apache.impala.thrift.TColumnValue;
import org.apache.impala.thrift.TResultRow;
import org.apache.impala.thrift.TResultSet;
import org.apache.impala.thrift.TResultSetMetadata;
import org.apache.impala.util.PatternMatcher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Strings;
import com.google.common.collect.Lists;

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
  private static final String TABLE_TYPE_TABLE = "TABLE";
  private static final String TABLE_TYPE_VIEW = "VIEW";

  // Result set schema for each of the metadata operations.
  private final static TResultSetMetadata GET_CATALOGS_MD = new TResultSetMetadata();
  private final static TResultSetMetadata GET_COLUMNS_MD = new TResultSetMetadata();
  private final static TResultSetMetadata GET_SCHEMAS_MD = new TResultSetMetadata();
  private final static TResultSetMetadata GET_TABLES_MD = new TResultSetMetadata();
  private static final TResultSetMetadata GET_TYPEINFO_MD = new TResultSetMetadata();
  private static final TResultSetMetadata GET_TABLE_TYPES_MD = new TResultSetMetadata();
  private static final TResultSetMetadata GET_FUNCTIONS_MD = new TResultSetMetadata();

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
  }

  /**
   * Contains lists of databases, lists of table belonging to the dbs, list of columns
   * belonging to the tables, and list of user functions.
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
    DbsMetadata result = new DbsMetadata();

    // Hive does not have a catalog concept. Returns nothing if the request specifies an
    // non-empty catalog pattern.
    if (!isEmptyPattern(catalogName)) {
      return result;
    }

    ImpaladCatalog catalog = fe.getCatalog();
    for (Db db: fe.getDbs(schemaPatternMatcher, user)) {
      if (fnPatternMatcher != PatternMatcher.MATCHER_MATCH_NONE) {
        // Get function metadata
        List<Function> fns = db.getFunctions(null, fnPatternMatcher);
        result.functions.add(fns);
      } else {
        // Get table metadata
        List<String> tableList = Lists.newArrayList();
        List<List<Column>> tablesColumnsList = Lists.newArrayList();
        List<String> tableComments = Lists.newArrayList();
        List<String> tableTypes = Lists.newArrayList();
        for (String tabName: fe.getTableNames(db.getName(), tablePatternMatcher, user)) {
          Table table = catalog.getTable(db.getName(), tabName);
          if (table == null) continue;

          String comment = null;
          List<Column> columns = Lists.newArrayList();
          // If the table is not yet loaded, the columns will be unknown. Add it
          // to the set of missing tables.
          String tableType = TABLE_TYPE_TABLE;
          if (!table.isLoaded()) {
            result.missingTbls.add(new TableName(db.getName(), tabName));
          } else {
            if (table.getMetaStoreTable() != null) {
              comment = table.getMetaStoreTable().getParameters().get("comment");
              tableType = mapToInternalTableType(table.getMetaStoreTable().getTableType());
            }
            columns.addAll(fe.getColumns(table, columnPatternMatcher, user));
          }
          tableList.add(tabName);
          tablesColumnsList.add(columns);
          tableComments.add(Strings.nullToEmpty(comment));
          tableTypes.add(tableType);
        }
        result.dbs.add(db.getName());
        result.tableNames.add(tableList);
        result.comments.add(tableComments);
        result.columns.add(tablesColumnsList);
        result.tableTypes.add(tableTypes);
      }
    }
    return result;
  }

  private static String mapToInternalTableType(String typeStr) {
    String defaultTableType = TABLE_TYPE_TABLE;
    TableType tType;

    if (typeStr == null) return defaultTableType;
    try {
      tType = TableType.valueOf(typeStr.toUpperCase());
    } catch (Exception e) {
      return defaultTableType;
    }
    switch (tType) {
      case EXTERNAL_TABLE:
      case MANAGED_TABLE:
      case INDEX_TABLE:
        return TABLE_TYPE_TABLE;
      case VIRTUAL_VIEW:
        return TABLE_TYPE_VIEW;
      default:
        return defaultTableType;
    }
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
        if (tableType.equals(TABLE_TYPE_TABLE)) hasValidTableType = true;
        if (tableType.equals(TABLE_TYPE_VIEW)) hasValidTableType = true;
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
   * Fills the GET_TYPEINFO_RESULTS with supported primitive types.
   */
  private static void createGetTypeInfoResults() {
    for (PrimitiveType ptype: PrimitiveType.values()) {
      if (ptype.equals(PrimitiveType.INVALID_TYPE) ||
          ptype.equals(PrimitiveType.DATE) ||
          ptype.equals(PrimitiveType.DATETIME) ||
          ptype.equals(PrimitiveType.DECIMAL) ||
          ptype.equals(PrimitiveType.CHAR) ||
          ptype.equals(PrimitiveType.VARCHAR) ||
          ptype.equals(PrimitiveType.FIXED_UDA_INTERMEDIATE)) {
        continue;
      }
      Type type = ScalarType.createType(ptype);
      TResultRow row = new TResultRow();
      row.colVals = Lists.newArrayList();
      row.colVals.add(createTColumnValue(ptype.name())); // TYPE_NAME
      row.colVals.add(createTColumnValue(type.getJavaSqlType()));  // DATA_TYPE
      row.colVals.add(createTColumnValue(type.getPrecision()));  // PRECISION
      row.colVals.add(NULL_COL_VAL); // LITERAL_PREFIX
      row.colVals.add(NULL_COL_VAL); // LITERAL_SUFFIX
      row.colVals.add(NULL_COL_VAL); // CREATE_PARAMS
      row.colVals.add(createTColumnValue(DatabaseMetaData.typeNullable));  // NULLABLE
      row.colVals.add(createTColumnValue(type.isStringType())); // CASE_SENSITIVE
      row.colVals.add(createTColumnValue(DatabaseMetaData.typeSearchable));  // SEARCHABLE
      row.colVals.add(createTColumnValue(!type.isNumericType())); // UNSIGNED_ATTRIBUTE
      row.colVals.add(createTColumnValue(false));  // FIXED_PREC_SCALE
      row.colVals.add(createTColumnValue(false));  // AUTO_INCREMENT
      row.colVals.add(NULL_COL_VAL); // LOCAL_TYPE_NAME
      row.colVals.add(createTColumnValue(0));  // MINIMUM_SCALE
      row.colVals.add(createTColumnValue(0));  // MAXIMUM_SCALE
      row.colVals.add(NULL_COL_VAL); // SQL_DATA_TYPE
      row.colVals.add(NULL_COL_VAL); // SQL_DATETIME_SUB
      row.colVals.add(createTColumnValue(type.getNumPrecRadix()));  // NUM_PREC_RADIX
      GET_TYPEINFO_RESULTS.add(row);
    }
  }

  /**
   * Fills the GET_TYPEINFO_RESULTS with "TABLE", "VIEW".
   */
  private static void createGetTableTypesResults() {
    TResultRow row = new TResultRow();
    row.colVals = Lists.newArrayList();
    row.colVals.add(createTColumnValue(TABLE_TYPE_TABLE));
    GET_TABLE_TYPES_RESULTS.add(row);
    row = new TResultRow();
    row.colVals = Lists.newArrayList();
    row.colVals.add(createTColumnValue(TABLE_TYPE_VIEW));
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
