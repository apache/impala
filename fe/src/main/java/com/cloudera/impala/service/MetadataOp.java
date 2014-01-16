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

package com.cloudera.impala.service;

import java.sql.DatabaseMetaData;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.regex.Pattern;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.cloudera.impala.analysis.TableName;
import com.cloudera.impala.authorization.Privilege;
import com.cloudera.impala.authorization.User;
import com.cloudera.impala.catalog.Column;
import com.cloudera.impala.catalog.ColumnType;
import com.cloudera.impala.catalog.Db;
import com.cloudera.impala.catalog.ImpaladCatalog;
import com.cloudera.impala.catalog.PrimitiveType;
import com.cloudera.impala.catalog.Table;
import com.cloudera.impala.common.ImpalaException;
import com.cloudera.impala.thrift.TColumn;
import com.cloudera.impala.thrift.TColumnValue;
import com.cloudera.impala.thrift.TFunctionType;
import com.cloudera.impala.thrift.TResultRow;
import com.cloudera.impala.thrift.TResultSet;
import com.cloudera.impala.thrift.TResultSetMetadata;
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
  private static final TColumnValue TABLE_TYPE_COL_VAL = createTColumnValue("TABLE");

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

  // GetTableTypes only returns a single value: "TABLE".
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
    GET_CATALOGS_MD.addToColumns(new TColumn("TABLE_CAT", ColumnType.STRING.toThrift()));

    GET_COLUMNS_MD.addToColumns(
        new TColumn("TABLE_CAT", ColumnType.STRING.toThrift()));
    GET_COLUMNS_MD.addToColumns(
        new TColumn("TABLE_MD", ColumnType.STRING.toThrift()));
    GET_COLUMNS_MD.addToColumns(
        new TColumn("TABLE_NAME", ColumnType.STRING.toThrift()));
    GET_COLUMNS_MD.addToColumns(
        new TColumn("COLUMN_NAME", ColumnType.STRING.toThrift()));
    GET_COLUMNS_MD.addToColumns(
        new TColumn("DATA_TYPE", ColumnType.INT.toThrift()));
    GET_COLUMNS_MD.addToColumns(
        new TColumn("TYPE_NAME", ColumnType.STRING.toThrift()));
    GET_COLUMNS_MD.addToColumns(
        new TColumn("COLUMN_SIZE", ColumnType.INT.toThrift()));
    GET_COLUMNS_MD.addToColumns(
        new TColumn("BUFFER_LENGTH", ColumnType.INT.toThrift()));
    GET_COLUMNS_MD.addToColumns(
        new TColumn("DECIMAL_DIGITS", ColumnType.INT.toThrift()));
    GET_COLUMNS_MD.addToColumns(
        new TColumn("NUM_PREC_RADIX", ColumnType.INT.toThrift()));
    GET_COLUMNS_MD.addToColumns(
        new TColumn("NULLABLE", ColumnType.INT.toThrift()));
    GET_COLUMNS_MD.addToColumns(
        new TColumn("REMARKS", ColumnType.STRING.toThrift()));
    GET_COLUMNS_MD.addToColumns(
        new TColumn("COLUMN_DEF", ColumnType.STRING.toThrift()));
    GET_COLUMNS_MD.addToColumns(
        new TColumn("SQL_DATA_TYPE", ColumnType.INT.toThrift()));
    GET_COLUMNS_MD.addToColumns(
        new TColumn("SQL_DATETIME_SUB", ColumnType.INT.toThrift()));
    GET_COLUMNS_MD.addToColumns(
        new TColumn("CHAR_OCTET_LENGTH", ColumnType.INT.toThrift()));
    GET_COLUMNS_MD.addToColumns(
        new TColumn("ORDINAL_POSITION", ColumnType.INT.toThrift()));
    GET_COLUMNS_MD.addToColumns(
        new TColumn("IS_NULLABLE", ColumnType.STRING.toThrift()));
    GET_COLUMNS_MD.addToColumns(
        new TColumn("SCOPE_CATALOG", ColumnType.STRING.toThrift()));
    GET_COLUMNS_MD.addToColumns(
        new TColumn("SCOPE_SCHEMA", ColumnType.STRING.toThrift()));
    GET_COLUMNS_MD.addToColumns(
        new TColumn("SCOPE_TABLE", ColumnType.STRING.toThrift()));
    GET_COLUMNS_MD.addToColumns(
        new TColumn("SOURCE_DATA_TYPE", ColumnType.SMALLINT.toThrift()));
    GET_COLUMNS_MD.addToColumns(
        new TColumn("IS_AUTO_INCREMENT", ColumnType.STRING.toThrift()));

    GET_SCHEMAS_MD.addToColumns(
        new TColumn("TABLE_SCHEM", ColumnType.STRING.toThrift()));
    GET_SCHEMAS_MD.addToColumns(
        new TColumn("TABLE_CATALOG", ColumnType.STRING.toThrift()));

    GET_TABLES_MD.addToColumns(
        new TColumn("TABLE_CAT", ColumnType.STRING.toThrift()));
    GET_TABLES_MD.addToColumns(
        new TColumn("TABLE_SCHEM", ColumnType.STRING.toThrift()));
    GET_TABLES_MD.addToColumns(
        new TColumn("TABLE_NAME", ColumnType.STRING.toThrift()));
    GET_TABLES_MD.addToColumns(
        new TColumn("TABLE_TYPE", ColumnType.STRING.toThrift()));
    GET_TABLES_MD.addToColumns(
        new TColumn("REMARKS", ColumnType.STRING.toThrift()));

    GET_TYPEINFO_MD.addToColumns(
        new TColumn("TYPE_NAME", ColumnType.STRING.toThrift()));
    GET_TYPEINFO_MD.addToColumns(
        new TColumn("DATA_TYPE", ColumnType.INT.toThrift()));
    GET_TYPEINFO_MD.addToColumns(
        new TColumn("PRECISION", ColumnType.INT.toThrift()));
    GET_TYPEINFO_MD.addToColumns(
        new TColumn("LITERAL_PREFIX", ColumnType.STRING.toThrift()));
    GET_TYPEINFO_MD.addToColumns(
        new TColumn("LITERAL_SUFFIX", ColumnType.STRING.toThrift()));
    GET_TYPEINFO_MD.addToColumns(
        new TColumn("CREATE_PARAMS", ColumnType.STRING.toThrift()));
    GET_TYPEINFO_MD.addToColumns(
        new TColumn("NULLABLE", ColumnType.INT.toThrift()));
    GET_TYPEINFO_MD.addToColumns(
        new TColumn("CASE_SENSITIVE", ColumnType.BOOLEAN.toThrift()));
    GET_TYPEINFO_MD.addToColumns(
        new TColumn("SEARCHABLE", ColumnType.SMALLINT.toThrift()));
    GET_TYPEINFO_MD.addToColumns(
        new TColumn("UNSIGNED_ATTRIBUTE", ColumnType.BOOLEAN.toThrift()));
    GET_TYPEINFO_MD.addToColumns(
        new TColumn("FIXED_PREC_SCALE", ColumnType.BOOLEAN.toThrift()));
    GET_TYPEINFO_MD.addToColumns(
        new TColumn("AUTO_INCREMENT", ColumnType.BOOLEAN.toThrift()));
    GET_TYPEINFO_MD.addToColumns(
        new TColumn("LOCAL_TYPE_NAME", ColumnType.STRING.toThrift()));
    GET_TYPEINFO_MD.addToColumns(
        new TColumn("MINIMUM_SCALE", ColumnType.SMALLINT.toThrift()));
    GET_TYPEINFO_MD.addToColumns(
        new TColumn("MAXIMUM_SCALE", ColumnType.SMALLINT.toThrift()));
    GET_TYPEINFO_MD.addToColumns(
        new TColumn("SQL_DATA_TYPE", ColumnType.INT.toThrift()));
    GET_TYPEINFO_MD.addToColumns(
        new TColumn("SQL_DATETIME_SUB", ColumnType.INT.toThrift()));
    GET_TYPEINFO_MD.addToColumns(
        new TColumn("NUM_PREC_RADIX", ColumnType.INT.toThrift()));

    GET_TABLE_TYPES_MD.addToColumns(
        new TColumn("TABLE_TYPE", ColumnType.STRING.toThrift()));

    GET_FUNCTIONS_MD.addToColumns(
        new TColumn("FUNCTION_CAT", ColumnType.STRING.toThrift()));
    GET_FUNCTIONS_MD.addToColumns(
        new TColumn("FUNCTION_SCHEM", ColumnType.STRING.toThrift()));
    GET_FUNCTIONS_MD.addToColumns(
        new TColumn("FUNCTION_NAME", ColumnType.STRING.toThrift()));
    GET_FUNCTIONS_MD.addToColumns(
        new TColumn("REMARKS", ColumnType.STRING.toThrift()));
    GET_FUNCTIONS_MD.addToColumns(
        new TColumn("FUNCTION_TYPE", ColumnType.INT.toThrift()));
    GET_FUNCTIONS_MD.addToColumns(
        new TColumn("SPECIFIC_NAME", ColumnType.STRING.toThrift()));
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

    // columns[i][j] are the columns of tableNames[j] in dbs[i].
    // If the table is missing (not yet loaded) its column list will be empty.
    public List<List<List<Column>>> columns = Lists.newArrayList();

    // functions[i] are the functions within dbs[i]
    public List<List<String>> functions = Lists.newArrayList();

    // Set of tables that are missing (not yet loaded).
    public Set<TableName> missingTbls = new HashSet<TableName>();
  }

  /**
   * Returns the list of schemas, tables, columns and user functions that satisfy the
   * search pattern. catalogName, schemaName, tableName, columnName and functionName
   * are JDBC search patterns.
   *
   * The return value DbsTablesColumns.dbs contains the list of databases that satisfy
   * the "schemaName" search pattern.
   * DbsTablesColumns.tableNames[i] contains the list of tables inside dbs[i] that satisfy
   * the "tableName" search pattern.
   * DbsTablesColumns.columns[i][j] contains the list of columns of table[j] in dbs[i]
   * that satisfy the search condition "columnName".
   * DbsTablesColumns.functions[i] contains the list of functions inside dbs[i] that
   * satisfy the "functionName" search pattern.
   *
   * If tableName is null, then DbsTablesColumns.tableNames and DbsTablesColumns.columns
   * will not be populated.
   * If columns is null, then DbsTablesColumns.columns will not be populated.
   */
  private static DbsMetadata getDbsMetadata(ImpaladCatalog catalog, String catalogName,
      String schemaName, String tableName, String columnName, String functionName,
      User user) throws ImpalaException {
    DbsMetadata result = new DbsMetadata();

    // Hive does not have a catalog concept. Returns nothing if the request specifies an
    // non-empty catalog pattern.
    if (!isEmptyPattern(catalogName)) {
      return result;
    }

    // Creates the schema, table and column search patterns
    String convertedSchemaPattern = convertPattern(schemaName);
    String convertedTablePattern = convertPattern(tableName);
    String convertedColumnPattern = convertPattern(columnName);
    String convertedFunctionPattern = convertPattern(functionName);
    Pattern schemaPattern = Pattern.compile(convertedSchemaPattern);
    Pattern tablePattern = Pattern.compile(convertedTablePattern);
    Pattern columnPattern = Pattern.compile(convertedColumnPattern);
    Pattern functionPattern = Pattern.compile(convertedFunctionPattern);

    for (String dbName: catalog.getDbNames(null, user)) {
      if (!schemaPattern.matcher(dbName).matches()) {
        continue;
      }

      Db db = catalog.getDb(dbName, user, Privilege.ANY);

      List<String> tableList = Lists.newArrayList();
      List<List<Column>> tablesColumnsList = Lists.newArrayList();
      if (tableName != null) {
        for (String tabName: catalog.getTableNames(db.getName(), "*", user)) {
          if (!tablePattern.matcher(tabName).matches()) {
            continue;
          }
          tableList.add(tabName);
          List<Column> columns = Lists.newArrayList();

          if (columnName != null) {
            Table table = catalog.getTable(dbName, tabName, user, Privilege.ANY);
            if (table == null) continue;
            // If the table is not yet loaded, the columns will be unknown. Add it
            // to the set of missing tables.
            if (!table.isLoaded()) {
              result.missingTbls.add(new TableName(dbName, tabName));
            } else {
              for (Column column: table.getColumns()) {
                String colName = column.getName();
                if (!columnPattern.matcher(colName).matches()) {
                  continue;
                }
                columns.add(column);
              }
            }
          }
          tablesColumnsList.add(columns);
        }
      }
      if (functionName != null) {
        List<String> fns = db.getAllFunctionSignatures(TFunctionType.SCALAR);
        fns.addAll(db.getAllFunctionSignatures(TFunctionType.AGGREGATE));
        List<String> filteredFns = Lists.newArrayList();
        for (String fn: fns) {
          if (functionPattern.matcher(fn).matches()) {
            filteredFns.add(fn);
          }
        }
        result.functions.add(filteredFns);
      }

      result.dbs.add(dbName);
      result.tableNames.add(tableList);
      result.columns.add(tablesColumnsList);
    }
    return result;
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
  public static TResultSet getColumns(Frontend fe,
      String catalogName, String schemaName, String tableName, String columnName,
      User user)
      throws ImpalaException {
    TResultSet result = createEmptyResultSet(GET_COLUMNS_MD);

    // Get the list of schemas, tables, and columns that satisfy the search conditions.
    DbsMetadata dbsMetadata = null;
    while (dbsMetadata == null || !dbsMetadata.missingTbls.isEmpty()) {
      dbsMetadata = getDbsMetadata(fe.getCatalog(), catalogName,
          schemaName, tableName, columnName, null, user);
      if (!fe.requestTblLoadAndWait(dbsMetadata.missingTbls)) {
        LOG.info("Timed out waiting for missing tables. Load request will be retried.");
      }
    }

    for (int i = 0; i < dbsMetadata.dbs.size(); ++i) {
      String dbName = dbsMetadata.dbs.get(i);
      for (int j = 0; j < dbsMetadata.tableNames.get(i).size(); ++j) {
        String tabName = dbsMetadata.tableNames.get(i).get(j);
        for (int k = 0; k < dbsMetadata.columns.get(i).get(j).size(); ++k) {
          Column column = dbsMetadata.columns.get(i).get(j).get(k);
          ColumnType colType = column.getType();
          TResultRow row = new TResultRow();
          row.colVals = Lists.newArrayList();
          row.colVals.add(NULL_COL_VAL); // TABLE_CAT
          row.colVals.add(createTColumnValue(dbName)); // TABLE_SCHEM
          row.colVals.add(createTColumnValue(tabName)); // TABLE_NAME
          row.colVals.add(createTColumnValue(column.getName())); // COLUMN_NAME
          row.colVals.add(createTColumnValue(colType.getJavaSQLType())); // DATA_TYPE
          row.colVals.add(
              createTColumnValue(colType.getPrimitiveType().name())); // TYPE_NAME
          row.colVals.add(createTColumnValue(colType.getColumnSize())); // COLUMN_SIZE
          row.colVals.add(NULL_COL_VAL); // BUFFER_LENGTH, unused
          // DECIMAL_DIGITS
          row.colVals.add(createTColumnValue(colType.getDecimalDigits()));
          // NUM_PREC_RADIX
          row.colVals.add(createTColumnValue(colType.getNumPrecRadix()));
          // NULLABLE
          row.colVals.add(createTColumnValue(DatabaseMetaData.columnNullable));
          row.colVals.add(NULL_COL_VAL); // REMARKS
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
    LOG.debug("Returning " + result.rows.size() + " table columns");
    return result;
  }

  /**
   * Executes the GetSchemas HiveServer2 operation and returns TResultSet.
   * It queries the Impala catalog to return the list of schemas that fit the search
   * pattern.
   * catalogName and schemaName are JDBC search patterns.
   */
  public static TResultSet getSchemas(ImpaladCatalog catalog,
      String catalogName, String schemaName, User user) throws ImpalaException {
    TResultSet result = createEmptyResultSet(GET_SCHEMAS_MD);

    // Get the list of schemas that satisfy the search condition.
    DbsMetadata dbsMetadata = getDbsMetadata(catalog, catalogName,
        schemaName, null, null, null, user);

    for (int i = 0; i < dbsMetadata.dbs.size(); ++i) {
      String dbName = dbsMetadata.dbs.get(i);
      TResultRow row = new TResultRow();
      row.colVals = Lists.newArrayList();
      row.colVals.add(createTColumnValue(dbName)); // TABLE_SCHEM
      row.colVals.add(EMPTY_COL_VAL); // default Hive catalog is an empty string.
      result.rows.add(row);
    }

    LOG.debug("Returning " + result.rows.size() + " schemas");
    return result;
  }

  /**
   * Executes the GetTables HiveServer2 operation and returns TResultSet.
   * It queries the Impala catalog to return the list of tables that fit the search
   * patterns.
   * catalogName, schemaName and tableName are JDBC search patterns.
   * tableTypes specifies which table types to search for (TABLE, VIEW, etc).
   */
  public static TResultSet getTables(ImpaladCatalog catalog, String catalogName,
      String schemaName, String tableName, List<String> tableTypes, User user)
          throws ImpalaException{
    TResultSet result = createEmptyResultSet(GET_TABLES_MD);

    // Impala catalog only contains TABLE. Returns an empty set if the search does not
    // include TABLE.
    if (tableTypes != null && !tableTypes.isEmpty()) {
      boolean hasTableType = false;
      for (String tableType: tableTypes) {
        if (tableType.toLowerCase().equals("table")) {
          hasTableType = true;
          break;
        }
      }
      if (!hasTableType) {
        return result;
      }
    }

    // Get the list of schemas, tables that satisfy the search conditions.
    DbsMetadata dbsMetadata = getDbsMetadata(catalog, catalogName,
        schemaName, tableName, null, null, user);

    for (int i = 0; i < dbsMetadata.dbs.size(); ++i) {
      String dbName = dbsMetadata.dbs.get(i);
      for (int j = 0; j < dbsMetadata.tableNames.get(i).size(); ++j) {
        String tabName = dbsMetadata.tableNames.get(i).get(j);
        TResultRow row = new TResultRow();
        row.colVals = Lists.newArrayList();
        row.colVals.add(EMPTY_COL_VAL);
        row.colVals.add(createTColumnValue(dbName));
        row.colVals.add(createTColumnValue(tabName));
        row.colVals.add(TABLE_TYPE_COL_VAL);
        // TODO: Return table comments when it is available in the Impala catalog.
        row.colVals.add(EMPTY_COL_VAL);
        result.rows.add(row);
      }
    }
    LOG.debug("Returning " + result.rows.size() + " tables");
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

  private static TResultRow createFunctionResultRow(String name) {
    TResultRow row = new TResultRow();
    row.colVals = Lists.newArrayList();
    row.colVals.add(NULL_COL_VAL); // FUNCTION_CAT
    row.colVals.add(NULL_COL_VAL); // FUNCTION_SCHEM
    row.colVals.add(createTColumnValue(name)); // FUNCTION_NAME
    row.colVals.add(EMPTY_COL_VAL); // REMARKS
    // FUNCTION_TYPE
    row.colVals.add(createTColumnValue(DatabaseMetaData.functionNoTable));
    row.colVals.add(createTColumnValue(name)); // SPECIFIC_NAME
    return row;
  }

  /**
   * Executes the GetFunctions HiveServer2 operation and returns TResultSet.
   * Returns the list of functions that fit the search patterns.
   * catalogName, schemaName and functionName are JDBC search patterns.
   * @throws ImpalaException
   */
  public static TResultSet getFunctions(ImpaladCatalog catalog,
      String catalogName, String schemaName, String functionName,
      User user) throws ImpalaException {
    TResultSet result = createEmptyResultSet(GET_FUNCTIONS_MD);

    // Impala's built-in functions do not have a catalog name or schema name.
    if (!isEmptyPattern(catalogName) || !isEmptyPattern(schemaName)) {
      return result;
    }

    DbsMetadata dbsMetadata = getDbsMetadata(catalog, catalogName,
        schemaName, null, null, functionName, user);
    for (List<String> fns: dbsMetadata.functions) {
      for (String fn: fns) {
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
          ptype.equals(PrimitiveType.CHAR)) {
        continue;
      }
      ColumnType type = ColumnType.createType(ptype);
      TResultRow row = new TResultRow();
      row.colVals = Lists.newArrayList();
      row.colVals.add(createTColumnValue(ptype.name())); // TYPE_NAME
      row.colVals.add(createTColumnValue(type.getJavaSQLType()));  // DATA_TYPE
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
   * Fills the GET_TYPEINFO_RESULTS with "TABLE".
   */
  private static void createGetTableTypesResults() {
    TResultRow row = new TResultRow();
    row.colVals = Lists.newArrayList();
    row.colVals.add(createTColumnValue("TABLE"));
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
      colVal.setStringVal(val);
    }
    return colVal;
  }

  public static TColumnValue createTColumnValue(Integer val) {
    TColumnValue colVal = new TColumnValue();
    if (val != null) {
      colVal.setIntVal(val.intValue());
    }
    return colVal;
  }

  public static TColumnValue createTColumnValue(Boolean val) {
    TColumnValue colVal = new TColumnValue();
    if (val != null) {
      colVal.setBoolVal(val);
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

  /**
   * Convert a pattern containing JDBC catalog search wildcard into Java regex patterns.
   */
  public static String convertPattern(final String pattern) {
    String wildcardPattern = ".*";
    String workPattern = pattern;
    if (workPattern == null || pattern.isEmpty()) {
      workPattern = "%";
    }
    String result = workPattern
        .replaceAll("([^\\\\])%", "$1" + wildcardPattern)
        .replaceAll("\\\\%", "%")
        .replaceAll("^%", wildcardPattern)
        .replaceAll("([^\\\\])_", "$1.")
        .replaceAll("\\\\_", "_")
        .replaceAll("^_", ".");
    return result;
  }
}
