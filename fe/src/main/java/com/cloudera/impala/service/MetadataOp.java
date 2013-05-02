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
import java.util.List;
import java.util.regex.Pattern;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.cloudera.impala.analysis.OpcodeRegistry;
import com.cloudera.impala.authorization.Privilege;
import com.cloudera.impala.authorization.User;
import com.cloudera.impala.catalog.Catalog;
import com.cloudera.impala.catalog.Column;
import com.cloudera.impala.catalog.Db;
import com.cloudera.impala.catalog.PrimitiveType;
import com.cloudera.impala.common.ImpalaException;
import com.cloudera.impala.thrift.TColumnDesc;
import com.cloudera.impala.thrift.TColumnValue;
import com.cloudera.impala.thrift.TMetadataOpResponse;
import com.cloudera.impala.thrift.TPrimitiveType;
import com.cloudera.impala.thrift.TResultRow;
import com.cloudera.impala.thrift.TResultSetMetadata;
import com.google.common.collect.Lists;

/**
 * Metadata operation. It contains static methods to execute HiveServer2 metadata
 * operations and return the results, result schema and an unique request id in
 * TMetadataOpResponse.
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
    GET_CATALOGS_MD.addToColumnDescs(new TColumnDesc("TABLE_CAT", TPrimitiveType.STRING));

    GET_COLUMNS_MD.addToColumnDescs(
        new TColumnDesc("TABLE_CAT", TPrimitiveType.STRING));
    GET_COLUMNS_MD.addToColumnDescs(
        new TColumnDesc("TABLE_MD", TPrimitiveType.STRING));
    GET_COLUMNS_MD.addToColumnDescs(
        new TColumnDesc("TABLE_NAME", TPrimitiveType.STRING));
    GET_COLUMNS_MD.addToColumnDescs(
        new TColumnDesc("COLUMN_NAME", TPrimitiveType.STRING));
    GET_COLUMNS_MD.addToColumnDescs(
        new TColumnDesc("DATA_TYPE", TPrimitiveType.INT));
    GET_COLUMNS_MD.addToColumnDescs(
        new TColumnDesc("TYPE_NAME", TPrimitiveType.STRING));
    GET_COLUMNS_MD.addToColumnDescs(
        new TColumnDesc("COLUMN_SIZE", TPrimitiveType.INT));
    GET_COLUMNS_MD.addToColumnDescs(
        new TColumnDesc("BUFFER_LENGTH", TPrimitiveType.INT));
    GET_COLUMNS_MD.addToColumnDescs(
        new TColumnDesc("DECIMAL_DIGITS", TPrimitiveType.INT));
    GET_COLUMNS_MD.addToColumnDescs(
        new TColumnDesc("NUM_PREC_RADIX", TPrimitiveType.INT));
    GET_COLUMNS_MD.addToColumnDescs(
        new TColumnDesc("NULLABLE", TPrimitiveType.INT));
    GET_COLUMNS_MD.addToColumnDescs(
        new TColumnDesc("REMARKS", TPrimitiveType.STRING));
    GET_COLUMNS_MD.addToColumnDescs(
        new TColumnDesc("COLUMN_DEF", TPrimitiveType.STRING));
    GET_COLUMNS_MD.addToColumnDescs(
        new TColumnDesc("SQL_DATA_TYPE", TPrimitiveType.INT));
    GET_COLUMNS_MD.addToColumnDescs(
        new TColumnDesc("SQL_DATETIME_SUB", TPrimitiveType.INT));
    GET_COLUMNS_MD.addToColumnDescs(
        new TColumnDesc("CHAR_OCTET_LENGTH", TPrimitiveType.INT));
    GET_COLUMNS_MD.addToColumnDescs(
        new TColumnDesc("ORDINAL_POSITION", TPrimitiveType.INT));
    GET_COLUMNS_MD.addToColumnDescs(
        new TColumnDesc("IS_NULLABLE", TPrimitiveType.STRING));
    GET_COLUMNS_MD.addToColumnDescs(
        new TColumnDesc("SCOPE_CATALOG", TPrimitiveType.STRING));
    GET_COLUMNS_MD.addToColumnDescs(
        new TColumnDesc("SCOPE_SCHEMA", TPrimitiveType.STRING));
    GET_COLUMNS_MD.addToColumnDescs(
        new TColumnDesc("SCOPE_TABLE", TPrimitiveType.STRING));
    GET_COLUMNS_MD.addToColumnDescs(
        new TColumnDesc("SOURCE_DATA_TYPE", TPrimitiveType.SMALLINT));
    GET_COLUMNS_MD.addToColumnDescs(
        new TColumnDesc("IS_AUTO_INCREMENT", TPrimitiveType.STRING));

    GET_SCHEMAS_MD.addToColumnDescs(
        new TColumnDesc("TABLE_SCHEM", TPrimitiveType.STRING));
    GET_SCHEMAS_MD.addToColumnDescs(
        new TColumnDesc("TABLE_CATALOG", TPrimitiveType.STRING));

    GET_TABLES_MD.addToColumnDescs(
        new TColumnDesc("TABLE_CAT", TPrimitiveType.STRING));
    GET_TABLES_MD.addToColumnDescs(
        new TColumnDesc("TABLE_SCHEM", TPrimitiveType.STRING));
    GET_TABLES_MD.addToColumnDescs(
        new TColumnDesc("TABLE_NAME", TPrimitiveType.STRING));
    GET_TABLES_MD.addToColumnDescs(
        new TColumnDesc("TABLE_TYPE", TPrimitiveType.STRING));
    GET_TABLES_MD.addToColumnDescs(
        new TColumnDesc("REMARKS", TPrimitiveType.STRING));

    GET_TYPEINFO_MD.addToColumnDescs(
        new TColumnDesc("TYPE_NAME", TPrimitiveType.STRING));
    GET_TYPEINFO_MD.addToColumnDescs(
        new TColumnDesc("DATA_TYPE", TPrimitiveType.INT));
    GET_TYPEINFO_MD.addToColumnDescs(
        new TColumnDesc("PRECISION", TPrimitiveType.INT));
    GET_TYPEINFO_MD.addToColumnDescs(
        new TColumnDesc("LITERAL_PREFIX", TPrimitiveType.STRING));
    GET_TYPEINFO_MD.addToColumnDescs(
        new TColumnDesc("LITERAL_SUFFIX", TPrimitiveType.STRING));
    GET_TYPEINFO_MD.addToColumnDescs(
        new TColumnDesc("CREATE_PARAMS", TPrimitiveType.STRING));
    GET_TYPEINFO_MD.addToColumnDescs(
        new TColumnDesc("NULLABLE", TPrimitiveType.INT));
    GET_TYPEINFO_MD.addToColumnDescs(
        new TColumnDesc("CASE_SENSITIVE", TPrimitiveType.BOOLEAN));
    GET_TYPEINFO_MD.addToColumnDescs(
        new TColumnDesc("SEARCHABLE", TPrimitiveType.SMALLINT));
    GET_TYPEINFO_MD.addToColumnDescs(
        new TColumnDesc("UNSIGNED_ATTRIBUTE", TPrimitiveType.BOOLEAN));
    GET_TYPEINFO_MD.addToColumnDescs(
        new TColumnDesc("FIXED_PREC_SCALE", TPrimitiveType.BOOLEAN));
    GET_TYPEINFO_MD.addToColumnDescs(
        new TColumnDesc("AUTO_INCREMENT", TPrimitiveType.BOOLEAN));
    GET_TYPEINFO_MD.addToColumnDescs(
        new TColumnDesc("LOCAL_TYPE_NAME", TPrimitiveType.STRING));
    GET_TYPEINFO_MD.addToColumnDescs(
        new TColumnDesc("MINIMUM_SCALE", TPrimitiveType.SMALLINT));
    GET_TYPEINFO_MD.addToColumnDescs(
        new TColumnDesc("MAXIMUM_SCALE", TPrimitiveType.SMALLINT));
    GET_TYPEINFO_MD.addToColumnDescs(
        new TColumnDesc("SQL_DATA_TYPE", TPrimitiveType.INT));
    GET_TYPEINFO_MD.addToColumnDescs(
        new TColumnDesc("SQL_DATETIME_SUB", TPrimitiveType.INT));
    GET_TYPEINFO_MD.addToColumnDescs(
        new TColumnDesc("NUM_PREC_RADIX", TPrimitiveType.INT));

    GET_TABLE_TYPES_MD.addToColumnDescs(
        new TColumnDesc("TABLE_TYPE", TPrimitiveType.STRING));

    GET_FUNCTIONS_MD.addToColumnDescs(
        new TColumnDesc("FUNCTION_CAT", TPrimitiveType.STRING));
    GET_FUNCTIONS_MD.addToColumnDescs(
        new TColumnDesc("FUNCTION_SCHEM", TPrimitiveType.STRING));
    GET_FUNCTIONS_MD.addToColumnDescs(
        new TColumnDesc("FUNCTION_NAME", TPrimitiveType.STRING));
    GET_FUNCTIONS_MD.addToColumnDescs(
        new TColumnDesc("REMARKS", TPrimitiveType.STRING));
    GET_FUNCTIONS_MD.addToColumnDescs(
        new TColumnDesc("FUNCTION_TYPE", TPrimitiveType.INT));
    GET_FUNCTIONS_MD.addToColumnDescs(
        new TColumnDesc("SPECIFIC_NAME", TPrimitiveType.STRING));
  }

  /**
   * Contains lists of databases, lists of table belonging to the dbs, and list of columns
   *  belonging to the tables.
   */
  private static class DbsTablesColumns {
     // the list of database
    public List<String> dbs = Lists.newArrayList();

    // tableNames[i] are the tables within dbs[i]
    public List<List<String>> tableNames = Lists.newArrayList();

    // columns[i][j] are the columns of tableNames[j] in dbs[i]
    public List<List<List<Column>>> columns = Lists.newArrayList();
  }

  /**
   * Returns the list of schemas, tables and columns that satisfy the search pattern.
   * catalogName, schemaName, tableName and columnName are JDBC search patterns.
   *
   * The return value DbsTablesColumns.dbs contains the list of databases that satisfy
   * the "schemaName" search pattern.
   * DbsTablesColumns.tableNames[i] contains the list of tables inside dbs[i] that satisfy
   * the "tableName" search pattern.
   * DbsTablesColumns.columns[i][j] contains the list of columns of table[j] in dbs[i]
   * that satisfy the search condition "columnName".
   *
   * If tableName is null, then DbsTablesColumns.tableNames and DbsTablesColumns.columns
   * will not be populated.
   * If columns is null, then DbsTablesColumns.columns will not be populated.
   */
  private static DbsTablesColumns getDbsTablesColumns(Catalog catalog, String catalogName,
      String schemaName, String tableName, String columnName, User user)
      throws ImpalaException {
    DbsTablesColumns result = new DbsTablesColumns();

    // Hive does not have a catalog concept. Returns nothing if the request specifies an
    // non-empty catalog pattern.
    if (!isEmptyPattern(catalogName)) {
      return result;
    }

    // Creates the schema, table and column search patterns
    String convertedSchemaPattern = convertPattern(schemaName);
    String convertedTablePattern = convertPattern(tableName);
    String convertedColumnPattern = convertPattern(columnName);
    Pattern schemaPattern = Pattern.compile(convertedSchemaPattern);
    Pattern tablePattern = Pattern.compile(convertedTablePattern);
    Pattern columnPattern = Pattern.compile(convertedColumnPattern);

    for (String dbName: catalog.getAllDbNames(user)) {
      if (!schemaPattern.matcher(dbName).matches()) {
        continue;
      }

      List<String> tableList = Lists.newArrayList();
      List<List<Column>> tablesColumnsList = Lists.newArrayList();
      if (tableName != null) {
        Db db = catalog.getDb(dbName, user, Privilege.ANY);
        for (String tabName: catalog.getTableNames(db.getName(), "*", user)) {
          if (!tablePattern.matcher(tabName).matches()) {
            continue;
          }
          tableList.add(tabName);
          List<Column> columns = Lists.newArrayList();

          if (columnName != null) {
            for (Column column: db.getTable(tabName).getColumns()) {
              String colName = column.getName();
              if (!columnPattern.matcher(colName).matches()) {
                continue;
              }
              columns.add(column);
            }
          }
          tablesColumnsList.add(columns);
        }
      }

      result.dbs.add(dbName);
      result.tableNames.add(tableList);
      result.columns.add(tablesColumnsList);
    }
    return result;
  }

  /**
   * Executes the GetCatalogs HiveServer2 operation and returns TMetadataOpResponse.
   * Hive does not have a catalog concept. It always returns an empty result set.
   */
  public static TMetadataOpResponse getCatalogs() {
    return createEmptyMetadataOpResponse(GET_CATALOGS_MD);
  }

  /**
   * Executes the GetColumns HiveServer2 operation and returns TMetadataOpResponse.
   * It queries the Impala catalog to return the list of table columns that fit the
   * search patterns.
   * catalogName, schemaName, tableName and columnName are JDBC search patterns.
   */
  public static TMetadataOpResponse getColumns(Catalog catalog, String catalogName,
      String schemaName, String tableName, String columnName, User user)
      throws ImpalaException {
    TMetadataOpResponse result = createEmptyMetadataOpResponse(GET_COLUMNS_MD);

    // Get the list of schemas, tables, and columns that satisfy the search conditions.
    DbsTablesColumns dbsTablesColumns = getDbsTablesColumns(catalog, catalogName,
        schemaName, tableName, columnName, user);

    for (int i = 0; i < dbsTablesColumns.dbs.size(); ++i) {
      String dbName = dbsTablesColumns.dbs.get(i);
      for (int j = 0; j < dbsTablesColumns.tableNames.get(i).size(); ++j) {
        String tabName = dbsTablesColumns.tableNames.get(i).get(j);
        for (int k = 0; k < dbsTablesColumns.columns.get(i).get(j).size(); ++k) {
          Column column = dbsTablesColumns.columns.get(i).get(j).get(k);
          PrimitiveType colType = column.getType();
          TResultRow row = new TResultRow();
          row.colVals = Lists.newArrayList();
          row.colVals.add(NULL_COL_VAL); // TABLE_CAT
          row.colVals.add(createTColumnValue(dbName)); // TABLE_SCHEM
          row.colVals.add(createTColumnValue(tabName)); // TABLE_NAME
          row.colVals.add(createTColumnValue(column.getName())); // COLUMN_NAME
          row.colVals.add(createTColumnValue(colType.getJavaSQLType())); // DATA_TYPE
          row.colVals.add(createTColumnValue(colType.name())); // TYPE_NAME
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
          result.results.add(row);
        }
      }
    }
    LOG.debug("Returning " + result.results.size() + " table columns");
    return result;
  }

  /**
   * Executes the GetSchemas HiveServer2 operation and returns TMetadataOpResponse.
   * It queries the Impala catalog to return the list of schemas that fit the search
   * pattern.
   * catalogName and schemaName are JDBC search patterns.
   */
  public static TMetadataOpResponse getSchemas(Catalog catalog, String catalogName,
      String schemaName, User user) throws ImpalaException {
    TMetadataOpResponse result = createEmptyMetadataOpResponse(GET_SCHEMAS_MD);

    // Get the list of schemas that satisfy the search condition.
    DbsTablesColumns dbsTablesColumns = getDbsTablesColumns(catalog, catalogName,
        schemaName, null, null, user);

    for (int i = 0; i < dbsTablesColumns.dbs.size(); ++i) {
      String dbName = dbsTablesColumns.dbs.get(i);
      TResultRow row = new TResultRow();
      row.colVals = Lists.newArrayList();
      row.colVals.add(createTColumnValue(dbName)); // TABLE_SCHEM
      row.colVals.add(EMPTY_COL_VAL); // default Hive catalog is an empty string.
      result.results.add(row);
    }

    LOG.debug("Returning " + result.results.size() + " schemas");
    return result;
  }

  /**
   * Executes the GetTables HiveServer2 operation and returns TMetadataOpResponse.
   * It queries the Impala catalog to return the list of tables that fit the search
   * patterns.
   * catalogName, schemaName and tableName are JDBC search patterns.
   * tableTypes specifies which table types to search for (TABLE, VIEW, etc).
   */
  public static TMetadataOpResponse getTables(Catalog catalog, String catalogName,
      String schemaName, String tableName, List<String> tableTypes, User user)
          throws ImpalaException{
    TMetadataOpResponse result = createEmptyMetadataOpResponse(GET_TABLES_MD);

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
    DbsTablesColumns dbsTablesColumns = getDbsTablesColumns(catalog, catalogName,
        schemaName, tableName, null, user);

    for (int i = 0; i < dbsTablesColumns.dbs.size(); ++i) {
      String dbName = dbsTablesColumns.dbs.get(i);
      for (int j = 0; j < dbsTablesColumns.tableNames.get(i).size(); ++j) {
        String tabName = dbsTablesColumns.tableNames.get(i).get(j);
        TResultRow row = new TResultRow();
        row.colVals = Lists.newArrayList();
        row.colVals.add(EMPTY_COL_VAL);
        row.colVals.add(createTColumnValue(dbName));
        row.colVals.add(createTColumnValue(tabName));
        row.colVals.add(TABLE_TYPE_COL_VAL);
        // TODO: Return table comments when it is available in the Impala catalog.
        row.colVals.add(EMPTY_COL_VAL);
        result.results.add(row);
      }
    }
    LOG.debug("Returning " + result.results.size() + " tables");
    return result;
  }

  /**
   * Executes the GetTables HiveServer2 operation and returns Impala supported types.
   */
  public static TMetadataOpResponse getTypeInfo() {
    TMetadataOpResponse result = createEmptyMetadataOpResponse(GET_TYPEINFO_MD);
    result.results = GET_TYPEINFO_RESULTS;
    return result;
  }

  /**
   * Executes the GetTableTypes HiveServer2 operation.
   */
  public static TMetadataOpResponse getTableTypes() {
    TMetadataOpResponse result = createEmptyMetadataOpResponse(GET_TABLE_TYPES_MD);
    result.results = GET_TABLE_TYPES_RESULTS;
    return result;
  }

  /**
   * Executes the GetFunctions HiveServer2 operation and returns TMetadataOpResponse.
   * Returns the list of functions that fit the search patterns.
   * catalogName, schemaName and functionName are JDBC search patterns.
   */
  public static TMetadataOpResponse getFunctions(String catalogName, String schemaName,
      String functionName) {
    TMetadataOpResponse result = createEmptyMetadataOpResponse(GET_FUNCTIONS_MD);

    // Impala's built-in functions do not have a catalog name or schema name.
    if (!isEmptyPattern(catalogName) || !isEmptyPattern(schemaName)) {
      return result;
    }

    Pattern functionPattern = Pattern.compile(convertPattern(functionName));

    for (String builtinFn: OpcodeRegistry.instance().getFunctionNames()) {
      if (!functionPattern.matcher(builtinFn).matches()) {
        continue;
      }

      TResultRow row = new TResultRow();
      row.colVals = Lists.newArrayList();
      row.colVals.add(NULL_COL_VAL); // FUNCTION_CAT
      row.colVals.add(NULL_COL_VAL); // FUNCTION_SCHEM
      row.colVals.add(createTColumnValue(builtinFn)); // FUNCTION_NAME
      row.colVals.add(EMPTY_COL_VAL); // REMARKS
      // FUNCTION_TYPE
      row.colVals.add(createTColumnValue(DatabaseMetaData.functionNoTable));
      row.colVals.add(createTColumnValue(builtinFn)); // SPECIFIC_NAME
      result.results.add(row);
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
          ptype.equals(PrimitiveType.DATETIME)) {
        continue;
      }
      TResultRow row = new TResultRow();
      row.colVals = Lists.newArrayList();
      row.colVals.add(createTColumnValue(ptype.name())); // TYPE_NAME
      row.colVals.add(createTColumnValue(ptype.getJavaSQLType()));  // DATA_TYPE
      row.colVals.add(createTColumnValue(ptype.getPrecision()));  // PRECISION
      row.colVals.add(NULL_COL_VAL); // LITERAL_PREFIX
      row.colVals.add(NULL_COL_VAL); // LITERAL_SUFFIX
      row.colVals.add(NULL_COL_VAL); // CREATE_PARAMS
      row.colVals.add(createTColumnValue(DatabaseMetaData.typeNullable));  // NULLABLE
      row.colVals.add(createTColumnValue(ptype.isStringType())); // CASE_SENSITIVE
      row.colVals.add(createTColumnValue(DatabaseMetaData.typeSearchable));  // SEARCHABLE
      row.colVals.add(createTColumnValue(!ptype.isNumericType())); // UNSIGNED_ATTRIBUTE
      row.colVals.add(createTColumnValue(false));  // FIXED_PREC_SCALE
      row.colVals.add(createTColumnValue(false));  // AUTO_INCREMENT
      row.colVals.add(NULL_COL_VAL); // LOCAL_TYPE_NAME
      row.colVals.add(createTColumnValue(0));  // MINIMUM_SCALE
      row.colVals.add(createTColumnValue(0));  // MAXIMUM_SCALE
      row.colVals.add(NULL_COL_VAL); // SQL_DATA_TYPE
      row.colVals.add(NULL_COL_VAL); // SQL_DATETIME_SUB
      row.colVals.add(createTColumnValue(ptype.getNumPrecRadix()));  // NUM_PREC_RADIX
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
   * Returns an TMetadataOpResponse with the specified schema. The
   * result set will be empty.
   */
  private static TMetadataOpResponse createEmptyMetadataOpResponse(
      TResultSetMetadata metadata) {
    TMetadataOpResponse result = new TMetadataOpResponse();
    result.results = Lists.newArrayList();
    result.result_set_metadata = metadata;
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
