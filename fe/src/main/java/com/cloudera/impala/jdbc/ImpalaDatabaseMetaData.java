// Copyright (c) 2011 Cloudera, Inc. All rights reserved.

package com.cloudera.impala.jdbc;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.RowIdLifetime;
import java.sql.SQLException;

/**
 * Minimal implementation required to run select queries with sqlline.
 * The implemented methods must return non-null values.
 * Most methods are not implemented because they are not required to make sqlline work.
 * Unimplemented methods throw an UnsupportedOperationException that includes the method name of the
 * called method for easier debugging.
 *
 * This class provides the connection URL and information about the database,
 * e.g. version, JDBC driver version, etc.
 */
public class ImpalaDatabaseMetaData implements DatabaseMetaData {

  // Connection url passed in by the user.
  private final String url;

  public ImpalaDatabaseMetaData(String url) {
    this.url = url;
  }

  @Override
  public String getURL() throws SQLException {
    return url;
  }

  @Override
  public boolean isReadOnly() throws SQLException {
    return true;
  }

  @Override
  public String getDatabaseProductName() throws SQLException {
    return "Cloudera Impala";
  }

  @Override
  public String getDatabaseProductVersion() throws SQLException {
    return "0.0.1";
  }

  @Override
  public String getDriverName() throws SQLException {
    return "Impala JDBC Driver";
  }

  @Override
  public String getDriverVersion() throws SQLException {
    return "0.0.1";
  }

  @Override
  public int getDriverMajorVersion() {
    return 0;
  }

  @Override
  public int getDriverMinorVersion() {
    return 0;
  }

  // As far as I know, sqlline uses the following methods for performing tab completion.
  // sqlline will use default values if we throw exceptions.

  @Override
  public String getNumericFunctions() throws SQLException {
    throw UnsupportedOpHelper.newUnimplementedMethodException();
  }

  @Override
  public String getStringFunctions() throws SQLException {
    throw UnsupportedOpHelper.newUnimplementedMethodException();
  }

  @Override
  public String getSystemFunctions() throws SQLException {
    throw UnsupportedOpHelper.newUnimplementedMethodException();
  }

  @Override
  public String getTimeDateFunctions() throws SQLException {
    throw UnsupportedOpHelper.newUnimplementedMethodException();
  }

  @Override
  public String getSQLKeywords() throws SQLException {
    throw UnsupportedOpHelper.newUnimplementedMethodException();
  }

  @Override
  public String getExtraNameCharacters() throws SQLException {
    return null;
  }

  // Non-essential and unimplemented methods start here.

  @Override
  public String getUserName() throws SQLException {
    throw UnsupportedOpHelper.newUnimplementedMethodException();
  }

  @Override
  public <T> T unwrap(Class<T> iface) throws SQLException {
    throw UnsupportedOpHelper.newUnimplementedMethodException();
  }

  @Override
  public boolean isWrapperFor(Class<?> iface) throws SQLException {
    throw UnsupportedOpHelper.newUnimplementedMethodException();
  }

  @Override
  public boolean allProceduresAreCallable() throws SQLException {
    throw UnsupportedOpHelper.newUnimplementedMethodException();
  }

  @Override
  public boolean allTablesAreSelectable() throws SQLException {
    throw UnsupportedOpHelper.newUnimplementedMethodException();
  }

  @Override
  public boolean nullsAreSortedHigh() throws SQLException {
    throw UnsupportedOpHelper.newUnimplementedMethodException();
  }

  @Override
  public boolean nullsAreSortedLow() throws SQLException {
    throw UnsupportedOpHelper.newUnimplementedMethodException();
  }

  @Override
  public boolean nullsAreSortedAtStart() throws SQLException {
    throw UnsupportedOpHelper.newUnimplementedMethodException();
  }

  @Override
  public boolean nullsAreSortedAtEnd() throws SQLException {
    throw UnsupportedOpHelper.newUnimplementedMethodException();
  }

  @Override
  public boolean usesLocalFiles() throws SQLException {
    throw UnsupportedOpHelper.newUnimplementedMethodException();
  }

  @Override
  public boolean usesLocalFilePerTable() throws SQLException {
    throw UnsupportedOpHelper.newUnimplementedMethodException();
  }

  @Override
  public boolean supportsMixedCaseIdentifiers() throws SQLException {
    throw UnsupportedOpHelper.newUnimplementedMethodException();
  }

  @Override
  public boolean storesUpperCaseIdentifiers() throws SQLException {
    throw UnsupportedOpHelper.newUnimplementedMethodException();
  }

  @Override
  public boolean storesLowerCaseIdentifiers() throws SQLException {
    throw UnsupportedOpHelper.newUnimplementedMethodException();
  }

  @Override
  public boolean storesMixedCaseIdentifiers() throws SQLException {
    throw UnsupportedOpHelper.newUnimplementedMethodException();
  }

  @Override
  public boolean supportsMixedCaseQuotedIdentifiers() throws SQLException {
    throw UnsupportedOpHelper.newUnimplementedMethodException();
  }

  @Override
  public boolean storesUpperCaseQuotedIdentifiers() throws SQLException {
    throw UnsupportedOpHelper.newUnimplementedMethodException();
  }

  @Override
  public boolean storesLowerCaseQuotedIdentifiers() throws SQLException {
    throw UnsupportedOpHelper.newUnimplementedMethodException();
  }

  @Override
  public boolean storesMixedCaseQuotedIdentifiers() throws SQLException {
    throw UnsupportedOpHelper.newUnimplementedMethodException();
  }

  @Override
  public String getIdentifierQuoteString() throws SQLException {
    throw UnsupportedOpHelper.newUnimplementedMethodException();
  }

  @Override
  public String getSearchStringEscape() throws SQLException {
    throw UnsupportedOpHelper.newUnimplementedMethodException();
  }

  @Override
  public boolean supportsAlterTableWithAddColumn() throws SQLException {
    throw UnsupportedOpHelper.newUnimplementedMethodException();
  }

  @Override
  public boolean supportsAlterTableWithDropColumn() throws SQLException {
    throw UnsupportedOpHelper.newUnimplementedMethodException();
  }

  @Override
  public boolean supportsColumnAliasing() throws SQLException {
    throw UnsupportedOpHelper.newUnimplementedMethodException();
  }

  @Override
  public boolean nullPlusNonNullIsNull() throws SQLException {
    throw UnsupportedOpHelper.newUnimplementedMethodException();
  }

  @Override
  public boolean supportsConvert() throws SQLException {
    throw UnsupportedOpHelper.newUnimplementedMethodException();
  }

  @Override
  public boolean supportsConvert(int fromType, int toType) throws SQLException {
    throw UnsupportedOpHelper.newUnimplementedMethodException();
  }

  @Override
  public boolean supportsTableCorrelationNames() throws SQLException {
    throw UnsupportedOpHelper.newUnimplementedMethodException();
  }

  @Override
  public boolean supportsDifferentTableCorrelationNames() throws SQLException {
    throw UnsupportedOpHelper.newUnimplementedMethodException();
  }

  @Override
  public boolean supportsExpressionsInOrderBy() throws SQLException {
    throw UnsupportedOpHelper.newUnimplementedMethodException();
  }

  @Override
  public boolean supportsOrderByUnrelated() throws SQLException {
    throw UnsupportedOpHelper.newUnimplementedMethodException();
  }

  @Override
  public boolean supportsGroupBy() throws SQLException {
    throw UnsupportedOpHelper.newUnimplementedMethodException();
  }

  @Override
  public boolean supportsGroupByUnrelated() throws SQLException {
    throw UnsupportedOpHelper.newUnimplementedMethodException();
  }

  @Override
  public boolean supportsGroupByBeyondSelect() throws SQLException {
    throw UnsupportedOpHelper.newUnimplementedMethodException();
  }

  @Override
  public boolean supportsLikeEscapeClause() throws SQLException {
    throw UnsupportedOpHelper.newUnimplementedMethodException();
  }

  @Override
  public boolean supportsMultipleResultSets() throws SQLException {
    throw UnsupportedOpHelper.newUnimplementedMethodException();
  }

  @Override
  public boolean supportsMultipleTransactions() throws SQLException {
    throw UnsupportedOpHelper.newUnimplementedMethodException();
  }

  @Override
  public boolean supportsNonNullableColumns() throws SQLException {
    throw UnsupportedOpHelper.newUnimplementedMethodException();
  }

  @Override
  public boolean supportsMinimumSQLGrammar() throws SQLException {
    throw UnsupportedOpHelper.newUnimplementedMethodException();
  }

  @Override
  public boolean supportsCoreSQLGrammar() throws SQLException {
    throw UnsupportedOpHelper.newUnimplementedMethodException();
  }

  @Override
  public boolean supportsExtendedSQLGrammar() throws SQLException {
    throw UnsupportedOpHelper.newUnimplementedMethodException();
  }

  @Override
  public boolean supportsANSI92EntryLevelSQL() throws SQLException {
    throw UnsupportedOpHelper.newUnimplementedMethodException();
  }

  @Override
  public boolean supportsANSI92IntermediateSQL() throws SQLException {
    throw UnsupportedOpHelper.newUnimplementedMethodException();
  }

  @Override
  public boolean supportsANSI92FullSQL() throws SQLException {
    throw UnsupportedOpHelper.newUnimplementedMethodException();
  }

  @Override
  public boolean supportsIntegrityEnhancementFacility() throws SQLException {
    throw UnsupportedOpHelper.newUnimplementedMethodException();
  }

  @Override
  public boolean supportsOuterJoins() throws SQLException {
    throw UnsupportedOpHelper.newUnimplementedMethodException();
  }

  @Override
  public boolean supportsFullOuterJoins() throws SQLException {
    throw UnsupportedOpHelper.newUnimplementedMethodException();
  }

  @Override
  public boolean supportsLimitedOuterJoins() throws SQLException {
    throw UnsupportedOpHelper.newUnimplementedMethodException();
  }

  @Override
  public String getSchemaTerm() throws SQLException {
    throw UnsupportedOpHelper.newUnimplementedMethodException();
  }

  @Override
  public String getProcedureTerm() throws SQLException {
    throw UnsupportedOpHelper.newUnimplementedMethodException();
  }

  @Override
  public String getCatalogTerm() throws SQLException {
    throw UnsupportedOpHelper.newUnimplementedMethodException();
  }

  @Override
  public boolean isCatalogAtStart() throws SQLException {
    throw UnsupportedOpHelper.newUnimplementedMethodException();
  }

  @Override
  public String getCatalogSeparator() throws SQLException {
    throw UnsupportedOpHelper.newUnimplementedMethodException();
  }

  @Override
  public boolean supportsSchemasInDataManipulation() throws SQLException {
    throw UnsupportedOpHelper.newUnimplementedMethodException();
  }

  @Override
  public boolean supportsSchemasInProcedureCalls() throws SQLException {
    throw UnsupportedOpHelper.newUnimplementedMethodException();
  }

  @Override
  public boolean supportsSchemasInTableDefinitions() throws SQLException {
    throw UnsupportedOpHelper.newUnimplementedMethodException();
  }

  @Override
  public boolean supportsSchemasInIndexDefinitions() throws SQLException {
    throw UnsupportedOpHelper.newUnimplementedMethodException();
  }

  @Override
  public boolean supportsSchemasInPrivilegeDefinitions() throws SQLException {
    throw UnsupportedOpHelper.newUnimplementedMethodException();
  }

  @Override
  public boolean supportsCatalogsInDataManipulation() throws SQLException {
    throw UnsupportedOpHelper.newUnimplementedMethodException();
  }

  @Override
  public boolean supportsCatalogsInProcedureCalls() throws SQLException {
    throw UnsupportedOpHelper.newUnimplementedMethodException();
  }

  @Override
  public boolean supportsCatalogsInTableDefinitions() throws SQLException {
    throw UnsupportedOpHelper.newUnimplementedMethodException();
  }

  @Override
  public boolean supportsCatalogsInIndexDefinitions() throws SQLException {
    throw UnsupportedOpHelper.newUnimplementedMethodException();
  }

  @Override
  public boolean supportsCatalogsInPrivilegeDefinitions() throws SQLException {
    throw UnsupportedOpHelper.newUnimplementedMethodException();
  }

  @Override
  public boolean supportsPositionedDelete() throws SQLException {
    throw UnsupportedOpHelper.newUnimplementedMethodException();
  }

  @Override
  public boolean supportsPositionedUpdate() throws SQLException {
    throw UnsupportedOpHelper.newUnimplementedMethodException();
  }

  @Override
  public boolean supportsSelectForUpdate() throws SQLException {
    throw UnsupportedOpHelper.newUnimplementedMethodException();
  }

  @Override
  public boolean supportsStoredProcedures() throws SQLException {
    throw UnsupportedOpHelper.newUnimplementedMethodException();
  }

  @Override
  public boolean supportsSubqueriesInComparisons() throws SQLException {
    throw UnsupportedOpHelper.newUnimplementedMethodException();
  }

  @Override
  public boolean supportsSubqueriesInExists() throws SQLException {
    throw UnsupportedOpHelper.newUnimplementedMethodException();
  }

  @Override
  public boolean supportsSubqueriesInIns() throws SQLException {
    throw UnsupportedOpHelper.newUnimplementedMethodException();
  }

  @Override
  public boolean supportsSubqueriesInQuantifieds() throws SQLException {
    throw UnsupportedOpHelper.newUnimplementedMethodException();
  }

  @Override
  public boolean supportsCorrelatedSubqueries() throws SQLException {
    throw UnsupportedOpHelper.newUnimplementedMethodException();
  }

  @Override
  public boolean supportsUnion() throws SQLException {
    throw UnsupportedOpHelper.newUnimplementedMethodException();
  }

  @Override
  public boolean supportsUnionAll() throws SQLException {
    throw UnsupportedOpHelper.newUnimplementedMethodException();
  }

  @Override
  public boolean supportsOpenCursorsAcrossCommit() throws SQLException {
    throw UnsupportedOpHelper.newUnimplementedMethodException();
  }

  @Override
  public boolean supportsOpenCursorsAcrossRollback() throws SQLException {
    throw UnsupportedOpHelper.newUnimplementedMethodException();
  }

  @Override
  public boolean supportsOpenStatementsAcrossCommit() throws SQLException {
    throw UnsupportedOpHelper.newUnimplementedMethodException();
  }

  @Override
  public boolean supportsOpenStatementsAcrossRollback() throws SQLException {
    throw UnsupportedOpHelper.newUnimplementedMethodException();
  }

  @Override
  public int getMaxBinaryLiteralLength() throws SQLException {
    throw UnsupportedOpHelper.newUnimplementedMethodException();
  }

  @Override
  public int getMaxCharLiteralLength() throws SQLException {
    throw UnsupportedOpHelper.newUnimplementedMethodException();
  }

  @Override
  public int getMaxColumnNameLength() throws SQLException {
    throw UnsupportedOpHelper.newUnimplementedMethodException();
  }

  @Override
  public int getMaxColumnsInGroupBy() throws SQLException {
    throw UnsupportedOpHelper.newUnimplementedMethodException();
  }

  @Override
  public int getMaxColumnsInIndex() throws SQLException {
    throw UnsupportedOpHelper.newUnimplementedMethodException();
  }

  @Override
  public int getMaxColumnsInOrderBy() throws SQLException {
    throw UnsupportedOpHelper.newUnimplementedMethodException();
  }

  @Override
  public int getMaxColumnsInSelect() throws SQLException {
    throw UnsupportedOpHelper.newUnimplementedMethodException();
  }

  @Override
  public int getMaxColumnsInTable() throws SQLException {
    throw UnsupportedOpHelper.newUnimplementedMethodException();
  }

  @Override
  public int getMaxConnections() throws SQLException {
    throw UnsupportedOpHelper.newUnimplementedMethodException();
  }

  @Override
  public int getMaxCursorNameLength() throws SQLException {
    throw UnsupportedOpHelper.newUnimplementedMethodException();
  }

  @Override
  public int getMaxIndexLength() throws SQLException {
    throw UnsupportedOpHelper.newUnimplementedMethodException();
  }

  @Override
  public int getMaxSchemaNameLength() throws SQLException {
    throw UnsupportedOpHelper.newUnimplementedMethodException();
  }

  @Override
  public int getMaxProcedureNameLength() throws SQLException {
    throw UnsupportedOpHelper.newUnimplementedMethodException();
  }

  @Override
  public int getMaxCatalogNameLength() throws SQLException {
    throw UnsupportedOpHelper.newUnimplementedMethodException();
  }

  @Override
  public int getMaxRowSize() throws SQLException {
    throw UnsupportedOpHelper.newUnimplementedMethodException();
  }

  @Override
  public boolean doesMaxRowSizeIncludeBlobs() throws SQLException {
    throw UnsupportedOpHelper.newUnimplementedMethodException();
  }

  @Override
  public int getMaxStatementLength() throws SQLException {
    throw UnsupportedOpHelper.newUnimplementedMethodException();
  }

  @Override
  public int getMaxStatements() throws SQLException {
    throw UnsupportedOpHelper.newUnimplementedMethodException();
  }

  @Override
  public int getMaxTableNameLength() throws SQLException {
    throw UnsupportedOpHelper.newUnimplementedMethodException();
  }

  @Override
  public int getMaxTablesInSelect() throws SQLException {
    throw UnsupportedOpHelper.newUnimplementedMethodException();
  }

  @Override
  public int getMaxUserNameLength() throws SQLException {
    throw UnsupportedOpHelper.newUnimplementedMethodException();
  }

  @Override
  public int getDefaultTransactionIsolation() throws SQLException {
    throw UnsupportedOpHelper.newUnimplementedMethodException();
  }

  @Override
  public boolean supportsTransactions() throws SQLException {
    throw UnsupportedOpHelper.newUnimplementedMethodException();
  }

  @Override
  public boolean supportsTransactionIsolationLevel(int level) throws SQLException {
    throw UnsupportedOpHelper.newUnimplementedMethodException();
  }

  @Override
  public boolean supportsDataDefinitionAndDataManipulationTransactions() throws SQLException {
    throw UnsupportedOpHelper.newUnimplementedMethodException();
  }

  @Override
  public boolean supportsDataManipulationTransactionsOnly() throws SQLException {
    throw UnsupportedOpHelper.newUnimplementedMethodException();
  }

  @Override
  public boolean dataDefinitionCausesTransactionCommit() throws SQLException {
    throw UnsupportedOpHelper.newUnimplementedMethodException();
  }

  @Override
  public boolean dataDefinitionIgnoredInTransactions() throws SQLException {
    throw UnsupportedOpHelper.newUnimplementedMethodException();
  }

  @Override
  public ResultSet getProcedures(String catalog, String schemaPattern, String procedureNamePattern)
      throws SQLException {
    throw UnsupportedOpHelper.newUnimplementedMethodException();
  }

  @Override
  public ResultSet getProcedureColumns(String catalog, String schemaPattern,
      String procedureNamePattern, String columnNamePattern) throws SQLException {
    throw UnsupportedOpHelper.newUnimplementedMethodException();
  }

  @Override
  public ResultSet getTables(String catalog, String schemaPattern, String tableNamePattern,
      String[] types) throws SQLException {
    throw UnsupportedOpHelper.newUnimplementedMethodException();
  }

  @Override
  public ResultSet getSchemas() throws SQLException {
    throw UnsupportedOpHelper.newUnimplementedMethodException();
  }

  @Override
  public ResultSet getCatalogs() throws SQLException {
    throw UnsupportedOpHelper.newUnimplementedMethodException();
  }

  @Override
  public ResultSet getTableTypes() throws SQLException {
    throw UnsupportedOpHelper.newUnimplementedMethodException();
  }

  @Override
  public ResultSet getColumns(String catalog, String schemaPattern, String tableNamePattern,
      String columnNamePattern) throws SQLException {
    throw UnsupportedOpHelper.newUnimplementedMethodException();
  }

  @Override
  public ResultSet getColumnPrivileges(String catalog, String schema, String table,
      String columnNamePattern) throws SQLException {
    throw UnsupportedOpHelper.newUnimplementedMethodException();
  }

  @Override
  public ResultSet getTablePrivileges(String catalog, String schemaPattern, String tableNamePattern)
      throws SQLException {
    throw UnsupportedOpHelper.newUnimplementedMethodException();
  }

  @Override
  public ResultSet getBestRowIdentifier(String catalog, String schema, String table, int scope,
      boolean nullable) throws SQLException {
    throw UnsupportedOpHelper.newUnimplementedMethodException();
  }

  @Override
  public ResultSet getVersionColumns(String catalog, String schema, String table)
      throws SQLException {
    throw UnsupportedOpHelper.newUnimplementedMethodException();
  }

  @Override
  public ResultSet getPrimaryKeys(String catalog, String schema, String table) throws SQLException {
    throw UnsupportedOpHelper.newUnimplementedMethodException();
  }

  @Override
  public ResultSet getImportedKeys(String catalog, String schema, String table) throws SQLException {
    throw UnsupportedOpHelper.newUnimplementedMethodException();
  }

  @Override
  public ResultSet getExportedKeys(String catalog, String schema, String table) throws SQLException {
    throw UnsupportedOpHelper.newUnimplementedMethodException();
  }

  @Override
  public ResultSet getCrossReference(String parentCatalog, String parentSchema, String parentTable,
      String foreignCatalog, String foreignSchema, String foreignTable) throws SQLException {
    throw UnsupportedOpHelper.newUnimplementedMethodException();
  }

  @Override
  public ResultSet getTypeInfo() throws SQLException {
    throw UnsupportedOpHelper.newUnimplementedMethodException();
  }

  @Override
  public ResultSet getIndexInfo(String catalog, String schema, String table, boolean unique,
      boolean approximate) throws SQLException {
    throw UnsupportedOpHelper.newUnimplementedMethodException();
  }

  @Override
  public boolean supportsResultSetType(int type) throws SQLException {
    throw UnsupportedOpHelper.newUnimplementedMethodException();
  }

  @Override
  public boolean supportsResultSetConcurrency(int type, int concurrency) throws SQLException {
    throw UnsupportedOpHelper.newUnimplementedMethodException();
  }

  @Override
  public boolean ownUpdatesAreVisible(int type) throws SQLException {
    throw UnsupportedOpHelper.newUnimplementedMethodException();
  }

  @Override
  public boolean ownDeletesAreVisible(int type) throws SQLException {
    throw UnsupportedOpHelper.newUnimplementedMethodException();
  }

  @Override
  public boolean ownInsertsAreVisible(int type) throws SQLException {
    throw UnsupportedOpHelper.newUnimplementedMethodException();
  }

  @Override
  public boolean othersUpdatesAreVisible(int type) throws SQLException {
    throw UnsupportedOpHelper.newUnimplementedMethodException();
  }

  @Override
  public boolean othersDeletesAreVisible(int type) throws SQLException {
    throw UnsupportedOpHelper.newUnimplementedMethodException();
  }

  @Override
  public boolean othersInsertsAreVisible(int type) throws SQLException {
    throw UnsupportedOpHelper.newUnimplementedMethodException();
  }

  @Override
  public boolean updatesAreDetected(int type) throws SQLException {
    throw UnsupportedOpHelper.newUnimplementedMethodException();
  }

  @Override
  public boolean deletesAreDetected(int type) throws SQLException {
    throw UnsupportedOpHelper.newUnimplementedMethodException();
  }

  @Override
  public boolean insertsAreDetected(int type) throws SQLException {
    throw UnsupportedOpHelper.newUnimplementedMethodException();
  }

  @Override
  public boolean supportsBatchUpdates() throws SQLException {
    throw UnsupportedOpHelper.newUnimplementedMethodException();
  }

  @Override
  public ResultSet getUDTs(String catalog, String schemaPattern, String typeNamePattern, int[] types)
      throws SQLException {
    throw UnsupportedOpHelper.newUnimplementedMethodException();
  }

  @Override
  public Connection getConnection() throws SQLException {
    throw UnsupportedOpHelper.newUnimplementedMethodException();
  }

  @Override
  public boolean supportsSavepoints() throws SQLException {
    throw UnsupportedOpHelper.newUnimplementedMethodException();
  }

  @Override
  public boolean supportsNamedParameters() throws SQLException {
    throw UnsupportedOpHelper.newUnimplementedMethodException();
  }

  @Override
  public boolean supportsMultipleOpenResults() throws SQLException {
    throw UnsupportedOpHelper.newUnimplementedMethodException();
  }

  @Override
  public boolean supportsGetGeneratedKeys() throws SQLException {
    throw UnsupportedOpHelper.newUnimplementedMethodException();
  }

  @Override
  public ResultSet getSuperTypes(String catalog, String schemaPattern, String typeNamePattern)
      throws SQLException {
    throw UnsupportedOpHelper.newUnimplementedMethodException();
  }

  @Override
  public ResultSet getSuperTables(String catalog, String schemaPattern, String tableNamePattern)
      throws SQLException {
    throw UnsupportedOpHelper.newUnimplementedMethodException();
  }

  @Override
  public ResultSet getAttributes(String catalog, String schemaPattern, String typeNamePattern,
      String attributeNamePattern) throws SQLException {
    throw UnsupportedOpHelper.newUnimplementedMethodException();
  }

  @Override
  public boolean supportsResultSetHoldability(int holdability) throws SQLException {
    throw UnsupportedOpHelper.newUnimplementedMethodException();
  }

  @Override
  public int getResultSetHoldability() throws SQLException {
    throw UnsupportedOpHelper.newUnimplementedMethodException();
  }

  @Override
  public int getDatabaseMajorVersion() throws SQLException {
    throw UnsupportedOpHelper.newUnimplementedMethodException();
  }

  @Override
  public int getDatabaseMinorVersion() throws SQLException {
    throw UnsupportedOpHelper.newUnimplementedMethodException();
  }

  @Override
  public int getJDBCMajorVersion() throws SQLException {
    throw UnsupportedOpHelper.newUnimplementedMethodException();
  }

  @Override
  public int getJDBCMinorVersion() throws SQLException {
    throw UnsupportedOpHelper.newUnimplementedMethodException();
  }

  @Override
  public int getSQLStateType() throws SQLException {
    throw UnsupportedOpHelper.newUnimplementedMethodException();
  }

  @Override
  public boolean locatorsUpdateCopy() throws SQLException {
    throw UnsupportedOpHelper.newUnimplementedMethodException();
  }

  @Override
  public boolean supportsStatementPooling() throws SQLException {
    throw UnsupportedOpHelper.newUnimplementedMethodException();
  }

  @Override
  public RowIdLifetime getRowIdLifetime() throws SQLException {
    throw UnsupportedOpHelper.newUnimplementedMethodException();
  }

  @Override
  public ResultSet getSchemas(String catalog, String schemaPattern) throws SQLException {
    throw UnsupportedOpHelper.newUnimplementedMethodException();
  }

  @Override
  public boolean supportsStoredFunctionsUsingCallSyntax() throws SQLException {
    throw UnsupportedOpHelper.newUnimplementedMethodException();
  }

  @Override
  public boolean autoCommitFailureClosesAllResultSets() throws SQLException {
    throw UnsupportedOpHelper.newUnimplementedMethodException();
  }

  @Override
  public ResultSet getClientInfoProperties() throws SQLException {
    throw UnsupportedOpHelper.newUnimplementedMethodException();
  }

  @Override
  public ResultSet getFunctions(String catalog, String schemaPattern, String functionNamePattern)
      throws SQLException {
    throw UnsupportedOpHelper.newUnimplementedMethodException();
  }

  @Override
  public ResultSet getFunctionColumns(String catalog, String schemaPattern,
      String functionNamePattern, String columnNamePattern) throws SQLException {
    throw UnsupportedOpHelper.newUnimplementedMethodException();
  }
}
