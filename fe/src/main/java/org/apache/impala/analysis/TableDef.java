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

package org.apache.impala.analysis;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.UUID;
import java.util.stream.Collectors;

import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.hive.metastore.api.SQLForeignKey;
import org.apache.hadoop.hive.metastore.api.SQLPrimaryKey;
import org.apache.impala.authorization.Privilege;
import org.apache.impala.catalog.Column;
import org.apache.impala.catalog.FeFsTable;
import org.apache.impala.catalog.FeTable;
import org.apache.impala.catalog.HdfsStorageDescriptor;
import org.apache.impala.catalog.RowFormat;
import org.apache.impala.catalog.Type;
import org.apache.impala.common.AnalysisException;
import org.apache.impala.common.FileSystemUtil;
import org.apache.impala.common.Pair;
import org.apache.impala.thrift.TAccessEvent;
import org.apache.impala.thrift.TBucketInfo;
import org.apache.impala.thrift.TBucketType;
import org.apache.impala.thrift.TCatalogObjectType;
import org.apache.impala.thrift.THdfsFileFormat;
import org.apache.impala.thrift.TQueryOptions;
import org.apache.impala.thrift.TSortingOrder;
import org.apache.impala.util.AcidUtils;
import org.apache.impala.util.KuduUtil;
import org.apache.impala.util.MetaStoreUtil;
import org.apache.thrift.TException;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

/**
 * Represents the table parameters in a CREATE TABLE statement. These parameters
 * correspond to the following clauses in a CREATE TABLE statement:
 * - EXTERNAL
 * - IF NOT EXISTS
 * - PARTITIONED BY
 * - PARTITION BY
 * - ROWFORMAT
 * - FILEFORMAT
 * - COMMENT
 * - SERDEPROPERTIES
 * - TBLPROPERTIES
 * - LOCATION
 * - CACHED IN
 * - SORT BY
 */
class TableDef {
  // Name of the new table
  private final TableName tableName_;

  // List of column definitions
  private final List<ColumnDef> columnDefs_ = new ArrayList<>();

  // Names of primary key columns. Populated by the parser. An empty value doesn't
  // mean no primary keys were specified as the columnDefs_ could contain primary keys.
  private final List<String> primaryKeyColNames_ = new ArrayList<>();

  // If true, the primary key is unique. If not, and the table is a Kudu table then an
  // auto-incrementing column will be added automatically by Kudu engine. This extra key
  // column helps produce a unique composite primary key (primary keys +
  // auto-incrementing construct).
  // This is also used for Iceberg table and set to false if "NOT ENFORCED" is provided
  // for the primary key.
  private boolean isPrimaryKeyUnique_;

  // If true, the table's data will be preserved if dropped.
  private final boolean isExternal_;

  // If true, no errors are thrown if the table already exists.
  private final boolean ifNotExists_;

  // Partitioning parameters.
  private final TableDataLayout dataLayout_;

  /////////////////////////////////////////
  // BEGIN: Members that need to be reset()

  // Authoritative list of primary key column definitions populated during analysis.
  private final List<ColumnDef> primaryKeyColDefs_ = new ArrayList<>();

  // Hive primary keys and foreign keys structures populated during analysis.
  List<SQLPrimaryKey> sqlPrimaryKeys_ = new ArrayList<>();
  List<SQLForeignKey> sqlForeignKeys_ = new ArrayList<>();

  public List<SQLPrimaryKey> getSqlPrimaryKeys() {
    return sqlPrimaryKeys_;
  }

  public List<SQLForeignKey> getSqlForeignKeys() {
    return sqlForeignKeys_;
  }

  // True if analyze() has been called.
  private boolean isAnalyzed_ = false;

  // Generated properties set during analysis. Currently used by Kudu and Iceberg.
  private Map<String, String> generatedProperties_ = new HashMap<>();

  // END: Members that need to be reset()
  /////////////////////////////////////////

  /**
   * Set of table options. These options are grouped together for convenience while
   * parsing CREATE TABLE statements. They are typically found at the end of CREATE
   * TABLE statements.
   */
  static class Options {
    // Optional list of columns to sort data by when inserting into this table.
    final List<String> sortCols;

    // Comment to attach to the table
    final String comment;

    // Custom row format of the table. Leave null to specify default row format.
    final RowFormat rowFormat;

    // Key/values to persist with table serde metadata.
    final Map<String, String> serdeProperties;

    // File format of the table
    final THdfsFileFormat fileFormat;

    // The HDFS location of where the table data will stored.
    final HdfsUri location;

    // The HDFS caching op that should be applied to this table.
    final HdfsCachingOp cachingOp;

    // Key/values to persist with table metadata.
    final Map<String, String> tblProperties;

    // Sorting order for SORT BY queries.
    final TSortingOrder sortingOrder;

    // Bucket desc for CLUSTERED BY
    final TBucketInfo bucketInfo;

    Options(TBucketInfo bucketInfo, Pair<List<String>, TSortingOrder> sortProperties,
        String comment, RowFormat rowFormat, Map<String, String> serdeProperties,
        THdfsFileFormat fileFormat, HdfsUri location, HdfsCachingOp cachingOp,
        Map<String, String> tblProperties, TQueryOptions queryOptions) {
      this.sortCols = sortProperties.first;
      this.sortingOrder = sortProperties.second;
      this.comment = comment;
      this.rowFormat = rowFormat;
      Preconditions.checkNotNull(serdeProperties);
      this.serdeProperties = serdeProperties;
      // The file format passed via STORED AS <file format> has a higher precedence than
      // the one set in query options.
      this.fileFormat = (fileFormat != null) ?
          fileFormat : queryOptions.getDefault_file_format();
      this.location = location;
      this.cachingOp = cachingOp;
      Preconditions.checkNotNull(tblProperties);
      this.tblProperties = tblProperties;
      this.bucketInfo = bucketInfo;
    }

    public Options(String comment, TQueryOptions queryOptions) {
      // Passing null to file format so that it uses the file format from the query option
      // if specified, otherwise it will use the default file format, which is TEXT.
      this(null, new Pair<>(ImmutableList.of(), TSortingOrder.LEXICAL), comment,
          RowFormat.DEFAULT_ROW_FORMAT, new HashMap<>(), /* file format */null, null,
          null, new HashMap<>(), queryOptions);
    }
  }

  private Options options_;

  /**
   * Primary Key attributes grouped together to be populated by the parser.
   * Currently only defined for HDFS tables.
   */
  static class PrimaryKey {

    // Primary key table name
    final TableName pkTableName;

    // Primary Key columns
    final List<String> primaryKeyColNames;

    // Primary Key constraint name
    final String pkConstraintName;

    // Constraints
    final boolean relyCstr;
    final boolean validateCstr;
    final boolean enableCstr;


    public PrimaryKey(TableName pkTableName, List<String> primaryKeyColNames,
                      String pkConstraintName, boolean relyCstr,
                      boolean validateCstr, boolean enableCstr) {
      this.pkTableName = pkTableName;
      this.primaryKeyColNames = primaryKeyColNames;
      this.pkConstraintName = pkConstraintName;
      this.relyCstr = relyCstr;
      this.validateCstr = validateCstr;
      this.enableCstr = enableCstr;
    }

    public TableName getPkTableName() {
      return pkTableName;
    }

    public List<String> getPrimaryKeyColNames() {
      return primaryKeyColNames;
    }

    public String getPkConstraintName() {
      return pkConstraintName;
    }

    public boolean isRelyCstr() {
      return relyCstr;
    }

    public boolean isValidateCstr() {
      return validateCstr;
    }

    public boolean isEnableCstr() {
      return enableCstr;
    }
  }


  /**
   * Foreign Key attributes grouped together to be populated by the parser.
   * Currently only defined for HDFS tables. An FK definition is of the form
   * "foreign key(col1, col2) references pk_tbl(col3, col4)"
   */
  static class ForeignKey {
    // Primary key table
    final TableName pkTableName;

    // Primary key cols
    final List<String> primaryKeyColNames;

    // Foreign key cols
    final List<String> foreignKeyColNames;

    // Name of fk
    String fkConstraintName;

    // Fully qualified pk name. Set during analysis.
    TableName fullyQualifiedPkTableName;

    // Constraints
    final boolean relyCstr;
    final boolean validateCstr;
    final boolean enableCstr;

    ForeignKey(TableName pkTableName, List<String> primaryKeyColNames,
               List<String> foreignKeyColNames, String fkName, boolean relyCstr,
               boolean validateCstr, boolean enableCstr) {
      this.pkTableName = pkTableName;
      this.primaryKeyColNames = primaryKeyColNames;
      this.foreignKeyColNames = foreignKeyColNames;
      this.relyCstr = relyCstr;
      this.validateCstr = validateCstr;
      this.enableCstr = enableCstr;
      this.fkConstraintName = fkName;
    }

    public TableName getPkTableName() {
      return pkTableName;
    }

    public List<String> getPrimaryKeyColNames() {
      return primaryKeyColNames;
    }

    public List<String> getForeignKeyColNames() {
      return foreignKeyColNames;
    }

    public String getFkConstraintName() {
      return fkConstraintName;
    }

    public void setConstraintName(String constraintName) {
      fkConstraintName = constraintName;
    }

    public TableName getFullyQualifiedPkTableName() {
      return fullyQualifiedPkTableName;
    }

    public boolean isRelyCstr() {
      return relyCstr;
    }

    public boolean isValidateCstr() {
      return validateCstr;
    }

    public boolean isEnableCstr() {
      return enableCstr;
    }
  }

  // A TableDef will have only one primary key.
  private PrimaryKey primaryKey_;

  // There maybe multiple foreign keys for a TableDef forming multiple PK-FK
  // relationships.
  private List<ForeignKey> foreignKeysList_ = new ArrayList<>();

  // Result of analysis.
  private TableName fqTableName_;

  TableDef(TableName tableName, boolean isExternal, boolean ifNotExists) {
    tableName_ = tableName;
    isExternal_ = isExternal;
    ifNotExists_ = ifNotExists;
    dataLayout_ = TableDataLayout.createEmptyLayout();
  }

  public void reset() {
    primaryKeyColDefs_.clear();
    columnDefs_.clear();
    isAnalyzed_ = false;
    generatedProperties_.clear();
  }

  public TableName getTblName() {
    return fqTableName_ != null ? fqTableName_ : tableName_;
  }
  public String getTbl() { return tableName_.getTbl(); }
  public boolean isAnalyzed() { return isAnalyzed_; }
  List<ColumnDef> getColumnDefs() { return columnDefs_; }
  List<String> getColumnNames() { return ColumnDef.toColumnNames(columnDefs_); }

  List<Type> getColumnTypes() {
    return columnDefs_.stream().map(col -> col.getType()).collect(Collectors.toList());
  }

  public void setPrimaryKey(TableDef.PrimaryKey primaryKey) {
    this.primaryKey_ = primaryKey;
  }

  public void setPrimaryKeyUnique(boolean isKeyUnique) {
    this.isPrimaryKeyUnique_ = isKeyUnique;
  }

  List<String> getPartitionColumnNames() {
    return ColumnDef.toColumnNames(getPartitionColumnDefs());
  }

  List<ColumnDef> getPartitionColumnDefs() {
    return dataLayout_.getPartitionColumnDefs();
  }

  boolean isKuduTable() { return options_.fileFormat == THdfsFileFormat.KUDU; }
  boolean isIcebergTable() { return options_.fileFormat == THdfsFileFormat.ICEBERG; }
  List<String> getPrimaryKeyColumnNames() { return primaryKeyColNames_; }
  List<ColumnDef> getPrimaryKeyColumnDefs() { return primaryKeyColDefs_; }
  boolean isPrimaryKeyUnique() { return isPrimaryKeyUnique_; }
  boolean isExternal() { return isExternal_; }
  boolean getIfNotExists() { return ifNotExists_; }
  Map<String, String> getGeneratedProperties() { return generatedProperties_; }
  void putGeneratedProperty(String key, String value) {
    Preconditions.checkNotNull(key);
    generatedProperties_.put(key, value);
  }
  List<KuduPartitionParam> getKuduPartitionParams() {
    return dataLayout_.getKuduPartitionParams();
  }

  List<IcebergPartitionSpec> getIcebergPartitionSpecs() {
    return dataLayout_.getIcebergPartitionSpecs();
  }
  void setOptions(Options options) {
    Preconditions.checkNotNull(options);
    options_ = options;
  }
  List<String> getSortColumns() { return options_.sortCols; }
  String getComment() { return options_.comment; }
  Map<String, String> getTblProperties() { return options_.tblProperties; }
  HdfsCachingOp getCachingOp() { return options_.cachingOp; }
  HdfsUri getLocation() { return options_.location; }
  Map<String, String> getSerdeProperties() { return options_.serdeProperties; }
  THdfsFileFormat getFileFormat() { return options_.fileFormat; }
  RowFormat getRowFormat() { return options_.rowFormat; }
  TSortingOrder getSortingOrder() { return options_.sortingOrder; }
  List<ForeignKey> getForeignKeysList() { return foreignKeysList_; }
  TBucketInfo geTBucketInfo() { return options_.bucketInfo; }

  boolean isBucketableFormat() {
    return options_.fileFormat != THdfsFileFormat.KUDU
        && options_.fileFormat != THdfsFileFormat.ICEBERG
        && options_.fileFormat != THdfsFileFormat.HUDI_PARQUET
        && options_.fileFormat != THdfsFileFormat.JDBC;
  }

  /**
   * Analyzes the parameters of a CREATE TABLE statement.
   */
  void analyze(Analyzer analyzer) throws AnalysisException {
    if (isAnalyzed_) return;
    Preconditions.checkState(tableName_ != null && !tableName_.isEmpty());
    fqTableName_ = analyzer.getFqTableName(getTblName());
    fqTableName_.analyze();
    analyzeAcidProperties(analyzer);
    analyzeColumnDefs(analyzer);
    analyzePrimaryKeys(analyzer);
    analyzeForeignKeys(analyzer);

    if (analyzer.dbContainsTable(getTblName().getDb(), getTbl(), Privilege.CREATE)
        && !getIfNotExists()) {
      throw new AnalysisException(Analyzer.TBL_ALREADY_EXISTS_ERROR_MSG + getTblName());
    }

    analyzer.addAccessEvent(new TAccessEvent(fqTableName_.toString(),
        TCatalogObjectType.TABLE, Privilege.CREATE.toString()));

    Preconditions.checkNotNull(options_);
    analyzeOptions(analyzer);
    isAnalyzed_ = true;
  }

  /**
   * Analyzes table and partition column definitions, checking whether all column
   * names are unique.
   */
  private void analyzeColumnDefs(Analyzer analyzer) throws AnalysisException {
    Set<String> colNames = new HashSet<>();
    for (ColumnDef colDef: columnDefs_) {
      colDef.analyze(analyzer);
      if (!colNames.add(colDef.getColName().toLowerCase())) {
        throw new AnalysisException("Duplicate column name: " + colDef.getColName());
      }

      if (!analyzeColumnOption(colDef)) {
        throw new AnalysisException(String.format("Unsupported column options for " +
            "file format '%s': '%s'", getFileFormat().name(), colDef.toString()));
      }
    }
    for (ColumnDef colDef: getPartitionColumnDefs()) {
      colDef.analyze(analyzer);
      if (!colDef.getType().supportsTablePartitioning()) {
        throw new AnalysisException(
            String.format("Type '%s' is not supported as partition-column type " +
                "in column: %s", colDef.getType().toSql(), colDef.getColName()));
      }
      if (!colNames.add(colDef.getColName().toLowerCase())) {
        throw new AnalysisException("Duplicate column name: " + colDef.getColName());
      }
    }
  }

  /**
   * Kudu and Iceberg tables have their own column options, this function will return
   * false if we use column options incorrectly, e.g. primary key column option for an
   * Iceberg table.
   */
  private boolean analyzeColumnOption(ColumnDef columnDef) {
    if (isKuduTable()) {
      if (columnDef.hasIncompatibleKuduOptions()) return false;
    } else if (isIcebergTable()) {
      if (columnDef.hasIncompatibleIcebergOptions()) return false;
    } else if (columnDef.hasKuduOptions() || columnDef.hasIcebergOptions()) {
      // If the table is neither Kudu or Iceberg but has some incompatible column options.
      return false;
    }
    return true;
  }

  /**
   * Analyzes the primary key columns. Checks if the specified primary key columns exist
   * in the table column definitions and if composite primary keys are properly defined
   * using the PRIMARY KEY (col,..col) clause.
   */
  private void analyzePrimaryKeys(Analyzer analyzer) throws AnalysisException {
    for (ColumnDef colDef: columnDefs_) {
      if (colDef.isPrimaryKey()) {
        primaryKeyColDefs_.add(colDef);
        if (!colDef.isPrimaryKeyUnique() && !isKuduTable()) {
          throw new AnalysisException(
              "Non unique primary key is only supported for Kudu.");
        }
      }
    }
    if (primaryKeyColDefs_.size() > 1) {
      String primaryKeyString =
          KuduUtil.getPrimaryKeyString(primaryKeyColDefs_.get(0).isPrimaryKeyUnique());
      throw new AnalysisException(String.format(
          "Multiple %sS specified. Composite %s can be specified using the %s " +
          "(col1, col2, ...) syntax at the end of the column definition.",
          primaryKeyString, primaryKeyString, primaryKeyString));
    }

    if (primaryKeyColNames_.isEmpty()) {
      if (primaryKey_ == null || primaryKey_.getPrimaryKeyColNames().isEmpty()) {
        if (!isKuduTable()) return;

        if (!primaryKeyColDefs_.isEmpty()) {
          setPrimaryKeyUnique(primaryKeyColDefs_.get(0).isPrimaryKeyUnique());
          return;
        } else if (!getKuduPartitionParams().isEmpty()) {
          // Promote all partition columns as non unique primary key columns if primary
          // keys are not declared by the user for the Kudu table. Since key columns
          // must be as the first columns in the table, only all partition columns which
          // are the beginning columns of the table can be promoted as non unique primary
          // key columns.
          List<String> colNames = getColumnNames();
          TreeMap<Integer, String> partitionCols = new TreeMap<Integer, String>();
          for (KuduPartitionParam partitionParam: getKuduPartitionParams()) {
            for (String colName: partitionParam.getColumnNames()) {
              int index = colNames.indexOf(colName);
              Preconditions.checkState(index >= 0);
              partitionCols.put(index, colName);
            }
          }
          if (partitionCols.size() > 0
              && partitionCols.lastKey() == partitionCols.size() - 1) {
            primaryKeyColNames_.addAll(partitionCols.values());
            setPrimaryKeyUnique(false);
            analyzer.addWarning(String.format(
                "Partition columns (%s) are promoted as non unique primary key.",
                String.join(", ", partitionCols.values())));
          } else {
            throw new AnalysisException(
                "Specify primary key or non unique primary key for the Kudu table, " +
                "or create partitions with the beginning columns of the table.");
          }
        }
        if (primaryKeyColNames_.isEmpty()) return;
      } else {
        primaryKeyColNames_.addAll(primaryKey_.getPrimaryKeyColNames());
      }
    }

    String primaryKeyString = KuduUtil.getPrimaryKeyString(isPrimaryKeyUnique_);
    if (!primaryKeyColDefs_.isEmpty()) {
      throw new AnalysisException(String.format(
          "Multiple %sS specified. Composite %s can be specified using the %s " +
          "(col1, col2, ...) syntax at the end of the column definition.",
          primaryKeyString, primaryKeyString, primaryKeyString));
    } else if (!primaryKeyColNames_.isEmpty() && !isPrimaryKeyUnique()
        && !isKuduTable() && !isIcebergTable()) {
      throw new AnalysisException(primaryKeyString +
          " is only supported for Kudu and Iceberg.");
    }

    if (isIcebergTable() && isPrimaryKeyUnique_) {
      throw new AnalysisException(
          "Iceberg tables only support NOT ENFORCED primary keys.");
    }

    Set<String> hashedPKColNames = Sets.newHashSet(primaryKeyColNames_);
    List<IcebergPartitionSpec> icebergPartitionSpecs = getIcebergPartitionSpecs();
    if (!icebergPartitionSpecs.isEmpty()) {
      Preconditions.checkState(icebergPartitionSpecs.size() == 1);
      IcebergPartitionSpec partSpec = icebergPartitionSpecs.get(0);
      for (IcebergPartitionField partField : partSpec.getIcebergPartitionFields()) {
        if (!hashedPKColNames.contains(partField.getFieldName())) {
          throw new AnalysisException("Partition columns have to be part of the " +
              "primary key for Iceberg tables.");
        }
      }
    }

    Map<String, ColumnDef> colDefsByColName = ColumnDef.mapByColumnNames(columnDefs_);
    int keySeq = 1;
    String constraintName = null;
    for (String colName: primaryKeyColNames_) {
      colName = colName.toLowerCase();
      ColumnDef colDef = colDefsByColName.remove(colName);
      if (colDef == null) {
        if (ColumnDef.toColumnNames(primaryKeyColDefs_).contains(colName)) {
          throw new AnalysisException(String.format("Column '%s' is listed multiple " +
              "times as a %s.", colName, primaryKeyString));
        }
        throw new AnalysisException(String.format("%s column '%s' does not exist in " +
            "the table", primaryKeyString, colName));
      }
      if (colDef.isExplicitNullable()) {
        throw new AnalysisException(primaryKeyString + " columns cannot be nullable: " +
            colDef.toString());
      }
      // HDFS Table specific analysis.
      if (primaryKey_ != null) {
        // We do not support enable and validate for primary keys.
        if (primaryKey_.isEnableCstr()) {
          throw new AnalysisException("ENABLE feature is not supported yet.");
        }
        if (primaryKey_.isValidateCstr()) {
          throw new AnalysisException("VALIDATE feature is not supported yet.");
        }
        // All primary keys in a composite key should have the same constraint name. This
        // is necessary because of HIVE-16603. See IMPALA-9188 for details.
        if (constraintName == null) {
          constraintName = generateConstraintName();
        }
        // Each column of a primary key definition will be an SQLPrimaryKey.
        sqlPrimaryKeys_.add(new SQLPrimaryKey(getTblName().getDb(), getTbl(),
            colDef.getColName(), keySeq++, constraintName, primaryKey_.enableCstr,
            primaryKey_.validateCstr, primaryKey_.relyCstr));
      }
      primaryKeyColDefs_.add(colDef);
    }
  }

  private void analyzeForeignKeys(Analyzer analyzer) throws AnalysisException {
    if (foreignKeysList_ == null || foreignKeysList_.size() == 0) return;
    for (ForeignKey fk: foreignKeysList_) {
      // Foreign Key and Primary Key columns don't match.
      if (fk.getForeignKeyColNames().size() != fk.getPrimaryKeyColNames().size()){
        throw new AnalysisException("The number of foreign key columns should be same" +
            " as the number of parent key columns.");
      }
      String parentDb = fk.getPkTableName().getDb();
      if (parentDb == null) {
        parentDb = analyzer.getDefaultDb();
      }
      fk.fullyQualifiedPkTableName = new TableName(parentDb, fk.pkTableName.getTbl());
      //Check if parent table exits
      if (!analyzer.dbContainsTable(parentDb, fk.getPkTableName().getTbl(),
          Privilege.VIEW_METADATA)) {
        throw new AnalysisException("Parent table not found: "
            + analyzer.getFqTableName(fk.getPkTableName()));
      }

      //Check for primary key cols in parent table
      FeTable parentTable = analyzer.getTable(fk.getPkTableName(),
          Privilege.VIEW_METADATA);

      if (!(parentTable instanceof FeFsTable)) {
        throw new AnalysisException("Foreign keys on non-HDFS parent tables are not "
            + "supported.");
      }

      for (String pkCol : fk.getPrimaryKeyColNames()) {
        // TODO: Check column types of parent table and child tables match. Currently HMS
        //  API fails if they don't, it's good to fail early during analysis here.
        if (!parentTable.getColumnNames().contains(pkCol.toLowerCase())) {
          throw new AnalysisException("Parent column not found: " + pkCol.toLowerCase());
        }
        // Hive has a bug that prevents foreign keys from being added when pk column is
        // not part of primary key. This can be confusing. Till this bug is fixed, we
        // will not allow foreign keys definition on such columns.
        try {
          if (!((FeFsTable) parentTable).getPrimaryKeyColumnNames().contains(pkCol)) {
            throw new AnalysisException(String.format("Parent column %s is not part of "
                + "primary key.", pkCol));
          }
        } catch (TException e) {
          // In local catalog mode, we do not aggressively load PK/FK information, a
          // call to getPrimaryKeyColumnNames() will try to selectively load PK/FK
          // information. Hence, TException is thrown only in local catalog mode.
          throw new AnalysisException("Failed to get primary key columns for "
              + fk.pkTableName);
        }
      }

      // We do not support ENABLE and VALIDATE.
      if (fk.isEnableCstr()) {
        throw new AnalysisException("ENABLE feature is not supported yet.");
      }

      if (fk.isValidateCstr()) {
        throw new AnalysisException("VALIDATE feature is not supported yet.");
      }

      if (fk.getFkConstraintName() == null) {
        fk.setConstraintName(generateConstraintName());
      }

      for (int i = 0; i < fk.getForeignKeyColNames().size(); i++) {
        SQLForeignKey sqlForeignKey = new SQLForeignKey();
        sqlForeignKey.setPktable_db(parentDb);
        sqlForeignKey.setPktable_name(fk.getPkTableName().getTbl());
        sqlForeignKey.setFktable_db(getTblName().getDb());
        sqlForeignKey.setFktable_name(getTbl());
        sqlForeignKey.setPkcolumn_name(fk.getPrimaryKeyColNames().get(i).toLowerCase());
        sqlForeignKey.setFk_name(fk.getFkConstraintName());
        sqlForeignKey.setKey_seq(i+1);
        sqlForeignKey.setFkcolumn_name(fk.getForeignKeyColNames().get(i).toLowerCase());
        sqlForeignKey.setRely_cstr(fk.isRelyCstr());
        getSqlForeignKeys().add(sqlForeignKey);
      }
    }
  }

  /**
   * Utility method to generate a unique constraint name when user does not specify one.
   * TODO: Collisions possible? HMS doesn't have an API to query existing constraint
   * names.
   */
  private String generateConstraintName() {
    return UUID.randomUUID().toString();
  }

  /**
   * Analyzes the list of columns in 'sortCols' against the columns of 'table' and
   * returns their matching positions in the table's columns. Each column of 'sortCols'
   * must occur in 'table' as a non-partitioning column. 'table' must be an HDFS table.
   * If there are errors during the analysis, this will throw an AnalysisException.
   */
  public static List<Integer> analyzeSortColumns(List<String> sortCols, FeTable table,
      TSortingOrder sortingOrder) throws AnalysisException {
    Preconditions.checkState(table instanceof FeFsTable);

    List<Type> columnTypes = table.getNonClusteringColumns().stream().map(
        col -> col.getType()).collect(Collectors.toList());
    return analyzeSortColumns(sortCols,
        Column.toColumnNames(table.getNonClusteringColumns()),
        Column.toColumnNames(table.getClusteringColumns()), columnTypes, sortingOrder);
  }

  /**
   * Analyzes the list of columns in 'sortCols' and returns their matching positions in
   * 'tableCols'. Each column must occur in 'tableCols' and must not occur in
   * 'partitionCols'. If there are errors during the analysis, this will throw an
   * AnalysisException.
   */
  public static List<Integer> analyzeSortColumns(List<String> sortCols,
      List<String> tableCols, List<String> partitionCols, List<Type> columnTypes,
      TSortingOrder sortingOrder) throws AnalysisException {
    // The index of each sort column in the list of table columns.
    Set<Integer> colIdxs = new LinkedHashSet<>();

    int numColumns = 0;
    for (String sortColName: sortCols) {
      ++numColumns;
      // Make sure it's not a partition column.
      if (partitionCols.contains(sortColName)) {
        throw new AnalysisException(String.format("SORT BY column list must not " +
            "contain partition column: '%s'", sortColName));
      }

      // Determine the index of each sort column in the list of table columns.
      boolean foundColumn = false;
      for (int j = 0; j < tableCols.size(); ++j) {
        if (tableCols.get(j).equalsIgnoreCase(sortColName)) {
          if (colIdxs.contains(j)) {
            throw new AnalysisException(String.format("Duplicate column in SORT BY " +
                "list: %s", sortColName));
          }
          colIdxs.add(j);
          foundColumn = true;
          break;
        }
      }
      if (!foundColumn) {
        throw new AnalysisException(String.format("Could not find SORT BY column " +
            "'%s' in table.", sortColName));
      }
    }

    // Analyzing Z-Order specific constraints
    if (sortingOrder == TSortingOrder.ZORDER) {
      if (numColumns == 1) {
        throw new AnalysisException(String.format("SORT BY ZORDER with 1 column is " +
            "equivalent to SORT BY. Please, use the latter, if that was your " +
            "intention."));
      }
    }

    Preconditions.checkState(numColumns == colIdxs.size());
    return Lists.newArrayList(colIdxs);
  }

  private void analyzeOptions(Analyzer analyzer) throws AnalysisException {
    MetaStoreUtil.checkShortPropertyMap("Property", options_.tblProperties);
    MetaStoreUtil.checkShortPropertyMap("Serde property", options_.serdeProperties);

    if (options_.location != null) {
      options_.location.analyze(analyzer, Privilege.ALL, FsAction.READ_WRITE);
    }

    if (options_.cachingOp != null) {
      options_.cachingOp.analyze(analyzer);
      if (options_.cachingOp.shouldCache() && options_.location != null &&
          !FileSystemUtil.isPathCacheable(options_.location.getPath())) {
        throw new AnalysisException(String.format("Location '%s' cannot be cached. " +
            "Please retry without caching: CREATE TABLE ... UNCACHED",
            options_.location));
      }
    }

    // Analyze 'skip.header.line.format' property.
    AlterTableSetTblProperties.analyzeSkipHeaderLineCount(options_.tblProperties);
    analyzeRowFormat(analyzer);

    String sortByKey = AlterTableSortByStmt.TBL_PROP_SORT_COLUMNS;
    String sortOrderKey = AlterTableSortByStmt.TBL_PROP_SORT_ORDER;
    if (options_.tblProperties.containsKey(sortByKey)) {
      throw new AnalysisException(String.format("Table definition must not contain the " +
          "%s table property. Use SORT BY (...) instead.", sortByKey));
    }

    if (options_.tblProperties.containsKey(sortOrderKey)) {
      throw new AnalysisException(String.format("Table definition must not contain the " +
          "%s table property. Use SORT BY %s (...) instead.", sortOrderKey,
          options_.sortingOrder.toString()));
    }

    // analyze bucket columns
    analyzeBucketColumns(options_.bucketInfo, getColumnNames(),
        getPartitionColumnNames());

    // Analyze sort columns.
    if (options_.sortCols == null) return;
    if (isKuduTable()) {
      throw new AnalysisException(String.format("SORT BY is not supported for Kudu "+
          "tables."));
    }

    analyzeSortColumns(options_.sortCols, getColumnNames(), getPartitionColumnNames(),
        getColumnTypes(), options_.sortingOrder);
  }

  private void analyzeBucketColumns(TBucketInfo bucketInfo, List<String> tableCols,
      List<String> partitionCols) throws AnalysisException {
    if (bucketInfo == null || bucketInfo.getBucket_type() == TBucketType.NONE) {
      return;
    }
    // Bucketed Table only support hdfs fileformat
    if (!isBucketableFormat()) {
      throw new AnalysisException(String.format("CLUSTERED BY not support fileformat: " +
          "'%s'", options_.fileFormat));
    }
    if (bucketInfo.getNum_bucket() <= 0) {
      throw new AnalysisException(String.format(
          "Bucket's number must be greater than 0."));
    }
    if (bucketInfo.getBucket_columns() == null
        || bucketInfo.getBucket_columns().size() == 0) {
      throw new AnalysisException(String.format(
          "Bucket columns must be not null."));
    }

    // The index of each bucket column in the list of table columns.
    Set<Integer> colIdxs = new LinkedHashSet<>();
    for (String bucketCol : bucketInfo.getBucket_columns()) {
      // Make sure it's not a partition column.
      if (partitionCols.contains(bucketCol)) {
        throw new AnalysisException(String.format("CLUSTERED BY column list must not " +
            "contain partition column: '%s'", bucketCol));
      }

      // Determine the index of each bucket column in the list of table columns.
      boolean foundColumn = false;
      for (int j = 0; j < tableCols.size(); ++j) {
        if (tableCols.get(j).equalsIgnoreCase(bucketCol)) {
          if (colIdxs.contains(j)) {
            throw new AnalysisException(String.format("Duplicate column in CLUSTERED " +
                "BY list: %s", bucketCol));
          }
          colIdxs.add(j);
          foundColumn = true;
          break;
        }
      }
      if (!foundColumn) {
        throw new AnalysisException(String.format("Could not find CLUSTERED BY column " +
            "'%s' in table.", bucketCol));
      }
    }
  }

  private void analyzeRowFormat(Analyzer analyzer) throws AnalysisException {
    if (options_.rowFormat == null) return;
    if (isKuduTable()) {
      throw new AnalysisException(String.format(
          "ROW FORMAT cannot be specified for file format %s.", options_.fileFormat));
    }

    Byte fieldDelim = analyzeRowFormatValue(options_.rowFormat.getFieldDelimiter());
    Byte lineDelim = analyzeRowFormatValue(options_.rowFormat.getLineDelimiter());
    Byte escapeChar = analyzeRowFormatValue(options_.rowFormat.getEscapeChar());
    if (options_.fileFormat == THdfsFileFormat.TEXT) {
      if (fieldDelim == null) fieldDelim = HdfsStorageDescriptor.DEFAULT_FIELD_DELIM;
      if (lineDelim == null) lineDelim = HdfsStorageDescriptor.DEFAULT_LINE_DELIM;
      if (escapeChar == null) escapeChar = HdfsStorageDescriptor.DEFAULT_ESCAPE_CHAR;
      if (fieldDelim.equals(lineDelim)) {
        throw new AnalysisException("Field delimiter and line delimiter have same " +
            "value: byte " + fieldDelim);
      }
      if (fieldDelim.equals(escapeChar)) {
        analyzer.addWarning("Field delimiter and escape character have same value: " +
            "byte " + fieldDelim + ". Escape character will be ignored");
      }
      if (lineDelim.equals(escapeChar)) {
        analyzer.addWarning("Line delimiter and escape character have same value: " +
            "byte " + lineDelim + ". Escape character will be ignored");
      }
    }
  }

  private Byte analyzeRowFormatValue(String value) throws AnalysisException {
    if (value == null) return null;
    Byte byteVal = HdfsStorageDescriptor.parseDelim(value);
    if (byteVal == null) {
      throw new AnalysisException("ESCAPED BY values and LINE/FIELD " +
          "terminators must be specified as a single character or as a decimal " +
          "value in the range [-128:127]: " + value);
    }
    return byteVal;
  }

  /**
   * Analyzes Hive ACID related properties.
   * Can change table properties based on query options.
   */
  private void analyzeAcidProperties(Analyzer analyzer) throws AnalysisException {
    if (isExternal_) {
      if (AcidUtils.isTransactionalTable(options_.tblProperties)) {
        throw new AnalysisException("EXTERNAL tables cannot be transactional");
      }
      return;
    }

    if (options_.fileFormat == THdfsFileFormat.KUDU) {
      if (AcidUtils.isTransactionalTable(options_.tblProperties)) {
        throw new AnalysisException("Kudu tables cannot be transactional");
      }
      return;
    }

    if (options_.fileFormat == THdfsFileFormat.ICEBERG) {
      if (AcidUtils.isTransactionalTable(options_.tblProperties)) {
        throw new AnalysisException(
            "Iceberg tables cannot have Hive ACID table properties.");
      }
      return;
    }

    AcidUtils.setTransactionalProperties(options_.tblProperties,
          analyzer.getQueryOptions().getDefault_transactional_type());
  }
}
