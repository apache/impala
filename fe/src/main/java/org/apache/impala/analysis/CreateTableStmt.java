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

import java.util.List;
import java.util.Map;

import org.apache.avro.Schema;
import org.apache.avro.SchemaParseException;
import org.apache.impala.authorization.PrivilegeRequestBuilder;
import org.apache.impala.catalog.KuduTable;
import org.apache.impala.catalog.RowFormat;
import org.apache.impala.common.AnalysisException;
import org.apache.impala.common.ImpalaRuntimeException;
import org.apache.impala.thrift.TCreateTableParams;
import org.apache.impala.thrift.THdfsFileFormat;
import org.apache.impala.thrift.TTableName;
import org.apache.impala.util.AvroSchemaConverter;
import org.apache.impala.util.AvroSchemaParser;
import org.apache.impala.util.AvroSchemaUtils;
import org.apache.impala.util.KuduUtil;
import org.apache.impala.util.MetaStoreUtil;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import com.google.common.primitives.Ints;
import com.google.common.collect.Maps;

/**
 * Represents a CREATE TABLE statement.
 */
public class CreateTableStmt extends StatementBase {

  @VisibleForTesting
  final static String KUDU_STORAGE_HANDLER_ERROR_MESSAGE = "Kudu tables must be"
      + " specified using 'STORED AS KUDU'.";

  /////////////////////////////////////////
  // BEGIN: Members that need to be reset()

  // Table parameters specified in a CREATE TABLE statement
  private final TableDef tableDef_;

  // END: Members that need to be reset()
  /////////////////////////////////////////

  // Table owner. Set during analysis
  private String owner_;

  public CreateTableStmt(TableDef tableDef) {
    Preconditions.checkNotNull(tableDef);
    tableDef_ = tableDef;
  }

  /**
   * Copy c'tor.
   */
  CreateTableStmt(CreateTableStmt other) {
    this(other.tableDef_);
    owner_ = other.owner_;
  }

  @Override
  public void reset() {
    super.reset();
    tableDef_.reset();
  }

  @Override
  public CreateTableStmt clone() { return new CreateTableStmt(this); }

  public String getTbl() { return getTblName().getTbl(); }
  public TableName getTblName() { return tableDef_.getTblName(); }
  public boolean getIfNotExists() { return tableDef_.getIfNotExists(); }
  public List<ColumnDef> getColumnDefs() { return tableDef_.getColumnDefs(); }
  private void setColumnDefs(List<ColumnDef> colDefs) {
    getColumnDefs().clear();
    getColumnDefs().addAll(colDefs);
  }
  public List<ColumnDef> getPrimaryKeyColumnDefs() {
    return tableDef_.getPrimaryKeyColumnDefs();
  }
  public boolean isExternal() { return tableDef_.isExternal(); }
  public List<ColumnDef> getPartitionColumnDefs() {
    return tableDef_.getPartitionColumnDefs();
  }
  public List<KuduPartitionParam> getKuduPartitionParams() {
    return tableDef_.getKuduPartitionParams();
  }
  public List<String> getSortColumns() { return tableDef_.getSortColumns(); }
  public String getComment() { return tableDef_.getComment(); }
  Map<String, String> getTblProperties() { return tableDef_.getTblProperties(); }
  private HdfsCachingOp getCachingOp() { return tableDef_.getCachingOp(); }
  public HdfsUri getLocation() { return tableDef_.getLocation(); }
  Map<String, String> getSerdeProperties() { return tableDef_.getSerdeProperties(); }
  public THdfsFileFormat getFileFormat() { return tableDef_.getFileFormat(); }
  RowFormat getRowFormat() { return tableDef_.getRowFormat(); }
  private String getGeneratedKuduTableName() {
    return tableDef_.getGeneratedKuduTableName();
  }
  private void setGeneratedKuduTableName(String tableName) {
    tableDef_.setGeneratedKuduTableName(tableName);
  }

  // Only exposed for ToSqlUtils. Returns the list of primary keys declared by the user
  // at the table level. Note that primary keys may also be declared in column
  // definitions, those are not included here (they are stored in the ColumnDefs).
  List<String> getTblPrimaryKeyColumnNames() {
    return tableDef_.getPrimaryKeyColumnNames();
  }

  /**
   * Can only be called after analysis, returns the owner of this table (the user from
   * the current session).
   */
  public String getOwner() {
    Preconditions.checkNotNull(owner_);
    return owner_;
  }

  /**
   * Can only be called after analysis, returns the name of the database the table will
   * be created within.
   */
  public String getDb() {
    Preconditions.checkState(isAnalyzed());
    return getTblName().getDb();
  }

  @Override
  public String toSql() { return ToSqlUtils.getCreateTableSql(this); }

  public TCreateTableParams toThrift() {
    TCreateTableParams params = new TCreateTableParams();
    params.setTable_name(new TTableName(getDb(), getTbl()));
    List<org.apache.impala.thrift.TColumn> tColumns = Lists.newArrayList();
    for (ColumnDef col: getColumnDefs()) tColumns.add(col.toThrift());
    params.setColumns(tColumns);
    for (ColumnDef col: getPartitionColumnDefs()) {
      params.addToPartition_columns(col.toThrift());
    }
    params.setOwner(getOwner());
    params.setIs_external(isExternal());
    params.setComment(getComment());
    params.setLocation(getLocation() == null ? null : getLocation().toString());
    if (getCachingOp() != null) params.setCache_op(getCachingOp().toThrift());
    if (getRowFormat() != null) params.setRow_format(getRowFormat().toThrift());
    params.setFile_format(getFileFormat());
    params.setIf_not_exists(getIfNotExists());
    params.setSort_columns(getSortColumns());
    params.setTable_properties(Maps.newHashMap(getTblProperties()));
    if (!getGeneratedKuduTableName().isEmpty()) {
      params.getTable_properties().put(KuduTable.KEY_TABLE_NAME,
          getGeneratedKuduTableName());
    }
    params.setSerde_properties(getSerdeProperties());
    for (KuduPartitionParam d: getKuduPartitionParams()) {
      params.addToPartition_by(d.toThrift());
    }
    for (ColumnDef pkColDef: getPrimaryKeyColumnDefs()) {
      params.addToPrimary_key_column_names(pkColDef.getColName());
    }

    return params;
  }

  @Override
  public void collectTableRefs(List<TableRef> tblRefs) {
    tblRefs.add(new TableRef(tableDef_.getTblName().toPath(), null));
  }

  @Override
  public void analyze(Analyzer analyzer) throws AnalysisException {
    super.analyze(analyzer);
    owner_ = analyzer.getUser().getName();
    tableDef_.analyze(analyzer);
    analyzeKuduFormat(analyzer);
    // Avro tables can have empty column defs because they can infer them from the Avro
    // schema. Likewise for external Kudu tables, the schema can be read from Kudu.
    if (getColumnDefs().isEmpty() && getFileFormat() != THdfsFileFormat.AVRO
        && getFileFormat() != THdfsFileFormat.KUDU) {
      throw new AnalysisException("Table requires at least 1 column");
    }
    if (getFileFormat() == THdfsFileFormat.AVRO) {
      setColumnDefs(analyzeAvroSchema(analyzer));
      if (getColumnDefs().isEmpty()) {
        throw new AnalysisException(
            "An Avro table requires column definitions or an Avro schema.");
      }
      AvroSchemaUtils.setFromSerdeComment(getColumnDefs());
    }
  }

  /**
   * Analyzes the parameters of a CREATE TABLE ... STORED AS KUDU statement. Also checks
   * if Kudu specific properties and parameters are specified for non-Kudu tables.
   */
  private void analyzeKuduFormat(Analyzer analyzer) throws AnalysisException {
    if (getFileFormat() != THdfsFileFormat.KUDU) {
      if (KuduTable.KUDU_STORAGE_HANDLER.equals(
          getTblProperties().get(KuduTable.KEY_STORAGE_HANDLER))) {
        throw new AnalysisException(KUDU_STORAGE_HANDLER_ERROR_MESSAGE);
      }
      AnalysisUtils.throwIfNotEmpty(getKuduPartitionParams(),
          "Only Kudu tables can use the PARTITION BY clause.");
      if (hasPrimaryKey()) {
        throw new AnalysisException("Only Kudu tables can specify a PRIMARY KEY.");
      }
      return;
    }

    analyzeKuduTableProperties(analyzer);
    if (isExternal()) {
      analyzeExternalKuduTableParams(analyzer);
    } else {
      analyzeManagedKuduTableParams(analyzer);
    }
  }

  /**
   * Analyzes and checks table properties which are common to both managed and external
   * Kudu tables.
   */
  private void analyzeKuduTableProperties(Analyzer analyzer) throws AnalysisException {
    if (analyzer.getAuthzConfig().isEnabled()) {
      // Today there is no comprehensive way of enforcing a Sentry authorization policy
      // against tables stored in Kudu. This is why only users with ALL privileges on
      // SERVER may create external Kudu tables or set the master addresses.
      // See IMPALA-4000 for details.
      boolean isExternal = tableDef_.isExternal() ||
          MetaStoreUtil.findTblPropKeyCaseInsensitive(
              getTblProperties(), "EXTERNAL") != null;
      if (getTblProperties().containsKey(KuduTable.KEY_MASTER_HOSTS) || isExternal) {
        String authzServer = analyzer.getAuthzConfig().getServerName();
        Preconditions.checkNotNull(authzServer);
        analyzer.registerPrivReq(new PrivilegeRequestBuilder().onServer(
            authzServer).all().toRequest());
      }
    }

    // Only the Kudu storage handler may be specified for Kudu tables.
    String handler = getTblProperties().get(KuduTable.KEY_STORAGE_HANDLER);
    if (handler != null && !handler.equals(KuduTable.KUDU_STORAGE_HANDLER)) {
      throw new AnalysisException("Invalid storage handler specified for Kudu table: " +
          handler);
    }
    getTblProperties().put(KuduTable.KEY_STORAGE_HANDLER, KuduTable.KUDU_STORAGE_HANDLER);

    String masterHosts = getTblProperties().get(KuduTable.KEY_MASTER_HOSTS);
    if (Strings.isNullOrEmpty(masterHosts)) {
      masterHosts = analyzer.getCatalog().getDefaultKuduMasterHosts();
      if (masterHosts.isEmpty()) {
        throw new AnalysisException(String.format(
            "Table property '%s' is required when the impalad startup flag " +
            "-kudu_master_hosts is not used.", KuduTable.KEY_MASTER_HOSTS));
      }
      getTblProperties().put(KuduTable.KEY_MASTER_HOSTS, masterHosts);
    }

    // TODO: Find out what is creating a directory in HDFS and stop doing that. Kudu
    //       tables shouldn't have HDFS dirs: IMPALA-3570
    AnalysisUtils.throwIfNotNull(getCachingOp(),
        "A Kudu table cannot be cached in HDFS.");
    AnalysisUtils.throwIfNotNull(getLocation(), "LOCATION cannot be specified for a " +
        "Kudu table.");
    AnalysisUtils.throwIfNotEmpty(tableDef_.getPartitionColumnDefs(),
        "PARTITIONED BY cannot be used in Kudu tables.");
  }

  /**
   * Analyzes and checks parameters specified for external Kudu tables.
   */
  private void analyzeExternalKuduTableParams(Analyzer analyzer)
      throws AnalysisException {
    AnalysisUtils.throwIfNull(getTblProperties().get(KuduTable.KEY_TABLE_NAME),
        String.format("Table property %s must be specified when creating " +
            "an external Kudu table.", KuduTable.KEY_TABLE_NAME));
    if (hasPrimaryKey()
        || getTblProperties().containsKey(KuduTable.KEY_KEY_COLUMNS)) {
      throw new AnalysisException("Primary keys cannot be specified for an external " +
          "Kudu table");
    }
    AnalysisUtils.throwIfNotNull(getTblProperties().get(KuduTable.KEY_TABLET_REPLICAS),
        String.format("Table property '%s' cannot be used with an external Kudu table.",
            KuduTable.KEY_TABLET_REPLICAS));
    AnalysisUtils.throwIfNotEmpty(getColumnDefs(),
        "Columns cannot be specified with an external Kudu table.");
    AnalysisUtils.throwIfNotEmpty(getKuduPartitionParams(),
        "PARTITION BY cannot be used with an external Kudu table.");
  }

  /**
   * Analyzes and checks parameters specified for managed Kudu tables.
   */
  private void analyzeManagedKuduTableParams(Analyzer analyzer) throws AnalysisException {
    analyzeManagedKuduTableName();

    // Check column types are valid Kudu types
    for (ColumnDef col: getColumnDefs()) {
      try {
        KuduUtil.fromImpalaType(col.getType());
      } catch (ImpalaRuntimeException e) {
        throw new AnalysisException(String.format(
            "Cannot create table '%s': %s", getTbl(), e.getMessage()));
      }
    }
    AnalysisUtils.throwIfNotNull(getTblProperties().get(KuduTable.KEY_KEY_COLUMNS),
        String.format("PRIMARY KEY must be used instead of the table property '%s'.",
            KuduTable.KEY_KEY_COLUMNS));
    if (!hasPrimaryKey()) {
      throw new AnalysisException("A primary key is required for a Kudu table.");
    }
    String tabletReplicas = getTblProperties().get(KuduTable.KEY_TABLET_REPLICAS);
    if (tabletReplicas != null) {
      Integer r = Ints.tryParse(tabletReplicas);
      if (r == null) {
        throw new AnalysisException(String.format(
            "Table property '%s' must be an integer.", KuduTable.KEY_TABLET_REPLICAS));
      }
      if (r <= 0) {
        throw new AnalysisException("Number of tablet replicas must be greater than " +
            "zero. Given number of replicas is: " + r.toString());
      }
    }

    if (!getKuduPartitionParams().isEmpty()) {
      analyzeKuduPartitionParams(analyzer);
    } else {
      analyzer.addWarning(
          "Unpartitioned Kudu tables are inefficient for large data sizes.");
    }
  }

  /**
   * Generates a Kudu table name based on the target database and table and stores
   * it in TableDef.generatedKuduTableName_. Throws if the Kudu table
   * name was given manually via TBLPROPERTIES.
   */
  private void analyzeManagedKuduTableName() throws AnalysisException {
    AnalysisUtils.throwIfNotNull(getTblProperties().get(KuduTable.KEY_TABLE_NAME),
        String.format("Not allowed to set '%s' manually for managed Kudu tables .",
            KuduTable.KEY_TABLE_NAME));
    setGeneratedKuduTableName(KuduUtil.getDefaultCreateKuduTableName(getDb(), getTbl()));
  }

  /**
   * Analyzes the partitioning schemes specified in the CREATE TABLE statement.
   */
  private void analyzeKuduPartitionParams(Analyzer analyzer) throws AnalysisException {
    Preconditions.checkState(getFileFormat() == THdfsFileFormat.KUDU);
    Map<String, ColumnDef> pkColDefsByName =
        ColumnDef.mapByColumnNames(getPrimaryKeyColumnDefs());
    for (KuduPartitionParam partitionParam: getKuduPartitionParams()) {
      // If no column names were specified in this partitioning scheme, use all the
      // primary key columns.
      if (!partitionParam.hasColumnNames()) {
        partitionParam.setColumnNames(pkColDefsByName.keySet());
      }
      partitionParam.setPkColumnDefMap(pkColDefsByName);
      partitionParam.analyze(analyzer);
    }
  }

  /**
   * Checks if a primary key is specified in a CREATE TABLE stmt. Should only be called
   * after tableDef_ has been analyzed.
   */
  private boolean hasPrimaryKey() {
    Preconditions.checkState(tableDef_.isAnalyzed());
    return !tableDef_.getPrimaryKeyColumnDefs().isEmpty();
  }

  /**
   * Analyzes the Avro schema and compares it with the getColumnDefs() to detect
   * inconsistencies. Returns a list of column descriptors that should be
   * used for creating the table (possibly identical to getColumnDefs()).
   */
  private List<ColumnDef> analyzeAvroSchema(Analyzer analyzer) throws AnalysisException {
    Preconditions.checkState(getFileFormat() == THdfsFileFormat.AVRO);
    // Look for the schema in TBLPROPERTIES and in SERDEPROPERTIES, with latter
    // taking precedence.
    List<Map<String, String>> schemaSearchLocations = Lists.newArrayList();
    schemaSearchLocations.add(getSerdeProperties());
    schemaSearchLocations.add(getTblProperties());
    String avroSchema;
    List<ColumnDef> avroCols; // parsed from avroSchema
    try {
      avroSchema = AvroSchemaUtils.getAvroSchema(schemaSearchLocations);
      if (avroSchema == null) {
        // No Avro schema was explicitly set in the serde or table properties, so infer
        // the Avro schema from the column definitions.
        Schema inferredSchema = AvroSchemaConverter.convertColumnDefs(
            getColumnDefs(), getTblName().toString());
        avroSchema = inferredSchema.toString();
      }
      if (Strings.isNullOrEmpty(avroSchema)) {
        throw new AnalysisException("Avro schema is null or empty: " +
            getTblName().toString());
      }
      avroCols = AvroSchemaParser.parse(avroSchema);
    } catch (SchemaParseException e) {
      throw new AnalysisException(String.format(
          "Error parsing Avro schema for table '%s': %s", getTblName().toString(),
          e.getMessage()));
    }
    Preconditions.checkNotNull(avroCols);

    // Analyze the Avro schema to detect inconsistencies with the getColumnDefs().
    // In case of inconsistencies, the column defs are ignored in favor of the Avro
    // schema for simplicity and, in particular, to enable COMPUTE STATS (IMPALA-1104).
    StringBuilder warning = new StringBuilder();
    List<ColumnDef> reconciledColDefs =
        AvroSchemaUtils.reconcileSchemas(getColumnDefs(), avroCols, warning);
    if (warning.length() > 0) analyzer.addWarning(warning.toString());
    return reconciledColDefs;
  }

  /**
   * Unescapes all values in the property map.
   */
  static void unescapeProperties(Map<String, String> propertyMap) {
    if (propertyMap == null) return;
    for (Map.Entry<String, String> kv : propertyMap.entrySet()) {
      propertyMap.put(kv.getKey(),
          new StringLiteral(kv.getValue()).getUnescapedValue());
    }
  }
}
