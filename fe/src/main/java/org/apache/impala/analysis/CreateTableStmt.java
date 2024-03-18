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
import java.util.List;
import java.util.Map;

import org.apache.avro.Schema;
import org.apache.avro.SchemaParseException;
import org.apache.hadoop.hive.metastore.api.SQLForeignKey;
import org.apache.hadoop.hive.metastore.api.SQLPrimaryKey;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.mr.Catalogs;
import org.apache.impala.authorization.AuthorizationConfig;
import org.apache.impala.catalog.DataSourceTable;
import org.apache.impala.catalog.KuduTable;
import org.apache.impala.catalog.IcebergTable;
import org.apache.impala.catalog.RowFormat;
import org.apache.impala.catalog.Table;
import org.apache.impala.common.AnalysisException;
import org.apache.impala.common.ImpalaRuntimeException;
import org.apache.impala.common.RuntimeEnv;
import org.apache.impala.service.BackendConfig;
import org.apache.impala.thrift.TBucketInfo;
import org.apache.impala.thrift.TCreateTableParams;
import org.apache.impala.thrift.THdfsFileFormat;
import org.apache.impala.thrift.TIcebergCatalog;
import org.apache.impala.thrift.TIcebergFileFormat;
import org.apache.impala.thrift.TIcebergPartitionTransformType;
import org.apache.impala.thrift.TSortingOrder;
import org.apache.impala.thrift.TTableName;
import org.apache.impala.util.AvroSchemaConverter;
import org.apache.impala.util.AvroSchemaParser;
import org.apache.impala.util.AvroSchemaUtils;
import org.apache.impala.util.IcebergUtil;
import org.apache.impala.util.KuduUtil;
import org.apache.impala.util.MetaStoreUtil;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.primitives.Ints;

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

  // Server name needed for privileges. Set during analysis.
  private String serverName_;

  public CreateTableStmt(TableDef tableDef) {
    Preconditions.checkNotNull(tableDef);
    tableDef_ = tableDef;
  }

  /**
   * Copy c'tor.
   */
  public CreateTableStmt(CreateTableStmt other) {
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
  public boolean isPrimaryKeyUnique() { return tableDef_.isPrimaryKeyUnique(); }
  public List<SQLPrimaryKey> getPrimaryKeys() { return tableDef_.getSqlPrimaryKeys(); }
  public List<SQLForeignKey> getForeignKeys() { return tableDef_.getSqlForeignKeys(); }
  public boolean isExternal() { return tableDef_.isExternal(); }
  public List<ColumnDef> getPartitionColumnDefs() {
    return tableDef_.getPartitionColumnDefs();
  }
  public List<KuduPartitionParam> getKuduPartitionParams() {
    return tableDef_.getKuduPartitionParams();
  }
  public List<String> getSortColumns() { return tableDef_.getSortColumns(); }
  public TSortingOrder getSortingOrder() { return tableDef_.getSortingOrder(); }
  public String getComment() { return tableDef_.getComment(); }
  public Map<String, String> getTblProperties() { return tableDef_.getTblProperties(); }
  private HdfsCachingOp getCachingOp() { return tableDef_.getCachingOp(); }
  public HdfsUri getLocation() { return tableDef_.getLocation(); }
  Map<String, String> getSerdeProperties() { return tableDef_.getSerdeProperties(); }
  public THdfsFileFormat getFileFormat() { return tableDef_.getFileFormat(); }
  RowFormat getRowFormat() { return tableDef_.getRowFormat(); }
  private void putGeneratedProperty(String key, String value) {
    tableDef_.putGeneratedProperty(key, value);
  }
  public Map<String, String> getGeneratedKuduProperties() {
    return tableDef_.getGeneratedProperties();
  }
  public TBucketInfo geTBucketInfo() { return tableDef_.geTBucketInfo(); }

  // Only exposed for ToSqlUtils. Returns the list of primary keys declared by the user
  // at the table level. Note that primary keys may also be declared in column
  // definitions, those are not included here (they are stored in the ColumnDefs).
  List<String> getTblPrimaryKeyColumnNames() {
    return tableDef_.getPrimaryKeyColumnNames();
  }

  public List<IcebergPartitionSpec> getIcebergPartitionSpecs() {
    return tableDef_.getIcebergPartitionSpecs();
  }

  /**
   * Get foreign keys information as strings. Useful for toSqlUtils.
   * @return List of strings of the form "(col1, col2,..) REFERENCES [pk_db].pk_table
   * (colA, colB,..)".
   */
  List<String> getForeignKeysSql() {
    List<TableDef.ForeignKey> fkList = tableDef_.getForeignKeysList();
    List<String> foreignKeysSql = new ArrayList<>();
    if (fkList != null && !fkList.isEmpty()) {
      for (TableDef.ForeignKey fk : fkList) {
        StringBuilder sb = new StringBuilder("(");
        Joiner.on(", ").appendTo(sb, fk.getForeignKeyColNames()).append(")");
        sb.append(" REFERENCES ");
        sb.append(fk.getFullyQualifiedPkTableName() + "(");
        Joiner.on(", ").appendTo(sb, fk.getPrimaryKeyColNames()).append(")");
        foreignKeysSql.add(sb.toString());
      }
    }
    return foreignKeysSql;
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
  public String toSql(ToSqlOptions options) {
    return ToSqlUtils.getCreateTableSql(this);
  }

  public TCreateTableParams toThrift() {
    TCreateTableParams params = new TCreateTableParams();
    params.setTable_name(new TTableName(getDb(), getTbl()));
    List<org.apache.impala.thrift.TColumn> tColumns = new ArrayList<>();
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
    if (geTBucketInfo() != null) params.setBucket_info(geTBucketInfo());
    params.setSort_columns(getSortColumns());
    params.setSorting_order(getSortingOrder());
    params.setTable_properties(Maps.newHashMap(getTblProperties()));
    params.getTable_properties().putAll(Maps.newHashMap(getGeneratedKuduProperties()));
    params.setSerde_properties(getSerdeProperties());
    params.setIs_primary_key_unique(isPrimaryKeyUnique());
    for (KuduPartitionParam d: getKuduPartitionParams()) {
      params.addToPartition_by(d.toThrift());
    }
    for (ColumnDef pkColDef: getPrimaryKeyColumnDefs()) {
      params.addToPrimary_key_column_names(pkColDef.getColName());
    }
    for(SQLPrimaryKey pk: getPrimaryKeys()){
      params.addToPrimary_keys(pk);
    }
    for(SQLForeignKey fk: getForeignKeys()){
      params.addToForeign_keys(fk);
    }
    params.setServer_name(serverName_);

    // Create table stmt only have one PartitionSpec
    if (!getIcebergPartitionSpecs().isEmpty()) {
      Preconditions.checkState(getIcebergPartitionSpecs().size() == 1);
      params.setPartition_spec(getIcebergPartitionSpecs().get(0).toThrift());
    }

    return params;
  }

  @Override
  public void collectTableRefs(List<TableRef> tblRefs) {
    tblRefs.add(new TableRef(tableDef_.getTblName().toPath(), null));
    // When foreign keys are specified, we need to add all the tables the foreign keys are
    // referring to.
    for(TableDef.ForeignKey fk: tableDef_.getForeignKeysList()){
      tblRefs.add(new TableRef(fk.getPkTableName().toPath(), null));
    }
  }

  @Override
  public void analyze(Analyzer analyzer) throws AnalysisException {
    super.analyze(analyzer);
    owner_ = analyzer.getUserShortName();
    // Set the servername here if authorization is enabled because analyzer_ is not
    // available in the toThrift() method.
    serverName_ = analyzer.getServerName();
    tableDef_.analyze(analyzer);
    analyzeKuduFormat(analyzer);
    // Avro tables can have empty column defs because they can infer them from the Avro
    // schema. Likewise for external Kudu tables, the schema can be read from Kudu.
    if (getColumnDefs().isEmpty() && getFileFormat() != THdfsFileFormat.AVRO
        && getFileFormat() != THdfsFileFormat.KUDU && getFileFormat() !=
        THdfsFileFormat.ICEBERG) {
      throw new AnalysisException("Table requires at least 1 column");
    }
    if (getRowFormat() != null) {
      String fieldDelimiter = getRowFormat().getFieldDelimiter();
      String lineDelimiter = getRowFormat().getLineDelimiter();
      String escapeChar = getRowFormat().getEscapeChar();
      if (getFileFormat() != THdfsFileFormat.TEXT
          && getFileFormat() != THdfsFileFormat.SEQUENCE_FILE) {
        if (fieldDelimiter != null) {
          analyzer.addWarning("'ROW FORMAT DELIMITED FIELDS TERMINATED BY '"
              + fieldDelimiter + "'' is ignored.");
        }
        if (lineDelimiter != null) {
          analyzer.addWarning("'ROW FORMAT DELIMITED LINES TERMINATED BY '"
              + lineDelimiter + "'' is ignored.");
        }
        if (escapeChar != null) {
          analyzer.addWarning(
              "'ROW FORMAT DELIMITED ESCAPED BY '" + escapeChar + "'' is ignored.");
        }
      }
    }
    if (getFileFormat() == THdfsFileFormat.AVRO) {
      setColumnDefs(analyzeAvroSchema(analyzer));
      if (getColumnDefs().isEmpty()) {
        throw new AnalysisException(
            "An Avro table requires column definitions or an Avro schema.");
      }
      AvroSchemaUtils.setFromSerdeComment(getColumnDefs());
    }

    if (getFileFormat() == THdfsFileFormat.ICEBERG) {
      analyzeIcebergColumns();
      analyzeIcebergFormat(analyzer);
    } else {
      List<IcebergPartitionSpec> iceSpec = tableDef_.getIcebergPartitionSpecs();
      if (iceSpec != null && !iceSpec.isEmpty()) {
        throw new AnalysisException(
            "PARTITIONED BY SPEC is only valid for Iceberg tables.");
      }
    }

    if (getFileFormat() == THdfsFileFormat.JDBC) {
      analyzeJdbcSchema(analyzer);
    }

    // If lineage logging is enabled, compute minimal lineage graph.
    if (BackendConfig.INSTANCE.getComputeLineage() || RuntimeEnv.INSTANCE.isTestEnv()) {
       computeLineageGraph(analyzer);
    }
  }

  /**
   * Computes a minimal column lineage graph for create statement. This will just
   * populate a few fields of the graph including query text. If this is a CTAS,
   * the graph is enhanced during the "insert" phase of CTAS.
   */
  protected void computeLineageGraph(Analyzer analyzer) {
    ColumnLineageGraph graph = analyzer.getColumnLineageGraph();
    graph.computeLineageGraph(new ArrayList(), analyzer);
  }

  /**
   * Analyzes the parameters of a CREATE TABLE ... STORED AS KUDU statement. Also checks
   * if Kudu specific properties and parameters are specified for non-Kudu tables.
   */
  private void analyzeKuduFormat(Analyzer analyzer) throws AnalysisException {
    if (getFileFormat() != THdfsFileFormat.KUDU) {
      String handler = getTblProperties().get(KuduTable.KEY_STORAGE_HANDLER);
      if (KuduTable.isKuduStorageHandler(handler)) {
        throw new AnalysisException(KUDU_STORAGE_HANDLER_ERROR_MESSAGE);
      }
      AnalysisUtils.throwIfNotEmpty(getKuduPartitionParams(),
          "Only Kudu tables can use the PARTITION BY clause.");
      return;
    }

    analyzeKuduTableProperties(analyzer);
    if (isExternalWithNoPurge()) {
      // this is an external table
      analyzeExternalKuduTableParams(analyzer);
    } else {
      // this is either a managed table or an external table with external.table.purge
      // property set to true
      analyzeSynchronizedKuduTableParams(analyzer);
    }
  }

  /**
   * Analyzes and checks table properties which are common to both managed and external
   * Kudu tables.
   */
  private void analyzeKuduTableProperties(Analyzer analyzer) throws AnalysisException {
    String kuduMasters = getKuduMasters(analyzer);
    if (kuduMasters.isEmpty()) {
      throw new AnalysisException(String.format(
          "Table property '%s' is required when the impalad startup flag " +
          "-kudu_master_hosts is not used.", KuduTable.KEY_MASTER_HOSTS));
    }
    putGeneratedProperty(KuduTable.KEY_MASTER_HOSTS, kuduMasters);

    AuthorizationConfig authzConfig = analyzer.getAuthzConfig();
    if (authzConfig.isEnabled()) {
      boolean isExternal = tableDef_.isExternal() ||
          MetaStoreUtil.findTblPropKeyCaseInsensitive(
              getTblProperties(), "EXTERNAL") != null;
      if (isExternal) {
        String externalTableName = getTblProperties().get(KuduTable.KEY_TABLE_NAME);
        AnalysisUtils.throwIfNull(externalTableName,
            String.format("Table property %s must be specified when creating " +
                "an external Kudu table.", KuduTable.KEY_TABLE_NAME));
        List<String> storageUris = getUrisForAuthz(kuduMasters, externalTableName);
        for (String storageUri : storageUris) {
          analyzer.registerPrivReq(builder -> builder
                  .onStorageHandlerUri("kudu", storageUri)
                  .rwstorage().build());
        }
      }

      if (getTblProperties().containsKey(KuduTable.KEY_MASTER_HOSTS)) {
        String authzServer = authzConfig.getServerName();
        Preconditions.checkNotNull(authzServer);
        analyzer.registerPrivReq(builder -> builder.onServer(authzServer).all().build());
      }
    }

    // Only the Kudu storage handler may be specified for Kudu tables.
    String handler = getTblProperties().get(KuduTable.KEY_STORAGE_HANDLER);
    if (handler != null && !KuduTable.isKuduStorageHandler(handler)) {
      throw new AnalysisException("Invalid storage handler specified for Kudu table: " +
          handler);
    }
    putGeneratedProperty(KuduTable.KEY_STORAGE_HANDLER,
        KuduTable.KUDU_STORAGE_HANDLER);

    // TODO: Find out what is creating a directory in HDFS and stop doing that. Kudu
    //       tables shouldn't have HDFS dirs: IMPALA-3570
    AnalysisUtils.throwIfNotNull(getCachingOp(),
        "A Kudu table cannot be cached in HDFS.");
    AnalysisUtils.throwIfNotNull(getLocation(), "LOCATION cannot be specified for a " +
        "Kudu table.");
    AnalysisUtils.throwIfNotEmpty(tableDef_.getPartitionColumnDefs(),
        "PARTITIONED BY cannot be used in Kudu tables.");
    AnalysisUtils.throwIfNotNull(getTblProperties().get(KuduTable.KEY_TABLE_ID),
        String.format("Table property %s should not be specified when creating a " +
            "Kudu table.", KuduTable.KEY_TABLE_ID));
  }

  private List<String> getUrisForAuthz(String kuduMasterAddresses, String kuduTableName) {
    List<String> masterAddresses = Lists.newArrayList(kuduMasterAddresses.split(","));
    List<String> uris = new ArrayList<>();
    for (String masterAddress : masterAddresses) {
      uris.add(masterAddress + "/" + kuduTableName);
    }
    return uris;
  }

  /**
   *  Populates Kudu master addresses either from table property or
   *  the -kudu_master_hosts flag.
   */
  private String getKuduMasters(Analyzer analyzer) {
    String kuduMasters = getTblProperties().get(KuduTable.KEY_MASTER_HOSTS);
    if (Strings.isNullOrEmpty(kuduMasters)) {
      kuduMasters = analyzer.getCatalog().getDefaultKuduMasterHosts();
    }
    return kuduMasters;
  }

  /**
   * Analyzes and checks parameters specified for external Kudu tables.
   */
  private void analyzeExternalKuduTableParams(Analyzer analyzer)
      throws AnalysisException {
    Preconditions.checkState(!Boolean
        .parseBoolean(getTblProperties().get(KuduTable.TBL_PROP_EXTERNAL_TABLE_PURGE)));
    // this is just a regular external table. Kudu table name must be specified
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
   * Analyzes and checks parameters specified for synchronized Kudu tables.
   */
  private void analyzeSynchronizedKuduTableParams(Analyzer analyzer)
      throws AnalysisException {
    // A managed table cannot have 'external.table.purge' property set
    if (!isExternal() && Boolean.parseBoolean(
        getTblProperties().get(KuduTable.TBL_PROP_EXTERNAL_TABLE_PURGE))) {
      throw new AnalysisException(String.format("Table property '%s' cannot be set to " +
          "true with an managed Kudu table.", KuduTable.TBL_PROP_EXTERNAL_TABLE_PURGE));
    }
    analyzeSynchronizedKuduTableName(analyzer);

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
    analyzeKuduPartitionParams(analyzer);
  }

  /**
   * Generates a Kudu table name based on the target database and table and stores
   * it in TableDef.generatedKuduTableName_. Throws if the Kudu table
   * name was given manually via TBLPROPERTIES.
   */
  private void analyzeSynchronizedKuduTableName(Analyzer analyzer)
      throws AnalysisException {
    AnalysisUtils.throwIfNotNull(getTblProperties().get(KuduTable.KEY_TABLE_NAME),
        String.format("Not allowed to set '%s' manually for synchronized Kudu tables.",
            KuduTable.KEY_TABLE_NAME));
    String kuduMasters = getKuduMasters(analyzer);
    boolean isHMSIntegrationEnabled;
    try {
      // Check if Kudu's integration with the Hive Metastore is enabled. Validation
      // of whether Kudu is configured to use the same Hive Metstore as Impala is skipped
      // and is not necessary for syntax parsing.
      isHMSIntegrationEnabled = KuduTable.isHMSIntegrationEnabled(kuduMasters);
    } catch (ImpalaRuntimeException e) {
      throw new AnalysisException(String.format("Cannot analyze Kudu table '%s': %s",
          getTbl(), e.getMessage()));
    }
    putGeneratedProperty(KuduTable.KEY_TABLE_NAME,
        KuduUtil.getDefaultKuduTableName(getDb(), getTbl(), isHMSIntegrationEnabled));
  }

  /**
   * Analyzes the partitioning schemes specified in the CREATE TABLE statement. Also,
   * adds primary keys to the partitioning scheme if no partitioning keys are provided
   */
  private void analyzeKuduPartitionParams(Analyzer analyzer) throws AnalysisException {
    Preconditions.checkState(getFileFormat() == THdfsFileFormat.KUDU);
    if (getKuduPartitionParams().isEmpty()) {
      analyzer.addWarning(
          "Unpartitioned Kudu tables are inefficient for large data sizes.");
      return;
    }
    Map<String, ColumnDef> pkColDefsByName =
        ColumnDef.mapByColumnNames(getPrimaryKeyColumnDefs());
    for (KuduPartitionParam partitionParam: getKuduPartitionParams()) {
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
    List<Map<String, String>> schemaSearchLocations = new ArrayList<>();
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

  /**
   * For iceberg file format, add related storage handler
   */
  private void analyzeIcebergFormat(Analyzer analyzer) throws AnalysisException {
    // A managed table cannot have 'external.table.purge' property set
    if (!isExternal() && Boolean.parseBoolean(
        getTblProperties().get(IcebergTable.TBL_PROP_EXTERNAL_TABLE_PURGE))) {
      throw new AnalysisException(String.format("Table property '%s' cannot be set to " +
          "true with a managed Iceberg table.",
          IcebergTable.TBL_PROP_EXTERNAL_TABLE_PURGE));
    }

    // Check for managed table
    if (!isExternal() || Boolean.parseBoolean(getTblProperties().get(
        Table.TBL_PROP_EXTERNAL_TABLE_PURGE))) {
      if (getColumnDefs().isEmpty()) {
        // External iceberg table can have empty column, but managed iceberg table
        // requires at least one column.
        throw new AnalysisException("Table requires at least 1 column for " +
            "managed iceberg table.");
      }

      // Check partition columns for managed iceberg table
      checkPartitionColumns(analyzer);
    }

    String handler = getTblProperties().get(IcebergTable.KEY_STORAGE_HANDLER);
    if (handler != null && !IcebergTable.isIcebergStorageHandler(handler)) {
      throw new AnalysisException("Invalid storage handler " +
          "specified for Iceberg format: " + handler);
    }
    putGeneratedProperty(IcebergTable.KEY_STORAGE_HANDLER,
        IcebergTable.ICEBERG_STORAGE_HANDLER);
    putGeneratedProperty(TableProperties.ENGINE_HIVE_ENABLED, "true");
    addMergeOnReadPropertiesIfNeeded();

    String fileformat = getTblProperties().get(IcebergTable.ICEBERG_FILE_FORMAT);
    TIcebergFileFormat icebergFileFormat = IcebergUtil.getIcebergFileFormat(fileformat);
    if (fileformat != null && icebergFileFormat == null) {
      throw new AnalysisException("Invalid fileformat for Iceberg table: " + fileformat);
    }
    if (fileformat == null || fileformat.isEmpty()) {
      putGeneratedProperty(IcebergTable.ICEBERG_FILE_FORMAT, "parquet");
    }

    validateIcebergParquetCompressionCodec(icebergFileFormat);
    validateIcebergParquetRowGroupSize(icebergFileFormat);
    validateIcebergParquetPageSize(icebergFileFormat,
        IcebergTable.PARQUET_PLAIN_PAGE_SIZE, "page size");
    validateIcebergParquetPageSize(icebergFileFormat,
        IcebergTable.PARQUET_DICT_PAGE_SIZE, "dictionary page size");

    // Determine the Iceberg catalog being used. The default catalog is HiveCatalog.
    String catalogStr = getTblProperties().get(IcebergTable.ICEBERG_CATALOG);
    TIcebergCatalog catalog;
    if (catalogStr == null || catalogStr.isEmpty()) {
      catalog = TIcebergCatalog.HIVE_CATALOG;
    } else {
      catalog = IcebergUtil.getTIcebergCatalog(catalogStr);
    }
    validateIcebergTableProperties(catalog);
  }

  /**
   * When creating an Iceberg table that supports row-level modifications
   * (format-version >= 2) we set write modes to "merge-on-read" which is the write
   * mode Impala will eventually support (IMPALA-11664).
   */
  private void addMergeOnReadPropertiesIfNeeded() {
    Map<String, String> tblProps = getTblProperties();
    String formatVersion = tblProps.get(TableProperties.FORMAT_VERSION);
    if (formatVersion == null ||
        Integer.valueOf(formatVersion) < IcebergTable.ICEBERG_FORMAT_V2) {
      return;
    }

    // Only add "merge-on-read" if none of the write modes are specified.
    final String MERGE_ON_READ = IcebergTable.MERGE_ON_READ;
    if (!IcebergUtil.isAnyWriteModeSet(tblProps)) {
      putGeneratedProperty(TableProperties.DELETE_MODE, MERGE_ON_READ);
      putGeneratedProperty(TableProperties.UPDATE_MODE, MERGE_ON_READ);
      putGeneratedProperty(TableProperties.MERGE_MODE, MERGE_ON_READ);
    }
  }

  private void validateIcebergParquetCompressionCodec(
      TIcebergFileFormat icebergFileFormat) throws AnalysisException {
    if (icebergFileFormat != TIcebergFileFormat.PARQUET) {
      if (getTblProperties().containsKey(IcebergTable.PARQUET_COMPRESSION_CODEC)) {
          throw new AnalysisException(IcebergTable.PARQUET_COMPRESSION_CODEC +
              " should be set only for parquet file format");
      }
      if (getTblProperties().containsKey(IcebergTable.PARQUET_COMPRESSION_LEVEL)) {
          throw new AnalysisException(IcebergTable.PARQUET_COMPRESSION_LEVEL +
              " should be set only for parquet file format");
      }
    } else {
      StringBuilder errMsg = new StringBuilder();
      if (IcebergUtil.parseParquetCompressionCodec(true, getTblProperties(), errMsg)
          == null) {
        throw new AnalysisException(errMsg.toString());
      }
    }
  }

  private void validateIcebergParquetRowGroupSize(TIcebergFileFormat icebergFileFormat)
      throws AnalysisException {
    if (getTblProperties().containsKey(IcebergTable.PARQUET_ROW_GROUP_SIZE)) {
      if (icebergFileFormat != TIcebergFileFormat.PARQUET) {
        throw new AnalysisException(IcebergTable.PARQUET_ROW_GROUP_SIZE +
            " should be set only for parquet file format");
      }
    }

    StringBuilder errMsg = new StringBuilder();
    if (IcebergUtil.parseParquetRowGroupSize(getTblProperties(), errMsg) == null) {
      throw new AnalysisException(errMsg.toString());
    }
  }

  private void validateIcebergParquetPageSize(TIcebergFileFormat icebergFileFormat,
      String pageSizeTblProp, String descr) throws AnalysisException {
    if (getTblProperties().containsKey(pageSizeTblProp)) {
      if (icebergFileFormat != TIcebergFileFormat.PARQUET) {
        throw new AnalysisException(pageSizeTblProp +
            " should be set only for parquet file format");
      }
    }

    StringBuilder errMsg = new StringBuilder();
    if (IcebergUtil.parseParquetPageSize(getTblProperties(), pageSizeTblProp, descr,
        errMsg) == null) {
      throw new AnalysisException(errMsg.toString());
    }
  }

  private void validateIcebergTableProperties(TIcebergCatalog catalog)
      throws AnalysisException {
    // Metadata location is only used by HiveCatalog, but we shouldn't allow setting this
    // for any catalogs to avoid confusion.
    if (getTblProperties().get(IcebergTable.METADATA_LOCATION) != null) {
      throw new AnalysisException(String.format("%s cannot be set for Iceberg tables",
          IcebergTable.METADATA_LOCATION));
    }
    switch(catalog) {
      case HIVE_CATALOG: validateTableInHiveCatalog();
      break;
      case HADOOP_CATALOG: validateTableInHadoopCatalog();
      break;
      case HADOOP_TABLES: validateTableInHadoopTables();
      break;
      case CATALOGS: validateTableInCatalogs();
      break;
      default: throw new AnalysisException(String.format(
          "Unknown Iceberg catalog type: %s", catalog));
    }
    // HMS will override 'external.table.purge' to 'TRUE' When 'iceberg.catalog' is not
    // the Hive Catalog for managed tables.
    if (!isExternal() && !IcebergUtil.isHiveCatalog(getTblProperties())
        && "false".equalsIgnoreCase(getTblProperties().get(
            Table.TBL_PROP_EXTERNAL_TABLE_PURGE))) {
      analyzer_.addWarning("The table property 'external.table.purge' will be set "
          + "to 'TRUE' on newly created managed Iceberg tables.");
    }
    if (isExternalWithNoPurge() && IcebergUtil.isHiveCatalog(getTblProperties())) {
        throw new AnalysisException("Cannot create EXTERNAL Iceberg table in the " +
            "Hive Catalog.");
    }
  }

  private void validateTableInHiveCatalog() throws AnalysisException {
    if (getTblProperties().get(IcebergTable.ICEBERG_CATALOG_LOCATION) != null) {
      throw new AnalysisException(String.format("%s cannot be set for Iceberg table " +
          "stored in hive.catalog", IcebergTable.ICEBERG_CATALOG_LOCATION));
    }
  }

  private void validateTableInHadoopCatalog() throws AnalysisException {
    // Table location cannot be set in SQL when using 'hadoop.catalog'
    if (getLocation() != null) {
      throw new AnalysisException(String.format("Location cannot be set for Iceberg " +
          "table with 'hadoop.catalog'."));
    }
    String catalogLoc = getTblProperties().get(IcebergTable.ICEBERG_CATALOG_LOCATION);
    if (catalogLoc == null || catalogLoc.isEmpty()) {
      throw new AnalysisException(String.format("Table property '%s' is necessary " +
          "for Iceberg table with 'hadoop.catalog'.",
          IcebergTable.ICEBERG_CATALOG_LOCATION));
    }
  }

  private void validateTableInHadoopTables() throws AnalysisException {
    if (getTblProperties().get(IcebergTable.ICEBERG_CATALOG_LOCATION) != null) {
      throw new AnalysisException(String.format("%s cannot be set for Iceberg table " +
          "stored in hadoop.tables", IcebergTable.ICEBERG_CATALOG_LOCATION));
    }
    if (isExternalWithNoPurge() && getLocation() == null) {
      throw new AnalysisException("Set LOCATION for external Iceberg tables " +
          "stored in hadoop.tables. For creating a completely new Iceberg table, use " +
          "'CREATE TABLE' (no EXTERNAL keyword).");
    }
  }

  private void validateTableInCatalogs() {
    String tableId = getTblProperties().get(IcebergTable.ICEBERG_TABLE_IDENTIFIER);
    if (tableId != null && !tableId.isEmpty()) {
      putGeneratedProperty(Catalogs.NAME, tableId);
    }
  }

  /**
   * For iceberg table, partition column must be from source column
   */
  private void checkPartitionColumns(Analyzer analyzer) throws AnalysisException {
    // This check is unnecessary for iceberg table without partition spec
    List<IcebergPartitionSpec> specs = tableDef_.getIcebergPartitionSpecs();
    if (specs == null || specs.isEmpty()) return;

    // Iceberg table only has one partition spec now
    IcebergPartitionSpec spec = specs.get(0);
    // Analyzes the partition spec and the underlying partition fields.
    spec.analyze(analyzer);

    List<IcebergPartitionField> fields = spec.getIcebergPartitionFields();
    Preconditions.checkState(fields != null && !fields.isEmpty());
    for (IcebergPartitionField field : fields) {
      String fieldName = field.getFieldName();
      boolean containFlag = false;
      for (ColumnDef columnDef : tableDef_.getColumnDefs()) {
        if (columnDef.getColName().equalsIgnoreCase(fieldName)) {
          containFlag = true;
          break;
        }
      }
      if (!containFlag) {
        throw new AnalysisException("Cannot find source column: " + fieldName);
      }
    }
  }

  /**
   * Set column's nullable as true for default situation, so we can create optional
   * Iceberg field
   */
  private void analyzeIcebergColumns() {
    if (!getPartitionColumnDefs().isEmpty()) {
      createIcebergPartitionSpecFromPartitionColumns();
    }
    for (ColumnDef def : getColumnDefs()) {
      if (!def.isNullabilitySet()) {
        def.setNullable(true);
      }
    }
  }

  /**
   * Creates Iceberg partition spec from partition columns. Needed to support old-style
   * CREATE TABLE .. PARTITIONED BY (<cols>) syntax. In this case the column list in
   * 'cols' is appended to the table-level columns, but also Iceberg-level IDENTITY
   * partitions are created from this list.
   */
  private void createIcebergPartitionSpecFromPartitionColumns() {
    Preconditions.checkState(!getPartitionColumnDefs().isEmpty());
    Preconditions.checkState(getIcebergPartitionSpecs().isEmpty());
    List<IcebergPartitionField> partFields = new ArrayList<>();
    for (ColumnDef colDef : getPartitionColumnDefs()) {
      partFields.add(new IcebergPartitionField(colDef.getColName(),
          new IcebergPartitionTransform(TIcebergPartitionTransformType.IDENTITY)));
    }
    getIcebergPartitionSpecs().add(new IcebergPartitionSpec(partFields));
    getColumnDefs().addAll(getPartitionColumnDefs());
    getPartitionColumnDefs().clear();
  }

  /**
   * Analyzes the parameters of a CREATE TABLE ... STORED BY JDBC statement. Adds the
   * table properties of DataSource so that JDBC table is stored as DataSourceTable in
   * HMS.
   */
  private void analyzeJdbcSchema(Analyzer analyzer) throws AnalysisException {
    if (!isExternal()) {
      throw new AnalysisException("JDBC table must be created as external table");
    }
    for (ColumnDef col: getColumnDefs()) {
      if (!DataSourceTable.isSupportedColumnType(col.getType())) {
        throw new AnalysisException("Tables stored by JDBC do not support the column " +
            "type: " + col.getType());
      }
    }

    AnalysisUtils.throwIfNotNull(getCachingOp(),
        "A JDBC table cannot be cached in HDFS.");
    AnalysisUtils.throwIfNotNull(getLocation(), "LOCATION cannot be specified for a " +
        "JDBC table.");
    AnalysisUtils.throwIfNotEmpty(tableDef_.getPartitionColumnDefs(),
        "PARTITIONED BY cannot be used in a JDBC table.");

    // Set table properties of the DataSource to make the table saved as DataSourceTable
    // in HMS.
    try {
      DataSourceTable.setJdbcDataSourceProperties(getTblProperties());
    } catch (ImpalaRuntimeException e) {
      throw new AnalysisException(String.format(
          "Cannot create table '%s': %s", getTbl(), e.getMessage()));
    }
  }

  /**
   * @return true for external tables that don't have "external.table.purge" set to true.
   */
  private boolean isExternalWithNoPurge() {
    return isExternal() && !Boolean.parseBoolean(getTblProperties().get(
      Table.TBL_PROP_EXTERNAL_TABLE_PURGE));
  }
}
