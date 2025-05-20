/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.impala.analysis.paimon;

import com.google.common.collect.Sets;

import org.apache.impala.analysis.AnalysisUtils;
import org.apache.impala.analysis.Analyzer;
import org.apache.impala.analysis.ColumnDef;
import org.apache.impala.analysis.CreateTableStmt;
import org.apache.impala.analysis.ShowStatsStmt;
import org.apache.impala.catalog.HdfsFileFormat;
import org.apache.impala.catalog.Table;
import org.apache.impala.catalog.paimon.FePaimonTable;
import org.apache.impala.catalog.paimon.ImpalaTypeUtils;
import org.apache.impala.catalog.paimon.PaimonUtil;
import org.apache.impala.common.AnalysisException;
import org.apache.impala.thrift.TBucketType;
import org.apache.impala.thrift.TPaimonCatalog;
import org.apache.impala.thrift.TShowStatsParams;
import org.apache.paimon.CoreOptions;
import org.apache.paimon.options.OptionsUtils;

import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Paimon analyzer utils to perform paimon-related analyze tasks.
 *
 * This class is used to reduce coupling of paimon-related implementation
 * with {@link Analyzer}.
 */
public class PaimonAnalyzer {

  /**
   *  Check Paimon Create Table statement.
   * @param stmt
   * @param analyzer
   *
   */
  public static void analyzeCreateTableStmt(CreateTableStmt stmt, Analyzer analyzer)
      throws AnalysisException {

    // TODO: Not supported for now, try to enable later
    AnalysisUtils.throwIfNotNull(
        stmt.getCachingOp(), "A Paimon table cannot be cached in HDFS.");

    AnalysisUtils.throwIfTrue(
        stmt.getTblProperties().containsKey(CoreOptions.PARTITION.key()),
        "Can't specify 'partition' table property in Paimon DDL, " +
                "use PARTITIONED BY clause instead.");
    AnalysisUtils.throwIfTrue(
        stmt.geTBucketInfo().bucket_type != TBucketType.NONE,
        "CLUSTERED BY clause is not support by PAIMON now, " +
        "use property bucket-key instead.");
    // primary-key and PRIMARY KEY caused should not be both specified
    AnalysisUtils.throwIfTrue(
        stmt.getTblProperties().containsKey(CoreOptions.PRIMARY_KEY.key())
            && !stmt.getPrimaryKeys().isEmpty(),
        "Can't specify both PRIMARY KEY clause and 'primary-key' table property " +
                "in Paimon DDL.");
    analyzePaimonColumns(stmt, analyzer);
    analyzePaimonFormat(stmt, analyzer);
  }

  /**
   *  Check Paimon Show Table Stats statement.
   * @param statsOp
   * @param analyzer
   *
   */
  public static void analyzeShowStatStmt(ShowStatsStmt statsOp, FePaimonTable table,
      Analyzer analyzer) throws AnalysisException {
    TShowStatsParams params = statsOp.toThrift();
    switch (params.getOp()) {
      case TABLE_STATS:
      case COLUMN_STATS: return;
      case PARTITIONS:
        if (!PaimonUtil.hasPartition(table.getPaimonApiTable())) {
          throw new AnalysisException("Table is not partitioned: " + table.getFullName());
        }
        break;
      default:
        throw new AnalysisException(
            statsOp.toSql() + " is not supported for Paimon Table");
    }
  }

  /**
   *  Setup paimon related property.
   */
  private static void putPaimonProperty(CreateTableStmt stmt, String key, String value) {
    stmt.putGeneratedProperty(key, value);
  }

  /**
   *  Check paimon format related setting and update stmt object.
   */
  private static void analyzePaimonFormat(CreateTableStmt stmt, Analyzer analyzer)
      throws AnalysisException {
    Map<String, String> tblProperties = stmt.getTblProperties();
    Map<String, String> paimonTblProperties = OptionsUtils.convertToPropertiesPrefixKey(
        tblProperties, PaimonUtil.PAIMON_PROPERTY_PREFIX);
    boolean isExternal = stmt.isExternal();

    // A managed table cannot have 'external.table.purge' property set
    if (!isExternal
        && Boolean.parseBoolean(tblProperties.get(Table.TBL_PROP_EXTERNAL_TABLE_PURGE))) {
      throw new AnalysisException(String.format("Table property '%s' cannot be set to "
              + "true with a managed paimon table.",
          Table.TBL_PROP_EXTERNAL_TABLE_PURGE));
    }

    // External table with purging is not supported.
    if (stmt.isExternal()
        && Boolean.parseBoolean(
            tblProperties.getOrDefault(Table.TBL_PROP_EXTERNAL_TABLE_PURGE, "false"))) {
      throw new AnalysisException(" External table with purge is not supported.");
    }

    // check storage handler, add storage handler for compatibility.
    String handler = tblProperties.get(PaimonUtil.STORAGE_HANDLER);
    if (handler != null && !handler.equals(PaimonUtil.PAIMON_STORAGE_HANDLER)) {
      throw new AnalysisException("Invalid storage handler "
          + "specified for Paimon format: " + handler);
    }
    stmt.putGeneratedProperty(
        PaimonUtil.STORAGE_HANDLER, PaimonUtil.PAIMON_STORAGE_HANDLER);

    // enable the deletion-vector mode by default if not specified
    if (!paimonTblProperties.containsKey(CoreOptions.DELETION_VECTORS_ENABLED.key())
        && !paimonTblProperties.containsKey(
            CoreOptions.FULL_COMPACTION_DELTA_COMMITS.key())) {
      putPaimonProperty(stmt, CoreOptions.DELETION_VECTORS_ENABLED.key(), "true");
    }

    // check format support
    String fileformat = paimonTblProperties.get(CoreOptions.FILE_FORMAT.key());
    HdfsFileFormat hdfsFileFormat = PaimonUtil.getPaimonFileFormat(fileformat);
    if (fileformat != null && hdfsFileFormat == null) {
      throw new AnalysisException(
          "Unsupported fileformat for Paimon table: " + fileformat);
    }

    // Determine the Paimon catalog being used. The default catalog is HiveCatalog.
    TPaimonCatalog catalog = PaimonUtil.getTPaimonCatalog(tblProperties);
    switch (catalog) {
      case HIVE_CATALOG: validateTableInHiveCatalog(stmt, tblProperties); break;
      case HADOOP_CATALOG: validateTableInHadoopCatalog(stmt, tblProperties); break;
      default:
        throw new AnalysisException(
            String.format("Unknown Paimon catalog type: %s", catalog));
    }

    // HMS will override 'external.table.purge' to 'TRUE' When 'paimon.catalog' is not
    // the Hive Catalog for managed tables.
    if (!isExternal && !catalog.equals(TPaimonCatalog.HIVE_CATALOG)
        && "false".equalsIgnoreCase(
            tblProperties.get(Table.TBL_PROP_EXTERNAL_TABLE_PURGE))) {
      analyzer.addWarning("The table property 'external.table.purge' will be set "
          + "to 'TRUE' on newly created managed Paimon tables.");
    }
  }

  /**
   *  Validate for hive catalog.
   * @param stmt
   * @param tblProperties
   */
  private static void validateTableInHiveCatalog(
      CreateTableStmt stmt, Map<String, String> tblProperties) throws AnalysisException {

    // Check if hadoop catalog related properties are defined in the hive catalog
    if (tblProperties.get(PaimonUtil.PAIMON_HADOOP_CATALOG_LOCATION) != null) {
      throw new AnalysisException(String.format("%s cannot be set for Paimon table "
              + "stored in hive.catalog",
          PaimonUtil.PAIMON_HADOOP_CATALOG_LOCATION));
    }
    if (tblProperties.get(PaimonUtil.PAIMON_TABLE_IDENTIFIER) != null) {
      throw new AnalysisException(String.format("%s cannot be set for Paimon table "
              + "stored in hive.catalog",
          PaimonUtil.PAIMON_TABLE_IDENTIFIER));
    }

    AnalysisUtils.throwIfTrue(stmt.isExternal() && stmt.getLocation() == null,
        "Location must be set for external Paimon table stored in hive catalog");
  }

  /**
   *  Validate for hadoop catalog.
   * @param stmt
   * @param tblProperties
   */
  private static void validateTableInHadoopCatalog(
      CreateTableStmt stmt, Map<String, String> tblProperties) throws AnalysisException {
    String catalogLoc = tblProperties.get(PaimonUtil.PAIMON_HADOOP_CATALOG_LOCATION);
    if (catalogLoc == null || catalogLoc.isEmpty()) {
      throw new AnalysisException(String.format("Table property '%s' is necessary "
              + "for Paimon table with 'hadoop.catalog'.",
          PaimonUtil.PAIMON_HADOOP_CATALOG_LOCATION));
    }

    // Table identifier should be specified for external table
    AnalysisUtils.throwIfTrue(stmt.isExternal()
            && tblProperties.get(PaimonUtil.PAIMON_TABLE_IDENTIFIER) == null,
        String.format(
            "Table property '%s' is necessary for Paimon table with 'hadoop.catalog'.",
            PaimonUtil.PAIMON_TABLE_IDENTIFIER));
  }

  /**
   * Check column type support for the column definitions.
   * @param columnDef
   */
  private static void throwIfColumnTypeIsNotSupported(ColumnDef columnDef)
      throws AnalysisException {
    if (!ImpalaTypeUtils.isSupportedColumnType(columnDef.getType())) {
      throw new AnalysisException("Tables stored by Paimon do not support the column "
          + columnDef.getColName() + " type: " + columnDef.getType().toSql());
    }
  }

  /**
   * Check column definitions of paimon table.
   * @param stmt
   * @param analyzer
   */
  private static void analyzePaimonColumns(CreateTableStmt stmt, Analyzer analyzer)
      throws AnalysisException {
    Set<String> colSets = Sets.newHashSet();

    // Check if the columns definitions are supported
    for (ColumnDef col : stmt.getColumnDefs()) {
      throwIfColumnTypeIsNotSupported(col);
      colSets.add(col.getColName().toLowerCase());
    }

    // Check if the partition columns definitions are supported
    for (ColumnDef col : stmt.getPartitionColumnDefs()) {
      throwIfColumnTypeIsNotSupported(col);
      colSets.add(col.getColName().toLowerCase());
    }

    // Check if primary keys are in the column definitions
    if (stmt.getTblProperties().containsKey(CoreOptions.PRIMARY_KEY.key())) {
      List<String> colNames = PaimonUtil.extractColumnNames(
          stmt.getTblProperties().get(CoreOptions.PRIMARY_KEY.key()));
      for (String col : colNames) {
        AnalysisUtils.throwIfTrue(!colSets.contains(col),
            String.format(
                "Invalid col name %s specified in 'primary-key' table properties.", col));
      }
    }

    // Check if bucket keys are in the column definitions
    if (stmt.getTblProperties().containsKey(CoreOptions.BUCKET_KEY.key())) {
      List<String> parts = PaimonUtil.extractColumnNames(
          stmt.getTblProperties().get(CoreOptions.BUCKET_KEY.key()));
      for (String col : parts) {
        AnalysisUtils.throwIfTrue(!colSets.contains(col),
            String.format(
                "Invalid col name %s specified in 'bucket-key' table properties.", col));
      }
    }

    // Check rule: Managed table does not support inferring schema from underlying paimon
    // table.
    if (stmt.getColumnDefs().isEmpty()) {
      AnalysisUtils.throwIfTrue(!stmt.isExternal(),
          "Managed table does not support inferring schema from underlying paimon table");
    }
  }
}
