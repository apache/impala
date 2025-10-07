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
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.stream.Collectors;

import org.antlr.runtime.ANTLRStringStream;
import org.antlr.runtime.RecognitionException;
import org.antlr.runtime.Token;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.ObjectUtils;
import org.apache.commons.lang3.StringEscapeUtils;
import org.apache.hadoop.hive.common.StatsSetupConst;
import org.apache.hadoop.hive.ql.parse.HiveLexer;
import org.apache.iceberg.TableProperties;
import org.apache.impala.catalog.CatalogException;
import org.apache.impala.catalog.Column;
import org.apache.impala.catalog.ColumnStats;
import org.apache.impala.catalog.DataSourceTable;
import org.apache.impala.catalog.FeDataSourceTable;
import org.apache.impala.catalog.FeFsPartition;
import org.apache.impala.catalog.FeFsTable;
import org.apache.impala.catalog.FeHBaseTable;
import org.apache.impala.catalog.FeIcebergTable;
import org.apache.impala.catalog.FeKuduTable;
import org.apache.impala.catalog.FeTable;
import org.apache.impala.catalog.FeView;
import org.apache.impala.catalog.Function;
import org.apache.impala.catalog.HdfsCompression;
import org.apache.impala.catalog.HdfsFileFormat;
import org.apache.impala.catalog.HdfsPartition;
import org.apache.impala.catalog.IcebergColumn;
import org.apache.impala.catalog.IcebergTable;
import org.apache.impala.catalog.KuduColumn;
import org.apache.impala.catalog.KuduTable;
import org.apache.impala.catalog.PrunablePartition;
import org.apache.impala.catalog.RowFormat;
import org.apache.impala.catalog.Table;
import org.apache.impala.catalog.paimon.FePaimonTable;
import org.apache.impala.catalog.paimon.PaimonUtil;
import org.apache.impala.common.Pair;
import org.apache.impala.service.CatalogOpExecutor;
import org.apache.impala.thrift.TBucketInfo;
import org.apache.impala.thrift.TBucketType;
import org.apache.impala.thrift.TIcebergCatalog;
import org.apache.impala.thrift.TSortingOrder;
import org.apache.impala.util.AcidUtils;
import org.apache.impala.util.BucketUtils;
import org.apache.impala.util.IcebergUtil;
import org.apache.impala.util.KuduUtil;
import org.apache.paimon.CoreOptions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.base.Splitter;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

/**
 * Contains utility methods for creating SQL strings, for example,
 * for creating identifier strings that are compatible with Hive or Impala.
 */
public class ToSqlUtils {
  private final static Logger LOG = LoggerFactory.getLogger(ToSqlUtils.class);

  // Table properties to hide when generating the toSql() statement
  // EXTERNAL, SORT BY [order], and comment are hidden because they are part of the
  // toSql result, e.g.,
  // "CREATE EXTERNAL TABLE <name> ... SORT BY ZORDER (...) ... COMMENT <comment> ..."
  @VisibleForTesting
  protected static final ImmutableSet<String> HIDDEN_TABLE_PROPERTIES = ImmutableSet.of(
      "EXTERNAL",
      "comment",
      AlterTableSortByStmt.TBL_PROP_SORT_COLUMNS,
      AlterTableSortByStmt.TBL_PROP_SORT_ORDER,
      FeFsTable.NUM_ERASURE_CODED_FILES,
      FeFsTable.NUM_FILES,
      FeFsTable.TOTAL_SIZE,
      FeTable.CATALOG_SERVICE_ID,
      FeTable.CATALOG_VERSION,
      FeTable.LAST_MODIFIED_BY,
      FeTable.LAST_MODIFIED_TIME,
      FeTable.NUM_ROWS);

  // Internal Iceberg metadata table properties to remove from iceberg table
  @VisibleForTesting
  protected static final ImmutableSet<String> HIDDEN_ICEBERG_TABLE_PROPERTIES =
    ImmutableSet.of(
      IcebergTable.KEY_STORAGE_HANDLER,
      IcebergTable.METADATA_LOCATION,
      IcebergTable.PREVIOUS_METADATA_LOCATION,
      IcebergTable.CURRENT_SCHEMA,
      IcebergTable.SNAPSHOT_COUNT,
      IcebergTable.CURRENT_SNAPSHOT_ID,
      IcebergTable.CURRENT_SNAPSHOT_SUMMARY,
      IcebergTable.CURRENT_SNAPSHOT_TIMESTAMP_MS,
      IcebergTable.DEFAULT_PARTITION_SPEC,
      IcebergTable.UUID
    );

  /**
   * Removes all hidden properties from the given 'tblProperties' map.
   */
  @VisibleForTesting
  protected static void removeHiddenTableProperties(Map<String, String> tblProperties) {
    for (String key: HIDDEN_TABLE_PROPERTIES) tblProperties.remove(key);
  }

  /**
   * Removes all hidden Iceberg table properties from the given 'tblProperties' map.
   */
  @VisibleForTesting
  protected static void removeHiddenIcebergTableProperties(
      Map<String, String> tblProperties) {
    for (String key: HIDDEN_ICEBERG_TABLE_PROPERTIES) tblProperties.remove(key);
  }

  /**
   * Removes all hidden Kudu from the given 'tblProperties' map.
   */
  @VisibleForTesting
  protected static void removeHiddenKuduTableProperties(
      Map<String, String> tblProperties) {
    tblProperties.remove(KuduTable.KEY_TABLE_NAME);
  }

  /**
   * Centralized filtering/masking of table properties for output. Applies a common
   * baseline of removals and then table-format specific rules.
   * Populates two output maps: one for CREATE TABLE (more filtered) and one for
   * ALTER TABLE SET TBLPROPERTIES (less filtered, keeps more properties).
   *
   * @param rawProps The raw HMS properties to filter
   * @param table The table
   * @param msTbl The metastore table
   * @param createTableProps Output map for CREATE TABLE properties (more filtered)
   * @param alterTableProps Output map for ALTER TABLE properties (less filtered)
   */
  private static void filterTblProperties(Map<String, String> rawProps, FeTable table,
      org.apache.hadoop.hive.metastore.api.Table msTbl,
      Map<String, String> createTableProps, Map<String, String> alterTableProps) {
    if (rawProps == null || rawProps.isEmpty()) return;

    // Start with all properties in a single map
    Map<String, String> commonProps = Maps.newLinkedHashMap();
    commonProps.putAll(rawProps);

    // Common internal property we never show in either output
    commonProps.remove(StatsSetupConst.DO_NOT_UPDATE_STATS);

    // Table-format specific filtering (applies to both maps)
    if (table instanceof FeKuduTable) {
      // Remove storage handler and internal ids
      commonProps.remove(KuduTable.KEY_STORAGE_HANDLER);
      String kuduTableName = rawProps.get(KuduTable.KEY_TABLE_NAME);
      if (kuduTableName != null &&
        KuduUtil.isDefaultKuduTableName(kuduTableName,
          table.getDb().getName(), table.getName())) {
        commonProps.remove(KuduTable.KEY_TABLE_NAME);
      }
      commonProps.remove(KuduTable.KEY_TABLE_ID);
    } else if (table instanceof FeIcebergTable) {
      FeIcebergTable feIcebergTable = (FeIcebergTable) table;
      // Add "format-version" property if it's not already present.
      commonProps.putIfAbsent(IcebergTable.FORMAT_VERSION,
          Integer.toString(feIcebergTable.getFormatVersion()));

      // Hide Iceberg internal metadata properties
      removeHiddenIcebergTableProperties(commonProps);
    } else if (table instanceof FePaimonTable) {
      // Hide Paimon internals
      commonProps.remove(CoreOptions.PRIMARY_KEY.key());
      commonProps.remove(CoreOptions.PARTITION.key());
      commonProps.remove(PaimonUtil.STORAGE_HANDLER);
      commonProps.remove(CatalogOpExecutor.CAPABILITIES_KEY);
      if (msTbl != null && PaimonUtil.isSynchronizedTable(msTbl)) {
        commonProps.remove("TRANSLATED_TO_EXTERNAL");
        commonProps.remove(Table.TBL_PROP_EXTERNAL_TABLE_PURGE);
      }
    } else if (table instanceof FeDataSourceTable) {
      // Mask external JDBC sensitive properties (case-insensitively)
      Set<String> keysToBeMasked = DataSourceTable.getJdbcTblPropertyMaskKeys();
      for (String key : keysToBeMasked) {
        if (commonProps.containsKey(key)) commonProps.put(key, "******");
        String lower = key.toLowerCase();
        if (commonProps.containsKey(lower)) commonProps.put(lower, "******");
      }
    }

    // Duplicate the common properties into alterTableProps (less filtered)
    alterTableProps.putAll(commonProps);

    // Duplicate into createTableProps and remove additional properties
    // that are materialized elsewhere in CREATE TABLE DDL
    createTableProps.putAll(commonProps);
    removeHiddenTableProperties(createTableProps);
  }

  /**
   * Returns the list of sort columns from 'properties' or 'null' if 'properties' doesn't
   * contain 'sort.columns'.
   */
  @VisibleForTesting
  protected static List<String> getSortColumns(Map<String, String> properties) {
    String sortByKey = AlterTableSortByStmt.TBL_PROP_SORT_COLUMNS;
    if (!properties.containsKey(sortByKey)) return null;
    return Lists.newArrayList(Splitter.on(",").trimResults().omitEmptyStrings().split(
        properties.get(sortByKey)));
  }

  /**
   * Returns the sorting order from 'properties' or the default value (lexicographic
   * ordering), if 'properties' doesn't contain 'sort.order'.
   */
  @VisibleForTesting
  protected static String getSortingOrder(Map<String, String> properties) {
    String sortOrderKey = AlterTableSortByStmt.TBL_PROP_SORT_ORDER;
    if (!properties.containsKey(sortOrderKey)) return TSortingOrder.LEXICAL.toString();
    return properties.get(sortOrderKey);
  }

  /**
   * Returns a comma-delimited string of Kudu 'partition by' parameters on a
   * CreateTableStmt, or null if this isn't a CreateTableStmt for a Kudu table.
   */
  private static String getKuduPartitionByParams(CreateTableStmt stmt) {
    List<KuduPartitionParam> partitionParams = stmt.getKuduPartitionParams();
    Preconditions.checkNotNull(partitionParams);
    if (partitionParams.isEmpty()) return null;
    List<String> paramStrings = new ArrayList<>();
    for (KuduPartitionParam p : partitionParams) {
      paramStrings.add(p.toSql());
    }
    return Joiner.on(", ").join(paramStrings);
  }

  /**
   * Check if a column (or table) name will be parsed by Hive as an identifier.
   * If not, then the identifier must be quoted.
   * @param ident name to check
   * @return true if the name must be quoted for Hive, false if the
   * name is a valid identifier and so needs no quoting
   */
  public static boolean hiveNeedsQuotes(String ident) {
    // Lexer catches only upper-case keywords: "SELECT", but not "select".
    // So, do the check on an upper-case version of the identifier.
    // Hive uses ANTLRNoCaseStringStream to upper-case text, but that
    // class is a non-static inner class so we can't use it here.
    // Overrides HiveLexer to print error messages to TRACE level logs (see IMPALA-9921).
    HiveLexer hiveLexer = new HiveLexer(new ANTLRStringStream(ident.toUpperCase())) {
      @Override
      public void displayRecognitionError(String[] tokenNames, RecognitionException e) {
        if (LOG.isTraceEnabled()) {
          LOG.trace("Error in checking needsQuotes using HiveLexer {}: {}",
              getErrorHeader(e), getErrorMessage(e, tokenNames));
        }
      }
    };
    try {
      Token t = hiveLexer.nextToken();
      // Check that the lexer recognizes an identifier and then EOF.
      // Not an identifier? Needs quotes.
      if (t.getType() != HiveLexer.Identifier) return true;
      // Not a single identifier? Needs quotes.
      t = hiveLexer.nextToken();
      return t.getType() != HiveLexer.EOF;
    } catch (Exception e) {
      // Ignore exception and just quote the identifier to be safe.
      return true;
    }
  }

  /**
   * Determines if an identifier must be quoted for Impala. This is a very
   * weak test, it works only for simple identifiers. Use this in conjunction
   * with {@link #hiveNeedsQuotes} for a complete check.
   * @param ident the identifier to check
   * @return true if the identifier is an Impala keyword, or if starts
   * with a digit
   */
  public static boolean impalaNeedsQuotes(String ident) {
    return SqlScanner.isReserved(ident) ||
      // Quote numbers to avoid odd cases.
      // SELECT id AS 3a3 FROM functional.alltypestiny
      // is valid, but
      // SELECT id AS 3e3 FROM functional.alltypestiny
      // Is not. The "e" changes the meaning from identifier to number.
      Character.isDigit(ident.charAt(0)) ||
      // The parser-based checks fail if the identifier contains a comment
      // character: the parser ignores those characters and the rest of
      // the identifier. Treat them specially.
      ident.contains("#") || ident.contains("--");
  }

  /**
   * Given an unquoted identifier string, returns an identifier lexable by
   * Impala and Hive, possibly by enclosing the original identifier in "`" quotes.
   * For example, Hive cannot parse its own auto-generated column
   * names "_c0", "_c1" etc. unless they are quoted. Impala and Hive keywords
   * must also be quoted.
   *
   * The Impala and Hive lexical analyzers recognize a mostly-overlapping,
   * but sometimes distinct set of keywords. Impala further imposes certain
   * syntactic rules around identifiers that start with digits. To ensure
   * that views generated by Impala are readable both both Impala and Hive,
   * we quote names which are either Hive keywords, Impala keywords, or
   * are ambiguous in Impala.
   *
   * The wildcard ("*") is never quoted though it is not an identifier.
   */
  public static String getIdentSql(String ident) {
    // Don't quote the wildcard used in SELECT *.
    if (ident.equals("*")) return ident;
    return hiveNeedsQuotes(ident) || impalaNeedsQuotes(ident)
        ? "`" + ident + "`" : ident;
  }

  /**
   * Test case version of {@link #getIdentSql(String)}, with
   * special handling for the wildcard and multi-part names.
   * For creating generic expected values in tests.
   */
  public static String identSql(String ident) {
    List<String> parts = new ArrayList<>();
    for (String part : Splitter.on('.').split(ident)) {
      parts.add(ident.equals("*") ? part : getIdentSql(part));
    }
    return Joiner.on('.').join(parts);
  }

  public static List<String> getIdentSqlList(List<String> identList) {
    List<String> identSqlList = new ArrayList<>();
    for (String ident: identList) {
      identSqlList.add(getIdentSql(ident));
    }
    return identSqlList;
  }

  public static String getPathSql(List<String> path) {
    StringBuilder result = new StringBuilder();
    for (String p: path) {
      if (result.length() > 0) result.append(".");
      result.append(getIdentSql(p));
    }
    return result.toString();
  }

  /**
   * Gets the single IcebergPartitionSpec from a CreateTableStmt and returns its
   * toSql(). If there is no IcebergPartitionSpec in the statement then returns null.
   */
  private static String getIcebergPartitionSpecsSql(CreateTableStmt stmt) {
    List<IcebergPartitionSpec> partitionSpecs = stmt.getIcebergPartitionSpecs();
    // It's safe to get the first element from 'partitionSpecs' as in create table
    // statement only one is allowed.
    if (!partitionSpecs.isEmpty() && partitionSpecs.get(0).hasPartitionFields()) {
      return partitionSpecs.get(0).toSql();
    }
    return null;
  }

  /**
   * Returns the "CREATE TABLE" SQL string corresponding to the given CreateTableStmt
   * statement.
   */
  public static String getCreateTableSql(CreateTableStmt stmt) {
    List<String> colsSql = new ArrayList<>();
    for (ColumnDef col: stmt.getColumnDefs()) {
      colsSql.add(col.toString());
    }
    List<String> partitionColsSql = new ArrayList<>();
    for (ColumnDef col: stmt.getPartitionColumnDefs()) {
      partitionColsSql.add(col.toString());
    }
    Map<String, String> properties = Maps.newLinkedHashMap(
        stmt.getTblProperties());
    Map<String, String> generatedProperties = Maps.newLinkedHashMap(
        stmt.getGeneratedKuduProperties());
    removeHiddenTableProperties(properties);
    removeHiddenKuduTableProperties(generatedProperties);
    properties.putAll(generatedProperties);
    String kuduParamsSql = getKuduPartitionByParams(stmt);
    String icebergPartitionSpecs = getIcebergPartitionSpecsSql(stmt);
    // TODO: Pass the correct compression, if applicable.
    return getCreateTableSql(stmt.getDb(), stmt.getTbl(), stmt.getComment(), colsSql,
        partitionColsSql, stmt.isPrimaryKeyUnique(),
        stmt.getTblPrimaryKeyColumnNames(), stmt.getForeignKeysSql(),
        kuduParamsSql, new Pair<>(stmt.getSortColumns(), stmt.getSortingOrder()),
        properties, stmt.getSerdeProperties(), stmt.isExternal(), stmt.getIfNotExists(),
        stmt.getRowFormat(), HdfsFileFormat.fromThrift(stmt.getFileFormat()),
        HdfsCompression.NONE, null, stmt.getLocation(),
        icebergPartitionSpecs, stmt.geTBucketInfo());
  }

  /**
   * Returns the "CREATE TABLE" SQL string corresponding to the given
   * CreateTableAsSelectStmt statement. If rewritten is true, returns the rewritten SQL
   * only if the statement was rewritten. Otherwise, the original SQL will be returned
   * instead. It is the caller's responsibility to know if/when the statement was indeed
   * rewritten.
   */
  public static String getCreateTableSql(
      CreateTableAsSelectStmt stmt, ToSqlOptions options) {
    CreateTableStmt innerStmt = stmt.getCreateStmt();
    // Only add partition column labels to output. Table columns must not be specified as
    // they are deduced from the select statement.
    List<String> partitionColsSql = new ArrayList<>();
    for (ColumnDef col: innerStmt.getPartitionColumnDefs()) {
      partitionColsSql.add(col.getColName());
    }
    // Use a LinkedHashMap to preserve the ordering of the table properties.
    Map<String, String> properties =
        Maps.newLinkedHashMap(innerStmt.getTblProperties());
    Map<String, String> generatedProperties = Maps.newLinkedHashMap(
        stmt.getCreateStmt().getGeneratedKuduProperties());
    removeHiddenTableProperties(properties);
    removeHiddenKuduTableProperties(generatedProperties);
    properties.putAll(generatedProperties);
    String kuduParamsSql = getKuduPartitionByParams(innerStmt);
    String icebergPartitionSpecs = getIcebergPartitionSpecsSql(innerStmt);
    // TODO: Pass the correct compression, if applicable.
    String createTableSql = getCreateTableSql(innerStmt.getDb(), innerStmt.getTbl(),
        innerStmt.getComment(), null, partitionColsSql, innerStmt.isPrimaryKeyUnique(),
        innerStmt.getTblPrimaryKeyColumnNames(), innerStmt.getForeignKeysSql(),
        kuduParamsSql, new Pair<>(innerStmt.getSortColumns(),
        innerStmt.getSortingOrder()), properties, innerStmt.getSerdeProperties(),
        innerStmt.isExternal(), innerStmt.getIfNotExists(), innerStmt.getRowFormat(),
        HdfsFileFormat.fromThrift(innerStmt.getFileFormat()), HdfsCompression.NONE, null,
        innerStmt.getLocation(), icebergPartitionSpecs, innerStmt.geTBucketInfo());
    return createTableSql + " AS " + stmt.getQueryStmt().toSql(options);
  }

  /**
   * Returns a "CREATE TABLE" or "CREATE VIEW" statement that creates the specified
   * table.
   */
  public static String getCreateTableSql(FeTable table) throws CatalogException {
    return renderCreateTableSql(table).createSql;
  }
  // Holder for CREATE TABLE SQL and filtered properties to avoid recomputation
  private static final class CreateSqlArtifacts {
    final String createSql;
    final Map<String, String> createTableProperties; // Properties for CREATE TABLE
    final Map<String, String> alterTableProperties;  // Properties for ALTER TABLE

    private CreateSqlArtifacts(String createSql, Map<String, String>
        createTableProperties, Map<String, String> alterTableProperties) {
      this.createSql = createSql;
      this.createTableProperties = createTableProperties;
      this.alterTableProperties = alterTableProperties;
    }
  }

  private static CreateSqlArtifacts renderCreateTableSql(FeTable table)
      throws CatalogException {
    Preconditions.checkNotNull(table);
    if (table instanceof FeView) {
      return new CreateSqlArtifacts(getCreateViewSql((FeView)table),
          Maps.newLinkedHashMap(), Maps.newLinkedHashMap());
    }
    org.apache.hadoop.hive.metastore.api.Table msTable = table.getMetaStoreTable();
    // Raw HMS props (preserve ordering)
    Map<String, String> rawProps = Maps.newLinkedHashMap(msTable.getParameters());
    rawProps.remove(Table.TBL_PROP_LAST_DDL_TIME);
    boolean isExternal = Table.isExternalTable(msTable);

    // Values derived from raw props before filtering
    List<String> sortColsSql = getSortColumns(rawProps);
    TSortingOrder sortingOrder = TSortingOrder.valueOf(getSortingOrder(rawProps));
    String comment = rawProps.get("comment");

    // Filter properties into two maps: one for CREATE TABLE, one for ALTER TABLE
    Map<String, String> createTableProps = Maps.newLinkedHashMap();
    Map<String, String> alterTableProps = Maps.newLinkedHashMap();
    filterTblProperties(rawProps, table, msTable, createTableProps, alterTableProps);

    List<String> colsSql = new ArrayList<>();
    List<String> partitionColsSql = new ArrayList<>();
    boolean isHbaseTable = table instanceof FeHBaseTable;
    boolean isFullAcid = AcidUtils.isFullAcidTable(
        table.getMetaStoreTable().getParameters());
    for (int i = 0; i < table.getColumns().size(); i++) {
      Column col = table.getColumns().get(i);
      if (!isHbaseTable && i < table.getNumClusteringCols()) {
        partitionColsSql.add(columnToSql(col));
      } else if (isFullAcid && i == table.getNumClusteringCols()) {
        Preconditions.checkState(col.getName().equals("row__id"));
        continue;
      } else {
        colsSql.add(columnToSql(col));
      }
    }
    RowFormat rowFormat = RowFormat.fromStorageDescriptor(msTable.getSd());
    HdfsFileFormat format = null;
    HdfsCompression compression = null;
    String location = isHbaseTable ? null : msTable.getSd().getLocation();
    Map<String, String> serdeParameters = msTable.getSd().getSerdeInfo().getParameters();
    TBucketInfo bucketInfo = BucketUtils.fromStorageDescriptor(msTable.getSd());

    String storageHandlerClassName = table.getStorageHandlerClassName();
    boolean isPrimaryKeyUnique = true;
    List<String> primaryKeySql = new ArrayList<>();
    List<String> foreignKeySql = new ArrayList<>();
    String kuduPartitionByParams = null;
    String icebergPartitions = null;
    if (table instanceof FeKuduTable) {
      FeKuduTable kuduTable = (FeKuduTable) table;
      // Kudu tables don't use LOCATION syntax
      location = null;
      format = HdfsFileFormat.KUDU;
      // Kudu tables cannot use the Hive DDL syntax for the storage handler
      storageHandlerClassName = null;

      isPrimaryKeyUnique = kuduTable.isPrimaryKeyUnique();
      if (KuduTable.isSynchronizedTable(msTable)) {
        primaryKeySql.addAll(kuduTable.getPrimaryKeyColumnNames());

        List<String> paramsSql = new ArrayList<>();
        for (KuduPartitionParam param: kuduTable.getPartitionBy()) {
          paramsSql.add(param.toSql());
        }
        kuduPartitionByParams = Joiner.on(", ").join(paramsSql);
      } else {
        // we don't output the column spec if this is not a synchronized table (not
        // managed and not external.purge table)
        colsSql = null;
      }
    } else if (table instanceof FeFsTable) {
      if (table instanceof FeIcebergTable) {
        storageHandlerClassName = null;

        FeIcebergTable feIcebergTable = (FeIcebergTable)table;
        // Fill "PARTITIONED BY SPEC" part if the Iceberg table is partitioned.
        if (!feIcebergTable.getPartitionSpecs().isEmpty()) {
          IcebergPartitionSpec latestPartitionSpec =
              feIcebergTable.getDefaultPartitionSpec();
          if (latestPartitionSpec.hasPartitionFields()) {
            icebergPartitions = feIcebergTable.getDefaultPartitionSpec().toSql();
          }
        }
      }

      String inputFormat = msTable.getSd().getInputFormat();
      String serDeLib = msTable.getSd().getSerdeInfo().getSerializationLib();
      format = HdfsFileFormat.fromHdfsInputFormatClass(inputFormat, serDeLib);
      compression = HdfsCompression.fromHdfsInputFormatClass(inputFormat);
      try {
        primaryKeySql = ((FeFsTable) table).getPrimaryKeyColumnNames();
        foreignKeySql = ((FeFsTable) table).getForeignKeysSql();
      } catch (Exception e) {
        throw new CatalogException("Could not get primary key/foreign keys sql.", e);
      }
    } else if (table instanceof FePaimonTable) {
      // format
      String inputFormat = msTable.getSd().getInputFormat();
      String serDeLib = msTable.getSd().getSerdeInfo().getSerializationLib();
      format = HdfsFileFormat.fromHdfsInputFormatClass(inputFormat, serDeLib);
      storageHandlerClassName = null;
      isPrimaryKeyUnique = true;
      // for synchronized table, show sql like a managed table
      if (PaimonUtil.isSynchronizedTable(msTable)) {
        if ((location != null)
            && location.toLowerCase().endsWith(table.getName().toLowerCase())) {
          location = null;
        }
        isExternal = false;
      }
      try {
        FePaimonTable fePaimonTable = (FePaimonTable) table;
        primaryKeySql = fePaimonTable.getPaimonApiTable()
                            .primaryKeys()
                            .stream()
                            .map(String::toLowerCase)
                            .collect(Collectors.toList());

        partitionColsSql = new ArrayList<>();
        for (int i = 0; i < table.getNumClusteringCols(); i++) {
          Column col = table.getColumns().get(i);
          partitionColsSql.add(columnToSql(col));
        }

        colsSql = new ArrayList<>();

        for (int i = table.getNumClusteringCols(); i < table.getColumns().size(); i++) {
          Column col = table.getColumns().get(i);
          colsSql.add(columnToSql(col));
        }
      } catch (Exception e) {
        throw new CatalogException("Could not get primary key/foreign keys sql.", e);
      }
    } else if (table instanceof FeDataSourceTable) {
      // masking handled by filterTblProperties(); nothing to do for CREATE
    }

    HdfsUri tableLocation = location == null ? null : new HdfsUri(location);
    String sql = getCreateTableSql(
        table.getDb().getName(), table.getName(), comment, colsSql, partitionColsSql,
        isPrimaryKeyUnique, primaryKeySql, foreignKeySql, kuduPartitionByParams,
        new Pair<>(sortColsSql, sortingOrder), createTableProps, serdeParameters,
        isExternal, false, rowFormat, format, compression,
        storageHandlerClassName, tableLocation, icebergPartitions, bucketInfo);
    return new CreateSqlArtifacts(sql, createTableProps, alterTableProps);
  }

  /**
   * Returns the SHOW CREATE TABLE output with WITH STATS, which includes the base
   * CREATE statement, followed by ALTER statements to set
   * table properties and column stats where available, and partition statements.
   * @param table The table to generate SQL for
   * @param partitionLimit Maximum number of partitions to output (0 = no limit)
   */
  public static String getCreateTableWithStatsSql(FeTable table, int partitionLimit)
      throws CatalogException {
    StringBuilder out = new StringBuilder();
    StringBuilder warnings = new StringBuilder();

    // CREATE statement (and get both filtered property maps)
    CreateSqlArtifacts artifacts = renderCreateTableSql(table);
    out.append(artifacts.createSql).append(";\n\n");

    // Use the pre-filtered ALTER TABLE properties (less filtered, keeps more properties)
    org.apache.hadoop.hive.metastore.api.Table msTbl = table.getMetaStoreTable();
    Map<String, String> allTblProps = Maps.newLinkedHashMap(
        artifacts.alterTableProperties);

    // Add STATS_GENERATED_VIA_STATS_TASK if present in HMS parameters
    if (msTbl != null && msTbl.getParameters() != null) {
      String statsGenerated = msTbl.getParameters().get("STATS_GENERATED_VIA_STATS_TASK");
      if (statsGenerated != null) {
        allTblProps.put("STATS_GENERATED_VIA_STATS_TASK", statsGenerated);
      }
    }

    // Emit explicit table-level TBLPROPERTIES
    if (!allTblProps.isEmpty()) {
      out.append("ALTER TABLE ")
          .append(getIdentSql(table.getDb().getName())).append('.')
          .append(getIdentSql(table.getName())).append(' ')
          .append("SET TBLPROPERTIES ")
          .append(propertyMapToSql(allTblProps))
          .append(";\n\n");
    }

    // Column stats for non-partition columns
    if (appendColumnStatsStatements(table, out)) {
      out.append("\n");
    }

    // Add partition information if this is a partitioned table
    appendPartitionStatements(table, partitionLimit, out, warnings);

    // Append warnings at the end if any
    if (warnings.length() > 0) {
      out.append(warnings);
    }

    return out.toString();
  }

  /**
   * Appends ALTER TABLE ... SET COLUMN STATS statements for all non-partition columns
   * that have statistics.
   * @param table The table to generate column stats for
   * @param out StringBuilder to append the statements to
   * @return true if any column stats were appended, false otherwise
   */
  private static boolean appendColumnStatsStatements(FeTable table, StringBuilder out) {
    int numClusterCols = table.getNumClusteringCols();
    boolean hasColumnStats = false;

    for (int i = numClusterCols; i < table.getColumns().size(); i++) {
      Column c = table.getColumns().get(i);
      ColumnStats s = c.getStats();
      if (s == null) continue;
      boolean isFixed = c.getType() != null && c.getType().isFixedLengthType();

      List<String> kvs = new ArrayList<>();
      // Always include NDV and nulls (may be -1 for unknown)
      kvs.add("'numDVs'='" + s.getNumDistinctValues() + "'");
      kvs.add("'numNulls'='" + s.getNumNulls() + "'");
      // Include size stats only for variable-length types
      if (!isFixed) {
        kvs.add("'maxSize'='" + s.getMaxSize() + "'");
        double avg = s.getAvgSize();
        String avgStr = (Math.rint(avg) == avg) ?
            Long.toString((long) avg) : Double.toString(avg);
        kvs.add("'avgSize'='" + avgStr + "'");
      }
      // Include boolean-specific counts (may be -1 for non-boolean types)
      kvs.add("'numTrues'='" + s.getNumTrues() + "'");
      kvs.add("'numFalses'='" + s.getNumFalses() + "'");

      hasColumnStats = true;
      out.append("ALTER TABLE ")
          .append(getIdentSql(table.getDb().getName())).append('.')
          .append(getIdentSql(table.getName())).append(' ')
          .append("SET COLUMN STATS ")
          .append(getIdentSql(c.getName())).append(" (")
          .append(Joiner.on(", ").join(kvs)).append(");\n");
    }

    return hasColumnStats;
  }

  /**
   * Appends partition-related statements (ADD PARTITION and partition properties)
   * for partitioned tables.
   * @param table The table to generate partition statements for
   * @param partitionLimit Maximum number of partitions to output (0 = no limit)
   * @param out StringBuilder to append the partition statements to
   * @param warnings StringBuilder to append warnings to
   */
  private static void appendPartitionStatements(FeTable table, int partitionLimit,
      StringBuilder out, StringBuilder warnings) {
    if (!(table instanceof FeFsTable)) return;

    FeFsTable fsTable = (FeFsTable) table;
    Collection<? extends PrunablePartition> partitions = fsTable.getPartitions();
    int numClusterCols = table.getNumClusteringCols();

    if (partitions == null || partitions.isEmpty() || numClusterCols == 0) return;

    // Optimization: First sort lightweight PrunablePartition objects,
    // then load only the required number of full FeFsPartition objects
    List<PrunablePartition> allPartitionRefs = new ArrayList<>(partitions);

    // Sort using the same comparison logic (compares partition values)
    Collections.sort(allPartitionRefs, (p1, p2) ->
        HdfsPartition.comparePartitionKeyValues(
            p1.getPartitionValues(), p2.getPartitionValues()));

    // Determine how many partitions to output
    int totalPartitions = allPartitionRefs.size();
    int partitionsToOutput = (partitionLimit > 0 && partitionLimit < totalPartitions)
        ? partitionLimit : totalPartitions;

    // Load only the partitions we need (not all of them!)
    List<Long> partitionIdsToLoad = new ArrayList<>(partitionsToOutput);
    for (int i = 0; i < partitionsToOutput; i++) {
      partitionIdsToLoad.add(allPartitionRefs.get(i).getId());
    }
    List<? extends FeFsPartition> sortedPartitions = fsTable.loadPartitions(
      partitionIdsToLoad);

    if (sortedPartitions.isEmpty()) return;

    // Generate ADD PARTITION statements
    appendAddPartitionStatements(table, sortedPartitions, out);

    // Generate per-partition TBLPROPERTIES
    appendPartitionPropertiesStatements(table, sortedPartitions, out);

    // Add warning if partitions were skipped
    if (partitionLimit > 0 && totalPartitions > partitionLimit) {
      int skipped = totalPartitions - partitionLimit;
      warnings.append("-- WARNING about partial output\n");
      warnings.append("-- WARNING: Emitted ").append(partitionLimit)
          .append(" of ").append(totalPartitions)
          .append(" partitions (show_create_table_partition_limit=")
          .append(partitionLimit)
          .append("). ").append(skipped).append(" partitions skipped.\n")
          .append("-- To export more partitions, re-run with ")
          .append("SHOW_CREATE_TABLE_PARTITION_LIMIT=<n>.\n");
    }
  }

  /**
   * Appends ALTER TABLE ... ADD PARTITION statements for the given partitions.
   * @param table The table the partitions belong to
   * @param sortedPartitions List of partitions to generate ADD PARTITION statements for
   * @param out StringBuilder to append the statements to
   */
  private static void appendAddPartitionStatements(FeTable table,
      List<? extends FeFsPartition> sortedPartitions, StringBuilder out) {
    int numClusterCols = table.getNumClusteringCols();

    for (FeFsPartition partition : sortedPartitions) {
      out.append("ALTER TABLE ")
          .append(getIdentSql(table.getDb().getName())).append('.')
          .append(getIdentSql(table.getName())).append(' ')
          .append("ADD PARTITION (");

      // Add partition key-value pairs
      List<LiteralExpr> partitionValues = partition.getPartitionValues();
      List<String> partitionCols = new ArrayList<>();
      for (int j = 0; j < numClusterCols; j++) {
        Column col = table.getColumns().get(j);
        LiteralExpr value = partitionValues.get(j);
        partitionCols.add(getIdentSql(col.getName()) + "=" + value.toSql());
      }
      out.append(Joiner.on(", ").join(partitionCols));
      out.append(")");

      // Add LOCATION if available
      String location = partition.getLocation();
      if (location != null && !location.isEmpty()) {
        out.append(" LOCATION '").append(location).append("'");
      }
      out.append(";\n");
    }
    out.append("\n");
  }

  /**
   * Appends ALTER TABLE ... PARTITION ... SET TBLPROPERTIES statements for partitions
   * that have properties to set.
   * @param table The table the partitions belong to
   * @param sortedPartitions List of partitions to generate property statements for
   * @param out StringBuilder to append the statements to
   */
  private static void appendPartitionPropertiesStatements(FeTable table,
      List<? extends FeFsPartition> sortedPartitions, StringBuilder out) {
    int numClusterCols = table.getNumClusteringCols();
    boolean hasPartitionProps = false;

    for (FeFsPartition partition : sortedPartitions) {
      // Get partition statistics
      Map<String, String> partitionProps = Maps.newLinkedHashMap();
      long partNumRows = partition.getNumRows();
      if (partNumRows >= 0) {
        partitionProps.put("numRows", Long.toString(partNumRows));
      }

      // Add additional properties from HMS parameters if present
      Map<String, String> hmsParams = partition.getParameters();
      if (hmsParams != null) {
        if (hmsParams.containsKey("STATS_GENERATED_VIA_STATS_TASK")) {
          partitionProps.put("STATS_GENERATED_VIA_STATS_TASK",
              hmsParams.get("STATS_GENERATED_VIA_STATS_TASK"));
        }

        // Add NUM_FILES if available
        if (hmsParams.containsKey(FeFsTable.NUM_FILES)) {
          partitionProps.put(FeFsTable.NUM_FILES, hmsParams.get(FeFsTable.NUM_FILES));
        }

        // Add TOTAL_SIZE if available
        if (hmsParams.containsKey(FeFsTable.TOTAL_SIZE)) {
          partitionProps.put(FeFsTable.TOTAL_SIZE, hmsParams.get(FeFsTable.TOTAL_SIZE));
        }
      }

      if (!partitionProps.isEmpty()) {
        hasPartitionProps = true;

        out.append("ALTER TABLE ")
            .append(getIdentSql(table.getDb().getName())).append('.')
            .append(getIdentSql(table.getName())).append(' ')
            .append("PARTITION (");

        // Add partition key-value pairs
        List<LiteralExpr> partitionValues = partition.getPartitionValues();
        List<String> partitionCols = new ArrayList<>();
        for (int j = 0; j < numClusterCols; j++) {
          Column col = table.getColumns().get(j);
          LiteralExpr value = partitionValues.get(j);
          partitionCols.add(getIdentSql(col.getName()) + "=" + value.toSql());
        }
        out.append(Joiner.on(", ").join(partitionCols));
        out.append(") SET TBLPROPERTIES ")
            .append(propertyMapToSql(partitionProps))
            .append(";\n");
      }
    }

    if (hasPartitionProps) {
      out.append("\n");
    }
  }

  /**
   * Returns a "CREATE TABLE" string that creates the table with the specified properties
   * The tableName must not be null. If columnsSql is null, the schema syntax will
   * not be generated.
   */
  public static String getCreateTableSql(String dbName, String tableName,
      String tableComment, List<String> columnsSql, List<String> partitionColumnsSql,
      boolean isPrimaryKeyUnique, List<String> primaryKeysSql,
      List<String> foreignKeysSql, String kuduPartitionByParams, Pair<List<String>,
      TSortingOrder> sortProperties, Map<String, String> tblProperties,
      Map<String, String> serdeParameters, boolean isExternal, boolean ifNotExists,
      RowFormat rowFormat, HdfsFileFormat fileFormat, HdfsCompression compression,
      String storageHandlerClass, HdfsUri location, String icebergPartitions,
      TBucketInfo bucketInfo) {
    Preconditions.checkNotNull(tableName);
    StringBuilder sb = new StringBuilder("CREATE ");
    if (isExternal) sb.append("EXTERNAL ");
    sb.append("TABLE ");
    if (ifNotExists) sb.append("IF NOT EXISTS ");
    if (dbName != null) sb.append(dbName + ".");
    sb.append(tableName);
    if (columnsSql != null && !columnsSql.isEmpty()) {
      sb.append(" (\n  ");
      sb.append(Joiner.on(",\n  ").join(columnsSql));
      if (CollectionUtils.isNotEmpty(primaryKeysSql)) {
        sb.append(",\n  ");
        sb.append(KuduUtil.getPrimaryKeyString(isPrimaryKeyUnique)).append(" (");
        Joiner.on(", ").appendTo(sb, primaryKeysSql).append(")");
      }
      if (CollectionUtils.isNotEmpty(foreignKeysSql)) {
        sb.append(",\n  FOREIGN KEY");
        Joiner.on(",\n  FOREIGN KEY").appendTo(sb, foreignKeysSql).append("\n");
      }
      sb.append("\n)");
    } else {
      // CTAS for Kudu tables still print the primary key
      if (primaryKeysSql != null && !primaryKeysSql.isEmpty()) {
        sb.append("\n ");
        sb.append(KuduUtil.getPrimaryKeyString(isPrimaryKeyUnique)).append(" (");
        Joiner.on(", ").appendTo(sb, primaryKeysSql).append(")");
      }
    }
    sb.append("\n");

    if (partitionColumnsSql != null && partitionColumnsSql.size() > 0) {
      sb.append(String.format("PARTITIONED BY (\n  %s\n)\n",
          Joiner.on(", \n  ").join(partitionColumnsSql)));
    }

    if (kuduPartitionByParams != null && !kuduPartitionByParams.equals("")) {
      sb.append("PARTITION BY " + kuduPartitionByParams + "\n");
    }
    if (bucketInfo != null && bucketInfo.getBucket_type() != TBucketType.NONE) {
      sb.append(String.format("CLUSTERED BY (\n %s\n)\n",
          Joiner.on(", \n  ").join(bucketInfo.getBucket_columns())));
      if (sortProperties.first != null) {
        sb.append(String.format("SORT BY %s (\n  %s\n)\n",
            sortProperties.second.toString(),
            Joiner.on(", \n  ").join(sortProperties.first)));
      }
      sb.append(String.format("INTO %s BUCKETS\n", bucketInfo.getNum_bucket()));
    } else if (sortProperties.first != null) {
      sb.append(String.format("SORT BY %s (\n  %s\n)\n", sortProperties.second.toString(),
          Joiner.on(", \n  ").join(sortProperties.first)));
    }
    if (icebergPartitions != null && !icebergPartitions.isEmpty()) {
      sb.append("PARTITIONED BY SPEC\n");
      sb.append(icebergPartitions);
      sb.append("\n");
    }

    if (tableComment != null) sb.append(" COMMENT '" + tableComment + "'\n");

    if (rowFormat != null && !rowFormat.isDefault()) {
      sb.append("ROW FORMAT DELIMITED");
      if (rowFormat.getFieldDelimiter() != null) {
        String fieldDelim = StringEscapeUtils.escapeJava(rowFormat.getFieldDelimiter());
        sb.append(" FIELDS TERMINATED BY '" + fieldDelim + "'");
      }
      if (rowFormat.getEscapeChar() != null) {
        String escapeChar = StringEscapeUtils.escapeJava(rowFormat.getEscapeChar());
        sb.append(" ESCAPED BY '" + escapeChar + "'");
      }
      if (rowFormat.getLineDelimiter() != null) {
        String lineDelim = StringEscapeUtils.escapeJava(rowFormat.getLineDelimiter());
        sb.append(" LINES TERMINATED BY '" + lineDelim + "'");
      }
      sb.append("\n");
    }

    if (storageHandlerClass == null) {
      // We must handle LZO_TEXT specially because Impala does not support creating
      // tables with this row format. In this case, we cannot output "WITH
      // SERDEPROPERTIES" because Hive does not support it with "STORED AS". For any
      // other HdfsFileFormat we want to output the serdeproperties because it is
      // supported by Impala.
      if (compression != HdfsCompression.LZO &&
          compression != HdfsCompression.LZO_INDEX &&
          serdeParameters != null && !serdeParameters.isEmpty()) {
        sb.append(
            "WITH SERDEPROPERTIES " + propertyMapToSql(serdeParameters) + "\n");
      }

      if (fileFormat != null) {
        sb.append("STORED AS " + fileFormat.toSql(compression) + "\n");
      }
    } else {
      // If the storageHandlerClass is set, then we will generate the proper Hive DDL
      // because we do not yet support creating HBase tables via Impala.
      sb.append("STORED BY '" + storageHandlerClass + "'\n");
      if (serdeParameters != null && !serdeParameters.isEmpty()) {
        sb.append(
            "WITH SERDEPROPERTIES " + propertyMapToSql(serdeParameters) + "\n");
      }
    }

    // Iceberg table with 'hadoop.catalog' do not display table LOCATION when using
    // 'show create table', user can use 'describe formatted/extended' to get location
    TIcebergCatalog icebergCatalog =
        IcebergUtil.getTIcebergCatalog(tblProperties.get(IcebergTable.ICEBERG_CATALOG));
    boolean isHadoopCatalog = fileFormat == HdfsFileFormat.ICEBERG &&
        icebergCatalog == TIcebergCatalog.HADOOP_CATALOG;
    if (location != null && !isHadoopCatalog) {
      sb.append("LOCATION '" + location.toString() + "'\n");
    }
    if (tblProperties != null && !tblProperties.isEmpty()) {
      sb.append("TBLPROPERTIES " + propertyMapToSql(tblProperties));
    }
    return sb.toString();
  }

  public static String getCreateFunctionSql(List<Function> functions) {
    Preconditions.checkNotNull(functions);
    StringBuilder sb = new StringBuilder();
    for (Function fn: functions) {
      if (sb.length() > 0) sb.append(";\n");
      sb.append(fn.toSql(false));
    }
    return sb.append("\n").toString();
  }

  public static String getCreateViewSql(FeView view) {
    StringBuffer sb = new StringBuffer();
    sb.append("CREATE VIEW ");
    // Use toSql() to ensure that the table name and query statement are normalized
    // and identifiers are quoted.
    sb.append(view.getTableName().toSql());
    sb.append(" AS\n");
    sb.append(view.getQueryStmt().toSql());
    return sb.toString();
  }

  private static String encodeColumnName(String name) {
    if (impalaNeedsQuotes(name)) {
      return String.format("`%s`", name);
    } else {
      return name;
    }
  }

  private static String columnToSql(Column col) {
    StringBuilder sb = new StringBuilder(encodeColumnName(col.getName()));
    if (col.getType() != null) sb.append(" " + col.getType().toSql());
    if (col instanceof KuduColumn) {
      KuduColumn kuduCol = (KuduColumn) col;
      Boolean isNullable = kuduCol.isNullable();
      if (isNullable != null) sb.append(isNullable ? " NULL" : " NOT NULL");
      if (kuduCol.getEncoding() != null) sb.append(" ENCODING " + kuduCol.getEncoding());
      if (kuduCol.getCompression() != null) {
        sb.append(" COMPRESSION " + kuduCol.getCompression());
      }
      if (kuduCol.hasDefaultValue()) {
        sb.append(" DEFAULT " + kuduCol.getDefaultValueSql());
      }
      if (kuduCol.getBlockSize() != 0) {
        sb.append(String.format(" BLOCK_SIZE %d", kuduCol.getBlockSize()));
      }
    } else if (col instanceof IcebergColumn) {
      IcebergColumn icebergCol = (IcebergColumn) col;
      Boolean isNullable = icebergCol.isNullable();
      if (isNullable != null) sb.append(isNullable ? " NULL" : " NOT NULL");
    }
    if (!Strings.isNullOrEmpty(col.getComment())) {
      sb.append(String.format(" COMMENT '%s'", col.getComment()));
    }
    return sb.toString();
  }

  public static String propertyMapToSql(Map<String, String> propertyMap) {
    // Sort entries on the key to ensure output is deterministic for tests (IMPALA-5757).
    List<Entry<String, String>> mapEntries = Lists.newArrayList(propertyMap.entrySet());
    Collections.sort(mapEntries, new Comparator<Entry<String, String>>() {
      @Override
      public int compare(Entry<String, String> o1, Entry<String, String> o2) {
        return ObjectUtils.compare(o1.getKey(), o2.getKey());
      } });

    List<String> properties = new ArrayList<>();
    for (Map.Entry<String, String> entry: mapEntries) {
      properties.add(String.format("'%s'='%s'", entry.getKey(),
          // Properties may contain characters that need to be escaped.
          // e.g. If the row format escape delimiter is '\', the map of serde properties
          // from the metastore table will contain 'escape.delim' => '\', which is not
          // properly escaped.
          StringEscapeUtils.escapeJava(entry.getValue())));
    }
    return "(" + Joiner.on(", ").join(properties) + ")";
  }

  /**
   * Returns a SQL representation of the given list of hints. Uses the end-of-line
   * commented plan hint style such that hinted views created by Impala are readable by
   * Hive (parsed as a comment by Hive).
   */
  public static String getPlanHintsSql(ToSqlOptions options, List<PlanHint> hints) {
    Preconditions.checkNotNull(hints);
    if (hints.isEmpty()) return "";
    StringBuilder sb = new StringBuilder();
    if (options.showRewritten()) {
      sb.append("/* +");
      sb.append(Joiner.on(",").join(hints));
      sb.append(" */");
    } else {
      sb.append("\n-- +");
      sb.append(Joiner.on(",").join(hints));
      sb.append("\n");
    }
    return sb.toString();
  }

  public static String formatAlias(String alias) {
    if (alias == null) return "";
    return " " + getIdentSql(alias);
  }
}
