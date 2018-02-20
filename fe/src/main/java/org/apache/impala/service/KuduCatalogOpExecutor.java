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

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.impala.catalog.KuduColumn;
import org.apache.impala.catalog.KuduTable;
import org.apache.impala.catalog.Table;
import org.apache.impala.catalog.TableNotFoundException;
import org.apache.impala.catalog.Type;
import org.apache.impala.common.ImpalaRuntimeException;
import org.apache.impala.common.Pair;
import org.apache.impala.thrift.TAlterTableAddDropRangePartitionParams;
import org.apache.impala.thrift.TColumn;
import org.apache.impala.thrift.TCreateTableParams;
import org.apache.impala.thrift.TKuduPartitionParam;
import org.apache.impala.thrift.TRangePartition;
import org.apache.impala.thrift.TRangePartitionOperationType;
import org.apache.impala.util.KuduUtil;
import org.apache.kudu.ColumnSchema;
import org.apache.kudu.ColumnSchema.ColumnSchemaBuilder;
import org.apache.kudu.Schema;
import org.apache.kudu.client.AlterTableOptions;
import org.apache.kudu.client.CreateTableOptions;
import org.apache.kudu.client.KuduClient;
import org.apache.kudu.client.KuduException;
import org.apache.kudu.client.PartialRow;
import org.apache.kudu.client.RangePartitionBound;
import org.apache.kudu.util.DecimalUtil;
import org.apache.log4j.Logger;

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

/**
 * This is a helper for the CatalogOpExecutor to provide Kudu related DDL functionality
 * such as creating and dropping tables from Kudu.
 */
public class KuduCatalogOpExecutor {
  public static final Logger LOG = Logger.getLogger(KuduCatalogOpExecutor.class);

  /**
   * Create a table in Kudu with a schema equivalent to the schema stored in 'msTbl'.
   * Throws an exception if 'msTbl' represents an external table or if the table couldn't
   * be created in Kudu.
   */
  static void createManagedTable(org.apache.hadoop.hive.metastore.api.Table msTbl,
      TCreateTableParams params) throws ImpalaRuntimeException {
    Preconditions.checkState(!Table.isExternalTable(msTbl));
    String kuduTableName = msTbl.getParameters().get(KuduTable.KEY_TABLE_NAME);
    String masterHosts = msTbl.getParameters().get(KuduTable.KEY_MASTER_HOSTS);
    if (LOG.isTraceEnabled()) {
      LOG.trace(String.format("Creating table '%s' in master '%s'", kuduTableName,
          masterHosts));
    }
    KuduClient kudu = KuduUtil.getKuduClient(masterHosts);
    try {
      // TODO: The IF NOT EXISTS case should be handled by Kudu to ensure atomicity.
      // (see KUDU-1710).
      if (kudu.tableExists(kuduTableName)) {
        if (params.if_not_exists) return;
        throw new ImpalaRuntimeException(String.format(
            "Table '%s' already exists in Kudu.", kuduTableName));
      }
      Schema schema = createTableSchema(params);
      CreateTableOptions tableOpts = buildTableOptions(msTbl, params, schema);
      kudu.createTable(kuduTableName, schema, tableOpts);
    } catch (Exception e) {
      throw new ImpalaRuntimeException(String.format("Error creating Kudu table '%s'",
          kuduTableName), e);
    }
  }

  private static ColumnSchema createColumnSchema(TColumn column, boolean isKey)
      throws ImpalaRuntimeException {
    Type type = Type.fromThrift(column.getColumnType());
    Preconditions.checkState(type != null);
    org.apache.kudu.Type kuduType = KuduUtil.fromImpalaType(type);

    ColumnSchemaBuilder csb = new ColumnSchemaBuilder(column.getColumnName(), kuduType);
    csb.key(isKey);
    if (column.isSetIs_nullable()) {
      // If nullability is explicitly set and the column is a key, it must have been
      // set as NOT NULL. This is the default, but it is also valid to specify it.
      Preconditions.checkArgument(!isKey || !column.isIs_nullable());
      csb.nullable(column.isIs_nullable());
    } else {
      // Non-key columns are by default nullable unless the user explicitly sets its
      // nullability. Key columns are not nullable.
      csb.nullable(!isKey);
    }
    if (column.isSetDefault_value()) {
      csb.defaultValue(KuduUtil.getKuduDefaultValue(
          column.getDefault_value(), type, column.getColumnName()));
    }
    if (column.isSetBlock_size()) csb.desiredBlockSize(column.getBlock_size());
    if (column.isSetEncoding()) {
      csb.encoding(KuduUtil.fromThrift(column.getEncoding()));
    }
    if (column.isSetCompression()) {
      csb.compressionAlgorithm(KuduUtil.fromThrift(column.getCompression()));
    }
    if (type.isDecimal()) {
      csb.typeAttributes(
          DecimalUtil.typeAttributes(type.getPrecision(), type.getDecimalDigits()));
    }
    return csb.build();
  }

  /**
   * Creates the schema of a new Kudu table.
   */
  private static Schema createTableSchema(TCreateTableParams params)
      throws ImpalaRuntimeException {
    Set<String> keyColNames = new HashSet<>(params.getPrimary_key_column_names());
    Preconditions.checkState(!keyColNames.isEmpty());
    List<ColumnSchema> colSchemas = new ArrayList<>(params.getColumnsSize());
    for (TColumn column: params.getColumns()) {
      boolean isKey = keyColNames.contains(column.getColumnName());
      colSchemas.add(createColumnSchema(column, isKey));
    }
    return new Schema(colSchemas);
  }

  /**
   * Builds the table options of a new Kudu table.
   */
  private static CreateTableOptions buildTableOptions(
      org.apache.hadoop.hive.metastore.api.Table msTbl,
      TCreateTableParams params, Schema schema) throws ImpalaRuntimeException {
    CreateTableOptions tableOpts = new CreateTableOptions();
    // Set the partitioning schemes
    List<TKuduPartitionParam> partitionParams = params.getPartition_by();
    if (partitionParams != null) {
      boolean hasRangePartitioning = false;
      for (TKuduPartitionParam partParam: partitionParams) {
        if (partParam.isSetBy_hash_param()) {
          Preconditions.checkState(!partParam.isSetBy_range_param());
          tableOpts.addHashPartitions(partParam.getBy_hash_param().getColumns(),
              partParam.getBy_hash_param().getNum_partitions());
        } else {
          Preconditions.checkState(partParam.isSetBy_range_param());
          hasRangePartitioning = true;
          List<String> rangePartitionColumns = partParam.getBy_range_param().getColumns();
          tableOpts.setRangePartitionColumns(rangePartitionColumns);
          for (TRangePartition rangePartition:
               partParam.getBy_range_param().getRange_partitions()) {
            List<Pair<PartialRow, RangePartitionBound>> rangeBounds =
                getRangePartitionBounds(rangePartition, schema, rangePartitionColumns);
            Preconditions.checkState(rangeBounds.size() == 2);
            Pair<PartialRow, RangePartitionBound> lowerBound = rangeBounds.get(0);
            Pair<PartialRow, RangePartitionBound> upperBound = rangeBounds.get(1);
            tableOpts.addRangePartition(lowerBound.first, upperBound.first,
                lowerBound.second, upperBound.second);
          }
        }
      }
      // If no range-based partitioning is specified in a CREATE TABLE statement, Kudu
      // generates one by default that includes all the primary key columns. To prevent
      // this from happening, explicitly set the range partition columns to be
      // an empty list.
      if (!hasRangePartitioning) {
        tableOpts.setRangePartitionColumns(Collections.<String>emptyList());
      }
    } else {
      // This table is unpartitioned, which Kudu represents as a table range partitioned
      // on no columns.
      tableOpts.setRangePartitionColumns(Collections.<String>emptyList());
    }

    // Set the number of table replicas, if specified.
    String replication = msTbl.getParameters().get(KuduTable.KEY_TABLET_REPLICAS);
    if (!Strings.isNullOrEmpty(replication)) {
      int parsedReplicas = -1;
      try {
        parsedReplicas = Integer.parseInt(replication);
        Preconditions.checkState(parsedReplicas > 0,
            "Invalid number of replicas table property:" + replication);
      } catch (Exception e) {
        throw new ImpalaRuntimeException(String.format("Invalid number of table " +
            "replicas specified: '%s'", replication));
      }
      tableOpts.setNumReplicas(parsedReplicas);
    }
    return tableOpts;
  }

  /**
   * Drops the table in Kudu. If the table does not exist and 'ifExists' is false, a
   * TableNotFoundException is thrown. If the table exists and could not be dropped,
   * an ImpalaRuntimeException is thrown.
   */
  static void dropTable(org.apache.hadoop.hive.metastore.api.Table msTbl,
      boolean ifExists) throws ImpalaRuntimeException, TableNotFoundException {
    Preconditions.checkState(!Table.isExternalTable(msTbl));
    String tableName = msTbl.getParameters().get(KuduTable.KEY_TABLE_NAME);
    String masterHosts = msTbl.getParameters().get(KuduTable.KEY_MASTER_HOSTS);
    if (LOG.isTraceEnabled()) {
      LOG.trace(String.format("Dropping table '%s' from master '%s'", tableName,
          masterHosts));
    }
    KuduClient kudu = KuduUtil.getKuduClient(masterHosts);
    try {
      Preconditions.checkState(!Strings.isNullOrEmpty(tableName));
      // TODO: The IF EXISTS case should be handled by Kudu to ensure atomicity.
      // (see KUDU-1710).
      if (kudu.tableExists(tableName)) {
        kudu.deleteTable(tableName);
      } else if (!ifExists) {
        throw new TableNotFoundException(String.format(
            "Table '%s' does not exist in Kudu master(s) '%s'.", tableName, masterHosts));
      }
    } catch (Exception e) {
      throw new ImpalaRuntimeException(String.format("Error dropping table '%s'",
          tableName), e);
    }
  }

  /**
   * Reads the column definitions from a Kudu table and populates 'msTbl' with
   * an equivalent schema. Throws an exception if any errors are encountered.
   */
  public static void populateColumnsFromKudu(
      org.apache.hadoop.hive.metastore.api.Table msTbl) throws ImpalaRuntimeException {
    org.apache.hadoop.hive.metastore.api.Table msTblCopy = msTbl.deepCopy();
    List<FieldSchema> cols = msTblCopy.getSd().getCols();
    String kuduTableName = msTblCopy.getParameters().get(KuduTable.KEY_TABLE_NAME);
    Preconditions.checkState(!Strings.isNullOrEmpty(kuduTableName));
    String masterHosts = msTblCopy.getParameters().get(KuduTable.KEY_MASTER_HOSTS);
    if (LOG.isTraceEnabled()) {
      LOG.trace(String.format("Loading schema of table '%s' from master '%s'",
          kuduTableName, masterHosts));
    }
    KuduClient kudu = KuduUtil.getKuduClient(masterHosts);
    try {
      if (!kudu.tableExists(kuduTableName)) {
        throw new ImpalaRuntimeException(String.format("Table does not exist in Kudu: " +
            "'%s'", kuduTableName));
      }
      org.apache.kudu.client.KuduTable kuduTable = kudu.openTable(kuduTableName);
      // Replace the columns in the Metastore table with the columns from the recently
      // accessed Kudu schema.
      cols.clear();
      Set<String> lowerCaseColNames = Sets.newHashSet();
      for (ColumnSchema colSchema : kuduTable.getSchema().getColumns()) {
        if (!lowerCaseColNames.add(colSchema.getName().toLowerCase())) {
          throw new ImpalaRuntimeException(String.format(
              "Error loading Kudu table: Impala does not support column names that " +
              "differ only in casing '%s'", colSchema.getName()));
        }
        Type type =
            KuduUtil.toImpalaType(colSchema.getType(), colSchema.getTypeAttributes());
        cols.add(new FieldSchema(colSchema.getName(), type.toSql().toLowerCase(), null));
      }
    } catch (Exception e) {
      throw new ImpalaRuntimeException(String.format("Error loading schema of table " +
          "'%s'", kuduTableName), e);
    }
    List<FieldSchema> newCols = msTbl.getSd().getCols();
    newCols.clear();
    newCols.addAll(cols);
  }

  /**
   * Validates the table properties of a Kudu table. It checks that the master
   * addresses point to valid Kudu masters and that the table exists.
   * Throws an ImpalaRuntimeException if this is not the case.
   */
  public static void validateKuduTblExists(
      org.apache.hadoop.hive.metastore.api.Table msTbl) throws ImpalaRuntimeException {
    Preconditions.checkArgument(KuduTable.isKuduTable(msTbl));

    Map<String, String> properties = msTbl.getParameters();
    String masterHosts = properties.get(KuduTable.KEY_MASTER_HOSTS);
    Preconditions.checkState(!Strings.isNullOrEmpty(masterHosts));
    String kuduTableName = properties.get(KuduTable.KEY_TABLE_NAME);
    Preconditions.checkState(!Strings.isNullOrEmpty(kuduTableName));
    KuduClient kudu = KuduUtil.getKuduClient(masterHosts);
    try {
      kudu.tableExists(kuduTableName);
    } catch (Exception e) {
      // TODO: This is misleading when there are other errors, e.g. timeouts.
      throw new ImpalaRuntimeException(String.format("Kudu table '%s' does not exist " +
          "on master '%s'", kuduTableName, masterHosts), e);
    }
  }

  /**
   * Renames a Kudu table.
   */
  public static void renameTable(KuduTable tbl, String newName)
      throws ImpalaRuntimeException {
    Preconditions.checkState(!Strings.isNullOrEmpty(newName));
    AlterTableOptions alterTableOptions = new AlterTableOptions();
    alterTableOptions.renameTable(newName);
    String errMsg = String.format("Error renaming Kudu table " +
        "%s to %s", tbl.getKuduTableName(), newName);
    KuduClient client = KuduUtil.getKuduClient(tbl.getKuduMasterHosts());
    try {
      client.alterTable(tbl.getKuduTableName(), alterTableOptions);
      if (!client.isAlterTableDone(newName)) {
        throw new ImpalaRuntimeException(errMsg + ": Kudu operation timed out");
      }
    } catch (KuduException e) {
      throw new ImpalaRuntimeException(errMsg, e);
    }
  }

  /**
   * Adds/drops a range partition.
   */
  public static void addDropRangePartition(KuduTable tbl,
      TAlterTableAddDropRangePartitionParams params) throws ImpalaRuntimeException {
    TRangePartition rangePartition = params.getRange_partition_spec();
    List<Pair<PartialRow, RangePartitionBound>> rangeBounds =
        getRangePartitionBounds(rangePartition, tbl);
    Preconditions.checkState(rangeBounds.size() == 2);
    Pair<PartialRow, RangePartitionBound> lowerBound = rangeBounds.get(0);
    Pair<PartialRow, RangePartitionBound> upperBound = rangeBounds.get(1);
    AlterTableOptions alterTableOptions = new AlterTableOptions();
    TRangePartitionOperationType type = params.getType();
    if (type == TRangePartitionOperationType.ADD) {
      alterTableOptions.addRangePartition(lowerBound.first, upperBound.first,
          lowerBound.second, upperBound.second);
    } else {
      alterTableOptions.dropRangePartition(lowerBound.first, upperBound.first,
          lowerBound.second, upperBound.second);
    }
    String errMsg = String.format("Error %s range partition in " +
        "table %s", (type == TRangePartitionOperationType.ADD ? "adding" : "dropping"),
        tbl.getName());
    try {
      alterKuduTable(tbl, alterTableOptions, errMsg);
    } catch (ImpalaRuntimeException e) {
      if (!params.isIgnore_errors()) throw e;
    }
  }

  private static List<Pair<PartialRow, RangePartitionBound>> getRangePartitionBounds(
      TRangePartition rangePartition, KuduTable tbl) throws ImpalaRuntimeException {
    List<String> rangePartitioningColNames = tbl.getRangePartitioningColNames();
    List<String> rangePartitioningKuduColNames =
      Lists.newArrayListWithCapacity(rangePartitioningColNames.size());
    for (String colName : rangePartitioningColNames) {
      rangePartitioningKuduColNames.add(((KuduColumn)tbl.getColumn(colName)).getKuduName());
    }
    return getRangePartitionBounds(rangePartition, tbl.getKuduSchema(),
        rangePartitioningKuduColNames);
  }

  /**
   * Returns the bounds of a range partition in two <PartialRow, RangePartitionBound>
   * pairs to be used in Kudu API calls for ALTER and CREATE TABLE statements.
   * 'rangePartitioningColNames' must be specified in Kudu case.
   */
  private static List<Pair<PartialRow, RangePartitionBound>> getRangePartitionBounds(
      TRangePartition rangePartition, Schema schema,
      List<String> rangePartitioningColNames) throws ImpalaRuntimeException {
    Preconditions.checkNotNull(schema);
    Preconditions.checkState(!rangePartitioningColNames.isEmpty());
    Preconditions.checkState(rangePartition.isSetLower_bound_values()
        || rangePartition.isSetUpper_bound_values());
    List<Pair<PartialRow, RangePartitionBound>> rangeBounds =
        Lists.newArrayListWithCapacity(2);
    Pair<PartialRow, RangePartitionBound> lowerBound =
        KuduUtil.buildRangePartitionBound(schema, rangePartitioningColNames,
        rangePartition.getLower_bound_values(),
        rangePartition.isIs_lower_bound_inclusive());
    rangeBounds.add(lowerBound);
    Pair<PartialRow, RangePartitionBound> upperBound =
        KuduUtil.buildRangePartitionBound(schema, rangePartitioningColNames,
        rangePartition.getUpper_bound_values(),
        rangePartition.isIs_upper_bound_inclusive());
    rangeBounds.add(upperBound);
    return rangeBounds;
  }

  /**
   * Adds a column to an existing Kudu table.
   */
  public static void addColumn(KuduTable tbl, List<TColumn> columns)
      throws ImpalaRuntimeException {
    AlterTableOptions alterTableOptions = new AlterTableOptions();
    for (TColumn column: columns) {
      alterTableOptions.addColumn(createColumnSchema(column, false));
    }
    String errMsg = "Error adding columns to Kudu table " + tbl.getName();
    alterKuduTable(tbl, alterTableOptions, errMsg);
  }

  /**
   * Drops a column from a Kudu table.
   */
  public static void dropColumn(KuduTable tbl, String colName)
      throws ImpalaRuntimeException {
    Preconditions.checkState(!Strings.isNullOrEmpty(colName));
    KuduColumn col = (KuduColumn) tbl.getColumn(colName);
    AlterTableOptions alterTableOptions = new AlterTableOptions();
    alterTableOptions.dropColumn(col.getKuduName());
    String errMsg = String.format("Error dropping column %s from " +
        "Kudu table %s", colName, tbl.getName());
    alterKuduTable(tbl, alterTableOptions, errMsg);
  }

  /**
   * Updates the column matching 'colName' to have the name and options specified in
   * 'newCol'. Setting comments or updating the type, primary key status, or nullability
   * are not currently supported by Kudu.
   *
   * For the storage attrbiutes - encoding, compression, and block size - Kudu does not
   * rewrite old rowsets to have these attributes during the alter. They are applied to
   * new rowsets as they are written out, and possibly to old rowsets if they are
   * compacted into new rowsets depending on cost based decisions Kudu makes.
   */
  public static void alterColumn(KuduTable tbl, String colName, TColumn newCol)
      throws ImpalaRuntimeException {
    Preconditions.checkState(!Strings.isNullOrEmpty(colName));
    Preconditions.checkNotNull(newCol);
    Preconditions.checkState(!newCol.isSetComment());
    Preconditions.checkState(!newCol.isIs_key());
    Preconditions.checkState(!newCol.isSetIs_nullable());
    if (LOG.isTraceEnabled()) {
      LOG.trace(
          String.format("Altering column '%s' to '%s'", colName, newCol.toString()));
    }
    KuduColumn col = (KuduColumn) tbl.getColumn(colName);
    String kuduColName = col.getKuduName();
    AlterTableOptions alterTableOptions = new AlterTableOptions();

    if (newCol.isSetDefault_value()) {
      Type type = Type.fromThrift(newCol.getColumnType());
      Object defaultValue = KuduUtil.getKuduDefaultValue(
          newCol.getDefault_value(), type, newCol.getColumnName());
      if (defaultValue == null) {
        alterTableOptions.removeDefault(kuduColName);
      } else {
        alterTableOptions.changeDefault(kuduColName, defaultValue);
      }
    }
    if (newCol.isSetBlock_size()) {
      alterTableOptions.changeDesiredBlockSize(kuduColName, newCol.getBlock_size());
    }
    if (newCol.isSetEncoding()) {
      alterTableOptions.changeEncoding(
          kuduColName, KuduUtil.fromThrift(newCol.getEncoding()));
    }
    if (newCol.isSetCompression()) {
      alterTableOptions.changeCompressionAlgorithm(
          kuduColName, KuduUtil.fromThrift(newCol.getCompression()));
    }
    String newColName = newCol.getColumnName();
    if (!newColName.toLowerCase().equals(colName.toLowerCase())) {
      alterTableOptions.renameColumn(kuduColName, newColName);
    }

    String errMsg = String.format(
        "Error altering column %s in Kudu table %s", colName, tbl.getName());
    alterKuduTable(tbl, alterTableOptions, errMsg);
  }

  /**
   * Alters a Kudu table based on the specified AlterTableOptions params. Blocks until
   * the alter table operation is finished or until the operation timeout is reached.
   * Throws an ImpalaRuntimeException if the operation cannot be completed successfully.
   */
  public static void alterKuduTable(KuduTable tbl, AlterTableOptions ato, String errMsg)
      throws ImpalaRuntimeException {
    KuduClient client = KuduUtil.getKuduClient(tbl.getKuduMasterHosts());
    try {
      client.alterTable(tbl.getKuduTableName(), ato);
      if (!client.isAlterTableDone(tbl.getKuduTableName())) {
        throw new ImpalaRuntimeException(errMsg + ": Kudu operation timed out");
      }
    } catch (KuduException e) {
      throw new ImpalaRuntimeException(errMsg, e);
    }
  }
}
