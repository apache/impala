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

package org.apache.impala.compat;

import static org.apache.impala.service.MetadataOp.TABLE_TYPE_TABLE;
import static org.apache.impala.service.MetadataOp.TABLE_TYPE_VIEW;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import java.util.EnumSet;
import java.util.List;

import org.apache.hadoop.hive.common.FileUtils;
import org.apache.hadoop.hive.common.StatsSetupConst;
import org.apache.hadoop.hive.ql.metadata.formatting.MetaDataFormatUtils;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.IMetaStoreClient;
import org.apache.hadoop.hive.metastore.MetaStoreUtils;
import org.apache.hadoop.hive.metastore.TableType;
import org.apache.hadoop.hive.metastore.Warehouse;
import org.apache.hadoop.hive.metastore.api.InvalidOperationException;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.ql.parse.BaseSemanticAnalyzer;
import org.apache.hadoop.hive.metastore.messaging.AlterTableMessage;
import org.apache.hadoop.hive.metastore.messaging.InsertMessage;
import org.apache.hadoop.hive.metastore.messaging.MessageDeserializer;
import org.apache.hadoop.hive.metastore.messaging.json.ExtendedJSONMessageFactory;
import org.apache.hive.service.rpc.thrift.TGetColumnsReq;
import org.apache.hive.service.rpc.thrift.TGetFunctionsReq;
import org.apache.hive.service.rpc.thrift.TGetSchemasReq;
import org.apache.hive.service.rpc.thrift.TGetTablesReq;
import org.apache.impala.authorization.User;
import org.apache.impala.common.ImpalaException;
import org.apache.impala.common.Pair;
import org.apache.impala.service.Frontend;
import org.apache.impala.service.MetadataOp;
import org.apache.impala.thrift.TMetadataOpRequest;
import org.apache.impala.thrift.TResultSet;
import org.apache.thrift.TException;

/**
 * A wrapper around some of Hive's Metastore API's to abstract away differences
 * between major versions of Hive. This implements the shimmed methods for Hive 2.
 */
public class MetastoreShim {
  /**
   * Wrapper around MetaStoreUtils.validateName() to deal with added arguments.
   */
  public static boolean validateName(String name) {
    return MetaStoreUtils.validateName(name, null);
  }

  /**
   * Wrapper around IMetaStoreClient.alter_partition() to deal with added
   * arguments.
   */
  public static void alterPartition(IMetaStoreClient client, Partition partition)
      throws InvalidOperationException, MetaException, TException {
    client.alter_partition(
        partition.getDbName(), partition.getTableName(), partition, null);
  }

  /**
   * Wrapper around IMetaStoreClient.alter_partitions() to deal with added
   * arguments.
   */
  public static void alterPartitions(IMetaStoreClient client, String dbName,
      String tableName, List<Partition> partitions)
      throws InvalidOperationException, MetaException, TException {
    client.alter_partitions(dbName, tableName, partitions, null);
  }

  /**
   * Wrapper around MetaStoreUtils.updatePartitionStatsFast() to deal with added
   * arguments.
   */
  public static void updatePartitionStatsFast(Partition partition, Table tbl,
      Warehouse warehouse) throws MetaException {
    MetaStoreUtils.updatePartitionStatsFast(partition, warehouse, null);
  }

  /**
   * Return the maximum number of Metastore objects that should be retrieved in
   * a batch.
   */
  public static String metastoreBatchRetrieveObjectsMaxConfigKey() {
    return HiveConf.ConfVars.METASTORE_BATCH_RETRIEVE_OBJECTS_MAX.toString();
  }

  /**
   * Return the key and value that should be set in the partition parameters to
   * mark that the stats were generated automatically by a stats task.
   */
  public static Pair<String, String> statsGeneratedViaStatsTaskParam() {
    return Pair.create(StatsSetupConst.STATS_GENERATED, StatsSetupConst.TASK);
  }

  public static TResultSet execGetFunctions(
      Frontend frontend, TMetadataOpRequest request, User user) throws ImpalaException {
    TGetFunctionsReq req = request.getGet_functions_req();
    return MetadataOp.getFunctions(
        frontend, req.getCatalogName(), req.getSchemaName(), req.getFunctionName(), user);
  }

  public static TResultSet execGetColumns(
      Frontend frontend, TMetadataOpRequest request, User user) throws ImpalaException {
    TGetColumnsReq req = request.getGet_columns_req();
    return MetadataOp.getColumns(frontend, req.getCatalogName(), req.getSchemaName(),
        req.getTableName(), req.getColumnName(), user);
  }

  public static TResultSet execGetTables(
      Frontend frontend, TMetadataOpRequest request, User user) throws ImpalaException {
    TGetTablesReq req = request.getGet_tables_req();
    return MetadataOp.getTables(frontend, req.getCatalogName(), req.getSchemaName(),
        req.getTableName(), req.getTableTypes(), user);
  }

  public static TResultSet execGetSchemas(
      Frontend frontend, TMetadataOpRequest request, User user) throws ImpalaException {
    TGetSchemasReq req = request.getGet_schemas_req();
    return MetadataOp.getSchemas(
        frontend, req.getCatalogName(), req.getSchemaName(), user);
  }

  /**
   * Supported HMS-2 types
   */
  public static final EnumSet<TableType> IMPALA_SUPPORTED_TABLE_TYPES = EnumSet
      .of(TableType.EXTERNAL_TABLE, TableType.MANAGED_TABLE, TableType.VIRTUAL_VIEW);

  /**
   * mapping between the HMS-2 type the Impala types
   */
  public static final ImmutableMap<String, String> HMS_TO_IMPALA_TYPE =
      new ImmutableMap.Builder<String, String>()
          .put("EXTERNAL_TABLE", TABLE_TYPE_TABLE)
          .put("MANAGED_TABLE", TABLE_TYPE_TABLE)
          .put("INDEX_TABLE", TABLE_TYPE_TABLE)
          .put("VIRTUAL_VIEW", TABLE_TYPE_VIEW).build();

  /**
   * Wrapper method which returns ExtendedJSONMessageFactory in case Impala is
   * building against Hive-2 to keep compatibility with Sentry
   */
  public static MessageDeserializer getMessageDeserializer() {
    return ExtendedJSONMessageFactory.getInstance().getDeserializer();
  }

  /**
   * Wrapper around FileUtils.makePartName to deal with package relocation in Hive 3
   * @param partitionColNames
   * @param values
   * @return
   */
  public static String makePartName(List<String> partitionColNames, List<String> values) {
    return FileUtils.makePartName(partitionColNames, values);
  }

  /**
   * Wrapper method around message factory's build alter table message due to added
   * arguments in hive 3.
   */
  @VisibleForTesting
  public static AlterTableMessage buildAlterTableMessage(Table before, Table after,
      boolean isTruncateOp, long writeId) {
    Preconditions.checkArgument(writeId < 0, "Write ids are not supported in Hive-2 "
        + "compatible build");
    Preconditions.checkArgument(!isTruncateOp, "Truncate operation is not supported in "
        + "alter table messages in Hive-2 compatible build");
    return ExtendedJSONMessageFactory.getInstance().buildAlterTableMessage(before, after);
  }

  @VisibleForTesting
  public static InsertMessage buildInsertMessage(Table msTbl, Partition partition,
      boolean isInsertOverwrite, List<String> newFiles) {
    return ExtendedJSONMessageFactory.getInstance().buildInsertMessage(msTbl, partition,
        isInsertOverwrite, newFiles);
  }

  public static String getAllColumnsInformation(List<FieldSchema> tabCols,
      List<FieldSchema> partitionCols, boolean printHeader, boolean isOutputPadded,
      boolean showPartColsSeparately) {
    return MetaDataFormatUtils
        .getAllColumnsInformation(tabCols, partitionCols, printHeader, isOutputPadded,
            showPartColsSeparately);
  }

  /**
   * Wrapper method around Hive's MetadataFormatUtils.getTableInformation which has
   * changed significantly in Hive-3
   * @return
   */
  public static String getTableInformation(
      org.apache.hadoop.hive.ql.metadata.Table table) {
    return MetaDataFormatUtils.getTableInformation(table);
  }

  /**
   * Wrapper method around BaseSemanticAnalyzer's unespaceSQLString to be compatibility
   * with Hive. Takes in a normalized value of the string surrounded by single quotes
   */
  public static String unescapeSQLString(String normalizedStringLiteral) {
    return BaseSemanticAnalyzer.unescapeSQLString(normalizedStringLiteral);
  }
}
