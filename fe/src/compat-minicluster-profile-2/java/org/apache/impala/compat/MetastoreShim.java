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

import java.util.List;

import org.apache.hadoop.hive.common.StatsSetupConst;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.IMetaStoreClient;
import org.apache.hadoop.hive.metastore.MetaStoreUtils;
import org.apache.hadoop.hive.metastore.Warehouse;
import org.apache.hadoop.hive.metastore.api.InvalidOperationException;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hive.service.cli.thrift.TGetColumnsReq;
import org.apache.hive.service.cli.thrift.TGetFunctionsReq;
import org.apache.hive.service.cli.thrift.TGetSchemasReq;
import org.apache.hive.service.cli.thrift.TGetTablesReq;
import org.apache.impala.authorization.User;
import org.apache.impala.common.Pair;
import org.apache.impala.common.ImpalaException;
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
    return MetaStoreUtils.validateName(name);
  }

  /**
   * Wrapper around IMetaStoreClient.alter_partition() to deal with added
   * arguments.
   */
  public static void alterPartition(IMetaStoreClient client, Partition partition)
      throws InvalidOperationException, MetaException, TException {
    client.alter_partition(partition.getDbName(), partition.getTableName(), partition);
  }

  /**
   * Wrapper around IMetaStoreClient.alter_partitions() to deal with added
   * arguments.
   */
  public static void alterPartitions(IMetaStoreClient client, String dbName,
      String tableName, List<Partition> partitions)
      throws InvalidOperationException, MetaException, TException {
    client.alter_partitions(dbName, tableName, partitions);
  }

  /**
   * Wrapper around MetaStoreUtils.updatePartitionStatsFast() to deal with added
   * arguments.
   */
  public static void updatePartitionStatsFast(Partition partition, Warehouse warehouse)
      throws MetaException {
    MetaStoreUtils.updatePartitionStatsFast(partition, warehouse);
  }

  /**
   * Return the maximum number of Metastore objects that should be retrieved in
   * a batch.
   */
  public static String metastoreBatchRetrieveObjectsMaxConfigKey() {
    return HiveConf.ConfVars.METASTORE_BATCH_RETRIEVE_TABLE_PARTITION_MAX.toString();
  }

  /**
   * Return the key and value that should be set in the partition parameters to
   * mark that the stats were generated automatically by a stats task.
   */
  public static Pair<String, String> statsGeneratedViaStatsTaskParam() {
    return Pair.create(
        StatsSetupConst.STATS_GENERATED_VIA_STATS_TASK, StatsSetupConst.TRUE);
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
}
