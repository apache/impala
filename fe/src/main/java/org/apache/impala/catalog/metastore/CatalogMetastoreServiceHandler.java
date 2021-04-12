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

package org.apache.impala.catalog.metastore;

import static org.apache.impala.catalog.metastore.HmsApiNameEnum.GET_PARTITION_BY_NAMES;

import org.apache.hadoop.hive.metastore.api.GetFieldsRequest;
import org.apache.hadoop.hive.metastore.api.GetFieldsResponse;
import org.apache.hadoop.hive.metastore.api.GetPartitionsByNamesRequest;
import org.apache.hadoop.hive.metastore.api.GetPartitionsByNamesResult;
import org.apache.hadoop.hive.metastore.api.GetPartitionNamesPsResponse;
import org.apache.hadoop.hive.metastore.api.GetPartitionNamesPsRequest;
import org.apache.hadoop.hive.metastore.api.GetPartitionRequest;
import org.apache.hadoop.hive.metastore.api.GetPartitionResponse;
import org.apache.hadoop.hive.metastore.api.GetPartitionsPsWithAuthRequest;
import org.apache.hadoop.hive.metastore.api.GetPartitionsPsWithAuthResponse;
import org.apache.hadoop.hive.metastore.api.GetSchemaRequest;
import org.apache.hadoop.hive.metastore.api.GetSchemaResponse;
import org.apache.hadoop.hive.metastore.api.GetTableRequest;
import org.apache.hadoop.hive.metastore.api.GetTableResult;
import org.apache.hadoop.hive.metastore.api.MaxAllocatedTableWriteIdRequest;
import org.apache.hadoop.hive.metastore.api.MaxAllocatedTableWriteIdResponse;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.NoSuchObjectException;
import org.apache.hadoop.hive.metastore.api.PartitionsByExprRequest;
import org.apache.hadoop.hive.metastore.api.PartitionsByExprResult;
import org.apache.hadoop.hive.metastore.api.PartitionsRequest;
import org.apache.hadoop.hive.metastore.api.PartitionsResponse;
import org.apache.hadoop.hive.metastore.api.SeedTableWriteIdsRequest;
import org.apache.hadoop.hive.metastore.api.SeedTxnIdRequest;
import org.apache.impala.catalog.CatalogException;
import org.apache.impala.catalog.CatalogHmsAPIHelper;
import org.apache.impala.catalog.CatalogServiceCatalog;
import org.apache.impala.common.Metrics;
import org.apache.impala.catalog.MetaStoreClientPool.MetaStoreClient;
import org.apache.impala.compat.MetastoreShim;
import org.apache.impala.service.BackendConfig;
import org.apache.impala.service.CatalogOpExecutor;
import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class implements the HMS APIs that are served by CatalogD
 * and is exposed via {@link CatalogMetastoreServer}.
 * HMS APIs that are redirected to HMS can be found in {@link MetastoreServiceHandler}.
 *
 */
public class CatalogMetastoreServiceHandler extends MetastoreServiceHandler {

  private static final Logger LOG = LoggerFactory
      .getLogger(CatalogMetastoreServiceHandler.class);

  public CatalogMetastoreServiceHandler(CatalogOpExecutor catalogOpExecutor,
      boolean fallBackToHMSOnErrors) {
    super(catalogOpExecutor, fallBackToHMSOnErrors);
  }

  @Override
  public GetTableResult get_table_req(GetTableRequest getTableRequest)
      throws MetaException, NoSuchObjectException, TException {

    if (!BackendConfig.INSTANCE.enableCatalogdHMSCache()) {
      return super.get_table_req(getTableRequest);
    }

    try {
      LOG.trace("Received get_Table_req for {}. File metadata is {}",
          getTableRequest.getTblName(), getTableRequest.isGetFileMetadata());
      return CatalogHmsAPIHelper.getTableReq(catalog_, defaultCatalogName_,
          getTableRequest);
    } catch (Exception e) {
      // we catch the CatalogException and fall-back to HMS
      throwIfNoFallback(e, "get_table_req");
    }
    return super.get_table_req(getTableRequest);
  }

  /**
   * This is the main API which is used by Hive to get the partitions. In case of Hive it
   * pushes the pruning logic to HMS by sending over the expression which is used to
   * filter the partitions during query compilation. The expression is specific to Hive
   * and loaded in the runtime based on whether we have hive-exec jar in the classpath
   * or not. If the hive-exec jar is not present in the classpath, we fall-back to HMS
   * since Catalog has no way to deserialize the expression sent over by the client.
   */
  @Override
  public PartitionsByExprResult get_partitions_by_expr(
      PartitionsByExprRequest partitionsByExprRequest) throws TException {

    if (!BackendConfig.INSTANCE.enableCatalogdHMSCache()) {
      return super.get_partitions_by_expr(partitionsByExprRequest);
    }

    try {
      // expressionProxy is null or if there were errors when loading the
      // PartitionExpressionProxy.
      if (expressionProxy_ != null) {
        return CatalogHmsAPIHelper.getPartitionsByExpr(
            catalog_, defaultCatalogName_, partitionsByExprRequest, expressionProxy_);
      } else {
        throw new CatalogException("PartitionExpressionProxy could not be initialized");
      }
    } catch (Exception e) {
      // we catch the CatalogException and fall-back to HMS
      throwIfNoFallback(e, HmsApiNameEnum.GET_PARTITION_BY_EXPR.apiName());
    }
    String tblName =
        partitionsByExprRequest.getDbName() + "." + partitionsByExprRequest.getTblName();
    LOG.info(String
        .format(HMS_FALLBACK_MSG_FORMAT, HmsApiNameEnum.GET_PARTITION_BY_EXPR.apiName(),
            tblName));
    return super.get_partitions_by_expr(partitionsByExprRequest);
  }

  /**
   * HMS API to get the partitions filtered by a provided list of names. The request
   * contains a list of partitions names which the client is interested in. Catalog
   * returns the partitions only for requested names. Additionally, this API also returns
   * the file-metadata for the returned partitions if the request has
   * {@code getFileMetadata} flag set. In case of errors, this API falls back to HMS if
   * {@code fallBackToHMSOnErrors_} is set.
   */
  @Override
  public GetPartitionsByNamesResult get_partitions_by_names_req(
      GetPartitionsByNamesRequest getPartitionsByNamesRequest) throws TException {

    if (!BackendConfig.INSTANCE.enableCatalogdHMSCache()) {
      return super.get_partitions_by_names_req(getPartitionsByNamesRequest);
    }

    try {
      return CatalogHmsAPIHelper
          .getPartitionsByNames(catalog_, serverConf_, getPartitionsByNamesRequest);
    } catch (Exception ex) {
      throwIfNoFallback(ex, GET_PARTITION_BY_NAMES.apiName());
    }
    String tblName =
        getPartitionsByNamesRequest.getDb_name() + "." + getPartitionsByNamesRequest
            .getTbl_name();
    LOG.info(String.format(HMS_FALLBACK_MSG_FORMAT, GET_PARTITION_BY_NAMES, tblName));
    return super.get_partitions_by_names_req(getPartitionsByNamesRequest);
  }
}
