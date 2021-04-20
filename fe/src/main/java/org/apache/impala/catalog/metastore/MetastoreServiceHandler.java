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

import com.facebook.fb303.fb_status;
import com.google.common.base.Preconditions;
import java.util.List;
import java.util.Map;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.common.ValidTxnList;
import org.apache.hadoop.hive.common.ValidWriteIdList;
import org.apache.hadoop.hive.metastore.AbstractThriftHiveMetastore;
import org.apache.hadoop.hive.metastore.DefaultPartitionExpressionProxy;
import org.apache.hadoop.hive.metastore.IMetaStoreClient;
import org.apache.hadoop.hive.metastore.PartFilterExprUtil;
import org.apache.hadoop.hive.metastore.PartitionExpressionProxy;
import org.apache.hadoop.hive.metastore.api.AbortTxnRequest;
import org.apache.hadoop.hive.metastore.api.AbortTxnsRequest;
import org.apache.hadoop.hive.metastore.api.AddCheckConstraintRequest;
import org.apache.hadoop.hive.metastore.api.AddDefaultConstraintRequest;
import org.apache.hadoop.hive.metastore.api.AddDynamicPartitions;
import org.apache.hadoop.hive.metastore.api.AddForeignKeyRequest;
import org.apache.hadoop.hive.metastore.api.AddNotNullConstraintRequest;
import org.apache.hadoop.hive.metastore.api.AddPartitionsRequest;
import org.apache.hadoop.hive.metastore.api.AddPartitionsResult;
import org.apache.hadoop.hive.metastore.api.AddPrimaryKeyRequest;
import org.apache.hadoop.hive.metastore.api.AddUniqueConstraintRequest;
import org.apache.hadoop.hive.metastore.api.AggrStats;
import org.apache.hadoop.hive.metastore.api.AllocateTableWriteIdsRequest;
import org.apache.hadoop.hive.metastore.api.AllocateTableWriteIdsResponse;
import org.apache.hadoop.hive.metastore.api.AlreadyExistsException;
import org.apache.hadoop.hive.metastore.api.AlterCatalogRequest;
import org.apache.hadoop.hive.metastore.api.AlterISchemaRequest;
import org.apache.hadoop.hive.metastore.api.AlterPartitionsRequest;
import org.apache.hadoop.hive.metastore.api.AlterPartitionsResponse;
import org.apache.hadoop.hive.metastore.api.AlterTableRequest;
import org.apache.hadoop.hive.metastore.api.AlterTableResponse;
import org.apache.hadoop.hive.metastore.api.CacheFileMetadataRequest;
import org.apache.hadoop.hive.metastore.api.CacheFileMetadataResult;
import org.apache.hadoop.hive.metastore.api.CheckConstraintsRequest;
import org.apache.hadoop.hive.metastore.api.CheckConstraintsResponse;
import org.apache.hadoop.hive.metastore.api.CheckLockRequest;
import org.apache.hadoop.hive.metastore.api.ClearFileMetadataRequest;
import org.apache.hadoop.hive.metastore.api.ClearFileMetadataResult;
import org.apache.hadoop.hive.metastore.api.CmRecycleRequest;
import org.apache.hadoop.hive.metastore.api.CmRecycleResponse;
import org.apache.hadoop.hive.metastore.api.ColumnStatistics;
import org.apache.hadoop.hive.metastore.api.CommitTxnRequest;
import org.apache.hadoop.hive.metastore.api.CompactionInfoStruct;
import org.apache.hadoop.hive.metastore.api.CompactionRequest;
import org.apache.hadoop.hive.metastore.api.CompactionResponse;
import org.apache.hadoop.hive.metastore.api.ConfigValSecurityException;
import org.apache.hadoop.hive.metastore.api.CreateCatalogRequest;
import org.apache.hadoop.hive.metastore.api.CreateTableRequest;
import org.apache.hadoop.hive.metastore.api.CreationMetadata;
import org.apache.hadoop.hive.metastore.api.CurrentNotificationEventId;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.DefaultConstraintsRequest;
import org.apache.hadoop.hive.metastore.api.DefaultConstraintsResponse;
import org.apache.hadoop.hive.metastore.api.DropCatalogRequest;
import org.apache.hadoop.hive.metastore.api.DropConstraintRequest;
import org.apache.hadoop.hive.metastore.api.DropPartitionsRequest;
import org.apache.hadoop.hive.metastore.api.DropPartitionsResult;
import org.apache.hadoop.hive.metastore.api.EnvironmentContext;
import org.apache.hadoop.hive.metastore.api.ExtendedTableInfo;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.FindSchemasByColsResp;
import org.apache.hadoop.hive.metastore.api.FindSchemasByColsRqst;
import org.apache.hadoop.hive.metastore.api.FireEventRequest;
import org.apache.hadoop.hive.metastore.api.FireEventResponse;
import org.apache.hadoop.hive.metastore.api.ForeignKeysRequest;
import org.apache.hadoop.hive.metastore.api.ForeignKeysResponse;
import org.apache.hadoop.hive.metastore.api.Function;
import org.apache.hadoop.hive.metastore.api.GetAllFunctionsResponse;
import org.apache.hadoop.hive.metastore.api.GetCatalogRequest;
import org.apache.hadoop.hive.metastore.api.GetCatalogResponse;
import org.apache.hadoop.hive.metastore.api.GetCatalogsResponse;
import org.apache.hadoop.hive.metastore.api.GetDatabaseRequest;
import org.apache.hadoop.hive.metastore.api.GetFieldsRequest;
import org.apache.hadoop.hive.metastore.api.GetFieldsResponse;
import org.apache.hadoop.hive.metastore.api.GetFileMetadataByExprRequest;
import org.apache.hadoop.hive.metastore.api.GetFileMetadataByExprResult;
import org.apache.hadoop.hive.metastore.api.GetFileMetadataRequest;
import org.apache.hadoop.hive.metastore.api.GetFileMetadataResult;
import org.apache.hadoop.hive.metastore.api.GetOpenTxnsInfoResponse;
import org.apache.hadoop.hive.metastore.api.GetOpenTxnsResponse;
import org.apache.hadoop.hive.metastore.api.GetPartitionsByNamesRequest;
import org.apache.hadoop.hive.metastore.api.GetPartitionsByNamesResult;
import org.apache.hadoop.hive.metastore.api.GetPartitionsResponse;
import org.apache.hadoop.hive.metastore.api.GetPartitionsRequest;
import org.apache.hadoop.hive.metastore.api.GetPartitionNamesPsResponse;
import org.apache.hadoop.hive.metastore.api.GetPartitionNamesPsRequest;
import org.apache.hadoop.hive.metastore.api.GetPartitionRequest;
import org.apache.hadoop.hive.metastore.api.GetPartitionResponse;
import org.apache.hadoop.hive.metastore.api.GetPartitionsPsWithAuthRequest;
import org.apache.hadoop.hive.metastore.api.GetPartitionsPsWithAuthResponse;
import org.apache.hadoop.hive.metastore.api.GetPrincipalsInRoleRequest;
import org.apache.hadoop.hive.metastore.api.GetPrincipalsInRoleResponse;
import org.apache.hadoop.hive.metastore.api.GetReplicationMetricsRequest;
import org.apache.hadoop.hive.metastore.api.GetRoleGrantsForPrincipalRequest;
import org.apache.hadoop.hive.metastore.api.GetRoleGrantsForPrincipalResponse;
import org.apache.hadoop.hive.metastore.api.GetRuntimeStatsRequest;
import org.apache.hadoop.hive.metastore.api.GetSchemaRequest;
import org.apache.hadoop.hive.metastore.api.GetSchemaResponse;
import org.apache.hadoop.hive.metastore.api.GetSerdeRequest;
import org.apache.hadoop.hive.metastore.api.GetTableRequest;
import org.apache.hadoop.hive.metastore.api.GetTableResult;
import org.apache.hadoop.hive.metastore.api.GetTablesExtRequest;
import org.apache.hadoop.hive.metastore.api.GetTablesRequest;
import org.apache.hadoop.hive.metastore.api.GetTablesResult;
import org.apache.hadoop.hive.metastore.api.GetValidWriteIdsRequest;
import org.apache.hadoop.hive.metastore.api.GetValidWriteIdsResponse;
import org.apache.hadoop.hive.metastore.api.GrantRevokePrivilegeRequest;
import org.apache.hadoop.hive.metastore.api.GrantRevokePrivilegeResponse;
import org.apache.hadoop.hive.metastore.api.GrantRevokeRoleRequest;
import org.apache.hadoop.hive.metastore.api.GrantRevokeRoleResponse;
import org.apache.hadoop.hive.metastore.api.HeartbeatRequest;
import org.apache.hadoop.hive.metastore.api.HeartbeatTxnRangeRequest;
import org.apache.hadoop.hive.metastore.api.HeartbeatTxnRangeResponse;
import org.apache.hadoop.hive.metastore.api.HiveObjectPrivilege;
import org.apache.hadoop.hive.metastore.api.HiveObjectRef;
import org.apache.hadoop.hive.metastore.api.ISchema;
import org.apache.hadoop.hive.metastore.api.ISchemaName;
import org.apache.hadoop.hive.metastore.api.InvalidInputException;
import org.apache.hadoop.hive.metastore.api.InvalidObjectException;
import org.apache.hadoop.hive.metastore.api.InvalidOperationException;
import org.apache.hadoop.hive.metastore.api.LockRequest;
import org.apache.hadoop.hive.metastore.api.LockResponse;
import org.apache.hadoop.hive.metastore.api.MapSchemaVersionToSerdeRequest;
import org.apache.hadoop.hive.metastore.api.Materialization;
import org.apache.hadoop.hive.metastore.api.MaxAllocatedTableWriteIdRequest;
import org.apache.hadoop.hive.metastore.api.MaxAllocatedTableWriteIdResponse;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.NoSuchLockException;
import org.apache.hadoop.hive.metastore.api.NoSuchObjectException;
import org.apache.hadoop.hive.metastore.api.NoSuchTxnException;
import org.apache.hadoop.hive.metastore.api.NotNullConstraintsRequest;
import org.apache.hadoop.hive.metastore.api.NotNullConstraintsResponse;
import org.apache.hadoop.hive.metastore.api.NotificationEventRequest;
import org.apache.hadoop.hive.metastore.api.NotificationEventResponse;
import org.apache.hadoop.hive.metastore.api.NotificationEventsCountRequest;
import org.apache.hadoop.hive.metastore.api.NotificationEventsCountResponse;
import org.apache.hadoop.hive.metastore.api.OpenTxnRequest;
import org.apache.hadoop.hive.metastore.api.OpenTxnsResponse;
import org.apache.hadoop.hive.metastore.api.OptionalCompactionInfoStruct;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.PartitionEventType;
import org.apache.hadoop.hive.metastore.api.PartitionSpec;
import org.apache.hadoop.hive.metastore.api.PartitionValuesRequest;
import org.apache.hadoop.hive.metastore.api.PartitionValuesResponse;
import org.apache.hadoop.hive.metastore.api.PartitionsByExprRequest;
import org.apache.hadoop.hive.metastore.api.PartitionsByExprResult;
import org.apache.hadoop.hive.metastore.api.PartitionsRequest;
import org.apache.hadoop.hive.metastore.api.PartitionsResponse;
import org.apache.hadoop.hive.metastore.api.PartitionsStatsRequest;
import org.apache.hadoop.hive.metastore.api.PartitionsStatsResult;
import org.apache.hadoop.hive.metastore.api.PrimaryKeysRequest;
import org.apache.hadoop.hive.metastore.api.PrimaryKeysResponse;
import org.apache.hadoop.hive.metastore.api.PrincipalPrivilegeSet;
import org.apache.hadoop.hive.metastore.api.PrincipalType;
import org.apache.hadoop.hive.metastore.api.PrivilegeBag;
import org.apache.hadoop.hive.metastore.api.PutFileMetadataRequest;
import org.apache.hadoop.hive.metastore.api.PutFileMetadataResult;
import org.apache.hadoop.hive.metastore.api.RenamePartitionRequest;
import org.apache.hadoop.hive.metastore.api.RenamePartitionResponse;
import org.apache.hadoop.hive.metastore.api.ReplicationMetricList;
import org.apache.hadoop.hive.metastore.api.ReplTblWriteIdStateRequest;
import org.apache.hadoop.hive.metastore.api.Role;
import org.apache.hadoop.hive.metastore.api.RuntimeStat;
import org.apache.hadoop.hive.metastore.api.SQLCheckConstraint;
import org.apache.hadoop.hive.metastore.api.SQLDefaultConstraint;
import org.apache.hadoop.hive.metastore.api.SQLForeignKey;
import org.apache.hadoop.hive.metastore.api.SQLNotNullConstraint;
import org.apache.hadoop.hive.metastore.api.SQLPrimaryKey;
import org.apache.hadoop.hive.metastore.api.SQLUniqueConstraint;
import org.apache.hadoop.hive.metastore.api.ScheduledQuery;
import org.apache.hadoop.hive.metastore.api.ScheduledQueryKey;
import org.apache.hadoop.hive.metastore.api.ScheduledQueryMaintenanceRequest;
import org.apache.hadoop.hive.metastore.api.ScheduledQueryPollRequest;
import org.apache.hadoop.hive.metastore.api.ScheduledQueryPollResponse;
import org.apache.hadoop.hive.metastore.api.ScheduledQueryProgressInfo;
import org.apache.hadoop.hive.metastore.api.SchemaVersion;
import org.apache.hadoop.hive.metastore.api.SchemaVersionDescriptor;
import org.apache.hadoop.hive.metastore.api.SeedTableWriteIdsRequest;
import org.apache.hadoop.hive.metastore.api.SeedTxnIdRequest;
import org.apache.hadoop.hive.metastore.api.SerDeInfo;
import org.apache.hadoop.hive.metastore.api.SetPartitionsStatsRequest;
import org.apache.hadoop.hive.metastore.api.SetPartitionsStatsResponse;
import org.apache.hadoop.hive.metastore.api.SetSchemaVersionStateRequest;
import org.apache.hadoop.hive.metastore.api.ShowCompactRequest;
import org.apache.hadoop.hive.metastore.api.ShowCompactResponse;
import org.apache.hadoop.hive.metastore.api.ShowLocksRequest;
import org.apache.hadoop.hive.metastore.api.ShowLocksResponse;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.metastore.api.TableMeta;
import org.apache.hadoop.hive.metastore.api.TableStatsRequest;
import org.apache.hadoop.hive.metastore.api.TableStatsResult;
import org.apache.hadoop.hive.metastore.api.ThriftHiveMetastore.Iface;
import org.apache.hadoop.hive.metastore.api.TruncateTableRequest;
import org.apache.hadoop.hive.metastore.api.TruncateTableResponse;
import org.apache.hadoop.hive.metastore.api.TxnAbortedException;
import org.apache.hadoop.hive.metastore.api.TxnOpenException;
import org.apache.hadoop.hive.metastore.api.Type;
import org.apache.hadoop.hive.metastore.api.UniqueConstraintsRequest;
import org.apache.hadoop.hive.metastore.api.UniqueConstraintsResponse;
import org.apache.hadoop.hive.metastore.api.UnknownDBException;
import org.apache.hadoop.hive.metastore.api.UnknownTableException;
import org.apache.hadoop.hive.metastore.api.UnlockRequest;
import org.apache.hadoop.hive.metastore.api.WMAlterPoolRequest;
import org.apache.hadoop.hive.metastore.api.WMAlterPoolResponse;
import org.apache.hadoop.hive.metastore.api.WMAlterResourcePlanRequest;
import org.apache.hadoop.hive.metastore.api.WMAlterResourcePlanResponse;
import org.apache.hadoop.hive.metastore.api.WMAlterTriggerRequest;
import org.apache.hadoop.hive.metastore.api.WMAlterTriggerResponse;
import org.apache.hadoop.hive.metastore.api.WMCreateOrDropTriggerToPoolMappingRequest;
import org.apache.hadoop.hive.metastore.api.WMCreateOrDropTriggerToPoolMappingResponse;
import org.apache.hadoop.hive.metastore.api.WMCreateOrUpdateMappingRequest;
import org.apache.hadoop.hive.metastore.api.WMCreateOrUpdateMappingResponse;
import org.apache.hadoop.hive.metastore.api.WMCreatePoolRequest;
import org.apache.hadoop.hive.metastore.api.WMCreatePoolResponse;
import org.apache.hadoop.hive.metastore.api.WMCreateResourcePlanRequest;
import org.apache.hadoop.hive.metastore.api.WMCreateResourcePlanResponse;
import org.apache.hadoop.hive.metastore.api.WMCreateTriggerRequest;
import org.apache.hadoop.hive.metastore.api.WMCreateTriggerResponse;
import org.apache.hadoop.hive.metastore.api.WMDropMappingRequest;
import org.apache.hadoop.hive.metastore.api.WMDropMappingResponse;
import org.apache.hadoop.hive.metastore.api.WMDropPoolRequest;
import org.apache.hadoop.hive.metastore.api.WMDropPoolResponse;
import org.apache.hadoop.hive.metastore.api.WMDropResourcePlanRequest;
import org.apache.hadoop.hive.metastore.api.WMDropResourcePlanResponse;
import org.apache.hadoop.hive.metastore.api.WMDropTriggerRequest;
import org.apache.hadoop.hive.metastore.api.WMDropTriggerResponse;
import org.apache.hadoop.hive.metastore.api.WMGetActiveResourcePlanRequest;
import org.apache.hadoop.hive.metastore.api.WMGetActiveResourcePlanResponse;
import org.apache.hadoop.hive.metastore.api.WMGetAllResourcePlanRequest;
import org.apache.hadoop.hive.metastore.api.WMGetAllResourcePlanResponse;
import org.apache.hadoop.hive.metastore.api.WMGetResourcePlanRequest;
import org.apache.hadoop.hive.metastore.api.WMGetResourcePlanResponse;
import org.apache.hadoop.hive.metastore.api.WMGetTriggersForResourePlanRequest;
import org.apache.hadoop.hive.metastore.api.WMGetTriggersForResourePlanResponse;
import org.apache.hadoop.hive.metastore.api.WMValidateResourcePlanRequest;
import org.apache.hadoop.hive.metastore.api.WMValidateResourcePlanResponse;
import org.apache.hadoop.hive.metastore.api.WriteNotificationLogRequest;
import org.apache.hadoop.hive.metastore.api.WriteNotificationLogResponse;
import org.apache.hadoop.hive.metastore.conf.MetastoreConf;
import org.apache.hadoop.hive.metastore.conf.MetastoreConf.ConfVars;
import org.apache.hadoop.hive.metastore.utils.MetaStoreUtils;
import org.apache.impala.catalog.CatalogHmsAPIHelper;
import org.apache.impala.catalog.CatalogServiceCatalog;
import org.apache.impala.catalog.MetaStoreClientPool.MetaStoreClient;
import org.apache.impala.compat.MetastoreShim;
import org.apache.impala.util.AcidUtils;
import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class implements the HMS APIs that are redirected to the HMS server from CatalogD.
 * APIs that should be served from CatalogD must be overridden in {@link
 * CatalogMetastoreServer}.
 * <p>
 * Implementation Notes: Care should taken to use
 * {@link IMetaStoreClient#getThriftClient()}
 * method when forwarding a API call to HMS service since IMetastoreClient itself modifies
 * the arguments before sending the RPC to the HMS server. This can lead to unexpected
 * side-effects like (processorCapabilities do not match with the actual client).
 */
public abstract class MetastoreServiceHandler extends AbstractThriftHiveMetastore {

  private static final Logger LOG = LoggerFactory
      .getLogger(MetastoreServiceHandler.class);
  protected static final String NOT_IMPLEMENTED_UNSUPPORTED = "%s method not supported"
      + " by Catalog metastore service.";
  protected static final String METAEXCEPTION_MSG_FORMAT =
      "Unexpected error occurred while"
          + " executing %s. Cause: %s. See catalog logs for details.";
  protected static final String HMS_FALLBACK_MSG_FORMAT = "Forwarding the request %s for "
      + "table %s to the backing HiveMetastore service";

  // constant used for logging error messages
  protected final CatalogServiceCatalog catalog_;
  protected final boolean fallBackToHMSOnErrors_;
  // TODO handle session configuration
  protected Configuration serverConf_;
  protected PartitionExpressionProxy expressionProxy_;
  protected final String defaultCatalogName_;

  public MetastoreServiceHandler(CatalogServiceCatalog catalog,
      boolean fallBackToHMSOnErrors) {
    catalog_ = Preconditions.checkNotNull(catalog);
    fallBackToHMSOnErrors_ = fallBackToHMSOnErrors;
    LOG.info("Fallback to hive metastore service on errors is {}",
        fallBackToHMSOnErrors_);
    // load the metastore configuration from the classpath
    serverConf_ = Preconditions.checkNotNull(MetastoreConf.newMetastoreConf());
    String className = MetastoreConf
        .get(serverConf_, ConfVars.EXPRESSION_PROXY_CLASS.getVarname());
    try {
      Preconditions.checkNotNull(className);
      LOG.info("Instantiating {}", className);
      expressionProxy_ = PartFilterExprUtil.createExpressionProxy(serverConf_);
      if (expressionProxy_ instanceof DefaultPartitionExpressionProxy) {
        LOG.error("PartFilterExprUtil.createExpressionProxy returned"
            + " DefaultPartitionExpressionProxy. Check if hive-exec"
            + " jar is available in the classpath.");
        expressionProxy_ = null;
      }
    } catch (Exception ex) {
      LOG.error("Could not instantiate {}", className, ex);
    }
    defaultCatalogName_ =
        MetaStoreUtils.getDefaultCatalog(serverConf_);
  }

  @Override
  public String get_hms_api_version() throws MetaException, TException {
    try (MetaStoreClient client = catalog_.getMetaStoreClient()) {
      return client.getHiveClient().getThriftClient().get_hms_api_version();
    }
  }

  @Override
  public String getMetaConf(String configKey) throws MetaException, TException {
    try (MetaStoreClient client = catalog_.getMetaStoreClient()) {
      return client.getHiveClient().getThriftClient().getMetaConf(configKey);
    }
  }

  @Override
  public void setMetaConf(String configKey, String configValue)
      throws MetaException, TException {
    try (MetaStoreClient client = catalog_.getMetaStoreClient()) {
      client.getHiveClient().getThriftClient().setMetaConf(configKey, configValue);
    }
  }

  @Override
  public void create_catalog(CreateCatalogRequest createCatalogRequest)
      throws AlreadyExistsException, InvalidObjectException, MetaException, TException {
    try (MetaStoreClient client = catalog_.getMetaStoreClient()) {
      client.getHiveClient().getThriftClient().create_catalog(createCatalogRequest);
    }
  }

  @Override
  public void alter_catalog(AlterCatalogRequest alterCatalogRequest)
      throws NoSuchObjectException, InvalidOperationException, MetaException, TException {
    try (MetaStoreClient client = catalog_.getMetaStoreClient()) {
      client.getHiveClient().getThriftClient().alter_catalog(alterCatalogRequest);
    }
  }

  @Override
  public GetCatalogResponse get_catalog(GetCatalogRequest getCatalogRequest)
      throws NoSuchObjectException, MetaException, TException {
    try (MetaStoreClient client = catalog_.getMetaStoreClient()) {
      return client.getHiveClient().getThriftClient().get_catalog(getCatalogRequest);
    }
  }

  @Override
  public GetCatalogsResponse get_catalogs() throws MetaException, TException {
    try (MetaStoreClient client = catalog_.getMetaStoreClient()) {
      return client.getHiveClient().getThriftClient().get_catalogs();
    }
  }

  @Override
  public void drop_catalog(DropCatalogRequest dropCatalogRequest)
      throws NoSuchObjectException, InvalidOperationException, MetaException, TException {
    try (MetaStoreClient client = catalog_.getMetaStoreClient()) {
      client.getHiveClient().getThriftClient().drop_catalog(dropCatalogRequest);
    }
  }

  @Override
  public void create_database(Database database)
      throws AlreadyExistsException, InvalidObjectException, MetaException, TException {
    try (MetaStoreClient client = catalog_.getMetaStoreClient()) {
      client.getHiveClient().getThriftClient().create_database(database);
    }
  }

  @Override
  public Database get_database(String databaseName)
      throws NoSuchObjectException, MetaException, TException {
    try (MetaStoreClient client = catalog_.getMetaStoreClient()) {
      return client.getHiveClient().getThriftClient().get_database(databaseName);
    }
  }

  @Override
  public Database get_database_req(GetDatabaseRequest getDatabaseRequest)
      throws NoSuchObjectException, MetaException, TException {
    try (MetaStoreClient client = catalog_.getMetaStoreClient()) {
      return client.getHiveClient().getThriftClient()
          .get_database_req(getDatabaseRequest);
    }
  }

  @Override
  public void drop_database(String databaseName, boolean deleteData,
      boolean ignoreUnknownDb)
      throws NoSuchObjectException, InvalidOperationException, MetaException, TException {
    try (MetaStoreClient client = catalog_.getMetaStoreClient()) {
      client.getHiveClient().getThriftClient()
          .drop_database(databaseName, deleteData, ignoreUnknownDb);
    }
  }

  @Override
  public List<String> get_databases(String pattern) throws MetaException, TException {
    try (MetaStoreClient client = catalog_.getMetaStoreClient()) {
      return client.getHiveClient().getThriftClient().get_databases(pattern);
    }
  }

  @Override
  public List<String> get_all_databases() throws MetaException, TException {
    try (MetaStoreClient client = catalog_.getMetaStoreClient()) {
      return client.getHiveClient().getThriftClient().get_all_databases();
    }
  }

  @Override
  public void alter_database(String dbname, Database database)
      throws MetaException, NoSuchObjectException, TException {
    try (MetaStoreClient client = catalog_.getMetaStoreClient()) {
      client.getHiveClient().getThriftClient().alter_database(dbname, database);
    }
  }

  @Override
  public Type get_type(String name) throws MetaException, NoSuchObjectException,
      TException {
    try (MetaStoreClient client = catalog_.getMetaStoreClient()) {
      return client.getHiveClient().getThriftClient().get_type(name);
    }
  }

  @Override
  public boolean create_type(Type type)
      throws AlreadyExistsException, InvalidObjectException, MetaException, TException {
    try (MetaStoreClient client = catalog_.getMetaStoreClient()) {
      return client.getHiveClient().getThriftClient().create_type(type);
    }
  }

  @Override
  public boolean drop_type(String type)
      throws MetaException, NoSuchObjectException, TException {
    try (MetaStoreClient client = catalog_.getMetaStoreClient()) {
      return client.getHiveClient().getThriftClient().drop_type(type);
    }
  }

  @Override
  public Map<String, Type> get_type_all(String s) throws MetaException, TException {
    try (MetaStoreClient client = catalog_.getMetaStoreClient()) {
      return client.getHiveClient().getThriftClient().get_type_all(s);
    }
  }

  @Override
  public List<FieldSchema> get_fields(String dbname, String tblname)
      throws MetaException, UnknownTableException, UnknownDBException, TException {
    try (MetaStoreClient client = catalog_.getMetaStoreClient()) {
      return client.getHiveClient().getThriftClient().get_fields(dbname, tblname);
    }
  }

  @Override
  public List<FieldSchema> get_fields_with_environment_context(String dbName,
      String tblName, EnvironmentContext environmentContext)
      throws MetaException, UnknownTableException, UnknownDBException, TException {
    try (MetaStoreClient client = catalog_.getMetaStoreClient()) {
      return client.getHiveClient().getThriftClient()
          .get_fields_with_environment_context(dbName, tblName, environmentContext);
    }
  }

  @Override
  public List<FieldSchema> get_schema(String dbname, String tblname)
      throws MetaException, UnknownTableException, UnknownDBException, TException {
    try (MetaStoreClient client = catalog_.getMetaStoreClient()) {
      return client.getHiveClient().getThriftClient().get_schema(dbname, tblname);
    }
  }

  @Override
  public List<FieldSchema> get_schema_with_environment_context(String dbname,
      String tblname, EnvironmentContext environmentContext)
      throws MetaException, UnknownTableException, UnknownDBException, TException {
    try (MetaStoreClient client = catalog_.getMetaStoreClient()) {
      return client.getHiveClient().getThriftClient()
          .get_schema_with_environment_context(dbname, tblname, environmentContext);
    }
  }

  @Override
  public void create_table(Table table)
      throws TException {
    try (MetaStoreClient client = catalog_.getMetaStoreClient()) {
      client.getHiveClient().getThriftClient().create_table(table);
    }
  }

  @Override
  public void create_table_with_environment_context(Table table,
      EnvironmentContext environmentContext)
      throws TException {
    try (MetaStoreClient client = catalog_.getMetaStoreClient()) {
      client.getHiveClient().getThriftClient()
          .create_table_with_environment_context(table, environmentContext);
    }
  }

  @Override
  public void create_table_with_constraints(Table table,
      List<SQLPrimaryKey> sqlPrimaryKeys,
      List<SQLForeignKey> sqlForeignKeys, List<SQLUniqueConstraint> sqlUniqueConstraints,
      List<SQLNotNullConstraint> sqlNotNullConstraints,
      List<SQLDefaultConstraint> sqlDefaultConstraints,
      List<SQLCheckConstraint> sqlCheckConstraints)
      throws TException {
    try (MetaStoreClient client = catalog_.getMetaStoreClient()) {
      client.getHiveClient().getThriftClient().create_table_with_constraints(table,
          sqlPrimaryKeys, sqlForeignKeys, sqlUniqueConstraints, sqlNotNullConstraints,
          sqlDefaultConstraints, sqlCheckConstraints);
    }
  }

  @Override
  public void create_table_req(CreateTableRequest createTableRequest)
      throws TException {
    try (MetaStoreClient client = catalog_.getMetaStoreClient()) {
      client.getHiveClient().getThriftClient().create_table_req(createTableRequest);
    }
  }

  @Override
  public void drop_constraint(DropConstraintRequest dropConstraintRequest)
      throws NoSuchObjectException, MetaException, TException {
    try (MetaStoreClient client = catalog_.getMetaStoreClient()) {
      client.getHiveClient().getThriftClient().drop_constraint(dropConstraintRequest);
    }
  }

  @Override
  public void add_primary_key(AddPrimaryKeyRequest addPrimaryKeyRequest)
      throws NoSuchObjectException, MetaException, TException {
    try (MetaStoreClient client = catalog_.getMetaStoreClient()) {
      client.getHiveClient().getThriftClient().add_primary_key(addPrimaryKeyRequest);
    }
  }

  @Override
  public void add_foreign_key(AddForeignKeyRequest addForeignKeyRequest)
      throws NoSuchObjectException, MetaException, TException {
    try (MetaStoreClient client = catalog_.getMetaStoreClient()) {
      client.getHiveClient().getThriftClient().add_foreign_key(addForeignKeyRequest);
    }
  }

  @Override
  public void add_unique_constraint(AddUniqueConstraintRequest addUniqueConstraintRequest)
      throws NoSuchObjectException, MetaException, TException {
    try (MetaStoreClient client = catalog_.getMetaStoreClient()) {
      client.getHiveClient().getThriftClient()
          .add_unique_constraint(addUniqueConstraintRequest);
    }
  }

  @Override
  public void add_not_null_constraint(
      AddNotNullConstraintRequest addNotNullConstraintRequest)
      throws NoSuchObjectException, MetaException, TException {
    try (MetaStoreClient client = catalog_.getMetaStoreClient()) {
      client.getHiveClient().getThriftClient()
          .add_not_null_constraint(addNotNullConstraintRequest);
    }
  }

  @Override
  public void add_default_constraint(
      AddDefaultConstraintRequest addDefaultConstraintRequest)
      throws NoSuchObjectException, MetaException, TException {
    try (MetaStoreClient client = catalog_.getMetaStoreClient()) {
      client.getHiveClient().getThriftClient()
          .add_default_constraint(addDefaultConstraintRequest);
    }
  }

  @Override
  public void add_check_constraint(AddCheckConstraintRequest addCheckConstraintRequest)
      throws NoSuchObjectException, MetaException, TException {
    try (MetaStoreClient client = catalog_.getMetaStoreClient()) {
      client.getHiveClient().getThriftClient()
          .add_check_constraint(addCheckConstraintRequest);
    }
  }

  @Override
  public void drop_table(String dbname, String tblname, boolean deleteData)
      throws NoSuchObjectException, MetaException, TException {
    try (MetaStoreClient client = catalog_.getMetaStoreClient()) {
      client.getHiveClient().getThriftClient().drop_table(dbname, tblname, deleteData);
    }
  }

  @Override
  public void drop_table_with_environment_context(String dbname, String tblname,
      boolean deleteData,
      EnvironmentContext environmentContext)
      throws NoSuchObjectException, MetaException, TException {
    try (MetaStoreClient client = catalog_.getMetaStoreClient()) {
      client.getHiveClient().getThriftClient()
          .drop_table_with_environment_context(dbname, tblname, deleteData,
              environmentContext);
    }
  }

  @Override
  public void truncate_table(String dbName, String tblName, List<String> partNames)
      throws MetaException, TException {
    try (MetaStoreClient client = catalog_.getMetaStoreClient()) {
      client.getHiveClient().getThriftClient().truncate_table(dbName, tblName, partNames);
    }
  }

  @Override
  public TruncateTableResponse truncate_table_req(
      TruncateTableRequest truncateTableRequest) throws MetaException, TException {
    try (MetaStoreClient client = catalog_.getMetaStoreClient()) {
      return client.getHiveClient().getThriftClient()
          .truncate_table_req(truncateTableRequest);
    }
  }

  @Override
  public List<String> get_tables(String dbname, String tblName)
      throws MetaException, TException {
    try (MetaStoreClient client = catalog_.getMetaStoreClient()) {
      return client.getHiveClient().getThriftClient().get_tables(dbname, tblName);
    }
  }

  @Override
  public List<String> get_tables_by_type(String dbname, String tablePattern,
      String tableType)
      throws MetaException, TException {
    try (MetaStoreClient client = catalog_.getMetaStoreClient()) {
      return client.getHiveClient().getThriftClient().get_tables_by_type(dbname,
          tablePattern, tableType);
    }
  }

  @Override
  public List<Table> get_all_materialized_view_objects_for_rewriting()
      throws MetaException, TException {
    try (MetaStoreClient client = catalog_.getMetaStoreClient()) {
      return client.getHiveClient().getThriftClient()
          .get_all_materialized_view_objects_for_rewriting();
    }
  }

  @Override
  public List<String> get_materialized_views_for_rewriting(String dbName)
      throws MetaException, TException {
    try (MetaStoreClient client = catalog_.getMetaStoreClient()) {
      return client.getHiveClient().getThriftClient()
          .get_materialized_views_for_rewriting(dbName);
    }
  }

  @Override
  public List<TableMeta> get_table_meta(String dbnamePattern, String tblNamePattern,
      List<String> tableTypes)
      throws MetaException, TException {
    try (MetaStoreClient client = catalog_.getMetaStoreClient()) {
      return client.getHiveClient().getThriftClient().get_table_meta(dbnamePattern,
          tblNamePattern, tableTypes);
    }
  }

  @Override
  public List<String> get_all_tables(String dbname) throws MetaException, TException {
    try (MetaStoreClient client = catalog_.getMetaStoreClient()) {
      return client.getHiveClient().getThriftClient().get_all_tables(dbname);
    }
  }

  @Override
  public Table get_table(String dbname, String tblname)
      throws MetaException, NoSuchObjectException, TException {
    try (MetaStoreClient client = catalog_.getMetaStoreClient()) {
      return client.getHiveClient().getThriftClient().get_table(dbname, tblname);
    }
  }

  @Override
  public List<Table> get_table_objects_by_name(String dbname, List<String> list)
      throws TException {
    try (MetaStoreClient client = catalog_.getMetaStoreClient()) {
      return client.getHiveClient().getThriftClient().get_table_objects_by_name(dbname,
          list);
    }
  }

  @Override
  public List<ExtendedTableInfo> get_tables_ext(GetTablesExtRequest getTablesExtRequest)
      throws MetaException, TException {
    try (MetaStoreClient client = catalog_.getMetaStoreClient()) {
      return client.getHiveClient().getThriftClient().get_tables_ext(getTablesExtRequest);
    }
  }

  /**
   * This method gets the table from the HMS directly. Additionally, if the request has
   * {@code getFileMetadata} set it computes the filemetadata and returns it in the
   * response. For transactional tables, it uses the ValidWriteIdList from the request and
   * gets the current ValidTxnList to get the requested snapshot of the file-metadata for
   * the table.
   */
  @Override
  public GetTableResult get_table_req(GetTableRequest getTableRequest)
      throws MetaException, NoSuchObjectException, TException {
    String tblName = getTableRequest.getDbName() + "." + getTableRequest.getTblName();
    LOG.debug(String.format(HMS_FALLBACK_MSG_FORMAT, "get_table_req", tblName));
    GetTableResult result;
    ValidTxnList txnList = null;
    ValidWriteIdList writeIdList = null;
    String requestWriteIdList = getTableRequest.getValidWriteIdList();
    try (MetaStoreClient client = catalog_.getMetaStoreClient()) {
      result = client.getHiveClient().getThriftClient()
          .get_table_req(getTableRequest);
      Table tbl = result.getTable();
      // return early if file-metadata is not requested
      if (!getTableRequest.isGetFileMetadata()) {
        LOG.trace("File metadata is not requested. Returning table {}",
            tbl.getTableName());
        return result;
      }
      // we need to get the current ValidTxnIdList to avoid returning
      // file-metadata for in-progress compactions. If the request does not
      // include ValidWriteIdList or if the table is not transactional we compute
      // the file-metadata as seen on the file-system.
      boolean isTransactional = tbl.getParameters() != null && AcidUtils
          .isTransactionalTable(tbl.getParameters());
      if (isTransactional && requestWriteIdList != null) {
        txnList = MetastoreShim.getValidTxns(client.getHiveClient());
        writeIdList = MetastoreShim
            .getValidWriteIdListFromString(requestWriteIdList);
      }
    }
    CatalogHmsAPIHelper.loadAndSetFileMetadataFromFs(txnList, writeIdList, result);
    return result;
  }

  @Override
  public GetTablesResult get_table_objects_by_name_req(GetTablesRequest getTablesRequest)
      throws MetaException, InvalidOperationException, UnknownDBException, TException {
    try (MetaStoreClient client = catalog_.getMetaStoreClient()) {
      return client.getHiveClient().getThriftClient()
          .get_table_objects_by_name_req(getTablesRequest);
    }
  }

  @Override
  public Materialization get_materialization_invalidation_info(
      CreationMetadata creationMetadata, String validTxnList)
      throws MetaException, InvalidOperationException, UnknownDBException, TException {
    try (MetaStoreClient client = catalog_.getMetaStoreClient()) {
      return client.getHiveClient().getThriftClient()
          .get_materialization_invalidation_info(creationMetadata, validTxnList);
    }
  }

  @Override
  public void update_creation_metadata(String catName, String dbName, String tblName,
      CreationMetadata creationMetadata)
      throws MetaException, InvalidOperationException, UnknownDBException, TException {
    try (MetaStoreClient client = catalog_.getMetaStoreClient()) {
      client.getHiveClient().getThriftClient().update_creation_metadata(catName,
          dbName, tblName, creationMetadata);
    }
  }

  @Override
  public List<String> get_table_names_by_filter(String dbname, String tblname,
      short maxParts)
      throws MetaException, InvalidOperationException, UnknownDBException, TException {
    try (MetaStoreClient client = catalog_.getMetaStoreClient()) {
      return client.getHiveClient().getThriftClient().get_table_names_by_filter(dbname,
          tblname, maxParts);
    }
  }

  @Override
  public void alter_table(String dbname, String tblName, Table newTable)
      throws InvalidOperationException, MetaException, TException {
    try (MetaStoreClient client = catalog_.getMetaStoreClient()) {
      client.getHiveClient().getThriftClient().alter_table(dbname, tblName, newTable);
    }
  }

  @Override
  public void alter_table_with_environment_context(String dbname, String tblName,
      Table table,
      EnvironmentContext environmentContext)
      throws InvalidOperationException, MetaException, TException {
    try (MetaStoreClient client = catalog_.getMetaStoreClient()) {
      client.getHiveClient().getThriftClient()
          .alter_table_with_environment_context(dbname,
              tblName, table, environmentContext);
    }
  }

  @Override
  public void alter_table_with_cascade(String dbname, String tblName, Table table,
      boolean cascade)
      throws InvalidOperationException, MetaException, TException {
    try (MetaStoreClient client = catalog_.getMetaStoreClient()) {
      client.getHiveClient().getThriftClient().alter_table_with_cascade(dbname, tblName,
          table, cascade);
    }
  }

  @Override
  public AlterTableResponse alter_table_req(AlterTableRequest alterTableRequest)
      throws InvalidOperationException, MetaException, TException {
    try (MetaStoreClient client = catalog_.getMetaStoreClient()) {
      return client.getHiveClient().getThriftClient().alter_table_req(alterTableRequest);
    }
  }

  @Override
  public Partition add_partition(Partition partition)
      throws InvalidObjectException, AlreadyExistsException, MetaException, TException {
    try (MetaStoreClient client = catalog_.getMetaStoreClient()) {
      return client.getHiveClient().getThriftClient().add_partition(partition);
    }
  }

  @Override
  public Partition add_partition_with_environment_context(Partition partition,
      EnvironmentContext environmentContext)
      throws InvalidObjectException, AlreadyExistsException, MetaException, TException {
    try (MetaStoreClient client = catalog_.getMetaStoreClient()) {
      return client.getHiveClient().getThriftClient()
          .add_partition_with_environment_context(partition, environmentContext);
    }
  }

  @Override
  public int add_partitions(List<Partition> partitionList)
      throws InvalidObjectException, AlreadyExistsException, MetaException, TException {
    try (MetaStoreClient client = catalog_.getMetaStoreClient()) {
      return client.getHiveClient().getThriftClient().add_partitions(partitionList);
    }
  }

  @Override
  public int add_partitions_pspec(List<PartitionSpec> list)
      throws InvalidObjectException, AlreadyExistsException, MetaException, TException {
    try (MetaStoreClient client = catalog_.getMetaStoreClient()) {
      return client.getHiveClient().getThriftClient().add_partitions_pspec(list);
    }
  }

  @Override
  public Partition append_partition(String dbname, String tblName, List<String> partVals)
      throws InvalidObjectException, AlreadyExistsException, MetaException, TException {
    try (MetaStoreClient client = catalog_.getMetaStoreClient()) {
      return client.getHiveClient().getThriftClient()
          .append_partition(dbname, tblName, partVals);
    }
  }

  @Override
  public AddPartitionsResult add_partitions_req(AddPartitionsRequest addPartitionsRequest)
      throws InvalidObjectException, AlreadyExistsException, MetaException, TException {
    try (MetaStoreClient client = catalog_.getMetaStoreClient()) {
      return client.getHiveClient().getThriftClient()
          .add_partitions_req(addPartitionsRequest);
    }
  }

  @Override
  public Partition append_partition_with_environment_context(String dbname,
      String tblname,
      List<String> partVals, EnvironmentContext environmentContext)
      throws InvalidObjectException, AlreadyExistsException, MetaException, TException {
    try (MetaStoreClient client = catalog_.getMetaStoreClient()) {
      return client.getHiveClient().getThriftClient()
          .append_partition_with_environment_context(dbname, tblname, partVals,
              environmentContext);
    }
  }

  @Override
  public Partition append_partition_by_name(String dbname, String tblname,
      String partName)
      throws InvalidObjectException, AlreadyExistsException, MetaException, TException {
    try (MetaStoreClient client = catalog_.getMetaStoreClient()) {
      return client.getHiveClient().getThriftClient()
          .append_partition_by_name(dbname, tblname, partName);
    }
  }

  @Override
  public Partition append_partition_by_name_with_environment_context(String dbname,
      String tblname, String partName, EnvironmentContext environmentContext)
      throws InvalidObjectException, AlreadyExistsException, MetaException, TException {
    try (MetaStoreClient client = catalog_.getMetaStoreClient()) {
      return client.getHiveClient().getThriftClient()
          .append_partition_by_name_with_environment_context(dbname, tblname, partName,
              environmentContext);
    }
  }

  @Override
  public boolean drop_partition(String dbname, String tblanme, List<String> partVals,
      boolean deleteData)
      throws NoSuchObjectException, MetaException, TException {
    try (MetaStoreClient client = catalog_.getMetaStoreClient()) {
      return client.getHiveClient().getThriftClient()
          .drop_partition(dbname, tblanme, partVals, deleteData);
    }
  }

  @Override
  public boolean drop_partition_with_environment_context(String dbname, String tblname,
      List<String> partNames, boolean deleteData, EnvironmentContext environmentContext)
      throws NoSuchObjectException, MetaException, TException {
    try (MetaStoreClient client = catalog_.getMetaStoreClient()) {
      return client.getHiveClient().getThriftClient()
          .drop_partition_with_environment_context(dbname, tblname, partNames, deleteData,
              environmentContext);
    }
  }

  @Override
  public boolean drop_partition_by_name(String dbname, String tblname, String partName,
      boolean deleteData)
      throws NoSuchObjectException, MetaException, TException {
    try (MetaStoreClient client = catalog_.getMetaStoreClient()) {
      return client.getHiveClient().getThriftClient().drop_partition_by_name(dbname,
          tblname, partName, deleteData);
    }
  }

  @Override
  public boolean drop_partition_by_name_with_environment_context(String dbName,
      String tableName,
      String partName, boolean deleteData, EnvironmentContext envContext)
      throws NoSuchObjectException, MetaException, TException {
    try (MetaStoreClient client = catalog_.getMetaStoreClient()) {
      return client.getHiveClient().getThriftClient()
          .drop_partition_by_name_with_environment_context(dbName, tableName, partName,
              deleteData, envContext);
    }
  }

  @Override
  public DropPartitionsResult drop_partitions_req(
      DropPartitionsRequest dropPartitionsRequest)
      throws NoSuchObjectException, MetaException, TException {
    try (MetaStoreClient client = catalog_.getMetaStoreClient()) {
      return client.getHiveClient().getThriftClient()
          .drop_partitions_req(dropPartitionsRequest);
    }
  }

  @Override
  public Partition get_partition(String dbName, String tblName, List<String> values)
      throws MetaException, NoSuchObjectException, TException {
    try (MetaStoreClient client = catalog_.getMetaStoreClient()) {
      return client.getHiveClient().getThriftClient().get_partition(dbName, tblName,
          values);
    }
  }

  @Override
  public Partition exchange_partition(Map<String, String> partitionSpecMap,
      String sourcedb, String sourceTbl,
      String destDb, String destTbl)
      throws TException {
    try (MetaStoreClient client = catalog_.getMetaStoreClient()) {
      return client.getHiveClient().getThriftClient()
          .exchange_partition(partitionSpecMap, sourcedb, sourceTbl, destDb
              , destTbl);
    }
  }

  @Override
  public List<Partition> exchange_partitions(Map<String, String> partitionSpecs,
      String sourceDb, String sourceTable, String destDb,
      String destinationTableName)
      throws TException {
    try (MetaStoreClient client = catalog_.getMetaStoreClient()) {
      return client.getHiveClient().getThriftClient().exchange_partitions(partitionSpecs,
          sourceDb, sourceTable, destDb, destinationTableName);
    }
  }

  @Override
  public Partition get_partition_with_auth(String dbname, String tblName,
      List<String> values,
      String user, List<String> groups)
      throws MetaException, NoSuchObjectException, TException {
    try (MetaStoreClient client = catalog_.getMetaStoreClient()) {
      return client.getHiveClient().getThriftClient().get_partition_with_auth(dbname,
          tblName, values, user,
          groups);
    }
  }

  @Override
  public Partition get_partition_by_name(String dbName, String tblName,
      String partitionName)
      throws MetaException, NoSuchObjectException, TException {
    try (MetaStoreClient client = catalog_.getMetaStoreClient()) {
      return client.getHiveClient().getThriftClient().get_partition_by_name(dbName,
          tblName, partitionName);
    }
  }

  @Override
  public List<Partition> get_partitions(String dbName, String tblName, short maxLimit)
      throws NoSuchObjectException, MetaException, TException {
    try (MetaStoreClient client = catalog_.getMetaStoreClient()) {
      return client.getHiveClient().getThriftClient()
          .get_partitions(dbName, tblName, maxLimit);
    }
  }

  @Override
  public List<Partition> get_partitions_with_auth(String dbName, String tblName,
      short maxParts, String username,
      List<String> groups) throws NoSuchObjectException, MetaException, TException {
    try (MetaStoreClient client = catalog_.getMetaStoreClient()) {
      return client.getHiveClient().getThriftClient().get_partitions_with_auth(dbName,
          tblName, maxParts, username,
          groups);
    }
  }

  @Override
  public List<PartitionSpec> get_partitions_pspec(String dbName, String tblName,
      int maxParts)
      throws NoSuchObjectException, MetaException, TException {
    try (MetaStoreClient client = catalog_.getMetaStoreClient()) {
      return client.getHiveClient().getThriftClient()
          .get_partitions_pspec(dbName, tblName, maxParts);
    }
  }

  @Override
  public GetPartitionsResponse get_partitions_with_specs(GetPartitionsRequest request)
      throws NoSuchObjectException, MetaException, TException {
    try (MetaStoreClient client = catalog_.getMetaStoreClient()) {
      return client.getHiveClient().getThriftClient()
          .get_partitions_with_specs(request);
    }
  }

  @Override
  public List<String> get_partition_names(String dbName, String tblName, short maxParts)
      throws NoSuchObjectException, MetaException, TException {
    try (MetaStoreClient client = catalog_.getMetaStoreClient()) {
      return client.getHiveClient().getThriftClient().get_partition_names(dbName,
          tblName, maxParts);
    }
  }

  @Override
  public PartitionValuesResponse get_partition_values(
      PartitionValuesRequest partitionValuesRequest)
      throws MetaException, NoSuchObjectException, TException {
    try (MetaStoreClient client = catalog_.getMetaStoreClient()) {
      return client.getHiveClient().getThriftClient()
          .get_partition_values(partitionValuesRequest);
    }
  }

  @Override
  public List<Partition> get_partitions_ps(String dbName, String tblName,
      List<String> partValues,
      short maxParts) throws MetaException, NoSuchObjectException, TException {
    try (MetaStoreClient client = catalog_.getMetaStoreClient()) {
      return client.getHiveClient().getThriftClient()
          .get_partitions_ps(dbName, tblName, partValues, maxParts);
    }
  }

  @Override
  public List<Partition> get_partitions_ps_with_auth(String dbName, String tblName,
      List<String> partVals, short maxParts, String user, List<String> groups)
      throws NoSuchObjectException, MetaException, TException {
    try (MetaStoreClient client = catalog_.getMetaStoreClient()) {
      return client.getHiveClient().getThriftClient()
          .get_partitions_ps_with_auth(dbName, tblName
              , partVals, maxParts, user, groups);
    }
  }

  @Override
  public List<String> get_partition_names_ps(String dbName, String tblName,
      List<String> partitionNames,
      short maxParts) throws MetaException, NoSuchObjectException, TException {
    try (MetaStoreClient client = catalog_.getMetaStoreClient()) {
      return client.getHiveClient().getThriftClient()
          .get_partition_names_ps(dbName, tblName,
              partitionNames, maxParts);
    }
  }

  @Override
  public List<Partition> get_partitions_by_filter(String dbName, String tblName,
      String filter, short maxParts)
      throws MetaException, NoSuchObjectException, TException {
    try (MetaStoreClient client = catalog_.getMetaStoreClient()) {
      return client.getHiveClient().getThriftClient()
          .get_partitions_by_filter(dbName, tblName,
              filter, maxParts);
    }
  }

  @Override
  public List<PartitionSpec> get_part_specs_by_filter(String dbName, String tblName,
      String filter,
      int maxParts) throws MetaException, NoSuchObjectException, TException {
    try (MetaStoreClient client = catalog_.getMetaStoreClient()) {
      return client.getHiveClient().getThriftClient()
          .get_part_specs_by_filter(dbName, tblName, filter
              , maxParts);
    }
  }

  @Override
  public GetFieldsResponse get_fields_req(GetFieldsRequest req)
      throws MetaException, UnknownTableException, UnknownDBException, TException {
    try (MetaStoreClient client = catalog_.getMetaStoreClient()) {
      List<FieldSchema> fields = client.getHiveClient().getThriftClient()
          .get_fields_with_environment_context(MetaStoreUtils
                  .prependCatalogToDbName(req.getCatName(), req.getDbName(), serverConf_),
              req.getTblName(), req.getEnvContext());
      GetFieldsResponse res = new GetFieldsResponse();
      res.setFields(fields);
      return res;
    }
  }

  @Override
  public GetSchemaResponse get_schema_req(GetSchemaRequest req)
      throws MetaException, UnknownTableException, UnknownDBException, TException {
    try (MetaStoreClient client = catalog_.getMetaStoreClient()) {
      // TODO Remove the usage of old API here once this API is ported to cdpd-master
      List<FieldSchema> fields = client.getHiveClient().getThriftClient()
          .get_schema_with_environment_context(MetaStoreUtils
                  .prependCatalogToDbName(req.getCatName(), req.getDbName(), serverConf_),
              req.getTblName(), req.getEnvContext());
      GetSchemaResponse res = new GetSchemaResponse();
      res.setFields(fields);
      return res;
    }
  }

  @Override
  public GetPartitionResponse get_partition_req(GetPartitionRequest req)
      throws MetaException, NoSuchObjectException, TException {
    try (MetaStoreClient client = catalog_.getMetaStoreClient()) {
      Partition p =
          client.getHiveClient().getThriftClient().get_partition(
              MetaStoreUtils
                  .prependCatalogToDbName(req.getCatName(), req.getDbName(), serverConf_),
              req.getTblName(), req.getPartVals());
      GetPartitionResponse res = new GetPartitionResponse();
      res.setPartition(p);
      return res;
    }
  }

  @Override
  public PartitionsResponse get_partitions_req(PartitionsRequest req)
      throws NoSuchObjectException, MetaException, TException {
    try (MetaStoreClient client = catalog_.getMetaStoreClient()) {
      List<Partition> partitions =
          client.getHiveClient().getThriftClient().get_partitions(MetaStoreUtils
                  .prependCatalogToDbName(req.getCatName(), req.getDbName(), serverConf_),
              req.getTblName(), req.getMaxParts());
      PartitionsResponse res = new PartitionsResponse();
      res.setPartitions(partitions);
      return res;
    }
  }

  @Override
  public GetPartitionNamesPsResponse get_partition_names_ps_req(
      GetPartitionNamesPsRequest req)
      throws MetaException, NoSuchObjectException, TException {
    try (MetaStoreClient client = catalog_.getMetaStoreClient()) {
      List<String> names = client.getHiveClient().getThriftClient()
          .get_partition_names_ps(MetaStoreUtils
                  .prependCatalogToDbName(req.getCatName(), req.getDbName(), serverConf_),
              req.getTblName(), req.getPartValues(), req.getMaxParts());
      GetPartitionNamesPsResponse res = new GetPartitionNamesPsResponse();
      res.setNames(names);
      return res;
    }
  }

 @Override
 public GetPartitionsPsWithAuthResponse get_partitions_ps_with_auth_req(
     GetPartitionsPsWithAuthRequest req)
     throws MetaException, NoSuchObjectException, TException {
   try (MetaStoreClient client = catalog_.getMetaStoreClient()) {
     List<Partition> partitions = client.getHiveClient().getThriftClient()
         .get_partitions_ps_with_auth(MetaStoreUtils
                 .prependCatalogToDbName(req.getCatName(), req.getDbName(), serverConf_),
             req.getTblName(), req.getPartVals(), req.getMaxParts(),
             req.getUserName(), req.getGroupNames());
     GetPartitionsPsWithAuthResponse res = new GetPartitionsPsWithAuthResponse();
     res.setPartitions(partitions);
     return res;
   }
 }

  @Override
  public PartitionsByExprResult get_partitions_by_expr(
      PartitionsByExprRequest partitionsByExprRequest) throws TException {
    try (MetaStoreClient client = catalog_.getMetaStoreClient()) {
      return client.getHiveClient().getThriftClient()
          .get_partitions_by_expr(partitionsByExprRequest);
    }
  }

  @Override
  public int get_num_partitions_by_filter(String dbName, String tblName, String filter)
      throws MetaException, NoSuchObjectException, TException {
    try (MetaStoreClient client = catalog_.getMetaStoreClient()) {
      return client.getHiveClient().getThriftClient().get_num_partitions_by_filter(dbName,
          tblName, filter);
    }
  }

  @Override
  public List<Partition> get_partitions_by_names(String dbName, String tblName,
      List<String> partitionNames)
      throws MetaException, NoSuchObjectException, TException {
    try (MetaStoreClient client = catalog_.getMetaStoreClient()) {
      return client.getHiveClient().getThriftClient()
          .get_partitions_by_names(dbName, tblName,
              partitionNames);
    }
  }

  /**
   * Util method to evaluate if the received exception needs to be thrown to the Client
   * based on the server configuration.
   *
   * @param cause   The underlying exception received from Catalog.
   * @param apiName The HMS API name which threw the given exception.
   * @throws TException Wrapped exception with the cause in case the given Exception is
   *                    not a TException. Else, throws the given TException.
   */
  protected void throwIfNoFallback(Exception cause, String apiName)
      throws TException {
    LOG.debug("Received exception while executing {}", apiName, cause);
    if (fallBackToHMSOnErrors_) return;
    if (cause instanceof TException) throw (TException) cause;
    // if this is not a TException we wrap it to a MetaException
    throw new MetaException(
        String.format(METAEXCEPTION_MSG_FORMAT, apiName, cause));
  }

  /**
   * This method gets the partitions for the given list of names from HMS. Additionally,
   * if the {@code getFileMetadata} flag is set in the request, it also computes the file
   * metadata and sets it in the partitions which are returned.
   *
   * @throws TException
   */
  public GetPartitionsByNamesResult get_partitions_by_names_req(
      GetPartitionsByNamesRequest getPartitionsByNamesRequest) throws TException {
    String tblName =
        getPartitionsByNamesRequest.getDb_name() + "." + getPartitionsByNamesRequest
            .getTbl_name();
    LOG.info(String
        .format(HMS_FALLBACK_MSG_FORMAT, HmsApiNameEnum.GET_PARTITION_BY_NAMES.apiName(),
            tblName));
    boolean getFileMetadata = getPartitionsByNamesRequest.isGetFileMetadata();
    GetPartitionsByNamesResult result;
    ValidWriteIdList validWriteIdList = null;
    ValidTxnList validTxnList = null;
    try (MetaStoreClient client = catalog_.getMetaStoreClient()) {
      result = client.getHiveClient().getThriftClient()
          .get_partitions_by_names_req(getPartitionsByNamesRequest);
      // if file-metadata is not request; return early
      if (!getFileMetadata) return result;
      // we don't really know if the requested partitions are for a transactional table
      // or not. Hence we should get the table from HMS to confirm.
      // TODO: may be we could assume that if ValidWriteIdList is not set, the table is
      // not transactional
      String[] parsedCatDbName = MetaStoreUtils
          .parseDbName(getPartitionsByNamesRequest.getDb_name(), serverConf_);
      Table tbl = client.getHiveClient().getTable(parsedCatDbName[0], parsedCatDbName[1],
          getPartitionsByNamesRequest.getTbl_name(),
          getPartitionsByNamesRequest.getValidWriteIdList());
      boolean isTransactional = tbl.getParameters() != null && AcidUtils
          .isTransactionalTable(tbl.getParameters());
      if (isTransactional) {
        if (getPartitionsByNamesRequest.getValidWriteIdList() == null) {
          throw new MetaException(
              "ValidWriteIdList is not set when requesting partitions for table " + tbl
                  .getDbName() + "." + tbl.getTableName());
        }
        validWriteIdList = MetastoreShim
            .getValidWriteIdListFromString(
                getPartitionsByNamesRequest.getValidWriteIdList());
        validTxnList = client.getHiveClient().getValidTxns();
      }
    }
    CatalogHmsAPIHelper
        .loadAndSetFileMetadataFromFs(validTxnList, validWriteIdList, result);
    return result;
  }

  @Override
  public void alter_partition(String dbName, String tblName, Partition partition)
      throws InvalidOperationException, MetaException, TException {
    try (MetaStoreClient client = catalog_.getMetaStoreClient()) {
      client.getHiveClient().getThriftClient()
          .alter_partition(dbName, tblName, partition);
    }
  }

  @Override
  public void alter_partitions(String dbNme, String tblName, List<Partition> partitions)
      throws InvalidOperationException, MetaException, TException {
    try (MetaStoreClient client = catalog_.getMetaStoreClient()) {
      client.getHiveClient().getThriftClient()
          .alter_partitions(dbNme, tblName, partitions);
    }
  }

  @Override
  public void alter_partitions_with_environment_context(String s, String s1,
      List<Partition> list, EnvironmentContext environmentContext)
      throws InvalidOperationException, MetaException, TException {
    try (MetaStoreClient client = catalog_.getMetaStoreClient()) {
      client.getHiveClient().getThriftClient()
          .alter_partitions_with_environment_context(s, s1, list, environmentContext);
    }
  }

  @Override
  public AlterPartitionsResponse alter_partitions_req(
      AlterPartitionsRequest alterPartitionsRequest)
      throws InvalidOperationException, MetaException, TException {
    try (MetaStoreClient client = catalog_.getMetaStoreClient()) {
      return client.getHiveClient().getThriftClient()
          .alter_partitions_req(alterPartitionsRequest);
    }
  }

  @Override
  public void alter_partition_with_environment_context(String dbName, String tblName,
      Partition partition, EnvironmentContext environmentContext)
      throws InvalidOperationException, MetaException, TException {
    try (MetaStoreClient client = catalog_.getMetaStoreClient()) {
      client.getHiveClient().getThriftClient()
          .alter_partition_with_environment_context(dbName, tblName, partition,
              environmentContext);
    }
  }

  @Override
  public void rename_partition(String dbName, String tblName, List<String> list,
      Partition partition) throws InvalidOperationException, MetaException, TException {
    try (MetaStoreClient client = catalog_.getMetaStoreClient()) {
      client.getHiveClient().getThriftClient()
          .rename_partition(dbName, tblName, list, partition);
    }
  }

  @Override
  public RenamePartitionResponse rename_partition_req(
      RenamePartitionRequest renamePartitionRequest)
      throws InvalidOperationException, MetaException, TException {
    try (MetaStoreClient client = catalog_.getMetaStoreClient()) {
      return client.getHiveClient().getThriftClient()
          .rename_partition_req(renamePartitionRequest);
    }
  }

  @Override
  public boolean partition_name_has_valid_characters(List<String> list, boolean b)
      throws MetaException, TException {
    try (MetaStoreClient client = catalog_.getMetaStoreClient()) {
      return client.getHiveClient().getThriftClient()
          .partition_name_has_valid_characters(list, b);
    }
  }

  @Override
  public String get_config_value(String key, String defaultVal)
      throws ConfigValSecurityException, TException {
    try (MetaStoreClient client = catalog_.getMetaStoreClient()) {
      return client.getHiveClient().getThriftClient().get_config_value(key, defaultVal);
    }
  }

  @Override
  public List<String> partition_name_to_vals(String name)
      throws MetaException, TException {
    try (MetaStoreClient client = catalog_.getMetaStoreClient()) {
      return client.getHiveClient().getThriftClient().partition_name_to_vals(name);
    }
  }

  @Override
  public Map<String, String> partition_name_to_spec(String name)
      throws MetaException, TException {
    try (MetaStoreClient client = catalog_.getMetaStoreClient()) {
      return client.getHiveClient().getThriftClient().partition_name_to_spec(name);
    }
  }

  @Override
  public void markPartitionForEvent(String s, String s1, Map<String, String> map,
      PartitionEventType partitionEventType) throws TException {
    try (MetaStoreClient client = catalog_.getMetaStoreClient()) {
      client.getHiveClient().getThriftClient()
          .markPartitionForEvent(s, s1, map, partitionEventType);
    }
  }

  @Override
  public boolean isPartitionMarkedForEvent(String s, String s1, Map<String, String> map,
      PartitionEventType partitionEventType) throws TException {
    try (MetaStoreClient client = catalog_.getMetaStoreClient()) {
      return client.getHiveClient().getThriftClient().isPartitionMarkedForEvent(s, s1,
          map, partitionEventType);
    }
  }

  @Override
  public PrimaryKeysResponse get_primary_keys(PrimaryKeysRequest primaryKeysRequest)
      throws MetaException, NoSuchObjectException, TException {
    try (MetaStoreClient client = catalog_.getMetaStoreClient()) {
      return client.getHiveClient().getThriftClient()
          .get_primary_keys(primaryKeysRequest);
    }
  }

  @Override
  public ForeignKeysResponse get_foreign_keys(ForeignKeysRequest foreignKeysRequest)
      throws MetaException, NoSuchObjectException, TException {
    try (MetaStoreClient client = catalog_.getMetaStoreClient()) {
      return client.getHiveClient().getThriftClient()
          .get_foreign_keys(foreignKeysRequest);
    }
  }

  @Override
  public UniqueConstraintsResponse get_unique_constraints(
      UniqueConstraintsRequest uniqueConstraintsRequest)
      throws MetaException, NoSuchObjectException, TException {
    try (MetaStoreClient client = catalog_.getMetaStoreClient()) {
      return client.getHiveClient().getThriftClient()
          .get_unique_constraints(uniqueConstraintsRequest);
    }
  }

  @Override
  public NotNullConstraintsResponse get_not_null_constraints(
      NotNullConstraintsRequest notNullConstraintsRequest)
      throws MetaException, NoSuchObjectException, TException {
    try (MetaStoreClient client = catalog_.getMetaStoreClient()) {
      return client.getHiveClient().getThriftClient()
          .get_not_null_constraints(notNullConstraintsRequest);
    }
  }

  @Override
  public DefaultConstraintsResponse get_default_constraints(
      DefaultConstraintsRequest defaultConstraintsRequest)
      throws MetaException, NoSuchObjectException, TException {
    try (MetaStoreClient client = catalog_.getMetaStoreClient()) {
      return client.getHiveClient().getThriftClient()
          .get_default_constraints(defaultConstraintsRequest);
    }
  }

  @Override
  public CheckConstraintsResponse get_check_constraints(
      CheckConstraintsRequest checkConstraintsRequest)
      throws MetaException, NoSuchObjectException, TException {
    try (MetaStoreClient client = catalog_.getMetaStoreClient()) {
      return client.getHiveClient().getThriftClient()
          .get_check_constraints(checkConstraintsRequest);
    }
  }

  @Override
  public boolean update_table_column_statistics(ColumnStatistics columnStatistics)
      throws NoSuchObjectException, InvalidObjectException, MetaException,
      InvalidInputException, TException {
    try (MetaStoreClient client = catalog_.getMetaStoreClient()) {
      return client.getHiveClient().getThriftClient()
          .update_table_column_statistics(columnStatistics);
    }
  }

  @Override
  public boolean update_partition_column_statistics(ColumnStatistics columnStatistics)
      throws NoSuchObjectException, InvalidObjectException, MetaException,
      InvalidInputException, TException {
    try (MetaStoreClient client = catalog_.getMetaStoreClient()) {
      return client.getHiveClient().getThriftClient()
          .update_partition_column_statistics(columnStatistics);
    }
  }

  @Override
  public SetPartitionsStatsResponse update_table_column_statistics_req(
      SetPartitionsStatsRequest setPartitionsStatsRequest)
      throws NoSuchObjectException, InvalidObjectException, MetaException,
      InvalidInputException, TException {
    try (MetaStoreClient client = catalog_.getMetaStoreClient()) {
      return client.getHiveClient().getThriftClient()
          .update_table_column_statistics_req(setPartitionsStatsRequest);
    }
  }

  @Override
  public SetPartitionsStatsResponse update_partition_column_statistics_req(
      SetPartitionsStatsRequest setPartitionsStatsRequest)
      throws NoSuchObjectException, InvalidObjectException, MetaException,
      InvalidInputException, TException {
    try (MetaStoreClient client = catalog_.getMetaStoreClient()) {
      return client.getHiveClient().getThriftClient()
          .update_partition_column_statistics_req(setPartitionsStatsRequest);
    }
  }

  @Override
  public ColumnStatistics get_table_column_statistics(String s, String s1, String s2)
      throws NoSuchObjectException, MetaException, InvalidInputException,
      InvalidObjectException, TException {
    try (MetaStoreClient client = catalog_.getMetaStoreClient()) {
      return client.getHiveClient().getThriftClient().get_table_column_statistics(s, s1
          , s2);
    }
  }

  @Override
  public ColumnStatistics get_partition_column_statistics(String s, String s1, String s2,
      String s3)
      throws NoSuchObjectException, MetaException, InvalidInputException,
      InvalidObjectException, TException {
    try (MetaStoreClient client = catalog_.getMetaStoreClient()) {
      return client.getHiveClient().getThriftClient().get_partition_column_statistics(s
          , s1, s2, s3);
    }
  }

  @Override
  public TableStatsResult get_table_statistics_req(TableStatsRequest tableStatsRequest)
      throws NoSuchObjectException, MetaException, TException {
    try (MetaStoreClient client = catalog_.getMetaStoreClient()) {
      return client.getHiveClient().getThriftClient()
          .get_table_statistics_req(tableStatsRequest);
    }
  }

  @Override
  public PartitionsStatsResult get_partitions_statistics_req(
      PartitionsStatsRequest partitionsStatsRequest)
      throws NoSuchObjectException, MetaException, TException {
    try (MetaStoreClient client = catalog_.getMetaStoreClient()) {
      return client.getHiveClient().getThriftClient()
          .get_partitions_statistics_req(partitionsStatsRequest);
    }
  }

  @Override
  public AggrStats get_aggr_stats_for(PartitionsStatsRequest partitionsStatsRequest)
      throws NoSuchObjectException, MetaException, TException {
    try (MetaStoreClient client = catalog_.getMetaStoreClient()) {
      return client.getHiveClient().getThriftClient()
          .get_aggr_stats_for(partitionsStatsRequest);
    }
  }

  @Override
  public boolean set_aggr_stats_for(SetPartitionsStatsRequest setPartitionsStatsRequest)
      throws NoSuchObjectException, InvalidObjectException, MetaException,
      InvalidInputException, TException {
    try (MetaStoreClient client = catalog_.getMetaStoreClient()) {
      return client.getHiveClient().getThriftClient()
          .set_aggr_stats_for(setPartitionsStatsRequest);
    }
  }

  @Override
  public boolean delete_partition_column_statistics(String dbName, String tblName,
      String partName,
      String colName, String engine)
      throws NoSuchObjectException, MetaException, InvalidObjectException,
      InvalidInputException, TException {
    try (MetaStoreClient client = catalog_.getMetaStoreClient()) {
      return client.getHiveClient().getThriftClient()
          .delete_partition_column_statistics(dbName, tblName
              , partName, colName, engine);
    }
  }

  @Override
  public boolean delete_table_column_statistics(String dbName, String tblName,
      String columnName, String engien)
      throws NoSuchObjectException, MetaException, InvalidObjectException,
      InvalidInputException, TException {
    try (MetaStoreClient client = catalog_.getMetaStoreClient()) {
      return client.getHiveClient().getThriftClient()
          .delete_table_column_statistics(dbName,
              tblName, columnName, engien);
    }
  }

  @Override
  public void create_function(Function function)
      throws AlreadyExistsException, InvalidObjectException, MetaException,
      NoSuchObjectException, TException {
    try (MetaStoreClient client = catalog_.getMetaStoreClient()) {
      client.getHiveClient().getThriftClient().create_function(function);
    }
  }

  @Override
  public void drop_function(String dbName, String funcName)
      throws NoSuchObjectException, MetaException, TException {
    try (MetaStoreClient client = catalog_.getMetaStoreClient()) {
      client.getHiveClient().getThriftClient().drop_function(dbName, funcName);
    }
  }

  @Override
  public void alter_function(String s, String s1, Function function)
      throws InvalidOperationException, MetaException, TException {
    try (MetaStoreClient client = catalog_.getMetaStoreClient()) {
      client.getHiveClient().getThriftClient().alter_function(s, s1, function);
    }
  }

  @Override
  public List<String> get_functions(String s, String s1)
      throws MetaException, TException {
    try (MetaStoreClient client = catalog_.getMetaStoreClient()) {
      return client.getHiveClient().getThriftClient().get_functions(s, s1);
    }
  }

  @Override
  public Function get_function(String s, String s1)
      throws MetaException, NoSuchObjectException, TException {
    try (MetaStoreClient client = catalog_.getMetaStoreClient()) {
      return client.getHiveClient().getThriftClient().get_function(s, s1);
    }
  }

  @Override
  public GetAllFunctionsResponse get_all_functions() throws MetaException, TException {
    try (MetaStoreClient client = catalog_.getMetaStoreClient()) {
      return client.getHiveClient().getThriftClient().get_all_functions();
    }
  }

  @Override
  public boolean create_role(Role role) throws MetaException, TException {
    try (MetaStoreClient client = catalog_.getMetaStoreClient()) {
      return client.getHiveClient().getThriftClient().create_role(role);
    }
  }

  @Override
  public boolean drop_role(String s) throws MetaException, TException {
    try (MetaStoreClient client = catalog_.getMetaStoreClient()) {
      return client.getHiveClient().getThriftClient().drop_role(s);
    }
  }

  @Override
  public List<String> get_role_names() throws MetaException, TException {
    try (MetaStoreClient client = catalog_.getMetaStoreClient()) {
      return client.getHiveClient().getThriftClient().get_role_names();
    }
  }

  @Override
  public boolean grant_role(String roleName, String userName, PrincipalType principalType,
      String grantor,
      PrincipalType grantorType, boolean grantOption) throws MetaException, TException {
    try (MetaStoreClient client = catalog_.getMetaStoreClient()) {
      return client.getHiveClient().getThriftClient()
          .grant_role(roleName, userName, principalType,
              grantor, grantorType, grantOption);
    }
  }

  @Override
  public boolean revoke_role(String s, String s1, PrincipalType principalType)
      throws MetaException, TException {
    try (MetaStoreClient client = catalog_.getMetaStoreClient()) {
      return client.getHiveClient().getThriftClient().revoke_role(s, s1, principalType);
    }
  }

  @Override
  public List<Role> list_roles(String s, PrincipalType principalType)
      throws MetaException, TException {
    try (MetaStoreClient client = catalog_.getMetaStoreClient()) {
      return client.getHiveClient().getThriftClient().list_roles(s, principalType);
    }
  }

  @Override
  public GrantRevokeRoleResponse grant_revoke_role(
      GrantRevokeRoleRequest grantRevokeRoleRequest) throws MetaException, TException {
    try (MetaStoreClient client = catalog_.getMetaStoreClient()) {
      return client.getHiveClient().getThriftClient()
          .grant_revoke_role(grantRevokeRoleRequest);
    }
  }

  @Override
  public GetPrincipalsInRoleResponse get_principals_in_role(
      GetPrincipalsInRoleRequest getPrincipalsInRoleRequest)
      throws MetaException, TException {
    try (MetaStoreClient client = catalog_.getMetaStoreClient()) {
      return client.getHiveClient().getThriftClient()
          .get_principals_in_role(getPrincipalsInRoleRequest);
    }
  }

  @Override
  public GetRoleGrantsForPrincipalResponse get_role_grants_for_principal(
      GetRoleGrantsForPrincipalRequest getRoleGrantsForPrincipalRequest)
      throws MetaException, TException {
    try (MetaStoreClient client = catalog_.getMetaStoreClient()) {
      return client.getHiveClient().getThriftClient()
          .get_role_grants_for_principal(getRoleGrantsForPrincipalRequest);
    }
  }

  @Override
  public PrincipalPrivilegeSet get_privilege_set(HiveObjectRef hiveObjectRef, String s,
      List<String> list) throws MetaException, TException {
    try (MetaStoreClient client = catalog_.getMetaStoreClient()) {
      return client.getHiveClient().getThriftClient().get_privilege_set(hiveObjectRef,
          s, list);
    }
  }

  @Override
  public List<HiveObjectPrivilege> list_privileges(String s, PrincipalType principalType,
      HiveObjectRef hiveObjectRef) throws MetaException, TException {
    try (MetaStoreClient client = catalog_.getMetaStoreClient()) {
      return client.getHiveClient().getThriftClient().list_privileges(s, principalType,
          hiveObjectRef);
    }
  }

  @Override
  public boolean grant_privileges(PrivilegeBag privilegeBag)
      throws MetaException, TException {
    try (MetaStoreClient client = catalog_.getMetaStoreClient()) {
      return client.getHiveClient().getThriftClient().grant_privileges(privilegeBag);
    }
  }

  @Override
  public boolean revoke_privileges(PrivilegeBag privilegeBag)
      throws MetaException, TException {
    try (MetaStoreClient client = catalog_.getMetaStoreClient()) {
      return client.getHiveClient().getThriftClient().revoke_privileges(privilegeBag);
    }
  }

  @Override
  public GrantRevokePrivilegeResponse grant_revoke_privileges(
      GrantRevokePrivilegeRequest grantRevokePrivilegeRequest)
      throws MetaException, TException {
    try (MetaStoreClient client = catalog_.getMetaStoreClient()) {
      return client.getHiveClient().getThriftClient()
          .grant_revoke_privileges(grantRevokePrivilegeRequest);
    }
  }

  @Override
  public GrantRevokePrivilegeResponse refresh_privileges(HiveObjectRef hiveObjectRef,
      String s, GrantRevokePrivilegeRequest grantRevokePrivilegeRequest)
      throws MetaException, TException {
    try (MetaStoreClient client = catalog_.getMetaStoreClient()) {
      return client.getHiveClient().getThriftClient().refresh_privileges(hiveObjectRef, s,
          grantRevokePrivilegeRequest);
    }
  }

  @Override
  public List<String> set_ugi(String s, List<String> list)
      throws MetaException, TException {
    try (MetaStoreClient client = catalog_.getMetaStoreClient()) {
      return client.getHiveClient().getThriftClient().set_ugi(s, list);
    }
  }

  @Override
  public String get_delegation_token(String s, String s1)
      throws MetaException, TException {
    try (MetaStoreClient client = catalog_.getMetaStoreClient()) {
      return client.getHiveClient().getThriftClient().get_delegation_token(s, s1);
    }
  }

  @Override
  public long renew_delegation_token(String s) throws MetaException, TException {
    try (MetaStoreClient client = catalog_.getMetaStoreClient()) {
      return client.getHiveClient().getThriftClient().renew_delegation_token(s);
    }
  }

  @Override
  public void cancel_delegation_token(String s) throws MetaException, TException {
    try (MetaStoreClient client = catalog_.getMetaStoreClient()) {
      client.getHiveClient().getThriftClient().cancel_delegation_token(s);
    }
  }

  @Override
  public boolean add_token(String tokenIdentifier, String delegationToken)
      throws TException {
    try (MetaStoreClient client = catalog_.getMetaStoreClient()) {
      return client.getHiveClient().getThriftClient()
          .add_token(tokenIdentifier, delegationToken);
    }
  }

  @Override
  public boolean remove_token(String tokenIdentifier) throws TException {
    try (MetaStoreClient client = catalog_.getMetaStoreClient()) {
      return client.getHiveClient().getThriftClient().remove_token(tokenIdentifier);
    }
  }

  @Override
  public String get_token(String s) throws TException {
    try (MetaStoreClient client = catalog_.getMetaStoreClient()) {
      return client.getHiveClient().getThriftClient().get_token(s);
    }
  }

  @Override
  public List<String> get_all_token_identifiers() throws TException {
    try (MetaStoreClient client = catalog_.getMetaStoreClient()) {
      return client.getHiveClient().getThriftClient().get_all_token_identifiers();
    }
  }

  @Override
  public int add_master_key(String s) throws MetaException, TException {
    try (MetaStoreClient client = catalog_.getMetaStoreClient()) {
      return client.getHiveClient().getThriftClient().add_master_key(s);
    }
  }

  @Override
  public void update_master_key(int i, String s)
      throws NoSuchObjectException, MetaException, TException {
    try (MetaStoreClient client = catalog_.getMetaStoreClient()) {
      client.getHiveClient().getThriftClient().update_master_key(i, s);
    }
  }

  @Override
  public boolean remove_master_key(int i) throws TException {
    try (MetaStoreClient client = catalog_.getMetaStoreClient()) {
      return client.getHiveClient().getThriftClient().remove_master_key(i);
    }
  }

  @Override
  public List<String> get_master_keys() throws TException {
    try (MetaStoreClient client = catalog_.getMetaStoreClient()) {
      return client.getHiveClient().getThriftClient().get_master_keys();
    }
  }

  @Override
  public GetOpenTxnsResponse get_open_txns() throws TException {
    try (MetaStoreClient client = catalog_.getMetaStoreClient()) {
      return client.getHiveClient().getThriftClient().get_open_txns();
    }
  }

  @Override
  public GetOpenTxnsInfoResponse get_open_txns_info() throws TException {
    try (MetaStoreClient client = catalog_.getMetaStoreClient()) {
      return client.getHiveClient().getThriftClient().get_open_txns_info();
    }
  }

  @Override
  public OpenTxnsResponse open_txns(OpenTxnRequest openTxnRequest) throws TException {
    try (MetaStoreClient client = catalog_.getMetaStoreClient()) {
      return client.getHiveClient().getThriftClient().open_txns(openTxnRequest);
    }
  }

  @Override
  public void abort_txn(AbortTxnRequest abortTxnRequest)
      throws NoSuchTxnException, TException {
    try (MetaStoreClient client = catalog_.getMetaStoreClient()) {
      client.getHiveClient().getThriftClient().abort_txn(abortTxnRequest);
    }
  }

  @Override
  public void abort_txns(AbortTxnsRequest abortTxnsRequest)
      throws NoSuchTxnException, TException {
    try (MetaStoreClient client = catalog_.getMetaStoreClient()) {
      client.getHiveClient().getThriftClient().abort_txns(abortTxnsRequest);
    }
  }

  @Override
  public void commit_txn(CommitTxnRequest commitTxnRequest)
      throws NoSuchTxnException, TxnAbortedException, TException {
    try (MetaStoreClient client = catalog_.getMetaStoreClient()) {
      client.getHiveClient().getThriftClient().commit_txn(commitTxnRequest);
    }
  }

  @Override
  public void repl_tbl_writeid_state(
      ReplTblWriteIdStateRequest replTblWriteIdStateRequest) throws TException {
    try (MetaStoreClient client = catalog_.getMetaStoreClient()) {
      client.getHiveClient().getThriftClient()
          .repl_tbl_writeid_state(replTblWriteIdStateRequest);
    }
  }

  @Override
  public GetValidWriteIdsResponse get_valid_write_ids(
      GetValidWriteIdsRequest getValidWriteIdsRequest)
      throws NoSuchTxnException, MetaException, TException {
    try (MetaStoreClient client = catalog_.getMetaStoreClient()) {
      return client.getHiveClient().getThriftClient()
          .get_valid_write_ids(getValidWriteIdsRequest);
    }
  }

  @Override
  public AllocateTableWriteIdsResponse allocate_table_write_ids(
      AllocateTableWriteIdsRequest allocateTableWriteIdsRequest)
      throws NoSuchTxnException, TxnAbortedException, MetaException, TException {
    try (MetaStoreClient client = catalog_.getMetaStoreClient()) {
      return client.getHiveClient().getThriftClient()
          .allocate_table_write_ids(allocateTableWriteIdsRequest);
    }
  }

  @Override
  public LockResponse lock(LockRequest lockRequest)
      throws NoSuchTxnException, TxnAbortedException, TException {
    try (MetaStoreClient client = catalog_.getMetaStoreClient()) {
      return client.getHiveClient().getThriftClient().lock(lockRequest);
    }
  }

  @Override
  public LockResponse check_lock(CheckLockRequest checkLockRequest)
      throws NoSuchTxnException, TxnAbortedException, NoSuchLockException, TException {
    try (MetaStoreClient client = catalog_.getMetaStoreClient()) {
      return client.getHiveClient().getThriftClient().check_lock(checkLockRequest);
    }
  }

  @Override
  public void unlock(UnlockRequest unlockRequest)
      throws NoSuchLockException, TxnOpenException, TException {
    try (MetaStoreClient client = catalog_.getMetaStoreClient()) {
      client.getHiveClient().getThriftClient().unlock(unlockRequest);
    }
  }

  @Override
  public ShowLocksResponse show_locks(ShowLocksRequest showLocksRequest)
      throws TException {
    try (MetaStoreClient client = catalog_.getMetaStoreClient()) {
      return client.getHiveClient().getThriftClient().show_locks(showLocksRequest);
    }
  }

  @Override
  public void heartbeat(HeartbeatRequest heartbeatRequest)
      throws NoSuchLockException, NoSuchTxnException, TxnAbortedException, TException {
    try (MetaStoreClient client = catalog_.getMetaStoreClient()) {
      client.getHiveClient().getThriftClient().heartbeat(heartbeatRequest);
    }
  }

  @Override
  public HeartbeatTxnRangeResponse heartbeat_txn_range(
      HeartbeatTxnRangeRequest heartbeatTxnRangeRequest) throws TException {
    try (MetaStoreClient client = catalog_.getMetaStoreClient()) {
      return client.getHiveClient().getThriftClient()
          .heartbeat_txn_range(heartbeatTxnRangeRequest);
    }
  }

  @Override
  public void compact(CompactionRequest compactionRequest) throws TException {
    try (MetaStoreClient client = catalog_.getMetaStoreClient()) {
      client.getHiveClient().getThriftClient().compact(compactionRequest);
    }
  }

  @Override
  public CompactionResponse compact2(CompactionRequest compactionRequest)
      throws TException {
    try (MetaStoreClient client = catalog_.getMetaStoreClient()) {
      return client.getHiveClient().getThriftClient().compact2(compactionRequest);
    }
  }

  @Override
  public ShowCompactResponse show_compact(ShowCompactRequest showCompactRequest)
      throws TException {
    try (MetaStoreClient client = catalog_.getMetaStoreClient()) {
      return client.getHiveClient().getThriftClient().show_compact(showCompactRequest);
    }
  }

  @Override
  public void add_dynamic_partitions(AddDynamicPartitions addDynamicPartitions)
      throws NoSuchTxnException, TxnAbortedException, TException {
    try (MetaStoreClient client = catalog_.getMetaStoreClient()) {
      client.getHiveClient().getThriftClient()
          .add_dynamic_partitions(addDynamicPartitions);
    }
  }

  @Override
  public OptionalCompactionInfoStruct find_next_compact(String s)
      throws MetaException, TException {
    try (MetaStoreClient client = catalog_.getMetaStoreClient()) {
      return client.getHiveClient().getThriftClient().find_next_compact(s);
    }
  }

  @Override
  public void update_compactor_state(CompactionInfoStruct compactionInfoStruct, long l)
      throws TException {
    try (MetaStoreClient client = catalog_.getMetaStoreClient()) {
      client.getHiveClient().getThriftClient()
          .update_compactor_state(compactionInfoStruct, l);
    }
  }

  @Override
  public List<String> find_columns_with_stats(CompactionInfoStruct compactionInfoStruct)
      throws TException {
    try (MetaStoreClient client = catalog_.getMetaStoreClient()) {
      return client.getHiveClient().getThriftClient()
          .find_columns_with_stats(compactionInfoStruct);
    }
  }

  @Override
  public void mark_cleaned(CompactionInfoStruct compactionInfoStruct)
      throws MetaException, TException {
    try (MetaStoreClient client = catalog_.getMetaStoreClient()) {
      client.getHiveClient().getThriftClient().mark_cleaned(compactionInfoStruct);
    }
  }

  @Override
  public void mark_compacted(CompactionInfoStruct compactionInfoStruct)
      throws MetaException, TException {
    try (MetaStoreClient client = catalog_.getMetaStoreClient()) {
      client.getHiveClient().markCompacted(compactionInfoStruct);
    }
  }

  @Override
  public void mark_failed(CompactionInfoStruct compactionInfoStruct)
      throws MetaException, TException {
    try (MetaStoreClient client = catalog_.getMetaStoreClient()) {
      client.getHiveClient().getThriftClient().mark_failed(compactionInfoStruct);
    }
  }

  @Override
  public MaxAllocatedTableWriteIdResponse get_max_allocated_table_write_id(
      MaxAllocatedTableWriteIdRequest rqst)
      throws MetaException, TException {
    try (MetaStoreClient client = catalog_.getMetaStoreClient()) {
      return client.getHiveClient().getThriftClient()
          .get_max_allocated_table_write_id(rqst);
    }
  }

  @Override
  public void seed_write_id(SeedTableWriteIdsRequest rqst)
      throws MetaException, TException {
    try (MetaStoreClient client = catalog_.getMetaStoreClient()) {
      client.getHiveClient().getThriftClient().seed_write_id(rqst);
    }
  }

  @Override
  public void seed_txn_id(SeedTxnIdRequest rqst)
      throws MetaException, TException {
    try (MetaStoreClient client = catalog_.getMetaStoreClient()) {
      client.getHiveClient().getThriftClient().seed_txn_id(rqst);
    }
  }

  @Override
  public void set_hadoop_jobid(String s, long l) throws TException {
    try (MetaStoreClient client = catalog_.getMetaStoreClient()) {
      client.getHiveClient().getThriftClient().set_hadoop_jobid(s, l);
    }
  }

  @Override
  public NotificationEventResponse get_next_notification(
      NotificationEventRequest notificationEventRequest) throws TException {
    try (MetaStoreClient client = catalog_.getMetaStoreClient()) {
      return client.getHiveClient().getThriftClient()
          .get_next_notification(notificationEventRequest);
    }
  }

  @Override
  public CurrentNotificationEventId get_current_notificationEventId() throws TException {
    try (MetaStoreClient client = catalog_.getMetaStoreClient()) {
      return client.getHiveClient().getThriftClient().get_current_notificationEventId();
    }
  }

  @Override
  public NotificationEventsCountResponse get_notification_events_count(
      NotificationEventsCountRequest notificationEventsCountRequest) throws TException {
    try (MetaStoreClient client = catalog_.getMetaStoreClient()) {
      return client.getHiveClient().getThriftClient()
          .get_notification_events_count(notificationEventsCountRequest);
    }
  }

  @Override
  public FireEventResponse fire_listener_event(FireEventRequest fireEventRequest)
      throws TException {
    try (MetaStoreClient client = catalog_.getMetaStoreClient()) {
      return client.getHiveClient().getThriftClient()
          .fire_listener_event(fireEventRequest);
    }
  }

  @Override
  public void flushCache() throws TException {
    try (MetaStoreClient client = catalog_.getMetaStoreClient()) {
      client.getHiveClient().getThriftClient().flushCache();
    }
  }

  @Override
  public WriteNotificationLogResponse add_write_notification_log(
      WriteNotificationLogRequest writeNotificationLogRequest) throws TException {
    try (MetaStoreClient client = catalog_.getMetaStoreClient()) {
      return client.getHiveClient().getThriftClient()
          .add_write_notification_log(writeNotificationLogRequest);
    }
  }

  @Override
  public CmRecycleResponse cm_recycle(CmRecycleRequest cmRecycleRequest)
      throws MetaException, TException {
    try (MetaStoreClient client = catalog_.getMetaStoreClient()) {
      return client.getHiveClient().getThriftClient().cm_recycle(cmRecycleRequest);
    }
  }

  @Override
  public GetFileMetadataByExprResult get_file_metadata_by_expr(
      GetFileMetadataByExprRequest getFileMetadataByExprRequest) throws TException {
    try (MetaStoreClient client = catalog_.getMetaStoreClient()) {
      return client.getHiveClient().getThriftClient()
          .get_file_metadata_by_expr(getFileMetadataByExprRequest);
    }
  }

  @Override
  public GetFileMetadataResult get_file_metadata(
      GetFileMetadataRequest getFileMetadataRequest) throws TException {
    try (MetaStoreClient client = catalog_.getMetaStoreClient()) {
      return client.getHiveClient().getThriftClient()
          .get_file_metadata(getFileMetadataRequest);
    }
  }

  @Override
  public PutFileMetadataResult put_file_metadata(
      PutFileMetadataRequest putFileMetadataRequest) throws TException {
    try (MetaStoreClient client = catalog_.getMetaStoreClient()) {
      return client.getHiveClient().getThriftClient()
          .put_file_metadata(putFileMetadataRequest);
    }
  }

  @Override
  public ClearFileMetadataResult clear_file_metadata(
      ClearFileMetadataRequest clearFileMetadataRequest) throws TException {
    try (MetaStoreClient client = catalog_.getMetaStoreClient()) {
      return client.getHiveClient().getThriftClient()
          .clear_file_metadata(clearFileMetadataRequest);
    }
  }

  @Override
  public CacheFileMetadataResult cache_file_metadata(
      CacheFileMetadataRequest cacheFileMetadataRequest) throws TException {
    try (MetaStoreClient client = catalog_.getMetaStoreClient()) {
      return client.getHiveClient().getThriftClient()
          .cache_file_metadata(cacheFileMetadataRequest);
    }
  }

  @Override
  public String get_metastore_db_uuid() throws MetaException, TException {
    try (MetaStoreClient client = catalog_.getMetaStoreClient()) {
      return client.getHiveClient().getThriftClient().get_metastore_db_uuid();
    }
  }

  @Override
  public WMCreateResourcePlanResponse create_resource_plan(
      WMCreateResourcePlanRequest wmCreateResourcePlanRequest)
      throws AlreadyExistsException, InvalidObjectException, MetaException, TException {
    try (MetaStoreClient client = catalog_.getMetaStoreClient()) {
      return client.getHiveClient().getThriftClient()
          .create_resource_plan(wmCreateResourcePlanRequest);
    }
  }

  @Override
  public WMGetResourcePlanResponse get_resource_plan(
      WMGetResourcePlanRequest wmGetResourcePlanRequest)
      throws NoSuchObjectException, MetaException, TException {
    try (MetaStoreClient client = catalog_.getMetaStoreClient()) {
      return client.getHiveClient().getThriftClient()
          .get_resource_plan(wmGetResourcePlanRequest);
    }
  }

  @Override
  public WMGetActiveResourcePlanResponse get_active_resource_plan(
      WMGetActiveResourcePlanRequest wmGetActiveResourcePlanRequest)
      throws MetaException, TException {
    try (MetaStoreClient client = catalog_.getMetaStoreClient()) {
      return client.getHiveClient().getThriftClient()
          .get_active_resource_plan(wmGetActiveResourcePlanRequest);
    }
  }

  @Override
  public WMGetAllResourcePlanResponse get_all_resource_plans(
      WMGetAllResourcePlanRequest wmGetAllResourcePlanRequest)
      throws MetaException, TException {
    try (MetaStoreClient client = catalog_.getMetaStoreClient()) {
      return client.getHiveClient().getThriftClient()
          .get_all_resource_plans(wmGetAllResourcePlanRequest);
    }
  }

  @Override
  public WMAlterResourcePlanResponse alter_resource_plan(
      WMAlterResourcePlanRequest wmAlterResourcePlanRequest)
      throws NoSuchObjectException, InvalidOperationException, MetaException, TException {
    try (MetaStoreClient client = catalog_.getMetaStoreClient()) {
      return client.getHiveClient().getThriftClient()
          .alter_resource_plan(wmAlterResourcePlanRequest);
    }
  }

  @Override
  public WMValidateResourcePlanResponse validate_resource_plan(
      WMValidateResourcePlanRequest wmValidateResourcePlanRequest)
      throws NoSuchObjectException, MetaException, TException {
    try (MetaStoreClient client = catalog_.getMetaStoreClient()) {
      return client.getHiveClient().getThriftClient()
          .validate_resource_plan(wmValidateResourcePlanRequest);
    }
  }

  @Override
  public WMDropResourcePlanResponse drop_resource_plan(
      WMDropResourcePlanRequest wmDropResourcePlanRequest)
      throws NoSuchObjectException, InvalidOperationException, MetaException, TException {
    try (MetaStoreClient client = catalog_.getMetaStoreClient()) {
      return client.getHiveClient().getThriftClient()
          .drop_resource_plan(wmDropResourcePlanRequest);
    }
  }

  @Override
  public WMCreateTriggerResponse create_wm_trigger(
      WMCreateTriggerRequest wmCreateTriggerRequest)
      throws AlreadyExistsException, NoSuchObjectException, InvalidObjectException,
      MetaException, TException {
    try (MetaStoreClient client = catalog_.getMetaStoreClient()) {
      return client.getHiveClient().getThriftClient()
          .create_wm_trigger(wmCreateTriggerRequest);
    }
  }

  @Override
  public WMAlterTriggerResponse alter_wm_trigger(
      WMAlterTriggerRequest wmAlterTriggerRequest)
      throws NoSuchObjectException, InvalidObjectException, MetaException, TException {
    try (MetaStoreClient client = catalog_.getMetaStoreClient()) {
      return client.getHiveClient().getThriftClient()
          .alter_wm_trigger(wmAlterTriggerRequest);
    }
  }

  @Override
  public WMDropTriggerResponse drop_wm_trigger(WMDropTriggerRequest wmDropTriggerRequest)
      throws NoSuchObjectException, InvalidOperationException, MetaException, TException {
    try (MetaStoreClient client = catalog_.getMetaStoreClient()) {
      return client.getHiveClient().getThriftClient()
          .drop_wm_trigger(wmDropTriggerRequest);
    }
  }

  @Override
  public WMGetTriggersForResourePlanResponse get_triggers_for_resourceplan(
      WMGetTriggersForResourePlanRequest wmGetTriggersForResourePlanRequest)
      throws NoSuchObjectException, MetaException, TException {
    try (MetaStoreClient client = catalog_.getMetaStoreClient()) {
      return client.getHiveClient().getThriftClient()
          .get_triggers_for_resourceplan(wmGetTriggersForResourePlanRequest);
    }
  }

  @Override
  public WMCreatePoolResponse create_wm_pool(WMCreatePoolRequest wmCreatePoolRequest)
      throws AlreadyExistsException, NoSuchObjectException, InvalidObjectException,
      MetaException, TException {
    try (MetaStoreClient client = catalog_.getMetaStoreClient()) {
      return client.getHiveClient().getThriftClient()
          .create_wm_pool(wmCreatePoolRequest);
    }
  }

  @Override
  public WMAlterPoolResponse alter_wm_pool(WMAlterPoolRequest wmAlterPoolRequest)
      throws AlreadyExistsException, NoSuchObjectException, InvalidObjectException,
      MetaException, TException {
    try (MetaStoreClient client = catalog_.getMetaStoreClient()) {
      return client.getHiveClient().getThriftClient().alter_wm_pool(wmAlterPoolRequest);
    }
  }

  @Override
  public WMDropPoolResponse drop_wm_pool(WMDropPoolRequest wmDropPoolRequest)
      throws NoSuchObjectException, InvalidOperationException, MetaException, TException {
    try (MetaStoreClient client = catalog_.getMetaStoreClient()) {
      return client.getHiveClient().getThriftClient().drop_wm_pool(wmDropPoolRequest);
    }
  }

  @Override
  public WMCreateOrUpdateMappingResponse create_or_update_wm_mapping(
      WMCreateOrUpdateMappingRequest wmCreateOrUpdateMappingRequest)
      throws AlreadyExistsException, NoSuchObjectException, InvalidObjectException,
      MetaException, TException {
    try (MetaStoreClient client = catalog_.getMetaStoreClient()) {
      return client.getHiveClient().getThriftClient()
          .create_or_update_wm_mapping(wmCreateOrUpdateMappingRequest);
    }
  }

  @Override
  public WMDropMappingResponse drop_wm_mapping(WMDropMappingRequest wmDropMappingRequest)
      throws NoSuchObjectException, InvalidOperationException, MetaException, TException {
    try (MetaStoreClient client = catalog_.getMetaStoreClient()) {
      return client.getHiveClient().getThriftClient()
          .drop_wm_mapping(wmDropMappingRequest);
    }
  }

  @Override
  public WMCreateOrDropTriggerToPoolMappingResponse
    create_or_drop_wm_trigger_to_pool_mapping(
      WMCreateOrDropTriggerToPoolMappingRequest wmCreateOrDropTriggerToPoolMappingRequest)
      throws AlreadyExistsException, NoSuchObjectException, InvalidObjectException,
      MetaException, TException {
    try (MetaStoreClient client = catalog_.getMetaStoreClient()) {
      return client.getHiveClient().getThriftClient()
          .create_or_drop_wm_trigger_to_pool_mapping(
              wmCreateOrDropTriggerToPoolMappingRequest);
    }
  }

  @Override
  public void create_ischema(ISchema iSchema)
      throws AlreadyExistsException, NoSuchObjectException, MetaException, TException {
    try (MetaStoreClient client = catalog_.getMetaStoreClient()) {
      client.getHiveClient().getThriftClient().create_ischema(iSchema);
    }
  }

  @Override
  public void alter_ischema(AlterISchemaRequest alterISchemaRequest)
      throws NoSuchObjectException, MetaException, TException {
    try (MetaStoreClient client = catalog_.getMetaStoreClient()) {
      client.getHiveClient().getThriftClient().alter_ischema(alterISchemaRequest);
    }
  }

  @Override
  public ISchema get_ischema(ISchemaName iSchemaName)
      throws NoSuchObjectException, MetaException, TException {
    try (MetaStoreClient client = catalog_.getMetaStoreClient()) {
      return client.getHiveClient().getThriftClient().get_ischema(iSchemaName);
    }
  }

  @Override
  public void drop_ischema(ISchemaName iSchemaName)
      throws NoSuchObjectException, InvalidOperationException, MetaException, TException {
    try (MetaStoreClient client = catalog_.getMetaStoreClient()) {
      client.getHiveClient().getThriftClient().drop_ischema(iSchemaName);
    }
  }

  @Override
  public void add_schema_version(SchemaVersion schemaVersion)
      throws AlreadyExistsException, NoSuchObjectException, MetaException, TException {
    try (MetaStoreClient client = catalog_.getMetaStoreClient()) {
      client.getHiveClient().getThriftClient().add_schema_version(schemaVersion);
    }
  }

  @Override
  public SchemaVersion get_schema_version(SchemaVersionDescriptor schemaVersionDescriptor)
      throws NoSuchObjectException, MetaException, TException {
    try (MetaStoreClient client = catalog_.getMetaStoreClient()) {
      return client.getHiveClient().getThriftClient()
          .get_schema_version(schemaVersionDescriptor);
    }
  }

  @Override
  public SchemaVersion get_schema_latest_version(ISchemaName iSchemaName)
      throws NoSuchObjectException, MetaException, TException {
    try (MetaStoreClient client = catalog_.getMetaStoreClient()) {
      return client.getHiveClient().getThriftClient()
          .get_schema_latest_version(iSchemaName);
    }
  }

  @Override
  public List<SchemaVersion> get_schema_all_versions(ISchemaName iSchemaName)
      throws NoSuchObjectException, MetaException, TException {
    try (MetaStoreClient client = catalog_.getMetaStoreClient()) {
      return client.getHiveClient().getThriftClient()
          .get_schema_all_versions(iSchemaName);
    }
  }

  @Override
  public void drop_schema_version(SchemaVersionDescriptor schemaVersionDescriptor)
      throws NoSuchObjectException, MetaException, TException {
    try (MetaStoreClient client = catalog_.getMetaStoreClient()) {
      client.getHiveClient().getThriftClient()
          .drop_schema_version(schemaVersionDescriptor);
    }
  }

  @Override
  public FindSchemasByColsResp get_schemas_by_cols(
      FindSchemasByColsRqst findSchemasByColsRqst) throws MetaException, TException {
    try (MetaStoreClient client = catalog_.getMetaStoreClient()) {
      return client.getHiveClient().getThriftClient()
          .get_schemas_by_cols(findSchemasByColsRqst);
    }
  }

  @Override
  public void map_schema_version_to_serde(
      MapSchemaVersionToSerdeRequest mapSchemaVersionToSerdeRequest)
      throws NoSuchObjectException, MetaException, TException {
    try (MetaStoreClient client = catalog_.getMetaStoreClient()) {
      client.getHiveClient().getThriftClient()
          .map_schema_version_to_serde(mapSchemaVersionToSerdeRequest);
    }
  }

  @Override
  public void set_schema_version_state(
      SetSchemaVersionStateRequest setSchemaVersionStateRequest)
      throws NoSuchObjectException, InvalidOperationException, MetaException, TException {
    try (MetaStoreClient client = catalog_.getMetaStoreClient()) {
      client.getHiveClient().getThriftClient()
          .set_schema_version_state(setSchemaVersionStateRequest);
    }
  }

  @Override
  public void add_serde(SerDeInfo serDeInfo)
      throws AlreadyExistsException, MetaException, TException {
    try (MetaStoreClient client = catalog_.getMetaStoreClient()) {
      client.getHiveClient().getThriftClient().add_serde(serDeInfo);
    }
  }

  @Override
  public SerDeInfo get_serde(GetSerdeRequest getSerdeRequest)
      throws NoSuchObjectException, MetaException, TException {
    try (MetaStoreClient client = catalog_.getMetaStoreClient()) {
      return client.getHiveClient().getThriftClient().get_serde(getSerdeRequest);
    }
  }

  @Override
  public LockResponse get_lock_materialization_rebuild(String s, String s1, long l)
      throws TException {
    try (MetaStoreClient client = catalog_.getMetaStoreClient()) {
      return client.getHiveClient().getThriftClient()
          .get_lock_materialization_rebuild(s, s1, l);
    }
  }

  @Override
  public boolean heartbeat_lock_materialization_rebuild(String s, String s1, long l)
      throws TException {
    try (MetaStoreClient client = catalog_.getMetaStoreClient()) {
      return client.getHiveClient().getThriftClient()
          .heartbeat_lock_materialization_rebuild(s, s1, l);
    }
  }

  @Override
  public void add_runtime_stats(RuntimeStat runtimeStat)
      throws MetaException, TException {
    try (MetaStoreClient client = catalog_.getMetaStoreClient()) {
      client.getHiveClient().getThriftClient().add_runtime_stats(runtimeStat);
    }
  }

  @Override
  public List<RuntimeStat> get_runtime_stats(
      GetRuntimeStatsRequest getRuntimeStatsRequest) throws MetaException, TException {
    try (MetaStoreClient client = catalog_.getMetaStoreClient()) {
      return client.getHiveClient().getThriftClient()
          .get_runtime_stats(getRuntimeStatsRequest);
    }
  }

  @Override
  public ScheduledQueryPollResponse scheduled_query_poll(
      ScheduledQueryPollRequest scheduledQueryPollRequest)
      throws MetaException, TException {
    try (MetaStoreClient client = catalog_.getMetaStoreClient()) {
      return client.getHiveClient().getThriftClient()
          .scheduled_query_poll(scheduledQueryPollRequest);
    }
  }

  @Override
  public void scheduled_query_maintenance(
      ScheduledQueryMaintenanceRequest scheduledQueryMaintenanceRequest)
      throws MetaException, NoSuchObjectException, AlreadyExistsException,
      InvalidInputException, TException {
    try (MetaStoreClient client = catalog_.getMetaStoreClient()) {
      client.getHiveClient().getThriftClient()
          .scheduled_query_maintenance(scheduledQueryMaintenanceRequest);
    }
  }

  @Override
  public void scheduled_query_progress(
      ScheduledQueryProgressInfo scheduledQueryProgressInfo)
      throws MetaException, InvalidOperationException, TException {
    try (MetaStoreClient client = catalog_.getMetaStoreClient()) {
      client.getHiveClient().getThriftClient()
          .scheduled_query_progress(scheduledQueryProgressInfo);
    }
  }

  @Override
  public ScheduledQuery get_scheduled_query(ScheduledQueryKey scheduledQueryKey)
      throws MetaException, NoSuchObjectException, TException {
    try (MetaStoreClient client = catalog_.getMetaStoreClient()) {
      return client.getHiveClient().getThriftClient()
          .get_scheduled_query(scheduledQueryKey);
    }
  }

  @Override
  public void add_replication_metrics(ReplicationMetricList replicationMetricList)
      throws MetaException, NoSuchObjectException, TException {
    try (MetaStoreClient client = catalog_.getMetaStoreClient()) {
      client.getHiveClient().getThriftClient()
          .add_replication_metrics(replicationMetricList);
    }
  }

  @Override
  public ReplicationMetricList get_replication_metrics(
      GetReplicationMetricsRequest getReplicationMetricsRequest)
      throws MetaException, NoSuchObjectException, TException {
    try (MetaStoreClient client = catalog_.getMetaStoreClient()) {
      return client.getHiveClient().getThriftClient()
          .get_replication_metrics(getReplicationMetricsRequest);
    }
  }

  @Override
  public long get_latest_txnid_in_conflict(long txnId) throws MetaException, TException {
    try (MetaStoreClient client = catalog_.getMetaStoreClient()) {
      return client.getHiveClient().getThriftClient().get_latest_txnid_in_conflict(txnId);
    }
  }

  @Override
  public String getName() throws TException {
    throw new UnsupportedOperationException(String.format(NOT_IMPLEMENTED_UNSUPPORTED,
        "getName"));
  }

  @Override
  public String getVersion() throws TException {
    throw new UnsupportedOperationException(String.format(NOT_IMPLEMENTED_UNSUPPORTED,
        "getVersion"));
  }

  @Override
  public fb_status getStatus() throws TException {
    throw new UnsupportedOperationException(String.format(NOT_IMPLEMENTED_UNSUPPORTED,
        "getStatus"));
  }

  @Override
  public String getStatusDetails() throws TException {
    throw new UnsupportedOperationException(String.format(NOT_IMPLEMENTED_UNSUPPORTED,
        "getStatusDetails"));
  }

  @Override
  public Map<String, Long> getCounters() throws TException {
    throw new UnsupportedOperationException(String.format(NOT_IMPLEMENTED_UNSUPPORTED,
        "getCounters"));
  }

  @Override
  public long getCounter(String s) throws TException {
    throw new UnsupportedOperationException(String.format(NOT_IMPLEMENTED_UNSUPPORTED,
        "getCounter"));
  }

  @Override
  public void setOption(String s, String s1) throws TException {
    throw new UnsupportedOperationException(String.format(NOT_IMPLEMENTED_UNSUPPORTED,
        "setOption"));

  }

  @Override
  public String getOption(String s) throws TException {
    throw new UnsupportedOperationException(String.format(NOT_IMPLEMENTED_UNSUPPORTED,
        "getOption"));
  }

  @Override
  public Map<String, String> getOptions() throws TException {
    throw new UnsupportedOperationException(String.format(NOT_IMPLEMENTED_UNSUPPORTED,
        "getOptions"));
  }

  @Override
  public String getCpuProfile(int i) throws TException {
    throw new UnsupportedOperationException(String.format(NOT_IMPLEMENTED_UNSUPPORTED,
        "getCpuProfile"));
  }

  @Override
  public long aliveSince() throws TException {
    throw new UnsupportedOperationException(String.format(NOT_IMPLEMENTED_UNSUPPORTED,
        "aliveSince"));
  }

  @Override
  public void reinitialize() throws TException {
    throw new UnsupportedOperationException(String.format(NOT_IMPLEMENTED_UNSUPPORTED,
        "reinitialize"));

  }

  @Override
  public void shutdown() throws TException {
    // nothing to do. Use this call to clean-up any session specific clean-up.
  }
}
