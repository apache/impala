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

import com.google.common.base.Preconditions;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang.exception.ExceptionUtils;
import org.apache.hadoop.hive.metastore.api.AddPartitionsRequest;
import org.apache.hadoop.hive.metastore.api.AddPartitionsResult;
import org.apache.hadoop.hive.metastore.api.AlreadyExistsException;
import org.apache.hadoop.hive.metastore.api.AlterPartitionsRequest;
import org.apache.hadoop.hive.metastore.api.AlterPartitionsResponse;
import org.apache.hadoop.hive.metastore.api.AlterTableRequest;
import org.apache.hadoop.hive.metastore.api.AlterTableResponse;
import org.apache.hadoop.hive.metastore.api.CreateTableRequest;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.DropDatabaseRequest;
import org.apache.hadoop.hive.metastore.api.DropPartitionsRequest;
import org.apache.hadoop.hive.metastore.api.DropPartitionsResult;
import org.apache.hadoop.hive.metastore.api.EnvironmentContext;
import org.apache.hadoop.hive.metastore.api.GetPartitionsByNamesRequest;
import org.apache.hadoop.hive.metastore.api.GetPartitionsByNamesResult;
import org.apache.hadoop.hive.metastore.api.GetTableRequest;
import org.apache.hadoop.hive.metastore.api.GetTableResult;
import org.apache.hadoop.hive.metastore.api.InvalidObjectException;
import org.apache.hadoop.hive.metastore.api.InvalidOperationException;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.NoSuchObjectException;
import org.apache.hadoop.hive.metastore.api.NotificationEvent;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.PartitionSpec;
import org.apache.hadoop.hive.metastore.api.PartitionsByExprRequest;
import org.apache.hadoop.hive.metastore.api.PartitionsByExprResult;
import org.apache.hadoop.hive.metastore.api.RenamePartitionRequest;
import org.apache.hadoop.hive.metastore.api.RenamePartitionResponse;
import org.apache.hadoop.hive.metastore.api.SQLCheckConstraint;
import org.apache.hadoop.hive.metastore.api.SQLDefaultConstraint;
import org.apache.hadoop.hive.metastore.api.SQLForeignKey;
import org.apache.hadoop.hive.metastore.api.SQLNotNullConstraint;
import org.apache.hadoop.hive.metastore.api.SQLPrimaryKey;
import org.apache.hadoop.hive.metastore.api.SQLUniqueConstraint;
import org.apache.hadoop.hive.metastore.api.TruncateTableRequest;
import org.apache.hadoop.hive.metastore.api.TruncateTableResponse;
import org.apache.hadoop.hive.metastore.utils.MetaStoreUtils;
import org.apache.hadoop.util.StringUtils;
import org.apache.impala.catalog.CatalogHmsAPIHelper;
import org.apache.impala.catalog.events.DeleteEventLog;
import org.apache.impala.catalog.events.MetastoreEvents;
import org.apache.impala.catalog.events.MetastoreEventsProcessor;
import org.apache.impala.common.Pair;
import org.apache.impala.service.BackendConfig;
import org.apache.impala.service.CatalogOpExecutor;
import org.apache.impala.service.MetadataOp;
import org.apache.impala.thrift.TImpalaTableType;
import org.apache.impala.thrift.TTableName;
import org.apache.impala.util.EventSequence;
import org.apache.impala.util.NoOpEventSequence;
import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.impala.catalog.Db;
import org.apache.impala.catalog.CatalogException;

/**
 * This class implements the HMS APIs that are served by CatalogD
 * and is exposed via {@link CatalogMetastoreServer}.
 * HMS APIs that are redirected to HMS can be found in {@link MetastoreServiceHandler}.
 *
 */
public class CatalogMetastoreServiceHandler extends MetastoreServiceHandler {

  private static final Logger LOG = LoggerFactory
      .getLogger(CatalogMetastoreServiceHandler.class);

  private final String SYNC_TABLE_LATEST_EVENT_ID_ERR_MSG = "Failed to sync table %s "
      + "to latest event id while executing %s";
  private final String SYNC_DB_LATEST_EVENT_ID_ERR_MSG = "Failed to sync db %s to " +
          "latest event id while executing %s";

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

  @Override
  public void create_database(Database msDb)
      throws AlreadyExistsException, InvalidObjectException, MetaException, TException {
    if (!BackendConfig.INSTANCE.enableCatalogdHMSCache() ||
            !BackendConfig.INSTANCE.enableSyncToLatestEventOnDdls()) {
      super.create_database(msDb);
      return;
    }
    catalogOpExecutor_.getMetastoreDdlLock().lock();
    String dbName = msDb.getName();
    String apiName = HmsApiNameEnum.CREATE_DATABASE.apiName();
    Db db = null;
    long fromEventId = -1;
    try {
      try {
        fromEventId = super.get_current_notificationEventId().getEventId();
        super.create_database(msDb);
      } catch (Exception e) {
        LOG.error("Caught exception when creating database {} in hms", dbName);
        if (!(e instanceof AlreadyExistsException)) {
          throw e;
        }
        if (catalog_.getDb(dbName) != null) {
          LOG.error("can not create database {} as it already exists in " +
                  "metastore and catalog", dbName);
          throw e;
        }
      }
      List<NotificationEvent> events = MetastoreEventsProcessor
          .getNextMetastoreEventsInBatchesForDb(catalog_, fromEventId, dbName,
              MetastoreEvents.CreateDatabaseEvent.EVENT_TYPE);
      Preconditions.checkArgument(events.size() == 1,
          "Db %s was recreated in metastore " +
              "while the current db creation was in progress", dbName);
      long createEventId = events.get(0).getEventId();
      catalog_.addDb(dbName, msDb, createEventId);
      LOG.info("Created database {} with create event id: {}", dbName, createEventId);
      // sync to latest event ID
      db = getDbAndAcquireLock(dbName, apiName);
      syncToLatestEventId(db, apiName);
    } catch (Exception e) {
      rethrowException(e, apiName);
    } finally {
      catalogOpExecutor_.UnlockWriteLockIfErronouslyLocked();
      catalogOpExecutor_.getMetastoreDdlLock().unlock();
      if (db != null && db.isLockHeldByCurrentThread()) {
        db.getLock().unlock();
      }
    }
  }

  @Override
  public void drop_database(String databaseName, boolean deleteData,
      boolean cascade) throws NoSuchObjectException,
          InvalidOperationException, MetaException, TException {
    if (!BackendConfig.INSTANCE.enableCatalogdHMSCache() ||
            !BackendConfig.INSTANCE.enableSyncToLatestEventOnDdls()) {
      super.drop_database(databaseName, deleteData, cascade);
      return;
    }
    // TODO: The complete logic can be moved to
    // drop_database in MetastoreserviceHandler
    String apiName = HmsApiNameEnum.DROP_DATABASE.apiName();
    String dbName = MetaStoreUtils.parseDbName(databaseName, serverConf_)[1];
    long currentEventId = -1;
    catalogOpExecutor_.getMetastoreDdlLock().lock();
    try  {
      try {
        currentEventId = super.get_current_notificationEventId().getEventId();
        super.drop_database(databaseName, deleteData, cascade);
      } catch (NoSuchObjectException e) {
        // db does not exist in metastore, remove it from
        // catalog if exists
        if (catalog_.removeDb(dbName) != null) {
          LOG.info("Db {} not known to metastore, removed it from catalog for " +
              "metastore api {}", dbName, apiName);
        }
        throw e;
      }
      dropDbIfExists(databaseName, false, currentEventId, apiName);
    } finally {
      catalogOpExecutor_.getMetastoreDdlLock().unlock();
    }
  }

  @Override
  public void drop_database_req(final DropDatabaseRequest dropDatabaseRequest)
      throws NoSuchObjectException, InvalidOperationException, MetaException {
    if (!BackendConfig.INSTANCE.enableCatalogdHMSCache() ||
        !BackendConfig.INSTANCE.enableSyncToLatestEventOnDdls()) {
      super.drop_database_req(dropDatabaseRequest);
      return;
    }
    String apiName = HmsApiNameEnum.DROP_DATABASE_REQ.apiName();
    String dbName =
        MetaStoreUtils.parseDbName(dropDatabaseRequest.getName(), serverConf_)[1];
    long currentEventId = -1;
    catalogOpExecutor_.getMetastoreDdlLock().lock();
    try  {
      try {
        currentEventId = super.get_current_notificationEventId().getEventId();
        super.drop_database_req(dropDatabaseRequest);
      } catch (NoSuchObjectException e) {
        // db does not exist in metastore, remove it from
        // catalog if exists
        if (catalog_.removeDb(dbName) != null) {
          LOG.info("Db {} not known to metastore, removed it from catalog for " +
              "metastore api {}", dbName, apiName);
        }
        throw e;
        // TODO: We should add TException to method signature in hive and we can remove
        // following two catch blocks.
      } catch (InvalidOperationException|MetaException e) {
        throw e;
      } catch (TException e) {
        throw new MetaException(StringUtils.stringifyException(e));
      }
      dropDbIfExists(dropDatabaseRequest.getName(),
          dropDatabaseRequest.isIgnoreUnknownDb(), currentEventId, apiName);
    } finally {
      catalogOpExecutor_.getMetastoreDdlLock().unlock();
    }
  }

  @Override
  public void alter_database(String databaseName, Database database)
      throws MetaException, NoSuchObjectException, TException {
    if (!BackendConfig.INSTANCE.enableCatalogdHMSCache() ||
        !BackendConfig.INSTANCE.enableSyncToLatestEventOnDdls()) {
      super.alter_database(databaseName, database);
      return;
    }
    String dbname = MetaStoreUtils.parseDbName(databaseName, serverConf_)[1];
    String apiName = HmsApiNameEnum.ALTER_DATABASE.apiName();
    Db db = getDbAndAcquireLock(dbname, apiName);
    catalog_.getLock().writeLock().unlock();
    try {
      super.alter_database(dbname, database);
      syncToLatestEventId(db, apiName);
    } catch (Exception e) {
      rethrowException(e, apiName);
    } finally {
      catalogOpExecutor_.UnlockWriteLockIfErronouslyLocked();
      if (db != null && db.isLockHeldByCurrentThread()) {
        db.getLock().unlock();
      }
    }
  }

  @Override
  public void create_table(org.apache.hadoop.hive.metastore.api.Table table)
      throws TException {
    if (!BackendConfig.INSTANCE.enableCatalogdHMSCache() ||
        !BackendConfig.INSTANCE.enableSyncToLatestEventOnDdls()) {
      super.create_table(table);
      return;
    }
    CreateTableTask<Void> task = new CreateTableTask<Void>() {
      @Override
      public Void execute() throws TException {
        CatalogMetastoreServiceHandler.super.create_table(table);
        return null;
      }
    };
    String apiName = HmsApiNameEnum.CREATE_TABLE.apiName();
    createTableCore(apiName, table.getDbName(), table.getTableName(),
        MetadataOp.getImpalaTableType(table.getTableType()), task);
  }

  @Override
  public void create_table_req(CreateTableRequest req)
      throws TException {
    if (!BackendConfig.INSTANCE.enableCatalogdHMSCache() ||
        !BackendConfig.INSTANCE.enableSyncToLatestEventOnDdls()) {
      super.create_table_req(req);
      return;
    }
    CreateTableTask<Void> task = new CreateTableTask<Void>() {
      @Override
      public Void execute() throws TException {
        CatalogMetastoreServiceHandler.super.create_table_req(req);
        return null;
      }
    };
    String apiName = HmsApiNameEnum.CREATE_TABLE_REQ.apiName();
    org.apache.hadoop.hive.metastore.api.Table table = req.getTable();
    createTableCore(apiName, table.getDbName(), table.getTableName(),
        MetadataOp.getImpalaTableType(table.getTableType()), task);
  }


  @Override
  public void create_table_with_constraints(
      org.apache.hadoop.hive.metastore.api.Table table,
      List<SQLPrimaryKey> sqlPrimaryKeys, List<SQLForeignKey> sqlForeignKeys,
      List<SQLUniqueConstraint> sqlUniqueConstraints,
      List<SQLNotNullConstraint> sqlNotNullConstraints,
      List<SQLDefaultConstraint> sqlDefaultConstraints,
      List<SQLCheckConstraint> sqlCheckConstraints) throws TException {
    if (!BackendConfig.INSTANCE.enableCatalogdHMSCache() ||
        !BackendConfig.INSTANCE.enableSyncToLatestEventOnDdls()) {
      super.create_table_with_constraints(table,
          sqlPrimaryKeys, sqlForeignKeys, sqlUniqueConstraints, sqlNotNullConstraints,
          sqlDefaultConstraints, sqlCheckConstraints);
      return;
    }
    CreateTableTask<Void> task = new CreateTableTask<Void>() {
      @Override
      public Void execute() throws TException {
        CatalogMetastoreServiceHandler.super.create_table_with_constraints(table,
            sqlPrimaryKeys, sqlForeignKeys, sqlUniqueConstraints, sqlNotNullConstraints,
            sqlDefaultConstraints, sqlCheckConstraints);
        return null;
      }
    };
    String apiName = HmsApiNameEnum.CREATE_TABLE_WITH_CONSTRAINTS.apiName();
    createTableCore(apiName, table.getDbName(), table.getTableName(),
        MetadataOp.getImpalaTableType(table.getTableType()), task);
  }

  @Override
  public void create_table_with_environment_context(
      org.apache.hadoop.hive.metastore.api.Table table,
      EnvironmentContext environmentContext) throws TException {
    if (!BackendConfig.INSTANCE.enableCatalogdHMSCache() ||
        !BackendConfig.INSTANCE.enableSyncToLatestEventOnDdls()) {
      super.create_table_with_environment_context(table, environmentContext);
      return;
    }
    CreateTableTask<Void> task = new CreateTableTask<Void>() {
      @Override
      public Void execute() throws TException {
        CatalogMetastoreServiceHandler.super
            .create_table_with_environment_context(table, environmentContext);
        return null;
      }
    };
    String apiName = HmsApiNameEnum.CREATE_TABLE_WITH_ENVIRONMENT_CONTEXT.apiName();
    createTableCore(apiName, table.getDbName(), table.getTableName(),
        MetadataOp.getImpalaTableType(table.getTableType()), task);
  }

  @Override
  public void alter_table(String dbname, String tblName,
      org.apache.hadoop.hive.metastore.api.Table newTable)
      throws InvalidOperationException, MetaException, TException {

    if (!BackendConfig.INSTANCE.enableCatalogdHMSCache() ||
        !BackendConfig.INSTANCE.enableSyncToLatestEventOnDdls()) {
      super.alter_table(dbname, tblName, newTable);
      return;
    }

    AlterTableTask<Void> task = new AlterTableTask<Void>() {
      @Override
      public Void execute() throws InvalidOperationException, MetaException,
          TException {
        CatalogMetastoreServiceHandler.super.alter_table(dbname, tblName, newTable);
        return null;
      }
    };
    String apiName = HmsApiNameEnum.ALTER_TABLE.apiName();
    alterTableCore(dbname, tblName, newTable, apiName, task);
  }

  @Override
  public void alter_table_with_environment_context(String dbname, String tblName,
      org.apache.hadoop.hive.metastore.api.Table table, EnvironmentContext envContext)
      throws InvalidOperationException, MetaException, TException {
    if (!BackendConfig.INSTANCE.enableCatalogdHMSCache() ||
        !BackendConfig.INSTANCE.enableSyncToLatestEventOnDdls()) {
      super.alter_table_with_environment_context(dbname, tblName, table, envContext);
      return;
    }
    String apiName = HmsApiNameEnum.ALTER_PARTITION_WITH_ENVIRONMENT_CONTEXT.apiName();
    AlterTableTask<Void> task = new AlterTableTask<Void>() {
      @Override
      public Void execute() throws InvalidOperationException, MetaException, TException {
        CatalogMetastoreServiceHandler.super.alter_table_with_environment_context(dbname,
            tblName, table, envContext);
        return null;
      }
    };
    alterTableCore(dbname, tblName, table, apiName, task);
  }


  @Override
  public void alter_table_with_cascade(String dbname, String tblName,
      org.apache.hadoop.hive.metastore.api.Table table, boolean cascade)
      throws InvalidOperationException, MetaException, TException {
    if (!BackendConfig.INSTANCE.enableCatalogdHMSCache() ||
        !BackendConfig.INSTANCE.enableSyncToLatestEventOnDdls()) {
      super.alter_table_with_cascade(dbname, tblName, table, cascade);
      return;
    }
    String apiName = HmsApiNameEnum.ALTER_TABLE_WITH_CASCADE.apiName();

    AlterTableTask<Void> task = new AlterTableTask<Void>() {
      @Override
      public Void execute() throws InvalidOperationException, MetaException,
          TException {
        CatalogMetastoreServiceHandler.super
            .alter_table_with_cascade(dbname, tblName, table, cascade);
        return null;
      }
    };
    alterTableCore(dbname, tblName, table, apiName, task);
  }


  @Override
  public AlterTableResponse alter_table_req(AlterTableRequest alterTableRequest)
          throws InvalidOperationException, MetaException, TException {
    if (!BackendConfig.INSTANCE.enableCatalogdHMSCache() ||
        !BackendConfig.INSTANCE.enableSyncToLatestEventOnDdls()) {
      return super.alter_table_req(alterTableRequest);
    }
    String apiName = HmsApiNameEnum.ALTER_TABLE_REQ.apiName();
    String dbname = alterTableRequest.getDbName();
    String tblName = alterTableRequest.getTableName();
    org.apache.hadoop.hive.metastore.api.Table newTable =
        alterTableRequest.getTable();
    AlterTableTask<AlterTableResponse> task = new AlterTableTask<AlterTableResponse>() {
      @Override
      public AlterTableResponse execute() throws InvalidOperationException, MetaException,
          TException {
        AlterTableResponse resp =
            CatalogMetastoreServiceHandler.super.alter_table_req(alterTableRequest);
        return resp;
      }
    };
    return alterTableCore(dbname, tblName, newTable, apiName, task);
  }

  @Override
  public Partition add_partition(Partition partition)
      throws InvalidObjectException, AlreadyExistsException, MetaException, TException {
    if (!BackendConfig.INSTANCE.enableCatalogdHMSCache() ||
        !BackendConfig.INSTANCE.enableSyncToLatestEventOnDdls()) {
      return super.add_partition(partition);
    }
    String apiName = HmsApiNameEnum.ADD_PARTITION.apiName();
    org.apache.impala.catalog.Table tbl = getTableAndAcquireWriteLock(
        partition.getDbName(), partition.getTableName(), apiName);
    catalog_.getLock().writeLock().unlock();
    Partition addedPartition = super.add_partition(partition);
    try {
      syncToLatestEventId(tbl, apiName);
    } finally {
      catalogOpExecutor_.UnlockWriteLockIfErronouslyLocked();
      tbl.releaseWriteLock();
    }
    return addedPartition;
  }

  @Override
  public Partition add_partition_with_environment_context(Partition partition,
      EnvironmentContext environmentContext) throws InvalidObjectException,
      AlreadyExistsException, MetaException, TException {
    if (!BackendConfig.INSTANCE.enableCatalogdHMSCache() ||
        !BackendConfig.INSTANCE.enableSyncToLatestEventOnDdls()) {
      return super.add_partition_with_environment_context(partition, environmentContext);
    }
    String apiName = HmsApiNameEnum.ADD_PARTITION_WITH_ENVIRONMENT_CONTEXT.apiName();
    org.apache.impala.catalog.Table tbl =
        getTableAndAcquireWriteLock(partition.getDbName(), partition.getTableName(),
            apiName);
    catalog_.getLock().writeLock().unlock();
    Partition addedPartition =
        super.add_partition_with_environment_context(partition, environmentContext);
    try {
      syncToLatestEventId(tbl, apiName);
    } finally {
      catalogOpExecutor_.UnlockWriteLockIfErronouslyLocked();
      tbl.releaseWriteLock();
    }
    return addedPartition;
  }

  @Override
  public int add_partitions(List<Partition> partitionList)
      throws InvalidObjectException, AlreadyExistsException, MetaException, TException {
    if (!BackendConfig.INSTANCE.enableCatalogdHMSCache() ||
        !BackendConfig.INSTANCE.enableSyncToLatestEventOnDdls() ||
        partitionList.isEmpty()) {
      return super.add_partitions(partitionList);
    }
    // don't execute the following if partition list is empty
    // since we can't get db and table info from empty list
    String apiName = HmsApiNameEnum.ADD_PARTITIONS.apiName();
    org.apache.hadoop.hive.metastore.api.Partition partition = partitionList.get(0);
    org.apache.impala.catalog.Table tbl =
        getTableAndAcquireWriteLock(partition.getDbName(), partition.getTableName(),
            apiName);
    catalog_.getLock().writeLock().unlock();
    int numPartitionsAdded = super.add_partitions(partitionList);

    try {
      syncToLatestEventId(tbl, apiName);
    } finally {
      catalogOpExecutor_.UnlockWriteLockIfErronouslyLocked();
      tbl.releaseWriteLock();
    }
    return numPartitionsAdded;
  }

  @Override
  public int add_partitions_pspec(List<PartitionSpec> list)
      throws InvalidObjectException, AlreadyExistsException, MetaException, TException {
    if (!BackendConfig.INSTANCE.enableCatalogdHMSCache() ||
        !BackendConfig.INSTANCE.enableSyncToLatestEventOnDdls() ||
        list.isEmpty()) {
      return super.add_partitions_pspec(list);
    }
    // don't execute the following if partition list is empty
    // since we can't get db and table info from empty list
    String apiName = HmsApiNameEnum.ADD_PARTITIONS_PSPEC.apiName();
    org.apache.hadoop.hive.metastore.api.PartitionSpec spec = list.get(0);
    org.apache.impala.catalog.Table tbl =
        getTableAndAcquireWriteLock(spec.getDbName(), spec.getTableName(),
            apiName);
    catalog_.getLock().writeLock().unlock();
    int numPartitionsAdded = super.add_partitions_pspec(list);
    try {
      syncToLatestEventId(tbl, apiName);
    } finally {
      catalogOpExecutor_.UnlockWriteLockIfErronouslyLocked();
      tbl.releaseWriteLock();
    }
    return numPartitionsAdded;
  }

  @Override
  public AddPartitionsResult add_partitions_req(AddPartitionsRequest request)
      throws InvalidObjectException, AlreadyExistsException, MetaException, TException {
    if (!BackendConfig.INSTANCE.enableCatalogdHMSCache() ||
        !BackendConfig.INSTANCE.enableSyncToLatestEventOnDdls()) {
      return super.add_partitions_req(request);
    }
    String apiName = HmsApiNameEnum.ADD_PARTITIONS_REQ.apiName();
    org.apache.impala.catalog.Table tbl =
        getTableAndAcquireWriteLock(request.getDbName(), request.getTblName(),
            apiName);
    catalog_.getLock().writeLock().unlock();
    AddPartitionsResult result = super.add_partitions_req(request);

    try {
      syncToLatestEventId(tbl, apiName);
    } finally {
      catalogOpExecutor_.UnlockWriteLockIfErronouslyLocked();
      tbl.releaseWriteLock();
    }
    return result;
  }

  @Override
  public Partition append_partition(String dbname, String tblName, List<String> partVals)
      throws InvalidObjectException, AlreadyExistsException, MetaException, TException {
    if (!BackendConfig.INSTANCE.enableCatalogdHMSCache() ||
        !BackendConfig.INSTANCE.enableSyncToLatestEventOnDdls()) {
      return super.append_partition(dbname, tblName, partVals);
    }
    String apiName = HmsApiNameEnum.APPEND_PARTITION.apiName();
    org.apache.impala.catalog.Table tbl = getTableAndAcquireWriteLock(dbname,
        tblName, apiName);
    catalog_.getLock().writeLock().unlock();
    Partition partition = super.append_partition(dbname, tblName, partVals);
    LOG.debug("Successfully executed HMS API: append_partition");
    try {
      syncToLatestEventId(tbl, apiName);
    } finally {
      catalogOpExecutor_.UnlockWriteLockIfErronouslyLocked();
      tbl.releaseWriteLock();
    }
    return partition;
  }

  @Override
  public Partition append_partition_with_environment_context(String dbName,
      String tblName, List<String> partVals, EnvironmentContext environmentContext)
      throws InvalidObjectException, AlreadyExistsException, MetaException, TException {
    if (!BackendConfig.INSTANCE.enableCatalogdHMSCache() ||
        !BackendConfig.INSTANCE.enableSyncToLatestEventOnDdls()) {
      return super.append_partition_with_environment_context(dbName, tblName, partVals,
          environmentContext);
    }
    String apiName = HmsApiNameEnum.APPEND_PARTITION_WITH_ENVIRONMENT_CONTEXT.apiName();
    org.apache.impala.catalog.Table tbl = getTableAndAcquireWriteLock(dbName,
        tblName, apiName);
    catalog_.getLock().writeLock().unlock();
    Partition partition = super.append_partition_with_environment_context(dbName,
        tblName, partVals, environmentContext);
    try {
      syncToLatestEventId(tbl, apiName);
    } finally {
      catalogOpExecutor_.UnlockWriteLockIfErronouslyLocked();
      tbl.releaseWriteLock();
    }
    return partition;
  }

  @Override
  public Partition append_partition_by_name(String dbName, String tblName,
      String partName) throws InvalidObjectException, AlreadyExistsException,
      MetaException, TException {
    if (!BackendConfig.INSTANCE.enableCatalogdHMSCache() ||
        !BackendConfig.INSTANCE.enableSyncToLatestEventOnDdls()) {
      return super.append_partition_by_name(dbName, tblName, partName);
    }
    String apiName = HmsApiNameEnum.APPEND_PARTITION_WITH_ENVIRONMENT_CONTEXT.apiName();
    org.apache.impala.catalog.Table tbl = getTableAndAcquireWriteLock(dbName,
        tblName, apiName);
    catalog_.getLock().writeLock().unlock();
    Partition partition = super.append_partition_by_name(dbName,
        tblName, partName);
    try {
      syncToLatestEventId(tbl, apiName);
    } finally {
      catalogOpExecutor_.UnlockWriteLockIfErronouslyLocked();
      tbl.releaseWriteLock();
    }
    return partition;
  }

  @Override
  public Partition append_partition_by_name_with_environment_context(String dbName,
      String tblName, String partName, EnvironmentContext context)
      throws InvalidObjectException, AlreadyExistsException, MetaException, TException {
    if (!BackendConfig.INSTANCE.enableCatalogdHMSCache() ||
        !BackendConfig.INSTANCE.enableSyncToLatestEventOnDdls()) {
      return super.append_partition_by_name_with_environment_context(dbName, tblName,
          partName, context);
    }
    String apiName =
        HmsApiNameEnum.APPEND_PARTITION_BY_NAME_WITH_ENVIRONMENT_CONTEXT.apiName();
    org.apache.impala.catalog.Table tbl = getTableAndAcquireWriteLock(dbName,
        tblName, apiName);
    catalog_.getLock().writeLock().unlock();
    Partition partition = super.append_partition_by_name_with_environment_context(dbName,
        tblName, partName, context);
    try {
      syncToLatestEventId(tbl, apiName);
    } finally {
      catalogOpExecutor_.UnlockWriteLockIfErronouslyLocked();
      tbl.releaseWriteLock();
    }
    return partition;
  }

  @Override
  public boolean drop_partition(String dbname, String tblname, List<String> partVals,
      boolean deleteData) throws NoSuchObjectException, MetaException, TException {
    if (!BackendConfig.INSTANCE.enableCatalogdHMSCache() ||
        !BackendConfig.INSTANCE.enableSyncToLatestEventOnDdls()) {
      return super.drop_partition(dbname, tblname, partVals, deleteData);
    }
    String apiName = HmsApiNameEnum.DROP_PARTITION.apiName();
    org.apache.impala.catalog.Table tbl = getTableAndAcquireWriteLock(dbname,
        tblname, apiName);
    catalog_.getLock().writeLock().unlock();
    boolean resp = super.drop_partition(dbname, tblname, partVals, deleteData);
    try {
      syncToLatestEventId(tbl, apiName);
    } finally {
      catalogOpExecutor_.UnlockWriteLockIfErronouslyLocked();
      tbl.releaseWriteLock();
    }
    return resp;
  }

  @Override
  public boolean drop_partition_by_name(String dbname, String tblname, String partName,
      boolean deleteData) throws NoSuchObjectException, MetaException, TException {
    if (!BackendConfig.INSTANCE.enableCatalogdHMSCache() ||
        !BackendConfig.INSTANCE.enableSyncToLatestEventOnDdls()) {
      return super.drop_partition_by_name(dbname, tblname, partName, deleteData);
    }
    String apiName = HmsApiNameEnum.DROP_PARTITION_BY_NAME.apiName();
    org.apache.impala.catalog.Table tbl = getTableAndAcquireWriteLock(dbname,
        tblname, apiName);
    catalog_.getLock().writeLock().unlock();
    boolean response =
        super.drop_partition_by_name(dbname, tblname, partName, deleteData);
    try {
      syncToLatestEventId(tbl, apiName);
    } finally {
      catalogOpExecutor_.UnlockWriteLockIfErronouslyLocked();
      tbl.releaseWriteLock();
    }
    return response;
  }

  @Override
  public boolean drop_partition_with_environment_context(String dbname, String tblname,
      List<String> partNames, boolean deleteData, EnvironmentContext environmentContext)
      throws NoSuchObjectException, MetaException, TException {
    if (!BackendConfig.INSTANCE.enableCatalogdHMSCache() ||
        !BackendConfig.INSTANCE.enableSyncToLatestEventOnDdls()) {
      return super.drop_partition_with_environment_context(dbname, tblname, partNames,
          deleteData, environmentContext);
    }
    String apiName = HmsApiNameEnum.DROP_PARTITION_WITH_ENVIRONMENT_CONTEXT.apiName();
    org.apache.impala.catalog.Table tbl = getTableAndAcquireWriteLock(dbname,
        tblname, apiName);
    catalog_.getLock().writeLock().unlock();
    boolean resp = super.drop_partition_with_environment_context(dbname, tblname,
        partNames, deleteData, environmentContext);
    try {
      syncToLatestEventId(tbl, apiName);
    } finally {
      catalogOpExecutor_.UnlockWriteLockIfErronouslyLocked();
      tbl.releaseWriteLock();
    }
    return resp;
  }

  @Override
  public boolean drop_partition_by_name_with_environment_context(String dbName,
      String tableName, String partName, boolean deleteData,
      EnvironmentContext envContext) throws NoSuchObjectException, MetaException,
      TException {
    if (!BackendConfig.INSTANCE.enableCatalogdHMSCache() ||
        !BackendConfig.INSTANCE.enableSyncToLatestEventOnDdls()) {
      return super.drop_partition_by_name_with_environment_context(dbName, tableName,
          partName, deleteData, envContext);
    }
    String apiName =
        HmsApiNameEnum.DROP_PARTITION_BY_NAME_WITH_ENVIRONMENT_CONTEXT.apiName();
    org.apache.impala.catalog.Table tbl = getTableAndAcquireWriteLock(dbName,
        tableName, apiName);
    catalog_.getLock().writeLock().unlock();
    boolean resp = super.drop_partition_by_name_with_environment_context(dbName,
        tableName, partName, deleteData, envContext);
    try {
      syncToLatestEventId(tbl, apiName);
    } finally {
      catalogOpExecutor_.UnlockWriteLockIfErronouslyLocked();
      tbl.releaseWriteLock();
    }
    return resp;
  }

  @Override
  public DropPartitionsResult drop_partitions_req(
      DropPartitionsRequest request)
      throws NoSuchObjectException, MetaException, TException {
    String dbName = request.getDbName();
    String tableName = request.getTblName();
    if (!BackendConfig.INSTANCE.enableCatalogdHMSCache() ||
        !BackendConfig.INSTANCE.enableSyncToLatestEventOnDdls()) {
      return super.drop_partitions_req(request);
    }
    String apiName =
        HmsApiNameEnum.DROP_PARTITIONS_REQ.apiName();
    org.apache.impala.catalog.Table tbl = getTableAndAcquireWriteLock(dbName,
        tableName, apiName);
    catalog_.getLock().writeLock().unlock();
    DropPartitionsResult result = super.drop_partitions_req(request);
    try {
      syncToLatestEventId(tbl, apiName);
    } finally {
      catalogOpExecutor_.UnlockWriteLockIfErronouslyLocked();
      tbl.releaseWriteLock();
    }
    return result;
  }

  @Override
  public Partition exchange_partition(Map<String, String> partitionSpecMap,
      String sourceDbWithCatalog, String sourceTbl, String destDbWithCatalog,
      String destTbl) throws TException {
    if (!BackendConfig.INSTANCE.enableCatalogdHMSCache() ||
        !BackendConfig.INSTANCE.enableSyncToLatestEventOnDdls()) {
      return super.exchange_partition(partitionSpecMap, sourceDbWithCatalog,
          sourceTbl, destDbWithCatalog, destTbl);
    }
    // acquire lock on multiple tables at once
    String apiName = HmsApiNameEnum.EXCHANGE_PARTITION.apiName();
    org.apache.impala.catalog.Table srcTbl = null, destinationTbl = null;
    Partition exchangedPartition = null;

    try {
      String sourceDb = MetaStoreUtils.parseDbName(sourceDbWithCatalog, serverConf_)[1];
      String destDb = MetaStoreUtils.parseDbName(destDbWithCatalog, serverConf_)[1];
      EventSequence catalogTimeline = NoOpEventSequence.INSTANCE;
      srcTbl = catalogOpExecutor_.getExistingTable(sourceDb, sourceTbl, apiName,
          catalogTimeline);
      destinationTbl = catalogOpExecutor_.getExistingTable(destDb, destTbl, apiName,
          catalogTimeline);

      if (!catalog_.tryWriteLock(
          new org.apache.impala.catalog.Table[] {srcTbl, destinationTbl})) {
        throw new CatalogException("Couldn't acquire lock on tables: "
            + srcTbl.getFullName() + ", " + destinationTbl.getFullName());
      }

      exchangedPartition = super.exchange_partition(partitionSpecMap, sourceDbWithCatalog,
          sourceTbl, destDbWithCatalog, destTbl);
      syncToLatestEventId(srcTbl, apiName);
      syncToLatestEventId(destinationTbl, apiName);
    } catch (Exception e) {
      rethrowException(e, apiName);
    } finally {
      catalogOpExecutor_.UnlockWriteLockIfErronouslyLocked();
      if (srcTbl != null && srcTbl.isWriteLockedByCurrentThread()) {
        srcTbl.releaseWriteLock();
      }
      if (destinationTbl != null && destinationTbl.isWriteLockedByCurrentThread()) {
        destinationTbl.releaseWriteLock();
      }
    }
    return exchangedPartition;
  }

  @Override
  public List<Partition> exchange_partitions(Map<String, String> partitionSpecs,
      String sourceDbWithCatalog, String sourceTbl, String destDbWithCatalog,
      String destTbl) throws TException {
    if (!BackendConfig.INSTANCE.enableCatalogdHMSCache() ||
        !BackendConfig.INSTANCE.enableSyncToLatestEventOnDdls()) {
      return super.exchange_partitions(partitionSpecs, sourceDbWithCatalog,
          sourceTbl, destDbWithCatalog, destTbl);
    }

    // acquire lock on multiple tables at once
    String apiName = HmsApiNameEnum.EXCHANGE_PARTITIONS.apiName();
    org.apache.impala.catalog.Table srcTbl = null, destinationTbl = null;
    List<Partition> exchangedPartitions = null;
    try {
      String sourceDb = MetaStoreUtils.parseDbName(sourceDbWithCatalog, serverConf_)[1];
      String destDb = MetaStoreUtils.parseDbName(destDbWithCatalog, serverConf_)[1];
      EventSequence catalogTimeline = NoOpEventSequence.INSTANCE;
      srcTbl = catalogOpExecutor_.getExistingTable(sourceDb, sourceTbl, apiName,
          catalogTimeline);
      destinationTbl = catalogOpExecutor_.getExistingTable(destDb, destTbl, apiName,
          catalogTimeline);

      if (!catalog_.tryWriteLock(
          new org.apache.impala.catalog.Table[] {srcTbl, destinationTbl})) {
        throw new CatalogException("Couldn't acquire lock on tables: "
            + srcTbl.getFullName() + ", " + destinationTbl.getFullName());
      }

      exchangedPartitions = super.exchange_partitions(partitionSpecs, sourceDbWithCatalog,
          sourceTbl, destDbWithCatalog, destTbl);
      syncToLatestEventId(srcTbl, apiName);
      syncToLatestEventId(destinationTbl, apiName);
    } catch (Exception e) {
      rethrowException(e, apiName);
    } finally {
      catalogOpExecutor_.UnlockWriteLockIfErronouslyLocked();
      if (srcTbl != null && srcTbl.isWriteLockedByCurrentThread()) {
        srcTbl.releaseWriteLock();
      }
      if (destinationTbl != null && destinationTbl.isWriteLockedByCurrentThread()) {
        destinationTbl.releaseWriteLock();
      }
    }
    return exchangedPartitions;
  }

  @Override
  public void alter_partition(String dbName, String tblName, Partition partition)
      throws InvalidOperationException, MetaException, TException {
    if (!BackendConfig.INSTANCE.enableCatalogdHMSCache() ||
        !BackendConfig.INSTANCE.enableSyncToLatestEventOnDdls()) {
      super.alter_partition(dbName, tblName, partition);
      return;
    }
    String apiName = HmsApiNameEnum.ALTER_PARTITION.apiName();
    org.apache.impala.catalog.Table tbl = getTableAndAcquireWriteLock(dbName,
        tblName, apiName);
    catalog_.getLock().writeLock().unlock();
    super.alter_partition(dbName, tblName, partition);
    try {
      syncToLatestEventId(tbl, apiName);
    } finally {
      catalogOpExecutor_.UnlockWriteLockIfErronouslyLocked();
      tbl.releaseWriteLock();
    }
  }

  @Override
  public void alter_partitions(String dbName, String tblName, List<Partition> partitions)
      throws InvalidOperationException, MetaException, TException {
    if (!BackendConfig.INSTANCE.enableCatalogdHMSCache() ||
        !BackendConfig.INSTANCE.enableSyncToLatestEventOnDdls()) {
      super.alter_partitions(dbName, tblName, partitions);
      return;
    }
    String apiName = HmsApiNameEnum.ALTER_PARTITIONS.apiName();
    org.apache.impala.catalog.Table tbl = getTableAndAcquireWriteLock(dbName,
        tblName, apiName);
    catalog_.getLock().writeLock().unlock();
    super.alter_partitions(dbName, tblName, partitions);
    try {
      syncToLatestEventId(tbl, apiName);
    } finally {
      catalogOpExecutor_.UnlockWriteLockIfErronouslyLocked();
      tbl.releaseWriteLock();
    }
  }

  @Override
  public void alter_partitions_with_environment_context(String dbName, String tblName,
      List<Partition> list, EnvironmentContext context) throws
      InvalidOperationException, MetaException, TException {
    if (!BackendConfig.INSTANCE.enableCatalogdHMSCache() ||
        !BackendConfig.INSTANCE.enableSyncToLatestEventOnDdls()) {
      super.alter_partitions_with_environment_context(dbName, tblName, list, context);
      return;
    }
    String apiName = HmsApiNameEnum.ALTER_PARTITIONS_WITH_ENVIRONMENT_CONTEXT.apiName();
    org.apache.impala.catalog.Table tbl = getTableAndAcquireWriteLock(dbName,
        tblName, apiName);
    catalog_.getLock().writeLock().unlock();
    super.alter_partitions_with_environment_context(dbName, tblName, list, context);
    try {
      syncToLatestEventId(tbl, apiName);
    } finally {
      catalogOpExecutor_.UnlockWriteLockIfErronouslyLocked();
      tbl.releaseWriteLock();
    }
  }

  @Override
  public AlterPartitionsResponse alter_partitions_req(
      AlterPartitionsRequest alterPartitionsRequest)
      throws InvalidOperationException, MetaException, TException {
    if (!BackendConfig.INSTANCE.enableCatalogdHMSCache() ||
        !BackendConfig.INSTANCE.enableSyncToLatestEventOnDdls()) {
      return super.alter_partitions_req(alterPartitionsRequest);
    }
    String apiName = HmsApiNameEnum.ALTER_PARTITIONS_REQ.apiName();
    org.apache.impala.catalog.Table tbl =
        getTableAndAcquireWriteLock(alterPartitionsRequest.getDbName(),
        alterPartitionsRequest.getTableName(), apiName);
    catalog_.getLock().writeLock().unlock();
    AlterPartitionsResponse response =
        super.alter_partitions_req(alterPartitionsRequest);
    try {
      syncToLatestEventId(tbl, apiName);
    } finally {
      catalogOpExecutor_.UnlockWriteLockIfErronouslyLocked();
      tbl.releaseWriteLock();
    }
    return response;
  }

  @Override
  public void alter_partition_with_environment_context(String dbName, String tblName,
      Partition partition, EnvironmentContext environmentContext)
      throws InvalidOperationException, MetaException, TException {
    if (!BackendConfig.INSTANCE.enableCatalogdHMSCache() ||
        !BackendConfig.INSTANCE.enableSyncToLatestEventOnDdls()) {
      super.alter_partition_with_environment_context(dbName, tblName,
          partition, environmentContext);
      return;
    }
    String apiName = HmsApiNameEnum.ALTER_PARTITION_WITH_ENVIRONMENT_CONTEXT.apiName();
    org.apache.impala.catalog.Table tbl = getTableAndAcquireWriteLock(dbName,
        tblName, apiName);
    catalog_.getLock().writeLock().unlock();
    super.alter_partition_with_environment_context(dbName, tblName,
        partition, environmentContext);
    try {
      syncToLatestEventId(tbl, apiName);
    } finally {
      catalogOpExecutor_.UnlockWriteLockIfErronouslyLocked();
      tbl.releaseWriteLock();
    }
  }

  @Override
  public void rename_partition(String dbName, String tblName, List<String> list,
      Partition partition) throws InvalidOperationException, MetaException, TException {
    if (!BackendConfig.INSTANCE.enableCatalogdHMSCache() ||
        !BackendConfig.INSTANCE.enableSyncToLatestEventOnDdls()) {
      super.rename_partition(dbName, tblName, list, partition);
      return;
    }
    String apiName = HmsApiNameEnum.RENAME_PARTITION.apiName();
    org.apache.impala.catalog.Table tbl = getTableAndAcquireWriteLock(dbName,
        tblName, apiName);
    catalog_.getLock().writeLock().unlock();
    super.rename_partition(dbName, tblName, list, partition);
    try {
      syncToLatestEventId(tbl, apiName);
    } finally {
      catalogOpExecutor_.UnlockWriteLockIfErronouslyLocked();
      tbl.releaseWriteLock();
    }
  }

  @Override
  public RenamePartitionResponse rename_partition_req(RenamePartitionRequest request)
      throws InvalidOperationException, MetaException, TException {
    if (!BackendConfig.INSTANCE.enableCatalogdHMSCache() ||
        !BackendConfig.INSTANCE.enableSyncToLatestEventOnDdls()) {
      return super.rename_partition_req(request);
    }
    String apiName = HmsApiNameEnum.RENAME_PARTITION_REQ.apiName();
    org.apache.impala.catalog.Table tbl = getTableAndAcquireWriteLock(
        request.getDbName(), request.getTableName(), apiName);
    catalog_.getLock().writeLock().unlock();
    RenamePartitionResponse response =
        super.rename_partition_req(request);
    try {
      syncToLatestEventId(tbl, apiName);
    } finally {
      catalogOpExecutor_.UnlockWriteLockIfErronouslyLocked();
      tbl.releaseWriteLock();
    }
    return response;
  }

  @Override
  public void drop_table(String dbname, String tblname, boolean deleteData)
      throws NoSuchObjectException, MetaException, TException {
    if (!BackendConfig.INSTANCE.enableCatalogdHMSCache() ||
        !BackendConfig.INSTANCE.enableSyncToLatestEventOnDdls()) {
      super.drop_table(dbname, tblname, deleteData);
      return;
    }
    org.apache.impala.catalog.Table tbl = null;
    String apiName = HmsApiNameEnum.DROP_TABLE.apiName();
    long currentEventId = -1;
    catalogOpExecutor_.getMetastoreDdlLock().lock();
    try {
      try {
        currentEventId = super.get_current_notificationEventId().getEventId();
        super.drop_table(dbname, tblname, deleteData);
      } catch (NoSuchObjectException e) {
        LOG.debug("Table {}.{} does not exist in metastore, removing it from catalog " +
                        "if exists", dbname, tblname);
        if (catalog_.removeTable(dbname, tblname) != null) {
          LOG.info("Table {}.{} does not exist in metastore, removed from catalog " +
                  "as well", dbname, tblname);
        }
        throw e;
      }
      dropTableIfExists(currentEventId, dbname, tblname, apiName);
    } finally {
      catalogOpExecutor_.getMetastoreDdlLock().unlock();
    }
  }

  @Override
  public void drop_table_with_environment_context(String dbname, String tblname,
      boolean deleteData, EnvironmentContext environmentContext)
      throws NoSuchObjectException, MetaException, TException {
    if (!BackendConfig.INSTANCE.enableCatalogdHMSCache() ||
        !BackendConfig.INSTANCE.enableSyncToLatestEventOnDdls()) {
      super.drop_table_with_environment_context(dbname, tblname,
          deleteData, environmentContext);
      return;
    }
    org.apache.impala.catalog.Table tbl = null;
    String apiName =
        HmsApiNameEnum.DROP_TABLE_WITH_ENVIRONMENT_CONTEXT.apiName();
    long currentEventId = -1;
    catalogOpExecutor_.getMetastoreDdlLock().lock();
    try {
      try {
        currentEventId = super.get_current_notificationEventId().getEventId();
        super.drop_table_with_environment_context(dbname, tblname,
                deleteData, environmentContext);
      } catch (NoSuchObjectException e) {
        LOG.debug("Table {}.{} does not exist in metastore, removing it from catalog " +
                "if exists", dbname, tblname);
        if (catalog_.removeTable(dbname, tblname) != null) {
          LOG.info("Table {}.{} does not exist in metastore, removed from catalog " +
                  "as well", dbname, tblname);
        }
        throw e;
      }
      dropTableIfExists(currentEventId, dbname, tblname, apiName);
    } finally {
      catalogOpExecutor_.getMetastoreDdlLock().unlock();
    }
  }

  @Override
  public void truncate_table(String dbName, String tblName, List<String> partNames)
      throws MetaException, TException {
    if (!BackendConfig.INSTANCE.enableCatalogdHMSCache() ||
        !BackendConfig.INSTANCE.enableSyncToLatestEventOnDdls()) {
      super.truncate_table(dbName, tblName, partNames);
      return;
    }
    String apiName = HmsApiNameEnum.TRUNCATE_TABLE.apiName();
    org.apache.impala.catalog.Table tbl = getTableAndAcquireWriteLock(dbName,
        tblName, apiName);
    catalog_.getLock().writeLock().unlock();
    super.truncate_table(dbName, tblName, partNames);
    try {
      syncToLatestEventId(tbl, apiName);
    } finally {
      catalogOpExecutor_.UnlockWriteLockIfErronouslyLocked();
      tbl.releaseWriteLock();
    }
  }

  @Override
  public TruncateTableResponse truncate_table_req(TruncateTableRequest req)
      throws MetaException, TException {
    if (!BackendConfig.INSTANCE.enableCatalogdHMSCache() ||
        !BackendConfig.INSTANCE.enableSyncToLatestEventOnDdls()) {
      return super.truncate_table_req(req);
    }
    String apiName = HmsApiNameEnum.TRUNCATE_TABLE_REQ.apiName();
    org.apache.impala.catalog.Table tbl = getTableAndAcquireWriteLock(
        req.getDbName(), req.getTableName(), apiName);
    catalog_.getLock().writeLock().unlock();
    TruncateTableResponse response = super.truncate_table_req(req);
    try {
      syncToLatestEventId(tbl, apiName);
    } finally {
      catalogOpExecutor_.UnlockWriteLockIfErronouslyLocked();
      tbl.releaseWriteLock();
    }
    return response;
  }

  private org.apache.impala.catalog.Table getTableAndAcquireWriteLock(
      String dbNameWithCatalog, String tblName, String apiName) throws TException {
    org.apache.impala.catalog.Table tbl = null;
    try {
      String dbName = MetaStoreUtils.parseDbName(dbNameWithCatalog, serverConf_)[1];
      tbl = catalogOpExecutor_.getExistingTable(dbName, tblName, apiName,
          NoOpEventSequence.INSTANCE);
    } catch (Exception e) {
      rethrowException(e, apiName);
    }
    if (!catalog_.tryWriteLock(tbl)) {
      // should it be an internal exception?
      CatalogException e =
          new CatalogException("Could not acquire lock on table: " + tbl.getFullName());
      rethrowException(e, apiName);
    }
    return tbl;
  }

  /**
   * Get db from cache and acquire lock on it
   * @param dbName
   * @param apiName
   * @return
   * @throws TException if either db does not exist in cache or couldn't
   *         acquire lock
   */
  private org.apache.impala.catalog.Db getDbAndAcquireLock(String dbName, String apiName)
      throws TException {
    Db db = catalog_.getDb(dbName);
    if (db == null) {
      rethrowException(new CatalogException("Database: " +
          dbName + " does not exist in cache"), apiName);
    }
    if (!catalog_.tryLockDb(db)) {
      rethrowException(
          new CatalogException("Couldn't acquire write lock on db: " + db.getName()),
          apiName);
    }
    return db;
  }

  /**
   * Sync the table tbl to latest event id. Catches
   * exceptions (if any) while syncing and convert them
   * to TException
   * @throws TException
   */
  private void syncToLatestEventId(org.apache.impala.catalog.Table tbl, String apiName)
      throws TException {
    Preconditions.checkState(
        BackendConfig.INSTANCE.enableSyncToLatestEventOnDdls(),
        "sync to latest event flag should be set to true");
    Preconditions.checkState(tbl.isWriteLockedByCurrentThread(),
        "Thread does not have write lock on table %s", tbl.getFullName());
    try {
      MetastoreEventsProcessor.syncToLatestEventId(catalog_, tbl,
          metastoreEventFactory_, metastoreEventsMetrics_);
    } catch (Exception e) {
      String errMsg = String.format(SYNC_TABLE_LATEST_EVENT_ID_ERR_MSG,
          tbl.getFullName(), apiName);
      LOG.error("{}. Exception stacktrace: {} ", errMsg,
          ExceptionUtils.getFullStackTrace(e));
      rethrowException(e, apiName);
    }
  }

  /**
   * Sync db to latest event id. Catches
   * exceptions (if any) while syncing and convert them
   * to TException
   * @throws TException
   */
  private void syncToLatestEventId(org.apache.impala.catalog.Db db, String apiName)
      throws TException {
    Preconditions.checkState(
        BackendConfig.INSTANCE.enableSyncToLatestEventOnDdls(),
            "sync to latest event flag should be set to true");
    Preconditions.checkState(db.isLockHeldByCurrentThread(),
        "Current thread does not hold lock on db: %s", db.getName());
    try {
      MetastoreEventsProcessor.syncToLatestEventId(catalog_, db,
              metastoreEventFactory_, metastoreEventsMetrics_);
    } catch (Exception e) {
      String errMsg = String.format(SYNC_DB_LATEST_EVENT_ID_ERR_MSG,
              db.getName(), apiName);
      LOG.error("{}. Exception stacktrace: {} ", errMsg,
          ExceptionUtils.getFullStackTrace(e));
      rethrowException(e, apiName);
    }
  }

  private <T extends Object> T alterTableCore(String dbname, String tblName,
      org.apache.hadoop.hive.metastore.api.Table newTable, String apiName,
      AlterTableTask<T> task) throws InvalidOperationException, MetaException,
      TException {
    boolean isRename = !dbname.equalsIgnoreCase(newTable.getDbName()) ||
        !tblName.equalsIgnoreCase(newTable.getTableName());
    org.apache.impala.catalog.Table tbl = getTableAndAcquireWriteLock(dbname, tblName,
        apiName);
    org.apache.impala.catalog.Table newTbl = null;
    try {
      // perform HMS operation
      if (!isRename) {
        // release lock if it is not rename. For rename operation the lock would
        // get released in finally block
        catalog_.getLock().writeLock().unlock();
        T resp = task.execute();
        syncToLatestEventId(tbl, apiName);
        return resp;
      }
      long currentEventId = super.get_current_notificationEventId().getEventId();
      T resp = task.execute();
      // Rename scenario, remove old table and add new one
      try {
        // the alter table event is generated on the renamed table
        List<NotificationEvent> events =
            MetastoreEventsProcessor.getNextMetastoreEventsInBatchesForTable(catalog_,
                currentEventId, newTable.getDbName(), newTable.getTableName(),
                MetastoreEvents.AlterTableEvent.EVENT_TYPE);
        Preconditions.checkState(events.size() == 1, String.format("For table %s.%s, "
            + "from event id: %s, expected ALTER_TABLE events size to be 1 but is %s",
            newTable.getDbName(), newTable.getTableName(), currentEventId,
            events.size()));

        MetastoreEvents.MetastoreEvent event = metastoreEventFactory_.get(events.get(0),
            metastoreEventsMetrics_);
        Preconditions.checkState(event instanceof MetastoreEvents.AlterTableEvent);

        MetastoreEvents.AlterTableEvent alterEvent =
            (MetastoreEvents.AlterTableEvent) event;

        org.apache.hadoop.hive.metastore.api.Table oldMsTable =
            alterEvent.getBeforeTable();
        org.apache.hadoop.hive.metastore.api.Table newMsTable =
            alterEvent.getAfterTable();

        TTableName oldTTable = new TTableName(oldMsTable.getDbName(),
            oldMsTable.getTableName());
        TTableName newTTable = new TTableName(newMsTable.getDbName(),
            newMsTable.getTableName());
        // Rename the table in the Catalog and get the resulting catalog object.
        // ALTER TABLE/VIEW RENAME is implemented as an ADD + DROP.
        Pair<org.apache.impala.catalog.Table, org.apache.impala.catalog.Table> result =
            catalog_.renameTable(oldTTable, newTTable);
        if (result == null || result.first == null || result.second == null) {
          throw new CatalogException("failed to rename table " + oldTTable + " to " +
              newTTable + " for " + apiName);
        }
        // first set the last synced event id to the alter table's event id
        result.second.setLastSyncedEventId(alterEvent.getEventId());
        result.second.setCreateEventId(alterEvent.getEventId());
        catalogOpExecutor_.addToDeleteEventLog(alterEvent.getEventId(),
            DeleteEventLog.getTblKey(oldTTable.getDb_name(), oldTTable.getTable_name()));
      } finally {
        catalog_.getLock().writeLock().unlock();
      }
      // sync renamed table to latest event id
      newTbl = getTableAndAcquireWriteLock(newTable.getDbName(), newTable.getTableName(),
          apiName);
      // release writelock
      catalog_.getLock().writeLock().unlock();
      syncToLatestEventId(newTbl, apiName);
      return resp;
    } catch (Exception e) {
      rethrowException(e, apiName);
    } finally {
      catalogOpExecutor_.UnlockWriteLockIfErronouslyLocked();
      if (tbl != null && tbl.isWriteLockedByCurrentThread()) {
        tbl.releaseWriteLock();
      }
      if (newTbl != null && newTbl.isWriteLockedByCurrentThread()) {
        newTbl.releaseWriteLock();
      }
    }
    return null;
  }

  private void createTableCore(String apiName, String dbNameWithCatalog, String tblName,
      TImpalaTableType tblType, CreateTableTask task) throws TException {
    String dbName = MetaStoreUtils.parseDbName(dbNameWithCatalog, serverConf_)[1];
    catalogOpExecutor_.getMetastoreDdlLock().lock();
    org.apache.impala.catalog.Table tbl = null;
    long fromEventId = -1;
    try {
      fromEventId = super.get_current_notificationEventId().getEventId();
      try {
        task.execute();
      } catch (TException e) {
        // rethrow if not AlreadyExistsException
        if (!(e instanceof AlreadyExistsException)) {
          throw e;
        }
        if (catalog_.getTableNoThrow(dbName, tblName) != null) {
          LOG.debug("Table {}.{} already exists in metastore as well as catalog",
              dbName, tblName);
          throw e;
        }
        LOG.debug("Table {}.{} exists in metastore but not in catalog. Ignoring " +
            "exception {} from metastore", dbName, tblName, e.getClass().getName());
      }
      List<NotificationEvent> events =
          MetastoreEventsProcessor.getNextMetastoreEventsInBatchesForTable(catalog_,
              fromEventId, dbName, tblName, MetastoreEvents.CreateTableEvent.EVENT_TYPE);
      Preconditions.checkState(events.size() == 1,
          "Table %s.%s was recreated in metastore since event id %s" +
              "while the current table creation was in progress", dbName, tblName,
          fromEventId);
      long createEventId = events.get(0).getEventId();
      catalog_.addIncompleteTable(dbName, tblName, tblType, /*tblComment*/null,
          createEventId);
      LOG.info("Added incomplete table {}.{} (type={}) with create event id: {}",
          dbName, tblName, tblType, createEventId);
      // sync to latest event ID
      tbl = getTableAndAcquireWriteLock(dbName, tblName, apiName);
      catalog_.getLock().writeLock().unlock();
      syncToLatestEventId(tbl, apiName);
    } catch (Exception e) {
      rethrowException(e, apiName);
    } finally {
      catalogOpExecutor_.getMetastoreDdlLock().unlock();
      catalogOpExecutor_.UnlockWriteLockIfErronouslyLocked();
      if (tbl != null && tbl.isWriteLockedByCurrentThread()) {
        tbl.releaseWriteLock();
      }
    }
  }

  private abstract class AlterTableTask<T> {
    public abstract T execute() throws InvalidOperationException, MetaException,
        TException;
  }

  private abstract class CreateTableTask<T> {
    public abstract T execute() throws TException;
  }
}
