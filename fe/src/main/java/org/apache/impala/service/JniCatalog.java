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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import java.util.Map;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.stream.Collectors;
import org.apache.hadoop.hive.metastore.api.CurrentNotificationEventId;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.impala.analysis.TableName;
import org.apache.impala.authorization.AuthorizationConfig;
import org.apache.impala.authorization.AuthorizationFactory;
import org.apache.impala.authorization.AuthorizationManager;
import org.apache.impala.catalog.Catalog;
import org.apache.impala.catalog.CatalogException;
import org.apache.impala.catalog.CatalogServiceCatalog;
import org.apache.impala.catalog.Db;
import org.apache.impala.catalog.FeDb;
import org.apache.impala.catalog.FileMetadataLoader;
import org.apache.impala.catalog.Function;
import org.apache.impala.catalog.MetaStoreClientPool;
import org.apache.impala.catalog.MetaStoreClientPool.MetaStoreClient;
import org.apache.impala.catalog.ParallelFileMetadataLoader;
import org.apache.impala.catalog.Table;
import org.apache.impala.catalog.events.ExternalEventsProcessor;
import org.apache.impala.catalog.events.MetastoreEvents.EventFactoryForSyncToLatestEvent;
import org.apache.impala.catalog.events.MetastoreEvents.MetastoreEventFactory;
import org.apache.impala.catalog.events.MetastoreEventsProcessor;
import org.apache.impala.catalog.events.NoOpEventProcessor;
import org.apache.impala.catalog.metastore.ICatalogMetastoreServer;
import org.apache.impala.catalog.metastore.NoOpCatalogMetastoreServer;
import org.apache.impala.common.ImpalaException;
import org.apache.impala.common.InternalException;
import org.apache.impala.common.JniUtil;
import org.apache.impala.common.Pair;
import org.apache.impala.compat.MetastoreShim;
import org.apache.impala.hive.executor.HiveJavaFunctionFactoryImpl;
import org.apache.impala.service.JniCatalogOp.JniCatalogOpCallable;
import org.apache.impala.thrift.TBackendGflags;
import org.apache.impala.thrift.TCatalogObject;
import org.apache.impala.thrift.TDatabase;
import org.apache.impala.thrift.TDdlExecRequest;
import org.apache.impala.thrift.TErrorCode;
import org.apache.impala.thrift.TGetCatalogDeltaRequest;
import org.apache.impala.thrift.TGetCatalogDeltaResponse;
import org.apache.impala.thrift.TGetCatalogServerMetricsResponse;
import org.apache.impala.thrift.TGetDbsParams;
import org.apache.impala.thrift.TGetDbsResult;
import org.apache.impala.thrift.TGetFunctionsRequest;
import org.apache.impala.thrift.TGetFunctionsResponse;
import org.apache.impala.thrift.TGetLatestCompactionsRequest;
import org.apache.impala.thrift.TGetLatestCompactionsResponse;
import org.apache.impala.thrift.TGetNullPartitionNameResponse;
import org.apache.impala.thrift.TGetPartialCatalogObjectRequest;
import org.apache.impala.thrift.TGetPartitionStatsRequest;
import org.apache.impala.thrift.TGetPartitionStatsResponse;
import org.apache.impala.thrift.TGetTableMetricsParams;
import org.apache.impala.thrift.TGetTablesParams;
import org.apache.impala.thrift.TGetTablesResult;
import org.apache.impala.thrift.TLogLevel;
import org.apache.impala.thrift.TPrioritizeLoadRequest;
import org.apache.impala.thrift.TResetMetadataRequest;
import org.apache.impala.thrift.TStatus;
import org.apache.impala.thrift.TTableUsage;
import org.apache.impala.thrift.TUniqueId;
import org.apache.impala.thrift.TUpdateCatalogRequest;
import org.apache.impala.thrift.TUpdateTableUsageRequest;
import org.apache.impala.thrift.TGetAllHadoopConfigsResponse;
import org.apache.impala.util.AuthorizationUtil;
import org.apache.impala.util.CatalogOpUtil;
import org.apache.impala.util.EventSequence;
import org.apache.impala.util.GlogAppender;
import org.apache.impala.util.MetaStoreUtil;
import org.apache.impala.util.NoOpEventSequence;
import org.apache.impala.util.PatternMatcher;
import org.apache.impala.util.ThreadNameAnnotator;
import org.apache.impala.util.TUniqueIdUtil;
import org.apache.thrift.TBase;
import org.apache.thrift.TException;
import org.apache.thrift.TSerializer;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TSimpleJSONProtocol;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * JNI-callable interface for the CatalogService. The main point is to serialize and
 * de-serialize thrift structures between C and Java parts of the CatalogService.
 */
public class JniCatalog {
  private static final Logger LOG = LoggerFactory.getLogger(JniCatalog.class);
  private static final TBinaryProtocol.Factory protocolFactory_ =
      new TBinaryProtocol.Factory();
  private final CatalogServiceCatalog catalog_;
  private final CatalogOpExecutor catalogOpExecutor_;
  private final ICatalogMetastoreServer catalogMetastoreServer_;
  private final AuthorizationManager authzManager_;

  // A unique identifier for this instance of the Catalog Service.
  // The service id will be regenerated when the CatalogD becomes active.
  private static TUniqueId catalogServiceId_ = generateId();

  // ReadWriteLock to protect catalogServiceId_.
  private final static ReentrantReadWriteLock catalogServiceIdLock_ =
      new ReentrantReadWriteLock(true /*fair ordering*/);

  private static TUniqueId generateId() {
    UUID uuid = UUID.randomUUID();
    return new TUniqueId(uuid.getMostSignificantBits(), uuid.getLeastSignificantBits());
  }

  private static final HiveConf HIVE_CONF = new HiveConf();

  public JniCatalog(byte[] thriftBackendConfig) throws ImpalaException {
    TBackendGflags cfg = new TBackendGflags();
    JniUtil.deserializeThrift(protocolFactory_, cfg, thriftBackendConfig);

    BackendConfig.create(cfg);

    Preconditions.checkArgument(cfg.num_metadata_loading_threads > 0);
    Preconditions.checkArgument(cfg.max_hdfs_partitions_parallel_load > 0);
    Preconditions.checkArgument(cfg.max_nonhdfs_partitions_parallel_load > 0);
    Preconditions.checkArgument(cfg.initial_hms_cnxn_timeout_s > 0);
    // This trick saves having to pass a TLogLevel enum, which is an object and more
    // complex to pass through JNI.
    GlogAppender.Install(TLogLevel.values()[cfg.impala_log_lvl],
        TLogLevel.values()[cfg.non_impala_java_vlog]);

    // create the appropriate auth factory from backend config
    // this logic is shared with JniFrontend
    final AuthorizationFactory authzFactory =
        AuthorizationUtil.authzFactoryFrom(BackendConfig.INSTANCE);

    LOG.info(JniUtil.getJavaVersion());

    final AuthorizationConfig authzConfig = authzFactory.getAuthorizationConfig();

    if (MetastoreShim.getMajorVersion() > 2) {
      MetastoreShim.setHiveClientCapabilities();
    }

    MetaStoreClientPool metaStoreClientPool =
        new MetaStoreClientPool(CatalogServiceCatalog.INITIAL_META_STORE_CLIENT_POOL_SIZE,
            cfg.initial_hms_cnxn_timeout_s);
    catalog_ = new CatalogServiceCatalog(cfg.load_catalog_in_background,
        cfg.num_metadata_loading_threads, cfg.local_library_path, metaStoreClientPool);
    authzManager_ = authzFactory.newAuthorizationManager(catalog_);
    catalog_.setAuthzManager(authzManager_);
    catalogOpExecutor_ = new CatalogOpExecutor(catalog_, authzConfig, authzManager_,
        new HiveJavaFunctionFactoryImpl(
            BackendConfig.INSTANCE.getBackendCfg().local_library_path));
    MetastoreEventFactory eventFactory =
        new EventFactoryForSyncToLatestEvent(catalogOpExecutor_);
    catalog_.setEventFactoryForSyncToLatestEvent(eventFactory);
    ExternalEventsProcessor eventsProcessor =
        getEventsProcessor(metaStoreClientPool, catalogOpExecutor_);
    catalog_.setMetastoreEventProcessor(eventsProcessor);
    catalog_.startEventsProcessor();
    catalogMetastoreServer_ = getCatalogMetastoreServer(catalogOpExecutor_);
    catalog_.setCatalogMetastoreServer(catalogMetastoreServer_);
    catalogMetastoreServer_.start();

    try {
      catalog_.reset(NoOpEventSequence.INSTANCE);
    } catch (CatalogException e) {
      LOG.error("Error initializing Catalog. Please run 'invalidate metadata'", e);
    }
  }

  /**
   * Returns an instance of CatalogMetastoreServer if start_hms_server configuration is
   * true. Otherwise, returns a NoOpCatalogMetastoreServer
   */
  @VisibleForTesting
  private ICatalogMetastoreServer getCatalogMetastoreServer(
      CatalogOpExecutor catalogOpExecutor) {
    if (!BackendConfig.INSTANCE.startHmsServer()) {
      return NoOpCatalogMetastoreServer.INSTANCE;
    }
    return MetastoreShim.getCatalogMetastoreServer(catalogOpExecutor);
  }

  /**
   * Returns a Metastore event processor object if
   * <code>BackendConfig#getHMSPollingIntervalInSeconds</code> returns a non-zero
   * value of polling interval. Otherwise, returns a no-op events processor. It is
   * important to fetch the current notification event id at the Catalog service
   * initialization time so that event processor starts to sync at the event id
   * corresponding to the catalog start time.
   */
  private ExternalEventsProcessor getEventsProcessor(
      MetaStoreClientPool metaStoreClientPool, CatalogOpExecutor catalogOpExecutor)
      throws ImpalaException {
    long eventPollingInterval = BackendConfig.INSTANCE.getHMSPollingIntervalInSeconds();
    if (eventPollingInterval <= 0) {
      LOG.info("Metastore event processing is disabled. Event polling interval is {}",
          eventPollingInterval);
      return NoOpEventProcessor.getInstance();
    }
    try (MetaStoreClient metaStoreClient = metaStoreClientPool.getClient()) {
      CurrentNotificationEventId currentNotificationId =
          metaStoreClient.getHiveClient().getCurrentNotificationEventId();
      return MetastoreEventsProcessor.getInstance(
          catalogOpExecutor, currentNotificationId.getEventId(), eventPollingInterval);
    } catch (TException e) {
      LOG.error("Unable to fetch the current notification event id from metastore.", e);
      throw new CatalogException(
          "Fatal error while initializing metastore event processor", e);
    }
  }

  private <RESULT, PARAMETER extends TBase<?, ?>> RESULT execOp(String methodName,
      String shortDescription, JniCatalogOpCallable<Pair<RESULT, Long>> operand,
      PARAMETER requestParameter) throws ImpalaException, TException {
    return JniCatalogOp.execOp(methodName, shortDescription, operand, requestParameter);
  }

  private byte[] execAndSerialize(String methodName, String shortDescription,
      JniCatalogOpCallable<TBase<?, ?>> operand, Runnable finalClause)
      throws ImpalaException, TException {
    TSerializer serializer = new TSerializer(protocolFactory_);
    return JniCatalogOp.execAndSerialize(
        methodName, shortDescription, operand, serializer, finalClause);
  }

  private byte[] execAndSerializeSilentStartAndFinish(String methodName,
      String shortDescription, JniCatalogOpCallable<TBase<?, ?>> operand)
      throws ImpalaException, TException {
    TSerializer serializer = new TSerializer(protocolFactory_);
    return JniCatalogOp.execAndSerializeSilentStartAndFinish(
        methodName, shortDescription, operand, serializer, () -> {});
  }

  private byte[] execAndSerialize(String methodName, String shortDescription,
      JniCatalogOpCallable<TBase<?, ?>> operand) throws ImpalaException, TException {
    return execAndSerialize(methodName, shortDescription, operand, () -> {});
  }

  private String fullyQualifiedTableName(String databaseName, String tableName) {
    return databaseName + "." + tableName;
  }

  public static TUniqueId getServiceId() {
    catalogServiceIdLock_.readLock().lock();
    try {
      return catalogServiceId_;
    } finally {
      catalogServiceIdLock_.readLock().unlock();
    }
  }

  public void regenerateServiceId() {
    catalogServiceIdLock_.writeLock().lock();
    try {
      TUniqueId oldCatalogServiceId = catalogServiceId_;
      catalogServiceId_ = generateId();
      LOG.info("Old Catalog Service ID " + TUniqueIdUtil.PrintId(oldCatalogServiceId) +
          ", Regenerate Catalog Service ID " + TUniqueIdUtil.PrintId(catalogServiceId_));
    } finally {
      catalogServiceIdLock_.writeLock().unlock();
    }
  }

  /**
   * Gets the current catalog version.
   */
  public long getCatalogVersion() {
    return catalog_.getCatalogVersion();
  }

  public byte[] getCatalogDelta(byte[] thriftGetCatalogDeltaReq)
      throws ImpalaException, TException {
    TGetCatalogDeltaRequest params = new TGetCatalogDeltaRequest();
    JniUtil.deserializeThrift(protocolFactory_, params, thriftGetCatalogDeltaReq);
    String shortDesc = "Getting catalog delta from version " + params.getFrom_version();

    return execAndSerialize("getCatalogDelta", shortDesc, () -> {
      long catalogDelta = catalog_.getCatalogDelta(
          params.getNative_catalog_server_ptr(), params.getFrom_version());
      return new TGetCatalogDeltaResponse(catalogDelta);
    });
  }

  /**
   * Executes the given DDL request and returns the result.
   */
  public byte[] execDdl(byte[] thriftDdlExecReq) throws ImpalaException, TException {
    TDdlExecRequest params = new TDdlExecRequest();
    JniUtil.deserializeThrift(protocolFactory_, params, thriftDdlExecReq);
    String shortDesc = CatalogOpUtil.getShortDescForExecDdl(params);

    return execAndSerialize(
        "execDdl", shortDesc, () -> catalogOpExecutor_.execDdlRequest(params));
  }

  /**
   * Execute a reset metadata statement. See comment in CatalogOpExecutor.java.
   */
  public byte[] resetMetadata(byte[] thriftResetMetadataReq)
      throws ImpalaException, TException {
    TResetMetadataRequest req = new TResetMetadataRequest();
    JniUtil.deserializeThrift(protocolFactory_, req, thriftResetMetadataReq);
    String shortDesc = CatalogOpUtil.getShortDescForReset(req);

    return execAndSerialize("resetMetadata", shortDesc,
        () -> catalogOpExecutor_.execResetMetadata(req));
  }

  /**
   * Returns a list of databases matching an optional pattern. The argument is a
   * serialized TGetDbParams object. The return type is a serialized TGetDbResult object.
   */
  public byte[] getDbs(byte[] thriftGetTablesParams) throws ImpalaException, TException {
    TGetDbsParams params = new TGetDbsParams();
    JniUtil.deserializeThrift(protocolFactory_, params, thriftGetTablesParams);
    TGetDbsResult result = new TGetDbsResult();
    String shortDesc = "Getting databases with pattern: " + params.getPattern();

    return execAndSerialize("getDbs", shortDesc, () -> {
      List<Db> dbs = catalog_.getDbs(PatternMatcher.MATCHER_MATCH_ALL);
      List<TDatabase> tDbs = Lists.newArrayListWithCapacity(dbs.size());
      for (FeDb db : dbs) {
        tDbs.add(db.toThrift());
      }
      result.setDbs(tDbs);
      return result;
    });
  }

  /**
   * Returns a list of table names matching an optional pattern. The argument is a
   * serialized TGetTablesParams object. The return type is a serialized TGetTablesResult
   * object.
   */
  public byte[] getTableNames(byte[] thriftGetTablesParams)
      throws ImpalaException, TException {
    TGetTablesParams params = new TGetTablesParams();
    JniUtil.deserializeThrift(protocolFactory_, params, thriftGetTablesParams);
    TGetTablesResult result = new TGetTablesResult();
    String shortDesc =
        String.format("Getting table names with parameters: database: %s, pattern: %s ",
            params.getDb(), params.getPattern());

    return execAndSerialize("getTableNames", shortDesc, () -> {
      List<String> tables = catalog_.getTableNames(
          params.getDb(), PatternMatcher.createHivePatternMatcher(params.getPattern()));
      result.setTables(tables);
      return result;
    });
  }

  /**
   * Returns the collected metrics of a table.
   */
  public String getTableMetrics(byte[] getTableMetricsParams)
      throws ImpalaException, TException {
    TGetTableMetricsParams params = new TGetTableMetricsParams();
    JniUtil.deserializeThrift(protocolFactory_, params, getTableMetricsParams);
    String shortDesc = "Getting table metrics for " + params.getTable_name();

    return execOp("getTableMetrics", shortDesc, () -> {
      String res = catalog_.getTableMetrics(params.table_name);
      return Pair.create(res, (long) res.length());
    }, params);
  }

  /**
   * Gets the thrift representation of a catalog object.
   */
  public byte[] getCatalogObject(byte[] thriftParams) throws ImpalaException, TException {
    TCatalogObject objectDesc = new TCatalogObject();
    JniUtil.deserializeThrift(protocolFactory_, objectDesc, thriftParams);
    String shortDesc =
        "Getting thrift catalog object of " + Catalog.toCatalogObjectKey(objectDesc);

    return execAndSerialize(
        "getCatalogObject", shortDesc, () -> catalog_.getTCatalogObject(objectDesc));
  }

  /**
   * Gets the json string of a catalog object. It can only be used in showing debug
   * messages and can't be deserialized to a thrift object. The returned object is also
   * slimmer than the one obtained from getCatalogObject() because binary data fields
   * are excluded.
   */
  public String getJsonCatalogObject(byte[] thriftParams)
      throws ImpalaException, TException {
    TCatalogObject objectDesc = new TCatalogObject();
    JniUtil.deserializeThrift(protocolFactory_, objectDesc, thriftParams);
    TSerializer jsonSerializer = new TSerializer(new TSimpleJSONProtocol.Factory());
    String shortDesc =
        "Getting JSON catalog object of " + Catalog.toCatalogObjectKey(objectDesc);

    return execOp("getJsonCatalogObject", shortDesc, () -> {
      String res = jsonSerializer.toString(catalog_.getTCatalogObject(objectDesc, true));
      return Pair.create(res, (long) res.length());
    }, objectDesc);
  }

  public byte[] getPartialCatalogObject(byte[] thriftParams)
      throws ImpalaException, TException {
    TGetPartialCatalogObjectRequest req = new TGetPartialCatalogObjectRequest();
    JniUtil.deserializeThrift(protocolFactory_, req, thriftParams);
    String shortDesc = "Getting partial catalog object of "
        + Catalog.toCatalogObjectKey(req.getObject_desc());

    return execAndSerializeSilentStartAndFinish("getPartialCatalogObject", shortDesc,
        () -> catalog_.getPartialCatalogObject(req));
  }

  /**
   * See comment in CatalogServiceCatalog.
   */
  public byte[] getFunctions(byte[] thriftParams) throws ImpalaException, TException {
    TGetFunctionsRequest request = new TGetFunctionsRequest();
    JniUtil.deserializeThrift(protocolFactory_, request, thriftParams);
    TGetFunctionsResponse response = new TGetFunctionsResponse();
    if (!request.isSetDb_name()) {
      throw new InternalException("Database name must be set in call to getFunctions()");
    }

    String shortDesc = "Getting functions for " + request.getDb_name();
    return execAndSerialize("getFunctions", shortDesc, () -> {
      // Get all the functions and convert them to their Thrift representation.
      List<Function> fns = catalog_.getFunctions(request.getDb_name());
      response.setFunctions(new ArrayList<>(fns.size()));
      for (Function fn : fns) {
        response.addToFunctions(fn.toThrift());
      }
      return response;
    });
  }

  public void prioritizeLoad(byte[] thriftLoadReq) throws ImpalaException, TException {
    TPrioritizeLoadRequest request = new TPrioritizeLoadRequest();
    JniUtil.deserializeThrift(protocolFactory_, request, thriftLoadReq);

    String shortDesc = "Prioritize load on table(s): "
        + request.getObject_descs()
              .stream()
              .map(TCatalogObject::getTable)
              .map(t -> fullyQualifiedTableName(t.getDb_name(), t.getTbl_name()))
              .collect(Collectors.joining(", "));
    execOp("prioritizeLoad", shortDesc, () -> {
      catalog_.prioritizeLoad(request.getObject_descs());
      return Pair.create(null, null);
    }, request);
  }

  public byte[] getPartitionStats(byte[] thriftParams)
      throws ImpalaException, TException {
    TGetPartitionStatsRequest request = new TGetPartitionStatsRequest();
    JniUtil.deserializeThrift(protocolFactory_, request, thriftParams);
    TGetPartitionStatsResponse response = new TGetPartitionStatsResponse();
    String shortDescGet = "Getting partition stats of " + request.getTable_name();
    String shortDescSer = "Serializing partition stats of " + request.getTable_name();

    return execAndSerialize("getPartitionStats", shortDescSer, () -> {
      try (ThreadNameAnnotator ignored = new ThreadNameAnnotator(shortDescGet)) {
        response.setPartition_stats(catalog_.getPartitionStats(request));
      } catch (CatalogException e) {
        response.setStatus(
            new TStatus(TErrorCode.INTERNAL_ERROR, ImmutableList.of(e.getMessage())));
      }
      return response;
    });
  }

  /**
   * Process any updates to the metastore required after a query executes. The argument is
   * a serialized TCatalogUpdate.
   */
  public byte[] updateCatalog(byte[] thriftUpdateCatalog)
      throws ImpalaException, TException {
    TUpdateCatalogRequest request = new TUpdateCatalogRequest();
    JniUtil.deserializeThrift(protocolFactory_, request, thriftUpdateCatalog);
    String shortDesc = "Update catalog for "
        + fullyQualifiedTableName(request.getDb_name(), request.getTarget_table());

    return execAndSerialize("updateCatalog", shortDesc,
        () -> catalogOpExecutor_.updateCatalog(request));
  }

  /**
   * Returns information about the current catalog usage.
   */
  public byte[] getCatalogUsage() throws ImpalaException, TException {
    String shortDesc = "Getting catalog usage";
    return execAndSerialize("getCatalogUsage", shortDesc, catalog_::getCatalogUsage);
  }

  /**
   * Returns information about the current catalog operation metrics.
   */
  public byte[] getOperationUsage() throws ImpalaException, TException {
    String shortDesc = "Getting operation usage";
    return execAndSerialize("getOperationUsage", shortDesc, catalog_::getOperationUsage);
  }

  public byte[] getEventProcessorSummary() throws ImpalaException, TException {
    String shortDesc = "Getting event processor summary";
    return execAndSerialize(
        "getEventProcessorSummary", shortDesc, catalog_::getEventProcessorSummary);
  }

  public void updateTableUsage(byte[] req) throws ImpalaException, TException {
    TUpdateTableUsageRequest thriftReq = new TUpdateTableUsageRequest();
    JniUtil.deserializeThrift(protocolFactory_, thriftReq, req);

    String shortDesc = "Update table usage(s):"
        + thriftReq.getUsages()
              .stream()
              .map(TTableUsage::getTable_name)
              .map(TableName::thriftToString)
              .collect(Collectors.joining(", "));
    execOp("updateTableUsage", shortDesc, () -> {
      catalog_.updateTableUsage(thriftReq);
      return Pair.create(null, null);
    }, thriftReq);
  }

  public byte[] getCatalogServerMetrics() throws ImpalaException, TException {
    TGetCatalogServerMetricsResponse response = new TGetCatalogServerMetricsResponse();
    String shortDesc = "Get catalog server metrics";
    return execAndSerializeSilentStartAndFinish(
        "getCatalogServerMetrics", shortDesc, () -> {
          response.setCatalog_partial_fetch_rpc_queue_len(
              catalog_.getPartialFetchRpcQueueLength());
          response.setCatalog_num_file_metadata_loading_threads(
              ParallelFileMetadataLoader.TOTAL_THREADS.get());
          response.setCatalog_num_tables_loading_file_metadata(
              ParallelFileMetadataLoader.TOTAL_TABLES.get());
          response.setCatalog_num_file_metadata_loading_tasks(
              FileMetadataLoader.TOTAL_TASKS.get());
          response.setCatalog_num_tables_loading_metadata(Table.LOADING_TABLES.get());
          response.setCatalog_num_tables_async_loading_metadata(
              catalog_.getNumAsyncLoadingTables());
          response.setCatalog_num_tables_waiting_for_async_loading(
              catalog_.getNumAsyncWaitingTables());
          response.setCatalog_num_dbs(catalog_.getNumDatabases());
          response.setCatalog_num_tables(catalog_.getNumTables());
          response.setCatalog_num_functions(catalog_.getNumFunctions());
          response.setCatalog_num_hms_clients_idle(catalog_.getNumHmsClientsIdle());
          response.setCatalog_num_hms_clients_in_use(catalog_.getNumHmsClientsInUse());
          response.setEvent_metrics(catalog_.getEventProcessorMetrics());
          return response;
        });
  }

  /**
   * Refresh data sources from metadata store.
   */
  public void refreshDataSources() throws TException {
    catalog_.refreshDataSources();
  }

  public byte[] getNullPartitionName() throws ImpalaException, TException {
    return execAndSerialize("getNullPartitionName", "Getting null partition name", () -> {
      TGetNullPartitionNameResponse response = new TGetNullPartitionNameResponse();
      try (MetaStoreClient msClient = catalog_.getMetaStoreClient()) {
        response.setPartition_value(
            MetaStoreUtil.getNullPartitionKeyValue(msClient.getHiveClient()));
        response.setStatus(new TStatus(TErrorCode.OK, Lists.newArrayList()));
      }
      return response;
    });
  }

  public byte[] getLatestCompactions(byte[] thriftParams)
      throws ImpalaException, TException {
    TGetLatestCompactionsRequest request = new TGetLatestCompactionsRequest();
    JniUtil.deserializeThrift(protocolFactory_, request, thriftParams);
    return execAndSerialize("getLatestCompactions", "Getting latest compactions", () -> {
      TGetLatestCompactionsResponse response = new TGetLatestCompactionsResponse();
      try (MetaStoreClient msClient = catalog_.getMetaStoreClient()) {
        response.setPartition_to_compaction_id(MetastoreShim.getLatestCompactions(
            msClient, request.db_name, request.table_name, request.partition_names,
            request.non_parition_name, request.last_compaction_id));
        response.setStatus(new TStatus(TErrorCode.OK, Lists.newArrayList()));
      }
      return response;
    });
  }

  /**
   * Returns the serialized byte array of TGetAllHadoopConfigsResponse
   */
  public byte[] getAllHadoopConfigs() throws ImpalaException {
    Map<String, String> configs = Maps.newHashMap();
    for (Map.Entry<String, String> e: HIVE_CONF) {
      configs.put(e.getKey(), e.getValue());
    }
    TGetAllHadoopConfigsResponse result = new TGetAllHadoopConfigsResponse();
    result.setConfigs(configs);
    try {
      TSerializer serializer = new TSerializer(protocolFactory_);
      return serializer.serialize(result);
    } catch (TException e) {
      throw new InternalException(e.getMessage());
    }
  }
}
