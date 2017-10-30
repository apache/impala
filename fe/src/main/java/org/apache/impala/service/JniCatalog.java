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

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.collect.Lists;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import org.apache.impala.authorization.SentryConfig;
import org.apache.impala.authorization.User;
import org.apache.impala.catalog.CatalogException;
import org.apache.impala.catalog.CatalogServiceCatalog;
import org.apache.impala.catalog.Db;
import org.apache.impala.catalog.Function;
import org.apache.impala.common.ImpalaException;
import org.apache.impala.common.InternalException;
import org.apache.impala.common.JniUtil;
import org.apache.impala.thrift.TCatalogObject;
import org.apache.impala.thrift.TDatabase;
import org.apache.impala.thrift.TDdlExecRequest;
import org.apache.impala.thrift.TFunction;
import org.apache.impala.thrift.TGetCatalogDeltaResponse;
import org.apache.impala.thrift.TGetCatalogDeltaRequest;
import org.apache.impala.thrift.TGetDbsParams;
import org.apache.impala.thrift.TGetDbsResult;
import org.apache.impala.thrift.TGetFunctionsRequest;
import org.apache.impala.thrift.TGetFunctionsResponse;
import org.apache.impala.thrift.TGetTablesParams;
import org.apache.impala.thrift.TGetTablesResult;
import org.apache.impala.thrift.TLogLevel;
import org.apache.impala.thrift.TPrioritizeLoadRequest;
import org.apache.impala.thrift.TResetMetadataRequest;
import org.apache.impala.thrift.TSentryAdminCheckRequest;
import org.apache.impala.thrift.TUniqueId;
import org.apache.impala.thrift.TUpdateCatalogRequest;
import org.apache.impala.thrift.TBackendGflags;
import org.apache.impala.util.GlogAppender;
import org.apache.impala.util.PatternMatcher;
import org.apache.thrift.TException;
import org.apache.thrift.TSerializer;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * JNI-callable interface for the CatalogService. The main point is to serialize
 * and de-serialize thrift structures between C and Java parts of the CatalogService.
 */
public class JniCatalog {
  private final static Logger LOG = LoggerFactory.getLogger(JniCatalog.class);
  private final static TBinaryProtocol.Factory protocolFactory_ =
      new TBinaryProtocol.Factory();
  private final CatalogServiceCatalog catalog_;
  private final CatalogOpExecutor catalogOpExecutor_;

  // A unique identifier for this instance of the Catalog Service.
  private static final TUniqueId catalogServiceId_ = generateId();

  private static TUniqueId generateId() {
    UUID uuid = UUID.randomUUID();
    return new TUniqueId(uuid.getMostSignificantBits(), uuid.getLeastSignificantBits());
  }

  public JniCatalog(byte[] thriftBackendConfig) throws InternalException,
      ImpalaException, TException {
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

    // Check if the Sentry Service is configured. If so, create a configuration object.
    SentryConfig sentryConfig = null;
    if (!Strings.isNullOrEmpty(cfg.sentry_config)) {
      sentryConfig = new SentryConfig(cfg.sentry_config);
      sentryConfig.loadConfig();
    }
    LOG.info(JniUtil.getJavaVersion());

    catalog_ = new CatalogServiceCatalog(cfg.load_catalog_in_background,
        cfg.num_metadata_loading_threads, cfg.initial_hms_cnxn_timeout_s, sentryConfig,
        getServiceId(), cfg.principal, cfg.local_library_path);
    try {
      catalog_.reset();
    } catch (CatalogException e) {
      LOG.error("Error initializing Catalog. Please run 'invalidate metadata'", e);
    }
    catalogOpExecutor_ = new CatalogOpExecutor(catalog_);
  }

  public static TUniqueId getServiceId() { return catalogServiceId_; }

  /**
   * Gets all catalog objects
   */
  public byte[] getCatalogDelta(byte[] thriftGetCatalogDeltaReq)
      throws ImpalaException, TException {
    TGetCatalogDeltaRequest params = new TGetCatalogDeltaRequest();
    JniUtil.deserializeThrift(protocolFactory_, params, thriftGetCatalogDeltaReq);
    TGetCatalogDeltaResponse resp = catalog_.getCatalogDelta(params.getFrom_version());
    TSerializer serializer = new TSerializer(protocolFactory_);
    return serializer.serialize(resp);
  }

  /**
   * Gets the current catalog version.
   */
  public long getCatalogVersion() {
    return catalog_.getCatalogVersion();
  }

  /**
   * Executes the given DDL request and returns the result.
   */
  public byte[] execDdl(byte[] thriftDdlExecReq) throws ImpalaException {
    TDdlExecRequest params = new TDdlExecRequest();
    JniUtil.deserializeThrift(protocolFactory_, params, thriftDdlExecReq);
    TSerializer serializer = new TSerializer(protocolFactory_);
    try {
      return serializer.serialize(catalogOpExecutor_.execDdlRequest(params));
    } catch (TException e) {
      throw new InternalException(e.getMessage());
    }
  }

  /**
   * Execute a reset metadata statement. See comment in CatalogOpExecutor.java.
   */
  public byte[] resetMetadata(byte[] thriftResetMetadataReq)
      throws ImpalaException, TException {
    TResetMetadataRequest req = new TResetMetadataRequest();
    JniUtil.deserializeThrift(protocolFactory_, req, thriftResetMetadataReq);
    TSerializer serializer = new TSerializer(protocolFactory_);
    return serializer.serialize(catalogOpExecutor_.execResetMetadata(req));
  }

  /**
   * Returns a list of databases matching an optional pattern.
   * The argument is a serialized TGetDbParams object.
   * The return type is a serialized TGetDbResult object.
   */
  public byte[] getDbs(byte[] thriftGetTablesParams) throws ImpalaException,
      TException {
    TGetDbsParams params = new TGetDbsParams();
    JniUtil.deserializeThrift(protocolFactory_, params, thriftGetTablesParams);
    List<Db> dbs = catalog_.getDbs(PatternMatcher.MATCHER_MATCH_ALL);
    TGetDbsResult result = new TGetDbsResult();
    List<TDatabase> tDbs = Lists.newArrayListWithCapacity(dbs.size());
    for (Db db: dbs) tDbs.add(db.toThrift());
    result.setDbs(tDbs);
    TSerializer serializer = new TSerializer(protocolFactory_);
    return serializer.serialize(result);
  }

  /**
   * Returns a list of table names matching an optional pattern.
   * The argument is a serialized TGetTablesParams object.
   * The return type is a serialized TGetTablesResult object.
   */
  public byte[] getTableNames(byte[] thriftGetTablesParams) throws ImpalaException,
      TException {
    TGetTablesParams params = new TGetTablesParams();
    JniUtil.deserializeThrift(protocolFactory_, params, thriftGetTablesParams);
    List<String> tables = catalog_.getTableNames(params.db,
        PatternMatcher.createHivePatternMatcher(params.pattern));
    TGetTablesResult result = new TGetTablesResult();
    result.setTables(tables);
    TSerializer serializer = new TSerializer(protocolFactory_);
    return serializer.serialize(result);
  }

  /**
   * Gets the thrift representation of a catalog object.
   */
  public byte[] getCatalogObject(byte[] thriftParams) throws ImpalaException,
      TException {
    TCatalogObject objectDescription = new TCatalogObject();
    JniUtil.deserializeThrift(protocolFactory_, objectDescription, thriftParams);
    TSerializer serializer = new TSerializer(protocolFactory_);
    return serializer.serialize(catalog_.getTCatalogObject(objectDescription));
  }

  /**
   * See comment in CatalogServiceCatalog.
   */
  public byte[] getFunctions(byte[] thriftParams) throws ImpalaException,
      TException {
    TGetFunctionsRequest request = new TGetFunctionsRequest();
    JniUtil.deserializeThrift(protocolFactory_, request, thriftParams);
    TSerializer serializer = new TSerializer(protocolFactory_);
    if (!request.isSetDb_name()) {
      throw new InternalException("Database name must be set in call to " +
          "getFunctions()");
    }

    // Get all the functions and convert them to their Thrift representation.
    List<Function> fns = catalog_.getFunctions(request.getDb_name());
    TGetFunctionsResponse response = new TGetFunctionsResponse();
    response.setFunctions(new ArrayList<TFunction>(fns.size()));
    for (Function fn: fns) {
      response.addToFunctions(fn.toThrift());
    }

    return serializer.serialize(response);
  }

  public void prioritizeLoad(byte[] thriftLoadReq) throws ImpalaException,
      TException  {
    TPrioritizeLoadRequest request = new TPrioritizeLoadRequest();
    JniUtil.deserializeThrift(protocolFactory_, request, thriftLoadReq);
    catalog_.prioritizeLoad(request.getObject_descs());
  }

  /**
   * Verifies whether the user is configured as an admin on the Sentry Service. Throws
   * an AuthorizationException if the user does not have admin privileges or if there
   * were errors communicating with the Sentry Service.
   */
  public void checkUserSentryAdmin(byte[] thriftReq) throws ImpalaException,
      TException  {
    TSentryAdminCheckRequest request = new TSentryAdminCheckRequest();
    JniUtil.deserializeThrift(protocolFactory_, request, thriftReq);
    catalog_.getSentryProxy().checkUserSentryAdmin(
        new User(request.getHeader().getRequesting_user()));
  }

  /**
   * Process any updates to the metastore required after a query executes.
   * The argument is a serialized TCatalogUpdate.
   */
  public byte[] updateCatalog(byte[] thriftUpdateCatalog) throws ImpalaException,
      TException  {
    TUpdateCatalogRequest request = new TUpdateCatalogRequest();
    JniUtil.deserializeThrift(protocolFactory_, request, thriftUpdateCatalog);
    TSerializer serializer = new TSerializer(protocolFactory_);
    return serializer.serialize(catalogOpExecutor_.updateCatalog(request));
  }
}
