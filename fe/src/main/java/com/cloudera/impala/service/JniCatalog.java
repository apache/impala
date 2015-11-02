// Copyright 2012 Cloudera Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package com.cloudera.impala.service;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import org.apache.thrift.TException;
import org.apache.thrift.TSerializer;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.cloudera.impala.authorization.SentryConfig;
import com.cloudera.impala.authorization.User;
import com.cloudera.impala.catalog.CatalogException;
import com.cloudera.impala.catalog.CatalogServiceCatalog;
import com.cloudera.impala.catalog.Db;
import com.cloudera.impala.catalog.Function;
import com.cloudera.impala.common.ImpalaException;
import com.cloudera.impala.common.InternalException;
import com.cloudera.impala.common.JniUtil;
import com.cloudera.impala.thrift.TCatalogObject;
import com.cloudera.impala.thrift.TDatabase;
import com.cloudera.impala.thrift.TDdlExecRequest;
import com.cloudera.impala.thrift.TFunction;
import com.cloudera.impala.thrift.TGetAllCatalogObjectsResponse;
import com.cloudera.impala.thrift.TGetDbsParams;
import com.cloudera.impala.thrift.TGetDbsResult;
import com.cloudera.impala.thrift.TGetFunctionsRequest;
import com.cloudera.impala.thrift.TGetFunctionsResponse;
import com.cloudera.impala.thrift.TGetTablesParams;
import com.cloudera.impala.thrift.TGetTablesResult;
import com.cloudera.impala.thrift.TLogLevel;
import com.cloudera.impala.thrift.TPrioritizeLoadRequest;
import com.cloudera.impala.thrift.TResetMetadataRequest;
import com.cloudera.impala.thrift.TSentryAdminCheckRequest;
import com.cloudera.impala.thrift.TUniqueId;
import com.cloudera.impala.thrift.TUpdateCatalogRequest;
import com.cloudera.impala.util.GlogAppender;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.collect.Lists;

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

  public JniCatalog(boolean loadInBackground, int numMetadataLoadingThreads,
      String sentryServiceConfig, int impalaLogLevel, int otherLogLevel)
      throws InternalException {
    Preconditions.checkArgument(numMetadataLoadingThreads > 0);
    // This trick saves having to pass a TLogLevel enum, which is an object and more
    // complex to pass through JNI.
    GlogAppender.Install(TLogLevel.values()[impalaLogLevel],
        TLogLevel.values()[otherLogLevel]);

    // Check if the Sentry Service is configured. If so, create a configuration object.
    SentryConfig sentryConfig = null;
    if (!Strings.isNullOrEmpty(sentryServiceConfig)) {
      sentryConfig = new SentryConfig(sentryServiceConfig);
      sentryConfig.loadConfig();
    }
    LOG.info(JniUtil.getJavaVersion());

    catalog_ = new CatalogServiceCatalog(loadInBackground,
        numMetadataLoadingThreads, sentryConfig, getServiceId());
    try {
      catalog_.reset();
    } catch (CatalogException e) {
      LOG.error("Error initialializing Catalog. Please run 'invalidate metadata'", e);
    }
    catalogOpExecutor_ = new CatalogOpExecutor(catalog_);
  }

  public static TUniqueId getServiceId() { return catalogServiceId_; }

  /**
   * Gets all catalog objects
   */
  public byte[] getCatalogObjects(long from_version) throws ImpalaException, TException {
    TGetAllCatalogObjectsResponse resp =
        catalog_.getCatalogObjects(from_version);
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
    List<Db> dbs = catalog_.getDbs(null);
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
    List<String> tables = catalog_.getTableNames(params.db, params.pattern);
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
