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
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.client.HdfsClientConfigKeys;
import org.apache.hadoop.security.Groups;
import org.apache.hadoop.security.JniBasedUnixGroupsMappingWithFallback;
import org.apache.hadoop.security.JniBasedUnixGroupsNetgroupMappingWithFallback;
import org.apache.hadoop.security.ShellBasedUnixGroupsMapping;
import org.apache.hadoop.security.ShellBasedUnixGroupsNetgroupMapping;
import org.apache.impala.analysis.DescriptorTable;
import org.apache.impala.analysis.ToSqlUtils;
import org.apache.impala.authentication.saml.WrappedWebContext;
import org.apache.impala.authorization.AuthorizationFactory;
import org.apache.impala.authorization.ImpalaInternalAdminUser;
import org.apache.impala.authorization.User;
import org.apache.impala.catalog.DatabaseNotFoundException;
import org.apache.impala.catalog.FeDataSource;
import org.apache.impala.catalog.FeDb;
import org.apache.impala.catalog.FeTable;
import org.apache.impala.catalog.Function;
import org.apache.impala.catalog.StructType;
import org.apache.impala.catalog.Type;
import org.apache.impala.common.FileSystemUtil;
import org.apache.impala.common.ImpalaException;
import org.apache.impala.common.InternalException;
import org.apache.impala.common.JniUtil;
import org.apache.impala.common.TransactionException;
import org.apache.impala.hooks.QueryCompleteContext;
import org.apache.impala.service.Frontend.PlanCtx;
import org.apache.impala.thrift.TBackendGflags;
import org.apache.impala.thrift.TBuildTestDescriptorTableParams;
import org.apache.impala.thrift.TCatalogObject;
import org.apache.impala.thrift.TColumnValue;
import org.apache.impala.thrift.TDatabase;
import org.apache.impala.thrift.TDescribeDbParams;
import org.apache.impala.thrift.TDescribeOutputStyle;
import org.apache.impala.thrift.TDescribeResult;
import org.apache.impala.thrift.TDescribeTableParams;
import org.apache.impala.thrift.TDescriptorTable;
import org.apache.impala.thrift.TExecRequest;
import org.apache.impala.thrift.TFunctionCategory;
import org.apache.impala.thrift.TGetAllHadoopConfigsResponse;
import org.apache.impala.thrift.TGetCatalogMetricsResult;
import org.apache.impala.thrift.TGetDataSrcsParams;
import org.apache.impala.thrift.TGetDataSrcsResult;
import org.apache.impala.thrift.TGetDbsParams;
import org.apache.impala.thrift.TGetDbsResult;
import org.apache.impala.thrift.TGetFunctionsParams;
import org.apache.impala.thrift.TGetFunctionsResult;
import org.apache.impala.thrift.TGetHadoopConfigRequest;
import org.apache.impala.thrift.TGetHadoopConfigResponse;
import org.apache.impala.thrift.TGetHadoopGroupsRequest;
import org.apache.impala.thrift.TGetHadoopGroupsResponse;
import org.apache.impala.thrift.TGetTableHistoryResult;
import org.apache.impala.thrift.TGetMetadataTablesParams;
import org.apache.impala.thrift.TGetTablesParams;
import org.apache.impala.thrift.TGetTablesResult;
import org.apache.impala.thrift.TLoadDataReq;
import org.apache.impala.thrift.TLoadDataResp;
import org.apache.impala.thrift.TLogLevel;
import org.apache.impala.thrift.TMetadataOpRequest;
import org.apache.impala.thrift.TConvertTableRequest;
import org.apache.impala.thrift.TQueryCompleteContext;
import org.apache.impala.thrift.TQueryCtx;
import org.apache.impala.thrift.TResultSet;
import org.apache.impala.thrift.TShowFilesParams;
import org.apache.impala.thrift.TShowGrantPrincipalParams;
import org.apache.impala.thrift.TShowRolesParams;
import org.apache.impala.thrift.TShowStatsOp;
import org.apache.impala.thrift.TShowStatsParams;
import org.apache.impala.thrift.TStringLiteral;
import org.apache.impala.thrift.TDescribeHistoryParams;
import org.apache.impala.thrift.TSessionState;
import org.apache.impala.thrift.TTableName;
import org.apache.impala.thrift.TUniqueId;
import org.apache.impala.thrift.TUpdateCatalogCacheRequest;
import org.apache.impala.thrift.TUpdateExecutorMembershipRequest;
import org.apache.impala.thrift.TWrappedHttpRequest;
import org.apache.impala.thrift.TWrappedHttpResponse;
import org.apache.impala.util.AuthorizationUtil;
import org.apache.impala.util.ExecutorMembershipSnapshot;
import org.apache.impala.util.GlogAppender;
import org.apache.impala.util.PatternMatcher;
import org.apache.impala.util.TSessionStateUtil;
import org.apache.log4j.Appender;
import org.apache.log4j.FileAppender;
import org.apache.thrift.TException;
import org.apache.thrift.TSerializer;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.lang.IllegalArgumentException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Enumeration;
import java.util.List;
import java.util.Map;

/**
 * JNI-callable interface onto a wrapped Frontend instance. The main point is to serialise
 * and deserialise thrift structures between C and Java.
 */
public class JniFrontend {
  private final static Logger LOG = LoggerFactory.getLogger(JniFrontend.class);
  private final static TBinaryProtocol.Factory protocolFactory_ =
      new TBinaryProtocol.Factory();
  private final Frontend frontend_;
  public final static String KEYSTORE_ERROR_MSG = "Failed to get password from" +
      "keystore, error: invalid key '%s' or password doesn't exist";

  /**
   * Create a new instance of the Jni Frontend.
   */
  public JniFrontend(byte[] thriftBackendConfig, boolean isBackendTest)
    throws ImpalaException, TException {
    TBackendGflags cfg = new TBackendGflags();
    JniUtil.deserializeThrift(protocolFactory_, cfg, thriftBackendConfig);

    BackendConfig.create(cfg);

    GlogAppender.Install(TLogLevel.values()[cfg.impala_log_lvl],
        TLogLevel.values()[cfg.non_impala_java_vlog]);
    LOG.info(JniUtil.getJavaVersion());

    if (cfg.is_coordinator) {
      final AuthorizationFactory authzFactory =
          AuthorizationUtil.authzFactoryFrom(BackendConfig.INSTANCE);
      frontend_ = new Frontend(authzFactory, isBackendTest);
    } else {
      // Avoid instantiating Frontend in executor only impalads.
      frontend_ = null;
    }
  }

  /**
   * Jni wrapper for Frontend.createExecRequest(). Accepts a serialized
   * TQueryContext; returns a serialized TQueryExecRequest.
   */
  public byte[] createExecRequest(byte[] thriftQueryContext)
      throws ImpalaException {
    Preconditions.checkNotNull(frontend_);
    TQueryCtx queryCtx = new TQueryCtx();
    JniUtil.deserializeThrift(protocolFactory_, queryCtx, thriftQueryContext);

    PlanCtx planCtx = new PlanCtx(queryCtx);
    TExecRequest result = frontend_.createExecRequest(planCtx);
    if (LOG.isTraceEnabled()) {
      String explainStr = planCtx.getExplainString();
      if (!explainStr.isEmpty()) LOG.trace(explainStr);
    }

    // TODO: avoid creating serializer for each query?
    try {
      TSerializer serializer = new TSerializer(protocolFactory_);
      return serializer.serialize(result);
    } catch (TException e) {
      throw new InternalException(e.getMessage());
    }
  }

  // Deserialize and merge each thrift catalog update into a single merged update
  public byte[] updateCatalogCache(byte[] req) throws ImpalaException, TException {
    Preconditions.checkNotNull(frontend_);
    TUpdateCatalogCacheRequest request = new TUpdateCatalogCacheRequest();
    JniUtil.deserializeThrift(protocolFactory_, request, req);
    return new TSerializer(protocolFactory_).serialize(
        frontend_.updateCatalogCache(request));
  }

  /**
   * Jni wrapper for Frontend.updateMembership(). Accepts a serialized
   * TUpdateExecutorMembershipRequest.
   */
  public void updateExecutorMembership(byte[] thriftMembershipUpdate)
      throws ImpalaException {
    TUpdateExecutorMembershipRequest req = new TUpdateExecutorMembershipRequest();
    JniUtil.deserializeThrift(protocolFactory_, req, thriftMembershipUpdate);
    ExecutorMembershipSnapshot.update(req);
  }

  /**
   * Loads a table or partition with one or more data files. If the "overwrite" flag
   * in the request is true, all existing data in the table/partition will be replaced.
   * If the "overwrite" flag is false, the files will be added alongside any existing
   * data files.
   */
  public byte[] loadTableData(byte[] thriftLoadTableDataParams)
      throws ImpalaException, IOException {
    Preconditions.checkNotNull(frontend_);
    TLoadDataReq request = new TLoadDataReq();
    JniUtil.deserializeThrift(protocolFactory_, request, thriftLoadTableDataParams);
    TLoadDataResp response = frontend_.loadTableData(request);
    try {
      TSerializer serializer = new TSerializer(protocolFactory_);
      return serializer.serialize(response);
    } catch (TException e) {
      throw new InternalException(e.getMessage());
    }
  }

  /**
   * Jni wrapper for Frontend#convertTable(TConvertRequest).
   */
  public void convertTable(byte[] params) throws ImpalaException {
    TExecRequest execRequest = new TExecRequest();
    JniUtil.deserializeThrift(protocolFactory_, execRequest, params);
    frontend_.convertTable(execRequest);
  }

  /**
   * Return an explain plan based on thriftQueryContext, a serialized TQueryContext.
   * This call is thread-safe.
   */
  public String getExplainPlan(byte[] thriftQueryContext) throws ImpalaException {
    Preconditions.checkNotNull(frontend_);
    TQueryCtx queryCtx = new TQueryCtx();
    JniUtil.deserializeThrift(protocolFactory_, queryCtx, thriftQueryContext);
    String plan = frontend_.getExplainString(queryCtx);
    if (LOG.isTraceEnabled()) LOG.trace("Explain plan: " + plan);
    return plan;
  }

  public byte[] getCatalogMetrics() throws ImpalaException {
    Preconditions.checkNotNull(frontend_);
    TGetCatalogMetricsResult metrics = frontend_.getCatalogMetrics();
    try {
      TSerializer serializer = new TSerializer(protocolFactory_);
      return serializer.serialize(metrics);
    } catch (TException e) {
      throw new InternalException(e.getMessage());
    }
  }

  /**
   * Returns a list of table names matching an optional pattern.
   *
   * Implements Hive's pattern-matching semantics for "SHOW TABLE [[LIKE] 'pattern']". The
   * only metacharacters are '*' which matches any string of characters, and '|' which
   * denotes choice. Doing the work here saves loading tables or databases from the
   * metastore (which Hive would do if we passed the call through to the metastore
   * client). If the pattern is null, all strings are considered to match. If it is an
   * empty string, no strings match.
   *
   * The argument is a serialized TGetTablesParams object.
   * The return type is a serialised TGetTablesResult object.
   * @see Frontend#getTableNames
   */
  public byte[] getTableNames(byte[] thriftGetTablesParams) throws ImpalaException {
    Preconditions.checkNotNull(frontend_);
    TGetTablesParams params = new TGetTablesParams();
    JniUtil.deserializeThrift(protocolFactory_, params, thriftGetTablesParams);

    TSessionState session = params.isSetSession() ? params.getSession() : null;
    User user = getUser(session);

    Preconditions.checkState(!params.isSetSession() || user != null );
    List<String> tables = frontend_.getTableNames(params.db,
        PatternMatcher.createHivePatternMatcher(params.pattern), user,
        params.getTable_types());

    TGetTablesResult result = new TGetTablesResult();
    result.setTables(tables);

    try {
      TSerializer serializer = new TSerializer(protocolFactory_);
      return serializer.serialize(result);
    } catch (TException e) {
      throw new InternalException(e.getMessage());
    }
  }

  /**
   * Returns the metadata tables available for the given table. Currently only Iceberg
   * metadata tables are supported.
   *
   * Pattern matching is done as in getTableNames().
   *
   * The argument is a serialized TGetMetadataTablesParams object.
   * The return type is a serialised TGetTablesResult object.
   * @see Frontend#getTableNames
   */
  public byte[] getMetadataTableNames(byte[] thriftGetMetadataTablesParams)
      throws ImpalaException {
    Preconditions.checkNotNull(frontend_);
    TGetMetadataTablesParams params = new TGetMetadataTablesParams();
    JniUtil.deserializeThrift(protocolFactory_, params, thriftGetMetadataTablesParams);

    TSessionState session = params.isSetSession() ? params.getSession() : null;
    User user = getUser(session);

    Preconditions.checkState(!params.isSetSession() || user != null );
    List<String> tables = frontend_.getMetadataTableNames(params.db, params.tbl,
        PatternMatcher.createHivePatternMatcher(params.pattern), user);

    TGetTablesResult result = new TGetTablesResult();
    result.setTables(tables);

    try {
      TSerializer serializer = new TSerializer(protocolFactory_);
      return serializer.serialize(result);
    } catch (TException e) {
      throw new InternalException(e.getMessage());
    }
  }

  /**
   * Returns files info of a table or partition.
   * The argument is a serialized TShowFilesParams object.
   * The return type is a serialised TResultSet object.
   * @see Frontend#getTableFiles
   */
  public byte[] getTableFiles(byte[] thriftShowFilesParams) throws ImpalaException {
    Preconditions.checkNotNull(frontend_);
    TShowFilesParams params = new TShowFilesParams();
    JniUtil.deserializeThrift(protocolFactory_, params, thriftShowFilesParams);
    TResultSet result = frontend_.getTableFiles(params);

    try {
      TSerializer serializer = new TSerializer(protocolFactory_);
      return serializer.serialize(result);
    } catch (TException e) {
      throw new InternalException(e.getMessage());
    }
  }

  /**
   * Implement Hive's pattern-matching semantics for "SHOW DATABASES [[LIKE] 'pattern']",
   * and return a list of databases matching an optional pattern.
   * @see JniFrontend#getTableNames(byte[]) for more detail.
   *
   * The argument is a serialized TGetDbParams object.
   * The return type is a serialised TGetDbResult object.
   * @see Frontend#getDbs
   */
  public byte[] getDbs(byte[] thriftGetTablesParams) throws ImpalaException {
    Preconditions.checkNotNull(frontend_);
    TGetDbsParams params = new TGetDbsParams();
    JniUtil.deserializeThrift(protocolFactory_, params, thriftGetTablesParams);

    TSessionState session = params.isSetSession() ? params.getSession() : null;
    User user = getUser(session);

    List<? extends FeDb> dbs = frontend_.getDbs(
        PatternMatcher.createHivePatternMatcher(params.pattern), user);
    TGetDbsResult result = new TGetDbsResult();
    List<TDatabase> tDbs = Lists.newArrayListWithCapacity(dbs.size());
    for (FeDb db: dbs) tDbs.add(db.toThrift());
    result.setDbs(tDbs);
    try {
      TSerializer serializer = new TSerializer(protocolFactory_);
      return serializer.serialize(result);
    } catch (TException e) {
      throw new InternalException(e.getMessage());
    }
  }

  /** Returns the snapshot history for an Iceberg table.
   *  The argument is a serialized TDescribeHistoryParams object.
   *  Returns a serialized TGetTableHistoryResult object.
   */
  public byte[] getTableHistory(byte[] thriftParams) throws ImpalaException {
    Preconditions.checkNotNull(frontend_);
    TDescribeHistoryParams params = new TDescribeHistoryParams();
    JniUtil.deserializeThrift(protocolFactory_, params, thriftParams);
    TGetTableHistoryResult result = frontend_.getTableHistory(params);
    try {
      TSerializer serializer = new TSerializer(protocolFactory_);
      return serializer.serialize(result);
    } catch (TException e) {
      throw new InternalException(e.getMessage());
    }
  }

  /**
   * Returns a list of data sources matching an optional pattern.
   * The argument is a serialized TGetDataSrcsResult object.
   * The return type is a serialised TGetDataSrcsResult object.
   * @see Frontend#getDataSrcs
   */
  public byte[] getDataSrcMetadata(byte[] thriftParams) throws ImpalaException {
    Preconditions.checkNotNull(frontend_);
    TGetDataSrcsParams params = new TGetDataSrcsParams();
    JniUtil.deserializeThrift(protocolFactory_, params, thriftParams);

    TGetDataSrcsResult result = new TGetDataSrcsResult();
    List<? extends FeDataSource> dataSources = frontend_.getDataSrcs(params.pattern);
    result.setData_src_names(Lists.<String>newArrayListWithCapacity(dataSources.size()));
    result.setLocations(Lists.<String>newArrayListWithCapacity(dataSources.size()));
    result.setClass_names(Lists.<String>newArrayListWithCapacity(dataSources.size()));
    result.setApi_versions(Lists.<String>newArrayListWithCapacity(dataSources.size()));
    for (FeDataSource dataSource: dataSources) {
      result.addToData_src_names(dataSource.getName());
      result.addToLocations(dataSource.getLocation());
      result.addToClass_names(dataSource.getClassName());
      result.addToApi_versions(dataSource.getApiVersion());
    }
    try {
      TSerializer serializer = new TSerializer(protocolFactory_);
      return serializer.serialize(result);
    } catch (TException e) {
      throw new InternalException(e.getMessage());
    }
  }

  public byte[] getStats(byte[] thriftShowStatsParams) throws ImpalaException {
    Preconditions.checkNotNull(frontend_);
    TShowStatsParams params = new TShowStatsParams();
    JniUtil.deserializeThrift(protocolFactory_, params, thriftShowStatsParams);
    Preconditions.checkState(params.isSetTable_name());
    TResultSet result;

    if (params.op == TShowStatsOp.COLUMN_STATS) {
      result = frontend_.getColumnStats(params.getTable_name().getDb_name(),
          params.getTable_name().getTable_name(),
          params.isSetShow_column_minmax_stats() && params.show_column_minmax_stats);
    } else {
      result = frontend_.getTableStats(params.getTable_name().getDb_name(),
          params.getTable_name().getTable_name(), params.op);
    }
    try {
      TSerializer serializer = new TSerializer(protocolFactory_);
      return serializer.serialize(result);
    } catch (TException e) {
      throw new InternalException(e.getMessage());
    }
  }

  /**
   * Returns a list of function names matching an optional pattern.
   * The argument is a serialized TGetFunctionsParams object.
   * The return type is a serialised TGetFunctionsResult object.
   * @see Frontend#getTableNames
   */
  public byte[] getFunctions(byte[] thriftGetFunctionsParams) throws ImpalaException {
    Preconditions.checkNotNull(frontend_);
    TGetFunctionsParams params = new TGetFunctionsParams();
    JniUtil.deserializeThrift(protocolFactory_, params, thriftGetFunctionsParams);

    TGetFunctionsResult result = new TGetFunctionsResult();
    List<String> signatures = Lists.newArrayList();
    List<String> retTypes = Lists.newArrayList();
    List<String> fnBinaryTypes = Lists.newArrayList();
    List<String> fnIsPersistent = Lists.newArrayList();
    List<Function> fns = frontend_.getFunctions(params.category, params.db,
        params.pattern, false);
    for (Function fn: fns) {
      signatures.add(fn.signatureString());
      retTypes.add(fn.getReturnType().toString());
      fnBinaryTypes.add(fn.getBinaryType().name());
      fnIsPersistent.add(String.valueOf(fn.isPersistent()));
    }
    result.setFn_signatures(signatures);
    result.setFn_ret_types(retTypes);
    result.setFn_binary_types(fnBinaryTypes);
    result.setFn_persistence(fnIsPersistent);
    try {
      TSerializer serializer = new TSerializer(protocolFactory_);
      return serializer.serialize(result);
    } catch (TException e) {
      throw new InternalException(e.getMessage());
    }
  }

  /**
   * Gets the thrift representation of a catalog object.
   */
  public byte[] getCatalogObject(byte[] thriftParams) throws ImpalaException,
      TException {
    Preconditions.checkNotNull(frontend_);
    TCatalogObject objectDescription = new TCatalogObject();
    JniUtil.deserializeThrift(protocolFactory_, objectDescription, thriftParams);
    TSerializer serializer = new TSerializer(protocolFactory_);
    return serializer.serialize(
        frontend_.getCatalog().getTCatalogObject(objectDescription));
  }

  /**
   * Returns a database's properties such as its location and comment.
   * The argument is a serialized TDescribeDbParams object.
   * The return type is a serialised TDescribeDbResult object.
   * @see Frontend#describeDb
   */
  public byte[] describeDb(byte[] thriftDescribeDbParams) throws ImpalaException {
    Preconditions.checkNotNull(frontend_);
    TDescribeDbParams params = new TDescribeDbParams();
    JniUtil.deserializeThrift(protocolFactory_, params, thriftDescribeDbParams);

    TDescribeResult result = frontend_.describeDb(
        params.getDb(), params.getOutput_style());

    try {
      TSerializer serializer = new TSerializer(protocolFactory_);
      return serializer.serialize(result);
    } catch (TException e) {
      throw new InternalException(e.getMessage());
    }
  }

  /**
   * Returns a list of the columns making up a table.
   * The argument is a serialized TDescribeParams object.
   * The return type is a serialised TDescribeResult object.
   * @see Frontend#describeTable
   */
  public byte[] describeTable(byte[] thriftDescribeTableParams) throws ImpalaException {
    Preconditions.checkNotNull(frontend_);
    TDescribeTableParams params = new TDescribeTableParams();
    JniUtil.deserializeThrift(protocolFactory_, params, thriftDescribeTableParams);
    Preconditions.checkState(params.isSetTable_name() ^ params.isSetResult_struct());
    User user = new User(TSessionStateUtil.getEffectiveUser(params.getSession()));
    TDescribeResult result = frontend_.describeTable(params, user);
    try {
      TSerializer serializer = new TSerializer(protocolFactory_);
      return serializer.serialize(result);
    } catch (TException e) {
      throw new InternalException(e.getMessage());
    }
  }

  /**
   * Returns a SQL DDL string for creating the specified table.
   */
  public String showCreateTable(byte[] thriftTableName)
      throws ImpalaException {
    Preconditions.checkNotNull(frontend_);
    TTableName params = new TTableName();
    JniUtil.deserializeThrift(protocolFactory_, params, thriftTableName);
    return ToSqlUtils.getCreateTableSql(frontend_.getCatalog().getTable(
        params.getDb_name(), params.getTable_name()));
  }

  /**
   * Returns a SQL DDL string for creating the specified function.
   */
  public String showCreateFunction(byte[] thriftShowCreateFunctionParams)
      throws ImpalaException {
    Preconditions.checkNotNull(frontend_);
    TGetFunctionsParams params = new TGetFunctionsParams();
    JniUtil.deserializeThrift(protocolFactory_, params, thriftShowCreateFunctionParams);
    Preconditions.checkArgument(params.category == TFunctionCategory.SCALAR ||
        params.category == TFunctionCategory.AGGREGATE);
    return ToSqlUtils.getCreateFunctionSql(frontend_.getFunctions(
        params.category, params.db, params.pattern, true));
  }

  /**
   * Creates a thrift descriptor table for testing.
   */
  public byte[] buildTestDescriptorTable(byte[] buildTestDescTblParams)
      throws ImpalaException {
    TBuildTestDescriptorTableParams params = new TBuildTestDescriptorTableParams();
    JniUtil.deserializeThrift(protocolFactory_, params, buildTestDescTblParams);
    Preconditions.checkNotNull(params.slot_types);
    TDescriptorTable result =
        DescriptorTable.buildTestDescriptorTable(params.slot_types);
    try {
      TSerializer serializer = new TSerializer(protocolFactory_);
      byte[] ret = serializer.serialize(result);
      return ret;
    } catch (TException e) {
      throw new InternalException(e.getMessage());
    }
  }

  /**
   * Gets all roles.
   */
  public byte[] getRoles(byte[] showRolesParams) throws ImpalaException {
    Preconditions.checkNotNull(frontend_);
    TShowRolesParams params = new TShowRolesParams();
    JniUtil.deserializeThrift(protocolFactory_, params, showRolesParams);
    try {
      TSerializer serializer = new TSerializer(protocolFactory_);
      return serializer.serialize(frontend_.getAuthzManager().getRoles(params));
    } catch (TException e) {
      throw new InternalException(e.getMessage());
    }
  }

  /**
   * Gets the principal privileges for the given principal.
   */
  public byte[] getPrincipalPrivileges(byte[] showGrantPrincipalParams)
      throws ImpalaException {
    Preconditions.checkNotNull(frontend_);
    TShowGrantPrincipalParams params = new TShowGrantPrincipalParams();
    JniUtil.deserializeThrift(protocolFactory_, params, showGrantPrincipalParams);
    try {
      TSerializer serializer = new TSerializer(protocolFactory_);
      return serializer.serialize(frontend_.getAuthzManager().getPrivileges(params));
    } catch (TException e) {
      throw new InternalException(e.getMessage());
    }
  }

  /**
   * Executes a HiveServer2 metadata operation and returns a TResultSet
   */
  public byte[] execHiveServer2MetadataOp(byte[] metadataOpsParams)
      throws ImpalaException {
    Preconditions.checkNotNull(frontend_);
    TMetadataOpRequest params = new TMetadataOpRequest();
    JniUtil.deserializeThrift(protocolFactory_, params, metadataOpsParams);
    TResultSet result = frontend_.execHiveServer2MetadataOp(params);

    try {
      TSerializer serializer = new TSerializer(protocolFactory_);
      return serializer.serialize(result);
    } catch (TException e) {
      throw new InternalException(e.getMessage());
    }
  }

  public void setCatalogIsReady() {
    Preconditions.checkNotNull(frontend_);
    frontend_.getCatalog().setIsReady(true);
  }

  public void waitForCatalog() {
    Preconditions.checkNotNull(frontend_);
    frontend_.waitForCatalog();
  }

  FeTable getCatalogTable(byte[] tableNameParam) throws ImpalaException {
    Preconditions.checkNotNull(frontend_);
    TTableName tableName = new TTableName();
    JniUtil.deserializeThrift(protocolFactory_, tableName, tableNameParam);
    return frontend_.getCatalog().getTable(tableName.db_name, tableName.table_name);
  }

  // Caching this saves ~50ms per call to getHadoopConfig
  private static final Configuration CONF = new Configuration();
  private static final Groups GROUPS = Groups.getUserToGroupsMappingService(CONF);

  // Caching this saves ~50ms per call to getAllHadoopConfigs
  // org.apache.hadoop.hive.conf.HiveConf inherrits org.apache.hadoop.conf.Configuration
  private static final HiveConf HIVE_CONF = new HiveConf();

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

  /**
   * Returns the corresponding config value for the given key as a serialized
   * TGetHadoopConfigResponse. If the config value is null, the 'value' field in the
   * thrift response object will not be set.
   */
  public byte[] getHadoopConfig(byte[] serializedRequest) throws ImpalaException {
    TGetHadoopConfigRequest request = new TGetHadoopConfigRequest();
    JniUtil.deserializeThrift(protocolFactory_, request, serializedRequest);
    TGetHadoopConfigResponse result = new TGetHadoopConfigResponse();
    result.setValue(CONF.get(request.getName()));
    try {
      TSerializer serializer = new TSerializer(protocolFactory_);
      return serializer.serialize(result);
    } catch (TException e) {
      throw new InternalException(e.getMessage());
    }
  }

  /**
   * Returns the list of Hadoop groups for the given user name.
   */
  public byte[] getHadoopGroups(byte[] serializedRequest) throws ImpalaException {
    TGetHadoopGroupsRequest request = new TGetHadoopGroupsRequest();
    JniUtil.deserializeThrift(protocolFactory_, request, serializedRequest);
    TGetHadoopGroupsResponse result = new TGetHadoopGroupsResponse();
    try {
      result.setGroups(GROUPS.getGroups(request.getUser()));
    } catch (IOException e) {
      // HACK: https://issues.apache.org/jira/browse/HADOOP-15505
      // There is no easy way to know if no groups found for a user
      // other than reading the exception message.
      if (e.getMessage().startsWith("No groups found for user")) {
        result.setGroups(Collections.<String>emptyList());
      } else {
        LOG.error("Error getting Hadoop groups for user: " + request.getUser(), e);
        throw new InternalException(e.getMessage());
      }
    }
    try {
      TSerializer serializer = new TSerializer(protocolFactory_);
      return serializer.serialize(result);
    } catch (TException e) {
      throw new InternalException(e.getMessage());
    }
  }

  /**
   * JNI wrapper for {@link Frontend#callQueryCompleteHooks(QueryCompleteContext)}.
   *
   * @param serializedRequest
   */
  public void callQueryCompleteHooks(byte[] serializedRequest) throws ImpalaException {
    Preconditions.checkNotNull(frontend_);
    final TQueryCompleteContext request = new TQueryCompleteContext();
    JniUtil.deserializeThrift(protocolFactory_, request, serializedRequest);

    final QueryCompleteContext context =
        new QueryCompleteContext(request.getLineage_string());
    this.frontend_.callQueryCompleteHooks(context);
  }

  /**
   * Adds a transaction started externally.
   * @param transactionId the id of the transaction to abort.
   * @throws TransactionException
   */
  public void addTransaction(byte[] thriftQueryContext)
      throws TransactionException, ImpalaException {
    Preconditions.checkNotNull(frontend_);
    TQueryCtx queryCtx = new TQueryCtx();
    JniUtil.deserializeThrift(protocolFactory_, queryCtx, thriftQueryContext);
    this.frontend_.addTransaction(queryCtx);
  }

  /**
   * Aborts a transaction.
   * @param transactionId the id of the transaction to abort.
   * @throws TransactionException
   */
  public void abortTransaction(long transactionId) throws TransactionException {
    Preconditions.checkNotNull(frontend_);
    this.frontend_.abortTransaction(transactionId);
  }

  /**
   * Unregister an already committed transaction.
   * @param transactionId the id of the transaction to clear.
   */
  public void unregisterTransaction(long transactionId) {
    Preconditions.checkNotNull(frontend_);
    this.frontend_.unregisterTransaction(transactionId);
  }

  public byte[] getSaml2Redirect(byte[] serializedRequest) throws ImpalaException {
    Preconditions.checkNotNull(frontend_);
    Preconditions.checkNotNull(frontend_.getSaml2Client());
    final TWrappedHttpRequest request = new TWrappedHttpRequest();
    final TWrappedHttpResponse response = new TWrappedHttpResponse();
    JniUtil.deserializeThrift(protocolFactory_, request, serializedRequest);
    WrappedWebContext webContext = new WrappedWebContext(request, response);
    frontend_.getSaml2Client().setRedirect(webContext);
    try {
      TSerializer serializer = new TSerializer(protocolFactory_);
      return serializer.serialize(response);
    } catch (TException e) {
      throw new InternalException(e.getMessage());
    }
  }

  public byte[] validateSaml2Response(byte[] serializedRequest) throws ImpalaException {
    Preconditions.checkNotNull(frontend_);
    Preconditions.checkNotNull(frontend_.getSaml2Client());
    final TWrappedHttpRequest request = new TWrappedHttpRequest();
    final TWrappedHttpResponse response = new TWrappedHttpResponse();
    JniUtil.deserializeThrift(protocolFactory_, request, serializedRequest);
    WrappedWebContext webContext = new WrappedWebContext(request, response);
    frontend_.getSaml2Client().validateAuthnResponse(webContext);
    try {
      TSerializer serializer = new TSerializer(protocolFactory_);
      return serializer.serialize(response);
    } catch (TException e) {
      throw new InternalException(e.getMessage());
    }
  }

  /**
   * Returns secret from the configured KeyStore.
   * @param secretKeyRequest the serialized secret key to be used for extracting secret.
   */
  public static String getSecretFromKeyStore(byte[] secretKeyRequest)
      throws ImpalaException {
    final TStringLiteral secretKey = new TStringLiteral();
    JniUtil.deserializeThrift(protocolFactory_, secretKey, secretKeyRequest);
    String secret = null;
    try {
      char[] secretCharArray = CONF.getPassword(secretKey.getValue());
      if (secretCharArray != null) {
        secret = new String(secretCharArray);
      } else {
        String errMsg = String.format(KEYSTORE_ERROR_MSG, secretKey.getValue());
        LOG.error(errMsg);
        throw new InternalException(errMsg);
      }
    } catch (IOException e) {
      LOG.error("Failed to get password from keystore, error: " + e);
      throw new InternalException(e.getMessage());
    }
    return secret;
  }

  public String validateSaml2Bearer(byte[] serializedRequest) throws ImpalaException{
    Preconditions.checkNotNull(frontend_);
    Preconditions.checkNotNull(frontend_.getSaml2Client());
    final TWrappedHttpRequest request = new TWrappedHttpRequest();
    // The responsee won't be used but it is needed to create a WebContext.
    final TWrappedHttpResponse dummyResponse = new TWrappedHttpResponse();
    JniUtil.deserializeThrift(protocolFactory_, request, serializedRequest);
    WrappedWebContext webContext = new WrappedWebContext(request, dummyResponse);
    return frontend_.getSaml2Client().validateBearer(webContext);
  }

  /**
   * Aborts a Kudu transaction.
   * @param queryId the id of the query.
   * @throws TransactionException, ImpalaException
   */
  public void abortKuduTransaction(byte[] thriftQueryId)
      throws TransactionException, ImpalaException {
    Preconditions.checkNotNull(frontend_);
    TUniqueId queryId = new TUniqueId();
    JniUtil.deserializeThrift(protocolFactory_, queryId, thriftQueryId);
    this.frontend_.abortKuduTransaction(queryId);
  }

  /**
   * Commits a Kudu transaction.
   * @param queryId the id of the query.
   * @throws TransactionException, ImpalaException
   */
  public void commitKuduTransaction(byte[] thriftQueryId)
      throws TransactionException, ImpalaException {
    Preconditions.checkNotNull(frontend_);
    TUniqueId queryId = new TUniqueId();
    JniUtil.deserializeThrift(protocolFactory_, queryId, thriftQueryId);
    this.frontend_.commitKuduTransaction(queryId);
  }

  /**
   * Returns an error string describing configuration issue with the groups mapping
   * provider implementation.
   */
  @VisibleForTesting
  protected static String checkGroupsMappingProvider(Configuration conf) {
    String provider = conf.get(CommonConfigurationKeys.HADOOP_SECURITY_GROUP_MAPPING);
    // Shell-based groups mapping providers fork a new process for each call.
    // This can cause issues such as zombie processes, running out of file descriptors,
    // etc.
    if (ShellBasedUnixGroupsNetgroupMapping.class.getName().equals(provider)) {
      return String.format("Hadoop groups mapping provider: %s is " +
          "known to be problematic. Consider using: %s instead.",
          provider, JniBasedUnixGroupsNetgroupMappingWithFallback.class.getName());
    }
    if (ShellBasedUnixGroupsMapping.class.getName().equals(provider)) {
      return String.format("Hadoop groups mapping provider: %s is " +
          "known to be problematic. Consider using: %s instead.",
          provider, JniBasedUnixGroupsMappingWithFallback.class.getName());
    }
    return "";
  }

  /**
   * Returns an error string describing all configuration issues. If no config issues are
   * found, returns an empty string.
   */
  public String checkConfiguration() throws ImpalaException {
    StringBuilder output = new StringBuilder();
    output.append(checkLogFilePermission());
    output.append(checkFileSystem(CONF));
    output.append(checkShortCircuitRead(CONF));
    if (BackendConfig.INSTANCE.isAuthorizedProxyGroupEnabled() &&
        !BackendConfig.INSTANCE.isShellBasedGroupsMappingEnabled()) {
      output.append(checkGroupsMappingProvider(CONF));
    }
    return output.toString();
  }

  /**
   * Returns an empty string if Impala has permission to write to FE log files. If not,
   * returns an error string describing the issues.
   */
  private String checkLogFilePermission() {
    org.apache.log4j.Logger l4jRootLogger = org.apache.log4j.Logger.getRootLogger();
    Enumeration appenders = l4jRootLogger.getAllAppenders();
    while (appenders.hasMoreElements()) {
      Appender appender = (Appender) appenders.nextElement();
      if (appender instanceof FileAppender) {
        if (((FileAppender) appender).getFile() == null) {
          // If Impala does not have permission to write to the log file, the
          // FileAppender will fail to initialize and logFile will be null.
          // Unfortunately, we can't get the log file name here.
          return "Impala does not have permission to write to the log file specified " +
              "in log4j.properties.";
        }
      }
    }
    return "";
  }

  /**
   * Returns an error message if short circuit reads are enabled but misconfigured.
   * Otherwise, returns an empty string,
   */
  @VisibleForTesting
  protected static String checkShortCircuitRead(Configuration conf) {
    if (!conf.getBoolean(HdfsClientConfigKeys.Read.ShortCircuit.KEY,
        HdfsClientConfigKeys.Read.ShortCircuit.DEFAULT)) {
      LOG.info("Short-circuit reads are not enabled.");
      return "";
    }

    StringBuilder output = new StringBuilder();
    String errorMessage = "Invalid short-circuit reads configuration:\n";
    String prefix = "  - ";
    StringBuilder errorCause = new StringBuilder();

    // dfs.domain.socket.path must be set properly
    String domainSocketPath =
        conf.getTrimmed(HdfsClientConfigKeys.DFS_DOMAIN_SOCKET_PATH_KEY,
            HdfsClientConfigKeys.DFS_DOMAIN_SOCKET_PATH_DEFAULT);
    if (domainSocketPath.isEmpty()) {
      errorCause.append(prefix);
      errorCause.append(HdfsClientConfigKeys.DFS_DOMAIN_SOCKET_PATH_KEY);
      errorCause.append(" is not configured.\n");
    } else {
      // The socket path parent directory must be readable and executable.
      File socketFile = new File(domainSocketPath);
      File socketDir = socketFile.getParentFile();
      if (BackendConfig.INSTANCE.isDedicatedCoordinator()) {
        LOG.warn("Dedicated coordinator instance will not read local data via "
            + "short-circuit reads, socket path directory checks are skipped.");
      } else if (socketDir == null || !socketDir.canRead() || !socketDir.canExecute()) {
        errorCause.append(prefix);
        errorCause.append("Impala cannot read or execute the parent directory of ");
        errorCause.append(HdfsClientConfigKeys.DFS_DOMAIN_SOCKET_PATH_KEY);
        errorCause.append("\n");
      }
    }

    // dfs.client.use.legacy.blockreader.local must be set to false
    if (conf.getBoolean(HdfsClientConfigKeys.DFS_CLIENT_USE_LEGACY_BLOCKREADERLOCAL,
        HdfsClientConfigKeys.DFS_CLIENT_USE_LEGACY_BLOCKREADERLOCAL_DEFAULT)) {
      errorCause.append(prefix);
      errorCause.append(HdfsClientConfigKeys.DFS_CLIENT_USE_LEGACY_BLOCKREADERLOCAL);
      errorCause.append(" should not be enabled.\n");
    }

    if (errorCause.length() > 0) {
      output.append(errorMessage);
      output.append(errorCause);
    }

    return output.toString();
  }

  private User getUser(TSessionState session) {
    // If the session was not set it indicates this is an internal Impala call.
    User user = session == null ?
        ImpalaInternalAdminUser.getInstance() :
        new User(TSessionStateUtil.getEffectiveUser(session));
    return user;
  }

  /**
   * Return an empty string if the default FileSystem configured in CONF refers to a
   * DistributedFileSystem and Impala can list the root directory "/". Otherwise,
   * return an error string describing the issues.
   */
  private String checkFileSystem(Configuration conf) {
    try {
      FileSystem fs = FileSystem.get(CONF);
      if (!FileSystemUtil.isValidDefaultFileSystem(fs)) {
        return "Currently configured default filesystem: " +
            fs.getClass().getSimpleName() + ". " +
            CommonConfigurationKeysPublic.FS_DEFAULT_NAME_KEY +
            " (" + CONF.get(CommonConfigurationKeysPublic.FS_DEFAULT_NAME_KEY) + ")" +
            " is not supported.";
      }
    } catch (IOException e) {
      return "couldn't retrieve FileSystem:\n" + e.getMessage();
    }

    String startup_filesystem_check_dirs =
        BackendConfig.INSTANCE.getStartupFilesystemCheckDirectories();
    String[] path_strings = startup_filesystem_check_dirs.split(",");
    for (String path_string : path_strings) {
      // Skip empty strings (or if there are no paths to check)
      if (path_string.isEmpty()) {
        continue;
      }
      try {
        LOG.info("Verifying access to " + path_string);
        Path path = new Path(path_string);
        FileSystem fs = path.getFileSystem(CONF);
        if (!fs.exists(path)) {
          return "Invalid path specified for startup_filesystem_check_directories: " +
              path_string + " does not exist.";
        }
        if (!fs.isDirectory(path)) {
          return "Invalid path specified for startup_filesystem_check_directories: " +
              path_string + " is not a directory.";
        }
        FileSystemUtil.getTotalNumVisibleFiles(path);
        LOG.info("Successfully listed " + path_string);
      } catch (IllegalArgumentException e) {
        return "Invalid path specified for startup_filesystem_check_directories: " +
            path_string + ". Error was: \n" + e.getMessage();
      } catch (IOException e) {
        return "Could not read the path at " + path_string +
            ". Error was: \n" + e.getMessage();
      }
    }
    return "";
  }

  public Frontend getFrontend() {
    return frontend_;
  }
}
