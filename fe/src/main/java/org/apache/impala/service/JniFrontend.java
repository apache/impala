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

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Enumeration;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.s3a.S3AFileSystem;
import org.apache.hadoop.fs.adl.AdlFileSystem;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.impala.analysis.DescriptorTable;
import org.apache.impala.analysis.ToSqlUtils;
import org.apache.impala.authorization.AuthorizationConfig;
import org.apache.impala.authorization.ImpalaInternalAdminUser;
import org.apache.impala.authorization.User;
import org.apache.impala.catalog.DataSource;
import org.apache.impala.catalog.Db;
import org.apache.impala.catalog.Function;
import org.apache.impala.catalog.Role;
import org.apache.impala.catalog.StructType;
import org.apache.impala.catalog.Type;
import org.apache.impala.common.FileSystemUtil;
import org.apache.impala.common.ImpalaException;
import org.apache.impala.common.InternalException;
import org.apache.impala.common.JniUtil;
import org.apache.impala.thrift.TBackendGflags;
import org.apache.impala.thrift.TBuildTestDescriptorTableParams;
import org.apache.impala.thrift.TCatalogObject;
import org.apache.impala.thrift.TDatabase;
import org.apache.impala.thrift.TDescribeDbParams;
import org.apache.impala.thrift.TDescribeOutputStyle;
import org.apache.impala.thrift.TDescribeResult;
import org.apache.impala.thrift.TDescribeTableParams;
import org.apache.impala.thrift.TDescriptorTable;
import org.apache.impala.thrift.TExecRequest;
import org.apache.impala.thrift.TFunctionCategory;
import org.apache.impala.thrift.TGetAllHadoopConfigsResponse;
import org.apache.impala.thrift.TGetDataSrcsParams;
import org.apache.impala.thrift.TGetDataSrcsResult;
import org.apache.impala.thrift.TGetDbsParams;
import org.apache.impala.thrift.TGetDbsResult;
import org.apache.impala.thrift.TGetFunctionsParams;
import org.apache.impala.thrift.TGetFunctionsResult;
import org.apache.impala.thrift.TGetHadoopConfigRequest;
import org.apache.impala.thrift.TGetHadoopConfigResponse;
import org.apache.impala.thrift.TGetTablesParams;
import org.apache.impala.thrift.TGetTablesResult;
import org.apache.impala.thrift.TLoadDataReq;
import org.apache.impala.thrift.TLoadDataResp;
import org.apache.impala.thrift.TLogLevel;
import org.apache.impala.thrift.TMetadataOpRequest;
import org.apache.impala.thrift.TQueryCtx;
import org.apache.impala.thrift.TResultSet;
import org.apache.impala.thrift.TShowFilesParams;
import org.apache.impala.thrift.TShowGrantRoleParams;
import org.apache.impala.thrift.TShowRolesParams;
import org.apache.impala.thrift.TShowRolesResult;
import org.apache.impala.thrift.TShowStatsOp;
import org.apache.impala.thrift.TShowStatsParams;
import org.apache.impala.thrift.TTableName;
import org.apache.impala.thrift.TUniqueId;
import org.apache.impala.thrift.TUpdateCatalogCacheRequest;
import org.apache.impala.thrift.TUpdateMembershipRequest;
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

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

/**
 * JNI-callable interface onto a wrapped Frontend instance. The main point is to serialise
 * and deserialise thrift structures between C and Java.
 */
public class JniFrontend {
  private final static Logger LOG = LoggerFactory.getLogger(JniFrontend.class);
  private final static TBinaryProtocol.Factory protocolFactory_ =
      new TBinaryProtocol.Factory();
  private final Frontend frontend_;

  /**
   * Create a new instance of the Jni Frontend.
   */
  public JniFrontend(byte[] thriftBackendConfig) throws ImpalaException, TException {
    TBackendGflags cfg = new TBackendGflags();
    JniUtil.deserializeThrift(protocolFactory_, cfg, thriftBackendConfig);

    BackendConfig.create(cfg);

    GlogAppender.Install(TLogLevel.values()[cfg.impala_log_lvl],
        TLogLevel.values()[cfg.non_impala_java_vlog]);

    // Validate the authorization configuration before initializing the Frontend.
    // If there are any configuration problems Impala startup will fail.
    AuthorizationConfig authConfig = new AuthorizationConfig(cfg.server_name,
        cfg.authorization_policy_file, cfg.sentry_config,
        cfg.authorization_policy_provider_class);
    authConfig.validateConfig();
    if (authConfig.isEnabled()) {
      LOG.info(String.format("Authorization is 'ENABLED' using %s",
          authConfig.isFileBasedPolicy() ? " file based policy from: " +
          authConfig.getPolicyFile() : " using Sentry Policy Service."));
    } else {
      LOG.info("Authorization is 'DISABLED'.");
    }
    LOG.info(JniUtil.getJavaVersion());

    frontend_ = new Frontend(authConfig, cfg.kudu_master_hosts);
  }

  /**
   * Jni wrapper for Frontend.createExecRequest(). Accepts a serialized
   * TQueryContext; returns a serialized TQueryExecRequest.
   */
  public byte[] createExecRequest(byte[] thriftQueryContext)
      throws ImpalaException {
    TQueryCtx queryCtx = new TQueryCtx();
    JniUtil.deserializeThrift(protocolFactory_, queryCtx, thriftQueryContext);

    StringBuilder explainString = new StringBuilder();
    TExecRequest result = frontend_.createExecRequest(queryCtx, explainString);
    if (explainString.length() > 0 && LOG.isTraceEnabled()) {
      LOG.trace(explainString.toString());
    }

    // TODO: avoid creating serializer for each query?
    TSerializer serializer = new TSerializer(protocolFactory_);
    try {
      return serializer.serialize(result);
    } catch (TException e) {
      throw new InternalException(e.getMessage());
    }
  }

  // Deserialize and merge each thrift catalog update into a single merged update
  public byte[] updateCatalogCache(byte[][] thriftCatalogUpdates) throws ImpalaException {
    TUniqueId defaultCatalogServiceId = new TUniqueId(0L, 0L);
    TUpdateCatalogCacheRequest mergedUpdateRequest = new TUpdateCatalogCacheRequest(
        false, defaultCatalogServiceId, new ArrayList<TCatalogObject>(),
        new ArrayList<TCatalogObject>());
    for (byte[] catalogUpdate: thriftCatalogUpdates) {
      TUpdateCatalogCacheRequest incrementalRequest = new TUpdateCatalogCacheRequest();
      JniUtil.deserializeThrift(protocolFactory_, incrementalRequest, catalogUpdate);
      mergedUpdateRequest.is_delta |= incrementalRequest.is_delta;
      if (!incrementalRequest.getCatalog_service_id().equals(defaultCatalogServiceId)) {
        mergedUpdateRequest.setCatalog_service_id(
            incrementalRequest.getCatalog_service_id());
      }
      mergedUpdateRequest.getUpdated_objects().addAll(
          incrementalRequest.getUpdated_objects());
      mergedUpdateRequest.getRemoved_objects().addAll(
          incrementalRequest.getRemoved_objects());
    }
    TSerializer serializer = new TSerializer(protocolFactory_);
    try {
      return serializer.serialize(frontend_.updateCatalogCache(mergedUpdateRequest));
    } catch (TException e) {
      throw new InternalException(e.getMessage());
    }
  }

  /**
   * Jni wrapper for Frontend.updateMembership(). Accepts a serialized
   * TUpdateMembershipRequest.
   */
  public void updateMembership(byte[] thriftMembershipUpdate) throws ImpalaException {
    TUpdateMembershipRequest req = new TUpdateMembershipRequest();
    JniUtil.deserializeThrift(protocolFactory_, req, thriftMembershipUpdate);
    frontend_.updateMembership(req);
  }

  /**
   * Loads a table or partition with one or more data files. If the "overwrite" flag
   * in the request is true, all existing data in the table/partition will be replaced.
   * If the "overwrite" flag is false, the files will be added alongside any existing
   * data files.
   */
  public byte[] loadTableData(byte[] thriftLoadTableDataParams)
      throws ImpalaException, IOException {
    TLoadDataReq request = new TLoadDataReq();
    JniUtil.deserializeThrift(protocolFactory_, request, thriftLoadTableDataParams);
    TLoadDataResp response = frontend_.loadTableData(request);
    TSerializer serializer = new TSerializer(protocolFactory_);
    try {
      return serializer.serialize(response);
    } catch (TException e) {
      throw new InternalException(e.getMessage());
    }
  }

  /**
   * Return an explain plan based on thriftQueryContext, a serialized TQueryContext.
   * This call is thread-safe.
   */
  public String getExplainPlan(byte[] thriftQueryContext) throws ImpalaException {
    TQueryCtx queryCtx = new TQueryCtx();
    JniUtil.deserializeThrift(protocolFactory_, queryCtx, thriftQueryContext);
    String plan = frontend_.getExplainString(queryCtx);
    if (LOG.isTraceEnabled()) LOG.trace("Explain plan: " + plan);
    return plan;
  }

  /**
   * Implement Hive's pattern-matching semantics for "SHOW TABLE [[LIKE] 'pattern']", and
   * return a list of table names matching an optional pattern.
   * The only metacharacters are '*' which matches any string of characters, and '|'
   * which denotes choice.  Doing the work here saves loading tables or databases from the
   * metastore (which Hive would do if we passed the call through to the metastore
   * client). If the pattern is null, all strings are considered to match. If it is an
   * empty string, no strings match.
   *
   * The argument is a serialized TGetTablesParams object.
   * The return type is a serialised TGetTablesResult object.
   * @see Frontend#getTableNames
   */
  public byte[] getTableNames(byte[] thriftGetTablesParams) throws ImpalaException {
    TGetTablesParams params = new TGetTablesParams();
    JniUtil.deserializeThrift(protocolFactory_, params, thriftGetTablesParams);
    // If the session was not set it indicates this is an internal Impala call.
    User user = params.isSetSession() ?
        new User(TSessionStateUtil.getEffectiveUser(params.getSession())) :
        ImpalaInternalAdminUser.getInstance();

    Preconditions.checkState(!params.isSetSession() || user != null );
    List<String> tables = frontend_.getTableNames(params.db,
        PatternMatcher.createHivePatternMatcher(params.pattern), user);

    TGetTablesResult result = new TGetTablesResult();
    result.setTables(tables);

    TSerializer serializer = new TSerializer(protocolFactory_);
    try {
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
    TShowFilesParams params = new TShowFilesParams();
    JniUtil.deserializeThrift(protocolFactory_, params, thriftShowFilesParams);
    TResultSet result = frontend_.getTableFiles(params);

    TSerializer serializer = new TSerializer(protocolFactory_);
    try {
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
    TGetDbsParams params = new TGetDbsParams();
    JniUtil.deserializeThrift(protocolFactory_, params, thriftGetTablesParams);
    // If the session was not set it indicates this is an internal Impala call.
    User user = params.isSetSession() ?
        new User(TSessionStateUtil.getEffectiveUser(params.getSession())) :
        ImpalaInternalAdminUser.getInstance();
    List<Db> dbs = frontend_.getDbs(
        PatternMatcher.createHivePatternMatcher(params.pattern), user);
    TGetDbsResult result = new TGetDbsResult();
    List<TDatabase> tDbs = Lists.newArrayListWithCapacity(dbs.size());
    for (Db db: dbs) tDbs.add(db.toThrift());
    result.setDbs(tDbs);
    TSerializer serializer = new TSerializer(protocolFactory_);
    try {
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
    TGetDataSrcsParams params = new TGetDataSrcsParams();
    JniUtil.deserializeThrift(protocolFactory_, params, thriftParams);

    TGetDataSrcsResult result = new TGetDataSrcsResult();
    List<DataSource> dataSources = frontend_.getDataSrcs(params.pattern);
    result.setData_src_names(Lists.<String>newArrayListWithCapacity(dataSources.size()));
    result.setLocations(Lists.<String>newArrayListWithCapacity(dataSources.size()));
    result.setClass_names(Lists.<String>newArrayListWithCapacity(dataSources.size()));
    result.setApi_versions(Lists.<String>newArrayListWithCapacity(dataSources.size()));
    for (DataSource dataSource: dataSources) {
      result.addToData_src_names(dataSource.getName());
      result.addToLocations(dataSource.getLocation());
      result.addToClass_names(dataSource.getClassName());
      result.addToApi_versions(dataSource.getApiVersion());
    }
    TSerializer serializer = new TSerializer(protocolFactory_);
    try {
      return serializer.serialize(result);
    } catch (TException e) {
      throw new InternalException(e.getMessage());
    }
  }

  public byte[] getStats(byte[] thriftShowStatsParams) throws ImpalaException {
    TShowStatsParams params = new TShowStatsParams();
    JniUtil.deserializeThrift(protocolFactory_, params, thriftShowStatsParams);
    Preconditions.checkState(params.isSetTable_name());
    TResultSet result;

    if (params.op == TShowStatsOp.COLUMN_STATS) {
      result = frontend_.getColumnStats(params.getTable_name().getDb_name(),
          params.getTable_name().getTable_name());
    } else {
      result = frontend_.getTableStats(params.getTable_name().getDb_name(),
          params.getTable_name().getTable_name(), params.op);
    }
    TSerializer serializer = new TSerializer(protocolFactory_);
    try {
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
    TSerializer serializer = new TSerializer(protocolFactory_);
    try {
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
    TDescribeDbParams params = new TDescribeDbParams();
    JniUtil.deserializeThrift(protocolFactory_, params, thriftDescribeDbParams);

    TDescribeResult result = frontend_.describeDb(
        params.getDb(), params.getOutput_style());

    TSerializer serializer = new TSerializer(protocolFactory_);
    try {
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
    TDescribeTableParams params = new TDescribeTableParams();
    JniUtil.deserializeThrift(protocolFactory_, params, thriftDescribeTableParams);

    Preconditions.checkState(params.isSetTable_name() ^ params.isSetResult_struct());
    TDescribeResult result = null;
    if (params.isSetTable_name()) {
      result = frontend_.describeTable(params.getTable_name(), params.output_style);
    } else {
      Preconditions.checkState(params.output_style == TDescribeOutputStyle.MINIMAL);
      StructType structType = (StructType)Type.fromThrift(params.result_struct);
      result = DescribeResultFactory.buildDescribeMinimalResult(structType);
    }

    TSerializer serializer = new TSerializer(protocolFactory_);
    try {
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
    TSerializer serializer = new TSerializer(protocolFactory_);
    try {
      byte[] ret = serializer.serialize(result);
      return ret;
    } catch (TException e) {
      throw new InternalException(e.getMessage());
    }
  }

  /**
   * Gets all roles
   */
  public byte[] getRoles(byte[] showRolesParams) throws ImpalaException {
    TShowRolesParams params = new TShowRolesParams();
    JniUtil.deserializeThrift(protocolFactory_, params, showRolesParams);
    TShowRolesResult result = new TShowRolesResult();

    List<Role> roles = Lists.newArrayList();
    if (params.isIs_show_current_roles() || params.isSetGrant_group()) {
      User user = new User(params.getRequesting_user());
      Set<String> groupNames;
      if (params.isIs_show_current_roles()) {
        groupNames = frontend_.getAuthzChecker().getUserGroups(user);
      } else {
        Preconditions.checkState(params.isSetGrant_group());
        groupNames = Sets.newHashSet(params.getGrant_group());
      }
      for (String groupName: groupNames) {
        roles.addAll(frontend_.getCatalog().getAuthPolicy().getGrantedRoles(groupName));
      }
    } else {
      Preconditions.checkState(!params.isIs_show_current_roles());
      roles = frontend_.getCatalog().getAuthPolicy().getAllRoles();
    }

    result.setRole_names(Lists.<String>newArrayListWithExpectedSize(roles.size()));
    for (Role role: roles) {
      result.getRole_names().add(role.getName());
    }

    Collections.sort(result.getRole_names());
    TSerializer serializer = new TSerializer(protocolFactory_);
    try {
      return serializer.serialize(result);
    } catch (TException e) {
      throw new InternalException(e.getMessage());
    }
  }

  public byte[] getRolePrivileges(byte[] showGrantRolesParams) throws ImpalaException {
    TShowGrantRoleParams params = new TShowGrantRoleParams();
    JniUtil.deserializeThrift(protocolFactory_, params, showGrantRolesParams);
    TResultSet result = frontend_.getCatalog().getAuthPolicy().getRolePrivileges(
        params.getRole_name(), params.getPrivilege());
    TSerializer serializer = new TSerializer(protocolFactory_);
    try {
      return serializer.serialize(result);
    } catch (TException e) {
      throw new InternalException(e.getMessage());
    }
  }

  /**
   * Executes a HiveServer2 metadata operation and returns a TResultSet
   */
  public byte[] execHiveServer2MetadataOp(byte[] metadataOpsParams)
      throws ImpalaException {
    TMetadataOpRequest params = new TMetadataOpRequest();
    JniUtil.deserializeThrift(protocolFactory_, params, metadataOpsParams);
    TResultSet result = frontend_.execHiveServer2MetadataOp(params);

    TSerializer serializer = new TSerializer(protocolFactory_);
    try {
      return serializer.serialize(result);
    } catch (TException e) {
      throw new InternalException(e.getMessage());
    }
  }

  public void setCatalogInitialized() {
    frontend_.getCatalog().setIsReady(true);
  }

  // Caching this saves ~50ms per call to getHadoopConfigAsHtml
  private static final Configuration CONF = new Configuration();

  /**
   * Returns a string of all loaded Hadoop configuration parameters as a table of keys
   * and values. If asText is true, output in raw text. Otherwise, output in html.
   */
  public byte[] getAllHadoopConfigs() throws ImpalaException {
    Map<String, String> configs = Maps.newHashMap();
    for (Map.Entry<String, String> e: CONF) {
      configs.put(e.getKey(), e.getValue());
    }
    TGetAllHadoopConfigsResponse result = new TGetAllHadoopConfigsResponse();
    result.setConfigs(configs);
    TSerializer serializer = new TSerializer(protocolFactory_);
    try {
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
    TSerializer serializer = new TSerializer(protocolFactory_);
    try {
      return serializer.serialize(result);
    } catch (TException e) {
      throw new InternalException(e.getMessage());
    }
  }

  /**
   * Returns an error string describing all configuration issues. If no config issues are
   * found, returns an empty string.
   */
  public String checkConfiguration() {
    StringBuilder output = new StringBuilder();
    output.append(checkLogFilePermission());
    output.append(checkFileSystem(CONF));
    output.append(checkShortCircuitRead(CONF));
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
  private String checkShortCircuitRead(Configuration conf) {
    if (!conf.getBoolean(DFSConfigKeys.DFS_CLIENT_READ_SHORTCIRCUIT_KEY,
        DFSConfigKeys.DFS_CLIENT_READ_SHORTCIRCUIT_DEFAULT)) {
      LOG.info("Short-circuit reads are not enabled.");
      return "";
    }

    StringBuilder output = new StringBuilder();
    String errorMessage = "Invalid short-circuit reads configuration:\n";
    String prefix = "  - ";
    StringBuilder errorCause = new StringBuilder();

    // dfs.domain.socket.path must be set properly
    String domainSocketPath = conf.getTrimmed(DFSConfigKeys.DFS_DOMAIN_SOCKET_PATH_KEY,
        DFSConfigKeys.DFS_DOMAIN_SOCKET_PATH_DEFAULT);
    if (domainSocketPath.isEmpty()) {
      errorCause.append(prefix);
      errorCause.append(DFSConfigKeys.DFS_DOMAIN_SOCKET_PATH_KEY);
      errorCause.append(" is not configured.\n");
    } else {
      // The socket path parent directory must be readable and executable.
      File socketFile = new File(domainSocketPath);
      File socketDir = socketFile.getParentFile();
      if (socketDir == null || !socketDir.canRead() || !socketDir.canExecute()) {
        errorCause.append(prefix);
        errorCause.append("Impala cannot read or execute the parent directory of ");
        errorCause.append(DFSConfigKeys.DFS_DOMAIN_SOCKET_PATH_KEY);
        errorCause.append("\n");
      }
    }

    // dfs.client.use.legacy.blockreader.local must be set to false
    if (conf.getBoolean(DFSConfigKeys.DFS_CLIENT_USE_LEGACY_BLOCKREADERLOCAL,
        DFSConfigKeys.DFS_CLIENT_USE_LEGACY_BLOCKREADERLOCAL_DEFAULT)) {
      errorCause.append(prefix);
      errorCause.append(DFSConfigKeys.DFS_CLIENT_USE_LEGACY_BLOCKREADERLOCAL);
      errorCause.append(" should not be enabled.\n");
    }

    if (errorCause.length() > 0) {
      output.append(errorMessage);
      output.append(errorCause);
    }

    return output.toString();
  }

  /**
   * Return an empty string if the default FileSystem configured in CONF refers to a
   * DistributedFileSystem and Impala can list the root directory "/". Otherwise,
   * return an error string describing the issues.
   */
  private String checkFileSystem(Configuration conf) {
    try {
      FileSystem fs = FileSystem.get(CONF);
      if (!(fs instanceof DistributedFileSystem ||
            fs instanceof S3AFileSystem ||
            fs instanceof AdlFileSystem)) {
        return "Currently configured default filesystem: " +
            fs.getClass().getSimpleName() + ". " +
            CommonConfigurationKeysPublic.FS_DEFAULT_NAME_KEY +
            " (" + CONF.get(CommonConfigurationKeysPublic.FS_DEFAULT_NAME_KEY) + ")" +
            " is not supported.";
      }
    } catch (IOException e) {
      return "couldn't retrieve FileSystem:\n" + e.getMessage();
    }

    try {
      FileSystemUtil.getTotalNumVisibleFiles(new Path("/"));
    } catch (IOException e) {
      return "Could not read the root directory at " +
          CONF.get(CommonConfigurationKeysPublic.FS_DEFAULT_NAME_KEY) +
          ". Error was: \n" + e.getMessage();
    }
    return "";
  }
}
