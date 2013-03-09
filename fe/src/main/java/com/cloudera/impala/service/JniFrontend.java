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

import java.io.File;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.thrift.TBase;
import org.apache.thrift.TDeserializer;
import org.apache.thrift.TException;
import org.apache.thrift.TSerializer;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.cloudera.impala.catalog.FileFormat;
import com.cloudera.impala.catalog.RowFormat;
import com.cloudera.impala.common.ImpalaException;
import com.cloudera.impala.common.InternalException;
import com.cloudera.impala.thrift.TCatalogUpdate;
import com.cloudera.impala.thrift.TClientRequest;
import com.cloudera.impala.thrift.TCreateDbParams;
import com.cloudera.impala.thrift.TCreateTableParams;
import com.cloudera.impala.thrift.TColumnDesc;
import com.cloudera.impala.thrift.TDescribeTableParams;
import com.cloudera.impala.thrift.TDescribeTableResult;
import com.cloudera.impala.thrift.TDropDbParams;
import com.cloudera.impala.thrift.TDropTableParams;
import com.cloudera.impala.thrift.TExecRequest;
import com.cloudera.impala.thrift.TGetDbsParams;
import com.cloudera.impala.thrift.TGetDbsResult;
import com.cloudera.impala.thrift.TGetTablesParams;
import com.cloudera.impala.thrift.TGetTablesResult;
import com.cloudera.impala.thrift.TMetadataOpRequest;
import com.cloudera.impala.thrift.TMetadataOpResponse;

import com.google.common.collect.Lists;

import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.AlreadyExistsException;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.NoSuchObjectException;
import org.apache.hadoop.hive.metastore.api.InvalidObjectException;
import org.apache.hadoop.hive.metastore.api.InvalidOperationException;

/**
 * JNI-callable interface onto a wrapped Frontend instance. The main point is to serialise
 * and deserialise thrift structures between C and Java.
 */
public class JniFrontend {
  private final static Logger LOG = LoggerFactory.getLogger(JniFrontend.class);

  private final static TBinaryProtocol.Factory protocolFactory =
      new TBinaryProtocol.Factory();

  private final Frontend frontend;

  public JniFrontend() {
    frontend = new Frontend();
  }

  public JniFrontend(boolean lazy) {
    frontend = new Frontend(lazy);
  }

  /**
   * Deserialized a serialized form of a Thrift data structure to its object form
   */
  private <T extends TBase> void deserializeThrift(T result, byte[] thriftData)
      throws ImpalaException {
    // TODO: avoid creating deserializer for each query?
    TDeserializer deserializer = new TDeserializer(protocolFactory);

    try {
      deserializer.deserialize(result, thriftData);
    } catch (TException e) {
      throw new InternalException(e.getMessage());
    }
  }

  /**
   * Jni wrapper for Frontend.createQueryExecRequest2(). Accepts a serialized
   * TClientRequest; returns a serialized TQueryExecRequest2.
   */
  public byte[] createExecRequest(byte[] thriftClientRequest)
      throws ImpalaException {
    TClientRequest request = new TClientRequest();
    deserializeThrift(request, thriftClientRequest);

    StringBuilder explainString = new StringBuilder();
    TExecRequest result = frontend.createExecRequest(request, explainString);
    LOG.info(explainString.toString());

    //LOG.info("returned TQueryExecRequest2: " + result.toString());
    // TODO: avoid creating serializer for each query?
    TSerializer serializer = new TSerializer(protocolFactory);
    try {
      return serializer.serialize(result);
    } catch (TException e) {
      throw new InternalException(e.getMessage());
    }
  }

  public void createDatabase(byte[] thriftCreateDbParams)
      throws ImpalaException, MetaException, org.apache.thrift.TException,
      AlreadyExistsException,InvalidObjectException {
    TCreateDbParams params = new TCreateDbParams();
    deserializeThrift(params, thriftCreateDbParams);
    frontend.createDatabase(params.getDb(), params.getComment(), params.getLocation(),
        params.isIf_not_exists());
  }

  public void createTable(byte[] thriftCreateTableParams)
      throws ImpalaException, MetaException, NoSuchObjectException,
      org.apache.thrift.TException, AlreadyExistsException,
      InvalidObjectException {
    TCreateTableParams params = new TCreateTableParams();
    deserializeThrift(params, thriftCreateTableParams);
    frontend.createTable(params.getDb(), params.getTable_name(), params.getColumns(),
        params.isIs_external(), params.getComment(),
        new RowFormat(params.getField_terminator(), params.getLine_terminator()),
        FileFormat.fromThrift(params.getFile_format()), params.getLocation(),
        params.isIf_not_exists());
  }

  public void dropDatabase(byte[] thriftDropDbParams)
      throws ImpalaException, MetaException, NoSuchObjectException,
      org.apache.thrift.TException, AlreadyExistsException, InvalidOperationException,
      InvalidObjectException {
    TDropDbParams params = new TDropDbParams();
    deserializeThrift(params, thriftDropDbParams);
    frontend.dropDatabase(params.getDb(), params.isIf_exists());
  }

  public void dropTable(byte[] thriftDropTableParams)
      throws ImpalaException, MetaException, NoSuchObjectException,
      org.apache.thrift.TException, AlreadyExistsException, InvalidOperationException,
      InvalidObjectException {
    TDropTableParams params = new TDropTableParams();
    deserializeThrift(params, thriftDropTableParams);
    frontend.dropTable(params.getDb(), params.getTable_name(), params.isIf_exists());
  }

  /**
   * Return an explain plan based on thriftQueryRequest, a serialized TQueryRequest.
   * This call is thread-safe.
   */
  public String getExplainPlan(byte[] thriftQueryRequest) throws ImpalaException {
    TClientRequest request = new TClientRequest();
    deserializeThrift(request, thriftQueryRequest);
    String plan = frontend.getExplainString(request);
    LOG.info("Explain plan: " + plan);
    return plan;
  }

  /**
   * Process any updates to the metastore required after a query executes.
   * The argument is a serialized TCatalogUpdate.
   * @see Frontend#updateMetastore
   */
  public void updateMetastore(byte[] thriftCatalogUpdate) throws ImpalaException {
    TCatalogUpdate update = new TCatalogUpdate();
    deserializeThrift(update, thriftCatalogUpdate);
    frontend.updateMetastore(update);
  }

  /**
   * Returns a list of table names matching an optional pattern.
   * The argument is a serialized TGetTablesParams object.
   * The return type is a serialised TGetTablesResult object.
   * @see Frontend#getTableNames
   */
  public byte[] getTableNames(byte[] thriftGetTablesParams) throws ImpalaException {
    TGetTablesParams params = new TGetTablesParams();
    deserializeThrift(params, thriftGetTablesParams);
    List<String> tables = frontend.getTableNames(params.db, params.pattern);

    TGetTablesResult result = new TGetTablesResult();
    result.setTables(tables);

    TSerializer serializer = new TSerializer(protocolFactory);
    try {
      return serializer.serialize(result);
    } catch (TException e) {
      throw new InternalException(e.getMessage());
    }
  }

  /**
   * Returns a list of table names matching an optional pattern.
   * The argument is a serialized TGetTablesParams object.
   * The return type is a serialised TGetTablesResult object.
   * @see Frontend#getTableNames
   */
  public byte[] getDbNames(byte[] thriftGetTablesParams) throws ImpalaException {
    TGetDbsParams params = new TGetDbsParams();
    deserializeThrift(params, thriftGetTablesParams);
    List<String> dbs = frontend.getDbNames(params.pattern);

    TGetDbsResult result = new TGetDbsResult();
    result.setDbs(dbs);

    TSerializer serializer = new TSerializer(protocolFactory);
    try {
      return serializer.serialize(result);
    } catch (TException e) {
      throw new InternalException(e.getMessage());
    }
  }

  /**
   * Returns a list of the columns making up a table.
   * The argument is a serialized TDescribeTableParams object.
   * The return type is a serialised TDescribeTableResult object.
   * @see Frontend#describeTable
   */
  public byte[] describeTable(byte[] thriftDescribeTableParams) throws ImpalaException {
    TDescribeTableParams params = new TDescribeTableParams();
    deserializeThrift(params, thriftDescribeTableParams);
    TDescribeTableResult result = new TDescribeTableResult();
    result.setColumns(frontend.describeTable(params.getDb(), params.getTable_name()));

    TSerializer serializer = new TSerializer(protocolFactory);
    try {
      return serializer.serialize(result);
    } catch (TException e) {
      throw new InternalException(e.getMessage());
    }
  }

  /**
   * Executes a HiveServer2 metadata operation and returns a TMetadataOpResponse
   */
  public byte[] execHiveServer2MetadataOp(byte[] metadataOpsParams)
      throws ImpalaException {
    TMetadataOpRequest params = new TMetadataOpRequest();
    deserializeThrift(params, metadataOpsParams);
    TMetadataOpResponse result = frontend.execHiveServer2MetadataOp(params);

    TSerializer serializer = new TSerializer(protocolFactory);
    try {
      return serializer.serialize(result);
    } catch (TException e) {
      throw new InternalException(e.getMessage());
    }
  }

  // Caching this saves ~50ms per call to getHadoopConfigAsHtml
  private static final Configuration CONF = new Configuration();

  /**
   * Returns a string of all loaded Hadoop configuration parameters as a table of keys
   * and values.
   */
  public String getHadoopConfigAsHtml() {
    StringBuilder output = new StringBuilder();
    // Write the set of files that make up the configuration
    output.append(CONF.toString());
    output.append("\n\n");

    // Write a table of key, value pairs
    output.append("<table class='table table-bordered table-hover'>");
    output.append("<tr><th>Key</th><th>Value</th></tr>");
    for (Map.Entry<String, String> e : CONF) {
      output.append("<tr><td>" + e.getKey() + "</td><td>" + e.getValue() + "</td></tr>");
    }
    output.append("</table>");
    return output.toString();
  }

  /**
   * Returns a string representation of a config value. If the config
   * can't be found, the empty string is returned. (This method is
   * called from JNI, and so NULL handling is easier to avoid.)
   */
  public String getHadoopConfigValue(String confName) {
    return CONF.get(confName, "");
  }

  /**
   * Returns an error string describing all configuration issues. If no config issues are
   * found, returns an empty string.
   * @return
   */
  public String checkHadoopConfig() {
    StringBuilder output = new StringBuilder();

    output.append(checkShortCircuitRead(CONF));
    output.append(checkBlockLocationTracking(CONF));

    return output.toString();
  }

  /**
   * Return an empty string if short circuit read is properly enabled. If not, return an
   * error string describing the issues.
   */
  private String checkShortCircuitRead(Configuration conf) {
    StringBuilder output = new StringBuilder();
    String errorMessage = "ERROR: short-circuit local reads is disabled because\n";
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

    // dfs.client.read.shortcircuit must be set to true.
    if (!conf.getBoolean(DFSConfigKeys.DFS_CLIENT_READ_SHORTCIRCUIT_KEY,
        DFSConfigKeys.DFS_CLIENT_READ_SHORTCIRCUIT_DEFAULT)) {
      errorCause.append(prefix);
      errorCause.append(DFSConfigKeys.DFS_CLIENT_READ_SHORTCIRCUIT_KEY);
      errorCause.append(" is not enabled.\n");
    }

    if (errorCause.length() > 0) {
      output.append(errorMessage);
      output.append(errorCause);
    }

    return output.toString();
  }

  /**
   * Return an empty string if block location tracking is properly enabled. If not,
   * return an error string describing the issues.
   */
  private String checkBlockLocationTracking(Configuration conf) {
    if (!conf.getBoolean(DFSConfigKeys.DFS_HDFS_BLOCKS_METADATA_ENABLED,
        DFSConfigKeys.DFS_HDFS_BLOCKS_METADATA_ENABLED_DEFAULT)) {
      return "ERROR: block location tracking is disabled because " +
          DFSConfigKeys.DFS_HDFS_BLOCKS_METADATA_ENABLED + " is not enabled.\n";
    }
    return "";
  }

  public void resetCatalog() {
    frontend.resetCatalog();
  }
}
