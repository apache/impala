// Copyright (c) 2012 Cloudera, Inc. All rights reserved.

package com.cloudera.impala.service;

import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.thrift.TBase;
import org.apache.thrift.TDeserializer;
import org.apache.thrift.TException;
import org.apache.thrift.TSerializer;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.cloudera.impala.common.ImpalaException;
import com.cloudera.impala.common.InternalException;
import com.cloudera.impala.thrift.TCatalogUpdate;
import com.cloudera.impala.thrift.TClientRequest;
import com.cloudera.impala.thrift.TDescribeTableParams;
import com.cloudera.impala.thrift.TDescribeTableResult;
import com.cloudera.impala.thrift.TExecRequest;
import com.cloudera.impala.thrift.TGetTablesParams;
import com.cloudera.impala.thrift.TGetTablesResult;

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
   * Create a TExecRequest as well as TResultSetMetadata for a given
   * serialized TClientRequest. The result is returned as a serialized
   * TExecRequest.
   * This call is thread-safe.
   */
  public byte[] createExecRequest(byte[] thriftQueryRequest) throws ImpalaException {
    TClientRequest request = new TClientRequest();
    deserializeThrift(request, thriftQueryRequest);

    // process front end
    StringBuilder explainString = new StringBuilder();
    TExecRequest result =
        frontend.createExecRequest(request, explainString);

    // Print explain string.
    LOG.info(explainString.toString());

    // TODO: avoid creating serializer for each query?
    TSerializer serializer = new TSerializer(protocolFactory);
    try {
      return serializer.serialize(result);
    } catch (TException e) {
      throw new InternalException(e.getMessage());
    }
  }

  /**
   * Jni wrapper for Frontend.createQueryExecRequest2(). Accepts a serialized
   * TClientRequest; returns a serialized TQueryExecRequest2.
   */
  public byte[] createExecRequest2(byte[] thriftClientRequest)
      throws ImpalaException {
    TClientRequest request = new TClientRequest();
    deserializeThrift(request, thriftClientRequest);

    StringBuilder explainString = new StringBuilder();
    TExecRequest result = frontend.createExecRequest2(request, explainString);
    LOG.info(explainString.toString());

    LOG.info("returned TQueryExecRequest2: " + result.toString());
    // TODO: avoid creating serializer for each query?
    TSerializer serializer = new TSerializer(protocolFactory);
    try {
      return serializer.serialize(result);
    } catch (TException e) {
      throw new InternalException(e.getMessage());
    }
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
    output.append("<table><tr><th>Key</th><th>Value</th></tr>");
    for (Map.Entry<String, String> e : CONF) {
      output.append("<tr><td>" + e.getKey() + "</td><td>" + e.getValue() + "</td</tr>");
    }
    output.append("</table>");
    return output.toString();
  }

  public void resetCatalog() {
    frontend.resetCatalog();
  }
}
