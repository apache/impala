// Copyright (c) 2012 Cloudera, Inc. All rights reserved.

package com.cloudera.impala.service;

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
import com.cloudera.impala.thrift.TCreateQueryExecRequestResult;
import com.cloudera.impala.thrift.TQueryRequest;

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
   * Create a TQueryExecRequest as well as TResultSetMetadata for a given
   * serialized TQueryRequest. The result is returned as a serialized
   * TCreateQueryExecRequestResult.
   * This call is thread-safe.
   */
  public byte[] createQueryExecRequest(byte[] thriftQueryRequest) throws ImpalaException {
    TQueryRequest request = new TQueryRequest();
    deserializeThrift(request, thriftQueryRequest);

    // process front end
    StringBuilder explainString = new StringBuilder();
    TCreateQueryExecRequestResult result =
        frontend.createQueryExecRequest(request, explainString);

    // Print explain string.
    LOG.info(explainString.toString());

    LOG.info("returned TCreateQueryExecRequestResult: " + result.toString());
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
    TQueryRequest request = new TQueryRequest();
    deserializeThrift(request, thriftQueryRequest);
    String plan = frontend.getExplainString(request);
    LOG.info("Explain plan: " + plan);
    return plan;
  }

  /**
   * Process any updates to the metastore required after a query executes
   */
  public void updateMetastore(byte[] thriftCatalogUpdate) throws ImpalaException {
    TCatalogUpdate update = new TCatalogUpdate();
    deserializeThrift(update, thriftCatalogUpdate);
    frontend.updateMetastore(update);
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
