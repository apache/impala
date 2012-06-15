// Copyright (c) 2012 Cloudera, Inc. All rights reserved.

package com.cloudera.impala.testutil;

import java.util.List;
import java.util.Queue;

import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.log4j.Logger;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;

import com.cloudera.beeswax.api.BeeswaxException;
import com.cloudera.beeswax.api.BeeswaxService.Client;
import com.cloudera.beeswax.api.Query;
import com.cloudera.beeswax.api.QueryHandle;
import com.cloudera.beeswax.api.QueryNotFoundException;
import com.cloudera.beeswax.api.Results;
import com.cloudera.beeswax.api.ResultsMetadata;

/*
 * Enables executing queries against the specified Impala Daemon.
 */
public class ImpaladClientExecutor {
  private static final Logger LOG = Logger.getLogger(ImpaladClientExecutor.class);
  private final TTransport transport;
  private final TProtocol protocol;
  private final Client client;

  public ImpaladClientExecutor(String hostname, int port) {
    transport = new TSocket(hostname, port);
    protocol = new TBinaryProtocol(transport);
    client = new Client(protocol);
  }

  public void init() throws TTransportException {
    transport.open();
  }

  public void close() throws TTransportException {
    if (transport.isOpen()) {
      transport.close();
    }
  }

  /**
   * Executes the given query string and saves the results to the result queue
   * @param queryString
   *        Query to execute
   * @param results
   *        Queue to save results to
   * @param colTypes
   *        List to save column types to
   * @param colLabels
   *        List to save column labels to
   * @return
   *        The number of rows returned
   */
  public int runQuery(String queryString, Queue<String> results,
                      List<String> colTypes, List<String> colLabels)
                      throws BeeswaxException, TException, QueryNotFoundException {
    LOG.info("query: " + queryString);
    Query query = new Query();
    query.query = queryString;
    QueryHandle queryHandle = client.executeAndWait(query, "");

    ResultsMetadata resultsMetadata = client.get_results_metadata(queryHandle);
    for (FieldSchema fs : resultsMetadata.schema.getFieldSchemas()) {
      colLabels.add(fs.getName());
      colTypes.add(fs.getType());
    }

    int numRows = 0;
    while (true) {
      // TODO: (lennik) Consider updating this to support different fetch sizes
      Results result = client.fetch(queryHandle, false, 1);
      if (result.data.size() > 0) {
        results.add(result.getData().get(0));
        ++numRows;
      }

      if (!result.has_more) {
        break;
      }
    }

    return numRows;
  }

  /**
   * Returns the query plan for the given query string.
   * @param queryString
   *        Query to get query plan info from
   * @return
   *        The query plan string
   */
  public String explain(String queryString) throws BeeswaxException, TException {
    Query query = new Query();
    query.query = queryString;
    return client.explain(query).textual;
  }
}