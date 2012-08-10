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
import com.cloudera.beeswax.api.Query;
import com.cloudera.beeswax.api.QueryHandle;
import com.cloudera.beeswax.api.QueryNotFoundException;
import com.cloudera.beeswax.api.Results;
import com.cloudera.beeswax.api.ResultsMetadata;
import com.cloudera.impala.thrift.ImpalaService.Client;
import com.cloudera.impala.thrift.TImpalaQueryOptions;
import com.cloudera.impala.thrift.TInsertResult;
import com.cloudera.impala.thrift.TQueryOptions;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;

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

  public void resetCatalog() throws TException {
    client.ResetCatalog();
  }

  /**
   * Executes the given query string and saves the results to the result queue
   * @param queryString
   *        Query to execute
   * @param execContext
   *        query execution options
   * @param results
   *        Queue to save results to
   * @param colTypes
   *        List to save column types to
   * @param colLabels
   *        List to save column labels to
   * @param insertResult
   *        Summary of the insert
   * @return
   *        The number of rows returned
   * @throws BeeswaxException
   * @throws TException
   * @throws QueryNotFoundException
   */
  public int runQuery(String queryString, TestExecContext execContext,
                      Queue<String> results,
                      List<String> colTypes, List<String> colLabels,
                      TInsertResult insertResult)
                      throws BeeswaxException, TException, QueryNotFoundException {
    LOG.info("query: " + queryString);
    LOG.info("query option: " + execContext.getTQueryOptions());
    Query query = new Query();
    query.query = queryString;
    query.configuration = getBeeswaxQueryConfigurations(execContext.getTQueryOptions());
    QueryHandle queryHandle = client.executeAndWait(query, "1");

    ResultsMetadata resultsMetadata = client.get_results_metadata(queryHandle);
    for (FieldSchema fs : resultsMetadata.schema.getFieldSchemas()) {
      colLabels.add(fs.getName());
      colTypes.add(fs.getType());
    }

    if (insertResult != null) {
      // Insert
      TInsertResult tInsertResult = client.CloseInsert(queryHandle);
      insertResult.setModified_hdfs_partitions(
          tInsertResult.getModified_hdfs_partitions());
      insertResult.setRows_appended(tInsertResult.getRows_appended());
      return 0;
    }

    // Query
    int numRows = 0;
    while (true) {
      Results result = client.fetch(queryHandle, false, execContext.getFetchSize());
      if (result.data.size() > 0) {
        results.add(result.getData().get(0));
        ++numRows;
      }

      if (!result.has_more) {
        break;
      }
    }
    client.close(queryHandle);

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

  private List<String> getBeeswaxQueryConfigurations(TQueryOptions queryOptions) {
    List<String> result = Lists.newArrayList();
    for (TImpalaQueryOptions option: TImpalaQueryOptions.values()) {
      String optionValue = "";
      switch(option) {
        case ABORT_ON_ERROR:
          optionValue = String.valueOf(queryOptions.isAbort_on_error());
          break;
        case MAX_ERRORS:
          optionValue = String.valueOf(queryOptions.getMax_errors());
          break;
        case DISABLE_CODEGEN:
          optionValue = String.valueOf(queryOptions.isDisable_codegen());
          break;
        case BATCH_SIZE:
          optionValue = String.valueOf(queryOptions.getBatch_size());
          break;
        case NUM_NODES:
          optionValue = String.valueOf(queryOptions.getNum_nodes());
          break;
        case MAX_SCAN_RANGE_LENGTH:
          optionValue = String.valueOf(queryOptions.getMax_scan_range_length());
          break;
        case FILE_BUFFER_SIZE:
          optionValue = String.valueOf(queryOptions.getFile_buffer_size());
          break;
        case MAX_IO_BUFFERS:
          optionValue = String.valueOf(queryOptions.getMax_io_buffers());
          break;
        case NUM_SCANNER_THREADS:
          optionValue = String.valueOf(queryOptions.getNum_scanner_threads());
          break;
        default:
          Preconditions.checkState(false, "Unhandled option:" + option.toString());
      }
      result.add(String.format("%s:%s", option.toString(), optionValue));
    }
    return result;
  }
}
