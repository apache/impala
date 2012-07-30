// Copyright (c) 2011 Cloudera, Inc. All rights reserved.

package com.cloudera.impala.service;

import java.io.IOException;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.AlreadyExistsException;
import org.apache.hadoop.hive.metastore.api.InvalidObjectException;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.NoSuchObjectException;
import org.apache.thrift.TException;
import org.apache.thrift.TSerializer;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.cloudera.impala.analysis.AnalysisContext;
import com.cloudera.impala.analysis.Expr;
import com.cloudera.impala.analysis.InsertStmt;
import com.cloudera.impala.analysis.QueryStmt;
import com.cloudera.impala.catalog.Catalog;
import com.cloudera.impala.catalog.HdfsTable;
import com.cloudera.impala.catalog.PrimitiveType;
import com.cloudera.impala.catalog.HdfsStorageDescriptor.InvalidStorageDescriptorException;
import com.cloudera.impala.common.ImpalaException;
import com.cloudera.impala.planner.Planner;
import com.cloudera.impala.thrift.TColumnValue;
import com.cloudera.impala.thrift.TPlanExecParams;
import com.cloudera.impala.thrift.TPlanExecRequest;
import com.cloudera.impala.thrift.TQueryExecRequest;
import com.cloudera.impala.thrift.TQueryRequest;
import com.cloudera.impala.thrift.TResultRow;
import com.cloudera.impala.thrift.TUniqueId;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;

public class Executor {
  private final static Logger LOG = LoggerFactory.getLogger(Executor.class);

  public static final boolean DEFAULT_ABORT_ON_ERROR = false;
  public static final int DEFAULT_MAX_ERRORS = 100;
  public static final int DEFAULT_BATCH_SIZE = 0;  // backend's default
  public static final boolean DEFAULT_DISABLE_CODEGEN = false;

  private Catalog catalog;

  // for async execution
  private Thread execThread;
  private String errorMsg;

  public Executor(Catalog catalog) {
    this.catalog = catalog;
    init();
  }

  /**
   * Updates the metadata catalog. Note: if using Executor from multiple
   * threads the updated catalog is not guaranteed to be visible to other threads without
   * a synchronisation point.
   */
  public void setCatalog(Catalog catalog) {
    this.catalog = catalog;
  }

  private void init() {
    this.execThread = null;
    this.errorMsg = null;
  }

  // Run the query synchronously (ie, this function blocks until the query
  // is finished) and place the results in resultQueue, followed by an empty
  // row that acts as an end-of-stream marker.
  // Also populates 'colTypes' and 'colLabels' and sets 'containsOrderBy'.
  public void runQuery(TQueryRequest request, List<PrimitiveType> colTypes,
      List<String> colLabels, AtomicBoolean containsOrderBy, int batchSize,
      boolean abortOnError, int maxErrors, boolean disableCodegen,
      List<String> errorLog, Map<String, Integer> fileErrors,
      BlockingQueue<TResultRow> resultQueue,
      InsertResult insertResult) throws ImpalaException {
    init();
    LOG.info("query: " + request.stmt);
    AnalysisContext.AnalysisResult analysisResult =
        analyzeQuery(request, colTypes, colLabels);
    if (containsOrderBy != null) {
      containsOrderBy.set(analysisResult.isQueryStmt()
          && analysisResult.getQueryStmt().hasOrderByClause());
    }
    execQuery(analysisResult, request.numNodes, batchSize, abortOnError, maxErrors,
              disableCodegen, errorLog, fileErrors, request.returnAsAscii, resultQueue,
              insertResult);
    addSentinelRow(resultQueue);
  }

  // Run the query asynchronously, returning immediately after starting
  // query execution and populating 'colTypes' and 'colLabels'. Places an empty row as
  // a marker at the end of the queue once all result rows have been added to the
  // queue or an error occurred.
  // getErrorMsg() will return a non-empty string if an error occurred, otherwise
  // null.
  public void asyncRunQuery(
      final TQueryRequest request, List<PrimitiveType> colTypes, List<String> colLabels,
      AtomicBoolean containsOrderBy,
      final int batchSize, final boolean abortOnError, final int maxErrors,
      final boolean disableCodegen,
      final List<String> errorLog, final Map<String, Integer> fileErrors,
      final BlockingQueue<TResultRow> resultQueue, final InsertResult insertResult)
  throws ImpalaException {
    init();
    LOG.info("query: " + request.stmt);
    final AnalysisContext.AnalysisResult analysisResult =
        analyzeQuery(request, colTypes, colLabels);
    if (containsOrderBy != null) {
      containsOrderBy.set(analysisResult.isQueryStmt()
          && analysisResult.getQueryStmt().hasOrderByClause());
    }
    Runnable execCall = new Runnable() {
      public void run() {
        try {
          execQuery(analysisResult, request.numNodes, batchSize, abortOnError,
                    maxErrors, disableCodegen, errorLog, fileErrors, request.returnAsAscii,
                    resultQueue, insertResult);
        } catch (ImpalaException e) {
          errorMsg = e.getMessage();
        }
        addSentinelRow(resultQueue);
      }
    };
    execThread = new Thread(execCall);
    execThread.start();
  }

  // When executing asynchronously, this waits for execution to finish and returns
  // an error message if an error occurred, otherwise null.
  public String getErrorMsg() {
    if (execThread != null) {
      try {
        execThread.join();
      } catch (InterruptedException e) {
        throw new AssertionError("unexpected interrupt: execThread.join()");
      }
      return errorMsg;
    } else {
      return null;
    }
  }

  private void addSentinelRow(BlockingQueue<TResultRow> resultQueue) {
    try {
      resultQueue.put(new TResultRow());
    } catch (InterruptedException e) {
      // we don't expect to get interrupted
      throw new AssertionError("unexpected blockingqueueinterrupt");
    }
  }

  // Analyze query and return analysis result and types of select list exprs.
  public AnalysisContext.AnalysisResult analyzeQuery(
      TQueryRequest request, List<PrimitiveType> colTypes, List<String> colLabels)
      throws ImpalaException {
    AnalysisContext analysisCtxt = new AnalysisContext(catalog);
    AnalysisContext.AnalysisResult analysisResult = analysisCtxt.analyze(request.stmt);
    Preconditions.checkNotNull(analysisResult.getStmt());

    // TODO: handle EXPLAIN
    if (analysisResult.isQueryStmt()) {
      QueryStmt queryStmt = analysisResult.getQueryStmt();
      // populate colTypes
      colTypes.clear();
      for (Expr expr : queryStmt.getResultExprs()) {
        colTypes.add(expr.getType());
      }

      // populate column labels for display purposes (e.g., via the CLI).
      colLabels.clear();
      colLabels.addAll(queryStmt.getColLabels());
    }

    return analysisResult;
  }

  // Execute query contained in 'analysisResult' and return result in
  // 'resultQueue' for select statements and in 'insertResults' for insert statements.
  // If 'returnAsAscii' is true, returns results in 'resultQueue' as printable
  // strings.
  private void execQuery(
      AnalysisContext.AnalysisResult analysisResult, int numNodes,
      int batchSize, boolean abortOnError, int maxErrors, boolean disableCodegen,
      List<String> errorLog, Map<String, Integer> fileErrors, boolean returnAsAscii,
      BlockingQueue<TResultRow> resultQueue, InsertResult insertResult)
  throws ImpalaException {
    // create plan fragments
    Planner planner = new Planner();
    StringBuilder explainString = new StringBuilder();
    // for now, only single-node execution
    TQueryExecRequest execRequest = planner.createPlanFragments(
        analysisResult, numNodes, explainString);
    // Log explain string.
    LOG.info(explainString.toString());

    // set remaining execution parameters
    UUID queryId = UUID.randomUUID();
    execRequest.setQuery_id(
        new TUniqueId(queryId.getMostSignificantBits(),
                      queryId.getLeastSignificantBits()));
    execRequest.setAs_ascii(returnAsAscii);
    execRequest.setAbort_on_error(abortOnError);
    execRequest.setMax_errors(maxErrors);
    execRequest.setDisable_codegen(disableCodegen);
    execRequest.setBatch_size(batchSize);
    execRequest.setSql_stmt(analysisResult.getStmt().toSql());

    // assign fragment ids
    for (int fragmentNum = 0; fragmentNum < execRequest.fragment_requests.size();
         ++fragmentNum) {
      TPlanExecRequest planRequest = execRequest.fragment_requests.get(fragmentNum);
      planRequest.setQuery_id(execRequest.query_id);
      TUniqueId fragmentId =
          new TUniqueId(queryId.getMostSignificantBits(),
                        queryId.getLeastSignificantBits() + fragmentNum);
      planRequest.setFragment_id(fragmentId);
    }

    // Node request params may not be set for queries w/o a coordinator
    if (execRequest.getNode_request_params() != null) {
      for (List<TPlanExecParams> planParamsList : execRequest.getNode_request_params()) {
        for (TPlanExecParams params : planParamsList) {
          params.setBatch_size(batchSize);
          params.setDisable_codegen(disableCodegen);
        }
      }

      // set dest_fragment_ids of non-coord fragment
      if (execRequest.node_request_params.size() == 2) {
        // we only have two fragments (1st one: coord); the destination
        // of the 2nd fragment is the coordinator fragment
        TUniqueId coordFragmentId = execRequest.fragment_requests.get(0).fragment_id;
        for (TPlanExecParams execParams: execRequest.node_request_params.get(1)) {
          execParams.setDest_fragment_id(coordFragmentId);
        }
      }
    }

    LOG.info(execRequest.toString());

    TSerializer serializer = new TSerializer(new TBinaryProtocol.Factory());
    try {
      NativeBackend.ExecQuery(
          serializer.serialize(execRequest), errorLog, fileErrors, resultQueue,
          insertResult);
    } catch (TException e) {
      throw new RuntimeException(e.getMessage());
    }

    // Update the metastore if necessary.
    if (analysisResult.isInsertStmt()) {
      try {
        updateMetastore(insertResult, analysisResult.getInsertStmt());
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    }
  }

  /**
   * Update partition metadata with those written to by insert statement. New partitions
   * are created in the metastore; existing partitions are not affected.
   */
  private void updateMetastore(InsertResult insertResult, InsertStmt insertStmt)
      throws MetaException, TException, NoSuchObjectException, InvalidObjectException,
      IOException, InvalidStorageDescriptorException {
    // Only update Metastore for Hdfs tables.
    if (!(insertStmt.getTargetTable() instanceof HdfsTable)) {
      LOG.warn("Unexpected table type in updateMetastore: "
          + insertStmt.getTargetTable().getClass());
      return;
    }
    HdfsTable table = (HdfsTable) insertStmt.getTargetTable();
    String dbName = table.getDb().getName();
    String tblName = table.getName();
    HiveMetaStoreClient msClient = catalog.getMetaStoreClient();
    if (table.getNumClusteringCols() > 0) {
      // Add all partitions to metastore.
      for (String hdfsPath : insertResult.getModifiedPartitions()) {
        String partName = HdfsTable.getPartitionName(table, hdfsPath);
        try {
          // TODO: Replace with appendPartition
          msClient.appendPartitionByName(dbName, tblName, partName);
        } catch (AlreadyExistsException e) {
          // Ignore since partition already exists.
        }
      }
    }
    org.apache.hadoop.hive.metastore.api.Table msTbl = msClient.getTable(dbName, tblName);
    // Reload the partition files from the Metastore into the Impala Catalog.
    table.loadPartitions(msClient.listPartitions(dbName, tblName, Short.MAX_VALUE), msTbl);
  }

  public static Catalog createCatalog() {
    return new Catalog();
  }

  /**
   * Run single query and write its results to the given PrintStream.
   * If catalog is null, a new one is is created from the HiveMetaStore.
   *
   * @param query
   *          Query to be executed.
   * @param catalog
   *          Catalog containing metadata. Must be non-null.
   * @param asyncExec
   *          Whether to use synchronous or asynchronous query execution.
   * @param batchSize
   *          Internal executor batch size.
   * @param targetStream
   *          Stream to write the query results to.
   *          Result rows are separated by newlines and fields are tabs.
   *          Runtime error log is written after the results.
   * @return
   *         The number of result rows or -1 on error.
   */
  public static int runQuery(String query, Catalog catalog, boolean asyncExec,
      int batchSize, PrintStream targetStream) throws ImpalaException {
    Preconditions.checkNotNull(catalog);
    int numRows = 0;
    TQueryRequest request = new TQueryRequest(query, true, 2);
    List<String> errorLog = new ArrayList<String>();
    Map<String, Integer> fileErrors = new HashMap<String, Integer>();
    List<PrimitiveType> dummyColTypes = Lists.newArrayList();
    List<String> dummyColLabels = Lists.newArrayList();
    BlockingQueue<TResultRow> resultQueue = new LinkedBlockingQueue<TResultRow>();
    InsertResult insertResult = new InsertResult();
    Executor coordinator = new Executor(catalog);
    if (asyncExec) {
      coordinator.asyncRunQuery(
          request, dummyColTypes, dummyColLabels, null, batchSize,
          DEFAULT_ABORT_ON_ERROR, DEFAULT_MAX_ERRORS, DEFAULT_DISABLE_CODEGEN, errorLog,
          fileErrors, resultQueue, insertResult);
    } else {
      coordinator.runQuery(
          request, dummyColTypes, dummyColLabels, null, batchSize,
          DEFAULT_ABORT_ON_ERROR, DEFAULT_MAX_ERRORS, DEFAULT_DISABLE_CODEGEN, errorLog,
          fileErrors, resultQueue, insertResult);
    }
    while (true) {
      TResultRow resultRow = null;
      try {
        resultRow = resultQueue.take();
      } catch (InterruptedException e) {
        throw new AssertionError("unexpected interrupt");
      }
      if (resultRow.colVals == null) {
        break;
      }
      ++numRows;
      for (TColumnValue val : resultRow.colVals) {
        targetStream.append('\t');
        targetStream.append(val.stringVal);
      }
      targetStream.print("\n");
    }
    // Append runtime error log.
    for (String s : errorLog) {
      targetStream.append(s);
      targetStream.append('\n');
    }
    // Append runtime file errors.
    for (Map.Entry<String, Integer> entry : fileErrors.entrySet()) {
      targetStream.append(entry.getValue() + " errors in " + entry.getKey());
      targetStream.append('\n');
    }
    if (asyncExec) {
      String errorMsg = coordinator.getErrorMsg();
      if (errorMsg != null) {
        System.err.println("encountered error:\n" + errorMsg);
        return -1;
      } else {
        return numRows;
      }
    } else {
      return numRows;
    }
  }

  private static String ParseOption(String option) {
    int equals = option.lastIndexOf("=");
    return option.substring(equals + 1);
  }


  // Run single query against test instance.  In addition to the query string,
  // the caller can specify options on how the query should be run.
  // Options are case-insensitive.
  // The options are:
  //    -batchsize=<int> : the batchsize the executor should use
  public static void main(String args[]) throws ImpalaException {
    String queryString = "";
    boolean valid = true;
    String option = "";

    // Default options
    int batchSize = Executor.DEFAULT_BATCH_SIZE;

    // Parse input for options
    try {
      for (int i = 0; i < args.length; ++i) {
        if (args[i].startsWith("-")) {
          option = args[i].toLowerCase();
          if (option.startsWith("-batchsize=")) {
            batchSize = Integer.parseInt(ParseOption(option));
          } else {
            System.err.println("Coordinator:: Invalid option: " + option);
            valid = false;
            break;
          }
        } else {
          if (queryString != "") {
            System.err.println("Coordinator:: can not pass multiple queries at once.");
            valid = false;
            break;
          }
          queryString = args[i];
        }
      }
    } catch (NumberFormatException e) {
      System.err.println("Coordinator::Invalid option: " + option);
      valid = false;
    }

    if (valid && queryString == "") {
      System.err.println("Coordinator:: No query string specified.");
      valid = false;
    }

    if (!valid) {
      System.exit(1);
    }

    Catalog catalog = createCatalog();
    try {
      int numRows = runQuery(queryString, catalog, true, batchSize, System.out);
      System.out.println("TOTAL ROWS: " + numRows);
    }
    finally {
      catalog.close();
    }
  }
}
