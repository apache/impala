// Copyright (c) 2011 Cloudera, Inc. All rights reserved.

package com.cloudera.impala.service;

import java.io.PrintStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.apache.thrift.TException;
import org.apache.thrift.TSerializer;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.cloudera.impala.analysis.AnalysisContext;
import com.cloudera.impala.analysis.Expr;
import com.cloudera.impala.analysis.SelectStmt;
import com.cloudera.impala.catalog.Catalog;
import com.cloudera.impala.catalog.PrimitiveType;
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

  private final Catalog catalog;

  // for async execution
  private Thread execThread;
  private String errorMsg;

  public Executor(Catalog catalog) {
    this.catalog = catalog;
    init();
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
      boolean abortOnError, int maxErrors,
      List<String> errorLog, Map<String, Integer> fileErrors,
      BlockingQueue<TResultRow> resultQueue) throws ImpalaException {
    init();
    LOG.info("query: " + request.stmt);
    AnalysisContext.AnalysisResult analysisResult =
        analyzeQuery(request, colTypes, colLabels);
    if (containsOrderBy != null) {
      containsOrderBy.set(analysisResult.isSelectStmt()
          && analysisResult.getSelectStmt().hasOrderByClause());
    }
    execQuery(analysisResult, request.numNodes, batchSize, abortOnError, maxErrors,
              errorLog, fileErrors, request.returnAsAscii, resultQueue);
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
      final List<String> errorLog, final Map<String, Integer> fileErrors,
      final BlockingQueue<TResultRow> resultQueue) throws ImpalaException {
    init();
    LOG.info("query: " + request.stmt);
    final AnalysisContext.AnalysisResult analysisResult =
        analyzeQuery(request, colTypes, colLabels);
    if (containsOrderBy != null) {
      containsOrderBy.set(analysisResult.isSelectStmt()
          && analysisResult.getSelectStmt().hasOrderByClause());
    }
    Runnable execCall = new Runnable() {
      public void run() {
        try {
          execQuery(analysisResult, request.numNodes, batchSize, abortOnError,
                    maxErrors, errorLog, fileErrors, request.returnAsAscii,
                    resultQueue);
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
    if (analysisResult.isSelectStmt()) {
      SelectStmt selectStmt = analysisResult.getSelectStmt();
      // populate colTypes
      colTypes.clear();
      for (Expr expr : selectStmt.getSelectListExprs()) {
        colTypes.add(expr.getType());
      }

      // populate column labels for display purposes (e.g., via the CLI).
      colLabels.clear();
      colLabels.addAll(selectStmt.getColLabels());
    }

    return analysisResult;
  }

  // Execute query contained in 'analysisResult' and return result in
  // 'resultQueue'. If 'returnAsAscii' is true, returns results as printable
  // strings.
  private void execQuery(
      AnalysisContext.AnalysisResult analysisResult, int numNodes,
      int batchSize, boolean abortOnError, int maxErrors, List<String> errorLog,
      Map<String, Integer> fileErrors, boolean returnAsAscii,
      BlockingQueue<TResultRow> resultQueue) throws ImpalaException {
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
    execRequest.setQueryId(
        new TUniqueId(queryId.getMostSignificantBits(),
                      queryId.getLeastSignificantBits()));
    execRequest.setAsAscii(returnAsAscii);
    execRequest.setAbortOnError(abortOnError);
    execRequest.setMaxErrors(maxErrors);
    execRequest.setBatchSize(batchSize);
    execRequest.setSqlStmt(analysisResult.getStmt().toSql());

    for (List<TPlanExecParams> planParamsList : execRequest.getNodeRequestParams()) {
      for (TPlanExecParams params : planParamsList) {
        params.setBatchSize(batchSize);
      }
    }

    LOG.info(execRequest.toString());

    for (TPlanExecRequest planRequest: execRequest.fragmentRequests) {
      planRequest.setQueryId(execRequest.queryId);
    }

    TSerializer serializer = new TSerializer(new TBinaryProtocol.Factory());
    try {
      NativeBackend.ExecQuery(
          serializer.serialize(execRequest), errorLog, fileErrors, resultQueue);
    } catch (TException e) {
      throw new RuntimeException(e.getMessage());
    }
  }

  public static Catalog createCatalog() {
    HiveMetaStoreClient client = null;
    try {
      client = new HiveMetaStoreClient(new HiveConf(Executor.class));
    } catch (Exception e) {
      System.err.println(e.getMessage());
      throw new AssertionError("couldn't create HiveMetaStoreClient");
    }
    return new Catalog(client);
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
    Executor coordinator = new Executor(catalog);
    if (asyncExec) {
      coordinator.asyncRunQuery(
          request, dummyColTypes, dummyColLabels, null, batchSize, DEFAULT_ABORT_ON_ERROR,
          DEFAULT_MAX_ERRORS, errorLog, fileErrors, resultQueue);
    } else {
      coordinator.runQuery(
          request, dummyColTypes, dummyColLabels, null, batchSize, DEFAULT_ABORT_ON_ERROR,
          DEFAULT_MAX_ERRORS, errorLog, fileErrors, resultQueue);
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
    int numRows = runQuery(queryString, catalog, true, batchSize, System.out);
    System.out.println("TOTAL ROWS: " + numRows);
  }
}
