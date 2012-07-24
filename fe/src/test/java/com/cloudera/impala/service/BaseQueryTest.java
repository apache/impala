// Copyright (c) 2012 Cloudera, Inc. All rights reserved.

package com.cloudera.impala.service;

import static org.junit.Assert.fail;

import java.io.IOException;
import java.util.List;
import java.util.Set;

import junit.framework.Assert;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.log4j.Logger;
import org.apache.thrift.transport.TTransportException;
import org.junit.AfterClass;
import org.junit.BeforeClass;

import com.cloudera.impala.catalog.Catalog;
import com.cloudera.impala.testutil.ImpaladClientExecutor;
import com.cloudera.impala.testutil.QueryExecTestResult;
import com.cloudera.impala.testutil.TestExecContext;
import com.cloudera.impala.testutil.TestFileParser;
import com.cloudera.impala.testutil.TestFileParser.Section;
import com.cloudera.impala.testutil.TestFileParser.TestCase;
import com.cloudera.impala.testutil.TestFileUtils;
import com.cloudera.impala.testutil.TestUtils;
import com.google.common.base.Joiner;
import com.google.common.base.Objects;
import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

/**
 * Abstract class that query tests suites can derive from. Contains methods to
 * help with execution and verification of queries across different test
 * dimensions (file format, number of execution nodes, etc).
 */
public abstract class BaseQueryTest {
  private static final Logger LOG = Logger.getLogger(BaseQueryTest.class);
  private static final String TEST_DIR = "QueryTest";
  private static final int DEFAULT_FE_PORT = 21000;

  // If set to true, new test results will be generated and saved to the specified
  // output directory.
  private final static boolean GENERATE_NEW_TEST_RESULTS = false;
  private final static String TEST_RESULT_OUTPUT_DIRECTORY = "/tmp/";

  // Commands recognized as part of the SETUP section
  private static final String RESET_CMD = "RESET";
  private static final String DROP_PARTITIONS_CMD = "DROP PARTITIONS";
  private static final String RELOAD_CATALOG_CMD = "RELOAD";

  private static Catalog catalog;
  private static Executor inProcessExecutor;
  private static ImpaladClientExecutor impaladClientExecutor;
  private static TargetTestEnvironment targetTestEnvironment =
      TargetTestEnvironment.IN_PROCESS;

  //Test dimension values
  protected static final List<Integer> ALL_BATCH_SIZES = ImmutableList.of(0, 16, 1);
  protected static final List<Integer> SMALL_BATCH_SIZES = ImmutableList.of(16, 1);
  // TODO: IMP-77 only cluster size of 1 and 2 execute correctly; to see incorrect result
  // caused by multi-node planning/execution, use the commented out lists. Cluster size
  // of 2 is a special case which "distributed execution" works because all the work are
  // performed between a single slave node and a coordinator.
  //protected  static final List<Integer> ALL_CLUSTER_SIZES = ImmutableList.of(1, 2, 3, 0);
  //protected  static final List<Integer> SMALL_CLUSTER_SIZES = ImmutableList.of(1, 2, 3);
  //protected static final List<Integer> ALL_NODES_ONLY = ImmutableList.of(0);
  protected static final List<Integer> ALL_CLUSTER_SIZES = ImmutableList.of(1, 2);
  protected static final List<Integer> SMALL_CLUSTER_SIZES = ImmutableList.of(1, 2);
  protected static final List<Integer> ALL_NODES_ONLY = ImmutableList.of(2);
  protected static final List<Boolean> ALL_LLVM_OPTIONS = ImmutableList.of(true, false);
  protected static final Set<TableFormat> NON_COMPRESSED_TYPES =
      Sets.newHashSet(TableFormat.TEXT);
  protected static final List<CompressionFormat> ALL_COMPRESSION_FORMATS =
      ImmutableList.of(CompressionFormat.DEFAULT, CompressionFormat.GZIP,
                       CompressionFormat.BZIP, CompressionFormat.SNAPPY);
  protected static final List<CompressionFormat> UNCOMPRESSED_ONLY =
      ImmutableList.of(CompressionFormat.NONE);
  protected static final List<TableFormat> ALL_TABLE_FORMATS =
      ImmutableList.copyOf(TableFormat.values());
  protected static final List<TableFormat> TEXT_FORMAT_ONLY =
      ImmutableList.of(TableFormat.TEXT);

  protected final static TestExecMode EXECUTION_MODE = TestExecMode.valueOf(
      System.getProperty("testExecutionMode", "reduced").toUpperCase());

  /**
   * The type of target test environments. Determines whether the front end is running
   * in-process or out-of-process (ImpalaD).
   */
  protected enum TargetTestEnvironment {
    IN_PROCESS,
    EXTERNAL_PROCESS;
  }

  /**
   * The different test execution modes that are supported. These modes control which
   * combination of test configurations to use during test execution.
   */
  protected enum TestExecMode {
    REDUCED,
    EXHAUSTIVE;
  }

  protected enum CompressionFormat {
    NONE(""),
    DEFAULT("_def"),
    GZIP("_gzip"),
    BZIP("_bzip"),
    SNAPPY("_snap");

    final String tableSuffix;
    private CompressionFormat(String tableSuffix) { this.tableSuffix = tableSuffix; }
    public String getTableSuffix() { return tableSuffix; }
  }

  protected enum TableFormat {
    TEXT(""),
    RCFILE("_rc"),
    SEQUENCEFILE("_seq"),
    SEQUENCEFILE_RECORD("_seq_record");

    final String tableSuffix;
    private TableFormat(String tableSuffix) { this.tableSuffix = tableSuffix; }
    public String getTableSuffix() { return tableSuffix; }
  }

  static private class TestConfiguration {
    private final CompressionFormat compressionFormat;
    private final TableFormat tableFormat;
    private final TestExecContext execContext;

    public TestConfiguration(int nodes, int batchSize, CompressionFormat compression,
        TableFormat tableFormat, boolean disableLlvm) {
      this.execContext = new TestExecContext(nodes, batchSize, disableLlvm, false, 1000);
      this.compressionFormat = compression;
      this.tableFormat = tableFormat;
    }

    public String getTableSuffix() {
      return tableFormat.getTableSuffix() + compressionFormat.getTableSuffix();
    }

    public CompressionFormat getCompressionFormat() { return compressionFormat; }
    public TableFormat getTableFormat() { return tableFormat; }
    public int getClusterSize() { return execContext.getNumNodes(); }
    public int getBatchSize() { return execContext.getBatchSize(); }
    public boolean getDisableLlvm() { return execContext.isCodegenDisabled(); }

    @Override
    public String toString() {
      return Objects.toStringHelper(this).add("Exec Context", execContext.toString())
                                         .add("Compression type", compressionFormat)
                                         .add("Table format", tableFormat)
                                         .toString();
    }
  }

  @BeforeClass
  public static void setUp() throws Exception {
    String impaladHostname = System.getProperty("impalad");
    impaladClientExecutor = null;

    // If hostname is set, ImpalaD is the target test environment.
    if (impaladHostname != null && !impaladHostname.isEmpty()) {
      impaladClientExecutor = createImpaladClientExecutor(impaladHostname);
      targetTestEnvironment = TargetTestEnvironment.EXTERNAL_PROCESS;
    }
    else {
      catalog = new Catalog(true);
      inProcessExecutor = new Executor(catalog);
      targetTestEnvironment = TargetTestEnvironment.IN_PROCESS;
    }

    LOG.info("Targeting Test Environment: " + targetTestEnvironment);
    LOG.info(String.format("Executing tests in mode: %s", EXECUTION_MODE));
  }

  @AfterClass
  public static void cleanUp() {
    if (catalog != null) {
      catalog.close();
    }

    if (impaladClientExecutor != null) {
      try {
        impaladClientExecutor.close();
      } catch (TTransportException e) {
        e.printStackTrace();
        fail("Problem closing transport: " + e.getMessage());
      }
    }
  }

  private static ImpaladClientExecutor createImpaladClientExecutor(String hostName) {
    int fePort = DEFAULT_FE_PORT;
    ImpaladClientExecutor client = null;
    try {
      fePort = Integer.parseInt(
                   System.getProperty("fe_port", Integer.toString(DEFAULT_FE_PORT)));
    } catch (NumberFormatException nfe) {
      fail("Invalid port format.");
    }

    client = new ImpaladClientExecutor(hostName, fePort);
    try {
      client.init();
    } catch (TTransportException e) {
      e.printStackTrace();
      fail("Error opening transport: " + e.getMessage());
    }

    return client;
  }

  private Object getTargetExecutor() {
    return (getTargetTestEnvironment() == TargetTestEnvironment.IN_PROCESS) ?
               inProcessExecutor : impaladClientExecutor;
  }

  protected TargetTestEnvironment getTargetTestEnvironment() {
    return targetTestEnvironment;
  }

  private static List<String> getCmdArguments(String cmd, String completeCmd) {
    Iterable<String> argsIter =
        Splitter.on(",").split(completeCmd.substring(cmd.length()));
    return Lists.newArrayList(argsIter);
  }

  private static void resetTables(List<String> tableNames) throws Exception {
    HiveMetaStoreClient client = new HiveMetaStoreClient(new HiveConf(QueryTest.class));
    for (String tableName: tableNames) {
      try {
        Table table = client.getTable("default", tableName.trim());
        client.dropTable("default", tableName, true, true);
        client.createTable(table);
      } catch (Exception e) {
        LOG.warn("Failed to drop and recreate table: " + tableName, e);
        throw e;
      }
    }
  }

  private static void dropPartitions(List<String> tableNames) throws Exception {
    HiveMetaStoreClient client = new HiveMetaStoreClient(new HiveConf(QueryTest.class));
    for (String tableName: tableNames) {
      try {
        for (String name:
          client.listPartitionNames("default", tableName.trim(), (short)0)) {
          client.dropPartitionByName("default", tableName.trim(), name, true);
        }
      } catch (Exception e) {
        LOG.warn("Failed to drop partition for table: " + tableName, e);
        throw e;
      }
    }
  }

  private void runSetupSection(List<String> setupCmds) throws Exception {
    for (String cmd: setupCmds) {
      if (cmd.startsWith(RESET_CMD)) {
        List<String> tableNames = getCmdArguments(RESET_CMD, cmd);
        resetTables(tableNames);
      } else if (cmd.startsWith(DROP_PARTITIONS_CMD)) {
        List<String> tableNames = getCmdArguments(DROP_PARTITIONS_CMD, cmd);
        dropPartitions(tableNames);
      } else if (cmd.startsWith(RELOAD_CATALOG_CMD)) {
        List<String> tableNames = getCmdArguments(RELOAD_CATALOG_CMD, cmd);
        // If running in-process we need to reload the catalog. If running vs Impalad
        // that should be taken care of for us.
        if (getTargetTestEnvironment() == TargetTestEnvironment.IN_PROCESS) {
          if (tableNames.size() == 0) {
            catalog.close();
            catalog = new Catalog(true);
            inProcessExecutor.setCatalog(catalog);
          } else {
            for (String table: tableNames) {
              catalog.invalidateTable(table.trim());
            }
          }
        }
      }
    }
  }

  /**
   * Run a test file for each of the table format name substitutions for all permutations
   * of the batch sizes and node numbers specified in BATCH_SIZES and NUM_NODE_VALUES.
   */
  protected void runQueryUncompressedTextOnly(String testFile, boolean abortOnError,
      int maxErrors) {
    runQueryInAllBatchAndClusterPerms(testFile, abortOnError, maxErrors,
        TEXT_FORMAT_ONLY, ALL_BATCH_SIZES, ALL_CLUSTER_SIZES);
  }

  /**
   * Run a test file for each of the table format name substitutions for all permutations
   * of batchSizes and numNodes.
   */
  protected void runQueryInAllBatchAndClusterPerms(String testFile, boolean abortOnError,
      int maxErrors, List<TableFormat> tableFormats, List<Integer> batchSizes,
      List<Integer> numNodes) {

    if (tableFormats == null) {
      tableFormats = TEXT_FORMAT_ONLY;
    }

    List<TestConfiguration> testConfigs =
      generateAllConfigurationPermutations(tableFormats, UNCOMPRESSED_ONLY,
          batchSizes, numNodes, ALL_LLVM_OPTIONS);
    runQueryWithTestConfigs(testConfigs, testFile, abortOnError, maxErrors);
  }

  /**
   * Generates a restricted list of test configurations to execute against a single query
   * file.
   *
   * Produces one compressed and one uncompressed configuration for each combination of
   * table format and batch size, except if a) the table format does not support
   * compression or b) the table format is SEQUENCEFILE_RECORD, in which case *only* the
   * compressed form is generated.
   *
   * Compression types, cluster sizes and using llvm are assigned in a round-robin
   * fashion, and therefore the set of configurations generated is dependent on the
   * order of all the list arguments.
   */
  private List<TestConfiguration> generateRoundRobinConfigurations(
      List<TableFormat> baseFormats, List<CompressionFormat> compressionSuffixes,
      List<Integer> batchSizes, List<Integer> clusterSizes) {
    List<TestConfiguration> configs = Lists.newArrayList();
    int compressionIdx = 0;
    int clusterSizesIdx = 0;
    boolean disableLlvm = true;
    for (TableFormat tableFormat: baseFormats) {
      for (int batchSize: batchSizes) {
        int clusterSize = clusterSizes.get(clusterSizesIdx++ % clusterSizes.size());
        disableLlvm = !disableLlvm;

        if (tableFormat != TableFormat.SEQUENCEFILE_RECORD) {
          configs.add(new TestConfiguration(clusterSize, batchSize,
              CompressionFormat.NONE, tableFormat, disableLlvm));
          clusterSize = clusterSizes.get(clusterSizesIdx++ % clusterSizes.size());
        }

        if (!NON_COMPRESSED_TYPES.contains(tableFormat)) {
          CompressionFormat compression =
            compressionSuffixes.get(compressionIdx++ % compressionSuffixes.size());
          configs.add(new TestConfiguration(clusterSize, batchSize, compression,
              tableFormat, disableLlvm));
        }
      }
    }

    return configs;
  }

  /**
   * Generates a list of all valid permutations of table format, compression format,
   * batch size and cluster size. Permutations which don't make sense are filtered out.
   */
  private static List<TestConfiguration> generateAllConfigurationPermutations(
      List<TableFormat> tableFormats, List<CompressionFormat> compressionFormats,
      List<Integer> batchSizes, List<Integer> clusterSizes, List<Boolean> llvmOptions) {
    List<TestConfiguration> configs = Lists.newArrayList();
    for (TableFormat tableFormat: tableFormats) {
      for (CompressionFormat compressionFormat: compressionFormats) {
        for (int batchSize: batchSizes) {
          for (int clusterSize: clusterSizes) {
            for (boolean disableLlvm: llvmOptions) {
              TestConfiguration config = new TestConfiguration(clusterSize, batchSize,
                  compressionFormat, tableFormat, disableLlvm);

              if (isValidTestConfiguration(config)) {
                configs.add(
                    new TestConfiguration(clusterSize, batchSize, compressionFormat,
                                          tableFormat, disableLlvm));
              }
            }
          }
        }
      }
    }
    return configs;
  }

  /**
   * Returns true if the given test configuration is valid and false if it is invalid.
   */
  private static boolean isValidTestConfiguration(TestConfiguration testConfiguration) {
    // Currently, compression of the 'text' file format is not supported.
    if (testConfiguration.getTableFormat() == TableFormat.TEXT) {
      return testConfiguration.getCompressionFormat() == CompressionFormat.NONE;
    }
    return true;
  }

  private void runQueryWithTestConfigs(List<TestConfiguration> testConfigs,
      String testFile, boolean abortOnError, int maxErrors) {
    String fileName = TEST_DIR + "/" + testFile + ".test";
    TestFileParser queryFileParser = new TestFileParser(fileName);

    LOG.debug("Running the following configurations over file " + fileName + " : ");
    LOG.debug(Joiner.on("\n").join(testConfigs));

    for (TestConfiguration config: testConfigs) {
      queryFileParser.parseFile(config.getTableSuffix());
      TestExecContext context = new TestExecContext(
          config.getClusterSize(), config.getBatchSize(), config.getDisableLlvm(),
          abortOnError, maxErrors);
      runOneQueryTest(queryFileParser, config, context, new StringBuilder());

      // Don't need to (or want to) run multiple test configurations if we are generating
      // new results.
      if (GENERATE_NEW_TEST_RESULTS) {
        break;
      }
    }
  }

  /**
   * Run a test file using all permutations of base tableFormat types and
   * batch sizes.
   * Compression types and number of nodes are selected in round robin for each
   * test.
   * The tableFormat types are:
   *   text (uncompressed)
   *   sequence (uncompressed)
   *   sequence with record compression
   *   sequence with block compression
   *   rcfile (uncompressed)
   *   rcfile with (block) compression (when supported)
   * For each loop over the batch sizes we run the uncompressed case and the
   * block compression case.  Sequence record compression is special cased
   * with a pseudo base tableFormat type so we do not run the uncompressed case.
   */
  protected void runPairTestFile(String testFile, boolean abortOnError, int maxErrors,
      List<TableFormat> baseFormats, List<CompressionFormat> compressionSuffixes,
      List<Integer> batchSizes, List<Integer> numNodes) {

    List<TestConfiguration> testConfigs =
      generateRoundRobinConfigurations(baseFormats, compressionSuffixes, batchSizes,
          numNodes);
    runQueryWithTestConfigs(testConfigs, testFile, abortOnError, maxErrors);
  }

  /**
   * Run a single query test file as specified in the queryFileParser.
   */
  private void runOneQueryTest(TestFileParser queryFileParser, TestConfiguration config,
                               TestExecContext context, StringBuilder errorLog) {

    List<QueryExecTestResult> results = Lists.newArrayList();
    for (TestCase testCase: queryFileParser.getTestCases()) {

      QueryExecTestResult expectedResult = testCase.getQueryExecTestResult();

      // We have to run the setup section once per query, not once per test. Therefore
      // they can be very expensive.
      if (expectedResult.getSetup().size() > 0) {
        try {
          runSetupSection(testCase.getSectionContents(Section.SETUP, false,
                                                      config.getTableSuffix()));
        } catch (Exception e) {
          fail(e.getMessage());
        }
      }

      String queryString = testCase.getSectionAsString(
          Section.QUERY, false, " ", config.getTableSuffix());

      QueryExecTestResult result = TestUtils.runQueryUsingExecutor(getTargetExecutor(),
          queryString, context, testCase.getStartingLineNum(), expectedResult, errorLog);

      if(GENERATE_NEW_TEST_RESULTS) {
        result.getQuery().addAll(expectedResult.getQuery());
        result.getSetup().addAll(expectedResult.getSetup());
        results.add((result));
      }
    }

    // Ignore failure messages if we are updating test results. They are expected.
    if (GENERATE_NEW_TEST_RESULTS) {
      LOG.info(errorLog.toString());
      try {
        TestFileUtils.saveUpdatedResults(
            TEST_RESULT_OUTPUT_DIRECTORY + queryFileParser.getTestFileName(), results);
      } catch (IOException e) {
        fail("Error updating results: " + e.toString());
      }
    } else if (errorLog.length() != 0) {
      fail(errorLog.toString());
    }
  }

  /**
   * Runs test with different configurations based on the current test execution mode.
   * For example, with EXHAUSTIVE execution mode we run all combinations of file formats
   * compression formats, batch sizes, etc.. If the test execution mode is REDUCED
   * we will run with a reduced set of test configurations.
   */
  protected void runTestInExecutionMode(TestExecMode executionMode, String testFile,
      boolean abortOnError, int maxErrors) {
    // TODO: TPCH Currently has a bug with when LLVM is enabled (IMP-129). This is a
    // temporary workaround for this problem. Once that is resolved this can be
    // removed.
    if (testFile.trim().startsWith("tpch")) {
      List<TestConfiguration> testConfigs = generateAllConfigurationPermutations(
          TEXT_FORMAT_ONLY, UNCOMPRESSED_ONLY,
          ImmutableList.of(16), ImmutableList.of(2),  ImmutableList.of(true));
      runQueryWithTestConfigs(testConfigs, testFile, abortOnError, maxErrors);
      return;
    }

    switch (executionMode) {
      case REDUCED:
        // TODO: Consider running with the fastest format to cut down on execution time
        runQueryUncompressedTextOnly(testFile, abortOnError, maxErrors);
        break;
      case EXHAUSTIVE:
        runQueryWithAllConfigurationPermutations(testFile, abortOnError, maxErrors);
        break;
      default:
        Assert.fail("Unexpected test execution mode: " + EXECUTION_MODE);
    }
  }

  /**
   * Runs the query with all valid permutations of table format, batch size,
   * cluster size, and llvm options.
   */
  private void runQueryWithAllConfigurationPermutations(String testFile,
      boolean abortOnError, int maxErrors) {
    List<TestConfiguration> testConfigs = generateAllConfigurationPermutations(
        ALL_TABLE_FORMATS, ALL_COMPRESSION_FORMATS, ALL_BATCH_SIZES,
        ALL_CLUSTER_SIZES, ALL_LLVM_OPTIONS);
    runQueryWithTestConfigs(testConfigs, testFile, abortOnError, maxErrors);
  }
}