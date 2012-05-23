// Copyright (c) 2011 Cloudera, Inc. All rights reserved.

package com.cloudera.impala.service;

import static org.junit.Assert.fail;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.log4j.Logger;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import com.cloudera.impala.catalog.Catalog;
import com.cloudera.impala.testutil.TestFileParser;
import com.cloudera.impala.testutil.TestFileParser.Section;
import com.cloudera.impala.testutil.TestFileParser.TestCase;
import com.cloudera.impala.testutil.TestUtils;
import com.google.common.base.Joiner;
import com.google.common.base.Objects;
import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

public class QueryTest {
  private static final Logger LOG = Logger.getLogger(QueryTest.class);
  private static Catalog catalog;
  private static Executor executor;
  private final String testDir = "QueryTest";

  @BeforeClass
  public static void setUp() throws Exception {
    catalog = new Catalog(true);
    executor = new Executor(catalog);
  }

  @AfterClass
  public static void cleanUp() {
    catalog.close();
  }

  // Commands recognized as part of the SETUP section
  private static final String RESET_CMD = "RESET";
  private static final String DROP_PARTITIONS_CMD = "DROP PARTITIONS";
  private static final String RELOAD_CATALOG_CMD = "RELOAD";

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
          client.dropPartitionByName("default", "alltypesinsert", name, true);
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
        if (tableNames.size() == 0) {
          catalog.close();
          catalog = new Catalog(true);
          executor.setCatalog(catalog);
        } else {
          for (String table: tableNames) {
            catalog.invalidateTable(table.trim());
          }
        }
      }
    }

  }

  private enum CompressionFormat {
    NONE(""),
    DEFAULT("_def"),
    GZIP("_gzip"),
    BZIP("_bzip"),
    SNAPPY("_snap");

    final String tableSuffix;
    private CompressionFormat(String tableSuffix) { this.tableSuffix = tableSuffix; }
    public String getTableSuffix() { return tableSuffix; }
  }

  private enum TableFormat {
    TEXT(""),
    RCFILE("_rc"),
    SEQUENCEFILE("_seq"),
    SEQUENCEFILE_RECORD("_seq_record");

    final String tableSuffix;
    private TableFormat(String tableSuffix) { this.tableSuffix = tableSuffix; }
    public String getTableSuffix() { return tableSuffix; }
  }

  private static final List<Integer> ALL_BATCH_SIZES = ImmutableList.of(0, 16, 1);
  private static final List<Integer> SMALL_BATCH_SIZES = ImmutableList.of(16, 1);
  private static final List<Integer> ALL_CLUSTER_SIZES = ImmutableList.of(1, 2, 3, 0);
  private static final List<Integer> SMALL_CLUSTER_SIZES = ImmutableList.of(1, 2, 3);
  private static final List<Integer> ALL_NODES_ONLY = ImmutableList.of(0);
  private static final List<Boolean> ALL_LLVM_OPTIONS = ImmutableList.of(true, false);

  private static final Set<TableFormat> NON_COMPRESSED_TYPES =
    Sets.newHashSet(TableFormat.TEXT);
  private static final List<CompressionFormat> ALL_COMPRESSION_FORMATS =
    ImmutableList.of(CompressionFormat.DEFAULT, CompressionFormat.GZIP,
        CompressionFormat.BZIP, CompressionFormat.SNAPPY);
  private static final List<CompressionFormat> UNCOMPRESSED_ONLY =
    ImmutableList.of(CompressionFormat.NONE);
  private static final List<TableFormat> ALL_TABLE_FORMATS =
    ImmutableList.copyOf(TableFormat.values());
  private static final List<TableFormat> TEXT_FORMAT_ONLY =
    ImmutableList.of(TableFormat.TEXT);


  /**
   * Run a test file for each of the table format name substitutions for all permutations
   * of the batch sizes and node numbers specified in BATCH_SIZES and NUM_NODE_VALUES.
   */
  private void runQueryUncompressedTextOnly(String testFile, boolean abortOnError,
      int maxErrors) {
    runQueryInAllBatchAndClusterPerms(testFile, abortOnError, maxErrors,
        TEXT_FORMAT_ONLY, ALL_BATCH_SIZES, ALL_CLUSTER_SIZES);
  }

  /**
   * Run a test file for each of the table format name substitutions for all permutations
   * of batchSizes and numNodes.
   */
  private void runQueryInAllBatchAndClusterPerms(String testFile, boolean abortOnError,
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

  static private class TestConfiguration {
    private final int numNodes;
    private final CompressionFormat compressionType;
    private final TableFormat tableFormat;
    private final int batchSize;
    private final boolean disableLlvm;

    public TestConfiguration(int nodes, int batchSize, CompressionFormat compression,
        TableFormat tableFormat, boolean disableLlvm) {
      this.numNodes = nodes;
      this.batchSize = batchSize;
      this.compressionType = compression;
      this.tableFormat = tableFormat;
      this.disableLlvm = disableLlvm;
    }

    public String getTableSuffix() {
      return tableFormat.getTableSuffix() + compressionType.getTableSuffix();
    }

    public int getClusterSize() { return numNodes; }
    public int getBatchSize() { return batchSize; }
    public boolean getDisableLlvm() { return disableLlvm; }

    @Override
    public String toString() {
      return Objects.toStringHelper(this).add("Cluster size", numNodes)
                                         .add("Batch size", batchSize)
                                         .add("Compression type", compressionType)
                                         .add("Table format", tableFormat)
                                         .add("Disable llvm", disableLlvm)
                                         .toString();
    }
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
   * Generates a list of *all* permutations of table format, compression format, batch
   * size and cluster size. No care is taken to filter out permutations which don't
   * make sense. TODO: Add filtering for safety's sake.
   */
  private List<TestConfiguration> generateAllConfigurationPermutations(
      List<TableFormat> tableFormats, List<CompressionFormat> compressionFormats,
      List<Integer> batchSizes, List<Integer> clusterSizes, List<Boolean> llvmOptions) {
    List<TestConfiguration> configs = Lists.newArrayList();
    for (TableFormat tableFormat: tableFormats) {
      for (CompressionFormat compressionFormat: compressionFormats) {
        for (int batchSize: batchSizes) {
          for (int clusterSize: clusterSizes) {
            for (boolean disableLlvm: llvmOptions) {
              configs.add(new TestConfiguration(clusterSize, batchSize, compressionFormat,
               tableFormat, disableLlvm));
            }
          }
        }
      }
    }
    return configs;
  }

  private void runQueryWithTestConfigs(List<TestConfiguration> testConfigs,
      String testFile, boolean abortOnError, int maxErrors) {
    String fileName = testDir + "/" + testFile + ".test";
    TestFileParser queryFileParser = new TestFileParser(fileName);

    LOG.debug("Running the following configurations over file " + fileName + " : ");
    LOG.debug(Joiner.on("\n").join(testConfigs));

    for (TestConfiguration config: testConfigs) {
      queryFileParser.parseFile(config.getTableSuffix());
      runOneQueryTest(queryFileParser, abortOnError, maxErrors, config.getBatchSize(),
          config.getClusterSize(), config.getDisableLlvm(), new StringBuilder());
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
  private void runPairTestFile(String testFile, boolean abortOnError, int maxErrors,
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
  private void runOneQueryTest(TestFileParser queryFileParser,
      boolean abortOnError, int maxErrors,
      int batchSize, int numNodes, boolean disableLlvm, StringBuilder errorLog) {
    for (TestCase testCase: queryFileParser.getTestCases()) {
      List<String> setupSection = testCase.getSectionContents(Section.SETUP);
      ArrayList<String> expectedTypes = testCase.getSectionContents(Section.TYPES);
      ArrayList<String> expectedResults = testCase.getSectionContents(Section.RESULTS);
      ArrayList<String> expectedPartitions =
        testCase.getSectionContents(Section.PARTITIONS);
      ArrayList<String> expectedNumAppendedRows =
        testCase.getSectionContents(Section.NUMROWS);
      // We have to run the setup section once per query, not once per test. Therefore
      // they can be very expensive.
      if (setupSection != null) {
        try {
          runSetupSection(setupSection);
        } catch (Exception e) {
          fail(e.getMessage());
        }
      }

      TestUtils.runQuery(
          executor, testCase.getSectionAsString(Section.QUERY, false, " "),
          numNodes, batchSize, abortOnError, maxErrors, disableLlvm,
          testCase.getStartingLineNum(), null, expectedTypes,
          expectedResults, null, null, expectedPartitions, expectedNumAppendedRows,
          errorLog);
    }
    if (errorLog.length() != 0) {
      fail(errorLog.toString());
    }
  }

  @Test
  public void TestDistinct() {
    runQueryUncompressedTextOnly("distinct", false, 1000);
  }

  @Test
  public void TestAggregation() {
    runQueryUncompressedTextOnly("aggregation", false, 1000);
  }

  @Test
  public void TestExprs() {
    runQueryUncompressedTextOnly("exprs", false, 1000);
  }

  @Test
  public void TestHdfsScanNode() {
    // Run fully distributed (nodes = 0) with all batch sizes.
    runPairTestFile("hdfs-scan-node", false, 1000,
        ALL_TABLE_FORMATS, ALL_COMPRESSION_FORMATS, ALL_BATCH_SIZES, ALL_NODES_ONLY);
    // Run other node numbers with small batch sizes on text
    runQueryInAllBatchAndClusterPerms("hdfs-scan-node", false, 1000, TEXT_FORMAT_ONLY,
        SMALL_BATCH_SIZES, SMALL_CLUSTER_SIZES);
  }

  @Test
  public void TestFilePartitions() {
    // Run fully distributed with all batch sizes.
    runPairTestFile("hdfs-partitions", false, 1000,
        ALL_TABLE_FORMATS, ALL_COMPRESSION_FORMATS, ALL_BATCH_SIZES, ALL_NODES_ONLY);

    // Run other node numbers with small batch sizes on text
    runQueryInAllBatchAndClusterPerms("hdfs-partitions", false, 1000, TEXT_FORMAT_ONLY,
        SMALL_BATCH_SIZES, SMALL_CLUSTER_SIZES);
  }

  @Test
  public void TestHBaseScanNode() {
    runQueryUncompressedTextOnly("hbase-scan-node", false, 1000);
  }

  @Test
  public void TestHBaseRowKeys() {
    runQueryUncompressedTextOnly("hbase-rowkeys", false, 1000);
  }

  @Test
  public void TestHBaseFilters() {
    runQueryUncompressedTextOnly("hbase-filters", false, 1000);
  }

  @Test
  public void TestJoins() {
    runQueryUncompressedTextOnly("joins", false, 1000);
  }

  @Test
  public void TestOuterJoins() {
    runQueryUncompressedTextOnly("outer-joins", false, 1000);
  }

  @Test
  public void TestLimit() {
    runQueryUncompressedTextOnly("limit", false, 1000);
  }

  @Test
  public void TestTopN() {
    runQueryUncompressedTextOnly("top-n", false, 1000);
  }

  @Test
  public void TestEmpty() {
    runQueryUncompressedTextOnly("empty", false, 1000);
  }

  @Test
  public void TestSubquery() {
    runQueryUncompressedTextOnly("subquery", false, 1000);
  }


  @Test
  public void TestInsert() {
    runQueryInAllBatchAndClusterPerms("insert", false, 1000, null,
        ImmutableList.of(0), ImmutableList.of(1));
  }

//  TODO - see hdfs-text-scanner.cc for what needs to be done to support NULL partition
//  keys.
//  @Test
//  public void TestInsertNulls() {
//    runQueryTestFile("insert-nulls", false, 1000);
//  }
}
