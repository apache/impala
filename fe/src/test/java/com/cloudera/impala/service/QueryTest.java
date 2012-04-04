// Copyright (c) 2011 Cloudera, Inc. All rights reserved.

package com.cloudera.impala.service;

import static org.junit.Assert.fail;

import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.log4j.Logger;
import org.junit.BeforeClass;
import org.junit.Test;

import com.cloudera.impala.catalog.Catalog;
import com.cloudera.impala.testutil.TestFileParser;
import com.cloudera.impala.testutil.TestUtils;
import com.cloudera.impala.testutil.TestFileParser.Section;
import com.cloudera.impala.testutil.TestFileParser.TestCase;
import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;

public class QueryTest {
  private static final Logger LOG = Logger.getLogger(QueryTest.class);
  private static Catalog catalog;
  private static Executor executor;
  private final String testDir = "QueryTest";
  private static ArrayList<String> tableSubstitutionList;

  @BeforeClass
  public static void setUp() throws Exception {
    catalog = new Catalog();
    executor = new Executor(catalog);
    tableSubstitutionList = new ArrayList<String>();
    tableSubstitutionList.add("");
    tableSubstitutionList.add("_rc");
    tableSubstitutionList.add("_seq");
    tableSubstitutionList.add("_seq_def");
    tableSubstitutionList.add("_seq_gzip");
    tableSubstitutionList.add("_seq_bzip");
    tableSubstitutionList.add("_seq_snap");
    tableSubstitutionList.add("_seq_record_def");
    tableSubstitutionList.add("_seq_record_gzip");
    tableSubstitutionList.add("_seq_record_bzip");
    tableSubstitutionList.add("_seq_record_snap");
  }

  // Commands recognised as part of the SETUP section
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
        executor.setCatalog(new Catalog());
      }
    }

  }

  private static List<Integer> BATCH_SIZES = ImmutableList.of(0, 16, 1);
  private static List<Integer> NUM_NODES = ImmutableList.of(1, 2, 3, 0);

  private void runQueryTestFile(String testFile, boolean abortOnError, int maxErrors) {
    runQueryTestFile(testFile, abortOnError, maxErrors, null, BATCH_SIZES, NUM_NODES);
  }

  private void runQueryTestFile(String testFile, boolean abortOnError, int maxErrors,
      ArrayList<String> tables, List<Integer> batchSizes, List<Integer> numNodes) {
    String fileName = testDir + "/" + testFile + ".test";
    TestFileParser queryFileParser = new TestFileParser(fileName);
    for (int f = 0; f < (tables == null ? 1 : tables.size()); f++) {
        queryFileParser.parseFile(tables == null ? null : tables.get(f));
      StringBuilder errorLog = new StringBuilder();
      for (TestCase testCase : queryFileParser.getTestCases()) {
        List<String> setupSection = testCase.getSectionContents(Section.SETUP);
        ArrayList<String> expectedTypes =
          testCase.getSectionContents(Section.TYPES);
        ArrayList<String> expectedResults =
          testCase.getSectionContents(Section.RESULTS);
        ArrayList<String> expectedPartitions =
          testCase.getSectionContents(Section.PARTITIONS);
        ArrayList<String> expectedNumAppendedRows =
          testCase.getSectionContents(Section.NUMROWS);
        // run each test against all possible combinations of batch sizes and
        // number of execution nodes
        for (Integer i: batchSizes) {
          for (Integer j: numNodes) {
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
                j, i, abortOnError, maxErrors,
                testCase.getStartingLineNum(), null, expectedTypes,
                expectedResults, null, null, expectedPartitions, expectedNumAppendedRows,
                errorLog);
          }
        }
      }
      if (errorLog.length() != 0) {
        fail(errorLog.toString());
      }
    }
  }


  @Test
  public void TestDistinct() {
    runQueryTestFile("distinct", false, 1000);
  }

  @Test
  public void TestAggregation() {
    runQueryTestFile("aggregation", false, 1000);
  }

  @Test
  public void TestExprs() {
    runQueryTestFile("exprs", false, 1000);
  }

  @Test
  public void TestHdfsScanNode() {
    runQueryTestFile("hdfs-scan-node", false, 1000, tableSubstitutionList, BATCH_SIZES,
        NUM_NODES);
  }

  @Test
  public void TestFilePartitions() {
    runQueryTestFile("hdfs-partitions", false, 1000, tableSubstitutionList, BATCH_SIZES,
        NUM_NODES);
  }

  @Test
  public void TestHBaseScanNode() {
    runQueryTestFile("hbase-scan-node", false, 1000);
  }

  @Test
  public void TestHBaseRowKeys() {
    runQueryTestFile("hbase-rowkeys", false, 1000);
  }

  @Test
  public void TestHBaseFilters() {
    runQueryTestFile("hbase-filters", false, 1000);
  }

  @Test
  public void TestJoins() {
    runQueryTestFile("joins", false, 1000);
  }

  @Test
  public void TestOuterJoins() {
    runQueryTestFile("outer-joins", false, 1000);
  }

  @Test
  public void TestLimit() {
    runQueryTestFile("limit", false, 1000);
  }

  @Test
  public void TestTopN() {
    runQueryTestFile("top-n", false, 1000);
  }

  @Test
  public void TestEmpty() {
    runQueryTestFile("empty", false, 1000);
  }

  @Test
  public void TestSubquery() {
    runQueryTestFile("subquery", false, 1000);
  }


  @Test
  public void TestInsert() {
    runQueryTestFile("insert", false, 1000, null,
        ImmutableList.of(0), ImmutableList.of(1));
  }

//  TODO - see hdfs-text-scanner.cc for what needs to be done to support NULL partition
//  keys.
//  @Test
//  public void TestInsertNulls() {
//    runQueryTestFile("insert-nulls", false, 1000);
//  }
}
