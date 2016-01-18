// Copyright (c) 2012 Cloudera, Inc. All rights reserved.
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

package com.cloudera.impala.planner;

import static org.junit.Assert.fail;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.lang.exception.ExceptionUtils;
import org.apache.hadoop.fs.Path;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.cloudera.impala.analysis.ColumnLineageGraph;
import com.cloudera.impala.authorization.AuthorizationConfig;
import com.cloudera.impala.catalog.CatalogException;
import com.cloudera.impala.common.ImpalaException;
import com.cloudera.impala.common.NotImplementedException;
import com.cloudera.impala.common.RuntimeEnv;
import com.cloudera.impala.service.Frontend;
import com.cloudera.impala.testutil.ImpaladTestCatalog;
import com.cloudera.impala.testutil.TestFileParser;
import com.cloudera.impala.testutil.TestFileParser.Section;
import com.cloudera.impala.testutil.TestFileParser.TestCase;
import com.cloudera.impala.testutil.TestUtils;
import com.cloudera.impala.thrift.ImpalaInternalServiceConstants;
import com.cloudera.impala.thrift.TDescriptorTable;
import com.cloudera.impala.thrift.TExecRequest;
import com.cloudera.impala.thrift.TExplainLevel;
import com.cloudera.impala.thrift.THBaseKeyRange;
import com.cloudera.impala.thrift.THdfsFileSplit;
import com.cloudera.impala.thrift.THdfsPartition;
import com.cloudera.impala.thrift.THdfsScanNode;
import com.cloudera.impala.thrift.THdfsTable;
import com.cloudera.impala.thrift.TLineageGraph;
import com.cloudera.impala.thrift.TNetworkAddress;
import com.cloudera.impala.thrift.TPlanFragment;
import com.cloudera.impala.thrift.TPlanNode;
import com.cloudera.impala.thrift.TQueryCtx;
import com.cloudera.impala.thrift.TQueryExecRequest;
import com.cloudera.impala.thrift.TQueryOptions;
import com.cloudera.impala.thrift.TScanRangeLocations;
import com.cloudera.impala.thrift.TTableDescriptor;
import com.cloudera.impala.thrift.TTupleDescriptor;
import com.cloudera.impala.thrift.TUpdateMembershipRequest;
import com.cloudera.impala.util.MembershipSnapshot;
import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

public class PlannerTestBase {
  private final static Logger LOG = LoggerFactory.getLogger(PlannerTest.class);
  private final static boolean GENERATE_OUTPUT_FILE = true;
  private static Frontend frontend_ = new Frontend(
      AuthorizationConfig.createAuthDisabledConfig(), new ImpaladTestCatalog());
  private final String testDir_ = "functional-planner/queries/PlannerTest";
  private final String outDir_ = "/tmp/PlannerTest/";

  // Map from plan ID (TPlanNodeId) to the plan node with that ID.
  private final Map<Integer, TPlanNode> planMap_ = Maps.newHashMap();
  // Map from tuple ID (TTupleId) to the tuple descriptor with that ID.
  private final Map<Integer, TTupleDescriptor> tupleMap_ = Maps.newHashMap();
  // Map from table ID (TTableId) to the table descriptor with that ID.
  private final Map<Integer, TTableDescriptor> tableMap_ = Maps.newHashMap();

  @BeforeClass
  public static void setUp() throws Exception {
    // Use 8 cores for resource estimation.
    RuntimeEnv.INSTANCE.setNumCores(8);
    // Set test env to control the explain level.
    RuntimeEnv.INSTANCE.setTestEnv(true);
    // Mimic the 3 node test mini-cluster.
    TUpdateMembershipRequest updateReq = new TUpdateMembershipRequest();
    updateReq.setIp_addresses(Sets.newHashSet("127.0.0.1"));
    updateReq.setHostnames(Sets.newHashSet("localhost"));
    updateReq.setNum_nodes(3);
    MembershipSnapshot.update(updateReq);
  }

  @AfterClass
  public static void cleanUp() {
    RuntimeEnv.INSTANCE.reset();
  }

  /**
   * Clears the old maps and constructs new maps based on the new
   * execRequest so that findPartitions() can locate various thrift
   * metadata structures quickly.
   */
  private void buildMaps(TQueryExecRequest execRequest) {
    // Build maps that will be used by findPartition().
    planMap_.clear();
    tupleMap_.clear();
    tableMap_.clear();
    for (TPlanFragment frag: execRequest.fragments) {
      for (TPlanNode node: frag.plan.nodes) {
        planMap_.put(node.node_id, node);
      }
    }
    if (execRequest.isSetDesc_tbl()) {
      TDescriptorTable descTbl = execRequest.desc_tbl;
      for (TTupleDescriptor tupleDesc: descTbl.tupleDescriptors) {
        tupleMap_.put(tupleDesc.id, tupleDesc);
      }
      if (descTbl.isSetTableDescriptors()) {
        for (TTableDescriptor tableDesc: descTbl.tableDescriptors) {
          tableMap_.put(tableDesc.id, tableDesc);
        }
      }
    }
  }

  /**
   * Look up the partition corresponding to the plan node (identified by
   * nodeId) and a file split.
   */
  private THdfsPartition findPartition(int nodeId, THdfsFileSplit split) {
    TPlanNode node = planMap_.get(nodeId);
    Preconditions.checkNotNull(node);
    Preconditions.checkState(node.node_id == nodeId && node.isSetHdfs_scan_node());
    THdfsScanNode scanNode = node.getHdfs_scan_node();
    int tupleId = scanNode.getTuple_id();
    TTupleDescriptor tupleDesc = tupleMap_.get(tupleId);
    Preconditions.checkNotNull(tupleDesc);
    Preconditions.checkState(tupleDesc.id == tupleId);
    TTableDescriptor tableDesc = tableMap_.get(tupleDesc.tableId);
    Preconditions.checkNotNull(tableDesc);
    Preconditions.checkState(tableDesc.id == tupleDesc.tableId &&
        tableDesc.isSetHdfsTable());
    THdfsTable hdfsTable = tableDesc.getHdfsTable();
    THdfsPartition partition = hdfsTable.getPartitions().get(split.partition_id);
    Preconditions.checkNotNull(partition);
    Preconditions.checkState(partition.id == split.partition_id);
    return partition;
  }

  /**
   * Verify that all THdfsPartitions included in the descriptor table are referenced by
   * at least one scan range or part of an inserted table.  PrintScanRangeLocations
   * will implicitly verify the converse (it'll fail if a scan range references a
   * table/partition descriptor that is not present).
   */
  private void testHdfsPartitionsReferenced(TQueryExecRequest execRequest,
      String query, StringBuilder errorLog) {
    long insertTableId = -1;
    // Collect all partitions that are referenced by a scan range.
    Set<THdfsPartition> scanRangePartitions = Sets.newHashSet();
    if (execRequest.per_node_scan_ranges != null) {
      for (Map.Entry<Integer, List<TScanRangeLocations>> entry:
           execRequest.per_node_scan_ranges.entrySet()) {
        if (entry.getValue() == null) {
          continue;
        }
        for (TScanRangeLocations locations: entry.getValue()) {
          if (locations.scan_range.isSetHdfs_file_split()) {
            THdfsFileSplit split = locations.scan_range.getHdfs_file_split();
            THdfsPartition partition = findPartition(entry.getKey(), split);
            scanRangePartitions.add(partition);
          }
        }
      }
    }
    if (execRequest.isSetFinalize_params()) {
      insertTableId = execRequest.getFinalize_params().getTable_id();
    }
    boolean first = true;
    // Iterate through all partitions of the descriptor table and verify all partitions
    // are referenced.
    if (execRequest.isSetDesc_tbl() && execRequest.desc_tbl.isSetTableDescriptors()) {
      for (TTableDescriptor tableDesc: execRequest.desc_tbl.tableDescriptors) {
        // All partitions of insertTableId are okay.
        if (tableDesc.getId() == insertTableId) continue;
        if (!tableDesc.isSetHdfsTable()) continue;
        THdfsTable hdfsTable = tableDesc.getHdfsTable();
        for (Map.Entry<Long, THdfsPartition> e :
             hdfsTable.getPartitions().entrySet()) {
          THdfsPartition partition = e.getValue();
          if (!scanRangePartitions.contains(partition)) {
            if (first) errorLog.append("query:\n" + query + "\n");
            errorLog.append(
                " unreferenced partition: HdfsTable: " + tableDesc.getId() +
                " HdfsPartition: " + partition.getId() + "\n");
            first = false;
          }
        }
      }
    }
  }

  /**
   * Construct a string representation of the scan ranges for this request.
   */
  private StringBuilder PrintScanRangeLocations(TQueryExecRequest execRequest) {
    StringBuilder result = new StringBuilder();
    if (execRequest.per_node_scan_ranges == null) {
      return result;
    }
    for (Map.Entry<Integer, List<TScanRangeLocations>> entry:
        execRequest.per_node_scan_ranges.entrySet()) {
      result.append("NODE " + entry.getKey().toString() + ":\n");
      if (entry.getValue() == null) {
        continue;
      }

      for (TScanRangeLocations locations: entry.getValue()) {
        // print scan range
        result.append("  ");
        if (locations.scan_range.isSetHdfs_file_split()) {
          THdfsFileSplit split = locations.scan_range.getHdfs_file_split();
          THdfsPartition partition = findPartition(entry.getKey(), split);
          Path filePath = new Path(partition.getLocation(), split.file_name);
          filePath = cleanseFilePath(filePath);
          result.append("HDFS SPLIT " + filePath.toString() + " "
              + Long.toString(split.offset) + ":" + Long.toString(split.length));
        }
        if (locations.scan_range.isSetHbase_key_range()) {
          THBaseKeyRange keyRange = locations.scan_range.getHbase_key_range();
          Integer hostIdx = locations.locations.get(0).host_idx;
          TNetworkAddress networkAddress = execRequest.getHost_list().get(hostIdx);
          result.append("HBASE KEYRANGE ");
          result.append("port=" + networkAddress.port + " ");
          if (keyRange.isSetStartKey()) {
            result.append(HBaseScanNode.printKey(keyRange.getStartKey().getBytes()));
          } else {
            result.append("<unbounded>");
          }
          result.append(":");
          if (keyRange.isSetStopKey()) {
            result.append(HBaseScanNode.printKey(keyRange.getStopKey().getBytes()));
          } else {
            result.append("<unbounded>");
          }
        }
        result.append("\n");
      }
    }
    return result;
  }

  /**
   * Normalize components of the given file path, removing any environment- or test-run
   * dependent components.  For example, substitutes the unique id portion of Impala
   * generated file names with a fixed literal.  Subclasses should override to do
   * filesystem specific cleansing.
   */
  protected Path cleanseFilePath(Path path) {
    String fileName = path.getName();
    Pattern pattern = Pattern.compile("\\w{16}-\\w{16}_\\d+_data");
    Matcher matcher = pattern.matcher(fileName);
    fileName = matcher.replaceFirst("<UID>_data");
    return new Path(path.getParent(), fileName);
  }

  /**
   * Extracts and returns the expected error message from expectedPlan.
   * Returns null if expectedPlan is empty or its first element is not an error message.
   * The accepted format for error messages is 'not implemented: expected error message'
   * Returns the empty string if expectedPlan starts with 'not implemented' but no
   * expected error message was given.
   */
  private String getExpectedErrorMessage(ArrayList<String> expectedPlan) {
    if (expectedPlan == null || expectedPlan.isEmpty()) return null;
    if (!expectedPlan.get(0).toLowerCase().startsWith("not implemented")) return null;
    // Find first ':' and extract string on right hand side as error message.
    int ix = expectedPlan.get(0).indexOf(":");
    if (ix + 1 > 0) {
      return expectedPlan.get(0).substring(ix + 1).trim();
    } else {
      return "";
    }
  }

  private void handleNotImplException(String query, String expectedErrorMsg,
      StringBuilder errorLog, StringBuilder actualOutput, Throwable e) {
    boolean isImplemented = expectedErrorMsg == null;
    actualOutput.append("not implemented: " + e.getMessage() + "\n");
    if (isImplemented) {
      errorLog.append("query:\n" + query + "\nPLAN not implemented: "
          + e.getMessage() + "\n");
    } else {
      // Compare actual and expected error messages.
      if (expectedErrorMsg != null && !expectedErrorMsg.isEmpty()) {
        if (!e.getMessage().toLowerCase().startsWith(expectedErrorMsg.toLowerCase())) {
          errorLog.append("query:\n" + query + "\nExpected error message: '"
              + expectedErrorMsg + "'\nActual error message: '"
              + e.getMessage() + "'\n");
        }
      }
    }
  }

  /**
   * Merge the options of b into a and return a
   */
  private TQueryOptions mergeQueryOptions(TQueryOptions a, TQueryOptions b) {
    for(TQueryOptions._Fields f : TQueryOptions._Fields.values()) {
      if (b.isSet(f)) {
        a.setFieldValue(f, b.getFieldValue(f));
      }
    }
    return a;
  }

  private TQueryOptions defaultQueryOptions() {
    TQueryOptions options = new TQueryOptions();
    options.setExplain_level(TExplainLevel.STANDARD);
    options.setAllow_unsupported_formats(true);
    options.setExec_single_node_rows_threshold(0);
    return options;
  }

  /**
   * Produces single-node and distributed plans for testCase and compares
   * plan and scan range results.
   * Appends the actual single-node and distributed plan as well as the printed
   * scan ranges to actualOutput, along with the requisite section header.
   * locations to actualScanRangeLocations; compares both to the appropriate sections
   * of 'testCase'.
   */
  private void RunTestCase(TestCase testCase, StringBuilder errorLog,
      StringBuilder actualOutput, String dbName, TQueryOptions options)
      throws CatalogException {

    if (options == null) {
      options = defaultQueryOptions();
    } else {
      options = mergeQueryOptions(defaultQueryOptions(), options);
    }

    String query = testCase.getQuery();
    LOG.info("running query " + query);
    if (query.isEmpty()) {
      throw new IllegalStateException("Cannot plan empty query in line: " +
          testCase.getStartingLineNum());
    }
    TQueryCtx queryCtx = TestUtils.createQueryContext(
        dbName, System.getProperty("user.name"));
    queryCtx.request.query_options = options;
    // Test single node plan, scan range locations, and column lineage.
    TExecRequest singleNodeExecRequest =
        testPlan(testCase, true, queryCtx, errorLog, actualOutput);
    checkScanRangeLocations(testCase, singleNodeExecRequest, errorLog, actualOutput);
    checkColumnLineage(testCase, singleNodeExecRequest, errorLog, actualOutput);
    // Test distributed plan.
    testPlan(testCase, false, queryCtx, errorLog, actualOutput);
  }

  /**
   * Produces the single-node or distributed plan for testCase and compares the
   * actual/expected plans if the corresponding test section exists in testCase.
   *
   * Returns the produced exec request or null if there was an error generating
   * the plan.
   */
  private TExecRequest testPlan(TestCase testCase, boolean singleNodePlan,
      TQueryCtx queryCtx, StringBuilder errorLog, StringBuilder actualOutput) {
    Section section = (singleNodePlan) ? Section.PLAN : Section.DISTRIBUTEDPLAN;
    String query = testCase.getQuery();
    queryCtx.request.setStmt(query);
    if (section == Section.PLAN) {
      queryCtx.request.getQuery_options().setNum_nodes(1);
    } else {
      queryCtx.request.getQuery_options().setNum_nodes(
          ImpalaInternalServiceConstants.NUM_NODES_ALL);
    }
    ArrayList<String> expectedPlan = testCase.getSectionContents(section);
    boolean sectionExists = expectedPlan != null && !expectedPlan.isEmpty();
    String expectedErrorMsg = getExpectedErrorMessage(expectedPlan);

    StringBuilder explainBuilder = new StringBuilder();
    TExecRequest execRequest = null;
    actualOutput.append(section.getHeader() + "\n");
    try {
      execRequest = frontend_.createExecRequest(queryCtx, explainBuilder);
    } catch (NotImplementedException e) {
      if (!sectionExists) return null;
      handleNotImplException(query, expectedErrorMsg, errorLog, actualOutput, e);
    } catch (Exception e) {
      errorLog.append(String.format("Query:\n%s\nError Stack:\n%s\n", query,
          ExceptionUtils.getStackTrace(e)));
    }
    // No expected plan was specified for section. Skip expected/actual comparison.
    if (!sectionExists) return execRequest;
    // Failed to produce an exec request.
    if (execRequest == null) return null;

    String explainStr = removeExplainHeader(explainBuilder.toString());
    actualOutput.append(explainStr);
    LOG.info(section.toString() + ":" + explainStr);
    if (expectedErrorMsg != null) {
      errorLog.append(String.format(
          "\nExpected failure, but query produced %s.\nQuery:\n%s\n\n%s:\n%s",
          section, query, section, explainStr));
    } else {
      String planDiff = TestUtils.compareOutput(
          Lists.newArrayList(explainStr.split("\n")), expectedPlan, true, true);
      if (!planDiff.isEmpty()) {
        errorLog.append(String.format(
            "\nSection %s of query:\n%s\n\n%s", section, query, planDiff));
        // Append the VERBOSE explain plan because it contains details about
        // tuples/sizes/cardinality for easier debugging.
        String verbosePlan = getVerboseExplainPlan(queryCtx);
        errorLog.append("\nVerbose plan:\n" + verbosePlan);
      }
    }
    return execRequest;
  }

  /**
   * Returns the VERBOSE explain plan for the given queryCtx, or a stack trace
   * if an error occurred while creating the plan.
   */
  private String getVerboseExplainPlan(TQueryCtx queryCtx) {
    StringBuilder explainBuilder = new StringBuilder();
    TExecRequest execRequest = null;
    TExplainLevel origExplainLevel =
        queryCtx.request.getQuery_options().getExplain_level();
    try {
      queryCtx.request.getQuery_options().setExplain_level(TExplainLevel.VERBOSE);
      execRequest = frontend_.createExecRequest(queryCtx, explainBuilder);
    } catch (ImpalaException e) {
      return ExceptionUtils.getStackTrace(e);
    } finally {
      queryCtx.request.getQuery_options().setExplain_level(origExplainLevel);
    }
    Preconditions.checkNotNull(execRequest);
    String explainStr = removeExplainHeader(explainBuilder.toString());
    return explainStr;
  }

  private void checkScanRangeLocations(TestCase testCase, TExecRequest execRequest,
      StringBuilder errorLog, StringBuilder actualOutput) {
    String query = testCase.getQuery();
    // Query exec request may not be set for DDL, e.g., CTAS.
    String locationsStr = null;
    if (execRequest != null && execRequest.isSetQuery_exec_request()) {
      buildMaps(execRequest.query_exec_request);
      testHdfsPartitionsReferenced(execRequest.query_exec_request, query, errorLog);
      locationsStr =
          PrintScanRangeLocations(execRequest.query_exec_request).toString();
    }

    // compare scan range locations
    LOG.info("scan range locations: " + locationsStr);
    ArrayList<String> expectedLocations =
        testCase.getSectionContents(Section.SCANRANGELOCATIONS);

    if (expectedLocations.size() > 0 && locationsStr != null) {
      // Locations' order does not matter.
      String result = TestUtils.compareOutput(
          Lists.newArrayList(locationsStr.split("\n")), expectedLocations, false, false);
      if (!result.isEmpty()) {
        errorLog.append("section " + Section.SCANRANGELOCATIONS + " of query:\n"
            + query + "\n" + result);
      }
      actualOutput.append(Section.SCANRANGELOCATIONS.getHeader() + "\n");
      // Print the locations out sorted since the order is random and messed up
      // the diffs. The values in locationStr contains "Node X" labels as well
      // as paths.
      ArrayList<String> locations = Lists.newArrayList(locationsStr.split("\n"));
      ArrayList<String> perNodeLocations = Lists.newArrayList();

      for (int i = 0; i < locations.size(); ++i) {
        if (locations.get(i).startsWith("NODE")) {
          if (!perNodeLocations.isEmpty()) {
            Collections.sort(perNodeLocations);
            actualOutput.append(Joiner.on("\n").join(perNodeLocations)).append("\n");
            perNodeLocations.clear();
          }
          actualOutput.append(locations.get(i)).append("\n");
        } else {
          perNodeLocations.add(locations.get(i));
        }
      }

      if (!perNodeLocations.isEmpty()) {
        Collections.sort(perNodeLocations);
        actualOutput.append(Joiner.on("\n").join(perNodeLocations)).append("\n");
      }

      // TODO: check that scan range locations are identical in both cases
    }
  }

  private void checkColumnLineage(TestCase testCase, TExecRequest execRequest,
      StringBuilder errorLog, StringBuilder actualOutput) {
    String query = testCase.getQuery();
    ArrayList<String> expectedLineage = testCase.getSectionContents(Section.LINEAGE);
    if (expectedLineage == null || expectedLineage.isEmpty()) return;
    TLineageGraph lineageGraph = null;
    if (execRequest != null && execRequest.isSetQuery_exec_request()) {
      lineageGraph = execRequest.query_exec_request.lineage_graph;
    } else if (execRequest.isSetCatalog_op_request()) {
      lineageGraph = execRequest.catalog_op_request.lineage_graph;
    }
    ArrayList<String> expected =
      testCase.getSectionContents(Section.LINEAGE);
    if (expected.size() > 0 && lineageGraph != null) {
      String serializedGraph = Joiner.on("\n").join(expected);
      ColumnLineageGraph expectedGraph =
        ColumnLineageGraph.createFromJSON(serializedGraph);
      ColumnLineageGraph outputGraph =
        ColumnLineageGraph.fromThrift(lineageGraph);
      if (expectedGraph == null || outputGraph == null ||
          !outputGraph.equals(expectedGraph)) {
        StringBuilder lineageError = new StringBuilder();
        lineageError.append("section " + Section.LINEAGE + " of query:\n"
            + query + "\n");
        lineageError.append("Output:\n");
        lineageError.append(outputGraph.toJson() + "\n");
        lineageError.append("Expected:\n");
        lineageError.append(serializedGraph + "\n");
        errorLog.append(lineageError.toString());
      }
      actualOutput.append(Section.LINEAGE.getHeader());
      actualOutput.append(TestUtils.prettyPrintJson(outputGraph.toJson()));
      actualOutput.append("\n");
    }
  }

  /**
   * Strips out the header containing resource estimates and the warning about missing
   * stats from the given explain plan, because the estimates can change easily with
   * stats/cardinality.
   */
  private String removeExplainHeader(String explain) {
    String[] lines = explain.split("\n");
    // Find the first empty line - the end of the header.
    for (int i = 0; i < lines.length - 1; ++i) {
      if (lines[i].isEmpty()) {
        return Joiner.on("\n").join(Arrays.copyOfRange(lines, i + 1 , lines.length))
            + "\n";
      }
    }
    return explain;
  }

  protected void runPlannerTestFile(String testFile, TQueryOptions options) {
    runPlannerTestFile(testFile, "default", options);
  }

  private void runPlannerTestFile(String testFile, String dbName, TQueryOptions options) {
    String fileName = testDir_ + "/" + testFile + ".test";
    TestFileParser queryFileParser = new TestFileParser(fileName);
    StringBuilder actualOutput = new StringBuilder();

    queryFileParser.parseFile();
    StringBuilder errorLog = new StringBuilder();
    for (TestCase testCase : queryFileParser.getTestCases()) {
      actualOutput.append(testCase.getSectionAsString(Section.QUERY, true, "\n"));
      actualOutput.append("\n");
      try {
        RunTestCase(testCase, errorLog, actualOutput, dbName, options);
      } catch (CatalogException e) {
        errorLog.append(String.format("Failed to plan query\n%s\n%s",
            testCase.getQuery(), e.getMessage()));
      }
      actualOutput.append("====\n");
    }

    // Create the actual output file
    if (GENERATE_OUTPUT_FILE) {
      try {
        File outDirFile = new File(outDir_);
        outDirFile.mkdirs();
        FileWriter fw = new FileWriter(outDir_ + testFile + ".test");
        fw.write(actualOutput.toString());
        fw.close();
      } catch (IOException e) {
        errorLog.append("Unable to create output file: " + e.getMessage());
      }
    }

    if (errorLog.length() != 0) {
      fail(errorLog.toString());
    }
  }

  protected void runPlannerTestFile(String testFile) {
    runPlannerTestFile(testFile, "default", null);
  }

  protected void runPlannerTestFile(String testFile, String dbName) {
    runPlannerTestFile(testFile, dbName, null);
  }
}

