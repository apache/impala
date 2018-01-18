// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package org.apache.impala.planner;

import static org.junit.Assert.fail;

import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.lang.exception.ExceptionUtils;
import org.apache.hadoop.fs.Path;
import org.apache.impala.analysis.ColumnLineageGraph;
import org.apache.impala.analysis.DescriptorTable;
import org.apache.impala.catalog.Catalog;
import org.apache.impala.catalog.CatalogException;
import org.apache.impala.common.FrontendTestBase;
import org.apache.impala.common.ImpalaException;
import org.apache.impala.common.NotImplementedException;
import org.apache.impala.common.RuntimeEnv;
import org.apache.impala.testutil.TestFileParser;
import org.apache.impala.testutil.TestFileParser.Section;
import org.apache.impala.testutil.TestFileParser.TestCase;
import org.apache.impala.testutil.TestUtils;
import org.apache.impala.thrift.ImpalaInternalServiceConstants;
import org.apache.impala.thrift.TDescriptorTable;
import org.apache.impala.thrift.TExecRequest;
import org.apache.impala.thrift.TExplainLevel;
import org.apache.impala.thrift.THBaseKeyRange;
import org.apache.impala.thrift.THdfsFileSplit;
import org.apache.impala.thrift.THdfsPartition;
import org.apache.impala.thrift.THdfsPartitionLocation;
import org.apache.impala.thrift.THdfsScanNode;
import org.apache.impala.thrift.THdfsTable;
import org.apache.impala.thrift.TLineageGraph;
import org.apache.impala.thrift.TNetworkAddress;
import org.apache.impala.thrift.TPlanExecInfo;
import org.apache.impala.thrift.TPlanFragment;
import org.apache.impala.thrift.TPlanNode;
import org.apache.impala.thrift.TQueryCtx;
import org.apache.impala.thrift.TQueryExecRequest;
import org.apache.impala.thrift.TQueryOptions;
import org.apache.impala.thrift.TScanRangeLocationList;
import org.apache.impala.thrift.TTableDescriptor;
import org.apache.impala.thrift.TTableSink;
import org.apache.impala.thrift.TTupleDescriptor;
import org.apache.impala.thrift.TUpdateMembershipRequest;
import org.apache.impala.util.MembershipSnapshot;
import org.apache.kudu.client.KuduClient;
import org.apache.kudu.client.KuduScanToken;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Before;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

public class PlannerTestBase extends FrontendTestBase {
  private final static Logger LOG = LoggerFactory.getLogger(PlannerTest.class);
  private final static boolean GENERATE_OUTPUT_FILE = true;
  private final java.nio.file.Path testDir_ = Paths.get("functional-planner", "queries",
      "PlannerTest");
  private static java.nio.file.Path outDir_;
  private static KuduClient kuduClient_;

  // Map from plan ID (TPlanNodeId) to the plan node with that ID.
  private final Map<Integer, TPlanNode> planMap_ = Maps.newHashMap();
  // Map from tuple ID (TTupleId) to the tuple descriptor with that ID.
  private final Map<Integer, TTupleDescriptor> tupleMap_ = Maps.newHashMap();
  // Map from table ID (TTableId) to the table descriptor with that ID.
  private final Map<Integer, TTableDescriptor> tableMap_ = Maps.newHashMap();

  @BeforeClass
  public static void setUp() throws Exception {
    // Mimic the 3 node test mini-cluster.
    TUpdateMembershipRequest updateReq = new TUpdateMembershipRequest();
    updateReq.setIp_addresses(Sets.newHashSet("127.0.0.1"));
    updateReq.setHostnames(Sets.newHashSet("localhost"));
    updateReq.setNum_nodes(3);
    MembershipSnapshot.update(updateReq);

    if (RuntimeEnv.INSTANCE.isKuduSupported()) {
      kuduClient_ = new KuduClient.KuduClientBuilder("127.0.0.1:7051").build();
    }
    String logDir = System.getenv("IMPALA_FE_TEST_LOGS_DIR");
    if (logDir == null) logDir = "/tmp";
    outDir_ = Paths.get(logDir, "PlannerTest");
  }

  @Before
  public void setUpTest() throws Exception {
    // Reset the RuntimeEnv - individual tests may change it.
    RuntimeEnv.INSTANCE.reset();
    // Use 8 cores for resource estimation.
    RuntimeEnv.INSTANCE.setNumCores(8);
    // Set test env to control the explain level.
    RuntimeEnv.INSTANCE.setTestEnv(true);
  }

  @AfterClass
  public static void cleanUp() throws Exception {
    RuntimeEnv.INSTANCE.reset();

    if (kuduClient_ != null) {
      kuduClient_.close();
      kuduClient_ = null;
    }
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
    for (TPlanExecInfo execInfo: execRequest.plan_exec_info) {
      for (TPlanFragment frag: execInfo.fragments) {
        for (TPlanNode node: frag.plan.nodes) {
          planMap_.put(node.node_id, node);
        }
      }
    }
    if (execRequest.query_ctx.isSetDesc_tbl()) {
      TDescriptorTable descTbl = execRequest.query_ctx.desc_tbl;
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
   * Look up the table corresponding to the plan node (identified by
   * nodeId).
   */
  private THdfsTable findTable(int nodeId) {
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
    return tableDesc.getHdfsTable();
  }

  /**
   * Look up the partition corresponding to the plan node (identified by
   * nodeId) and a file split.
   */
  private THdfsPartition findPartition(int nodeId, THdfsFileSplit split) {
    THdfsTable hdfsTable = findTable(nodeId);
    THdfsPartition partition = hdfsTable.getPartitions().get(split.partition_id);
    Preconditions.checkNotNull(partition);
    Preconditions.checkState(partition.id == split.partition_id);
    return partition;
  }

  /**
   * Verify that all THdfsPartitions included in the descriptor table are referenced by
   * at least one scan range or part of an inserted table.  printScanRangeLocations()
   * will implicitly verify the converse (it'll fail if a scan range references a
   * table/partition descriptor that is not present).
   */
  private void testHdfsPartitionsReferenced(TQueryExecRequest execRequest,
      String query, StringBuilder errorLog) {
    long insertTableId = -1;
    // Collect all partitions that are referenced by a scan range.
    Set<THdfsPartition> scanRangePartitions = Sets.newHashSet();
    for (TPlanExecInfo execInfo: execRequest.plan_exec_info) {
      if (execInfo.per_node_scan_ranges != null) {
        for (Map.Entry<Integer, List<TScanRangeLocationList>> entry:
             execInfo.per_node_scan_ranges.entrySet()) {
          if (entry.getValue() == null) {
            continue;
          }
          for (TScanRangeLocationList locationList: entry.getValue()) {
            if (locationList.scan_range.isSetHdfs_file_split()) {
              THdfsFileSplit split = locationList.scan_range.getHdfs_file_split();
              THdfsPartition partition = findPartition(entry.getKey(), split);
              scanRangePartitions.add(partition);
            }
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
    if (execRequest.query_ctx.isSetDesc_tbl()
        && execRequest.query_ctx.desc_tbl.isSetTableDescriptors()) {
      for (TTableDescriptor tableDesc: execRequest.query_ctx.desc_tbl.tableDescriptors) {
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
  private StringBuilder printScanRangeLocations(TQueryExecRequest execRequest) {
    StringBuilder result = new StringBuilder();
    for (TPlanExecInfo execInfo: execRequest.plan_exec_info) {
      if (execInfo.per_node_scan_ranges == null) continue;
      for (Map.Entry<Integer, List<TScanRangeLocationList>> entry:
          execInfo.per_node_scan_ranges.entrySet()) {
        result.append("NODE " + entry.getKey().toString() + ":\n");
        if (entry.getValue() == null) continue;

        for (TScanRangeLocationList locations: entry.getValue()) {
          // print scan range
          result.append("  ");
          if (locations.scan_range.isSetHdfs_file_split()) {
            THdfsFileSplit split = locations.scan_range.getHdfs_file_split();
            THdfsTable table = findTable(entry.getKey());
            THdfsPartition partition = table.getPartitions().get(split.partition_id);
            THdfsPartitionLocation location = partition.getLocation();
            String file_location = location.getSuffix();
            if (location.prefix_index != -1) {
              file_location =
                  table.getPartition_prefixes().get(location.prefix_index) + file_location;
            }
            Path filePath = new Path(file_location, split.file_name);
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

          if (locations.scan_range.isSetKudu_scan_token()) {
            Preconditions.checkNotNull(kuduClient_,
                "Test should not be invoked on platforms that do not support Kudu.");
            try {
              result.append(KuduScanToken.stringifySerializedToken(
                  locations.scan_range.kudu_scan_token.array(), kuduClient_));
            } catch (IOException e) {
              throw new IllegalStateException("Unable to parse Kudu scan token", e);
            }
          }
          result.append("\n");
        }
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
  protected TQueryOptions mergeQueryOptions(TQueryOptions a, TQueryOptions b) {
    for(TQueryOptions._Fields f : TQueryOptions._Fields.values()) {
      if (b.isSet(f)) {
        a.setFieldValue(f, b.getFieldValue(f));
      }
    }
    return a;
  }

  protected TQueryOptions defaultQueryOptions() {
    TQueryOptions options = new TQueryOptions();
    options.setExplain_level(TExplainLevel.STANDARD);
    options.setAllow_unsupported_formats(true);
    options.setExec_single_node_rows_threshold(0);
    return options;
  }

  /**
   * Produces single-node, distributed, and parallel plans for testCase and compares
   * plan and scan range results.
   * Appends the actual plans as well as the printed
   * scan ranges to actualOutput, along with the requisite section header.
   * locations to actualScanRangeLocations; compares both to the appropriate sections
   * of 'testCase'.
   */
  private void runTestCase(TestCase testCase, StringBuilder errorLog,
      StringBuilder actualOutput, String dbName, boolean ignoreExplainHeader)
      throws CatalogException {
    String query = testCase.getQuery();
    LOG.info("running query " + query);
    if (query.isEmpty()) {
      throw new IllegalStateException("Cannot plan empty query in line: " +
          testCase.getStartingLineNum());
    }
    // Set up the query context. Note that we need to deep copy it before planning each
    // time since planning modifies it.
    TQueryCtx queryCtx = TestUtils.createQueryContext(
        dbName, System.getProperty("user.name"));
    queryCtx.client_request.query_options = testCase.getOptions();
    // Test single node plan, scan range locations, and column lineage.
    TExecRequest singleNodeExecRequest = testPlan(testCase, Section.PLAN, queryCtx.deepCopy(),
        ignoreExplainHeader, errorLog, actualOutput);
    validateTableIds(singleNodeExecRequest);
    checkScanRangeLocations(testCase, singleNodeExecRequest, errorLog, actualOutput);
    checkColumnLineage(testCase, singleNodeExecRequest, errorLog, actualOutput);
    checkLimitCardinality(query, singleNodeExecRequest, errorLog);
    // Test distributed plan.
    testPlan(testCase, Section.DISTRIBUTEDPLAN, queryCtx.deepCopy(), ignoreExplainHeader,
        errorLog, actualOutput);
    // test parallel plans
    testPlan(testCase, Section.PARALLELPLANS, queryCtx.deepCopy(), ignoreExplainHeader,
        errorLog, actualOutput);
  }

  /**
   * Validate that all tables in the descriptor table of 'request' have a unique id and
   * those are properly referenced by tuple descriptors and table sink.
   */
  private void validateTableIds(TExecRequest request) {
    if (request == null || !request.isSetQuery_exec_request()) return;
    TQueryExecRequest execRequest = request.query_exec_request;
    HashSet<Integer> seenTableIds = Sets.newHashSet();
    if (execRequest.query_ctx.isSetDesc_tbl()) {
      TDescriptorTable descTbl = execRequest.query_ctx.desc_tbl;
      if (descTbl.isSetTableDescriptors()) {
        for (TTableDescriptor tableDesc: descTbl.tableDescriptors) {
          if (seenTableIds.contains(tableDesc.id)) {
            throw new IllegalStateException("Failed to verify table id for table: " +
                tableDesc.getDbName() + "." + tableDesc.getTableName() +
                ".\nTable id: " + tableDesc.id + " already used.");
          }
          seenTableIds.add(tableDesc.id);
        }
      }

      if (descTbl.isSetTupleDescriptors()) {
        for (TTupleDescriptor tupleDesc: descTbl.tupleDescriptors) {
          if (tupleDesc.isSetTableId() && !seenTableIds.contains(tupleDesc.tableId)) {
            throw new IllegalStateException("TableDescriptor does not include table id" +
                "of:\n" + tupleDesc.toString());
          }
        }
      }
    }

    if (execRequest.isSetPlan_exec_info() && !execRequest.plan_exec_info.isEmpty()) {
      TPlanFragment firstPlanFragment = execRequest.plan_exec_info.get(0).fragments.get(0);
      if (firstPlanFragment.isSetOutput_sink()
          && firstPlanFragment.output_sink.isSetTable_sink()) {
        TTableSink tableSink = firstPlanFragment.output_sink.table_sink;
        if (!seenTableIds.contains(tableSink.target_table_id)
            || tableSink.target_table_id != DescriptorTable.TABLE_SINK_ID) {
          throw new IllegalStateException("Table sink id error for target table:\n" +
              tableSink.toString());
        }
      }
    }
  }

  /**
   * Produces the single-node or distributed plan for testCase and compares the
   * actual/expected plans if the corresponding test section exists in testCase.
   *
   * Returns the produced exec request or null if there was an error generating
   * the plan.
   *
   * If ignoreExplainHeader is true, the explain header with warnings and resource
   * estimates is stripped out.
   */
  private TExecRequest testPlan(TestCase testCase, Section section,
      TQueryCtx queryCtx, boolean ignoreExplainHeader,
      StringBuilder errorLog, StringBuilder actualOutput) {
    String query = testCase.getQuery();
    queryCtx.client_request.setStmt(query);
    TQueryOptions queryOptions = queryCtx.client_request.getQuery_options();
    if (section == Section.PLAN) {
      queryOptions.setNum_nodes(1);
    } else {
      // for distributed and parallel execution we want to run on all available nodes
      queryOptions.setNum_nodes(
          ImpalaInternalServiceConstants.NUM_NODES_ALL);
    }
    if (section == Section.PARALLELPLANS
        && (!queryOptions.isSetMt_dop() || queryOptions.getMt_dop() == 0)) {
      // Set mt_dop to force production of parallel plans.
      queryCtx.client_request.query_options.setMt_dop(2);
    }
    ArrayList<String> expectedPlan = testCase.getSectionContents(section);
    boolean sectionExists = expectedPlan != null && !expectedPlan.isEmpty();
    String expectedErrorMsg = getExpectedErrorMessage(expectedPlan);

    StringBuilder explainBuilder = new StringBuilder();
    TExecRequest execRequest = null;
    if (sectionExists) actualOutput.append(section.getHeader() + "\n");
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

    String explainStr = explainBuilder.toString();
    if (ignoreExplainHeader) explainStr = removeExplainHeader(explainStr);
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
        queryCtx.client_request.getQuery_options().getExplain_level();
    try {
      queryCtx.client_request.getQuery_options().setExplain_level(TExplainLevel.VERBOSE);
      execRequest = frontend_.createExecRequest(queryCtx, explainBuilder);
    } catch (ImpalaException e) {
      return ExceptionUtils.getStackTrace(e);
    } finally {
      queryCtx.client_request.getQuery_options().setExplain_level(origExplainLevel);
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
      if (execRequest.query_exec_request.plan_exec_info == null) return;
      buildMaps(execRequest.query_exec_request);
      // If we optimize the partition key scans, we may get all the partition key values
      // from the metadata and don't reference any table. Skip the check in this case.
      TQueryOptions options = execRequest.getQuery_options();
      if (!(options.isSetOptimize_partition_key_scans() &&
          options.optimize_partition_key_scans)) {
        testHdfsPartitionsReferenced(execRequest.query_exec_request, query, errorLog);
      }
      locationsStr =
          printScanRangeLocations(execRequest.query_exec_request).toString();
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

  /** Checks that limits are accounted for in the cardinality of plan nodes.
   */
  private void checkLimitCardinality(
      String query, TExecRequest execRequest, StringBuilder errorLog) {
    if (execRequest == null) return;
    if (!execRequest.isSetQuery_exec_request()
        || execRequest.query_exec_request == null
        || execRequest.query_exec_request.plan_exec_info == null) {
      return;
    }
    for (TPlanExecInfo execInfo : execRequest.query_exec_request.plan_exec_info) {
      for (TPlanFragment planFragment : execInfo.fragments) {
        if (!planFragment.isSetPlan() || planFragment.plan == null) continue;
        for (TPlanNode node : planFragment.plan.nodes) {
          if (!node.isSetLimit() || -1 == node.limit) continue;
          if (!node.isSetEstimated_stats() || node.estimated_stats == null) continue;
          if (node.limit < node.estimated_stats.cardinality) {
            StringBuilder limitCardinalityError = new StringBuilder();
            limitCardinalityError.append("Query: " + query + "\n");
            limitCardinalityError.append(
                "Expected cardinality estimate less than or equal to LIMIT: "
                + node.limit + "\n");
            limitCardinalityError.append(
                "Actual cardinality estimate: "
                + node.estimated_stats.cardinality + "\n");
            limitCardinalityError.append(
                "In node id "
                + node.node_id + "\n");
            errorLog.append(limitCardinalityError.toString());
          }
        }
      }
    }
  }

  /**
   * This function plans the given query and fails if the estimated cardinalities are
   * not within the specified bounds [min, max].
   */
  protected void checkCardinality(String query, long min, long max)
        throws ImpalaException {
    TQueryCtx queryCtx = TestUtils.createQueryContext(Catalog.DEFAULT_DB,
        System.getProperty("user.name"));
    queryCtx.client_request.setStmt(query);
    StringBuilder explainBuilder = new StringBuilder();
    TExecRequest execRequest = frontend_.createExecRequest(queryCtx, explainBuilder);

    if (!execRequest.isSetQuery_exec_request()
        || execRequest.query_exec_request == null
        || execRequest.query_exec_request.plan_exec_info == null) {
      return;
    }
    for (TPlanExecInfo execInfo : execRequest.query_exec_request.plan_exec_info) {
      for (TPlanFragment planFragment : execInfo.fragments) {
        if (!planFragment.isSetPlan() || planFragment.plan == null) continue;
        for (TPlanNode node : planFragment.plan.nodes) {
          if (node.estimated_stats == null) {
            fail("Query: " + query + " has no estimated statistics");
          }
          long cardinality = node.estimated_stats.cardinality;
          if (cardinality < min || cardinality > max) {
            StringBuilder errorLog = new StringBuilder();
            errorLog.append("Query: " + query + "\n");
            errorLog.append(
                "Expected cardinality estimate between " + min + " and " + max + "\n");
            errorLog.append("Actual cardinality estimate: " + cardinality + "\n");
            errorLog.append("In node id " + node.node_id + "\n");
            fail(errorLog.toString());
          }
        }
      }
    }
  }

  private void checkColumnLineage(TestCase testCase, TExecRequest execRequest,
      StringBuilder errorLog, StringBuilder actualOutput) {
    String query = testCase.getQuery();
    ArrayList<String> expectedLineage = testCase.getSectionContents(Section.LINEAGE);
    if (expectedLineage == null || expectedLineage.isEmpty()) return;
    TLineageGraph lineageGraph = null;
    if (execRequest == null) return;
    if (execRequest.isSetQuery_exec_request()) {
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
    runPlannerTestFile(testFile, options, true);
  }

  protected void runPlannerTestFile(String testFile, TQueryOptions options,
      boolean ignoreExplainHeader) {
    runPlannerTestFile(testFile, "default", options, ignoreExplainHeader);
  }

  private void runPlannerTestFile(String testFile, String dbName, TQueryOptions options,
      boolean ignoreExplainHeader) {
    String fileName = testDir_.resolve(testFile + ".test").toString();
    if (options == null) {
      options = defaultQueryOptions();
    } else {
      options = mergeQueryOptions(defaultQueryOptions(), options);
    }
    TestFileParser queryFileParser = new TestFileParser(fileName, options);
    StringBuilder actualOutput = new StringBuilder();

    queryFileParser.parseFile();
    StringBuilder errorLog = new StringBuilder();
    for (TestCase testCase : queryFileParser.getTestCases()) {
      actualOutput.append(testCase.getSectionAsString(Section.QUERY, true, "\n"));
      actualOutput.append("\n");
      String queryOptionsSection = testCase.getSectionAsString(
          Section.QUERYOPTIONS, true, "\n");
      if (queryOptionsSection != null && !queryOptionsSection.isEmpty()) {
        actualOutput.append("---- QUERYOPTIONS\n");
        actualOutput.append(queryOptionsSection);
        actualOutput.append("\n");
      }
      try {
        runTestCase(testCase, errorLog, actualOutput, dbName, ignoreExplainHeader);
      } catch (CatalogException e) {
        errorLog.append(String.format("Failed to plan query\n%s\n%s",
            testCase.getQuery(), e.getMessage()));
      }
      actualOutput.append("====\n");
    }

    // Create the actual output file
    if (GENERATE_OUTPUT_FILE) {
      try {
        outDir_.toFile().mkdirs();
        FileWriter fw = new FileWriter(outDir_.resolve(testFile + ".test").toFile());
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
    runPlannerTestFile(testFile, "default", null, true);
  }

  protected void runPlannerTestFile(String testFile, String dbName) {
    runPlannerTestFile(testFile, dbName, null, true);
  }
}
