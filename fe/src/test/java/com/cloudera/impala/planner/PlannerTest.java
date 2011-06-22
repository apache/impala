// Copyright (c) 2011 Cloudera, Inc. All rights reserved.

package com.cloudera.impala.planner;

import static org.junit.Assert.fail;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Scanner;

import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.cloudera.impala.catalog.Catalog;
import com.cloudera.impala.catalog.TestSchemaUtils;
import com.cloudera.impala.common.AnalysisException;
import com.cloudera.impala.common.NotImplementedException;
import com.cloudera.impala.analysis.AnalysisContext;
import com.google.common.collect.Lists;

public class PlannerTest {
  private final static Logger LOG = LoggerFactory.getLogger(PlannerTest.class);

  private static Catalog catalog;
  private static AnalysisContext analysisCtxt;
  private final String testDir = "PlannerTest";

  @BeforeClass public static void setUp() throws Exception {
    HiveMetaStoreClient client = TestSchemaUtils.createSchemaAndClient();
    catalog = new Catalog(client);
    analysisCtxt = new AnalysisContext(catalog);
  }

  /**
   * Do a line-by-line comparison of actual and expected output.
   * Comparison of the individual lines ignores whitespace.
   * @param actual
   * @param expected
   * @return an error message if actual does not match expected, "" otherwise.
   */
  private String compareOutput(String[] actual, ArrayList<String> expected) {
    int mismatch = -1;  // line w/ mismatch
    int maxLen = Math.min(actual.length, expected.size());
    for (int i = 0; i < maxLen; ++i) {
      // do a whitespace-insensitive comparison
      Scanner a = new Scanner(actual[i]);
      Scanner e = new Scanner(expected.get(i));
      while (a.hasNext() && e.hasNext()) {
        if (!a.next().equals(e.next())) {
          mismatch = i;
          break;
        }
      }
      if (mismatch != -1) {
        break;
      }
      if (a.hasNext() != e.hasNext()) {
        mismatch = i;
        break;
      }
    }
    if (mismatch == -1 && actual.length < expected.size()) {
      mismatch = actual.length;
    }

    if (mismatch != -1) {
      // print actual and expected, highlighting mismatch
      StringBuilder output =
          new StringBuilder("actual plan doesn't match expected plan:\n");
      for (int i = 0; i <= mismatch; ++i) {
        output.append(actual[i]).append("\n");
      }
      // underline mismatched line with "^^^..."
      for (int i = 0; i < actual[mismatch].length(); ++i) {
        output.append('^');
      }
      output.append("\n");
      for (int i = mismatch +1; i < actual.length; ++i) {
        output.append(actual[i]).append("\n");
      }
      output.append("\nexpected:\n");
      for (String str: expected) {
        output.append(str).append("\n");
      }
      return output.toString();
    }

    if (actual.length > expected.size()) {
      // print actual and expected
      StringBuilder output =
          new StringBuilder("actual plan contains extra output:\n");
      for (String str: actual) {
        output.append(str).append("\n");
      }
      output.append("\nexpected:\n");
      for (String str: expected) {
        output.append(str).append("\n");
      }
      return output.toString();
    }

    return "";
  }

  private void RunQuery(String query, ArrayList<String> expectedPlan) {
    try {
      LOG.info("running query " + query);
      AnalysisContext.AnalysisResult analysisResult = analysisCtxt.analyze(query);
      Planner planner = new Planner();
      PlanNode plan = planner.createPlan(analysisResult.selectStmt, analysisResult.analyzer);
      String result = compareOutput(plan.getExplainString().split("\n"), expectedPlan);
      if (!result.isEmpty()) {
        fail("query:\n" + query + "\n" + result);
      }
    } catch (AnalysisException e) {
      fail("analysis error: " + e.getMessage());
    } catch (NotImplementedException e) {
      fail("plan not implemented");
    }
  }

  private void RunUnimplementedQuery(String query) {
    try {
      AnalysisContext.AnalysisResult analysisResult = analysisCtxt.analyze(query);
      Planner planner = new Planner();
      PlanNode plan = planner.createPlan(analysisResult.selectStmt, analysisResult.analyzer);
      fail("query produced a plan\nquery=" + query + "\nplan=\n" + plan.getExplainString());
    } catch (AnalysisException e) {
      fail("analysis error: " + e.getMessage());
    } catch (NotImplementedException e) {
      // expected
    }
  }

  private void RunTests(String testCase) {
    String fileName = testDir + "/" + testCase + ".test";
    ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
    InputStream stream = classLoader.getResourceAsStream(fileName);
    if (stream == null) {
      fail("couldn't find " + fileName);
    }
    Scanner scanner = new Scanner(stream);

    while (scanner.hasNextLine()) {
      StringBuilder query = new StringBuilder();
      ArrayList<String> plan = Lists.newArrayList();
      boolean readQuery = true;
      while (scanner.hasNextLine()) {
        String line = scanner.nextLine();
        // ignore comments
        if (line.startsWith("//") || line.startsWith("#")) {
          continue;
        }
        if (line.startsWith("=")) {
          break; // done w/ this query
        }
        if (line.startsWith("-")) {
          // start of plan output section
          readQuery = false;
        } else if (readQuery) {
          query.append(line);
          query.append(" ");
        } else {
          plan.add(line);
        }
      }
      if (query.toString().isEmpty()) {
        // we're done
        break;
      }
      if (plan.size() > 0 && plan.get(0).toLowerCase().startsWith("not implemented")) {
        RunUnimplementedQuery(query.toString());
      } else {
        RunQuery(query.toString(), plan);
      }
    }
    try {
      stream.close();
    } catch (IOException e) {
      // ignore
    }
  }

  @Test public void Test() {
    RunTests("basic");
    RunTests("joins");
  }
}
