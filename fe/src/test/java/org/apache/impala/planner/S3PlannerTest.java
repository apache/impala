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

import static org.junit.Assume.assumeTrue;

import java.net.URI;

import org.apache.hadoop.fs.Path;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import com.google.common.collect.ImmutableSet;

// S3 specific planner tests go here, and will run against tables in S3.  These tests
// are run only when test.fs.s3a.name is set in the configuration.
@Category(S3Tests.class)
public class S3PlannerTest extends PlannerTestBase {

  // The path that will replace the value of TEST_FS_S3A_NAME in file paths.
  private static final Path S3A_CANONICAL_BUCKET = new Path("s3a://bucket");

  // The value of the FILESYSTEM_PREFIX environment variable.
  private Path fsName;

  /**
   * Environment variable TARGET_FILESYSTEM will be set to s3 when running against S3.
   * If not, then skip this test.  Also remember the scheme://bucket for later.
   */
  @Before
  public void setUpTest() throws Exception {
    super.setUpTest();
    String targetFs = System.getenv("TARGET_FILESYSTEM");
    // Skip if the config property was not set. i.e. not running against S3.
    assumeTrue(targetFs != null && targetFs.equals("s3"));
    String fsNameStr = System.getenv("DEFAULT_FS");
    fsName = new Path(fsNameStr);
  }

  /**
   * Remove any non-constant components of the given file path.  For S3, the
   * actual bucket name, which will be unique to the tester's setup, needs to
   * be replaced with a fixed bucket name.
   */
  @Override
  protected Path cleanseFilePath(Path path) {
    path = super.cleanseFilePath(path);
    URI fsURI = fsName.toUri();
    URI pathURI = path.toUri();
    Assert.assertTrue("error: " + path + " is not on filesystem " + fsName,
        fsURI.getScheme().equals(pathURI.getScheme()) &&
        fsURI.getAuthority().equals(pathURI.getAuthority()));
    return Path.mergePaths(S3A_CANONICAL_BUCKET, path);
  }

  /**
   * Verify that S3 scan ranges are generated correctly.
   */
  @Ignore("IMPALA-8944, IMPALA-5931")
  @Test
  public void testS3ScanRanges() {
    runPlannerTestFile("s3");
  }

  @Test
  public void testPredicatePropagation() {
    runPlannerTestFile("predicate-propagation");
  }

  @Test
  public void testConstant() {
    runPlannerTestFile("constant");
  }

  @Test
  public void testDistinct() {
    runPlannerTestFile("distinct");
  }

  @Test
  public void testAggregation() {
    runPlannerTestFile("aggregation");
  }

  @Test
  public void testAnalyticFns() {
    runPlannerTestFile("analytic-fns");
  }

  @Test
  public void testNestedCollections() {
    runPlannerTestFile("nested-collections");
  }

  @Ignore("IMPALA-8949")
  @Test
  public void testJoinOrder() {
    runPlannerTestFile("join-order");
  }

  @Test
  public void testOuterJoins() {
    runPlannerTestFile("outer-joins");
  }

  @Test
  public void testImplicitJoins() {
    runPlannerTestFile("implicit-joins");
  }

  @Test
  public void testInlineViewLimit() {
    runPlannerTestFile("inline-view-limit");
  }

  @Ignore("IMPALA-8949")
  @Test
  public void testSubqueryRewrite() {
    runPlannerTestFile("subquery-rewrite");
  }

  @Test
  public void testUnion() {
    runPlannerTestFile("union");
  }

  @Test
  public void testValues() {
    runPlannerTestFile("values");
  }

  @Test
  public void testViews() {
    runPlannerTestFile("views");
  }

  @Test
  public void testDistinctEstimate() {
    runPlannerTestFile("distinct-estimate");
  }

  @Test
  public void testDataSourceTables() {
    runPlannerTestFile("data-source-tables");
  }

  @Ignore("IMPALA-8949")
  @Test
  public void testTpch() {
    runPlannerTestFile("tpch-all", "tpch",
        ImmutableSet.of(PlannerTestOption.INCLUDE_RESOURCE_HEADER,
            PlannerTestOption.VALIDATE_RESOURCES));
  }

  @Ignore("IMPALA-8949")
  @Test
  public void testTpcds() {
    // Uses ss_sold_date_sk as the partition key of store_sales to allow static partition
    // pruning. The original predicates were rephrased in terms of the ss_sold_date_sk
    // partition key, with the query semantics identical to the original queries.
    runPlannerTestFile("tpcds-all", "tpcds",
        ImmutableSet.of(PlannerTestOption.INCLUDE_RESOURCE_HEADER,
            PlannerTestOption.VALIDATE_RESOURCES));
  }

  @Override
  public boolean scanRangeLocationsCheckEnabled() {
    return false;
  }
}
