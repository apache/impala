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

import com.google.common.base.Preconditions;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.impala.catalog.Catalog;
import org.apache.impala.catalog.Db;
import org.apache.impala.common.FileSystemUtil;
import org.apache.impala.testutil.PlannerTestCaseLoader;
import org.apache.impala.util.PatternMatcher;
import org.apache.log4j.Logger;
import org.junit.Test;

import java.util.List;
import java.util.Random;

public class TestCaseLoaderTest {
  private static final Logger LOG = Logger.getLogger(TestCaseLoaderTest.class);

  // Testcase files are loaded along with the test-data snapshot. Refer to
  // create-tpcds-testcase-files.sh for details.
  private static final Path TESTCASE_DATA_DIR = new
      Path("/test-warehouse/tpcds-testcase-data");

  private static final String TESTCASE_FILE_PREFIX = "impala-testcase-data";

  /**
   * Randomly picks 10 testcase files from TESTCASE_DATA_DIR and loads them into a
   * clean Catalog and makes sure that the query statement from the testcase can be
   * planned correctly without any errors.
   */
  @Test
  public void testTestCaseImport() throws Exception {
    FileStatus[] testCaseFiles = FileSystemUtil.getFileSystemForPath(TESTCASE_DATA_DIR)
        .listStatus(TESTCASE_DATA_DIR);
    // Randomly pick testcases and try to replay them.
    Random rand = new Random();
    int maxIterations = 10;
    Preconditions.checkState(testCaseFiles.length > maxIterations);
    for (int i = 0; i < maxIterations; ++i) {
      FileStatus fs = testCaseFiles[rand.nextInt(testCaseFiles.length)];
      if (!fs.getPath().getName().contains(TESTCASE_FILE_PREFIX)) continue;
      try (PlannerTestCaseLoader testCaseLoader = new PlannerTestCaseLoader()) {
        Catalog srcCatalog = testCaseLoader.getSrcCatalog();
        // Make sure the catalog is empty (just the default database).
        List<Db> dbs = srcCatalog.getDbs(PatternMatcher.MATCHER_MATCH_ALL);
        assert dbs.size() == 1 && dbs.get(0).getName().equals("default");
        // TODO: Include the source cluster plan in the testcase file and compare it
        // here with the plan computed from the local metadata.
        LOG.info(testCaseLoader.loadTestCase(fs.getPath().toString()));
        dbs = srcCatalog.getDbs(PatternMatcher.MATCHER_MATCH_ALL);
        // Atleast one new database should be loaded per testcase.
        assert dbs.size() > 1;
      }
    }
  }
}

