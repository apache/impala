// Copyright (c) 2015 Cloudera, Inc. All rights reserved.
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

import static org.junit.Assume.assumeTrue;

import java.net.URI;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.cloudera.impala.planner.PlannerTestBase;

// S3 specific planner tests go here, and will run against tables in S3.  These tests
// are run only when test.fs.s3a.name is set in the configuration.
public class S3PlannerTest extends PlannerTestBase {
  // Set to the s3a bucket URI when running on S3. e.g. s3a://bucket/
  private static final String TEST_FS_S3A_NAME = "test.fs.s3a.name";

  // The path that will replace the value of TEST_FS_S3A_NAME in file paths.
  private static final Path S3A_CANONICAL_BUCKET = new Path("s3a://bucket");

  // The Hadoop configuration.
  private final Configuration CONF = new Configuration();

  // The value of the TEST_FS_S3A_NAME property.
  private Path fsName;

  /**
   * Config property test.fs.s3a.name must be set to the S3 scheme and bucket when
   * running on S3, and otherwise unset.  If it is not set, then skip this test.
   * Also remember the scheme://bucket for later.
   */
  @Before
  public void setUpTest() {
    String fsNameStr = CONF.getTrimmed(TEST_FS_S3A_NAME);
    // Skip if the config property was not set. i.e. not running against S3.
    assumeTrue(!StringUtils.isEmpty(fsNameStr));
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
  @Test
  public void testS3ScanRanges() {
    runPlannerTestFile("s3");
  }

}
