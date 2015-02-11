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

import static org.junit.Assert.fail;
import static org.junit.Assume.assumeTrue;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.junit.Test;

import com.cloudera.impala.planner.PlannerTestBase;

// S3 specific planner tests go here, and will run against tables in S3.  These tests
// are run only when test.fs.s3a.name is set in the configuration.
public class S3PlannerTest extends PlannerTestBase {
  // Set to the s3a bucket URI when running on S3. e.g. s3a://bucket/
  public static final String TEST_FS_S3A_NAME = "test.fs.s3a.name";

  private final Configuration CONF = new Configuration();

  private boolean isRunningOnS3() {
    String fsName = CONF.getTrimmed(TEST_FS_S3A_NAME);
    return !StringUtils.isEmpty(fsName);
  }

  @Test
  public void testNothing() {
    assumeTrue(isRunningOnS3());
    // TDOD: Add S3 scan range test.
    fail("Shouldn't get here since " + TEST_FS_S3A_NAME + " not set.");
  }

}
