// Copyright 2014 Cloudera Inc.
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

package com.cloudera.impala.util;

import java.io.File;

import org.apache.hadoop.yarn.server.resourcemanager.scheduler.fair.AllocationFileLoaderService;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.fair.AllocationFileLoaderServiceHelper;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import com.cloudera.impala.thrift.TPoolConfigResult;
import com.google.common.io.Files;

/**
 * Unit tests for the user to pool resolution, authorization, and getting configuration
 * parameters via {@link RequestPoolUtils}. Sets a configuration file and ensures the
 * appropriate user to pool resolution, authentication, and pool configs are returned.
 * This also tests that updating the files after startup causes them to be reloaded and
 * the updated values are returned.
 * TODO: Move tests to C++ to test the API that's actually used.
 */
public class TestRequestPoolUtils {
  // Pool definitions and includes memory resource limits, copied to a temporary file
  private static final String ALLOCATION_FILE = "fair-scheduler-test.xml";

  // A second allocation file which overwrites the temporary file to check for changes.
  private static final String ALLOCATION_FILE_MODIFIED = "fair-scheduler-test2.xml";

  // Contains per-pool configurations for maximum number of running queries and queued
  // requests.
  private static final String LLAMA_CONFIG_FILE = "llama-site-test.xml";

  // A second Llama config which overwrites the temporary file to check for changes.
  private static final String LLAMA_CONFIG_FILE_MODIFIED = "llama-site-test2.xml";

  // Set the file check interval to something short so we don't have to wait long after
  // changing the file.
  private static final long CHECK_INTERVAL_MS = 100L;

  // Temp folder where the config files are copied so we can modify them in place.
  // The JUnit @Rule creates and removes the temp folder between every test.
  @Rule
  public TemporaryFolder tempFolder = new TemporaryFolder();

  private RequestPoolUtils poolUtils_;
  private File allocationConfFile_;
  private File llamaConfFile_;

  @Before
  public void setUp() throws Exception {
    allocationConfFile_ = tempFolder.newFile("fair-scheduler-temp-file.xml");
    Files.copy(
        new File(RequestPoolUtils.getURL(ALLOCATION_FILE).getPath()),
        allocationConfFile_);
    llamaConfFile_ = tempFolder.newFile("llama-conf-temp-file.xml");
    Files.copy(
        new File(RequestPoolUtils.getURL(LLAMA_CONFIG_FILE).getPath()),
        llamaConfFile_);

    poolUtils_ = new RequestPoolUtils(allocationConfFile_.getAbsolutePath(),
        llamaConfFile_.getAbsolutePath());
    AllocationFileLoaderServiceHelper.setFileReloadIntervalMs(poolUtils_.allocLoader_,
        CHECK_INTERVAL_MS);
    poolUtils_.llamaConfWatcher_.setCheckIntervalMs(CHECK_INTERVAL_MS);
    poolUtils_.start();
  }

  @After
  public void cleanUp() {
    if (poolUtils_ != null) poolUtils_.stop();
  }

  @Test
  public void testPoolResolution() throws Exception {
    Assert.assertEquals("root.queueA", poolUtils_.assignToPool("queueA", "userA"));
    Assert.assertNull(poolUtils_.assignToPool("queueC", "userA"));
  }

  @Test
  public void testPoolAcls() throws Exception {
    Assert.assertTrue(poolUtils_.hasAccess("root.queueA", "userA"));
    Assert.assertTrue(poolUtils_.hasAccess("root.queueB", "userB"));
    Assert.assertFalse(poolUtils_.hasAccess("root.queueB", "userA"));
    Assert.assertTrue(poolUtils_.hasAccess("root.queueB", "root"));
  }

  @Test
  public void testPoolLimitConfigs() throws Exception {
    checkPoolConfigResult("root", 15, 50, -1);
    checkPoolConfigResult("root.queueA", 10, 30, 1024 * RequestPoolUtils.ONE_MEGABYTE);
    checkPoolConfigResult("root.queueB", 5, 10, -1);
  }

  @Test
  public void testUpdatingConfigs() throws Exception {
    // Tests updating the config files and then checking the pool resolution, ACLs, and
    // pool limit configs. This tests all three together rather than separating into
    // separate test cases because we updateConfigFiles() will end up waiting around 7
    // seconds, so this helps cut down on the total test execution time.
    // A one second pause is necessary to ensure the file timestamps are unique if the
    // test gets here within one second.
    Thread.sleep(1000L);
    Files.copy(
        new File(RequestPoolUtils.getURL(ALLOCATION_FILE_MODIFIED).getPath()),
        allocationConfFile_);
    Files.copy(
        new File(RequestPoolUtils.getURL(LLAMA_CONFIG_FILE_MODIFIED).getPath()),
        llamaConfFile_);

    // Wait at least 1 second more than the time it will take for the
    // AllocationFileLoaderService to update the file. The FileWatchService does not
    // have that additional wait time, so it will be updated within 'CHECK_INTERVAL_MS'
    Thread.sleep(1000L + CHECK_INTERVAL_MS +
        AllocationFileLoaderService.ALLOC_RELOAD_WAIT_MS);

    // Test pool resolution: now there's a queueC
    Assert.assertEquals("root.queueA", poolUtils_.assignToPool("queueA", "userA"));
    Assert.assertNull(poolUtils_.assignToPool("queueX", "userA"));
    Assert.assertEquals("root.queueC", poolUtils_.assignToPool("queueC", "userA"));

    // Test pool ACL changes
    Assert.assertTrue(poolUtils_.hasAccess("root.queueA", "userA"));
    Assert.assertTrue(poolUtils_.hasAccess("root.queueB", "userB"));
    Assert.assertTrue(poolUtils_.hasAccess("root.queueB", "userA"));
    Assert.assertFalse(poolUtils_.hasAccess("root.queueC", "userA"));
    Assert.assertTrue(poolUtils_.hasAccess("root.queueC", "root"));

    // Test pool limit changes
    checkPoolConfigResult("root", 15, 100, -1);
    checkPoolConfigResult("root.queueA", 10, 30, 100000 * RequestPoolUtils.ONE_MEGABYTE);
    checkPoolConfigResult("root.queueB", 5, 10, -1);
    checkPoolConfigResult("root.queueC", 10, 30, 128 * RequestPoolUtils.ONE_MEGABYTE);
  }

  /**
   * Helper method to verify the per-pool limits.
   */
  private void checkPoolConfigResult(String pool, long expectedMaxRequests,
      long expectedMaxQueued, long expectedMaxMemUsage) {
    TPoolConfigResult expectedResult = new TPoolConfigResult();
    expectedResult.setMax_requests(expectedMaxRequests);
    expectedResult.setMax_queued(expectedMaxQueued);
    expectedResult.setMem_limit(expectedMaxMemUsage);
    Assert.assertEquals("Unexpected config values for pool " + pool,
        expectedResult, poolUtils_.getPoolConfig(pool));
  }
}
