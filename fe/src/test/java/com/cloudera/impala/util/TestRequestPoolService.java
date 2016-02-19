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
import java.io.IOException;
import java.lang.reflect.Field;
import java.net.URISyntaxException;

import org.apache.hadoop.yarn.server.resourcemanager.scheduler.fair.AllocationFileLoaderService;
import org.junit.After;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import com.cloudera.impala.common.ByteUnits;
import com.cloudera.impala.thrift.TErrorCode;
import com.cloudera.impala.thrift.TPoolConfig;
import com.cloudera.impala.thrift.TResolveRequestPoolParams;
import com.cloudera.impala.thrift.TResolveRequestPoolResult;
import com.google.common.collect.Iterables;
import com.google.common.io.Files;

/**
 * Unit tests for the user to pool resolution, authorization, and getting configuration
 * parameters via {@link RequestPoolService}. Sets a configuration file and ensures the
 * appropriate user to pool resolution, authentication, and pool configs are returned.
 * This also tests that updating the files after startup causes them to be reloaded and
 * the updated values are returned.
 * TODO: Move tests to C++ to test the API that's actually used.
 */
public class TestRequestPoolService {
  // Pool definitions and includes memory resource limits, copied to a temporary file
  private static final String ALLOCATION_FILE = "fair-scheduler-test.xml";

  // A second allocation file which overwrites the temporary file to check for changes.
  private static final String ALLOCATION_FILE_MODIFIED = "fair-scheduler-test2.xml";
  private static final String ALLOCATION_FILE_EMPTY = "fair-scheduler-empty.xml";
  private static final String ALLOCATION_FILE_GROUP_RULE = "fair-scheduler-group-rule.xml";

  // Contains per-pool configurations for maximum number of running queries and queued
  // requests.
  private static final String LLAMA_CONFIG_FILE = "llama-site-test.xml";

  // A second Llama config which overwrites the temporary file to check for changes.
  private static final String LLAMA_CONFIG_FILE_MODIFIED = "llama-site-test2.xml";
  private static final String LLAMA_CONFIG_FILE_EMPTY = "llama-site-empty.xml";

  // Set the file check interval to something short so we don't have to wait long after
  // changing the file.
  private static final long CHECK_INTERVAL_MS = 100L;

  // Temp folder where the config files are copied so we can modify them in place.
  // The JUnit @Rule creates and removes the temp folder between every test.
  @Rule
  public TemporaryFolder tempFolder = new TemporaryFolder();

  private RequestPoolService poolService_;
  private File allocationConfFile_;
  private File llamaConfFile_;

  /**
   * Creates the poolService_ with the specified configuration.
   * @param allocationFile The file on the classpath of the allocation conf.
   * @param llamaConfFile The file on the classpath of the Llama conf. May be null to
   *                      create a RequestPoolService with no llama-conf.xml as it is
   *                      not required.
   */
  void createPoolService(String allocationFile, String llamaConfFile)
      throws Exception {
    allocationConfFile_ = tempFolder.newFile("fair-scheduler-temp-file.xml");
    Files.copy(getClasspathFile(allocationFile), allocationConfFile_);

    String llamaConfPath = null;
    if (llamaConfFile != null) {
      llamaConfFile_ = tempFolder.newFile("llama-conf-temp-file.xml");
      Files.copy(getClasspathFile(llamaConfFile), llamaConfFile_);
      llamaConfPath = llamaConfFile_.getAbsolutePath();
    }
    poolService_ = new RequestPoolService(allocationConfFile_.getAbsolutePath(),
        llamaConfPath);

    // Lower the wait times on the AllocationFileLoaderService and RequestPoolService so
    // the test doesn't have to wait very long to test that file changes are reloaded.
    Field f = AllocationFileLoaderService.class.getDeclaredField("reloadIntervalMs");
    f.setAccessible(true);
    f.set(poolService_.allocLoader_, CHECK_INTERVAL_MS);
    if (llamaConfFile != null) {
      poolService_.llamaConfWatcher_.setCheckIntervalMs(CHECK_INTERVAL_MS);
    }
    poolService_.start();
  }

  @After
  public void cleanUp() throws Exception {
    if (poolService_ != null) poolService_.stop();
  }

  /**
   * Returns a {@link File} for the file on the classpath.
   */
  private File getClasspathFile(String filename) throws URISyntaxException {
    return new File(getClass().getClassLoader().getResource(filename).toURI());
  }

  @Test
  public void testPoolResolution() throws Exception {
    createPoolService(ALLOCATION_FILE, LLAMA_CONFIG_FILE);
    Assert.assertEquals("root.queueA", poolService_.assignToPool("root.queueA", "userA"));
    Assert.assertNull(poolService_.assignToPool("queueC", "userA"));
  }

  @Test
  public void testResolvePrincipalName() throws Exception {
    // Tests that we can resolve user names that are Kerberos principals/LDAP users.
    createPoolService(ALLOCATION_FILE, LLAMA_CONFIG_FILE);
    TResolveRequestPoolResult result = poolService_.resolveRequestPool(
        new TResolveRequestPoolParams("userA@abc.com", "root.queueA"));
    Assert.assertEquals(TErrorCode.OK, result.getStatus().getStatus_code());
    Assert.assertEquals("root.queueA", result.getResolved_pool());

    result = poolService_.resolveRequestPool(
        new TResolveRequestPoolParams("userA/a.qualified.domain@abc.com", "root.queueA"));
    Assert.assertEquals(TErrorCode.OK, result.getStatus().getStatus_code());
    Assert.assertEquals("root.queueA", result.getResolved_pool());
  }

  @Test
  public void testUserNoGroupsError() throws Exception {
    // Test fix for IMPALA-922: "Return helpful errors with Yarn group rules"
    createPoolService(ALLOCATION_FILE_GROUP_RULE, LLAMA_CONFIG_FILE);
    TResolveRequestPoolResult result = poolService_.resolveRequestPool(
        new TResolveRequestPoolParams("userA", "root.NOT_A_POOL"));
    Assert.assertEquals(false, result.isSetResolved_pool());
    Assert.assertEquals(false, result.isSetHas_access());
    Assert.assertEquals(TErrorCode.INTERNAL_ERROR, result.getStatus().getStatus_code());

    String expectedMessage = "Failed to resolve user 'userA' to a pool while " +
    "evaluating the 'primaryGroup' or 'secondaryGroup' queue placement rules because " +
    "no groups were found for the user. This is likely because the user does not " +
    "exist on the local operating system.";
    Assert.assertEquals(expectedMessage,
        Iterables.getOnlyElement(result.getStatus().getError_msgs()));
  }

  @Test
  public void testPoolAcls() throws Exception {
    createPoolService(ALLOCATION_FILE, LLAMA_CONFIG_FILE);
    Assert.assertTrue(poolService_.hasAccess("root.queueA", "userA"));
    Assert.assertTrue(poolService_.hasAccess("root.queueB", "userB"));
    Assert.assertFalse(poolService_.hasAccess("root.queueB", "userA"));
    Assert.assertTrue(poolService_.hasAccess("root.queueB", "root"));
  }

  @Test
  public void testPoolLimitConfigs() throws Exception {
    createPoolService(ALLOCATION_FILE, LLAMA_CONFIG_FILE);
    checkPoolConfigResult("root", 15, 50, -1, 30000L, "mem_limit=1024m");
    checkPoolConfigResult("root.queueA", 10, 30, 1024 * ByteUnits.MEGABYTE,
        10000L, "mem_limit=1024m,query_timeout_s=10");
    checkPoolConfigResult("root.queueB", 5, 10, -1, 30000L, "mem_limit=1024m");
  }

  @Test
  public void testDefaultConfigs() throws Exception {
    createPoolService(ALLOCATION_FILE_EMPTY, LLAMA_CONFIG_FILE_EMPTY);
    Assert.assertEquals("root.userA", poolService_.assignToPool("", "userA"));
    Assert.assertTrue(poolService_.hasAccess("root.userA", "userA"));
    checkPoolConfigResult("root", -1, 200, -1);
  }

  @Test
  public void testUpdatingConfigs() throws Exception {
    // Tests updating the config files and then checking the pool resolution, ACLs, and
    // pool limit configs. This tests all three together rather than separating into
    // separate test cases because we updateConfigFiles() will end up waiting around 7
    // seconds, so this helps cut down on the total test execution time.
    // A one second pause is necessary to ensure the file timestamps are unique if the
    // test gets here within one second.
    createPoolService(ALLOCATION_FILE, LLAMA_CONFIG_FILE);
    Thread.sleep(1000L);
    Files.copy(getClasspathFile(ALLOCATION_FILE_MODIFIED), allocationConfFile_);
    Files.copy(getClasspathFile(LLAMA_CONFIG_FILE_MODIFIED), llamaConfFile_);
    // Wait at least 1 second more than the time it will take for the
    // AllocationFileLoaderService to update the file. The FileWatchService does not
    // have that additional wait time, so it will be updated within 'CHECK_INTERVAL_MS'
    Thread.sleep(1000L + CHECK_INTERVAL_MS +
        AllocationFileLoaderService.ALLOC_RELOAD_WAIT_MS);
    checkModifiedConfigResults();
  }

  @Test
  public void testModifiedConfigs() throws Exception {
    // Tests the results are the same as testUpdatingConfigs() as when we create the
    // pool service with the same modified configs initially (i.e. not updating).
    createPoolService(ALLOCATION_FILE_MODIFIED, LLAMA_CONFIG_FILE_MODIFIED);
    checkModifiedConfigResults();
  }

  @Test
  public void testNullLlamaSite() throws Exception {
    createPoolService(ALLOCATION_FILE_MODIFIED, null);

    // Test pool resolution
    Assert.assertEquals("root.queueA", poolService_.assignToPool("queueA", "userA"));
    Assert.assertNull(poolService_.assignToPool("queueX", "userA"));
    Assert.assertEquals("root.queueC", poolService_.assignToPool("queueC", "userA"));

    // Test pool ACLs
    Assert.assertTrue(poolService_.hasAccess("root.queueA", "userA"));
    Assert.assertTrue(poolService_.hasAccess("root.queueB", "userB"));
    Assert.assertTrue(poolService_.hasAccess("root.queueB", "userA"));
    Assert.assertFalse(poolService_.hasAccess("root.queueC", "userA"));
    Assert.assertTrue(poolService_.hasAccess("root.queueC", "root"));

    // Test pool limits
    checkPoolConfigResult("root", -1, 200, -1);
    checkPoolConfigResult("root.queueA", -1, 200, 100000 * ByteUnits.MEGABYTE);
    checkPoolConfigResult("root.queueB", -1, 200, -1);
    checkPoolConfigResult("root.queueC", -1, 200, 128 * ByteUnits.MEGABYTE);
  }

  private void checkModifiedConfigResults() throws IOException {
    // Test pool resolution: now there's a queueC
    Assert.assertEquals("root.queueA", poolService_.assignToPool("queueA", "userA"));
    Assert.assertNull(poolService_.assignToPool("queueX", "userA"));
    Assert.assertEquals("root.queueC", poolService_.assignToPool("queueC", "userA"));

    // Test pool ACL changes
    Assert.assertTrue(poolService_.hasAccess("root.queueA", "userA"));
    Assert.assertTrue(poolService_.hasAccess("root.queueB", "userB"));
    Assert.assertTrue(poolService_.hasAccess("root.queueB", "userA"));
    Assert.assertFalse(poolService_.hasAccess("root.queueC", "userA"));
    Assert.assertTrue(poolService_.hasAccess("root.queueC", "root"));

    // Test pool limit changes
    checkPoolConfigResult("root", 15, 100, -1, 30000L, "");
    checkPoolConfigResult("root.queueA", 1, 30, 100000 * ByteUnits.MEGABYTE,
        50L, "mem_limit=128m,query_timeout_s=5");
    checkPoolConfigResult("root.queueB", 5, 10, -1, 60000L, "");
    checkPoolConfigResult("root.queueC", 10, 30, 128 * ByteUnits.MEGABYTE,
        30000L, "mem_limit=2048m,query_timeout_s=60");
  }

  /**
   * Helper method to verify the per-pool limits.
   */
  private void checkPoolConfigResult(String pool, long expectedMaxRequests,
      long expectedMaxQueued, long expectedMaxMem, Long expectedQueueTimeoutMs,
      String expectedQueryOptions) {
    TPoolConfig expectedResult = new TPoolConfig();
    expectedResult.setMax_requests(expectedMaxRequests);
    expectedResult.setMax_queued(expectedMaxQueued);
    expectedResult.setMax_mem_resources(expectedMaxMem);
    if (expectedQueueTimeoutMs != null) {
      expectedResult.setQueue_timeout_ms(expectedQueueTimeoutMs);
    }
    if (expectedQueryOptions != null) {
      expectedResult.setDefault_query_options(expectedQueryOptions);
    }
    Assert.assertEquals("Unexpected config values for pool " + pool,
        expectedResult, poolService_.getPoolConfig(pool));
  }

  private void checkPoolConfigResult(String pool, long expectedMaxRequests,
      long expectedMaxQueued, long expectedMaxMemUsage) {
    checkPoolConfigResult(pool, expectedMaxRequests, expectedMaxQueued,
        expectedMaxMemUsage, null, "");
  }
}
