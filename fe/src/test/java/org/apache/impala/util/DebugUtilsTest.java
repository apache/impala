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

package org.apache.impala.util;

import org.junit.Assert;
import org.junit.Test;

/**
 * Tests for the debug actions implementation.
 */
public class DebugUtilsTest {

  @Test
  public void testSleepDebugAction() {
    DebugUtils.executeDebugAction("TEST_SLEEP_ACTION:SLEEP@1", "test_sleep_action");
    long startTime = System.currentTimeMillis();
    DebugUtils
        .executeDebugAction("TEST_SLEEP_ACTION:SLEEP@100|SOME_OTHER_ACTION:SLEEP@10",
            "SOME_OTHER_ACTION");
    long endTime = System.currentTimeMillis();
    // make sure you are executing the right sleep action
    Assert.assertTrue(endTime - startTime < 100 && endTime - startTime >= 10);
    // make sure that code doesn't throw if label is not found
    DebugUtils.executeDebugAction("TEST_SLEEP_ACTION:SLEEP@1", "INVALID_LABEL");
    // make sure that code doesn't throw if there is a unsupported action type
    DebugUtils.executeDebugAction("TEST_SLEEP_ACTION:NOT_FOUND@1", "TEST_SLEEP_ACTION");
  }

  @Test(expected = Exception.class)
  public void testSleepDebugActionNegative() throws Exception {
    DebugUtils.executeDebugAction("TEST_SLEEP_ACTION:SLEEP10", "TEST_SLEEP_ACTION");
    DebugUtils.executeDebugAction("TEST_SLEEP_ACTION|SLEEP10", "TEST_SLEEP_ACTION");
    DebugUtils.executeDebugAction("TEST_SLEEP_ACTION@SLEEP:10", "TEST_SLEEP_ACTION");
  }

  @Test
  public void testJitter() {
    DebugUtils.executeDebugAction("TEST_JITTER_ACTION:JITTER@1", "test_jitter_action");
    long startTime = System.currentTimeMillis();
    DebugUtils.executeDebugAction(
        "SOME_OTHER_ACTION:SLEEP@100|TEST_JITTER_ACTION:JITTER@10@0.2",
        "test_jitter_action");
    long endTime = System.currentTimeMillis();
    Assert.assertTrue(endTime - startTime < 100);
  }

  @Test(expected = Exception.class)
  public void testJitterNegative() throws Exception {
    DebugUtils.executeDebugAction("TEST_JITTER_ACTION@JITTER:1", "test_jitter_action");
    DebugUtils.executeDebugAction("TEST_JITTER_ACTION:JITTER", "test_jitter_action");
  }


  /**
   * Test the EXCEPTION DebugAction.
   */
  @Test
  public void testException() {
    try {
      DebugUtils.executeDebugAction(
          "TEST_FAIL_ACTION:EXCEPTION@CommitFailedException@some text",
          "test_fail_action");
      Assert.fail("should have got exception");
    } catch (Exception e) {
      Assert.assertTrue(e.getClass().getName().contains("CommitFailedException"));
      Assert.assertTrue(e.getMessage().contains("some text"));
    }
  }

  /**
   * Negative test for the EXCEPTION DebugAction.
   */
  @Test
  public void testExceptionNegative() {
    // Unimplemented Exception type. This logs an error but does not fail.
    DebugUtils.executeDebugAction(
        "TEST_FAIL_ACTION:EXCEPTION@LocalCatalogException@some text",
        "test_fail_action");
    try {
      // No exception text specified in debug action string.
      DebugUtils.executeDebugAction(
          "TEST_FAIL_ACTION:EXCEPTION@CommitFailedException", "test_fail_action");
    } catch (IllegalStateException e) {
      Assert.assertTrue(
          e.getMessage().contains("EXCEPTION debug action needs 3 action params"));
    }
  }
}