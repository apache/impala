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

package org.apache.impala.service;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.impala.common.AnalysisException;
import org.apache.impala.thrift.TExecRequest;
import org.apache.impala.thrift.TExecutorGroupSet;
import org.apache.impala.thrift.TQueryCtx;
import org.apache.impala.thrift.TQueryExecRequest;
import org.apache.impala.thrift.TQueryOptions;
import org.apache.impala.thrift.TStmtType;
import org.junit.Test;

/**
 * Tests a cluster exposing the default executor group alongside custom executor groups.
 * The default group arrives as a group set with an empty name prefix, so it names no
 * request pool: it must stay usable for pool-less queries without becoming a candidate
 * for queries that name a pool.
 *
 * REQUEST_POOL is unset by setting it to the empty string, so there is no
 * "REQUEST_POOL=default" case.
 */
public class ExecutorGroupSetSelectionTest {
  // As sent for the default executor group.
  private static final String DEFAULT_PREFIX = "";

  private static final String NO_REQUEST_POOL = "";

  private static TExecutorGroupSet groupSet(int currNumExecutors, String prefix) {
    TExecutorGroupSet s = new TExecutorGroupSet();
    s.setCurr_num_executors(currNumExecutors);
    s.setExpected_num_executors(20);
    s.setExec_group_name_prefix(prefix);
    return s;
  }

  /** A cluster with both default and custom executors. */
  private static List<TExecutorGroupSet> defaultAndCustomGroupSets() {
    return new ArrayList<>(
        Arrays.asList(groupSet(1, DEFAULT_PREFIX), groupSet(2, "foo")));
  }

  /** A cluster with only custom executor groups. */
  private static List<TExecutorGroupSet> customOnlyGroupSets() {
    return new ArrayList<>(Arrays.asList(groupSet(2, "foo"), groupSet(3, "bar")));
  }

  private static List<TExecutorGroupSet> select(
      List<TExecutorGroupSet> sets, String requestPool) throws Exception {
    return Frontend.setupThresholdsForExecutorGroupSets(
        sets, requestPool, /*default_executor_group*/ false, /*test_replan*/ false);
  }

  private static List<String> prefixesOf(List<TExecutorGroupSet> sets) {
    List<String> prefixes = new ArrayList<>();
    for (TExecutorGroupSet s : sets) prefixes.add(s.getExec_group_name_prefix());
    return prefixes;
  }

  /** The default executors must stay usable in a mixed cluster. */
  @Test
  public void testNoRequestPoolKeepsDefaultGroupSet() throws Exception {
    List<TExecutorGroupSet> result = select(defaultAndCustomGroupSets(), NO_REQUEST_POOL);
    // Both unrestricted, so the sort breaks the tie on the prefix.
    assertEquals(Arrays.asList(DEFAULT_PREFIX, "foo"), prefixesOf(result));
    assertEquals(Long.MAX_VALUE, result.get(0).getMax_mem_limit());
    assertEquals(1, result.get(0).getCurr_num_executors());
  }

  /**
   * A custom pool must not pick up the default group set as an extra candidate, or the
   * query could be planned for the default executors.
   */
  @Test
  public void testCustomPoolSelectsCustomGroupSetOnly() throws Exception {
    List<TExecutorGroupSet> result = select(defaultAndCustomGroupSets(), "foo");
    assertEquals(Arrays.asList("foo"), prefixesOf(result));
    assertEquals(2, result.get(0).getCurr_num_executors());
  }

  /** An unset prefix on the default group set must be handled like an empty one. */
  @Test
  public void testUnsetDefaultPrefixTreatedAsEmpty() throws Exception {
    List<TExecutorGroupSet> sets = defaultAndCustomGroupSets();
    sets.get(0).unsetExec_group_name_prefix();
    // Ties with "foo" on memory and cores, so the sort compares the prefixes.
    assertEquals(Arrays.asList(DEFAULT_PREFIX, "foo"),
        prefixesOf(select(sets, NO_REQUEST_POOL)));
    assertEquals(Arrays.asList("foo"), prefixesOf(select(sets, "foo")));
  }

  /**
   * A mistyped pool must still be rejected, not silently planned for the default
   * executors.
   */
  @Test
  public void testUnknownPoolRejectedWithDefaultGroup() throws Exception {
    assertUnknownPoolRejected(defaultAndCustomGroupSets());
  }

  /** Unchanged without default executors. */
  @Test
  public void testUnknownPoolRejectedWithoutDefaultGroup() throws Exception {
    assertUnknownPoolRejected(customOnlyGroupSets());
  }

  private static void assertUnknownPoolRejected(List<TExecutorGroupSet> sets)
      throws Exception {
    try {
      List<TExecutorGroupSet> result = select(sets, "no-such-pool");
      fail("expected an unknown request pool to be rejected, got " + result);
    } catch (AnalysisException e) {
      assertTrue(e.getMessage(),
          e.getMessage().contains("does not map to any known executor group set"));
    }
  }

  /** Unchanged without default executors. */
  @Test
  public void testCustomOnlyPoolSelectionUnchanged() throws Exception {
    List<TExecutorGroupSet> result = select(customOnlyGroupSets(), "bar");
    assertEquals(Arrays.asList("bar"), prefixesOf(result));
  }

  /** Unchanged without default executors: every group set stays a candidate. */
  @Test
  public void testCustomOnlyNoRequestPoolUnchanged() throws Exception {
    List<TExecutorGroupSet> result = select(customOnlyGroupSets(), NO_REQUEST_POOL);
    assertEquals(Arrays.asList("bar", "foo"), prefixesOf(result));
  }

  /** Unchanged on a default-only cluster: the caller flags it, no pool matching runs. */
  @Test
  public void testDefaultOnlyClusterUnchanged() throws Exception {
    List<TExecutorGroupSet> defaultOnly =
        new ArrayList<>(Arrays.asList(groupSet(1, DEFAULT_PREFIX)));
    List<TExecutorGroupSet> result = Frontend.setupThresholdsForExecutorGroupSets(
        defaultOnly, NO_REQUEST_POOL, /*default_executor_group*/ true,
        /*test_replan*/ false);
    assertEquals(Arrays.asList(DEFAULT_PREFIX), prefixesOf(result));
    assertEquals(Long.MAX_VALUE, result.get(0).getMax_mem_limit());
  }

  private static TExecRequest newExecRequest() {
    TExecRequest req = new TExecRequest();
    req.setStmt_type(TStmtType.QUERY);
    req.setQuery_options(new TQueryOptions());
    TQueryExecRequest execRequest = new TQueryExecRequest();
    execRequest.setQuery_ctx(new TQueryCtx());
    req.setQuery_exec_request(execRequest);
    return req;
  }

  private static void callSetGroupNamePrefix(TExecRequest req,
      TExecutorGroupSet groupSet) throws Exception {
    Method m = Frontend.class.getDeclaredMethod("setGroupNamePrefix", boolean.class,
        boolean.class, TExecRequest.class, TExecutorGroupSet.class);
    m.setAccessible(true);
    m.invoke(null, /*default_executor_group*/ false, /*clientSetRequestPool*/ false, req,
        groupSet);
  }

  /**
   * The selected default group set has no prefix to stamp. Stamping the empty one marks
   * REQUEST_POOL set-but-empty, which getTExecRequest() asserts against and child
   * queries inherit.
   */
  @Test
  public void testDefaultGroupSetLeavesRequestPoolUnset() throws Exception {
    TExecRequest req = newExecRequest();
    callSetGroupNamePrefix(req, groupSet(1, DEFAULT_PREFIX));
    assertFalse("REQUEST_POOL must stay unset, not set to an empty string",
        req.getQuery_options().isSetRequest_pool());
    assertFalse(req.isRequest_pool_set_by_frontend());
    assertFalse(req.getQuery_exec_request().getQuery_ctx().isSetRequest_pool());
  }

  /** A custom group set must still stamp its prefix. */
  @Test
  public void testCustomGroupSetStampsRequestPool() throws Exception {
    TExecRequest req = newExecRequest();
    callSetGroupNamePrefix(req, groupSet(2, "root.foo"));
    assertEquals("root.foo", req.getQuery_options().getRequest_pool());
    assertTrue(req.isRequest_pool_set_by_frontend());
    assertEquals(
        "root.foo", req.getQuery_exec_request().getQuery_ctx().getRequest_pool());
  }
}
