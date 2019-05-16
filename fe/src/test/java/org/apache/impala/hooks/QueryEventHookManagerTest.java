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

package org.apache.impala.hooks;

import org.apache.impala.common.InternalException;
import org.apache.impala.service.BackendConfig;
import org.apache.impala.testutil.AlwaysErrorQueryEventHook;
import org.apache.impala.testutil.CountingQueryEventHook;
import org.apache.impala.testutil.PostQueryErrorEventHook;
import org.apache.impala.thrift.TBackendGflags;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.util.List;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;

public class QueryEventHookManagerTest {
  private TBackendGflags origFlags;

  @Rule
  public ExpectedException expectedException = ExpectedException.none();
  private QueryCompleteContext mockQueryCompleteContext =
      new QueryCompleteContext("unit-test lineage");

  @Before
  public void setUp()  {
    // since some test cases will need to modify the (static)
    // be flags, we need to save the original values so they
    // can be restored and not break other tests
    if (BackendConfig.INSTANCE == null) {
      BackendConfig.create(new TBackendGflags());
    }
    origFlags = BackendConfig.INSTANCE.getBackendCfg();
  }

  @After
  public void tearDown() {
    BackendConfig.create(origFlags);
  }

  private static QueryEventHookManager createQueryEventHookManager(int nThreads,
      String... hooks) throws Exception {
    if (hooks.length == 0) {
      BackendConfig.INSTANCE.getBackendCfg().setQuery_event_hook_classes("");
    } else {
      BackendConfig.INSTANCE.getBackendCfg().setQuery_event_hook_classes(
          String.join(",", hooks));
    }

    BackendConfig.INSTANCE.getBackendCfg().setQuery_event_hook_nthreads(nThreads);

    return QueryEventHookManager.createFromConfig(BackendConfig.INSTANCE);
  }

  @Test
  public void testHookRegistration() throws Exception {
    final QueryEventHookManager mgr = createQueryEventHookManager(1,
    CountingQueryEventHook.class.getCanonicalName(),
        CountingQueryEventHook.class.getCanonicalName());

    final List<QueryEventHook> hooks = mgr.getHooks();
    assertEquals(2, hooks.size());
    hooks.forEach(h -> assertEquals(CountingQueryEventHook.class, h.getClass()));
  }

  @Test
  public void testHookPostQueryExecuteErrorsDoNotKillExecution() throws Exception {
    // a hook that exceptions should not prevent a subsequent hook from executing
    final QueryEventHookManager mgr = createQueryEventHookManager(1,
        PostQueryErrorEventHook.class.getCanonicalName(),
        CountingQueryEventHook.class.getCanonicalName());

    // make sure error hook will execute first
    assertEquals(mgr.getHooks().get(0).getClass(), PostQueryErrorEventHook.class);

    final List<Future<QueryEventHook>> futures =
        mgr.executeQueryCompleteHooks(mockQueryCompleteContext);

    // this should not exception
    final QueryEventHook hookImpl = futures.get(1).get(2, TimeUnit.SECONDS);

    assertEquals(hookImpl.getClass(), CountingQueryEventHook.class);
  }

  @Test
  public void testHookExceptionDuringStartupKillsStartup() throws Exception {
    expectedException.expect(InternalException.class);

    createQueryEventHookManager(1,
        AlwaysErrorQueryEventHook.class.getCanonicalName(),
        CountingQueryEventHook.class.getCanonicalName());
  }

  @Test
  public void testHookPostQueryExecuteInvokedCorrectly() throws Exception {
    final QueryEventHookManager mgr = createQueryEventHookManager(1,
        CountingQueryEventHook.class.getCanonicalName(),
        CountingQueryEventHook.class.getCanonicalName());

    List<Future<QueryEventHook>> futures =
        mgr.executeQueryCompleteHooks(mockQueryCompleteContext);

    assertEquals(
        futures.size(),
        mgr.getHooks().size());

    for (Future<QueryEventHook> f : futures) {
      CountingQueryEventHook hook = (CountingQueryEventHook) f.get(2, TimeUnit.SECONDS);
      assertEquals(1, hook.getPostQueryExecuteInvocations());
    }

    futures = mgr.executeQueryCompleteHooks(mockQueryCompleteContext);

    assertEquals(
        futures.size(),
        mgr.getHooks().size());

    for (Future<QueryEventHook> f : futures) {
      CountingQueryEventHook hook = (CountingQueryEventHook) f.get(2, TimeUnit.SECONDS);
      assertEquals(2, hook.getPostQueryExecuteInvocations());
    }
  }

}

