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

import org.apache.commons.lang.StringUtils;
import org.apache.impala.common.InternalException;
import org.apache.impala.service.BackendConfig;
import org.apache.impala.thrift.TBackendGflags;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.stream.Collectors;

import com.google.common.util.concurrent.ThreadFactoryBuilder;

/**
 * {@link QueryEventHookManager} manages the registration and execution of
 * {@link QueryEventHook}s. Each manager instance may manage its own hooks,
 * though the expected use-case is to have 1 instance per process, usually
 * owned by the frontend. This class is not thread-safe.
 *
 * <h3>Hook Registration</h3>
 *
 * The hook implementation(s) to use at runtime are specified through the
 * backend config flag {@link TBackendGflags#query_event_hook_classes}
 * at Impala startup. See {@link #createFromConfig(BackendConfig)}.
 *
 * <h3>Hook Classloading</h3>
 *
 * Each hook implementation is loaded using `this` manager's classloader; no
 * classloader isolation is performed.  Individual hook implementations should
 * take care to properly handle any dependencies they bring in to avoid shadowing
 * existing dependencies on the Impala classpath.
 *
 * <h3>Hook Execution</h3>
 *
 * Hook initialization ({@link QueryEventHook#onImpalaStartup()} is
 * performed synchronously during {@link #createFromConfig(BackendConfig)}.
 * <p>
 * {@link QueryEventHook#onQueryComplete(QueryCompleteContext)} is performed
 * asynchronously during {@link #executeQueryCompleteHooks(QueryCompleteContext)}.
 * This execution is performed by a thread-pool executor, whose size is set at
 * compile-time.  This means that hooks may also execute concurrently.
 * </p>
 *
 */
public class QueryEventHookManager {
  private static final Logger LOG =
      LoggerFactory.getLogger(QueryEventHookManager.class);

  // TODO: figure out a way to source these from the defn so
  //       we don't have to manually sync when they change
  private static final String BE_HOOKS_FLAG = "query_event_hook_classes";
  private static final String BE_HOOKS_THREADS_FLAG = "query_event_hook_nthreads";

  private final List<QueryEventHook> hooks_;
  private final ExecutorService hookExecutor_;

  /**
   * Static factory method to create a manager instance.  This will register
   * all {@link QueryEventHook}s specified by the backend config flag
   * {@code query_event_hook_classes} and then invoke their
   * {@link QueryEventHook#onImpalaStartup()} methods synchronously.
   *
   * @throws IllegalArgumentException if config is invalid
   * @throws InternalException if any hook could not be instantiated
   * @throws InternalException if any hook.onImpalaStartup() throws an exception
   */
  public static QueryEventHookManager createFromConfig(BackendConfig config)
      throws InternalException {

    final int nHookThreads = config.getNumQueryExecHookThreads();
    final String queryExecHookClasses = config.getQueryExecHookClasses();
    LOG.info("QueryEventHook config:");
    LOG.info("- {}={}", BE_HOOKS_THREADS_FLAG, nHookThreads);
    LOG.info("- {}={}", BE_HOOKS_FLAG, queryExecHookClasses);

    final String[] hookClasses;
    if (StringUtils.isNotEmpty(queryExecHookClasses)) {
      hookClasses = queryExecHookClasses.split("\\s*,\\s*");
    } else {
      hookClasses = new String[0];
    }

    return new QueryEventHookManager(nHookThreads, hookClasses);
  }

  /**
   * Instantiates a manager with a fixed-size thread-pool executor for
   * executing {@link QueryEventHook#onQueryComplete(QueryCompleteContext)}.
   *
   * @param nHookExecutorThreads
   * @param hookClasses
   *
   * @throws IllegalArgumentException if {@code nHookExecutorThreads <= 0}
   * @throws InternalException if any hookClass cannot be instantiated
   * @throws InternalException if any hookClass.onImpalaStartup throws an exception
   */
  private QueryEventHookManager(int nHookExecutorThreads, String[] hookClasses)
      throws InternalException {

    this.hookExecutor_ = Executors.newFixedThreadPool(nHookExecutorThreads,
        new ThreadFactoryBuilder().setNameFormat("QueryEventHookExecutor-%d").build());
    Runtime.getRuntime().addShutdownHook(new Thread(() -> this.cleanUp()));

    final List<QueryEventHook> hooks = new ArrayList<>(hookClasses.length);
    this.hooks_ = Collections.unmodifiableList(hooks);

    for (String postExecHook : hookClasses) {
      final QueryEventHook hook;
      try {
        final Class<QueryEventHook> clsHook =
            (Class<QueryEventHook>) Class.forName(postExecHook);
        hook = clsHook.newInstance();
      } catch (InstantiationException
          | IllegalAccessException
          | ClassNotFoundException e) {
        final String msg = String.format(
            "Unable to instantiate query event hook class %s. Please check %s config",
            postExecHook, BE_HOOKS_FLAG);
        LOG.error(msg, e);
        throw new InternalException(msg, e);
      }

      hooks.add(hook);
    }

    for (QueryEventHook hook : hooks) {
      try {
        LOG.debug("Initiating hook.onImpalaStartup for {}", hook.getClass().getName());
        hook.onImpalaStartup();
      }
      catch (Exception e) {
        final String msg = String.format(
            "Exception during onImpalaStartup from QueryEventHook %s instance=%s",
            hook.getClass(), hook);
        LOG.error(msg, e);
        throw new InternalException(msg, e);
      }
    }
  }

  private void cleanUp() {
    if (!hookExecutor_.isShutdown()) {
      hookExecutor_.shutdown();
    }
    // TODO (IMPALA-8571): we may want to await termination (up to a timeout)
    // to ensure that hooks have a chance to complete execution.  Executor
    // threads will typically run to completion after executor shutdown, but
    // there are some instances where this doesnt hold. e.g.
    //
    // - executor thread is sleeping when shutdown is called
    // - system.exit called
  }

  /**
   * Returns an unmodifiable view of all the {@link QueryEventHook}s
   * registered at construction.
   *
   * @return unmodifiable view of all currently-registered hooks
   */
  public List<QueryEventHook> getHooks() {
    return hooks_;
  }

  /**
   * Hook method to be called after query execution.  This implementation
   * will execute all currently-registered {@link QueryEventHook}s
   * asynchronously, returning immediately with a List of {@link Future}s
   * representing each hook's {@link QueryEventHook#onQueryComplete(QueryCompleteContext)}
   * invocation.
   *
   * <h3>Futures</h3>
   *
   * This method will return a list of {@link Future}s representing the future results
   * of each hook's invocation.  The {@link Future#get()} method will return the
   * hook instance whose invocation it represents.  The list of futures are in the
   * same order as the order in which each hook's job was submitted.
   *
   * <h3>Error-Handling</h3>
   *
   * Exceptions thrown from {@link QueryEventHook#onQueryComplete(QueryCompleteContext)}
   * will be logged and then rethrown on the executor thread(s), meaning that they
   * will not halt execution.  Rather, they will be encapsulated in the returned
   * {@link Future}s, meaning that the caller may choose to check or ignore them
   * at some later time.
   *
   * @param context
   */
  public List<Future<QueryEventHook>> executeQueryCompleteHooks(
      QueryCompleteContext context) {
    LOG.debug("Query complete hook invoked with: {}", context);
    return hooks_.stream().map(hook -> {
      LOG.debug("Initiating onQueryComplete: {}", hook.getClass().getName());
      return hookExecutor_.submit(() -> {
        try {
          hook.onQueryComplete(context);
        } catch (Throwable t) {
          final String msg = String.format("Exception thrown by QueryEventHook %s"+
              ".onQueryComplete method.  Hook instance %s. This exception is "+
              "currently being ignored by Impala, "+
              "but may cause subsequent problems in that hook's execution",
              hook.getClass().getName(), hook);
          LOG.error(msg, t);
          throw t;
        }
        return hook;
      });
    }).collect(Collectors.toList());
  }
}
