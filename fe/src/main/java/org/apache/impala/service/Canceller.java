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

import static org.apache.impala.util.TUniqueIdUtil.PrintId;

import com.google.common.base.Preconditions;

import org.apache.impala.common.Pair;
import org.apache.impala.common.UserCancelledException;
import org.apache.impala.thrift.TUniqueId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.Lock;

/**
 * Global Java thread cancellation handler. Query-related operations register their thread
 * for a given query ID, allowing a Cancel call to interrupt that thread when requested.
 *
 * Uses a separate UserCancelledException to interrupt execution, as that has a more
 * specific meaning than InterruptedException (which our code sometimes ignores).
 */
public final class Canceller {
  private final static Logger LOG = LoggerFactory.getLogger(Canceller.class);

  // Registers threads by their query ID so the threads can be interrupted on
  // cancellation, along with a thread-local flag to indicate cancellation.
  private final static ConcurrentHashMap<TUniqueId, Pair<Thread, AtomicBoolean>>
      queryThreads_ = new ConcurrentHashMap<>();
  // Registers that the current thread has been cancelled and should clean up.
  private final static ThreadLocal<AtomicBoolean> cancelled_ = new ThreadLocal<>();

  private Canceller() { /* Private constructor to prevent instantiation. */ }

  public static class Registration implements AutoCloseable {
    private final TUniqueId queryId_;

    public Registration(TUniqueId queryId) { queryId_ = queryId; }

    /**
     * Removes association of current thread with queryId.
     */
    public void close() {
      LOG.trace("unregister {}", PrintId(queryId_));
      Pair<Thread, AtomicBoolean> curr = queryThreads_.remove(queryId_);
      Preconditions.checkState(curr.first == Thread.currentThread());
      Preconditions.checkState(curr.second == cancelled_.get());
      cancelled_.remove();
    }
  }

  /**
   * Associates the current thread with queryId.
   */
  public static Registration register(TUniqueId queryId) {
    if (queryId == null) return null;
    LOG.trace("register {}", PrintId(queryId));
    cancelled_.set(new AtomicBoolean(false));
    queryThreads_.put(queryId, new Pair<>(Thread.currentThread(), cancelled_.get()));
    return new Registration(queryId);
  }

  /**
   * Cancels thread associated with queryId.
   */
  public static void cancel(TUniqueId queryId) {
    if (queryId == null) return;
    Pair<Thread, AtomicBoolean> queryPair = queryThreads_.get(queryId);
    if (queryPair == null) {
      LOG.info(
          "Unable to cancel request: thread for query {} not found", PrintId(queryId));
      return;
    }

    Thread queryThread = queryPair.first;
    LOG.debug(
        "Cancelling request: thread {} for query {}", queryThread, PrintId(queryId));
    queryPair.second.set(true);
    queryThread.interrupt();
  }

  /**
   * Throws UserCancelledException if the current thread is cancelled.
   */
  public static void throwIfCancelled() throws UserCancelledException {
    if (isCancelled()) { throw new UserCancelledException(); }
  }

  private static boolean isCancelled() {
    AtomicBoolean cancelled = cancelled_.get();
    return cancelled != null && cancelled.get();
  }
}
