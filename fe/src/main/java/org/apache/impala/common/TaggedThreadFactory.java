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

package org.apache.impala.common;

import org.apache.impala.service.FeSupport;
import org.apache.impala.thrift.TUniqueId;
import org.apache.impala.util.TUniqueIdUtil;
import org.apache.thrift.TException;
import org.apache.thrift.TSerializer;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * ThreadFactory to create threads that can be tagged with query ids which will appear in
 * their logs. The query id is stored in a thread-local ThreadDebugInfo variable in the
 * Backend. GLog retrieves the query id from it and prepend it to the logs. See more
 * details in common/thread-debug-info.h and MessageListener() in common/logging.cc.
 */
public class TaggedThreadFactory implements ThreadFactory {
  private final static Logger LOG = LoggerFactory.getLogger(TaggedThreadFactory.class);
  private final static TUniqueId ZERO_QUERY_ID = new TUniqueId();
  private final static AtomicInteger poolNumber = new AtomicInteger(0);
  private final String nameFormat_;
  private byte[] thriftQueryId_;

  /**
   * Initialize threads without query ids. They can be updated later.
   */
  public TaggedThreadFactory(String nameFormat) {
    this(ZERO_QUERY_ID, nameFormat);
  }

  /**
   * Initialize threads with a given query id.
   */
  public TaggedThreadFactory(TUniqueId queryId, String nameFormat) {
    nameFormat_ = nameFormat;
    if (queryId == null) queryId = ZERO_QUERY_ID;
    try {
      thriftQueryId_ = new TSerializer().serialize(queryId);
    } catch (TException e) {
      LOG.error("Failed to serialize query id {}", TUniqueIdUtil.PrintId(queryId));
    }
  }

  @Override
  public Thread newThread(@NotNull Runnable r) {
    Runnable initializerRunnable = () -> {
      long ptr = 0;
      try {
        ptr = FeSupport.NativeInitThreadDebugInfo(thriftQueryId_);
        r.run();
      } catch (Throwable e) {
        LOG.error("Pool thread exception", e);
      } finally {
        // The thread-local ThreadDebugInfo variable is owned by the Java thread so we
        // should delete it at the end.
        FeSupport.NativeDeleteThreadDebugInfo(ptr);
      }
    };
    return new Thread(initializerRunnable, String.format(
        nameFormat_, poolNumber.getAndIncrement()));
  }

  public static void updateQueryId(TUniqueId queryId) {
    if (queryId == null) return;
    try {
      FeSupport.NativeUpdateThreadDebugInfo(new TSerializer().serialize(queryId));
    } catch (TException e) {
      LOG.error("Failed to update query id {}", TUniqueIdUtil.PrintId(queryId));
    }
  }

  public static void resetQueryId() {
    FeSupport.NativeResetThreadDebugInfo();
  }
}
