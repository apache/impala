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

import com.google.common.base.Preconditions;

/**
 * AutoCloseable implementation which modifies the current thread's name during
 * a scope. This can be used in a try-with-resources block around long operations
 * in order to make it easier for an operator to understand what's going on while
 * reading a jstack. For example, when making calls to external services which might
 * respond slowly, it may be useful to know more details of the higher-level operation
 * that is blocked or which service is being waited upon.
 *
 * Intended to be used by the renamed thread itself to temporarily rename that
 * thread. Used in lieu of renaming the thread at thread start or when obtaining
 * a thread from the thread pool. Primarily for debugging as above. Is not a
 * substitute for logging since the name is ephemeral and only available via jstack.
 *
 * Example usage:
 * <code>
 *   try (ThreadNameAnnotator tna = new ThreadNameAnnotator("downloading " + url)) {
 *     doFetch(url);
 *   }
 * </code>
 */
public class ThreadNameAnnotator implements AutoCloseable {
  private final Thread thr_;
  private final String oldName_;
  private final String newName_;

  public ThreadNameAnnotator(String annotation) {
    thr_ = Thread.currentThread();
    if ("main".equals(thr_.getName())) {
      // Use the process name from sun.java.command.
      oldName_ = System.getProperty("sun.java.command", thr_.getName());
    } else {
      oldName_ = thr_.getName();
    }
    newName_ = oldName_ + " [" + annotation + "]";
    thr_.setName(newName_);
  }

  @Override
  public void close() {
    // Must be called in the renamed thread itself.
    Preconditions.checkState(thr_ == Thread.currentThread());
    // Only reset the thread name if it hasn't been changed by someone else in the
    // meantime.
    if (thr_.getName().equals(newName_)) thr_.setName(oldName_);
  }
}
