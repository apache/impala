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

import java.io.File;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.util.concurrent.ThreadFactoryBuilder;

/**
 * Service to watch a file for changes. A thread periodically checks the file
 * modification time and uses the provided {@link FileChangeListener} to notify
 * a consumer.
 */
public class FileWatchService {
  final static Logger LOG = LoggerFactory.getLogger(FileWatchService.class);

  // Default time to wait between checking the file.
  static final long DEFAULT_CHECK_INTERVAL_MS = 10 * 1000;

  // Time between checking for changes. Mutable for unit tests.
  private long checkIntervalMs_ = DEFAULT_CHECK_INTERVAL_MS;

  // Future returned by scheduleAtFixedRate(), needed to stop the checking thread.
  private ScheduledFuture<?> fileCheckFuture_;

  private final AtomicBoolean running_;
  private final FileChangeListener changeListener_; // Used to notify when changes occur.
  private final File file_; // The file to check for changes.
  private boolean alreadyWarned_; // Avoid repeatedly warning if the file is missing
  private long prevChange_; // Time of the last observed change

  /**
   * Listener used to notify of file changes.
   */
  public interface FileChangeListener {

    /**
     * Called when the file changes.
     */
    void onFileChange();
  }

  public FileWatchService(File file, FileChangeListener listener) {
    Preconditions.checkNotNull(file);
    Preconditions.checkNotNull(listener);
    Preconditions.checkArgument(file.exists());
    running_ = new AtomicBoolean(false);
    file_ = file;
    changeListener_ = listener;
    prevChange_ = 0L;
    alreadyWarned_ = false;
  }

  /**
   * Set the time (in milliseconds) to wait between checking the file for changes.
   * Only used in tests.
   */
  @VisibleForTesting
  public void setCheckIntervalMs(long checkIntervalMs) {
    checkIntervalMs_ = checkIntervalMs;
  }

  /**
   * Checks if the file has changed since the last observed change and if so,
   * notifies the listener.
   */
  private void checkFile() {
    if (file_.exists()) {
      long lastChange = file_.lastModified();
      if (lastChange > prevChange_) {
        changeListener_.onFileChange();
        prevChange_ = lastChange;
        alreadyWarned_ = false;
      }
    } else {
      if (!alreadyWarned_) {
        LOG.warn("File does not exist: {}", file_.getPath());
        alreadyWarned_ = true;
      }
    }
  }

  /**
   * Starts the thread to check for file changes. Continues checking for file changes
   * every 'checkIntervalMs_' milliseconds until stop() is called.
   */
  public synchronized void start() {
    Preconditions.checkState(!running_.get());
    running_.set(true);

    ScheduledExecutorService executor = Executors.newSingleThreadScheduledExecutor(
        new ThreadFactoryBuilder()
        .setDaemon(true)
        .setNameFormat("FileWatchThread(" + file_.getPath() + ")-%d")
        .build());
    fileCheckFuture_ = executor.scheduleAtFixedRate(new Runnable() {
      public void run() {
        try {
          checkFile();
        } catch (SecurityException e) {
          LOG.warn("Not allowed to check read file existence: " + file_.getPath(), e);
        }
      }
    }, 0L, checkIntervalMs_, TimeUnit.MILLISECONDS);
  }

  /**
   * Stops the file watching thread.
   */
  public synchronized void stop() {
    Preconditions.checkState(running_.get());
    running_.set(false);
    fileCheckFuture_.cancel(false);
  }
}