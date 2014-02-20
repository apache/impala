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
import java.util.concurrent.atomic.AtomicBoolean;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;

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

  // TODO: See if we can use Executors.newScheduledThreadPool
  private Thread watchThread_;
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
    watchThread_ = new Thread() {
      @Override
      public void run() {
        while (running_.get()) {
          try {
            checkFile();
          } catch (SecurityException e) {
            LOG.warn("Not allowed to check read file existence: " + file_.getPath(), e);
          }

          try {
            Thread.sleep(checkIntervalMs_);
          } catch (InterruptedException ex) {
            LOG.info("Interrupted while waiting to check for file changes.");
          }
        }
      }
    };
    watchThread_.setName("FileWatchThread(" + file_.getPath() + ")");
    watchThread_.setDaemon(true);
    watchThread_.start();
  }

  /**
   * Stops the file watching thread.
   */
  public synchronized void stop() {
    Preconditions.checkState(running_.get());
    running_.set(false);
    watchThread_.interrupt();
  }
}