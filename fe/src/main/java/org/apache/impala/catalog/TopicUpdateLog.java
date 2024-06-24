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

package org.apache.impala.catalog;

import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;

import org.apache.impala.service.BackendConfig;

// A log of topic update information for each catalog object. An entry is added to
// the log when a catalog object is processed (added/removed/skipped) in a topic
// update and it is replaced every time the catalog object is processed in a
// topic update.
//
// To prevent the log from growing indefinitely, the oldest entries
// (in terms of last topic update that processed the associated catalog objects) are
// garbage collected every topicUpdateLogGcFrequency_ topic updates. That will cause
// entries of deleted catalog objects or entries of objects that haven't been processed
// by the catalog for at least topicUpdateLogGcFrequency_ updates to be removed from
// the log.
public class TopicUpdateLog {
  private static final Logger LOG = LoggerFactory.getLogger(TopicUpdateLog.class);
  // Frequency at which the entries of the topic update log are garbage collected.
  // An entry may survive for (2 * topicUpdateLogGcFrequency_) - 1 topic updates.
  private int topicUpdateLogGcFrequency_ = BackendConfig.INSTANCE
      .getBackendCfg().topic_update_log_gc_frequency;

  // Number of topic updates left to trigger a gc of topic update log entries.
  private int numTopicUpdatesToGc_ = topicUpdateLogGcFrequency_;

  // In the next gc cycle of topic update log entries, all the entries that were last
  // added to a topic update with version less than or equal to
  // 'oldestTopicUpdateToGc_' are removed from the update log.
  private long oldestTopicUpdateToGc_ = -1;

  // Represents an entry in the topic update log. A topic update log entry is
  // associated with a catalog object and stores information about the last topic update
  // that processed that object.
  public static class Entry {
    // Number of times the entry has skipped a topic update because the table version
    // was out of the requested update version window.
    private final int numSkippedUpdates_;
    // Last version of the corresponding catalog object that was added to a topic
    // update. -1 if the object was never added to a topic update.
    private final long lastSentVersion_;
    // Version of the last topic update to include the corresponding catalog object.
    // -1 if the object was never added to a topic update.
    private final long lastSentTopicUpdate_;
    // number of time the topic update skipped this table due to lock contention
    private final int numSkippedUpdatesLockContention_;

    Entry() {
      numSkippedUpdates_ = 0;
      lastSentVersion_ = -1;
      lastSentTopicUpdate_ = -1;
      numSkippedUpdatesLockContention_ = 0;
    }

    Entry(int numSkippedUpdates, long lastSentVersion, long lastSentCatalogUpdate,
        int numSkippedUpdatesLockContention) {
      numSkippedUpdates_ = numSkippedUpdates;
      lastSentVersion_ = lastSentVersion;
      lastSentTopicUpdate_ = lastSentCatalogUpdate;
      numSkippedUpdatesLockContention_ = numSkippedUpdatesLockContention;
    }

    public int getNumSkippedTopicUpdates() { return numSkippedUpdates_; }
    public long getLastSentVersion() { return lastSentVersion_; }
    public long getLastSentCatalogUpdate() { return lastSentTopicUpdate_; }

    public int getNumSkippedUpdatesLockContention() {
      return numSkippedUpdatesLockContention_;
    }

    @Override
    public boolean equals(Object other) {
      if (this.getClass() != other.getClass()) return false;
      Entry entry = (Entry) other;
      return numSkippedUpdates_ == entry.getNumSkippedTopicUpdates()
          && lastSentVersion_ == entry.getLastSentVersion()
          && lastSentTopicUpdate_ == entry.getLastSentCatalogUpdate()
          && numSkippedUpdatesLockContention_ == entry
          .getNumSkippedUpdatesLockContention();
    }

    @Override
    public int hashCode() {
      return Objects.hash(
          numSkippedUpdates_,
          lastSentVersion_,
          lastSentTopicUpdate_,
          numSkippedUpdatesLockContention_);
    }
  }

  // Entries in the topic update log stored as a map of catalog object keys to
  // Entry objects.
  private final Map<String, Entry> topicLogEntries_ =
      new ConcurrentHashMap<>();

  /**
   * Garbage-collects topic update log entries. These are entries that haven't been
   * added to any of the last topicUpdateLogGcFrequency_ topic updates.
   */
  public void garbageCollectUpdateLogEntries(long lastTopicUpdateVersion) {
    if (oldestTopicUpdateToGc_ == -1) {
      oldestTopicUpdateToGc_ = lastTopicUpdateVersion;
      return;
    }
    if (numTopicUpdatesToGc_ == 0) {
      LOG.info("Topic update log GC started. GC-ing topics with versions " +
          "<= {}", oldestTopicUpdateToGc_);
      Preconditions.checkState(oldestTopicUpdateToGc_ > 0);
      int numEntriesRemoved = 0;
      for (Map.Entry<String, Entry> entry:
           topicLogEntries_.entrySet()) {
        if (entry.getValue().getLastSentVersion() == -1) continue;
        if (entry.getValue().getLastSentCatalogUpdate() <= oldestTopicUpdateToGc_) {
          if (topicLogEntries_.remove(entry.getKey(), entry.getValue())) {
            ++numEntriesRemoved;
          }
        }
      }
      numTopicUpdatesToGc_ = topicUpdateLogGcFrequency_;
      oldestTopicUpdateToGc_ = lastTopicUpdateVersion;
      LOG.info("Topic update log GC finished. Removed {} entries.",
          numEntriesRemoved);
    } else {
      --numTopicUpdatesToGc_;
    }
  }

  public void add(String catalogObjectKey, Entry logEntry) {
    Preconditions.checkState(!Strings.isNullOrEmpty(catalogObjectKey));
    Preconditions.checkNotNull(logEntry);
    topicLogEntries_.put(catalogObjectKey, logEntry);
  }

  public Entry get(String catalogObjectKey) {
    Preconditions.checkState(!Strings.isNullOrEmpty(catalogObjectKey));
    return topicLogEntries_.get(catalogObjectKey);
  }

  // Returns the topic update log entry for the catalog object with key
  // 'catalogObjectKey'. If the key does not exist, a newly constructed log entry is
  // returned.
  public Entry getOrCreateLogEntry(String catalogObjectKey) {
    Preconditions.checkState(!Strings.isNullOrEmpty(catalogObjectKey));
    Entry entry = topicLogEntries_.get(catalogObjectKey);
    if (entry == null) entry = new Entry();
    return entry;
  }

  public long getOldestTopicUpdateToGc() {
    return oldestTopicUpdateToGc_;
  }
}

