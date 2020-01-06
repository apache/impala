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

package org.apache.impala.catalog.events;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import java.util.LinkedList;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class is used to keep track of the in-flight events after a DDL operation is
 * completed. The MetastoreEventsProcessor uses this information to determine if a
 * received event is self-generated or not. Not thread-safe.
 */
public class InFlightEvents {

  // maximum number of catalog versions to store for in-flight events for this table
  private static final int DEFAULT_MAX_NUMBER_OF_INFLIGHT_EVENTS = 10;

  private static final Logger LOG = LoggerFactory.getLogger(InFlightEvents.class);
  // FIFO list of versions for all the in-flight metastore events in this table
  // This queue can only grow up to MAX_NUMBER_OF_INFLIGHT_EVENTS size. Anything which
  // is attempted to be added to this list when its at maximum capacity is ignored
  private final LinkedList<Long> versionsForInflightEvents_ = new LinkedList<>();

  // maximum number of versions to store
  private final int capacity_;

  public InFlightEvents() {
    this.capacity_ = DEFAULT_MAX_NUMBER_OF_INFLIGHT_EVENTS;
  }

  public InFlightEvents(int capacity) {
    Preconditions.checkState(capacity > 0);
    this.capacity_ = capacity;
  }

  /**
   * Gets the current list of versions for in-flight events for this table
   */
  public List<Long> getAll() {
    return ImmutableList.copyOf(versionsForInflightEvents_);
  }

  /**
   * Removes a given version from the collection of version numbers for in-flight
   * events.
   *
   * @param versionNumber version number to remove from the collection
   * @return true if the version was found and successfully removed, false
   * otherwise
   */
  public boolean remove(long versionNumber) {
    return versionsForInflightEvents_.remove(versionNumber);
  }

  /**
   * Adds a version number to the collection of versions for in-flight events. If the
   * collection is already at the max size defined by
   * <code>MAX_NUMBER_OF_INFLIGHT_EVENTS</code>, then it ignores the given version and
   * does not add it
   *
   * @param versionNumber version number to add
   * @return True if version number was added, false if the collection is at its max
   * capacity
   */
  public boolean add(long versionNumber) {
    if (versionsForInflightEvents_.size() == capacity_) {
      LOG.warn(String.format("Number of versions to be stored is at "
              + " its max capacity %d. Ignoring add request for version number %d.",
          DEFAULT_MAX_NUMBER_OF_INFLIGHT_EVENTS, versionNumber));
      return false;
    }
    versionsForInflightEvents_.add(versionNumber);
    return true;
  }

  public int size() {
    return versionsForInflightEvents_.size();
  }
}
