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

import com.google.common.base.Joiner;
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
  private static final int DEFAULT_MAX_NUMBER_OF_INFLIGHT_EVENTS = 100;

  // maximum number of eventIds to store for in-flight events for this table
  private static final int DEFAULT_MAX_NUMBER_OF_INFLIGHT_INSERT_EVENTS = 100;

  private static final Logger LOG = LoggerFactory.getLogger(InFlightEvents.class);
  // FIFO list of versions for all the in-flight metastore DDL events in this table
  // This queue can only grow up to MAX_NUMBER_OF_INFLIGHT_EVENTS size. Anything which
  // is attempted to be added to this list when its at maximum capacity is ignored
  private final LinkedList<Long> versionsForInflightEvents_ = new LinkedList<>();

  // FIFO list of eventIds for all the in-flight metastore Insert events in this table
  // This queue can only grow up to MAX_NUMBER_OF_INFLIGHT_INSERT_EVENTS size. Anything
  // which is attempted to be added to this list when its at maximum capacity is ignored
  private final LinkedList<Long> idsForInflightDmlEvents_ = new LinkedList<>();

  // maximum number of versions to store
  private final int capacity_for_versions_;

  // maximum number of eventIds to store
  private final int capacity_for_eventIds_;

  public InFlightEvents() {
    this.capacity_for_versions_ = DEFAULT_MAX_NUMBER_OF_INFLIGHT_EVENTS;
    this.capacity_for_eventIds_ = DEFAULT_MAX_NUMBER_OF_INFLIGHT_INSERT_EVENTS;
  }

  /**
   * Gets the current list of versions for in-flight events for this table
   * @param isInsertEvent if true, return list of eventIds for in-flight Insert events
   * if false, return list of versions for in-flight DDL events
   */
  public List<Long> getAll(boolean isInsertEvent) {
    if (isInsertEvent) {
      return ImmutableList.copyOf(idsForInflightDmlEvents_);
    } else {
      return ImmutableList.copyOf(versionsForInflightEvents_);
    }
  }

  /**
   * Removes a given version from the collection of version numbers for in-flight
   * events.
   * @param isInsertEvent If true, remove eventId from list of eventIds for in-flight
   * Insert events. If false, remove version number from list of versions for in-flight
   * DDL events.
   * @param versionNumber when isInsertEvent is true, it's eventId to remove
   * when isInsertEvent is false, it's version number to remove
   * @return true if the version was found and successfully removed, false
   * otherwise
   */
  public boolean remove(boolean isInsertEvent, long versionNumber) {
    if (isInsertEvent) {
      return idsForInflightDmlEvents_.remove(versionNumber);
    } else {
      return versionsForInflightEvents_.remove(versionNumber);
    }
  }

  /**
   * Adds a version number to the collection of versions for in-flight events. If the
   * collection is already at the max size defined by
   * <code>MAX_NUMBER_OF_INFLIGHT_EVENTS</code>, then it ignores the given version and
   * does not add it
   * @param isInsertEvent If true, add eventId to list of eventIds for in-flight Insert
   * events. If false, add versionNumber to list of versions for in-flight DDL events.
   * @param versionNumber when isInsertEvent is true, it's eventId to add
   * when isInsertEvent is false, it's version number to add
   * @return True if version number was added, false if the collection is at its max
   * capacity
   */
  public boolean add(boolean isInsertEvent, long versionNumber) {
    if (isInsertEvent) {
      if (idsForInflightDmlEvents_.size() == capacity_for_eventIds_) {
        LOG.warn(String.format("Number of Insert events to be stored is at "
                + " its max capacity %d. Ignoring add request for eventId %d.",
            DEFAULT_MAX_NUMBER_OF_INFLIGHT_EVENTS, versionNumber));
        return false;
      }
      idsForInflightDmlEvents_.add(versionNumber);
    } else {
      if (versionsForInflightEvents_.size() == capacity_for_versions_) {
        LOG.warn(String.format("Number of DDL events to be stored is at "
                + "its max capacity %d. Ignoring add request for version %d.",
            DEFAULT_MAX_NUMBER_OF_INFLIGHT_EVENTS, versionNumber));
        return false;
      }
      versionsForInflightEvents_.add(versionNumber);
    }
    return true;
  }

  /**
   * Get the size of in-flight DDL or DML events list
   * @param isInsertEvent if true, return size of Insert events list
   *              if false, return size of DDL events list
   * @return size of events list
   */
  public int size(boolean isInsertEvent) {
    if (isInsertEvent) {
      return idsForInflightDmlEvents_.size();
    } else {
      return versionsForInflightEvents_.size();
    }
  }

  /**
   * String representation of the current InFlightEvents. Useful for logging and debugging
   * purposes.
   */
  public String print() {
    return Joiner.on(',').join(versionsForInflightEvents_);
  }
}
