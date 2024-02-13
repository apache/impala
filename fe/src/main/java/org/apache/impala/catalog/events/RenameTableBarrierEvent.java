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

import com.codahale.metrics.Timer;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;

import org.apache.impala.catalog.CatalogException;
import org.apache.impala.catalog.Db;
import org.apache.impala.catalog.Table;
import org.apache.impala.catalog.events.MetastoreEvents.AlterTableEvent;
import org.apache.impala.catalog.events.MetastoreEvents.DerivedMetastoreEvent;
import org.apache.impala.catalog.events.MetastoreEvents.MetastoreEventType;
import org.apache.impala.catalog.events.MetastoreEvents.MetastoreTableEvent;
import org.apache.impala.common.Reference;
import org.apache.impala.service.BackendConfig;
import org.apache.impala.util.DebugUtils;

import static org.apache.impala.catalog.Table.TBL_EVENTS_PROCESS_DURATION;

/**
 * An instance of this class is used to synchronize the source and target TableProcessors
 * to process rename table event. It holds the reference to alter table event,
 * pseudo-event(either drop or create table) and the combined processing state of the
 * pseudo-events.
 * <p>
 * When alter table event for rename is seen at the event dispatcher, two instances of
 * this class are created. One instance is with a drop table pseudo-event and the other
 * is with a create table pseudo-event. Both instances have the same reference to
 * processing state and actual alter table event. These events are dispatched to
 * appropriate DbEventExecutor.
 * <p>
 * When a TableProcessor's event processing encounter an instance of
 * {@link RenameTableBarrierEvent}, it determines whether the event can be processed at
 * the moment with {@link RenameTableBarrierEvent#canProcess()}. Event can be processed
 * only iff it is drop table pseudo-event, or it is create table pseudo-event and drop
 * table pseudo-event is already processed.
 * <p>
 * This ensures that table is removed from the source TableProcessor prior to creation
 * on the target TableProcessor.
 *
 * @see org.apache.impala.catalog.events.TableEventExecutor.TableProcessor
 */
public class RenameTableBarrierEvent extends MetastoreTableEvent
    implements DerivedMetastoreEvent {
  // alter table event with rename true
  private final AlterTableEvent actualEvent_;

  // Drop table or Create table event
  private final MetastoreTableEvent pseudoEvent_;

  // Combined event processing state of both the pseudo-events
  private final RenameEventState state_;

  /**
   * Rename event processing state of alter table rename event. It is combined state of
   * both pseudo-events.
   */
  static class RenameEventState {
    // Whether pseudo drop table event is skipped upon the event process.
    private boolean dropSkipped_ = false;

    // Whether pseudo drop table event is processed.
    private boolean dropProcessed_ = false;

    // Whether pseudo create table event is skipped upon the event process.
    private boolean createSkipped_ = false;

    // Whether pseudo create table event is processed.
    private boolean createProcessed_ = false;

    /**
     * Determines whether pseudo drop table event is processed.
     * @return
     */

    synchronized boolean isDropProcessed() {
      return dropProcessed_;
    }

    /**
     * Determines whether pseudo create table event is processed.
     * @return
     */

    synchronized boolean isCreateProcessed() {
      return createProcessed_;
    }

    /**
     * Sets the pseudo-event process result
     * @param eventType
     * @param skipped
     */
    synchronized void setProcessed(MetastoreEventType eventType, boolean skipped) {
      if (eventType == MetastoreEventType.DROP_TABLE) {
        Preconditions.checkState(!dropProcessed_);
        dropSkipped_ = skipped;
        dropProcessed_ = true;
      } else {
        Preconditions.checkState(eventType == MetastoreEventType.CREATE_TABLE);
        Preconditions.checkState(!createProcessed_);
        createSkipped_ = skipped;
        createProcessed_ = true;
      }
    }

    /**
     * Determines whether both pseudo drop and create table events are skipped. This
     * method return true only when both pseudo drop and create table events are
     * processed and skipped. Otherwise, false.
     * @return True or False
     */
    private synchronized boolean isSkipped() {
      return dropSkipped_ && createSkipped_;
    }

    /**
     * Determines whether both pseudo drop and create table events are processed.
     * @return True or False
     */
    private synchronized boolean isProcessed() {
      return dropProcessed_ && createProcessed_;
    }
  }

  RenameTableBarrierEvent(AlterTableEvent actualEvent,
      MetastoreTableEvent pseudoEvent, RenameEventState state) {
    super(pseudoEvent.catalogOpExecutor_, pseudoEvent.metrics_, pseudoEvent.event_);
    Preconditions.checkArgument(actualEvent != null);
    Preconditions.checkArgument(state != null);
    Preconditions.checkArgument(
        pseudoEvent.getEventType() == MetastoreEventType.DROP_TABLE ||
        pseudoEvent.getEventType() == MetastoreEventType.CREATE_TABLE);
    actualEvent_ = actualEvent;
    pseudoEvent_ = pseudoEvent;
    this.state_ = state;
  }

  /**
   * Determines whether this event can be processed. TableProcessor uses this method to
   * check if the event can be processed before processing it.
   * @return True if the event can be processed. False otherwise
   */
  boolean canProcess() {
    if (getEventType() == MetastoreEventType.DROP_TABLE) {
      return !state_.isDropProcessed();
    }
    return state_.isDropProcessed() && !state_.isCreateProcessed();
  }

  private void updateStatus(boolean skip) {
    state_.setProcessed(getEventType(), skip);
    if (state_.isSkipped()) {
      // Update the skipped metrics if both events are skipped
      metrics_.getCounter(MetastoreEventsProcessor.EVENTS_SKIPPED_METRIC).inc();
      actualEvent_.debugLog("Incremented skipped metric to {}",
          metrics_.getCounter(MetastoreEventsProcessor.EVENTS_SKIPPED_METRIC).getCount());
    }
    if (state_.isProcessed()) {
      catalog_.getMetastoreEventProcessor().getDeleteEventLog().removeEvent(getEventId());
    }
  }

  @VisibleForTesting
  RenameEventState getState() {
    return state_;
  }

  @Override
  public void processIfEnabled() throws CatalogException {
    Preconditions.checkState(canProcess());
    if (actualEvent_.isEventProcessingDisabled()) {
      updateStatus(true);
      return;
    }
    if (!state_.isDropProcessed()) {
      // Execute debug action before processing first pseudo-event
      DebugUtils.executeDebugAction(BackendConfig.INSTANCE.debugActions(),
          DebugUtils.EVENT_PROCESSING_DELAY);
    }
    process();
    if (state_.isProcessed()) {
      actualEvent_.injectErrorIfNeeded();
    }
  }

  @Override
  protected void process() throws CatalogException {
    Timer.Context context = null;
    Db db = catalog_.getDb(getDbName());
    Table table = null;
    if (db != null) {
      table = db.getTable(getTableName());
    }
    if (table != null) {
      context = table.getMetrics().getTimer(TBL_EVENTS_PROCESS_DURATION).time();
    }
    String fqTableName = getFullyQualifiedTblName();
    boolean skipped = true;
    try {
      if (getEventType() == MetastoreEventType.DROP_TABLE) {
        if (table != null && catalogOpExecutor_.removeTableIfNotAddedLater(getEventId(),
            getDbName(), getTableName(), new Reference<>())) {
          skipped = false;
          actualEvent_.infoLog("Successfully removed table {}", fqTableName);
          metrics_.getCounter(MetastoreEventsProcessor.NUMBER_OF_TABLES_REMOVED).inc();
        }
      } else {
        Preconditions.checkState(getEventType() == MetastoreEventType.CREATE_TABLE);
        if (db != null && catalogOpExecutor_.addTableIfNotRemovedLater(getEventId(),
            pseudoEvent_.getTable())) {
          skipped = false;
          actualEvent_.infoLog("Successfully added table {}", fqTableName);
          metrics_.getCounter(MetastoreEventsProcessor.NUMBER_OF_TABLES_ADDED).inc();
        }
      }
    } finally {
      if (context != null) {
        context.stop();
      }
    }
    updateStatus(skipped);
  }

  @Override
  protected boolean onFailure(Exception e) {
    return false;
  }

  @Override
  protected boolean shouldSkipWhenSyncingToLatestEventId() {
    throw new UnsupportedOperationException(
        "Not supported for rename table barrier event");
  }

  @Override
  protected boolean isEventProcessingDisabled() {
    throw new UnsupportedOperationException(
        "Not supported for rename table barrier event");
  }

  @Override
  protected void processTableEvent() {
    throw new UnsupportedOperationException(
        "Not supported for rename table barrier event");
  }

  @Override
  protected SelfEventContext getSelfEventContext() {
    throw new UnsupportedOperationException(
        "Not supported for rename table barrier event");
  }

  @Override
  public String getEventDesc() {
    return actualEvent_.getEventDesc() + " pseudo-event " + getEventType();
  }
}
