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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import javax.annotation.Nullable;

import org.apache.hadoop.hive.metastore.api.NotificationEvent;
import org.apache.impala.analysis.TableName;
import org.apache.impala.catalog.TableWriteId;
import org.apache.impala.catalog.events.MetastoreEvents.AbortTxnEvent;
import org.apache.impala.catalog.events.MetastoreEvents.AlterTableEvent;
import org.apache.impala.catalog.events.MetastoreEvents.CreateTableEvent;
import org.apache.impala.catalog.events.MetastoreEvents.DropTableEvent;
import org.apache.impala.catalog.events.MetastoreEvents.IgnoredEvent;
import org.apache.impala.catalog.events.MetastoreEvents.MetastoreEvent;
import org.apache.impala.catalog.events.MetastoreEvents.MetastoreEventType;
import org.apache.impala.catalog.events.MetastoreEvents.MetastoreTableEvent;
import org.apache.impala.catalog.events.MetastoreEvents.PseudoAbortTxnEvent;
import org.apache.impala.compat.MetastoreShim;

/**
 * This class provides the necessary methods to init, start, clear, shutdown the
 * executor service and a method to dispatch the metastore events for processing in
 * hierarchical mode. After instantiation, it needs to be started by invoking
 * {@link #start()} so that metastore events can be processed when
 * {@link #dispatch(MetastoreEvent event)} is invoked thereafter. Once
 * {@link #shutdown(boolean graceful)} is invoked, cannot be started or dispatch events.
 */
public class EventExecutorService {
  // DbEventExecutor and TableEventExecutor threads task schedule interval
  private static final int EXECUTOR_SCHEDULE_INTERVAL_MS = 10;

  // Possible status of event executor service
  public enum EventExecutorStatus {
    INACTIVE,
    ACTIVE,
    STOPPED
  }

  // Event executor status
  private EventExecutorStatus status_ = EventExecutorStatus.INACTIVE;

  private final MetastoreEventsProcessor eventProcessor_;

  // List of DbEventExecutor
  private final List<DbEventExecutor> dbEventExecutors_;

  /**
   * Database name to DbEventExecutor map. This map is mutated only by
   * DbEventExecutor through
   * {@link DbEventExecutor#assignEventExecutor(String, DbEventExecutor)} and
   * {@link DbEventExecutor#unAssignEventExecutor(String)}
   */
  private final Map<String, DbEventExecutor> dbNameToEventExecutor_ =
      new ConcurrentHashMap<>();

  EventExecutorService(MetastoreEventsProcessor eventProcessor,
      int numDbEventExecutor, int numTableEventExecutor) {
    Preconditions.checkArgument(eventProcessor != null);
    Preconditions.checkArgument(numDbEventExecutor > 0);
    eventProcessor_ = eventProcessor;
    // Initialize the EventExecutorService
    dbEventExecutors_ = new ArrayList<>(numDbEventExecutor);
    for (int i = 0; i < numDbEventExecutor; i++) {
      dbEventExecutors_.add(new DbEventExecutor(eventProcessor, String.valueOf(i),
          EXECUTOR_SCHEDULE_INTERVAL_MS, numTableEventExecutor, dbNameToEventExecutor_));
    }
  }

  /**
   * This method is only for testing.
   * @return List of DbEventExecutors it holds.
   */
  @VisibleForTesting
  List<DbEventExecutor> getDbEventExecutors() {
    return dbEventExecutors_;
  }

  /**
   * This method is only for testing and must not be used anywhere else.
   * @param status EventExecutorStatus
   */
  @VisibleForTesting
  public void setStatus(EventExecutorStatus status) {
    Preconditions.checkState(status_ != EventExecutorStatus.STOPPED);
    if (status == EventExecutorStatus.ACTIVE) {
      status_ = EventExecutorStatus.ACTIVE;
    } else if (status == EventExecutorStatus.STOPPED) {
      shutdown(true);
    }
  }

  /**
   * Start the EventExecutorService. It has to be started to make it ready to process the
   * events. It is invoked from {@link ExternalEventsProcessor#start()}.
   * Once shutdown, it cannot be started.
   */
  synchronized void start() {
    Preconditions.checkState(status_ != EventExecutorStatus.STOPPED);
    if (status_ == EventExecutorStatus.INACTIVE) {
      dbEventExecutors_.forEach(DbEventExecutor::start);
      status_ = EventExecutorStatus.ACTIVE;
    }
  }

  /**
   * Clear the EventExecutorService to discard pending events, remove DbProcessors and
   * TableProcessors if any for all the DbEventExecutor instances. It is invoked from
   * {@link ExternalEventsProcessor#clear()} only upon catalog reset.
   */
  synchronized void clear() {
    dbEventExecutors_.parallelStream().forEach(DbEventExecutor::clear);
  }

  /**
   * Shutdown the EventExecutorService. It is invoked from
   * {@link ExternalEventsProcessor#shutdown()} when event processor is being shutdown.
   * Once shutdown, it cannot be started again.
   */
  synchronized void shutdown(boolean graceful) {
    Preconditions.checkState(status_ != EventExecutorStatus.STOPPED);
    status_ = EventExecutorStatus.STOPPED;
    if (graceful) {
      eventProcessor_.ensureEventsProcessedInHierarchicalMode(3600000);
    }
    dbEventExecutors_.parallelStream().forEach(DbEventExecutor::stop);
  }

  /**
   * Cleanup the EventExecutorService to remove idle DbProcessors if any for all the
   * DbEventExecutor instances. It is invoked periodically from the
   * {@link ExternalEventsProcessor#processEvents()} thread upon each schedule.
   */
  synchronized void cleanup() {
    dbEventExecutors_.parallelStream().forEach(DbEventExecutor::cleanup);
  }

  /**
   * Gets the aggregated outstanding event count of all the DbEventExecutor instances.
   * @return Outstanding event count
   */
  long getOutstandingEventCount() {
    return dbEventExecutors_.stream().mapToLong(DbEventExecutor::getOutstandingEventCount)
        .sum();
  }

  /**
   * Dispatches the event to appropriate dbEventExecutor for processing. It is invoked
   * from the {@link ExternalEventsProcessor#processEvents()} thread.
   * <p>
   * For the events involving multiple tables(commit and abort transaction),
   * pseudo-events are created for each table so that processing happens independently for
   * each table.
   * <p>
   * For alter table rename event, 2 pseudo-events are created. One to drop existing
   * table and other to create new table. These pseudo-events are wrapped in barrier
   * events to synchronize the processing of the pseudo-events such that drop table is
   * processed before adding a new table.
   * <p>
   * Events that do not have any processing at catalogd(i.e., IgnoredEvent) are simply
   * ignored without dispatching them to dbEventExecutor.
   * <p>
   * Note:
   * <ul>
   * <li> This method can be invoked only when EventExecutorService is active.</li>
   * <li> Even if, only subset of pseudo-event have completed processing at respective
   * DbEventExecutor and TableEventExecutor threads when {@link #clear()} is performed,
   * catalog reset brings the cache to consistent state. So it is not required to wait
   * for all the pseudo-events to process before clear operation. Clear is only performed
   * upon catalog reset.</li>
   * </ul>
   * @param event Metastore event
   * @throws MetastoreNotificationException
   */
  synchronized void dispatch(MetastoreEvent event) throws MetastoreNotificationException {
    Preconditions.checkState(status_ == EventExecutorStatus.ACTIVE,
        "EventExecutorService must be active");
    Preconditions.checkNotNull(event);
    if (event instanceof MetastoreShim.CommitTxnEvent) {
      processCommitTxnEvent((MetastoreShim.CommitTxnEvent) event);
    } else if (event instanceof AbortTxnEvent) {
      processAbortTxnEvent((AbortTxnEvent) event);
    } else if (event instanceof AlterTableEvent && ((AlterTableEvent) event).isRename()) {
      processAlterTableRenameEvent((AlterTableEvent) event);
    } else if (event instanceof IgnoredEvent) {
      event.debugLog("Ignoring event type {}", event.getEvent().getEventType());
    } else {
      DbEventExecutor dbEventExecutor = getOrFindDbEventExecutor(event.getDbName());
      dbEventExecutor.enqueue(event);
    }
  }

  /**
   * Dispatches pseudo commit transaction events to the tables involved in transaction for
   * processing.
   * @param commitTxnEvent Commit transaction event
   * @throws MetastoreNotificationException
   */
  private void processCommitTxnEvent(MetastoreShim.CommitTxnEvent commitTxnEvent)
      throws MetastoreNotificationException {
    List<MetastoreShim.PseudoCommitTxnEvent> pseudoCommitTxnEvents =
        MetastoreShim.getPseudoCommitTxnEvents(commitTxnEvent);
    for (MetastoreShim.PseudoCommitTxnEvent pseudoCommitTxnEvent :
        pseudoCommitTxnEvents) {
      DbEventExecutor dbEventExecutor = getOrFindDbEventExecutor(
          pseudoCommitTxnEvent.getDbName());
      dbEventExecutor.enqueue(pseudoCommitTxnEvent);
    }
  }

  /**
   * Dispatches pseudo abort transaction events to the tables involved in transaction for
   * processing.
   * @param abortTxnEvent Abort transaction event
   */
  private void processAbortTxnEvent(AbortTxnEvent abortTxnEvent) {
    Set<TableWriteId> tableWriteIds = abortTxnEvent.getTableWriteIds();
    Map<TableName, List<Long>> tableWriteIdsMap = new HashMap<>();
    for (TableWriteId tableWriteId : tableWriteIds) {
      tableWriteIdsMap.computeIfAbsent(
          new TableName(tableWriteId.getDbName(), tableWriteId.getTblName()),
          k -> new ArrayList<>()).add(tableWriteId.getWriteId());
    }
    for (Map.Entry<TableName, List<Long>> entry : tableWriteIdsMap.entrySet()) {
      DbEventExecutor dbEventExecutor = getOrFindDbEventExecutor(entry.getKey().getDb());
      dbEventExecutor.enqueue(
          new PseudoAbortTxnEvent(abortTxnEvent, entry.getKey().getDb(),
              entry.getKey().getTbl(), entry.getValue()));
    }
  }

  /**
   * Gets the list of rename table barrier events for the given alter table rename event.
   * A pseudo-event for current table is to drop existing table and a pseudo-event for
   * the renamed table is to create a new table. These pseudo-events are wrapped in
   * barrier events.
   * @param alterEvent Alter table rename event
   * @return List of two RenameTableBarrierEvent
   */
  @VisibleForTesting
  List<RenameTableBarrierEvent> getRenameTableBarrierEvents(AlterTableEvent alterEvent) {
    List<RenameTableBarrierEvent> barrierEvents = new ArrayList<>();

    NotificationEvent pseudoDropTableEvent = new NotificationEvent();
    pseudoDropTableEvent.setEventType(MetastoreEventType.DROP_TABLE.toString());
    pseudoDropTableEvent.setTableName(alterEvent.getBeforeTable().getTableName());
    pseudoDropTableEvent.setDbName(alterEvent.getBeforeTable().getDbName());
    pseudoDropTableEvent.setCatName(alterEvent.getCatalogName());
    pseudoDropTableEvent.setEventId(alterEvent.getEventId());
    MetastoreTableEvent dropTableEvent = new DropTableEvent(
        alterEvent.getCatalogOpExecutor(), alterEvent.getMetrics(), pseudoDropTableEvent,
        alterEvent.getBeforeTable());

    NotificationEvent pseudoCreateTableEvent = new NotificationEvent();
    pseudoCreateTableEvent.setEventType(MetastoreEventType.CREATE_TABLE.toString());
    pseudoCreateTableEvent.setTableName(alterEvent.getAfterTable().getTableName());
    pseudoCreateTableEvent.setDbName(alterEvent.getAfterTable().getDbName());
    pseudoCreateTableEvent.setCatName(alterEvent.getCatalogName());
    pseudoCreateTableEvent.setEventId(alterEvent.getEventId());
    MetastoreTableEvent createTableEvent = new CreateTableEvent(
        alterEvent.getCatalogOpExecutor(), alterEvent.getMetrics(),
        pseudoCreateTableEvent, alterEvent.getAfterTable());

    RenameTableBarrierEvent.RenameEventState state =
        new RenameTableBarrierEvent.RenameEventState();
    barrierEvents.add(new RenameTableBarrierEvent(alterEvent, dropTableEvent, state));
    barrierEvents.add(new RenameTableBarrierEvent(alterEvent, createTableEvent, state));
    return barrierEvents;
  }

  /**
   * Dispatches pseudo table events to current and the renamed tables for processing.
   * Event for current table is to drop existing table and event for the renamed table
   * is to create a new table. These pseudo-events are wrapped in barrier events to
   * synchronize the processing of the pseudo-events such that drop table is processed
   * before adding a new table.
   * @param alterEvent Alter table rename event
   */
  private void processAlterTableRenameEvent(AlterTableEvent alterEvent) {
    for (RenameTableBarrierEvent event : getRenameTableBarrierEvents(alterEvent)) {
      DbEventExecutor dbEventExecutor = getOrFindDbEventExecutor(event.getDbName());
      dbEventExecutor.enqueue(event);
    }
  }

  /**
   * Gets the DbEventExecutor for the given database.
   * @param dbName Database name
   * @return DbEventExecutor if mapping exists otherwise null
   */
  @Nullable DbEventExecutor getDbEventExecutor(String dbName) {
    Preconditions.checkNotNull(dbName);
    return dbNameToEventExecutor_.get(dbName);
  }

  /**
   * Get or find a suitable DbEventExecutor for the given database to queue the metastore
   * event for processing. If the database to executor mapping already exists, returns it.
   * Otherwise, looks for a DbEventExecutor that is serving lesser number of
   * DbProcessors. If the number of DbProcessors in potential DbEventExecutors are same,
   * then looks for executor that has less outstanding events to tiebreak.
   * count.
   * @param dbName Database name
   * @return Instance of DbEventExecutor
   */
  private DbEventExecutor getOrFindDbEventExecutor(String dbName) {
    Preconditions.checkNotNull(dbName);
    DbEventExecutor eventExecutor = getDbEventExecutor(dbName.toLowerCase());
    if (eventExecutor == null) {
      long minOutStandingEvents = Long.MAX_VALUE;
      long minDbCount = Long.MAX_VALUE;
      for (DbEventExecutor dee : dbEventExecutors_) {
        long dbCount = dee.getDbCount();
        if (dbCount < minDbCount) {
          minDbCount = dbCount;
          minOutStandingEvents = dee.getOutstandingEventCount();
          eventExecutor = dee;
        } else if (dbCount == minDbCount) {
          long outstandingEventCount = dee.getOutstandingEventCount();
          if (outstandingEventCount < minOutStandingEvents) {
            minOutStandingEvents = outstandingEventCount;
            eventExecutor = dee;
          }
        }
      }
    }
    return eventExecutor;
  }
}
