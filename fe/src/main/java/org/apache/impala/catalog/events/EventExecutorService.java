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
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import javax.annotation.Nullable;

import org.apache.hadoop.hive.metastore.api.NotificationEvent;
import org.apache.impala.analysis.TableName;
import org.apache.impala.catalog.TableWriteId;
import org.apache.impala.catalog.events.MetastoreEvents.AbortTxnEvent;
import org.apache.impala.catalog.events.MetastoreEvents.AlterTableEvent;
import org.apache.impala.catalog.events.MetastoreEvents.CreateTableEvent;
import org.apache.impala.catalog.events.MetastoreEvents.DerivedMetastoreEventContext;
import org.apache.impala.catalog.events.MetastoreEvents.DropTableEvent;
import org.apache.impala.catalog.events.MetastoreEvents.IgnoredEvent;
import org.apache.impala.catalog.events.MetastoreEvents.MetastoreEvent;
import org.apache.impala.catalog.events.MetastoreEvents.MetastoreEventType;
import org.apache.impala.catalog.events.MetastoreEvents.MetastoreTableEvent;
import org.apache.impala.catalog.events.MetastoreEvents.PseudoAbortTxnEvent;
import org.apache.impala.common.Pair;
import org.apache.impala.common.PrintUtils;
import org.apache.impala.compat.MetastoreShim;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class provides the necessary methods to init, start, clear, shutdown the
 * executor service and a method to dispatch the metastore events for processing in
 * hierarchical mode. After instantiation, it needs to be started by invoking
 * {@link #start()} so that metastore events can be processed when
 * {@link #dispatch(MetastoreEvent event)} is invoked thereafter. Once
 * {@link #shutdown(boolean graceful)} is invoked, cannot be started or dispatch events.
 */
public class EventExecutorService {
  private static final Logger LOG = LoggerFactory.getLogger(EventExecutorService.class);

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

  // In-progress event log. Maintains the metastore events waiting to be processed on
  // executors. It is a map of metastore event id to the metastore event. Metastore event
  // can be a delimiter or non-delimiter event. Delimiter is a kind of metastore event
  // that do not require event processing. Delimeter event can be:
  // 1. A CommitTxnEvent that do not have any write event info for a given transaction.
  // 2. An AbortTxnEvent that do not have write ids for a given transaction.
  // 3. An IgnoredEvent.
  // They are not queued to DbEventExecutor for processing. They are just maintained in
  // the inProgressLog_ to preserve continuity and correctness in synchronization
  // tracking. The delimiter events are removed from this map when their preceding
  //non-delimiter metastore event is removed.
  private final TreeMap<Long, MetastoreEvent> inProgressLog_ = new TreeMap<>();

  // Processed event log. Maintains event id to HMS event notification time mapping.
  private TreeMap<Long, Long> processedLog_ = new TreeMap<>();

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
   * This method is to get DbEventExecutor list. It is only for testing.
   * @return List of DbEventExecutors it holds.
   */
  @VisibleForTesting
  List<DbEventExecutor> getDbEventExecutors() {
    return dbEventExecutors_;
  }

  /**
   * This method is to set the event executor status without actually starting it. It is
   * only for testing and must not be used anywhere else.
   * @param status EventExecutorStatus
   */
  @VisibleForTesting
  void setStatus(EventExecutorStatus status) {
    Preconditions.checkState(status_ != EventExecutorStatus.STOPPED);
    if (status == EventExecutorStatus.ACTIVE) {
      makeActive();
    } else if (status == EventExecutorStatus.STOPPED) {
      shutdown(true);
    }
  }

  /**
   * This method is to get in-progress log. It is only for testing.
   * @return in-progress log tree map.
   */
  @VisibleForTesting
  TreeMap<Long, MetastoreEvent> getInProgressLog() {
    return inProgressLog_;
  }

  /**
   * This method is to get processed log. It is only for testing.
   * @return processed log tree map
   */
  @VisibleForTesting
  TreeMap<Long, Long> getProcessedLog() {
    return processedLog_;
  }

  /**
   * Method to make the EventExecutorService active. This method is invoked from
   * {@link #start()}.
   */
  private synchronized void makeActive() {
    clearLogs();
    // Event processor set its lastSyncedEventId to the event id from which event
    // processing must start. Event processor start is a synchronized method and this
    // method gets through that flow.
    addToProcessedLog(eventProcessor_.getLastSyncedEventId(),
        eventProcessor_.getLastSyncedEventTime());
    status_ = EventExecutorStatus.ACTIVE;
  }

  /**
   * Start the EventExecutorService. It has to be started to make it ready to process the
   * events. It is invoked from {@link ExternalEventsProcessor#start()} and
   * {@link ExternalEventsProcessor#start(long)}. Once shutdown, it cannot be started.
   */
  synchronized void start() {
    Preconditions.checkState(status_ != EventExecutorStatus.STOPPED);
    if (status_ == EventExecutorStatus.INACTIVE) {
      dbEventExecutors_.forEach(DbEventExecutor::start);
    }
    makeActive();
  }

  /**
   * Clear the EventExecutorService to discard pending events, remove DbProcessors and
   * TableProcessors if any for all the DbEventExecutor instances. It is invoked from
   * {@link ExternalEventsProcessor#clear()} only upon catalog reset.
   */
  synchronized void clear() {
    dbEventExecutors_.parallelStream().forEach(DbEventExecutor::clear);
    clearLogs();
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
    clearLogs();
  }

  /**
   * Method to clear in-progress and processed event logs.
   */
  private synchronized void clearLogs() {
    inProgressLog_.clear();
    processedLog_.clear();
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
   * Returns the number of outstanding events currently queued for processing.
   * @return Outstanding event count
   */
  synchronized long getOutstandingEventCount() {
    return inProgressLog_.size();
  }

  /**
   * Calculates the number of events that must be synchronized in order to ensure that
   * the greatest synced event id reaches the specified {@code eventId}.
   * @param eventId Target event id to which the greatest synced event id should advance
   * @return Estimated number of events that need to be synchronized
   */
  synchronized long getPendingEventCount(long eventId) {
    Preconditions.checkState(status_ == EventExecutorStatus.ACTIVE,
        "EventExecutorService must be active");
    long greatestSyncedEventId = processedLog_.firstKey();
    if (eventId <= greatestSyncedEventId) return 0;
    if (inProgressLog_.isEmpty()) return eventId - greatestSyncedEventId;
    long lastUnprocessedEventId = inProgressLog_.lastKey();
    if (eventId <= lastUnprocessedEventId) {
      return inProgressLog_.headMap(eventId, true).size();
    }
    long closerEventId = processedLog_.floorKey(eventId);
    if (closerEventId == greatestSyncedEventId) {
      return eventId - lastUnprocessedEventId + inProgressLog_.size();
    }
    return eventId - closerEventId + inProgressLog_.headMap(closerEventId).size();
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
    boolean isEventDispatched = false;
    if (event instanceof MetastoreShim.CommitTxnEvent) {
      isEventDispatched = processCommitTxnEvent((MetastoreShim.CommitTxnEvent) event);
    } else if (event instanceof AbortTxnEvent) {
      isEventDispatched = processAbortTxnEvent((AbortTxnEvent) event);
    } else if (event instanceof AlterTableEvent && ((AlterTableEvent) event).isRename()) {
      processAlterTableRenameEvent((AlterTableEvent) event);
      isEventDispatched = true;
    } else if (event instanceof IgnoredEvent) {
      event.debugLog("Ignoring event type {}", event.getEvent().getEventType());
    } else {
      DbEventExecutor dbEventExecutor = getOrFindDbEventExecutor(event.getDbName());
      dbEventExecutor.enqueue(event);
      isEventDispatched = true;
    }
    addToInProgressLog(event, !isEventDispatched);
  }

  /**
   * Dispatches pseudo commit transaction events to the tables involved in transaction for
   * processing.
   * @param commitTxnEvent Commit transaction event
   * @return True if any pseudo-events are dispatched. False otherwise.
   * @throws MetastoreNotificationException
   */
  private boolean processCommitTxnEvent(MetastoreShim.CommitTxnEvent commitTxnEvent)
      throws MetastoreNotificationException {
    List<MetastoreShim.PseudoCommitTxnEvent> pseudoCommitTxnEvents =
        MetastoreShim.getPseudoCommitTxnEvents(commitTxnEvent);
    for (MetastoreShim.PseudoCommitTxnEvent pseudoCommitTxnEvent :
        pseudoCommitTxnEvents) {
      DbEventExecutor dbEventExecutor = getOrFindDbEventExecutor(
          pseudoCommitTxnEvent.getDbName());
      dbEventExecutor.enqueue(pseudoCommitTxnEvent);
    }
    return !pseudoCommitTxnEvents.isEmpty();
  }

  /**
   * Dispatches pseudo abort transaction events to the tables involved in transaction for
   * processing.
   * @param abortTxnEvent Abort transaction event
   * @return True if any pseudo-events are dispatched. False otherwise.
   */
  private boolean processAbortTxnEvent(AbortTxnEvent abortTxnEvent) {
    Set<TableWriteId> tableWriteIds = abortTxnEvent.getTableWriteIds();
    Map<TableName, List<Long>> tableWriteIdsMap = new HashMap<>();
    for (TableWriteId tableWriteId : tableWriteIds) {
      tableWriteIdsMap.computeIfAbsent(
          new TableName(tableWriteId.getDbName(), tableWriteId.getTblName()),
          k -> new ArrayList<>()).add(tableWriteId.getWriteId());
    }
    DerivedMetastoreEventContext context = new DerivedMetastoreEventContext(abortTxnEvent,
        tableWriteIdsMap.size());
    for (Map.Entry<TableName, List<Long>> entry : tableWriteIdsMap.entrySet()) {
      DbEventExecutor dbEventExecutor = getOrFindDbEventExecutor(entry.getKey().getDb());
      dbEventExecutor.enqueue(new PseudoAbortTxnEvent(context, entry.getKey().getDb(),
          entry.getKey().getTbl(), entry.getValue()));
    }
    return !tableWriteIdsMap.isEmpty();
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
    DerivedMetastoreEventContext context = new DerivedMetastoreEventContext(alterEvent,
        2);
    barrierEvents.add(new RenameTableBarrierEvent(context, dropTableEvent, state));
    barrierEvents.add(new RenameTableBarrierEvent(context, createTableEvent, state));
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

  /**
   * Records the given event as processed in processed log.
   * @param eventId Metastore event id
   * @param eventTime Metastore event time
   */
  synchronized void addToProcessedLog(long eventId, long eventTime) {
    processedLog_.put(eventId, eventTime);
    if (processedLog_.size() > 1) pruneProcessedLog();
  }

  /**
   * Adds the given event to in-progress log.
   * <p>
   * Delimiter events are special markers used to preserve continuity and correctness
   * in synchronization tracking. They are not queued to DbEventExecutor, and are
   * automatically removed when their preceding non-delimiter event is removed.
   * @param event       Metastore event to be tracked
   * @param isDelimiter True if the event is a delimiter event. False otherwise
   */
  private synchronized void addToInProgressLog(MetastoreEvent event,
      boolean isDelimiter) {
    event.setDelimiter(isDelimiter);
    long currentTime = System.currentTimeMillis();
    // Log time taken to dispatch event. It is the time since the metastore event object
    // is created
    event.debugLog("Dispatch time: {}",
        PrintUtils.printTimeMs(currentTime - event.getCreationTime()));
    if (inProgressLog_.isEmpty() && isDelimiter) {
      // Event is considered implicitly processed and is directly recorded in the
      // processed log
      addToProcessedLog(event.getEventId(), event.getEventTime());
      return;
    }
    // Initialize the event dispatch time. It is used to calculate the time taken to
    // process event, when event is removed from inProgressLog_
    event.setDispatchTime(currentTime);
    inProgressLog_.put(event.getEventId(), event);
  }

  /**
   * Removes the specified event from the in-progress log and records it as processed.
   * <p>
   * This method also removes and marks any subsequent delimiter events as processed
   * until a non-delimiter event is encountered. This ensures the in-progress log
   * accurately reflects only unprocessed events.
   * <p>
   * After recording processed events, {@link #pruneProcessedLog()} method is called to
   * discard obsolete entries from the processed log.
   * @param eventId Id of the event to remove and mark as processed
   */
  synchronized void removeFromInProgressLog(long eventId) {
    long currentTime = System.currentTimeMillis();
    MetastoreEvent event = inProgressLog_.remove(eventId);
    // event may be null when catalog is being reset. Metastore event processing and
    // catalog reset happens in different threads. In that case, clearLogs() would
    // have already cleared both inProgressLog_ and processedLog_.
    if (event == null) return;
    // Log time taken to process event. It is the time since the metastore event is
    // dispatched for processing
    event.debugLog("Complete process time: {}",
        PrintUtils.printTimeMs(currentTime - event.getDispatchTime()));
    processedLog_.put(eventId, event.getEventTime());
    Preconditions.checkState(!event.isDelimiter());
    // Remove all the subsequent delimiter events until a non-delimiter event is
    // encountered
    Iterator<Map.Entry<Long, MetastoreEvent>> it =
        inProgressLog_.tailMap(eventId).entrySet().iterator();
    while (it.hasNext()) {
      Map.Entry<Long, MetastoreEvent> entry = it.next();
      event = entry.getValue();
      if (!event.isDelimiter()) break;
      processedLog_.put(entry.getKey(), event.getEventTime());
      it.remove();
    }
    LOG.debug("Current count of dispatched events that are being tracked: {} ",
        inProgressLog_.size());
    pruneProcessedLog();
  }

  /**
   * Prunes the processed event log by removing entries that are no longer needed for
   * synchronization tracking.
   * <p>
   * This method determines the greatest event id up to which all previous events are
   * guaranteed to have been synced and discards entries older than that event. The first
   * entry in the processed log is always considered the greatest synced event id.
   */
  private synchronized void pruneProcessedLog() {
    long newGreatestSyncedEventId = processedLog_.lastKey();
    if (!inProgressLog_.isEmpty()) {
      Long firstUnprocessedEventId = inProgressLog_.firstKey();
      // First unprocessed event id is always greater than greatest synced event id
      Preconditions.checkState(firstUnprocessedEventId > processedLog_.firstKey());
      // Adjust the greatest synced event id to highest processed event id just before
      // first unprocessed event id.
      newGreatestSyncedEventId = processedLog_.lowerKey(firstUnprocessedEventId);
    }
    // Retain only the entries from the updated greatest synced event id onward
    processedLog_ = new TreeMap<>(processedLog_.tailMap(newGreatestSyncedEventId));
    LOG.debug("Current count of processed events that are tracked: {}, " +
        "greatest synced event id: {}", processedLog_.size(), newGreatestSyncedEventId);
  }

  /**
   * Gets the greatest synced event id. Greatest synced event is the latest event such
   * that all events with id less than or equal to the latest event are definitely synced.
   * @return Event id of the greatest synced event, or -1 if unknown.
   */
  synchronized long getGreatestSyncedEventId() {
    long greatestSyncedEventId = -1;
    Map.Entry<Long, Long> entry = processedLog_.firstEntry();
    if (entry != null) greatestSyncedEventId = entry.getKey();
    return greatestSyncedEventId;
  }

  /**
   * Gets the greatest synced event time. Greatest synced event is the latest event such
   * that all events with id less than or equal to the latest event are definitely synced.
   * @return Time of the greatest synced event, or -1 if unknown.
   */
  synchronized long getGreatestSyncedEventTime() {
    long greatestSyncedEventTime = 0;
    Map.Entry<Long, Long> entry = processedLog_.firstEntry();
    if (entry != null) greatestSyncedEventTime = entry.getValue();
    return greatestSyncedEventTime;
  }
}
