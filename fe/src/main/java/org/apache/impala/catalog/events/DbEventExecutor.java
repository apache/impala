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

import com.codahale.metrics.Counter;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.util.concurrent.ThreadFactoryBuilder;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.impala.catalog.events.MetastoreEvents.DropTableEvent;
import org.apache.impala.catalog.events.MetastoreEvents.MetastoreDatabaseEvent;
import org.apache.impala.catalog.events.MetastoreEvents.MetastoreEvent;
import org.apache.impala.catalog.events.MetastoreEvents.MetastoreEventType;
import org.apache.impala.catalog.events.MetastoreEventsProcessor.EventProcessorStatus;
import org.apache.impala.catalog.events.TableEventExecutor.TableProcessor;
import org.apache.impala.service.BackendConfig;
import org.apache.impala.util.ClassUtil;
import org.apache.impala.util.ThreadNameAnnotator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * An instance of this class has an execution thread, manage events of multiple
 * databases(i.e., with {@link DbProcessor}). An instance of DbProcessor is maintained
 * for each database within the DbEventExecutor. On each scheduled execution,
 * {@link DbProcessor#process()} is invoked for each DbProcessor it manages.
 * Once a DbEventExecutor is assigned to a database, all the subsequent events of the
 * database and its tables are dispatched to the same DbEventExecutor.
 * <p>
 * DbEventExecutor has a fixed list of {@link TableEventExecutor}. An instance of
 * TableEventExecutor has an execution thread. They are assigned to the tables of
 * databases managed by the instance of DbEventExecutor. Henceforth, all its subsequent
 * table events are dispatched to the same TableEventExecutor. DbEventExecutor is
 * responsible for assigning and un-assigning DbEventExecutors and TableEventExecutors
 * to dbs and tables respectively, dispatching events to appropriate DbProcessor
 * and TableProcessor, processing database events in DbProcessors.
 *
 * <p> This class is instantiated by {@link EventExecutorService}, which invokes the
 * following methods of this class in a synchronized manner:
 * {@link DbEventExecutor#start()}, {@link DbEventExecutor#stop()},
 * {@link DbEventExecutor#cleanup()}, {@link DbEventExecutor#clear()} and
 * {@link DbEventExecutor#enqueue(MetastoreEvent)}
 *
 * @see org.apache.impala.catalog.events.DbEventExecutor.DbProcessor
 * @see org.apache.impala.catalog.events.TableEventExecutor
 * @see org.apache.impala.catalog.events.TableEventExecutor.TableProcessor
 */
public class DbEventExecutor {
  private static final Logger LOG = LoggerFactory.getLogger(DbEventExecutor.class);

  // Database name to DbEventExecutor assignment
  private final Map<String, DbEventExecutor> dbNameToEventExecutor_;

  // Executor name, like "DbEventExecutor-0"
  private final String name_;
  private final MetastoreEventsProcessor eventProcessor_;

  // Task schedule interval
  private final long interval_;
  private final ScheduledExecutorService service_;

  // Database name to DbProcessor map. DbProcessors managed by the DbEventExecutor
  private final Map<String, DbProcessor> dbProcessors_ = new ConcurrentHashMap<>();

  // List of TableEventExecutor that can be assigned to the tables of databases
  // managed by the DbEventExecutor
  private final List<TableEventExecutor> tableEventExecutors_;

  // Fully qualified table name to TableEventExecutor assignment
  private final Map<String, TableEventExecutor> tableToEventExecutor_ =
      new ConcurrentHashMap<>();

  // Outstanding events to process count
  private AtomicLong outstandingEventCount_ = new AtomicLong();

  /**
   * A DbProcessor contains information about database. It maintains input event
   * queue(to receive db events and table events from the dispatcher). Upon
   * {@link DbProcessor#process()} invocation by DbEventExecutor thread, events are
   * segregated from the input event queue as database events and table events. Table
   * events are dispatched to the appropriate {@link TableProcessor} for processing.
   * Database events are wrapped in {@link DbBarrierEvent}, enqueued to barrier events
   * queue for processing and are also dispatched to all the available TableProcessors
   * of the database(i.e., in the DbProcessor). Once the TableProcessors reach the
   * DbBarrierEvent and indicate that to DbProcessor, event is processed and marked
   * as processed.
   *
   * @see org.apache.impala.catalog.events.DbBarrierEvent
   * @see org.apache.impala.catalog.events.TableEventExecutor.TableProcessor
   */
  public static class DbProcessor {
    // DB name
    private final String dbName_;

    // last event queued time
    private long lastEventQueuedTime_;

    // Used to skip all events prior to this event id
    private final AtomicLong skipEventId_ = new AtomicLong(-1);
    private final DbEventExecutor dbEventExecutor_;

    // Input event queue to receive db events and table events from the dispatcher
    private final Queue<MetastoreEvent> inEvents_ = new ConcurrentLinkedQueue<>();
    private final Queue<DbBarrierEvent> barrierEvents_ = new ConcurrentLinkedQueue<>();

    // TableProcessors for the database
    private final Set<TableProcessor> tableProcessors_ = ConcurrentHashMap.newKeySet();

    /**
     * Indicates whether DbProcessor is terminating. Events are dispatched and processed
     * only when isTerminating_ is false.
     * <p>
     * Event dispatching and processing (i.e., {@link DbProcessor#process()}) and clearing
     * (i.e., {@link DbProcessor#clear()}) are invoked from  different threads.
     * {@link DbProcessor#clear()} acquires processorLock_ to immediately set
     * isTerminating_ to true, thereby preventing any further event dispatching or
     * processing.
     * <p>
     * {@link DbProcessor#process()} acquires processorLock_ in following cases:
     * <ul>
     * <li> To protect event dispatching in
     * {@link DbProcessor#dispatchDbEvent(MetastoreDatabaseEvent)} and
     * {@link DbProcessor#dispatchTableEvent(MetastoreEvent)}. Event can be dispatched to
     * the appropriate TableProcessors only if the DbProcessor is not terminating.</li>
     * <li> To check if DbProcessor is not terminating before processing an event in
     * {@link DbProcessor#processDbEvents()}, using {@link DbProcessor#isTerminating()}.
     * </li>
     * <li> To safely poll dispatched and processed events from inEvents_ and
     * barrierEvents_, and to decrement the outstanding event count in
     * {@link DbProcessor#process()} and {@link DbProcessor#processDbEvents()}
     * respectively, if the DbProcessor is not terminating.</li>
     * </ul>
     * <p>
     * Normally, a DbProcessor is removed after an idle timeout, determined by
     * {@link DbProcessor#canBeRemoved()}. However, when {@link DbEventExecutor#clear()}
     * or {@link DbEventExecutor#stop()} is invoked, all associated DbProcessors are
     * forcibly cleared and removed.
     * <p>
     * Lock contention is unlikely at the most frequent call sites (i.e., within
     * {@link DbProcessor#process()} and its related methods), as clear() is called
     * infrequently.
     * <p>
     * There is no lock contention between the {@link DbEventExecutor#stop()} and
     * {@link DbEventExecutor#process()} flows, since scheduling of process() is halted
     * by stop().
     */
    private final Object processorLock_ = new Object();
    private boolean isTerminating_ = false;

    private DbProcessor(DbEventExecutor executor, String dbName) {
      dbEventExecutor_ = executor;
      dbName_ = dbName;
    }

    /**
     * Queue event to the DbProcessor for processing.
     * @param event Metastore event
     */
    private void enqueue(MetastoreEvent event) {
      lastEventQueuedTime_ = System.currentTimeMillis();
      if (event.getEventType() == MetastoreEventType.DROP_DATABASE) {
        skipEventId_.set(event.getEventId());
      }
      inEvents_.offer(event);
      dbEventExecutor_.incrOutstandingEventCount();
      event.debugLog("Enqueued for db: {} on executor: {}", dbName_,
          dbEventExecutor_.name_);
    }

    /**
     * Determines whether the DbProcessor can be removed.
     * @return True if DbProcessor can be removed from the DbEventExecutor. False
     *         otherwise
     */
    private boolean canBeRemoved() {
      return inEvents_.isEmpty() && barrierEvents_.isEmpty() &&
          tableProcessors_.isEmpty() &&
          (System.currentTimeMillis() - lastEventQueuedTime_) >
              BackendConfig.INSTANCE.getMinEventProcessorIdleMs();
    }

    /**
     * Dispatch the event to appropriate TableProcessor for processing
     * @param event Metastore event
     */
    private void dispatchTableEvent(MetastoreEvent event) {
      String fqTableName = (event.getDbName() + '.' + event.getTableName()).toLowerCase();
      synchronized (processorLock_) {
        if (isTerminating()) return;
        TableEventExecutor tableEventExecutor =
            dbEventExecutor_.getOrAssignTableEventExecutor(fqTableName);
        TableProcessor tableProcessor =
            tableEventExecutor.getOrCreateTableProcessor(fqTableName);
        tableProcessors_.add(tableProcessor);
        if (tableProcessor.isEmpty()) {
          // Prepend all the outstanding db barrier events to TableProcessor so that they
          // do not process the table events received before these db barrier events are
          // processed
          barrierEvents_.forEach(tableProcessor::enqueue);
        }
        tableProcessor.enqueue(event);
      }
    }

    /**
     * Dispatch the database event to all its TableProcessors and queue it for local
     * processing as well.
     * @param event Metastore database event
     */
    private void dispatchDbEvent(MetastoreDatabaseEvent event) {
      DbBarrierEvent barrierEvent;
      synchronized (processorLock_) {
        if (isTerminating()) return;
        barrierEvent = new DbBarrierEvent(event);
        tableProcessors_.forEach(tableProcessor -> {
          if (!tableProcessor.isEmpty()) {
            tableProcessor.enqueue(barrierEvent);
          }
        });
        barrierEvents_.offer(barrierEvent);
        dbEventExecutor_.incrOutstandingEventCount();
      }
    }

    /**
     * Process the database events
     * @throws Exception
     */
    private void processDbEvents() throws Exception {
      DbBarrierEvent barrierEvent;
      String annotation = "Processing %s for db: " + dbName_;
      while ((barrierEvent = barrierEvents_.peek()) != null) {
        if (isTerminating()) return;
        if (barrierEvent.getExpectedProceedCount() != 0) {
          return;
        }
        try (ThreadNameAnnotator tna = new ThreadNameAnnotator(
            String.format(annotation, barrierEvent.getEventDesc()))) {
          barrierEvent.processIfEnabled();
          barrierEvent.infoLog("Processed for db: {}", dbName_);
        } catch (Exception processingEx) {
          try {
            if (!barrierEvent.onFailure(processingEx)) {
              throw processingEx;
            }
          } catch (Exception onFailureEx) {
            barrierEvent.errorLog("Failed to handle event processing failure for db: {}",
                dbName_, onFailureEx);
            throw processingEx;
          }
        }
        synchronized (processorLock_) {
          if (isTerminating()) return;
          Preconditions.checkState(barrierEvents_.poll() == barrierEvent);
          dbEventExecutor_.decrOutstandingEventCount(1);
        }
      }
    }

    /**
     * Clears the DbProcessor and all its TableProcessor forcefully.
     */
    private void clear() {
      // Set isTerminating_ to true before clearing the events from queues.
      // ConcurrentLinkedQueue.clear() is not atomic
      synchronized (processorLock_) {
        isTerminating_ = true;
      }
      // size() is O(n) operation for ConcurrentLinkedQueue. These queues are not updated
      // from other places if isTerminating_ becomes true.
      dbEventExecutor_.decrOutstandingEventCount(inEvents_.size());
      dbEventExecutor_.decrOutstandingEventCount(barrierEvents_.size());
      inEvents_.clear();
      barrierEvents_.clear();
      skipEventId_.set(-1);
      lastEventQueuedTime_ = 0;
      cleanIfNecessary(true);
      Preconditions.checkState(tableProcessors_.isEmpty());
    }

    /**
     * Removes all TableProcessor forcefully or just the idle TableProcessors if any.
     * <p>
     * Note: This method can be called from multiple threads. It is thread safe and
     * reentrant. Methods invoked from it are safe and reentrant too.
     * @param force Passed with true to indicate the removal of all the TableProcessors.
     *              And false to indicate the removal of idle TableProcessor if any.
     */
    private void cleanIfNecessary(boolean force) {
      Iterator<TableProcessor> it = tableProcessors_.iterator();
      while (it.hasNext()) {
        TableProcessor tableProcessor = it.next();
        if (force || tableProcessor.canBeRemoved()) {
          TableEventExecutor tableEventExecutor = tableProcessor.getTableEventExecutor();
          tableEventExecutor.deleteTableProcessor(tableProcessor.getTableName());
          dbEventExecutor_.unAssignTableEventExecutor(tableProcessor.getTableName());
          it.remove();
        }
      }
    }

    /**
     * Determines whether DbProcessor is terminating.
     * @return True if terminating. False otherwise
     */
    private boolean isTerminating() {
      synchronized (processorLock_) {
        if (isTerminating_ && LOG.isDebugEnabled()) {
          LOG.debug("Processor is terminating for db: {}. Caller stacktrace: {}", dbName_,
              ClassUtil.getStackTraceForThread());
        }
        return isTerminating_;
      }
    }

    /**
     * Skip the metastore event from processing if possible.
     * @param event Metastore event
     * @param dropDbEventId Drop database event id if drop database event is queued for
     *                      processing. Else -1.
     * @return True if event is skipped. Else false.
     */
    private boolean skipEventIfPossible(MetastoreEvent event, long dropDbEventId) {
      if (event.getEventId() >= dropDbEventId || event instanceof DropTableEvent ||
          event instanceof RenameTableBarrierEvent) {
        return false;
      }
      Counter eventSkipCounter = event.getMetrics()
          .getCounter(MetastoreEventsProcessor.EVENTS_SKIPPED_METRIC);
      eventSkipCounter.inc();
      event.debugLog("Incremented skipped metric to {}", eventSkipCounter.getCount());
      if (event.isDropEvent()) {
        dbEventExecutor_.eventProcessor_.getDeleteEventLog()
            .removeEvent(event.getEventId());
      }
      return true;
    }

    /**
     * Process the events on the DbProcessor. It is invoked from DbEventExecutor's thread
     * periodically to process events for the DbProcessor. It dispatches table events and
     * database events to the TableProcessors that needs to process them and also process
     * the database events that are eligible for processing.
     * @throws Exception
     */
    private void process() throws Exception {
      MetastoreEvent event;
      long skipEventId = skipEventId_.get();
      while ((event = inEvents_.peek()) != null) {
        if (isTerminating()) return;
        if (dbEventExecutor_.eventProcessor_.getStatus() != EventProcessorStatus.ACTIVE) {
          LOG.warn("Event processing is skipped for executor: {} since status is {}",
              dbEventExecutor_.name_,
              dbEventExecutor_.eventProcessor_.getStatus());
          return;
        }
        if (!skipEventIfPossible(event, skipEventId)) {
          if (event.isDatabaseEvent()) {
            dispatchDbEvent((MetastoreDatabaseEvent) event);
          } else {
            dispatchTableEvent(event);
          }
        }
        synchronized (processorLock_) {
          if (isTerminating()) return;
          Preconditions.checkState(inEvents_.poll() == event);
          dbEventExecutor_.decrOutstandingEventCount(1);
        }
      }
      processDbEvents();
      skipEventId_.compareAndSet(skipEventId, -1);
      cleanIfNecessary(false);
    }
  }

  DbEventExecutor(MetastoreEventsProcessor eventProcessor, String name, long interval,
      int numTableEventExecutor, Map<String, DbEventExecutor> dbNameToEventExecutor) {
    Preconditions.checkArgument(eventProcessor != null);
    Preconditions.checkArgument(dbNameToEventExecutor != null);
    Preconditions.checkArgument(numTableEventExecutor > 0);
    eventProcessor_ = eventProcessor;
    name_ = "DbEventExecutor-" + name;
    interval_ = interval;
    service_ = Executors.newSingleThreadScheduledExecutor(
        new ThreadFactoryBuilder().setDaemon(true).setNameFormat(name_).build());
    tableEventExecutors_ = new ArrayList<>(numTableEventExecutor);
    for (int i = 0; i < numTableEventExecutor; i++) {
      tableEventExecutors_.add(
          new TableEventExecutor(eventProcessor, name_, String.valueOf(i), interval_));
    }
    dbNameToEventExecutor_ = dbNameToEventExecutor;
  }

  @VisibleForTesting
  List<TableEventExecutor> getTableEventExecutors() {
    return tableEventExecutors_;
  }

  /**
   * Gets the DbProcessor count within the DbEventExecutor.
   * @return Count of DbProcessor
   */
  long getDbCount() {
    return dbProcessors_.size();
  }

  /**
   * Increments the outstanding event count for the DbEventExecutor.
   * <p>
   * This method is invoked in the following cases:
   * <ul>
   * <li> When a metastore event is added to the inEvents_ queue on the
   * DbProcessor via {@link DbProcessor#enqueue(MetastoreEvent)}.</li>
   * <li> When a DbBarrierEvent is added to the barrierEvents_ queue on the DbProcessor
   * via {@link DbProcessor#dispatchDbEvent(MetastoreDatabaseEvent)}.</li>
   * </ul>
   */
  private void incrOutstandingEventCount() {
    outstandingEventCount_.incrementAndGet();
  }

  /**
   * Decrements the outstanding event count by the given value for the DbEventExecutor.
   * <p>
   * This method is invoked in the following cases:
   * <ul>
   * <li> When a metastore event is processed by the DbProcessor and removed from the
   * inEvents_ queue in {@link DbProcessor#process()}.</li>
   * <li> When a DbBarrierEvent is processed by the DbProcessor and removed from the
   * barrierEvents_ queue in {@link DbProcessor#processDbEvents()}.</li>
   * <li> When a DbProcessor is cleared in {@link DbProcessor#clear()}.</li>
   * </ul>
   * @param delta Value to decrement
   */
  private void decrOutstandingEventCount(long delta) {
    Preconditions.checkState(outstandingEventCount_.addAndGet(-delta) >= 0,
        "outstandingEventCount is negative after decrement.");
  }

  /**
   * Gets the aggregated outstanding event count of the DbEventExecutor and all its
   * TableEventExecutors.
   * @return Outstanding event count
   */
  long getOutstandingEventCount() {
    long outstandingEventCount = outstandingEventCount_.get();
    return outstandingEventCount + tableEventExecutors_.stream()
        .mapToLong(TableEventExecutor::getOutstandingEventCount).sum();
  }

  /**
   * Starts the DbEventExecutor.
   */
  void start() {
    Preconditions.checkNotNull(service_);
    service_.scheduleAtFixedRate(this::process, interval_, interval_,
        TimeUnit.MILLISECONDS);
    tableEventExecutors_.forEach(TableEventExecutor::start);
    LOG.debug("Started executor: {}", name_);
  }

  /**
   * Removes all DbProcessors forcefully or just the idle DbProcessors if any
   * <p>
   * Note: This method can be called from multiple threads. It is thread safe and
   * reentrant. Methods invoked from it are safe and reentrant too.
   * @param force Passed with true to indicate the removal of all the DbProcessors. And
   *              false to indicate the removal of idle DbProcessor if any.
   */
  private void cleanIfNecessary(boolean force) {
    Iterator<Map.Entry<String, DbProcessor>> it = dbProcessors_.entrySet().iterator();
    while (it.hasNext()) {
      DbProcessor dbProcessor = it.next().getValue();
      if (force) {
        dbProcessor.clear();
      }
      if (dbProcessor.canBeRemoved()) {
        unAssignEventExecutor(dbProcessor.dbName_);
        it.remove();
      }
    }
  }

  /**
   * Cleanup the DbEventExecutor to remove idle DbProcessors if any.
   */
  void cleanup() {
    cleanIfNecessary(false);
  }

  /**
   * Internal method used to clear the DbEventExecutor. It is invoked from
   * {@link DbEventExecutor#clear()} and {@link DbEventExecutor#stop()}.
   */
  private void clearInternal() {
    cleanIfNecessary(true);
    tableEventExecutors_.forEach(TableEventExecutor::clear);
    Preconditions.checkState(outstandingEventCount_.get() == 0,
        "outstandingEventCount is non-zero after clear.");
  }

  /**
   * Clears the DbEventExecutor.
   */
  void clear() {
    clearInternal();
    LOG.debug("Cleared executor: {}", name_);
  }

  /**
   * Stops the DbEventExecutor.
   */
  void stop() {
    MetastoreEventsProcessor.shutdownAndAwaitTermination(service_);
    tableEventExecutors_.parallelStream().forEach(TableEventExecutor::stop);
    clearInternal();
    LOG.debug("Stopped executor: {}", name_);
  }

  /**
   * Queue the event to appropriate DbProcessor for processing.
   * @param event Metastore event
   */
  void enqueue(MetastoreEvent event) {
    if (eventProcessor_.getStatus() != EventProcessorStatus.ACTIVE) {
      event.warnLog("Event is not queued to executor: {} since status is {}", name_,
          eventProcessor_.getStatus());
      return;
    }
    String dbName = event.getDbName().toLowerCase();
    DbProcessor dbProcessor = dbProcessors_.get(dbName);
    if (dbProcessor == null) {
      dbProcessor = new DbProcessor(this, dbName);
      dbProcessors_.put(dbName, dbProcessor);
      assignEventExecutor(dbName, this);
    }
    dbProcessor.enqueue(event);
  }

  @VisibleForTesting
  TableEventExecutor getTableEventExecutor(String fqTableName) {
    Preconditions.checkNotNull(fqTableName);
    return tableToEventExecutor_.get(fqTableName);
  }

  /**
   * Assigns a TableEventExecutor to the table if not already assigned. Otherwise, gets
   * the assigned TableEventExecutor. Assignment is done when a metastore event need
   * to be queued for a table that do not have a TableProcessor yet.
   * @param fqTableName Fully qualified table name
   * @return TableEventExecutor
   */
  private TableEventExecutor getOrAssignTableEventExecutor(String fqTableName) {
    Preconditions.checkNotNull(fqTableName);
    TableEventExecutor executor = tableToEventExecutor_.get(fqTableName);
    if (executor != null) {
      return executor;
    }
    long minOutStandingEvents = Long.MAX_VALUE;
    long minTableCount = Long.MAX_VALUE;
    for (TableEventExecutor tee : tableEventExecutors_) {
      long tableCount = tee.getTableCount();
      if (tableCount < minTableCount) {
        minTableCount = tableCount;
        minOutStandingEvents = tee.getOutstandingEventCount();
        executor = tee;
      } else if (tableCount == minTableCount) {
        long outstandingEventCount = tee.getOutstandingEventCount();
        if (outstandingEventCount < minOutStandingEvents) {
          minOutStandingEvents = outstandingEventCount;
          executor = tee;
        }
      }
    }
    Preconditions.checkNotNull(executor);
    tableToEventExecutor_.put(fqTableName, executor);
    LOG.info("Assigned executor: {} for table: {}", executor.getName(), fqTableName);
    return executor;
  }

  /**
   * UnAssigns the TableEventExecutor for the table. This happens when TableProcessor is
   * removed for the table.
   * @param fqTableName Fully qualified table name
   * @return
   */
  private void unAssignTableEventExecutor(String fqTableName) {
    Preconditions.checkNotNull(fqTableName);
    TableEventExecutor executor = tableToEventExecutor_.remove(fqTableName);
    String executorName = executor != null ? executor.getName() : "";
    LOG.info("Unassigned executor: {} for table: {}", executorName, fqTableName);
  }

  /**
   * Process the events of DbProcessors. It is DbEventExecutor's thread task to execute
   * periodically.
   */
  void process() {
    try {
      for (Map.Entry<String, DbProcessor> entry : dbProcessors_.entrySet()) {
        DbProcessor dbProcessor = entry.getValue();
        if (eventProcessor_.getStatus() != EventProcessorStatus.ACTIVE) {
          break;
        }
        dbProcessor.process();
      }
    } catch (Exception e) {
      eventProcessor_.handleEventProcessException(e);
    }
  }

  /**
   * Assigns the given DbEventExecutor to the database if not already assigned. Assignment
   * is done when a metastore event need to be queued for a db that do not have
   * DbProcessor yet.
   * @param dbName Database name
   * @param executor DbEventExecutor to assign
   */
  private void assignEventExecutor(String dbName, DbEventExecutor executor) {
    Preconditions.checkNotNull(dbName);
    Preconditions.checkNotNull(executor);
    Preconditions.checkState(dbNameToEventExecutor_.get(dbName) == null);
    dbNameToEventExecutor_.put(dbName, executor);
    LOG.info("Assigned executor: {} for db: {}", executor.name_, dbName);
  }

  /**
   * UnAssigns the DbEventExecutor for the database. This happens when DbProcessor is
   * removed for the db.
   * @param dbName Database name
   */
  private void unAssignEventExecutor(String dbName) {
    Preconditions.checkNotNull(dbName);
    DbEventExecutor executor = dbNameToEventExecutor_.remove(dbName);
    String executorName = executor != null ? executor.name_ : "";
    LOG.info("Unassigned executor: {} for db: {}", executorName, dbName);
  }
}
