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
import com.google.common.base.Preconditions;
import com.google.common.util.concurrent.ThreadFactoryBuilder;

import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.impala.catalog.events.MetastoreEvents.DerivedMetastoreTableEvent;
import org.apache.impala.catalog.events.MetastoreEvents.MetastoreEvent;
import org.apache.impala.catalog.events.MetastoreEvents.MetastoreEventType;
import org.apache.impala.common.PrintUtils;
import org.apache.impala.service.BackendConfig;
import org.apache.impala.util.ClassUtil;
import org.apache.impala.util.ThreadNameAnnotator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * An instance of this class has an execution thread, processes events of multiple
 * tables(i.e., with {@link TableProcessor}). An instance of TableProcessor is
 * maintained for each table within TableEventExecutor. On each scheduled execution,
 * {@link TableProcessor#process()} is invoked for each TableProcessor it manages.
 * Once a TableEventExecutor is assigned to table, all its subsequent table events are
 * processed by same TableEventExecutor.
 *
 * @see org.apache.impala.catalog.events.TableEventExecutor.TableProcessor
 */
public class TableEventExecutor {
  private static final Logger LOG = LoggerFactory.getLogger(TableEventExecutor.class);

  // Fully qualified table name to TableProcessor map. TableProcessors managed by
  // TableEventExecutor
  private final Map<String, TableProcessor> tableProcessors_ = new ConcurrentHashMap<>();

  // Executor name, like "DbEventExecutor-0.TableEventExecutor-0"
  private final String name_;
  private final MetastoreEventsProcessor eventProcessor_;

  // Task schedule interval
  private final long interval_;
  private final ScheduledExecutorService service_;

  // Outstanding events to process count
  private AtomicLong outstandingEventCount_ = new AtomicLong();

  /**
   * A TableProcessor contains information about table. It maintains event queue to
   * receive events from {@link DbEventExecutor.DbProcessor}. Upon
   * {@link TableProcessor#process()} invocation by TableEventExecutor thread, events
   * are processed.
   * <p>
   * When a {@link DbBarrierEvent} is encountered while processing, it is
   * indicated to the DbProcessor with {@link DbBarrierEvent#proceed()} and do not
   * process any further events till the DbProcessor has processed that event.
   * <p>
   * When a {@link RenameTableBarrierEvent} is encountered while processing, determines
   * whether the event can be processed with {@link RenameTableBarrierEvent#canProcess()}
   * method and process the event only if it returns true otherwise do not process any
   * further events until the event is processed later.
   *
   * @see org.apache.impala.catalog.events.DbBarrierEvent
   * @see org.apache.impala.catalog.events.DbEventExecutor.DbProcessor
   * @see org.apache.impala.catalog.events.RenameTableBarrierEvent
   */
  public static class TableProcessor {
    // Fully qualified table name
    private final String fqTableName_;

    // last event queued time
    private long lastEventQueuedTime_;

    // Last processed event id
    private long lastProcessedEventId_ = -1;

    // Used to skip all events prior to this event id
    private final AtomicLong skipEventId_ = new AtomicLong(-1);
    private final TableEventExecutor tableEventExecutor_;

    /**
     * Indicates whether TableProcessor is terminating. Events are processed only when
     * isTerminating_ is false.
     * <p>
     * TableProcessor event queueing {@link TableProcessor#enqueue(MetastoreEvent)} and
     * processing {@link TableProcessor#process()} are invoked from different threads.
     * {@link TableProcessor#enqueue(MetastoreEvent)} acquires processorLock_ to
     * atomically update the event queue, increment outstanding event count so that
     * {@link TableProcessor#process()} gets the consistent view of them together.
     * <p>
     * TableProcessor deletion {@link TableEventExecutor#deleteTableProcessor(String)}
     * and processing {@link TableProcessor#process()} are invoked from different threads.
     * {@link TableEventExecutor#deleteTableProcessor(String)} acquires processorLock_ to
     * immediately set isTerminating_ to true, thereby preventing any further event
     * processing in {@link TableProcessor#process()}.
     * <p>
     * TableProcessor event queueing {@link TableProcessor#enqueue(MetastoreEvent)}
     * and deletion {@link TableEventExecutor#deleteTableProcessor(String)} are invoked
     * from DbProcessor in same thread(upon TableProcessor idle timeout) or in different
     * threads(forcibly clear). But their invocation is mutually exclusive because
     * DbProcessor protects with its processorLock_.
     * <p>
     * {@link TableProcessor#process()} acquires processorLock_ in following cases:
     * <ul>
     * <li> To check if TableProcessor is not terminating before processing an event.</li>
     * <li> To safely poll processed events from queue and to decrement the outstanding
     * event count if the TableProcessor is not terminating.</li>
     * </ul>
     * <p>
     * Normally, a TableProcessor is deleted by the DbProcessor after an idle timeout,
     * determined by {@link TableProcessor#canBeRemoved()}. However, when
     * {@link DbEventExecutor#clear()} or {@link DbEventExecutor#stop()} is invoked,
     * all associated DbProcessors are forcibly cleared and removed. During that process,
     * each DbProcessor also forcefully deletes its TableProcessors.
     */
    private final Object processorLock_ = new Object();
    private volatile boolean isTerminating_ = false;

    // Events received from DbProcessor to process
    private final Queue<MetastoreEvent> events_ = new ConcurrentLinkedQueue<>();

    private TableProcessor(TableEventExecutor executor, String fqTableName) {
      tableEventExecutor_ = executor;
      fqTableName_ = fqTableName;
    }

    TableEventExecutor getTableEventExecutor() {
      return tableEventExecutor_;
    }

    /**
     * Gets fully qualified table name.
     * @return
     */
    String getTableName() {
      return fqTableName_;
    }

    /**
     * To determine if TableProcessor do not have any events to process.
     * @return
     */
    boolean isEmpty() {
      return events_.isEmpty();
    }

    /**
     * Gets Metastore event processor
     * @return
     */
    private MetastoreEventsProcessor getEventProcessor() {
      return tableEventExecutor_.eventProcessor_;
    }

    /**
     * Queue the event to the TableProcessor for processing.
     * @param event
     */
    void enqueue(MetastoreEvent event) {
      MetastoreEventsProcessor eventProcessor = getEventProcessor();
      if (!eventProcessor.canProcessEventInCurrentStatus()) {
        event.warnLog("Event is not queued to executor: {} since status is {}",
            tableEventExecutor_.name_, eventProcessor.getStatus());
        return;
      }
      lastEventQueuedTime_ = System.currentTimeMillis();
      if (event instanceof DbBarrierEvent) {
        ((DbBarrierEvent) event).incrExpectedProceedCount();
      } else if (event.getEventType() == MetastoreEventType.DROP_TABLE) {
        skipEventId_.set(event.getEventId());
      }
      synchronized (processorLock_) {
        Preconditions.checkState(!isTerminating());
        events_.offer(event);
        tableEventExecutor_.incrOutstandingEventCount();
      }
      event.debugLog("Enqueued for table: {} on executor: {}", fqTableName_,
          tableEventExecutor_.name_);
    }

    /**
     * Determines whether the TableProcessor can be removed.
     * @return True if TableProcessor can be removed from the TableEventExecutor. False
     *         otherwise
     */
    boolean canBeRemoved() {
      return events_.isEmpty() &&
          (System.currentTimeMillis() - lastEventQueuedTime_) >
              BackendConfig.INSTANCE.getMinEventProcessorIdleMs();
    }

    /**
     * Determines whether TableProcessor is terminating.
     * @return True if terminating. False otherwise
     */
    private boolean isTerminating() {
      synchronized (processorLock_) {
        if (isTerminating_ && LOG.isDebugEnabled()) {
          LOG.debug("Processor is terminating for table: {}. Caller stacktrace: {}",
              fqTableName_, ClassUtil.getStackTraceForThread());
        }
        return isTerminating_;
      }
    }

    /**
     * Skip the metastore event from processing if possible.
     * @param event Metastore event
     * @return True if event is skipped. Else false.
     */
    private boolean skipEventIfPossible(MetastoreEvent event) {
      if (event.getEventId() >= skipEventId_.get() || event instanceof DbBarrierEvent ||
          event instanceof RenameTableBarrierEvent) {
        return false;
      }
      Counter eventSkipCounter = event.getMetrics()
          .getCounter(MetastoreEventsProcessor.EVENTS_SKIPPED_METRIC);
      eventSkipCounter.inc();
      event.debugLog("Incremented skipped metric to {}", eventSkipCounter.getCount());
      return true;
    }

    /**
     * Method invoked after processing the metastore event.
     * @param event Metastore event
     */
    private void postProcessEvent(MetastoreEvent event) {
      if (event instanceof DbBarrierEvent) return;
      lastProcessedEventId_ = event.getEventId();
      MetastoreEventsProcessor eventProcessor = getEventProcessor();
      boolean removeFromInProgressLog = true;
      if (event instanceof DerivedMetastoreTableEvent) {
        DerivedMetastoreTableEvent derivedEvent = (DerivedMetastoreTableEvent) event;
        derivedEvent.markProcessed();
        removeFromInProgressLog = derivedEvent.isAllDerivedEventsProcessed();
      }
      if (removeFromInProgressLog) {
        eventProcessor.getEventExecutorService()
            .removeFromInProgressLog(event.getEventId());
      }
      if (event.isDropEvent()) {
        eventProcessor.getDeleteEventLog().removeEvent(event.getEventId());
      }
    }

    /**
     * Process the given metastore event.
     * @param event Metastore event
     * @return True if event is processed. False otherwise
     * @throws Exception
     */
    private boolean processEvent(MetastoreEvent event) throws Exception {
      if (isTerminating()) return false;
      MetastoreEventsProcessor eventProcessor = getEventProcessor();
      if (!eventProcessor.canProcessEventInCurrentStatus()) {
        LOG.warn("Event processing is skipped for executor: {} since status is {}",
            tableEventExecutor_.name_, eventProcessor.getStatus());
        return false;
      }
      boolean isRenameTableBarrier = event instanceof RenameTableBarrierEvent;
      if (isRenameTableBarrier && !((RenameTableBarrierEvent) event).canProcess()) {
        event.traceLog("Rename table barrier waiting for table: {}", fqTableName_);
        return false;
      }
      boolean isDbEvent = event instanceof DbBarrierEvent;
      if (isDbEvent && event.getEventId() != lastProcessedEventId_) {
        // Indicate the processing has reached this event
        ((DbBarrierEvent) event).proceed();
        lastProcessedEventId_ = event.getEventId();
      }
      if (!skipEventIfPossible(event)) {
        if (isDbEvent) {
          if (!((DbBarrierEvent) event).isAllDerivedEventsProcessed()) {
            // Waiting for the db event to be processed
            event.traceLog("DB barrier waiting for table: {}", fqTableName_);
            return false;
          }
        } else {
          try (ThreadNameAnnotator tna = new ThreadNameAnnotator(
              String.format("Processing %s for table: %s", event.getEventDesc(),
                  fqTableName_))) {
            long processingStartTime = System.currentTimeMillis();
            event.processIfEnabled();
            event.infoLog("Scheduling delay: {}, Process time: {}",
                PrintUtils.printTimeMs(processingStartTime - event.getDispatchTime()),
                PrintUtils.printTimeMs(System.currentTimeMillis() - processingStartTime));
          } catch (Exception processingEx) {
            try {
              if (!event.onFailure(processingEx)) throw processingEx;
            } catch (Exception onFailureEx) {
              event.errorLog("Failed to handle event processing failure for table: {}",
                  fqTableName_, onFailureEx);
              throw processingEx;
            }
          }
        }
      }
      return true;
    }

    /**
     * Process the events on the TableProcessor. It is invoked from TableEventExecutor's
     * thread periodically to process events for the TableProcessor.
     * @throws EventProcessException
     */
    private void process() throws EventProcessException {
      MetastoreEvent event;
      while ((event = events_.peek()) != null) {
        try {
          boolean isProcessed = processEvent(event);
          if (!isProcessed) return;
          postProcessEvent(event);
          synchronized (processorLock_) {
            if (isTerminating()) return;
            Preconditions.checkState(events_.poll() == event);
            tableEventExecutor_.decrOutstandingEventCount(1);
          }
        } catch (Exception e) {
          // Throwing EventProcessException triggers global invalidates metadata without
          // user intervention iff invalidate_global_metadata_on_event_processing_failure
          // flag is true. Otherwise, user has to explicitly issue invalidate metadata.
          // Invalidate metadata resets catalog instance that clears EventExecutorService.
          // And EventExecutorService inherently clears all DbEventExecutor and
          // TableEventExecutor thereby removing all DbProcessors and TableProcessors.
          throw new EventProcessException(event.getEvent(), e);
        }
      }
    }
  }

  TableEventExecutor(MetastoreEventsProcessor eventProcessor, String executorNamePrefix,
      String name, long interval) {
    Preconditions.checkArgument(eventProcessor != null);
    eventProcessor_ = eventProcessor;
    name_ = executorNamePrefix + ".TableEventExecutor-" + name;
    interval_ = interval;
    service_ = Executors.newSingleThreadScheduledExecutor(
        new ThreadFactoryBuilder().setDaemon(true).setNameFormat(name_).build());
  }

  /**
   * Get the TableEventExecutor name.
   * @return Name
   */
  String getName() {
    return name_;
  }

  /**
   * Gets the TableProcessor count within the TableEventExecutor.
   * @return Count of TableProcessor
   */
  long getTableCount() {
    return tableProcessors_.size();
  }

  /**
   * Gets the outstanding event count of the TableEventExecutor.
   * @return Outstanding event count
   */
  long getOutstandingEventCount() {
    return outstandingEventCount_.get();
  }

  /**
   * Increments the outstanding event count for the TableEventExecutor.
   * <p>
   * This method is invoked when a metastore event is added to the events_ queue on the
   * TableProcessor via {@link TableProcessor#enqueue(MetastoreEvent)}.
   */
  private void incrOutstandingEventCount() {
    outstandingEventCount_.incrementAndGet();
  }

  /**
   * Decrements the outstanding event count by the given value for the TableEventExecutor.
   * <p>
   * This method is invoked when a metastore event is processed by the TableProcessor and
   * removed from the events_ queue in {@link TableProcessor#process()}. It is also
   * invoked when the TableProcessor is deleted in
   * {@link TableEventExecutor#deleteTableProcessor(String)}
   * @param delta Value to decrement
   */
  private void decrOutstandingEventCount(long delta) {
    Preconditions.checkState(outstandingEventCount_.addAndGet(-delta) >= 0,
        "outstandingEventCount is negative after decrement.");
  }

  /**
   * Starts the TableEventExecutor.
   */
  void start() {
    Preconditions.checkNotNull(service_);
    service_.scheduleAtFixedRate(this::process, interval_, interval_,
        TimeUnit.MILLISECONDS);
    LOG.debug("Started executor: {}", name_);
  }

  /**
   * Clears the TableEventExecutor.
   */
  void clear() {
    Preconditions.checkState(tableProcessors_.isEmpty());
    Preconditions.checkState(outstandingEventCount_.get() == 0,
        "outstandingEventCount is non-zero after clear.");
    LOG.debug("Cleared executor: {}", name_);
  }

  /**
   * Stops the TableEventExecutor.
   */
  void stop() {
    MetastoreEventsProcessor.shutdownAndAwaitTermination(service_);
    LOG.debug("Stopped executor: {}", name_);
  }

  /**
   * Gets a TableProcessor within the TableEventExecutor for the given fully qualified
   * table name if exists. Otherwise, creates a new TableProcessor, adds it to
   * TableEventExecutor and return it.
   * @param fqTableName
   * @return TableProcessor
   */
  TableProcessor getOrCreateTableProcessor(String fqTableName) {
    TableProcessor tableProcessor = tableProcessors_.get(fqTableName);
    if (tableProcessor == null) {
      tableProcessor = new TableProcessor(this, fqTableName);
      tableProcessors_.put(fqTableName, tableProcessor);
    }
    return tableProcessor;
  }

  /**
   * Deletes the TableProcessor within the TableEventExecutor for the given fully
   * qualified table name.
   * @param fqTableName
   */
  void deleteTableProcessor(String fqTableName) {
    TableProcessor tableProcessor = tableProcessors_.remove(fqTableName);
    if (tableProcessor != null) {
      // Set isTerminating_ to true before clearing the events from queue.
      // ConcurrentLinkedQueue.clear() is not atomic
      synchronized (tableProcessor.processorLock_) {
        tableProcessor.isTerminating_ = true;
      }
      // size() is O(n) operation for ConcurrentLinkedQueue. Queue is not updated from
      // other places if isTerminating_ becomes true.
      decrOutstandingEventCount(tableProcessor.events_.size());
      tableProcessor.events_.clear();
    }
  }

  /**
   * Process the events of TableProcessors. It is TableEventExecutor's thread task to
   * execute periodically.
   */
  void process() {
    try {
      for (Map.Entry<String, TableProcessor> entry : tableProcessors_.entrySet()) {
        TableProcessor tableProcessor = entry.getValue();
        if (!eventProcessor_.canProcessEventInCurrentStatus()) break;
        tableProcessor.process();
      }
    } catch (EventProcessException e) {
      LOG.error("Exception occurred for executor: {}", name_);
      eventProcessor_.handleEventProcessException(e.getException(), e.getEvent());
    }
  }
}
