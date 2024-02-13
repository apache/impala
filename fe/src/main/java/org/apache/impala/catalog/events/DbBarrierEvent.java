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

import java.util.concurrent.atomic.AtomicInteger;

import org.apache.impala.catalog.CatalogException;
import org.apache.impala.catalog.events.MetastoreEvents.DerivedMetastoreEvent;
import org.apache.impala.catalog.events.MetastoreEvents.MetastoreDatabaseEvent;
import org.apache.impala.catalog.events.MetastoreEvents.MetastoreEventType;

/**
 * An instance of this class is used to synchronize all the TableProcessors to process
 * a database event. It holds the reference to database event.
 * <p>
 * On a DbProcessor's event processing, when a database event is seen at head end of
 * input event queue, a new instance of this class is created, added to the event queue
 * of all the TableProcessors(belonging to the database) and also to DB event queue of
 * the DbProcessor. It acts as a barrier to restrict the processing of table events that
 * occurred later than the database event, till the database event is processed on
 * DbProcessor.
 * <p>
 * When a TableProcessor's event processing reaches the DbBarrierEvent, it
 * indicates that to DbProcessor event processing with {@link DbBarrierEvent#proceed()}
 * and do not process any further events beyond it. On subsequent event processing
 * schedules, TableProcessor's event processing determines whether database event is
 * processed on DbProcessor with {@link DbBarrierEvent#isProcessed()}.
 * <p>
 * Once all the TableProcessors indicate DbProcessor event processing, DbProcessor
 * process the DbBarrierEvent and marks it as processed. And the TableProcessors detects
 * the event status with {@link DbBarrierEvent#isProcessed()}. Henceforth, all the
 * TableProcessors continue to process their table events independently.
 *
 * @see org.apache.impala.catalog.events.DbEventExecutor.DbProcessor
 * @see org.apache.impala.catalog.events.TableEventExecutor.TableProcessor
 */
public class DbBarrierEvent extends MetastoreDatabaseEvent
    implements DerivedMetastoreEvent {
  // Whether event is processed on DbProcessor
  private volatile boolean isProcessed_;

  // database event
  private final MetastoreDatabaseEvent actualEvent_;

  // Number of TableProcessors this event is added to. When a TableProcessor's
  // event processing reaches this event, count is decremented by 1. Once the
  // count becomes 0(i.e., when last TableProcessor's event processing reach it),
  // the database event is processed on DbProcessor
  private final AtomicInteger expectedProceedCount_ = new AtomicInteger();

  DbBarrierEvent(MetastoreDatabaseEvent actualEvent) {
    super(actualEvent.catalogOpExecutor_, actualEvent.metrics_, actualEvent.event_);
    actualEvent_ = actualEvent;
  }

  @Override
  public void processIfEnabled() throws CatalogException, MetastoreNotificationException {
    Preconditions.checkState(expectedProceedCount_.get() == 0);
    actualEvent_.processIfEnabled();
    if (getEventType() == MetastoreEventType.DROP_DATABASE) {
      catalog_.getMetastoreEventProcessor().getDeleteEventLog().removeEvent(getEventId());
    }
    isProcessed_ = true;
  }

  @Override
  protected void process() {
    throw new UnsupportedOperationException("Not supported for DB barrier event");
  }

  @Override
  protected SelfEventContext getSelfEventContext() {
    throw new UnsupportedOperationException("Not supported for DB barrier event");
  }

  /**
   * To indicate a TableProcessor has reached processing till this event.
   * <p>
   * TableProcessor invokes it when this event is seen during event processing. And
   * waits for DbProcessor to process the event. TableProcessor determines whether
   * the event is processed at DbProcessor using {@link DbBarrierEvent#isProcessed()}
   * method.
   */
  void proceed() {
    decrExpectedProceedCount();
  }

  /**
   * Used to determine if this event is processed on DbProcessor.
   * @return True if the event is processed. False otherwise
   */
  boolean isProcessed() {
    return isProcessed_;
  }

  /**
   * Returns the count of TableProcessor that are expected to reach this event during
   * event processing.
   * @return Number of TableProcessor yet to reach the event
   */
  int getExpectedProceedCount() {
    return expectedProceedCount_.get();
  }

  /**
   * Increments the TableProcessor count that are expected to reach this event during
   * event processing.
   */
  void incrExpectedProceedCount() {
    int value = expectedProceedCount_.incrementAndGet();
    debugLog("Number of table processors expected to process the event: {}", value);
  }

  /**
   * Decrements TableProcessor count that are expected to reach this event during
   * event processing.
   */
  private void decrExpectedProceedCount() {
    int value = expectedProceedCount_.decrementAndGet();
    debugLog("Number of table processors expected to process the event: {}", value);
  }
}
