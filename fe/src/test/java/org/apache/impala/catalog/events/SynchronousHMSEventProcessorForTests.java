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

import java.util.List;

import org.apache.hadoop.hive.metastore.api.NotificationEvent;
import org.apache.impala.catalog.CatalogException;
import org.apache.impala.common.Metrics;
import org.apache.impala.service.CatalogOpExecutor;
import org.junit.Assert;

/**
 * A test MetastoreEventProcessor which executes in the same thread. Useful for testing
 * functionality of MetastoreEventsProcessor
 */
public class SynchronousHMSEventProcessorForTests extends MetastoreEventsProcessor {
  public SynchronousHMSEventProcessorForTests(CatalogOpExecutor catalogOpExecutor,
      long startSyncFromId, long pollingFrequencyInMilliSec) throws CatalogException {
    super(catalogOpExecutor, startSyncFromId, pollingFrequencyInMilliSec);
  }

  @Override
  public Metrics getMetrics() {
    return super.getMetrics();
  }

  @Override
  public void startScheduler() {
    // nothing to do here; there is no background thread for this processor
  }

  @Override
  public void processEvents() {
    super.processEvents();
    Assert.assertTrue(ensureEventsProcessedInHierarchicalMode(100000));
    super.updateLatestEventId();
    verifyEventSyncedMetrics();
  }

  @Override
  protected void processEvents(long currentEventId, List<NotificationEvent> events)
      throws MetastoreNotificationException {
    super.processEvents(currentEventId, events);
    Assert.assertTrue(ensureEventsProcessedInHierarchicalMode(100000));
  }

  private void verifyEventSyncedMetrics() {
    Metrics metrics = getMetrics();
    long lastSyncedEventId = (Long) metrics.getGauge(
        MetastoreEventsProcessor.LAST_SYNCED_ID_METRIC).getValue();
    long latestEventId = (Long) metrics.getGauge(
        MetastoreEventsProcessor.LATEST_EVENT_ID).getValue();
    long lastSyncedEventTime = (Long) metrics.getGauge(
        MetastoreEventsProcessor.LAST_SYNCED_EVENT_TIME).getValue();
    long latestEventTime = (Long) metrics.getGauge(
        MetastoreEventsProcessor.LATEST_EVENT_TIME).getValue();
    long greatestSyncedEventId = (Long) metrics.getGauge(
        MetastoreEventsProcessor.GREATEST_SYNCED_EVENT_ID).getValue();
    long greatestSyncedEventTime = (Long) metrics.getGauge(
        MetastoreEventsProcessor.GREATEST_SYNCED_EVENT_TIME).getValue();
    Assert.assertEquals(greatestSyncedEventId, lastSyncedEventId);
    Assert.assertEquals(greatestSyncedEventTime, lastSyncedEventTime);
    if (lastSyncedEventId == latestEventId) {
      Assert.assertEquals("Incorrect last synced event time for event " + latestEventId,
          latestEventTime, lastSyncedEventTime);
    }
  }
}
