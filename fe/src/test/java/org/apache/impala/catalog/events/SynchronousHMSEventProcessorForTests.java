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

import org.apache.impala.catalog.CatalogException;
import org.apache.impala.common.Metrics;
import org.apache.impala.service.CatalogOpExecutor;
import org.junit.Assert;

/**
 * A test MetastoreEventProcessor which executes in the same thread. Useful for testing
 * functionality of MetastoreEventsProcessor
 */
public class SynchronousHMSEventProcessorForTests extends MetastoreEventsProcessor {

  public SynchronousHMSEventProcessorForTests(
      CatalogOpExecutor catalogOpExecutor, long startSyncFromId,
      long pollingFrequencyInSec) throws CatalogException {
    super(catalogOpExecutor, startSyncFromId, pollingFrequencyInSec);
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
    super.updateLatestEventId();
    verifyEventSyncedMetrics();
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
    if (lastSyncedEventId == latestEventId) {
      Assert.assertEquals("Incorrect last synced event time for event " + latestEventId,
          latestEventTime, lastSyncedEventTime);
    }
  }
}
