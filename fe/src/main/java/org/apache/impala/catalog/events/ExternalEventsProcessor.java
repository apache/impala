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
import org.apache.impala.catalog.events.MetastoreEvents.MetastoreEventFactory;
import org.apache.impala.service.CatalogOpExecutor;
import org.apache.impala.thrift.TEventProcessorMetrics;
import org.apache.impala.thrift.TEventProcessorMetricsSummaryResponse;
import org.apache.kudu.client.Delete;

/**
 * Interface to process external events
 */
public interface ExternalEventsProcessor {
  /**
   * Start the event processing. This could also be used to initialize the configuration
   * like polling interval of the event processor
   */
  void start();

  /**
   * Get the current event id on metastore. Useful for restarting the event processing
   * from a given event id
   */
  long getCurrentEventId() throws MetastoreNotificationFetchException;

  /**
   * Pauses the event processing. Use <code>start(fromEventId)</code> method below to
   * restart the event processing
   */
  void pause();

  /**
   * Starts the event processing from the given eventId. This method can be used to jump
   * ahead in the event processing under certain cases where it is okay skip certain
   * events
   */
  void start(long fromEventId);

  /**
   * Shutdown the events processor. Cannot be restarted again.
   */
  void shutdown();

  /**
   * Implements the core logic of processing external events
   */
  void processEvents();

  /**
   * Gets the event processor status and metrics. This method is used to show up the
   * metrics on the metrics UI page
   */
  TEventProcessorMetrics getEventProcessorMetrics();

  /**
   * Gets a detailed view of the event processor which can be used to populate the
   * content of a dedicated page for the event processor
   */
  TEventProcessorMetricsSummaryResponse getEventProcessorSummary();

  /**
   * Gets the {@link MetastoreEventFactory} to be used for creating
   * {@link MetastoreEvents.MetastoreEvent}.
   */
  EventFactory getEventsFactory() throws MetastoreNotificationException;

  /**
   * Gets the delete event log for this events processor.
   */
  DeleteEventLog getDeleteEventLog();
}