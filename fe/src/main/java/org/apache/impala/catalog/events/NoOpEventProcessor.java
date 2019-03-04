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

import org.apache.impala.catalog.events.MetastoreEventsProcessor.EventProcessorStatus;
import org.apache.impala.thrift.TEventProcessorMetrics;
import org.apache.impala.thrift.TEventProcessorMetricsSummaryResponse;

/**
 * A simple no-op events processor which does nothing. Used to plugin to the catalog
 * when event processing is disabled so that we don't have to do a null check every
 * time the event processor is called
 */
public class NoOpEventProcessor implements ExternalEventsProcessor {
  private static final ExternalEventsProcessor INSTANCE = new NoOpEventProcessor();

  private final TEventProcessorMetrics DEFAULT_METRICS_RESPONSE =
      new TEventProcessorMetrics();

  private final TEventProcessorMetricsSummaryResponse DEFAULT_SUMMARY_RESPONSE =
      new TEventProcessorMetricsSummaryResponse();

  /**
   * Gets the instance of NoOpEventProcessor
   */
  public static ExternalEventsProcessor getInstance() { return INSTANCE; }

  private NoOpEventProcessor() {
    // prevents instantiation
    DEFAULT_METRICS_RESPONSE.setStatus(EventProcessorStatus.DISABLED.toString());
    DEFAULT_SUMMARY_RESPONSE.setSummary("");
  }

  @Override
  public void start() {
    // no-op
  }

  @Override
  public long getCurrentEventId() {
    // dummy event id
    return -1;
  }

  @Override
  public void pause() {
    // no-op
  }

  @Override
  public void start(long fromEventId) {
    // no-op
  }

  @Override
  public void shutdown() {
    // no-op
  }

  @Override
  public void processEvents() {
    // no-op
  }

  @Override
  public TEventProcessorMetrics getEventProcessorMetrics() {
    return DEFAULT_METRICS_RESPONSE;
  }

  @Override
  public TEventProcessorMetricsSummaryResponse getEventProcessorSummary() {
    return DEFAULT_SUMMARY_RESPONSE;
  }
}