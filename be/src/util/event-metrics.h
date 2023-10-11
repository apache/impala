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

#pragma once

#include <string>

#include "util/metrics-fwd.h"

namespace impala {

class TEventProcessorMetrics;

/// class which is used to refresh the metastore event related metrics from catalog
class MetastoreEventMetrics {
 public:
  /// Registers and initializes the Metastore event metrics
  static void InitMetastoreEventMetrics(MetricGroup* metric_group);

  /// refresh all the metrics which are used to display on webui based on the given
  /// response this method should be called at regular intervals to update the metrics
  /// information on the webui
  static void refresh(TEventProcessorMetrics* response);

  /// Total number of events received so far
  static IntCounter* NUM_EVENTS_RECEIVED_COUNTER;

  /// Total number of events skipped so far
  static IntCounter* NUM_EVENTS_SKIPPED_COUNTER;

  /// Mean/p75/p95/p99 duration required to fetch a batch of events
  static DoubleGauge* EVENTS_FETCH_DURATION_MEAN;
  static DoubleGauge* EVENTS_FETCH_DURATION_P75;
  static DoubleGauge* EVENTS_FETCH_DURATION_P95;
  static DoubleGauge* EVENTS_FETCH_DURATION_P99;

  /// Duration of fetching the last event batch
  static DoubleGauge* EVENTS_FETCH_LAST_DURATION;

  /// Mean/p75/p95/p99 duration required to process the fetched batch of events
  static DoubleGauge* EVENTS_PROCESS_DURATION_MEAN;
  static DoubleGauge* EVENTS_PROCESS_DURATION_P75;
  static DoubleGauge* EVENTS_PROCESS_DURATION_P95;
  static DoubleGauge* EVENTS_PROCESS_DURATION_P99;

  /// Duration of processing the last event batch
  static DoubleGauge* EVENTS_PROCESS_LAST_DURATION;

  /// The current status of Metastore events processor.
  /// See MetastoreEventProcessor.EventProcessorStatus for possible state values
  static StringProperty* EVENT_PROCESSOR_STATUS;

  /// Exponentially weighted moving avg (EWMA) of number of events received in last 1 min
  static DoubleGauge* EVENTS_RECEIVED_1MIN_RATE;

  /// EWMA of number of events received in last 5 min
  static DoubleGauge* EVENTS_RECEIVED_5MIN_RATE;

  /// EWMA of number of events received in last 15 min
  static DoubleGauge* EVENTS_RECEIVED_15MIN_RATE;

  /// Last metastore event id that the catalog server synced to.
  static IntCounter* LAST_SYNCED_EVENT_ID;

  /// Last metastore event time that the catalog server synced to.
  static IntCounter* LAST_SYNCED_EVENT_TIME;

  /// Latest metastore event id
  static IntCounter* LATEST_EVENT_ID;

  /// Latest metastore event time
  static IntCounter* LATEST_EVENT_TIME;

  /// Number of events pending to be synced
  static IntCounter* PENDING_EVENTS;

  /// Lag time of the event processing
  static IntCounter* LAG_TIME;

 private:
  /// Following metric names must match with the key in metrics.json

  /// metric name for events received counter.
  static std::string NUMBER_EVENTS_RECEIVED_METRIC_NAME;

  /// metric name for events skipped counter
  static std::string NUMBER_EVENTS_SKIPPED_METRIC_NAME;

  /// metric name for event processor status
  static std::string EVENT_PROCESSOR_STATUS_METRIC_NAME;

  /// metric name for the mean/p75/p95/p99 time taken for events fetch metric
  static std::string EVENTS_FETCH_DURATION_MEAN_METRIC_NAME;
  static std::string EVENTS_FETCH_DURATION_P75_METRIC_NAME;
  static std::string EVENTS_FETCH_DURATION_P95_METRIC_NAME;
  static std::string EVENTS_FETCH_DURATION_P99_METRIC_NAME;

  /// metric name for the duration of fetching the last event batch
  static std::string EVENTS_FETCH_LAST_DURATION_METRIC_NAME;

  /// metric name for the mean/p75/p95/p99 time taken for events processing metric
  static std::string EVENTS_PROCESS_DURATION_MEAN_METRIC_NAME;
  static std::string EVENTS_PROCESS_DURATION_P75_METRIC_NAME;
  static std::string EVENTS_PROCESS_DURATION_P95_METRIC_NAME;
  static std::string EVENTS_PROCESS_DURATION_P99_METRIC_NAME;

  /// metric name for the duration of processing the last event batch
  static std::string EVENTS_PROCESS_LAST_DURATION_METRIC_NAME;

  /// metric name for EWMA of number of events in last 1 min
  static std::string EVENTS_RECEIVED_1MIN_METRIC_NAME;

  /// metric name for EWMA of number of events in last 5 min
  static std::string EVENTS_RECEIVED_5MIN_METRIC_NAME;

  /// metric name for EWMA of number of events in last 15 min
  static std::string EVENTS_RECEIVED_15MIN_METRIC_NAME;

  /// Metric name for last metastore event id that the catalog server synced to.
  static std::string LAST_SYNCED_EVENT_ID_METRIC_NAME;

  /// Metric name for the event time of the last synced metastore event
  static std::string LAST_SYNCED_EVENT_TIME_METRIC_NAME;

  /// Metric name for the latest metastore event id
  static std::string LATEST_EVENT_ID_METRIC_NAME;

  /// Metric name for the event time of the latest metastore event
  static std::string LATEST_EVENT_TIME_METRIC_NAME;

  /// Metric name for the number of pending events to be synced
  static std::string PENDING_EVENTS_METRIC_NAME;

  /// Metric name for the lag time of the event processing
  static std::string LAG_TIME_METRIC_NAME;
};

} // namespace impala
