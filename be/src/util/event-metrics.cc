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

#include "util/event-metrics.h"

#include <ostream>
#include <unordered_map>

#include <gflags/gflags_declare.h>

#include "common/logging.h"
#include "gen-cpp/JniCatalog_types.h"
#include "util/metrics.h"

DECLARE_int32(hms_event_polling_interval_s);

namespace impala {
string MetastoreEventMetrics::NUMBER_EVENTS_RECEIVED_METRIC_NAME =
    "events-processor.events-received";
string MetastoreEventMetrics::NUMBER_EVENTS_SKIPPED_METRIC_NAME =
    "events-processor.events-skipped";
string MetastoreEventMetrics::EVENT_PROCESSOR_STATUS_METRIC_NAME =
    "events-processor.status";
string MetastoreEventMetrics::EVENTS_FETCH_DURATION_MEAN_METRIC_NAME =
    "events-processor.events-fetch-duration-avg";
string MetastoreEventMetrics::EVENTS_FETCH_DURATION_P75_METRIC_NAME =
    "events-processor.events-fetch-duration-p75";
string MetastoreEventMetrics::EVENTS_FETCH_DURATION_P95_METRIC_NAME =
    "events-processor.events-fetch-duration-p95";
string MetastoreEventMetrics::EVENTS_FETCH_DURATION_P99_METRIC_NAME =
    "events-processor.events-fetch-duration-p99";
string MetastoreEventMetrics::EVENTS_FETCH_LAST_DURATION_METRIC_NAME =
    "events-processor.events-fetch-duration-latest";
string MetastoreEventMetrics::EVENTS_PROCESS_DURATION_MEAN_METRIC_NAME =
    "events-processor.events-process-duration-avg";
string MetastoreEventMetrics::EVENTS_PROCESS_DURATION_P75_METRIC_NAME =
    "events-processor.events-process-duration-p75";
string MetastoreEventMetrics::EVENTS_PROCESS_DURATION_P95_METRIC_NAME =
    "events-processor.events-process-duration-p95";
string MetastoreEventMetrics::EVENTS_PROCESS_DURATION_P99_METRIC_NAME =
    "events-processor.events-process-duration-p99";
string MetastoreEventMetrics::EVENTS_PROCESS_LAST_DURATION_METRIC_NAME =
    "events-processor.events-process-duration-latest";

string MetastoreEventMetrics::EVENTS_RECEIVED_1MIN_METRIC_NAME =
    "events-processor.events-received-1min-rate";
string MetastoreEventMetrics::EVENTS_RECEIVED_5MIN_METRIC_NAME =
    "events-processor.events-received-5min-rate";
string MetastoreEventMetrics::EVENTS_RECEIVED_15MIN_METRIC_NAME =
    "events-processor.events-received-15min-rate";
string MetastoreEventMetrics::LAST_SYNCED_EVENT_ID_METRIC_NAME =
    "events-processor.last-synced-event-id";
string MetastoreEventMetrics::LAST_SYNCED_EVENT_TIME_METRIC_NAME =
    "events-processor.last-synced-event-time";
string MetastoreEventMetrics::LATEST_EVENT_ID_METRIC_NAME =
    "events-processor.latest-event-id";
string MetastoreEventMetrics::LATEST_EVENT_TIME_METRIC_NAME =
    "events-processor.latest-event-time";
string MetastoreEventMetrics::PENDING_EVENTS_METRIC_NAME =
    "events-processor.pending-events";
string MetastoreEventMetrics::LAG_TIME_METRIC_NAME = "events-processor.lag-time";

IntCounter* MetastoreEventMetrics::NUM_EVENTS_RECEIVED_COUNTER = nullptr;
IntCounter* MetastoreEventMetrics::NUM_EVENTS_SKIPPED_COUNTER = nullptr;

DoubleGauge* MetastoreEventMetrics::EVENTS_FETCH_DURATION_MEAN = nullptr;
DoubleGauge* MetastoreEventMetrics::EVENTS_FETCH_DURATION_P75 = nullptr;
DoubleGauge* MetastoreEventMetrics::EVENTS_FETCH_DURATION_P95 = nullptr;
DoubleGauge* MetastoreEventMetrics::EVENTS_FETCH_DURATION_P99 = nullptr;
DoubleGauge* MetastoreEventMetrics::EVENTS_FETCH_LAST_DURATION = nullptr;

DoubleGauge* MetastoreEventMetrics::EVENTS_PROCESS_DURATION_MEAN = nullptr;
DoubleGauge* MetastoreEventMetrics::EVENTS_PROCESS_DURATION_P75 = nullptr;
DoubleGauge* MetastoreEventMetrics::EVENTS_PROCESS_DURATION_P95 = nullptr;
DoubleGauge* MetastoreEventMetrics::EVENTS_PROCESS_DURATION_P99 = nullptr;
DoubleGauge* MetastoreEventMetrics::EVENTS_PROCESS_LAST_DURATION = nullptr;

StringProperty* MetastoreEventMetrics::EVENT_PROCESSOR_STATUS = nullptr;

DoubleGauge* MetastoreEventMetrics::EVENTS_RECEIVED_1MIN_RATE = nullptr;
DoubleGauge* MetastoreEventMetrics::EVENTS_RECEIVED_5MIN_RATE = nullptr;
DoubleGauge* MetastoreEventMetrics::EVENTS_RECEIVED_15MIN_RATE = nullptr;
IntCounter* MetastoreEventMetrics::LAST_SYNCED_EVENT_ID = nullptr;
IntCounter* MetastoreEventMetrics::LAST_SYNCED_EVENT_TIME = nullptr;
IntCounter* MetastoreEventMetrics::LATEST_EVENT_ID = nullptr;
IntCounter* MetastoreEventMetrics::LATEST_EVENT_TIME = nullptr;
IntCounter* MetastoreEventMetrics::PENDING_EVENTS = nullptr;
IntCounter* MetastoreEventMetrics::LAG_TIME = nullptr;

// Initialize all the metrics for the events metric group
void MetastoreEventMetrics::InitMetastoreEventMetrics(MetricGroup* metric_group) {
  MetricGroup* event_metrics = metric_group->GetOrCreateChildGroup("events");
  EVENT_PROCESSOR_STATUS = event_metrics->AddProperty<string>(
      EVENT_PROCESSOR_STATUS_METRIC_NAME, "Unavailable");

  // if event processing is disabled no need to initialized the other metrics
  if (FLAGS_hms_event_polling_interval_s <= 0) return;

  NUM_EVENTS_RECEIVED_COUNTER =
      event_metrics->AddCounter(NUMBER_EVENTS_RECEIVED_METRIC_NAME, 0);
  NUM_EVENTS_SKIPPED_COUNTER =
      event_metrics->AddCounter(NUMBER_EVENTS_SKIPPED_METRIC_NAME, 0);

  EVENTS_FETCH_DURATION_MEAN =
      event_metrics->AddDoubleGauge(EVENTS_FETCH_DURATION_MEAN_METRIC_NAME, 0.0);
  EVENTS_FETCH_DURATION_P75 =
      event_metrics->AddDoubleGauge(EVENTS_FETCH_DURATION_P75_METRIC_NAME, 0.0);
  EVENTS_FETCH_DURATION_P95 =
      event_metrics->AddDoubleGauge(EVENTS_FETCH_DURATION_P95_METRIC_NAME, 0.0);
  EVENTS_FETCH_DURATION_P99 =
      event_metrics->AddDoubleGauge(EVENTS_FETCH_DURATION_P99_METRIC_NAME, 0.0);
  EVENTS_FETCH_LAST_DURATION =
      event_metrics->AddDoubleGauge(EVENTS_FETCH_LAST_DURATION_METRIC_NAME, 0.0);

  EVENTS_PROCESS_DURATION_MEAN =
      event_metrics->AddDoubleGauge(EVENTS_PROCESS_DURATION_MEAN_METRIC_NAME, 0.0);
  EVENTS_PROCESS_DURATION_P75 =
      event_metrics->AddDoubleGauge(EVENTS_PROCESS_DURATION_P75_METRIC_NAME, 0.0);
  EVENTS_PROCESS_DURATION_P95 =
      event_metrics->AddDoubleGauge(EVENTS_PROCESS_DURATION_P95_METRIC_NAME, 0.0);
  EVENTS_PROCESS_DURATION_P99 =
      event_metrics->AddDoubleGauge(EVENTS_PROCESS_DURATION_P99_METRIC_NAME, 0.0);
  EVENTS_PROCESS_LAST_DURATION =
      event_metrics->AddDoubleGauge(EVENTS_PROCESS_LAST_DURATION_METRIC_NAME, 0.0);

  EVENTS_RECEIVED_1MIN_RATE =
      event_metrics->AddDoubleGauge(EVENTS_RECEIVED_1MIN_METRIC_NAME, 0.0);
  EVENTS_RECEIVED_5MIN_RATE =
      event_metrics->AddDoubleGauge(EVENTS_RECEIVED_5MIN_METRIC_NAME, 0.0);
  EVENTS_RECEIVED_15MIN_RATE =
      event_metrics->AddDoubleGauge(EVENTS_RECEIVED_15MIN_METRIC_NAME, 0.0);
  LAST_SYNCED_EVENT_ID =
      event_metrics->AddCounter(LAST_SYNCED_EVENT_ID_METRIC_NAME, 0);
  LAST_SYNCED_EVENT_TIME =
      event_metrics->AddCounter(LAST_SYNCED_EVENT_TIME_METRIC_NAME, 0);
  LATEST_EVENT_ID =
      event_metrics->AddCounter(LATEST_EVENT_ID_METRIC_NAME, 0);
  LATEST_EVENT_TIME =
      event_metrics->AddCounter(LATEST_EVENT_TIME_METRIC_NAME, 0);
  PENDING_EVENTS = event_metrics->AddCounter(PENDING_EVENTS_METRIC_NAME, 0);
  LAG_TIME = event_metrics->AddCounter(LAG_TIME_METRIC_NAME, 0);
}

void MetastoreEventMetrics::refresh(TEventProcessorMetrics* response) {
  if (!response) {
    LOG(ERROR)
        << "Received a null response when trying to refresh metastore event metrics";
    return;
  }
  EVENT_PROCESSOR_STATUS->SetValue(response->status.c_str());

  if (response->__isset.events_received) {
    NUM_EVENTS_RECEIVED_COUNTER->SetValue(response->events_received);
  }
  if (response->__isset.events_skipped) {
    NUM_EVENTS_SKIPPED_COUNTER->SetValue(response->events_skipped);
  }
  if (response->__isset.events_fetch_duration_mean) {
    EVENTS_FETCH_DURATION_MEAN->SetValue(response->events_fetch_duration_mean);
  }
  if (response->__isset.events_fetch_duration_p75) {
    EVENTS_FETCH_DURATION_P75->SetValue(response->events_fetch_duration_p75);
  }
  if (response->__isset.events_fetch_duration_p95) {
    EVENTS_FETCH_DURATION_P95->SetValue(response->events_fetch_duration_p95);
  }
  if (response->__isset.events_fetch_duration_p99) {
    EVENTS_FETCH_DURATION_P99->SetValue(response->events_fetch_duration_p99);
  }
  if (response->__isset.last_events_fetch_duration) {
    EVENTS_FETCH_LAST_DURATION->SetValue(response->last_events_fetch_duration);
  }
  if (response->__isset.events_process_duration_mean) {
    EVENTS_PROCESS_DURATION_MEAN->SetValue(response->events_process_duration_mean);
  }
  if (response->__isset.events_process_duration_p75) {
    EVENTS_PROCESS_DURATION_P75->SetValue(response->events_process_duration_p75);
  }
  if (response->__isset.events_process_duration_p95) {
    EVENTS_PROCESS_DURATION_P95->SetValue(response->events_process_duration_p95);
  }
  if (response->__isset.events_process_duration_p99) {
    EVENTS_PROCESS_DURATION_P99->SetValue(response->events_process_duration_p99);
  }
  if (response->__isset.last_events_process_duration) {
    EVENTS_PROCESS_LAST_DURATION->SetValue(response->last_events_process_duration);
  }
  if (response->__isset.events_received_1min_rate) {
    EVENTS_RECEIVED_1MIN_RATE->SetValue(response->events_received_1min_rate);
  }
  if (response->__isset.events_received_5min_rate) {
    EVENTS_RECEIVED_5MIN_RATE->SetValue(response->events_received_1min_rate);
  }
  if (response->__isset.events_received_15min_rate) {
    EVENTS_RECEIVED_15MIN_RATE->SetValue(response->events_received_15min_rate);
  }
  if (response->__isset.last_synced_event_id) {
    LAST_SYNCED_EVENT_ID->SetValue(response->last_synced_event_id);
  }
  if (response->__isset.last_synced_event_time) {
    LAST_SYNCED_EVENT_TIME->SetValue(response->last_synced_event_time);
  }
  if (response->__isset.latest_event_id) {
    LATEST_EVENT_ID->SetValue(response->latest_event_id);
  }
  if (response->__isset.latest_event_time) {
    LATEST_EVENT_TIME->SetValue(response->latest_event_time);
  }
  // last_synced_event_time is 0 at the startup until we have synced any events.
  if (response->__isset.latest_event_time && response->__isset.last_synced_event_time
      && response->last_synced_event_time > 0) {
    // latest_event_time and last_synced_event_time are updated by different threads.
    // It's possible that latest_event_time is stale and smaller than
    // last_synced_event_time. Set the lag to 0 in this case.
    if (response->latest_event_time <= response->last_synced_event_time) {
      LAG_TIME->SetValue(0);
    } else {
      LAG_TIME->SetValue(response->latest_event_time - response->last_synced_event_time);
    }
  }
  if (response->__isset.latest_event_id && response->__isset.last_synced_event_id) {
    // Same as above, latest_event_id and last_synced_event_id are updated by different
    // threads. Set the value to 0 if latest_event_id is stale.
    if (response->latest_event_id <= response->last_synced_event_id) {
      PENDING_EVENTS->SetValue(0);
    } else {
      PENDING_EVENTS->SetValue(
          response->latest_event_id - response->last_synced_event_id);
    }
  }
}
} // namespace impala
