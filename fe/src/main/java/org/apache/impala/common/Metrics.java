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

package org.apache.impala.common;

import java.text.DecimalFormat;
import java.util.Map.Entry;

import com.codahale.metrics.Counter;
import com.codahale.metrics.Gauge;
import com.codahale.metrics.Histogram;
import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Snapshot;
import com.codahale.metrics.Timer;

/**
 * Thin wrapper class around MetricRegisty. Allows users to register and access metrics of
 * various types (counter, meter, histogram, and timer). This class is not thread-safe.
 * TODO: Expose the metrics in Json format via a toJson() function.
 */
public final class Metrics {

  // formatter to round the double values of the rate counters to 4 digits after decimal
  private static final DecimalFormat decimalFormatter_ = new DecimalFormat("#.####");

  private final MetricRegistry registry_ = new MetricRegistry();

  public Metrics() {}

  public void addCounter(String name) { registry_.counter(name); }
  public void addMeter(String name) { registry_.meter(name); }
  public void addHistogram(String name) { registry_.histogram(name); }
  public void addTimer(String name) { registry_.timer(name); }

  @SuppressWarnings("rawtypes")
  public <T extends Gauge> void addGauge(String name, T gauge) {
    registry_.register(name, gauge);
  }

  /**
   * Returns a counter named 'name'. If the counter does not exist, it is registered in
   * the metrics registry.
   */
  public Counter getCounter(String name) {
    Counter counter = registry_.getCounters().get(name);
    if (counter == null) counter = registry_.counter(name);
    return counter;
  }

  /**
   * Returns a meter named 'name'. If the meter does not exist, it is registered in the
   * metrics registry.
   */
  public Meter getMeter(String name) {
    Meter meter = registry_.getMeters().get(name);
    if (meter == null) meter = registry_.meter(name);
    return meter;
  }

  /**
   * Returns a histogram named 'name'. If the histogram does not exist, it is registered
   * in the metrics registry.
   */
  public Histogram getHistogram(String name) {
    Histogram histogram = registry_.getHistograms().get(name);
    if (histogram == null) histogram = registry_.histogram(name);
    return histogram;
  }

  /**
   * Returns a timer named 'name'. If the timer does not exist, it is registered in the
   * metrics registry.
   */
  public Timer getTimer(String name) {
    Timer timer = registry_.getTimers().get(name);
    if (timer == null) timer = registry_.timer(name);
    return timer;
  }

  @SuppressWarnings("rawtypes")
  public Gauge getGauge(String name) { return registry_.getGauges().get(name); }

  /**
   * Returns a string representation of all registered metrics.
   */
  @Override
  @SuppressWarnings("rawtypes")
  public String toString() {
    StringBuilder result = new StringBuilder();
    for (Entry<String, Counter> entry: registry_.getCounters().entrySet()) {
      result.append(entry.getKey() + ": " + String.valueOf(entry.getValue().getCount()));
      result.append("\n");
    }
    for (Entry<String, Timer> entry: registry_.getTimers().entrySet()) {
      result.append(entry.getKey() + ": " + timerToString(entry.getValue()));
      result.append("\n");
    }
    for (Entry<String, Gauge> entry: registry_.getGauges().entrySet()) {
      result.append(entry.getKey() + ": " + String.valueOf(entry.getValue().getValue()));
      result.append("\n");
    }
    for (Entry<String, Histogram> entry: registry_.getHistograms().entrySet()) {
      result.append(entry.getKey() + ": " +
          snapshotToString(entry.getValue().getSnapshot()));
      result.append("\n");
    }
    for (Entry<String, Meter> entry : registry_.getMeters().entrySet()) {
      result.append(entry.getKey()).append(":").append(meterToString(entry.getValue()));
      result.append("\n");
    }
    return result.toString();
  }

  /**
   * Helper function that pretty prints the contents of a timer metric.
   */
  private String timerToString(Timer timer) {
    StringBuilder builder = new StringBuilder();
    return builder.append("\n   Count: " + timer.getCount())
        .append("\n   Mean rate: ")
        .append(decimalFormatter_.format(timer.getMeanRate()))
        .append("\n   1 min. rate: ")
        .append(decimalFormatter_.format(timer.getOneMinuteRate()))
        .append("\n   5 min. rate: ")
        .append(decimalFormatter_.format(timer.getFiveMinuteRate()))
        .append("\n   15 min. rate: ")
        .append(decimalFormatter_.format(timer.getFifteenMinuteRate()))
        .append(snapshotToString(timer.getSnapshot()))
        .toString();
  }

  /**
   * Helper method to pretty print the contents of a Meter
   */
  private String meterToString(Meter meter) {
    StringBuilder builder = new StringBuilder();
    return builder.append("\n   Count: ")
        .append(meter.getCount())
        .append("\n   Mean rate: ")
        .append(decimalFormatter_.format(meter.getMeanRate()))
        .append("\n   1 min. rate: ")
        .append(decimalFormatter_.format(meter.getOneMinuteRate()))
        .append("\n   5 min. rate: ")
        .append(decimalFormatter_.format(meter.getFiveMinuteRate()))
        .append("\n   15 min. rate: ")
        .append(decimalFormatter_.format(meter.getFifteenMinuteRate()))
        .toString();
  }

  /**
   * Helper function that pretty prints the contents of a metric snapshot.
   */
  private String snapshotToString(Snapshot snapshot) {
    StringBuilder builder = new StringBuilder();
    return builder.append("\n   Min (msec): ")
        .append(decimalFormatter_.format(snapshot.getMin() / 1000000))
        .append("\n   Max (msec): ")
        .append(decimalFormatter_.format(snapshot.getMax() / 1000000))
        .append("\n   Mean (msec): ")
        .append(decimalFormatter_.format(snapshot.getMean() / 1000000))
        .append("\n   Median (msec): ")
        .append(decimalFormatter_.format(snapshot.getMedian() / 1000000))
        .append("\n   75th-% (msec): ")
        .append(decimalFormatter_.format(snapshot.get75thPercentile() / 1000000))
        .append("\n   95th-% (msec): ")
        .append(decimalFormatter_.format(snapshot.get95thPercentile() / 1000000))
        .append("\n   99th-% (msec): ")
        .append(decimalFormatter_.format(snapshot.get99thPercentile() / 1000000))
        .append("\n")
        .toString();
  }
}
