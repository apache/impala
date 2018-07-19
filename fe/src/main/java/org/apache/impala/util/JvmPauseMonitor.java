/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.impala.util;

import java.lang.management.GarbageCollectorMXBean;
import java.lang.management.ManagementFactory;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import org.apache.log4j.Logger;

import com.google.common.base.Joiner;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.common.base.Stopwatch;

/**
 * Class which sets up a simple thread which runs in a loop sleeping
 * for a short interval of time. If the sleep takes significantly longer
 * than its target time, it implies that the JVM or host machine has
 * paused processing, which may cause other problems. If such a pause is
 * detected, the thread logs a message.
 */
public class JvmPauseMonitor {
  private static final Logger LOG = Logger.getLogger(JvmPauseMonitor.class);

  // The target sleep time.
  private static final long SLEEP_INTERVAL_MS = 500;

  // log WARN if we detect a pause longer than this threshold.
  private long warnThresholdMs_;
  private static final long WARN_THRESHOLD_MS = 10000;

  // log INFO if we detect a pause longer than this threshold.
  private long infoThresholdMs_;
  private static final long INFO_THRESHOLD_MS = 1000;

  // Daemon thread running the pause monitor loop.
  private Thread monitorThread_;
  private volatile boolean shouldRun = true;

  // Singleton instance of this pause monitor.
  public static JvmPauseMonitor INSTANCE = new JvmPauseMonitor();

  // Initializes the pause monitor. No-op if called multiple times.
  public static void initPauseMonitor() {
    if (INSTANCE.isStarted()) return;
    INSTANCE.init();
  }

  private JvmPauseMonitor() {
    this(INFO_THRESHOLD_MS, WARN_THRESHOLD_MS);
  }

  private JvmPauseMonitor(long infoThresholdMs, long warnThresholdMs) {
    this.infoThresholdMs_ = infoThresholdMs;
    this.warnThresholdMs_ = warnThresholdMs;
  }

  protected void init() {
    monitorThread_ = new Thread(new Monitor(), "JVM pause monitor");
    monitorThread_.setDaemon(true);
    monitorThread_.start();
  }

  public boolean isStarted() {
    return monitorThread_ != null;
  }

  /**
   * Helper method that formats the message to be logged, along with
   * the GC metrics.
   */
  private String formatMessage(long extraSleepTime,
      Map<String, GcTimes> gcTimesAfterSleep,
      Map<String, GcTimes> gcTimesBeforeSleep) {

    Set<String> gcBeanNames = Sets.intersection(
        gcTimesAfterSleep.keySet(),
        gcTimesBeforeSleep.keySet());
    List<String> gcDiffs = Lists.newArrayList();
    for (String name : gcBeanNames) {
      GcTimes diff = gcTimesAfterSleep.get(name).subtract(
          gcTimesBeforeSleep.get(name));
      if (diff.gcCount != 0) {
        gcDiffs.add("GC pool '" + name + "' had collection(s): " +
            diff.toString());
      }
    }

    String ret = "Detected pause in JVM or host machine (eg GC): " +
        "pause of approximately " + extraSleepTime + "ms\n";
    if (gcDiffs.isEmpty()) {
      ret += "No GCs detected";
    } else {
      ret += Joiner.on("\n").join(gcDiffs);
    }
    return ret;
  }

  private Map<String, GcTimes> getGcTimes() {
    Map<String, GcTimes> map = Maps.newHashMap();
    List<GarbageCollectorMXBean> gcBeans =
        ManagementFactory.getGarbageCollectorMXBeans();
    for (GarbageCollectorMXBean gcBean : gcBeans) {
      map.put(gcBean.getName(), new GcTimes(gcBean));
    }
    return map;
  }

  private static class GcTimes {
    private GcTimes(GarbageCollectorMXBean gcBean) {
      gcCount = gcBean.getCollectionCount();
      gcTimeMillis = gcBean.getCollectionTime();
    }

    private GcTimes(long count, long time) {
      this.gcCount = count;
      this.gcTimeMillis = time;
    }

    private GcTimes subtract(GcTimes other) {
      return new GcTimes(this.gcCount - other.gcCount,
          this.gcTimeMillis - other.gcTimeMillis);
    }

    @Override
    public String toString() {
      return "count=" + gcCount + " time=" + gcTimeMillis + "ms";
    }

    private long gcCount;
    private long gcTimeMillis;
  }

  /**
   * Runnable instance of the pause monitor loop. Launched from serviceStart().
   */
  private class Monitor implements Runnable {
    @Override
    public void run() {
      Stopwatch sw = new Stopwatch();
      Map<String, GcTimes> gcTimesBeforeSleep = getGcTimes();
      LOG.info("Starting JVM pause monitor");
      while (shouldRun) {
        sw.reset().start();
        try {
          Thread.sleep(SLEEP_INTERVAL_MS);
        } catch (InterruptedException ie) {
          return;
        }
        sw.stop();
        long extraSleepTime = sw.elapsedTime(TimeUnit.MILLISECONDS) - SLEEP_INTERVAL_MS;
        Map<String, GcTimes> gcTimesAfterSleep = getGcTimes();

        if (extraSleepTime > warnThresholdMs_) {
          LOG.warn(formatMessage(
              extraSleepTime, gcTimesAfterSleep, gcTimesBeforeSleep));
        } else if (extraSleepTime > infoThresholdMs_) {
          LOG.info(formatMessage(
              extraSleepTime, gcTimesAfterSleep, gcTimesBeforeSleep));
        }
        gcTimesBeforeSleep = gcTimesAfterSleep;
      }
    }
  }

  /**
   * Simple 'main' to facilitate manual testing of the pause monitor.
   *
   * This main function just leaks memory into a list. Running this class
   * with a 1GB heap will very quickly go into "GC hell" and result in
   * log messages about the GC pauses.
   */
  @SuppressWarnings("resource")
  public static void main(String []args) throws Exception {
    JvmPauseMonitor monitor = new JvmPauseMonitor();
    monitor.init();
    List<String> list = Lists.newArrayList();
    int i = 0;
    while (true) {
      list.add(String.valueOf(i++));
    }
  }
}


