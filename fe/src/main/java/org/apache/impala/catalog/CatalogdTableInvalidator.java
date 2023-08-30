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
// under the License

package org.apache.impala.catalog;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.base.Ticker;
import com.google.common.util.concurrent.Uninterruptibles;
import com.sun.management.GarbageCollectorMXBean;
import com.sun.management.GcInfo;
import org.apache.impala.common.Reference;
import org.apache.impala.service.BackendConfig;
import org.apache.impala.thrift.TTableName;
import org.apache.impala.util.NoOpEventSequence;
import org.apache.log4j.Logger;

import javax.management.Notification;
import javax.management.NotificationEmitter;
import javax.management.NotificationListener;
import java.lang.management.MemoryUsage;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Automatically invalidates recently unused tables. There are currently 2 rules
 * implemented:
 * 1. Invalidate a certain percentage of the least recently used tables after a GC with an
 * almost full old generation. The fullness of the GC generation depends on the maximum
 * heap size.
 * 2. If invalidate_tables_timeout_s is set in the backend, unused tables older than the
 * threshold are invalidated periodically.
 */
public class CatalogdTableInvalidator {
  public static final Logger LOG = Logger.getLogger(CatalogdTableInvalidator.class);
  /**
   * Plugable time source for tests. Defined as static to avoid passing
   * CatalogdTableInvalidator everywhere the clock is used.
   */
  @VisibleForTesting
  static Ticker TIME_SOURCE = Ticker.systemTicker();
  private static long DAEMON_MAXIMUM_SLEEP_NANO = TimeUnit.MINUTES.toNanos(5);
  final private long unusedTableTtlNano_;
  final private boolean invalidateTableOnMemoryPressure_;
  final private CatalogServiceCatalog catalog_;
  /**
   * A thread waking up periodically to check if eviction is needed.
   */
  final private Thread daemonThread_;
  /**
   * The threshold above which the old gen is considered almost full.
   */
  final private double oldGenFullThreshold_;
  /**
   * The ratio of tables to invalidate when the old gen is almost full.
   */
  final private double gcInvalidationFraction_;
  /**
   * The number of times the daemon thread wakes up and scans the tables for invalidation.
   * It's useful for tests to ensure that a scan happened.
   */
  @VisibleForTesting
  AtomicLong scanCount_ = new AtomicLong();
  private GarbageCollectorMXBean oldGenGcBean_;
  /**
   * The name of the old gen memory pool.
   */
  private String oldGcGenName_;
  /**
   * The value of oldGenGcBean_.getCollectionCount() when the last memory-based
   * invalidation was executed.
   */
  private long lastObservedGcCount_;
  private boolean stopped_ = false;
  /**
   * Last time an time-based invalidation is executed in nanoseconds.
   */
  private long lastInvalidationTime_;

  CatalogdTableInvalidator(CatalogServiceCatalog catalog, final long unusedTableTtlSec,
      boolean invalidateTableOnMemoryPressure, double oldGenFullThreshold,
      double gcInvalidationFraction) {
    catalog_ = catalog;
    unusedTableTtlNano_ = TimeUnit.SECONDS.toNanos(unusedTableTtlSec);
    oldGenFullThreshold_ = oldGenFullThreshold;
    gcInvalidationFraction_ = gcInvalidationFraction;
    lastInvalidationTime_ = TIME_SOURCE.read();
    invalidateTableOnMemoryPressure_ =
        invalidateTableOnMemoryPressure && tryInstallGcListener();
    daemonThread_ = new Thread(new DaemonThread());
    daemonThread_.setDaemon(true);
    daemonThread_.setName("CatalogTableInvalidator timer");
    daemonThread_.start();
  }

  public static CatalogdTableInvalidator create(CatalogServiceCatalog catalog,
      BackendConfig config) {
    final boolean invalidateTableOnMemoryPressure =
        config.invalidateTablesOnMemoryPressure();
    final int timeoutSec = config.getInvalidateTablesTimeoutS();
    final double gcOldGenFullThreshold =
        config.getInvalidateTablesGcOldGenFullThreshold();
    final double fractionOnMemoryPressure =
        config.getInvalidateTablesFractionOnMemoryPressure();
    Preconditions.checkArgument(timeoutSec >= 0,
        "invalidate_tables_timeout_s must be a non-negative integer.");
    Preconditions.checkArgument(gcOldGenFullThreshold >= 0 && gcOldGenFullThreshold <= 1,
        "invalidate_tables_gc_old_gen_full_threshold must be in [0, 1].");
    Preconditions
        .checkArgument(fractionOnMemoryPressure >= 0 && fractionOnMemoryPressure <= 1,
            "invalidate_tables_fraction_on_memory_pressure must be in [0, 1].");
    if (timeoutSec > 0 || invalidateTableOnMemoryPressure) {
      return new CatalogdTableInvalidator(catalog, timeoutSec,
          invalidateTableOnMemoryPressure, gcOldGenFullThreshold,
          fractionOnMemoryPressure);
    } else {
      return null;
    }
  }

  static long nanoTime() {
    return TIME_SOURCE.read();
  }

  /**
   * Try to find the old generation memory pool in the GC beans and listen to the GC bean
   * which the old gen memory pool belongs to. Return whether the GC beans are supported.
   * If the return value is false, the listener is not installed.
   */
  private boolean tryInstallGcListener() {
    String commonErrMsg = "Continuing without GC-triggered invalidation of tables.";
    List<GarbageCollectorMXBean> gcbeans = java.lang.management.ManagementFactory
        .getPlatformMXBeans(GarbageCollectorMXBean.class);
    GcNotificationListener gcNotificationListener = new GcNotificationListener();

    boolean foundOldPool = false;
    for (GarbageCollectorMXBean gcbean : gcbeans) {
      for (String poolName : gcbean.getMemoryPoolNames()) {
        if (!poolName.contains("Old")) continue;
        if (!(gcbean instanceof NotificationEmitter)) {
          LOG.warn("GCBean " + gcbean.getClass().getName() + " is not supported " +
              "because it does not implement NotificationEmitter. " + commonErrMsg);
          return false;
        }
        oldGenGcBean_ = gcbean;
        oldGcGenName_ = poolName;
        lastObservedGcCount_ = gcbean.getCollectionCount();
        foundOldPool = true;
        ((NotificationEmitter) gcbean)
            .addNotificationListener(gcNotificationListener, null, null);
        break;
      }
    }
    if (!foundOldPool) {
      LOG.warn("Cannot find old generation memory pool in the GC beans. " + commonErrMsg);
    }
    return foundOldPool;
  }

  /**
   * Detect whether a GC happened since the last observation and the old generation is
   * loaded more than the configured threshold. If so it returns true indicating that
   * tables should be evicted because of memory pressure.
   */
  private boolean shouldEvictFromFullHeapAfterGc() {
    if (!invalidateTableOnMemoryPressure_) return false;
    long gcCount = oldGenGcBean_.getCollectionCount();
    if (gcCount > lastObservedGcCount_) {
      lastObservedGcCount_ = gcCount;
      GcInfo lastGcInfo = oldGenGcBean_.getLastGcInfo();
      if (lastGcInfo == null) {
        LOG.warn("gcBean.getLastGcInfo() returned null. Table invalidation based on " +
            "memory pressure was skipped.");
        return false;
      }
      MemoryUsage tenuredGenUsage = lastGcInfo.getMemoryUsageAfterGc().get(oldGcGenName_);
      Preconditions.checkState(tenuredGenUsage != null);
      return tenuredGenUsage.getMax() * oldGenFullThreshold_ < tenuredGenUsage.getUsed();
    }
    return false;
  }

  private void invalidateSome(double invalidationFraction) {
    List<Table> tables = new ArrayList<>();
    for (Db db : catalog_.getAllDbs()) {
      for (Table table : db.getTables()) {
        if (!(table instanceof IncompleteTable)) tables.add(table);
      }
    }
    // TODO: use quick select
    Collections.sort(tables, new Comparator<Table>() {
      @Override
      public int compare(Table o1, Table o2) {
        return Long.compare(o1.getLastUsedTime(), o2.getLastUsedTime());
      }
    });
    for (int i = 0; i < tables.size() * invalidationFraction; ++i) {
      TTableName tTableName = tables.get(i).getTableName().toThrift();
      Reference<Boolean> tblWasRemoved = new Reference<>();
      Reference<Boolean> dbWasAdded = new Reference<>();
      catalog_.invalidateTable(tTableName, tblWasRemoved, dbWasAdded,
          NoOpEventSequence.INSTANCE);
      LOG.info("Table " + tables.get(i).getFullName() + " invalidated due to memory " +
          "pressure.");
    }
  }

  private void invalidateOlderThan(long retireAgeNano) {
    long now = TIME_SOURCE.read();
    for (Db db : catalog_.getAllDbs()) {
      for (Table table : catalog_.getAllTables(db)) {
        if (table instanceof IncompleteTable) continue;
        long inactivityTime = now - table.getLastUsedTime();
        if (inactivityTime <= retireAgeNano) continue;
        Reference<Boolean> tblWasRemoved = new Reference<>();
        Reference<Boolean> dbWasAdded = new Reference<>();
        TTableName tTableName = table.getTableName().toThrift();
        catalog_.invalidateTable(tTableName, tblWasRemoved, dbWasAdded,
            NoOpEventSequence.INSTANCE);
        LOG.info(
            "Invalidated " + table.getFullName() + " due to inactivity for " +
                TimeUnit.NANOSECONDS.toSeconds(inactivityTime) + " seconds.");
      }
    }
  }

  void stop() {
    synchronized (this) {
      stopped_ = true;
      notify();
    }
    try {
      daemonThread_.join();
    } catch (InterruptedException e) {
      LOG.warn("stop() is interrupted", e);
    }
  }

  @VisibleForTesting
  synchronized void wakeUpForTests() {
    notify();
  }

  private class DaemonThread implements Runnable {
    @Override
    public void run() {
      long sleepTimeNano = DAEMON_MAXIMUM_SLEEP_NANO;
      if (unusedTableTtlNano_ > 0) {
        sleepTimeNano = Math.min(unusedTableTtlNano_ / 10, sleepTimeNano);
      }
      while (true) {
        try {
          synchronized (CatalogdTableInvalidator.this) {
            if (stopped_) return;
            if (shouldEvictFromFullHeapAfterGc()) {
              invalidateSome(gcInvalidationFraction_);
              scanCount_.incrementAndGet();
            }
            long now = nanoTime();
            // Wait for a fraction of unusedTableTtlNano_ if time-based invalidation is
            // enabled
            if (unusedTableTtlNano_ > 0 && now >= lastInvalidationTime_ + sleepTimeNano) {
              invalidateOlderThan(unusedTableTtlNano_);
              lastInvalidationTime_ = now;
              scanCount_.incrementAndGet();
            }
            // Wait unusedTableTtlSec if it is configured. Otherwise wait
            // indefinitely.
            TimeUnit.NANOSECONDS.timedWait(CatalogdTableInvalidator.this, sleepTimeNano);
          }
        } catch (Exception e) {
          LOG.warn("Unexpected exception thrown while attempting to automatically " +
              "invalidate tables. Will retry in 5 seconds.", e);
          Uninterruptibles.sleepUninterruptibly(5, TimeUnit.SECONDS);
        }
      }
    }
  }

  private class GcNotificationListener implements NotificationListener {
    @Override
    public void handleNotification(Notification notification, Object handback) {
      synchronized (CatalogdTableInvalidator.this) {
        CatalogdTableInvalidator.this.notify();
      }
    }
  }
}
