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

package org.apache.impala.catalog;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;

import org.apache.impala.analysis.TableName;
import org.apache.impala.common.JniUtil;
import org.apache.impala.service.BackendConfig;
import org.apache.impala.service.FeSupport;
import org.apache.impala.thrift.TErrorCode;
import org.apache.impala.thrift.TTableName;
import org.apache.impala.thrift.TTableUsage;
import org.apache.impala.thrift.TUpdateTableUsageRequest;
import org.apache.impala.thrift.TUpdateTableUsageResponse;
import org.apache.log4j.Logger;
import org.apache.thrift.TSerializer;
import org.apache.thrift.protocol.TBinaryProtocol;

import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;

/**
 * Track the names and the number of usages of the recently used tables and report the
 * data to catalogd asynchronously in order to invalidate the recently unused tables.
 */
public class ImpaladTableUsageTracker {
  private static final Logger LOG = Logger.getLogger(ImpaladTableUsageTracker.class);
  private final static long REPORT_INTERVAL_MS = 10000;
  private Map<TTableName, TTableUsage> unreportedUsages;
  private Thread reportThread_;

  private ImpaladTableUsageTracker(boolean enabled) {
    if (!enabled) return;
    unreportedUsages = new HashMap<>();
    reportThread_ = new Thread(new Runnable() {
      @Override
      public void run() {
        report();
      }
    });
    reportThread_.setDaemon(true);
    reportThread_.setName("ImpaladTableUsageTracker daemon thread");
    reportThread_.start();
  }

  public static ImpaladTableUsageTracker createFromConfig(BackendConfig config) {
    final boolean invalidateTableOnMemoryPressure =
        config.invalidateTablesOnMemoryPressure();
    final int unusedTableTtlSec = config.getInvalidateTablesTimeoutS();
    Preconditions.checkArgument(unusedTableTtlSec >= 0,
        "unused_table_ttl_sec flag must be a non-negative integer.");
    return new ImpaladTableUsageTracker(
        unusedTableTtlSec > 0 || invalidateTableOnMemoryPressure);
  }

  /**
   * Report used table names asynchronously. This might be called even if automatic
   * invalidation is disabled, but in that case, it will be a no-op.
   */
  public synchronized void recordTableUsage(Collection<TableName> tableNames) {
    if (reportThread_ == null) return;
    for (TableName tableName : tableNames) {
      TTableName tTableName = tableName.toThrift();
      if (unreportedUsages.containsKey(tTableName)) {
        unreportedUsages.get(tTableName).num_usages++;
      } else {
        unreportedUsages.put(tTableName, new TTableUsage(tTableName, 1));
      }
    }
    notify();
  }

  private void report() {
    Random random = new Random();
    String updateFailureMessage =
        "Unable to report table usage information to catalog server. ";
    while (true) {
      try {
        // Randomly sleep for [0.5, 1.5) * REPORT_INTERVAL_MS, to avoid flooding catalogd.
        Thread.sleep((long) (REPORT_INTERVAL_MS * (0.5 + random.nextDouble())));
        TUpdateTableUsageRequest reqToSend;
        synchronized (this) {
          if (unreportedUsages.isEmpty()) continue;
          reqToSend = new TUpdateTableUsageRequest();
          reqToSend.setUsages(new ArrayList<>(unreportedUsages.values()));
          unreportedUsages.clear();
        }
        byte[] byteResp =
            FeSupport.NativeUpdateTableUsage(new TSerializer().serialize(reqToSend));
        TUpdateTableUsageResponse resp = new TUpdateTableUsageResponse();
        JniUtil.deserializeThrift(new TBinaryProtocol.Factory(), resp, byteResp);
        if (resp.isSetStatus() && !resp.status.status_code.equals(TErrorCode.OK)) {
          LOG.warn(
              updateFailureMessage + Joiner.on("\n").join(resp.status.getError_msgs()));
        }
      } catch (Exception e) {
        LOG.warn(updateFailureMessage, e);
      }
    }
  }
}
