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

package org.apache.impala.service;

import java.util.List;
import java.util.Map;

import org.apache.impala.planner.CanonicalizationStrategy;
import org.apache.impala.thrift.TCanonicalizationStrategy;
import org.apache.impala.thrift.THboStatsType;
import org.apache.impala.thrift.THistoricalStatsUpdate;
import org.apache.impala.thrift.TPlanNodeRun;
import org.apache.impala.thrift.TPlanNodeRunWithKeys;
import org.apache.impala.thrift.TScanInputStats;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;

public class HistoricalStats {
  private final static Logger LOG = LoggerFactory.getLogger(HistoricalStats.class);
  public static HistoricalStats INSTANCE = new HistoricalStats();
  private final CacheBackend cacheBackend_;
  private final double similarityThreshold_;
  private final int maxRunsPerKey_;

  private HistoricalStats() {
    int concurrencyLevel;
    long cacheSizeBytes;
    if (BackendConfig.INSTANCE != null) {
      concurrencyLevel = BackendConfig.INSTANCE.getUnregistrationThreadPoolSize();
      cacheSizeBytes = BackendConfig.INSTANCE.getHboInMemoryBackendCacheSizeBytes();
      similarityThreshold_ = BackendConfig.INSTANCE.getHboSimilarityThreshold();
      maxRunsPerKey_ = BackendConfig.INSTANCE.getHboMaxRunsPerKey();
    } else {
      // BackendConfig.INSTANCE could be null in tests.
      concurrencyLevel = 4;
      cacheSizeBytes = 1024L * 1024 * 1024;
      similarityThreshold_ = 0.1;
      maxRunsPerKey_ = 100;
    }
    cacheBackend_ = new InMemoryCacheBackend(concurrencyLevel, cacheSizeBytes);
  }

  private boolean exceedsThreshold(long curr, long hist) {
    return curr < hist * (1 - similarityThreshold_)
        || curr > hist * (1 + similarityThreshold_);
  }

  /**
   * Returns true if {@code currRun} and {@code histRun} are similar enough to share HBO
   * stats. Each scan-node input is compared independently:
   *   - If both inputs have valid input_rows (>= 0), compare row counts.
   *   - Otherwise (either input_rows is missing):
   *       - For EXPR_REWRITE, an exact catalog version match makes the pair similar;
   *         if catalog versions differ, compare input file sizes.
   *       - For other strategies, catalog version is not a reliable signal, so compare
   *         input file sizes only.
   * A run matches only if every scan-node pair matches. Note that the hash key matching
   * already ensures table names and conjuncts are matched.
   */
  private boolean scanInputStatsMatch(TPlanNodeRun currRun, TPlanNodeRun histRun,
      CanonicalizationStrategy strategy) {
    List<TScanInputStats> currStats = currRun.getScan_input_stats();
    List<TScanInputStats> histStats = histRun.getScan_input_stats();
    // UnionNode with only constant operands and its ancestor nodes have no scans.
    if (!currRun.isSetScan_input_stats() || currStats.isEmpty()) {
      return !histRun.isSetScan_input_stats() || histStats.isEmpty();
    }
    if (currStats.size() != histStats.size()) return false;
    for (int i = 0; i < currStats.size(); i++) {
      TScanInputStats sc = currStats.get(i);
      TScanInputStats sh = histStats.get(i);
      long currRows = sc.getInput_rows();
      long histRows = sh.getInput_rows();
      if (currRows >= 0 && histRows >= 0) {
        if (exceedsThreshold(currRows, histRows)) return false;
      } else {
        // At least one side is missing input_rows. For EXPR_REWRITE, compare the catalog
        // versions first. Note that for other more aggressive strategies it's unsafe to
        // depend on catalog versions. E.g. IGNORE_PARTITION_CONSTANTS maps "year=2009"
        // and "year=2019" to the same partition predicate "year=<CONST>". But "year=2019"
        // picks no files in the functional.alltypes table. We should depend on the total
        // input file size for such cases.
        if (strategy == CanonicalizationStrategy.EXPR_REWRITE
            && sc.isSetCatalog_version() && sh.isSetCatalog_version()
            && sc.getCatalog_version() == sh.getCatalog_version()) {
          continue;
        }
        // In the future input_file_size could be unset, e.g. for KuduScanNode.
        // Currently we just support HdfsScanNode so these should be set.
        Preconditions.checkState(sc.isSetInput_file_size());
        Preconditions.checkState(sh.isSetInput_file_size());
        if (exceedsThreshold(sc.getInput_file_size(), sh.getInput_file_size())) {
          return false;
        }
      }
    }
    return true;
  }

  private int getSimilarRunIndex(List<TPlanNodeRun> runs, TPlanNodeRun currRun,
      CanonicalizationStrategy strategy) {
    for (int i = 0; i < runs.size(); i++) {
      if (scanInputStatsMatch(currRun, runs.get(i), strategy)) return i;
    }
    return -1;
  }

  public void writeStats(THistoricalStatsUpdate stats) {
    for (TPlanNodeRunWithKeys runWithKeys : stats.plan_node_runs) {
      writePlanNodeStats(runWithKeys);
    }
  }

  public void writePlanNodeStats(TPlanNodeRunWithKeys runWithKeys) {
    TPlanNodeRun currRun = runWithKeys.run;
    THboStatsType statsType = runWithKeys.stats_type;
    // TODO: handle races from concurrent writers.
    for (Map.Entry<TCanonicalizationStrategy, String> entry :
        runWithKeys.hash_keys.entrySet()) {
      CanonicalizationStrategy strategy =
          CanonicalizationStrategy.fromThrift(entry.getKey());
      String hashKey = entry.getValue();
      @SuppressWarnings("unchecked")
      HistoricalStatsValue<TPlanNodeRun> statsValue =
          (HistoricalStatsValue<TPlanNodeRun>) cacheBackend_.getIfPresent(
              statsType, hashKey);
      if (statsValue == null) {
        cacheBackend_.put(statsType, hashKey, new HistoricalStatsValue<>(currRun));
      } else {
        List<TPlanNodeRun> runs = statsValue.getRuns();
        int similarRunIndex = getSimilarRunIndex(runs, currRun, strategy);
        if (similarRunIndex >= 0) {
          // Remove the similar one since we are adding a newer run.
          runs.remove(similarRunIndex);
        }
        if (runs.size() >= maxRunsPerKey_) {
          // Remove the oldest run since we are at the limit.
          runs.remove(0);
        }
        runs.add(currRun);
        cacheBackend_.put(statsType, hashKey, statsValue);
      }
      LOG.debug("Wrote HBO key: {}, stats type: {}, stats: {}",
          hashKey, statsType, currRun);
    }
  }

  /**
   * Retrieves the number of output rows from historical stats, trying multiple hash keys
   * in order from most accurate to most aggressive canonicalization strategy.
   * Returns the first match found, or null if no match exists.
   *
   * @param hashKeys HBO hash strings keyed by canonicalization strategy.
   * @param node display string for the PlanNode. Only used in logging.
   * @param currRun scan input stats for the current run.
   * @return Number of rows from matched historical run, or null if no match.
   */
  public Long getPlanNodeOutputRows(Map<CanonicalizationStrategy, String> hashKeys,
      String node, TPlanNodeRun currRun) {
    for (CanonicalizationStrategy strategy : CanonicalizationStrategy.values()) {
      String hashKey = hashKeys.get(strategy);
      if (hashKey == null) continue;
      @SuppressWarnings("unchecked")
      HistoricalStatsValue<TPlanNodeRun> statsValue =
          (HistoricalStatsValue<TPlanNodeRun>) cacheBackend_.getIfPresent(
              THboStatsType.CARDINALITY, hashKey);
      if (statsValue != null) {
        List<TPlanNodeRun> runs = statsValue.getRuns();
        int similarRunIndex = getSimilarRunIndex(runs, currRun, strategy);
        if (similarRunIndex >= 0) {
          LOG.debug("HBO cache hit for {} using strategy {} (key: {}, currRun: {}):"
                  + "cardinality={}",
              node, strategy, hashKey, currRun,
              runs.get(similarRunIndex).getNum_rows());
          return runs.get(similarRunIndex).getNum_rows();
        } else {
          LOG.debug("HBO cache miss for {} using strategy {} (key: {}, "
                  + "scanInputRows: {}). No similar run",
              node, strategy, hashKey, currRun);
        }
      } else {
        LOG.debug("HBO cache miss for {} using strategy {} (key: {}, scanInputRows:"
                + " {}). Hash key not found",
            node, strategy, hashKey, currRun);
      }
    }
    return null;
  }

  public String getCacheStats() {
    return cacheBackend_.getStats();
  }
}
