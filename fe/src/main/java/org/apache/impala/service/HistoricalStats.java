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

  private boolean catalogVersionMatches(TPlanNodeRun a, TPlanNodeRun b) {
    if (!a.isSetScan_input_stats() || !b.isSetScan_input_stats()) {
      return false;
    }
    if (a.getScan_input_stats().size() != b.getScan_input_stats().size()) {
      return false;
    }
    for (int i = 0; i < a.getScan_input_stats().size(); i++) {
      TScanInputStats sa = a.getScan_input_stats().get(i);
      TScanInputStats sb = b.getScan_input_stats().get(i);
      if (!(sa.isSetCatalog_version() && sb.isSetCatalog_version()
          && sa.getCatalog_version() == sb.getCatalog_version())) {
        return false;
      }
    }
    return true;
  }

  private boolean scanInputSizeMatches(TPlanNodeRun a, TPlanNodeRun b) {
    if (!a.isSetScan_input_stats() || !b.isSetScan_input_stats()) {
      return false;
    }
    if (a.getScan_input_stats().isEmpty() || b.getScan_input_stats().isEmpty()) {
      return false;
    }
    TScanInputStats sa = a.getScan_input_stats().get(0);
    TScanInputStats sb = b.getScan_input_stats().get(0);
    if (!sa.isSetInput_file_size() || !sb.isSetInput_file_size()) return false;
    long x = sa.getInput_file_size();
    long y = sb.getInput_file_size();
    if (x == 0) return y == 0;
    return Math.abs(x - y) / (double)x < similarityThreshold_;
  }

  private int getSimilarRunIndexWithoutStats(List<TPlanNodeRun> runs,
      TPlanNodeRun currRun) {
    if (currRun.isSetScan_input_stats()) {
      for (TScanInputStats s : currRun.getScan_input_stats()) {
        Preconditions.checkState(s.isSetInput_rows(),
            "exactMatch is only used when missing numRows but input_rows unset");
        Preconditions.checkState(s.getInput_rows() < 0,
            "exactMatch is only used when missing numRows but got %s", s.getInput_rows());
      }
    }
    // For exact match, first find a run with the exact catalog version. If missing,
    // find a run with the similar scan input size. Note that the hash key matching
    // already ensures conjuncts are the same.
    int sizeMatchIndex = -1;
    for (int i = 0; i < runs.size(); i++) {
      TPlanNodeRun run = runs.get(i);
      if (catalogVersionMatches(run, currRun)) return i;
      if (scanInputSizeMatches(run, currRun)) sizeMatchIndex = i;
    }
    return sizeMatchIndex;
  }

  private int getSimilarRunIndexWithNumRows(List<TPlanNodeRun> runs,
      TPlanNodeRun currRun) {
    Preconditions.checkState(currRun.isSetScan_input_stats()
        && !currRun.getScan_input_stats().isEmpty());
    for (int i = 0; i < runs.size(); i++) {
      TPlanNodeRun run = runs.get(i);
      // Currently HBO only supports HdfsScanNode which just has one table thus only one
      // scan_input_stat. We just need to compare the first one.
      Preconditions.checkState(run.isSetScan_input_stats()
          && !run.getScan_input_stats().isEmpty());
      long curr = currRun.getScan_input_stats().get(0).getInput_rows();
      long historical = run.getScan_input_stats().get(0).getInput_rows();
      if (curr == 0) {
        if (historical == 0) {
          return i;
        }
      } else if (Math.abs(historical - curr) / (double)curr < similarityThreshold_) {
        return i;
      }
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
    for (String hashKey : runWithKeys.hash_keys.values()) {
      @SuppressWarnings("unchecked")
      HistoricalStatsValue<TPlanNodeRun> statsValue =
          (HistoricalStatsValue<TPlanNodeRun>) cacheBackend_.getIfPresent(
              statsType, hashKey);
      if (statsValue == null) {
        cacheBackend_.put(statsType, hashKey, new HistoricalStatsValue<>(currRun));
      } else {
        List<TPlanNodeRun> runs = statsValue.getRuns();
        int similarRunIndex = getSimilarRunIndexWithNumRows(runs, currRun);
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
   * @param hashKeys HBO hash strings keyed by canonicalization strategy
   * @return Number of rows from matched historical run, or null if no match
   */
  public Long getPlanNodeOutputRows(Map<CanonicalizationStrategy, String> hashKeys,
      String tblName, TPlanNodeRun currRun) {
    Preconditions.checkNotNull(currRun.getScan_input_stats());
    Preconditions.checkArgument(!currRun.getScan_input_stats().isEmpty());
    boolean missingStats = currRun.getScan_input_stats().stream()
        .anyMatch(s -> s.getInput_rows() < 0);
    for (CanonicalizationStrategy strategy : CanonicalizationStrategy.values()) {
      String hashKey = hashKeys.get(strategy);
      if (hashKey == null) continue;
      // If scanInputRows is unknown, only allow exact match, i.e. EXPR_REWRITE strategy
      // with catalog version match.
      if (missingStats && strategy != CanonicalizationStrategy.EXPR_REWRITE) break;
      @SuppressWarnings("unchecked")
      HistoricalStatsValue<TPlanNodeRun> statsValue =
          (HistoricalStatsValue<TPlanNodeRun>) cacheBackend_.getIfPresent(
              THboStatsType.CARDINALITY, hashKey);
      if (statsValue != null) {
        List<TPlanNodeRun> runs = statsValue.getRuns();
        int similarRunIndex;
        if (missingStats && strategy == CanonicalizationStrategy.EXPR_REWRITE) {
          similarRunIndex = getSimilarRunIndexWithoutStats(runs, currRun);
        } else {
          similarRunIndex = getSimilarRunIndexWithNumRows(runs, currRun);
        }
        if (similarRunIndex >= 0) {
          // TODO: Consider moving this to the tail.
          LOG.debug("HBO cache hit for {} using strategy {} (key: {}, currRun: {}):"
                  + "cardinality={}",
              tblName, strategy, hashKey, currRun,
              runs.get(similarRunIndex).getNum_rows());
          return runs.get(similarRunIndex).getNum_rows();
        } else {
          LOG.debug("HBO cache miss for {} using strategy {} (key: {}, "
                  + "scanInputRows: {}). No similar run",
              tblName, strategy, hashKey, currRun);
        }
      } else {
        LOG.debug("HBO cache miss for {} using strategy {} (key: {}, scanInputRows:"
                + " {}). Hash key not found",
            tblName, strategy, hashKey, currRun);
      }
    }
    return null;
  }

  public String getCacheStats() {
    return cacheBackend_.getStats();
  }
}
