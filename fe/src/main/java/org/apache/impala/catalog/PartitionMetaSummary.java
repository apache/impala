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

import com.google.common.collect.Iterables;
import org.apache.impala.service.BackendConfig;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

/**
 * Helper class to get a summary for partition updates of a table when logging topic
 * updates. We log each topic item except partition items since they are too many.
 * Catalogd and impalad use this to log a summary of the partition items. See some
 * examples in PartitionMetaSummaryTest.
 */
public class PartitionMetaSummary {
  private static final String ORIGINAL_SIZE_METRIC = "original-size";
  private static final String ACTUAL_SIZE_METRIC = "actual-size";
  private static final String V1 = "v1";
  private static final String V2 = "v2";

  // Whether we are used in catalogd. If true, toString() will return info about catalog
  // modes (v1/v2).
  private final boolean inCatalogd_;
  private final String fullTableName_;

  // Whether we have v1 updates. Set in update().
  private final boolean hasV1Updates_;
  // Whether we have v2 updates. Set in update().
  private final boolean hasV2Updates_;

  // Number of total updates. If there are both v1 and v2 updates, only counts on v1.
  private int numUpdatedParts_;
  // Number of total deletes. If there are both v1 and v2 deletes, only counts on v1.
  private int numDeletedParts_;
  // Stores the lexicographically smallest, second smallest and the largest partition
  // names. Number of the updated partitions could be more than 100K, so we just keep
  // three of them. For easily picking up the three values together, we use an array with
  // 3 items instead of 3 individual vars.
  private final String[] updatedPartNames_ = new String[3];
  // Same as above but for deleted partitions.
  private final String[] deletedPartNames_ = new String[3];
  // All update versions we've seen. In case the impalad is processing multiple updates of
  // a table at once (e.g. after restart), there could be several versions.
  private final Set<Long> updateVersions_ = new TreeSet<>();
  // Same as above but for deletions.
  private final Set<Long> deleteVersions_ = new TreeSet<>();

  // Aggregated metrics of sizes of partitions, i.e. min, max, avg, sum of the original
  // and compressed sizes.
  private final Map<String, Map<String, Metrics>> updateMetrics_ = new HashMap<>();
  private final Map<String, Map<String, Metrics>> deleteMetrics_ = new HashMap<>();

  public PartitionMetaSummary(String fullTableName, boolean inCatalogd,
      boolean hasV1Updates, boolean hasV2Updates) {
    fullTableName_ = fullTableName;
    inCatalogd_ = inCatalogd;
    hasV1Updates_ = hasV1Updates;
    hasV2Updates_ = hasV2Updates;
    updateMetrics_.put(V1, newMetrics());
    updateMetrics_.put(V2, newMetrics());
    deleteMetrics_.put(V1, newMetrics());
    deleteMetrics_.put(V2, newMetrics());
  }

  private Map<String, Metrics> newMetrics() {
    Map<String, Metrics> res = new HashMap<>();
    res.put(ORIGINAL_SIZE_METRIC, new Metrics());
    res.put(ACTUAL_SIZE_METRIC, new Metrics());
    return res;
  }

  /**
   * Collects basic info of a partition level topic item.
   */
  public void update(boolean isV1Key, boolean delete, String partName, long version,
      int originalSize, int actualSize) {
    if (delete) {
      deleteVersions_.add(version);
    } else {
      updateVersions_.add(version);
    }

    // Update metrics
    Map<String, Metrics> metrics = delete ?
        deleteMetrics_.get(isV1Key ? V1 : V2) :
        updateMetrics_.get(isV1Key ? V1 : V2);
    metrics.get(ORIGINAL_SIZE_METRIC).update(originalSize);
    metrics.get(ACTUAL_SIZE_METRIC).update(actualSize);

    // Processing the partition name. Skip processing v2 partition names if we are
    // processing v1 keys to avoid duplication.
    if (!isV1Key && hasV1Updates_) return;
    boolean isFirst;
    String[] partNames;
    if (delete) {
      isFirst = (numDeletedParts_ == 0);
      numDeletedParts_++;
      partNames = deletedPartNames_;
    } else {
      isFirst = (numUpdatedParts_ == 0);
      numUpdatedParts_++;
      partNames = updatedPartNames_;
    }
    // Updates the partition names array. partNames[0] is the lexicographically smallest
    // one. partNames[1] is the second smallest one. partNames[2] is the largest one.
    if (isFirst) {
      // Init the lexicographically smallest and largest one as the first partition name.
      partNames[0] = partName;
      partNames[1] = null;
      partNames[2] = partName;
    } else {
      // Update the lexicographically smallest and second smallest partition name.
      if (partName.compareTo(partNames[0]) < 0) {
        // 'partName' is smaller than the current smallest one. Shift partNames[0] to
        // partNames[1] and update partNames[0].
        partNames[1] = partNames[0];
        partNames[0] = partName;
      } else if (partNames[1] == null || partName.compareTo(partNames[1]) < 0) {
        partNames[1] = partName;
      }
      // Update the lexicographically largest partition name.
      if (partNames[2].compareTo(partName) < 0) partNames[2] = partName;
    }
  }

  private void appendSummary(String mode, boolean isDelete, StringBuilder res) {
    int numParts;
    String[] partNames;
    Map<String, Metrics> metrics;
    Set<Long> versions;
    if (isDelete) {
      numParts = numDeletedParts_;
      partNames = deletedPartNames_;
      metrics = deleteMetrics_.get(mode);
      versions = deleteVersions_;
    } else {
      numParts = numUpdatedParts_;
      partNames = updatedPartNames_;
      metrics = updateMetrics_.get(mode);
      versions = updateVersions_;
    }
    if (numParts == 0) return;
    if (res.length() > 0) res.append("\n");
    if (inCatalogd_) {
      res.append(String.format("Collected %d partition %s(s): ",
          numParts, isDelete ? "deletion" : "update"));
      res.append(V1.equals(mode) ? "1:" : "2:");
    } else {
      res.append(isDelete ? "Deleting " : "Adding ").append(numParts)
          .append(" partition(s): ");
    }
    res.append("HDFS_PARTITION:").append(fullTableName_).append(":");
    if (numParts > 1) res.append("(");
    res.append(partNames[0]);
    if (numParts > 1) res.append(",").append(partNames[1]);
    if (numParts > 3) res.append(",...");
    if (numParts > 2) res.append(",").append(partNames[2]);
    if (numParts > 1) res.append(")");
    if (versions.size() == 1) {
      res.append(", version=").append(Iterables.getOnlyElement(versions));
    } else {
      res.append(", versions=").append(versions);
    }
    res.append(inCatalogd_ ? ", original size=" : ", size=")
        .append(metrics.get(ORIGINAL_SIZE_METRIC));
    // Compressed sizes are not collected in impalad.
    if (inCatalogd_ && BackendConfig.INSTANCE.isCompactCatalogTopic()) {
      res.append(", compressed size=").append(metrics.get(ACTUAL_SIZE_METRIC));
    }
  }

  /**
   * Returns whether we have collected any partition updates/deletes.
   */
  public boolean hasUpdates() { return numUpdatedParts_ > 0 || numDeletedParts_ > 0; }

  @Override
  public String toString() {
    StringBuilder res = new StringBuilder();
    if (hasV1Updates_) appendSummary(V1, false, res);
    if (hasV2Updates_) appendSummary(V2, false, res);
    if (hasV1Updates_) appendSummary(V1, true, res);
    if (hasV2Updates_) appendSummary(V2, true, res);
    return res.toString();
  }

  /**
   * Metrics that are used in logging. Only aggregated values are stored.
   */
  static class Metrics {
    private long min, max, sum, count;

    public void update(int value) {
      if (count == 0) {
        min = max = sum = value;
      } else {
        sum += value;
        max = Math.max(max, value);
        min = Math.min(min, value);
      }
      count++;
    }

    public long getMean() { return (long) (sum * 1.0 / count); }
    public long getMin() { return min; }
    public long getMax() { return max; }
    public long getSum() { return sum; }
    public long getCount() { return count; }

    @Override
    public String toString() {
      if (count == 0) return "";
      if (count == 1) return Long.toString(sum);
      return String.format("(avg=%d, min=%d, max=%d, sum=%d)", getMean(), min,
          max, sum);
    }
  }
}
