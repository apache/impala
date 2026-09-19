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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import org.apache.hadoop.hive.metastore.api.ColumnStatisticsData;
import org.apache.hadoop.hive.metastore.api.LongColumnStatsData;
import org.junit.Assume;
import org.junit.Test;

public class ColumnStatsTest {
  private static final long NUM_NULLS = 3;
  private static final long NUM_DVS = 17;

  private void verifyTimestampStats(ColumnStatisticsData statsData) {
    ColumnStats stats = new ColumnStats(Type.TIMESTAMP);
    assertTrue(stats.update("timestamp_col", Type.TIMESTAMP, statsData));
    assertEquals(NUM_NULLS, stats.getNumNulls());
    assertEquals(NUM_DVS, stats.getNumDistinctValues());
  }

  @Test
  public void testTimestampLongStatsCompatibility() {
    ColumnStatisticsData longStatsData = new ColumnStatisticsData();
    longStatsData.setLongStats(new LongColumnStatsData(NUM_NULLS, NUM_DVS));
    verifyTimestampStats(longStatsData);
  }

  @Test
  public void testHive4TimestampStatsCompatibility() throws Exception {
    Class<?> timestampStatsClass;
    try {
      timestampStatsClass = Class.forName(
          "org.apache.hadoop.hive.metastore.api.TimestampColumnStatsData");
    } catch (ClassNotFoundException e) {
      // Apache Hive 2 and 3 do not have the timestampStats union field.
      Assume.assumeNoException(e);
      return;
    }
    Object timestampStats = timestampStatsClass.getConstructor().newInstance();
    timestampStatsClass.getMethod("setNumNulls", long.class)
        .invoke(timestampStats, NUM_NULLS);
    timestampStatsClass.getMethod("setNumDVs", long.class)
        .invoke(timestampStats, NUM_DVS);

    ColumnStatisticsData statsData = new ColumnStatisticsData();
    ColumnStatisticsData.class
        .getMethod("setTimestampStats", timestampStatsClass)
        .invoke(statsData, timestampStats);
    verifyTimestampStats(statsData);
  }
}
