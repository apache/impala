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

import org.apache.impala.service.FeSupport;
import org.junit.BeforeClass;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class PartitionMetaSummaryTest {

  @BeforeClass
  public static void setUp() {
    FeSupport.loadLibrary();
  }

  @Test
  public void testSortingInCatalogd() {
    PartitionMetaSummary summary = new PartitionMetaSummary("test_db.test_tbl", true,
        true, false);
    summary.update(true, false, "p=1", 123, 100, 10);
    summary.update(true, false, "p=4", 123, 100, 10);
    summary.update(true, false, "p=3", 123, 100, 10);
    summary.update(true, false, "p=5", 123, 100, 10);
    summary.update(true, false, "p=2", 123, 100, 10);
    assertEquals("Collected 5 partition update(s): " +
        "1:HDFS_PARTITION:test_db.test_tbl:(p=1,p=2,...,p=5), version=123, " +
        "original size=(avg=100, min=100, max=100, sum=500), " +
        "compressed size=(avg=10, min=10, max=10, sum=50)", summary.toString());
  }

  @Test
  public void testSorting2InCatalogd() {
    PartitionMetaSummary summary = new PartitionMetaSummary("test_db.test_tbl", true,
        false, true);
    summary.update(false, false, "p=5", 123, 100, 10);
    summary.update(false, false, "p=4", 123, 100, 10);
    summary.update(false, false, "p=3", 123, 100, 10);
    summary.update(false, false, "p=2", 123, 100, 10);
    summary.update(false, false, "p=1", 123, 100, 10);
    assertEquals("Collected 5 partition update(s): " +
        "2:HDFS_PARTITION:test_db.test_tbl:(p=1,p=2,...,p=5), version=123, " +
        "original size=(avg=100, min=100, max=100, sum=500), " +
        "compressed size=(avg=10, min=10, max=10, sum=50)", summary.toString());
  }

  @Test
  public void testMixModeInCatalogd() {
    PartitionMetaSummary summary = new PartitionMetaSummary("test_db.test_tbl", true,
        true, true);
    summary.update(true, false, "p=2", 123, 100, 10);
    summary.update(false, false, "p=2", 123, 10, 10);
    summary.update(true, false, "p=4", 123, 100, 10);
    summary.update(false, false, "p=4", 123, 10, 10);
    summary.update(true, false, "p=3", 123, 100, 10);
    summary.update(false, false, "p=3", 123, 10, 10);
    summary.update(true, true, "p=0", 121, 100, 10);
    summary.update(false, true, "p=0", 121, 10, 10);
    summary.update(true, true, "p=1", 122, 100, 10);
    summary.update(false, true, "p=1", 122, 10, 10);
    assertEquals("Collected 3 partition update(s): " +
        "1:HDFS_PARTITION:test_db.test_tbl:(p=2,p=3,p=4), version=123, " +
        "original size=(avg=100, min=100, max=100, sum=300), " +
        "compressed size=(avg=10, min=10, max=10, sum=30)\n" +
        "Collected 3 partition update(s): " +
        "2:HDFS_PARTITION:test_db.test_tbl:(p=2,p=3,p=4), version=123, " +
        "original size=(avg=10, min=10, max=10, sum=30), " +
        "compressed size=(avg=10, min=10, max=10, sum=30)\n" +
        "Collected 2 partition deletion(s): " +
        "1:HDFS_PARTITION:test_db.test_tbl:(p=0,p=1), versions=[121, 122], " +
        "original size=(avg=100, min=100, max=100, sum=200), " +
        "compressed size=(avg=10, min=10, max=10, sum=20)\n" +
        "Collected 2 partition deletion(s): " +
        "2:HDFS_PARTITION:test_db.test_tbl:(p=0,p=1), versions=[121, 122], " +
        "original size=(avg=10, min=10, max=10, sum=20), " +
        "compressed size=(avg=10, min=10, max=10, sum=20)", summary.toString());
  }

  @Test
  public void testUpdatesAndDeletesInCatalogd() {
    PartitionMetaSummary summary = new PartitionMetaSummary("test_db.test_tbl", true,
        true, false);
    summary.update(true, false, "p=1", 123, 100, 10);
    summary.update(true, false, "p=2", 123, 100, 10);
    summary.update(true, false, "p=3", 123, 100, 10);
    summary.update(true, false, "p=4", 123, 100, 10);
    summary.update(true, false, "p=5", 123, 100, 10);
    summary.update(true, true, "p=0", 200, 100, 10);
    assertEquals("Collected 5 partition update(s): " +
        "1:HDFS_PARTITION:test_db.test_tbl:(p=1,p=2,...,p=5), version=123, " +
        "original size=(avg=100, min=100, max=100, sum=500), " +
        "compressed size=(avg=10, min=10, max=10, sum=50)\n" +
        "Collected 1 partition deletion(s): 1:HDFS_PARTITION:test_db.test_tbl:p=0, " +
        "version=200, original size=100, compressed size=10", summary.toString());
  }

  @Test
  public void testSortingInImpalad() {
    PartitionMetaSummary summary = new PartitionMetaSummary("test_db.test_tbl", false,
        true, false);
    summary.update(true, false, "p=1", 123, 100, 10);
    summary.update(true, false, "p=2", 123, 100, 10);
    summary.update(true, false, "p=4", 123, 100, 10);
    summary.update(true, false, "p=3", 123, 100, 10);
    summary.update(true, true, "p=0", 200, 100, 10);
    assertEquals("Adding 4 partition(s): " +
        "HDFS_PARTITION:test_db.test_tbl:(p=1,p=2,...,p=4), version=123, " +
        "size=(avg=100, min=100, max=100, sum=400)\n" +
        "Deleting 1 partition(s): HDFS_PARTITION:test_db.test_tbl:p=0, " +
        "version=200, size=100", summary.toString());
  }
}
