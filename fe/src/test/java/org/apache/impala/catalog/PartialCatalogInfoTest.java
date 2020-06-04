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
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.fail;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.hadoop.hive.metastore.api.ColumnStatisticsObj;
import org.apache.hadoop.hive.metastore.api.SQLForeignKey;
import org.apache.hadoop.hive.metastore.api.SQLPrimaryKey;
import org.apache.hadoop.hive.serde.serdeConstants;
import org.apache.impala.common.InternalException;
import org.apache.impala.service.BackendConfig;
import org.apache.impala.testutil.CatalogServiceTestCatalog;
import org.apache.impala.thrift.CatalogLookupStatus;
import org.apache.impala.thrift.TBriefTableMeta;
import org.apache.impala.thrift.TCatalogInfoSelector;
import org.apache.impala.thrift.TCatalogObject;
import org.apache.impala.thrift.TCatalogObjectType;
import org.apache.impala.thrift.TDatabase;
import org.apache.impala.thrift.TDbInfoSelector;
import org.apache.impala.thrift.TGetPartialCatalogObjectRequest;
import org.apache.impala.thrift.TGetPartialCatalogObjectResponse;
import org.apache.impala.thrift.THdfsFileFormat;
import org.apache.impala.thrift.TPartialPartitionInfo;
import org.apache.impala.thrift.TTable;
import org.apache.impala.thrift.TTableInfoSelector;
import org.apache.thrift.TDeserializer;
import org.apache.thrift.TException;
import org.apache.thrift.TSerializer;
import org.junit.AfterClass;
import org.junit.Test;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;

public class PartialCatalogInfoTest {
  private static CatalogServiceCatalog catalog_ =
      CatalogServiceTestCatalog.create();

  @AfterClass
  public static void cleanUp() { catalog_.close(); }

  /**
   * A Callable wrapper around getPartialCatalogObject() call.
   */
  private class CallableGetPartialCatalogObjectRequest
      implements Callable<TGetPartialCatalogObjectResponse> {
    private final TGetPartialCatalogObjectRequest request_;

    CallableGetPartialCatalogObjectRequest(TGetPartialCatalogObjectRequest request) {
      request_ = request;
    }

    @Override
    public TGetPartialCatalogObjectResponse call() throws Exception {
      return sendRequest(request_);
    }
  }

  private TGetPartialCatalogObjectResponse sendRequest(
      TGetPartialCatalogObjectRequest req)
      throws CatalogException, InternalException, TException {
    TGetPartialCatalogObjectResponse resp;
    resp = catalog_.getPartialCatalogObject(req);
    // Round-trip the response through serialization, so if we accidentally forgot to
    // set the "isset" flag for any fields, we'll catch that bug.
    byte[] respBytes = new TSerializer().serialize(resp);
    resp.clear();
    new TDeserializer().deserialize(resp, respBytes);
    return resp;
  }

  /**
   * Sends the same 'request' from 'requestCount' threads in parallel and waits for
   * them to finish.
   */
  private void sendParallelRequests(TGetPartialCatalogObjectRequest request, int
      requestCount) throws Exception {
    Preconditions.checkState(requestCount > 0);
    final ExecutorService threadPoolExecutor = Executors.newFixedThreadPool(requestCount);
    final List<Future<TGetPartialCatalogObjectResponse>> tasksToWaitFor =
        new ArrayList<>();
    for (int i = 0; i < requestCount; ++i) {
      tasksToWaitFor.add(threadPoolExecutor.submit(new
          CallableGetPartialCatalogObjectRequest(request)));
    }
    for (Future<?> task: tasksToWaitFor) task.get();
  }

  @Test
  public void testDbList() throws Exception {
    TGetPartialCatalogObjectRequest req = new TGetPartialCatalogObjectRequest();
    req.object_desc = new TCatalogObject();
    req.object_desc.setType(TCatalogObjectType.CATALOG);
    req.catalog_info_selector = new TCatalogInfoSelector();
    req.catalog_info_selector.want_db_names = true;
    TGetPartialCatalogObjectResponse resp = sendRequest(req);
    assertTrue(resp.catalog_info.db_names.contains("functional"));
  }

  @Test
  public void testDb() throws Exception {
    TGetPartialCatalogObjectRequest req = new TGetPartialCatalogObjectRequest();
    req.object_desc = new TCatalogObject();
    req.object_desc.setType(TCatalogObjectType.DATABASE);
    req.object_desc.db = new TDatabase("functional");
    req.db_info_selector = new TDbInfoSelector();
    req.db_info_selector.want_hms_database = true;
    req.db_info_selector.want_brief_meta_of_tables = true;
    TGetPartialCatalogObjectResponse resp = sendRequest(req);
    assertTrue(resp.isSetObject_version_number());
    assertEquals(resp.db_info.hms_database.getName(), "functional");
    assertTrue(resp.db_info.brief_meta_of_tables.stream().map(TBriefTableMeta::getName)
        .anyMatch("alltypes"::equals));
  }

  @Test
  public void testTable() throws Exception {
    TGetPartialCatalogObjectRequest req = new TGetPartialCatalogObjectRequest();
    req.object_desc = new TCatalogObject();
    req.object_desc.setType(TCatalogObjectType.TABLE);
    req.object_desc.table = new TTable("functional", "alltypes");
    req.table_info_selector = new TTableInfoSelector();
    req.table_info_selector.want_hms_table = true;
    req.table_info_selector.want_partition_names = true;
    TGetPartialCatalogObjectResponse resp = sendRequest(req);
    assertTrue(resp.isSetObject_version_number());
    assertEquals(resp.table_info.hms_table.getTableName(), "alltypes");
    assertTrue(resp.table_info.partitions.size() > 0);
    TPartialPartitionInfo partInfo = resp.table_info.partitions.get(1);
    assertTrue("bad part name: " + partInfo.name,
        partInfo.name.matches("year=\\d+/month=\\d+"));

    // Fetch again, but specify two specific partitions and ask for metadata.
    req.table_info_selector.clear();
    req.table_info_selector.want_partition_metadata = true;
    req.table_info_selector.partition_ids = ImmutableList.of(
        resp.table_info.partitions.get(1).id,
        resp.table_info.partitions.get(3).id);
    resp = sendRequest(req);
    assertNull(resp.table_info.hms_table);
    assertEquals(2, resp.table_info.partitions.size());
    assertEquals(1, resp.table_info.getPartition_prefixesSize());
    assertEquals("hdfs://localhost:20500/test-warehouse/alltypes/",
        resp.table_info.partition_prefixes.get(0));
    partInfo = resp.table_info.partitions.get(0);
    assertNull(partInfo.name);
    assertEquals(req.table_info_selector.partition_ids.get(0), (Long)partInfo.id);
    assertEquals(0, partInfo.location.prefix_index);
    assertTrue("Bad suffix " + partInfo.location.suffix,
        partInfo.location.suffix.matches("year=\\d+/month=\\d+"));
    assertNotNull(partInfo.hdfs_storage_descriptor);
    assertEquals(THdfsFileFormat.TEXT, partInfo.hdfs_storage_descriptor.fileFormat);
  }

  @Test
  public void testFetchMissingPartId() throws Exception {
    TGetPartialCatalogObjectRequest req = new TGetPartialCatalogObjectRequest();
    req.object_desc = new TCatalogObject();
    req.object_desc.setType(TCatalogObjectType.TABLE);
    req.object_desc.table = new TTable("functional", "alltypes");
    req.table_info_selector = new TTableInfoSelector();
    req.table_info_selector.want_partition_metadata = true;
    req.table_info_selector.partition_ids = ImmutableList.of(-12345L); // non-existent
    TGetPartialCatalogObjectResponse resp = sendRequest(req);
    assertEquals(resp.lookup_status, CatalogLookupStatus.PARTITION_NOT_FOUND);
  }

  private List<ColumnStatisticsObj> fetchColumStats(String dbName, String tblName,
      ImmutableList<String> columns) throws Exception {
    TGetPartialCatalogObjectRequest req = new TGetPartialCatalogObjectRequest();
    req.object_desc = new TCatalogObject();
    req.object_desc.setType(TCatalogObjectType.TABLE);
    req.object_desc.table = new TTable(dbName, tblName);
    req.table_info_selector = new TTableInfoSelector();
    req.table_info_selector.want_stats_for_column_names = columns;
    TGetPartialCatalogObjectResponse resp = sendRequest(req);
    return resp.table_info.column_stats;
  }

  @Test
  public void testTableStats() throws Exception {
    List<ColumnStatisticsObj> stats = fetchColumStats(
        "functional", "alltypes", ImmutableList.of(
            "year", "month", "id", "bool_col", "tinyint_col", "smallint_col",
            "int_col", "bigint_col", "float_col", "double_col", "date_string_col",
            "string_col", "timestamp_col"));
    // We have 13 columns, but 2 are the clustering columns which don't have stats.
    assertEquals(11, stats.size());
    assertEquals("ColumnStatisticsObj(colName:id, colType:INT, " +
        "statsData:<ColumnStatisticsData longStats:LongColumnStatsData(" +
        "numNulls:0, numDVs:7300)>)", stats.get(0).toString());
  }

  @Test
  public void testDateTableStats() throws Exception {
    List<ColumnStatisticsObj> stats = fetchColumStats(
        "functional", "date_tbl",
        ImmutableList.of("date_col", "date_part"));
    // We have 2 columns, but 1 is the clustering column which doesn't have stats.
    assertEquals(1, stats.size());
    assertEquals("ColumnStatisticsObj(colName:date_col, colType:DATE, " +
        "statsData:<ColumnStatisticsData dateStats:DateColumnStatsData(" +
        "numNulls:2, numDVs:16)>)", stats.get(0).toString());
  }

  @Test
  public void testBinaryTableStats() throws Exception {
    List<ColumnStatisticsObj> stats = fetchColumStats(
        "functional", "binary_tbl", ImmutableList.of("binary_col"));
    assertEquals(1, stats.size());
    assertEquals("ColumnStatisticsObj(colName:binary_col, colType:BINARY, " +
        "statsData:<ColumnStatisticsData binaryStats:BinaryColumnStatsData(" +
        "maxColLen:26, avgColLen:8.714285850524902, numNulls:1)>)",
        stats.get(0).toString());
  }

  @Test
  public void testFetchErrorTable() throws Exception {
    TGetPartialCatalogObjectRequest req = new TGetPartialCatalogObjectRequest();
    req.object_desc = new TCatalogObject();
    req.object_desc.setType(TCatalogObjectType.TABLE);
    req.object_desc.table = new TTable("functional", "bad_serde");
    req.table_info_selector = new TTableInfoSelector();
    req.table_info_selector.want_hms_table = true;
    req.table_info_selector.want_partition_names = true;
    try {
      sendRequest(req);
      fail("expected exception");
    } catch (TableLoadingException tle) {
      assertEquals("Failed to load metadata for table: functional.bad_serde",
          tle.getMessage());
    }
  }

  @Test
  public void testGetSqlConstraints() throws Exception {
    // Test constraints of parent table.
    TGetPartialCatalogObjectRequest req = new TGetPartialCatalogObjectRequest();
    req.object_desc = new TCatalogObject();
    req.object_desc.setType(TCatalogObjectType.TABLE);
    req.object_desc.table = new TTable("functional", "parent_table");
    req.table_info_selector = new TTableInfoSelector();
    req.table_info_selector.want_hms_table = true;
    req.table_info_selector.want_table_constraints = true;

    TGetPartialCatalogObjectResponse resp = sendRequest(req);
    List<SQLPrimaryKey> primaryKeys = resp.table_info.sql_constraints.primary_keys;
    List<SQLForeignKey> foreignKeys = resp.table_info.sql_constraints.foreign_keys;

    assertEquals(2, primaryKeys.size());
    assertEquals(0, foreignKeys.size());
    for (SQLPrimaryKey pk: primaryKeys) {
      assertEquals("functional", pk.getTable_db());
      assertEquals("parent_table", pk.getTable_name());
    }
    assertEquals("id", primaryKeys.get(0).getColumn_name());
    assertEquals("year", primaryKeys.get(1).getColumn_name());

    // Test constraints of child_table.
    req = new TGetPartialCatalogObjectRequest();
    req.object_desc = new TCatalogObject();
    req.object_desc.setType(TCatalogObjectType.TABLE);
    req.object_desc.table = new TTable("functional", "child_table");
    req.table_info_selector = new TTableInfoSelector();
    req.table_info_selector.want_hms_table = true;
    req.table_info_selector.want_table_constraints = true;

    resp = sendRequest(req);
    primaryKeys = resp.table_info.sql_constraints.primary_keys;
    foreignKeys = resp.table_info.sql_constraints.foreign_keys;

    assertEquals(1, primaryKeys.size());
    assertEquals(3, foreignKeys.size());
    assertEquals("functional", primaryKeys.get(0).getTable_db());
    assertEquals("child_table",primaryKeys.get(0).getTable_name());
    for (SQLForeignKey fk : foreignKeys) {
      assertEquals("functional", fk.getFktable_db());
      assertEquals("child_table", fk.getFktable_name());
      assertEquals("functional", fk.getPktable_db());
    }
    assertEquals("parent_table", foreignKeys.get(0).getPktable_name());
    assertEquals("parent_table", foreignKeys.get(1).getPktable_name());
    assertEquals("parent_table_2", foreignKeys.get(2).getPktable_name());
    assertEquals("id", foreignKeys.get(0).getPkcolumn_name());
    assertEquals("year", foreignKeys.get(1).getPkcolumn_name());
    assertEquals("a", foreignKeys.get(2).getPkcolumn_name());
    // FK name for the composite primary key (id, year) should be equal.
    assertEquals(foreignKeys.get(0).getFk_name(), foreignKeys.get(1).getFk_name());

    // Check tables without constraints.
    req = new TGetPartialCatalogObjectRequest();
    req.object_desc = new TCatalogObject();
    req.object_desc.setType(TCatalogObjectType.TABLE);
    req.object_desc.table = new TTable("functional", "alltypes");
    req.table_info_selector = new TTableInfoSelector();
    req.table_info_selector.want_hms_table = true;
    req.table_info_selector.want_table_constraints = true;

    resp = sendRequest(req);
    primaryKeys = resp.table_info.sql_constraints.primary_keys;
    foreignKeys = resp.table_info.sql_constraints.foreign_keys;
    assertNotNull(primaryKeys);
    assertNotNull(foreignKeys);
    assertEquals(0, primaryKeys.size());
    assertEquals(0, foreignKeys.size());
  }

  @Test
  public void testConcurrentPartialObjectRequests() throws Exception {
    // Create a request.
    TGetPartialCatalogObjectRequest req = new TGetPartialCatalogObjectRequest();
    req.object_desc = new TCatalogObject();
    req.object_desc.setType(TCatalogObjectType.TABLE);
    req.object_desc.table = new TTable("functional", "alltypes");
    req.table_info_selector = new TTableInfoSelector();
    req.table_info_selector.want_hms_table = true;
    req.table_info_selector.want_partition_names = true;
    req.table_info_selector.want_partition_metadata = true;

    // Run 64 concurrent requests and run a tight loop in the background to make sure the
    // concurrent request count never exceeds 32 (--catalog_partial_rpc_max_parallel_runs)
    final AtomicBoolean requestsFinished = new AtomicBoolean(false);
    final int maxParallelRuns = BackendConfig.INSTANCE
        .getCatalogMaxParallelPartialFetchRpc();

    // Uses a callable<Void> instead of Runnable because junit does not catch exceptions
    // from threads other than the main thread. Callable here makes sure the exception
    // is propagated to the main thread.
    final Callable<Void> assertReqCount = new Callable<Void>() {
      @Override
      public Void call() throws Exception {
        while (!requestsFinished.get()) {
          int currentReqCount = catalog_.getConcurrentPartialRpcReqCount();
          assertTrue("Invalid concurrent request count: " + currentReqCount,
              currentReqCount <= maxParallelRuns);
        }
        return null;
      }
    };
    Future<Void> assertThreadTask;
    try {
      // Assert the request count in a tight loop.
      assertThreadTask = Executors.newSingleThreadExecutor().submit(assertReqCount);
      sendParallelRequests(req, 64);
    } finally {
      requestsFinished.set(true);
    }
    // 5 minutes is a reasonable timeout for this test. If timed out, an exception is
    // thrown.
    assertThreadTask.get(5, TimeUnit.MINUTES);
  }
}
