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
package org.apache.impala.catalog.events;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.collections.CollectionUtils;
import org.apache.hadoop.hive.common.ValidWriteIdList;
import org.apache.hadoop.hive.metastore.api.CurrentNotificationEventId;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.impala.catalog.HdfsTable;
import org.apache.impala.catalog.IncompleteTable;
import org.apache.impala.catalog.MetaStoreClientPool.MetaStoreClient;
import org.apache.impala.catalog.MetastoreApiTestUtils;
import org.apache.impala.catalog.Table;
import org.apache.impala.catalog.events.MetastoreEvents.AlterTableEvent;
import org.apache.impala.catalog.events.MetastoreEvents.MetastoreEvent;
import org.apache.impala.catalog.events.MetastoreEvents.MetastoreEventType;
import org.apache.impala.common.Metrics;
import org.apache.impala.compat.MetastoreShim;
import org.apache.impala.service.BackendConfig;
import org.apache.impala.service.CatalogOpExecutor;
import org.apache.impala.testutil.CatalogServiceTestCatalog;
import org.apache.thrift.TException;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

public class EventExecutorServiceTest {
  private static final String DB_NAME1 = "event_executor_service_test_db1";
  private static final String DB_NAME2 = "event_executor_service_test_db2";
  private static final int TEST_EVENT_PROCESSOR_IDLE_TIME_MS = 200;
  private static CatalogServiceTestCatalog catalog_;
  private static CatalogOpExecutor catalogOpExecutor_;
  private static MetastoreEventsProcessor eventsProcessor_;

  private static EventExecutorService eventExecutorService_;
  private static boolean prev_hierarchical_event_processing_;
  private static int prev_min_event_processor_idle_ms;

  @BeforeClass
  public static void setUpClass() throws Exception {
    catalog_ = CatalogServiceTestCatalog.create();
    catalogOpExecutor_ = catalog_.getCatalogOpExecutor();
    prev_hierarchical_event_processing_ =
        BackendConfig.INSTANCE.getBackendCfg().enable_hierarchical_event_processing;
    prev_min_event_processor_idle_ms =
        BackendConfig.INSTANCE.getBackendCfg().min_event_processor_idle_ms;
    BackendConfig.INSTANCE.getBackendCfg().enable_hierarchical_event_processing = true;
    BackendConfig.INSTANCE.getBackendCfg().min_event_processor_idle_ms =
        TEST_EVENT_PROCESSOR_IDLE_TIME_MS;
    try (MetaStoreClient msClient = catalog_.getMetaStoreClient()) {
      CurrentNotificationEventId currentNotificationId = msClient.getHiveClient()
          .getCurrentNotificationEventId();
      eventsProcessor_ = new SynchronousHMSEventProcessorForTests(catalogOpExecutor_,
          currentNotificationId.getEventId(), 1000L);
      eventsProcessor_.start();
    }
    eventExecutorService_ = eventsProcessor_.getEventExecutorService();
    assertNotNull(eventExecutorService_);
    catalog_.setMetastoreEventProcessor(eventsProcessor_);
  }

  @AfterClass
  public static void cleanUpClass() throws Exception {
    eventsProcessor_.setEventExecutorService(eventExecutorService_);
    dropDatabase(DB_NAME1);
    dropDatabase(DB_NAME2);
    eventsProcessor_.processEvents();
    eventsProcessor_.shutdown();
    BackendConfig.INSTANCE.getBackendCfg().enable_hierarchical_event_processing =
        prev_hierarchical_event_processing_;
    BackendConfig.INSTANCE.getBackendCfg().min_event_processor_idle_ms =
        prev_min_event_processor_idle_ms;
  }

  @Before
  public void setUp() throws Exception {
    // Ensure default EventExecutorService created within eventsProcessor_ is restored
    // before running new test
    eventsProcessor_.setEventExecutorService(eventExecutorService_);
    // Drop the databases if already present
    dropDatabase(DB_NAME1);
    dropDatabase(DB_NAME2);
    eventsProcessor_.processEvents();
    assertEquals(MetastoreEventsProcessor.EventProcessorStatus.ACTIVE,
        eventsProcessor_.getStatus());
  }

  @After
  public void cleanUp() {
    assertEquals(MetastoreEventsProcessor.EventProcessorStatus.ACTIVE,
        eventsProcessor_.getStatus());
  }

  private void createDatabase(String dbName) throws TException {
    try (MetaStoreClient msClient = catalog_.getMetaStoreClient()) {
      MetastoreApiTestUtils.createDatabase(msClient, null, dbName, null);
    }
  }

  private void createTable(String dbName, String tblName) throws TException {
    createTable(dbName, tblName, null, false, null);
  }

  private void createTransactionalTable(String dbName, String tblName,
      boolean isPartitioned) throws TException {
    Map<String, String> params = new HashMap<>();
    params.put("transactional", "true");
    params.put("transactional_properties", "insert_only");
    createTable(dbName, tblName, params, isPartitioned, "MANAGED_TABLE");
  }

  private void createTable(String dbName, String tblName, Map<String, String> params,
      boolean isPartitioned, String tableType) throws TException {
    try (MetaStoreClient msClient = catalog_.getMetaStoreClient()) {
      MetastoreApiTestUtils.createTable(msClient, null, dbName, tblName, params,
          isPartitioned, tableType);
    }
  }

  private void addPartitions(String dbName, String tblName,
      List<List<String>> partitionValues) throws TException {
    try (MetaStoreClient msClient = catalog_.getMetaStoreClient()) {
      MetastoreApiTestUtils.addPartitions(msClient, dbName, tblName, partitionValues);
    }
  }

  private void insertIntoTransactionalTable(MetaStoreClient msClient, Table table,
      List<String> partVal, long txnId, long writeId) throws TException, IOException {
    Partition partition = null;
    if (!CollectionUtils.isEmpty(partVal)) {
      partition = msClient.getHiveClient().getPartition(table.getDb().getName(),
          table.getName(), partVal);
    }
    MetastoreApiTestUtils.simulateInsertIntoTransactionalTableFromFS(
        catalog_.getMetaStoreClient(), table.getMetaStoreTable(), partition, 1, txnId,
        writeId);
  }

  private void alterTableRename(String dbName, String tblName, String newDbName,
      String newTblName) throws TException {
    try (MetaStoreClient msClient = catalog_.getMetaStoreClient()) {
      org.apache.hadoop.hive.metastore.api.Table newTable = msClient.getHiveClient()
          .getTable(dbName, tblName);
      newTable.setTableName(newTblName);
      newTable.setDbName(newDbName);
      msClient.getHiveClient()
          .alter_table_with_environmentContext(dbName, tblName, newTable, null);
    }
  }

  private static void dropDatabase(String dbName) throws TException {
    try (MetaStoreClient msClient = catalog_.getMetaStoreClient()) {
      msClient.getHiveClient().dropDatabase(dbName, true, true, true);
    }
  }

  private void processEventsSynchronously(EventExecutorService eventExecutorService) {
    boolean isNeedProcess;
    int maxLoopTimes = 10; // Bound value just to ensure there is no infinite loop
    do {
      isNeedProcess = false;
      for (DbEventExecutor dbExecutor : eventExecutorService.getDbEventExecutors()) {
        if (dbExecutor.getOutstandingEventCount() != 0) {
          isNeedProcess = true;
          dbExecutor.process();
        }
        for (TableEventExecutor tableExecutor : dbExecutor.getTableEventExecutors()) {
          if (tableExecutor.getOutstandingEventCount() != 0) {
            isNeedProcess = true;
            tableExecutor.process();
          }
        }
      }
    } while (--maxLoopTimes > 0 && isNeedProcess);
    assertTrue("Events should be processed within the limit", maxLoopTimes > 0);
  }

  private EventExecutorService createEventExecutorService(int numDbEventExecutor,
      int numTableEventExecutor) {
    EventExecutorService eventExecutorService = new EventExecutorService(eventsProcessor_,
        numDbEventExecutor, numTableEventExecutor);
    eventExecutorService.start();
    eventsProcessor_.setEventExecutorService(eventExecutorService);
    return eventExecutorService;
  }

  private void shutDownEventExecutorService(EventExecutorService eventExecutorService)
      throws Exception {
    assertTrue(eventsProcessor_.getEventExecutorService() == eventExecutorService);
    List<MetastoreEvent> metastoreEvents = eventsProcessor_.getEventsFactory()
        .getFilteredEvents(eventsProcessor_.getNextMetastoreEvents(),
            eventsProcessor_.getMetrics());
    assertEquals("Remaining events: " + metastoreEvents, 0, metastoreEvents.size());
    assertEquals(0, eventsProcessor_.getOutstandingEventCount());
    eventsProcessor_.setEventExecutorService(eventExecutorService_);
    eventExecutorService.shutdown(true);
  }

  /**
   * Tests assignment and un-assignment of executors to databases and tables
   * @throws Exception
   */
  @Test
  public void testAssignAndUnAssignExecutors() throws Exception {
    EventExecutorService eventExecutorService = new EventExecutorService(eventsProcessor_,
        2, 3);
    List<DbEventExecutor> dbEventExecutorList =
        eventExecutorService.getDbEventExecutors();
    assertEquals(2, dbEventExecutorList.size());
    DbEventExecutor dbExecutor1 = dbEventExecutorList.get(0);
    DbEventExecutor dbExecutor2 = dbEventExecutorList.get(1);
    assertEquals(3, dbExecutor1.getTableEventExecutors().size());
    assertEquals(3, dbExecutor2.getTableEventExecutors().size());
    TableEventExecutor tableExecutor1 = dbExecutor1.getTableEventExecutors().get(0);
    TableEventExecutor tableExecutor2 = dbExecutor1.getTableEventExecutors().get(1);
    TableEventExecutor tableExecutor3 = dbExecutor2.getTableEventExecutors().get(0);
    eventExecutorService.start();
    eventsProcessor_.setEventExecutorService(eventExecutorService);
    assertEquals(0, dbExecutor1.getDbCount());
    assertEquals(0, dbExecutor2.getDbCount());
    assertEquals(0, tableExecutor1.getTableCount());
    assertEquals(0, tableExecutor2.getTableCount());
    assertEquals(0, tableExecutor3.getTableCount());

    // Create dbs and tables
    String t1 = "t1";
    String t2 = "t2";
    String t3 = "t3";
    createDatabase(DB_NAME1);
    createTable(DB_NAME1, t1);
    createTable(DB_NAME1, t2);
    createDatabase(DB_NAME2);
    createTable(DB_NAME2, t3);
    eventsProcessor_.processEvents();
    assertNotNull(catalog_.getDb(DB_NAME1));
    assertNotNull(catalog_.getDb(DB_NAME2));
    Table table = catalog_.getTable(DB_NAME1, t1);
    assertTrue(table instanceof IncompleteTable);
    table = catalog_.getTable(DB_NAME1, t2);
    assertTrue(table instanceof IncompleteTable);
    table = catalog_.getTable(DB_NAME2, t3);
    assertTrue(table instanceof IncompleteTable);

    // Check executor assignment for dbs and tables
    assertEquals(dbExecutor1, eventExecutorService.getDbEventExecutor(DB_NAME1));
    assertEquals(tableExecutor1, dbExecutor1.getTableEventExecutor(DB_NAME1 + '.' + t1));
    assertEquals(tableExecutor2, dbExecutor1.getTableEventExecutor(DB_NAME1 + '.' + t2));
    assertEquals(dbExecutor2, eventExecutorService.getDbEventExecutor(DB_NAME2));
    assertEquals(tableExecutor3, dbExecutor2.getTableEventExecutor(DB_NAME2 + '.' + t3));

    // Check the db and table processor count on executors
    assertEquals(1, dbExecutor1.getDbCount());
    assertEquals(1, dbExecutor2.getDbCount());
    assertEquals(1, tableExecutor1.getTableCount());
    assertEquals(1, tableExecutor2.getTableCount());
    assertEquals(1, tableExecutor3.getTableCount());
    eventExecutorService.cleanup();

    // Check the db and table processor count again
    assertEquals(1, dbExecutor1.getDbCount());
    assertEquals(1, dbExecutor2.getDbCount());
    assertEquals(1, tableExecutor1.getTableCount());
    assertEquals(1, tableExecutor2.getTableCount());
    assertEquals(1, tableExecutor3.getTableCount());
    Thread.sleep(5 * TEST_EVENT_PROCESSOR_IDLE_TIME_MS);
    eventExecutorService.cleanup();

    // Check the db and table processor count again
    assertEquals(0, dbExecutor1.getDbCount());
    assertEquals(0, dbExecutor2.getDbCount());
    assertEquals(0, tableExecutor1.getTableCount());
    assertEquals(0, tableExecutor2.getTableCount());
    assertEquals(0, tableExecutor3.getTableCount());

    // Check if executor are unassigned for dbs and tables
    assertNull(eventExecutorService.getDbEventExecutor(DB_NAME1));
    assertNull(dbExecutor1.getTableEventExecutor(DB_NAME1 + '.' + t1));
    assertNull(dbExecutor1.getTableEventExecutor(DB_NAME1 + '.' + t2));
    assertNull(eventExecutorService.getDbEventExecutor(DB_NAME2));
    assertNull(dbExecutor2.getTableEventExecutor(DB_NAME2 + '.' + t3));
    shutDownEventExecutorService(eventExecutorService);
  }

  /**
   * Tests clear executors
   * @throws Exception
   */
  @Test
  public void testClearExecutors() throws Exception {
    EventExecutorService eventExecutorService = new EventExecutorService(eventsProcessor_,
        2, 2);
    List<DbEventExecutor> dbEventExecutorList =
        eventExecutorService.getDbEventExecutors();
    assertEquals(2, dbEventExecutorList.size());
    DbEventExecutor dbExecutor1 = dbEventExecutorList.get(0);
    DbEventExecutor dbExecutor2 = dbEventExecutorList.get(1);
    assertEquals(2, dbExecutor1.getTableEventExecutors().size());
    assertEquals(2, dbExecutor2.getTableEventExecutors().size());
    TableEventExecutor tableExecutor1 = dbExecutor1.getTableEventExecutors().get(0);
    TableEventExecutor tableExecutor2 = dbExecutor1.getTableEventExecutors().get(1);
    eventExecutorService.start();
    createDatabase(DB_NAME1);
    createDatabase(DB_NAME2);
    for (int i = 0; i < 10; i++) {
      createTable(DB_NAME1, "t"+i);
      createTable(DB_NAME2, "t"+i);
    }
    List<MetastoreEvent> metastoreEvents = eventsProcessor_.getEventsFactory()
        .getFilteredEvents(eventsProcessor_.getNextMetastoreEvents(),
            eventsProcessor_.getMetrics());
    for (MetastoreEvent event : metastoreEvents) {
      eventExecutorService.dispatch(event);
    }
    eventExecutorService.clear();
    assertEquals(0, dbExecutor1.getDbCount());
    assertEquals(0, dbExecutor1.getOutstandingEventCount());
    assertEquals(0, tableExecutor1.getTableCount());
    assertEquals(0, tableExecutor2.getTableCount());
    eventExecutorService.shutdown(true);
  }

  /**
   * Tests force shutdown of executors
   * @throws Exception
   */
  @Test
  public void testForceShutdownExecutors() throws Exception {
    EventExecutorService eventExecutorService = new EventExecutorService(eventsProcessor_,
        2, 2);
    List<DbEventExecutor> dbEventExecutorList =
        eventExecutorService.getDbEventExecutors();
    assertEquals(2, dbEventExecutorList.size());
    DbEventExecutor dbExecutor1 = dbEventExecutorList.get(0);
    DbEventExecutor dbExecutor2 = dbEventExecutorList.get(1);
    assertEquals(2, dbExecutor1.getTableEventExecutors().size());
    assertEquals(2, dbExecutor2.getTableEventExecutors().size());
    TableEventExecutor tableExecutor1 = dbExecutor1.getTableEventExecutors().get(0);
    TableEventExecutor tableExecutor2 = dbExecutor1.getTableEventExecutors().get(1);
    eventExecutorService.start();
    createDatabase(DB_NAME1);
    createDatabase(DB_NAME2);
    for (int i = 0; i < 10; i++) {
      createTable(DB_NAME1, "t"+i);
      createTable(DB_NAME2, "t"+i);
    }
    List<MetastoreEvent> metastoreEvents = eventsProcessor_.getEventsFactory()
        .getFilteredEvents(eventsProcessor_.getNextMetastoreEvents(),
            eventsProcessor_.getMetrics());
    for (MetastoreEvent event : metastoreEvents) {
      eventExecutorService.dispatch(event);
    }
    eventExecutorService.shutdown(false);
    assertEquals(0, dbExecutor1.getDbCount());
    assertEquals(0, dbExecutor1.getOutstandingEventCount());
    assertEquals(0, tableExecutor1.getTableCount());
    assertEquals(0, tableExecutor2.getTableCount());
  }

  private void renameTableTest(String srcDbName, String srcTableName, String targetDbName,
      String targetTableName) throws Exception {
    createDatabase(srcDbName);
    if (!srcDbName.equalsIgnoreCase(targetDbName)) {
      createDatabase(targetDbName);
    }
    createTable(srcDbName, srcTableName);
    eventsProcessor_.processEvents();
    Table table = catalog_.getOrLoadTable(srcDbName, srcTableName, "test", null);
    assertTrue("Table should have been loaded.", table instanceof HdfsTable);
    alterTableRename(srcDbName, srcTableName, targetDbName, targetTableName);
    eventsProcessor_.processEvents();
    Table tableAfterRename = catalog_.getTable(targetDbName, targetTableName);
    assertTrue("Table after rename should be incomplete.",
        tableAfterRename instanceof IncompleteTable);
  }

  private List<RenameTableBarrierEvent> renameTableAndGetBarrierEvents(String srcDbName,
      String srcTableName, String targetDbName, String targetTableName) throws Exception {
    createDatabase(srcDbName);
    if (!srcDbName.equalsIgnoreCase(targetDbName)) {
      createDatabase(targetDbName);
    }
    createTable(srcDbName, srcTableName);
    eventsProcessor_.processEvents();
    Table table = catalog_.getOrLoadTable(srcDbName, srcTableName, "test", null);
    assertTrue("Table should have been loaded.", table instanceof HdfsTable);
    alterTableRename(srcDbName, srcTableName, targetDbName, targetTableName);
    List<MetastoreEvent> metastoreEvents = eventsProcessor_.getEventsFactory()
        .getFilteredEvents(eventsProcessor_.getNextMetastoreEvents(),
            eventsProcessor_.getMetrics());
    AlterTableEvent alterTableEvent = null;
    for (MetastoreEvent event : metastoreEvents) {
      if (event instanceof AlterTableEvent && ((AlterTableEvent) event).isRename()) {
        alterTableEvent = (AlterTableEvent) event;
        break;
      }
    }
    assertNotNull(alterTableEvent);
    return eventsProcessor_.getEventExecutorService()
        .getRenameTableBarrierEvents(alterTableEvent);
  }

  /**
   * Tests rename table within the database with one DbEventExecutor and one
   * TableEventExecutor threads
   * @throws Exception
   */
  @Test
  public void testRenameTableWithinDBWithOneDBExecutorAndOneTableExecutor()
      throws Exception {
    EventExecutorService eventExecutorService = createEventExecutorService(1, 1);
    renameTableTest(DB_NAME1, "t1", DB_NAME1, "t2");
    shutDownEventExecutorService(eventExecutorService);
  }

  /**
   * Tests rename table across the database with one DbEventExecutor and one
   * TableEventExecutor threads
   * @throws Exception
   */
  @Test
  public void testRenameTableAcrossDBWithOneDBExecutorAndOneTableExecutor()
      throws Exception {
    EventExecutorService eventExecutorService = createEventExecutorService(1, 1);
    renameTableTest(DB_NAME1, "t1", DB_NAME2, "t2");
    shutDownEventExecutorService(eventExecutorService);
  }

  /**
   * Tests rename table within the database with one DbEventExecutor and two
   * TableEventExecutor threads
   * @throws Exception
   */
  @Test
  public void testRenameTableWithinDBWithOneDBExecutorAndTwoTableExecutor()
      throws Exception {
    EventExecutorService eventExecutorService = createEventExecutorService(1, 2);
    renameTableTest(DB_NAME1, "t1", DB_NAME1, "t2");
    shutDownEventExecutorService(eventExecutorService);
  }

  /**
   * Tests rename table across the database with one DbEventExecutor and two
   * TableEventExecutor threads
   * @throws Exception
   */
  @Test
  public void testRenameTableAcrossDBWithOneDBExecutorAndTwoTableExecutor()
      throws Exception {
    EventExecutorService eventExecutorService = createEventExecutorService(1, 2);
    renameTableTest(DB_NAME1, "t1", DB_NAME2, "t2");
    shutDownEventExecutorService(eventExecutorService);
  }

  /**
   * Tests rename table across the database with two DbEventExecutors and one
   * TableEventExecutor threads
   * @throws Exception
   */
  @Test
  public void testRenameTableAcrossDBWithTwoDBExecutorAndOneTableExecutor()
      throws Exception {
    EventExecutorService eventExecutorService = createEventExecutorService(2, 1);
    renameTableTest(DB_NAME1, "t1", DB_NAME2, "t2");
    shutDownEventExecutorService(eventExecutorService);
  }

  private void alterDatabaseAddParameters(String dbName, String key, String val)
      throws TException {
    try (MetaStoreClient msClient = catalog_.getMetaStoreClient()) {
      MetastoreApiTestUtils.addDatabaseParametersInHms(msClient, dbName, key, val);
    }
  }

  private void alterDatabase(Database newDatabase) throws TException {
    try (MetaStoreClient msClient = catalog_.getMetaStoreClient()) {
      msClient.getHiveClient().alterDatabase(newDatabase.getName(), newDatabase);
    }
  }

  private void alterDatabaseTest(String dbName, String[] tableNames) throws Exception {
    createDatabase(dbName);
    eventsProcessor_.processEvents();

    // Create tables
    for (String tableName : tableNames) {
      createTable(dbName, tableName);
    }

    // Alter database
    String currentLocation = catalog_.getDb(dbName).getMetaStoreDb().getLocationUri();
    String newLocation = currentLocation + File.separatorChar + "newTestLocation";
    Database alteredDb = catalog_.getDb(dbName).getMetaStoreDb().deepCopy();
    String dbKey = "dbKey";
    String dbVal = "dbVal";
    alteredDb.putToParameters(dbKey, dbVal);
    alteredDb.setLocationUri(newLocation);
    alterDatabase(alteredDb);

    // process add tables and alter database events together
    eventsProcessor_.processEvents();

    Database finalDb = catalog_.getDb(dbName).getMetaStoreDb();
    assertTrue(
        String.format("Altered database should have set key %s to value %s in parameters",
            dbKey, dbVal), dbVal.equals(finalDb.getParameters().get(dbKey)));
    assertTrue("Altered database should have the updated location",
        newLocation.equals(finalDb.getLocationUri()));
  }

  /**
   * Tests alter database location
   * @throws Exception
   */
  @Test
  public void testAlterDatabase() throws Exception {
    EventExecutorService eventExecutorService = createEventExecutorService(2, 2);
    // Alter database without tables in it.
    alterDatabaseTest(DB_NAME1, new String[] {});
    // Alter database after adding some tables in it.
    alterDatabaseTest(DB_NAME2, new String[] { "t1", "t2", "t3" });
    shutDownEventExecutorService(eventExecutorService);
  }

  public void transactionTest(boolean abortTxn) throws Exception {
    createDatabase(DB_NAME1);
    createDatabase(DB_NAME2);
    String t1 = "t1";
    String t2 = "t2";
    String t3 = "t3";
    createTransactionalTable(DB_NAME1, t1, false);
    createTransactionalTable(DB_NAME1, t2, true);
    List<List<String>> t2PartVals = new ArrayList<>(2);
    t2PartVals.add(Arrays.asList("1"));
    t2PartVals.add(Arrays.asList("2"));
    addPartitions(DB_NAME1, t2, t2PartVals);
    createTransactionalTable(DB_NAME2, t3, true);
    List<List<String>> t3PartVals = new ArrayList<>(1);
    t3PartVals.add(Arrays.asList("3"));
    addPartitions(DB_NAME2, t3, t3PartVals);
    eventsProcessor_.processEvents();
    Table t1Table = catalog_.getOrLoadTable(DB_NAME1, t1, "test", null);
    assertTrue(t1Table instanceof HdfsTable);
    Table t2Table = catalog_.getOrLoadTable(DB_NAME1, t2, "test", null);
    assertTrue(t2Table instanceof HdfsTable);
    Table t3Table = catalog_.getOrLoadTable(DB_NAME2, t3, "test", null);
    assertTrue(t3Table instanceof HdfsTable);
    long txnId;
    long writeId;
    try (MetaStoreClient msClient = catalog_.getMetaStoreClient()) {
      // Commit a transaction on t1
      txnId = MetastoreShim.openTransaction(msClient.getHiveClient());
      writeId = MetastoreShim.allocateTableWriteId(msClient.getHiveClient(), txnId,
          DB_NAME1, t1);
      insertIntoTransactionalTable(msClient, t1Table, null, txnId, writeId);
      MetastoreShim.commitTransaction(msClient.getHiveClient(), txnId);
      eventsProcessor_.processEvents();
      ValidWriteIdList writeIdList = t1Table.getValidWriteIds();
      assertTrue(writeIdList.isWriteIdValid(writeId));

      // Commit a transaction on t2
      txnId = MetastoreShim.openTransaction(msClient.getHiveClient());
      writeId = MetastoreShim.allocateTableWriteId(msClient.getHiveClient(), txnId,
          DB_NAME1, t2);
      insertIntoTransactionalTable(msClient, t2Table, Arrays.asList("1"), txnId, writeId);
      insertIntoTransactionalTable(msClient, t2Table, Arrays.asList("2"), txnId, writeId);
      MetastoreShim.commitTransaction(msClient.getHiveClient(), txnId);
      eventsProcessor_.processEvents();
      writeIdList = t2Table.getValidWriteIds();
      assertTrue(writeIdList.isWriteIdValid(writeId));

      // Make a transaction having 3 tables(t1, t2 and t3)
      txnId = MetastoreShim.openTransaction(msClient.getHiveClient());
      long writeId1 = MetastoreShim.allocateTableWriteId(msClient.getHiveClient(), txnId,
          DB_NAME1, t1);
      insertIntoTransactionalTable(msClient, t1Table, null, txnId, writeId1);
      long writeId2 = MetastoreShim.allocateTableWriteId(msClient.getHiveClient(), txnId,
          DB_NAME1, t2);
      insertIntoTransactionalTable(msClient, t2Table, Arrays.asList("2"), txnId,
          writeId2);
      long writeId3 = MetastoreShim.allocateTableWriteId(msClient.getHiveClient(), txnId,
          DB_NAME2, t3);
      insertIntoTransactionalTable(msClient, t3Table, Arrays.asList("3"), txnId,
          writeId3);
      if (abortTxn) {
        MetastoreShim.abortTransaction(msClient.getHiveClient(), txnId);
      } else {
        MetastoreShim.commitTransaction(msClient.getHiveClient(), txnId);
      }
      eventsProcessor_.processEvents();
      ValidWriteIdList writeIdList1 = t1Table.getValidWriteIds();
      ValidWriteIdList writeIdList2 = t2Table.getValidWriteIds();
      ValidWriteIdList writeIdList3 = t3Table.getValidWriteIds();
      if (abortTxn) {
        assertTrue(writeIdList2.isWriteIdAborted(writeId2));
        assertTrue(writeIdList3.isWriteIdAborted(writeId3));
      } else {
        assertTrue(writeIdList1.isWriteIdValid(writeId1));
        assertTrue(writeIdList2.isWriteIdValid(writeId2));
        assertTrue(writeIdList3.isWriteIdValid(writeId3));
      }
    }
  }

  /**
   * Tests commit transaction
   * @throws Exception
   */
  @Test
  public void testCommitTxn() throws Exception {
    EventExecutorService eventExecutorService = createEventExecutorService(2, 2);
    transactionTest(false);
    shutDownEventExecutorService(eventExecutorService);
  }

  /**
   * Tests abort transaction
   * @throws Exception
   */
  @Test
  public void testAbortTxn() throws Exception {
    EventExecutorService eventExecutorService = createEventExecutorService(2, 2);
    transactionTest(true);
    shutDownEventExecutorService(eventExecutorService);
  }

  /**
   * Tests rename event state class to verify the pseudo-events processing order
   * @throws Exception
   */
  @Test
  public void testRenameEventState() throws Exception {
    EventExecutorService eventExecutorService = createEventExecutorService(2, 1);
    List<RenameTableBarrierEvent> barrierEvents = renameTableAndGetBarrierEvents(DB_NAME1,
        "t1", DB_NAME2, "t2");
    eventsProcessor_.processEvents();

    RenameTableBarrierEvent dropTableBarrierEvent = null;
    RenameTableBarrierEvent createTableBarrierEvent = null;
    for (RenameTableBarrierEvent event : barrierEvents) {
      if (event.getEventType() == MetastoreEventType.DROP_TABLE) {
        dropTableBarrierEvent = event;
      } else if (event.getEventType() == MetastoreEventType.CREATE_TABLE) {
        createTableBarrierEvent = event;
      }
    }
    int sleep = 10;
    assertNotNull(createTableBarrierEvent);
    RenameTableBarrierEvent.RenameEventState state = createTableBarrierEvent.getState();
    RenameTableBarrierEvent finalCreateTableBarrierEvent = createTableBarrierEvent;
    Callable<Boolean> createTableCallable = () -> {
      assertFalse(state.isCreateProcessed());
      assertFalse(finalCreateTableBarrierEvent.canProcess());
      while (!state.isDropProcessed()) {
        Thread.sleep(sleep);
      }
      assertFalse(state.isCreateProcessed());
      assertTrue(finalCreateTableBarrierEvent.canProcess());
      state.setProcessed(MetastoreEventType.CREATE_TABLE, false);
      assertTrue(state.isCreateProcessed());
      assertFalse(finalCreateTableBarrierEvent.canProcess());
      return true;
    };
    assertNotNull(dropTableBarrierEvent);
    RenameTableBarrierEvent finalDropTableBarrierEvent = dropTableBarrierEvent;
    Callable<Boolean> dropTableCallable = () -> {
      assertFalse(state.isDropProcessed());
      // Delay before processing the drop table barrier event
      int delayCount = 5;
      while (delayCount-- > 0) {
        assertTrue(finalDropTableBarrierEvent.canProcess());
        Thread.sleep(sleep);
      }
      assertFalse(state.isDropProcessed());
      assertTrue(finalDropTableBarrierEvent.canProcess());
      state.setProcessed(MetastoreEventType.DROP_TABLE, false);
      assertTrue(state.isDropProcessed());
      assertFalse(finalDropTableBarrierEvent.canProcess());
      return true;
    };
    ExecutorService executorService = Executors.newFixedThreadPool(2);
    try {
      Future<Boolean> createTableTask = executorService.submit(createTableCallable);
      Future<Boolean> dropTableTask = executorService.submit(dropTableCallable);
      assertTrue(dropTableTask.get() && createTableTask.get());
    } finally {
      executorService.shutdown();
      shutDownEventExecutorService(eventExecutorService);
    }
  }

  /**
   * Test event skipping when drop database event is queued for processing along with
   * the events
   * @throws Exception
   */
  @Test
  public void testEventSkippingWhenDropDatabaseQueuedBehind() throws Exception {
    EventExecutorService eventExecutorService = new EventExecutorService(eventsProcessor_,
        2, 1);
    eventExecutorService.setStatus(EventExecutorService.EventExecutorStatus.ACTIVE);
    String t1 = "t1";
    createDatabase(DB_NAME1);
    createDatabase(DB_NAME2);
    createTable(DB_NAME1, t1);
    alterDatabaseAddParameters(DB_NAME1, "dbKey", "dbVal");
    alterTableRename(DB_NAME1, t1, DB_NAME2, "t2");
    dropDatabase(DB_NAME1);
    dropDatabase(DB_NAME2);
    Metrics metrics = eventsProcessor_.getMetrics();
    long prevEventsSkipped = metrics.getCounter(
        MetastoreEventsProcessor.EVENTS_SKIPPED_METRIC).getCount();
    long dbsAddedMetric = metrics.getCounter(
        MetastoreEventsProcessor.NUMBER_OF_DATABASES_ADDED).getCount();
    long dbsRemovedMetric = metrics.getCounter(
        MetastoreEventsProcessor.NUMBER_OF_DATABASES_REMOVED).getCount();
    long tablesAddedMetric = metrics.getCounter(
        MetastoreEventsProcessor.NUMBER_OF_TABLES_ADDED).getCount();
    long tablesRemovedMetric = metrics.getCounter(
        MetastoreEventsProcessor.NUMBER_OF_TABLES_REMOVED).getCount();

    List<MetastoreEvent> metastoreEvents = eventsProcessor_.getEventsFactory()
        .getFilteredEvents(eventsProcessor_.getNextMetastoreEvents(),
            eventsProcessor_.getMetrics());
    assertTrue(metastoreEvents.size() > 0);
    for (MetastoreEvent event : metastoreEvents) {
      eventExecutorService.dispatch(event);
    }
    processEventsSynchronously(eventExecutorService);
    eventExecutorService.setStatus(EventExecutorService.EventExecutorStatus.STOPPED);
    // Skips all the events i.e., create database events(DB_NAME1 and DB_NAME2), create
    // table event(DB_NAME1.t1), alter database event for DB_NAME1, rename table from
    // DB_NAME1.t1 to DB_NAME2.t2 event, drop database event for DB_NAME1, drop table
    // for DB_NAME2.t2 and drop database event for DB_NAME2 are skipped
    assertEquals(prevEventsSkipped + 8,
        metrics.getCounter(MetastoreEventsProcessor.EVENTS_SKIPPED_METRIC).getCount());
    assertEquals(dbsAddedMetric,
        metrics.getCounter(MetastoreEventsProcessor.NUMBER_OF_DATABASES_ADDED)
            .getCount());
    assertEquals(dbsRemovedMetric,
        metrics.getCounter(MetastoreEventsProcessor.NUMBER_OF_DATABASES_REMOVED)
            .getCount());
    assertEquals(tablesAddedMetric,
        metrics.getCounter(MetastoreEventsProcessor.NUMBER_OF_TABLES_ADDED).getCount());
    assertEquals(tablesRemovedMetric,
        metrics.getCounter(MetastoreEventsProcessor.NUMBER_OF_TABLES_REMOVED).getCount());
  }
}
