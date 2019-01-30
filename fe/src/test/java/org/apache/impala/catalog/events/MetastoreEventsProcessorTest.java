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
import static org.junit.Assert.fail;

import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.hive.metastore.api.CurrentNotificationEventId;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.GetPartitionsRequest;
import org.apache.hadoop.hive.metastore.api.NotificationEvent;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.PrincipalType;
import org.apache.hadoop.hive.metastore.client.builder.DatabaseBuilder;
import org.apache.hadoop.hive.metastore.client.builder.PartitionBuilder;
import org.apache.hadoop.hive.metastore.client.builder.TableBuilder;
import org.apache.impala.catalog.CatalogException;
import org.apache.impala.catalog.CatalogServiceCatalog;
import org.apache.impala.catalog.DatabaseNotFoundException;
import org.apache.impala.catalog.HdfsFileFormat;
import org.apache.impala.catalog.HdfsTable;
import org.apache.impala.catalog.IncompleteTable;
import org.apache.impala.catalog.MetaStoreClientPool.MetaStoreClient;
import org.apache.impala.catalog.Table;
import org.apache.impala.catalog.events.MetastoreEventUtils.MetastoreEvent;
import org.apache.impala.catalog.events.MetastoreEventUtils.MetastoreEventType;
import org.apache.impala.catalog.events.MetastoreEventsProcessor.EventProcessorStatus;
import org.apache.impala.common.ImpalaException;
import org.apache.impala.service.CatalogOpExecutor;
import org.apache.impala.testutil.CatalogServiceTestCatalog;
import org.apache.impala.thrift.TAlterTableOrViewRenameParams;
import org.apache.impala.thrift.TAlterTableParams;
import org.apache.impala.thrift.TAlterTableType;
import org.apache.impala.thrift.TColumn;
import org.apache.impala.thrift.TColumnType;
import org.apache.impala.thrift.TCreateDbParams;
import org.apache.impala.thrift.TCreateTableParams;
import org.apache.impala.thrift.TDdlExecRequest;
import org.apache.impala.thrift.TDdlType;
import org.apache.impala.thrift.TDropDbParams;
import org.apache.impala.thrift.TDropTableOrViewParams;
import org.apache.impala.thrift.THdfsFileFormat;
import org.apache.impala.thrift.TPrimitiveType;
import org.apache.impala.thrift.TScalarType;
import org.apache.impala.thrift.TTableName;
import org.apache.impala.thrift.TTypeNode;
import org.apache.impala.thrift.TTypeNodeType;
import org.apache.thrift.TException;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

import com.google.common.collect.Lists;

/**
 * Main test class to cover the functionality of MetastoreEventProcessor. In order to make
 * the test deterministic, this test relies on the fact the default value of
 * hms_event_polling_interval_s is 0. This means that there is no automatic scheduled
 * frequency of the polling for events from metastore. In order to simulate a poll
 * operation this test issues the <code>processEvents</code> method
 * manually to process the pending events. This test relies on a external HMS process
 * running in a minicluster environment such that events are generated and they have the
 * thrift objects enabled in the event messages.
 */
public class MetastoreEventsProcessorTest {
  private static final String TEST_TABLE_NAME_PARTITIONED = "test_partitioned_tbl";
  private static final String TEST_DB_NAME = "events_test_db";
  private static final String TEST_TABLE_NAME_NONPARTITIONED = "test_nonpartitioned_tbl";

  private static CatalogServiceCatalog catalog_;
  private static CatalogOpExecutor catalogOpExecutor_;
  private static MetastoreEventsProcessor eventsProcessor_;

  @BeforeClass
  public static void setUpTestEnvironment() throws TException {
    catalog_ = CatalogServiceTestCatalog.create();
    catalogOpExecutor_ = new CatalogOpExecutor(catalog_);
    try (MetaStoreClient metaStoreClient = catalog_.getMetaStoreClient()) {
      CurrentNotificationEventId currentNotificationId =
          metaStoreClient.getHiveClient().getCurrentNotificationEventId();
      eventsProcessor_ = new SynchronousHMSEventProcessorForTests(
          catalog_, currentNotificationId.getEventId(), 10L);
      eventsProcessor_.start();
    }
    catalog_.setMetastoreEventProcessor(eventsProcessor_);
  }

  @AfterClass
  public static void tearDownTestSetup() {
    try {
      dropDatabaseCascadeFromHMS();
      // remove database from catalog as well to clean up catalog state
      catalog_.removeDb(TEST_DB_NAME);
    } catch (Exception ex) {
      // ignored
    }
  }

  private static void dropDatabaseCascadeFromHMS() throws TException {
    dropDatabaseCascade(TEST_DB_NAME);
  }

  private static void dropDatabaseCascade(String dbName) throws TException {
    try (MetaStoreClient msClient = catalog_.getMetaStoreClient()) {
      msClient.getHiveClient().dropDatabase(dbName, true, true, true);
    }
  }

  /**
   * Cleans up the test database from both metastore and catalog
   * @throws TException
   */
  @Before
  public void beforeTest() throws TException, CatalogException {
    try (MetaStoreClient msClient = catalog_.getMetaStoreClient()) {
      msClient.getHiveClient().dropDatabase(TEST_DB_NAME, true, true, true);
    }
    catalog_.removeDb(TEST_DB_NAME);
    // reset the event processor to the current eventId
    eventsProcessor_.stop();
    eventsProcessor_.start(eventsProcessor_.getCurrentEventId());
    assertEquals(EventProcessorStatus.ACTIVE, eventsProcessor_.getStatus());
  }

  /**
   * Make sure the eventProcessor is in ACTIVE state after processing all the events in
   * the test. All tests should make sure that the eventprocessor is returned back to
   * active state so that next test execution starts clean
   */
  @After
  public void afterTest() {
    assertEquals(EventProcessorStatus.ACTIVE, eventsProcessor_.getStatus());
  }

  /**
   * Checks that database exists after processing a CREATE_DATABASE event
   */
  @Test
  public void testCreateDatabaseEvent() throws TException, ImpalaException {
    createDatabase();
    eventsProcessor_.processEvents();
    assertNotNull(catalog_.getDb(TEST_DB_NAME));
  }

  /**
   * Checks that Db object does not exist after processing DROP_DATABASE event when the
   * dropped database is empty
   */
  @Test
  public void testDropEmptyDatabaseEvent() throws TException, ImpalaException {
    dropDatabaseCascade("database_to_be_dropped");
    // create empty database
    createDatabase("database_to_be_dropped");
    eventsProcessor_.processEvents();
    assertNotNull(catalog_.getDb("database_to_be_dropped"));
    dropDatabaseCascade("database_to_be_dropped");
    eventsProcessor_.processEvents();
    assertNull("Database should not be found after processing drop_database event",
        catalog_.getDb("database_to_be_dropped"));
  }

  /**
   * Checks that Db object does not exist after processing DROP_DATABASE event when the
   * dropped database is not empty. This event could be generated by issuing a DROP
   * DATABASE .. CASCADE command. In this case since the tables in the database are also
   * dropped, we expect to see a DatabaseNotFoundException when we query for the tables in
   * the dropped database.
   */
  @Test
  public void testdropDatabaseEvent() throws TException, ImpalaException {
    createDatabase();
    String tblToBeDropped = "tbl_to_be_dropped";
    createTable(tblToBeDropped, true);
    createTable("tbl_to_be_dropped_unpartitioned", false);
    // create 2 partitions
    List<List<String>> partVals = new ArrayList<>(2);
    partVals.add(Arrays.asList("1"));
    partVals.add(Arrays.asList("2"));
    addPartitions(tblToBeDropped, partVals);
    eventsProcessor_.processEvents();
    loadTable(tblToBeDropped);
    // now drop the database with cascade option
    dropDatabaseCascadeFromHMS();
    eventsProcessor_.processEvents();
    assertTrue(
        "Dropped database should not be found after processing drop_database event",
        catalog_.getDb(TEST_DB_NAME) == null);
    // throws DatabaseNotFoundException
    try {
      catalog_.getTable(TEST_DB_NAME, tblToBeDropped);
      fail();
    } catch (DatabaseNotFoundException expectedEx) {
      // expected exception; ignored
    }
  }

  @Ignore("Disabled until we fix Hive bug to deserialize alter_database event messages")
  @Test
  public void testAlterDatabaseEvents() throws TException, ImpalaException {
    createDatabase();
    String testDbParamKey = "testKey";
    String testDbParamVal = "testVal";
    eventsProcessor_.processEvents();
    assertFalse("Newly created test database has db should not have parameter with key "
            + testDbParamKey,
        catalog_.getDb(TEST_DB_NAME)
            .getMetaStoreDb()
            .getParameters()
            .containsKey(testDbParamKey));
    // test change of parameters to the Database
    addDatabaseParameters(testDbParamKey, "someDbParamVal");
    eventsProcessor_.processEvents();
    assertTrue("Altered database should have set the key " + testDbParamKey + " to value "
            + testDbParamVal + " in parameters",
        testDbParamVal.equals(catalog_.getDb(TEST_DB_NAME)
                                  .getMetaStoreDb()
                                  .getParameters()
                                  .get(testDbParamKey)));

    // test update to the default location
    String currentLocation =
        catalog_.getDb(TEST_DB_NAME).getMetaStoreDb().getLocationUri();
    String newLocation = currentLocation + File.separatorChar + "newTestLocation";
    Database alteredDb = catalog_.getDb(TEST_DB_NAME).getMetaStoreDb().deepCopy();
    alteredDb.setLocationUri(newLocation);
    alterDatabase(alteredDb);
    eventsProcessor_.processEvents();
    assertTrue("Altered database should have the updated location",
        newLocation.equals(
            catalog_.getDb(TEST_DB_NAME).getMetaStoreDb().getLocationUri()));

    // test change of owner
    String owner = catalog_.getDb(TEST_DB_NAME).getMetaStoreDb().getOwnerName();
    final String newOwner = "newTestOwner";
    // sanity check
    assertFalse(newOwner.equals(owner));
    alteredDb = catalog_.getDb(TEST_DB_NAME).getMetaStoreDb().deepCopy();
    alteredDb.setOwnerName(newOwner);
    alterDatabase(alteredDb);
    eventsProcessor_.processEvents();
    assertTrue("Altered database should have the updated owner",
        newOwner.equals(catalog_.getDb(TEST_DB_NAME).getMetaStoreDb().getOwnerName()));
  }

  /**
   * Test creates two table (partitioned and non-partitioned) and makes sure that CatalogD
   * has the two created table objects after the CREATE_TABLE events are processed.
   */
  @Test
  public void testCreateTableEvent() throws TException, ImpalaException {
    createDatabase();
    eventsProcessor_.processEvents();
    assertNull(TEST_TABLE_NAME_NONPARTITIONED + " is not expected to exist",
        catalog_.getTable(TEST_DB_NAME, TEST_TABLE_NAME_NONPARTITIONED));
    // create a non-partitioned table
    createTable(TEST_TABLE_NAME_NONPARTITIONED, false);
    eventsProcessor_.processEvents();
    assertNotNull("Catalog should have a incomplete instance of table after CREATE_TABLE "
            + "event is received",
        catalog_.getTable(TEST_DB_NAME, TEST_TABLE_NAME_NONPARTITIONED));
    assertTrue("Newly created table from events should be a IncompleteTable",
        catalog_.getTable(TEST_DB_NAME, TEST_TABLE_NAME_NONPARTITIONED)
                instanceof IncompleteTable);
    // test partitioned table case
    createTable(TEST_TABLE_NAME_PARTITIONED, true);
    eventsProcessor_.processEvents();
    assertNotNull("Catalog should have create a incomplete table after receiving "
            + "CREATE_TABLE event",
        catalog_.getTable(TEST_DB_NAME, TEST_TABLE_NAME_PARTITIONED));
    assertTrue("Newly created table should be instance of IncompleteTable",
        catalog_.getTable(TEST_DB_NAME, TEST_TABLE_NAME_PARTITIONED)
                instanceof IncompleteTable);
  }

  /**
   * This tests adds few partitions to a existing table and makes sure that the subsequent
   * load table command fetches the expected number of partitions. It relies on the fact
   * the HMSEventProcessor currently just issues a invalidate command on the table instead
   * of directly refreshing the partition objects TODO: This test can be improved further
   * to check if the table has new partitions without the load command once IMPALA-7973 is
   * fixed
   */
  @Test
  public void testPartitionEvents() throws TException, ImpalaException {
    createDatabase();
    createTable(TEST_TABLE_NAME_PARTITIONED, true);
    // sync to latest event id
    eventsProcessor_.processEvents();

    // simulate the table being loaded by explicitly calling load table
    loadTable(TEST_TABLE_NAME_PARTITIONED);
    List<List<String>> partVals = new ArrayList<>();

    // create 4 partitions
    partVals.add(Arrays.asList("1"));
    partVals.add(Arrays.asList("2"));
    partVals.add(Arrays.asList("3"));
    partVals.add(Arrays.asList("4"));
    addPartitions(TEST_TABLE_NAME_PARTITIONED, partVals);

    eventsProcessor_.processEvents();
    // after ADD_PARTITION event is received currently we just invalidate the table
    assertTrue("Table should have been invalidated after add partition event",
        catalog_.getTable(TEST_DB_NAME, TEST_TABLE_NAME_PARTITIONED)
                instanceof IncompleteTable);

    loadTable(TEST_TABLE_NAME_PARTITIONED);
    assertEquals("Unexpected number of partitions fetched for the loaded table", 4,
        ((HdfsTable) catalog_.getTable(TEST_DB_NAME, TEST_TABLE_NAME_PARTITIONED))
            .getPartitions()
            .size());

    // now remove some partitions to see if catalogD state gets invalidated
    partVals.clear();
    partVals.add(Arrays.asList("1"));
    partVals.add(Arrays.asList("2"));
    partVals.add(Arrays.asList("3"));
    dropPartitions(TEST_TABLE_NAME_PARTITIONED, partVals);
    eventsProcessor_.processEvents();

    assertTrue("Table should have been invalidated after drop partition event",
        catalog_.getTable(TEST_DB_NAME, TEST_TABLE_NAME_PARTITIONED)
            instanceof IncompleteTable);
    loadTable(TEST_TABLE_NAME_PARTITIONED);
    assertEquals("Unexpected number of partitions fetched for the loaded table", 1,
        ((HdfsTable) catalog_.getTable(TEST_DB_NAME, TEST_TABLE_NAME_PARTITIONED))
            .getPartitions().size());

    // issue alter partition ops
    partVals.clear();
    partVals.add(Arrays.asList("4"));
    Map<String, String> newParams = new HashMap<>(2);
    newParams.put("alterKey1", "alterVal1");
    alterPartitions(TEST_TABLE_NAME_PARTITIONED, partVals, newParams);
    eventsProcessor_.processEvents();
    assertTrue("Table should have been invalidated after alter partition event",
        catalog_.getTable(TEST_DB_NAME, TEST_TABLE_NAME_PARTITIONED)
            instanceof IncompleteTable);
  }

  /**
   * Test generates ALTER_TABLE events for various cases (table rename, parameter change,
   * add/remove/change column) and makes sure that the table is updated on the CatalogD
   * side after the ALTER_TABLE event is processed.
   */
  @Test
  public void testAlterTableEvent() throws TException, ImpalaException {
    createDatabase();
    createTable("old_name", false);
    // sync to latest events
    eventsProcessor_.processEvents();
    // simulate the table being loaded by explicitly calling load table
    loadTable("old_name");

    // test renaming a table from outside aka metastore client
    alterTableRename("old_name", TEST_TABLE_NAME_NONPARTITIONED);
    eventsProcessor_.processEvents();
    // table with the old name should not be present anymore
    assertNull(
        "Old named table still exists", catalog_.getTable(TEST_DB_NAME, "old_name"));
    // table with the new name should be present in Incomplete state
    Table newTable = catalog_.getTable(TEST_DB_NAME, TEST_TABLE_NAME_NONPARTITIONED);
    assertNotNull("Table with the new name is not found", newTable);
    assertTrue("Table with the new name should be incomplete",
        newTable instanceof IncompleteTable);

    // check invalidate after alter table add parameter
    loadTable(TEST_TABLE_NAME_NONPARTITIONED);
    alterTableAddParameter(TEST_TABLE_NAME_NONPARTITIONED, "somekey", "someval");
    eventsProcessor_.processEvents();
    assertTrue("Table should be incomplete after alter table add parameter",
        catalog_.getTable(TEST_DB_NAME, TEST_TABLE_NAME_NONPARTITIONED)
                instanceof IncompleteTable);

    // check invalidate after alter table add col
    loadTable(TEST_TABLE_NAME_NONPARTITIONED);
    alterTableAddCol(TEST_TABLE_NAME_NONPARTITIONED, "newCol", "int", "null");
    eventsProcessor_.processEvents();
    assertTrue("Table should have been invalidated after alter table add column",
        catalog_.getTable(TEST_DB_NAME, TEST_TABLE_NAME_NONPARTITIONED)
                instanceof IncompleteTable);

    // check invalidate after alter table change column type
    loadTable(TEST_TABLE_NAME_NONPARTITIONED);
    altertableChangeCol(TEST_TABLE_NAME_NONPARTITIONED, "newCol", "string", null);
    eventsProcessor_.processEvents();
    assertTrue("Table should have been invalidated after changing column type",
        catalog_.getTable(TEST_DB_NAME, TEST_TABLE_NAME_NONPARTITIONED)
                instanceof IncompleteTable);

    // check invalidate after alter table remove column
    loadTable(TEST_TABLE_NAME_NONPARTITIONED);
    alterTableRemoveCol(TEST_TABLE_NAME_NONPARTITIONED, "newCol");
    eventsProcessor_.processEvents();
    assertTrue("Table should have been invalidated after removing a column",
        catalog_.getTable(TEST_DB_NAME, TEST_TABLE_NAME_NONPARTITIONED)
                instanceof IncompleteTable);
  }

  /**
   * Test drops table using a metastore client and makes sure that the table does not
   * exist in the catalogD after processing DROP_TABLE event is processed. Repeats the
   * same test for a partitioned table.
   */
  @Test
  public void testDropTableEvent() throws TException, ImpalaException {
    createDatabase();
    final String TBL_TO_BE_DROPPED = "tbl_to_be_dropped";
    createTable(TBL_TO_BE_DROPPED, false);
    eventsProcessor_.processEvents();
    loadTable(TBL_TO_BE_DROPPED);
    // issue drop table and make sure it doesn't exist after processing the events
    dropTable(TBL_TO_BE_DROPPED);
    eventsProcessor_.processEvents();
    assertTrue("Table should not be found after processing drop_table event",
        catalog_.getTable(TEST_DB_NAME, TBL_TO_BE_DROPPED) == null);

    // test partitioned table drop
    createTable(TBL_TO_BE_DROPPED, true);

    eventsProcessor_.processEvents();
    loadTable(TBL_TO_BE_DROPPED);
    // create 2 partitions
    List<List<String>> partVals = new ArrayList<>(2);
    partVals.add(Arrays.asList("1"));
    partVals.add(Arrays.asList("2"));
    addPartitions(TBL_TO_BE_DROPPED, partVals);
    dropTable(TBL_TO_BE_DROPPED);
    eventsProcessor_.processEvents();
    assertTrue("Partitioned table should not be found after processing drop_table event",
        catalog_.getTable(TEST_DB_NAME, TBL_TO_BE_DROPPED) == null);
  }

  /**
   * Test makes sure that the events are not processed when the event processor is in
   * STOPPED state
   * @throws TException
   */
  @Test
  public void testStopEventProcessing() throws TException {
    try {
      assertEquals(EventProcessorStatus.ACTIVE, eventsProcessor_.getStatus());
      eventsProcessor_.stop();
      createDatabase();
      eventsProcessor_.processEvents();
      assertEquals(EventProcessorStatus.STOPPED, eventsProcessor_.getStatus());
      assertNull(
          "Test database should not be in catalog when event processing is stopped",
          catalog_.getDb(TEST_DB_NAME));
    } finally {
      eventsProcessor_.start();
    }
  }

  /**
   * Test makes sure that event processing is restarted after a stop/start(eventId)
   * call sequence to event processor
   */
  @Test
  public void testEventProcessorRestart() throws TException {
    try {
      assertEquals(EventProcessorStatus.ACTIVE, eventsProcessor_.getStatus());
      long syncedIdBefore = eventsProcessor_.getLastSyncedEventId();
      eventsProcessor_.stop();
      createDatabase();
      eventsProcessor_.processEvents();
      assertEquals(EventProcessorStatus.STOPPED, eventsProcessor_.getStatus());
      assertNull(
          "Test database should not be in catalog when event processing is stopped",
          catalog_.getDb(TEST_DB_NAME));
      eventsProcessor_.start(syncedIdBefore);
      assertEquals(EventProcessorStatus.ACTIVE, eventsProcessor_.getStatus());
      eventsProcessor_.processEvents();
      assertNotNull(
          "Test database should be in catalog when event processing is restarted",
          catalog_.getDb(TEST_DB_NAME));
    } finally {
      if (eventsProcessor_.getStatus() != EventProcessorStatus.ACTIVE) {
        eventsProcessor_.start();
      }
    }
  }

  /**
   * Test makes sure that event processor is restarted after reset()
   */
  @Test
  public void testEventProcessingAfterReset() throws ImpalaException {
    assertEquals(EventProcessorStatus.ACTIVE, eventsProcessor_.getStatus());
    long syncedIdBefore = eventsProcessor_.getLastSyncedEventId();
    catalog_.reset();
    assertEquals(EventProcessorStatus.ACTIVE, eventsProcessor_.getStatus());
    // nothing changed so event id remains the same
    assertEquals(syncedIdBefore, eventsProcessor_.getLastSyncedEventId());
  }

  /**
   * Test creates, drops and creates a table with the same name from Impala. This would
   * lead to an interesting sequence of CREATE_TABLE, DROP_TABLE, CREATE_TABLE events
   * while the catalogD state has the latest version of the table cached. Test makes sure
   * that Event processor does not modify catalogd state since the catalog table is
   * already at its latest state
   */
  @Test
  public void testCreateDropCreateTableFromImpala() throws ImpalaException, TException {
    assertEquals(EventProcessorStatus.ACTIVE, eventsProcessor_.getStatus());
    createDatabase();
    eventsProcessor_.processEvents();
    createTableFromImpala(TEST_TABLE_NAME_NONPARTITIONED, false);
    assertNotNull("Table should have been found after create table statement",
        catalog_.getTable(TEST_DB_NAME, TEST_TABLE_NAME_NONPARTITIONED));
    loadTable(TEST_TABLE_NAME_NONPARTITIONED);
    dropTableFromImpala(TEST_TABLE_NAME_NONPARTITIONED);
    // now catalogD does not have the table entry, create the table again
    createTableFromImpala(TEST_TABLE_NAME_NONPARTITIONED, false);
    assertNotNull("Table should have been found after create table statement",
        catalog_.getTable(TEST_DB_NAME, TEST_TABLE_NAME_NONPARTITIONED));
    loadTable(TEST_TABLE_NAME_NONPARTITIONED);
    List<NotificationEvent> events = eventsProcessor_.getNextMetastoreEvents();
    // the first create table event should not change anything to the catalogd's
    // created table
    assertEquals(3, events.size());
    Table existingTable = catalog_.getTable(TEST_DB_NAME, TEST_TABLE_NAME_NONPARTITIONED);
    int creationTime = existingTable.getMetaStoreTable().getCreateTime();
    assertEquals("CREATE_TABLE", events.get(0).getEventType());
    eventsProcessor_.processEvents(Lists.newArrayList(events.get(0)));
    // after processing the create_table the original table should still remain the same
    assertEquals(creationTime, catalog_.getTable(TEST_DB_NAME,
        TEST_TABLE_NAME_NONPARTITIONED).getMetaStoreTable().getCreateTime());
    //second event should be drop_table. This event should also be skipped since
    // catalog state is more recent than the event
    assertEquals("DROP_TABLE", events.get(1).getEventType());
    eventsProcessor_.processEvents(Lists.newArrayList(events.get(1)));
    // even after drop table event, the table should still exist
    assertNotNull("Table should have existed since catalog state is current and event "
        + "is stale", catalog_.getTable(TEST_DB_NAME, TEST_TABLE_NAME_NONPARTITIONED));
    // the final create table event should also be ignored since its a self-event
    assertEquals("CREATE_TABLE", events.get(2).getEventType());
    eventsProcessor_.processEvents(Lists.newArrayList(events.get(2)));
    assertFalse(
        "Table should have been loaded since the create_table should be " + "ignored",
        catalog_.getTable(TEST_DB_NAME,
            TEST_TABLE_NAME_NONPARTITIONED) instanceof IncompleteTable);
    //finally make sure the table is still the same
    assertEquals(creationTime, catalog_.getTable(TEST_DB_NAME,
        TEST_TABLE_NAME_NONPARTITIONED).getMetaStoreTable().getCreateTime());
  }

  /**
   * Test generates DDL events on table and makes sure that event processing does not
   * modify the catalog state
   *
   * @throws ImpalaException
   */
  @Test
  public void testTableEventsFromImpala() throws ImpalaException {
    createDatabaseFromImpala(TEST_DB_NAME, "created from Impala");
    createTableFromImpala(TEST_TABLE_NAME_PARTITIONED, true);
    loadTable(TEST_TABLE_NAME_PARTITIONED);
    List<NotificationEvent> events = eventsProcessor_.getNextMetastoreEvents();
    assertEquals(2, events.size());

    eventsProcessor_.processEvents(events);
    assertNotNull(catalog_.getDb(TEST_DB_NAME));
    assertNotNull(catalog_.getTable(TEST_DB_NAME, TEST_TABLE_NAME_PARTITIONED));
    assertFalse("Table should have been loaded since it was already latest", catalog_
        .getTable(TEST_DB_NAME, TEST_TABLE_NAME_PARTITIONED) instanceof IncompleteTable);

    dropTableFromImpala(TEST_TABLE_NAME_PARTITIONED);
    assertNull(catalog_.getTable(TEST_DB_NAME, TEST_TABLE_NAME_PARTITIONED));
    events = eventsProcessor_.getNextMetastoreEvents();
    // should have 1 drop_table event
    assertEquals(1, events.size());
    eventsProcessor_.processEvents(events);
    // dropping a non-existant table should cause event processor to go into error state
    assertEquals(EventProcessorStatus.ACTIVE, eventsProcessor_.getStatus());
    assertNull(catalog_.getTable(TEST_DB_NAME, TEST_TABLE_NAME_PARTITIONED));
  }

  /**
   * Creates events like create, drop with the same tblName. In such case the create
   * table should not create a in
   */
  @Test
  public void testEventFiltering() throws ImpalaException {
    createDatabaseFromImpala(TEST_DB_NAME, "");
    createTableFromImpala(TEST_TABLE_NAME_NONPARTITIONED, false);
    loadTable(TEST_TABLE_NAME_NONPARTITIONED);
    assertNotNull(catalog_.getTable(TEST_DB_NAME, TEST_TABLE_NAME_NONPARTITIONED));
    dropTableFromImpala(TEST_TABLE_NAME_NONPARTITIONED);
    // the create table event should be filtered out
    List<NotificationEvent> events = eventsProcessor_.getNextMetastoreEvents();
    assertEquals(3, events.size());
    List<MetastoreEvent> filteredEvents =
        eventsProcessor_.getMetastoreEventFactory().getFilteredEvents(events);
    assertEquals(2, filteredEvents.size());
    assertEquals(MetastoreEventType.CREATE_DATABASE, filteredEvents.get(0).eventType_);
    assertEquals(MetastoreEventType.DROP_TABLE, filteredEvents.get(1).eventType_);
    eventsProcessor_.processEvents();
    assertNull(catalog_.getTable(TEST_DB_NAME, TEST_TABLE_NAME_NONPARTITIONED));

    // test the table rename case
    createTableFromImpala(TEST_TABLE_NAME_NONPARTITIONED, false);
    renameTableFromImpala(TEST_TABLE_NAME_NONPARTITIONED, "new_name");
    events = eventsProcessor_.getNextMetastoreEvents();
    assertEquals(2, events.size());
    filteredEvents =
        eventsProcessor_.getMetastoreEventFactory().getFilteredEvents(events);
    assertEquals(1, filteredEvents.size());
    assertEquals(MetastoreEventType.ALTER_TABLE, filteredEvents.get(0).eventType_);
  }

  /**
   * Similar to create,drop,create sequence table as in
   * <code>testCreateDropCreateTableFromImpala</code> but operates on Database instead
   * of Table. Makes sure that the database creationTime is checked before processing
   * create and drop database events
   */
  @Ignore("Ignored since database createTime is unavailable until we have HIVE-21077")
  @Test
  public void testCreateDropCreateDatabaseFromImpala() throws ImpalaException {
    assertEquals(EventProcessorStatus.ACTIVE, eventsProcessor_.getStatus());
    createDatabaseFromImpala(TEST_DB_NAME, "first");
    assertNotNull("Db should have been found after create database statement",
        catalog_.getDb(TEST_DB_NAME));
    dropDatabaseFromImpala(TEST_DB_NAME);
    assertNull(catalog_.getDb(TEST_DB_NAME));
    createDatabaseFromImpala(TEST_DB_NAME, "second");
    assertNotNull(catalog_.getDb(TEST_DB_NAME));
    List<NotificationEvent> events = eventsProcessor_.getNextMetastoreEvents();
    // should have 3 events for create,drop and create database
    assertEquals(3, events.size());

    assertEquals("CREATE_DATABASE", events.get(0).getEventType());
    eventsProcessor_.processEvents(Lists.newArrayList(events.get(0)));
    // create_database event should have no effect since catalogD has already a later
    // version of database with the same name.
    assertNotNull(catalog_.getDb(TEST_DB_NAME));
    assertEquals("second",
        catalog_.getDb(TEST_DB_NAME).getMetaStoreDb().getDescription());

    // now process drop_database event
    assertEquals("DROP_DATABASE", events.get(1).getEventType());
    eventsProcessor_.processEvents(Lists.newArrayList(events.get(1)));
    // database should not be dropped since catalogD is at the latest state
    assertNotNull(catalog_.getDb(TEST_DB_NAME));
    assertEquals("second",
        catalog_.getDb(TEST_DB_NAME).getMetaStoreDb().getDescription());

    // the third create_database event should have no effect too
    assertEquals("CREATE_DATABASE", events.get(2).getEventType());
    eventsProcessor_.processEvents(Lists.newArrayList(events.get(2)));
    assertNotNull(catalog_.getDb(TEST_DB_NAME));
    assertEquals("second",
        catalog_.getDb(TEST_DB_NAME).getMetaStoreDb().getDescription());
  }

  private void createDatabase() throws TException { createDatabase(TEST_DB_NAME); }

  private void createDatabase(String dbName) throws TException {
    Database database =
        new DatabaseBuilder()
            .setName(dbName)
            .setDescription("Notification test database")
            .addParam("dbparamkey", "dbparamValue")
            .setOwnerName("NotificationTestOwner")
            .setOwnerType(PrincipalType.USER).build();
    try (MetaStoreClient msClient = catalog_.getMetaStoreClient()) {
      msClient.getHiveClient().createDatabase(database);
    }
  }

  private void addDatabaseParameters(String key, String val) throws TException {
    try (MetaStoreClient msClient = catalog_.getMetaStoreClient()) {
      Database msDb = msClient.getHiveClient().getDatabase(TEST_DB_NAME);
      assertFalse(key + " already exists in the database parameters",
          msDb.getParameters().containsKey(key));
      msDb.putToParameters(key, val);
      msClient.getHiveClient().alterDatabase(TEST_DB_NAME, msDb);
    }
  }

  private void alterDatabase(Database newDatabase) throws TException {
    try (MetaStoreClient msClient = catalog_.getMetaStoreClient()) {
      msClient.getHiveClient().alterDatabase(newDatabase.getName(), newDatabase);
    }
  }

  private void createTable(String tblName, boolean isPartitioned) throws TException {
    TableBuilder tblBuilder =
        new TableBuilder()
            .setTableName(tblName)
            .setDbName(TEST_DB_NAME)
            .addTableParam("tblParamKey", "tblParamValue")
            .addCol("c1", "string", "c1 description")
            .addCol("c2", "string", "c2 description")
            .setSerdeLib(HdfsFileFormat.PARQUET.serializationLib())
            .setInputFormat(HdfsFileFormat.PARQUET.inputFormat())
            .setOutputFormat(HdfsFileFormat.PARQUET.outputFormat());
    if (isPartitioned) {
      tblBuilder.addPartCol("p1", "string", "partition p1 description");
    }

    try (MetaStoreClient msClient = catalog_.getMetaStoreClient()) {
      msClient.getHiveClient().createTable(tblBuilder.build());
    }
  }

  /**
   * Drops table from Impala
   */
  private void dropTableFromImpala(String tblName) throws ImpalaException {
    TDdlExecRequest req = new TDdlExecRequest();
    req.setDdl_type(TDdlType.DROP_TABLE);
    TDropTableOrViewParams dropTableParams = new TDropTableOrViewParams();
    dropTableParams.setTable_name(new TTableName(TEST_DB_NAME, tblName));
    dropTableParams.setIf_exists(true);
    req.setDrop_table_or_view_params(dropTableParams);
    catalogOpExecutor_.execDdlRequest(req);
  }

  /**
   * Creates db from Impala
   */
  private void createDatabaseFromImpala(String dbName, String desc)
      throws ImpalaException {
    TDdlExecRequest req = new TDdlExecRequest();
    req.setDdl_type(TDdlType.CREATE_DATABASE);
    TCreateDbParams createDbParams = new TCreateDbParams();
    createDbParams.setDb(dbName);
    createDbParams.setComment(desc);
    req.setCreate_db_params(createDbParams);
    catalogOpExecutor_.execDdlRequest(req);
  }

  /**
   * Drops db from Impala
   */
  private void dropDatabaseFromImpala(String dbName) throws ImpalaException {
    TDdlExecRequest req = new TDdlExecRequest();
    req.setDdl_type(TDdlType.DROP_DATABASE);
    TDropDbParams dropDbParams = new TDropDbParams();
    dropDbParams.setDb(dbName);
    req.setDrop_db_params(dropDbParams);
    catalogOpExecutor_.execDdlRequest(req);
  }

  /**
   * Creates a table using CatalogOpExecutor to simulate a DDL operation from Impala
   * client
   */
  private void createTableFromImpala(String tblName, boolean isPartitioned)
      throws ImpalaException {
    TDdlExecRequest req = new TDdlExecRequest();
    req.setDdl_type(TDdlType.CREATE_TABLE);
    TCreateTableParams createTableParams = new TCreateTableParams();
    createTableParams.setTable_name(new TTableName(TEST_DB_NAME, tblName));
    createTableParams.setFile_format(THdfsFileFormat.PARQUET);
    createTableParams.setIs_external(false);
    createTableParams.setIf_not_exists(false);
    Map<String, String> properties = new HashMap<>();
    properties.put("tblParamKey", "tblParamValue");
    createTableParams.setTable_properties(properties);
    List<TColumn> columns = new ArrayList<>(2);
    columns.add(getScalarColumn("c1", TPrimitiveType.STRING));
    columns.add(getScalarColumn("c2", TPrimitiveType.STRING));
    createTableParams.setColumns(columns);
    // create two partition columns if specified
    if (isPartitioned) {
      List<TColumn> partitionColumns = new ArrayList<>(2);
      partitionColumns.add(getScalarColumn("p1", TPrimitiveType.INT));
      partitionColumns.add(getScalarColumn("p2", TPrimitiveType.STRING));
      createTableParams.setPartition_columns(partitionColumns);
    }
    req.setCreate_table_params(createTableParams);
    catalogOpExecutor_.execDdlRequest(req);
  }

  /**
   * Renames a table from oldTblName to newTblName from Impala
   */
  private void renameTableFromImpala(String oldTblName, String newTblName)
      throws ImpalaException {
    TDdlExecRequest req = new TDdlExecRequest();
    req.setDdl_type(TDdlType.ALTER_TABLE);
    TAlterTableOrViewRenameParams renameParams = new TAlterTableOrViewRenameParams();
    renameParams.new_table_name = new TTableName(TEST_DB_NAME, newTblName);
    TAlterTableParams alterTableParams = new TAlterTableParams();
    alterTableParams.setAlter_type(TAlterTableType.RENAME_TABLE);
    alterTableParams.setTable_name(new TTableName(TEST_DB_NAME, oldTblName));
    alterTableParams.setRename_params(renameParams);
    req.setAlter_table_params(alterTableParams);
    catalogOpExecutor_.execDdlRequest(req);
  }

  private TColumn getScalarColumn(String colName, TPrimitiveType type) {
    TTypeNode tTypeNode = new TTypeNode(TTypeNodeType.SCALAR);
    tTypeNode.setScalar_type(new TScalarType(type));
    TColumnType columnType = new TColumnType(Arrays.asList(tTypeNode));
    return new TColumn(colName, columnType);
  }

  private void dropTable(String tableName) throws TException {
    try (MetaStoreClient client = catalog_.getMetaStoreClient()) {
      client.getHiveClient().dropTable(TEST_DB_NAME, tableName, true, false);
    }
  }

  private void alterTableRename(String tblName, String newTblName) throws TException {
    try (MetaStoreClient msClient = catalog_.getMetaStoreClient()) {
      org.apache.hadoop.hive.metastore.api.Table msTable =
          msClient.getHiveClient().getTable(TEST_DB_NAME, tblName);
      msTable.setTableName(newTblName);
      msClient.getHiveClient().alter_table_with_environmentContext(
          TEST_DB_NAME, tblName, msTable, null);
    }
  }

  private void alterTableAddParameter(String tblName, String key, String val)
      throws TException {
    try (MetaStoreClient msClient = catalog_.getMetaStoreClient()) {
      org.apache.hadoop.hive.metastore.api.Table msTable =
          msClient.getHiveClient().getTable(TEST_DB_NAME, tblName);
      msTable.getParameters().put(key, val);
      msClient.getHiveClient().alter_table_with_environmentContext(
          TEST_DB_NAME, tblName, msTable, null);
    }
  }

  private void alterTableAddCol(
      String tblName, String colName, String colType, String comment) throws TException {
    try (MetaStoreClient msClient = catalog_.getMetaStoreClient()) {
      org.apache.hadoop.hive.metastore.api.Table msTable =
          msClient.getHiveClient().getTable(TEST_DB_NAME, tblName);
      msTable.getSd().getCols().add(new FieldSchema(colName, colType, comment));
      msClient.getHiveClient().alter_table_with_environmentContext(
          TEST_DB_NAME, tblName, msTable, null);
    }
  }

  private void altertableChangeCol(
      String tblName, String colName, String colType, String comment) throws TException {
    try (MetaStoreClient msClient = catalog_.getMetaStoreClient()) {
      org.apache.hadoop.hive.metastore.api.Table msTable =
          msClient.getHiveClient().getTable(TEST_DB_NAME, tblName);
      FieldSchema targetCol = null;
      for (FieldSchema col : msTable.getSd().getCols()) {
        if (col.getName().equalsIgnoreCase(colName)) {
          targetCol = col;
          break;
        }
      }
      assertNotNull("Column " + colName + " does not exist", targetCol);
      targetCol.setName(colName);
      targetCol.setType(colType);
      targetCol.setComment(comment);
      msClient.getHiveClient().alter_table_with_environmentContext(
          TEST_DB_NAME, tblName, msTable, null);
    }
  }

  private void alterTableRemoveCol(String tblName, String colName) throws TException {
    try (MetaStoreClient msClient = catalog_.getMetaStoreClient()) {
      org.apache.hadoop.hive.metastore.api.Table msTable =
          msClient.getHiveClient().getTable(TEST_DB_NAME, tblName);
      FieldSchema targetCol = null;
      for (FieldSchema col : msTable.getSd().getCols()) {
        if (col.getName().equalsIgnoreCase(colName)) {
          targetCol = col;
          break;
        }
      }
      assertNotNull("Column " + colName + " does not exist", targetCol);
      msTable.getSd().getCols().remove(targetCol);
      msClient.getHiveClient().alter_table_with_environmentContext(
          TEST_DB_NAME, tblName, msTable, null);
    }
  }

  /**
   * Removes the partition by values from HMS
   * @param tblName
   * @param partitionValues
   * @throws TException
   */
  private void dropPartitions(String tblName, List<List<String>> partitionValues)
      throws TException {
    try (MetaStoreClient metaStoreClient = catalog_.getMetaStoreClient()) {
      for (List<String> partVals : partitionValues) {
        metaStoreClient.getHiveClient().dropPartition(TEST_DB_NAME, tblName,
            partVals, true);
      }
    }
  }

  private void alterPartitions(String tblName, List<List<String>> partValsList,
      Map<String, String> newParams)
      throws TException {
    GetPartitionsRequest request = new GetPartitionsRequest();
    request.setDbName(TEST_DB_NAME);
    List<Partition> partitions = new ArrayList<>();
    try (MetaStoreClient metaStoreClient = catalog_.getMetaStoreClient()) {
      for (List<String> partVal : partValsList) {
        Partition partition = metaStoreClient.getHiveClient().getPartition(TEST_DB_NAME,
            tblName,
            partVal);
        partition.setParameters(newParams);
        partitions.add(partition);
      }

      metaStoreClient.getHiveClient().alter_partitions(TEST_DB_NAME, tblName, partitions);
    }
  }

  private void addPartitions(String tblName, List<List<String>> partitionValues)
      throws TException {
    int i = 0;
    List<Partition> partitions = new ArrayList<>(partitionValues.size());
    try (MetaStoreClient msClient = catalog_.getMetaStoreClient()) {
      org.apache.hadoop.hive.metastore.api.Table msTable =
          msClient.getHiveClient().getTable(TEST_DB_NAME, tblName);
      for (List<String> partVals : partitionValues) {
        partitions.add(
            new PartitionBuilder()
                .fromTable(msTable)
                .setInputFormat(msTable.getSd().getInputFormat())
                .setSerdeLib(msTable.getSd().getSerdeInfo().getSerializationLib())
                .setOutputFormat(msTable.getSd().getOutputFormat())
                .setValues(partVals)
                .build());
      }
    }
    try (MetaStoreClient metaStoreClient = catalog_.getMetaStoreClient()) {
      metaStoreClient.getHiveClient().add_partitions(partitions);
    }
  }

  private Table loadTable(String tblName) throws CatalogException {
    Table loadedTable = catalog_.getOrLoadTable(TEST_DB_NAME, tblName);
    assertFalse("Table should have been loaded after getOrLoadTable call",
        loadedTable instanceof IncompleteTable);
    return loadedTable;
  }
}
