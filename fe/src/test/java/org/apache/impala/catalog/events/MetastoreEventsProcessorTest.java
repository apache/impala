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
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.when;

import com.google.common.base.Preconditions;
import com.google.common.collect.Iterables;
import com.google.common.io.Files;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.commons.lang.RandomStringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.hive.common.ValidWriteIdList;
import org.apache.hadoop.hive.metastore.api.Catalog;
import org.apache.hadoop.hive.metastore.api.CurrentNotificationEventId;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.InsertEventRequestData;
import org.apache.hadoop.hive.metastore.api.NotificationEvent;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.PrincipalType;
import org.apache.hadoop.hive.metastore.IMetaStoreClient.NotificationFilter;
import org.apache.hadoop.hive.metastore.messaging.AlterTableMessage;
import org.apache.hadoop.io.IOUtils;
import org.apache.impala.analysis.FunctionName;
import org.apache.impala.analysis.HdfsUri;
import org.apache.impala.authorization.AuthorizationConfig;
import org.apache.impala.authorization.AuthorizationManager;
import org.apache.impala.authorization.NoopAuthorizationFactory;
import org.apache.impala.authorization.NoopAuthorizationFactory.NoopAuthorizationManager;
import org.apache.impala.catalog.CatalogException;
import org.apache.impala.catalog.CatalogServiceCatalog;
import org.apache.impala.catalog.DatabaseNotFoundException;
import org.apache.impala.catalog.Db;
import org.apache.impala.catalog.FeCatalogUtils;
import org.apache.impala.catalog.FeFsPartition;
import org.apache.impala.catalog.HdfsPartition;
import org.apache.impala.catalog.HdfsTable;
import org.apache.impala.catalog.IncompleteTable;
import org.apache.impala.catalog.MetaStoreClientPool;
import org.apache.impala.catalog.MetaStoreClientPool.MetaStoreClient;
import org.apache.impala.catalog.MetastoreApiTestUtils;
import org.apache.impala.catalog.ScalarFunction;
import org.apache.impala.catalog.Table;
import org.apache.impala.catalog.TableWriteId;
import org.apache.impala.catalog.Type;
import org.apache.impala.catalog.events.ConfigValidator.ValidationResult;
import org.apache.impala.catalog.events.MetastoreEvents.AlterDatabaseEvent;
import org.apache.impala.catalog.events.MetastoreEvents.DropDatabaseEvent;
import org.apache.impala.catalog.events.MetastoreEvents.AlterPartitionEvent;
import org.apache.impala.catalog.events.MetastoreEvents.BatchPartitionEvent;
import org.apache.impala.catalog.events.MetastoreEvents.AlterTableEvent;
import org.apache.impala.catalog.events.MetastoreEvents.InsertEvent;
import org.apache.impala.catalog.events.MetastoreEvents.MetastoreEvent;
import org.apache.impala.catalog.events.MetastoreEvents.MetastoreEventFactory;
import org.apache.impala.catalog.events.MetastoreEvents.MetastoreEventPropertyKey;
import org.apache.impala.catalog.events.MetastoreEvents.MetastoreEventType;
import org.apache.impala.catalog.events.MetastoreEventsProcessor.EventProcessorStatus;
import org.apache.impala.catalog.events.MetastoreEventsProcessor.MetaDataFilter;
import org.apache.impala.catalog.FileMetadataLoadOpts;
import org.apache.impala.common.FileSystemUtil;
import org.apache.impala.common.ImpalaException;
import org.apache.impala.common.Pair;
import org.apache.impala.common.Reference;
import org.apache.impala.common.TransactionException;
import org.apache.impala.compat.MetastoreShim;
import org.apache.impala.service.BackendConfig;
import org.apache.impala.hive.executor.HiveJavaFunctionFactory;
import org.apache.impala.hive.executor.TestHiveJavaFunctionFactory;
import org.apache.impala.service.CatalogOpExecutor;
import org.apache.impala.service.FeSupport;
import org.apache.impala.testutil.CatalogServiceTestCatalog;
import org.apache.impala.testutil.HiveJdbcClientPool;
import org.apache.impala.testutil.IncompetentMetastoreClientPool;
import org.apache.impala.testutil.TestUtils;
import org.apache.impala.thrift.TAlterDbParams;
import org.apache.impala.thrift.TAlterDbSetOwnerParams;
import org.apache.impala.thrift.TAlterDbType;
import org.apache.impala.thrift.TAlterTableAddColsParams;
import org.apache.impala.thrift.TAlterTableAddPartitionParams;
import org.apache.impala.thrift.TAlterTableDropColParams;
import org.apache.impala.thrift.TAlterTableDropPartitionParams;
import org.apache.impala.thrift.TAlterTableOrViewRenameParams;
import org.apache.impala.thrift.TAlterTableOrViewSetOwnerParams;
import org.apache.impala.thrift.TAlterTableParams;
import org.apache.impala.thrift.TAlterTableReplaceColsParams;
import org.apache.impala.thrift.TAlterTableSetFileFormatParams;
import org.apache.impala.thrift.TAlterTableSetLocationParams;
import org.apache.impala.thrift.TAlterTableSetRowFormatParams;
import org.apache.impala.thrift.TAlterTableSetTblPropertiesParams;
import org.apache.impala.thrift.TAlterTableType;
import org.apache.impala.thrift.TAlterTableUpdateStatsParams;
import org.apache.impala.thrift.TBackendGflags;
import org.apache.impala.thrift.TCatalogServiceRequestHeader;
import org.apache.impala.thrift.TColumn;
import org.apache.impala.thrift.TColumnType;
import org.apache.impala.thrift.TCreateDbParams;
import org.apache.impala.thrift.TCreateFunctionParams;
import org.apache.impala.thrift.TCreateTableLikeParams;
import org.apache.impala.thrift.TCreateTableParams;
import org.apache.impala.thrift.TDdlExecRequest;
import org.apache.impala.thrift.TDdlQueryOptions;
import org.apache.impala.thrift.TDdlType;
import org.apache.impala.thrift.TDropDbParams;
import org.apache.impala.thrift.TDropFunctionParams;
import org.apache.impala.thrift.TDropTableOrViewParams;
import org.apache.impala.thrift.TEventProcessorMetrics;
import org.apache.impala.thrift.TEventProcessorMetricsSummaryResponse;
import org.apache.impala.thrift.TFunctionBinaryType;
import org.apache.impala.thrift.THdfsFileFormat;
import org.apache.impala.thrift.TOwnerType;
import org.apache.impala.thrift.TPartitionDef;
import org.apache.impala.thrift.TPartitionKeyValue;
import org.apache.impala.thrift.TPartitionStats;
import org.apache.impala.thrift.TPrimitiveType;
import org.apache.impala.thrift.TScalarType;
import org.apache.impala.thrift.TTableName;
import org.apache.impala.thrift.TTablePropertyType;
import org.apache.impala.thrift.TTableRowFormat;
import org.apache.impala.thrift.TTableStats;
import org.apache.impala.thrift.TTypeNode;
import org.apache.impala.thrift.TTypeNodeType;
import org.apache.impala.thrift.TUpdateCatalogRequest;
import org.apache.impala.thrift.TUpdatedPartition;
import org.apache.impala.util.EventSequence;
import org.apache.impala.util.MetaStoreUtil;
import org.apache.impala.util.NoOpEventSequence;
import org.apache.thrift.TException;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;

import org.mockito.Mockito;

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
  private static final String TEST_DB_NAME = "events_test_db";

  private static CatalogServiceTestCatalog catalog_;
  private static CatalogOpExecutor catalogOpExecutor_;
  private static MetastoreEventsProcessor eventsProcessor_;
  private static final Logger LOG =
      LoggerFactory.getLogger(MetastoreEventsProcessorTest.class);

  @BeforeClass
  public static void setUpTestEnvironment() throws TException, ImpalaException {
    catalog_ = CatalogServiceTestCatalog.create();
    catalogOpExecutor_ = catalog_.getCatalogOpExecutor();
    try (MetaStoreClient metaStoreClient = catalog_.getMetaStoreClient()) {
      CurrentNotificationEventId currentNotificationId =
          metaStoreClient.getHiveClient().getCurrentNotificationEventId();
      eventsProcessor_ = new SynchronousHMSEventProcessorForTests(
          catalogOpExecutor_, currentNotificationId.getEventId(), 10L);
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
    } finally {
      if (eventsProcessor_ != null) {
        eventsProcessor_.shutdown();
      }
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

  private static void dropDatabaseCascade(String catName, String dbName)
      throws TException {
    try (MetaStoreClient msClient = catalog_.getMetaStoreClient()) {
      msClient.getHiveClient().dropDatabase(catName, dbName, true, true, true);
    }
  }

  /**
   * Cleans up the test database from both metastore and catalog
   * @throws TException
   */
  @Before
  public void beforeTest() throws Exception {
    try (MetaStoreClient msClient = catalog_.getMetaStoreClient()) {
      msClient.getHiveClient().dropDatabase(TEST_DB_NAME, true, true, true);
    }
    catalog_.removeDb(TEST_DB_NAME);
    // reset the event processor to the current eventId
    eventsProcessor_.pause();
    eventsProcessor_.start(eventsProcessor_.getCurrentEventId());
    eventsProcessor_.processEvents();
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
   * Test each Metastore config required for event processing. This test just validates
   * that if event processor starts, the required configs are set.
   */
  @Test
  public void testConfigValidation() throws CatalogException {
    assertEquals(EventProcessorStatus.ACTIVE, eventsProcessor_.getStatus());
    eventsProcessor_.validateConfigs();
  }

  /**
   * Test if validateConfigs() works as expected. If more than one configuration keys
   * in metastore are incorrect, we present all of those incorrect results together so
   * users can change them in one go. This test will assert the number of failed
   * configurations for Hive major version >=2 or otherwise.
   * @throws TException
   */
  @Test
  public void testValidateConfig() throws TException {
    MetastoreEventsProcessor mockMetastoreEventsProcessor =
        Mockito.spy(eventsProcessor_);
    for (MetastoreEventProcessorConfig config: MetastoreEventProcessorConfig.values()) {
      String configKey = config.getValidator().getConfigKey();
      when(mockMetastoreEventsProcessor.getConfigValueFromMetastore(configKey,
          "")).thenReturn("false");
      if (config.equals(MetastoreEventProcessorConfig.METASTORE_DEFAULT_CATALOG_NAME)) {
        when(mockMetastoreEventsProcessor.getConfigValueFromMetastore(configKey,
            "")).thenReturn("test_custom_catalog");
      }
    }
    try {
      mockMetastoreEventsProcessor.validateConfigs();
    } catch (CatalogException e) {
      String errorMessage = "Found %d incorrect metastore configuration(s).";
      // Use MetastoreShim to determine the number of failed configs.
      assertTrue(e.getMessage().contains(String.format(errorMessage,
          MetastoreEventsProcessor.getEventProcessorConfigsToValidate().size())));
    }
  }

  @Test
  public void testNextMetastoreEvents() throws Exception {
    long currentEventId = eventsProcessor_.getCurrentEventId();
    createDatabaseFromImpala(TEST_DB_NAME, null);
    createTableFromImpala(TEST_DB_NAME, "testNextMetastoreEvents1", false);
    createTable("testNextMetastoreEvents2", false);
    List<NotificationEvent> events =
        MetastoreEventsProcessor.getNextMetastoreEventsInBatches(
        eventsProcessor_.catalog_, currentEventId, /*filter*/null, 2);
    assertEquals(3, events.size());
    events = MetastoreEventsProcessor.getNextMetastoreEventsInBatches(
        eventsProcessor_.catalog_, currentEventId+1, /*filter*/null, 10);
    assertEquals(2, events.size());
    events = MetastoreEventsProcessor.getNextMetastoreEventsInBatches(
        eventsProcessor_.catalog_, currentEventId, /*filter*/null, 3);
    assertEquals(3, events.size());
    events = MetastoreEventsProcessor.getNextMetastoreEventsInBatches(
        eventsProcessor_.catalog_, currentEventId+3, /*filter*/null, 3);
    assertEquals(0, events.size());
  }

  /**
   * Test provides a mock value and confirms if the MetastoreEventConfig validate
   * suceeds or fails as expected
   */
  @Test
  public void testConfigValidationWithIncorrectValues() {
    Map<MetastoreEventProcessorConfig, String> incorrectValues = new HashMap<>();
    incorrectValues.put(MetastoreEventProcessorConfig.FIRE_EVENTS_FOR_DML, "false");
    incorrectValues
        .put(MetastoreEventProcessorConfig.METASTORE_DEFAULT_CATALOG_NAME, "custom");
    for (MetastoreEventProcessorConfig config : incorrectValues.keySet()) {
      testConfigValidation(config, incorrectValues.get(config), false);
    }
    testConfigValidation(MetastoreEventProcessorConfig.FIRE_EVENTS_FOR_DML,
        "true", true);
    testConfigValidation(MetastoreEventProcessorConfig.FIRE_EVENTS_FOR_DML,
        "TRUE", true);
    testConfigValidation(MetastoreEventProcessorConfig.FIRE_EVENTS_FOR_DML,
        "tRue", true);
    testConfigValidation(MetastoreEventProcessorConfig.METASTORE_DEFAULT_CATALOG_NAME,
        "hIve", true);
  }

  private void testConfigValidation(MetastoreEventProcessorConfig config,
      String mockValue, boolean expectSuccess) {
    ValidationResult result = config.validate(mockValue);
    assertNotNull(result);
    if (expectSuccess) {
      assertTrue(result.isValid());
    } else {
      assertFalse(result.isValid());
      assertNotNull(result.getReason());
    }
  }

  /**
   * Checks that database exists after processing a CREATE_DATABASE event
   */
  @Test
  public void testCreateDatabaseEvent() throws TException, ImpalaException {
    createDatabase(TEST_DB_NAME, null);
    eventsProcessor_.processEvents();
    assertNotNull(catalog_.getDb(TEST_DB_NAME));
  }

  @Test
  public void testIgnoreNonDefaultCatalogs() throws Exception {
    String catName = "custom_hive_catalog";
    try {
      dropHiveCatalogIfExists(catName);
      createHiveCatalog(catName);
      createDatabase(TEST_DB_NAME, null);
      String tblName = "test";
      createTable(tblName, false);
      eventsProcessor_.processEvents();
      // create a database and table in the custom hive catalog whose name matches
      // with one already existing in Impala
      createDatabase(catName, TEST_DB_NAME, null);
      createTable(catName, TEST_DB_NAME, tblName, null, false, null);
      eventsProcessor_.processEvents();
      assertEquals(EventProcessorStatus.ACTIVE, eventsProcessor_.getStatus());
      // assert that dbname and table in the default catalog exist
      assertNotNull(catalog_.getDb(TEST_DB_NAME));
      assertNotNull(catalog_.getTable(TEST_DB_NAME, tblName));
      dropDatabaseCascade(catName, TEST_DB_NAME);
      // when a catalog is created a default database is also created within it
      dropDatabaseCascade(catName, "default");
      dropHiveCatalogIfExists(catName);
      eventsProcessor_.processEvents();
      assertEquals(EventProcessorStatus.ACTIVE, eventsProcessor_.getStatus());
      // assert that dbname and table in the default catalog exist
      assertNotNull(catalog_.getDb(TEST_DB_NAME));
      assertNotNull(catalog_.getTable(TEST_DB_NAME, tblName));
    } finally {
      dropHiveCatalogIfExists(catName);
    }
  }

  /**
   * Checks that Db object does not exist after processing DROP_DATABASE event when the
   * dropped database is empty
   */
  @Test
  public void testDropEmptyDatabaseEvent() throws TException, ImpalaException {
    dropDatabaseCascade("database_to_be_dropped");
    // create empty database
    createDatabase("database_to_be_dropped", null);
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
    createDatabase(TEST_DB_NAME, null);
    String tblToBeDropped = "tbl_to_be_dropped";
    createTable(tblToBeDropped, true);
    createTable("tbl_to_be_dropped_unpartitioned", false);

    // create 2 partitions
    List<List<String>> partVals = new ArrayList<>(2);
    partVals.add(Arrays.asList("1"));
    partVals.add(Arrays.asList("2"));
    addPartitions(TEST_DB_NAME, tblToBeDropped, partVals);
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

  /**
   * DROP_DATABASE uses CREATION_TIME to filter events that try to drop an earlier version
   * of DB, hence this test is to verify that the sequence of operations CREATE_DB,
   * DROP_DB, CREATE_DB from Hive will produce expected result in Impala's Catalog.
   */
  @Test
  public void testCreateDropCreateDatabase() throws TException {
    createDatabase(TEST_DB_NAME, null);
    dropDatabaseCascade(TEST_DB_NAME);
    createDatabase(TEST_DB_NAME, null);
    eventsProcessor_.processEvents();
    // Check that the database exists in Catalog
    assertNotNull(catalog_.getDb(TEST_DB_NAME));
  }

  @Test
  public void testAlterDatabaseEvents() throws TException, ImpalaException {
    createDatabase(TEST_DB_NAME, null);
    String testDbParamKey = "testKey";
    String testDbParamVal = "testVal";
    eventsProcessor_.processEvents();
    Db db = catalog_.getDb(TEST_DB_NAME);
    assertNotNull(db);
    // db parameters should not have the test key
    assertTrue("Newly created test database should not have parameter with key"
            + testDbParamKey,
        !db.getMetaStoreDb().isSetParameters() || !db.getMetaStoreDb().getParameters()
            .containsKey(testDbParamKey));
    // test change of parameters to the Database
    addDatabaseParameters(testDbParamKey, testDbParamVal);
    eventsProcessor_.processEvents();
    String getParamValFromDb =
        catalog_.getDb(TEST_DB_NAME).getMetaStoreDb().getParameters().get(testDbParamKey);
    assertTrue("Altered database should have set the key " + testDbParamKey + " to value "
            + testDbParamVal + " in parameters, instead we get " + getParamValFromDb,
        testDbParamVal.equals(getParamValFromDb));

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

  /*
   * Test to verify alter Db from Impala works fine and self events are caught
   * successfully
   */
  @Test
  public void testAlterDatabaseSetOwnerFromImpala() throws ImpalaException {
    assertEquals(EventProcessorStatus.ACTIVE, eventsProcessor_.getStatus());
    createDatabaseFromImpala(TEST_DB_NAME, null);
    assertNotNull("Db should have been found after create database statement",
        catalog_.getDb(TEST_DB_NAME));
    eventsProcessor_.processEvents();
    Db db = catalog_.getDb(TEST_DB_NAME);
    long createEventId = db.getCreateEventId();
    long beforeLastSyncedEventId = db.getLastSyncedEventId();
    if (BackendConfig.INSTANCE.enableSyncToLatestEventOnDdls()) {
      assertEquals(createEventId, beforeLastSyncedEventId);
    }

    long numberOfSelfEventsBefore =
        eventsProcessor_.getMetrics()
            .getCounter(MetastoreEventsProcessor.EVENTS_SKIPPED_METRIC)
            .getCount();
    String owner = catalog_.getDb(TEST_DB_NAME).getMetaStoreDb().getOwnerName();
    String newOwnerUser = "newUserFromImpala";
    String newOwnerRole = "newRoleFromImpala";
    assertFalse(newOwnerUser.equals(owner) || newOwnerRole.equals(owner));
    alterDbSetOwnerFromImpala(TEST_DB_NAME, newOwnerUser, TOwnerType.USER);
    eventsProcessor_.processEvents();
    assertEquals(
        newOwnerUser, catalog_.getDb(TEST_DB_NAME).getMetaStoreDb().getOwnerName());

    if (BackendConfig.INSTANCE.enableSyncToLatestEventOnDdls()) {
      // self events should reset last synced event id
      assertTrue(catalog_.getDb(TEST_DB_NAME).getLastSyncedEventId() >
          beforeLastSyncedEventId);
    }
    beforeLastSyncedEventId = db.getLastSyncedEventId();

    alterDbSetOwnerFromImpala(TEST_DB_NAME, newOwnerRole, TOwnerType.ROLE);
    eventsProcessor_.processEvents();
    assertEquals(
        newOwnerRole, catalog_.getDb(TEST_DB_NAME).getMetaStoreDb().getOwnerName());

    if (BackendConfig.INSTANCE.enableSyncToLatestEventOnDdls()) {
      assertTrue(catalog_.getDb(TEST_DB_NAME).getLastSyncedEventId() >
          beforeLastSyncedEventId);
    }

    long selfEventsCountAfter =
        eventsProcessor_.getMetrics()
            .getCounter(MetastoreEventsProcessor.EVENTS_SKIPPED_METRIC)
            .getCount();
    // 2 alter commands above, so we expect the count to go up by 2
    assertEquals("Unexpected number of self-events generated",
        numberOfSelfEventsBefore + 2, selfEventsCountAfter);
  }

  /**
   * Test empty alter database events generated by operations like create function.
   */
  @Test
  public void testEmptyAlterDatabaseEventsFromImpala() throws ImpalaException {
    assertEquals(EventProcessorStatus.ACTIVE, eventsProcessor_.getStatus());
    createDatabaseFromImpala(TEST_DB_NAME, null);
    assertNotNull("Db should have been found after create database statement",
        catalog_.getDb(TEST_DB_NAME));
    eventsProcessor_.processEvents();
    long numberOfSelfEventsBefore =
        eventsProcessor_.getMetrics()
            .getCounter(MetastoreEventsProcessor.EVENTS_SKIPPED_METRIC).getCount();

    // Create a dummy scalar function.
    String fnName = "fn1";
    ScalarFunction fn1 = new ScalarFunction(new FunctionName(TEST_DB_NAME, fnName),
        new ArrayList<Type>(){{ add(Type.STRING);}}, Type.INT, false);
    fn1.setBinaryType(TFunctionBinaryType.JAVA);
    fn1.setLocation(new HdfsUri("hdfs://foo:bar/fn/fn1.jar"));
    fn1.setSymbolName("FnClass");

    // Verify alter database events generated by create function and drop function are
    // identified as self-event.
    createScalarFunctionFromImpala(fn1);
    dropScalarFunctionFromImapala(fn1);
    eventsProcessor_.processEvents();
    long numberOfSelfEventsAfter =
        eventsProcessor_.getMetrics()
            .getCounter(MetastoreEventsProcessor.EVENTS_SKIPPED_METRIC).getCount();
    assertEquals("Unexpected number of self-events generated",
        numberOfSelfEventsBefore + 2, numberOfSelfEventsAfter);

    // Verify alter database on a dropped database does not put event processor in an
    // error state.
    createScalarFunctionFromImpala(fn1);
    dropDatabaseCascadeFromImpala(TEST_DB_NAME);
    eventsProcessor_.processEvents();
    assertEquals(EventProcessorStatus.ACTIVE, eventsProcessor_.getStatus());
  }

  /**
   * Test creates two table (partitioned and non-partitioned) and makes sure that CatalogD
   * has the two created table objects after the CREATE_TABLE events are processed.
   */
  @Test
  public void testCreateTableEvent() throws TException, ImpalaException {
    createDatabase(TEST_DB_NAME, null);
    final String testTblName = "testCreateTableEvent";
    eventsProcessor_.processEvents();
    assertNull(testTblName + " is not expected to exist",
        catalog_.getTable(TEST_DB_NAME, testTblName));
    // create a non-partitioned table
    createTable(testTblName, false);

    eventsProcessor_.processEvents();
    assertNotNull("Catalog should have a incomplete instance of table after CREATE_TABLE "
            + "event is received",
        catalog_.getTable(TEST_DB_NAME, testTblName));
    assertTrue("Newly created table from events should be a IncompleteTable",
        catalog_.getTable(TEST_DB_NAME, testTblName)
                instanceof IncompleteTable);
    // test partitioned table case
    final String testPartitionedTbl = "testCreateTableEventPartitioned";
    createTable(testPartitionedTbl, true);

    eventsProcessor_.processEvents();
    assertNotNull("Catalog should have create a incomplete table after receiving "
            + "CREATE_TABLE event",
        catalog_.getTable(TEST_DB_NAME, testPartitionedTbl));
    assertTrue("Newly created table should be instance of IncompleteTable",
        catalog_.getTable(TEST_DB_NAME, testPartitionedTbl)
                instanceof IncompleteTable);

    // Test create table on a drop database event.
    dropDatabaseCascadeFromImpala(TEST_DB_NAME);
    assertNull("Database not expected to exist.", catalog_.getDb(TEST_DB_NAME));
    createDatabaseFromImpala(TEST_DB_NAME, null);
    eventsProcessor_.processEvents();
    createTable("createondroppeddb", false);

    dropDatabaseCascadeFromImpala(TEST_DB_NAME);
    eventsProcessor_.processEvents();
    assertEquals(EventProcessorStatus.ACTIVE, eventsProcessor_.getStatus());

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
    createDatabase(TEST_DB_NAME, null);
    final String testTblName = "testPartitionEvents";
    createTable(testTblName, true);
    // sync to latest event id
    eventsProcessor_.processEvents();

    // simulate the table being loaded by explicitly calling load table
    loadTable(testTblName);
    List<List<String>> partVals = new ArrayList<>();

    // create 4 partitions
    partVals.add(Arrays.asList("1"));
    partVals.add(Arrays.asList("2"));
    partVals.add(Arrays.asList("3"));
    partVals.add(Arrays.asList("4"));
    addPartitions(TEST_DB_NAME, testTblName, partVals);

    eventsProcessor_.processEvents();
    assertEquals("Unexpected number of partitions fetched for the loaded table", 4,
        ((HdfsTable) catalog_.getTable(TEST_DB_NAME, testTblName))
            .getPartitions()
            .size());

    // now remove some partitions to see if catalogD state gets invalidated
    partVals.clear();
    partVals.add(Arrays.asList("1"));
    partVals.add(Arrays.asList("2"));
    partVals.add(Arrays.asList("3"));
    dropPartitions(testTblName, partVals);
    eventsProcessor_.processEvents();

    assertEquals("Unexpected number of partitions fetched for the loaded table", 1,
        ((HdfsTable) catalog_.getTable(TEST_DB_NAME, testTblName))
            .getPartitions().size());

    // issue alter partition ops
    partVals.clear();
    partVals.add(Arrays.asList("4"));
    String newLocation = "/path/to/location/";
    alterPartitions(testTblName, partVals, newLocation);
    eventsProcessor_.processEvents();

    Collection<? extends FeFsPartition> parts =
        FeCatalogUtils.loadAllPartitions((HdfsTable)
            catalog_.getTable(TEST_DB_NAME, testTblName));
    FeFsPartition singlePartition =
        Iterables.getOnlyElement(parts);
    assertTrue(newLocation.equals(singlePartition.getLocation()));

    // Test if trivial partition inserts are skipped. Verify that changing parameters
    // from TableInvalidatingEvent.parametersToIgnore doesn't trigger a refresh.
    List partitionValue = Arrays.asList("4");
    alterPartitionsTrivial(testTblName, partitionValue );
    eventsProcessor_.processEvents();

    Collection<? extends FeFsPartition> partsAfterTrivialAlter =
        FeCatalogUtils.loadAllPartitions((HdfsTable)
            catalog_.getTable(TEST_DB_NAME, testTblName));
    FeFsPartition singlePartitionAfterTrivialAlter =
        Iterables.getOnlyElement(partsAfterTrivialAlter);
    for (String parameter : MetastoreEvents.parametersToIgnore) {
      assertEquals("Unexpected parameter value after trivial alter partition "
          + "event", singlePartition.getParameters().get(parameter),
          singlePartitionAfterTrivialAlter.getParameters().get(parameter));
    }
  }

  /**
   * Test insert events. Test creates a partitioned and a non-partitioned table and
   * calls insertEvent tests on them.
   */
  @Test
  public void testInsertEvents() throws TException, ImpalaException, IOException {
    // Test insert into partition
    createDatabase(TEST_DB_NAME, null);
    String tableToInsertPart = "tbl_to_insert_part";
    createTable(TEST_DB_NAME, tableToInsertPart, true);
    testInsertEvents(TEST_DB_NAME, tableToInsertPart, true);

    // Test insert into table
    String tableToInsertNoPart = "tbl_to_insert_no_part";
    createTable(TEST_DB_NAME, tableToInsertNoPart, false);
    testInsertEvents(TEST_DB_NAME, tableToInsertNoPart,false);
  }

  /**
   * Tests insert events into partitioned table and asserts that
   * last synced event id is advanced after the insert
   */
  @Test
  public void testPartitionedTblInsertSyncToLatestEvent() throws Exception {
    String tblName = "test_insert_into_part_tbl_sync_to_latest_event";
    testTblInsertsSyncToLatestEvent(TEST_DB_NAME, tblName, true);
  }

  /**
   * Tests insert events into non partitioned table and asserts that
   * last synced event id is advanced after the insert
   */
  @Test
  public void testNonPartitionedTblInsertSyncToLatestEvent() throws Exception {
    String tblName = "test_insert_into_non_part_tbl_sync_to_latest_event";
    testTblInsertsSyncToLatestEvent(TEST_DB_NAME, tblName, false);
  }

  private void testTblInsertsSyncToLatestEvent(String dbName, String tblName,
      boolean isPartitioned) throws Exception {
    // Test insert into partition
    boolean prevFlagVal = BackendConfig.INSTANCE.enableSyncToLatestEventOnDdls();
    boolean invalidateHMSFlag = BackendConfig.INSTANCE.invalidateCatalogdHMSCacheOnDDLs();
    try {
      BackendConfig.INSTANCE.setEnableSyncToLatestEventOnDdls(true);
      BackendConfig.INSTANCE.setInvalidateCatalogdHMSCacheOnDDLs(false);
      createDatabase(dbName, null);
      createTable(dbName, tblName, isPartitioned);
      eventsProcessor_.processEvents();
      assertNotNull("Table " + tblName + " not present in catalog",
          catalog_.getTableNoThrow(dbName, tblName));
      long lastSyncedEventIdBefore =
          catalog_.getTable(dbName, tblName).getLastSyncedEventId();
      assertTrue("expected lastSyncedEventIdBefore to be > 0",
          lastSyncedEventIdBefore > 0);
      testInsertEvents(dbName, tblName, isPartitioned);

      Table tbl = catalog_.getTable(dbName, tblName);
      long lastSyncedEventIdAfter = tbl.getLastSyncedEventId();
      long currentEventIdHms = -1;
      try (MetaStoreClient metaStoreClient = catalog_.getMetaStoreClient()) {
        currentEventIdHms =
            metaStoreClient.getHiveClient().getCurrentNotificationEventId().getEventId();
      }
      assertTrue(String.format("for table %s, expected lastSyncedEventIdBefore %s to be "
              + "less than lastSyncedEventIdAfter %s", tblName, lastSyncedEventIdBefore,
          lastSyncedEventIdAfter), lastSyncedEventIdBefore < lastSyncedEventIdAfter);

      assertTrue(String.format("for table %s, expected lastSyncedEventIdAfter %s to be "
                  + "less than equal to currentEventIdHms %s", tblName,
              lastSyncedEventIdAfter, currentEventIdHms),
          lastSyncedEventIdAfter <= currentEventIdHms);
    } finally {
      BackendConfig.INSTANCE.setEnableSyncToLatestEventOnDdls(prevFlagVal);
      BackendConfig.INSTANCE.setInvalidateCatalogdHMSCacheOnDDLs(invalidateHMSFlag);
    }
  }

  /**
   * Test insert from impala. Insert into table and partition from impala
   * should be treated as self-event.
   */
  @Test
  public void testInsertFromImpala() throws Exception {
    Assume.assumeTrue("Skipping this test because it only works with Hive-3 or greater",
        TestUtils.getHiveMajorVersion() >= 3);
    // Test insert into multiple partitions
    createDatabaseFromImpala(TEST_DB_NAME, null);
    String tableToInsertPart = "tbl_with_mul_part";
    String tableToInsertMulPart = "tbl_to_insert_mul_part";
    createInsertTestTbls(tableToInsertPart, tableToInsertMulPart);
    eventsProcessor_.processEvents();
    // count self event from here, numberOfSelfEventsBefore=4 as we have 4 ADD PARTITION
    // events
    long numberOfSelfEventsBefore =
        eventsProcessor_.getMetrics()
            .getCounter(MetastoreEventsProcessor.EVENTS_SKIPPED_METRIC)
            .getCount();
    runInsertTest(tableToInsertPart, tableToInsertMulPart, numberOfSelfEventsBefore,
        false);
  }

  @Test
  public void testInsertOverwriteFromImpala() throws Exception {
    Assume.assumeTrue("Skipping this test because it only works with Hive-3 or greater",
        TestUtils.getHiveMajorVersion() >= 3);
    // Test insert into multiple partitions
    createDatabaseFromImpala(TEST_DB_NAME, null);
    String tableToInsertPart = "tbl_with_mul_part";
    String tableToInsertMulPart = "tbl_to_insert_mul_part";
    createInsertTestTbls(tableToInsertPart, tableToInsertMulPart);
    eventsProcessor_.processEvents();
    // count self event from here, numberOfSelfEventsBefore=4 as we have 4 ADD PARTITION
    // events
    long numberOfSelfEventsBefore =
        eventsProcessor_.getMetrics()
            .getCounter(MetastoreEventsProcessor.EVENTS_SKIPPED_METRIC)
            .getCount();
    runInsertTest(tableToInsertPart, tableToInsertMulPart, numberOfSelfEventsBefore,
        true);
  }

  private void createInsertTestTbls(String tableToInsertPart, String tableToInsertMulPart)
      throws Exception {
    createTableLike("functional", "alltypes", TEST_DB_NAME, tableToInsertPart);
    createTableLike("functional", "alltypes", TEST_DB_NAME, tableToInsertMulPart);
    // add first partition
    TPartitionDef partitionDef = new TPartitionDef();
    partitionDef.addToPartition_spec(new TPartitionKeyValue("year", "2009"));
    partitionDef.addToPartition_spec(new TPartitionKeyValue("month", "1"));
    alterTableAddPartition(TEST_DB_NAME, tableToInsertPart, partitionDef);
    alterTableAddPartition(TEST_DB_NAME, tableToInsertMulPart, partitionDef);
    // add second partition
    partitionDef = new TPartitionDef();
    partitionDef.addToPartition_spec(new TPartitionKeyValue("year", "2009"));
    partitionDef.addToPartition_spec(new TPartitionKeyValue("month", "2"));
    alterTableAddPartition(TEST_DB_NAME, tableToInsertPart, partitionDef);
    alterTableAddPartition(TEST_DB_NAME, tableToInsertMulPart, partitionDef);

    HdfsTable allTypes = (HdfsTable) catalog_
        .getOrLoadTable("functional", "alltypes", "test", null);
    HdfsTable insertTbl = (HdfsTable) catalog_
        .getOrLoadTable(TEST_DB_NAME, tableToInsertPart, "test", null);
    HdfsTable multiInsertTbl = (HdfsTable) catalog_
        .getOrLoadTable(TEST_DB_NAME, tableToInsertMulPart, "test", null);
    // copy files from the source tables so that we have some data
    copyFiles(allTypes.getFileSystem(),
        new Path(allTypes.getHdfsBaseDir() + "/year=2009/month=1"),
        insertTbl.getFileSystem(),
        new Path(insertTbl.getHdfsBaseDir() + "/year=2009/month=1"), true, null);
    copyFiles(allTypes.getFileSystem(),
        new Path(allTypes.getHdfsBaseDir() + "/year=2009/month=2"),
        insertTbl.getFileSystem(),
        new Path(insertTbl.getHdfsBaseDir() + "/year=2009/month=2"), true, null);
    copyFiles(allTypes.getFileSystem(),
        new Path(allTypes.getHdfsBaseDir() + "/year=2009/month=1"),
        multiInsertTbl.getFileSystem(),
        new Path(multiInsertTbl.getHdfsBaseDir() + "/year=2009/month=1"), true, null);
    copyFiles(allTypes.getFileSystem(),
        new Path(allTypes.getHdfsBaseDir() + "/year=2009/month=2"),
        multiInsertTbl.getFileSystem(),
        new Path(multiInsertTbl.getHdfsBaseDir() + "/year=2009/month=2"), true, null);
    // load the created tables
    EventSequence catalogTimeline = NoOpEventSequence.INSTANCE;
    catalog_.reloadTable(multiInsertTbl, "test", catalogTimeline);
    catalog_.reloadTable(insertTbl, "test", catalogTimeline);
    eventsProcessor_.processEvents();
  }

  private void runInsertTest(String tableToInsertPart, String tableToInsertMulPart,
      long numberOfSelfEventsBefore, boolean overwrite) throws Exception {
    // insert into partition
    HdfsTable allTypes = (HdfsTable) catalog_
        .getOrLoadTable("functional", "alltypes", "test", null);
    HdfsTable insertTbl = (HdfsTable) catalog_
        .getOrLoadTable(TEST_DB_NAME, tableToInsertPart, "test", null);
    HdfsTable multiInsertTbl = (HdfsTable) catalog_
        .getOrLoadTable(TEST_DB_NAME, tableToInsertMulPart, "test", null);
    // get lastSyncedEvent id of tables before inserting from Impala
    long insertTblLatestEventIdBefore = insertTbl.getLastSyncedEventId();
    long multiInsertTblLatestEventIdBefore = multiInsertTbl.getLastSyncedEventId();

    // we copy files from the src tbl and then issue a insert catalogOp to simulate a
    // insert operation
    List<String> tbl1Part1Files = copyFiles(allTypes.getFileSystem(),
        new Path(allTypes.getHdfsBaseDir() + "/year=2009/month=1"),
        insertTbl.getFileSystem(),
        new Path(insertTbl.getHdfsBaseDir() + "/year=2009/month=1"), overwrite, "copy_");
    List<String> tbl1Part2Files = copyFiles(allTypes.getFileSystem(),
        new Path(allTypes.getHdfsBaseDir() + "/year=2009/month=2"),
        insertTbl.getFileSystem(),
        new Path(insertTbl.getHdfsBaseDir() + "/year=2009/month=2"), overwrite, "copy_");
    List<String> tbl2Part1Files = copyFiles(allTypes.getFileSystem(),
        new Path(allTypes.getHdfsBaseDir() + "/year=2009/month=1"),
        multiInsertTbl.getFileSystem(),
        new Path(multiInsertTbl.getHdfsBaseDir() + "/year=2009/month=1"), overwrite,
        "copy_");
    List<String> tbl2Part2Files = copyFiles(allTypes.getFileSystem(),
        new Path(allTypes.getHdfsBaseDir() + "/year=2009/month=2"),
        multiInsertTbl.getFileSystem(),
        new Path(multiInsertTbl.getHdfsBaseDir() + "/year=2009/month=2"), overwrite,
        "copy_");
    insertFromImpala(
        tableToInsertPart, true, "year=2009", "month=1", overwrite, tbl1Part1Files);
    insertFromImpala(
        tableToInsertPart, true, "year=2009", "month=2", overwrite, tbl1Part2Files);
    // insert into multiple partition
    Map<String, TUpdatedPartition> updated_partitions = new HashMap<>();
    String partition1 = "year=2009/month=1";
    String partition2 = "year=2009/month=2";
    TUpdatedPartition updatedPartition1 = new TUpdatedPartition();
    TUpdatedPartition updatedPartition2 = new TUpdatedPartition();
    updatedPartition1.setFiles(tbl2Part1Files);
    updatedPartition2.setFiles(tbl2Part2Files);
    updated_partitions.put(partition1, updatedPartition1);
    updated_partitions.put(partition2, updatedPartition2);
    insertMulPartFromImpala(tableToInsertMulPart, tableToInsertPart, updated_partitions,
        overwrite);
    // we expect 4 INSERT events (2 for the insertTbl and 2 for multiInsertTbl)
    List<NotificationEvent> events = eventsProcessor_.getNextMetastoreEvents();
    assertEquals(4, events.size());
    assertEquals(tbl1Part1Files, getFilesFromEvent(events.get(0)));
    assertEquals(tbl1Part2Files, getFilesFromEvent(events.get(1)));
    assertEquals(tbl2Part1Files, getFilesFromEvent(events.get(2)));
    assertEquals(tbl2Part2Files, getFilesFromEvent(events.get(3)));
    eventsProcessor_.processEvents();

    if (BackendConfig.INSTANCE.enableSyncToLatestEventOnDdls()) {
      // when processing self events, eventProcessor should update last synced event id
      // for the table
      assertTrue(
          catalog_.getTable(TEST_DB_NAME, tableToInsertPart).getLastSyncedEventId() >
              insertTblLatestEventIdBefore);
      assertTrue(
          catalog_.getTable(TEST_DB_NAME, tableToInsertMulPart).getLastSyncedEventId() >
              multiInsertTblLatestEventIdBefore);
    }
    // Test insert into table
    String unpartitionedTbl = "tbl_to_insert";
    // create table self-event 5
    createTableLike("functional", "tinytable", TEST_DB_NAME, unpartitionedTbl);
    HdfsTable tinyTable = (HdfsTable) catalog_
        .getOrLoadTable("functional", "tinytable", "test", null);
    HdfsTable unpartTable =
        (HdfsTable) catalog_.getOrLoadTable(TEST_DB_NAME, unpartitionedTbl, "test", null);
    List<String> copied_files =
        copyFiles(tinyTable.getFileSystem(), new Path(tinyTable.getHdfsBaseDir()),
            unpartTable.getFileSystem(), new Path(unpartTable.getHdfsBaseDir()),
            overwrite, "copy_");
    // insert self-event 6
    insertFromImpala(unpartitionedTbl, false, "", "", overwrite, copied_files);
    eventsProcessor_.processEvents();

    long selfEventsCountAfter =
        eventsProcessor_.getMetrics()
            .getCounter(MetastoreEventsProcessor.EVENTS_SKIPPED_METRIC)
            .getCount();
    // 2 single insert partition events, 1 multi insert partitions which includes 2 single
    // insert events 1 single insert table event, 1 create table event
    assertEquals("Unexpected number of self-events generated",
        numberOfSelfEventsBefore + 6, selfEventsCountAfter);
  }

  private List<String> getFilesFromEvent(NotificationEvent event) {
    assertEquals("INSERT", event.getEventType());
    List<String> files= new ArrayList<>();
    for (String f : MetastoreEventsProcessor.getMessageDeserializer()
        .getInsertMessage(event.getMessage()).getFiles()) {
      // Metastore InsertMessage appends "###" for some reason. Ignoring that bit in the
      // comparison below.
      files.add(f.replaceAll("###", ""));
    }
    return files;
  }

  private static final Configuration CONF = new Configuration();

  private static List<String> copyFiles(FileSystem srcFs, Path src, FileSystem destFs,
      Path dest, boolean overwrite, String prefix) throws Exception {
    FSDataOutputStream out = null;
    try {
      if (srcFs.isDirectory(src)) {
        if (!destFs.exists(dest)) {
          destFs.mkdirs(dest);
        } else if(overwrite) {
          destFs.delete(dest, true);
          destFs.mkdirs(dest);
        }
      }
      List<String> filesCopied = new ArrayList<>();
      RemoteIterator<? extends FileStatus> it = FileSystemUtil
          .listStatus(srcFs, src, true, null);
      while (it.hasNext()) {
        FileStatus status = it.next();
        if (status.isDirectory()) continue;
        InputStream in = srcFs.open(status.getPath());
        String copyFileName = (prefix == null ? "" : prefix) + status.getPath().getName();
        out = destFs.create(new Path(dest, copyFileName), false);
        IOUtils.copyBytes(in, out, CONF, true);
        filesCopied.add(new Path(dest, copyFileName).toString());
      }
      return filesCopied;
    } catch (IOException ex) {
      IOUtils.closeStream(out);
      throw ex;
    }
  }
  /**
   * Test generates a sequence of create_table, insert and drop_table in the event stream
   * to make sure when the insert event is processed on a removed table, it doesn't cause
   * any issues with the event processing.
   */
  @Test
  public void testInsertEventOnRemovedTable()
      throws Exception {
    createDatabaseFromImpala(TEST_DB_NAME, "");
    final String createInsertDropTable = "tbl_create_insert_drop";
    createTableFromImpala(TEST_DB_NAME, createInsertDropTable, null, false);
    org.apache.hadoop.hive.metastore.api.Table msTbl;
    try (MetaStoreClient metaStoreClient = catalog_.getMetaStoreClient()) {
      msTbl =
          metaStoreClient.getHiveClient().getTable(TEST_DB_NAME, createInsertDropTable);
    }
    simulateInsertIntoTableFromFS(msTbl, 2, null, false);
    dropTable(createInsertDropTable);
    // this will generate create db, create_table, insert and droptable events
    assertEquals(4, eventsProcessor_.getNextMetastoreEvents().size());
    eventsProcessor_.processEvents();
    // make sure that the insert event on a dropped table does not cause a error during
    // event processing
    assertEquals(EventProcessorStatus.ACTIVE, eventsProcessor_.getStatus());
  }

  /**
   * Util method to create empty files in a given path
   */
  private List<String> addFilesToDirectory(Path parentPath, String fileNamePrefix,
      int totalNumberOfFilesToAdd, boolean isOverwrite) throws IOException {
    List<String> newFiles = new ArrayList<>();
    final FileSystem fs = parentPath.getFileSystem(FileSystemUtil.getConfiguration());
    if (isOverwrite && fs.exists(parentPath)) fs.delete(parentPath, true);
    for (int i = 0; i < totalNumberOfFilesToAdd; i++) {
      Path filename = new Path(parentPath,
          fileNamePrefix + RandomStringUtils.random(5, true, true).toUpperCase());
      try (FSDataOutputStream out = fs.create(filename)) {
        newFiles.add(filename.getName());
      }
    }
    return newFiles;
  }

  /**
   * Helper to test insert events. Creates a fake InsertEvent notification in the
   * catalog and processes it. To simulate an insert, we load a file using FS APIs and
   * verify the new file shows up after table/partition refresh.
   */
  public void testInsertEvents(String dbName, String tblName,
      boolean isPartitionInsert) throws TException,
      ImpalaException, IOException {

    if (isPartitionInsert) {
      // Add a partition
      List<List<String>> partVals = new ArrayList<>();
      partVals.add(new ArrayList<>(Arrays.asList("testPartVal")));
      addPartitions(dbName, tblName, partVals);
    }
    eventsProcessor_.processEvents();

    // Simulate a load table
    Table tbl = catalog_.getOrLoadTable(dbName, tblName, "test", null);
    Partition partition = null;
    if (isPartitionInsert) {
      // Get the partition from metastore. This should now contain the new file.
      try (MetaStoreClient metaStoreClient = catalog_.getMetaStoreClient()) {
        partition = metaStoreClient.getHiveClient().getPartition(dbName, tblName, "p1"
            + "=testPartVal");
      }
    }
    // first insert 3 files
    assertFalse("Table must be already loaded to verify correctness",
        tbl instanceof IncompleteTable);
    simulateInsertIntoTableFromFS(tbl.getMetaStoreTable(), 3,
        partition, true);
    verifyNumberOfFiles(tbl, 3);

    // another insert which adds 2 more files; total number of files will now be 5
    simulateInsertIntoTableFromFS(tbl.getMetaStoreTable(), 2,
        partition, false);
    verifyNumberOfFiles(tbl, 5);

    // create a insert overwrite op now to reset the file count to 1
    simulateInsertIntoTableFromFS(tbl.getMetaStoreTable(), 1,
        partition, true);
    verifyNumberOfFiles(tbl, 1);

    // create insert overwrite again to add 2 files
    simulateInsertIntoTableFromFS(tbl.getMetaStoreTable(), 2,
        partition, true);
    verifyNumberOfFiles(tbl, 2);
  }

  private void verifyNumberOfFiles(Table tbl, int expectedNumberOfFiles)
      throws DatabaseNotFoundException {
    // Now check if the table is refreshed by checking the files size. A partition
    // refresh will make the new file show up in the partition. NOTE: This is same
    // for table and partition inserts as impala treats a non-partitioned table as a
    // table with a single partition.
    eventsProcessor_.processEvents();
    Table tblAfterInsert = catalog_.getTable(tbl.getDb().getName(), tbl.getName());
    assertFalse(tblAfterInsert instanceof IncompleteTable);
    Collection<? extends FeFsPartition> partsAfterInsert =
        FeCatalogUtils.loadAllPartitions((HdfsTable) tblAfterInsert);
    assertTrue("Partition not found after insert.",
        partsAfterInsert.size() > 0);
    FeFsPartition singlePart =
        Iterables.getOnlyElement((List<FeFsPartition>) partsAfterInsert);
    Set<String> filesAfterInsertForTable =
        (((HdfsPartition) singlePart).getFileNames());
    assertEquals("File count mismatch after insert.",
        expectedNumberOfFiles, filesAfterInsertForTable.size());
  }

  private void simulateInsertIntoTableFromFS(
      org.apache.hadoop.hive.metastore.api.Table msTbl,
      int totalNumberOfFilesToAdd, Partition partition,
      boolean isOverwrite) throws IOException, TException {
    Path parentPath = partition == null ? new Path(msTbl.getSd().getLocation())
        : new Path(partition.getSd().getLocation());
    List <String> newFiles = addFilesToDirectory(parentPath, "testFile.",
        totalNumberOfFilesToAdd, isOverwrite);
    try (MetaStoreClient metaStoreClient = catalog_.getMetaStoreClient()) {
      List<InsertEventRequestData> partitionInsertEventInfos = new ArrayList<>();
      List<List<String>> partitionInsertEventVals = new ArrayList<>();
      InsertEventRequestData insertEventRequestData = new InsertEventRequestData();
      insertEventRequestData.setFilesAdded(newFiles);
      insertEventRequestData.setReplace(isOverwrite);
      if (partition != null) {
        MetastoreShim.setPartitionVal(insertEventRequestData, partition.getValues());
      }
      partitionInsertEventInfos.add(insertEventRequestData);
      partitionInsertEventVals.add(partition != null ? partition.getValues() : null);
      MetastoreShim.fireInsertEventHelper(metaStoreClient.getHiveClient(),
          partitionInsertEventInfos, partitionInsertEventVals, msTbl.getDbName(),
          msTbl.getTableName());
    }
  }

  private void simulateInsertIntoTransactionalTableFromFS(
      org.apache.hadoop.hive.metastore.api.Table msTbl, Partition partition,
      int totalNumberOfFilesToAdd, long txnId, long writeId) throws IOException {
    Path parentPath = partition == null ?
        new Path(msTbl.getSd().getLocation()) : new Path(partition.getSd().getLocation());
    Path deltaPath = new Path(parentPath, String.format("delta_%d_%d", writeId, writeId));
    List<String> newFiles = addFilesToDirectory(deltaPath, "testFile.",
        totalNumberOfFilesToAdd, false);
    List<InsertEventRequestData> insertEventReqDatas = new ArrayList<>();
    List<List<String>> insertEventVals = new ArrayList<>();
    InsertEventRequestData insertEventRequestData = new InsertEventRequestData();
    if (partition != null) {
      MetastoreShim.setPartitionVal(insertEventRequestData, partition.getValues());
    }
    insertEventRequestData.setFilesAdded(newFiles);
    insertEventRequestData.setReplace(false);
    insertEventReqDatas.add(insertEventRequestData);
    insertEventVals.add(partition != null ? partition.getValues() : null);
    MetaStoreUtil.TableInsertEventInfo insertEventInfo =
        new MetaStoreUtil.TableInsertEventInfo(insertEventReqDatas, insertEventVals,
            true, txnId, writeId);
    MetastoreShim.fireInsertEvents(catalog_.getMetaStoreClient(), insertEventInfo,
        msTbl.getDbName(), msTbl.getTableName());
  }

  /**
   * Test generates ALTER_TABLE events for various cases (table rename, parameter change,
   * add/remove/change column) and makes sure that the table is updated on the CatalogD
   * side after the ALTER_TABLE event is processed.
   * We also remove the DDL_TIME from the table parameters so that it would be set from
   * the metastore, same way as Impala does in actual alters.
   */
  @Test
  public void testAlterTableEvent() throws TException, ImpalaException {
    createDatabase(TEST_DB_NAME, null);
    final String testTblName = "testAlterTableEvent";
    createTable("old_name", false);
    // sync to latest events
    eventsProcessor_.processEvents();
    // simulate the table being loaded by explicitly calling load table
    loadTable("old_name");
    long numOfRefreshesBefore = eventsProcessor_.getMetrics()
        .getCounter(MetastoreEventsProcessor.NUMBER_OF_TABLE_REFRESHES).getCount();

    // test renaming a table from outside aka metastore client
    alterTableRename("old_name", testTblName, null);
    eventsProcessor_.processEvents();
    // table with the old name should not be present anymore
    assertNull(
        "Old named table still exists", catalog_.getTable(TEST_DB_NAME, "old_name"));
    // table with the new name should be present in Incomplete state
    Table newTable = catalog_.getTable(TEST_DB_NAME, testTblName);
    assertNotNull("Table with the new name is not found", newTable);
    assertTrue("Table with the new name should be incomplete",
        newTable instanceof IncompleteTable);

    // Test renaming a table to a different database
    createTable("old_table_name", false);
    // sync to latest events
    eventsProcessor_.processEvents();
    // simulate the table being loaded by explicitly calling load table
    loadTable("old_table_name");
    // create a new database.
    createDatabase("new_db", null);
    // rename old table to new table in new_db.
    alterTableRename("old_table_name", "new_table_name", "new_db");
    eventsProcessor_.processEvents();
    Table tableAfterRename = catalog_.getTable("new_db", "new_table_name");
    assertNotNull(tableAfterRename);
    assertTrue("Table after rename should be incomplete.",
        tableAfterRename instanceof IncompleteTable);
    // clean up
    dropDatabaseCascadeFromImpala("new_db");

    // check refresh after alter table add parameter
    loadTable(testTblName);
    Table testTbl = catalog_.getTable(TEST_DB_NAME, testTblName);
    long lastSyncedEventIdBefore = testTbl.getLastSyncedEventId();
    alterTableAddParameter(testTblName, "somekey", "someval");
    eventsProcessor_.processEvents();
    assertFalse("Table should have been refreshed after alter table add parameter",
        catalog_.getTable(TEST_DB_NAME, testTblName)
                instanceof IncompleteTable);
    if (BackendConfig.INSTANCE.enableSyncToLatestEventOnDdls()) {
      assertTrue("Table's last synced event id should have been advanced after"
              + " processing alter table event",
          catalog_.getTable(TEST_DB_NAME, testTblName).getLastSyncedEventId() >
              lastSyncedEventIdBefore);
    }
    // check refresh after alter table add col
    loadTable(testTblName);
    alterTableAddCol(testTblName, "newCol", "int", "null");
    eventsProcessor_.processEvents();
    assertFalse("Table should have been refreshed after alter table add column",
        catalog_.getTable(TEST_DB_NAME, testTblName)
                instanceof IncompleteTable);
    // check refresh after alter table change column type
    loadTable(testTblName);
    altertableChangeCol(testTblName, "newCol", "string", null);
    eventsProcessor_.processEvents();
    assertFalse("Table should have been refreshed after changing column type",
        catalog_.getTable(TEST_DB_NAME, testTblName)
                instanceof IncompleteTable);
    // check refresh after alter table remove column
    loadTable(testTblName);
    alterTableRemoveCol(testTblName, "newCol");
    eventsProcessor_.processEvents();
    assertFalse("Table should have been refreshed after removing a column",
        catalog_.getTable(TEST_DB_NAME, testTblName)
                instanceof IncompleteTable);
    // 4 alters above. Each one of them except rename should increment the counter by 1
    long numOfRefreshesAfter = eventsProcessor_.getMetrics()
        .getCounter(MetastoreEventsProcessor.NUMBER_OF_TABLE_REFRESHES).getCount();
    assertEquals("Unexpected number of table refreshes",
        numOfRefreshesBefore + 4, numOfRefreshesAfter);
    // Check if trivial alters are ignored.
    loadTable(testTblName);
    alterTableChangeTrivialProperties(testTblName);
    // The above alter should not cause a refresh.
    long numberOfInvalidatesAfterTrivialAlter = eventsProcessor_.getMetrics()
        .getCounter(MetastoreEventsProcessor.NUMBER_OF_TABLE_REFRESHES).getCount();
    assertEquals("Unexpected number of table refreshes after trivial alters",
        numOfRefreshesBefore + 4, numberOfInvalidatesAfterTrivialAlter);

    // Simulate rename and drop sequence for table/db.
    String tblName = "alter_drop_test";
    createTable(tblName, false);
    eventsProcessor_.processEvents();
    alterTableRename(tblName, "new_table_1", null);
    dropTableFromImpala(TEST_DB_NAME, tblName);
    eventsProcessor_.processEvents();
    // Alter event generated by a rename on a dropped table should be skipped.
    assertEquals(EventProcessorStatus.ACTIVE, eventsProcessor_.getStatus());

    // Rename from Impala and drop table.
    createTable(tblName, false);
    eventsProcessor_.processEvents();
    alterTableRenameFromImpala(TEST_DB_NAME, tblName, "new_table_2");
    dropTableFromImpala(TEST_DB_NAME, tblName);
    eventsProcessor_.processEvents();

    createTable(tblName, false);
    eventsProcessor_.processEvents();
    alterTableRename(tblName, "new_tbl_3", null);
    dropDatabaseCascadeFromImpala(TEST_DB_NAME);
    eventsProcessor_.processEvents();
    // Alter event generated by a rename on a dropped database should be skipped.
    assertEquals(EventProcessorStatus.ACTIVE, eventsProcessor_.getStatus());
  }

  /**
   * Test drops table using a metastore client and makes sure that the table does not
   * exist in the catalogD after processing DROP_TABLE event is processed. Repeats the
   * same test for a partitioned table.
   */
  @Test
  public void testDropTableEvent() throws TException, ImpalaException {
    createDatabase(TEST_DB_NAME, null);
    final String testTblName = "tbl_to_be_dropped";
    createTable(testTblName, false);
    eventsProcessor_.processEvents();
    loadTable(testTblName);
    // issue drop table and make sure it doesn't exist after processing the events
    dropTable(testTblName);
    eventsProcessor_.processEvents();
    assertTrue("Table should not be found after processing drop_table event",
        catalog_.getTable(TEST_DB_NAME, testTblName) == null);

    // test partitioned table drop
    createTable(testTblName, true);

    eventsProcessor_.processEvents();
    loadTable(testTblName);
    // create 2 partitions
    List<List<String>> partVals = new ArrayList<>(2);
    partVals.add(Arrays.asList("1"));
    partVals.add(Arrays.asList("2"));
    addPartitions(TEST_DB_NAME, testTblName, partVals);
    dropTable(testTblName);
    eventsProcessor_.processEvents();
    assertTrue("Partitioned table should not be found after processing drop_table event",
        catalog_.getTable(TEST_DB_NAME, testTblName) == null);
  }

  /**
   * Test makes sure that the events are not processed when the event processor is in
   * PAUSED state
   * @throws TException
   */
  @Test
  public void testPauseEventProcessing() throws TException {
    try {
      assertEquals(EventProcessorStatus.ACTIVE, eventsProcessor_.getStatus());
      eventsProcessor_.pause();
      createDatabase(TEST_DB_NAME, null);
      eventsProcessor_.processEvents();
      assertEquals(EventProcessorStatus.PAUSED, eventsProcessor_.getStatus());
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
      eventsProcessor_.pause();
      createDatabase(TEST_DB_NAME, null);
      eventsProcessor_.processEvents();
      assertEquals(EventProcessorStatus.PAUSED, eventsProcessor_.getStatus());
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
   * A MetastoreEventsProcessor that simulates HMS failures.
   */
  private static class HMSFetchNotificationsEventProcessor
      extends MetastoreEventsProcessor {
    HMSFetchNotificationsEventProcessor(
        CatalogOpExecutor catalogOp, long startSyncFromId, long pollingFrequencyInSec)
        throws CatalogException {
      super(catalogOp, startSyncFromId, pollingFrequencyInSec);
    }

    @Override
    public List<NotificationEvent> getNextMetastoreEvents()
        throws MetastoreNotificationFetchException {
      // Throw exception roughly half of the time
      Random rand = new Random();
      if (rand.nextInt(10) % 2 == 0) {
        throw new MetastoreNotificationFetchException("Fetch Exception");
      }
      return super.getNextMetastoreEvents();
    }
  }

  /**
   * Tests event processor is active after HMS restarts.
   */
  @Test
  public void testEventProcessorFetchAfterHMSRestart() throws ImpalaException {
    CatalogServiceTestCatalog catalog = CatalogServiceTestCatalog.create();
    CatalogOpExecutor catalogOpExecutor = catalog.getCatalogOpExecutor();
    MetastoreEventsProcessor fetchProcessor =
        new HMSFetchNotificationsEventProcessor(catalogOpExecutor,
            eventsProcessor_.getCurrentEventId(), 2L);
    fetchProcessor.start();
    try {
      assertEquals(EventProcessorStatus.ACTIVE, fetchProcessor.getStatus());
      // Roughly half of the time an exception is thrown. Make sure the event processor
      // is still active.
      while (true) {
        try {
          fetchProcessor.getNextMetastoreEvents();
        } catch (MetastoreNotificationFetchException ex) {
          break;
        }
      }
      assertEquals(EventProcessorStatus.ACTIVE, fetchProcessor.getStatus());
    } finally {
      fetchProcessor.shutdown();
    }
  }

  /**
   * Test makes sure that event processor is restarted after reset()
   */
  @Test
  public void testEventProcessingAfterReset() throws ImpalaException {
    assertEquals(EventProcessorStatus.ACTIVE, eventsProcessor_.getStatus());
    long syncedIdBefore = eventsProcessor_.getLastSyncedEventId();
    catalog_.reset(NoOpEventSequence.INSTANCE);
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
  public void testCreateDropCreateTableFromImpala()
      throws ImpalaException, TException, InterruptedException {
    assertEquals(EventProcessorStatus.ACTIVE, eventsProcessor_.getStatus());
    createDatabase(TEST_DB_NAME, null);
    final String testTblName = "testCreateDropCreateTableFromImpala";
    eventsProcessor_.processEvents();
    createTableFromImpala(TEST_DB_NAME, testTblName, false);
    assertNotNull("Table should have been found after create table statement",
        catalog_.getTable(TEST_DB_NAME, testTblName));
    loadTable(testTblName);
    // Adding sleep here to make sure that the CREATION_TIME is not same
    // as the previous CREATE_TABLE operation, so as to trigger the filtering logic
    // based on CREATION_TIME in DROP_TABLE event processing. This is currently a
    // limitation : the DROP_TABLE event filtering expects that while processing events,
    // the CREATION_TIME of two tables with same name won't have the same
    // creation timestamp.
    Thread.sleep(2000);
    dropTableFromImpala(TEST_DB_NAME, testTblName);
    // now catalogD does not have the table entry, create the table again
    createTableFromImpala(TEST_DB_NAME, testTblName, false);
    assertNotNull("Table should have been found after create table statement",
        catalog_.getTable(TEST_DB_NAME, testTblName));
    loadTable(testTblName);
    long currentEventId = eventsProcessor_.getCurrentEventId();
    List<NotificationEvent> events =
        eventsProcessor_.getNextMetastoreEvents(currentEventId);
    // the first create table event should not change anything to the catalogd's
    // created table
    assertEquals(3, events.size());
    Table existingTable = catalog_.getTable(TEST_DB_NAME, testTblName);
    long id = MetastoreShim.getTableId(existingTable.getMetaStoreTable());
    assertEquals("CREATE_TABLE", events.get(0).getEventType());
    eventsProcessor_.processEvents(currentEventId, Lists.newArrayList(events.get(0)));
    // after processing the create_table the original table should still remain the same
    long testId = MetastoreShim.getTableId(catalog_.getTable(TEST_DB_NAME,
        testTblName).getMetaStoreTable());
    assertEquals(id, testId);
    //second event should be drop_table. This event should also be skipped since
    // catalog state is more recent than the event
    assertEquals("DROP_TABLE", events.get(1).getEventType());
    long numFilteredEvents =
        eventsProcessor_.getMetrics()
            .getCounter(MetastoreEventsProcessor.EVENTS_SKIPPED_METRIC).getCount();
    eventsProcessor_.processEvents(currentEventId, Lists.newArrayList(events.get(1)));
    // Verify that the drop_table event is skipped and the metric is incremented.
    assertEquals(numFilteredEvents + 1, eventsProcessor_.getMetrics()
        .getCounter(MetastoreEventsProcessor.EVENTS_SKIPPED_METRIC).getCount());
    // even after drop table event, the table should still exist
    assertNotNull("Table should have existed since catalog state is current and event "
        + "is stale", catalog_.getTable(TEST_DB_NAME, testTblName));
    // the final create table event should also be ignored since its a self-event
    assertEquals("CREATE_TABLE", events.get(2).getEventType());
    eventsProcessor_.processEvents(currentEventId, Lists.newArrayList(events.get(2)));
    assertFalse(
        "Table should have been loaded since the create_table should be " + "ignored",
        catalog_.getTable(TEST_DB_NAME,
            testTblName) instanceof IncompleteTable);
    //finally make sure the table is still the same
    testId = MetastoreShim.getTableId(catalog_.getTable(TEST_DB_NAME,
        testTblName).getMetaStoreTable());
    assertEquals(id, testId);
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
    final String testTblName = "testTableEventsFromImpala";
    createTableFromImpala(TEST_DB_NAME, testTblName, true);
    loadTable(testTblName);
    List<NotificationEvent> events = eventsProcessor_.getNextMetastoreEvents();
    assertEquals(2, events.size());

    eventsProcessor_.processEvents();
    assertNotNull(catalog_.getDb(TEST_DB_NAME));
    assertNotNull(catalog_.getTable(TEST_DB_NAME, testTblName));
    assertFalse("Table should have been loaded since it was already latest", catalog_
        .getTable(TEST_DB_NAME, testTblName) instanceof IncompleteTable);

    dropTableFromImpala(TEST_DB_NAME, testTblName);
    assertNull(catalog_.getTable(TEST_DB_NAME, testTblName));
    events = eventsProcessor_.getNextMetastoreEvents();
    // should have 1 drop_table event
    assertEquals(1, events.size());
    eventsProcessor_.processEvents();
    // dropping a non-existant table should cause event processor to go into error state
    assertEquals(EventProcessorStatus.ACTIVE, eventsProcessor_.getStatus());
    assertNull(catalog_.getTable(TEST_DB_NAME, testTblName));
  }

  /**
   * Similar to create,drop,create sequence table as in
   * <code>testCreateDropCreateTableFromImpala</code> but operates on Database instead
   * of Table.
   */
  @Test
  public void testCreateDropCreateDatabaseFromImpala()
      throws ImpalaException, InterruptedException {
    assertEquals(EventProcessorStatus.ACTIVE, eventsProcessor_.getStatus());
    createDatabaseFromImpala(TEST_DB_NAME, "first");
    assertNotNull("Db should have been found after create database statement",
        catalog_.getDb(TEST_DB_NAME));
    // Adding sleep here to make sure that the CREATION_TIME is not same
    // as the previous CREATE_DB operation, so as to trigger the filtering logic
    // based on CREATION_TIME in DROP_DB event processing. This is currently a
    // limitation : the DROP_DB event filtering expects that while processing events,
    // the CREATION_TIME of two Databases with same name won't have the same
    // creation timestamp.
    Thread.sleep(2000);
    dropDatabaseCascadeFromImpala(TEST_DB_NAME);
    assertNull(catalog_.getDb(TEST_DB_NAME));
    createDatabaseFromImpala(TEST_DB_NAME, "second");
    assertNotNull(catalog_.getDb(TEST_DB_NAME));
    long currentEventId = eventsProcessor_.getCurrentEventId();
    List<NotificationEvent> events =
        eventsProcessor_.getNextMetastoreEvents(currentEventId);
    // should have 3 events for create,drop and create database
    assertEquals(3, events.size());

    assertEquals("CREATE_DATABASE", events.get(0).getEventType());
    eventsProcessor_.processEvents(currentEventId, Lists.newArrayList(events.get(0)));
    // create_database event should have no effect since catalogD has already a later
    // version of database with the same name.
    assertNotNull(catalog_.getDb(TEST_DB_NAME));
    assertEquals("second",
        catalog_.getDb(TEST_DB_NAME).getMetaStoreDb().getDescription());

    // now process drop_database event
    assertEquals("DROP_DATABASE", events.get(1).getEventType());
    eventsProcessor_.processEvents(currentEventId, Lists.newArrayList(events.get(1)));
    // database should not be dropped since catalogD is at the latest state
    assertNotNull(catalog_.getDb(TEST_DB_NAME));
    assertEquals("second",
        catalog_.getDb(TEST_DB_NAME).getMetaStoreDb().getDescription());

    // the third create_database event should have no effect too
    assertEquals("CREATE_DATABASE", events.get(2).getEventType());
    eventsProcessor_.processEvents(currentEventId, Lists.newArrayList(events.get(2)));
    assertNotNull(catalog_.getDb(TEST_DB_NAME));
    assertEquals("second",
        catalog_.getDb(TEST_DB_NAME).getMetaStoreDb().getDescription());
  }

  /**
   * Test checks if the events are processed or ignored when the value of parameter
   * <code>MetastoreEventPropertyKey.DISABLE_EVENT_HMS_SYNC</code> is changed.
   * Currently, this test only changes the flags for at the table level, since
   * alter_database events are not supported currently. In order to confirm that the
   * event processing happens as expected, this test generates a alter_table event
   * using a mock notificationEvent and a mock catalog which returns the dbFlag flag as
   * expected. Then it makes sure that the <code>isEventProcessingDisabled</code>
   * method of the AlterTableEvent returns the expected result, given the flags. And then
   * generates a additional alter table event to make sure that the subsequent event is
   * processed/skipped based on the new flag values
   */
  @Test
  public void testEventSyncFlagTransitions() throws Exception {
    // each db level flag and tbl level flag can undergo following transition
    // unset(defaults to false) -> true
    // false -> true
    // true -> false
    Pair<String, String> unsetToTrue = new Pair<>(null, "true");
    Pair<String, String> unsetToFalse = new Pair<>(null, "false");
    Pair<String, String> falseToTrue = new Pair<>("false", "true");
    Pair<String, String> falseToUnset = new Pair<>("false", null);
    Pair<String, String> trueToFalse = new Pair<>("true", "false");
    Pair<String, String> trueToUnset = new Pair<>("true", null);

    List<Pair<String, String>> allTblTransitions = Arrays.asList(unsetToTrue,
        unsetToFalse, falseToTrue, falseToUnset, trueToFalse, trueToUnset);
    List<String> dbFlagVals = Arrays.asList(null, "true", "false");
    // dbFlag transition is not tested here since ALTER_DATABASE events are ignored
    // currently. dbFlags do not change in the following loop
    final String testTblName = "testEventSyncFlagTransitions";
    for (String dbFlag : dbFlagVals) {
      for (Pair<String, String> tblTransition : allTblTransitions) {
        // subsequent event is skipped based on the new value of tblTransition flag or
        // the dbFlag if the new value unsets it
        boolean shouldSubsequentEventsBeSkipped = tblTransition.second == null ?
            Boolean.valueOf(dbFlag) : Boolean.valueOf(tblTransition.second);
        runDDLTestForFlagTransitionWithMock(TEST_DB_NAME, testTblName,
            dbFlag, tblTransition, shouldSubsequentEventsBeSkipped);
      }
    }
  }

  /**
   * Test exercises the event processing condition when table level HMS sync is changed
   * from HMS. Consider the scenario where table creation is skipped because event
   * processing is disabled for that table. But then user alters the flag to re-enable
   * the event processing. Since the table doesn't exist in the catalog in the first
   * place, event processing should not stop and go into error state, instead an
   * incomplete table object should be added in cache, so that any direct access to
   * that object fetches latest snapshot from HMS. Also this test verifies that older
   * event of disabling HMS sync flag will be ignored if there is a drop & re-create of
   * table in Impala. This test also marks the table object as stale if disableSync is
   * enabled at table level and given that table object is loaded in the cache.
   */
  @Test
  public void testEventSyncFlagChanged()
      throws ImpalaException, TException, CatalogException {
    // when the event sync flag is changed from true to false (or null), it is possible
    // that the table is not existing in catalog anymore. Event processing should not
    // error out in such a case
    Pair<String, String> trueToFalse = new Pair<>("true", "false");
    Pair<String, String> trueToUnset = new Pair<>("true", null);
    List<Pair<String, String>> tblFlagTransitions = Arrays.asList(trueToFalse,
        trueToUnset);
    List<String> dbFlagVals = Arrays.asList(null, "false");
    List<Boolean> createDropFromImpala = Arrays.asList(true, false);
    final String testTblName = "testEventSyncFlagChanged";
    boolean prevFlagVal = BackendConfig.INSTANCE.enableSkippingOlderEvents();
    try {
      BackendConfig.INSTANCE.setSkippingOlderEvents(true);
      for (String dbFlag : dbFlagVals) {
        for (Pair<String, String> tblTransition : tblFlagTransitions) {
          Map<String, String> dbParams = new HashMap<>(1);
          if (dbFlag != null) {
            dbParams.put(MetastoreEventPropertyKey.DISABLE_EVENT_HMS_SYNC.getKey(),
                dbFlag);
          }
          Map<String, String> tblParams = new HashMap<>(1);
          if (tblTransition.first != null) {
            tblParams.put(MetastoreEventPropertyKey.DISABLE_EVENT_HMS_SYNC.getKey(),
                tblTransition.first);
          }
          createDatabase(TEST_DB_NAME, dbParams);
          createTable(null, TEST_DB_NAME, testTblName, tblParams, false, null);
          eventsProcessor_.processEvents();
          // table creation is skipped since the flag says so
          assertNull(catalog_.getTable(TEST_DB_NAME, testTblName));
          // now turn on the flag
          for (boolean doCreateDrop : createDropFromImpala) {
            alterTableAddParameter(testTblName,
                MetastoreEventPropertyKey.DISABLE_EVENT_HMS_SYNC.getKey(),
                tblTransition.second);
            if (doCreateDrop) {
              catalog_.invalidateTable(new TTableName(TEST_DB_NAME, testTblName),
                  new Reference<>(), new Reference<>(),
                  NoOpEventSequence.INSTANCE);
              dropTableFromImpala(TEST_DB_NAME, testTblName);
              createTableFromImpala(TEST_DB_NAME, testTblName, null, false);
              loadTable(TEST_DB_NAME, testTblName);
            }
            eventsProcessor_.processEvents();
            assertEquals(EventProcessorStatus.ACTIVE,
                eventsProcessor_.getStatus());
            Table tbl = catalog_.getTable(TEST_DB_NAME, testTblName);
            if (!doCreateDrop) {
              assertTrue(tbl instanceof IncompleteTable);
            } else {
              // when the event sync is disabled on the table, make sure that any
              // subsequent operations on the table don't mark the table as stale
              // if the table object is loaded in the cache
              assertTrue(tbl instanceof HdfsTable);
              alterTableAddParameter(testTblName,
                  MetastoreEventPropertyKey.DISABLE_EVENT_HMS_SYNC.getKey(),
                  tblTransition.first);
              alterTableAddParameter(testTblName, "somekey", "someval");
              eventsProcessor_.processEvents();
              tbl = catalog_.getTable(TEST_DB_NAME, testTblName);
              assertTrue(tbl instanceof HdfsTable);
            }
          }
          dropDatabaseCascade(TEST_DB_NAME);
        }
      }
    } finally {
      BackendConfig.INSTANCE.setSkippingOlderEvents(prevFlagVal);
    }
  }

  private static class FakeCatalogOpExecutorForTests extends CatalogOpExecutor {

    public FakeCatalogOpExecutorForTests(CatalogServiceCatalog catalog,
        AuthorizationConfig authzConfig,
        AuthorizationManager authzManager,
        HiveJavaFunctionFactory hiveJavaFuncFactory)
        throws ImpalaException {
      super(catalog, authzConfig, authzManager, hiveJavaFuncFactory);
    }

    public static CatalogOpExecutor create() throws ImpalaException {
      return new FakeCatalogOpExecutorForTests(
          FakeCatalogServiceCatalogForFlagTests.create(),
          new NoopAuthorizationFactory().getAuthorizationConfig(),
          new NoopAuthorizationManager(),
          new TestHiveJavaFunctionFactory());
    }
  }

  /**
   * Test catalog service catalog which takes a value of db and tbl flags for a given
   * table
   */
  private static class FakeCatalogServiceCatalogForFlagTests extends
      CatalogServiceCatalog {

    private String dbFlag_;
    private String dbName_;
    private String tblFlag_;
    private String tblName_;

    private FakeCatalogServiceCatalogForFlagTests(boolean loadInBackground,
        int numLoadingThreads, String localLibraryPath,
        MetaStoreClientPool metaStoreClientPool) throws ImpalaException {
      super(loadInBackground, numLoadingThreads, localLibraryPath, metaStoreClientPool);
    }

    public static CatalogServiceCatalog create() {
      FeSupport.loadLibrary();
      CatalogServiceCatalog cs;
      try {
        cs = new FakeCatalogServiceCatalogForFlagTests(false, 16,
            System.getProperty("java.io.tmpdir"), new MetaStoreClientPool(0, 0));
        cs.setAuthzManager(new NoopAuthorizationManager());
        cs.setMetastoreEventProcessor(NoOpEventProcessor.getInstance());
        cs.reset(NoOpEventSequence.INSTANCE);
      } catch (ImpalaException e) {
        throw new IllegalStateException(e.getMessage(), e);
      }
      return cs;
    }

    public void setFlags(String dbName, String tblName, String dbFlag,
        String tblFlag) {
      Preconditions.checkNotNull(dbName);
      Preconditions.checkNotNull(tblName);
      this.dbFlag_ = dbFlag;
      this.dbName_ = dbName;
      this.tblFlag_ = tblFlag;
      this.tblName_ = tblName;
    }

    @Override
    public String getDbProperty(String dbName, String propertyKey) {
      if (dbName_.equals(dbName)) {
        return dbFlag_;
      }
      return super.getDbProperty(dbName, propertyKey);
    }

    private static final List<String> TABLE_SYNC_PROPERTYLIST =
        Arrays.asList(MetastoreEventPropertyKey.DISABLE_EVENT_HMS_SYNC.getKey());

    @Override
    public List<String> getTableProperties(
        String dbName, String tblName, List<String> propertyKey) {
      if (TABLE_SYNC_PROPERTYLIST.equals(propertyKey) && dbName_.equals(dbName)
          && tblName_.equals(tblName)) {
        return Arrays.asList(tblFlag_);
      }
      return super.getTableProperties(dbName, tblName, propertyKey);
    }
  }

  /**
   * Method creates a test tableBefore and tableAfter with the given value of tblFlags.
   * It then generates a mock notificationEvent which is used to confirm that
   * AlterTableEvent is not skipped. A subsequent alter table event is generated to
   * make sure that the new flag transition is working as expected
   */
  private void runDDLTestForFlagTransitionWithMock(String dbName, String tblName,
      String dbFlag, Pair<String, String> tblFlagTransition,
      boolean shouldNextEventBeSkipped) throws Exception {
    Map<String, String> beforeParams = new HashMap<>(2);
    beforeParams.put(Table.TBL_PROP_LAST_DDL_TIME, String.valueOf(1000));
    if (tblFlagTransition.first != null) {
      beforeParams
          .put(MetastoreEventPropertyKey.DISABLE_EVENT_HMS_SYNC.getKey(),
              tblFlagTransition.first);
    }

    Map<String, String> afterParams = new HashMap<>(2);
    afterParams.put(Table.TBL_PROP_LAST_DDL_TIME, String.valueOf(1001));
    if (tblFlagTransition.second != null) {
      afterParams
          .put(MetastoreEventPropertyKey.DISABLE_EVENT_HMS_SYNC.getKey(),
              tblFlagTransition.second);
    }

    org.apache.hadoop.hive.metastore.api.Table tableBefore =
        MetastoreApiTestUtils.getTestTable(null, dbName, tblName, beforeParams, false);
    org.apache.hadoop.hive.metastore.api.Table tableAfter =
        MetastoreApiTestUtils.getTestTable(null, dbName, tblName, afterParams, false);
    Map<String, String> dbParams = new HashMap<>(1);
    if (dbFlag != null) {
      dbParams.put(MetastoreEventPropertyKey.DISABLE_EVENT_HMS_SYNC.getKey(), dbFlag);
    }

    CatalogOpExecutor fakeCatalogOpExecutor = FakeCatalogOpExecutorForTests.create();
    FakeCatalogServiceCatalogForFlagTests fakeCatalog =
        (FakeCatalogServiceCatalogForFlagTests) fakeCatalogOpExecutor
        .getCatalog();
    fakeCatalog.setFlags(dbName, tblName, dbFlag, tblFlagTransition.first);
    NotificationEvent fakeAlterTableNotification =
        createFakeAlterTableNotification(dbName, tblName, tableBefore, tableAfter);

    AlterTableEvent alterTableEvent = new AlterTableEvent(
        fakeCatalogOpExecutor, eventsProcessor_.getMetrics(), fakeAlterTableNotification);
    Assert.assertFalse("Alter table which changes the flags should not be skipped. "
            + printFlagTransistions(dbFlag, tblFlagTransition),
        alterTableEvent.isEventProcessingDisabled());

    // issue a dummy alter table by adding a param
    afterParams.put("dummy", "value");
    org.apache.hadoop.hive.metastore.api.Table nextTable =
        MetastoreApiTestUtils.getTestTable(null, dbName, tblName, afterParams, false);
    NotificationEvent nextNotification =
        createFakeAlterTableNotification(dbName, tblName, tableAfter, nextTable);
    alterTableEvent =
        new AlterTableEvent(fakeCatalogOpExecutor, eventsProcessor_.getMetrics(),
            nextNotification);
    if (shouldNextEventBeSkipped) {
      assertTrue("Alter table event should not skipped following this table flag "
              + "transition. " + printFlagTransistions(dbFlag, tblFlagTransition),
          alterTableEvent.isEventProcessingDisabled());
    } else {
      assertFalse("Alter table event should have been skipped following the table flag "
              + "transistion. " + printFlagTransistions(dbFlag, tblFlagTransition),
          alterTableEvent.isEventProcessingDisabled());
    }
  }

  private AtomicLong eventIdGenerator = new AtomicLong(0);

  private NotificationEvent createFakeAlterTableNotification(String dbName,
      String tblName, org.apache.hadoop.hive.metastore.api.Table tableBefore,
      org.apache.hadoop.hive.metastore.api.Table tableAfter) {
    NotificationEvent fakeEvent = new NotificationEvent();
    fakeEvent.setTableName(tblName);
    fakeEvent.setDbName(dbName);
    fakeEvent.setEventId(eventIdGenerator.incrementAndGet());
    AlterTableMessage alterTableMessage =
        MetastoreShim.buildAlterTableMessage(tableBefore, tableAfter, false, -1L);
    fakeEvent.setMessage(
        MetastoreShim.serializeEventMessage(alterTableMessage));
    fakeEvent.setEventType("ALTER_TABLE");
    return fakeEvent;
  }

  private String printFlagTransistions(String dbFlag,
      Pair<String, String> tblFlagTransition) {
    return new StringBuilder("Db flag value: ")
        .append(dbFlag)
        .append(" Tbl flag changed from ")
        .append(tblFlagTransition.first)
        .append(" -> ")
        .append(tblFlagTransition.second)
        .toString();
  }

  /**
   * Test generates some events and makes sure that the metrics match with expected
   * number of events
   */
  @Test
  public void testEventProcessorMetrics() throws TException {
    TEventProcessorMetrics responseBefore = eventsProcessor_.getEventProcessorMetrics();
    long numEventsReceivedBefore = responseBefore.getEvents_received();
    long numEventsSkippedBefore = responseBefore.getEvents_skipped();
    long lastEventSyncId = responseBefore.getLast_synced_event_id();
    final String testTblName = "testEventProcessorMetrics";
    // event 1
    createDatabase(TEST_DB_NAME, null);
    Map<String, String> tblParams = new HashMap<>(1);
    tblParams.put(MetastoreEventPropertyKey.DISABLE_EVENT_HMS_SYNC.getKey(), "true");
    // event 2
    createTable(null, TEST_DB_NAME, "tbl_should_skipped", tblParams, true, null);
    // event 3
    createTable(null, TEST_DB_NAME, testTblName, null, true, null);
    List<List<String>> partitionVals = new ArrayList<>();
    partitionVals.add(Arrays.asList("1"));
    partitionVals.add(Arrays.asList("2"));
    partitionVals.add(Arrays.asList("3"));
    // event 4
    addPartitions(TEST_DB_NAME, "tbl_should_skipped", partitionVals);
    // event 5
    addPartitions(TEST_DB_NAME, testTblName, partitionVals);
    eventsProcessor_.processEvents();
    TEventProcessorMetrics response = eventsProcessor_.getEventProcessorMetrics();
    assertEquals(EventProcessorStatus.ACTIVE.toString(), response.getStatus());
    assertTrue("Atleast 5 events should have been received",
        response.getEvents_received() >= numEventsReceivedBefore + 5);
    // two events on tbl which is skipped
    assertTrue("Atleast 2 events should have been skipped",
        response.getEvents_skipped() >= numEventsSkippedBefore + 2);
    assertTrue("Event fetch duration should be greater than zero",
        response.getEvents_fetch_duration_mean() > 0);
    assertTrue("Event process duration should be greater than zero",
        response.getEvents_process_duration_mean() > 0);
    TEventProcessorMetricsSummaryResponse summaryResponse =
        catalog_.getEventProcessorSummary();
    assertNotNull(summaryResponse);
    assertTrue(response.getLast_synced_event_id() > lastEventSyncId);
  }

  /**
   * Test makes sure that the event metrics are not set when event processor is not active
   */
  @Test
  public void testEventProcessorWhenNotActive() throws TException {
    try {
      eventsProcessor_.pause();
      assertEquals(EventProcessorStatus.PAUSED, eventsProcessor_.getStatus());
      TEventProcessorMetrics response = eventsProcessor_.getEventProcessorMetrics();
      assertNotNull(response);
      assertEquals(EventProcessorStatus.PAUSED.toString(), response.getStatus());
      assertFalse(response.isSetEvents_fetch_duration_mean());
      assertFalse(response.isSetEvents_process_duration_mean());
      assertFalse(response.isSetEvents_received());
      assertFalse(response.isSetEvents_skipped());
      assertFalse(response.isSetEvents_received_1min_rate());
      assertFalse(response.isSetEvents_received_5min_rate());
      assertFalse(response.isSetEvents_received_15min_rate());
      TEventProcessorMetricsSummaryResponse summaryResponse =
          eventsProcessor_.getEventProcessorSummary();
      assertNotNull(summaryResponse);
      // Last synced id must be set even when event processor is not active.
      assertTrue(response.isSetLast_synced_event_id());
    } finally {
      // reset the state of event process once the test completes
      eventsProcessor_.start();
    }
  }

  /**
   * Tests makes sure that event metrics show valid state when event processing is not
   * configured
   */
  @Test
  public void testEventMetricsWhenNotConfigured() {
    CatalogServiceTestCatalog testCatalog = CatalogServiceTestCatalog.create();
    assertTrue("Events processed is not expected to be configured for this test",
        testCatalog.getMetastoreEventProcessor() instanceof NoOpEventProcessor);
    TEventProcessorMetrics response = testCatalog.getEventProcessorMetrics();
    assertNotNull(response);
    assertEquals(EventProcessorStatus.DISABLED.toString(), response.getStatus());
    TEventProcessorMetricsSummaryResponse summaryResponse =
        testCatalog.getEventProcessorSummary();
    assertNotNull(summaryResponse);
  }
  /**
   * Test runs all the supported DDL operations with the given value of flag at
   * database and table level
   */
  @Test
  public void testDisableEventSyncFlag() throws Exception {
    // base case, flags not present
    runDDLTestsWithFlags(null, null, true);
    // tbl level flag should get precedence which says event processing is not disabled
    runDDLTestsWithFlags(false, false, true);
    runDDLTestsWithFlags(true, false, true);
    runDDLTestsWithFlags(null, false, true);

    // tblFlag should get precedence which says event processing is disabled
    runDDLTestsWithFlags(false, true, false);
    runDDLTestsWithFlags(true, true, false);
    runDDLTestsWithFlags(null, true, false);

    // when tblFlag is not set, use dbFlag
    runDDLTestsWithFlags(false, null, true);
    runDDLTestsWithFlags(true, null, false);
  }

  /**
   * Helper method to run all the supported DDL operations on table with the given
   * values of flag at db and table levels. Takes in a boolean shouldEventGetProcessed
   * which is used to determine the state of the table in catalog based on whether
   * event is expected to be processed or skipped
   */
  void runDDLTestsWithFlags(Boolean dbFlag, Boolean tblFlag,
      boolean shouldEventGetProcessed) throws Exception {
    Map<String, String> dbParams = new HashMap<>(1);
    Map<String, String> tblParams = new HashMap<>(1);
    if (dbFlag == null) {
      // if null, remove the flag
      dbParams.remove(MetastoreEventPropertyKey.DISABLE_EVENT_HMS_SYNC.getKey());
    } else {
      dbParams.put(MetastoreEventPropertyKey.DISABLE_EVENT_HMS_SYNC.getKey(),
          String.valueOf(dbFlag));
    }
    if (tblFlag == null) {
      tblParams.remove(MetastoreEventPropertyKey.DISABLE_EVENT_HMS_SYNC.getKey());
    } else {
      tblParams
          .put(MetastoreEventPropertyKey.DISABLE_EVENT_HMS_SYNC.getKey(),
              String.valueOf(tblFlag));
    }

    final String testTblName = "runDDLTestsWithFlags";
    testDDLOpUsingEvent(TEST_DB_NAME, testTblName, dbParams,
        tblParams, MetastoreEventType.CREATE_DATABASE, shouldEventGetProcessed);
    testDDLOpUsingEvent(TEST_DB_NAME, testTblName, dbParams,
        tblParams, MetastoreEventType.ALTER_DATABASE, shouldEventGetProcessed);

    testDDLOpUsingEvent(TEST_DB_NAME, testTblName, dbParams, tblParams,
        MetastoreEventType.CREATE_TABLE, shouldEventGetProcessed);
    testDDLOpUsingEvent(TEST_DB_NAME, testTblName, dbParams, tblParams,
        MetastoreEventType.ALTER_TABLE, shouldEventGetProcessed);

    testDDLOpUsingEvent(TEST_DB_NAME, testTblName, dbParams, tblParams,
        MetastoreEventType.ADD_PARTITION, shouldEventGetProcessed);
    testDDLOpUsingEvent(TEST_DB_NAME, testTblName, dbParams, tblParams,
        MetastoreEventType.ALTER_PARTITION, shouldEventGetProcessed);
    testDDLOpUsingEvent(TEST_DB_NAME, testTblName, dbParams, tblParams,
        MetastoreEventType.DROP_PARTITION, shouldEventGetProcessed);

    testDDLOpUsingEvent(TEST_DB_NAME, testTblName, dbParams, tblParams,
        MetastoreEventType.DROP_TABLE, shouldEventGetProcessed);
    testDDLOpUsingEvent(TEST_DB_NAME, testTblName, dbParams,
        tblParams, MetastoreEventType.DROP_DATABASE, shouldEventGetProcessed);
  }

  private void cleanUpTblsForFlagTests(String dbName)
      throws TException, MetastoreNotificationFetchException, CatalogException {
    if (catalog_.getDb(dbName) == null) return;

    dropDatabaseCascade(dbName);
    assertFalse(eventsProcessor_.getNextMetastoreEvents().isEmpty());
    eventsProcessor_.processEvents();
    assertNull(catalog_.getDb(dbName));
  }

  private void initTblsForFlagTests(String dbName, String tblName,
      Map<String, String> dbParams, Map<String, String> tblParams) throws Exception {
    assertNull(catalog_.getDb(dbName));
    createDatabase(dbName, dbParams);
    createTable(null, dbName, tblName, tblParams, true, null);
    List<List<String>> partVals = new ArrayList<>(3);
    partVals.add(Arrays.asList("1"));
    partVals.add(Arrays.asList("2"));
    partVals.add(Arrays.asList("3"));
    addPartitions(dbName, tblName, partVals);
    assertEquals(3, eventsProcessor_.getNextMetastoreEvents().size());
  }

  private void testDDLOpUsingEvent(String dbName, String tblName,
      Map<String, String> dbParams,
      Map<String, String> tblParams, MetastoreEventType ddlOpCode,
      boolean shouldEventBeProcessed)
      throws Exception {
    switch (ddlOpCode) {
      case CREATE_TABLE: {
        initTblsForFlagTests(dbName, tblName, dbParams, tblParams);
        eventsProcessor_.processEvents();
        if (shouldEventBeProcessed) {
          assertNotNull(catalog_.getTable(dbName, tblName));
        } else {
          assertNull(catalog_.getTable(dbName, tblName));
        }
        cleanUpTblsForFlagTests(dbName);
        return;
      }
      case ALTER_TABLE: {
        initTblsForFlagTests(dbName, tblName, dbParams, tblParams);
        eventsProcessor_.processEvents();
        loadTable(tblName);
        alterTableAddCol(tblName, "newCol", "string", "test new column");
        alterTableAddParameter(tblName, "testParamKey", "somevalue");
        alterTableRename(tblName, "newTblName", null);
        altertableChangeCol("newTblName", "newCol", "int", "changed type to int");
        alterTableRemoveCol("newTblName", "newCol");
        alterTableRename("newTblName", tblName, null);
        assertEquals(6, eventsProcessor_.getNextMetastoreEvents().size());
        eventsProcessor_.processEvents();
        if (shouldEventBeProcessed) {
          assertNotNull(catalog_.getTable(dbName, tblName));
          assertTrue(catalog_.getTable(dbName, tblName) instanceof IncompleteTable);
        } else {
          assertNull(catalog_.getTable(dbName, tblName));
        }
        cleanUpTblsForFlagTests(dbName);
        return;
      }
      case DROP_TABLE: {
        // in case of drop table use impala to directly create table since if you use
        // hive to create the table the create table could be ignored given the flags
        createDatabase(dbName, dbParams);
        eventsProcessor_.processEvents();
        createTableFromImpala(dbName, "impala_test_tbl", tblParams, true);
        eventsProcessor_.processEvents();
        assertNotNull(catalog_.getTable(dbName, "impala_test_tbl"));
        dropTable("impala_test_tbl");
        assertEquals(1, eventsProcessor_.getNextMetastoreEvents().size());
        eventsProcessor_.processEvents();
        if (shouldEventBeProcessed) {
          assertNull(catalog_.getTable(dbName, "impala_test_tbl"));
        } else {
          assertNotNull(catalog_.getTable(dbName, "impala_test_tbl"));
        }
        cleanUpTblsForFlagTests(dbName);
        return;
      }
      case ADD_PARTITION: {
        initTblsForFlagTests(dbName, tblName, dbParams, tblParams);
        eventsProcessor_.processEvents();
        loadTable(tblName);
        List<List<String>> partValues = new ArrayList<>(3);
        partValues.add(Arrays.asList("4"));
        partValues.add(Arrays.asList("5"));
        partValues.add(Arrays.asList("6"));
        addPartitions(dbName, tblName, partValues);
        assertEquals(1, eventsProcessor_.getNextMetastoreEvents().size());
        eventsProcessor_.processEvents();
        if (shouldEventBeProcessed) {
          Collection<? extends FeFsPartition> partsAfterAdd =
              FeCatalogUtils.loadAllPartitions((HdfsTable)
                  catalog_.getTable(dbName, tblName));
          assertTrue("Partitions should have been added.", partsAfterAdd.size() == 6);
        } else {
          assertFalse("Table should still have been in loaded state since sync is "
              + "disabled",
              catalog_.getTable(dbName, tblName) instanceof IncompleteTable);
        }
        cleanUpTblsForFlagTests(dbName);
        return;
      }
      case DROP_PARTITION: {
        initTblsForFlagTests(dbName, tblName, dbParams, tblParams);
        eventsProcessor_.processEvents();
        loadTable(tblName);
        List<List<String>> partValues = new ArrayList<>(3);
        partValues.add(Arrays.asList("3"));
        dropPartitions(tblName, partValues);
        assertEquals(1, eventsProcessor_.getNextMetastoreEvents().size());
        eventsProcessor_.processEvents();
        if (shouldEventBeProcessed) {
          Collection<? extends FeFsPartition> partsAfterDrop =
              FeCatalogUtils.loadAllPartitions((HdfsTable) catalog_.getTable(dbName,
                  tblName));
          assertTrue("Partitions should have been dropped", partsAfterDrop.size() == 2);
        } else {
          assertFalse("Table should still have been in loaded state since sync is "
                  + "disabled",
              catalog_.getTable(dbName, tblName) instanceof IncompleteTable);
        }
        cleanUpTblsForFlagTests(dbName);
        return;
      }
      case ALTER_PARTITION: {
        initTblsForFlagTests(dbName, tblName, dbParams, tblParams);
        eventsProcessor_.processEvents();
        loadTable(tblName);
        List<List<String>> partValues = new ArrayList<>(1);
        partValues.add(Arrays.asList("3"));
        partValues.add(Arrays.asList("2"));
        partValues.add(Arrays.asList("1"));
        String location = "/path/to/partition";
        alterPartitions(tblName, partValues, location);
        assertEquals(3, eventsProcessor_.getNextMetastoreEvents().size());
        eventsProcessor_.processEvents();
        if (shouldEventBeProcessed) {
          Collection<? extends FeFsPartition> partsAfterAlter =
              FeCatalogUtils.loadAllPartitions((HdfsTable)
                  catalog_.getTable(dbName, tblName));
          for (FeFsPartition part : partsAfterAlter) {
            assertTrue("Partition location should have been modified by alter.",
                location.equals(part.getLocation()));
          }
        } else {
          assertFalse("Table should still have been in loaded state since sync is "
                  + "disabled",
              catalog_.getTable(dbName, tblName) instanceof IncompleteTable);
        }
        cleanUpTblsForFlagTests(dbName);
        return;
      }
      case CREATE_DATABASE: {
        initTblsForFlagTests(dbName, tblName, dbParams, tblParams);
        eventsProcessor_.processEvents();
        // database ops do not use disable flag, so they should always be processed
        assertNotNull("Database should have been created after create database event",
            catalog_.getDb(dbName));
        cleanUpTblsForFlagTests(dbName);
        return;
      }
      case DROP_DATABASE: {
        initTblsForFlagTests(dbName, tblName, dbParams, tblParams);
        eventsProcessor_.processEvents();
        assertNotNull(catalog_.getDb(dbName));
        dropDatabaseCascade(dbName);
        eventsProcessor_.processEvents();
        assertNull("Database should have been dropped after drop database event",
            catalog_.getDb(dbName));
        cleanUpTblsForFlagTests(dbName);
        return;
      }
      case ALTER_DATABASE: {
        // TODO alter database events are currently ignored
        return;
      }
    }
  }

  @Test
  public void testAlterDisableFlagFromDb()
      throws TException, CatalogException, MetastoreNotificationFetchException {
    createDatabase(TEST_DB_NAME, null);
    final String testTblName = "testAlterDisableFlagFromDb";
    eventsProcessor_.processEvents();
    Database alteredDb = catalog_.getDb(TEST_DB_NAME).getMetaStoreDb().deepCopy();
    alteredDb.putToParameters(MetastoreEventPropertyKey.DISABLE_EVENT_HMS_SYNC.getKey(),
        "true");
    alterDatabase(alteredDb);

    createTable(testTblName, false);
    assertEquals(2, eventsProcessor_.getNextMetastoreEvents().size());
    long numSkippedEvents =
        eventsProcessor_.getMetrics()
            .getCounter(MetastoreEventsProcessor.EVENTS_SKIPPED_METRIC)
            .getCount();
    eventsProcessor_.processEvents();
    assertEquals(numSkippedEvents + 1,
        eventsProcessor_.getMetrics()
            .getCounter(MetastoreEventsProcessor.EVENTS_SKIPPED_METRIC)
            .getCount());
    assertNull("Table creation should be skipped when database level event sync flag is"
            + " disabled",
        catalog_.getTable(TEST_DB_NAME, testTblName));
  }

  private void confirmTableIsLoaded(String dbName, String tblname)
      throws DatabaseNotFoundException {
    Table catalogTbl = catalog_.getTable(dbName, tblname);
    assertNotNull(catalogTbl);
    assertFalse(
        "Table should not be invalidated after process events as it is a self-event.",
        catalogTbl instanceof IncompleteTable);
  }

  /**
   * Test issues different types of alter table command from the test catalogOpExecutor
   * and makes sure that the self-event generated does not invalidate the table.
   */
  @Test
  public void testSelfEventsForTable() throws ImpalaException, TException {
    createDatabase(TEST_DB_NAME, null);
    eventsProcessor_.processEvents();
    final String testTblName = "testSelfEventsForTable";
    createTableFromImpala(TEST_DB_NAME, testTblName, true);
    Table testTbl = catalog_.getTable(TEST_DB_NAME, testTblName);
    assertTrue(testTbl.getCreateEventId() == testTbl.getLastSyncedEventId());
    eventsProcessor_.processEvents();
    long numberOfSelfEventsBefore = eventsProcessor_.getMetrics()
        .getCounter(MetastoreEventsProcessor.EVENTS_SKIPPED_METRIC).getCount();

    long lastSyncedEventIdBefore = testTbl.getLastSyncedEventId();
    alterTableSetTblPropertiesFromImpala(testTblName);
    eventsProcessor_.processEvents();
    confirmTableIsLoaded(TEST_DB_NAME, testTblName);

    if (BackendConfig.INSTANCE.enableSyncToLatestEventOnDdls()) {
      // event processor when processing a self event should update
      // last synced event id as well
      assertTrue(catalog_.getTable(TEST_DB_NAME, testTblName).getLastSyncedEventId() >
          lastSyncedEventIdBefore);
    }

    // add a col
    alterTableAddColsFromImpala(
        TEST_DB_NAME, testTblName, "newCol", TPrimitiveType.STRING);
    eventsProcessor_.processEvents();
    confirmTableIsLoaded(TEST_DB_NAME, testTblName);

    // remove a col
    alterTableRemoveColFromImpala(TEST_DB_NAME, testTblName, "newCol");
    eventsProcessor_.processEvents();
    confirmTableIsLoaded(TEST_DB_NAME, testTblName);

    // replace cols
    alterTableReplaceColFromImpala(TEST_DB_NAME, testTblName,
        Arrays.asList(getScalarColumn("testCol", TPrimitiveType.STRING)));
    eventsProcessor_.processEvents();
    confirmTableIsLoaded(TEST_DB_NAME, testTblName);

    TPartitionDef partitionDef = new TPartitionDef();
    partitionDef.addToPartition_spec(new TPartitionKeyValue("p1", "100"));
    partitionDef.addToPartition_spec(new TPartitionKeyValue("p2", "200"));

    // set fileformat
    alterTableSetFileFormatFromImpala(
        TEST_DB_NAME, testTblName, THdfsFileFormat.TEXT);
    eventsProcessor_.processEvents();
    confirmTableIsLoaded(TEST_DB_NAME, testTblName);

    // set rowformat
    alterTableSetRowFormatFromImpala(TEST_DB_NAME, testTblName, ",");
    eventsProcessor_.processEvents();
    confirmTableIsLoaded(TEST_DB_NAME, testTblName);

    // set owner
    alterTableSetOwnerFromImpala(TEST_DB_NAME, testTblName, "testowner");
    eventsProcessor_.processEvents();
    confirmTableIsLoaded(TEST_DB_NAME, testTblName);

    // rename table
    // Since rename is implemented as add+remove in impala, the new table is already
    // incomplete. We should load the table manually and make sure that events does not
    // invalidate it
    String newTblName = "newTableName";
    alterTableRenameFromImpala(TEST_DB_NAME, testTblName, newTblName);
    loadTable(TEST_DB_NAME, newTblName);
    eventsProcessor_.processEvents();
    confirmTableIsLoaded(TEST_DB_NAME, newTblName);

    Table newTbl = catalog_.getTable(TEST_DB_NAME, newTblName);
    lastSyncedEventIdBefore = newTbl.getLastSyncedEventId();
    org.apache.hadoop.hive.metastore.api.Table hmsTbl = catalog_.getTable(TEST_DB_NAME,
        newTblName).getMetaStoreTable();
    assertNotNull("Location is expected to be set to proceed forward in the test",
        hmsTbl.getSd().getLocation());
    String tblLocation = hmsTbl.getSd().getLocation();
    // set location
    alterTableSetLocationFromImpala(TEST_DB_NAME, newTblName, tblLocation + "_changed");
    eventsProcessor_.processEvents();
    confirmTableIsLoaded(TEST_DB_NAME, newTblName);

    if (BackendConfig.INSTANCE.enableSyncToLatestEventOnDdls()) {
      // self event processing by event processor should update last synced event id
      assertTrue(catalog_.getTable(TEST_DB_NAME, newTblName).getLastSyncedEventId() >
          lastSyncedEventIdBefore);
    }
    //TODO add test for alterview
    //add test for alterCommentOnTableOrView

    long selfEventsCountAfter = eventsProcessor_.getMetrics()
        .getCounter(MetastoreEventsProcessor.EVENTS_SKIPPED_METRIC).getCount();
    // 9 alter commands above.
    assertEquals("Unexpected number of self-events generated",
        numberOfSelfEventsBefore + 9, selfEventsCountAfter);
  }

  @Test
  public void testEventBatching() throws Exception {
    // create test table and generate a real ALTER_PARTITION and INSERT event on it
    String testTblName = "testEventBatching";
    createDatabaseFromImpala(TEST_DB_NAME, "test");
    createTable(TEST_DB_NAME, testTblName, true);
    List<List<String>> partVals = new ArrayList<>();
    partVals.add(Collections.singletonList("1"));
    partVals.add(Collections.singletonList("2"));
    addPartitions(TEST_DB_NAME, testTblName, partVals);
    eventsProcessor_.processEvents();

    // Collect some other types of events to test scenarios where an event cuts batching
    Map<String, String> miscEventTypesToMessage = new HashMap<>();
    createDatabaseFromImpala("database_to_be_dropped", "whatever");
    eventsProcessor_.processEvents();

    // Get ALTER_DATABASE event
    String currentLocation =
        catalog_.getDb("database_to_be_dropped").getMetaStoreDb().getLocationUri();
    String newLocation = currentLocation + File.separatorChar + "newTestLocation";
    Database alteredDb =
        catalog_.getDb("database_to_be_dropped").getMetaStoreDb().deepCopy();
    alteredDb.setLocationUri(newLocation);
    alterDatabase(alteredDb);
    List<NotificationEvent> alterDbEvents = eventsProcessor_.getNextMetastoreEvents();
    assertEquals(1, alterDbEvents.size());
    assertEquals("ALTER_DATABASE", alterDbEvents.get(0).getEventType());
    miscEventTypesToMessage.put("ALTER_DATABASE", alterDbEvents.get(0).getMessage());
    eventsProcessor_.processEvents();

    // Get DROP_DATABASE event
    dropDatabaseCascade("database_to_be_dropped");
    List<NotificationEvent> dropDbEvents = eventsProcessor_.getNextMetastoreEvents();
    assertEquals(1, dropDbEvents.size());
    assertEquals("DROP_DATABASE", dropDbEvents.get(0).getEventType());
    miscEventTypesToMessage.put("DROP_DATABASE", dropDbEvents.get(0).getMessage());
    eventsProcessor_.processEvents();

    // Get ALTER_TABLE rename event
    createTable(TEST_DB_NAME, "table_before_rename", false);
    eventsProcessor_.processEvents();
    alterTableRename("table_before_rename", "table_after_rename", null);
    List<NotificationEvent> alterTblEvents = eventsProcessor_.getNextMetastoreEvents();
    assertEquals(1, alterTblEvents.size());
    assertEquals("ALTER_TABLE", alterTblEvents.get(0).getEventType());
    miscEventTypesToMessage.put("ALTER_TABLE", alterTblEvents.get(0).getMessage());
    eventsProcessor_.processEvents();
    dropTable("table_after_rename");
    eventsProcessor_.processEvents();

    alterPartitionsParams(TEST_DB_NAME, testTblName, "testkey", "val", partVals);
    // we fetch a real alter partition event so that we can generate mocks using its
    // contents below
    List<NotificationEvent> events = eventsProcessor_.getNextMetastoreEvents();
    NotificationEvent alterPartEvent = null;
    String alterPartitionEventType = "ALTER_PARTITION";
    for (NotificationEvent event : events) {
      if (event.getEventType().equalsIgnoreCase(alterPartitionEventType)) {
        alterPartEvent = event;
        break;
      }
    }
    assertNotNull(alterPartEvent);
    String alterPartMessage = alterPartEvent.getMessage();

    // test insert event batching
    org.apache.hadoop.hive.metastore.api.Table msTbl;
    List<Partition> partitions;
    try (MetaStoreClient metaStoreClient = catalog_.getMetaStoreClient()) {
      msTbl = metaStoreClient.getHiveClient().getTable(TEST_DB_NAME, testTblName);
      partitions = MetastoreShim
          .getPartitions(metaStoreClient.getHiveClient(), TEST_DB_NAME, testTblName);
    }
    assertNotNull(msTbl);
    assertNotNull(partitions);
    eventsProcessor_.processEvents();
    // generate 2 insert event on each of the partition
    for (Partition part : partitions) {
      simulateInsertIntoTableFromFS(msTbl, 1, part, false);
    }
    List<NotificationEvent> insertEvents = eventsProcessor_.getNextMetastoreEvents();
    NotificationEvent insertEvent = null;
    for (NotificationEvent event : insertEvents) {
      if (event.getEventType().equalsIgnoreCase("INSERT")) {
        insertEvent = event;
        break;
      }
    }
    assertNotNull(insertEvent);
    Map<String, String> eventTypeToMessage = new HashMap<>();
    eventTypeToMessage.put("ALTER_PARTITION", alterPartMessage);
    eventTypeToMessage.put("INSERT", insertEvent.getMessage());
    runEventBatchingTest(testTblName, eventTypeToMessage, miscEventTypesToMessage);
  }

  @SuppressWarnings({"rawtypes", "unchecked"})
  private void runEventBatchingTest(String testTblName,
      Map<String, String> eventTypeToMessage,
      Map<String, String> miscEventTypesToMessage) throws DatabaseNotFoundException,
      MetastoreNotificationException {
    Table tbl = catalog_.getTable(TEST_DB_NAME, testTblName);
    long lastSyncedEventId = tbl.getLastSyncedEventId();
    for (String eventType : eventTypeToMessage.keySet()) {
      String eventMessage = eventTypeToMessage.get(eventType);
      // we have 10 mock batchable events which should be batched into 1. Always create
      // mock events with start event id much greater than table's lastSyncEventId
      List<MetastoreEvent> mockEvents = createMockEvents(lastSyncedEventId + 100, 10,
          eventType, TEST_DB_NAME, testTblName, eventMessage);
      MetastoreEventFactory eventFactory = eventsProcessor_.getEventsFactory();
      List<MetastoreEvent> batch = eventFactory.createBatchEvents(mockEvents,
          eventsProcessor_.getMetrics());
      assertEquals(1, batch.size());
      assertTrue(batch.get(0) instanceof BatchPartitionEvent);
      BatchPartitionEvent batchEvent = (BatchPartitionEvent) batch.get(0);
      assertEquals(10, batchEvent.getBatchEvents().size());

      // We support batching some non-contiguous events on the same table
      // as long as the events are independent. Test successful batching
      // of interleaved events from two tables.
      // Events on Table 1: 13-15,17-18,20 => 13-20 (batch size = 6)
      // Events on Table 2: 10-12,16,19,21-22 => 10-22 (batch size = 7)
      // Table 2
      mockEvents = createMockEvents(lastSyncedEventId + 110, 3,
          eventType, TEST_DB_NAME, "table2", eventMessage);
      // Table 1
      mockEvents.addAll(createMockEvents(lastSyncedEventId + 113, 3,
          eventType, TEST_DB_NAME, "table1", eventMessage));
      // Table 2
      mockEvents.addAll(createMockEvents(lastSyncedEventId + 116, 1,
          eventType, TEST_DB_NAME, "table2", eventMessage));
      // Table 1
      mockEvents.addAll(createMockEvents(lastSyncedEventId + 117, 2,
          eventType, TEST_DB_NAME, "table1", eventMessage));
      // Table 2
      mockEvents.addAll(createMockEvents(lastSyncedEventId + 119, 1,
          eventType, TEST_DB_NAME, "table2", eventMessage));
      // Table 1
      mockEvents.addAll(createMockEvents(lastSyncedEventId + 120, 1,
          eventType, TEST_DB_NAME, "table1", eventMessage));
      // Table 2
      mockEvents.addAll(createMockEvents(lastSyncedEventId + 121, 2,
          eventType, TEST_DB_NAME, "table2", eventMessage));
      batch = eventFactory.createBatchEvents(mockEvents, eventsProcessor_.getMetrics());
      assertEquals(2, batch.size());
      // Batch 1 must be the batch with the lower Event ID (i.e. table 1)
      BatchPartitionEvent batch1 = (BatchPartitionEvent) batch.get(0);
      assertEquals(6, batch1.getNumberOfEvents());
      assertEquals(lastSyncedEventId + 120, batch1.getEventId());
      assertEquals("table1", batch1.getTableName());
      // Batch 2 must be the batch with the higher Event ID (i.e. table 2)
      BatchPartitionEvent batch2 = (BatchPartitionEvent) batch.get(1);
      assertEquals(7, batch2.getNumberOfEvents());
      assertEquals(lastSyncedEventId + 122, batch2.getEventId());
      assertEquals("table2", batch2.getTableName());

      // test to make sure that events on different tables are still monotonic
      mockEvents = createMockEvents(lastSyncedEventId + 110, 1,
          eventType, TEST_DB_NAME, "table1", eventMessage);
      mockEvents.addAll(createMockEvents(lastSyncedEventId + 111, 1,
          eventType, TEST_DB_NAME, "table2", eventMessage));
      mockEvents.addAll(createMockEvents(lastSyncedEventId + 112, 1,
          eventType, TEST_DB_NAME, "table3", eventMessage));
      mockEvents.addAll(createMockEvents(lastSyncedEventId + 113, 1,
          eventType, TEST_DB_NAME, "table4", eventMessage));
      mockEvents.addAll(createMockEvents(lastSyncedEventId + 114, 1,
          eventType, TEST_DB_NAME, "table5", eventMessage));
      batch = eventFactory.createBatchEvents(mockEvents, eventsProcessor_.getMetrics());
      assertEquals(5, batch.size());
      for (int i = 0; i < batch.size(); i++) {
        MetastoreEvent monotonicEvent = batch.get(i);
        assertEquals(1, monotonicEvent.getNumberOfEvents());
        assertEquals(lastSyncedEventId + 110+i, monotonicEvent.getEventId());
        assertEquals("table"+(i+1), monotonicEvent.getTableName());
      }

      // test to make sure that events which have different database name are not
      // batched
      mockEvents = createMockEvents(lastSyncedEventId + 100, 1, eventType, TEST_DB_NAME,
          testTblName, eventMessage);
      mockEvents.addAll(createMockEvents(lastSyncedEventId + 101, 1, eventType, "db1",
          testTblName, eventMessage));

      List<MetastoreEvent> batchEvents = eventFactory.createBatchEvents(mockEvents,
          eventsProcessor_.getMetrics());
      assertEquals(2, batchEvents.size());
      for (MetastoreEvent event : batchEvents) {
        if (eventType.equalsIgnoreCase("ALTER_PARTITION")) {
          assertTrue(event instanceof AlterPartitionEvent);
        } else {
          assertTrue(event instanceof InsertEvent);
        }
      }

      // test no batching when table name is different
      mockEvents = createMockEvents(lastSyncedEventId + 100, 1, eventType, TEST_DB_NAME,
          testTblName, eventMessage);
      mockEvents.addAll(createMockEvents(lastSyncedEventId + 101, 1, eventType,
          TEST_DB_NAME, "testtbl", eventMessage));
      batchEvents = eventFactory.createBatchEvents(mockEvents,
          eventsProcessor_.getMetrics());
      assertEquals(2, batchEvents.size());
      for (MetastoreEvent event : batchEvents) {
        if (eventType.equalsIgnoreCase("ALTER_PARTITION")) {
          assertTrue(event instanceof AlterPartitionEvent);
        } else {
          assertTrue(event instanceof InsertEvent);
        }
      }

      // Test that alter database cuts batches for tables in that database
      // This test case may not be strictly necessary. Alter database should not
      // impact batchable events. Out of an abundance of caution, we test it
      // anyways.
      mockEvents = createMockEvents(lastSyncedEventId + 100, 1, eventType,
          "database_to_be_dropped", testTblName, eventMessage);
      mockEvents.addAll(
          createMockEvents(lastSyncedEventId + 101, 1, eventType, "other_database",
              testTblName, eventMessage));
      mockEvents.addAll(
          createMockEvents(lastSyncedEventId + 102, 1, "ALTER_DATABASE",
              "database_to_be_dropped", null,
              miscEventTypesToMessage.get("ALTER_DATABASE")));
      mockEvents.addAll(
          createMockEvents(lastSyncedEventId + 103, 1, eventType,
              "database_to_be_dropped", testTblName, eventMessage));
      mockEvents.addAll(
          createMockEvents(lastSyncedEventId + 104, 1, eventType, "other_database",
              testTblName, eventMessage));
      batchEvents = eventFactory.createBatchEvents(mockEvents,
          eventsProcessor_.getMetrics());
      assertEquals(4, batchEvents.size());
      assertTrue(batchEvents.get(1) instanceof AlterDatabaseEvent);
      assertEquals("database_to_be_dropped", batchEvents.get(0).getDbName());
      assertEquals("database_to_be_dropped", batchEvents.get(2).getDbName());
      assertEquals("other_database", batchEvents.get(3).getDbName());

      // Test that drop database cuts batches for tables in that database
      // This is a synthetic sequence that should not occur in the wild.
      mockEvents = createMockEvents(lastSyncedEventId + 100, 1, eventType,
          "database_to_be_dropped", testTblName, eventMessage);
      mockEvents.addAll(
          createMockEvents(lastSyncedEventId + 101, 1, eventType, "other_database",
              testTblName, eventMessage));
      mockEvents.addAll(
          createMockEvents(lastSyncedEventId + 102, 1, "DROP_DATABASE",
              "database_to_be_dropped", null,
              miscEventTypesToMessage.get("DROP_DATABASE")));
      mockEvents.addAll(
          createMockEvents(lastSyncedEventId + 103, 1, eventType,
              "database_to_be_dropped", testTblName, eventMessage));
      mockEvents.addAll(
          createMockEvents(lastSyncedEventId + 104, 1, eventType, "other_database",
              testTblName, eventMessage));
      batchEvents = eventFactory.createBatchEvents(mockEvents,
          eventsProcessor_.getMetrics());
      assertEquals(4, batchEvents.size());
      assertTrue(batchEvents.get(1) instanceof DropDatabaseEvent);
      assertEquals("database_to_be_dropped", batchEvents.get(0).getDbName());
      assertEquals("database_to_be_dropped", batchEvents.get(2).getDbName());
      assertEquals("other_database", batchEvents.get(3).getDbName());

      // Test that alter table rename cuts batches for the two table
      // names involved (but not an unrelated table).
      // This is a synthetic sequence that should not occur in the wild.
      mockEvents = createMockEvents(lastSyncedEventId + 100, 2, eventType, TEST_DB_NAME,
          "table_before_rename", eventMessage);
      mockEvents.addAll(
          createMockEvents(lastSyncedEventId + 102, 2, eventType, TEST_DB_NAME,
              "table_after_rename", eventMessage));
      mockEvents.addAll(
          createMockEvents(lastSyncedEventId + 104, 1, eventType, TEST_DB_NAME,
              "other_table", eventMessage));
      mockEvents.addAll(
          createMockEvents(lastSyncedEventId + 105, 1, "ALTER_TABLE", TEST_DB_NAME,
              "table_before_rename", miscEventTypesToMessage.get("ALTER_TABLE")));
      mockEvents.addAll(
          createMockEvents(lastSyncedEventId + 106, 2, eventType, TEST_DB_NAME,
              "table_before_rename", eventMessage));
      mockEvents.addAll(
          createMockEvents(lastSyncedEventId + 108, 2, eventType, TEST_DB_NAME,
              "table_after_rename", eventMessage));
      mockEvents.addAll(
          createMockEvents(lastSyncedEventId + 110, 1, eventType, TEST_DB_NAME,
              "other_table", eventMessage));
      batchEvents = eventFactory.createBatchEvents(mockEvents,
          eventsProcessor_.getMetrics());
      // The table_before_rename and table_after_rename tables keep their
      // events in order (with batching only for adjacent events).
      // The other_table can batch across the alter table event.
      assertEquals(6, batchEvents.size());
      BatchPartitionEvent altTblBatch0 = (BatchPartitionEvent) batchEvents.get(0);
      assertEquals(2, altTblBatch0.getNumberOfEvents());
      assertEquals("table_before_rename", altTblBatch0.getTableName());
      BatchPartitionEvent altTblBatch1 = (BatchPartitionEvent) batchEvents.get(1);
      assertEquals(2, altTblBatch1.getNumberOfEvents());
      assertEquals("table_after_rename", altTblBatch1.getTableName());
      assertTrue(batchEvents.get(2) instanceof AlterTableEvent);
      BatchPartitionEvent altTblBatch3 = (BatchPartitionEvent) batchEvents.get(3);
      assertEquals(2, altTblBatch3.getNumberOfEvents());
      assertEquals("table_before_rename", altTblBatch3.getTableName());
      BatchPartitionEvent altTblBatch4 = (BatchPartitionEvent) batchEvents.get(4);
      assertEquals(2, altTblBatch4.getNumberOfEvents());
      assertEquals("table_after_rename", altTblBatch4.getTableName());
      BatchPartitionEvent altTblBatch5 = (BatchPartitionEvent) batchEvents.get(5);
      assertEquals(2, altTblBatch5.getNumberOfEvents());
      assertEquals("other_table", altTblBatch5.getTableName());
    }
    // make sure 2 events of different event types are not batched together
    long startEventId = lastSyncedEventId + 117;
    // batch 1
    List<MetastoreEvent> mockEvents = createMockEvents(startEventId, 3,
        "ALTER_PARTITION", TEST_DB_NAME, testTblName,
        eventTypeToMessage.get("ALTER_PARTITION"));
    // batch 2
    mockEvents.addAll(createMockEvents(startEventId + mockEvents.size(), 3, "INSERT",
        TEST_DB_NAME, testTblName, eventTypeToMessage.get("INSERT")));
    // batch 3 : single event not-batched
    mockEvents.addAll(
        createMockEvents(startEventId + mockEvents.size(), 1, "ALTER_PARTITION",
        TEST_DB_NAME, testTblName, eventTypeToMessage.get("ALTER_PARTITION")));
    // batch 4 : single event not-batched
    mockEvents.addAll(createMockEvents(startEventId + mockEvents.size(), 1, "INSERT",
        TEST_DB_NAME, testTblName, eventTypeToMessage.get("INSERT")));
    // batch 5 : single event not-batched
    mockEvents.addAll(
        createMockEvents(startEventId + mockEvents.size(), 1, "ALTER_PARTITION",
            TEST_DB_NAME, testTblName, eventTypeToMessage.get("ALTER_PARTITION")));
    // batch 6
    mockEvents.addAll(createMockEvents(startEventId + mockEvents.size(), 5, "INSERT",
        TEST_DB_NAME, testTblName, eventTypeToMessage.get("INSERT")));
    // batch 7
    mockEvents.addAll(
        createMockEvents(startEventId + mockEvents.size(), 5, "ALTER_PARTITION",
            TEST_DB_NAME, testTblName, eventTypeToMessage.get("ALTER_PARTITION")));
    List<MetastoreEvent> batchedEvents = eventsProcessor_.getEventsFactory()
        .createBatchEvents(mockEvents, eventsProcessor_.getMetrics());
    assertEquals(7, batchedEvents.size());
    // batch 1 should contain 3 AlterPartitionEvent
    BatchPartitionEvent<AlterPartitionEvent> batch1 = (BatchPartitionEvent
        <AlterPartitionEvent>) batchedEvents.get(0);
    assertEquals(3, batch1.getNumberOfEvents());
    // batch 2 should contain 3 InsertEvent
    BatchPartitionEvent<InsertEvent> batch2 = (BatchPartitionEvent
        <InsertEvent>) batchedEvents.get(1);
    assertEquals(3, batch2.getNumberOfEvents());
    // 3rd is a single non-batch event
    assertTrue(batchedEvents.get(2) instanceof AlterPartitionEvent);
    // 4th is a single non-batch insert event
    assertTrue(batchedEvents.get(3) instanceof InsertEvent);
    // 5th is a single non-batch alter partition event
    assertTrue(batchedEvents.get(4) instanceof AlterPartitionEvent);
    // 6th is batch insert event of size 5
    BatchPartitionEvent<InsertEvent> batch6 = (BatchPartitionEvent
        <InsertEvent>) batchedEvents.get(5);
    assertEquals(5, batch6.getNumberOfEvents());
    // 7th is batch of alter partitions of size 5
    BatchPartitionEvent<AlterPartitionEvent> batch7 = (BatchPartitionEvent
        <AlterPartitionEvent>) batchedEvents.get(6);
    assertEquals(5, batch7.getNumberOfEvents());
  }


  /**
   * Creates mock notification event from the given list of parameters. The message
   * should be a legal parsable message from a real notification event.
   */
  private List<MetastoreEvent> createMockEvents(long startEventId, int numEvents,
      String eventType, String dbName, String tblName, String message)
      throws MetastoreNotificationException {
    List<NotificationEvent> mockEvents = new ArrayList<>();
    for (int i=0; i<numEvents; i++) {
      NotificationEvent mock = Mockito.mock(NotificationEvent.class);
      when(mock.getEventId()).thenReturn(startEventId);
      when(mock.getEventType()).thenReturn(eventType);
      when(mock.getDbName()).thenReturn(dbName);
      when(mock.getTableName()).thenReturn(tblName);
      when(mock.getMessage()).thenReturn(message);
      mockEvents.add(mock);
      startEventId++;
    }
    List<MetastoreEvent> metastoreEvents = new ArrayList<>();
    for (NotificationEvent notificationEvent : mockEvents) {
      metastoreEvents.add(eventsProcessor_.getEventsFactory().get(notificationEvent,
          eventsProcessor_.getMetrics()));
    }
    return metastoreEvents;
  }

  @Test
  public void testCommitEvent() throws TException, ImpalaException, IOException {
    // Turn on incremental refresh for transactional table
    final TBackendGflags origCfg = BackendConfig.INSTANCE.getBackendCfg();
    try {
      final TBackendGflags stubCfg = origCfg.deepCopy();
      stubCfg.setHms_event_incremental_refresh_transactional_table(true);
      // also enable sync to latest event on ddls
      stubCfg.setEnable_sync_to_latest_event_on_ddls(true);
      BackendConfig.create(stubCfg);

      createDatabase(TEST_DB_NAME, null);
      testInsertIntoTransactionalTable("testCommitEvent_transactional", false, false);
      testInsertIntoTransactionalTable("testCommitEvent_transactional_part", false, true);
    } finally {
      // Restore original config
      BackendConfig.create(origCfg);
    }
  }

  @Test
  public void testAbortEvent() throws TException, ImpalaException, IOException {
    // Turn on incremental refresh for transactional table
    final TBackendGflags origCfg = BackendConfig.INSTANCE.getBackendCfg();
    try {
      final TBackendGflags stubCfg = origCfg.deepCopy();
      stubCfg.setHms_event_incremental_refresh_transactional_table(true);
      // also enable sync to latest event on ddls
      stubCfg.setEnable_sync_to_latest_event_on_ddls(true);
      BackendConfig.create(stubCfg);

      createDatabase(TEST_DB_NAME, null);
      testInsertIntoTransactionalTable("testAbortEvent_transactional", true, false);
      testInsertIntoTransactionalTable("testAbortEvent_transactional_part", true, true);
    } finally {
      // Restore original config
      BackendConfig.create(origCfg);
    }
  }

  private void testInsertIntoTransactionalTable(String tblName, boolean toAbort
      , boolean isPartitioned) throws TException, CatalogException,
      TransactionException, IOException {
    createTransactionalTable(TEST_DB_NAME, tblName, isPartitioned);
    if (isPartitioned) {
      List<List<String>> partVals = new ArrayList<>();
      partVals.add(Arrays.asList("1"));
      addPartitions(TEST_DB_NAME, tblName, partVals);
    }
    eventsProcessor_.processEvents();
    loadTable(tblName);
    HdfsTable tbl = (HdfsTable)catalog_.getTable(TEST_DB_NAME, tblName);
    long lastSyncedEventId = tbl.getLastSyncedEventId();

    try (MetaStoreClient client = catalog_.getMetaStoreClient()) {
      long txnId = MetastoreShim.openTransaction(client.getHiveClient());
      long writeId = MetastoreShim.allocateTableWriteId(client.getHiveClient(),
          txnId, TEST_DB_NAME, tblName);
      eventsProcessor_.processEvents();

      if (isPartitioned) {
        // open and alloc write id events should be processed by event processor
        // these events are ignored for non-partitioned tables
        assertTrue(String.format("Expected last synced event id: %s for table %s to be "
                + "greater than %s", tbl.getLastSyncedEventId(), tbl.getFullName(),
            lastSyncedEventId), tbl.getLastSyncedEventId() > lastSyncedEventId);
      }
      lastSyncedEventId = tbl.getLastSyncedEventId();
      ValidWriteIdList writeIdList = tbl.getValidWriteIds();
      assertFalse(writeIdList.isWriteIdValid(writeId));
      assertFalse(writeIdList.isWriteIdAborted(writeId));
      Partition partition = null;
      if (isPartitioned) {
        partition = client.getHiveClient().getPartition(
            TEST_DB_NAME, tblName, Arrays.asList("1"));
      }
      simulateInsertIntoTransactionalTableFromFS(
          tbl.getMetaStoreTable(), partition, 1, txnId, writeId);
      if (toAbort) {
        MetastoreShim.abortTransaction(client.getHiveClient(), txnId);
      } else {
        MetastoreShim.commitTransaction(client.getHiveClient(), txnId);
      }
      eventsProcessor_.processEvents();
      String partName = isPartitioned ? "p1=1" : "";
      int numFiles = ((HdfsTable) tbl)
          .getPartitionsForNames(Collections.singletonList(partName))
          .get(0)
          .getNumFileDescriptors();
      writeIdList = tbl.getValidWriteIds();
      if (toAbort) {
        // For non-partitioned tables, we don't keep track of write ids in the Catalog
        // to reduce memory footprint so the writeIdList won't be up-to-date until next
        // table reloading.
        assertEquals(0, numFiles);
        if (isPartitioned) {
          assertTrue(String.format("Expected last synced event id: %s for table %s to be "
                  + "greater than %s", tbl.getLastSyncedEventId(), tbl.getFullName(),
              lastSyncedEventId), tbl.getLastSyncedEventId() > lastSyncedEventId);
        }
      } else {
        assertTrue(writeIdList.isWriteIdValid(writeId));
        assertEquals(1, numFiles);
        assertTrue(String.format("Expected last synced event id: %s for table %s to be "
                + "greater than %s", tbl.getLastSyncedEventId(), tbl.getFullName(),
            lastSyncedEventId), tbl.getLastSyncedEventId() > lastSyncedEventId);

      }
    }
  }

  /**
   * The test asserts that file metadata is not reloaded when processing
   * alter partition event for a transactional table. Because for such tables,
   * file metadata reloading is done during commit txn event processing
   * @throws Exception
   */
  @Test
  public void testAlterPartitionNotReloadFMD() throws Exception {
    // Turn on incremental refresh for transactional table
    final TBackendGflags origCfg = BackendConfig.INSTANCE.getBackendCfg();
    try {
      final TBackendGflags stubCfg = origCfg.deepCopy();
      stubCfg.setHms_event_incremental_refresh_transactional_table(true);
      BackendConfig.create(stubCfg);

      String testTblName = "testAlterPartitionNotReloadFMD";
      createDatabase(TEST_DB_NAME, null);
      createTransactionalTable(TEST_DB_NAME, testTblName, true);
      List<List<String>> partVals = new ArrayList<>();
      partVals.add(Arrays.asList("1"));
      addPartitions(TEST_DB_NAME, testTblName, partVals);
      try (MetaStoreClient client = catalog_.getMetaStoreClient()) {
        long txnId = MetastoreShim.openTransaction(client.getHiveClient());
        long writeId = MetastoreShim.allocateTableWriteId(client.getHiveClient(),
            txnId, TEST_DB_NAME, testTblName);
        Partition partition = client.getHiveClient().getPartition(TEST_DB_NAME,
            testTblName, Arrays.asList("1"));
        org.apache.hadoop.hive.metastore.api.Table table =
            client.getHiveClient().getTable(TEST_DB_NAME, testTblName);
        simulateInsertIntoTransactionalTableFromFS(table, partition, 1, txnId, writeId);
        MetastoreShim.commitTransaction(client.getHiveClient(), txnId);
      }
      eventsProcessor_.processEvents();
      loadTable(testTblName);
      HdfsTable tbl = (HdfsTable) catalog_.getTable(TEST_DB_NAME, testTblName);

      alterPartitionsParamsInTxn(TEST_DB_NAME, testTblName, "testAlterPartition", "true",
          partVals);
      assertNull(tbl.getPartitionsForNames(Collections.singletonList("p1=1")).get(0)
          .getParameters().get("testAlterPartition"));
      long numLoadFMDBefore =
          tbl.getMetrics().getCounter(HdfsTable.NUM_LOAD_FILEMETADATA_METRIC).getCount();
      List<HdfsPartition.FileDescriptor> FDbefore = tbl.getPartitionsForNames(
          Collections.singletonList("p1=1")).get(0).getFileDescriptors();
      eventsProcessor_.processEvents();
      // After event processing, parameters should be updated
      assertNotNull(tbl.getPartitionsForNames(Collections.singletonList("p1=1")).get(0)
          .getParameters().get("testAlterPartition"));
      // However, file metadata should not be reloaded after an alter partition event
      long numLoadFMDAfter =
          tbl.getMetrics().getCounter(HdfsTable.NUM_LOAD_FILEMETADATA_METRIC).getCount();
      List<HdfsPartition.FileDescriptor> FDafter = tbl.getPartitionsForNames(
          Collections.singletonList("p1=1")).get(0).getFileDescriptors();
      assertEquals("File metadata should not be reloaded",
          numLoadFMDBefore, numLoadFMDAfter);
      // getFileDescriptors() always returns a new instance, so we need to compare the
      // underlying array
      assertEquals(Lists.transform(FDbefore, HdfsPartition.FileDescriptor.TO_BYTES),
          Lists.transform(FDafter, HdfsPartition.FileDescriptor.TO_BYTES));
    } finally {
      // Restore original config
      BackendConfig.create(origCfg);
    }
  }

  /**
   * Some of the alter table events doesn't require to reload file metadata, this
   * test asserts that file metadata is not reloaded for such alter table events
   */
  @Test
  public void testAlterTableNoFileMetadataReload() throws Exception {
    createDatabase(TEST_DB_NAME, null);
    final String testTblName = "testAlterTableNoFileMetadataReload";
    // Create a non-empty partitioned table.
    createTable(testTblName, true);
    List<List<String>> partVals = new ArrayList<>();
    partVals.add(Arrays.asList("1"));
    addPartitions(TEST_DB_NAME, testTblName, partVals);
    eventsProcessor_.processEvents();
    // load the table first
    loadTable(testTblName);
    HdfsTable tbl = (HdfsTable) catalog_.getTable(TEST_DB_NAME, testTblName);
    // get file metadata load counter before altering the partition
    long fileMetadataLoadBefore =
        tbl.getMetrics().getCounter(HdfsTable.NUM_LOAD_FILEMETADATA_METRIC).getCount();
    // Test 1: alter table add cols
    alterTableAddCol(testTblName, "newCol", "string", "");
    eventsProcessor_.processEvents();

    // Test 2: alter table change column type
    altertableChangeCol(testTblName, "newCol", "int", null);
    eventsProcessor_.processEvents();

    // Test 3: alter table set owner
    alterTableSetOwner(testTblName, "testOwner");
    eventsProcessor_.processEvents();

    // Test 4: alter table set non-whitelisted tbl properties
    alterTableAddParameter(testTblName, "dummyKey1", "dummyValue1");
    eventsProcessor_.processEvents();

    // Verify that the file metadata isn't reloaded
    tbl = (HdfsTable) catalog_.getTable(TEST_DB_NAME, testTblName);
    long fileMetadataLoadAfter =
        tbl.getMetrics().getCounter(HdfsTable.NUM_LOAD_FILEMETADATA_METRIC).getCount();
    assertEquals(fileMetadataLoadAfter, fileMetadataLoadBefore);

    // Test 5: alter table add whitelisted property
    alterTableAddParameter(testTblName, "EXTERNAL", "true");
    eventsProcessor_.processEvents();
    // Verify that the file metadata is reloaded
    tbl = (HdfsTable) catalog_.getTable(TEST_DB_NAME, testTblName);
    fileMetadataLoadAfter =
        tbl.getMetrics().getCounter(HdfsTable.NUM_LOAD_FILEMETADATA_METRIC).getCount();
    assertNotEquals(fileMetadataLoadAfter, fileMetadataLoadBefore);
    fileMetadataLoadBefore = fileMetadataLoadAfter;

    // Test 6: alter table change whitelisted property
    alterTableAddParameter(testTblName, "EXTERNAL", "false");
    eventsProcessor_.processEvents();
    // Verify that the file metadata is reloaded
    tbl = (HdfsTable) catalog_.getTable(TEST_DB_NAME, testTblName);
    fileMetadataLoadAfter =
        tbl.getMetrics().getCounter(HdfsTable.NUM_LOAD_FILEMETADATA_METRIC).getCount();
    assertNotEquals(fileMetadataLoadAfter, fileMetadataLoadBefore);
    fileMetadataLoadBefore = fileMetadataLoadAfter;

    // Test 6: alter table set the same value for whitelisted property
    alterTableAddParameter(testTblName, "EXTERNAL", "false");
    eventsProcessor_.processEvents();
    // Verify that the file metadata isn't reloaded
    tbl = (HdfsTable) catalog_.getTable(TEST_DB_NAME, testTblName);
    fileMetadataLoadAfter =
        tbl.getMetrics().getCounter(HdfsTable.NUM_LOAD_FILEMETADATA_METRIC).getCount();
    assertEquals(fileMetadataLoadAfter, fileMetadataLoadBefore);

    // Test 7: alter table remove whitelisted property
    alterTableAddParameter(testTblName, "EXTERNAL", null); // sending null value will
    // unset the parameter
    eventsProcessor_.processEvents();
    // Verify that the file metadata is reloaded
    tbl = (HdfsTable) catalog_.getTable(TEST_DB_NAME, testTblName);
    fileMetadataLoadAfter =
        tbl.getMetrics().getCounter(HdfsTable.NUM_LOAD_FILEMETADATA_METRIC).getCount();
    assertNotEquals(fileMetadataLoadAfter, fileMetadataLoadBefore);
  }

  /**
   * Some of the the alter table events on Storage Descriptor doesn't require to reload
   * file metadata, this test asserts that file metadata is not reloaded for such alter
   * table events
   */
  @Test
  public void testAlterTableSdVerifyMetadataReload() throws Exception {
    createDatabase(TEST_DB_NAME, null);
    final String testTblName = "testSdFileMetadataReload";
    createTable(testTblName, true);
    List<List<String>> partVals = new ArrayList<>();
    partVals.add(Arrays.asList("1"));
    addPartitions(TEST_DB_NAME, testTblName, partVals);
    eventsProcessor_.processEvents();
    // load the table first
    loadTable(testTblName);
    HdfsTable tbl = (HdfsTable) catalog_.getTable(TEST_DB_NAME, testTblName);
    // get file metadata load counter before altering the partition
    long fileMetadataLoadBefore =
        tbl.getMetrics().getCounter(HdfsTable.NUM_LOAD_FILEMETADATA_METRIC).getCount();

    // Test 1: Set file format
    LOG.info("Test changes in file format for an Alter table statement");
    org.apache.hadoop.hive.metastore.api.Table hmsTbl = catalog_.getTable(TEST_DB_NAME,
        testTblName).getMetaStoreTable();
    hmsTbl.getSd().setInputFormat("org.apache.hadoop.mapred.TextInputFormat");
    hmsTbl.getSd().setOutputFormat(
        "org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat");
    long fileMetadataLoadAfter = processAlterTableAndReturnMetric(testTblName, hmsTbl);
    assertEquals(fileMetadataLoadBefore + 1, fileMetadataLoadAfter);
    fileMetadataLoadBefore = fileMetadataLoadAfter;

    // Test 2: set rowformat
    LOG.info("Test changes in row format for Alter table statement");
    hmsTbl = catalog_.getTable(TEST_DB_NAME, testTblName).getMetaStoreTable();
    hmsTbl.getSd().getSerdeInfo().setSerializerClass(
        "org.apache.hadoop.hive.contrib.serde2.RegexSerDe");
    fileMetadataLoadAfter = processAlterTableAndReturnMetric(testTblName, hmsTbl);
    assertEquals(fileMetadataLoadBefore + 1, fileMetadataLoadAfter);
    fileMetadataLoadBefore = fileMetadataLoadAfter;

    // Test 3: set location
    LOG.info("Test changes in table location for Alter table statement");
    hmsTbl = catalog_.getTable(TEST_DB_NAME, testTblName).getMetaStoreTable();
    assertNotNull("Location is expected to be set to proceed forward in the test",
        hmsTbl.getSd().getLocation());
    String tblLocation = hmsTbl.getSd().getLocation() + "_changed";
    hmsTbl.getSd().setLocation(tblLocation);
    fileMetadataLoadAfter = processAlterTableAndReturnMetric(testTblName, hmsTbl);
    assertEquals(fileMetadataLoadBefore + 1, fileMetadataLoadAfter);
    fileMetadataLoadBefore = fileMetadataLoadAfter;

    // Test 4: set storedAsSubDirectories from false to unset
    LOG.info("Test changes in storedAsSubDirectories from false to unset");
    hmsTbl = tbl.getMetaStoreTable().deepCopy();
    hmsTbl.getSd().unsetStoredAsSubDirectories();
    fileMetadataLoadAfter = processAlterTableAndReturnMetric(testTblName, hmsTbl);
    assertEquals(fileMetadataLoadBefore, fileMetadataLoadAfter);

    // Test 5: set storedAsSubDirectories to true
    LOG.info("Test changes in storedAsSubDirectories from unset to true");
    hmsTbl.getSd().setStoredAsSubDirectories(true);
    fileMetadataLoadAfter = processAlterTableAndReturnMetric(testTblName, hmsTbl);
    assertEquals(fileMetadataLoadBefore + 1, fileMetadataLoadAfter);
    fileMetadataLoadBefore = fileMetadataLoadAfter;

    // Test 6: set storedAsSubDirectories from true to false
    LOG.info("Test changes in storedAsSubDirectories from true to false");
    hmsTbl.getSd().setStoredAsSubDirectories(false);
    fileMetadataLoadAfter = processAlterTableAndReturnMetric(testTblName, hmsTbl);
    assertEquals(fileMetadataLoadBefore + 1, fileMetadataLoadAfter);

    // Test 7: set storedAsSubDirectories from unset to false
    LOG.info("Test changes in storedAsSubDirectories from unset to false");
    // first set the value to unset from false
    hmsTbl.getSd().unsetStoredAsSubDirectories();
    fileMetadataLoadAfter = processAlterTableAndReturnMetric(testTblName, hmsTbl);
    fileMetadataLoadBefore = fileMetadataLoadAfter;
    hmsTbl.getSd().setStoredAsSubDirectories(false);
    fileMetadataLoadAfter = processAlterTableAndReturnMetric(testTblName, hmsTbl);
    assertEquals(fileMetadataLoadBefore, fileMetadataLoadAfter);

    // Test 8: keep SD unchanged by setting the same value and also change non-trivial
    // TblProperties to verify file metadata is reloaded
    LOG.info("Test changes in non-trivial tbl props and same SD");
    hmsTbl = tbl.getMetaStoreTable().deepCopy();
    hmsTbl.getSd().setStoredAsSubDirectories(false);
    hmsTbl.getParameters().put("EXTERNAL", "false");
    fileMetadataLoadAfter = processAlterTableAndReturnMetric(testTblName, hmsTbl);
    assertEquals(fileMetadataLoadBefore + 1, fileMetadataLoadAfter);
  }

  private long processAlterTableAndReturnMetric(String testTblName,
      org.apache.hadoop.hive.metastore.api.Table msTbl) throws Exception {
    try (MetaStoreClient msClient = catalog_.getMetaStoreClient()) {
      msClient.getHiveClient().alter_table_with_environmentContext(
          TEST_DB_NAME, testTblName, msTbl, null);
    }
    eventsProcessor_.processEvents();
    HdfsTable tbl = (HdfsTable) catalog_.getTable(TEST_DB_NAME, testTblName);
    return
        tbl.getMetrics().getCounter(HdfsTable.NUM_LOAD_FILEMETADATA_METRIC).getCount();
  }

  /**
   * For an external table, the test asserts file metadata is not reloaded during
   * AlterPartitionEvent processing if only partition parameters are changed
   * @throws Exception
   */
  @Test
  public void testAlterPartitionNoFileMetadataReload() throws Exception {
    createDatabase(TEST_DB_NAME, null);
    final String testTblName = "testAlterPartitionNoFileMetadataReload";
    createTable(testTblName, true);
    eventsProcessor_.processEvents();
    // simulate the table being loaded by explicitly calling load table
    loadTable(testTblName);
    List<List<String>> partVals = new ArrayList<>();

    // create 1 partition
    partVals.add(Arrays.asList("1"));
    addPartitions(TEST_DB_NAME, testTblName, partVals);

    eventsProcessor_.processEvents();
    assertEquals("Unexpected number of partitions fetched for the loaded table", 1,
        ((HdfsTable) catalog_.getTable(TEST_DB_NAME, testTblName))
            .getPartitions()
            .size());
    HdfsTable tbl = (HdfsTable) catalog_.getTable(TEST_DB_NAME, testTblName);
    // get file metadata load counter before altering the partition
    long fileMetadataLoadBefore =
        tbl.getMetrics().getCounter(HdfsTable.NUM_LOAD_FILEMETADATA_METRIC).getCount();
    // issue alter partition ops
    partVals.clear();
    partVals.add(Arrays.asList("1"));
    String testKey = "randomDummyKey1";
    String testVal = "randomDummyVal1";
    alterPartitionsParams(TEST_DB_NAME, testTblName, testKey, testVal,
        partVals);
    eventsProcessor_.processEvents();

    tbl = (HdfsTable) catalog_.getTable(TEST_DB_NAME, testTblName);
    long fileMetadataLoadAfter =
        tbl.getMetrics().getCounter(HdfsTable.NUM_LOAD_FILEMETADATA_METRIC).getCount();
    assertEquals(fileMetadataLoadAfter, fileMetadataLoadBefore);

    Collection<? extends FeFsPartition> parts =
        FeCatalogUtils.loadAllPartitions((HdfsTable)
            catalog_.getTable(TEST_DB_NAME, testTblName));
    FeFsPartition singlePartition =
        Iterables.getOnlyElement(parts);
    String val = singlePartition.getParameters().getOrDefault(testKey, null);
    assertNotNull("Expected " + testKey + " to be present in partition parameters",
        val);
    assertEquals(testVal, val);
  }

  /*
  For an external table, the test asserts that file metadata is reloaded for an alter
   partition event if the storage descriptor of partition to be altered has changed or
  partition does not exist in cache but is present in HMS
   */
  @Test
  public void testAlterPartitionFileMetadataReload() throws Exception {
    createDatabase(TEST_DB_NAME, null);
    final String testTblName = "testAlterPartitionFileMetadataReload";
    createTable(testTblName, true);
    // sync to latest event id
    eventsProcessor_.processEvents();

    // simulate the table being loaded by explicitly calling load table
    loadTable(testTblName);
    List<List<String>> partVals = new ArrayList<>();

    // create 1 partition
    partVals.add(Arrays.asList("1"));
    addPartitions(TEST_DB_NAME, testTblName, partVals);

    eventsProcessor_.processEvents();
    assertEquals("Unexpected number of partitions fetched for the loaded table", 1,
        ((HdfsTable) catalog_.getTable(TEST_DB_NAME, testTblName))
            .getPartitions()
            .size());
    // HdfsPartitionSdCompareTest already covers all the ways in which storage
    // descriptor can differ. Here we will only alter sd location to test that
    // file metadata is reloaded.
    HdfsTable tbl = (HdfsTable) catalog_.getTable(TEST_DB_NAME, testTblName);
    long fileMetadataLoadBefore =
        tbl.getMetrics().getCounter(HdfsTable.NUM_LOAD_FILEMETADATA_METRIC).getCount();

    partVals.clear();
    partVals.add(Arrays.asList("1"));
    String newLocation = "/path/to/new_location/";
    alterPartitions(testTblName, partVals, newLocation);
    eventsProcessor_.processEvents();
    // not asserting new location check since it is already done in previous tests
    long fileMetadataLoadAfter =
        tbl.getMetrics().getCounter(HdfsTable.NUM_LOAD_FILEMETADATA_METRIC).getCount();
    assertEquals(fileMetadataLoadBefore + 1, fileMetadataLoadAfter);
    /*
     Drop a partition only from cache not from HMS. In such case,
     the expected behavior is that tbl.reloadPartitions() for such
     partition should *not* skip file metadata reload even if
     FileMetadataLoadOpts is LOAD_IF_SD_CHANGED. The following code asserts
     this behaviour
     */
    String partName = FeCatalogUtils.getPartitionName(tbl, partVals.get(0));
    List<HdfsPartition> partitionsToDrop =
        tbl.getPartitionsForNames(Arrays.asList(partName));
    tbl.dropPartitions(partitionsToDrop, false);
    assertEquals(0, tbl.getPartitions().size());

    fileMetadataLoadBefore =
        tbl.getMetrics().getCounter(HdfsTable.NUM_LOAD_FILEMETADATA_METRIC).getCount();
    Partition msPartition = null;
    try (MetaStoreClient metaStoreClient = catalog_.getMetaStoreClient()) {
      msPartition = metaStoreClient.getHiveClient().getPartition(TEST_DB_NAME,
          testTblName, Arrays.asList("1"));
      Map<Partition, HdfsPartition> mp = new HashMap();
      mp.put(msPartition, null);
      // reload the just dropped partition again with LOAD_IF_SD_CHANGED file
      // metadataOpts. Assert that partition is loaded back in tbl
      if (!catalog_.tryWriteLock(tbl)) {
        throw new CatalogException("Couldn't acquire write lock on table: " +
            tbl.getFullName());
      }
      catalog_.getLock().writeLock().unlock();
      try {
        tbl.reloadPartitions(metaStoreClient.getHiveClient(), mp,
            FileMetadataLoadOpts.LOAD_IF_SD_CHANGED, NoOpEventSequence.INSTANCE);
      } finally {
        if (tbl.isWriteLockedByCurrentThread()) {
          tbl.releaseWriteLock();
        }
      }
    }
    assertEquals(1, tbl.getPartitionsForNames(Arrays.asList(partName)).size());

    fileMetadataLoadAfter =
        tbl.getMetrics().getCounter(HdfsTable.NUM_LOAD_FILEMETADATA_METRIC).getCount();

    assertEquals(fileMetadataLoadBefore+1, fileMetadataLoadAfter);
  }

  private abstract class AlterTableExecutor {
    protected abstract void execute() throws Exception;

    protected long getNumTblsRefreshed() {
      return eventsProcessor_.getMetrics()
          .getCounter(MetastoreEventsProcessor.NUMBER_OF_TABLE_REFRESHES).getCount();
    }
  }

  private class HiveAlterTableExecutor extends AlterTableExecutor {
    private final String tblName_;
    private final String colName_ = "hiveColName";
    private final String colType_ = "string";
    private final AtomicBoolean toggle_ = new AtomicBoolean(true);

    private HiveAlterTableExecutor(String dbName, String tblName) {
      this.tblName_ = tblName;
    }

    public void execute() throws Exception {
      Table tblBefore = Preconditions.checkNotNull(catalog_.getTable(TEST_DB_NAME,
          tblName_));
      boolean incompleteBefore = tblBefore instanceof IncompleteTable;
      if (toggle_.get()) {
        alterTableAddCol(tblName_, colName_, colType_, "");
      } else {
        alterTableRemoveCol(tblName_, colName_);
      }
      verify(incompleteBefore);
      toggle_.compareAndSet(toggle_.get(), !toggle_.get());
    }

    private void verify(boolean wasTblIncompleteBefore) throws Exception {
      long numTblsRefreshedBefore = getNumTblsRefreshed();
      eventsProcessor_.processEvents();
      Table catTable = catalog_.getTable(TEST_DB_NAME, tblName_);
      assertNotNull(catTable);
      if (wasTblIncompleteBefore) {
        assertTrue("Table should not reloaded if its already incomplete",
            catTable instanceof IncompleteTable);
        assertTrue(numTblsRefreshedBefore == getNumTblsRefreshed());
      } else {
        assertFalse("Table should have been reloaded if its loaded before",
            catTable instanceof IncompleteTable);
        assertTrue(numTblsRefreshedBefore < getNumTblsRefreshed());
      }

    }
  }


  private class ImpalaAlterTableExecutor extends AlterTableExecutor {
    private final String tblName_;
    private final String colName_ = "impalaColName";
    private final TPrimitiveType colType_ = TPrimitiveType.STRING;
    private final AtomicBoolean toggle_ = new AtomicBoolean(true);

    private ImpalaAlterTableExecutor(String dbName, String tblName) {
      this.tblName_ = tblName;
    }

    public void execute() throws Exception {
      if (toggle_.get()) {
        alterTableAddColsFromImpala(TEST_DB_NAME, tblName_, colName_, colType_);
      } else {
        alterTableRemoveColFromImpala(TEST_DB_NAME, tblName_, colName_);
      }
      verify();
      toggle_.compareAndSet(toggle_.get(), !toggle_.get());
    }

    public void verify() throws Exception {
      long numTblsRefreshedBefore = getNumTblsRefreshed();
      eventsProcessor_.processEvents();
      Table catTable = catalog_.getTable(TEST_DB_NAME, tblName_);
      assertNotNull(catTable);
      assertFalse(catTable instanceof IncompleteTable);
      // this is a self-event, table should not be refreshed
      assertTrue(numTblsRefreshedBefore == getNumTblsRefreshed());
    }
  }

  /**
   * Test generates alter table events from Hive and Impala. Any event generated by
   * Impala should not invalidate the table while the event from Hive should invalidate
   * the table
   */
  @Test
  public void testSelfEventsWithInterleavedClients() throws Exception {
    createDatabase(TEST_DB_NAME, null);
    createTable("self_event_tbl", false);
    eventsProcessor_.processEvents();

    int numOfExecutions = 100;
    AlterTableExecutor hiveExecutor = new HiveAlterTableExecutor(TEST_DB_NAME,
        "self_event_tbl");
    AlterTableExecutor impalaExecutor = new ImpalaAlterTableExecutor(TEST_DB_NAME,
        "self_event_tbl");
    // fixed seed makes the test repeatable
    Random random = new Random(117);
    for (int i=0; i<numOfExecutions; i++) {
      if (random.nextBoolean()) {
        hiveExecutor.execute();
      } else {
        impalaExecutor.execute();
      }
    }
  }

  /**
   * Tests executes DDLs for which self-events are not supported. Makes sure that the
   * table is invalidated after receiving these events
   */
  @Test
  public void testSelfEventsForTableUnsupportedCases() throws Exception {
    createDatabase(TEST_DB_NAME, null);
    eventsProcessor_.processEvents();
    final String testTblName = "testSelfEventsForTableUnsupportedCases";
    createTableFromImpala(TEST_DB_NAME, testTblName, true);
    TPartitionDef partitionDef = new TPartitionDef();
    partitionDef.addToPartition_spec(new TPartitionKeyValue("p1", "100"));
    partitionDef.addToPartition_spec(new TPartitionKeyValue("p2", "200"));
    alterTableAddPartition(TEST_DB_NAME, testTblName, partitionDef);
    eventsProcessor_.processEvents();
    confirmTableIsLoaded(TEST_DB_NAME, testTblName);

    TPartitionKeyValue partitionKeyValue1 = new TPartitionKeyValue("p1", "100");
    TPartitionKeyValue partitionKeyValue2 = new TPartitionKeyValue("p2", "200");

    // remove a partition
    alterTableDropPartition(TEST_DB_NAME, testTblName,
        Arrays.asList(partitionKeyValue1, partitionKeyValue2));
    eventsProcessor_.processEvents();
    assertNotNull(catalog_.getTable(TEST_DB_NAME, testTblName));
  }

  /**
   * Test executes alter partition events from Impala and makes sure that table is not
   * invalidated
   */
  @Test
  public void testSelfEventsForPartition() throws ImpalaException, TException {
    createDatabase(TEST_DB_NAME, null);
    final String testTblName = "testSelfEventsForPartition";
    createTable(testTblName, true);
    // create 2 partitions
    List<List<String>> partVals = new ArrayList<>(2);
    partVals.add(Arrays.asList("1"));
    partVals.add(Arrays.asList("2"));
    addPartitions(TEST_DB_NAME, testTblName, partVals);
    eventsProcessor_.processEvents();
    Table catalogTbl = catalog_.getTable(TEST_DB_NAME, testTblName);
    List<TPartitionKeyValue> partKeyVals = new ArrayList<>();
    partKeyVals.add(new TPartitionKeyValue("p1", "1"));
    long lastSyncedEventIdBefore = catalogTbl.getLastSyncedEventId();
    alterTableSetPartitionPropertiesFromImpala(testTblName, partKeyVals);
    HdfsPartition hdfsPartition =
        catalog_.getHdfsPartition(TEST_DB_NAME, testTblName, partKeyVals);
    assertNotNull(hdfsPartition.getParameters());
    assertEquals("dummyValue1", hdfsPartition.getParameters().get("dummyKey1"));

    eventsProcessor_.processEvents();
    if (BackendConfig.INSTANCE.enableSyncToLatestEventOnDdls()) {
      // event processor when processing a self event should update last synced
      // event id to this event id
      assertTrue(catalog_.getTable(TEST_DB_NAME, testTblName).getLastSyncedEventId()
          > lastSyncedEventIdBefore);
    }

    confirmTableIsLoaded(TEST_DB_NAME, testTblName);
    // we check for the object hash of the HDFSPartition to make sure that it was not
    // refresh
    assertEquals("Partition should not have been refreshed after receiving "
            + "self-event", hdfsPartition,
        catalog_.getHdfsPartition(TEST_DB_NAME, testTblName, partKeyVals));

    // compute stats on the table and make sure that the table and its partitions are
    // not refreshed due to the events
    alterTableComputeStats(testTblName, Arrays.asList(Arrays.asList("1"),
        Arrays.asList("2")));
    // currently there is no good way to find out if a partition was refreshed or not.
    // When a partition is refreshed, we replace the HDFSPartition objects in the
    // HDFSTable with the new ones which are reloaded from updated information in HMS.
    // In order to detect whether the partitions were refreshed, we
    // compare the HDFSPartition object before and after the events are
    // processed to make sure that they are the same instance of HDFSPartition
    HdfsPartition part1Before = catalog_.getHdfsPartition(TEST_DB_NAME, testTblName,
        partKeyVals);
    List<TPartitionKeyValue> partKeyVals2 = new ArrayList<>();
    partKeyVals2.add(new TPartitionKeyValue("p1", "2"));
    HdfsPartition part2Before = catalog_.getHdfsPartition(TEST_DB_NAME, testTblName,
        partKeyVals2);
    // we updated the stats on 2 partitions, we should see atleast 2 alter partition
    // events
    assertTrue(eventsProcessor_.getNextMetastoreEvents().size() >= 2);
    eventsProcessor_.processEvents();
    confirmTableIsLoaded(TEST_DB_NAME, testTblName);
    // make sure that the partitions are the same instance
    assertEquals("Partition should not have been refreshed after receiving the "
            + "self-event", part1Before,
        catalog_.getHdfsPartition(TEST_DB_NAME, testTblName, partKeyVals));
    assertEquals("Partition should not have been refreshed after receiving the "
            + "self-event", part2Before,
        catalog_.getHdfsPartition(TEST_DB_NAME, testTblName, partKeyVals2));
  }

  /**
   * Test verifies if lastRefreshEventId is updated or not after a table is refreshed
   * This is useful for skipping older events in the event processor
   * @throws Exception
   */
  @Test
  public void testSkippingOlderEvents() throws Exception {
    boolean prevFlagVal = BackendConfig.INSTANCE.enableSyncToLatestEventOnDdls();
    boolean invalidateHMSFlag = BackendConfig.INSTANCE.invalidateCatalogdHMSCacheOnDDLs();
    try {
      BackendConfig.INSTANCE.setEnableSyncToLatestEventOnDdls(true);
      BackendConfig.INSTANCE.setInvalidateCatalogdHMSCacheOnDDLs(false);
      BackendConfig.INSTANCE.setSkippingOlderEvents(true);
      createDatabase(TEST_DB_NAME, null);
      final String testTblName = "testSkippingOlderEvents";
      createTable(testTblName, true);
      eventsProcessor_.processEvents();
      AlterTableExecutor hiveExecutor = new HiveAlterTableExecutor(TEST_DB_NAME,
          testTblName);
      hiveExecutor.execute();
      HdfsTable testTbl = (HdfsTable) catalog_.getOrLoadTable(TEST_DB_NAME, testTblName,
          "test", null);
      long lastSyncEventIdBefore = testTbl.getLastRefreshEventId();
      alterTableAddParameter(testTblName, "somekey", "someval");
      eventsProcessor_.processEvents();
      assertEquals(testTbl.getLastRefreshEventId(), eventsProcessor_.getCurrentEventId());
      assertTrue(testTbl.getLastRefreshEventId() > lastSyncEventIdBefore);
      confirmTableIsLoaded(TEST_DB_NAME, testTblName);
      final String testUnpartTblName = "testUnPartSkippingOlderEvents";
      createTable(testUnpartTblName, false);
      testInsertEvents(TEST_DB_NAME, testUnpartTblName, false);
      testTbl = (HdfsTable) catalog_.getOrLoadTable(TEST_DB_NAME, testUnpartTblName,
          "test", null);
      eventsProcessor_.processEvents();
      assertEquals(testTbl.getLastRefreshEventId(), eventsProcessor_.getCurrentEventId());
      confirmTableIsLoaded(TEST_DB_NAME, testUnpartTblName);
      // Verify older HMS events are skipped by doing refresh in Impala
      alterTableAddCol(testUnpartTblName, "newCol", "string", "test new column");
      testTbl = (HdfsTable) catalog_.getOrLoadTable(TEST_DB_NAME, testUnpartTblName,
          "test", null);
      lastSyncEventIdBefore = testTbl.getLastRefreshEventId();
      catalog_.reloadTable(testTbl, "test", NoOpEventSequence.INSTANCE);
      eventsProcessor_.processEvents();
      assertEquals(testTbl.getLastRefreshEventId(), eventsProcessor_.getCurrentEventId());
      assertTrue(testTbl.getLastRefreshEventId() > lastSyncEventIdBefore);
      confirmTableIsLoaded(TEST_DB_NAME, testTblName);
    } finally {
      BackendConfig.INSTANCE.setEnableSyncToLatestEventOnDdls(prevFlagVal);
      BackendConfig.INSTANCE.setInvalidateCatalogdHMSCacheOnDDLs(invalidateHMSFlag);
    }
  }

  /**
   * Test whether open transaction event is skipped while fetching notification events
   * @throws Exception
   */
  @Test
  public void testSkipFetchOpenTransactionEvent() throws Exception {
    long currentEventId = eventsProcessor_.getCurrentEventId();
    try (MetaStoreClient client = catalog_.getMetaStoreClient()) {
      // 1. Fetch notification events after open and commit transaction
      long txnId = MetastoreShim.openTransaction(client.getHiveClient());
      assertEquals(currentEventId + 1, eventsProcessor_.getCurrentEventId());
      // Verify latest event id
      eventsProcessor_.updateLatestEventId();
      assertEquals(currentEventId + 1, eventsProcessor_.getLatestEventId());

      MetastoreShim.commitTransaction(client.getHiveClient(), txnId);
      assertEquals(currentEventId + 2, eventsProcessor_.getCurrentEventId());
      // Verify the latest event id again
      eventsProcessor_.updateLatestEventId();
      assertEquals(currentEventId + 2, eventsProcessor_.getLatestEventId());
      // Commit transaction event alone is returned from metastore
      List<NotificationEvent> events = eventsProcessor_.getNextMetastoreEvents();
      assertEquals(1, events.size());
      assertEquals(MetastoreEventType.COMMIT_TXN,
          MetastoreEventType.from(events.get(0).getEventType()));
      // Verify last synced event id before and after processEvents
      assertEquals(currentEventId, eventsProcessor_.getLastSyncedEventId());
      eventsProcessor_.processEvents();
      assertEquals(currentEventId + 2, eventsProcessor_.getLastSyncedEventId());

      // 2. Fetch notification events right after open transaction
      currentEventId = eventsProcessor_.getCurrentEventId();
      txnId = MetastoreShim.openTransaction(client.getHiveClient());
      assertEquals(currentEventId + 1, eventsProcessor_.getCurrentEventId());
      // Open transaction event is not returned from metastore
      events = eventsProcessor_.getNextMetastoreEvents();
      assertEquals(0, events.size());
      // Verify last synced event id before and after processEvents
      assertEquals(currentEventId, eventsProcessor_.getLastSyncedEventId());
      eventsProcessor_.processEvents();
      assertEquals(currentEventId + 1, eventsProcessor_.getLastSyncedEventId());

      MetastoreShim.commitTransaction(client.getHiveClient(), txnId);
      assertEquals(currentEventId + 2, eventsProcessor_.getCurrentEventId());
    }
  }

  /**
   * Test fetching events in batch when last occurred event is open transaction
   */
  @Test
  public void testFetchEventsInBatchWithOpenTxnAsLastEvent() throws Exception {
    long currentEventId = eventsProcessor_.getCurrentEventId();
    try (MetaStoreClient client = catalog_.getMetaStoreClient()) {
      long txnId = MetastoreShim.openTransaction(client.getHiveClient());
      assertEquals(currentEventId + 1, eventsProcessor_.getCurrentEventId());
      List<NotificationEvent> events =
          MetastoreEventsProcessor.getNextMetastoreEventsInBatches(
              eventsProcessor_.catalog_, currentEventId, null, null);
      // Open transaction event is not returned from metastore
      assertEquals(0, events.size());
      MetastoreShim.commitTransaction(client.getHiveClient(), txnId);
      assertEquals(currentEventId + 2, eventsProcessor_.getCurrentEventId());
    }
  }

  @Test
  public void testNotFetchingUnwantedEvents() throws Exception {
    String tblName = "test_event_skip_list";
    createDatabase(TEST_DB_NAME, null);
    Map<String, String> params = new HashMap<>();
    params.put("EXTERNAL", "true");
    params.put("external.table.purge", "true");
    createTable(null, TEST_DB_NAME, tblName, params, true, "EXTERNAL_TABLE");
    eventsProcessor_.processEvents();

    Table tbl = catalog_.getTable(TEST_DB_NAME, tblName);
    assertTrue("tbl should be unloaded", tbl instanceof IncompleteTable);
    long createEventId = tbl.getCreateEventId();
    assertEquals(eventsProcessor_.getLatestEventId(), createEventId);

    // create 2 partitions and update partition stats using Hive
    try (HiveJdbcClientPool jdbcClientPool = HiveJdbcClientPool.create(1);
         HiveJdbcClientPool.HiveJdbcClient hiveClient = jdbcClientPool.getClient()) {
      hiveClient.executeSql("set hive.exec.dynamic.partition.mode=nonstrict");
      hiveClient.executeSql(String.format(
          "insert into %s.%s partition(p1) values (0,0,0),(1,1,1)",
          TEST_DB_NAME, tblName));
      hiveClient.executeSql(String.format(
          "analyze table %s.%s compute statistics for columns", TEST_DB_NAME, tblName));
    }
    // All the new events are:
    // OPEN_TXN
    // ADD_PARTITION one for adding 2 partitions
    // COMMIT_TXN
    // OPEN_TXN
    // ALTER_PARTITION
    // ALTER_PARTITION
    // UPDATE_PART_COL_STAT_EVENT
    // UPDATE_PART_COL_STAT_EVENT
    // COMMIT_TXN
    // Fetch all events with the default skip list.
    List<NotificationEvent> events =
        MetastoreEventsProcessor.getNextMetastoreEventsInBatches(
            eventsProcessor_.catalog_, createEventId, null);
    assertEquals(5, events.size());

    // Fetch events with a null filter but specifying the wanted event type.
    events = MetastoreEventsProcessor.getNextMetastoreEventsInBatches(
        eventsProcessor_.catalog_, createEventId, /*filter*/null,
        MetastoreEvents.CreateTableEvent.EVENT_TYPE);
    assertEquals(0, events.size());
    events = MetastoreEventsProcessor.getNextMetastoreEventsInBatches(
        eventsProcessor_.catalog_, createEventId, /*filter*/null,
        MetastoreEvents.AddPartitionEvent.EVENT_TYPE);
    assertEquals(1, events.size());
    events = MetastoreEventsProcessor.getNextMetastoreEventsInBatches(
        eventsProcessor_.catalog_, createEventId, /*filter*/null,
        AlterPartitionEvent.EVENT_TYPE);
    assertEquals(2, events.size());
  }

  /**
   * Test getCurrentEventId() throws MetastoreNotificationFetchException when there are
   * connection issues with HMS.
   */
  @Test(expected = MetastoreNotificationFetchException.class)
  public void testHMSClientFailureInGettingCurrentEventId() throws Exception {
    MetaStoreClientPool origPool = catalog_.getMetaStoreClientPool();
    try {
      MetaStoreClientPool badPool = new IncompetentMetastoreClientPool(0, 0);
      catalog_.setMetaStoreClientPool(badPool);
      eventsProcessor_.getCurrentEventId();
    } finally {
      catalog_.setMetaStoreClientPool(origPool);
    }
  }

  /**
   * Test getNextMetastoreEvents() throws MetastoreNotificationFetchException when there
   * are connection issues with HMS.
   */
  @Test(expected = MetastoreNotificationFetchException.class)
  public void testHMSClientFailureInFetchingEvents() throws Exception {
    MetaStoreClientPool origPool = catalog_.getMetaStoreClientPool();
    try {
      MetaStoreClientPool badPool = new IncompetentMetastoreClientPool(0, 0);
      catalog_.setMetaStoreClientPool(badPool);
      // Use a fake currentEventId that is larger than lastSyncedEventId
      // so getNextMetastoreEvents() will fetch new events.
      long currentEventId = eventsProcessor_.getLastSyncedEventId() + 1;
      eventsProcessor_.getNextMetastoreEvents(currentEventId);
    } finally {
      catalog_.setMetaStoreClientPool(origPool);
    }
  }

  /**
   * Test getNextMetastoreEventsInBatches() throws MetastoreNotificationFetchException
   * when there are connection issues with HMS.
   */
  @Test(expected = MetastoreNotificationFetchException.class)
  public void testHMSClientFailureInFetchingEventsInBatches() throws Exception {
    MetaStoreClientPool origPool = catalog_.getMetaStoreClientPool();
    try {
      MetaStoreClientPool badPool = new IncompetentMetastoreClientPool(0, 0);
      catalog_.setMetaStoreClientPool(badPool);
      MetastoreEventsProcessor.getNextMetastoreEventsInBatches(catalog_, 0, null, null);
    } finally {
      catalog_.setMetaStoreClientPool(origPool);
    }
  }

  /**
   * Test getEventTimeFromHMS() returns 0 when there are connection issues with HMS.
   */
  @Test
  public void testHMSClientFailureInGettingEventTime() {
    MetaStoreClientPool origPool = catalog_.getMetaStoreClientPool();
    try {
      MetaStoreClientPool badPool = new IncompetentMetastoreClientPool(0, 0);
      catalog_.setMetaStoreClientPool(badPool);
      assertEquals(0, eventsProcessor_.getEventTimeFromHMS(0));
    } finally {
      catalog_.setMetaStoreClientPool(origPool);
    }
  }

  @Test
  public void testAllocWriteIdEventForPartTableWithoutLoad() throws Exception {
    String tblName = "test_alloc_writeid_part_table";
    testAllocWriteIdEvent(tblName, true, false);
  }

  @Test
  public void testAllocWriteIdEventForNonPartTableWithoutLoad() throws Exception {
    String tblName = "test_alloc_writeid_table";
    testAllocWriteIdEvent(tblName, false, false);
  }

  @Test
  public void testAllocWriteIdEventForPartTable() throws Exception {
    String tblName = "test_alloc_writeid_part_table_load";
    testAllocWriteIdEvent(tblName, true, true);
  }

  @Test
  public void testAllocWriteIdEventForNonPartTable() throws Exception {
    String tblName = "test_alloc_writeid_table_load";
    testAllocWriteIdEvent(tblName, false, true);
  }

  @Test
  public void testReloadEventOnLoadedTable() throws Exception {
    String tblName = "test_reload";
    createDatabase(TEST_DB_NAME, null);
    eventsProcessor_.processEvents();
    createTable(tblName, false);
    // Fire a reload event
    MetastoreShim.fireReloadEventHelper(catalog_.getMetaStoreClient(), true, null,
        TEST_DB_NAME, tblName, Collections.emptyMap());
    // Fetch all the events
    List<NotificationEvent> events = eventsProcessor_.getNextMetastoreEvents();
    List<MetastoreEvent> filteredEvents =
        eventsProcessor_.getEventsFactory().getFilteredEvents(
            events, eventsProcessor_.getMetrics());
    assertTrue(filteredEvents.size() == 2);
    // Process create table event and load table
    MetastoreEvent event = filteredEvents.get(0);
    assertEquals(MetastoreEventType.CREATE_TABLE, event.getEventType());
    event.processIfEnabled();
    loadTable(tblName);
    // Process reload event twice and check if table is refreshed on first reload event
    // and skipped for second event.
    event = filteredEvents.get(1);
    assertEquals(MetastoreEventType.RELOAD, event.getEventType());
    // First reload should refresh table
    long refreshCount =
        eventsProcessor_.getMetrics()
            .getCounter(MetastoreEventsProcessor.NUMBER_OF_TABLE_REFRESHES)
            .getCount();
    event.processIfEnabled();
    assertEquals(refreshCount + 1,
        eventsProcessor_.getMetrics()
            .getCounter(MetastoreEventsProcessor.NUMBER_OF_TABLE_REFRESHES)
            .getCount());
    // Second reload should be skipped
    long skipCount = eventsProcessor_.getMetrics()
                         .getCounter(MetastoreEventsProcessor.EVENTS_SKIPPED_METRIC)
                         .getCount();
    event.processIfEnabled();
    assertEquals(skipCount + 1,
        eventsProcessor_.getMetrics()
            .getCounter(MetastoreEventsProcessor.EVENTS_SKIPPED_METRIC)
            .getCount());
  }

  @Test
  public void testCommitCompactionEventOnLoadedTable() throws Exception {
    String tblName = "test_commit_compaction";
    createDatabase(TEST_DB_NAME, null);
    eventsProcessor_.processEvents();
    createTransactionalTable(TEST_DB_NAME, tblName, false);
    insertIntoTable(TEST_DB_NAME, tblName);
    insertIntoTable(TEST_DB_NAME, tblName);
    alterTableAddParameter(
        tblName, MetastoreEventPropertyKey.DISABLE_EVENT_HMS_SYNC.getKey(), "true");

    // Run hive query to trigger compaction
    try (HiveJdbcClientPool jdbcClientPool = HiveJdbcClientPool.create(1);
         HiveJdbcClientPool.HiveJdbcClient hiveClient = jdbcClientPool.getClient()) {
      hiveClient.executeSql(
          "alter table " + TEST_DB_NAME + '.' + tblName + " compact 'minor' and wait");
    }
    // Fetch all the events
    List<NotificationEvent> events = eventsProcessor_.getNextMetastoreEvents();
    List<MetastoreEvent> filteredEvents =
        eventsProcessor_.getEventsFactory().getFilteredEvents(
            events, eventsProcessor_.getMetrics());
    assertTrue(filteredEvents.size() > 1);
    // Process create table event and load table
    MetastoreEvent event = filteredEvents.get(0);
    assertEquals(MetastoreEventType.CREATE_TABLE, event.getEventType());
    event.processIfEnabled();
    loadTable(tblName);
    // Process commit compaction event should skip the event because
    // DISABLE_EVENT_HMS_SYNC is set to true
    event = filteredEvents.get(filteredEvents.size() - 1);
    assertEquals(MetastoreEventType.COMMIT_COMPACTION, event.getEventType());
    long skipCount = eventsProcessor_.getMetrics()
                         .getCounter(MetastoreEventsProcessor.EVENTS_SKIPPED_METRIC)
                         .getCount();
    event.processIfEnabled();
    assertEquals(skipCount + 1,
        eventsProcessor_.getMetrics()
            .getCounter(MetastoreEventsProcessor.EVENTS_SKIPPED_METRIC)
            .getCount());
  }

  @Test
  public void testEventProcessorErrorState() throws Exception {
    boolean prevFlagVal =
        BackendConfig.INSTANCE.isInvalidateGlobalMetadataOnEventProcessFailureEnabled();
    try {
      BackendConfig.INSTANCE.setInvalidateGlobalMetadataOnEventProcessFailure(true);
      createDatabase(TEST_DB_NAME, null);
      eventsProcessor_.updateStatus(EventProcessorStatus.NEEDS_INVALIDATE);
      eventsProcessor_.processEvents();
      // wait and check EP status
      assertEquals(EventProcessorStatus.ACTIVE, eventsProcessor_.getStatus());
      dropDatabaseCascadeFromHMS();
      eventsProcessor_.updateStatus(EventProcessorStatus.ERROR);
      eventsProcessor_.processEvents();
      // wait and check EP status
      assertEquals(EventProcessorStatus.ACTIVE, eventsProcessor_.getStatus());
    } finally {
      BackendConfig.INSTANCE.setInvalidateGlobalMetadataOnEventProcessFailure(
          prevFlagVal);
    }
  }

  @Test
  public void testCreateTblOnUnloadedDB() throws Exception {
    eventsProcessor_.pause();
    createDatabase(TEST_DB_NAME, null);
    eventsProcessor_.processEvents();
    assertEquals(EventProcessorStatus.PAUSED, eventsProcessor_.getStatus());
    assertNull(
        "Test database should not be in catalog when event processing is stopped",
        catalog_.getDb(TEST_DB_NAME));
    long currentEventId = eventsProcessor_.getCurrentEventId();
    eventsProcessor_.start(currentEventId);
    assertEquals(EventProcessorStatus.ACTIVE, eventsProcessor_.getStatus());
    String tblName = "test_create_tbl";
    createTable(tblName, false);
    eventsProcessor_.processEvents();
    assertEquals(EventProcessorStatus.ACTIVE, eventsProcessor_.getStatus());
  }

  private void insertIntoTable(String dbName, String tableName) throws Exception {
    try (MetaStoreClient client = catalog_.getMetaStoreClient()) {
      org.apache.hadoop.hive.metastore.api.Table msTable =
          client.getHiveClient().getTable(dbName, tableName);
      long txnId = MetastoreShim.openTransaction(client.getHiveClient());
      long writeId = MetastoreShim.allocateTableWriteId(
          client.getHiveClient(), txnId, dbName, tableName);
      simulateInsertIntoTransactionalTableFromFS(msTable, null, 1, txnId, writeId);
      MetastoreShim.commitTransaction(client.getHiveClient(), txnId);
    }
  }

  public void testAllocWriteIdEvent(String tblName, boolean isPartitioned,
      boolean isLoadTable) throws TException, TransactionException, CatalogException {
    createDatabase(TEST_DB_NAME, null);
    eventsProcessor_.processEvents();
    createTransactionalTable(TEST_DB_NAME, tblName, isPartitioned);
    if (isLoadTable) {
      eventsProcessor_.processEvents();
      // Load table
      loadTable(tblName);
    }
    long txnId;
    long writeId;
    try (MetaStoreClient client = catalog_.getMetaStoreClient()) {
      txnId = MetastoreShim.openTransaction(client.getHiveClient());
      writeId = MetastoreShim.allocateTableWriteId(
          client.getHiveClient(), txnId, TEST_DB_NAME, tblName);
      eventsProcessor_.processEvents();
      // Abort transaction and do not process event
      MetastoreShim.abortTransaction(client.getHiveClient(), txnId);
    }
    Table table = catalog_.getTableNoThrow(TEST_DB_NAME, tblName);
    assertNotNull("Table is not present in catalog", table);
    // Check whether txnId to write id mapping is present
    Set<TableWriteId> writeIds = catalog_.getWriteIds(txnId);
    assertEquals(1, writeIds.size());
    assertTrue(writeIds.contains(
        new TableWriteId(TEST_DB_NAME, tblName, table.getCreateEventId(), writeId)));
    if (isLoadTable) {
      assertTrue(table instanceof HdfsTable);
      // For loaded partitioned table, write id is added on alloc write id event process
      // Whereas, for non-partitioned table, write id is not added on alloc write id
      // event process
      if (isPartitioned) {
        assertEquals(writeId, table.getValidWriteIds().getHighWatermark());
      } else {
        assertNotEquals(writeId, table.getValidWriteIds().getHighWatermark());
      }
    } else {
      assertTrue(table instanceof IncompleteTable);
      assertNull(table.getValidWriteIds());
    }
  }

  @Test
  public void testNotificationEventRequest() throws Exception {
    long currentEventId = eventsProcessor_.getCurrentEventId();
    int EVENTS_BATCH_SIZE_PER_RPC = 1000;
    // Generate some DB only related events
    createDatabaseFromImpala(TEST_DB_NAME, null);
    String testDbParamKey = "testKey", testDbParamVal = "testVal";
    String testTable1 = "testNotifyTable1", testTable2 = "testNotifyTable2";
    addDatabaseParameters(testDbParamKey, testDbParamVal);
    eventsProcessor_.processEvents();
    // Verify DB related events only.

    // Generate some table events for managed table
    testInsertIntoTransactionalTable(testTable1, true, false);
    alterTableAddColsFromImpala(
        TEST_DB_NAME, testTable1, "newCol", TPrimitiveType.STRING);
    alterTableRemoveColFromImpala(TEST_DB_NAME, testTable1, "newCol");
    eventsProcessor_.processEvents();

    // Generate some table events for external table
    createTable(testTable2, false);
    eventsProcessor_.processEvents();
    alterTableSetOwnerFromImpala(TEST_DB_NAME, testTable2, "testowner");
    alterTableSetFileFormatFromImpala(
        TEST_DB_NAME, testTable2, THdfsFileFormat.TEXT);
    eventsProcessor_.processEvents();

    // verify DB only events
    NotificationFilter filter = event ->
        MetastoreEvents.CreateDatabaseEvent.EVENT_TYPE
        .equals(event.getEventType());
    MetaDataFilter metaDataFilter = new MetaDataFilter(filter,
        MetastoreShim.getDefaultCatalogName(), TEST_DB_NAME);
    assertEquals(MetastoreEventsProcessor
        .getNextMetastoreEventsWithFilterInBatches(catalog_, currentEventId,
            metaDataFilter, EVENTS_BATCH_SIZE_PER_RPC).size(), 1);
    Db db = catalog_.getDb(TEST_DB_NAME);
    metaDataFilter.setNotificationFilter(
        MetastoreEventsProcessor.getDbNotificationEventFilter(db));
    assertEquals(MetastoreEventsProcessor.getNextMetastoreEventsWithFilterInBatches(
        catalog_, currentEventId, metaDataFilter, EVENTS_BATCH_SIZE_PER_RPC).size(), 2);

    // verify events for table 1
    filter = event -> "ALTER_TABLE".equals(event.getEventType());
    metaDataFilter = new MetaDataFilter(filter, MetastoreShim.getDefaultCatalogName(),
        TEST_DB_NAME, testTable1);
    assertEquals(MetastoreEventsProcessor.getNextMetastoreEventsWithFilterInBatches(
        catalog_, currentEventId, metaDataFilter, EVENTS_BATCH_SIZE_PER_RPC).size(), 2);
    metaDataFilter.setNotificationFilter(null);
    assertEquals(MetastoreEventsProcessor.getNextMetastoreEventsWithFilterInBatches(
        catalog_, currentEventId, metaDataFilter, EVENTS_BATCH_SIZE_PER_RPC).size(), 3);

    // verify events for table 2
    filter = event -> "ALTER_TABLE".equals(event.getEventType());
    metaDataFilter = new MetaDataFilter(filter, MetastoreShim.getDefaultCatalogName(),
        TEST_DB_NAME, testTable2);
    assertEquals(MetastoreEventsProcessor.getNextMetastoreEventsWithFilterInBatches(
        catalog_, currentEventId, metaDataFilter, EVENTS_BATCH_SIZE_PER_RPC).size(), 2);
    metaDataFilter.setNotificationFilter(null);
    assertEquals(MetastoreEventsProcessor.getNextMetastoreEventsWithFilterInBatches(
        catalog_, currentEventId, metaDataFilter, EVENTS_BATCH_SIZE_PER_RPC).size(), 3);

    // verify all events
    filter = event -> "ALTER_TABLE".equals(event.getEventType());
    metaDataFilter = new MetaDataFilter(filter, MetastoreShim.getDefaultCatalogName(),
        TEST_DB_NAME);
    assertEquals(MetastoreEventsProcessor.getNextMetastoreEventsWithFilterInBatches(
        catalog_, currentEventId, metaDataFilter, EVENTS_BATCH_SIZE_PER_RPC).size(), 4);
    metaDataFilter.setNotificationFilter(null);
    // 2 DB + 6 table events
    assertEquals(MetastoreEventsProcessor.getNextMetastoreEventsWithFilterInBatches(
        catalog_, currentEventId, metaDataFilter, EVENTS_BATCH_SIZE_PER_RPC).size(), 8);
  }

  @Test
  public void testAlterTableWithEpDisabled() throws Exception {
    try {
      createDatabaseFromImpala(TEST_DB_NAME, null);
      String testTable = "testAlterTableNoError";
      createTableFromImpala(TEST_DB_NAME, testTable, true);
      eventsProcessor_.processEvents();
      // set EP to paused state and execute Alter table add partition query
      eventsProcessor_.pause();
      long numberOfSelfEventsBefore =
          eventsProcessor_.getMetrics()
              .getCounter(MetastoreEventsProcessor.EVENTS_SKIPPED_METRIC).getCount();
      TPartitionDef partitionDef = new TPartitionDef();
      partitionDef.addToPartition_spec(new TPartitionKeyValue("p1", "100"));
      partitionDef.addToPartition_spec(new TPartitionKeyValue("p2", "200"));
      alterTableAddPartition(TEST_DB_NAME, testTable, partitionDef,
          "enable_event_processor");
      eventsProcessor_.processEvents();
      assertEquals(EventProcessorStatus.ACTIVE, eventsProcessor_.getStatus());
      long numberOfSelfEventsAfter =
          eventsProcessor_.getMetrics()
              .getCounter(MetastoreEventsProcessor.EVENTS_SKIPPED_METRIC).getCount();
      // expect ADD_PARTITION event to be skipped as self-event
      assertEquals("Unexpected self events skipped: ", numberOfSelfEventsAfter,
          numberOfSelfEventsBefore + 1);
    } catch (NullPointerException ex) {
      throw new CatalogException("Exception occured while applying AlterTableEvent", ex);
    } finally {
      if (eventsProcessor_.getStatus() != EventProcessorStatus.ACTIVE) {
        eventsProcessor_.start();
      }
    }
  }

  private void createDatabase(String catName, String dbName,
      Map<String, String> params) throws TException {
    try(MetaStoreClient msClient = catalog_.getMetaStoreClient()) {
      MetastoreApiTestUtils.createDatabase(msClient, catName, dbName, params);
    }
  }

  private void createDatabase(String dbName, Map<String, String> params)
      throws TException {
    createDatabase(null, dbName, params);
  }

  private void createHiveCatalog(String catName) throws TException {
    Catalog catalog = new Catalog();
    catalog.setName(catName);
    catalog.setLocationUri(Files.createTempDir().getAbsolutePath());
    try (MetaStoreClient metaStoreClient = catalog_.getMetaStoreClient()) {
      metaStoreClient.getHiveClient().createCatalog(catalog);
    }
  }

  private void dropHiveCatalogIfExists(String catName) throws TException {
    try (MetaStoreClient msClient = catalog_.getMetaStoreClient()) {
      // unfortunately the dropCatalog API doesn't seem to take ifExists or cascade flag
      if (msClient.getHiveClient().getCatalogs().contains(catName)) {
        for (String db : msClient.getHiveClient().getAllDatabases(catName)) {
          msClient.getHiveClient().dropDatabase(catName, db, true, true, true);
        }
        msClient.getHiveClient().dropCatalog(catName);
      }
    }
  }

  private void addDatabaseParameters(String key, String val) throws TException {
    try (MetaStoreClient msClient = catalog_.getMetaStoreClient()) {
      MetastoreApiTestUtils.addDatabaseParametersInHms(msClient, TEST_DB_NAME, key, val);
    }
  }


  private void alterDatabase(Database newDatabase) throws TException {
    try (MetaStoreClient msClient = catalog_.getMetaStoreClient()) {
      msClient.getHiveClient().alterDatabase(newDatabase.getName(), newDatabase);
    }
  }


  private void createTransactionalTable(String dbName, String tblName,
      boolean isPartitioned) throws TException {
    Map<String, String> params = new HashMap<>();
    params.put("transactional", "true");
    params.put("transactional_properties", "insert_only");
    createTable(null, dbName, tblName, params, isPartitioned, "MANAGED_TABLE");
  }

  private void createTable(String catName, String dbName,
      String tblName, Map<String, String> params, boolean isPartitioned,
      String tableType) throws TException {
    try(MetaStoreClient msClient = catalog_.getMetaStoreClient()) {
      MetastoreApiTestUtils.createTable(msClient, catName, dbName, tblName, params,
          isPartitioned, tableType);
    }
  }

  /**
   * Drops table from Impala
   */
  private void dropTableFromImpala(String dbName, String tblName) throws ImpalaException {
    TDdlExecRequest req = new TDdlExecRequest();
    req.setQuery_options(new TDdlQueryOptions());
    req.setDdl_type(TDdlType.DROP_TABLE);
    TDropTableOrViewParams dropTableParams = new TDropTableOrViewParams();
    dropTableParams.setTable_name(new TTableName(dbName, tblName));
    dropTableParams.setIf_exists(true);
    req.setDrop_table_or_view_params(dropTableParams);
    catalogOpExecutor_.execDdlRequest(req);
  }

  /**
   * Creates db from Impala
   */
  public static void createDatabaseFromImpala(CatalogOpExecutor catalogOpExecutor,
      String dbName, String desc) throws ImpalaException {
    TDdlExecRequest req = new TDdlExecRequest();
    req.setQuery_options(new TDdlQueryOptions());
    req.setDdl_type(TDdlType.CREATE_DATABASE);
    TCreateDbParams createDbParams = new TCreateDbParams();
    createDbParams.setDb(dbName);
    createDbParams.setComment(desc);
    req.setCreate_db_params(createDbParams);
    catalogOpExecutor.execDdlRequest(req);
  }

  private void createDatabaseFromImpala(String dbName, String desc)
      throws ImpalaException {
    createDatabaseFromImpala(catalogOpExecutor_, dbName, desc);
  }

  /**
   * Sets the owner for the given Db from Impala
   */
  private void alterDbSetOwnerFromImpala(
      String dbName, String owner, TOwnerType ownerType) throws ImpalaException {
    TDdlExecRequest req = new TDdlExecRequest();
    req.setQuery_options(new TDdlQueryOptions());
    req.setDdl_type(TDdlType.ALTER_DATABASE);
    TAlterDbParams alterDbParams = new TAlterDbParams();
    alterDbParams.setDb(dbName);
    alterDbParams.setAlter_type(TAlterDbType.SET_OWNER);
    TAlterDbSetOwnerParams alterDbSetOwnerParams = new TAlterDbSetOwnerParams();
    alterDbSetOwnerParams.setOwner_name(owner);
    alterDbSetOwnerParams.setOwner_type(ownerType);
    alterDbParams.setSet_owner_params(alterDbSetOwnerParams);
    req.setAlter_db_params(alterDbParams);
    catalogOpExecutor_.execDdlRequest(req);
  }

  /**
   * Drops db from Impala
   */
  private void dropDatabaseCascadeFromImpala(String dbName) throws ImpalaException {
    TDdlExecRequest req = new TDdlExecRequest();
    req.setQuery_options(new TDdlQueryOptions());
    req.setDdl_type(TDdlType.DROP_DATABASE);
    TDropDbParams dropDbParams = new TDropDbParams();
    dropDbParams.setDb(dbName);
    dropDbParams.setCascade(true);
    req.setDrop_db_params(dropDbParams);
    catalogOpExecutor_.execDdlRequest(req);
  }

  private void createTableLike(String srcDb, String srcTbl, String destDb, String destTbl)
      throws Exception {
    HdfsTable table = (HdfsTable) catalog_.getOrLoadTable(srcDb, srcTbl, "Test", null);
    TCreateTableLikeParams createTableLikeParams = new TCreateTableLikeParams();
    createTableLikeParams.setSrc_table_name(new TTableName(srcDb, srcTbl));
    createTableLikeParams.setTable_name(new TTableName(destDb, destTbl));
    createTableLikeParams.setIs_external(false);
    TDdlExecRequest req = new TDdlExecRequest();
    req.setQuery_options(new TDdlQueryOptions());
    req.setDdl_type(TDdlType.CREATE_TABLE_LIKE);
    req.create_table_like_params = createTableLikeParams;
    catalogOpExecutor_.execDdlRequest(req);
  }

  private void createTableFromImpala(String dbName, String tblName, boolean isPartitioned)
      throws ImpalaException {
    createTableFromImpala(dbName, tblName, null, isPartitioned);
  }

  /**
   * Creates a table using CatalogOpExecutor to simulate a DDL operation from Impala
   * client
   */
  public static void createTableFromImpala(CatalogOpExecutor opExecutor, String dbName,
      String tblName, Map<String, String> tblParams, boolean isPartitioned)
      throws ImpalaException {
    TDdlExecRequest req = new TDdlExecRequest();
    req.setQuery_options(new TDdlQueryOptions());
    req.setDdl_type(TDdlType.CREATE_TABLE);
    TCreateTableParams createTableParams = new TCreateTableParams();
    createTableParams.setTable_name(new TTableName(dbName, tblName));
    createTableParams.setFile_format(THdfsFileFormat.PARQUET);
    createTableParams.setIs_external(false);
    createTableParams.setIf_not_exists(false);
    if (tblParams != null) {
      createTableParams.setTable_properties(tblParams);
    }
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
    opExecutor.execDdlRequest(req);
  }

  private void createTableFromImpala(String dbName, String tblName,
      Map<String, String> tblParams, boolean isPartitioned) throws ImpalaException {
    createTableFromImpala(catalogOpExecutor_, dbName, tblName, tblParams, isPartitioned);
  }

  /**
   * Create a scalar function from Impala.
   */
  private void createScalarFunctionFromImpala(ScalarFunction fn) throws
      ImpalaException {
    TDdlExecRequest req = new TDdlExecRequest();
    req.setQuery_options(new TDdlQueryOptions());
    req.setDdl_type(TDdlType.CREATE_FUNCTION);
    TCreateFunctionParams createFunctionParams = new TCreateFunctionParams();
    createFunctionParams.setFn(fn.toThrift());
    req.setCreate_fn_params(createFunctionParams);
    catalogOpExecutor_.execDdlRequest(req);
  }

  /**
   * Drop a scalar function from Impala.
   */
  private void dropScalarFunctionFromImapala(ScalarFunction fn) throws ImpalaException {
    TDdlExecRequest req = new TDdlExecRequest();
    req.setQuery_options(new TDdlQueryOptions());
    req.setDdl_type(TDdlType.DROP_FUNCTION);
    TDropFunctionParams dropFunctionParams = new TDropFunctionParams();
    dropFunctionParams.setFn_name(fn.getFunctionName().toThrift());
    dropFunctionParams.setArg_types(fn.toThrift().getArg_types());
    dropFunctionParams.setSignature(fn.toThrift().getSignature());
    req.setDrop_fn_params(dropFunctionParams);
    catalogOpExecutor_.execDdlRequest(req);
  }
  /**
   * Renames a table from oldTblName to newTblName from Impala
   */
  private void renameTableFromImpala(String oldTblName, String newTblName)
      throws ImpalaException {
    TDdlExecRequest req = new TDdlExecRequest();
    req.setQuery_options(new TDdlQueryOptions());
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

  /**
   * Adds a dummy col to a given table from Impala
   */
  private void alterTableAddColsFromImpala(String dbName, String tblName, String colName,
      TPrimitiveType colType) throws ImpalaException {
    TDdlExecRequest req = new TDdlExecRequest();
    req.setQuery_options(new TDdlQueryOptions());
    req.setDdl_type(TDdlType.ALTER_TABLE);
    TAlterTableParams alterTableParams = new TAlterTableParams();
    alterTableParams.setTable_name(new TTableName(dbName, tblName));
    alterTableParams.setAlter_type(TAlterTableType.ADD_COLUMNS);
    TAlterTableAddColsParams addColsParams = new TAlterTableAddColsParams();
    addColsParams.addToColumns(getScalarColumn(colName, colType));
    alterTableParams.setAdd_cols_params(addColsParams);
    req.setAlter_table_params(alterTableParams);
    catalogOpExecutor_.execDdlRequest(req);
    Table tbl = catalog_.getTable(dbName, tblName);
    assertNotNull(tbl.getColumn(colName));
  }

  /**
   * Adds a dummy col to a given table from Impala
   */
  private void alterTableRemoveColFromImpala(
      String dbName, String tblName, String colName) throws ImpalaException {
    TDdlExecRequest req = new TDdlExecRequest();
    req.setQuery_options(new TDdlQueryOptions());
    req.setDdl_type(TDdlType.ALTER_TABLE);
    TAlterTableParams alterTableParams = new TAlterTableParams();
    alterTableParams.setTable_name(new TTableName(dbName, tblName));
    alterTableParams.setAlter_type(TAlterTableType.DROP_COLUMN);
    TAlterTableDropColParams dropColParams = new TAlterTableDropColParams();
    dropColParams.setCol_name(colName);
    alterTableParams.setDrop_col_params(dropColParams);
    req.setAlter_table_params(alterTableParams);
    catalogOpExecutor_.execDdlRequest(req);
    Table tbl = catalog_.getTable(dbName, tblName);
    assertNull(tbl.getColumn(colName));
  }

  private void alterTableReplaceColFromImpala(
      String dbName, String tblName, List<TColumn> newCols) throws ImpalaException {
    TDdlExecRequest req = new TDdlExecRequest();
    req.setQuery_options(new TDdlQueryOptions());
    req.setDdl_type(TDdlType.ALTER_TABLE);
    TAlterTableParams alterTableParams = new TAlterTableParams();
    alterTableParams.setTable_name(new TTableName(dbName, tblName));
    alterTableParams.setAlter_type(TAlterTableType.REPLACE_COLUMNS);
    TAlterTableReplaceColsParams replaceColsParams = new TAlterTableReplaceColsParams();
    replaceColsParams.setColumns(newCols);
    alterTableParams.setReplace_cols_params(replaceColsParams);
    req.setAlter_table_params(alterTableParams);
    catalogOpExecutor_.execDdlRequest(req);
    Table tbl = catalog_.getTable(dbName, tblName);
    assertNotNull(tbl.getColumn(newCols.get(0).getColumnName()));
  }

  /**
   * Adds dummy partition from Impala. Assumes that given table is a partitioned table
   * and the has a partition key of type string and value of type string
   */
  private void alterTableAddPartition(
      String dbName, String tblName, TPartitionDef partitionDef) throws ImpalaException {
    alterTableAddPartition(dbName, tblName,partitionDef, null);
  }

  private void alterTableAddPartition(String dbName, String tblName,
      TPartitionDef partitionDef, String debugActions) throws ImpalaException {
    TDdlExecRequest req = new TDdlExecRequest();
    TDdlQueryOptions queryOptions = new TDdlQueryOptions();
    if (debugActions != null) {
      queryOptions.setDebug_action(debugActions);
    }
    req.setQuery_options(queryOptions);
    req.setDdl_type(TDdlType.ALTER_TABLE);
    TAlterTableParams alterTableParams = new TAlterTableParams();
    alterTableParams.setTable_name(new TTableName(dbName, tblName));
    alterTableParams.setAlter_type(TAlterTableType.ADD_PARTITION);
    TAlterTableAddPartitionParams addPartitionParams =
        new TAlterTableAddPartitionParams();
    addPartitionParams.addToPartitions(partitionDef);
    alterTableParams.setAdd_partition_params(addPartitionParams);
    req.setAlter_table_params(alterTableParams);
    catalogOpExecutor_.execDdlRequest(req);
  }

  /**
   * Drops the partition for the given table
   */
  private void alterTableDropPartition(String dbName, String tblName,
      List<TPartitionKeyValue> keyValue) throws ImpalaException {
    TDdlExecRequest req = new TDdlExecRequest();
    req.setQuery_options(new TDdlQueryOptions());
    req.setDdl_type(TDdlType.ALTER_TABLE);
    TAlterTableParams alterTableParams = new TAlterTableParams();
    alterTableParams.setTable_name(new TTableName(dbName, tblName));
    alterTableParams.setAlter_type(TAlterTableType.DROP_PARTITION);
    TAlterTableDropPartitionParams dropPartitionParams =
        new TAlterTableDropPartitionParams();
    dropPartitionParams.addToPartition_set(keyValue);
    alterTableParams.setDrop_partition_params(dropPartitionParams);
    req.setAlter_table_params(alterTableParams);
    catalogOpExecutor_.execDdlRequest(req);
  }

  /**
   * Sets the given file foramt for a table
   */
  private void alterTableSetFileFormatFromImpala(
      String dbName, String tblName, THdfsFileFormat fileformat) throws ImpalaException {
    TDdlExecRequest req = new TDdlExecRequest();
    req.setQuery_options(new TDdlQueryOptions());
    req.setDdl_type(TDdlType.ALTER_TABLE);
    TAlterTableParams alterTableParams = new TAlterTableParams();
    alterTableParams.setTable_name(new TTableName(dbName, tblName));
    alterTableParams.setAlter_type(TAlterTableType.SET_FILE_FORMAT);
    TAlterTableSetFileFormatParams fileFormatParams =
        new TAlterTableSetFileFormatParams();
    fileFormatParams.setFile_format(fileformat);
    alterTableParams.setSet_file_format_params(fileFormatParams);
    req.setAlter_table_params(alterTableParams);
    catalogOpExecutor_.execDdlRequest(req);
  }

  /**
   * Sets the given field delimite for the table row format of the given table
   */
  private void alterTableSetRowFormatFromImpala(
      String dbName, String tblName, String fieldTerminator) throws ImpalaException {
    TDdlExecRequest req = new TDdlExecRequest();
    req.setQuery_options(new TDdlQueryOptions());
    req.setDdl_type(TDdlType.ALTER_TABLE);
    TAlterTableParams alterTableParams = new TAlterTableParams();
    alterTableParams.setTable_name(new TTableName(dbName, tblName));
    alterTableParams.setAlter_type(TAlterTableType.SET_ROW_FORMAT);
    TAlterTableSetRowFormatParams rowFormatParams = new TAlterTableSetRowFormatParams();
    TTableRowFormat rowFormat = new TTableRowFormat();
    rowFormat.setField_terminator(fieldTerminator);
    rowFormatParams.setRow_format(rowFormat);
    alterTableParams.setSet_row_format_params(rowFormatParams);
    req.setAlter_table_params(alterTableParams);
    catalogOpExecutor_.execDdlRequest(req);
  }

  /**
   * Sets the owner for the given table
   */
  private void alterTableSetOwnerFromImpala(String dbName, String tblName, String owner)
      throws ImpalaException {
    TDdlExecRequest req = new TDdlExecRequest();
    req.setQuery_options(new TDdlQueryOptions());
    req.setDdl_type(TDdlType.ALTER_TABLE);
    TAlterTableParams alterTableParams = new TAlterTableParams();
    alterTableParams.setTable_name(new TTableName(dbName, tblName));
    alterTableParams.setAlter_type(TAlterTableType.SET_OWNER);
    TAlterTableOrViewSetOwnerParams alterTableOrViewSetOwnerParams =
        new TAlterTableOrViewSetOwnerParams();
    alterTableOrViewSetOwnerParams.setOwner_name(owner);
    alterTableOrViewSetOwnerParams.setOwner_type(TOwnerType.USER);
    alterTableParams.setSet_owner_params(alterTableOrViewSetOwnerParams);
    req.setAlter_table_params(alterTableParams);
    catalogOpExecutor_.execDdlRequest(req);
  }

  /**
   * Sets the given location for a the give table using impala
   */
  private void alterTableSetLocationFromImpala(
      String dbName, String tblName, String location) throws ImpalaException {
    TDdlExecRequest req = new TDdlExecRequest();
    req.setQuery_options(new TDdlQueryOptions());
    req.setDdl_type(TDdlType.ALTER_TABLE);
    TAlterTableParams alterTableParams = new TAlterTableParams();
    alterTableParams.setTable_name(new TTableName(dbName, tblName));
    alterTableParams.setAlter_type(TAlterTableType.SET_LOCATION);
    TAlterTableSetLocationParams setLocationParams = new TAlterTableSetLocationParams();
    setLocationParams.setLocation(location);
    alterTableParams.setSet_location_params(setLocationParams);
    req.setAlter_table_params(alterTableParams);
    catalogOpExecutor_.execDdlRequest(req);
  }

  /**
   * Renames a table from Impala
   */
  private void alterTableRenameFromImpala(String dbName, String tblName, String newTable)
      throws ImpalaException {
    TDdlExecRequest req = new TDdlExecRequest();
    req.setQuery_options(new TDdlQueryOptions());
    req.setDdl_type(TDdlType.ALTER_TABLE);
    TAlterTableParams alterTableParams = new TAlterTableParams();
    alterTableParams.setTable_name(new TTableName(dbName, tblName));
    alterTableParams.setAlter_type(TAlterTableType.RENAME_TABLE);
    TAlterTableOrViewRenameParams renameParams = new TAlterTableOrViewRenameParams();
    renameParams.setNew_table_name(new TTableName(dbName, newTable));
    alterTableParams.setRename_params(renameParams);
    req.setAlter_table_params(alterTableParams);
    catalogOpExecutor_.execDdlRequest(req);
  }

  /**
   * Set table properties from impala
   */
  private void alterTableSetTblPropertiesFromImpala(String tblName)
      throws ImpalaException {
    TDdlExecRequest req = new TDdlExecRequest();
    req.setQuery_options(new TDdlQueryOptions());
    req.setDdl_type(TDdlType.ALTER_TABLE);
    TAlterTableParams alterTableParams = new TAlterTableParams();
    alterTableParams.setTable_name(new TTableName(TEST_DB_NAME, tblName));
    TAlterTableSetTblPropertiesParams setTblPropertiesParams =
        new TAlterTableSetTblPropertiesParams();
    setTblPropertiesParams.setTarget(TTablePropertyType.TBL_PROPERTY);
    Map<String, String> propertiesMap = new HashMap<String, String>() {
      { put("dummyKey1", "dummyValue1"); }
    };
    setTblPropertiesParams.setProperties(propertiesMap);
    alterTableParams.setSet_tbl_properties_params(setTblPropertiesParams);
    alterTableParams.setAlter_type(TAlterTableType.SET_TBL_PROPERTIES);
    req.setAlter_table_params(alterTableParams);
    catalogOpExecutor_.execDdlRequest(req);
    Table catalogTbl = catalog_.getTable(TEST_DB_NAME, tblName);
    assertNotNull(catalogTbl.getMetaStoreTable().getParameters());
    assertEquals(
        "dummyValue1", catalogTbl.getMetaStoreTable().getParameters().get("dummyKey1"));
  }

  private void alterTableComputeStats(String tblName, List<List<String>> partValsList)
      throws ImpalaException {
    TDdlExecRequest req = new TDdlExecRequest();
    req.setQuery_options(new TDdlQueryOptions());
    req.setDdl_type(TDdlType.ALTER_TABLE);

    TAlterTableParams alterTableParams = new TAlterTableParams();
    alterTableParams.setAlter_type(TAlterTableType.UPDATE_STATS);
    alterTableParams.setTable_name(new TTableName(TEST_DB_NAME, tblName));
    req.setAlter_table_params(alterTableParams);

    TAlterTableUpdateStatsParams updateStatsParams = new TAlterTableUpdateStatsParams();
    TTableStats tTableStats = new TTableStats();
    tTableStats.num_rows = 10;
    tTableStats.total_file_bytes = 1000;
    updateStatsParams.setTable_stats(tTableStats);
    Map<List<String>, TPartitionStats> partitionStats = new HashMap<>();
    for (List<String> partVals : partValsList) {
      TPartitionStats partStats = new TPartitionStats();
      partStats.stats = new TTableStats();
      partStats.stats.num_rows = 6;
      partitionStats.put(partVals, partStats);
    }

    updateStatsParams.setPartition_stats(partitionStats);

    alterTableParams.setUpdate_stats_params(updateStatsParams);
    catalogOpExecutor_.execDdlRequest(req);
  }
  /**
   * Set partition properties from Impala
   */
  private void alterTableSetPartitionPropertiesFromImpala(
      String tblName, List<TPartitionKeyValue> partKeyVal) throws ImpalaException {
    TDdlExecRequest req = new TDdlExecRequest();
    req.setQuery_options(new TDdlQueryOptions());
    req.setDdl_type(TDdlType.ALTER_TABLE);
    TAlterTableParams alterTableParams = new TAlterTableParams();
    alterTableParams.setTable_name(new TTableName(TEST_DB_NAME, tblName));
    TAlterTableSetTblPropertiesParams setTblPropertiesParams =
        new TAlterTableSetTblPropertiesParams();
    List<List<TPartitionKeyValue>> partitionsToAlter = new ArrayList<>();
    partitionsToAlter.add(partKeyVal);

    setTblPropertiesParams.setPartition_set(partitionsToAlter);
    setTblPropertiesParams.setTarget(TTablePropertyType.TBL_PROPERTY);
    Map<String, String> propertiesMap = new HashMap<String, String>() {
      { put("dummyKey1", "dummyValue1"); }
    };
    setTblPropertiesParams.setProperties(propertiesMap);
    alterTableParams.setSet_tbl_properties_params(setTblPropertiesParams);
    alterTableParams.setAlter_type(TAlterTableType.SET_TBL_PROPERTIES);
    req.setAlter_table_params(alterTableParams);
    catalogOpExecutor_.execDdlRequest(req);
  }

  /**
   * Insert multiple partitions into table from Impala
   */
  private void insertMulPartFromImpala(String tblName1, String tblName2,
      Map<String, TUpdatedPartition> updated_partitions, boolean overwrite)
      throws ImpalaException {
    String insert_mul_part = String.format(
        "insert into table %s partition(p1, p2) select * from %s", tblName1, tblName2);
    TUpdateCatalogRequest testInsertRequest = createTestTUpdateCatalogRequest(
        TEST_DB_NAME, tblName1, insert_mul_part, updated_partitions, overwrite, -1, -1);
    catalogOpExecutor_.updateCatalog(testInsertRequest);
  }

  /**
   * Insert into table or partition from Impala
   * @param tblName
   * @param isPartitioned
   * @return
   */
  private void insertFromImpala(String tblName, boolean isPartitioned, String p1val,
      String p2val, boolean isOverwrite, List<String> files) throws ImpalaException {
    insertFromImpala(tblName, isPartitioned, p1val, p2val, isOverwrite, files, -1, -1);
  }

  private void insertFromImpala(String tblName, boolean isPartitioned, String p1val,
      String p2val, boolean isOverwrite, List<String> files, long txnId, long writeId)
      throws ImpalaException {
    String partition = String.format("partition (%s, %s)", p1val, p2val);
    String test_insert_tbl = String.format("insert into table %s %s values ('a','aa') ",
        tblName, isPartitioned ? partition : "");
    Map<String, TUpdatedPartition> updated_partitions = new HashMap<>();
    TUpdatedPartition updatedPartition = new TUpdatedPartition();
    updatedPartition.setFiles(files);
    String created_part_str =
        isPartitioned ? String.format("%s/%s", p1val, p2val) : "";
    updated_partitions.put(created_part_str, updatedPartition);
    TUpdateCatalogRequest testInsertRequest = createTestTUpdateCatalogRequest(
        TEST_DB_NAME, tblName, test_insert_tbl, updated_partitions, isOverwrite,
        txnId, writeId);
    catalogOpExecutor_.updateCatalog(testInsertRequest);
  }

  /**
   * Create DML request to Catalog.
   */
  private TUpdateCatalogRequest createTestTUpdateCatalogRequest(String dBName,
      String tableName, String redacted_sql_stmt,
      Map<String, TUpdatedPartition> updated_partitions, boolean isOverwrite,
      long txnId, long writeId) {
    TUpdateCatalogRequest tUpdateCatalogRequest = new TUpdateCatalogRequest();
    tUpdateCatalogRequest.setDb_name(dBName);
    tUpdateCatalogRequest.setTarget_table(tableName);
    tUpdateCatalogRequest.setUpdated_partitions((updated_partitions));
    tUpdateCatalogRequest.setHeader(new TCatalogServiceRequestHeader());
    tUpdateCatalogRequest.getHeader().setRedacted_sql_stmt(redacted_sql_stmt);
    if (isOverwrite) tUpdateCatalogRequest.setIs_overwrite(true);
    if (txnId > 0) tUpdateCatalogRequest.setTransaction_id(txnId);
    if (writeId > 0) tUpdateCatalogRequest.setWrite_id(writeId);
    return tUpdateCatalogRequest;
  }

  private static TColumn getScalarColumn(String colName, TPrimitiveType type) {
    TTypeNode tTypeNode = new TTypeNode(TTypeNodeType.SCALAR);
    tTypeNode.setScalar_type(new TScalarType(type));
    TColumnType columnType = new TColumnType(Arrays.asList(tTypeNode));
    return new TColumn(colName, columnType);
  }

  private TPartitionDef getScalarPartitionDef(
      List<String> partNames, List<String> partVals) {
    TPartitionDef partitionDef = new TPartitionDef();
    List<TPartitionKeyValue> partKeyVals = new ArrayList<>();
    int i = 0;
    for (String partName : partNames) {
      partKeyVals.add(new TPartitionKeyValue(partName, partVals.get(i)));
      i++;
    }
    partitionDef.setPartition_spec(partKeyVals);
    return partitionDef;
  }

  private void createTable(String dbName, String tblName, boolean isPartitioned)
      throws TException {
    createTable(null, dbName, tblName, null, isPartitioned, null);
  }

  private void createTable(String tblName, boolean isPartitioned) throws TException {
    createTable(null, TEST_DB_NAME, tblName, null, isPartitioned, null);
  }

  private void dropTable(String tableName) throws TException {
    try (MetaStoreClient client = catalog_.getMetaStoreClient()) {
      client.getHiveClient().dropTable(TEST_DB_NAME, tableName, true, false);
    }
  }

  private void alterTableRename(String tblName, String newTblName, String newDbName)
      throws TException {
    try (MetaStoreClient msClient = catalog_.getMetaStoreClient()) {
      org.apache.hadoop.hive.metastore.api.Table newTable =
          msClient.getHiveClient().getTable(TEST_DB_NAME, tblName);
      String dbName = newDbName != null ? newDbName : TEST_DB_NAME;
      newTable.setTableName(newTblName);
      newTable.setDbName(dbName);
      msClient.getHiveClient().alter_table_with_environmentContext(
          TEST_DB_NAME, tblName, newTable, null);
    }
  }

  private void alterTableAddParameter(String tblName, String key, String val)
      throws TException {
    try (MetaStoreClient msClient = catalog_.getMetaStoreClient()) {
      org.apache.hadoop.hive.metastore.api.Table msTable =
          msClient.getHiveClient().getTable(TEST_DB_NAME, tblName);
      if (val == null) {
        msTable.getParameters().remove(key);
      } else {
        msTable.getParameters().put(key, val);
      }
      msClient.getHiveClient().alter_table_with_environmentContext(
          TEST_DB_NAME, tblName, msTable, null);
    }
  }

  /**
   * Alters trivial table properties which must be ignored by the event processor
   */
  private void alterTableChangeTrivialProperties(String tblName)
      throws TException {
    try (MetaStoreClient msClient = catalog_.getMetaStoreClient()) {
      org.apache.hadoop.hive.metastore.api.Table msTable =
          msClient.getHiveClient().getTable(TEST_DB_NAME, tblName);
      for (String parameter : MetastoreEvents.parametersToIgnore) {
        msTable.getParameters().put(parameter, "1234567");
      }
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
   * Sets the owner info from HMS
   * @param tblName
   * @param newOwner
   * @throws TException
   */
  private void alterTableSetOwner(String tblName, String newOwner) throws TException {
    try (MetaStoreClient msClient = catalog_.getMetaStoreClient()) {
      org.apache.hadoop.hive.metastore.api.Table msTable =
          msClient.getHiveClient().getTable(TEST_DB_NAME, tblName);
      msTable.setOwner(newOwner);
      msTable.setOwnerType(PrincipalType.USER);
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
      String location)
      throws TException {
    List<Partition> partitions = new ArrayList<>();
    try (MetaStoreClient metaStoreClient = catalog_.getMetaStoreClient()) {
      for (List<String> partVal : partValsList) {
        Partition partition = metaStoreClient.getHiveClient().getPartition(TEST_DB_NAME,
            tblName,
            partVal);
        partition.getSd().setLocation(location);
        partitions.add(partition);
      }
      metaStoreClient.getHiveClient().alter_partitions(TEST_DB_NAME, tblName, partitions);
    }
  }

  private void alterPartitionsParams(String db, String tblName, String key,
      String val, List<List<String>> partVals) throws Exception {
    try (MetaStoreClient metaStoreClient = catalog_.getMetaStoreClient()) {
      List<Partition> partitions = new ArrayList<>();
      for (List<String> partVal : partVals) {
        Partition partition = metaStoreClient.getHiveClient().getPartition(db,
            tblName, partVal);
        partition.getParameters().put(key, val);
        partitions.add(partition);
      }
      metaStoreClient.getHiveClient().alter_partitions(db, tblName, partitions);
    }
  }

  private void alterPartitionsParamsInTxn(String db, String tblName, String key,
      String val, List<List<String>> partVals) throws Exception {
    try (MetaStoreClient metaStoreClient = catalog_.getMetaStoreClient()) {
      long txnId = MetastoreShim.openTransaction(metaStoreClient.getHiveClient());
      long writeId = MetastoreShim.allocateTableWriteId(metaStoreClient.getHiveClient(),
          txnId, db, tblName);
      List<Partition> partitions = new ArrayList<>();
      for (List<String> partVal : partVals) {
        Partition partition = metaStoreClient.getHiveClient().getPartition(db,
            tblName, partVal);
        partition.getParameters().put(key, val);
        MetastoreShim.setWriteIdToMSPartition(partition, writeId);
        partitions.add(partition);
      }
      metaStoreClient.getHiveClient().alter_partitions(db, tblName, partitions);
      MetastoreShim.commitTransaction(metaStoreClient.getHiveClient(), txnId);
    }
  }

  /**
   * Alters trivial partition properties which must be ignored by the event processor
   */
  private void alterPartitionsTrivial(String tblName, List<String> partVal)
      throws TException {
    List<Partition> partitions = new ArrayList<>();
    try (MetaStoreClient metaStoreClient = catalog_.getMetaStoreClient()) {
      Partition partition = metaStoreClient.getHiveClient().getPartition(TEST_DB_NAME,
          tblName, partVal);
      for (String parameter : MetastoreEvents.parametersToIgnore) {
        partition.getParameters().put(parameter, "12334567");
        partitions.add(partition);
      }
      metaStoreClient.getHiveClient().alter_partitions(TEST_DB_NAME, tblName, partitions);
    }
  }

  private void addPartitions(String dbName, String tblName,
      List<List<String>> partitionValues)
      throws TException {
    try(MetaStoreClient client = catalog_.getMetaStoreClient()) {
      MetastoreApiTestUtils.addPartitions(client, dbName, tblName, partitionValues);
    }
  }

  private Table loadTable(String dbName, String tblName) throws CatalogException {
    Table loadedTable = catalog_.getOrLoadTable(dbName, tblName, "test", null);
    assertFalse("Table should have been loaded after getOrLoadTable call",
        loadedTable instanceof IncompleteTable);
    return loadedTable;
  }

  private Table loadTable(String tblName) throws CatalogException {
    return loadTable(TEST_DB_NAME, tblName);
  }
}
