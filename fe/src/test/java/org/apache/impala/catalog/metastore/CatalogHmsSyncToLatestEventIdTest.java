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

package org.apache.impala.catalog.metastore;

import com.google.common.collect.Lists;
import java.util.HashMap;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.apache.hadoop.hive.metastore.conf.MetastoreConf;
import org.apache.hadoop.hive.metastore.conf.MetastoreConf.ConfVars;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.NoSuchObjectException;

import org.apache.impala.catalog.*;
import org.apache.impala.catalog.MetaStoreClientPool.MetaStoreClient;
import org.apache.impala.catalog.events.ExternalEventsProcessor;
import org.apache.impala.catalog.events.MetastoreEventsProcessor;
import org.apache.impala.catalog.MetastoreApiTestUtils;
import org.apache.impala.catalog.events.NoOpEventProcessor;
import org.apache.impala.catalog.events.SynchronousHMSEventProcessorForTests;
import org.apache.impala.service.BackendConfig;
import org.apache.impala.service.CatalogOpExecutor;
import org.apache.impala.service.MetadataOp;
import org.apache.impala.testutil.CatalogServiceTestCatalog;
import org.apache.impala.testutil.CatalogTestMetastoreServer;
import org.apache.thrift.TException;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assume;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.apache.hadoop.hive.metastore.api.CurrentNotificationEventId;
import org.apache.impala.catalog.CatalogException;
import org.apache.impala.compat.MetastoreShim;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;


import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

/**
 * This class mainly tests ddl operations from catalogHmsClient and asserts that
 * db/table is synced to the latest event id. It also processes the same events
 * from MetastoreEventProcessor and asserts that those events are skipped since
 * they have already been processed by catalogHmsClient
 */
@RunWith(Parameterized.class)
public class CatalogHmsSyncToLatestEventIdTest extends AbstractCatalogMetastoreTest {
    private static String TEST_DB_NAME = "sync_to_latest_events_test_db";
    private static Logger LOG =
            LoggerFactory.getLogger(CatalogHmsSyncToLatestEventIdTest.class);
    protected static CatalogServiceTestCatalog catalog_;
    protected static CatalogOpExecutor catalogOpExecutor_;
    protected static CatalogMetastoreServer catalogMetastoreServer_;
    protected static HiveMetaStoreClient catalogHmsClient_;
    private static SynchronousHMSEventProcessorForTests eventsProcessor_;
    protected static final Configuration CONF = MetastoreConf.newMetastoreConf();
    private String tableType_;
    private static String managedTableType =
        org.apache.hadoop.hive.metastore.TableType.MANAGED_TABLE.toString();
    private static String externalTableType =
        org.apache.hadoop.hive.metastore.TableType.EXTERNAL_TABLE.toString();
    private static boolean flagEnableCatalogCache ,flagInvalidateCache,
        flagSyncToLatestEventId;

    @BeforeClass
    public static void setup() throws Exception {
        catalog_ = CatalogServiceTestCatalog.create();
        catalogOpExecutor_ = catalog_.getCatalogOpExecutor();
        catalogMetastoreServer_ = new CatalogTestMetastoreServer(
                catalogOpExecutor_);
        catalog_.setCatalogMetastoreServer(catalogMetastoreServer_);
        try (MetaStoreClient metaStoreClient = catalog_.getMetaStoreClient()) {
            CurrentNotificationEventId currentNotificationId =
                    metaStoreClient.getHiveClient().getCurrentNotificationEventId();
            eventsProcessor_ = new SynchronousHMSEventProcessorForTests(
                    catalogOpExecutor_, currentNotificationId.getEventId(), 10L);
            eventsProcessor_.start();
        }
        // Don't set event processor in catalog because
        // sync to latest event id should work even if event processor
        // is disabled
        catalogMetastoreServer_.start();
        MetastoreConf.setVar(CONF, ConfVars.THRIFT_URIS,
                "thrift://localhost:" + catalogMetastoreServer_.getPort());
        // metastore clients which connect to catalogd's HMS endpoint need this
        // configuration set since the forwarded HMS call use catalogd's HMS client
        // not the end-user's UGI.
        CONF.set("hive.metastore.execute.setugi", "false");
        catalogHmsClient_ = new HiveMetaStoreClient(CONF);
        assertTrue("Event processor should not be set",
            catalog_.getMetastoreEventProcessor() instanceof NoOpEventProcessor);
        // get previous values of flag to be set in cleanup
        flagEnableCatalogCache = BackendConfig.INSTANCE.enableCatalogdHMSCache();
        flagInvalidateCache = BackendConfig.INSTANCE.invalidateCatalogdHMSCacheOnDDLs();
        flagSyncToLatestEventId = BackendConfig.INSTANCE.enableSyncToLatestEventOnDdls();
    }

    @AfterClass
    public static void cleanUp() throws Exception {
        // in cleanup, set flag's values to previous value
        BackendConfig.INSTANCE.setEnableCatalogdHMSCache(flagEnableCatalogCache);
        BackendConfig.INSTANCE.setEnableSyncToLatestEventOnDdls(flagSyncToLatestEventId);
        BackendConfig.INSTANCE.setInvalidateCatalogdHMSCacheOnDDLs(flagInvalidateCache);
        if (eventsProcessor_ != null) {
            eventsProcessor_.shutdown();
        }
        catalogMetastoreServer_.stop();
        catalog_.close();

    }

    @After
    public void afterTest() throws TException, CatalogException {
        String dbName = TEST_DB_NAME;
        try {
            try (MetaStoreClient msClient = catalog_.getMetaStoreClient()) {
                msClient.getHiveClient().dropDatabase(dbName, true, true, true);
            }
            catalog_.removeDb(dbName);
        } catch (NoSuchObjectException e) {
            LOG.error("database {} does not exist in catalogd", dbName);
            catalog_.removeDb(dbName);
        }
    }

    @Before
    public void beforeTest() throws Exception {
        BackendConfig.INSTANCE.setEnableCatalogdHMSCache(true);
        BackendConfig.INSTANCE.setEnableSyncToLatestEventOnDdls(true);
        BackendConfig.INSTANCE.setInvalidateCatalogdHMSCacheOnDDLs(false);
    }

    public CatalogHmsSyncToLatestEventIdTest(String tableType) {
        tableType_ = tableType;
    }

    @Parameterized.Parameters
    public static String[] createTableTypes() {
        return new String[] {managedTableType, externalTableType};
    }
    @Test
    public void testCreateDatabase() throws Exception {
        LOG.info("Executing testCreateDatabase");
        String dbName = "test_create_database";
        try {
            Database msDb = MetastoreApiTestUtils
                .createHmsDatabaseObject(null, dbName, null);
            catalogHmsClient_.createDatabase(msDb);
            Db db = catalog_.getDb(dbName);
            assertTrue(db != null);
            assertTrue(db.getLastSyncedEventId() != -1);
            assertTrue(db.getLastSyncedEventId() == db.getCreateEventId());
        } finally {
            catalogHmsClient_.dropDatabase(dbName, true, true, true);
            assertTrue("db " + dbName + " should not be present in catalogd",
                catalog_.getDb(dbName) == null);
        }
    }

    @Test
    public void testAlterDatabase() throws Exception {
        LOG.info("Executing testAlterDatabase");
        String dbName = "test_alter_database";
        try {
            createDatabaseInCatalog(dbName);
            eventsProcessor_.processEvents();
            long lastSkippedEventsCount = eventsProcessor_.getMetrics()
                .getCounter(MetastoreEventsProcessor.EVENTS_SKIPPED_METRIC).getCount();
            Db catalogDb = catalog_.getDb(dbName);
            long prevSyncedEventId = catalogDb.getLastSyncedEventId();

            try (MetaStoreClient msClient = catalog_.getMetaStoreClient()) {
                MetastoreApiTestUtils.addDatabaseParametersInHms(msClient, dbName,
                    "key1", "val1");
            }
            String newOwner = "new_owner";
            Database alteredMsDb = getDatabaseInHms(dbName);
            alteredMsDb.setOwnerName(newOwner);
            // alter db via catalogHmsClient
            catalogHmsClient_.alterDatabase(dbName, alteredMsDb);

            catalogDb = catalog_.getDb(dbName);
            assertTrue(catalogDb.getOwnerUser().equals(newOwner));
            assertTrue(catalogDb.getMetaStoreDb().getParameters()
                .get("key1").equals("val1"));
            assertTrue(catalogDb.getLastSyncedEventId() >
                prevSyncedEventId);
            eventsProcessor_.processEvents();
            long currentSkippedEventsCount = eventsProcessor_.getMetrics()
                .getCounter(MetastoreEventsProcessor.EVENTS_SKIPPED_METRIC).getCount();
            assertTrue(lastSkippedEventsCount + 2 == currentSkippedEventsCount);
        } finally {
            catalogHmsClient_.dropDatabase(dbName, true, true, true);
        }
    }

    @Test
    public void testAddDropAlterPartitions() throws Exception {
        LOG.info("Executing testAddDropAlterPartitions");
        String tblName = "test_add_drop_alter_partitions_" + tableType_ + "_tbl" ;
        try {
            createDatabaseInCatalog(TEST_DB_NAME);
            try {
                catalogHmsClient_.dropTable(TEST_DB_NAME, tblName, true, true);
            } catch (Exception e) {
                LOG.error("Failed to drop table {} from HMS", tblName);
            }
            catalogHmsClient_
                .createTable(MetastoreApiTestUtils.getTestTable(null,
                TEST_DB_NAME, tblName, null, true, tableType_));

            HdfsTable tbl = getCatalogHdfsTable(TEST_DB_NAME, tblName);

            assertTrue(tbl != null);
            assertTrue("table's last synced id should not be -1",
                tbl.getLastSyncedEventId() != -1);
            assertTrue(tbl.getLastSyncedEventId() == tbl.getCreateEventId());

            eventsProcessor_.processEvents();
            long lastSkippedCount = eventsProcessor_.getMetrics()
                .getCounter(MetastoreEventsProcessor.EVENTS_SKIPPED_METRIC).getCount();
            long prevSyncedEventId = tbl.getLastSyncedEventId();
            List<List<String> > partVals = new ArrayList<>();
            partVals.add(Arrays.asList("1"));
            partVals.add(Arrays.asList("2"));
            partVals.add(Arrays.asList("3"));
            addPartitionsInHms(TEST_DB_NAME, tblName, partVals);
            // added partitions should not reflect in table
            // stored in catalog cache
            assertTrue(tbl.getPartitions().size() == 0);

            // alter partition 2 directly in HMS
            Partition partitionToAlter =
                getPartitionInHms(TEST_DB_NAME, tblName, Arrays.asList("2"));

            String newLocation = "/path/to/newLocation/";
            partitionToAlter.getSd().setLocation(newLocation);
            alterPartitionInHms(TEST_DB_NAME, tblName, partitionToAlter);

            // when dropping partitions from catalogHmsClient, sync to latest
            // event id adds 3 partitions and drops 1
            catalogHmsClient_.dropPartition(TEST_DB_NAME, tblName,
                Arrays.asList("3"), true);
            tbl = getCatalogHdfsTable(TEST_DB_NAME, tblName);
            assertTrue("Table should have 2 partitions after dropping 1 "
                    + "out of 3 partitions", tbl.getPartitions().size() == 2);

            // assert that  partition with new location from cached table
            // exists
            FeFsPartition modifiedPartition = null;
            for (FeFsPartition part : FeCatalogUtils.loadAllPartitions(tbl)) {
                if (part.getLocation().equals(newLocation)) {
                    modifiedPartition = part;
                    break;
                }
            }
            assertTrue(modifiedPartition != null);
            assertTrue(tbl.getLastSyncedEventId() > prevSyncedEventId);
            // test that events processor skipped all events
            // since last synced event
            eventsProcessor_.processEvents();
            long currentSkippedCount = eventsProcessor_.getMetrics()
                .getCounter(MetastoreEventsProcessor.EVENTS_SKIPPED_METRIC).getCount();

            assertTrue( String.format("CurrentSkippedCount %s differs from "
                        + "lastSkippedCount + 3 %s", currentSkippedCount,
                    lastSkippedCount),
                currentSkippedCount == lastSkippedCount + 3);
        } finally {
            catalogHmsClient_.dropTable(TEST_DB_NAME, tblName, true, true);
        }
    }

    @Test
    public void testExchangePartition() throws Exception {
        // run this test only for managed table
        Assume.assumeTrue(tableType_.equals(managedTableType));
        LOG.info("Executing testExchangePartition");
        String srcTblName = "test_exchange_partition_src_" + tableType_ + "_tbl";
        String destTblName = "test_exchange_partition_dest_" + tableType_ + "_tbl";
        try {
            createDatabaseInCatalog(TEST_DB_NAME);
            // drop tables if already exist
            catalogHmsClient_.dropTable(TEST_DB_NAME, srcTblName, true, true);
            catalogHmsClient_.dropTable(TEST_DB_NAME, destTblName, true, true);
            org.apache.hadoop.hive.metastore.api.Table srcMsTable =
                MetastoreApiTestUtils.getTestTable(null, TEST_DB_NAME,
                    srcTblName, null, true, managedTableType);
            catalogHmsClient_.createTable(srcMsTable);
            // add 3 partitions but only in HMS
            List<List<String> > srcPartVals = new ArrayList<>();
            srcPartVals.add(Arrays.asList("1"));
            srcPartVals.add(Arrays.asList("2"));
            srcPartVals.add(Arrays.asList("3"));
            addPartitionsInHms(TEST_DB_NAME, srcTblName, srcPartVals);
            Map<String, String> partitionSpec =
                getPartitionSpec(srcMsTable, Arrays.asList("1"));
            org.apache.hadoop.hive.metastore.api.Table destMsTable =
                MetastoreApiTestUtils.getTestTable(null,
                TEST_DB_NAME, destTblName, null, true, managedTableType);
            catalogHmsClient_.createTable(destMsTable);
            // add partition with val 4 but only in HMS
            List<List<String> > destPartVals = new ArrayList<>();
            destPartVals.add(Arrays.asList("4"));
            addPartitionsInHms(TEST_DB_NAME, destTblName, destPartVals);
            long eventIdBeforeExchange = getLatestEventIdFromHMS();

            catalogHmsClient_.exchange_partition(partitionSpec,TEST_DB_NAME, srcTblName,
                TEST_DB_NAME, destTblName);
            HdfsTable srcCatalogTbl = getCatalogHdfsTable(TEST_DB_NAME, srcTblName);
            HdfsTable destCatalogTbl = getCatalogHdfsTable(TEST_DB_NAME, destTblName);
            assertTrue(srcCatalogTbl.getPartitions().size() == 2);
            assertTrue(destCatalogTbl.getPartitions().size() == 2);

            // assert that part with val 1 does not exist in src table
            for (FeFsPartition srcPartition :
                FeCatalogUtils.loadAllPartitions(srcCatalogTbl)) {
                List<String> partVals =
                    srcPartition.getPartitionValuesAsStrings(false);
                assertFalse(partVals.equals(Arrays.asList("1")));
            }
            // it is enough to assert that last synced event id of Hdfs table >
            // event id before exchange partition api
            assertTrue(srcCatalogTbl.getLastSyncedEventId() > eventIdBeforeExchange);
            assertTrue(destCatalogTbl.getLastSyncedEventId() > eventIdBeforeExchange);
        } finally {
            catalogHmsClient_.dropTable(TEST_DB_NAME, srcTblName, true, true);
            catalogHmsClient_.dropTable(TEST_DB_NAME, destTblName, true, true);
        }
    }

    @Test
    public void testTableCreateDropCreate() throws Exception {
        LOG.info("Executing testTableCreateDropCreate");
        String tblName = "test_create_drop_create_" + tableType_ + "_tbl";
        String tblNameLowerCase = tblName.toLowerCase();
        try {
            createDatabaseInCatalog(TEST_DB_NAME);
            catalogHmsClient_.dropTable(TEST_DB_NAME, tblName, true, true);
            catalogHmsClient_.createTable(MetastoreApiTestUtils.getTestTable(null,
                TEST_DB_NAME, tblName, null, true, tableType_));
            HdfsTable tbl = (HdfsTable) catalog_.getTable(TEST_DB_NAME, tblNameLowerCase);
            assertTrue(tbl.isPartitioned());
            // last synced event id is same as create event id
            long prevCreateEventId = tbl.getLastSyncedEventId();
            // drop table from HMS skipping catalog metastore
            try (MetaStoreClient msClient = catalog_.getMetaStoreClient()) {
                msClient.getHiveClient().dropTable(TEST_DB_NAME, tblName, true, false);
            }
            // recreate table with same name but unpartitioned to distinguish it
            // from previous table
            catalogHmsClient_.createTable(MetastoreApiTestUtils.getTestTable(null,
                TEST_DB_NAME, tblName, null, false, tableType_));
            HdfsTable currentTbl = (HdfsTable) catalog_.getTable(TEST_DB_NAME,
                tblNameLowerCase);
            assertTrue(currentTbl.getLastSyncedEventId() != prevCreateEventId);
            assertTrue(!currentTbl.isPartitioned());
        } finally {
            catalogHmsClient_.dropTable(TEST_DB_NAME, tblName, true, true);
        }
    }

    @Test
    public void testAlterTableNoRename() throws Exception {
        LOG.info("Executing testAlterTableNoRename");
        String tblName = "test_alter_table_" + tableType_ + "_tbl";
        try {
            createDatabaseInCatalog(TEST_DB_NAME);
            // drop table if it already exists
            catalogHmsClient_.dropTable(TEST_DB_NAME, tblName, true, true);
            catalogHmsClient_.createTable(MetastoreApiTestUtils.getTestTable(null,
                TEST_DB_NAME, tblName, null, true, tableType_));
            HdfsTable tbl = getCatalogHdfsTable(TEST_DB_NAME, tblName);
            eventsProcessor_.processEvents();
            long lastSkippedEventsCount = eventsProcessor_.getMetrics()
                .getCounter(MetastoreEventsProcessor.EVENTS_SKIPPED_METRIC).getCount();

            org.apache.hadoop.hive.metastore.api.Table newMsTable =
                tbl.getMetaStoreTable().deepCopy();
            List<FieldSchema> cols = Lists.newArrayList(
                new FieldSchema("c1","string","c1 description"));

            org.apache.hadoop.hive.metastore.api.StorageDescriptor updatedSd =
                newMsTable.getSd();
            updatedSd.setCols(cols);
            newMsTable.setSd(updatedSd);

            // alter table but not from catalogHMSClient so that it is
            // synced up later
            try (MetaStoreClient msClient = catalog_.getMetaStoreClient()) {
                msClient.getHiveClient().alter_table_with_environmentContext(TEST_DB_NAME,
                    tblName, newMsTable, null);
            }
            // assert the cached table's SD is not changed
            tbl = getCatalogHdfsTable(TEST_DB_NAME, tblName);
            long prevSyncedEventId = tbl.getLastSyncedEventId();
            org.apache.hadoop.hive.metastore.api.StorageDescriptor oldSd =
                tbl.getMetaStoreTable().getSd();
            assertFalse(oldSd.equals(updatedSd));

            // get the latest table from metastore and alter it via catalogHmsClient
            org.apache.hadoop.hive.metastore.api.Table latestMsTable =
                getHmsTable(TEST_DB_NAME, tblName);

            String newOwner = "newOwnerForTestAlterTable";
            latestMsTable.setOwner(newOwner);

            // alter latest table via catalogHMSClient
            catalogHmsClient_.alter_table_with_environmentContext(TEST_DB_NAME, tblName,
                latestMsTable, null);

            // get latest table from the cache
            HdfsTable updatedTbl = getCatalogHdfsTable(TEST_DB_NAME, tblName);
            assertTrue(updatedTbl.getOwnerUser().equals(newOwner));
            assertTrue(updatedTbl.getMetaStoreTable().getSd().equals(updatedSd));
            assertTrue(
                updatedTbl.getLastSyncedEventId() > prevSyncedEventId);
            // assert that alter table events are skipped by event processor
            eventsProcessor_.processEvents();
            long currentSkippedEventsCount = eventsProcessor_.getMetrics()
                .getCounter(MetastoreEventsProcessor.EVENTS_SKIPPED_METRIC).getCount();
            assertTrue(lastSkippedEventsCount + 2 == currentSkippedEventsCount);

        } finally {
            catalogHmsClient_.dropTable(TEST_DB_NAME, tblName, true, true);
        }
    }

    @Test
    public void testAlterTableRename() throws Exception {
        LOG.info("Executing testALterTableRename");
        String tblName = ("test_alter_table_rename_" + tableType_ + "_tbl").toLowerCase();
        String newTblName = tblName + "_new";
        try {
            createDatabaseInCatalog(TEST_DB_NAME);
            catalogHmsClient_.dropTable(TEST_DB_NAME, tblName, true, true);
            catalogHmsClient_.dropTable(TEST_DB_NAME, newTblName, true, true);
            catalogHmsClient_.createTable(MetastoreApiTestUtils.getTestTable(null,
                TEST_DB_NAME, tblName, null, true, tableType_));
            HdfsTable oldTbl = getCatalogHdfsTable(TEST_DB_NAME, tblName);
            eventsProcessor_.processEvents();
            long lastSkippedEventsCount = eventsProcessor_.getMetrics()
                .getCounter(MetastoreEventsProcessor.EVENTS_SKIPPED_METRIC).getCount();
            org.apache.hadoop.hive.metastore.api.Table newMsTable =
                oldTbl.getMetaStoreTable().deepCopy();
            newMsTable.setTableName(newTblName);

            catalogHmsClient_.alter_table_with_environmentContext(TEST_DB_NAME, tblName,
                newMsTable, null);
            // check that old table does not exist in cache
            assertTrue(catalog_.getTableNoThrow(TEST_DB_NAME, tblName) == null);
            HdfsTable newTbl = getCatalogHdfsTable(TEST_DB_NAME, newTblName);
            assertTrue(newTbl != null);
            assertTrue(newTbl.getLastSyncedEventId() > -1);
            eventsProcessor_.processEvents();
            long currentSkippedEventsCount = eventsProcessor_.getMetrics()
                .getCounter(MetastoreEventsProcessor.EVENTS_SKIPPED_METRIC).getCount();
            assertTrue( lastSkippedEventsCount + 1 == currentSkippedEventsCount);
        } finally {
            catalogHmsClient_.dropTable(TEST_DB_NAME, tblName, true, true);
            catalogHmsClient_.dropTable(TEST_DB_NAME, newTblName, true, true);
        }
    }

    @Test
    public void testSyncToLatestEventIdFlag() throws Exception {
        String tblName = "test_sync_to_latest_event_id_flag_" + tableType_ + "_tbl";
        LOG.info("Executing testSyncToLatestEventIdFlag");
        boolean prevFlag =
            BackendConfig.INSTANCE.enableSyncToLatestEventOnDdls();
        try {
            createDatabaseInCatalog(TEST_DB_NAME);
            catalogHmsClient_.dropTable(TEST_DB_NAME, tblName, true, true);
            catalogHmsClient_
                .createTable(MetastoreApiTestUtils.getTestTable(null,
                    TEST_DB_NAME, tblName, null, true, tableType_));

            HdfsTable tbl = getCatalogHdfsTable(TEST_DB_NAME, tblName);
            long lastSyncedEventId = tbl.getLastSyncedEventId();
            // set sync to latest event id flag to false so that further
            // table is not synced for HMS operations
            BackendConfig.INSTANCE.setEnableSyncToLatestEventOnDdls(false);
            List<List<String> > partVals = new ArrayList<>();
            partVals.add(Arrays.asList("1"));
            partVals.add(Arrays.asList("2"));
            partVals.add(Arrays.asList("3"));
            addPartitionsInHms(TEST_DB_NAME, tblName, partVals);

            catalogHmsClient_.dropPartition(TEST_DB_NAME, tblName,
                Arrays.asList("3"), true);
            tbl = getCatalogHdfsTable(TEST_DB_NAME, tblName);
            // with sync to latest event id flag set to false
            // last synced event id for a table should not change
            assertTrue(lastSyncedEventId == tbl.getLastSyncedEventId());

        } finally {
            BackendConfig.INSTANCE.setEnableSyncToLatestEventOnDdls(prevFlag);
            catalogHmsClient_.dropTable(TEST_DB_NAME, tblName, true, true);
        }
    }

    @Test
    public void testFullTableReload() throws Exception {
        LOG.info("Executing testFullTableReload");
        String tblName = "full_table_reload_test_"+ tableType_ + "_tbl";
        try {
            createDatabaseInCatalog(TEST_DB_NAME);
            catalogHmsClient_.dropTable(TEST_DB_NAME, tblName, true, true);
            // create a table in HMS and add it as incomplete table
            // so that full table refresh can reload it
            createTableInHms(TEST_DB_NAME, tblName, true);
            IncompleteTable tbl =
                IncompleteTable.createUninitializedTable(catalog_.getDb(TEST_DB_NAME),
                    tblName, MetadataOp.getImpalaTableType(tableType_), null);
            tbl.setCreateEventId(getLatestEventIdFromHMS());
            catalog_.addTable(catalog_.getDb(TEST_DB_NAME), tbl);
            long prevLastSyncedEventId =
                catalog_.getTable(TEST_DB_NAME, tblName).getLastSyncedEventId();
            // add partitions but only in HMS so that
            // request for full table reload syncs table to latest
            // event id
            List<List<String> > partVals = new ArrayList<>();
            partVals.add(Arrays.asList("1"));
            partVals.add(Arrays.asList("2"));
            partVals.add(Arrays.asList("3"));
            addPartitionsInHms(TEST_DB_NAME, tblName, partVals);
            Table refreshedTbl = catalog_.getOrLoadTable(TEST_DB_NAME, tblName,
                "testing table syncing to latest event id", null);

            assertTrue(
                refreshedTbl.getLastSyncedEventId() > refreshedTbl.getCreateEventId());
            assertTrue(refreshedTbl.getLastSyncedEventId() > prevLastSyncedEventId);
            assertTrue(refreshedTbl instanceof HdfsTable);
            HdfsTable hdfsTable = (HdfsTable) refreshedTbl;
            assertTrue(hdfsTable.getPartitions().size() == 3);

        } finally {
            catalogHmsClient_.dropTable(TEST_DB_NAME, tblName, true, true);
        }
    }

    @Test
    public void testTableEventsProcessedByEventProcessor() throws Exception {
        // TODO: Move this to new file and add more tests
        // that cover more events MetastoreEvents
        LOG.info("Executing testEventsProcessedByEventProcessor");
        String tblName = "test_table_events_processed_by_event_processor_" +
            tableType_ + "_tbl";
        ExternalEventsProcessor prevEventProcessor =
            catalog_.getMetastoreEventProcessor();
        try {
            catalog_.setMetastoreEventProcessor(eventsProcessor_);
            eventsProcessor_.processEvents();
            createDatabaseInCatalog(TEST_DB_NAME);
            catalogHmsClient_.dropTable(TEST_DB_NAME, tblName, true, true);
            catalogHmsClient_
                .createTable(MetastoreApiTestUtils.getTestTable(null,
                    TEST_DB_NAME, tblName, null, true, tableType_));
            HdfsTable tbl = getCatalogHdfsTable(TEST_DB_NAME, tblName);
            assertTrue(tbl != null);
            assertTrue("table's last synced id should not be -1",
                tbl.getLastSyncedEventId() != -1);
            assertTrue(tbl.getLastSyncedEventId() == tbl.getCreateEventId());
            long prevSyncedEventId = tbl.getLastSyncedEventId();
            eventsProcessor_.processEvents();
            List<List<String> > partVals = new ArrayList<>();
            partVals.add(Arrays.asList("1"));
            partVals.add(Arrays.asList("2"));
            partVals.add(Arrays.asList("3"));
            addPartitionsInHms(TEST_DB_NAME, tblName, partVals);
            // added partitions should not reflect in table
            // stored in catalog cache
            assertTrue(tbl.getPartitions().size() == 0);
            eventsProcessor_.processEvents();
            tbl = getCatalogHdfsTable(TEST_DB_NAME, tblName);
            LOG.info("After add partititons, table last synced event id {}, latest "
                    + "event id in HMS {}", tbl.getLastSyncedEventId(),
                getLatestEventIdFromHMS());
            assertTrue(tbl.getLastSyncedEventId() > prevSyncedEventId);
            prevSyncedEventId = tbl.getLastSyncedEventId();

            // alter partition 2 directly in HMS
            Partition partitionToAlter =
                getPartitionInHms(TEST_DB_NAME, tblName, Arrays.asList("2"));

            String newLocation = "/path/to/newLocation/";
            partitionToAlter.getSd().setLocation(newLocation);
            alterPartitionInHms(TEST_DB_NAME, tblName, partitionToAlter);
            eventsProcessor_.processEvents();
            LOG.info("After alter partititons, table last synced event id {}, latest "
                    + "event id in HMS {}", tbl.getLastSyncedEventId(),
                getLatestEventIdFromHMS());
            assertTrue(tbl.getLastSyncedEventId() > prevSyncedEventId);
            prevSyncedEventId = tbl.getLastSyncedEventId();
            dropPartitionInHms(TEST_DB_NAME, tblName, Arrays.asList("3"), true);
            eventsProcessor_.processEvents();
            LOG.info("After drop partitions, table last synced event id {}, latest "
                    + "event id in HMS {}", tbl.getLastSyncedEventId(),
                getLatestEventIdFromHMS());
            assertTrue(tbl.getLastSyncedEventId() > prevSyncedEventId);

            tbl = getCatalogHdfsTable(TEST_DB_NAME, tblName);
            assertTrue("Table should have 2 partitions after dropping 1 "
                + "out of 3 partitions", tbl.getPartitions().size() == 2);
            // assert that  partition with new location from cached table
            // exists
            FeFsPartition modifiedPartition = null;
            for (FeFsPartition part : FeCatalogUtils.loadAllPartitions(tbl)) {
                if (part.getLocation().equals(newLocation)) {
                    modifiedPartition = part;
                    break;
                }
            }
            assertTrue(modifiedPartition != null);
        } finally {
            catalog_.setMetastoreEventProcessor(prevEventProcessor);
            eventsProcessor_.processEvents();
            catalogHmsClient_.dropTable(TEST_DB_NAME, tblName, true, true);
        }
    }

    @Test
    public void testDbEventProcessedByEventProcessor() throws Exception {
        LOG.info("Executing testDbEventProcessedByEventProcessor");
        String dbName = "test_db_event_processed_by_event_processor_db";
        ExternalEventsProcessor prevEventProcessor =
            catalog_.getMetastoreEventProcessor();
        try {
            catalog_.setMetastoreEventProcessor(eventsProcessor_);
            createDatabaseInCatalog(dbName);
            long prevSyncedEventId = catalog_.getDb(dbName).getLastSyncedEventId();
            try (MetaStoreClient msClient = catalog_.getMetaStoreClient()) {
                MetastoreApiTestUtils.addDatabaseParametersInHms(msClient, dbName,
                    "key1", "val1");
            }
            // assert that db's parameters are null
            assertTrue(
                catalog_.getDb(dbName).getMetaStoreDb().getParameters() == null);

            eventsProcessor_.processEvents();
            // after processing event, key1 should reflect in msDb parameters
            assertTrue(catalog_.getDb(dbName).getMetaStoreDb().getParameters()
                .get("key1").equals("val1"));
            assertTrue(
                catalog_.getDb(dbName).getLastSyncedEventId() > prevSyncedEventId);
        } finally {
            catalog_.setMetastoreEventProcessor(prevEventProcessor);
            catalogHmsClient_.dropDatabase(dbName, true, true, true);
        }
    }

    private void createDatabaseInHms(String catName, String dbName,
        Map<String, String> params) throws TException {
        try (MetaStoreClient msClient = catalog_.getMetaStoreClient()) {
            MetastoreApiTestUtils.createDatabase(msClient,
                null, TEST_DB_NAME, null);
        }
    }

    private void createTableInHms(String dbName, String tblName, boolean isPartitioned)
        throws TException {
        try (MetaStoreClient msClient = catalog_.getMetaStoreClient()) {
            MetastoreApiTestUtils.createTable(msClient, null,
                dbName, tblName, null, isPartitioned);
        }
    }

    private void addPartitionsInHms(String dbName, String tblName,
        List<List<String>> partitionValues) throws TException {
        try(MetaStoreClient msClient = catalog_.getMetaStoreClient()) {
            MetastoreApiTestUtils.addPartitions(msClient, dbName,
                tblName, partitionValues);
        }
    }

    private long getLatestEventIdFromHMS() throws TException {
        long lastEventId = -1;
        try(MetaStoreClient client = catalog_.getMetaStoreClient()) {
            lastEventId =
                client.getHiveClient().getCurrentNotificationEventId().getEventId();
        }
        return lastEventId;
    }

    private org.apache.hadoop.hive.metastore.api.Table getHmsTable(String dbName,
        String tblName) throws TException {
        try (MetaStoreClient msClient = catalog_.getMetaStoreClient()) {
            return msClient.getHiveClient().getTable(dbName, tblName);
        }
    }

    private org.apache.impala.catalog.HdfsTable getCatalogHdfsTable(String dbName,
        String tblName) throws CatalogException{
        org.apache.impala.catalog.Table tbl =  catalog_.getTable(dbName, tblName);
        if (tbl instanceof HdfsTable) {
            return (HdfsTable) tbl;
        } else {
            return null;
        }
    }

    private Partition getPartitionInHms(String dbName, String tblName,
        List<String> partVal) throws TException {
        try (MetaStoreClient msClient = catalog_.getMetaStoreClient()) {
            return msClient.getHiveClient().getPartition(dbName,
                tblName, partVal);
        }
    }

    private void alterPartitionInHms(String dbName, String tblName, Partition partition)
        throws TException {
        try(MetaStoreClient msClient = catalog_.getMetaStoreClient()) {
            msClient.getHiveClient().alter_partition(dbName, tblName, partition);
        }
    }

    private void dropPartitionInHms(String dbName, String tblName, List<String> partVals,
        boolean deleteData) throws TException {
        try(MetaStoreClient msClient = catalog_.getMetaStoreClient()) {
            msClient.getHiveClient().dropPartition(dbName, tblName, partVals, deleteData);
        }
    }

    private Database getDatabaseInHms(String dbName) throws TException {
        try (MetaStoreClient msClient = catalog_.getMetaStoreClient()) {
            return  msClient.getHiveClient().getDatabase(dbName);
        }
    }

    private Map<String, String> getPartitionSpec(
        org.apache.hadoop.hive.metastore.api.Table tbl, List<String> vals) {
        Map<String, String> partitionSpec = new HashMap<>();
        for(int i = 0; i < tbl.getPartitionKeys().size(); i++) {
            FieldSchema partCol = tbl.getPartitionKeys().get(i);
            partitionSpec.put(partCol.getName(), vals.get(i));
        }
        return partitionSpec;
    }

    private void createDatabaseInCatalog(String dbName) throws TException {
        Database msDb = MetastoreApiTestUtils
            .createHmsDatabaseObject(null, dbName, null);
        catalogHmsClient_.createDatabase(msDb);
        assertTrue("db " + dbName + " not present in catalogd",
            catalog_.getDb(dbName) != null);
    }
}
