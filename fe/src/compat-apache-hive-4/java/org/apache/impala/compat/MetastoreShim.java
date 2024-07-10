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

package org.apache.impala.compat;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.base.Stopwatch;
import com.google.common.collect.Lists;

import java.lang.reflect.Proxy;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.apache.commons.lang3.math.NumberUtils;
import org.apache.commons.lang3.reflect.FieldUtils;
import org.apache.hadoop.hive.common.ValidTxnList;
import org.apache.hadoop.hive.common.ValidWriteIdList;
import org.apache.hadoop.hive.metastore.IMetaStoreClient;
import org.apache.hadoop.hive.metastore.RetryingMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.AlreadyExistsException;
import org.apache.hadoop.hive.metastore.api.ColumnStatistics;
import org.apache.hadoop.hive.metastore.api.ColumnStatisticsObj;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.FireEventRequest;
import org.apache.hadoop.hive.metastore.api.FireEventRequestData;
import org.apache.hadoop.hive.metastore.api.InsertEventRequestData;
import org.apache.hadoop.hive.metastore.api.InvalidInputException;
import org.apache.hadoop.hive.metastore.api.InvalidObjectException;
import org.apache.hadoop.hive.metastore.api.InvalidOperationException;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.NoSuchObjectException;
import org.apache.hadoop.hive.metastore.api.NotificationEvent;
import org.apache.hadoop.hive.metastore.api.NotificationEventRequest;
import org.apache.hadoop.hive.metastore.api.NotificationEventResponse;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.metastore.api.TableValidWriteIds;
import org.apache.hadoop.hive.metastore.api.ThriftHiveMetastore;
import org.apache.hadoop.hive.metastore.api.ThriftHiveMetastore.Client;
import org.apache.hadoop.hive.metastore.messaging.AlterTableMessage;
import org.apache.hadoop.hive.metastore.messaging.EventMessage;
import org.apache.hadoop.hive.metastore.messaging.MessageDeserializer;
import org.apache.hadoop.hive.metastore.messaging.MessageFactory;
import org.apache.hadoop.hive.metastore.messaging.json.JSONDropDatabaseMessage;
//import org.apache.hadoop.hive.metastore.messaging.json.JSONMessageFactory;
import org.apache.hadoop.hive.metastore.messaging.MessageBuilder;
import org.apache.hadoop.hive.metastore.txn.TxnUtils;
import org.apache.hadoop.hive.metastore.utils.FileUtils;
import org.apache.impala.catalog.CatalogException;
import org.apache.impala.catalog.CatalogServiceCatalog;
import org.apache.impala.catalog.DataSource;
import org.apache.impala.catalog.Db;
import org.apache.impala.catalog.HdfsPartition;
import org.apache.impala.catalog.HdfsTable;
import org.apache.impala.catalog.Hive4MetastoreShimBase;
import org.apache.impala.catalog.MetaStoreClientPool;
import org.apache.impala.catalog.MetaStoreClientPool.MetaStoreClient;
import org.apache.impala.catalog.events.MetastoreEvents.MetastoreEvent;
import org.apache.impala.catalog.events.MetastoreEventsProcessor.MetaDataFilter;
import org.apache.impala.catalog.events.MetastoreNotificationException;
import org.apache.impala.catalog.events.SelfEventContext;
import org.apache.impala.catalog.local.MetaProvider.PartitionMetadata;
import org.apache.impala.catalog.local.MetaProvider.PartitionRef;
import org.apache.impala.catalog.local.MetaProvider.TableMetaRef;
import org.apache.impala.catalog.metastore.ICatalogMetastoreServer;
import org.apache.impala.common.ImpalaRuntimeException;
import org.apache.impala.common.Metrics;
import org.apache.impala.service.CatalogOpExecutor;
import org.apache.impala.util.AcidUtils;
import org.apache.hadoop.hive.metastore.api.SetPartitionsStatsRequest;
import org.apache.impala.util.AcidUtils.TblTransaction;
import org.apache.impala.util.MetaStoreUtil.TableInsertEventInfo;
import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Set;

import org.apache.impala.catalog.TableWriteId;
import org.apache.impala.analysis.TableName;

import java.util.HashMap;
import java.util.stream.Collectors;

import org.apache.impala.catalog.TableNotFoundException;
import org.apache.impala.catalog.TableNotFoundException;
import org.apache.impala.catalog.TableNotLoadedException;
import org.apache.impala.catalog.TableWriteId;
import org.apache.impala.catalog.DatabaseNotFoundException;
import org.apache.hadoop.hive.metastore.utils.MetaStoreUtils;
import org.apache.hadoop.hive.metastore.messaging.CommitTxnMessage;

import org.apache.impala.catalog.events.MetastoreEvents.MetastoreEventType;
import org.apache.impala.catalog.events.MetastoreEventsProcessor;
import org.apache.hadoop.hive.metastore.api.WriteEventInfo;
import org.apache.hadoop.hive.metastore.api.WriteNotificationLogRequest;
import org.apache.hadoop.hive.metastore.api.GetAllWriteEventInfoRequest;
import org.apache.impala.catalog.events.MetastoreNotificationNeedsInvalidateException;
import org.apache.impala.hive.common.MutableValidWriteIdList;
import org.apache.hadoop.hive.metastore.api.GetLatestCommittedCompactionInfoRequest;
import org.apache.hadoop.hive.metastore.api.GetLatestCommittedCompactionInfoResponse;
import org.apache.impala.catalog.CompactionInfoLoader;
import org.apache.hadoop.hive.metastore.api.CompactionInfoStruct;
import com.google.common.collect.Iterables;

import org.apache.hadoop.hive.metastore.messaging.MessageEncoder;
import org.apache.hadoop.hive.metastore.messaging.MessageSerializer;

import static org.apache.impala.util.HiveMetadataFormatUtils.formatOutput;

import org.apache.hadoop.hive.metastore.conf.MetastoreConf;
import org.apache.hadoop.hive.metastore.txn.TxnCommonUtils;


/**
 * A wrapper around some of Hive's Metastore API's to abstract away differences
 * between major versions of different Hive publishers. This implements the shimmed
 * methods for Apache Hive 3.
 */
public class MetastoreShim extends Hive4MetastoreShimBase {
    private static final Logger LOG = LoggerFactory.getLogger(MetastoreShim.class);

    public static final byte ACCESSTYPE_NONE = (byte) 1;
    public static final byte ACCESSTYPE_READONLY = (byte) 2;
    public static final byte ACCESSTYPE_WRITEONLY = (byte) 4;
    public static final byte ACCESSTYPE_READWRITE = (byte) 8;

    private static final String ACCESSTYPE = "accessType";
    private static final String WRITEID = "writeId";
    private static final String ID = "id";
    private static final String MANAGEDLOCATIONURI = "managedLocationUri";

    private static final String CONNECTORREAD = "CONNECTORREAD";
    private static final String CONNECTORWRITE = "CONNECTORWRITE";
    public static final String IMPALA_ENGINE = "impala";


    private static List<String> processorCapabilities = Lists.newArrayList();

    /**
     * Wrapper around IMetaStoreClient.alter_table with validWriteIds as a param.
     */
    public static void alterTableWithTransaction(IMetaStoreClient client,
                                                 Table tbl, TblTransaction tblTxn)
            throws ImpalaRuntimeException {
        try {
            client.alter_table(null, tbl.getDbName(), tbl.getTableName(),
                    tbl, null, tblTxn.validWriteIds);
        } catch (TException e) {
            throw new ImpalaRuntimeException(
                    String.format(HMS_RPC_ERROR_FORMAT_STR, "alter_table"), e);
        }
    }

    /**
     * Wrapper around IMetaStoreClient.alter_partitions with transaction information
     */
    public static void alterPartitionsWithTransaction(IMetaStoreClient client,
                                                      String dbName, String tblName, List<Partition> partitions, TblTransaction tblTxn
    ) throws InvalidOperationException, MetaException, TException {
        for (Partition part : partitions) {
            part.setWriteId(tblTxn.writeId);
        }
        // Correct validWriteIdList is needed
        // to commit the alter partitions operation in hms side.
        client.alter_partitions(dbName, tblName, partitions, null,
                tblTxn.validWriteIds, tblTxn.writeId);
    }

    /**
     * Wrapper around IMetaStoreClient.getTableColumnStatistics() to deal with added
     * arguments.
     */
    public static List<ColumnStatisticsObj> getTableColumnStatistics(
            IMetaStoreClient client, String dbName, String tableName, List<String> colNames)
            throws NoSuchObjectException, MetaException, TException {
        return client.getTableColumnStatistics(dbName, tableName, colNames,/*engine*/IMPALA_ENGINE);
    }

    /**
     * Wrapper around IMetaStoreClient.deleteTableColumnStatistics() to deal with added
     * arguments.
     */
    public static boolean deleteTableColumnStatistics(IMetaStoreClient client,
                                                      String dbName, String tableName, String colName)
            throws NoSuchObjectException, MetaException, InvalidObjectException, TException,
            InvalidInputException {
        return client.deleteTableColumnStatistics(dbName, tableName, colName,/*engine*/IMPALA_ENGINE);
    }

    /**
     * Wrapper around ColumnStatistics c'tor to deal with the added engine property.
     */
    public static ColumnStatistics createNewHiveColStats() {
        ColumnStatistics colStats = new ColumnStatistics();
        colStats.setEngine(IMPALA_ENGINE);
        return colStats;
    }

    private static final MessageEncoder eventMessageEncoder_ =
            MessageFactory.getDefaultInstance(MetastoreConf.newMetastoreConf());

    /**
     * Wrapper method which returns HMS-3 Message factory in case Impala is
     * building against Apache Hive-3
     */
    public static MessageDeserializer getMessageDeserializer() {
        return eventMessageEncoder_.getDeserializer();
    }

    public static MessageSerializer getMessageSerializer() {
        return eventMessageEncoder_.getSerializer();
    }

    /**
     * Wrapper around FileUtils.makePartName to deal with package relocation in Hive 3.
     * This method uses the metastore's FileUtils method instead of one from hive-exec
     *
     * @param partitionColNames
     * @param values
     * @return
     */
    public static String makePartName(List<String> partitionColNames, List<String> values) {
        return FileUtils.makePartName(partitionColNames, values);
    }

    /**
     * Wrapper method around message factory's build alter table message due to added
     * arguments in hive 3.
     */
    @VisibleForTesting
    public static AlterTableMessage buildAlterTableMessage(Table before, Table after,
                                                           boolean isTruncateOp, long writeId) {
        return MessageBuilder.getInstance().buildAlterTableMessage(before, after,
                isTruncateOp, writeId);
    }

    /**
     * Wrapper around HMS-3 message serializer
     *
     * @param message
     * @return serialized string to use used in the NotificationEvent's message field
     */
    @VisibleForTesting
    public static String serializeEventMessage(EventMessage message) {
        return message.toString();
    }

    /**
     * Get valid write ids from HMS for the acid table
     *
     * @param client        the client to access HMS
     * @param tableFullName the name for the table
     * @return ValidWriteIdList object
     */
    public static ValidWriteIdList fetchValidWriteIds(IMetaStoreClient client,
                                                      String tableFullName) throws TException {
        // fix HIVE-20929
        ValidTxnList txns = client.getValidTxns();
        List<String> tablesList = Collections.singletonList(tableFullName);
        List<TableValidWriteIds> writeIdList = client
                .getValidWriteIds(tablesList, txns.toString());
        return TxnCommonUtils.createValidReaderWriteIdList(writeIdList.get(0));
    }

    /**
     * Wrapper around HMS Partition object to get writeID
     * WriteID is introduced in ACID 2
     * It is used to detect changes of the partition
     */
    public static long getWriteIdFromMSPartition(Partition partition) {
        Preconditions.checkNotNull(partition);
        return NumberUtils.toLong(partition.getParameters().get(WRITEID), -1);
    }

    /**
     * Wrapper around HMS Partition object to set writeID
     * WriteID is introduced in ACID 2
     * It is used to detect changes of the partition
     */
    public static void setWriteIdToMSPartition(Partition partition, long writeId) {
        Preconditions.checkNotNull(partition);
        partition.getParameters().put(WRITEID, String.valueOf(writeId));
    }

    /**
     * Wrapper around HMS Table object to get writeID
     * Per table writeId is introduced in ACID 2
     * It is used to detect changes of the table
     */
    public static long getWriteIdFromMSTable(Table msTbl) {
        Preconditions.checkNotNull(msTbl);
        return NumberUtils.toLong(msTbl.getParameters().get(WRITEID), -1);
    }

    /**
     * Set impala capabilities to check the accessType of the table created by hive
     * Impala supports:
     * - external table read/write
     * - insert-only Acid table read
     * - virtual view read
     * - materialized view read
     */
    public static synchronized void setHiveClientCapabilities() {
        if (capabilitiestSet_) return;
        String[] capabilities = new String[]{
                EXTWRITE, // External table write
                EXTREAD,  // External table read
                HIVEMANAGEDINSERTREAD, // Insert-only table read
                HIVEMANAGEDINSERTWRITE, // Insert-only table write
                HIVEFULLACIDREAD,
                HIVEFULLACIDWRITE,
                HIVESQL,
                HIVEMQT,
                HIVEBUCKET2 // Includes the capability to get the correct bucket number.
                // Currently, without this capability, for an external bucketed
                // table, Hive will return the table as Read-only with bucket
                // number -1. It makes clients unable to know it is a bucketed table.
                // TODO: will remove this capability when Hive can provide
                // API calls to tell the changing of bucket number.
        };
        processorCapabilities = Lists.newArrayList(capabilities);
        capabilitiestSet_ = true;
    }

    /**
     * Check if a table has a capability
     *
     * @param msTbl              hms table
     * @param requiredCapability hive access types or combination of them
     * @return true if the table has the capability
     */
    public static boolean hasTableCapability(Table msTbl, byte requiredCapability) {
        Preconditions.checkNotNull(msTbl);
        // access types in binary:
        // ACCESSTYPE_NONE:      00000001
        // ACCESSTYPE_READONLY:  00000010
        // ACCESSTYPE_WRITEONLY: 00000100
        // ACCESSTYPE_READWRITE: 00001000
        return requiredCapability != ACCESSTYPE_NONE
                && ((getAccessType(msTbl) & requiredCapability) != 0);
    }

    /**
     * Get Access type in string
     *
     * @param msTbl hms table
     * @return the string represents the table access type.
     */
    public static String getTableAccessType(Table msTbl) {
        Preconditions.checkNotNull(msTbl);
        switch (getAccessType(msTbl)) {
            case ACCESSTYPE_READONLY:
                return "READONLY";
            case ACCESSTYPE_WRITEONLY:
                return "WRITEONLY";
            case ACCESSTYPE_READWRITE:
                return "READWRITE";
            case ACCESSTYPE_NONE:
            default:
                return "NONE";
        }
    }

    /**
     * Set table access type. This is useful for hms Table object constructed for create
     * table statement. For example, to create a table, we need Read/Write capabilities
     * not default 0(not defined)
     */
    public static void setTableAccessType(Table msTbl, byte accessType) {
        Preconditions.checkNotNull(msTbl);
        msTbl.getParameters().put(ACCESSTYPE, String.valueOf(accessType));
    }

    /**
     * CDP Hive-3 only function
     */
    public static void setTableColumnStatsTransactional(IMetaStoreClient client,
                                                        Table msTbl, ColumnStatistics colStats, TblTransaction tblTxn)
            throws ImpalaRuntimeException {
        List<ColumnStatistics> colStatsList = new ArrayList<>();
        colStatsList.add(colStats);
        SetPartitionsStatsRequest request = new SetPartitionsStatsRequest();
        request.setColStats(colStatsList);
        request.setWriteId(tblTxn.writeId);
        request.setValidWriteIdList(tblTxn.validWriteIds);
        request.setEngine(/*engine*/IMPALA_ENGINE);
        try {
            client.setPartitionColumnStatistics(request);
        } catch (TException e) {
            throw new ImpalaRuntimeException(
                    String.format(HMS_RPC_ERROR_FORMAT_STR, "setPartitionColumnStatistics"), e);
        }
    }

    /**
     * Fire insert events for table and partition.
     * In case of any exception, we just log the failure of firing insert events.
     */
    public static List<Long> fireInsertEvents(MetaStoreClient msClient,
                                              TableInsertEventInfo insertEventInfo, String dbName, String tableName) {
        Stopwatch sw = Stopwatch.createStarted();
        try {
            if (insertEventInfo.isTransactional()) {
                // if the table is transactional we use a different API to fire these
                // events. Note that we don't really need the event ids here for self-event
                // detection since these events are not fetched by the EventsProcessor later
                // These events are mostly required for incremental replication in Hive
                fireInsertTransactionalEventHelper(msClient.getHiveClient(),
                        insertEventInfo, dbName, tableName);
            } else {
                return fireInsertEventHelper(msClient.getHiveClient(),
                        insertEventInfo.getInsertEventReqData(),
                        insertEventInfo.getInsertEventPartVals(), dbName,
                        tableName);
            }
        } catch (Exception e) {
            LOG.error("Failed to fire insert event. Some tables might not be"
                    + " refreshed on other impala clusters.", e);
        } finally {
            LOG.info("Time taken to fire insert events on table {}.{}: {} msec", dbName,
                    tableName, sw.stop().elapsed(TimeUnit.MILLISECONDS));
            msClient.close();
        }
        return Collections.emptyList();
    }

    /**
     * CDP Hive-3 only function
     * Fires a listener event of the type ACID_WRITE on a transactional table in metastore.
     * This event is polled by other external systems to detect insert operations into
     * ACID tables.
     *
     * @throws TException in case of errors during HMS API call.
     */
    private static void fireInsertTransactionalEventHelper(
            IMetaStoreClient hiveClient, TableInsertEventInfo insertEventInfo, String dbName,
            String tableName) throws TException {
        for (InsertEventRequestData insertData : insertEventInfo.getInsertEventReqData()) {
            if (LOG.isDebugEnabled()) {
                String msg =
                        "Firing write notification log request for table " + dbName + "." + tableName
                                + (insertData.isSetPartitionVal() ? " on partition " + insertData
                                .getPartitionVal() : "");
                LOG.debug(msg);
            }
            WriteNotificationLogRequest rqst = new WriteNotificationLogRequest(
                    insertEventInfo.getTxnId(), insertEventInfo.getWriteId(), dbName, tableName,
                    insertData);
            if (insertData.isSetPartitionVal()) {
                rqst.setPartitionVals(insertData.getPartitionVal());
            }
            hiveClient.addWriteNotificationLog(rqst);
        }
    }

    /**
     * Fires an insert event to HMS notification log. In Hive-3 for partitioned table,
     * all partition insert events will be fired by a bulk API.
     *
     * @param msClient               Metastore client,
     * @param insertEventDataList    A list of insert event info encapsulating the information
     *                               needed to fire insert events.
     * @param insertEventPartValList The partition list corresponding to
     *                               insertEventDataList, used by Apache Hive 3
     * @param dbName
     * @param tableName
     * @return a list of eventIds for the insert events
     */
    @VisibleForTesting
    public static List<Long> fireInsertEventHelper(IMetaStoreClient msClient,
                                                   List<InsertEventRequestData> insertEventDataList,
                                                   List<List<String>> insertEventPartValList, String dbName, String tableName)
            throws TException {
        Preconditions.checkNotNull(msClient);
        Preconditions.checkNotNull(dbName);
        Preconditions.checkNotNull(tableName);
        Preconditions.checkState(!insertEventDataList.isEmpty(), "Atleast one insert event "
                + "info must be provided.");
        Preconditions.checkState(insertEventDataList.size() == insertEventPartValList.size());
        LOG.debug(String.format(
                "Firing %s insert event(s) for %s.%s", insertEventDataList.size(), dbName,
                tableName));
        for (int i = 0; i < insertEventDataList.size(); i++) {
            InsertEventRequestData insertEventData = insertEventDataList.get(i);
            List<String> partitionVals = insertEventPartValList.get(i);
            FireEventRequestData data = new FireEventRequestData();
            FireEventRequest rqst = new FireEventRequest(true, data);
            rqst.setDbName(dbName);
            rqst.setTableName(tableName);
            if (partitionVals != null && !partitionVals.isEmpty()) {
                rqst.setPartitionVals(partitionVals);
            }
            data.setInsertData(insertEventData);
            msClient.fireListenerEvent(rqst);
        }
        //TODO: IMPALA-8632: Add support for self-event detection for insert events
        return Collections.EMPTY_LIST;
    }

    /**
     * CDP Hive-3 only function.
     */
    @VisibleForTesting
    public static List<Long> fireReloadEventHelper(MetaStoreClient msClient,
                                                   boolean isRefresh, List<String> partVals, String dbName, String tableName,
                                                   Map<String, String> selfEventParams) throws TException {
        throw new UnsupportedOperationException("Reload event is not supported.");
    }

    /**
     * CDP Hive-3 only function.
     */
    public static Map<String, Object> getFieldsFromReloadEvent(NotificationEvent event)
            throws MetastoreNotificationException {
        throw new UnsupportedOperationException("Reload event is not supported.");
    }

    /**
     * CDP Hive-3 only function.
     */
    public static String getPartitionNameFromCommitCompactionEvent(
            NotificationEvent event) {
        throw new UnsupportedOperationException("CommitCompaction event is not supported.");
    }

    /**
     * Use thrift API directly instead of HiveMetastoreClient#getNextNotification because
     * the HMS client can throw an IllegalStateException when there is a gap between the
     * eventIds returned.
     *
     * @param msClient          Metastore client
     * @param eventRequest      Notification event request
     * @param eventTypeSkipList unused
     * @return NotificationEventResponse
     * @throws TException
     * @see org.apache.hadoop.hive.metastore.HiveMetaStoreClient#getNextNotification
     */
    public static NotificationEventResponse getNextNotification(IMetaStoreClient msClient,
                                                                NotificationEventRequest eventRequest, List<String> eventTypeSkipList)
            throws TException {
        return getThriftClient(msClient).get_next_notification(eventRequest);
    }

    private static ThriftHiveMetastore.Client getThriftClient(IMetaStoreClient msClient)
            throws MetaException {
        try {
            // The HMS client does not expose the getThriftClient function, which is obtained
            // by reflection here.
            if (Proxy.isProxyClass(msClient.getClass())) {
                RetryingMetaStoreClient handler =
                        (RetryingMetaStoreClient) Proxy.getInvocationHandler(msClient);
                msClient = (IMetaStoreClient) FieldUtils.readField(handler, "base",
                        true);
            }
            Object client = FieldUtils.readField(msClient, "client", true);
            if (client == null) {
                throw new MetaException("Client is not initialized");
            }
            if (!(client instanceof ThriftHiveMetastore.Client)) {
                throw new MetaException("getThriftClient is only supported in remote metastore "
                        + "mode.");
            }
            return (Client) client;
        } catch (IllegalAccessException e) {
            throw new MetaException("getThriftClient() fail: " + e.getMessage());
        }
    }

    /**
     * Wrapper around Database.setManagedLocationUri() to deal with added arguments.
     */
    public static void setManagedLocationUri(Database db, String managedLocation) {
        db.getParameters().put(MANAGEDLOCATIONURI, managedLocation);
    }

    /**
     * Wrapper around HMS Database object to get managedLocationUri.
     */
    public static String getManagedLocationUri(Database db) {
        if (db.getParameters().containsKey(MANAGEDLOCATIONURI)) {
            return db.getParameters().get(MANAGEDLOCATIONURI);
        }
        return null;
    }

    /**
     * Set the default table path for a new table.
     */
    public static void setTableLocation(Db db, Table tbl) throws ImpalaRuntimeException {
        Preconditions.checkNotNull(db);
        Preconditions.checkNotNull(tbl);
        // Set a valid location of this table using the same rules as the cdp metastore,
        // unless the user specified a path.
        try {
            if (tbl.getSd().getLocation() == null || tbl.getSd().getLocation().isEmpty()) {
                tbl.getSd().setLocation(
                        MetastoreShim.getPathForNewTable(db.getMetaStoreDb(), tbl));
            }
        } catch (MetaException e) {
            throw new ImpalaRuntimeException("setTableLocation", e);
        }
    }

    /**
     * Wrapper around HMS Table object to get table id.
     */
    public static long getTableId(Table tbl) {
        return NumberUtils.toLong(tbl.getParameters().get(ID), -1);
    }

    /**
     * Wrapper around JSONDropDatabaseMessage to get database.
     */
    public static Database getDatabaseObject(JSONDropDatabaseMessage dropDatabaseMessage) {
        Database database = new Database();
        database.setName(dropDatabaseMessage.getDB());
        return database;
    }

    /**
     * Wrapper around IMetaStoreClient.truncateTable() to deal with added arguments.
     */
    public static void truncateTable(IMetaStoreClient msClient, String dbName,
                                     String tableName, List<String> partNames,
                                     String validWriteIds, long writeId) throws TException {
        msClient.truncateTable(dbName, tableName, partNames);
    }

    /**
     * Wrapper around IMetaStoreClient.listPartitions() to deal with added arguments.
     */
    public static List<Partition> getPartitions(IMetaStoreClient msClient,
                                                String testDbName, String testTblName) throws TException {
        return msClient.listPartitions(testDbName, testTblName, (short) -1);
    }

    /**
     * CDP Hive-3 only function.
     * For compatibility, fireInsertEventHelper() added the partitionValList parameter
     */
    public static void setPartitionVal(InsertEventRequestData insertEventRequestData,
                                       List<String> partVals) {
    }

    /**
     * CDP Hive-3 only function.
     */
    public static void addToSubDirectoryList(InsertEventRequestData insertEventRequestData,
                                             String acidDirPath) {
        insertEventRequestData.addToSubDirectoryList(acidDirPath);
    }

    /**
     * CDP Hive-3 only function.
     */
    public static List<HdfsPartition.Builder> getPartitionsForRefreshingFileMetadata(
            CatalogServiceCatalog catalog, HdfsTable hdfsTable) throws CatalogException {
        List<HdfsPartition.Builder> partBuilders = new ArrayList<>();
        GetLatestCommittedCompactionInfoRequest request =
                new GetLatestCommittedCompactionInfoRequest(
                        hdfsTable.getDb().getName(), hdfsTable.getName());
        if (hdfsTable.getLastCompactionId() > 0) {
            request.setLastCompactionId(hdfsTable.getLastCompactionId());
        }

        GetLatestCommittedCompactionInfoResponse response;
        try (MetaStoreClientPool.MetaStoreClient client = catalog.getMetaStoreClient()) {
            response = CompactionInfoLoader.getLatestCompactionInfo(client, request);
        } catch (Exception e) {
            throw new CatalogException("Error getting latest compaction info for "
                    + hdfsTable.getFullName(), e);
        }

        Map<String, Long> partNameToCompactionId = new HashMap<>();
        if (hdfsTable.isPartitioned()) {
            for (CompactionInfoStruct ci : response.getCompactions()) {
                if (ci.getPartitionname() != null) {
                    partNameToCompactionId.put(ci.getPartitionname(), ci.getId());
                } else {
                    LOG.warn(
                            "Partitioned table {} has null partitionname in CompactionInfoStruct: {}",
                            hdfsTable.getFullName(), ci.toString());
                }
            }
        } else {
            CompactionInfoStruct ci = Iterables.getOnlyElement(response.getCompactions(), null);
            if (ci != null) {
                partNameToCompactionId.put(HdfsTable.DEFAULT_PARTITION_NAME, ci.getId());
            }
        }

        for (HdfsPartition partition : hdfsTable.getPartitionsForNames(
                partNameToCompactionId.keySet())) {
            long latestCompactionId = partNameToCompactionId.get(partition.getPartitionName());
            HdfsPartition.Builder builder = new HdfsPartition.Builder(partition);
            LOG.debug(
                    "Cached compaction id for {} partition {}: {} but the latest compaction id: {}",
                    hdfsTable.getFullName(), partition.getPartitionName(),
                    partition.getLastCompactionId(), latestCompactionId);
            builder.setLastCompactionId(latestCompactionId);
            partBuilders.add(builder);
        }
        return partBuilders;
    }

    /**
     * CDP Hive-3 only function.
     */
    public static Map<String, Long> getLatestCompactions(MetaStoreClient client,
                                                         String dbName, String tableName, List<String> partitionNames,
                                                         String unPartitionedName, long lastCompactionId) throws TException {
        throw new UnsupportedOperationException("getLatestCompactions is not supported.");
    }

    /**
     * CDP Hive-3 only function.
     */
    public static List<PartitionRef> checkLatestCompaction(MetaStoreClientPool msClientPool,
                                                           String dbName, String tableName, TableMetaRef table,
                                                           Map<PartitionRef, PartitionMetadata> metas, String unPartitionedName)
            throws TException {
        throw new UnsupportedOperationException("checkLatestCompaction is not supported.");
    }

    /**
     * CDP Hive-3 only function.
     */
    public static ICatalogMetastoreServer getCatalogMetastoreServer(
            CatalogOpExecutor catalogOpExecutor) {
        throw new UnsupportedOperationException(
                "getCatalogMetastoreServer is not supported.");
    }

    /**
     * CDP Hive-3 only function.
     */
    public static class CommitTxnEvent extends MetastoreEvent {
        private final CommitTxnMessage commitTxnMessage_;
        private final long txnId_;

        public CommitTxnEvent(CatalogOpExecutor catalogOpExecutor, Metrics metrics,
                              NotificationEvent event) {
            super(catalogOpExecutor, metrics, event);
            Preconditions.checkState(getEventType().equals(MetastoreEventType.COMMIT_TXN));
            Preconditions.checkNotNull(event.getMessage());
            commitTxnMessage_ = MetastoreEventsProcessor.getMessageDeserializer()
                    .getCommitTxnMessage(event.getMessage());
            txnId_ = commitTxnMessage_.getTxnId();
        }

        @Override
        protected void process() throws MetastoreNotificationException {
            Set<TableWriteId> committedWriteIds = catalog_.removeWriteIds(txnId_);
            List<WriteEventInfo> writeEventInfoList;
            try (MetaStoreClientPool.MetaStoreClient client = catalog_.getMetaStoreClient()) {
                writeEventInfoList = client.getHiveClient().getAllWriteEventInfo(
                        new GetAllWriteEventInfoRequest(txnId_));
            } catch (TException e) {
                throw new MetastoreNotificationNeedsInvalidateException(debugString("Failed to "
                        + "get write event infos for txn {}. Event processing cannot continue. Issue "
                        + "an invalidate metadata command to reset event processor.", txnId_), e);
            }

            try {
                if (writeEventInfoList != null && !writeEventInfoList.isEmpty()) {
                    commitTxnMessage_.addWriteEventInfo(writeEventInfoList);
                    addCommittedWriteIdsAndRefreshPartitions();
                }
                addCommittedWriteIdsToTables(committedWriteIds);
            } catch (Exception e) {
                throw new MetastoreNotificationNeedsInvalidateException(debugString("Failed to "
                        + "mark committed write ids and refresh partitions for txn {}. Event "
                        + "processing cannot continue. Issue an invalidate metadata command to reset "
                        + "event processor.", txnId_), e);
            }
        }

        private void addCommittedWriteIdsToTables(Set<TableWriteId> tableWriteIds)
                throws CatalogException {
            for (TableWriteId tableWriteId : tableWriteIds) {
                catalog_.addWriteIdsToTable(tableWriteId.getDbName(), tableWriteId.getTblName(),
                        getEventId(),
                        Collections.singletonList(tableWriteId.getWriteId()),
                        MutableValidWriteIdList.WriteIdStatus.COMMITTED);
            }
        }

        private void addCommittedWriteIdsAndRefreshPartitions() throws Exception {
            Preconditions.checkNotNull(commitTxnMessage_.getWriteIds());
            List<Long> writeIds = Collections.unmodifiableList(commitTxnMessage_.getWriteIds());
            List<Partition> parts = new ArrayList<>();
            Map<TableName, List<Integer>> tableNameToIdxs = new HashMap<>();
            for (int i = 0; i < writeIds.size(); i++) {
                org.apache.hadoop.hive.metastore.api.Table tbl = commitTxnMessage_.getTableObj(i);
                TableName tableName = new TableName(tbl.getDbName(), tbl.getTableName());
                parts.add(commitTxnMessage_.getPartitionObj(i));
                tableNameToIdxs.computeIfAbsent(tableName, k -> new ArrayList<>()).add(i);
            }
            for (Map.Entry<TableName, List<Integer>> entry : tableNameToIdxs.entrySet()) {
                org.apache.hadoop.hive.metastore.api.Table tbl =
                        commitTxnMessage_.getTableObj(entry.getValue().get(0));
                List<Long> writeIdsForTable = entry.getValue().stream()
                        .map(i -> writeIds.get(i))
                        .collect(Collectors.toList());
                List<Partition> partsForTable = entry.getValue().stream()
                        .map(i -> parts.get(i))
                        .collect(Collectors.toList());
                if (tbl.getPartitionKeysSize() > 0
                        && !MetaStoreUtils.isMaterializedViewTable(tbl)) {
                    try {
                        catalogOpExecutor_.addCommittedWriteIdsAndReloadPartitionsIfExist(
                                getEventId(), entry.getKey().getDb(), entry.getKey().getTbl(),
                                writeIdsForTable, partsForTable, "Processing event id: " +
                                        getEventId() + ", event type: " + getEventType());
                    } catch (TableNotLoadedException e) {
                        debugLog("Ignoring reloading since table {} is not loaded",
                                entry.getKey());
                    } catch (DatabaseNotFoundException | TableNotFoundException e) {
                        debugLog("Ignoring reloading since table {} is not found",
                                entry.getKey());
                    }
                } else {
                    catalog_.reloadTableIfExists(entry.getKey().getDb(), entry.getKey().getTbl(),
                            "CommitTxnEvent", getEventId(), true);
                }
            }
        }

        @Override
        protected boolean onFailure(Exception e) {
            return false;
        }

        @Override
        protected boolean isEventProcessingDisabled() {
            return false;
        }

        @Override
        protected SelfEventContext getSelfEventContext() {
            return null;
        }

        @Override
        protected boolean shouldSkipWhenSyncingToLatestEventId() {
            return false;
        }
    }

    /**
     * Get Access type in byte from table property
     *
     * @param msTbl hms table
     * @return the table access type.
     */
    private static byte getAccessType(Table msTbl) {
        Preconditions.checkNotNull(msTbl);
        byte accessType = ACCESSTYPE_NONE;
        Map<String, String> params = msTbl.getParameters();
        if (params.containsKey(ACCESSTYPE)) {
            String accessTypeStr = msTbl.getParameters().get(ACCESSTYPE);
            accessType = accessTypeStr.getBytes()[0];
        } else { // Table not created by Impala
            if (!capabilitiestSet_) setHiveClientCapabilities();
            // The following logic comes from hive's MetastoreDefaultTransformer.transform()
            String tableType = msTbl.getTableType();
            String tCapabilities = params.get(CatalogOpExecutor.CAPABILITIES_KEY);
            int numBuckets = msTbl.isSetSd() ? msTbl.getSd().getNumBuckets() : 0;
            boolean isBucketed = numBuckets > 0;

            LOG.info("Table " + msTbl.getTableName() + ",#bucket=" + numBuckets + ",isBucketed:"
                    + isBucketed + ",tableType=" + tableType + ",tableCapabilities="
                    + tCapabilities);

            // if the table has no tCapabilities
            if (tCapabilities == null) {
                LOG.debug("Table has no specific required capabilities");

                switch (tableType) {
                    case "EXTERNAL_TABLE":
                        if (numBuckets > 0) {
                            if (processorCapabilities.contains(HIVEBUCKET2)) {
                                LOG.debug("External bucketed table with HB2 capability:RW");
                                accessType = ACCESSTYPE_READWRITE;
                            } else {
                                LOG.debug("External bucketed table without HB2 capability:RO");
                                accessType = ACCESSTYPE_READONLY;
                            }
                        } else { // Unbucketed
                            if (processorCapabilities.contains(EXTWRITE) && processorCapabilities
                                    .contains(EXTREAD)) {
                                LOG.debug("External unbucketed table with EXTREAD/WRITE capability:RW");
                                accessType = ACCESSTYPE_READWRITE;
                            } else if (processorCapabilities.contains(EXTREAD)) {
                                LOG.debug("External unbucketed table with EXTREAD capability:RO");
                                accessType = ACCESSTYPE_READONLY;
                            } else {
                                LOG.debug(
                                        "External unbucketed table without EXTREAD/WRITE capability:NONE");
                                accessType = ACCESSTYPE_NONE;
                            }
                        }
                        break;
                    case "MANAGED_TABLE":
                        String txnal = params.get(AcidUtils.TABLE_IS_TRANSACTIONAL);
                        if (txnal == null || txnal
                                .equalsIgnoreCase("FALSE")) { // non-ACID MANAGED table
                            LOG.debug("Managed non-acid table:RW");
                            accessType = ACCESSTYPE_READWRITE;
                        }

                        if (txnal != null && txnal.equalsIgnoreCase("TRUE")) { // ACID table
                            String txntype = params.get(AcidUtils.TABLE_TRANSACTIONAL_PROPERTIES);
                            if (txntype != null && txntype
                                    .equalsIgnoreCase("insert_only")) { // MICRO_MANAGED Tables
                                // MGD table is insert only, not full ACID
                                if (processorCapabilities.contains(HIVEMANAGEDINSERTWRITE)
                                        || processorCapabilities.contains(CONNECTORWRITE)) {
                                    LOG.debug("Managed acid table with INSERTWRITE or CONNECTORWRITE "
                                            + "capability:RW");
                                    // clients have RW access to INSERT-ONLY ACID tables
                                    accessType = ACCESSTYPE_READWRITE;
                                    LOG.info("Processor has one of the write capabilities on insert-only, "
                                            + "granting RW");
                                } else if (processorCapabilities.contains(HIVEMANAGEDINSERTREAD)
                                        || processorCapabilities.contains(CONNECTORREAD)) {
                                    LOG.debug("Managed acid table with INSERTREAD or CONNECTORREAD "
                                            + "capability:RO");
                                    // clients have RO access to INSERT-ONLY ACID tables
                                    accessType = ACCESSTYPE_READONLY;
                                    LOG.info("Processor has one of the read capabilities on insert-only, "
                                            + "granting RO");
                                } else {
                                    // clients have NO access to INSERT-ONLY ACID tables
                                    accessType = ACCESSTYPE_NONE;
                                    LOG.info("Processor has no read or write capabilities on insert-only, "
                                            + "NO access");
                                }
                            } else { // FULL ACID MANAGED TABLE
                                if (processorCapabilities.contains(HIVEFULLACIDWRITE)
                                        || processorCapabilities.contains(CONNECTORWRITE)) {
                                    LOG.debug(
                                            "Full acid table with ACIDWRITE or CONNECTORWRITE capability:RW");
                                    // clients have RW access to IUD ACID tables
                                    accessType = ACCESSTYPE_READWRITE;
                                } else if (processorCapabilities.contains(HIVEFULLACIDREAD)
                                        || processorCapabilities.contains(CONNECTORREAD)) {
                                    LOG.debug(
                                            "Full acid table with ACIDREAD or CONNECTORREAD capability:RO");
                                    // clients have RO access to IUD ACID tables
                                    accessType = ACCESSTYPE_READONLY;
                                } else {
                                    LOG.debug("Full acid table without ACIDREAD/WRITE or "
                                            + "CONNECTORREAD/WRITE capability:NONE");
                                    // clients have NO access to IUD ACID tables
                                    accessType = ACCESSTYPE_NONE;
                                }
                            }
                        }
                        break;
                    case "VIRTUAL_VIEW":
                        if (processorCapabilities.contains(HIVESQL) ||
                                processorCapabilities.contains(CONNECTORREAD)) {
                            accessType = ACCESSTYPE_READONLY;
                        } else {
                            accessType = ACCESSTYPE_NONE;
                        }
                        break;
                    case "MATERIALIZED_VIEW":
                        if ((processorCapabilities.contains(CONNECTORREAD) ||
                                processorCapabilities.contains(HIVEFULLACIDREAD)) && processorCapabilities
                                .contains(HIVEMQT)) {
                            LOG.info(
                                    "Processor has one of the READ abilities and HIVEMQT, AccessType=RO");
                            accessType = ACCESSTYPE_READONLY;
                        } else {
                            LOG.info("Processor has no READ abilities or HIVEMQT, AccessType=None");
                            accessType = ACCESSTYPE_NONE;
                        }
                        break;
                    default:
                        accessType = ACCESSTYPE_NONE;
                        break;
                }
                return accessType;
            }

            // WITH CAPABLITIES ON TABLE
            tCapabilities = tCapabilities.replaceAll("\\s", "")
                    .toUpperCase(); // remove spaces between tCapabilities + toUppercase
            List<String> requiredCapabilities = Arrays.asList(tCapabilities.split(","));
            switch (tableType) {
                case "EXTERNAL_TABLE":
                    if (processorCapabilities.containsAll(requiredCapabilities)) {
                        // AccessType is RW
                        LOG.info(
                                "Abilities for match: Table type=" + tableType + ",accesstype is RW");
                        accessType = ACCESSTYPE_READWRITE;
                        break;
                    }

                    if (requiredCapabilities.contains(EXTWRITE) && processorCapabilities
                            .contains(EXTWRITE)) {
                        if (!isBucketed) {
                            LOG.info("EXTWRITE Matches, accessType=" + ACCESSTYPE_READWRITE);
                            accessType = ACCESSTYPE_READWRITE;
                            return accessType;
                        }
                    }

                    if (requiredCapabilities.contains(EXTREAD) && processorCapabilities
                            .contains(EXTREAD)) {
                        LOG.info("EXTREAD Matches, accessType=" + ACCESSTYPE_READONLY);
                        accessType = ACCESSTYPE_READONLY;
                    } else {
                        LOG.debug("No matches, accessType=" + ACCESSTYPE_NONE);
                        accessType = ACCESSTYPE_NONE;
                    }
                    break;
                case "MANAGED_TABLE":
                    if (processorCapabilities.size() == 0) { // processor has no capabilities
                        LOG.info("Client has no capabilities for type " + tableType
                                + ",accesstype is NONE");
                        accessType = ACCESSTYPE_NONE;
                        return accessType;
                    }

                    if (processorCapabilities.containsAll(requiredCapabilities)) {
                        // AccessType is RW
                        LOG.info(
                                "Abilities for match: Table type=" + tableType + ",accesstype is RW");
                        accessType = ACCESSTYPE_READWRITE;
                        return accessType;
                    }

                    String txnal = params.get(AcidUtils.TABLE_IS_TRANSACTIONAL);
                    if (txnal == null || txnal
                            .equalsIgnoreCase("FALSE")) { // non-ACID MANAGED table
                        LOG.info("Table is non ACID, accesstype is RO");
                        accessType = ACCESSTYPE_READONLY;
                        return accessType;
                    }

                    if (txnal != null && txnal.equalsIgnoreCase("TRUE")) { // ACID table
                        String txntype = params.get(AcidUtils.TABLE_TRANSACTIONAL_PROPERTIES);
                        List<String> hintList = new ArrayList<>();
                        if (txntype != null && txntype
                                .equalsIgnoreCase("insert_only")) { // MICRO_MANAGED Tables
                            LOG.info("Table is INSERTONLY ACID");
                            // MGD table is insert only, not full ACID
                            if (processorCapabilities.containsAll(getWrites(requiredCapabilities))
                                    // contains all writes on table
                                    || processorCapabilities.contains(HIVEFULLACIDWRITE)) {
                                LOG.info("Processor has all writes or full acid write, access is RW");
                                // clients have RW access to INSERT-ONLY ACID tables
                                accessType = ACCESSTYPE_READWRITE;
                                return accessType;
                            }

                            if (processorCapabilities.contains(CONNECTORWRITE)) {
                                LOG.debug("Managed acid table with CONNECTORWRITE capability:RW");
                                // clients have RW access to INSERT-ONLY ACID tables with CONNWRITE
                                accessType = ACCESSTYPE_READWRITE;
                                return accessType;
                            } else if (processorCapabilities.containsAll(getReads(requiredCapabilities))
                                    || processorCapabilities.contains(HIVEMANAGEDINSERTREAD)) {
                                LOG.debug("Managed acid table with MANAGEDREAD capability:RO");
                                accessType = ACCESSTYPE_READONLY;
                                return accessType;
                            } else if (processorCapabilities.contains(CONNECTORREAD)) {
                                LOG.debug("Managed acid table with CONNECTORREAD capability:RO");
                                accessType = ACCESSTYPE_READONLY;
                                return accessType;
                            } else {
                                LOG.debug("Managed acid table without any READ capability:NONE");
                                accessType = ACCESSTYPE_NONE;
                                return accessType;
                            }
                        } else { // MANAGED FULL ACID TABLES
                            LOG.info("Table is FULLACID");
                            if (processorCapabilities.containsAll(getWrites(requiredCapabilities))
                                    // contains all writes on table
                                    || processorCapabilities.contains(HIVEFULLACIDWRITE)) {
                                LOG.info("Processor has all writes or atleast " + HIVEFULLACIDWRITE
                                        + ", access is RW");
                                // clients have RW access to ACID tables
                                accessType = ACCESSTYPE_READWRITE;
                                return accessType;
                            }

                            if (processorCapabilities.contains(CONNECTORWRITE)) {
                                LOG.debug("Full acid table with CONNECTORWRITE capability:RW");
                                // clients have RW access to IUD ACID tables
                                accessType = ACCESSTYPE_READWRITE;
                                return accessType;
                            } else if (processorCapabilities.contains(HIVEFULLACIDREAD)
                                    || (processorCapabilities.contains(CONNECTORREAD))) {
                                LOG.debug("Full acid table with CONNECTORREAD/ACIDREAD capability:RO");
                                // clients have RO access to IUD ACID tables
                                accessType = ACCESSTYPE_READONLY;
                                return accessType;
                            } else {
                                LOG.debug("Full acid table without READ capability:RO");
                                // clients have NO access to IUD ACID tables
                                accessType = ACCESSTYPE_NONE;
                                return accessType;
                            }
                        }
                    }
                    break;
                case "VIRTUAL_VIEW":
                case "MATERIALIZED_VIEW":
                    if (processorCapabilities.containsAll(requiredCapabilities)) {
                        accessType = ACCESSTYPE_READONLY;
                    } else {
                        accessType = ACCESSTYPE_NONE;
                    }
                    break;
                default:
                    accessType = ACCESSTYPE_NONE;
                    break;
            }
        }
        return accessType;
    }

    private static List<String> getWrites(List<String> capabilities) {
        List<String> writes = new ArrayList<>();
        for (String capability : capabilities) {
            if (capability.toUpperCase().endsWith("WRITE") ||
                    capability.toUpperCase().endsWith("STATS") ||
                    capability.toUpperCase().endsWith("INVALIDATE")) {
                writes.add(capability);
            }
        }
        return writes;
    }

    private static List<String> getReads(List<String> capabilities) {
        List<String> reads = new ArrayList<>();
        for (String capability : capabilities) {
            if (capability.toUpperCase().endsWith("READ") ||
                    capability.toUpperCase().endsWith("SQL")) {
                reads.add(capability);
            }
        }
        return reads;
    }

    public static void getMaterializedViewInfo(StringBuilder tableInfo,
                                               Table tbl, boolean isOutputPadded) {
        formatOutput("View Original Text:", tbl.getViewOriginalText(), tableInfo);
        formatOutput("View Expanded Text:", tbl.getViewExpandedText(), tableInfo);
        formatOutput("Rewrite Enabled:", tbl.isRewriteEnabled() ? "Yes" : "No", tableInfo);
        // TODO: IMPALA-11815: hive metastore doesn't privide api to judge whether the
        //  materialized view is outdated for rewriting, so set the flag to "Unknown"
        formatOutput("Outdated for Rewriting:", "Unknown", tableInfo);
    }

    public static void createDataSource(IMetaStoreClient client, DataSource dataSource)
            throws InvalidObjectException, AlreadyExistsException, MetaException, TException {
        // noop for unsupported operation IMetaStoreClient.createDataConnector().
    }

    public static void dropDataSource(IMetaStoreClient client, String name,
                                      boolean ifExists) throws NoSuchObjectException, InvalidOperationException,
            MetaException, TException {
        // noop for unsupported operation IMetaStoreClient.dropDataConnector().
    }

    public static Map<String, DataSource> loadAllDataSources(IMetaStoreClient client)
            throws MetaException, TException {
        // Unsupported operation IMetaStoreClient.getAllDataConnectorNames() and
        // IMetaStoreClient.getDataConnector().
        return null;
    }

    public static void setNotificationEventRequestWithFilter(
            NotificationEventRequest eventRequest, MetaDataFilter metaDataFilter) {
        // noop for non-existent fields of NotificationEventRequest
    }
}
