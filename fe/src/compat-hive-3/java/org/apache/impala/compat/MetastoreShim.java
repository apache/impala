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

import static org.apache.hadoop.hive.metastore.api.hive_metastoreConstants.ACCESSTYPE_NONE;
import static org.apache.hadoop.hive.metastore.api.hive_metastoreConstants.ACCESSTYPE_READONLY;
import static org.apache.hadoop.hive.metastore.api.hive_metastoreConstants.ACCESSTYPE_READWRITE;
import static org.apache.hadoop.hive.metastore.api.hive_metastoreConstants.ACCESSTYPE_WRITEONLY;
import static org.apache.impala.util.HiveMetadataFormatUtils.LINE_DELIM;
import static org.apache.impala.util.HiveMetadataFormatUtils.formatOutput;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.base.Stopwatch;
import com.google.common.base.Strings;
import com.google.common.collect.Iterables;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import org.apache.hadoop.hive.common.ValidWriteIdList;
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.apache.hadoop.hive.metastore.IMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.AlreadyExistsException;
import org.apache.hadoop.hive.metastore.api.ColumnStatistics;
import org.apache.hadoop.hive.metastore.api.ColumnStatisticsObj;
import org.apache.hadoop.hive.metastore.api.CompactionInfoStruct;
import org.apache.hadoop.hive.metastore.api.DataConnector;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.FireEventRequest;
import org.apache.hadoop.hive.metastore.api.FireEventRequestData;
import org.apache.hadoop.hive.metastore.api.FireEventResponse;
import org.apache.hadoop.hive.metastore.api.GetAllWriteEventInfoRequest;
import org.apache.hadoop.hive.metastore.api.GetLatestCommittedCompactionInfoRequest;
import org.apache.hadoop.hive.metastore.api.GetLatestCommittedCompactionInfoResponse;
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
import org.apache.hadoop.hive.metastore.api.PartitionsRequest;
import org.apache.hadoop.hive.metastore.api.SetPartitionsStatsRequest;
import org.apache.hadoop.hive.metastore.api.SourceTable;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.metastore.api.WriteEventInfo;
import org.apache.hadoop.hive.metastore.api.WriteNotificationLogRequest;
import org.apache.hadoop.hive.metastore.conf.MetastoreConf;
import org.apache.hadoop.hive.metastore.messaging.AlterTableMessage;
import org.apache.hadoop.hive.metastore.messaging.CommitTxnMessage;
import org.apache.hadoop.hive.metastore.messaging.CommitCompactionMessage;
import org.apache.hadoop.hive.metastore.messaging.EventMessage;
import org.apache.hadoop.hive.metastore.messaging.MessageBuilder;
import org.apache.hadoop.hive.metastore.messaging.MessageDeserializer;
import org.apache.hadoop.hive.metastore.messaging.MessageEncoder;
import org.apache.hadoop.hive.metastore.messaging.MessageFactory;
import org.apache.hadoop.hive.metastore.messaging.MessageSerializer;
import org.apache.hadoop.hive.metastore.messaging.json.JSONDropDatabaseMessage;
import org.apache.hadoop.hive.metastore.messaging.ReloadMessage;
import org.apache.hadoop.hive.metastore.utils.MetaStoreUtils;
import org.apache.hadoop.hive.ql.metadata.formatting.TextMetaDataTable;
import org.apache.impala.analysis.TableName;
import org.apache.impala.catalog.CatalogException;
import org.apache.impala.catalog.CatalogServiceCatalog;
import org.apache.impala.catalog.CompactionInfoLoader;
import org.apache.impala.catalog.DataSource;
import org.apache.impala.catalog.DatabaseNotFoundException;
import org.apache.impala.catalog.Db;
import org.apache.impala.catalog.HdfsPartition;
import org.apache.impala.catalog.HdfsTable;
import org.apache.impala.catalog.Hive3MetastoreShimBase;
import org.apache.impala.catalog.MetaStoreClientPool;
import org.apache.impala.catalog.MetaStoreClientPool.MetaStoreClient;
import org.apache.impala.catalog.TableNotFoundException;
import org.apache.impala.catalog.TableNotLoadedException;
import org.apache.impala.catalog.TableWriteId;
import org.apache.impala.catalog.events.MetastoreEvents.MetastoreEvent;
import org.apache.impala.catalog.events.MetastoreEvents.MetastoreEventType;
import org.apache.impala.catalog.events.MetastoreEventsProcessor;
import org.apache.impala.catalog.events.MetastoreEventsProcessor.MetaDataFilter;
import org.apache.impala.catalog.events.MetastoreNotificationException;
import org.apache.impala.catalog.events.MetastoreNotificationNeedsInvalidateException;
import org.apache.impala.catalog.events.SelfEventContext;
import org.apache.impala.catalog.local.MetaProvider.PartitionMetadata;
import org.apache.impala.catalog.local.MetaProvider.PartitionRef;
import org.apache.impala.catalog.local.MetaProvider.TableMetaRef;
import org.apache.impala.catalog.metastore.CatalogMetastoreServer;
import org.apache.impala.catalog.metastore.ICatalogMetastoreServer;
import org.apache.impala.common.ImpalaRuntimeException;
import org.apache.impala.common.Metrics;
import org.apache.impala.common.PrintUtils;
import org.apache.impala.hive.common.MutableValidWriteIdList;
import org.apache.impala.service.BackendConfig;
import org.apache.impala.service.CatalogOpExecutor;
import org.apache.impala.util.AcidUtils.TblTransaction;
import org.apache.impala.util.MetaStoreUtil.TableInsertEventInfo;
import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A wrapper around some of Hive's Metastore API's to abstract away differences
 * between major versions of Hive. This implements the shimmed methods for CDP Hive 3
 * (using Hive 4 api).
 */
public class MetastoreShim extends Hive3MetastoreShimBase {
  private static final Logger LOG = LoggerFactory.getLogger(MetastoreShim.class);

  // Impala DataSource object is saved in HMS as DataConnector with type
  // as 'impalaDataSource'
  private static final String HMS_DATA_CONNECTOR_TYPE = "impalaDataSource";
  private static final String HMS_DATA_CONNECTOR_DESC = "Impala DataSource Object";
  private static final String HMS_DATA_CONNECTOR_PARAM_KEY_CLASS_NAME = "className";
  private static final String HMS_DATA_CONNECTOR_PARAM_KEY_API_VERSION = "apiVersion";

  /**
   * Wrapper around IMetaStoreClient.alter_table with validWriteIds as a param.
   */
  public static void alterTableWithTransaction(IMetaStoreClient client,
     Table tbl, TblTransaction tblTxn)
     throws ImpalaRuntimeException {
    tbl.setWriteId(tblTxn.writeId);
    try {
      client.alter_table(null, tbl.getDbName(), tbl.getTableName(),
        tbl, null, tblTxn.validWriteIds);
    }
    catch (TException e) {
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
    return client.getTableColumnStatistics(dbName, tableName, colNames,
        /*engine*/IMPALA_ENGINE);
  }

  /**
   * Wrapper around IMetaStoreClient.deleteTableColumnStatistics() to deal with added
   * arguments.
   */
  public static boolean deleteTableColumnStatistics(IMetaStoreClient client,
      String dbName, String tableName, String colName)
      throws NoSuchObjectException, MetaException, InvalidObjectException, TException,
             InvalidInputException {
    return client.deleteTableColumnStatistics(dbName, tableName, colName,
        /*engine*/IMPALA_ENGINE);
  }

  /**
   * Wrapper around ColumnStatistics c'tor to deal with the added engine property.
   */
  public static ColumnStatistics createNewHiveColStats() {
    ColumnStatistics colStats = new ColumnStatistics();
    colStats.setEngine(IMPALA_ENGINE);
    return colStats;
  }

  //hive-3 has a different class to encode and decode event messages
  private static final MessageEncoder eventMessageEncoder_ =
      MessageFactory.getDefaultInstance(MetastoreConf.newMetastoreConf());

  /**
   * Wrapper method which returns HMS-3 Message factory in case Impala is
   * building against Hive-3
   */
  public static MessageDeserializer getMessageDeserializer() {
    return eventMessageEncoder_.getDeserializer();
  }

  /**
   * Wrapper around HMS-3 message encoder to get the serializer
   * @return
   */
  public static MessageSerializer getMessageSerializer() {
    return eventMessageEncoder_.getSerializer();
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
   * @param message
   * @return serialized string to use used in the NotificationEvent's message field
   */
  @VisibleForTesting
  public static String serializeEventMessage(EventMessage message) {
    return getMessageSerializer().serialize(message);
  }

  /**
   * Get valid write ids from HMS for the acid table
   * @param client the client to access HMS
   * @param tableFullName the name for the table
   * @return ValidWriteIdList object
   */
  public static ValidWriteIdList fetchValidWriteIds(IMetaStoreClient client,
      String tableFullName) throws TException {
    return client.getValidWriteIds(tableFullName);
  }

  /**
   * Wrapper around HMS Partition object to get writeID
   * WriteID is introduced in ACID 2
   * It is used to detect changes of the partition
   */
  public static long getWriteIdFromMSPartition(Partition partition) {
    Preconditions.checkNotNull(partition);
    return partition.getWriteId();
  }

  /**
   * Wrapper around HMS Partition object to set writeID
   * WriteID is introduced in ACID 2
   * It is used to detect changes of the partition
   */
  public static void setWriteIdToMSPartition(Partition partition, long writeId) {
    Preconditions.checkNotNull(partition);
    partition.setWriteId(writeId);
  }

  /**
   * Wrapper around HMS Table object to get writeID
   * Per table writeId is introduced in ACID 2
   * It is used to detect changes of the table
   */
  public static long getWriteIdFromMSTable(Table msTbl) {
    Preconditions.checkNotNull(msTbl);
    return msTbl.getWriteId();
  }

  /**
   * Set impala capabilities to hive client
   * Impala supports:
   * - external table read/write
   * - insert-only Acid table read
   * - virtual view read
   * - materialized view read
   */
  public static synchronized void setHiveClientCapabilities() {
    String hostName;
    if (capabilitiestSet_) return;
    try {
      hostName = InetAddress.getLocalHost().getCanonicalHostName();
    } catch (UnknownHostException ue) {
      hostName = "unknown";
    }
    String buildVersion = BackendConfig.INSTANCE != null ?
        BackendConfig.INSTANCE.getImpalaBuildVersion() : String.valueOf(MAJOR_VERSION);
    if (buildVersion == null) buildVersion = String.valueOf(MAJOR_VERSION);

    String impalaId = String.format("Impala%s@%s", buildVersion, hostName);
    String[] capabilities = new String[] {
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

    HiveMetaStoreClient.setProcessorIdentifier(impalaId);
    HiveMetaStoreClient.setProcessorCapabilities(capabilities);
    capabilitiestSet_ = true;
  }

  /**
   * Check if a table has a capability
   * @param msTble hms table
   * @param requireCapability hive access types or combination of them
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
        && ((msTbl.getAccessType() & requiredCapability) != 0);
  }

  /**
   * Get Access type in string
   * @param msTble hms table
   * @return the string represents the table access type.
   */
  public static String getTableAccessType(Table msTbl) {
    Preconditions.checkNotNull(msTbl);
    switch (msTbl.getAccessType()) {
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
    msTbl.setAccessType(accessType);
  }

  public static void setTableColumnStatsTransactional(IMetaStoreClient client,
      Table msTbl, ColumnStatistics colStats, TblTransaction tblTxn)
      throws ImpalaRuntimeException {
    List<ColumnStatistics> colStatsList = new ArrayList<>();
    colStatsList.add(colStats);
    SetPartitionsStatsRequest request = new SetPartitionsStatsRequest(colStatsList);
    request.setWriteId(tblTxn.writeId);
    request.setValidWriteIdList(tblTxn.validWriteIds);
    try {
      // Despite its name, the function below can and (and currently must) be used
      // to set table level column statistics in transactional tables.
      client.setPartitionColumnStatistics(request);
    }
    catch (TException e) {
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
   * Fires a listener event of the type ACID_WRITE on a transactional table in metastore.
   * This event is polled by other external systems to detect insert operations into
   * ACID tables.
   * @throws TException in case of errors during HMS API call.
   */
  private static void fireInsertTransactionalEventHelper(
      IMetaStoreClient hiveClient, TableInsertEventInfo insertEventInfo, String dbName,
      String tableName) throws TException {
    for (InsertEventRequestData insertData : insertEventInfo.getInsertEventReqData()) {
      // TODO(Vihang) unfortunately there is no bulk insert event API for transactional
      // tables. It is possible that this may take long time here if there are lots of
      // partitions which were inserted.
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
      // TODO(Vihang) metastore should return the event id here so that we can get rid
      // of firing INSERT event types for transactional tables.
      hiveClient.addWriteNotificationLog(rqst);
    }
  }

  /**
   *  Fires an insert event to HMS notification log. In Hive-3 for partitioned table,
   *  all partition insert events will be fired by a bulk API.
   *
   * @param msClient Metastore client,
   * @param insertEventDataList A list of insert event info encapsulating the information
   *                            needed to fire insert events.
   * @param insertEventPartValList   The partition list corresponding to
   *                                 insertEventDataList, used by Apache Hive 3
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
    LOG.debug(String.format(
        "Firing %s insert event(s) for %s.%s", insertEventDataList.size(), dbName,
        tableName));
    FireEventRequestData data = new FireEventRequestData();
    FireEventRequest rqst = new FireEventRequest(true, data);
    rqst.setDbName(dbName);
    rqst.setTableName(tableName);
    if (insertEventDataList.size() == 1) {
      InsertEventRequestData insertEventData = Iterables
          .getOnlyElement(insertEventDataList);
      if (insertEventData.getPartitionVal() != null) {
        rqst.setPartitionVals(insertEventData.getPartitionVal());
      }
      // single insert event API
      data.setInsertData(insertEventData);
    } else {
      // use bulk insert API
      data.setInsertDatas(insertEventDataList);
    }
    FireEventResponse response = msClient.fireListenerEvent(rqst);
    if (!response.isSetEventIds()) {
      LOG.error("FireEventResponse does not have event ids set for table {}.{}. This "
              + "may cause the table to unnecessarily be refreshed when the insert event "
              + "is received.", dbName, tableName);
      return Collections.EMPTY_LIST;
    }
    return response.getEventIds();
  }

  /**
   *  Fires a reload event to HMS notification log. In Hive-3 the relaod event
   *  in HMS corresponds to refresh table or invalidate metadata of table in impala.
   *
   * @param msClient Metastore client,
   * @param isRefresh if this flag is set to true then it is a refresh query, else it
   *                  is an invalidate metadata query.
   * @param partVals The partition list corresponding to
   *                                 the table, used by Apache Hive 3
   * @param dbName
   * @param tableName
   * @return a list of eventIds for the reload events
   */
  @VisibleForTesting
  public static List<Long> fireReloadEventHelper(MetaStoreClient msClient,
      boolean isRefresh, List<String> partVals, String dbName, String tableName,
      Map<String, String> selfEventParams) throws TException {
    Preconditions.checkNotNull(msClient);
    Preconditions.checkNotNull(dbName);
    Preconditions.checkNotNull(tableName);
    FireEventRequestData data = new FireEventRequestData();
    data.setRefreshEvent(isRefresh);
    FireEventRequest rqst = new FireEventRequest(true, data);
    rqst.setDbName(dbName);
    rqst.setTableName(tableName);
    rqst.setPartitionVals(partVals);
    rqst.setTblParams(selfEventParams);
    FireEventResponse response = msClient.getHiveClient().fireListenerEvent(rqst);
    if (!response.isSetEventIds()) {
      LOG.error("FireEventResponse does not have event ids set for table {}.{}. This "
              + "may cause the table to unnecessarily be refreshed when the " +
              "refresh/invalidate event is received.", dbName, tableName);
      return Collections.emptyList();
    }
    return response.getEventIds();
  }

  /**
   *  This method extracts the table, partition, and isRefresh fields from the
   *  notification event and returns them in a map.
   *
   * @param event Metastore notification event,
   * @return a Map of fields required for the reload event.
   */
  public static Map<String, Object> getFieldsFromReloadEvent(NotificationEvent event)
      throws MetastoreNotificationException{
    ReloadMessage reloadMessage =
        MetastoreEventsProcessor.getMessageDeserializer()
            .getReloadMessage(event.getMessage());
    Map<String, Object> updatedFields = new HashMap<>();
    try {
      org.apache.hadoop.hive.metastore.api.Table msTbl = Preconditions.checkNotNull(
          reloadMessage.getTableObj());
      Partition reloadPartition = reloadMessage.getPtnObj();
      boolean isRefresh = reloadMessage.isRefreshEvent();
      updatedFields.put("table", msTbl);
      updatedFields.put("partition", reloadPartition);
      updatedFields.put("isRefresh", isRefresh);
    } catch (Exception e) {
      throw new MetastoreNotificationException(e);
    }
    return updatedFields;
  }

  /**
   *  This method extracts the partition name field from the
   *  notification event and returns it in the form of string.
   *
   * @param event Metastore notification event,
   * @return the partition name, required for the commit compaction event.
   */
  public static String getPartitionNameFromCommitCompactionEvent(
      NotificationEvent event) {
    CommitCompactionMessage commitCompactionMessage = MetastoreEventsProcessor.
        getMessageDeserializer().getCommitCompactionMessage(event.getMessage());
    return commitCompactionMessage.getPartName();
  }

  /**
   * Wrapper around IMetaStoreClient.getThriftClient().get_next_notification() to deal
   * with added arguments.
   *
   * @param msClient Metastore client
   * @param eventRequest Notification event request
   * @param eventTypeSkipList Unwanted event types
   * @return NotificationEventResponse
   * @throws TException
   */
  public static NotificationEventResponse getNextNotification(IMetaStoreClient msClient,
      NotificationEventRequest eventRequest, List<String> eventTypeSkipList)
      throws TException {
    if (eventTypeSkipList != null) {
      eventRequest.setEventTypeSkipList(eventTypeSkipList);
    }
    return msClient.getThriftClient().get_next_notification(eventRequest);
  }

  /**
   * Wrapper around Database.setManagedLocationUri() to deal with added arguments.
   */
  public static void setManagedLocationUri(Database db, String managedLocation) {
    db.setManagedLocationUri(managedLocation);
  }

  /**
   * Wrapper around HMS Database object to get managedLocationUri.
   */
  public static String getManagedLocationUri(Database db) {
    return db.getManagedLocationUri();
  }

  /**
   * Apache Hive-3 only function.
   */
  public static void setTableLocation(Db db, Table tbl) throws ImpalaRuntimeException {
  }

  /**
   * Wrapper around HMS Table object to get table id.
   */
  public static long getTableId(Table tbl) {
    return tbl.getId();
  }

  /**
   * Wrapper around JSONDropDatabaseMessage to get database.
   */
  public static Database getDatabaseObject(JSONDropDatabaseMessage dropDatabaseMessage)
      throws Exception {
    return dropDatabaseMessage.getDatabaseObject();
  }

  /**
   * Wrapper around IMetaStoreClient.truncateTable() to deal with added arguments.
   */
  public static void truncateTable(IMetaStoreClient msClient, String dbName,
      String tableName, List<String> partNames,
      String validWriteIds, long writeId) throws TException {
    msClient.truncateTable(dbName, tableName, partNames, validWriteIds, writeId);
  }

  /**
   * Wrapper around IMetaStoreClient.getPartitionsRequest() to deal with added arguments.
   */
  public static List<Partition> getPartitions(IMetaStoreClient msClient,
      String testDbName, String testTblName) throws TException {
    PartitionsRequest req = new PartitionsRequest();
    req.setDbName(testDbName);
    req.setTblName(testTblName);
    return msClient.getPartitionsRequest(req).getPartitions();
  }

  /**
   * Wrapper around InsertEventRequestData.setPartitionVal() to deal with added arguments.
   */
  public static void setPartitionVal(InsertEventRequestData insertEventRequestData,
      List<String> partVals) {
    if (partVals != null && !partVals.isEmpty()) {
      insertEventRequestData.setPartitionVal(partVals);
    }
  }

  /**
   * Wrapper around InsertEventRequestData.addToSubDirectoryList() to deal with added
   * arguments.
   */
  public static void addToSubDirectoryList(InsertEventRequestData insertEventRequestData,
      String acidDirPath) {
    insertEventRequestData.addToSubDirectoryList(acidDirPath);
  }

  /**
   * This method fetches latest compaction id from HMS and compares it with the cached
   * compaction id. It returns list of partitions that need to refresh file metadata due
   * to compactions. If none of the partitions need to refresh, it returns empty list.
   */
  public static List<HdfsPartition.Builder> getPartitionsForRefreshingFileMetadata(
      CatalogServiceCatalog catalog, HdfsTable hdfsTable) throws CatalogException {
    List<HdfsPartition.Builder> partBuilders = new ArrayList<>();
    // fetch the latest compaction info from HMS
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
        // It is possible for a partitioned table to have null partitionname in case of
        // an aborted dynamic partition insert.
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
   * Fetches the latest compactions from HMS
   */
  public static Map<String, Long> getLatestCompactions(MetaStoreClient client,
      String dbName, String tableName, List<String> partitionNames,
      String unPartitionedName, long lastCompactionId) throws TException {
    GetLatestCommittedCompactionInfoRequest request =
        new GetLatestCommittedCompactionInfoRequest(dbName, tableName);
    request.setPartitionnames(partitionNames);
    if (lastCompactionId > 0) request.setLastCompactionId(lastCompactionId);
    GetLatestCommittedCompactionInfoResponse response;
    response = CompactionInfoLoader.getLatestCompactionInfo(client, request);
    Map<String, Long> partNameToCompactionId = new HashMap<>();
    if (partitionNames != null) {
      for (CompactionInfoStruct ci : response.getCompactions()) {
        if (ci.getPartitionname() != null) {
          partNameToCompactionId.put(ci.getPartitionname(), ci.getId());
        } else {
          LOG.warn(
              "Partitioned table {} has null partitionname in CompactionInfoStruct: {}",
              tableName, ci.toString());
        }
      }
    } else {
      CompactionInfoStruct ci = Iterables.getOnlyElement(response.getCompactions(), null);
      if (ci != null) partNameToCompactionId.put(unPartitionedName, ci.getId());
    }
    return partNameToCompactionId;
  }

  /**
   * Fetches the latest compaction id from HMS and compares with partition metadata in
   * cache. If a partition is stale due to compaction, removes it from metas.
   */
  public static List<PartitionRef> checkLatestCompaction(MetaStoreClientPool msClientPool,
      String dbName, String tableName, TableMetaRef table,
      Map<PartitionRef, PartitionMetadata> metas, String unPartitionedName)
      throws TException {
    Preconditions.checkNotNull(table, "TableMetaRef must be non-null");
    Preconditions.checkNotNull(metas, "Partition map must be non-null");
    if (!table.isTransactional() || metas.isEmpty()) return Collections.emptyList();
    Stopwatch sw = Stopwatch.createStarted();
    List<String> partitionNames = null;
    if (table.isPartitioned()) {
      partitionNames =
          metas.keySet().stream().map(PartitionRef::getName).collect(Collectors.toList());
    }
    long lastCompactionId = metas.values()
                                .stream()
                                .mapToLong(PartitionMetadata::getLastCompactionId)
                                .max()
                                .orElse(-1);
    Map<String, Long> partNameToCompactionId = Collections.emptyMap();
    try (MetaStoreClientPool.MetaStoreClient client = msClientPool.getClient()) {
      partNameToCompactionId = getLatestCompactions(
          client, dbName, tableName, partitionNames, unPartitionedName, lastCompactionId);
    }
    List<PartitionRef> stalePartitions = new ArrayList<>();
    Iterator<Entry<PartitionRef, PartitionMetadata>> iter = metas.entrySet().iterator();
    while (iter.hasNext()) {
      Map.Entry<PartitionRef, PartitionMetadata> entry = iter.next();
      if (partNameToCompactionId.containsKey(entry.getKey().getName())) {
        stalePartitions.add(entry.getKey());
        iter.remove();
      }
    }
    LOG.debug("Checked the latest compaction info for {}.{}. Time taken: {}", dbName,
        tableName, PrintUtils.printTimeMs(sw.stop().elapsed(TimeUnit.MILLISECONDS)));
    return stalePartitions;
  }

  /**
   * Returns an instance of CatalogMetastoreServer.
   */
  public static ICatalogMetastoreServer getCatalogMetastoreServer(
      CatalogOpExecutor catalogOpExecutor) {
    int portNumber = BackendConfig.INSTANCE.getHMSPort();
    Preconditions.checkState(portNumber > 0, "Invalid port number for HMS service.");
    return new CatalogMetastoreServer(catalogOpExecutor);
  }

  /**
   * Metastore event handler for COMMIT_TXN events. Handles commit event for transactional
   * tables.
   */
  public static class CommitTxnEvent extends MetastoreEvent {
    private final CommitTxnMessage commitTxnMessage_;
    private final long txnId_;
    private Set<TableWriteId> tableWriteIds_ = Collections.emptySet();

    public CommitTxnEvent(CatalogOpExecutor catalogOpExecutor, Metrics metrics,
        NotificationEvent event) {
      super(catalogOpExecutor, metrics, event);
      Preconditions.checkState(getEventType().equals(MetastoreEventType.COMMIT_TXN));
      Preconditions.checkNotNull(event.getMessage());
      commitTxnMessage_ = MetastoreEventsProcessor.getMessageDeserializer()
          .getCommitTxnMessage(event.getMessage());
      txnId_ = commitTxnMessage_.getTxnId();
      LOG.info("EventId: {} EventType: COMMIT_TXN transaction id: {}", getEventId(),
          txnId_);
    }

    @Override
    protected void process() throws MetastoreNotificationException {
      // To ensure no memory leaking in case an exception is thrown, we remove entries
      // at first.
      Set<TableWriteId> committedWriteIds = catalog_.removeWriteIds(txnId_);
      tableWriteIds_ = committedWriteIds;
      // Via getAllWriteEventInfo, we can get data insertion info for transactional tables
      // even though there are no insert events generated for transactional tables. Note
      // that we cannot get DDL info from this API.
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
        // committed write ids for DDL need to be added here
        addCommittedWriteIdsToTables(committedWriteIds);
      } catch (Exception e) {
        throw new MetastoreNotificationNeedsInvalidateException(debugString("Failed to "
            + "mark committed write ids and refresh partitions for txn {}. Event "
            + "processing cannot continue. Issue an invalidate metadata command to reset "
            + "event processor.", txnId_), e);
      }
    }

    @Override
    protected boolean onFailure(Exception e) {
      if (!BackendConfig.INSTANCE.isInvalidateMetadataOnEventProcessFailureEnabled()
          || !canInvalidateTable(e)) {
        return false;
      }
      errorLog(
          "Invalidating tables in transaction due to exception during event processing",
          e);
      Set<TableName> tableNames =
          tableWriteIds_.stream()
              .map(writeId -> new TableName(writeId.getDbName(), writeId.getTblName()))
              .collect(Collectors.toSet());
      for (TableName tableName : tableNames) {
        errorLog("Invalidate table {}.{}", tableName.getDb(), tableName.getTbl());
        catalog_.invalidateTableIfExists(tableName.getDb(), tableName.getTbl());
      }
      return true;
    }

    private void addCommittedWriteIdsToTables(Set<TableWriteId> tableWriteIds)
        throws CatalogException {
      for (TableWriteId tableWriteId: tableWriteIds) {
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
      // To load partitions together for the same table, indexes are grouped by table name
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
              "CommitTxnEvent", getEventId(), /*isSkipFileMetadataReload*/false);
        }
      }
    }

    @Override
    protected boolean isEventProcessingDisabled() {
      return false;
    }

    @Override
    protected SelfEventContext getSelfEventContext() {
      throw new UnsupportedOperationException("Self-event evaluation is not needed for "
          + "this event type");
    }

    /*
    Not skipping the event since there can be multiple tables involved. The actual
    processing of event would skip or process the event on a table by table basis
     */
    @Override
    protected boolean shouldSkipWhenSyncingToLatestEventId() {
      return false;
    }
  }

  public static void getMaterializedViewInfo(StringBuilder tableInfo, Table tbl,
       boolean isOutputPadded) {
    formatOutput("View Original Text:", tbl.getViewOriginalText(), tableInfo);
    formatOutput("View Expanded Text:", tbl.getViewExpandedText(), tableInfo);
    formatOutput("Rewrite Enabled:", tbl.isRewriteEnabled() ? "Yes" : "No", tableInfo);
    // TODO: IMPALA-11815: hive metastore doesn't privide api to judge whether the
    //  materialized view is outdated for rewriting, so set the flag to "Unknown"
    formatOutput("Outdated for Rewriting:", "Unknown", tableInfo);

    tableInfo.append(LINE_DELIM)
        .append("# Materialized View Source table information")
        .append(LINE_DELIM);
    TextMetaDataTable metaDataTable = new TextMetaDataTable();
    metaDataTable.addRow("Table name", "I/U/D since last rebuild");
    List<SourceTable> sourceTableList =
        new ArrayList<>(tbl.getCreationMetadata().getSourceTables());
    sourceTableList.sort(
        Comparator
            .<SourceTable, String>comparing(
                sourceTable -> sourceTable.getTable().getDbName())
            .thenComparing(sourceTable -> sourceTable.getTable().getTableName()));
    for (SourceTable sourceTable : sourceTableList) {
      String qualifiedTableName = org.apache.hadoop.hive.common.TableName.getQualified(
          sourceTable.getTable().getCatName(),
          sourceTable.getTable().getDbName(),
          sourceTable.getTable().getTableName());
      metaDataTable.addRow(qualifiedTableName,
          String.format("%d/%d/%d",
              sourceTable.getInsertedCount(), sourceTable.getUpdatedCount(),
              sourceTable.getDeletedCount()));
      tableInfo.append(metaDataTable.renderTable(isOutputPadded));
    }
  }

  /**
   * Wrapper around IMetaStoreClient.createDataConnector().
   */
  public static void createDataSource(IMetaStoreClient client, DataSource dataSource)
      throws InvalidObjectException, AlreadyExistsException, MetaException, TException {
    DataConnector connector = MetastoreShim.dataSourceToDataConnector(dataSource);
    Preconditions.checkNotNull(connector);
    client.createDataConnector(connector);
  }

  /**
   * Wrapper around IMetaStoreClient.dropDataConnector().
   */
  public static void dropDataSource(IMetaStoreClient client, String name,
      boolean ifExists) throws NoSuchObjectException, InvalidOperationException,
          MetaException, TException {
    // Set 'checkReferences' as false, e.g. drop DataSource object without checking its
    // reference since the properties of DataSource object are copied to the table
    // property of the referenced DataSource tables.
    client.dropDataConnector(
        name, /* ifNotExists */ !ifExists, /* checkReferences */ false);
  }

  /**
   * Wrapper around IMetaStoreClient.getAllDataConnectorNames() and
   * IMetaStoreClient.getDataConnector().
   */
  public static Map<String, DataSource> loadAllDataSources(IMetaStoreClient client)
      throws MetaException, TException {
    Map<String, DataSource> newDataSrcs = new HashMap<String, DataSource>();
    // Load DataSource objects from HMS DataConnector objects.
    List<String> allConnectorNames = client.getAllDataConnectorNames();
    for (String connectorName: allConnectorNames) {
      DataConnector connector = client.getDataConnector(connectorName);
      if (connector != null) {
        DataSource dataSrc = MetastoreShim.dataConnectorToDataSource(connector);
        if (dataSrc != null) newDataSrcs.put(connectorName, dataSrc);
      }
    }
    return newDataSrcs;
  }

  /**
   * Convert DataSource object to DataConnector object.
   */
  private static DataConnector dataSourceToDataConnector(DataSource dataSource) {
    DataConnector connector = new DataConnector(
        dataSource.getName(), HMS_DATA_CONNECTOR_TYPE, dataSource.getLocation());
    connector.setDescription(HMS_DATA_CONNECTOR_DESC);
    connector.putToParameters(
        HMS_DATA_CONNECTOR_PARAM_KEY_CLASS_NAME, dataSource.getClassName());
    connector.putToParameters(
        HMS_DATA_CONNECTOR_PARAM_KEY_API_VERSION, dataSource.getApiVersion());
    return connector;
  }

  /**
   * Convert DataConnector object to DataSource object.
   */
  private static DataSource dataConnectorToDataSource(DataConnector connector) {
    if (!connector.isSetName() || !connector.isSetType()
        || !connector.isSetDescription() || connector.getParametersSize() == 0
        || !connector.getType().equalsIgnoreCase(HMS_DATA_CONNECTOR_TYPE)) {
      return null;
    }
    String name = connector.getName();
    String location = connector.isSetUrl() ? connector.getUrl() : "";
    String className =
        connector.getParameters().get(HMS_DATA_CONNECTOR_PARAM_KEY_CLASS_NAME);
    String apiVersion =
        connector.getParameters().get(HMS_DATA_CONNECTOR_PARAM_KEY_API_VERSION);
    if (!Strings.isNullOrEmpty(name) && location != null &&
        !Strings.isNullOrEmpty(className) && !Strings.isNullOrEmpty(apiVersion)) {
      return new DataSource(name, location, className, apiVersion);
    }
    return null;
  }

  public static void setNotificationEventRequestWithFilter(
      NotificationEventRequest eventRequest, MetaDataFilter metaDataFilter) {
    if (metaDataFilter != null) {
      if (metaDataFilter.getCatName() != null &&
          !metaDataFilter.getCatName().isEmpty()) {
        eventRequest.setCatName(metaDataFilter.getCatName());
      }
      if (metaDataFilter.getDbName() != null &&
          !metaDataFilter.getDbName().isEmpty()) {
        eventRequest.setDbName(metaDataFilter.getDbName());
      }
      if (metaDataFilter.getTableName() != null &&
          !metaDataFilter.getTableName().isEmpty()) {
        eventRequest.setTableNames(Arrays.asList(metaDataFilter.getTableName()));
      }
    }
  }
}
