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

import com.codahale.metrics.Gauge;
import com.codahale.metrics.Snapshot;
import com.codahale.metrics.Timer;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.google.common.util.concurrent.Uninterruptibles;
import java.time.LocalDateTime;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import javax.annotation.Nullable;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.hadoop.hive.metastore.IMetaStoreClient;
import org.apache.hadoop.hive.metastore.IMetaStoreClient.NotificationFilter;
import org.apache.hadoop.hive.metastore.api.CurrentNotificationEventId;
import org.apache.hadoop.hive.metastore.api.NoSuchObjectException;
import org.apache.hadoop.hive.metastore.api.NotificationEvent;
import org.apache.hadoop.hive.metastore.api.NotificationEventRequest;
import org.apache.hadoop.hive.metastore.api.NotificationEventResponse;
import org.apache.hadoop.hive.metastore.api.TableMeta;
import org.apache.hadoop.hive.metastore.api.UnknownDBException;
import org.apache.hadoop.hive.metastore.messaging.MessageDeserializer;
import org.apache.impala.analysis.TableRef;
import org.apache.impala.catalog.CatalogException;
import org.apache.impala.catalog.CatalogServiceCatalog;
import org.apache.impala.catalog.Db;
import org.apache.impala.catalog.HdfsTable;
import org.apache.impala.catalog.MetaStoreClientPool.MetaStoreClient;
import org.apache.impala.catalog.MetastoreClientInstantiationException;
import org.apache.impala.catalog.Table;
import org.apache.impala.catalog.IncompleteTable;
import org.apache.impala.catalog.View;
import org.apache.impala.catalog.events.ConfigValidator.ValidationResult;
import org.apache.impala.catalog.events.MetastoreEvents.AlterDatabaseEvent;
import org.apache.impala.catalog.events.MetastoreEvents.CreateDatabaseEvent;
import org.apache.impala.catalog.events.MetastoreEvents.CreateTableEvent;
import org.apache.impala.catalog.events.MetastoreEvents.DropDatabaseEvent;
import org.apache.impala.catalog.events.MetastoreEvents.DropTableEvent;
import org.apache.impala.catalog.events.MetastoreEvents.MetastoreEvent;
import org.apache.impala.catalog.events.MetastoreEvents.MetastoreEventFactory;
import org.apache.impala.catalog.events.MetastoreEvents.MetastoreEventType;
import org.apache.impala.common.Metrics;
import org.apache.impala.common.PrintUtils;
import org.apache.impala.common.Reference;
import org.apache.impala.compat.MetastoreShim;
import org.apache.impala.service.BackendConfig;
import org.apache.impala.service.CatalogOpExecutor;
import org.apache.impala.thrift.TCatalogObject;
import org.apache.impala.thrift.TCatalogObjectType;
import org.apache.impala.thrift.TDatabase;
import org.apache.impala.thrift.TErrorCode;
import org.apache.impala.thrift.TEventBatchProgressInfo;
import org.apache.impala.thrift.TEventProcessorMetrics;
import org.apache.impala.thrift.TEventProcessorMetricsSummaryRequest;
import org.apache.impala.thrift.TEventProcessorMetricsSummaryResponse;
import org.apache.impala.thrift.TImpalaTableType;
import org.apache.impala.thrift.TStatus;
import org.apache.impala.thrift.TTable;
import org.apache.impala.thrift.TTableName;
import org.apache.impala.thrift.TWaitForHmsEventRequest;
import org.apache.impala.util.AcidUtils;
import org.apache.impala.util.EventSequence;
import org.apache.impala.util.MetaStoreUtil;
import org.apache.impala.util.NoOpEventSequence;
import org.apache.impala.util.ThreadNameAnnotator;
import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A metastore event is a instance of the class
 * <code>org.apache.hadoop.hive.metastore.api.NotificationEvent</code>. Metastore can be
 * configured, to work with Listeners which are called on various DDL operations like
 * create/alter/drop operations on database, table, partition etc. Each event has a unique
 * incremental id and the generated events are be fetched from Metastore to get
 * incremental updates to the metadata stored in Hive metastore using the the public API
 * <code>get_next_notification</code> These events could be generated by external
 * Metastore clients like Apache Hive or Apache Spark as well as other Impala clusters
 * configured to talk with the same metastore.
 *
 * This class is used to poll metastore for such events at a given frequency. By observing
 * such events, we can take appropriate action on the catalogD
 * (refresh/invalidate/add/remove) so that catalog represents the latest information
 * available in metastore. We keep track of the last synced event id in each polling
 * iteration so the next batch can be requested appropriately. The current batch size is
 * constant and set to MAX_EVENTS_PER_RPC.
 *
 * <pre>
 *      +---------------+   +----------------+        +--------------+
 *      |Catalog state  |   |Catalog         |        |              |
 *      |stale          |   |State up-to-date|        |Catalog state |
 *      |(apply event)  |   |(ignore)        |        |is newer than |
 *      |               |   |                |        |event         |
 *      |               |   |                |        |(ignore)      |
 *      +------+--------+   +-----+----------+        +-+------------+
 *             |                  |                     |
 *             |                  |                     |
 *             |                  |                     |
 *             |                  |                     |
 *             |                  |                     |
 * +-----------V------------------V---------------------V----------->  Event Timeline
 *                                ^
 *                                |
 *                                |
 *                                |
 *                                |
 *                                E
 *
 * </pre>
 * Consistency model: Events could be seen as DDLs operations from past either done from
 * this same cluster or some other external system. For example in the events timeline
 * given above, consider a Event E at any given time. The catalog state for the
 * corresponding object of the event could either be stale, exactly-same or at a version
 * which is higher than one provided by event. Catalog state should only be updated when
 * it is stale with respect to the event. In order to determine if the catalog object is
 * stale, we rely on a combination of create EventId and object version.
 * In case of create/drop events on database/table/partitions we use the
 * <code>createEventId</code> field of the corresponding object in the catalogd
 * to determine if the event needs to be processed or ignored. E.g. if Impala creates
 * a table, {@link CatalogOpExecutor} will create the table and assign the createEventId
 * of the table by fetching the CREATE_TABLE event from HMS. Later on when the event
 * is fetched by events processor, it uses the createEventId of the Catalogd's table to
 * ignore the event. Similar approach is used for databases and partition create events.
 *
 * In case of Drop events for database/table/partition events processor looks at the
 * {@link DeleteEventLog} in the CatalogOpExecutor to determine if the table has been
 * dropped already from catalogd.
 *
 * In case of ALTER/INSERT events events processor relies on the object version in the
 * properties of the table to determine if this is a self-event or not.
 *
 * Following table shows the actions to be taken when the given event type is received.
 *
 * <pre>
 *               +------------------------------------------------+---------------------+
 *               |    Catalog object state                                              |
 * +--------------------------------------+-----------------------+---------------------+
 * | Event type  | Loaded                 | Incomplete            | Not present         |
 * |             |                        |                       |                     |
 * +--------------------------------------------------------------+---------------------+
 * |             |                        |                       |                     |
 * | CREATE EVENT| Ignore                 | Ignore                | addIfNotRemovedLater|
 * |             |                        |                       |                     |
 * |             |                        |                       |                     |
 * | ALTER EVENT | Refresh                | Ignore                | Ignore              |
 * |             |                        |                       |                     |
 * |             |                        |                       |                     |
 * | DROP EVENT  | removeIfNotAddedLater  | removeIfNotAddedLater | Ignore              |
 * |             |                        |                       |                     |
 * |             |                        |                       |                     |
 * | INSERT EVENT| Refresh                | Ignore                | Ignore              |
 * |             |                        |                       |                     |
 * +-------------+------------------------+-----------------------+---------------------+
 * </pre>
 *
 * Currently event handlers rely on createEventId on Database, Table and Partition to
 * uniquely determine if the object from event is same as object in the catalog. This
 * information is used to make sure that we are deleting the right incarnation of the
 * object when compared to Metastore.
 *
 * Self-events:
 * Events could be generated by this Catalog's operations as well. Invalidating table
 * for such events is unnecessary and inefficient. In order to detect such self-events
 * when catalog executes a DDL operation it appends the current catalog version to the
 * list of version numbers for the in-flight events for the table. Events processor
 * clears this version when the corresponding version number identified by serviceId is
 * received in the event. This is needed since it is possible that a external
 * non-Impala system which generates the event presents the same serviceId and version
 * number later on. The algorithm to detect such self-event is as below.
 *
 * 1. Add the service id and expected catalog version to database/table/partition
 * parameters when executing the DDL operation. When the HMS operation is successful, add
 * the version number to the list of version for in-flight events at
 * table/database/partition level.
 * 2. When the event is received, the first time you see the combination of serviceId
 * and version number, event processor clears the version number from table's list and
 * determines the event as self-generated (and hence ignored).
 * 3. If the event data presents a unknown serviceId or if the version number is not
 * present in the list of in-flight versions, event is not a self-event and needs to be
 * processed.
 *
 * In order to limit the total memory footprint, only 100 version numbers are stored at
 * the catalog object. Since the event processor is expected to poll every few seconds
 * this should be a reasonable bound which satisfies most use-cases. Otherwise, event
 * processor may wrongly process a self-event to invalidate the table. In such a case,
 * its a performance penalty not a correctness issue.
 *
 * All the operations which change the state of catalog cache while processing a certain
 * event type must be atomic in nature. We rely on taking a DDL lock in CatalogOpExecutor
 * in case of create/drop events and object (Db or table) level writeLock in case of alter
 * events to make sure that readers are blocked while the metadata
 * update operation is being performed. Since the events are generated post-metastore
 * operations, such catalog updates do not need to update the state in Hive Metastore.
 *
 * Error Handling: The event processor could be in ACTIVE, PAUSED, ERROR states. In case
 * of any errors while processing the events the state of event processor changes to ERROR
 * and no subsequent events are polled. In such a case a invalidate metadata command
 * restarts the event polling which updates the lastSyncedEventId to the latest from
 * metastore.
 *
 * TODO:
 * 1. a global invalidate metadata command to get the events processor out of error state
 * is too heavy weight. We should make it easier to recover from the error state.
 * 2. The createEventId logic can be extended to track the last eventId which the table
 * has synced to and we can then get rid of self-event logic for alter events too.
 */
public class MetastoreEventsProcessor implements ExternalEventsProcessor {

  public static final String HMS_ADD_THRIFT_OBJECTS_IN_EVENTS_CONFIG_KEY =
      "hive.metastore.notifications.add.thrift.objects";

  private static final Logger LOG =
      LoggerFactory.getLogger(MetastoreEventsProcessor.class);

  private static final MessageDeserializer MESSAGE_DESERIALIZER =
      MetastoreShim.getMessageDeserializer();

  private static MetastoreEventsProcessor instance;

  // maximum number of events to poll in each RPC
  private static final int EVENTS_BATCH_SIZE_PER_RPC = 1000;

  // maximum time to wait for a clean shutdown of scheduler in seconds
  private static final int SCHEDULER_SHUTDOWN_TIMEOUT = 10;

  // time taken to fetch events during each poll
  public static final String EVENTS_FETCH_DURATION_METRIC = "events-fetch-duration";
  // time taken to apply all the events received duration each poll
  public static final String EVENTS_PROCESS_DURATION_METRIC = "events-apply-duration";
  // rate of events received per unit time
  public static final String EVENTS_RECEIVED_METRIC = "events-received";
  // total number of events which are skipped because of the flag setting or
  // in case of [CREATE|DROP|ALTER] events on [DATABASE|TABLE|PARTITION] which were
  // ignored because the [DATABASE|TABLE|PARTITION] was already [PRESENT|ABSENT] in
  // the catalogd.
  public static final String EVENTS_SKIPPED_METRIC = "events-skipped";
  // name of the event processor status metric
  public static final String STATUS_METRIC = "status";
  // last synced event id
  public static final String LAST_SYNCED_ID_METRIC = "last-synced-event-id";
  // last synced event time
  public static final String LAST_SYNCED_EVENT_TIME = "last-synced-event-time";
  // Greatest synced event id
  public static final String GREATEST_SYNCED_EVENT_ID = "greatest-synced-event-id";
  // Greatest synced event time
  public static final String GREATEST_SYNCED_EVENT_TIME = "greatest-synced-event-time";
  // latest event id in Hive metastore
  public static final String LATEST_EVENT_ID = "latest-event-id";
  // event time of the latest event in Hive metastore
  public static final String LATEST_EVENT_TIME = "latest-event-time";
  // delay(secs) in events processing
  public static final String EVENT_PROCESSING_DELAY = "event-processing-delay";

  // Number of events pending to process on DbEventExecutors and TableEventExecutors, when
  // hierarchical mode is enabled. It is 0, when hierarchical mode is disabled.
  public static final String OUTSTANDING_EVENT_COUNT = "outstanding-event-count";

  // metric name for number of tables which are refreshed by event processor so far
  public static final String NUMBER_OF_TABLE_REFRESHES = "tables-refreshed";
  // number of times events processor refreshed a partition
  public static final String NUMBER_OF_PARTITION_REFRESHES = "partitions-refreshed";
  // number of tables which were added to the catalogd based on events.
  public static final String NUMBER_OF_TABLES_ADDED = "tables-added";
  // number of tables which were removed to the catalogd based on events.
  public static final String NUMBER_OF_TABLES_REMOVED = "tables-removed";
  // number of databases which were added to the catalogd based on events.
  public static final String NUMBER_OF_DATABASES_ADDED = "databases-added";
  // number of database which were removed to the catalogd based on events.
  public static final String NUMBER_OF_DATABASES_REMOVED = "databases-removed";
  // number of partitions which were added to the catalogd based on events.
  public static final String NUMBER_OF_PARTITIONS_ADDED = "partitions-added";
  // number of partitions which were removed to the catalogd based on events.
  public static final String NUMBER_OF_PARTITIONS_REMOVED = "partitions-removed";
  // number of entries in the delete event log
  public static final String DELETE_EVENT_LOG_SIZE = "delete-event-log-size";
  // number of batch events generated
  public static final String NUMBER_OF_BATCH_EVENTS = "batch-events-created";

  // metric to measure the delay in msec, between the event created in metastore and time
  // it took to be consumed by the event processor
  public static final String AVG_DELAY_IN_CONSUMING_EVENTS = "events-consuming" +
      "-delay";

  private static final long SECOND_IN_NANOS = 1000 * 1000 * 1000L;

  public static List<NotificationEvent> getNextMetastoreEventsInBatchesForDb(
      CatalogServiceCatalog catalog, long eventId, String dbName, String eventType)
      throws MetastoreNotificationException {
    return getNextMetastoreEventsInBatchesForDb(catalog, eventId,
        MetastoreShim.getDefaultCatalogName(), dbName, eventType);
  }

  public static List<NotificationEvent> getNextMetastoreEventsInBatchesForDb(
      CatalogServiceCatalog catalog, long eventId, String catName,
      String dbName, String eventType) throws MetastoreNotificationException {
    Preconditions.checkNotNull(eventType, "eventType is null in fetching db events");
    Preconditions.checkNotNull(catName, "catName is null in fetching db events");
    Preconditions.checkNotNull(dbName, "dbName is null in fetching db events");
    NotificationFilter filter = notificationEvent ->
        eventType.equals(notificationEvent.getEventType())
            && catName.equalsIgnoreCase(notificationEvent.getCatName())
            && dbName.equalsIgnoreCase(notificationEvent.getDbName());
    MetaDataFilter metaDataFilter = new MetaDataFilter(filter, catName, dbName);
    return getNextMetastoreEventsWithFilterInBatches(catalog, eventId, metaDataFilter,
        EVENTS_BATCH_SIZE_PER_RPC, eventType);
  }

  public static List<NotificationEvent> getNextMetastoreEventsInBatchesForTable(
      CatalogServiceCatalog catalog, long eventId, String dbName, String tblName,
      String eventType) throws MetastoreNotificationException {
    return getNextMetastoreEventsInBatchesForTable(catalog, eventId,
        MetastoreShim.getDefaultCatalogName(), dbName, tblName, eventType);
  }

  public static List<NotificationEvent> getNextMetastoreEventsInBatchesForTable(
      CatalogServiceCatalog catalog, long eventId, String catName, String dbName,
      String tblName, String eventType) throws MetastoreNotificationException {
    Preconditions.checkNotNull(eventType, "eventType is null in fetching table events");
    Preconditions.checkNotNull(catName, "catName is null in fetching table events");
    Preconditions.checkNotNull(dbName, "dbName is null in fetching table events");
    Preconditions.checkNotNull(tblName, "tblName is null in fetching table events");
    NotificationFilter filter = notificationEvent ->
        eventType.equals(notificationEvent.getEventType())
            && catName.equalsIgnoreCase(notificationEvent.getCatName())
            && dbName.equalsIgnoreCase(notificationEvent.getDbName())
            && tblName.equalsIgnoreCase(notificationEvent.getTableName());
    MetaDataFilter metaDataFilter = new MetaDataFilter(filter, catName, dbName, tblName);
    return getNextMetastoreEventsWithFilterInBatches(catalog, eventId, metaDataFilter,
        EVENTS_BATCH_SIZE_PER_RPC, eventType);
  }

  public static List<NotificationEvent> getNextMetastoreEventsInBatches(
      CatalogServiceCatalog catalog, long eventId, NotificationFilter filter,
      String... eventTypes) throws MetastoreNotificationFetchException {
    return getNextMetastoreEventsInBatches(catalog, eventId, filter,
        EVENTS_BATCH_SIZE_PER_RPC, eventTypes);
  }

  @VisibleForTesting
  public static List<NotificationEvent> getNextMetastoreEventsInBatches(
      CatalogServiceCatalog catalog, long eventId, NotificationFilter filter,
      int eventsBatchSize, String... eventTypes)
      throws MetastoreNotificationFetchException {
    MetaDataFilter metaDataFilter = new MetaDataFilter(filter);
    return getNextMetastoreEventsWithFilterInBatches(catalog, eventId, metaDataFilter,
        eventsBatchSize, eventTypes);
  }

      /**
       * Gets the next list of {@link NotificationEvent} from Hive Metastore which are
       * greater than the given eventId and filtered according to the provided filter.
       * @param catalog The CatalogServiceCatalog used to get the metastore client
       * @param eventId The eventId after which the events are needed.
       * @param metaDataFilter The {@link MetaDataFilter} used to filter the list of
       *               fetched events based on catName/dbName/tableName and then filter
       *               by required event types. Note that this is a server side filter.
       * @param eventsBatchSize the batch size for fetching the events from metastore.
       * @return List of {@link NotificationEvent} which are all greater than eventId and
       * satisfy the given filter.
       * @throws MetastoreNotificationFetchException in case of RPC errors to metastore.
       */
  @VisibleForTesting
  public static List<NotificationEvent> getNextMetastoreEventsWithFilterInBatches(
      CatalogServiceCatalog catalog, long eventId, MetaDataFilter metaDataFilter,
      int eventsBatchSize, String... eventTypes)
      throws MetastoreNotificationFetchException {
    Preconditions.checkArgument(eventsBatchSize > 0);
    List<NotificationEvent> result = new ArrayList<>();
    NotificationFilter filter = metaDataFilter.getNotificationFilter();
    try (MetaStoreClient msc = catalog.getMetaStoreClient()) {
      long toEventId = msc.getHiveClient().getCurrentNotificationEventId()
          .getEventId();
      if (toEventId <= eventId) return result;
      long currentEventId = eventId;
      List<String> eventTypeSkipList = catalog.getDefaultSkippedHmsEventTypes();
      String typeStr = null;
      if (eventTypes != null && eventTypes.length > 0) {
        eventTypeSkipList = Lists.newArrayList(catalog.getCommonHmsEventTypes());
        eventTypeSkipList.removeIf(s -> Arrays.asList(eventTypes).contains(s));
        typeStr = String.join(",", eventTypes) + " ";
      }
      LOG.info("Fetching {}events started from id {} to {}. Gap: {}",
          typeStr == null ? "" : typeStr, eventId, toEventId,
          toEventId - eventId);
      int numFilteredEvents = 0;
      while (currentEventId < toEventId) {
        int batchSize = Math
            .min(eventsBatchSize, (int)(toEventId - currentEventId));
        // we don't call the HiveMetaStoreClient's getNextNotification()
        // call here because it can throw a IllegalStateException if the eventId
        // which we pass in is very old and metastore has already cleaned up
        // the events since that eventId.
        NotificationEventRequest eventRequest = new NotificationEventRequest();
        eventRequest.setMaxEvents(batchSize);
        eventRequest.setLastEvent(currentEventId);
        // Need to set table/db names in the request according to the filter
        MetastoreShim.setNotificationEventRequestWithFilter(eventRequest,
            metaDataFilter);
        NotificationEventResponse notificationEventResponse =
            MetastoreShim.getNextNotification(msc.getHiveClient(), eventRequest,
                eventTypeSkipList);
        if (notificationEventResponse.getEvents().isEmpty()) {
          // Possible to receive empty list due to event skip list in request
          break;
        }

        for (NotificationEvent event : notificationEventResponse.getEvents()) {
          // if no filter is provided we add all the events
          if (filter == null || filter.accept(event)) {
            result.add(event);
          } else {
            numFilteredEvents++;
          }
          currentEventId = event.getEventId();
        }
      }
      if (numFilteredEvents > 0) {
        LOG.info("Got {} events and filtered out {} locally from {} events start " +
                "from id {}",
            result.size(), numFilteredEvents, toEventId - eventId, eventId + 1);
      }
      return result;
    } catch (MetastoreClientInstantiationException | TException e) {
      throw new MetastoreNotificationFetchException(String.format(
          CatalogOpExecutor.HMS_RPC_ERROR_FORMAT_STR, "getNextNotification"), e);
    }
  }

  /**
   * Sync table to latest event id starting from last synced
   * event id.
   * @param catalog
   * @param tbl: Catalog table to be synced
   * @param eventFactory
   * @throws CatalogException
   * @throws MetastoreNotificationException
   */
  public static void syncToLatestEventId(CatalogServiceCatalog catalog,
      org.apache.impala.catalog.Table tbl, EventFactory eventFactory, Metrics metrics)
      throws CatalogException, MetastoreNotificationException {
    Preconditions.checkArgument(tbl != null, "tbl is null");
    Preconditions.checkState(!(tbl instanceof IncompleteTable) &&
        tbl.isLoaded(), "table %s is either incomplete or not loaded",
        tbl.getFullName());
    Preconditions.checkState(tbl.isWriteLockedByCurrentThread(),
        String.format("Write lock is not held on table %s by current thread",
            tbl.getFullName()));
    long lastEventId = tbl.getLastSyncedEventId();
    Preconditions.checkArgument(lastEventId > 0, "lastEvent " +
        " Id %s for table %s should be greater than 0", lastEventId, tbl.getFullName());

    String annotation = String.format("sync table %s to latest HMS event id",
        tbl.getFullName());
    try(ThreadNameAnnotator tna = new ThreadNameAnnotator(annotation)) {
      MetaDataFilter metaDataFilter;
      // For ACID tables, events may include commit_txn and abort_txn which doesn't have
      // db_name and table_name. So it makes sense to fetch all the events and filter
      // them in catalogD.
      if (AcidUtils.isTransactionalTable(tbl)) {
        metaDataFilter = new MetaDataFilter(getTableNotificationEventFilter(tbl));
      } else {
        metaDataFilter = new MetaDataFilter(getTableNotificationEventFilter(tbl),
            MetastoreShim.getDefaultCatalogName(), tbl.getDb().getName(), tbl.getName());
      }
      List<NotificationEvent> events = getNextMetastoreEventsWithFilterInBatches(catalog,
          lastEventId, metaDataFilter, EVENTS_BATCH_SIZE_PER_RPC);

      if (events.isEmpty()) {
        LOG.debug("table {} synced till event id {}. No new HMS events to process from "
                + "event id: {}", tbl.getFullName(), lastEventId, lastEventId + 1);
        return;
      }
      MetastoreEvents.MetastoreEvent currentEvent = null;
      for (NotificationEvent event : events) {
        currentEvent = eventFactory.get(event, metrics);
        LOG.trace("for table {}, processing event {}", tbl.getFullName(), currentEvent);
        currentEvent.processIfEnabled();
        if (currentEvent.isDropEvent()) {
          // currentEvent can only be DropPartition or DropTable
          Preconditions.checkNotNull(currentEvent.getDbName());
          Preconditions.checkNotNull(currentEvent.getTableName());
          String key = DeleteEventLog.getTblKey(currentEvent.getDbName(),
              currentEvent.getTableName());
          catalog.getMetastoreEventProcessor().getDeleteEventLog()
              .addRemovedObject(currentEvent.getEventId(), key);
        }
        if (currentEvent instanceof MetastoreEvents.DropTableEvent) {
          // return after processing table drop event
          return;
        }
      }
      // setting HMS Event ID after all the events
      // are successfully processed only if table was
      // not dropped
      // Certain events like alter_table, do an incremental reload which sets the event
      // id to the current hms event id at that time. Therefore, check table's last
      // synced event id again before setting currentEvent id
      if (currentEvent.getEventId() > tbl.getLastSyncedEventId()) {
        tbl.setLastSyncedEventId(currentEvent.getEventId());
      }
      LOG.info("Synced table {} till HMS event:  {}", tbl.getFullName(),
          tbl.getLastSyncedEventId());
    }
  }

  /**
   * Sync database to latest event id starting from the last synced
   * event id
   * @param catalog
   * @param db
   * @param eventFactory
   * @throws CatalogException
   * @throws MetastoreNotificationException
   */
  public static void syncToLatestEventId(CatalogServiceCatalog catalog,
      org.apache.impala.catalog.Db db, EventFactory eventFactory, Metrics metrics)
      throws CatalogException, MetastoreNotificationException {
    Preconditions.checkArgument(db != null, "db is null");
    long lastEventId = db.getLastSyncedEventId();
    Preconditions.checkArgument(lastEventId > 0, "Invalid " +
        "last synced event ID %s for db %s ", lastEventId, db.getName());
    Preconditions.checkState(db.isLockHeldByCurrentThread(),
        "Current thread does not hold lock on db: %s", db.getName());

    String annotation = String.format("sync db %s to latest HMS event id", db.getName());
    try(ThreadNameAnnotator tna = new ThreadNameAnnotator(annotation)) {
      MetaDataFilter metaDataFilter = new MetaDataFilter(
          getDbNotificationEventFilter(db), MetastoreShim.getDefaultCatalogName(),
          db.getName());
      List<NotificationEvent> events = getNextMetastoreEventsWithFilterInBatches(catalog,
          lastEventId, metaDataFilter, EVENTS_BATCH_SIZE_PER_RPC);

      if (events.isEmpty()) {
        LOG.debug("db {} already synced till event id: {}, no new hms events from "
            + "event id: {}", db.getName(), lastEventId, lastEventId+1);
        return;
      }

      MetastoreEvents.MetastoreEvent currentEvent = null;
      for (NotificationEvent event : events) {
        currentEvent = eventFactory.get(event, metrics);
        LOG.trace("for db {}, processing event: {}", db.getName(), currentEvent);
        currentEvent.processIfEnabled();
        if (currentEvent.isDropEvent()) {
          Preconditions.checkState(currentEvent instanceof DropDatabaseEvent,
              "invalid drop event {} ", currentEvent);
          Preconditions.checkNotNull(currentEvent.getDbName());
          String key = DeleteEventLog.getDbKey(currentEvent.getDbName());
          catalog.getMetastoreEventProcessor().getDeleteEventLog()
              .addRemovedObject(currentEvent.getEventId(), key);
          // return after processing drop db event
          return;
        }
      }
      // setting HMS Event Id after all the events
      // are successfully processed only if db was not dropped
      db.setLastSyncedEventId(currentEvent.getEventId());
      LOG.info("Synced db {} till HMS event {}", db.getName(),
          currentEvent);
    }
  }

  /*
  This filter is used when syncing events for a table to the latest HMS event id.
  It filters all events except db related ones.
   */
  private static NotificationFilter getTableNotificationEventFilter(Table tbl) {
    NotificationFilter filter = new NotificationFilter() {
      @Override
      public boolean accept(NotificationEvent event) {
        if (event.getDbName() != null && event.getTableName() != null) {
          return tbl.getDb().getName().equalsIgnoreCase(event.getDbName()) &&
              tbl.getName().equalsIgnoreCase(event.getTableName());
        }
        // filter all except db events
        return event.getDbName() == null;
      }
    };
    return filter;
  }

  /*
  This filter is used when syncing db to the latest HMS event id. The
  filter accepts all events except table related ones
   */
  @VisibleForTesting
  public static NotificationFilter getDbNotificationEventFilter(Db db) {
    NotificationFilter filter = new NotificationFilter() {
      @Override
      public boolean accept(NotificationEvent event) {
        if (event.getDbName() != null && event.getTableName() == null) {
          return db.getName().equalsIgnoreCase(event.getDbName());
        }
        // filter all events except table events
        return event.getTableName() == null;
      }
    };
    return filter;
  }

  // possible status of event processor
  public enum EventProcessorStatus {
    PAUSED, // event processor is paused because catalog is being reset concurrently
    ACTIVE, // event processor is scheduled at a given frequency
    ERROR, // event processor is in error state and event processing has stopped
    NEEDS_INVALIDATE, // event processor could not resolve certain events and needs a
    // manual invalidate command to reset the state (See AlterEvent for a example)
    STOPPED, // event processor is shutdown. No events will be processed
    DISABLED // event processor is not configured to run
  }

  // supported commands for :event_processor(), e.g.
  //   :event_processor('pause');
  //   :event_processor('start', -1);
  public enum EventProcessorCmdType {
    PAUSE,
    START,
    STATUS,
  }

  // current status of this event processor
  private EventProcessorStatus eventProcessorStatus_ = EventProcessorStatus.STOPPED;

  // error message when event processor comes into ERROR/NEEDS_INVALIDATE states
  private String eventProcessorErrorMsg_ = null;

  // event factory which is used to get or create MetastoreEvents
  private final MetastoreEventFactory metastoreEventFactory_;

  // keeps track of the current event that we are processing
  private NotificationEvent currentEvent_;
  private List<NotificationEvent> currentEventBatch_;
  private MetastoreEvent currentFilteredEvent_;
  private List<MetastoreEvent> currentFilteredEvents_;
  private long currentBatchStartTimeMs_ = 0;
  private long currentEventStartTimeMs_ = 0;
  private int currentEventIndex_ = 0;

  // keeps track of the last event id which we have synced to
  private final AtomicLong lastSyncedEventId_ = new AtomicLong(-1);
  private final AtomicLong lastSyncedEventTimeSecs_ = new AtomicLong(0);

  // The event id and eventTime of the latest event in HMS. Only used in metrics to show
  // how far we are lagging behind.
  private final AtomicLong latestEventId_ = new AtomicLong(0);
  private final AtomicLong latestEventTimeSecs_ = new AtomicLong(0);

  // The duration in nanoseconds of the processing of the last event batch.
  private final AtomicLong lastEventProcessDurationNs_ = new AtomicLong(0);

  // polling interval in milliseconds. Note this is a time we wait AFTER each fetch call
  private final long pollingFrequencyInMilliSec_;

  // Event executor service is used when hierarchical event processing is enabled
  private EventExecutorService eventExecutorService_ = null;

  // catalog service instance to be used while processing events
  protected final CatalogServiceCatalog catalog_;

  // scheduler daemon thread executor for processing events at given frequency
  private final ScheduledExecutorService processEventsScheduler_ =
      Executors.newSingleThreadScheduledExecutor(
          new ThreadFactoryBuilder()
              .setDaemon(true)
              .setNameFormat("MetastoreEventsProcessor-ProcessEvents")
              .build());

  // scheduler daemon thread executor to update the latest event id at given frequency
  private final ScheduledExecutorService updateEventIdScheduler_ =
      Executors.newSingleThreadScheduledExecutor(
          new ThreadFactoryBuilder()
              .setDaemon(true)
              .setNameFormat("MetastoreEventsProcessor-UpdateEventId")
              .build());

  // metrics registry to keep track of metrics related to event processing
  //TODO create a separate singleton class which wraps around this so that we don't
  // have to pass it around as a argument in constructor in MetastoreEvents
  private final Metrics metrics_ = new Metrics();

  // When events processing is ACTIVE this delete event log is used to keep track of
  // DROP events for databases, tables and partitions so that the MetastoreEventsProcessor
  // can ignore the drop events when they are received later.
  private final DeleteEventLog deleteEventLog_ = new DeleteEventLog();

  // Sleep interval when waiting for HMS events to be synced.
  private final int hmsEventSyncSleepIntervalMs_;

  @VisibleForTesting
  MetastoreEventsProcessor(CatalogOpExecutor catalogOpExecutor, long startSyncFromId,
      long pollingFrequencyInMilliSec) throws CatalogException {
    Preconditions.checkState(pollingFrequencyInMilliSec > 0);
    this.catalog_ = Preconditions.checkNotNull(catalogOpExecutor.getCatalog());
    validateConfigs();
    lastSyncedEventId_.set(startSyncFromId);
    lastSyncedEventTimeSecs_.set(getEventTimeFromHMS(startSyncFromId));
    initMetrics();
    metastoreEventFactory_ = new MetastoreEventFactory(catalogOpExecutor);
    pollingFrequencyInMilliSec_ = pollingFrequencyInMilliSec;
    hmsEventSyncSleepIntervalMs_ = BackendConfig.INSTANCE
        .getBackendCfg().hms_event_sync_sleep_interval_ms;
    Preconditions.checkState(hmsEventSyncSleepIntervalMs_ > 0,
        "hms_event_sync_sleep_interval_ms must be positive");
    if (BackendConfig.INSTANCE.isHierarchicalEventProcessingEnabled()) {
      int numDbEventExecutor = BackendConfig.INSTANCE.getNumDbEventExecutors();
      int numTableEventExecutor =
          BackendConfig.INSTANCE.getNumTableEventExecutorsPerDbEventExecutor();
      Preconditions.checkArgument(numDbEventExecutor > 0,
          "num_db_event_executors should be positive: %s", numDbEventExecutor);
      Preconditions.checkArgument(numTableEventExecutor > 0,
          "num_table_event_executors_per_db_event_executor should be positive: %s",
          numTableEventExecutor);
      eventExecutorService_ = new EventExecutorService(this, numDbEventExecutor,
          numTableEventExecutor);
    }
  }

  /**
   * Fetches the required metastore config values and validates them against the
   * expected values. The configurations to validate are different for HMS-2 v/s HMS-3
   * and hence it uses MetastoreShim to get the configurations which need to be validated.
   * @throws CatalogException if one or more validations fail or if metastore is not
   * accessible
   */
  @VisibleForTesting
  void validateConfigs() throws CatalogException {
    List<ValidationResult> validationErrors = new ArrayList<>();
    for (MetastoreEventProcessorConfig config : getEventProcessorConfigsToValidate()) {
      String configKey = config.getValidator().getConfigKey();
      try {
        String value = getConfigValueFromMetastore(configKey, "");
        ValidationResult result = config.validate(value);
        if (!result.isValid()) validationErrors.add(result);
      } catch (TException e) {
        String msg = String.format("Unable to get configuration %s from metastore. Check "
            + "if metastore is accessible", configKey);
        LOG.error(msg, e);
        throw new CatalogException(msg);
      }
    }
    if (!validationErrors.isEmpty()) {
      LOG.error("Found {} incorrect metastore configuration(s).",
          validationErrors.size());
      for (ValidationResult invalidConfig: validationErrors) {
        LOG.error(invalidConfig.getReason());
      }
      throw new CatalogException(String.format("Found %d incorrect metastore "
          + "configuration(s). Events processor cannot start. See ERROR log for more "
          + "details.", validationErrors.size()));
    }
  }

  public DeleteEventLog getDeleteEventLog() { return deleteEventLog_; }

  /**
   * Returns the list of Metastore configurations to validate depending on the hive
   * version
   */
  public static List<MetastoreEventProcessorConfig> getEventProcessorConfigsToValidate() {
    return Arrays.asList(MetastoreEventProcessorConfig.FIRE_EVENTS_FOR_DML,
        MetastoreEventProcessorConfig.METASTORE_DEFAULT_CATALOG_NAME);
  }

  private void initMetrics() {
    metrics_.addTimer(EVENTS_FETCH_DURATION_METRIC);
    metrics_.addTimer(EVENTS_PROCESS_DURATION_METRIC);
    metrics_.addMeter(EVENTS_RECEIVED_METRIC);
    metrics_.addCounter(EVENTS_SKIPPED_METRIC);
    metrics_.addGauge(STATUS_METRIC, (Gauge<String>) () -> getStatus().toString());
    metrics_.addGauge(LAST_SYNCED_ID_METRIC, (Gauge<Long>) lastSyncedEventId_::get);
    metrics_.addGauge(LAST_SYNCED_EVENT_TIME,
        (Gauge<Long>) lastSyncedEventTimeSecs_::get);
    metrics_.addGauge(GREATEST_SYNCED_EVENT_ID,
        (Gauge<Long>) this::getGreatestSyncedEventId);
    metrics_.addGauge(GREATEST_SYNCED_EVENT_TIME,
        (Gauge<Long>) this::getGreatestSyncedEventTime);
    metrics_.addGauge(LATEST_EVENT_ID, (Gauge<Long>) latestEventId_::get);
    metrics_.addGauge(LATEST_EVENT_TIME, (Gauge<Long>) latestEventTimeSecs_::get);
    metrics_.addGauge(EVENT_PROCESSING_DELAY,
        (Gauge<Long>) () -> latestEventTimeSecs_.get() - getGreatestSyncedEventTime());
    metrics_.addCounter(NUMBER_OF_TABLE_REFRESHES);
    metrics_.addCounter(NUMBER_OF_PARTITION_REFRESHES);
    metrics_.addCounter(NUMBER_OF_TABLES_ADDED);
    metrics_.addCounter(NUMBER_OF_TABLES_REMOVED);
    metrics_.addCounter(NUMBER_OF_DATABASES_ADDED);
    metrics_.addCounter(NUMBER_OF_DATABASES_REMOVED);
    metrics_.addCounter(NUMBER_OF_PARTITIONS_ADDED);
    metrics_.addCounter(NUMBER_OF_PARTITIONS_REMOVED);
    metrics_
        .addGauge(DELETE_EVENT_LOG_SIZE, (Gauge<Integer>) deleteEventLog_::size);
    metrics_.addCounter(NUMBER_OF_BATCH_EVENTS);
    metrics_.addTimer(AVG_DELAY_IN_CONSUMING_EVENTS);
    metrics_.addGauge(OUTSTANDING_EVENT_COUNT,
        (Gauge<Long>) this::getOutstandingEventCount);
  }

  /**
   * Gets the count of events pending to process when hierarchical mode is enabled. Always
   * return 0 if hierarchical mode is disabled.
   * @return Outstanding event count
   */
  public long getOutstandingEventCount() {
    if (!isHierarchicalEventProcessingEnabled()) return 0;
    return eventExecutorService_.getOutstandingEventCount();
  }

  /**
   * Determines whether hierarchical event processing is enabled. EventExecutorService
   * exists when hierarchical event processing is enabled.
   * @return True, if hierarchical event processing is enabled. False, otherwise.
   */
  public boolean isHierarchicalEventProcessingEnabled() {
    return eventExecutorService_ != null;
  }

  /**
   * Schedules the daemon thread at a given frequency. It is important to note that this
   * method schedules with FixedDelay instead of FixedRate. The reason it is scheduled at
   * a fixedDelay is to make sure that we don't pile up the pending tasks in case each
   * polling operation is taking longer than the given frequency. Because of the fixed
   * delay, the new poll operation is scheduled at the time when previousPoll operation
   * completes + givenDelayInSec
   */
  @Override
  public synchronized void start() {
    Preconditions.checkState(eventProcessorStatus_ != EventProcessorStatus.ACTIVE);
    resetProgressInfo();
    if (isHierarchicalEventProcessingEnabled()) {
      eventExecutorService_.start();
    }
    startScheduler();
    updateStatus(EventProcessorStatus.ACTIVE);
    LOG.info(String.format("Successfully started metastore event processing."
        + " Polling interval: %d milliseconds.", pollingFrequencyInMilliSec_));
  }

  /**
   * Gets the current event processor status
   */
  public EventProcessorStatus getStatus() {
    return eventProcessorStatus_;
  }

  /**
   * Whether metastore event can be processed in current status.
   * @return True if event can be processed. False Otherwise.
   */
  public boolean canProcessEventInCurrentStatus() {
    return (eventProcessorStatus_ == EventProcessorStatus.ACTIVE) ||
        (eventProcessorStatus_ == EventProcessorStatus.PAUSED);
  }

  /**
   * This method is only for testing.
   * @return EventExecutorService
   */
  @VisibleForTesting
  public @Nullable EventExecutorService getEventExecutorService() {
    return eventExecutorService_;
  }

  /**
   * This method is only for testing and must not be used anywhere else.
   * @param eventExecutorService
   */
  @VisibleForTesting
  public void setEventExecutorService(EventExecutorService eventExecutorService) {
    Preconditions.checkArgument(eventExecutorService != null);
    eventExecutorService_ = eventExecutorService;
  }

  /**
   * This method ensure all the outstanding events are processed on DbEventExecutor and
   * TableEventExecutor threads within the time limit.
   * @param timeLimit Time bound in milliseconds
   * @return True, if the all events are processed. False, Otherwise
   */
  boolean ensureEventsProcessedInHierarchicalMode(int timeLimit) {
    // Time limit is to avoid infinite loop if the exception occur in any event processing
    // and that event’s onFailure() return false and auto global invalidation also cannot
    // happen.
    int sleepTime = 5;
    int maxLoopTimes = timeLimit / sleepTime;
    // Ensure all the outstanding events are processed
    while (maxLoopTimes-- > 0 && getOutstandingEventCount() > 0) {
      Uninterruptibles.sleepUninterruptibly(sleepTime, TimeUnit.MILLISECONDS);
    }
    if (maxLoopTimes == 0) {
      LOG.warn("Failed to process all the outstanding events within {}",
          PrintUtils.printTimeMs(timeLimit));
      return false;
    }
    return true;
  }

  /**
   * Returns the value for a given config key from Hive Metastore.
   * @param config Hive configuration name
   * @param defaultVal Default value to return if config not present in Hive
   */
  @VisibleForTesting
  public String getConfigValueFromMetastore(String config, String defaultVal)
      throws TException {
    try (MetaStoreClient metaStoreClient = catalog_.getMetaStoreClient()) {
      IMetaStoreClient iMetaStoreClient = metaStoreClient.getHiveClient();
      return MetaStoreUtil.getMetastoreConfigValue(iMetaStoreClient, config, defaultVal);
    }
  }

  /**
   * returns the current value of LastSyncedEventId.
   */
  public long getLastSyncedEventId() {
    return lastSyncedEventId_.get();
  }

  /**
   * Gets the current value of lastSyncedEventTimeSecs.
   * @return
   */
  public long getLastSyncedEventTime() {
    return lastSyncedEventTimeSecs_.get();
  }

  /**
   * Gets the greatest synced event id. Greatest synced event is the latest event such
   * that all events with id less than or equal to the latest event are definitely synced.
   * When hierarchical event processing is not enabled, it is the same as the last synced
   * event id.
   * @return Event id of the greatest synced event
   */
  public long getGreatestSyncedEventId() {
    if (isHierarchicalEventProcessingEnabled()) {
      return eventExecutorService_.getGreatestSyncedEventId();
    }
    return getLastSyncedEventId();
  }

  /**
   * Gets the greatest synced event time. Greatest synced event is the latest event such
   * that all events with id less than or equal to the latest event are definitely synced.
   * When hierarchical event processing is not enabled, it is the same as the last synced
   * event time.
   * @return Time of the greatest synced event
   */
  public long getGreatestSyncedEventTime() {
    if (isHierarchicalEventProcessingEnabled()) {
      return eventExecutorService_.getGreatestSyncedEventTime();
    }
    return getLastSyncedEventTime();
  }

  /**
   * Calculates the number of metastore events that are pending synchronization up to the
   * specified {@code latestEventId}.
   * @param latestEventId Latest event id available on HMS
   * @return Number of events that need to be synchronized. Returns 0 if all events up to
   *         {@code latestEventId} have already been synchronized.
   */
  public long getPendingEventCount(long latestEventId) {
    if (isHierarchicalEventProcessingEnabled()) {
      return eventExecutorService_.getPendingEventCount(latestEventId);
    }
    long lastSyncedEventId = lastSyncedEventId_.get();
    if (latestEventId <= lastSyncedEventId) return 0;
    return latestEventId - lastSyncedEventId;
  }

  /**
   * Returns the current value of latestEventId_. This method is not thread-safe and
   * only to be used for testing purposes
   */
  @VisibleForTesting
  public long getLatestEventId() {
    return latestEventId_.get();
  }

  @VisibleForTesting
  void startScheduler() {
    Preconditions.checkState(pollingFrequencyInMilliSec_ > 0);
    long interval = pollingFrequencyInMilliSec_;
    LOG.info(
        String.format("Starting metastore event polling with interval %d ms.", interval));
    processEventsScheduler_.scheduleAtFixedRate(this::processEvents, interval, interval,
        TimeUnit.MILLISECONDS);
    // Update latestEventId in another thread in case that the processEvents() thread is
    // blocked by slow metadata reloading or waiting for table locks.
    updateEventIdScheduler_.scheduleAtFixedRate(this::updateLatestEventId, interval,
        interval, TimeUnit.MILLISECONDS);
  }

  /**
   * Stops the event processing and changes the status of event processor to
   * <code>EventProcessorStatus.PAUSED</code>. No new events will be processed as long
   * the status is stopped. If this event processor is actively processing events when
   * stop is called, this method blocks until the current processing is complete
   */
  @Override
  public synchronized void pause() {
    // when concurrent invalidate metadata are running, it is possible the we receive
    // a pause method call on a already paused events processor.
    if (eventProcessorStatus_ == EventProcessorStatus.PAUSED) {
      return;
    }
    updateStatus(EventProcessorStatus.PAUSED);
    LOG.info("Event processing is paused. {} synced event id is {}",
        isHierarchicalEventProcessingEnabled() ? "Greatest" : "Last",
        getGreatestSyncedEventId());
  }

  /**
   * Gracefully pauses the event processor by setting its status to
   * <code>EventProcessorStatus.PAUSED</code>.
   * <p>
   * Ensures that all currently fetched events from HMS are processed before the processor
   * is fully paused.
   * No new events will be fetched for processing while the processor remains in the
   * paused state.
   */
  @Override
  public void pauseGracefully() {
    pause();
    LOG.info("Process already fetched events if any");
    if (isHierarchicalEventProcessingEnabled()) {
      ensureEventsProcessedInHierarchicalMode(3600000);
    } else {
      boolean isNeedWait = isCurrentFilteredEventsExist();
      while (isNeedWait) {
        Uninterruptibles.sleepUninterruptibly(5, TimeUnit.MILLISECONDS);
        isNeedWait = isCurrentFilteredEventsExist();
      }
    }
    LOG.info("Event processing is paused. Greatest synced event id is {}",
        getGreatestSyncedEventId());
  }

  /**
   * Determines whether current filtered metastore events exists.
   * @return
   */
  public synchronized boolean isCurrentFilteredEventsExist() {
    return currentFilteredEvents_ != null && !currentFilteredEvents_.isEmpty();
  }

  /**
   * Resets the current filtered metastore events.
   */
  public synchronized void resetCurrentFilteredEvents() {
    currentFilteredEvents_ = null;
  }

  /**
   * Populates the current filtered metastore events from the given HMS notification
   * events.
   * @param events List of NotificationEvent
   * @throws MetastoreNotificationException
   */
  public synchronized void populateCurrentFilteredEvents(List<NotificationEvent> events)
      throws MetastoreNotificationException {
    currentFilteredEvents_ = Collections.emptyList();
    // Do not create metastore events for this batch of HMS events if status is not active
    if (eventProcessorStatus_ != EventProcessorStatus.ACTIVE) return;
    currentFilteredEvents_ = metastoreEventFactory_.getFilteredEvents(events, metrics_);
  }

  /**
   * Get the current notification event id from metastore
   */
  @Override
  public long getCurrentEventId() throws MetastoreNotificationFetchException {
    try (MetaStoreClient metaStoreClient = catalog_.getMetaStoreClient()) {
      return metaStoreClient.getHiveClient().getCurrentNotificationEventId().getEventId();
    } catch (MetastoreClientInstantiationException | TException e) {
      throw new MetastoreNotificationFetchException("Unable to fetch the current " +
          "notification event id. Check if metastore service is accessible", e);
    }
  }

  public static long getCurrentEventIdNoThrow(IMetaStoreClient client) {
    long latestEventId = -1L;
    try {
      latestEventId = client.getCurrentNotificationEventId().getEventId();
    } catch (TException exception) {
      LOG.warn(String.format("Unable to fetch latest event id from HMS: %s",
          exception.getMessage()));
    }
    return latestEventId;
  }

  /**
   * Fetch the event from HMS specified by 'eventId'
   * @return null if the event has been cleaned up or any error occurs.
   */
  private NotificationEvent getEventFromHMS(MetaStoreClient msClient, long eventId) {
    NotificationEventRequest eventRequest = new NotificationEventRequest();
    eventRequest.setLastEvent(eventId - 1);
    eventRequest.setMaxEvents(1);
    try {
      NotificationEventResponse response = MetastoreShim.getNextNotification(
          msClient.getHiveClient(), eventRequest, null);
      Iterator<NotificationEvent> eventIter = response.getEventsIterator();
      if (!eventIter.hasNext()) {
        LOG.warn("Unable to fetch event {}. It has been cleaned up", eventId);
        return null;
      }
      return eventIter.next();
    } catch (TException e) {
      LOG.warn("Unable to fetch event {}", eventId, e);
    }
    return null;
  }

  /**
   * Get the event time by fetching the specified event from HMS.
   * @return 0 if the event has been cleaned up or any error occurs.
   */
  @VisibleForTesting
  public int getEventTimeFromHMS(long eventId) {
    try (MetaStoreClient msClient = catalog_.getMetaStoreClient()) {
      NotificationEvent event = getEventFromHMS(msClient, eventId);
      if (event != null) return event.getEventTime();
    } catch (MetastoreClientInstantiationException e) {
      LOG.error("Failed to get event time from HMS for event {}", eventId, e);
    }
    return 0;
  }

  /**
   * Starts the event processor from a given event id
   */
  @Override
  public synchronized void start(long fromEventId) {
    Preconditions.checkArgument(fromEventId >= 0);
    EventProcessorStatus currentStatus = eventProcessorStatus_;
    long prevLastSyncedEventId = getGreatestSyncedEventId();
    if (currentStatus == EventProcessorStatus.ACTIVE) {
      // if events processor is already active, we should make sure that the
      // start event id provided is not behind the lastSyncedEventId. This could happen
      // when there are concurrent invalidate metadata calls. if we detect such a case
      // we should return here.
      if (prevLastSyncedEventId >= fromEventId) {
        return;
      }
    }
    // Clear the error message of the last failure and reset the progress info
    eventProcessorErrorMsg_ = null;
    resetProgressInfo();
    // Clear delete event log
    deleteEventLog_.garbageCollect(fromEventId);
    lastSyncedEventId_.set(fromEventId);
    lastSyncedEventTimeSecs_.set(getEventTimeFromHMS(fromEventId));
    if (isHierarchicalEventProcessingEnabled()) eventExecutorService_.start();
    updateStatus(EventProcessorStatus.ACTIVE);
    LOG.info(String.format(
        "Metastore event processing restarted. Last synced event id was updated "
            + "from %d to %d", prevLastSyncedEventId, lastSyncedEventId_.get()));
  }

  /**
   * Clear the event processor if it has any pending events available on event executor
   * service when hierarchical mode is enabled.
   */
  @Override
  public void clear() {
    if (isHierarchicalEventProcessingEnabled()) {
      eventExecutorService_.clear();
    }
  }

  /**
   * Stops the event processing and shuts down the scheduler. In case there is a batch of
   * events which is being processed currently, the
   * <code>processEvents</code> method releases lock after every event is processed.
   * Hence, it is possible that at-least 1 event from the batch be
   * processed while shutdown() waits to acquire lock on this object.
   */
  @Override
  public synchronized void shutdown() {
    Preconditions.checkState(eventProcessorStatus_ != EventProcessorStatus.STOPPED,
        "Event processing is already stopped");
    shutdownAndAwaitTermination(processEventsScheduler_);
    shutdownAndAwaitTermination(updateEventIdScheduler_);
    if (isHierarchicalEventProcessingEnabled()) {
      eventExecutorService_.shutdown(true);
    }
    updateStatus(EventProcessorStatus.STOPPED);
    LOG.info("Metastore event processing stopped.");
  }

  /**
   * Attempts to cleanly shutdown the scheduler pool. If the pool does not shutdown
   * within timeout, does a force shutdown which might interrupt currently running tasks.
   */
  public static void shutdownAndAwaitTermination(ScheduledExecutorService ses) {
    Preconditions.checkNotNull(ses);
    ses.shutdown(); // disable new tasks from being submitted
    try {
      // wait for 10 secs for scheduler to complete currently running tasks
      if (!ses.awaitTermination(SCHEDULER_SHUTDOWN_TIMEOUT, TimeUnit.SECONDS)) {
        // executor couldn't terminate and timed-out, force the termination
        LOG.info(String.format("Scheduler pool did not terminate within %d seconds. "
            + "Attempting to stop currently running tasks", SCHEDULER_SHUTDOWN_TIMEOUT));
        ses.shutdownNow();
      }
    } catch (InterruptedException e) {
      // current thread interrupted while pool was waiting for termination
      // issue a shutdownNow before returning to cancel currently running tasks
      LOG.info("Received interruptedException. Terminating currently running tasks.", e);
      ses.shutdownNow();
    }
  }

  /**
   * Gets metastore notification events from the given eventId. The returned list of
   * NotificationEvents are filtered using the NotificationFilter provided if it is not
   * null.
   * @param eventId The returned events are all after this given event id.
   * @param currentEventId Current event id on metastore
   * @param getAllEvents If this is true all the events since eventId are returned.
   *                     Note that Hive MetaStore can limit the response to a specific
   *                     maximum number of limit based on the value of configuration
   *                     {@code hive.metastore.max.event.response}.
   *                     If it is false, only EVENTS_BATCH_SIZE_PER_RPC events are
   *                     returned, caller is expected to issue more calls to this method
   *                     to fetch the remaining events.
   * @param filter This is a nullable argument. If not null, the events are filtered
   *               and then returned using this. Otherwise, all the events are returned.
   * @return List of NotificationEvents from metastore since eventId.
   * @throws MetastoreNotificationFetchException In case of exceptions from HMS.
   */
  public List<NotificationEvent> getNextMetastoreEvents(final long eventId,
      final long currentEventId, final boolean getAllEvents,
      @Nullable final NotificationFilter filter)
      throws MetastoreNotificationFetchException {
    // no new events since we last polled
    if (currentEventId <= eventId) {
      return Collections.emptyList();
    }
    final Timer.Context context = metrics_.getTimer(EVENTS_FETCH_DURATION_METRIC).time();
    try (MetaStoreClient msClient = catalog_.getMetaStoreClient()) {
      int batchSize = getAllEvents ? -1 : EVENTS_BATCH_SIZE_PER_RPC;
      // we use the thrift API directly instead of
      // HiveMetastoreClient#getNextNotification because the HMS client can throw an
      // exception when there is a gap between the eventIds returned.
      NotificationEventRequest eventRequest = new NotificationEventRequest();
      eventRequest.setLastEvent(eventId);
      eventRequest.setMaxEvents(batchSize);
      // Currently filter is always null. No need to set table/db in the request object
      NotificationEventResponse response =
          MetastoreShim.getNextNotification(msClient.getHiveClient(), eventRequest,
              catalog_.getDefaultSkippedHmsEventTypes());
      LOG.info("Received {} events. First event id: {}.", response.getEvents().size(),
          (response.getEvents().size() > 0 ? response.getEvents().get(0).getEventId() :
                                             "none"));
      if (filter == null) return response.getEvents();
      List<NotificationEvent> filteredEvents = new ArrayList<>();
      for (NotificationEvent event : response.getEvents()) {
        if (filter.accept(event)) filteredEvents.add(event);
      }
      return filteredEvents;
    } catch (MetastoreClientInstantiationException | TException e) {
      throw new MetastoreNotificationFetchException(
          "Unable to fetch notifications from metastore. Last synced event id is "
              + eventId, e);
    } finally {
      context.stop();
    }
  }

  /**
   * Fetch the next batch of NotificationEvents from metastore. The default batch size is
   * <code>EVENTS_BATCH_SIZE_PER_RPC</code>
   */
  @VisibleForTesting
  protected List<NotificationEvent> getNextMetastoreEvents()
      throws MetastoreNotificationFetchException {
    return getNextMetastoreEvents(getCurrentEventId());
  }

  /**
   * Fetch the next batch of NotificationEvents from metastore. The default batch size is
   * <code>EVENTS_BATCH_SIZE_PER_RPC</code>
   * @param currentEventId Current event id on metastore
   * @return List of NotificationEvents from metastore since lastSyncedEventId
   * @throws MetastoreNotificationFetchException
   */
  @VisibleForTesting
  public List<NotificationEvent> getNextMetastoreEvents(long currentEventId)
      throws MetastoreNotificationFetchException {
    return getNextMetastoreEvents(lastSyncedEventId_.get(), currentEventId, false, null);
  }

  /**
   * This method issues a request to Hive Metastore if needed, based on the current event
   * id in metastore and the last synced event_id. Events are fetched in fixed sized
   * batches. Each NotificationEvent received is processed by its corresponding
   * <code>MetastoreEvent</code>
   */
  @Override
  public void processEvents() {
    try {
      EventProcessorStatus currentStatus = eventProcessorStatus_;
      if (currentStatus != EventProcessorStatus.ACTIVE) {
        LOG.warn(String.format(
            "Event processing is skipped since status is %s. Last synced event id is %d",
            currentStatus, lastSyncedEventId_.get()));
        tryAutoGlobalInvalidateOnFailure();
        return;
      }
      long eventCount = getOutstandingEventCount();
      if (eventCount >= BackendConfig.INSTANCE.getMaxOutstandingEventsOnExecutors()) {
        LOG.warn("Outstanding events to process exceeds threshold. Event count: {}",
            eventCount);
        return;
      }
      // fetch the current notification event id. We assume that the polling interval
      // is small enough that most of these polling operations result in zero new
      // events. In such a case, fetching current notification event id is much faster
      // (and cheaper on HMS side) instead of polling for events directly
      long currentEventId = getCurrentEventId();
      List<NotificationEvent> events = getNextMetastoreEvents(currentEventId);
      processEvents(currentEventId, events);
    } catch (Exception ex) {
      handleEventProcessException(ex, currentEvent_);
    } finally {
      if (isHierarchicalEventProcessingEnabled()) {
        // Do maintenance cleanup to remove idle DbProcessors if any, regardless of
        // exception occurrence or not.
        eventExecutorService_.cleanup();
      }
    }
  }

  /**
   * Updates event processor status and invalidates global metadata iff
   * invalidate_global_metadata_on_event_processing_failure flag is true. If the flag
   * is false, user need to issue invalidate metadata command to clear event processor
   * (which in turn clears event executor service) and start event processor.
   * Note: Event processor status is not changed if exception occurred is
   * MetastoreNotificationFetchException, since event processor and event executor
   * service will continue to process events once the HMS is UP again.
   * @param ex Exception in event processing
   * @param event HMS Notification event when the exception occurred
   */
  public void handleEventProcessException(Exception ex, NotificationEvent event) {
    if (ex instanceof MetastoreNotificationFetchException) {
      // No need to change the EventProcessor state to error since we want the
      // EventProcessor to continue getting new events after HMS is back up.
      LOG.error("Unable to fetch the next batch of metastore events. Hive Metastore " +
        "may be unavailable. Will retry.", ex);
    } else if (ex instanceof MetastoreNotificationNeedsInvalidateException) {
      updateStatus(EventProcessorStatus.NEEDS_INVALIDATE);
      String msg = "Event processing needs an invalidate command to resolve the state";
      LOG.error(msg, ex);
      eventProcessorErrorMsg_ = LocalDateTime.now().toString() + '\n' + msg + '\n' +
          ExceptionUtils.getStackTrace(ex);
      tryAutoGlobalInvalidateOnFailure();
    } else {
      // There are lot of Preconditions which can throw RuntimeExceptions when we
      // process events this catch all exception block is needed so that the scheduler
      // thread does not die silently
      updateStatus(EventProcessorStatus.ERROR);
      String msg = "Unexpected exception received while processing event";
      LOG.error(msg, ex);
      dumpEventInfoToLog(event, LocalDateTime.now().toString() + '\n' + msg +
          '\n' + ExceptionUtils.getStackTrace(ex));
      tryAutoGlobalInvalidateOnFailure();
    }
  }

  /**
   * This method does global invalidation when
   * invalidate_global_metadata_on_event_processing_failure flag is enabled and the
   * event processor is in error state or need invalidate state
   */
  private void tryAutoGlobalInvalidateOnFailure() {
    EventProcessorStatus currentStatus = eventProcessorStatus_;
    if (BackendConfig.INSTANCE.isInvalidateGlobalMetadataOnEventProcessFailureEnabled()
        && ((currentStatus == EventProcessorStatus.ERROR)
               || (currentStatus == EventProcessorStatus.NEEDS_INVALIDATE))) {
      try {
        LOG.error("Triggering auto global invalidation");
        catalog_.reset(NoOpEventSequence.INSTANCE);
        eventProcessorErrorMsg_ = null;
      } catch (Exception e) {
        // Catching generic exception so that scheduler thread does not die silently
        LOG.error("Automatic global invalidate metadata failed", e);
      }
    }
  }

  /**
   * Update the latest event id regularly so we know how far we are lagging behind.
   */
  @VisibleForTesting
  public void updateLatestEventId() {
    EventProcessorStatus currentStatus = eventProcessorStatus_;
    if (currentStatus == EventProcessorStatus.DISABLED) {
      return;
    }
    try (MetaStoreClient msClient = catalog_.getMetaStoreClient()) {
      CurrentNotificationEventId currentNotificationEventId =
          msClient.getHiveClient().getCurrentNotificationEventId();
      long currentEventId = currentNotificationEventId.getEventId();
      // no new events since we last polled
      if (currentEventId <= latestEventId_.get()) {
        return;
      }
      // Fetch the last event to get its eventTime.
      NotificationEvent event = getEventFromHMS(msClient, currentEventId);
      // Events could be empty if they are just cleaned up.
      if (event == null) return;
      long lastSyncedEventId = getGreatestSyncedEventId();
      long lastSyncedEventTime = getGreatestSyncedEventTime();
      long currentEventTime = event.getEventTime();
      latestEventId_.set(currentEventId);
      latestEventTimeSecs_.set(currentEventTime);
      LOG.info("Latest event in HMS: id={}, time={}. {} synced event: id={}, time={}.",
          currentEventId, currentEventTime,
          isHierarchicalEventProcessingEnabled() ? "Greatest" : "Last",
          lastSyncedEventId, lastSyncedEventTime);
      if (currentEventTime > lastSyncedEventTime) {
        LOG.warn("Lag: {}. Approximately {} events pending to be processed.",
            PrintUtils.printTimeMs((currentEventTime - lastSyncedEventTime) * 1000),
            getPendingEventCount(currentEventId));
      }
    } catch (Exception e) {
      LOG.error("Unable to update current notification event id. Last value: {}",
          latestEventId_, e);
    }
  }

  /**
   * Gets the current event processor metrics along with its status. If the status is
   * not active the metrics are skipped. Only the status is sent
   */
  @Override
  public TEventProcessorMetrics getEventProcessorMetrics() {
    TEventProcessorMetrics eventProcessorMetrics = new TEventProcessorMetrics();
    EventProcessorStatus currentStatus = getStatus();
    eventProcessorMetrics.setStatus(currentStatus.toString());
    eventProcessorMetrics.setLast_synced_event_id(getGreatestSyncedEventId());
    eventProcessorMetrics.setGreatest_synced_event_id(getGreatestSyncedEventId());
    if (currentStatus != EventProcessorStatus.ACTIVE) return eventProcessorMetrics;
    // The following counters are only updated when event-processor is active.
    eventProcessorMetrics.setLast_synced_event_time(getGreatestSyncedEventTime());
    eventProcessorMetrics.setGreatest_synced_event_time(getGreatestSyncedEventTime());
    eventProcessorMetrics.setPending_event_count(
        getPendingEventCount(latestEventId_.get()));
    eventProcessorMetrics.setLatest_event_id(latestEventId_.get());
    eventProcessorMetrics.setLatest_event_time(latestEventTimeSecs_.get());

    long eventsReceived = metrics_.getMeter(EVENTS_RECEIVED_METRIC).getCount();
    long eventsSkipped = metrics_.getCounter(EVENTS_SKIPPED_METRIC).getCount();
    eventProcessorMetrics.setEvents_received(eventsReceived);
    eventProcessorMetrics.setEvents_skipped(eventsSkipped);
    eventProcessorMetrics.setOutstanding_event_count(getOutstandingEventCount());

    Snapshot fetchDuration =
        metrics_.getTimer(EVENTS_FETCH_DURATION_METRIC).getSnapshot();
    double avgFetchDuration = fetchDuration.getMean() / SECOND_IN_NANOS;
    double p75FetchDuration = fetchDuration.get75thPercentile() / SECOND_IN_NANOS;
    double p95FetchDuration = fetchDuration.get95thPercentile() / SECOND_IN_NANOS;
    double p99FetchDuration = fetchDuration.get99thPercentile() / SECOND_IN_NANOS;
    eventProcessorMetrics.setEvents_fetch_duration_mean(avgFetchDuration);
    eventProcessorMetrics.setEvents_fetch_duration_p75(p75FetchDuration);
    eventProcessorMetrics.setEvents_fetch_duration_p95(p95FetchDuration);
    eventProcessorMetrics.setEvents_fetch_duration_p99(p99FetchDuration);

    Snapshot processDuration =
        metrics_.getTimer(EVENTS_PROCESS_DURATION_METRIC).getSnapshot();
    double avgProcessDuration = processDuration.getMean() / SECOND_IN_NANOS;
    double p75ProcessDuration = processDuration.get75thPercentile() / SECOND_IN_NANOS;
    double p95ProcessDuration = processDuration.get95thPercentile() / SECOND_IN_NANOS;
    double p99ProcessDuration = processDuration.get99thPercentile() / SECOND_IN_NANOS;
    eventProcessorMetrics.setEvents_process_duration_mean(avgProcessDuration);
    eventProcessorMetrics.setEvents_process_duration_p75(p75ProcessDuration);
    eventProcessorMetrics.setEvents_process_duration_p95(p95ProcessDuration);
    eventProcessorMetrics.setEvents_process_duration_p99(p99ProcessDuration);

    double lastProcessDuration = lastEventProcessDurationNs_.get() /
        (double) SECOND_IN_NANOS;
    eventProcessorMetrics.setLast_events_process_duration(lastProcessDuration);

    double avgNumberOfEventsReceived1Min =
        metrics_.getMeter(EVENTS_RECEIVED_METRIC).getOneMinuteRate();
    double avgNumberOfEventsReceived5Min =
        metrics_.getMeter(EVENTS_RECEIVED_METRIC).getFiveMinuteRate();
    double avgNumberOfEventsReceived15Min =
        metrics_.getMeter(EVENTS_RECEIVED_METRIC).getFifteenMinuteRate();
    eventProcessorMetrics.setEvents_received_1min_rate(avgNumberOfEventsReceived1Min);
    eventProcessorMetrics.setEvents_received_5min_rate(avgNumberOfEventsReceived5Min);
    eventProcessorMetrics.setEvents_received_15min_rate(avgNumberOfEventsReceived15Min);

    LOG.trace("Events Received: {} Events skipped: {} Avg fetch duration: {} Avg process "
            + "duration: {} Events received rate (1min) : {}",
        eventsReceived, eventsSkipped, avgFetchDuration, avgProcessDuration,
        avgNumberOfEventsReceived1Min);
    return eventProcessorMetrics;
  }

  @Override
  public TEventProcessorMetricsSummaryResponse getEventProcessorSummary(
      TEventProcessorMetricsSummaryRequest req) {
    TEventProcessorMetricsSummaryResponse summaryResponse =
        new TEventProcessorMetricsSummaryResponse();
    summaryResponse.setSummary(metrics_.toString());
    if (eventProcessorErrorMsg_ != null) {
      summaryResponse.setError_msg(eventProcessorErrorMsg_);
    }
    TEventBatchProgressInfo progressInfo = new TEventBatchProgressInfo();
    progressInfo.last_synced_event_id = getGreatestSyncedEventId();
    progressInfo.last_synced_event_time_s = getGreatestSyncedEventTime();
    progressInfo.latest_event_id = latestEventId_.get();
    progressInfo.latest_event_time_s = latestEventTimeSecs_.get();
    if (req.get_latest_event_from_hms) {
      try (MetaStoreClient metaStoreClient = catalog_.getMetaStoreClient()) {
        progressInfo.latest_event_id =
            metaStoreClient.getHiveClient().getCurrentNotificationEventId().getEventId();
        progressInfo.latest_event_time_s = -1;
      } catch (MetastoreClientInstantiationException | TException e) {
        progressInfo.latest_event_id = - 1;
        progressInfo.latest_event_time_s = -1;
        LOG.warn("Failed to fetch the latest HMS event id. Returning -1", e);
      }
    }
    // Assign these lists to local variables in case they are replaced concurrently.
    // It's best effort to make the members in 'progressInfo' consistent but we can't
    // guarantee it.
    List<NotificationEvent> eventBatch = currentEventBatch_;
    List<MetastoreEvent> filteredEvents = currentFilteredEvents_;
    if (eventBatch != null && !eventBatch.isEmpty()) {
      int numHmsEvents = eventBatch.size();
      progressInfo.num_hms_events = numHmsEvents;
      progressInfo.min_event_id = eventBatch.get(0).getEventId();
      progressInfo.min_event_time_s = eventBatch.get(0).getEventTime();
      NotificationEvent lastEvent = eventBatch.get(numHmsEvents - 1);
      progressInfo.max_event_id = lastEvent.getEventId();
      progressInfo.max_event_time_s = lastEvent.getEventTime();
      progressInfo.current_batch_start_time_ms = currentBatchStartTimeMs_;
      progressInfo.current_event_start_time_ms = currentEventStartTimeMs_;
      if (filteredEvents != null) {
        progressInfo.num_filtered_events = filteredEvents.size();
      }
      progressInfo.current_event_index = currentEventIndex_;
      progressInfo.current_event_batch_size = currentFilteredEvent_ != null ?
          currentFilteredEvent_.getNumberOfEvents() : 0;
      progressInfo.current_event = currentEvent_;
    }
    summaryResponse.setProgress(progressInfo);
    return summaryResponse;
  }

  @VisibleForTesting
  Metrics getMetrics() {
    return metrics_;
  }

  private void resetProgressInfo() {
    currentEvent_ = null;
    currentEventBatch_ = null;
    currentFilteredEvent_ = null;
    resetCurrentFilteredEvents();
    currentBatchStartTimeMs_ = 0;
    currentEventStartTimeMs_ = 0;
    currentEventIndex_ = 0;
  }

  /**
   * Process the given list of notification events. Useful for tests which provide a list
   * of events
   * @param currentEventId Current event id on metastore
   * @param events List of NotificationEvents
   * @throws MetastoreNotificationException
   */
  @VisibleForTesting
  protected void processEvents(long currentEventId, List<NotificationEvent> events)
      throws MetastoreNotificationException {
    currentEventBatch_ = events;
    boolean isHierarchical = isHierarchicalEventProcessingEnabled();
    // update the events received metric before returning
    metrics_.getMeter(EVENTS_RECEIVED_METRIC).mark(events.size());
    if (events.isEmpty()) {
      if (lastSyncedEventId_.get() < currentEventId) {
        // Possible to receive empty list due to event skip list in notification event
        // request. Update the last synced event id with current event id on metastore
        long currentEventTime = getEventTimeFromHMS(currentEventId);
        lastSyncedEventId_.set(currentEventId);
        lastSyncedEventTimeSecs_.set(currentEventTime);
        if (isHierarchical) {
          eventExecutorService_.addToProcessedLog(currentEventId, currentEventTime);
        }
      }
      return;
    }
    final Timer.Context context =
        metrics_.getTimer(EVENTS_PROCESS_DURATION_METRIC).time();
    currentBatchStartTimeMs_ = System.currentTimeMillis();
    Map<MetastoreEvent, Long> eventProcessingTime = new HashMap<>();
    try {
      populateCurrentFilteredEvents(events);
      if (currentFilteredEvents_.isEmpty()) {
        NotificationEvent e = events.get(events.size() - 1);
        lastSyncedEventId_.set(e.getEventId());
        lastSyncedEventTimeSecs_.set(e.getEventTime());
        if (isHierarchical) {
          eventExecutorService_.addToProcessedLog(e.getEventId(), e.getEventTime());
        }
        resetProgressInfo();
        return;
      }
      for (MetastoreEvent event : currentFilteredEvents_) {
        // synchronizing each event processing reduces the scope of the lock so the a
        // potential reset() during event processing is not blocked for longer than
        // necessary
        synchronized (this) {
          if (!canProcessEventInCurrentStatus()) {
            break;
          }
          currentEvent_ = event.metastoreNotificationEvent_;
          currentFilteredEvent_ = event;
          String targetName = event.getTargetName();
          String desc = String.format("%s %s on %s, eventId=%d",
              isHierarchical ? "Dispatching" : "Processing", event.getEventType(),
              targetName, event.getEventId());
          try (ThreadNameAnnotator tna = new ThreadNameAnnotator(desc)) {
            currentEventStartTimeMs_ = System.currentTimeMillis();
            if (isHierarchical) {
              eventExecutorService_.dispatch(event);
            } else {
              event.processIfEnabled();
            }
            long elapsedTimeMs = System.currentTimeMillis() - currentEventStartTimeMs_;
            eventProcessingTime.put(event, elapsedTimeMs);
          } catch (Exception processingEx) {
            try {
              if (!event.onFailure(processingEx)) {
                event.errorLog("Unable to handle event processing failure");
                throw processingEx;
              }
            } catch (Exception onFailureEx) {
              event.errorLog("Failed to handle event processing failure", onFailureEx);
              throw processingEx;
            }
          }
          currentEventIndex_++;
          if (!isHierarchical) {
            deleteEventLog_.garbageCollect(event.getEventId());
          }
          lastSyncedEventId_.set(event.getEventId());
          lastSyncedEventTimeSecs_.set(event.getEventTime());
          metrics_.getTimer(AVG_DELAY_IN_CONSUMING_EVENTS).update(
              (System.currentTimeMillis() / 1000) - event.getEventTime(),
                  TimeUnit.SECONDS);
        }
      }
      resetProgressInfo();
    } catch (CatalogException e) {
      throw new MetastoreNotificationException(String.format(
          "Unable to process event %d of type %s. Event processing will be stopped.",
          currentEvent_.getEventId(), currentEvent_.getEventType()), e);
    } finally {
      long elapsedNs = context.stop();
      lastEventProcessDurationNs_.set(elapsedNs);
      logEventMetrics(eventProcessingTime, elapsedNs);
    }
  }

  private void logEventMetrics(Map<MetastoreEvent, Long> eventProcessingTime,
      long elapsedNs) {
    boolean isHierarchical = isHierarchicalEventProcessingEnabled();
    LOG.info("Time elapsed in {} event batch: {}",
        isHierarchical ? "dispatching" : "processing",
        PrintUtils.printTimeNs(elapsedNs));
    // Only log the metrics when the processing on this batch is slow.
    if (elapsedNs < HdfsTable.LOADING_WARNING_TIME_NS) return;
    // Get the top-10 expensive events
    List<Map.Entry<MetastoreEvent, Long>> eventList =
        new ArrayList<>(eventProcessingTime.entrySet());
    eventList.sort(Map.Entry.<MetastoreEvent, Long>comparingByValue().reversed());
    int num = Math.min(10, eventList.size());
    StringBuilder report = new StringBuilder("Top " + num + " expensive events: ");
    for (Map.Entry<MetastoreEvent, Long> entry : eventList.subList(0, num)) {
      MetastoreEvent event = entry.getKey();
      long durationMs = entry.getValue();
      report.append(String.format("(type=%s, id=%s, target=%s, duration_ms=%d) ",
          event.getEventType(), event.getEventId(), event.getTargetName(), durationMs));
    }
    // Get the top-10 expensive targets
    Map<String, Long> durationPerTable = new HashMap<>();
    for (MetastoreEvent event : eventProcessingTime.keySet()) {
      String targetName = event.getTargetName();
      long durationMs = durationPerTable.getOrDefault(targetName, 0L) +
          eventProcessingTime.get(event);
      durationPerTable.put(targetName, durationMs);
    }
    List<Map.Entry<String, Long>> targetList =
        new ArrayList<>(durationPerTable.entrySet());
    targetList.sort(Map.Entry.<String, Long>comparingByValue().reversed());
    num = Math.min(10, targetList.size());
    report.append("\nTop ").append(num).append(String.format(" targets in event %s: ",
        isHierarchical ? "dispatching" : "processing"));
    for (Map.Entry<String, Long> entry : targetList.subList(0, num)) {
      String targetName = entry.getKey();
      long durationMs = entry.getValue();
      report.append(String.format("(target=%s, duration_ms=%d) ",
          targetName, durationMs));
    }
    LOG.warn(report.toString());
  }

  /**
   * Updates the current states to the given status.
   */
  @VisibleForTesting
  public synchronized void updateStatus(EventProcessorStatus toStatus) {
    eventProcessorStatus_ = toStatus;
  }

  private void dumpEventInfoToLog(NotificationEvent event, String errorMessage) {
    if (event == null) {
      String error = "Notification event is null";
      LOG.error(error);
      eventProcessorErrorMsg_ = errorMessage + '\n' + error;
      return;
    }
    StringBuilder msg =
        new StringBuilder().append("Event id: ").append(event.getEventId()).append("\n")
            .append("Event Type: ").append(event.getEventType()).append("\n")
            .append("Event time: ").append(event.getEventTime()).append("\n")
            .append("Database name: ").append(event.getDbName()).append("\n");
    if (event.getTableName() != null) {
      msg.append("Table name: ").append(event.getTableName()).append("\n");
    }
    msg.append("Event message: ").append(event.getMessage()).append("\n");
    String msgStr = msg.toString();
    LOG.error(msgStr);
    eventProcessorErrorMsg_ = errorMessage + '\n' + msgStr;
  }

  /**
   * Create a instance of this object if it is not initialized. Currently, this object is
   * a singleton and should only be created during catalogD initialization time, so that
   * the start syncId matches with the catalogD startup time.
   *
   * @param catalogOpExecutor the CatalogOpExecutor instance to which this event
   *     processor belongs.
   * @param startSyncFromId Start event id. Events will be polled starting from this
   *     event id
   * @param eventPollingInterval HMS polling interval in milliseconds
   * @return this object is already created, or create a new one if it is not yet
   *     instantiated
   */
  public static synchronized ExternalEventsProcessor getInstance(
      CatalogOpExecutor catalogOpExecutor, long startSyncFromId,
      long eventPollingInterval) throws CatalogException {
    if (instance != null) return instance;
    instance =
        new MetastoreEventsProcessor(catalogOpExecutor, startSyncFromId,
            eventPollingInterval);
    return instance;
  }

  @Override
  public MetastoreEventFactory getEventsFactory() {
    return metastoreEventFactory_;
  }

  public static MessageDeserializer getMessageDeserializer() {
    return MESSAGE_DESERIALIZER;
  }

  public static class MetaDataFilter {
    public static final List<String> DB_EVENT_TYPES = Lists.newArrayList(
        AlterDatabaseEvent.EVENT_TYPE, CreateDatabaseEvent.EVENT_TYPE,
        DropDatabaseEvent.EVENT_TYPE);
    // Event types required to check for a SHOW TABLES statement. ALTER_DATABASE is
    // required to check changes on the ownership.
    public static final List<String> TABLE_LIST_EVENT_TYPES = Lists.newArrayList(
        AlterDatabaseEvent.EVENT_TYPE, CreateDatabaseEvent.EVENT_TYPE,
        DropDatabaseEvent.EVENT_TYPE, CreateTableEvent.EVENT_TYPE,
        DropTableEvent.EVENT_TYPE);
    public static final List<String> TABLE_EXIST_EVENT_TYPES = Lists.newArrayList(
        CreateTableEvent.EVENT_TYPE, DropTableEvent.EVENT_TYPE);
    public static final List<String> TABLE_EVENT_TYPES =
        Stream.of(
            MetastoreEventType.CREATE_TABLE, MetastoreEventType.DROP_TABLE,
            MetastoreEventType.ALTER_TABLE, MetastoreEventType.ADD_PARTITION,
            MetastoreEventType.ALTER_PARTITION, MetastoreEventType.ALTER_PARTITIONS,
            MetastoreEventType.DROP_PARTITION, MetastoreEventType.INSERT,
            MetastoreEventType.INSERT_PARTITIONS, MetastoreEventType.RELOAD,
            MetastoreEventType.COMMIT_COMPACTION_EVENT
        ).map(MetastoreEventType::toString).collect(Collectors.toList());
    public NotificationFilter filter_;
    public String catName_;
    public String dbName_;
    public List<String> tableNames_;

    public MetaDataFilter(NotificationFilter notificationFilter) {
      this.filter_ = notificationFilter; // if this null then don't build event filter
    }

    public MetaDataFilter(NotificationFilter notificationFilter, String catName,
        String dbName) {
      this(notificationFilter);
      this.catName_ = Preconditions.checkNotNull(catName);
      this.dbName_ = Preconditions.checkNotNull(dbName);
    }

    public MetaDataFilter(NotificationFilter notificationFilter, String catName,
        String databaseName, String tblName) {
      this(notificationFilter, catName, databaseName);
      if (tblName != null && !tblName.isEmpty()) {
        this.tableNames_ = Arrays.asList(tblName);
      }
    }

    public MetaDataFilter(NotificationFilter notificationFilter, String catName,
        String databaseName, List<String> tblNames) {
      this(notificationFilter, catName, databaseName);
      if (tblNames != null && !tblNames.isEmpty()) {
        this.tableNames_ = tblNames;
      }
    }

    public void setNotificationFilter(NotificationFilter notificationFilter) {
      this.filter_ = notificationFilter;
    }

    public NotificationFilter getNotificationFilter() {
      return filter_;
    }

    public String getCatName() {
      return catName_;
    }

    public String getDbName() {
      return dbName_;
    }

    public List<String> getTableNames() {
      return tableNames_;
    }
  }

  /**
   * Wait until the catalog doesn't have stale metadata that could be used by the query.
   * The min required event id is calculated based on the pending HMS events on requested
   * db/table names. We then wait until that event is processed by the EventProcessor.
   */
  public TStatus waitForMostRecentMetadata(TWaitForHmsEventRequest req) {
    long timeoutMs = req.timeout_s * 1000L;
    TStatus res = new TStatus();
    // Only waits when event-processor is in ACTIVE/PAUSED states. PAUSED states happen
    // at startup or when global invalidate is running, so it's ok to wait for.
    if (!canProcessEventInCurrentStatus()) {
      res.setStatus_code(TErrorCode.GENERAL);
      res.addToError_msgs(
          "Current state of HMS event processor is " + eventProcessorStatus_);
      return res;
    }
    long waitForEventId;
    long latestEventId;
    try {
      latestEventId = getCurrentEventId();
    } catch (MetastoreNotificationFetchException e) {
      res.setStatus_code(TErrorCode.GENERAL);
      res.addToError_msgs("Failed to fetch current HMS event id: " + e.getMessage());
      return res;
    }
    try {
      waitForEventId = getMinEventIdToWaitFor(req);
    } catch (Throwable e) {
      LOG.warn("Failed to check the min required event id to wait for. Fallback to use " +
          "the latest event id {}. Query might wait longer than it needs.",
          latestEventId, e);
      waitForEventId = latestEventId;
    }
    long lastSyncedEventId = getGreatestSyncedEventId();
    long startMs = System.currentTimeMillis();
    long sleepIntervalMs = Math.min(timeoutMs, hmsEventSyncSleepIntervalMs_);
    // Avoid too many log entries if the waiting interval is smaller than 500ms.
    int logIntervals = Math.max(1, 1000 / hmsEventSyncSleepIntervalMs_);
    int numIters = 0;
    while (lastSyncedEventId < waitForEventId
        && System.currentTimeMillis() - startMs < timeoutMs) {
      if (numIters++ % logIntervals == 0) {
        LOG.info("Waiting for last synced event id ({}) to reach {}",
            lastSyncedEventId, waitForEventId);
      }
      Uninterruptibles.sleepUninterruptibly(sleepIntervalMs, TimeUnit.MILLISECONDS);
      lastSyncedEventId = getGreatestSyncedEventId();
      if (!canProcessEventInCurrentStatus()) {
        res.setStatus_code(TErrorCode.GENERAL);
        res.addToError_msgs(
            "Current state of HMS event processor is " + eventProcessorStatus_);
        return res;
      }
    }
    if (lastSyncedEventId < waitForEventId) {
      res.setStatus_code(TErrorCode.GENERAL);
      res.addToError_msgs(String.format("Timeout waiting for HMS events to be synced. " +
          "Event id to wait for: %d. Last synced event id: %d",
          waitForEventId, lastSyncedEventId));
      return res;
    }
    LOG.info("Last synced event id ({}) reached {}", lastSyncedEventId, waitForEventId);
    res.setStatus_code(TErrorCode.OK);
    return res;
  }

  /**
   * Find the min required event id that should be synced to avoid the query using
   * stale metadata.
   */
  private long getMinEventIdToWaitFor(TWaitForHmsEventRequest req)
      throws MetastoreNotificationFetchException, TException, CatalogException {
    if (req.should_expand_views) expandViews(req);
    // requiredEventId starts from the last synced event id. While checking pending
    // events of a target, we just fetch events after requiredEventId, since events
    // before it are decided to be waited for.
    long requiredEventId = getGreatestSyncedEventId();
    if (req.want_db_list) {
      return getMinRequiredEventIdForDbList(requiredEventId);
    }
    if (req.isSetObject_descs()) {
      // Step 1: Collect db/table names requested by the query.
      // Group table names by the db name. HMS events on tables in the same db can be
      // checked by one RPC.
      Set<String> dbNames = new HashSet<>();
      Map<String, List<String>> db2Tables = new HashMap<>();
      for (TCatalogObject catalogObject: req.getObject_descs()) {
        if (catalogObject.isSetDb()) {
          dbNames.add(catalogObject.getDb().db_name);
        } else if (catalogObject.isSetTable()) {
          TTable table = catalogObject.getTable();
          if (catalog_.getDb(table.db_name) == null) {
            // We will check existence of missing dbs.
            dbNames.add(table.db_name);
          }
          db2Tables.computeIfAbsent(table.db_name, k -> new ArrayList<>())
              .add(table.tbl_name);
        }
      }
      // Step 2: Check DB events
      requiredEventId = getMinRequiredEventIdForDb(requiredEventId, dbNames,
          req.want_table_list);
      // Step 3: Check transactional events if there are transactional tables.
      // Such events (COMMIT_TXN, ABORT_TXN) don't have the db/table names since they
      // might modify multiple transactional tables. If there are transactional tables,
      // wait for all the transactional events.
      requiredEventId = getMinRequiredTxnEventId(requiredEventId, db2Tables);
      // Step 4: Check table events
      requiredEventId = getMinRequiredTableEventId(requiredEventId, db2Tables);
    }
    return requiredEventId;
  }

  private static void doForAllObjectNames(TWaitForHmsEventRequest req,
      Function<TTableName, Object> tblFn, @Nullable Function<String, Object> dbFn) {
    for (TCatalogObject catalogObject : req.getObject_descs()) {
      if (catalogObject.isSetTable()) {
        TTable table = catalogObject.getTable();
        tblFn.apply(new TTableName(table.db_name, table.tbl_name));
      } else if (dbFn != null && catalogObject.isSetDb()) {
        dbFn.apply(catalogObject.getDb().getDb_name());
      }
    }
  }

  /**
   * Expand views used by the query so we know what additional tables need to wait for
   * their metadata to be synced. Throws exceptions if we can't expand any views.
   */
  private void expandViews(TWaitForHmsEventRequest req)
      throws TException, CatalogException {
    if (!req.isSetObject_descs()) return;
    // Check all the table names in the request. Expand views and add underlying tables
    // into the queue if they are new.
    Queue<TTableName> uncheckedNames = new ArrayDeque<>();
    doForAllObjectNames(req, uncheckedNames::offer, /*dbFn*/null);
    Set<TTableName> checkedNames = new HashSet<>();
    String loadReason = "expand view to check HMS events on underlying tables";
    try (MetaStoreClient msClient = catalog_.getMetaStoreClient()) {
      while (!uncheckedNames.isEmpty()) {
        TTableName tblName = uncheckedNames.poll();
        checkedNames.add(tblName);
        Db db = catalog_.getDb(tblName.db_name);
        Table tbl = null;
        // Case 1: handle loaded table/views.
        // If 'tbl' is a loaded view, reload it in case the view definition is modified.
        // Note that loading a view from HMS is cheaper than checking HMS events. So we
        // always reload it regardless of whether there are pending events on it.
        if (db != null) {
          tbl = db.getTable(tblName.table_name);
          if (tbl instanceof View) {
            catalog_.reloadTable(tbl, loadReason, getLastSyncedEventId(),
                /*unused*/true, EventSequence.getUnusedTimeline());
            expandView((View) tbl, checkedNames, uncheckedNames);
            continue;
          }
          if (tbl instanceof IncompleteTable && tbl.isLoaded()) {
            throw new CatalogException(String.format("Cannot expand view %s.%s due to " +
                "failures in metadata loading", tblName.db_name, tblName.table_name),
                ((IncompleteTable) tbl).getCause());
          }
          // Ignore loaded tables.
          if (tbl != null && tbl.isLoaded()) continue;
        }
        // Case 2: handle unloaded/missing tables/views.
        // Check the metadata in HMS directly. Note that this is cheaper than checking
        // HMS events. Only trigger metadata loading for views. Unloaded tables will
        // remain unloaded which helps EventProcessor to skip most of their events.
        List<TableMeta> metaRes = null;
        try {
          metaRes = msClient.getHiveClient().getTableMeta(
              tblName.db_name, tblName.table_name, MetastoreShim.HIVE_VIEW_TYPE);
        } catch (UnknownDBException | NoSuchObjectException e) {
          // Ignore non-existing db/tables
        }
        // 'metaRes' could be null since we are just fetching views.
        if (metaRes != null && !metaRes.isEmpty() && TImpalaTableType.VIEW.equals(
            MetastoreShim.mapToInternalTableType(metaRes.get(0).getTableType()))) {
          // View exists in HMS. If it's missing in the cache, invalidate it to bring
          // it up.
          if (db == null || tbl == null) {
            catalog_.invalidateTable(tblName, /*unused*/new Reference<>(),
                /*unused*/new Reference<>(), EventSequence.getUnusedTimeline());
          }
          tbl = catalog_.getOrLoadTable(tblName.db_name, tblName.table_name, loadReason,
              /*validWriteIdList*/null);
          if (tbl instanceof View) {
            expandView((View) tbl, checkedNames, uncheckedNames);
          } else {
            // The view could be dropped concurrently or loading its metadata might fail.
            throw new CatalogException(String.format("Failed to expand view %s.%s",
                tblName.db_name, tblName.table_name));
          }
        }
      }
    }
    // Add new table names we found in view expansion to 'req'.
    // Remove table names in 'checkedNames' that already exist in 'req' and collect
    // existing db names.
    Set<String> existingDbNames = new HashSet<>();
    doForAllObjectNames(req, checkedNames::remove, existingDbNames::add);
    // Add new tables and dbs to 'req'.
    for (TTableName tblName : checkedNames) {
      TCatalogObject tblDesc = new TCatalogObject(TCatalogObjectType.TABLE, 0);
      tblDesc.setTable(new TTable(tblName.db_name, tblName.table_name));
      req.addToObject_descs(tblDesc);
      if (!existingDbNames.contains(tblName.db_name)) {
        existingDbNames.add(tblName.db_name);
        TCatalogObject dbDesc = new TCatalogObject(TCatalogObjectType.DATABASE, 0);
        dbDesc.setDb(new TDatabase(tblName.db_name));
        req.addToObject_descs(dbDesc);
      }
    }
  }

  private void expandView(View view, Set<TTableName> checkedNames,
      Queue<TTableName> uncheckedNames) throws CatalogException {
    for (TableRef tblRef : view.getQueryStmt().collectTableRefs()) {
      List<String> strs = tblRef.getPath();
      // Table names in the view definition should all be fully qualified.
      // Add a check here in case something wrong happens in Hive.
      if (strs.size() < 2) {
        String str = String.join(".", strs);
        LOG.error("Illegal table name found in view {}: {}. View definition:\n{}",
            view.getFullName(), str, view.getMetaStoreTable().getViewExpandedText());
        throw new CatalogException(String.format(
            "Illegal table name found in view %s: %s", view.getFullName(), str));
      }
      TTableName name = new TTableName(strs.get(0), strs.get(1));
      if (!checkedNames.contains(name)) {
        uncheckedNames.add(name);
        LOG.info("Found new table name used by view {}: {}.{}", view.getFullName(),
            name.db_name, name.table_name);
      }
    }
  }

  /**
   * Get the min required event id to avoid the query using stale metadata on the
   * requested dbs.
   */
  private long getMinRequiredEventIdForDb(long startEventId, Set<String> dbNames,
      boolean wantTableList) throws MetastoreNotificationFetchException {
    long requiredEventId = startEventId;
    for (String dbName : dbNames) {
      // For SHOW TABLES, also check the CREATE/DROP table events.
      List<String> eventTypes = wantTableList ?
          MetaDataFilter.TABLE_LIST_EVENT_TYPES : MetaDataFilter.DB_EVENT_TYPES;
      NotificationFilter filter = e -> dbName.equalsIgnoreCase(e.getDbName())
          && MetastoreShim.isDefaultCatalog(e.getCatName())
          && eventTypes.contains(e.getEventType());
      // Use 'requiredEventId' as the startEventId since events before it are decided
      // to wait for.
      List<NotificationEvent> dbEvents = getNextMetastoreEventsInBatches(
          catalog_, /*startEventId*/ requiredEventId, filter,
          eventTypes.toArray(new String[0]));
      if (dbEvents.isEmpty()) {
        LOG.info("No pending events found on db {}", dbName);
        continue;
      }
      NotificationEvent e = dbEvents.get(dbEvents.size() - 1);
      LOG.info("Found db {} has pending event. EventId:{} EventType:{}",
          dbName, e.getEventId(), e.getEventType());
      requiredEventId = Math.max(requiredEventId, e.getEventId());
    }
    return requiredEventId;
  }

  /**
   * Check if there are any transactional tables loaded in catalog
   */
  private boolean hasLoadedTxnTables(Map<String, List<String>> db2Tables) {
    for (String dbName : db2Tables.keySet()) {
      Db db = catalog_.getDb(dbName);
      if (db == null) continue;
      for (String tableName : db2Tables.get(dbName)) {
        Table table = db.getTable(tableName);
        if (table instanceof HdfsTable && AcidUtils.isTransactionalTable(
            table.getMetaStoreTable().getParameters())) {
          return true;
        }
      }
    }
    return false;
  }

  private boolean hasLoadedTables(Map<String, List<String>> db2Tables) {
    for (String dbName : db2Tables.keySet()) {
      Db db = catalog_.getDb(dbName);
      if (db == null) continue;
      for (String tableName : db2Tables.get(dbName)) {
        Table table = db.getTable(tableName);
        if (table != null && !(table instanceof IncompleteTable)) {
          return true;
        }
      }
    }
    return false;
  }

  private long getMinRequiredTxnEventId(long startEventId,
      Map<String, List<String>> db2Tables) throws MetastoreNotificationFetchException {
    if (!hasLoadedTxnTables(db2Tables)) return startEventId;
    NotificationFilter filter = e ->
        MetastoreShim.CommitTxnEvent.EVENT_TYPE.equals(e.getEventType())
            || MetastoreEvents.AbortTxnEvent.EVENT_TYPE.equals(e.getEventType());
    List<NotificationEvent> txnEvents = getNextMetastoreEventsInBatches(catalog_,
        startEventId, filter, MetastoreShim.CommitTxnEvent.EVENT_TYPE,
        MetastoreEvents.AbortTxnEvent.EVENT_TYPE);
    if (!txnEvents.isEmpty()) {
      NotificationEvent e = txnEvents.get(txnEvents.size() - 1);
      LOG.info("Some of the requested tables are transactional tables. Found {} " +
          "pending transactional events. The last event is EventId: {} " +
          "EventType: {}", txnEvents.size(), e.getEventId(), e.getEventType());
      return e.getEventId();
    }
    LOG.info("Some of the requested tables are transactional tables. " +
        "No pending transactional events found.");
    return startEventId;
  }

  private long getMinRequiredTableEventId(long startEventId,
      Map<String, List<String>> db2Tables) throws MetastoreNotificationFetchException {
    long requiredEventId = startEventId;
    // If all requested tables are unloaded, just check CREATE/DROP table events.
    List<String> eventTypes = hasLoadedTables(db2Tables) ?
        MetaDataFilter.TABLE_EVENT_TYPES : MetaDataFilter.TABLE_EXIST_EVENT_TYPES;
    for (String dbName : db2Tables.keySet()) {
      Set<String> tblNames = new HashSet<>(db2Tables.get(dbName));
      // Due to HIVE-28912 we don't check catName in the filter
      NotificationFilter filter = e -> dbName.equalsIgnoreCase(e.getDbName())
          && tblNames.contains(e.getTableName().toLowerCase())
          // Due to HIVE-28912 we don't check catName in the filter
          // && MetastoreShim.isDefaultCatalog(e.getCatName())
          && eventTypes.contains(e.getEventType());
      MetaDataFilter metaDataFilter = new MetaDataFilter(filter,
          MetastoreShim.getDefaultCatalogName(), dbName, db2Tables.get(dbName));
      List<NotificationEvent> tableEvents = getNextMetastoreEventsWithFilterInBatches(
          catalog_, requiredEventId, metaDataFilter, EVENTS_BATCH_SIZE_PER_RPC,
          eventTypes.toArray(new String[0]));
      if (tableEvents.isEmpty()) {
        LOG.info("No pending events on specified tables under db {}", dbName);
        continue;
      }
      NotificationEvent e = tableEvents.get(tableEvents.size() - 1);
      requiredEventId = Math.max(requiredEventId, e.getEventId());
      LOG.info("Found {} pending events on table {} of db {}. The last event is " +
              "EventId: {} EventType: {} Table: {}",
          tableEvents.size(), String.join(",", db2Tables.get(dbName)), dbName,
          e.getEventId(), e.getEventType(), e.getTableName());
    }
    return requiredEventId;
  }

  /**
   * Get the min required event id to avoid the query using stale db list.
   */
  private long getMinRequiredEventIdForDbList(long startEventId)
      throws MetastoreNotificationFetchException {
    NotificationFilter filter = e -> MetastoreShim.isDefaultCatalog(e.getCatName())
        && MetaDataFilter.DB_EVENT_TYPES.contains(e.getEventType());
    List<NotificationEvent> dbEvents = getNextMetastoreEventsInBatches(
        catalog_, startEventId, filter,
        MetaDataFilter.DB_EVENT_TYPES.toArray(new String[0]));
    if (dbEvents.isEmpty()) {
      LOG.info("No events need to be synced for db list");
      return startEventId;
    }
    NotificationEvent e = dbEvents.get(dbEvents.size() - 1);
    LOG.info("Found {} pending events on db list. The last one is EventId: {} " +
            "EventType: {} db: {}", dbEvents.size(), e.getEventId(), e.getEventType(),
        e.getDbName());
    return e.getEventId();
  }
}
