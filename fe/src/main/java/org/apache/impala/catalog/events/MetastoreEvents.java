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

import com.codahale.metrics.Timer;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Objects;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.stream.Collectors;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.NotificationEvent;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.metastore.api.TxnToWriteId;
import org.apache.hadoop.hive.metastore.messaging.AbortTxnMessage;
import org.apache.hadoop.hive.metastore.messaging.AddPartitionMessage;
import org.apache.hadoop.hive.metastore.messaging.AllocWriteIdMessage;
import org.apache.hadoop.hive.metastore.messaging.AlterPartitionMessage;
import org.apache.hadoop.hive.metastore.messaging.CreateTableMessage;
import org.apache.hadoop.hive.metastore.messaging.DropPartitionMessage;
import org.apache.hadoop.hive.metastore.messaging.json.JSONAlterDatabaseMessage;
import org.apache.hadoop.hive.metastore.messaging.InsertMessage;
import org.apache.hadoop.hive.metastore.messaging.json.JSONAlterTableMessage;
import org.apache.hadoop.hive.metastore.messaging.json.JSONCreateDatabaseMessage;
import org.apache.hadoop.hive.metastore.messaging.json.JSONDropDatabaseMessage;
import org.apache.hadoop.hive.metastore.messaging.json.JSONDropTableMessage;
import org.apache.hadoop.hive.metastore.utils.MetaStoreUtils;
import org.apache.impala.analysis.TableName;
import org.apache.impala.catalog.CatalogException;
import org.apache.impala.catalog.CatalogServiceCatalog;
import org.apache.impala.catalog.DatabaseNotFoundException;
import org.apache.impala.catalog.Db;
import org.apache.impala.catalog.FileMetadataLoadOpts;
import org.apache.impala.catalog.HdfsTable;
import org.apache.impala.catalog.IncompleteTable;
import org.apache.impala.catalog.MetastoreClientInstantiationException;
import org.apache.impala.catalog.TableLoadingException;
import org.apache.impala.catalog.TableNotFoundException;
import org.apache.impala.catalog.TableNotLoadedException;
import org.apache.impala.catalog.TableWriteId;
import org.apache.impala.common.Metrics;
import org.apache.impala.common.PrintUtils;
import org.apache.impala.common.Reference;
import org.apache.impala.compat.MetastoreShim;
import org.apache.impala.hive.common.MutableValidWriteIdList;

import static org.apache.impala.catalog.Table.TBL_EVENTS_PROCESS_DURATION;

import org.apache.impala.service.BackendConfig;
import org.apache.impala.service.CatalogOpExecutor;
import org.apache.impala.thrift.TPartitionKeyValue;
import org.apache.impala.thrift.TTableName;
import org.apache.impala.util.AcidUtils;
import org.apache.impala.util.DebugUtils;
import org.apache.impala.util.MetaStoreUtil;
import org.apache.impala.util.NoOpEventSequence;
import org.slf4j.LoggerFactory;
import org.slf4j.Logger;

/**
 * Main class which provides Metastore event objects for various event types. Also
 * provides a MetastoreEventFactory to get or create the event instances for a given event
 * type
 */
public class MetastoreEvents {

  /**
   * This enum contains keys for parameters added in Metastore entities, relevant for
   * event processing. When eventProcessor is instantiated, we make sure during config
   * validation that these parameters are not filtered out through the Metastore config
   * EVENT_NOTIFICATION_PARAMETERS_EXCLUDE_PATTERNS.
   */
  public enum MetastoreEventPropertyKey {
    // key to be used for catalog version in table properties for detecting self-events
    CATALOG_VERSION("impala.events.catalogVersion"),
    // key to be used for catalog service id for detecting self-events
    CATALOG_SERVICE_ID("impala.events.catalogServiceId"),
    // flag to be set in the table/database parameters to disable event based metadata
    // sync. Note the this is a user-facing property. Any changes to this key name
    // will break backwards compatibility
    DISABLE_EVENT_HMS_SYNC("impala.disableHmsSync");

    private String key_;

    MetastoreEventPropertyKey(String key) { this.key_ = key; }

    public String getKey() { return key_; }
  }

  public enum MetastoreEventType {
    CREATE_TABLE("CREATE_TABLE"),
    DROP_TABLE("DROP_TABLE"),
    ALTER_TABLE("ALTER_TABLE"),
    CREATE_DATABASE("CREATE_DATABASE"),
    DROP_DATABASE("DROP_DATABASE"),
    ALTER_DATABASE("ALTER_DATABASE"),
    ADD_PARTITION("ADD_PARTITION"),
    ALTER_PARTITION("ALTER_PARTITION"),
    ALTER_PARTITIONS("ALTER_PARTITIONS"),
    DROP_PARTITION("DROP_PARTITION"),
    INSERT("INSERT"),
    INSERT_PARTITIONS("INSERT_PARTITIONS"),
    RELOAD("RELOAD"),
    ALLOC_WRITE_ID_EVENT("ALLOC_WRITE_ID_EVENT"),
    COMMIT_TXN("COMMIT_TXN"),
    ABORT_TXN("ABORT_TXN"),
    COMMIT_COMPACTION("COMMIT_COMPACTION_EVENT"),
    OTHER("OTHER");

    private final String eventType_;

    MetastoreEventType(String msEventType) {
      this.eventType_ = msEventType;
    }

    @Override
    public String toString() {
      return eventType_;
    }

    /**
     * Returns the MetastoreEventType from a given string value of event from Metastore's
     * NotificationEvent.eventType. If none of the supported MetastoreEventTypes match,
     * return OTHER
     *
     * @param eventType EventType value from the <code>NotificationEvent</code>
     */
    public static MetastoreEventType from(String eventType) {
      for (MetastoreEventType metastoreEventType : values()) {
        if (metastoreEventType.eventType_.equalsIgnoreCase(eventType)) {
          return metastoreEventType;
        }
      }
      return OTHER;
    }
  }

  /**
   * Factory class to create various MetastoreEvents.
   */
  public static class MetastoreEventFactory implements EventFactory {

    private static final Logger LOG =
        LoggerFactory.getLogger(MetastoreEventFactory.class);

    // catalog service instance to be used for creating eventHandlers
    protected final CatalogServiceCatalog catalog_;
    // metrics registry to be made available for each events to publish metrics
    //protected final Metrics metrics_;
    // catalogOpExecutor needed for the create/drop events for table and database.
    protected final CatalogOpExecutor catalogOpExecutor_;
    private static MetastoreEventFactory INSTANCE = null;

    public MetastoreEventFactory(CatalogOpExecutor catalogOpExecutor) {
      this.catalogOpExecutor_ = Preconditions.checkNotNull(catalogOpExecutor);
      this.catalog_ = Preconditions.checkNotNull(catalogOpExecutor.getCatalog());
    }

    /**
     * creates instance of <code>MetastoreEvent</code> used to process a given event type.
     * If the event type is unknown, returns a IgnoredEvent
     */
    public MetastoreEvent get(NotificationEvent event, Metrics metrics)
        throws MetastoreNotificationException {
      Preconditions.checkNotNull(event.getEventType());
      MetastoreEventType metastoreEventType =
          MetastoreEventType.from(event.getEventType());
      if (BackendConfig.INSTANCE.getHMSEventIncrementalRefreshTransactionalTable()) {
        switch (metastoreEventType) {
          case ALLOC_WRITE_ID_EVENT:
            return new AllocWriteIdEvent(catalogOpExecutor_, metrics, event);
          case COMMIT_TXN:
            return new MetastoreShim.CommitTxnEvent(catalogOpExecutor_, metrics, event);
          case ABORT_TXN:
            return new AbortTxnEvent(catalogOpExecutor_, metrics, event);
        }
      }
      switch (metastoreEventType) {
        case CREATE_TABLE:
          return new CreateTableEvent(catalogOpExecutor_, metrics, event);
        case DROP_TABLE:
          return new DropTableEvent(catalogOpExecutor_, metrics, event);
        case ALTER_TABLE:
          return new AlterTableEvent(catalogOpExecutor_, metrics, event);
        case CREATE_DATABASE:
          return new CreateDatabaseEvent(catalogOpExecutor_, metrics, event);
        case DROP_DATABASE:
          return new DropDatabaseEvent(catalogOpExecutor_, metrics, event);
        case ALTER_DATABASE:
          return new AlterDatabaseEvent(catalogOpExecutor_, metrics, event);
        case ADD_PARTITION:
          return new AddPartitionEvent(catalogOpExecutor_, metrics, event);
        case DROP_PARTITION:
          return new DropPartitionEvent(catalogOpExecutor_, metrics, event);
        case ALTER_PARTITION:
          return new AlterPartitionEvent(catalogOpExecutor_, metrics, event);
        case RELOAD:
          return new ReloadEvent(catalogOpExecutor_, metrics, event);
        case INSERT:
          return new InsertEvent(catalogOpExecutor_, metrics, event);
        case COMMIT_COMPACTION:
          return new CommitCompactionEvent(catalogOpExecutor_, metrics, event);
        default:
          // ignore all the unknown events by creating a IgnoredEvent
          return new IgnoredEvent(catalogOpExecutor_, metrics, event);
      }
    }

    /**
     * Given a list of notification events, returns a list of <code>MetastoreEvent</code>
     * In case there are create events which are followed by drop events for the same
     * object, the create events are filtered out. The drop events do not need to be
     * filtered out
     *
     * This is needed to avoid the replay problem. For example, if catalog created and
     * removed a table, the create event received will try to add the object again. This
     * table will be visible until the drop table event is processed. This can be avoided
     * by "looking ahead" in the event stream to see if the table with the same name was
     * dropped. In such a case, the create event can be ignored
     *
     * @param events NotificationEvents fetched from metastore
     * @param metrics Metrics to update while filtering events
     * @return A list of MetastoreEvents corresponding to the given the NotificationEvents
     * @throws MetastoreNotificationException if a NotificationEvent could not be
     *     parsed into MetastoreEvent
     */
    List<MetastoreEvent> getFilteredEvents(List<NotificationEvent> events,
        Metrics metrics) throws MetastoreNotificationException {
      Preconditions.checkNotNull(events);
      if (events.isEmpty()) return Collections.emptyList();

      if (StringUtils.isNotEmpty(BackendConfig.INSTANCE.debugActions())) {
        DebugUtils.executeDebugAction(
            BackendConfig.INSTANCE.debugActions(), DebugUtils.GET_FILTERED_EVENTS_DELAY);
      }

      List<MetastoreEvent> metastoreEvents = new ArrayList<>(events.size());
      for (NotificationEvent event : events) {
        metastoreEvents.add(get(event, metrics));
      }
      // filter out the create events which has a corresponding drop event later
      int sizeBefore = metastoreEvents.size();
      int numFilteredEvents = 0;
      int i = 0;
      while (i < metastoreEvents.size()) {
        MetastoreEvent currentEvent = metastoreEvents.get(i);
        String catalogName = currentEvent.getCatalogName();
        String eventDb = currentEvent.getDbName();
        String eventTbl = currentEvent.getTableName();
        if (catalogName != null && !MetastoreShim.isDefaultCatalog(catalogName)) {
          // currently Impala doesn't support custom hive catalogs and hence we should
          // ignore all the events which are on non-default catalog namespaces.
          LOG.debug(currentEvent.debugString(
              "Filtering out this event since it is on a non-default hive catalog %s",
              catalogName));
          metastoreEvents.remove(i);
          numFilteredEvents++;
        } else if ((eventDb != null && catalog_.isBlacklistedDb(eventDb)) || (
            eventTbl != null && catalog_.isBlacklistedTable(eventDb, eventTbl))) {
          // if the event is on blacklisted db or table we should filter it out
          String blacklistedObject = eventTbl != null ? new TableName(eventDb,
              eventTbl).toString() : eventDb;
          LOG.info(currentEvent.debugString("Filtering out this event since it is on a "
              + "blacklisted database or table %s", blacklistedObject));
          metastoreEvents.remove(i);
          numFilteredEvents++;
        } else {
          i++;
        }
      }
      LOG.info(String.format("Total number of events received: %d Total number of events "
          + "filtered out: %d", sizeBefore, numFilteredEvents));
      if (numFilteredEvents > 0) {
        metrics.getCounter(MetastoreEventsProcessor.EVENTS_SKIPPED_METRIC)
            .inc(numFilteredEvents);
        LOG.debug("Incremented skipped metric to "
            + metrics.getCounter(MetastoreEventsProcessor.EVENTS_SKIPPED_METRIC)
                  .getCount());
      }
      return createBatchEvents(metastoreEvents, metrics);
    }

    /**
     * This method flushes all in-progress batches for tables from the specified
     * database from the pendingTableEventsMap to the sortedFinalBatches.
     */
    void flushBatchesForDb(
        Map<String, Map<String, MetastoreEvent>> pendingTableEventsMap,
        TreeMap<Long, MetastoreEvent> sortedFinalBatches, String dbName) {
      String lowerDbName = dbName.toLowerCase();
      Map<String, MetastoreEvent> dbMap = pendingTableEventsMap.get(lowerDbName);
      if (dbMap != null) {
        // Flush out any pending events in the database map and delete it
        for (MetastoreEvent event : dbMap.values()) {
          sortedFinalBatches.put(event.getEventId(), event);
        }
        pendingTableEventsMap.remove(lowerDbName);
      }
    }

    /**
     * This method flushes any in-progress batch for the specified table
     * from the pendingTableEventsMap to the sortedFinalBatches.
     */
    void flushBatchForTable(
        Map<String, Map<String, MetastoreEvent>> pendingTableEventsMap,
        TreeMap<Long, MetastoreEvent> sortedFinalBatches, Table table) {
      // Produce the lower-cased fully qualified table name
      String dbName = table.getDbName().toLowerCase();
      String tableName = table.getTableName().toLowerCase();
      Map<String, MetastoreEvent> dbMap = pendingTableEventsMap.get(dbName);
      if (dbMap != null) {
        MetastoreEvent tableEvent = dbMap.get(tableName);
        if (tableEvent != null) {
          sortedFinalBatches.put(tableEvent.getEventId(), tableEvent);
          dbMap.remove(tableName);
          // If this was the last table, delete the DB map
          if (dbMap.isEmpty()) {
            pendingTableEventsMap.remove(dbName);
          }
        }
      }
    }

    /**
     * Event batching is done on a per-table basis to allow more batching in
     * interleaved circumstances. Single-table events still follow the same rules
     * for batching, but certain events cross table boundaries and should cut
     * batches across multiple tables. This method detects cross-table events and
     * cuts the appropriate batches. Currently, it handles drop database, alter
     * database, and alter table rename. It is a no-op for events that are not
     * cross-table.
     */
    void cutBatchesForCrossTableEvents(MetastoreEvent event,
        Map<String, Map<String, MetastoreEvent>> pendingTableEventsMap,
        TreeMap<Long, MetastoreEvent> sortedFinalBatches) {
      // drop database - cuts any existing batches for tables in that database
      // alter database - cuts any existing batches for tables in the database
      // alter table rename - cuts any existing batches for the source or destination
      //   table
      if (event instanceof DropDatabaseEvent || event instanceof AlterDatabaseEvent) {
        // Any batched events for tables from this database need to be flushed
        // before emitting the AlterDatabaseEvent or DropDatabaseEvent.
        flushBatchesForDb(pendingTableEventsMap, sortedFinalBatches, event.getDbName());
      } else if (event instanceof AlterTableEvent) {
        AlterTableEvent alterTable = (AlterTableEvent) event;
        if (alterTable.isRename()) {
          // Flush any batched events for the source table.
          Table beforeTable = alterTable.getBeforeTable();
          flushBatchForTable(pendingTableEventsMap, sortedFinalBatches, beforeTable);

          // Flush any batched events for the destination table. Given that the
          // destination table must not exist when doing this rename, valid sequences
          // are already handled implicitly by the existing batch-breaking logic
          // (combined with the sorting of the final batches). This does the flushing
          // explicitly in case there are any edge cases not handled by the existing
          // mechanisms.
          Table afterTable = alterTable.getAfterTable();
          flushBatchForTable(pendingTableEventsMap, sortedFinalBatches, afterTable);
        }
      }
    }

    /**
     * This method batches together any eligible events from the given list of
     * {@code MetastoreEvent}. The returned list may or may not contain batch
     * events depending on whether it finds any events which could be batched together.
     * Events on a table do not need to be contiguous to be batched, but there must
     * not be an intervening event that cuts the batch.
     */
    @VisibleForTesting
    List<MetastoreEvent> createBatchEvents(List<MetastoreEvent> events, Metrics metrics) {
      if (events.size() < 2) return events;
      // We can batch certain events on the same table as long as there is no
      // intervening event that cuts the batch. To allow this non-contiguous batching,
      // we keep state for each table separately. This is a two-level structure with
      // the first layer keyed on the database name and the second layer keyed on the
      // table name. This makes it possible to flush all entries for a database
      // efficiently. Both database name and table name are lowercased to make this case
      // insensitive.
      //
      // Entries in this hash map are still pending and can accept more entries into
      // a batch when eligible. Each time an event is added a batch, it changes the
      // ending Event ID, so this holds the pending batches until the batch is finalized.
      // When the batch is finalized (either by an event that cuts the batch or by
      // running out of events), it is moved to the sortedFinalBatches.
      Map<String, Map<String, MetastoreEvent>> pendingTableEventsMap = new HashMap<>();

      // The output events need to be monotonically increasing in their Event IDs,
      // so we insert the resulting batches into a TreeMap and use that to produce
      // the output list. Examples:
      // 1. Basic ordering
      // Suppose there are inserts on 4 different tables (Event ID in parens):
      // A(1), B(2), C(3), D(4)
      // The sorting will emit those in the same order they arrived. This also applies
      // to any contiguous batching.
      // 2. Interleaved events
      // Suppose there are interleaved events that can be batched for different tables:
      // A(1), B(2), A(3), B(4)
      // The sorting will emit the batches in order of ascending ending Event ID, i.e.
      // A(1-3), B(2-4)
      // Since the ending Event ID of a batch changes each time an extra event is added,
      // this structure should only contain finalized batches that can't change.
      TreeMap<Long, MetastoreEvent> sortedFinalBatches = new TreeMap<>();

      for (MetastoreEvent next : events) {
        // Events that impact multiple tables need special handling to cut event batches
        // for all impacted tables. This logic is in addition to the regular branch
        // cutting logic that happens on a single-table basis.
        cutBatchesForCrossTableEvents(next, pendingTableEventsMap, sortedFinalBatches);

        if (!(next instanceof MetastoreTableEvent)) {
          // No batching for non-table events
          sortedFinalBatches.put(next.getEventId(), next);
          continue;
        }
        String dbName = next.getDbName().toLowerCase();
        String tableName = next.getTableName().toLowerCase();
        // First, lookup the dbMap or create it if it doesn't exist
        Map<String, MetastoreEvent> dbMap =
            pendingTableEventsMap.computeIfAbsent(dbName, k -> new HashMap<>());
        // Second, find the table entry in the dbMap
        MetastoreEvent current = dbMap.get(tableName);
        if (current != null) {
          // Check if the next metastore event for the table can be batched into the
          // current event for the table.
          if (!current.canBeBatched(next)) {
            // Events cannot be batched. Flush the current event in the table map to the
            // output and put the next element into the table map.
            sortedFinalBatches.put(current.getEventId(), current);
            dbMap.put(tableName, next);
          } else {
            // next can be batched into current event
            dbMap.put(tableName,
                      Preconditions.checkNotNull(current.addToBatchEvents(next)));
          }
        } else {
          // New entry for this table
          dbMap.put(tableName, next);
        }
      }
      // Flush out any pending events
      for (Map<String, MetastoreEvent> dbMap : pendingTableEventsMap.values()) {
        for (MetastoreEvent event : dbMap.values()) {
          sortedFinalBatches.put(event.getEventId(), event);
        }
      }

      // We defer logging about the batches created until the end so that we can output
      // them in the sorted order used for the actual output list.
      List<MetastoreEvent> batchedEventList =
          new ArrayList<>(sortedFinalBatches.values());
      for (MetastoreEvent event : batchedEventList) {
        if (event.getNumberOfEvents() > 1) {
          Preconditions.checkState(event instanceof BatchPartitionEvent);
          BatchPartitionEvent batchEvent = (BatchPartitionEvent) event;
          batchEvent.infoLog("Created a batch event for {} events between {} and {}",
              batchEvent.getNumberOfEvents(), batchEvent.getFirstEventId(),
              batchEvent.getLastEventId());
          metrics.getCounter(MetastoreEventsProcessor.NUMBER_OF_BATCH_EVENTS).inc();
        }
      }
      return batchedEventList;
    }
  }

  // A factory class for creating metastore events for syncing to latest event id
  // We can't reuse existing event factory because of the following scenario:
  // A ddl is executed from Impala shell. As a result of it, a self event generated
  // for it should be ignored by event factory in MetastoreEventProcessor. If
  // MetastoreEventProcessor also uses EventFactoryForSyncToLatestEvent then it would
  // skip self event check and end up re processing the event which defeats the purpose
  // of self event code. The reason for this behavior is - ddl ops from Impala shell
  // i.e catalogOpExecutor don't sync db/table to latest event id. After
  // IMPALA-10976 is merged, we can use one event factory and disable self event
  // check in that.

  public static class EventFactoryForSyncToLatestEvent extends
      MetastoreEvents.MetastoreEventFactory {

    private static final Logger LOG =
        LoggerFactory.getLogger(MetastoreEventFactory.class);

    public EventFactoryForSyncToLatestEvent(CatalogOpExecutor catalogOpExecutor) {
      super(catalogOpExecutor);
    }

    public MetastoreEvent get(NotificationEvent event, Metrics metrics)
        throws MetastoreNotificationException {
      Preconditions.checkNotNull(event.getEventType());
      MetastoreEventType metastoreEventType =
          MetastoreEventType.from(event.getEventType());
      switch (metastoreEventType) {
        case ALTER_DATABASE:
          return new AlterDatabaseEvent(catalogOpExecutor_, metrics, event) {
            @Override
            protected boolean isSelfEvent() {
              return false;
            }
          };
        case ALTER_TABLE:
          return new AlterTableEvent(catalogOpExecutor_, metrics, event) {
            @Override
            protected boolean isSelfEvent() {
              return false;
            }
          };
        case ADD_PARTITION:
          return new AddPartitionEvent(catalogOpExecutor_, metrics, event) {
            @Override
            public boolean isSelfEvent() {
              return false;
            }
          };
        case ALTER_PARTITION:
          return new AlterPartitionEvent(catalogOpExecutor_, metrics, event) {
            @Override
            public boolean isSelfEvent() {
              return false;
            }
          };
        case DROP_PARTITION:
          return new DropPartitionEvent(catalogOpExecutor_, metrics, event) {
            @Override
            public boolean isSelfEvent() {
              return false;
            }
          };
        case INSERT:
          return new InsertEvent(catalogOpExecutor_, metrics, event) {
            @Override
            public boolean isSelfEvent() {
              return false;
            }
          };
        default:
          return super.get(event, metrics);
      }
    }
  }


  /**
   * Abstract base class for all MetastoreEvents. A MetastoreEvent is a object used to
   * process a NotificationEvent received from metastore. It is self-contained with all
   * the information needed to take action on catalog based on a the given
   * NotificationEvent
   */
  public static abstract class MetastoreEvent {

    // String.format compatible string to prepend event id and type
    private static final String STR_FORMAT_EVENT_ID_TYPE = "EventId: %d EventType: %s ";

    // logger format compatible string to prepend to a log formatted message
    private static final String LOG_FORMAT_EVENT_ID_TYPE = "EventId: {} EventType: {} ";

    // CatalogServiceCatalog instance on which the event needs to be acted upon
    protected final CatalogServiceCatalog catalog_;

    protected final CatalogOpExecutor catalogOpExecutor_;

    // the notification received from metastore which is processed by this
    protected final NotificationEvent event_;

    // Logger available for all the sub-classes
    protected final Logger LOG = LoggerFactory.getLogger(this.getClass());

    // catalog name from the event
    protected final String catalogName_;

    // dbName from the event
    protected final String dbName_;

    // tblName from the event
    protected final String tblName_;

    // eventId of the event. Used instead of calling getter on event_ everytime
    private long eventId_;

    // eventType from the NotificationEvent or in case of batch events set using
    // the setter for this field
    private MetastoreEventType eventType_;

    // Actual notificationEvent object received from Metastore
    protected final NotificationEvent metastoreNotificationEvent_;

    // metrics registry so that events can add metrics
    protected final Metrics metrics_;

    protected MetastoreEvent(CatalogOpExecutor catalogOpExecutor, Metrics metrics,
        NotificationEvent event) {
      this.catalogOpExecutor_ = catalogOpExecutor;
      this.catalog_ = catalogOpExecutor_.getCatalog();
      this.event_ = event;
      this.eventId_ = event_.getEventId();
      this.eventType_ = MetastoreEventType.from(event.getEventType());
      // certain event types in Hive-3 like COMMIT_TXN may not have dbName set
      this.catalogName_ = event.getCatName();
      this.dbName_ = event.getDbName();
      this.tblName_ = event.getTableName();
      this.metastoreNotificationEvent_ = event;
      this.metrics_ = metrics;
    }

    public long getEventId() { return eventId_; }

    public long getEventTime() { return event_.getEventTime(); }

    public MetastoreEventType getEventType() { return eventType_; }

    /**
     * Certain events like {@link BatchPartitionEvent} batch a group of events
     * to create a batch event type. This method is used to override the event type
     * in such cases since the event type is not really derived from NotificationEvent.
     */
    public void setEventType(MetastoreEventType type) {
      this.eventType_ = type;
    }

    public String getCatalogName() { return catalogName_; }

    public String getDbName() { return dbName_; }

    public String getTableName() { return tblName_; }

    public String getTargetName() {
      if (dbName_ == null && tblName_ == null) return "CLUSTER_WIDE";
      if (tblName_ == null) return dbName_;
      return dbName_ + "." + tblName_;
    }

    /**
     * Method to inject error randomly for certain events during the processing.
     * It is used for testing purpose.
     */
    private void injectErrorIfNeeded() {
      if (catalog_.getFailureEventsForTesting().contains(eventType_.toString())) {
        double random = Math.random();
        if (random < BackendConfig.INSTANCE.getProcessEventFailureRatio()) {
          throw new RuntimeException("Event processing failed due to error injection");
        }
      }
    }

    /**
     * Process this event if it is enabled based on the flags on this object
     *
     * @throws CatalogException If  Catalog operations fail
     * @throws MetastoreNotificationException If NotificationEvent parsing fails
     */
    public void processIfEnabled()
        throws CatalogException, MetastoreNotificationException {
      if (isEventProcessingDisabled()) {
        infoLog("Skipping this event because of flag evaluation");
        metrics_.getCounter(MetastoreEventsProcessor.EVENTS_SKIPPED_METRIC).inc();
        debugLog("Incremented skipped metric to " + metrics_
            .getCounter(MetastoreEventsProcessor.EVENTS_SKIPPED_METRIC).getCount());
        return;
      }
      if (BackendConfig.INSTANCE.enableSyncToLatestEventOnDdls()) {
        if (shouldSkipWhenSyncingToLatestEventId()) {
          metrics_.getCounter(MetastoreEventsProcessor.EVENTS_SKIPPED_METRIC).inc();
          return;
        }
      }
      DebugUtils.executeDebugAction(
          BackendConfig.INSTANCE.debugActions(), DebugUtils.EVENT_PROCESSING_DELAY);
      process();
      injectErrorIfNeeded();
    }

    /**
     * Checks if the given event can be batched into this event. Default behavior is
     * to return false which can be overridden by a sub-class.
     *
     * @param event The event under consideration to be batched into this event.
     * @return false if event cannot be batched into this event; otherwise true.
     */
    protected boolean canBeBatched(MetastoreEvent event) { return false; }

    /**
     * Adds the given event into the batch of events represented by this event. Default
     * implementation is to return null. Sub-classes must override this method to
     * implement batching.
     *
     * @param event The event which needs to be added to the batch.
     * @return The batch event which represents all the events batched into this event
     * until now including the given event.
     */
    protected MetastoreEvent addToBatchEvents(MetastoreEvent event) { return null; }

    /**
     * Returns the number of events represented by this event. For most events this is
     * 1. In case of batch events this could be more than 1.
     */
    protected int getNumberOfEvents() { return 1; }

    /**
     * Certain events like ALTER_TABLE or ALTER_PARTITION implement logic to ignore
     * some events because they are not interesting from catalogd's perspective.
     * @return true if this event can be skipped.
     */
    protected boolean canBeSkipped() { return false; }

    /**
     * In case of batch events, this method can be used override the {@code eventType_}
     * field which is used for logging purposes.
     */
    protected MetastoreEventType getBatchEventType() { return null; }

    /**
     * Evaluates whether processing of this event should be skipped
     * if sync to latest event id is enabled. The event should
     * be skipped if the db/table is already synced atleast till
     * this event.
     * @return true if the event should be skipped. False
     *          otherwise
     * @throws CatalogException
     */
    protected abstract boolean shouldSkipWhenSyncingToLatestEventId()
        throws CatalogException;

    /**
     * Process the information available in the NotificationEvent to take appropriate
     * action on Catalog
     *
     * @throws MetastoreNotificationException in case of event parsing errors out
     * @throws CatalogException in case catalog operations could not be performed
     */
    protected abstract void process()
        throws MetastoreNotificationException, CatalogException;

    /**
     * Process event failure handles the exception occurred during processing.
     * @param e Exception occurred during process
     * @return Returns true when failure is handled. Otherwise, false
     */
    protected abstract boolean onFailure(Exception e);

    protected boolean canInvalidateTable(Exception e) {
      if (e instanceof MetastoreClientInstantiationException) {
        // All runtime exceptions except this exception are considered for invalidation.
        return false;
      }
      // This RuntimeException covers all the exceptions from
      // com.google.common.base.Preconditions methods. IllegalStateException is one such
      // exception seen in IMPALA-12827.
      return (e instanceof RuntimeException) || (e instanceof CatalogException)
          || (e instanceof MetastoreNotificationNeedsInvalidateException);
    }

    /**
     * Helper method to get debug string with helpful event information prepended to the
     * message. This can be used to generate helpful exception messages
     *
     * @param msgFormatString String value to be used in String.format() for the given
     *     message
     * @param args args to the <code>String.format()</code> for the given
     *     msgFormatString
     */
    protected String debugString(String msgFormatString, Object... args) {
      String formatString =
          new StringBuilder(STR_FORMAT_EVENT_ID_TYPE).append(msgFormatString).toString();
      Object[] formatArgs = getLogFormatArgs(args);
      return String.format(formatString, formatArgs);
    }

    /**
     * Helper method to generate the format args after prepending the event id and type
     */
    private Object[] getLogFormatArgs(Object[] args) {
      Object[] formatArgs = new Object[args.length + 2];
      formatArgs[0] = getEventId();
      formatArgs[1] = getEventType();
      int i = 2;
      for (Object arg : args) {
        formatArgs[i] = arg;
        i++;
      }
      return formatArgs;
    }

    /**
     * Logs at info level the given log formatted string and its args. The log formatted
     * string should have {} pair at the appropriate location in the string for each arg
     * value provided. This method prepends the event id and event type before logging the
     * message. No-op if the log level is not at INFO
     */
    protected void infoLog(String logFormattedStr, Object... args) {
      if (!LOG.isInfoEnabled()) return;
      String formatString =
          new StringBuilder(LOG_FORMAT_EVENT_ID_TYPE).append(logFormattedStr).toString();
      Object[] formatArgs = getLogFormatArgs(args);
      LOG.info(formatString, formatArgs);
    }

    /**
     * Similar to infoLog excepts logs at debug level
     */
    protected void debugLog(String logFormattedStr, Object... args) {
      if (!LOG.isDebugEnabled()) return;
      String formatString =
          new StringBuilder(LOG_FORMAT_EVENT_ID_TYPE).append(logFormattedStr).toString();
      Object[] formatArgs = getLogFormatArgs(args);
      LOG.debug(formatString, formatArgs);
    }

    /**
     * Similar to infoLog excepts logs at trace level
     */
    protected void traceLog(String logFormattedStr, Object... args) {
      if (!LOG.isTraceEnabled()) return;
      String formatString =
          new StringBuilder(LOG_FORMAT_EVENT_ID_TYPE).append(logFormattedStr).toString();
      Object[] formatArgs = getLogFormatArgs(args);
      LOG.trace(formatString, formatArgs);
    }

    /**
     * Similar to infoLog excepts logs at warn level
     */
    protected void warnLog(String logFormattedStr, Object... args) {
      if (!LOG.isWarnEnabled()) return;
      String formatString =
          new StringBuilder(LOG_FORMAT_EVENT_ID_TYPE).append(logFormattedStr).toString();
      Object[] formatArgs = getLogFormatArgs(args);
      LOG.warn(formatString, formatArgs);
    }

    /**
     * Method to log error for an event
     */
    protected void errorLog(String logFormattedStr, Object... args) {
      if (!LOG.isErrorEnabled()) return;
      String formatString =
          new StringBuilder(LOG_FORMAT_EVENT_ID_TYPE).append(logFormattedStr).toString();
      Object[] formatArgs = getLogFormatArgs(args);
      LOG.error(formatString, formatArgs);
    }

    /**
     * Search for a inverse event (for example drop_table is a inverse event for
     * create_table) for this event from a given list of notificationEvents starting for
     * the startIndex. This is useful for skipping certain events from processing
     *
     * @param events List of NotificationEvents to be searched
     * @return true if the object is removed after this event, else false
     */
    protected boolean isRemovedAfter(List<MetastoreEvent> events) {
      return false;
    }

    /**
     * Returns true if event based sync is disabled for this table/database associated
     * with this event
     */
    protected abstract boolean isEventProcessingDisabled();

    protected abstract SelfEventContext getSelfEventContext();

    /**
     * This method detects if this event is self-generated or not (see class
     * documentation of <code>MetastoreEventProcessor</code> to understand what a
     * self-event is).
     *
     * In order to determine this, it compares the value of catalogVersion from the
     * event with the list of pending version numbers stored in the catalog
     * database/table. The event could be generated by another instance of CatalogService
     * which can potentially have the same versionNumber. In order to resolve such
     * conflict, it compares the CatalogService's serviceId before comparing the version
     * number. If it is determined that this is indeed a self-event, this method also
     * clears the version number from the catalog database/table's list of pending
     * versions for in-flight events. This is needed so that a subsequent event with the
     * same service id or version number is not incorrectly determined as a self-event. A
     * subsequent event with the same serviceId and versionNumber is most likely generated
     * by a non-Impala system because it cached the table object having those values of
     * serviceId and version. More details on complete flow of self-event handling
     * logic can be read in <code>MetastoreEventsProcessor</code> documentation.
     *
     * @return True if this event is a self-generated event. If the returned value is
     * true, this method also clears the version number from the catalog database/table.
     * Returns false if the version numbers or service id don't match
     */
    protected boolean isSelfEvent() {
      try {
        if (catalog_.evaluateSelfEvent(getSelfEventContext())) {
          metrics_.getCounter(MetastoreEventsProcessor.EVENTS_SKIPPED_METRIC)
              .inc(getNumberOfEvents());
          infoLog("Incremented events skipped counter to {}",
              metrics_.getCounter(MetastoreEventsProcessor.EVENTS_SKIPPED_METRIC)
                  .getCount());
          return true;
        }
      } catch (CatalogException e) {
        debugLog("Received exception {}. Ignoring self-event evaluation",
            e.getMessage());
      }
      return false;
    }

    public final boolean isDropEvent() {
      return (this instanceof DropTableEvent ||
          this instanceof DropDatabaseEvent ||
          this instanceof DropPartitionEvent);
    }

    @Override
    public String toString() {
      return String.format(STR_FORMAT_EVENT_ID_TYPE, eventId_, eventType_);
    }

    protected boolean isOlderThanLastSyncEventId(MetastoreEvent event) {
      org.apache.impala.catalog.Table tbl = catalog_.getTableNoThrow(dbName_, tblName_);
      if (tbl != null && tbl.getLastSyncedEventId() >= event.getEventId()) {
        return true;
      }
      return false;
    }
  }

  public static String getStringProperty(
      Map<String, String> params, String key, String defaultVal) {
    if (params == null) return defaultVal;
    return params.getOrDefault(key, defaultVal);
  }

  /**
   * Base class for all the table events
   */
  public static abstract class MetastoreTableEvent extends MetastoreEvent {
    // tblName from the event
    protected final String tblName_;

    // tbl object from the Notification event, corresponds to the before tableObj in
    // case of alter events
    protected org.apache.hadoop.hive.metastore.api.Table msTbl_;

    // A boolean flag used in alter table event to record if the file metadata reload
    // can be skipped for certain type of alter table statements
    protected boolean skipFileMetadataReload_ = false;

    // in case of partition batch events, this method can be overridden to return
    // the partition object from the events which are batched together.
    protected Partition getPartitionForBatching() { return null; }

    private MetastoreTableEvent(CatalogOpExecutor catalogOpExecutor,
        Metrics metrics, NotificationEvent event) {
      super(catalogOpExecutor, metrics, event);
      Preconditions.checkNotNull(dbName_, "Database name cannot be null");
      tblName_ = Preconditions.checkNotNull(event.getTableName());
    }


    /**
     * Util method to return the fully qualified table name which is of the format
     * dbName.tblName for this event
     */
    protected String getFullyQualifiedTblName() {
      return new TableName(dbName_, tblName_).toString();
    }

    /**
     * Checks if the table level property is set in the parameters of the table from the
     * event. If it is available, it takes precedence over database level flag for this
     * table. If the table level property is not set, returns the value from the database
     * level property.f
     *
     * @return Boolean value of the table property with the key
     *     <code>MetastoreEventPropertyKey.DISABLE_EVENT_HMS_SYNC</code>. Else,
     *     returns the database property which is associated with this table. Returns
     *     false if neither of the properties are set.
     */
    @Override
    protected boolean isEventProcessingDisabled() {
      Preconditions.checkNotNull(msTbl_);
      Boolean tblProperty = getHmsSyncProperty(msTbl_);
      if (tblProperty != null) {
        infoLog("Found table level flag {} is set to {} for table {}",
            MetastoreEventPropertyKey.DISABLE_EVENT_HMS_SYNC.getKey(),
            tblProperty.toString(),
            getFullyQualifiedTblName());
        return tblProperty;
      }
      // if the tbl property is not set check at db level
      String dbFlagVal = catalog_.getDbProperty(dbName_,
          MetastoreEventPropertyKey.DISABLE_EVENT_HMS_SYNC.getKey());
      if (dbFlagVal != null) {
        // no need to spew unnecessary logs. Most tables/databases are expected to not
        // have this flag set when event based HMS polling is enabled
        debugLog("Table level flag is not set. Db level flag {} is {} for "
                + "database {}",
            MetastoreEventPropertyKey.DISABLE_EVENT_HMS_SYNC.getKey(),
            dbFlagVal, dbName_);
      }
      // flag value of null also returns false
      return Boolean.valueOf(dbFlagVal);
    }

    /**
     * Gets the value of the parameter with the key
     * <code>MetastoreEventPropertyKey.DISABLE_EVENT_HMS_SYNC</code> from the given
     * table
     *
     * @return the Boolean value of the property with the key
     *     <code>MetastoreEventPropertyKey.DISABLE_EVENT_HMS_SYNC</code> if it is
     *     available else returns null
     */
    public static Boolean getHmsSyncProperty(
        org.apache.hadoop.hive.metastore.api.Table tbl) {
      if (!tbl.isSetParameters()) return null;
      String val =
          tbl.getParameters()
              .get(MetastoreEventPropertyKey.DISABLE_EVENT_HMS_SYNC.getKey());
      if (val == null || val.isEmpty()) return null;
      return Boolean.valueOf(val);
    }

    /**
     * Util method to create partition key-value map from HMS Partition objects.
     */
    protected static List<TPartitionKeyValue> getTPartitionSpecFromHmsPartition(
        org.apache.hadoop.hive.metastore.api.Table msTbl, Partition partition) {
      List<TPartitionKeyValue> tPartSpec = new ArrayList<>();
      List<org.apache.hadoop.hive.metastore.api.FieldSchema> fsList =
          msTbl.getPartitionKeys();
      List<String> partVals = partition.getValues();
      Preconditions.checkNotNull(partVals);
      Preconditions.checkState(fsList.size() == partVals.size());
      for (int i = 0; i < fsList.size(); i++) {
        tPartSpec.add(new TPartitionKeyValue(fsList.get(i).getName(), partVals.get(i)));
      }
      return tPartSpec;
    }

    /*
     * Helper function to initiate a table reload on Catalog. Re-throws the exception if
     * the catalog operation throws.
     */
    protected void reloadTableFromCatalog(String operation, boolean isTransactional)
        throws CatalogException {
      try {
        if (!catalog_.reloadTableIfExists(dbName_, tblName_,
            "Processing " + operation + " event from HMS", getEventId(),
            skipFileMetadataReload_)) {
          debugLog("Automatic refresh on table {} failed as the table "
                  + "either does not exist anymore or is not in loaded state.",
              getFullyQualifiedTblName());
          metrics_.getCounter(MetastoreEventsProcessor.EVENTS_SKIPPED_METRIC).inc();
          debugLog("Incremented skipped metric to "
              + metrics_.getCounter(MetastoreEventsProcessor.EVENTS_SKIPPED_METRIC)
                    .getCount());
          return;
        }
      } catch (TableLoadingException | DatabaseNotFoundException e) {
        // there could be many reasons for receiving a tableLoading exception,
        // eg. table doesn't exist in HMS anymore or table schema is not supported
        // or Kudu threw an exception due to some internal error. There is not much
        // we can do here other than log it appropriately.
        debugLog("Table {} was not refreshed due to error {}",
            getFullyQualifiedTblName(), e.getMessage());
        return;
      }
      String tblStr = isTransactional ? "transactional table" : "table";
      infoLog("Refreshed {} {}", tblStr, getFullyQualifiedTblName());
      metrics_.getCounter(MetastoreEventsProcessor.NUMBER_OF_TABLE_REFRESHES).inc();
    }

    /**
     * Reloads the partitions provided, only if the table is loaded and if the partitions
     * exist in catalogd.
     * @param partitions the list of Partition objects which need to be reloaded.
     * @param fileMetadataLoadOpts: describes how to reload file metadata for partitions
     * @param reason The reason for reload operation which is used for logging by
     *               catalogd.
     */
    protected void reloadPartitions(List<Partition> partitions,
        FileMetadataLoadOpts fileMetadataLoadOpts, String reason)
        throws CatalogException {
      try {
        int numPartsRefreshed = catalogOpExecutor_.reloadPartitionsIfExist(getEventId(),
            getEventType().toString(), dbName_, tblName_, partitions, reason,
            fileMetadataLoadOpts);
        if (numPartsRefreshed > 0) {
          metrics_.getCounter(MetastoreEventsProcessor.NUMBER_OF_PARTITION_REFRESHES)
              .inc(numPartsRefreshed);
        }
      } catch (TableNotLoadedException e) {
        debugLog("Ignoring the event since table {} is not loaded",
            getFullyQualifiedTblName());
      } catch (DatabaseNotFoundException | TableNotFoundException e) {
        debugLog("Ignoring the event since table {} is not found",
            getFullyQualifiedTblName());
      }
    }

    /**
     * Reloads partitions from the event if they exist. Does not fetch those partitions
     * from HMS. Does NOT reload file metadata
     * @param partitions: Partitions to be reloaded
     * @param reason: Reason for reloading. Mainly used for logging in catalogD
     * @throws CatalogException
     */
    protected void reloadPartitionsFromEvent(List<Partition> partitions, String reason)
        throws CatalogException {
      try {
        int numPartsRefreshed = catalogOpExecutor_.reloadPartitionsFromEvent(getEventId(),
            dbName_, tblName_, partitions, reason);
        if (numPartsRefreshed > 0) {
          metrics_.getCounter(MetastoreEventsProcessor.NUMBER_OF_PARTITION_REFRESHES)
              .inc(numPartsRefreshed);
        }
      } catch (TableNotLoadedException e) {
        debugLog("Ignoring the event since table {} is not loaded",
            getFullyQualifiedTblName());
      } catch (DatabaseNotFoundException | TableNotFoundException e) {
        debugLog("Ignoring the event since table {} is not found",
            getFullyQualifiedTblName());
      }
    }

    protected void reloadPartitionsFromNames(List<String> partitionNames, String reason,
        FileMetadataLoadOpts fileMetadataLoadOpts) throws CatalogException {
      try {
        int numPartsRefreshed = catalogOpExecutor_.reloadPartitionsFromNamesIfExists(
            getEventId(), getEventType().toString(), dbName_, tblName_, partitionNames,
            reason, fileMetadataLoadOpts);
        if (numPartsRefreshed > 0) {
          metrics_.getCounter(MetastoreEventsProcessor.NUMBER_OF_PARTITION_REFRESHES)
                  .inc(numPartsRefreshed);
        }
      } catch (TableNotLoadedException e) {
        debugLog("Ignoring the event since table {} is not loaded",
            getFullyQualifiedTblName());
      } catch (DatabaseNotFoundException | TableNotFoundException e) {
        debugLog("Ignoring the event since table {} is not found",
            getFullyQualifiedTblName());
      }
    }

    /**
     * To decide whether to skip processing this event, fetch table from cache
     * and compare the last synced event id of cache table with this event id.
     * Skip this event if the table is already synced till this event id. Otherwise,
     * process this event.
     * @return true if processing of this event should be skipped. False otherwise
     * @throws CatalogException
     */
    protected boolean shouldSkipWhenSyncingToLatestEventId() throws CatalogException {
      Preconditions.checkState(
          BackendConfig.INSTANCE.enableSyncToLatestEventOnDdls(),
          "sync to latest event flag is not set to true");
      long eventId = this.getEventId();
      Preconditions.checkState(eventId > 0,
          "Invalid event id %s. Should be greater than "
              + "0", eventId);
      org.apache.impala.catalog.Table tbl = null;
      try {
        tbl = catalog_.getTable(dbName_, tblName_);
        if (tbl == null) {
          infoLog("Skipping on table {}.{} since it does not exist in cache", dbName_,
              tblName_);
          return true;
        }
        // During alter table rename, the renamed table is created as Incomplete table
        // with create event id set to alter_table event id (i.e not -1) and therefore
        // should *NOT* be skipped in event processing
        if (tbl instanceof IncompleteTable && tbl.getLastSyncedEventId() == -1) {
          infoLog("Skipping on an incomplete table {} since last synced event id is "
              + "set to {}", tbl.getFullName(), tbl.getLastSyncedEventId());
          return true;
        }
      } catch (DatabaseNotFoundException e) {
        infoLog("Skipping on table {} because db {} not found in cache", tblName_,
            dbName_);
        return true;
      }
      boolean shouldSkip = false;
      // do not acquire read lock on tbl because lastSyncedEventId_ is volatile.
      // It is fine if this method returns false because at the time of actual
      // processing of the event, we would again check lastSyncedEventId_ after
      // acquiring write lock on table and if the table was already synced till
      // this event id, the event processing would be skipped.
      if (tbl.getLastSyncedEventId() >= eventId) {
        infoLog("Skipping on table {} since it is already synced till event id {}",
            tbl.getFullName(), tbl.getLastSyncedEventId());
        shouldSkip = true;
      }
      return shouldSkip;
    }

    /**
     * Overrides parent's isSelfEvent method. If the event turns out to be a self event
     * then this implementation checks and sets table's lastSyncedEvent if it is less
     * than this event id. It is done so that when syncing table to latest event id on
     * subsequent ddl operations, the self event is not processed again.
     * @return
     */
    @Override
    protected boolean isSelfEvent() {
      boolean isSelfEvent = super.isSelfEvent();
      if (!isSelfEvent || !BackendConfig.INSTANCE.enableSyncToLatestEventOnDdls()) {
        return isSelfEvent;
      }
      org.apache.impala.catalog.Table tbl = null;
      try {
        tbl = catalog_.getTable(getDbName(), getTableName());

        if (tbl != null && catalog_.tryWriteLock(tbl)) {
          catalog_.getLock().writeLock().unlock();
          if (tbl.getLastSyncedEventId() < getEventId()) {
            infoLog("is a self event. last synced event id for "
                    + "table {} is {}. Setting it to {}", tbl.getFullName(),
                tbl.getLastSyncedEventId(), getEventId());
            tbl.setLastSyncedEventId(getEventId());
          }
        }
      } catch (CatalogException e) {
        debugLog("ignoring exception when trying to set latest event for a self event "
            + "on table {}.{}", getDbName(), getTableName(), e);
      } finally {
        catalogOpExecutor_.UnlockWriteLockIfErronouslyLocked();
        if (tbl != null && tbl.isWriteLockedByCurrentThread()) {
          tbl.releaseWriteLock();
        }
      }
      return true;
    }

    protected boolean isOlderEvent(Partition partitionEventObj) {
      if (!BackendConfig.INSTANCE.enableSkippingOlderEvents()) {
        return false;
      }
      org.apache.impala.catalog.Table tbl = null;
      try {
        tbl = catalog_.getTable(dbName_, tblName_);
        if (tbl == null || tbl instanceof IncompleteTable) {
          return false;
        }
        if (getEventId() > 0 && getEventId() <= tbl.getCreateEventId()) {
          // Older event, so this event will be skipped.
          metrics_.getCounter(MetastoreEventsProcessor.EVENTS_SKIPPED_METRIC).inc();
          infoLog("Table: {} createEventId: {} is >= to the current " +
              "eventId: {}. Incremented skipped metric to {}", tbl.getFullName(),
              tbl.getCreateEventId(), getEventId(),
              metrics_.getCounter(MetastoreEventsProcessor.EVENTS_SKIPPED_METRIC)
                  .getCount());
          return true;
        }
        // Always check the lastRefreshEventId on the table first for table level refresh
        if (tbl.getLastRefreshEventId() > getEventId() || (partitionEventObj != null &&
            catalog_.isPartitionLoadedAfterEvent(dbName_, tblName_,
                partitionEventObj, getEventId()))) {
          metrics_.getCounter(MetastoreEventsProcessor.EVENTS_SKIPPED_METRIC)
              .inc(getNumberOfEvents());
          String messageStr = partitionEventObj == null ? "Skipping the event since the" +
              " table " + dbName_+ "." + tblName_ + " has last refresh id as " +
              tbl.getLastRefreshEventId() + ". Comparing it with current event " +
              getEventId() + ". " : "";
          infoLog("{}Incremented events skipped counter to {}", messageStr,
              metrics_.getCounter(MetastoreEventsProcessor.EVENTS_SKIPPED_METRIC)
                  .getCount());
          return true;
        }
      } catch (CatalogException e) {
        debugLog("ignoring exception while checking if it is an older event "
            + "on table {}.{}", dbName_, tblName_, e);
      }
      return false;
    }

    @Override
    protected void process() throws MetastoreNotificationException, CatalogException {
      Timer.Context context = null;
      org.apache.impala.catalog.Table tbl = catalog_.getTableNoThrow(dbName_, tblName_);
      if (tbl != null) {
        context = tbl.getMetrics().getTimer(TBL_EVENTS_PROCESS_DURATION).time();
      }
      try {
        processTableEvent();
      } finally {
        if (context != null) {
          context.stop();
        }
      }
    }

    @Override
    protected boolean onFailure(Exception e) {
      if (!BackendConfig.INSTANCE.isInvalidateMetadataOnEventProcessFailureEnabled()) {
        return false;
      }
      boolean isInvalidate = canInvalidateTable(e);
      if (isInvalidate) {
        errorLog("Invalidate table {}.{} due to exception during event processing",
            dbName_, tblName_, e);
        catalog_.invalidateTableIfExists(dbName_, tblName_);
      }
      return isInvalidate;
    }

    protected abstract void processTableEvent() throws MetastoreNotificationException,
        CatalogException;
  }

  /**
   * Base class for all the database events
   */
  public static abstract class MetastoreDatabaseEvent extends MetastoreEvent {
    MetastoreDatabaseEvent(CatalogOpExecutor catalogOpExecutor, Metrics metrics,
        NotificationEvent event) {
      super(catalogOpExecutor, metrics, event);
      Preconditions.checkNotNull(dbName_, debugString("Database name cannot be null"));
      debugLog("Creating event {} of type {} on database {}", getEventId(),
              getEventType(), dbName_);
    }

    /**
     * Even though there is a database level property
     * <code>MetastoreEventPropertyKey.DISABLE_EVENT_HMS_SYNC</code> it is only used
     * for tables within that
     * database. As such this property does not control if the database level DDLs are
     * skipped or not.
     *
     * @return false
     */
    @Override
    protected boolean isEventProcessingDisabled() {
      return false;
    }

    protected boolean shouldSkipWhenSyncingToLatestEventId() throws CatalogException {
      return false;
    }

    /**
     * Overrides parent's isSelfEvent method. If the event turns out to be a self event
     * then this implementation checks and sets db's lastSyncedEvent if it is less
     * than this event id. It is done so that when syncing db to latest event id on
     * subsequent ddl operations, the self event is not processed again.
     * @return
     */
    @Override
    protected boolean isSelfEvent() {
      boolean isSelfEvent = super.isSelfEvent();
      if (!isSelfEvent || !BackendConfig.INSTANCE.enableSyncToLatestEventOnDdls()) {
        return isSelfEvent;
      }
      Db db = null;
      try {
        db = catalog_.getDb(getDbName());
        if (db != null && catalog_.tryLockDb(db)) {
          catalog_.getLock().writeLock().unlock();
          if (db.getLastSyncedEventId() < getEventId()) {
            infoLog("is a self event. last synced event id for "
                    + "db {} is {}. Setting it to {}", getDbName(),
                db.getLastSyncedEventId(), getEventId());
            db.setLastSyncedEventId(getEventId());
          }
        }
      } finally {
        catalogOpExecutor_.UnlockWriteLockIfErronouslyLocked();
        if (db != null && db.isLockHeldByCurrentThread()) {
          db.getLock().unlock();
        }
      }
      return true;
    }

    @Override
    protected boolean onFailure(Exception e) {
      // TODO: Need to check db event failure in future
      return false;
    }
  }

  /**
   * MetastoreEvent for CREATE_TABLE event type
   */
  public static class CreateTableEvent extends MetastoreTableEvent {

    public static final String EVENT_TYPE = "CREATE_TABLE";
    /**
     * Prevent instantiation from outside should use MetastoreEventFactory instead
     */
    private CreateTableEvent(CatalogOpExecutor catalogOpExecutor, Metrics metrics,
        NotificationEvent event) throws MetastoreNotificationException {
      super(catalogOpExecutor, metrics, event);
      Preconditions.checkArgument(MetastoreEventType.CREATE_TABLE.equals(getEventType()));
      Preconditions
          .checkNotNull(event.getMessage(), debugString("Event message is null"));
      CreateTableMessage createTableMessage =
          MetastoreEventsProcessor.getMessageDeserializer()
              .getCreateTableMessage(event.getMessage());
      try {
        msTbl_ = createTableMessage.getTableObj();
      } catch (Exception e) {
        throw new MetastoreNotificationException(
            debugString("Unable to deserialize the event message"), e);
      }
    }

    @Override
    public SelfEventContext getSelfEventContext() {
      throw new UnsupportedOperationException("Self-event evaluation is unnecessary for"
          + " this event type");
    }

    /**
     * If the table provided in the catalog does not exist in the catalog, this method
     * will create it. If the table in the catalog already exists, it relies of the
     * creationTime of the Metastore Table to resolve the conflict. If the catalog table's
     * creation time is less than creationTime of the table from the event, it will be
     * overridden. Else, it will ignore the event
     */
    @Override
    public void processTableEvent() throws MetastoreNotificationException {
      // check if the table exists already. This could happen in corner cases of the
      // table being dropped and recreated with the same name or in case this event is
      // a self-event (see description of self-event in the class documentation of
      // MetastoreEventsProcessor)
      try {
        if (catalogOpExecutor_.addTableIfNotRemovedLater(getEventId(), msTbl_)) {
          infoLog("Successfully added table {}", getFullyQualifiedTblName());
          metrics_.getCounter(MetastoreEventsProcessor.NUMBER_OF_TABLES_ADDED).inc();
        } else {
          metrics_.getCounter(MetastoreEventsProcessor.EVENTS_SKIPPED_METRIC).inc();
          debugLog("Incremented skipped metric to " + metrics_
              .getCounter(MetastoreEventsProcessor.EVENTS_SKIPPED_METRIC).getCount());
        }
      } catch (DatabaseNotFoundException ex) {
        // This is an incorrect setup where DB is not found in cache for a table event.
        // Don't put the event processor into error state, instead ignore this event.
        errorLog("Create table {}.{} failed because the database does not exist cache." +
            " Ignoring the CREATE_TABLE event", dbName_, tblName_, ex);
      } catch (CatalogException e) {
        // if a DatabaseNotFoundException is caught here it means either we incorrectly
        // determined that the event needs to be processed instead of skipped, or we
        // somehow missed the previous create database event.
        throw new MetastoreNotificationException(
            debugString("Unable to process event"), e);
      }
    }

    @Override
    protected boolean onFailure(Exception e) {
      return false;
    }

    @Override
    public boolean isRemovedAfter(List<MetastoreEvent> events) {
      Preconditions.checkNotNull(events);
      for (MetastoreEvent event : events) {
        if (event.eventType_.equals(MetastoreEventType.DROP_TABLE)) {
          DropTableEvent dropTableEvent = (DropTableEvent) event;
          if (dbName_.equalsIgnoreCase(dropTableEvent.dbName_) && tblName_
              .equalsIgnoreCase(dropTableEvent.tblName_)) {
            infoLog("Found table {} is removed later in event {} type {}", tblName_,
                dropTableEvent.getEventId(), dropTableEvent.getEventType());
            return true;
          }
        } else if (event.eventType_.equals(MetastoreEventType.ALTER_TABLE)) {
          // renames are implemented as a atomic (drop+create) so rename events can
          // also be treated as a inverse event of the create_table event. Consider a
          // DDL op sequence like create table, alter table rename from impala. Since
          // the rename operation is internally implemented as drop+add, processing a
          // create table event on this cluster will show up the table for small window
          // of time, until the actual rename event is processed. If however, we ignore
          // the create table event, the alter rename event just becomes a addIfNotExists
          // event which is valid for both a self-event and external event cases
          AlterTableEvent alterTableEvent = (AlterTableEvent) event;
          if (alterTableEvent.isRename_
              && dbName_.equalsIgnoreCase(alterTableEvent.msTbl_.getDbName())
              && tblName_.equalsIgnoreCase(alterTableEvent.msTbl_.getTableName())) {
            infoLog("Found table {} is renamed later in event {} type {}", tblName_,
                alterTableEvent.getEventId(), alterTableEvent.getEventType());
            return true;
          }
        }
      }
      return false;
    }

    public Table getTable() {
      return msTbl_;
    }

    protected boolean shouldSkipWhenSyncingToLatestEventId() {
      return false;
    }
  }

  /**
   *  Metastore event handler for INSERT events. Handles insert events at both table
   *  and partition scopes.
   */
  public static class InsertEvent extends MetastoreTableEvent {

    // Represents the partition for this insert. Null if the table is unpartitioned.
    private Partition insertPartition_;

    /**
     * Prevent instantiation from outside should use MetastoreEventFactory instead
     */
    @VisibleForTesting
    InsertEvent(CatalogOpExecutor catalogOpExecutor, Metrics metrics,
        NotificationEvent event) throws MetastoreNotificationException {
      super(catalogOpExecutor, metrics, event);
      Preconditions.checkArgument(MetastoreEventType.INSERT.equals(getEventType()));
      InsertMessage insertMessage =
          MetastoreEventsProcessor.getMessageDeserializer()
              .getInsertMessage(event.getMessage());
      try {
        msTbl_ = Preconditions.checkNotNull(insertMessage.getTableObj());
        insertPartition_ = insertMessage.getPtnObj();
      } catch (Exception e) {
        throw new MetastoreNotificationException(debugString("Unable to "
            + "parse insert message"), e);
      }
    }

    @Override
    protected MetastoreEventType getBatchEventType() {
      return MetastoreEventType.INSERT_PARTITIONS;
    }

    @Override
    protected Partition getPartitionForBatching() { return insertPartition_; }

    @Override
    public boolean canBeBatched(MetastoreEvent event) {
      if (!(event instanceof InsertEvent)) return false;
      if (isOlderThanLastSyncEventId(event)) return false;
      InsertEvent insertEvent = (InsertEvent) event;
      // make sure that the event is on the same table
      if (!getFullyQualifiedTblName().equalsIgnoreCase(
          insertEvent.getFullyQualifiedTblName())) {
        return false;
      }
      // we currently only batch partition level insert events
      if (this.insertPartition_ == null || insertEvent.insertPartition_ == null) {
        return false;
      }
      return true;
    }

    @Override
    public MetastoreEvent addToBatchEvents(MetastoreEvent event) {
      if (!(event instanceof InsertEvent)) return null;
      BatchPartitionEvent<InsertEvent> batchEvent = new BatchPartitionEvent<>(
          this);
      Preconditions.checkState(batchEvent.canBeBatched(event));
      batchEvent.addToBatchEvents(event);
      return batchEvent;
    }

    @Override
    public SelfEventContext getSelfEventContext() {
      if (insertPartition_ != null) {
        // create selfEventContext for insert partition event
        List<TPartitionKeyValue> tPartSpec =
            getTPartitionSpecFromHmsPartition(msTbl_, insertPartition_);
        return new SelfEventContext(dbName_, tblName_, Arrays.asList(tPartSpec),
            insertPartition_.getParameters(), Arrays.asList(getEventId()));
      } else {
        // create selfEventContext for insert table event
        return new SelfEventContext(
            dbName_, tblName_, null, msTbl_.getParameters(),
            Arrays.asList(getEventId()));
      }
    }

    @Override
    public void processTableEvent() throws MetastoreNotificationException {
      if (isSelfEvent()) {
        infoLog("Not processing the insert event as it is a self-event");
        return;
      }

      if (isOlderEvent(insertPartition_)) {
        infoLog("Not processing the insert event {} as it is an older event",
            getEventId());
        return;
      }
      // Reload the whole table if it's a transactional table or materialized view.
      // Materialized views are treated as a special case because it causes problems
      // on the reloading partition logic which expects it to be a HdfsTable.
      if (AcidUtils.isTransactionalTable(msTbl_.getParameters())
          || MetaStoreUtils.isMaterializedViewTable(msTbl_)) {
        insertPartition_ = null;
      }

      if (insertPartition_ != null) {
        processPartitionInserts();
      } else {
        processTableInserts();
      }
    }

    /**
     * Process partition inserts
     */
    private void processPartitionInserts() throws MetastoreNotificationException {
      // For partitioned table, refresh the partition only.
      Preconditions.checkNotNull(insertPartition_);
      try {
        // Ignore event if table or database is not in catalog. Throw exception if
        // refresh fails. If the partition does not exist in metastore the reload
        // method below removes it from the catalog
        // forcing file metadata reload so that new files (due to insert) are reflected
        // HdfsPartition
        reloadPartitions(Arrays.asList(insertPartition_),
            FileMetadataLoadOpts.FORCE_LOAD, "INSERT event");
      } catch (CatalogException e) {
        throw new MetastoreNotificationNeedsInvalidateException(debugString("Refresh "
                + "partition on table {} partition {} failed. Event processing cannot "
                + "continue. Issue an invalidate metadata command to reset the event "
                + "processor state.", getFullyQualifiedTblName(),
            Joiner.on(',').join(insertPartition_.getValues())), e);
      }
    }

    /**
     *  Process unpartitioned table inserts
     */
    private void processTableInserts() throws MetastoreNotificationException {
      // For non-partitioned tables, refresh the whole table.
      Preconditions.checkState(insertPartition_ == null);
      try {
        reloadTableFromCatalog("INSERT event", false);
      } catch (CatalogException e) {
        throw new MetastoreNotificationNeedsInvalidateException(
            debugString("Refresh table {} failed. Event processing "
                + "cannot continue. Issue an invalidate metadata command to reset "
                + "the event processor state.", getFullyQualifiedTblName()), e);
      }
    }
  }

  /**
   * MetastoreEvent for ALTER_TABLE event type
   */
  public static class AlterTableEvent extends MetastoreTableEvent {
    public static final String EVENT_TYPE = "ALTER_TABLE";
    protected org.apache.hadoop.hive.metastore.api.Table tableBefore_;
    // the table object after alter operation, as parsed from the NotificationEvent
    protected org.apache.hadoop.hive.metastore.api.Table tableAfter_;
    // true if this alter event was due to a rename operation
    protected final boolean isRename_;
    // value of event sync flag for this table before the alter operation
    private final Boolean eventSyncBeforeFlag_;
    // value of the event sync flag if available at this table after the alter operation
    private final Boolean eventSyncAfterFlag_;
    // value of the db flag at the time of event creation
    private final boolean dbFlagVal;
    // true if this alter event was due to a truncate operation in metastore
    private final boolean isTruncateOp_;

    /**
     * Prevent instantiation from outside should use MetastoreEventFactory instead
     */
    @VisibleForTesting
    AlterTableEvent(CatalogOpExecutor catalogOpExecutor, Metrics metrics,
        NotificationEvent event) throws MetastoreNotificationException {
      super(catalogOpExecutor, metrics, event);
      Preconditions.checkArgument(MetastoreEventType.ALTER_TABLE.equals(getEventType()));
      JSONAlterTableMessage alterTableMessage =
          (JSONAlterTableMessage) MetastoreEventsProcessor.getMessageDeserializer()
              .getAlterTableMessage(event.getMessage());
      try {
        msTbl_ = Preconditions.checkNotNull(alterTableMessage.getTableObjBefore());
        tableAfter_ = Preconditions.checkNotNull(alterTableMessage.getTableObjAfter());
        tableBefore_ = Preconditions.checkNotNull(alterTableMessage.getTableObjBefore());
        isTruncateOp_ = alterTableMessage.getIsTruncateOp();
      } catch (Exception e) {
        throw new MetastoreNotificationException(
            debugString("Unable to parse the alter table message"), e);
      }
      // this is a rename event if either dbName or tblName of before and after object
      // changed
      isRename_ = !msTbl_.getDbName().equalsIgnoreCase(tableAfter_.getDbName())
          || !msTbl_.getTableName().equalsIgnoreCase(tableAfter_.getTableName());
      eventSyncBeforeFlag_ = getHmsSyncProperty(msTbl_);
      eventSyncAfterFlag_ = getHmsSyncProperty(tableAfter_);
      dbFlagVal =
          Boolean.valueOf(catalog_.getDbProperty(dbName_,
              MetastoreEventPropertyKey.DISABLE_EVENT_HMS_SYNC.getKey()));
    }

    public boolean isRename() { return isRename_; }

    public Table getBeforeTable() { return tableBefore_; }

    public Table getAfterTable() { return tableAfter_; }

    @Override
    protected SelfEventContext getSelfEventContext() {
      return new SelfEventContext(tableAfter_.getDbName(), tableAfter_.getTableName(),
          tableAfter_.getParameters());
    }

    private void processRename() throws CatalogException {
      if (!isRename_) return;
      Reference<Boolean> oldTblRemoved = new Reference<>();
      Reference<Boolean> newTblAdded = new Reference<>();
      catalogOpExecutor_
          .renameTableFromEvent(getEventId(), tableBefore_, tableAfter_, oldTblRemoved,
              newTblAdded);

      if (oldTblRemoved.getRef()) {
        metrics_.getCounter(MetastoreEventsProcessor.NUMBER_OF_TABLES_REMOVED).inc();
      }
      if (newTblAdded.getRef()) {
        metrics_.getCounter(MetastoreEventsProcessor.NUMBER_OF_TABLES_ADDED).inc();
      }
      if (!oldTblRemoved.getRef() || !newTblAdded.getRef()) {
        metrics_.getCounter(MetastoreEventsProcessor.EVENTS_SKIPPED_METRIC).inc();
        debugLog("Incremented skipped metric to " + metrics_
            .getCounter(MetastoreEventsProcessor.EVENTS_SKIPPED_METRIC).getCount());
      }
    }

    /**
     * If the alter table event is generated because of table rename then event
     * should *NOT* be skipped if old table is not synced this till event AND
     * new table doesn't exist in cache. Skip otherwise
     * @return true if event should be skipped. false otherwise
     * @throws CatalogException
     */
    protected boolean shouldSkipWhenSyncingToLatestEventId() throws CatalogException {
      // always process rename since renameTableFromEvent will make sure that
      // the old table was removed and new was added
      if (isRename_) {
        return false;
      }
      return super.shouldSkipWhenSyncingToLatestEventId();
    }

    /**
     * If the ALTER_TABLE event is due a table rename, this method removes the old table
     * and creates a new table with the new name. Else, this just issues a refresh
     * table on the tblName from the event
     */
    @Override
    public void processTableEvent() throws MetastoreNotificationException,
        CatalogException {
      if (isRename_) {
        processRename();
        return;
      }

      if (isOlderEvent(null)) {
        infoLog("Not processing the alter table event {} as it is an older event",
            getEventId());
        return;
      }

      // Determine whether this is an event which we have already seen or if it is a new
      // event
      if (isSelfEvent()) {
        infoLog("Not processing the event as it is a self-event");
        return;
      }
      // Ignore the event if this is a trivial event. See javadoc for
      // canBeSkipped() for examples.
      if (canBeSkipped()) {
        infoLog("Not processing this event as it only modifies some table parameters "
            + "which can be ignored.");
        return;
      }
      skipFileMetadataReload_ = !isTruncateOp_ && canSkipFileMetadataReload(tableBefore_,
          tableAfter_);
      long startNs = System.nanoTime();
      if (wasEventSyncTurnedOn()) {
        handleEventSyncTurnedOn();
      } else {
        // in case of table level alters from external systems it is better to do a full
        // refresh, eg. this could be due to as simple as adding a new parameter or a
        // full blown adding or changing column type
        // rename is already handled above
        reloadTableFromCatalog("ALTER_TABLE", false);
      }
      long durationNs = System.nanoTime() - startNs;
      // Log event details for those triggered slow reload.
      if (durationNs > HdfsTable.LOADING_WARNING_TIME_NS) {
        warnLog("Slow event processing. Duration: {}. TableBefore: {}. " +
            "TableAfter: {}", PrintUtils.printTimeNs(durationNs),
            tableBefore_.toString(), tableAfter_.toString());
      }
    }

    @Override
    protected boolean onFailure(Exception e) {
      if (!BackendConfig.INSTANCE.isInvalidateMetadataOnEventProcessFailureEnabled()
          || !canInvalidateTable(e)) {
        return false;
      }
      if (isRename()) {
        /* In case of rename table, not invalidating tables due of following reasons:
        1. When failure happened before old table is removed from catalog,
        invalidateTableIfExists may trigger table load for a non-existing table later.
        2. When failure happened before adding new table to catalog,
        invalidateTableIfExists does not invalidate table
        */
        errorLog(
            "Rename table {}.{} to {}.{} failed due to exception during event processing",
            tableBefore_.getDbName(), tableBefore_.getTableName(), dbName_, tblName_, e);
        return false;
      }
      return super.onFailure(e);
    }

    private void handleEventSyncTurnedOn() throws DatabaseNotFoundException,
        MetastoreNotificationNeedsInvalidateException {
      // check if the table exists or not. 1) if the table doesn't exist create an
      // incomplete instance of the table. 2) If the table exists, there can be two
      // scenarios a) current table eventId is greater than table's createEventId,
      // then we should mark the table as stale. b) current table eventId <= table's
      // createEventId, then we should ignore the event as it is an older event.
      org.apache.impala.catalog.Table tbl = catalog_.getTableNoThrow(dbName_, tblName_);
      if (tbl == null) { // table doesn't exist. Go with option (1)
        if (catalogOpExecutor_.addTableIfNotRemovedLater(getEventId(), msTbl_)) {
          infoLog("Successfully added table {}", getFullyQualifiedTblName());
          metrics_.getCounter(MetastoreEventsProcessor.NUMBER_OF_TABLES_ADDED).inc();
        } else {
          metrics_.getCounter(MetastoreEventsProcessor.EVENTS_SKIPPED_METRIC).inc();
          debugLog("Incremented skipped metric to " +
              metrics_.getCounter(MetastoreEventsProcessor.EVENTS_SKIPPED_METRIC)
                  .getCount());
        }
      } else if (tbl instanceof IncompleteTable) {
        // No-Op
      } else if (getEventId() > tbl.getCreateEventId()) {
        catalog_.invalidateTable(tbl.getTableName().toThrift(), new Reference<>(),
            new Reference<>(), NoOpEventSequence.INSTANCE, getEventId());
        LOG.info("Table " + tbl.getFullName() + " is invalidated from catalog cache" +
            " since eventSync is turned on for this table.");
      } else {
        // Unknown state of metadata object, make event processor go into error state
        throw new MetastoreNotificationNeedsInvalidateException(debugString(
            "Detected that event sync was turned on for the table %s "
                + "with createEventId %s. This event should have been skipped as stale "
                + "event. Event processing cannot be continued further. Issue a "
                + "invalidate metadata command to reset the event processing state",
            getFullyQualifiedTblName(), tbl.getCreateEventId()));
      }
    }

    /**
     * This method checks if the reloading of file metadata can be skipped for an alter
     * statement. This method accepts two arguments, 1) pre-modified HMS table instance
     * 2) post-modified HMS table instance and compare what really changed in the alter
     * event.
     */
    private boolean canSkipFileMetadataReload(
        org.apache.hadoop.hive.metastore.api.Table beforeTable,
        org.apache.hadoop.hive.metastore.api.Table afterTable) {
      Set<String> whitelistedTblProperties = catalog_.getWhitelistedTblProperties();
      // If the whitelisted table properties are empty, then we skip this optimization
      if (whitelistedTblProperties.isEmpty()) {
        return false;
      }

      boolean incrementalAcidRefresh =
          BackendConfig.INSTANCE.getHMSEventIncrementalRefreshTransactionalTable();
      boolean unpartitioned = afterTable.getPartitionKeysSize() == 0;
      if (!incrementalAcidRefresh && unpartitioned
          && AcidUtils.isTransactionalTable(afterTable.getParameters())) {
        // In case of ACID tables no INSERT event is generated. If flag
        // hms_event_incremental_refresh_transactional_table is false, then transaction
        // related events are ignored (including COMMIT_TXN), so Impala has to rely on
        // ALTER_TABLE events to detect INSERTs to unpartitioned tables (IMPALA-12835).
        return false;
      }

      // There are lot of other alter statements which doesn't require file metadata
      // reload but these are the most common types for alter statements.
      boolean skipFileMetadata = false;
      if (isFieldSchemaChanged(beforeTable, afterTable) ||
          isTableOwnerChanged(beforeTable.getOwner(), afterTable.getOwner())) {
        skipFileMetadata = true;
      } else if (!Objects.equals(beforeTable.getSd(), afterTable.getSd())) {
        if (isTrivialSdPropsChanged(beforeTable.getSd(), afterTable.getSd())) {
          skipFileMetadata = true;
        }
      } else if (!isCustomTblPropsChanged(whitelistedTblProperties, beforeTable,
          afterTable)) {
        skipFileMetadata = true;
      }
      return skipFileMetadata;
    }

    private boolean isFieldSchemaChanged(
        org.apache.hadoop.hive.metastore.api.Table beforeTable,
        org.apache.hadoop.hive.metastore.api.Table afterTable) {
      List<FieldSchema> beforeCols = beforeTable.getSd().getCols();
      List<FieldSchema> afterCols = afterTable.getSd().getCols();
      // check if columns are added or removed
      if (beforeCols.size() != afterCols.size()) {
        infoLog("Change in number of columns detected for table {}.{} from {} to {}. " +
            "So file metadata reload can be skipped.", dbName_, tblName_,
            beforeCols.size(), afterCols.size());
        return true;
      }
      // check if columns are replaced or column definition is changed
      // Field schema's comment is rarely used, so it'll be ignored
      for (int i = 0; i < beforeCols.size(); i++) {
        if (!beforeCols.get(i).getName().equals(afterCols.get(i).getName()) ||
            !beforeCols.get(i).getType().equals(afterCols.get(i).getType())) {
          infoLog("Change in table schema detected for table {}.{} from {} ({}) " +
              "to {} ({}). So file metadata reload can be skipped.", dbName_, tblName_,
              beforeCols.get(i).getName(), beforeCols.get(i).getType(),
              afterCols.get(i).getName(), afterCols.get(i).getType());
          return true;
        }
      }
      return false;
    }

    private boolean isTableOwnerChanged(String ownerBefore, String ownerAfter) {
      if (!Objects.equals(ownerBefore, ownerAfter)) {
        infoLog("Change in Ownership detected for table {}.{}, oldOwner: {}, " +
            "newOwner: {}. So file metadata reload can be skipped.",
            dbName_, tblName_, ownerBefore, ownerAfter);
        return true;
      }
      return false;
    }

    // Check if the whitelisted properties are changed during the alter statement
    private boolean isCustomTblPropsChanged(Set<String> whitelistedTblProperties,
        org.apache.hadoop.hive.metastore.api.Table beforeTable,
        org.apache.hadoop.hive.metastore.api.Table afterTable) {
      for (String whitelistConfig : whitelistedTblProperties) {
        String configBefore = beforeTable.getParameters().get(whitelistConfig);
        String configAfter = afterTable.getParameters().get(whitelistConfig);
        if (!Objects.equals(configBefore, configAfter)) {
          infoLog("Change in whitelisted table properties detected for table {}.{} " +
              "whitelisted config: {}, value before: {}, value after: {}. So file " +
              "metadata should be reloaded.", dbName_, tblName_, whitelistConfig,
              configBefore, configAfter);
          return true;
        }
      }
      return false;
    }

    // Check if the trivial SD properties are changed during the alter statement.
    // Also, the caller should make sure that 'beforeSd' and 'afterSd' are not equal.
    private boolean isTrivialSdPropsChanged(StorageDescriptor beforeSd,
        StorageDescriptor afterSd) {
      Preconditions.checkNotNull(beforeSd, "beforeSd is null");
      Preconditions.checkNotNull(afterSd, "afterSd is null");
      StorageDescriptor previousSD = normalizeStorageDescriptor(beforeSd.deepCopy());
      StorageDescriptor currentSD = normalizeStorageDescriptor(afterSd.deepCopy());
      if (!Objects.equals(previousSD, currentSD)) {
        infoLog("Non-trivial change in table storage descriptor (SD) detected for " +
            "table {}.{}. So file metadata should be reloaded. SD before: {}, SD " +
            "after: {}", dbName_, tblName_, beforeSd.toString(), afterSd.toString());
        return false;
      }
      infoLog("Trivial changes in table storage descriptor (SD) detected for table " +
          "{}.{}. So file metadata reload can be skipped.", dbName_, tblName_);
      return true;
    }

    /**
     * Normalize the storage descriptor by unsetting the trivial fields in SD like
     * columns, compressed, numBuckets, bucketCols,SkewedInfo, and
     * setStoredAsSubDirectories.
     */
    private StorageDescriptor normalizeStorageDescriptor(StorageDescriptor sd) {
      sd.unsetCols();
      sd.unsetCompressed();
      sd.unsetNumBuckets();
      sd.unsetBucketCols();
      sd.unsetSortCols();
      sd.unsetSkewedInfo();
      // setStoredAsSubDirectories = null or 'false' are trivial changes, so we normalize
      // this value to false if it is null.
      if (!sd.isSetStoredAsSubDirectories()) {
        sd.setStoredAsSubDirectories(false);
      }
      return sd;
    }

    /**
     * Detects a event sync flag was turned on in this event
     */
    private boolean wasEventSyncTurnedOn() {
      // the eventsync flag was not changed
      if (Objects.equals(eventSyncBeforeFlag_, eventSyncAfterFlag_)) return false;
      // eventSync after flag is null or if it is explicitly set to false
      if ((eventSyncAfterFlag_ == null && !dbFlagVal) || !eventSyncAfterFlag_) {
        return true;
      }
      return false;
    }

    @Override
    protected boolean canBeSkipped() {
      // Certain alter events just modify some parameters such as
      // "transient_lastDdlTime" in Hive. For eg: the alter table event generated
      // along with insert events. Check if the alter table event is such a trivial
      // event by setting those parameters equal before and after the event and
      // comparing the objects.

      // alter table event from truncate ops always can't be skipped.
      if (isTruncateOp_) {
        return false;
      }

      // Avoid modifying the object from event.
      org.apache.hadoop.hive.metastore.api.Table tblAfter = tableAfter_.deepCopy();
      setTrivialParameters(tableBefore_.getParameters(), tblAfter.getParameters());
      return tblAfter.equals(tableBefore_);
    }

    private String qualify(TTableName tTableName) {
      return new TableName(tTableName.db_name, tTableName.table_name).toString();
    }

    /**
     * In case of alter table events, it is possible that the alter event is generated
     * because user changed the value of the parameter
     * <code>MetastoreEventPropertyKey.DISABLE_EVENT_HMS_SYNC</code>. If the
     * parameter is unchanged, it doesn't
     * matter if you use the before or after table object here since the eventual action
     * is going be refresh or rename. If however, the parameter is changed, couple of
     * things could happen. The flag changes from unset/false to true or it changes from
     * true to false/unset. In the first case, we want to process the event (and ignore
     * subsequent events on this table). In the second case, we should process the event
     * (as well as all the subsequent events on the table). So we always process this
     * event when the value of the flag is changed.
     *
     * @return true, if this event needs to be skipped. false if this event needs to be
     *     processed.
     */
    @Override
    protected boolean isEventProcessingDisabled() {
      // if the event sync flag was changed then we always process this event
      if (!Objects.equals(eventSyncBeforeFlag_, eventSyncAfterFlag_)) {
        infoLog("Before flag value {} after flag value {} changed for table {}",
            eventSyncBeforeFlag_, eventSyncAfterFlag_, getFullyQualifiedTblName());
        return false;
      }
      // flag is unchanged, use the default impl from base class
      return super.isEventProcessingDisabled();
    }
  }

  /**
   * MetastoreEvent for the DROP_TABLE event type
   */
  public static class DropTableEvent extends MetastoreTableEvent {

    public static final String EVENT_TYPE = "DROP_TABLE";

    /**
     * Prevent instantiation from outside should use MetastoreEventFactory instead
     */
    private DropTableEvent(CatalogOpExecutor catalogOpExecutor, Metrics metrics,
        NotificationEvent event) throws MetastoreNotificationException {
      super(catalogOpExecutor, metrics, event);
      Preconditions.checkArgument(MetastoreEventType.DROP_TABLE.equals(getEventType()));
      JSONDropTableMessage dropTableMessage =
          (JSONDropTableMessage) MetastoreEventsProcessor.getMessageDeserializer()
              .getDropTableMessage(event.getMessage());
      try {
        msTbl_ = Preconditions.checkNotNull(dropTableMessage.getTableObj());
      } catch (Exception e) {
        throw new MetastoreNotificationException(debugString(
            "Could not parse event message. "
                + "Check if %s is set to true in metastore configuration",
            MetastoreEventsProcessor.HMS_ADD_THRIFT_OBJECTS_IN_EVENTS_CONFIG_KEY), e);
      }
    }

    @Override
    public SelfEventContext getSelfEventContext() {
      throw new UnsupportedOperationException("self-event evaluation is not needed for "
          + "this event type");
    }

    /**
     * Process the drop table event type. If the table from the event doesn't exist in the
     * catalog, ignore the event. If the table exists in the catalog, compares the
     * createTime of the table in catalog with the createTime of the table from the event
     * and remove the catalog table if there is a match. If the catalog table is a
     * incomplete table it is removed as well. The creation_time from HMS is unfortunately
     * in seconds granularity, which means there is a limitation that we cannot
     * distinguish between tables which are created with the same name within a second.
     * So a sequence of create_table, drop_table, create_table happening within the
     * same second might cause false positives on drop_table event processing. This is
     * not a huge problem since the tables will eventually be created when the
     * create events are processed but there will be a non-zero amount of time when the
     * table will not be existing in catalog.
     * TODO: IMPALA-12646, to track average process time for drop operations.
     */
    @Override
    public void processTableEvent() throws MetastoreNotificationException {
      Reference<Boolean> tblRemovedLater = new Reference<>();
      boolean removedTable;
      removedTable = catalogOpExecutor_
          .removeTableIfNotAddedLater(getEventId(), msTbl_.getDbName(),
              msTbl_.getTableName(), tblRemovedLater);
      if (removedTable) {
        infoLog("Successfully removed table {}", getFullyQualifiedTblName());
        metrics_.getCounter(MetastoreEventsProcessor.NUMBER_OF_TABLES_REMOVED).inc();
      } else {
        metrics_.getCounter(MetastoreEventsProcessor.EVENTS_SKIPPED_METRIC).inc();
        debugLog("Incremented skipped metric to " + metrics_
            .getCounter(MetastoreEventsProcessor.EVENTS_SKIPPED_METRIC).getCount());
      }
    }

    @Override
    protected boolean onFailure(Exception e) {
      return false;
    }
  }

  /**
   * MetastoreEvent for CREATE_DATABASE event type
   */
  public static class CreateDatabaseEvent extends MetastoreDatabaseEvent {

    public static final String EVENT_TYPE = "CREATE_DATABASE";
    // metastore database object as parsed from NotificationEvent message
    private final Database createdDatabase_;

    /**
     * Prevent instantiation from outside should use MetastoreEventFactory instead
     */
    private CreateDatabaseEvent(CatalogOpExecutor catalogOpExecutor, Metrics metrics,
        NotificationEvent event) throws MetastoreNotificationException {
      super(catalogOpExecutor, metrics, event);
      Preconditions.checkArgument(
          MetastoreEventType.CREATE_DATABASE.equals(getEventType()));
      JSONCreateDatabaseMessage createDatabaseMessage =
          (JSONCreateDatabaseMessage) MetastoreEventsProcessor.getMessageDeserializer()
              .getCreateDatabaseMessage(event.getMessage());
      try {
        createdDatabase_ =
            Preconditions.checkNotNull(createDatabaseMessage.getDatabaseObject());
      } catch (Exception e) {
        throw new MetastoreNotificationException(debugString(
            "Database object is null in the event. "
                + "This could be a metastore configuration problem. "
                + "Check if %s is set to true in metastore configuration",
            MetastoreEventsProcessor.HMS_ADD_THRIFT_OBJECTS_IN_EVENTS_CONFIG_KEY), e);
      }
    }

    public Database getDatabase() { return createdDatabase_; }

    @Override
    public SelfEventContext getSelfEventContext() {
      throw new UnsupportedOperationException("Self-event evaluation is unnecessary for"
          + " this event type");
    }

    /**
     * Processes the create database event by adding the Db object from the event if it
     * does not exist in the catalog already.
     */
    @Override
    public void process() {
      boolean dbAdded = catalogOpExecutor_
          .addDbIfNotRemovedLater(getEventId(), createdDatabase_);
      if (!dbAdded) {
        debugLog(
            "Database {} was not added since it either exists or was "
                + "removed since the event was generated", dbName_);
        metrics_.getCounter(MetastoreEventsProcessor.EVENTS_SKIPPED_METRIC).inc();
        debugLog("Incremented skipped metric to " + metrics_
            .getCounter(MetastoreEventsProcessor.EVENTS_SKIPPED_METRIC).getCount());
      } else {
        infoLog("Successfully added database {}", dbName_);
        metrics_.getCounter(MetastoreEventsProcessor.NUMBER_OF_DATABASES_ADDED).inc();
      }
    }
  }

  /**
   * MetastoreEvent for ALTER_DATABASE event type
   */
  public static class AlterDatabaseEvent extends MetastoreDatabaseEvent {
    // metastore database object as parsed from NotificationEvent message
    private final Database alteredDatabase_;

    /**
     * Prevent instantiation from outside should use MetastoreEventFactory instead
     */
    private AlterDatabaseEvent(CatalogOpExecutor catalogOpExecutor, Metrics metrics,
        NotificationEvent event) throws MetastoreNotificationException {
      super(catalogOpExecutor, metrics, event);
      Preconditions.checkArgument(
          MetastoreEventType.ALTER_DATABASE.equals(getEventType()));
      JSONAlterDatabaseMessage alterDatabaseMessage =
          (JSONAlterDatabaseMessage) MetastoreEventsProcessor.getMessageDeserializer()
              .getAlterDatabaseMessage(event.getMessage());
      try {
        alteredDatabase_ =
            Preconditions.checkNotNull(alterDatabaseMessage.getDbObjAfter());
      } catch (Exception e) {
        throw new MetastoreNotificationException(
            debugString("Unable to parse the alter database message"), e);
      }
    }

    /**
     * Processes the alter database event by replacing the catalog cached Db object with
     * the Db object from the event
     */
    @Override
    public void process() throws CatalogException {
      if (isSelfEvent()) {
        infoLog("Not processing the event as it is a self-event");
        return;
      }
      Preconditions.checkNotNull(alteredDatabase_);
      // If not self event, copy Db object from event to catalog
      if (!catalogOpExecutor_.alterDbIfExists(getEventId(), alteredDatabase_)) {
        // Okay to skip this event. Events processor will not error out.
        debugLog("Update database {} failed as the database is not present in the "
            + "catalog.", alteredDatabase_.getName());
      } else {
        infoLog("Database {} updated after alter database event id {}",
            alteredDatabase_.getName(), getEventId());
      }
    }

    @Override
    protected SelfEventContext getSelfEventContext() {
      return new SelfEventContext(dbName_, null, alteredDatabase_.getParameters());
    }

    /**
     * Skip processing this event if either db does not exist in cache or is already
     * synced atleast to this event id.
     * @return
     * @throws CatalogException
     */
    @Override
    protected boolean shouldSkipWhenSyncingToLatestEventId() throws CatalogException {
      Preconditions.checkState(
          BackendConfig.INSTANCE.enableSyncToLatestEventOnDdls(),
          "sync to latest event id flag should be set");
      long eventId = this.getEventId();
      Db db = catalog_.getDb(getDbName());
      if (db == null) {
        infoLog("Skipping since db {} does not exist in cache", getDbName());
        return true;
      }
      if (!catalog_.tryLockDb(db)) {
        throw new CatalogException(String.format("Couldn't acquire lock on db %s",
            db.getName()));
      }
      catalog_.getLock().writeLock().unlock();
      boolean shouldSkip = false;
      if (db.getLastSyncedEventId() >= eventId) {
        infoLog("Skipping on db {} since db is already synced till event id {}",
            getDbName(), db.getLastSyncedEventId());
        shouldSkip = true;
      }
      db.getLock().unlock();
      return shouldSkip;
    }
  }

  /**
   * MetastoreEvent for the DROP_DATABASE event
   */
  public static class DropDatabaseEvent extends MetastoreDatabaseEvent {

    public static final String EVENT_TYPE = "DROP_DATABASE";
    // Metastore database object as parsed from NotificationEvent message
    private final Database droppedDatabase_;
    /**
     * Prevent instantiation from outside should use MetastoreEventFactory instead
     */
    private DropDatabaseEvent(
        CatalogOpExecutor catalogOpExecutor, Metrics metrics, NotificationEvent event)
        throws MetastoreNotificationException {
      super(catalogOpExecutor, metrics, event);
      Preconditions.checkArgument(
          MetastoreEventType.DROP_DATABASE.equals(getEventType()));
      JSONDropDatabaseMessage dropDatabaseMessage =
          (JSONDropDatabaseMessage) MetastoreEventsProcessor.getMessageDeserializer()
              .getDropDatabaseMessage(event.getMessage());
      try {
        droppedDatabase_ =
            Preconditions
                .checkNotNull(MetastoreShim.getDatabaseObject(dropDatabaseMessage));
      } catch (Exception e) {
        throw new MetastoreNotificationException(debugString(
            "Database object is null in the event. "
                + "This could be a metastore configuration problem. "
                + "Check if %s is set to true in metastore configuration",
            MetastoreEventsProcessor.HMS_ADD_THRIFT_OBJECTS_IN_EVENTS_CONFIG_KEY), e);
      }
    }

    @Override
    public SelfEventContext getSelfEventContext() {
      throw new UnsupportedOperationException("Self-event evaluation is not needed for "
          + "this event");
    }

    @Override
    protected boolean shouldSkipWhenSyncingToLatestEventId() {
      return false;
    }

    /**
     * Process the drop database event. This handler removes the db object from catalog
     * only if the CREATION_TIME of the catalog's database object is lesser than or equal
     * to that of the database object present in the notification event. If the
     * CREATION_TIME of the catalog's DB object is greater than that of the notification
     * event's DB object, it means that the Database object present in the catalog is a
     * later version and we can skip the event. (For instance, when user does a create db,
     * drop db and create db again with the same dbName.).
     * The creation_time from HMS is unfortunately in seconds granularity, which means
     * there is a limitation that we cannot distinguish between databases which are
     * created with the same name within a second. So a sequence of create_database,
     * drop_database, create_database happening within the same second might cause
     * false positives on drop_database event processing. This is not a huge problem
     * since the databases will eventually be created when the create events are
     * processed but there will be a non-zero amount of time when the database will not
     * be existing in catalog.
     */
    @Override
    public void process() {
      boolean dbRemoved = catalogOpExecutor_
          .removeDbIfNotAddedLater(getEventId(), droppedDatabase_.getName());
      if (dbRemoved) {
        infoLog("Removed Database {} ", dbName_);
        metrics_.getCounter(MetastoreEventsProcessor.NUMBER_OF_DATABASES_REMOVED).inc();
      } else {
        metrics_.getCounter(MetastoreEventsProcessor.EVENTS_SKIPPED_METRIC).inc();
        debugLog("Incremented skipped metric to " + metrics_
            .getCounter(MetastoreEventsProcessor.EVENTS_SKIPPED_METRIC).getCount());
      }
    }
  }

  /**
   * Returns a list of parameters that are set by Hive for tables/partitions that can be
   * ignored to determine if the alter table/partition event is a trivial one.
   */
  @VisibleForTesting
  static final List<String> parametersToIgnore =
      new ImmutableList.Builder<String>()
      .add("transient_lastDdlTime")
      .add("totalSize")
      .add("numFilesErasureCoded")
      .add("numFiles")
      .build();

  /**
   * Util method that sets the parameters that can be ignored equal before and after
   * event.
   */
  private static void setTrivialParameters(Map<String, String> parametersBefore,
      Map<String, String> parametersAfter) {
    for (String parameter: parametersToIgnore) {
      String val = parametersBefore.get(parameter);
      if (val == null) {
        parametersAfter.remove(parameter);
      } else {
        parametersAfter.put(parameter, val);
      }
    }
  }
  public static class AddPartitionEvent extends MetastoreTableEvent {

    public static final String EVENT_TYPE = "ADD_PARTITION";
    private final List<Partition> addedPartitions_;
    private final List<List<TPartitionKeyValue>> partitionKeyVals_;

    /**
     * Prevent instantiation from outside should use MetastoreEventFactory instead
     */
    private AddPartitionEvent(CatalogOpExecutor catalogOpExecutor, Metrics metrics,
        NotificationEvent event) throws MetastoreNotificationException {
      super(catalogOpExecutor, metrics, event);
      Preconditions.checkState(getEventType().equals(MetastoreEventType.ADD_PARTITION));
      if (event.getMessage() == null) {
        throw new IllegalStateException(debugString("Event message is null"));
      }
      try {
        AddPartitionMessage addPartitionMessage_ =
            MetastoreEventsProcessor.getMessageDeserializer()
                .getAddPartitionMessage(event.getMessage());
        addedPartitions_ =
            Lists.newArrayList(addPartitionMessage_.getPartitionObjs());
        // it is possible that the added partitions is empty in certain cases. See
        // IMPALA-8847 for example
        msTbl_ = addPartitionMessage_.getTableObj();
        MetaStoreUtil.replaceSchemaFromTable(addedPartitions_, msTbl_);
        partitionKeyVals_ = new ArrayList<>(addedPartitions_.size());
        for (Partition part : addedPartitions_) {
          partitionKeyVals_.add(getTPartitionSpecFromHmsPartition(msTbl_, part));
        }
      } catch (Exception ex) {
        throw new MetastoreNotificationException(ex);
      }
    }

    @Override
    public SelfEventContext getSelfEventContext() {
      // self event evaluation is only done for transactional tables currently.
      // for non-transactional tables we use the partition level createEventId
      if (AcidUtils.isTransactionalTable(msTbl_.getParameters())) {
        Map<String, String> params = new HashMap<>();
        // all the partitions are added as one transaction and hence we expect all the
        // added partitions to have the same catalog service identifiers. Using the first
        // one for the params is enough for the purpose of self-event evaluation
        if (!addedPartitions_.isEmpty()) {
          params.putAll(addedPartitions_.get(0).getParameters());
        }
        return new SelfEventContext(dbName_, tblName_, partitionKeyVals_,
            params);
      }
      throw new UnsupportedOperationException("Self-event evaluation is unnecessary for"
          + " this event type");
    }

    @Override
    public void processTableEvent() throws MetastoreNotificationException,
        CatalogException {
      // bail out early if there are not partitions to process
      if (addedPartitions_.isEmpty()) {
        metrics_.getCounter(MetastoreEventsProcessor.EVENTS_SKIPPED_METRIC).inc();
        infoLog("Partition list is empty. Ignoring this event.");
        return;
      }
      try {
        // Reload the whole table if it's a transactional table and incremental
        // refresh is not enabled. Materialized views are treated as a special case
        // because it's possible to receive partition event on MVs, but they are
        // regular views in Impala. That cause problems on the reloading partition
        // logic which expects it to be a HdfsTable.
        boolean incrementalRefresh =
            BackendConfig.INSTANCE.getHMSEventIncrementalRefreshTransactionalTable();
        if ((AcidUtils.isTransactionalTable(msTbl_.getParameters()) && !isSelfEvent() &&
            !incrementalRefresh) || MetaStoreUtils.isMaterializedViewTable(msTbl_)) {
          reloadTableFromCatalog("ADD_PARTITION", true);
        } else {
          // HMS adds partitions in a transactional way. This means there may be multiple
          // HMS partition objects in an add_partition event. We try to do the same here
          // by refreshing all those partitions in a loop. If any partition refresh fails,
          // we throw MetastoreNotificationNeedsInvalidateException exception. We skip
          // refresh of the partitions if the table is not present in the catalog.
          int numPartsAdded = catalogOpExecutor_
              .addPartitionsIfNotRemovedLater(getEventId(), dbName_, tblName_,
                  addedPartitions_, "ADD_PARTITION");
          if (numPartsAdded != 0) {
            infoLog("Successfully added {} partitions to table {}",
                numPartsAdded, getFullyQualifiedTblName());
            metrics_.getCounter(MetastoreEventsProcessor.NUMBER_OF_PARTITIONS_ADDED)
                .inc(numPartsAdded);
          } else {
            metrics_.getCounter(MetastoreEventsProcessor.EVENTS_SKIPPED_METRIC).inc();
            debugLog("Incremented skipped metric to " + metrics_
                .getCounter(MetastoreEventsProcessor.EVENTS_SKIPPED_METRIC).getCount());
          }
        }
      } catch (CatalogException e) {
        throw new MetastoreNotificationNeedsInvalidateException(debugString("Failed to "
                + "refresh newly added partitions of table '%s'. Event processing cannot "
                + "continue. Issue an invalidate metadata command to reset event "
                + "processor.", getFullyQualifiedTblName()), e);
      }
    }

    public List<Partition> getPartitions() {
      return addedPartitions_;
    }
  }

  public static class AlterPartitionEvent extends MetastoreTableEvent {
    public static final String EVENT_TYPE = "ALTER_PARTITION";
    // the Partition object before alter operation, as parsed from the NotificationEvent
    private final org.apache.hadoop.hive.metastore.api.Partition partitionBefore_;
    // the Partition object after alter operation, as parsed from the NotificationEvent
    private final org.apache.hadoop.hive.metastore.api.Partition partitionAfter_;
    // the version number from the partition parameters of the event.
    private final long versionNumberFromEvent_;
    // the service id from the partition parameters of the event.
    private final String serviceIdFromEvent_;
    // true if this alter event was due to a truncate operation in metastore
    private final boolean isTruncateOp_;

    /**
     * Prevent instantiation from outside should use MetastoreEventFactory instead
     */
    private AlterPartitionEvent(CatalogOpExecutor catalogOpExecutor, Metrics metrics,
        NotificationEvent event) throws MetastoreNotificationException {
      super(catalogOpExecutor, metrics, event);
      Preconditions.checkState(getEventType().equals(MetastoreEventType.ALTER_PARTITION));
      Preconditions.checkNotNull(event.getMessage());
      AlterPartitionMessage alterPartitionMessage =
          MetastoreEventsProcessor.getMessageDeserializer()
              .getAlterPartitionMessage(event.getMessage());

      try {
        partitionBefore_ =
            Preconditions.checkNotNull(alterPartitionMessage.getPtnObjBefore());
        partitionAfter_ =
            Preconditions.checkNotNull(alterPartitionMessage.getPtnObjAfter());
        isTruncateOp_ = alterPartitionMessage.getIsTruncateOp();
        msTbl_ = alterPartitionMessage.getTableObj();
        Map<String, String> parameters = partitionAfter_.getParameters();
        versionNumberFromEvent_ = Long.parseLong(
            MetastoreEvents.getStringProperty(parameters,
                MetastoreEventPropertyKey.CATALOG_VERSION.getKey(), "-1"));
        serviceIdFromEvent_ = MetastoreEvents.getStringProperty(
            parameters, MetastoreEventPropertyKey.CATALOG_SERVICE_ID.getKey(), "");
      } catch (Exception e) {
        throw new MetastoreNotificationException(
            debugString("Unable to parse the alter partition message"), e);
      }
    }

    @Override
    protected MetastoreEventType getBatchEventType() {
      return MetastoreEventType.ALTER_PARTITIONS;
    }

    @Override
    protected Partition getPartitionForBatching() { return partitionAfter_; }

    @Override
    public boolean canBeBatched(MetastoreEvent event) {
      if (!(event instanceof AlterPartitionEvent)) return false;
      if (isOlderThanLastSyncEventId(event)) return false;
      AlterPartitionEvent alterPartitionEvent = (AlterPartitionEvent) event;
      // make sure that the event is on the same table
      if (!getFullyQualifiedTblName().equalsIgnoreCase(
          alterPartitionEvent.getFullyQualifiedTblName())) {
        return false;
      }

      // in case of ALTER_PARTITION we only batch together the events
      // which have same versionNumber and serviceId from the event. This simplifies
      // the self-event evaluation for the batch since either the whole batch is
      // self-events or not.
      Map<String, String> parametersFromEvent =
          alterPartitionEvent.partitionAfter_.getParameters();
      long versionNumberOfEvent = Long.parseLong(
          MetastoreEvents.getStringProperty(parametersFromEvent,
              MetastoreEventPropertyKey.CATALOG_VERSION.getKey(), "-1"));
      String serviceIdOfEvent = MetastoreEvents.getStringProperty(parametersFromEvent,
          MetastoreEventPropertyKey.CATALOG_SERVICE_ID.getKey(), "");
      return versionNumberFromEvent_ == versionNumberOfEvent
          && serviceIdFromEvent_.equals(serviceIdOfEvent);
    }

    @Override
    public MetastoreEvent addToBatchEvents(MetastoreEvent event) {
      if (!(event instanceof AlterPartitionEvent)) return null;
      BatchPartitionEvent<AlterPartitionEvent> batchEvent = new BatchPartitionEvent<>(
          this);
      Preconditions.checkState(batchEvent.canBeBatched(event));
      batchEvent.addToBatchEvents(event);
      return batchEvent;
    }

    @Override
    public void processTableEvent() throws MetastoreNotificationException,
        CatalogException {
      if (isSelfEvent()) {
        infoLog("Not processing the event as it is a self-event");
        return;
      }

      if (isOlderEvent(partitionBefore_)) {
        infoLog("Not processing the alter partition event {} as it is an older event",
            getEventId());
        return;
      }

      // Ignore the event if this is a trivial event. See javadoc for
      // isTrivialAlterPartitionEvent() for examples.
      if (canBeSkipped()) {
        infoLog("Not processing this event as it only modifies some partition "
            + "parameters which can be ignored.");
        return;
      }
      // Reload the whole table if it's a transactional table or materialized view.
      // Materialized views are treated as a special case because it's possible to
      // receive partition event on MVs, but they are regular views in Impala. That
      // cause problems on the reloading partition logic which expects it to be a
      // HdfsTable.
      if (AcidUtils.isTransactionalTable(msTbl_.getParameters())
          || MetaStoreUtils.isMaterializedViewTable(msTbl_)) {
        reloadTransactionalTable();
      } else {
        // Refresh the partition that was altered.
        Preconditions.checkNotNull(partitionAfter_);
        List<TPartitionKeyValue> tPartSpec = getTPartitionSpecFromHmsPartition(msTbl_,
            partitionAfter_);
        try {
          // load file metadata only if storage descriptor of partitionAfter_ differs
          // from sd of HdfsPartition. If the alter_partition event type is of truncate
          // then force load the file metadata.
          FileMetadataLoadOpts fileMetadataLoadOpts =
              isTruncateOp_ ? FileMetadataLoadOpts.FORCE_LOAD :
                  FileMetadataLoadOpts.LOAD_IF_SD_CHANGED;
          reloadPartitions(Arrays.asList(partitionAfter_), fileMetadataLoadOpts,
              "ALTER_PARTITION event");
        } catch (CatalogException e) {
          throw new MetastoreNotificationNeedsInvalidateException(
              debugString("Refresh partition on table {} partition {} failed. Event " +
                  "processing cannot continue. Issue an invalidate command to reset " +
                  "the event processor state.", getFullyQualifiedTblName(),
                  HdfsTable.constructPartitionName(tPartSpec)), e);
        }
      }
    }

    @Override
    protected boolean canBeSkipped() {
      // Certain alter events just modify some parameters such as
      // "transient_lastDdlTime" in Hive. For eg: the alter table event generated
      // along with insert events. Check if the alter table event is such a trivial
      // event by setting those parameters equal before and after the event and
      // comparing the objects.

      // alter partition event from truncate ops always can't be skipped.
      if (isTruncateOp_) {
        return false;
      }

      // Avoid modifying the object from event.
      Partition afterPartition = partitionAfter_.deepCopy();
      setTrivialParameters(partitionBefore_.getParameters(),
          afterPartition.getParameters());
      return afterPartition.equals(partitionBefore_);
    }

    @Override
    public SelfEventContext getSelfEventContext() {
      return new SelfEventContext(dbName_, tblName_,
          Arrays.asList(getTPartitionSpecFromHmsPartition(msTbl_, partitionAfter_)),
          partitionAfter_.getParameters());
    }

    private void reloadTransactionalTable() throws CatalogException {
      boolean incrementalRefresh =
          BackendConfig.INSTANCE.getHMSEventIncrementalRefreshTransactionalTable();
      if (incrementalRefresh) {
        reloadPartitionsFromEvent(Collections.singletonList(partitionAfter_),
            "ALTER_PARTITION EVENT FOR TRANSACTIONAL TABLE");
      } else {
        reloadTableFromCatalog("ALTER_PARTITION", true);
      }
    }
  }

  /**
   * This event represents a batch of events of type T. The batch of events is
   * initialized from a single initial event called baseEvent. More events can be added
   * to the batch using {@code addToBatchEvents} method. Currently we only support
   * ALTER_PARTITION and INSERT partition events for batching.
   * @param <T> The type of event which is batched by this event.
   */
  public static class BatchPartitionEvent<T extends MetastoreTableEvent> extends
      MetastoreTableEvent {
    private final T baseEvent_;
    private final List<T> batchedEvents_ = new ArrayList<>();

    private BatchPartitionEvent(T baseEvent) {
      super(baseEvent.catalogOpExecutor_, baseEvent.metrics_, baseEvent.event_);
      this.msTbl_ = baseEvent.msTbl_;
      this.baseEvent_ = baseEvent;
      batchedEvents_.add(baseEvent);
      // override the eventType_ to represent that this is a batch of events.
      setEventType(baseEvent.getBatchEventType());
    }

    @Override
    public MetastoreEvent addToBatchEvents(MetastoreEvent event) {
      Preconditions.checkState(canBeBatched(event));
      batchedEvents_.add((T) event);
      return this;
    }

    @Override
    public int getNumberOfEvents() { return batchedEvents_.size(); }

    /**
     * Return the event id of this batch event. We return the last eventId
     * from this batch which is important since it is used to determined the event
     * id for fetching next set of events from metastore.
     */
    @Override
    public long getEventId() {
      Preconditions.checkState(!batchedEvents_.isEmpty());
      return batchedEvents_.get(batchedEvents_.size()-1).getEventId();
    }

    /**
     * Same as the above but returns the event time.
     */
    @Override
    public long getEventTime() {
      Preconditions.checkState(!batchedEvents_.isEmpty());
      return batchedEvents_.get(batchedEvents_.size() - 1).getEventTime();
    }

    /**
     *
     * @param event The event under consideration to be batched into this event. It can
     *              be added to the batch if it can be batched into the last event of the
     *              current batch.
     * @return true if we can add the event to the current batch; else false.
     */
    @Override
    public boolean canBeBatched(MetastoreEvent event) {
      Preconditions.checkState(!batchedEvents_.isEmpty());
      return batchedEvents_.get(batchedEvents_.size()-1).canBeBatched(event);
    }

    @VisibleForTesting
    List<T> getBatchEvents() { return batchedEvents_; }

    @Override
    protected void processTableEvent() throws MetastoreNotificationException,
        CatalogException {
      if (isSelfEvent()) {
        infoLog("Not processing the event as it is a self-event");
        return;
      }
      // Ignore the event if this is a trivial event. See javadoc for
      // isTrivialAlterPartitionEvent() for examples.
      List<T> eventsToProcess = new ArrayList<>();
      List<Partition> partitionEventsToForceReload = new ArrayList<>();
      for (T event : batchedEvents_) {
        if (isOlderEvent(event.getPartitionForBatching())) {
          infoLog("Not processing the current event id {} as it is an older event",
              event.getEventId());
          continue;
        }
        boolean isTruncateOp = (event instanceof AlterPartitionEvent &&
            ((AlterPartitionEvent)event).isTruncateOp_);
        if (isTruncateOp) {
          partitionEventsToForceReload.add(event.getPartitionForBatching());
        } else if (!event.canBeSkipped()){
          eventsToProcess.add(event);
        }
      }
      if (eventsToProcess.isEmpty() && partitionEventsToForceReload.isEmpty()) {
        LOG.info(
            "Ignoring {} events between event id {} and {} since they modify parameters"
            + " which can be ignored", getNumberOfEvents(), getFirstEventId(),
            getLastEventId());
        return;
      }

      // Reload the whole table if it's a transactional table.
      if (AcidUtils.isTransactionalTable(msTbl_.getParameters())) {
        reloadTableFromCatalog(getEventType().toString(), true);
      } else {
        // Reload the partitions from the batch.
        List<Partition> partitions = new ArrayList<>();
        for (T event : eventsToProcess) {
          partitions.add(event.getPartitionForBatching());
        }
        try {
          if (baseEvent_ instanceof InsertEvent) {
            // for insert event, always reload file metadata so that new files
            // are reflected in HdfsPartition
            reloadPartitions(partitions, FileMetadataLoadOpts.FORCE_LOAD,
                getEventType().toString() + " event");
          } else {
            if (!partitionEventsToForceReload.isEmpty()) {
              // force reload truncated partitions
              reloadPartitions(partitionEventsToForceReload,
                  FileMetadataLoadOpts.FORCE_LOAD, getEventType().toString() + " event");
            }
            if (!partitions.isEmpty()) {
              // alter partition event. Reload file metadata of only those partitions
              // for which sd has changed
              reloadPartitions(partitions, FileMetadataLoadOpts.LOAD_IF_SD_CHANGED,
                  getEventType().toString() + " event");
            }
          }
        } catch (CatalogException e) {
          throw new MetastoreNotificationNeedsInvalidateException(String.format(
              "Refresh partitions on table %s failed when processing a batch of %s "
              + "events between event ids %s and %s. "
              + "Issue an invalidate command to reset the event processor state.",
              getFullyQualifiedTblName(), getNumberOfEvents(), getFirstEventId(),
              getLastEventId()), e);
        }
      }
    }

    /**
     * Gets the event id of the first event in the batch.
     */
    private long getFirstEventId() {
      return batchedEvents_.get(0).getEventId();
    }

    /**
     * Gets the event id of the last event in the batch.
     */
    private long getLastEventId() {
      return batchedEvents_.get(batchedEvents_.size()-1).getEventId();
    }

    @Override
    protected SelfEventContext getSelfEventContext() {
      List<List<TPartitionKeyValue>> partitionKeyValues = new ArrayList<>();
      List<Long> eventIds = new ArrayList<>();
      // We treat insert event as a special case since the self-event context for an
      // insert event is generated differently using the eventIds.
      boolean isInsertEvent = baseEvent_ instanceof InsertEvent;
      for (T event : batchedEvents_) {
        partitionKeyValues.add(
            getTPartitionSpecFromHmsPartition(event.msTbl_,
                event.getPartitionForBatching()));
        eventIds.add(event.getEventId());
      }
      return new SelfEventContext(dbName_, tblName_, partitionKeyValues,
          baseEvent_.getPartitionForBatching().getParameters(),
          isInsertEvent ? eventIds : null);
    }
  }

  public static class DropPartitionEvent extends MetastoreTableEvent {
    private final List<Map<String, String>> droppedPartitions_;
    public static final String EVENT_TYPE = "DROP_PARTITION";

    /**
     * Prevent instantiation from outside should use MetastoreEventFactory instead
     */
    private DropPartitionEvent(CatalogOpExecutor catalogOpExecutor, Metrics metrics,
        NotificationEvent event) throws MetastoreNotificationException {
      super(catalogOpExecutor, metrics, event);
      Preconditions.checkState(getEventType().equals(MetastoreEventType.DROP_PARTITION));
      Preconditions.checkNotNull(event.getMessage());
      DropPartitionMessage dropPartitionMessage =
          MetastoreEventsProcessor.getMessageDeserializer()
              .getDropPartitionMessage(event.getMessage());
      try {
        msTbl_ = Preconditions.checkNotNull(dropPartitionMessage.getTableObj());
        droppedPartitions_ = dropPartitionMessage.getPartitions();
        Preconditions.checkNotNull(droppedPartitions_);
      } catch (Exception ex) {
        throw new MetastoreNotificationException(
            debugString("Could not parse event message. "
                    + "Check if %s is set to true in metastore configuration",
                MetastoreEventsProcessor.HMS_ADD_THRIFT_OBJECTS_IN_EVENTS_CONFIG_KEY),
            ex);
      }
    }

    public List<Map<String, String>> getDroppedPartitions() {
      return droppedPartitions_;
    }

    @Override
    public void processTableEvent() throws MetastoreNotificationException,
        CatalogException {
      // we have seen cases where a add_partition event is generated with empty
      // partition list (see IMPALA-8547 for details. Make sure that droppedPartitions
      // list is not empty
      if (droppedPartitions_.isEmpty()) {
        metrics_.getCounter(MetastoreEventsProcessor.EVENTS_SKIPPED_METRIC).inc();
        infoLog("Partition list is empty. Ignoring this event.");
        return;
      }
      try {
        // Reload the whole table if it's a transactional table or materialized view.
        // Materialized views are treated as a special case because it's possible to
        // receive partition event on MVs, but they are regular views in Impala. That
        // cause problems on the reloading partition logic which expects it to be a
        // HdfsTable.
        boolean incrementalRefresh =
            BackendConfig.INSTANCE.getHMSEventIncrementalRefreshTransactionalTable();
        if ((AcidUtils.isTransactionalTable(msTbl_.getParameters()) &&
            !incrementalRefresh) || MetaStoreUtils.isMaterializedViewTable(msTbl_)) {
          reloadTableFromCatalog("DROP_PARTITION", true);
        } else {
          int numPartsRemoved = catalogOpExecutor_
              .removePartitionsIfNotAddedLater(getEventId(), dbName_, tblName_,
                  droppedPartitions_, "DROP_PARTITION");
          if (numPartsRemoved > 0) {
            infoLog("{} partitions dropped from table {}", numPartsRemoved,
                getFullyQualifiedTblName());
            metrics_.getCounter(MetastoreEventsProcessor.NUMBER_OF_PARTITIONS_REMOVED)
                .inc(numPartsRemoved);
          } else {
            metrics_.getCounter(MetastoreEventsProcessor.EVENTS_SKIPPED_METRIC).inc();
            debugLog("Incremented skipped metric to " + metrics_
                .getCounter(MetastoreEventsProcessor.EVENTS_SKIPPED_METRIC).getCount());
          }
        }
      } catch (CatalogException e) {
        throw new MetastoreNotificationNeedsInvalidateException(debugString("Failed to "
            + "drop some partitions from table {} after a drop partitions event. Event "
            + "processing cannot continue. Issue an invalidate metadata command to "
            + "reset event processor state.", getFullyQualifiedTblName()), e);
      }
    }

    @Override
    protected SelfEventContext getSelfEventContext() {
      throw new UnsupportedOperationException("self-event evaluation is not needed for "
          + "this event type");
    }
  }

  /**
   * Metastore event handler for ALLOC_WRITE_ID_EVENT events. This event is used to keep
   * track of write ids for partitioned transactional tables.
   */
  public static class AllocWriteIdEvent extends MetastoreTableEvent {
    private final List<TxnToWriteId> txnToWriteIdList_;

    private AllocWriteIdEvent(CatalogOpExecutor catalogOpExecutor,
        Metrics metrics, NotificationEvent event) throws MetastoreNotificationException {
      super(catalogOpExecutor, metrics, event);
      Preconditions.checkState(
          getEventType().equals(MetastoreEventType.ALLOC_WRITE_ID_EVENT));
      Preconditions.checkNotNull(event.getMessage());
      AllocWriteIdMessage allocWriteIdMessage =
          MetastoreEventsProcessor.getMessageDeserializer().getAllocWriteIdMessage(
              event.getMessage());
      txnToWriteIdList_ = allocWriteIdMessage.getTxnToWriteIdList();
    }

    @Override
    protected void processTableEvent() throws MetastoreNotificationException {
      org.apache.impala.catalog.Table tbl = catalog_.getTableNoThrow(dbName_, tblName_);
      if (tbl == null) {
        debugLog("Ignoring the event since table {} does not exist",
            getFullyQualifiedTblName());
        return;
      }
      try {
        List<Long> writeIds = txnToWriteIdList_.stream()
            .map(TxnToWriteId::getWriteId)
            .collect(Collectors.toList());
        catalog_.addWriteIdsToTable(dbName_, tblName_, getEventId(), writeIds,
            MutableValidWriteIdList.WriteIdStatus.OPEN);
        for (TxnToWriteId txnToWriteId : txnToWriteIdList_) {
          TableWriteId tableWriteId = new TableWriteId(
              dbName_, tblName_, tbl.getCreateEventId(), txnToWriteId.getWriteId());
          catalog_.addWriteId(txnToWriteId.getTxnId(), tableWriteId);
          infoLog("Added write id {} on table {}.{} for txn {}",
              txnToWriteId.getWriteId(), dbName_, tblName_, txnToWriteId.getTxnId());
        }
      } catch (CatalogException e) {
        throw new MetastoreNotificationNeedsInvalidateException("Failed to mark open "
            + "write ids to table. Event processing cannot continue. Issue an "
            + "invalidate metadata command to reset event processor.", e);
      }
    }

    @Override
    protected SelfEventContext getSelfEventContext() {
      throw new UnsupportedOperationException("self-event evaluation is not needed for "
          + "this event type");
    }

    @Override
    protected boolean isEventProcessingDisabled() {
      // TODO:  Have an init method to set fields that cannot be initialized in the
      // event constructors and invoke it as a first step before processing event. It
      // can be useful for other such events.
      org.apache.impala.catalog.Table tbl = catalog_.getTableNoThrow(dbName_, tblName_);
      if (tbl != null && tbl.getCreateEventId() < getEventId()) {
        msTbl_ = tbl.getMetaStoreTable();
      }
      if (msTbl_ == null) {
        return false;
      }
      return super.isEventProcessingDisabled();
    }
  }

  /**
   *  Metastore event handler for Reload events. A reload event can be generated by
   *  refresh table/partition or invalidate table event. Handles reload events at both
   *  table and partition scopes (If applicable).
   */
  public static class ReloadEvent extends MetastoreTableEvent {

    // The partition for this reload event. Null if the table is unpartitioned
    private Partition reloadPartition_;

    // if isRefresh_ is set to true then it is refresh query, else it is invalidate query
    private boolean isRefresh_;

    /**
     * Prevent instantiation from outside should use MetastoreEventFactory instead
     */
    @VisibleForTesting
    ReloadEvent(CatalogOpExecutor catalogOpExecutor, Metrics metrics,
        NotificationEvent event) throws MetastoreNotificationException {
      super(catalogOpExecutor, metrics, event);
      Preconditions.checkArgument(MetastoreEventType.RELOAD.equals(getEventType()));
      try {
        Map<String, Object> updatedFields =
            MetastoreShim.getFieldsFromReloadEvent(event);
        msTbl_ = (org.apache.hadoop.hive.metastore.api.Table)Preconditions.checkNotNull(
            updatedFields.get("table"));
        reloadPartition_ = (Partition)updatedFields.get("partition");
        isRefresh_ = (boolean)updatedFields.get("isRefresh");
      } catch (Exception e) {
        throw new MetastoreNotificationException(debugString("Unable to "
                + "parse reload message"), e);
      }
    }

    @Override
    public SelfEventContext getSelfEventContext() {
      throw new UnsupportedOperationException("Self-event evaluation is unnecessary for"
          + " this event type");
    }

    @Override
    public void processTableEvent() throws MetastoreNotificationException {
      if (isOlderEvent()) {
        metrics_.getCounter(MetastoreEventsProcessor.EVENTS_SKIPPED_METRIC)
            .inc(getNumberOfEvents());
        infoLog("Incremented events skipped counter to {}",
            metrics_.getCounter(MetastoreEventsProcessor.EVENTS_SKIPPED_METRIC)
                .getCount());
        return;
      }
      if (isRefresh_) {
        if (reloadPartition_ != null) {
          processPartitionReload();
        } else {
          processTableReload();
        }
      } else {
        processTableInvalidate();
      }
    }

    private boolean isOlderEvent() {
      org.apache.impala.catalog.Table tbl = catalog_.getTableNoThrow(dbName_, tblName_);
      if (tbl == null || tbl instanceof IncompleteTable) { return false; }
      // Always check the lastRefreshEventId on the table first for table level refresh
      if (tbl.getLastRefreshEventId() >= getEventId()
          || (reloadPartition_ != null
                 && catalog_.isPartitionLoadedAfterEvent(
                        dbName_, tblName_, reloadPartition_, getEventId()))) {
        return true;
      }
      return false;
    }

    /**
     * Process partition reload
     */
    private void processPartitionReload() throws MetastoreNotificationException {
      // For partitioned table, refresh the partition only.
      Preconditions.checkNotNull(reloadPartition_);
      try {
        // Ignore event if table or database is not in catalog. Throw exception if
        // refresh fails. If the partition does not exist in metastore the reload
        // method below removes it from the catalog
        // forcing file metadata reload so that new files (due to refresh) are reflected
        // HdfsPartition
        reloadPartitions(Arrays.asList(reloadPartition_),
            FileMetadataLoadOpts.FORCE_LOAD, "RELOAD event");
      } catch (CatalogException e) {
        throw new MetastoreNotificationNeedsInvalidateException(debugString("Refresh "
            + "partition on table {} partition {} failed. Event processing cannot "
            + "continue. Issue an invalidate metadata command to reset the event "
            + "processor state.", getFullyQualifiedTblName(),
            Joiner.on(',').join(reloadPartition_.getValues())), e);
      }
    }

    /**
     *  Process unpartitioned table reload
     */
    private void processTableReload() throws MetastoreNotificationException {
      // For non-partitioned tables, refresh the whole table.
      Preconditions.checkState(reloadPartition_ == null);
      try {
        // we always treat the table as non-transactional so all the files are reloaded
        reloadTableFromCatalog("RELOAD event", false);
      } catch (CatalogException e) {
        throw new MetastoreNotificationNeedsInvalidateException(
            debugString("Refresh table {} failed. Event processing "
            + "cannot continue. Issue an invalidate metadata " +
            "command to reset the event processor state.",
            getFullyQualifiedTblName()), e);
      }
    }

    private void processTableInvalidate() throws MetastoreNotificationException {
      Reference<Boolean> tblWasRemoved = new Reference<>();
      Reference<Boolean> dbWasAdded = new Reference<>();
      org.apache.impala.catalog.Table tbl = null;
      try {
        tbl = catalog_.getTable(dbName_, tblName_);
        if (tbl == null) {
          infoLog("Skipping on table {}.{} since it does not exist in cache", dbName_,
              tblName_);
          return ;
        }
        if (tbl instanceof IncompleteTable) {
          infoLog("Skipping on an incomplete table {}", tbl.getFullName());
          return ;
        }
      } catch (DatabaseNotFoundException e) {
        infoLog("Skipping on table {} because db {} not found in cache", tblName_,
            dbName_);
        return ;
      }
      catalog_.invalidateTable(tbl.getTableName().toThrift(),
          tblWasRemoved, dbWasAdded, NoOpEventSequence.INSTANCE, getEventId());
      LOG.info("Table " + tbl.getFullName() + " is invalidated from catalog cache");
    }
  }

  /**
   * Metastore event handler for ABORT_TXN events. Handles abort event for transactional
   * tables.
   */
  public static class AbortTxnEvent extends MetastoreEvent {
    private final long txnId_;
    private Set<TableWriteId> tableWriteIds_ = Collections.emptySet();

    AbortTxnEvent(CatalogOpExecutor catalogOpExecutor, Metrics metrics,
        NotificationEvent event) {
      super(catalogOpExecutor, metrics, event);
      Preconditions.checkState(getEventType().equals(MetastoreEventType.ABORT_TXN));
      Preconditions.checkNotNull(event.getMessage());
      AbortTxnMessage abortTxnMessage =
          MetastoreEventsProcessor.getMessageDeserializer().getAbortTxnMessage(
              event.getMessage());
      txnId_ = abortTxnMessage.getTxnId();
      infoLog("Received AbortTxnEvent for transaction " + txnId_);
    }

    @Override
    protected void process() throws MetastoreNotificationException {
      try {
        tableWriteIds_ = catalog_.getWriteIds(txnId_);
        infoLog("Adding {} aborted write ids", tableWriteIds_.size());
        addAbortedWriteIdsToTables(tableWriteIds_);
      } catch (CatalogException e) {
        throw new MetastoreNotificationNeedsInvalidateException(debugString("Failed to "
            + "mark aborted write ids to table for txn {}. Event processing cannot "
            + "continue. Issue an invalidate metadata command to reset event processor.",
            txnId_), e);
      } finally {
        catalog_.removeWriteIds(txnId_);
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

    private void addAbortedWriteIdsToTables(Set<TableWriteId> tableWriteIds)
        throws CatalogException {
      for (TableWriteId tableWriteId: tableWriteIds) {
        catalog_.addWriteIdsToTable(tableWriteId.getDbName(), tableWriteId.getTblName(),
            getEventId(), Collections.singletonList(tableWriteId.getWriteId()),
            MutableValidWriteIdList.WriteIdStatus.ABORTED);
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

  /**
   * Metastore event handler for COMMIT_COMPACTION events. Handles
   * COMMIT_COMPACTION event for transactional tables.
   */
  public static class CommitCompactionEvent extends MetastoreTableEvent {
    private String partitionName_;

    CommitCompactionEvent(CatalogOpExecutor catalogOpExecutor, Metrics metrics,
        NotificationEvent event) throws MetastoreNotificationException {
      super(catalogOpExecutor, metrics, event);
      Preconditions.checkState(
          getEventType().equals(MetastoreEventType.COMMIT_COMPACTION));
      Preconditions.checkNotNull(event.getMessage());
      try {
        partitionName_ =
            MetastoreShim.getPartitionNameFromCommitCompactionEvent(event);
      } catch (Exception ex) {
        warnLog("Unable to parse commit compaction message: {}", ex.getMessage());
      }
    }

    @Override
    protected void processTableEvent() throws MetastoreNotificationException {
      try {
        if (partitionName_ == null) {
          reloadTableFromCatalog("Commit Compaction event", true);
        } else {
          reloadPartitionsFromNames(Arrays.asList(partitionName_),
                  "Commit compaction event", FileMetadataLoadOpts.FORCE_LOAD);
        }
      } catch (CatalogException e) {
        throw new MetastoreNotificationNeedsInvalidateException(debugString("Failed to "
            + "commit compaction for the table {}. Event processing cannot "
            + "continue. Issue an invalidate metadata command to reset " +
            "event processor.", tblName_), e);
      }
    }

    @Override
    protected SelfEventContext getSelfEventContext() {
      throw new UnsupportedOperationException("Self-event evaluation is not needed for "
          + "this event type");
    }

    @Override
    protected boolean isEventProcessingDisabled() {
      org.apache.impala.catalog.Table tbl = catalog_.getTableNoThrow(dbName_, tblName_);
      if (tbl != null && tbl.getCreateEventId() < getEventId()) {
        msTbl_ = tbl.getMetaStoreTable();
      }
      if (msTbl_ == null) {
        return false;
      }
      return super.isEventProcessingDisabled();
    }
  }

  /**
   * An event type which is ignored. Useful for unsupported metastore event types
   */
  public static class IgnoredEvent extends MetastoreEvent {

    /**
     * Prevent instantiation from outside should use MetastoreEventFactory instead
     */
    private IgnoredEvent(
        CatalogOpExecutor catalogOpExecutor, Metrics metrics, NotificationEvent event) {
      super(catalogOpExecutor, metrics, event);
    }

    @Override
    public void process() {
      debugLog(
          "Ignoring unknown event type " + metastoreNotificationEvent_.getEventType());
    }

    @Override
    protected boolean isEventProcessingDisabled() {
      return false;
    }

    @Override
    protected boolean shouldSkipWhenSyncingToLatestEventId() {
      return false;
    }

    @Override
    protected SelfEventContext getSelfEventContext() {
      throw new UnsupportedOperationException("Self-event evaluation is not needed for "
          + "this event type");
    }

    @Override
    protected boolean onFailure(Exception e) {
      return false;
    }
  }
}
