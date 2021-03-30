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

import com.google.common.annotations.VisibleForTesting;
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

import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.NotificationEvent;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.NoSuchObjectException;
import org.apache.hadoop.hive.metastore.messaging.AddPartitionMessage;
import org.apache.hadoop.hive.metastore.messaging.AlterPartitionMessage;
import org.apache.hadoop.hive.metastore.messaging.CreateTableMessage;
import org.apache.hadoop.hive.metastore.messaging.DropPartitionMessage;
import org.apache.hadoop.hive.metastore.messaging.json.JSONAlterDatabaseMessage;
import org.apache.hadoop.hive.metastore.messaging.InsertMessage;
import org.apache.hadoop.hive.metastore.messaging.json.JSONAlterTableMessage;
import org.apache.hadoop.hive.metastore.messaging.json.JSONCreateDatabaseMessage;
import org.apache.hadoop.hive.metastore.messaging.json.JSONDropDatabaseMessage;
import org.apache.hadoop.hive.metastore.messaging.json.JSONDropTableMessage;
import org.apache.impala.analysis.TableName;
import org.apache.impala.catalog.CatalogException;
import org.apache.impala.catalog.CatalogServiceCatalog;
import org.apache.impala.catalog.DatabaseNotFoundException;
import org.apache.impala.catalog.Db;
import org.apache.impala.catalog.HdfsTable;
import org.apache.impala.catalog.Table;
import org.apache.impala.catalog.TableLoadingException;
import org.apache.impala.catalog.TableNotFoundException;
import org.apache.impala.catalog.TableNotLoadedException;
import org.apache.impala.common.Metrics;
import org.apache.impala.common.Pair;
import org.apache.impala.common.Reference;
import org.apache.impala.thrift.TPartitionKeyValue;
import org.apache.impala.thrift.TTableName;
import org.apache.impala.util.AcidUtils;
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
    DROP_PARTITION("DROP_PARTITION"),
    INSERT("INSERT"),
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
  public static class MetastoreEventFactory {

    private static final Logger LOG =
        LoggerFactory.getLogger(MetastoreEventFactory.class);

    // catalog service instance to be used for creating eventHandlers
    private final CatalogServiceCatalog catalog_;
    // metrics registry to be made available for each events to publish metrics
    private final Metrics metrics_;

    public MetastoreEventFactory(CatalogServiceCatalog catalog, Metrics metrics) {
      this.catalog_ = Preconditions.checkNotNull(catalog);
      this.metrics_ = Preconditions.checkNotNull(metrics);
    }

    /**
     * creates instance of <code>MetastoreEvent</code> used to process a given event type.
     * If the event type is unknown, returns a IgnoredEvent
     */
    private MetastoreEvent get(NotificationEvent event)
        throws MetastoreNotificationException {
      Preconditions.checkNotNull(event.getEventType());
      MetastoreEventType metastoreEventType =
          MetastoreEventType.from(event.getEventType());
      switch (metastoreEventType) {
        case CREATE_TABLE: return new CreateTableEvent(catalog_, metrics_, event);
        case DROP_TABLE: return new DropTableEvent(catalog_, metrics_, event);
        case ALTER_TABLE: return new AlterTableEvent(catalog_, metrics_, event);
        case CREATE_DATABASE: return new CreateDatabaseEvent(catalog_, metrics_, event);
        case DROP_DATABASE: return new DropDatabaseEvent(catalog_, metrics_, event);
        case ALTER_DATABASE: return new AlterDatabaseEvent(catalog_, metrics_, event);
        case ADD_PARTITION: return new AddPartitionEvent(catalog_, metrics_, event);
        case DROP_PARTITION: return new DropPartitionEvent(catalog_, metrics_, event);
        case ALTER_PARTITION: return new AlterPartitionEvent(catalog_, metrics_, event);
        case INSERT: return new InsertEvent(catalog_, metrics_, event);
        default:
          // ignore all the unknown events by creating a IgnoredEvent
          return new IgnoredEvent(catalog_, metrics_, event);
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
     * @return A list of MetastoreEvents corresponding to the given the NotificationEvents
     * @throws MetastoreNotificationException if a NotificationEvent could not be
     *     parsed into MetastoreEvent
     */
    List<MetastoreEvent> getFilteredEvents(List<NotificationEvent> events)
        throws MetastoreNotificationException {
      Preconditions.checkNotNull(events);
      if (events.isEmpty()) return Collections.emptyList();

      List<MetastoreEvent> metastoreEvents = new ArrayList<>(events.size());
      for (NotificationEvent event : events) {
        metastoreEvents.add(get(event));
      }
      // filter out the create events which has a corresponding drop event later
      int sizeBefore = metastoreEvents.size();
      int numFilteredEvents = 0;
      int i = 0;
      while (i < metastoreEvents.size()) {
        MetastoreEvent currentEvent = metastoreEvents.get(i);
        String eventDb = currentEvent.getDbName();
        String eventTbl = currentEvent.getTableName();
        // if the event is on blacklisted db or table we should filter it out
        if ((eventDb != null && catalog_.isBlacklistedDb(eventDb)) || (eventTbl != null
            && catalog_.isBlacklistedTable(eventDb, eventTbl))) {
          String blacklistedObject = eventTbl != null ? new TableName(eventDb,
              eventTbl).toString() : eventDb;
          LOG.info(currentEvent.debugString("Filtering out this event since it is on a "
              + "blacklisted database or table %s", blacklistedObject));
          metastoreEvents.remove(i);
          numFilteredEvents++;
        } else if (currentEvent.isRemovedAfter(metastoreEvents.subList(i + 1,
            metastoreEvents.size()))) {
          LOG.info(currentEvent.debugString("Filtering out this event since the object is "
              + "either removed or renamed later in the event stream"));
          metastoreEvents.remove(i);
          numFilteredEvents++;
        } else {
          i++;
        }
      }
      LOG.info(String.format("Total number of events received: %d Total number of events "
          + "filtered out: %d", sizeBefore, numFilteredEvents));
      metrics_.getCounter(MetastoreEventsProcessor.EVENTS_SKIPPED_METRIC)
              .inc(numFilteredEvents);
      return metastoreEvents;
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

    // the notification received from metastore which is processed by this
    protected final NotificationEvent event_;

    // Logger available for all the sub-classes
    protected final Logger LOG = LoggerFactory.getLogger(this.getClass());

    // dbName from the event
    protected final String dbName_;

    // tblName from the event
    protected final String tblName_;

    // eventId of the event. Used instead of calling getter on event_ everytime
    protected long eventId_;

    // eventType from the NotificationEvent
    protected final MetastoreEventType eventType_;

    // Actual notificationEvent object received from Metastore
    protected final NotificationEvent metastoreNotificationEvent_;

    // metrics registry so that events can add metrics
    protected final Metrics metrics_;


    MetastoreEvent(CatalogServiceCatalog catalogServiceCatalog, Metrics metrics,
        NotificationEvent event) {
      this.catalog_ = catalogServiceCatalog;
      this.event_ = event;
      this.eventId_ = event_.getEventId();
      this.eventType_ = MetastoreEventType.from(event.getEventType());
      // certain event types in Hive-3 like COMMIT_TXN may not have dbName set
      this.dbName_ = event.getDbName();
      this.tblName_ = event.getTableName();
      this.metastoreNotificationEvent_ = event;
      this.metrics_ = metrics;
    }

    public String getDbName() { return dbName_; }

    public String getTableName() { return tblName_; }

    /**
     * Process this event if it is enabled based on the flags on this object
     *
     * @throws CatalogException If  Catalog operations fail
     * @throws MetastoreNotificationException If NotificationEvent parsing fails
     */
    public void processIfEnabled()
        throws CatalogException, MetastoreNotificationException {
      if (isEventProcessingDisabled()) {
        LOG.info(debugString("Skipping this event because of flag evaluation"));
        metrics_.getCounter(MetastoreEventsProcessor.EVENTS_SKIPPED_METRIC).inc();
        return;
      }
      process();
    }

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
      formatArgs[0] = eventId_;
      formatArgs[1] = eventType_;
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
     * @param isInsertEvent if true, check in flight events list of Insert event
     * if false, check events list of DDL
     * @return True if this event is a self-generated event. If the returned value is
     * true, this method also clears the version number from the catalog database/table.
     * Returns false if the version numbers or service id don't match
     */
    protected boolean isSelfEvent(boolean isInsertEvent) {
      try {
        if (catalog_.evaluateSelfEvent(isInsertEvent, getSelfEventContext())) {
          metrics_.getCounter(MetastoreEventsProcessor.NUMBER_OF_SELF_EVENTS).inc();
          return true;
        }
      } catch (CatalogException e) {
        debugLog("Received exception {}. Ignoring self-event evaluation", e.getMessage());
      }
      return false;
    }

    protected boolean isSelfEvent() { return isSelfEvent(false); }
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

    private MetastoreTableEvent(CatalogServiceCatalog catalogServiceCatalog,
        Metrics metrics, NotificationEvent event) {
      super(catalogServiceCatalog, metrics, event);
      Preconditions.checkNotNull(dbName_, debugString("Database name cannot be null"));
      tblName_ = Preconditions.checkNotNull(event.getTableName());
      if (MetastoreEventType.OTHER.equals(eventType_)) {
        debugLog("Creating event {} of type {} ({}) on table {}", eventId_, eventType_,
            event.getEventType(), getFullyQualifiedTblName());
      } else {
        debugLog("Creating event {} of type {} on table {}", eventId_, eventType_,
            getFullyQualifiedTblName());
      }
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
    protected boolean reloadTableFromCatalog(String operation, boolean isTransactional)
        throws CatalogException {
      try {
        if (!catalog_.reloadTableIfExists(dbName_, tblName_,
            "Processing " + operation + " event from HMS")) {
          debugLog("Automatic refresh on table {} failed as the table "
              + "either does not exist anymore or is not in loaded state.",
              getFullyQualifiedTblName());
          return false;
        }
      } catch (DatabaseNotFoundException e) {
        debugLog("Refresh table {} failed as "
                + "the database was not present in the catalog.",
            getFullyQualifiedTblName());
        return false;
      }
      String tblStr = isTransactional ? "transactional table" : "table";
      infoLog("Refreshed {} {}", tblStr, getFullyQualifiedTblName());
      metrics_.getCounter(MetastoreEventsProcessor.NUMBER_OF_TABLE_REFRESHES).inc();
      return true;
    }

    /**
     * Refreshes a partition provided by given spec only if the table is loaded
     * @param tPartSpec
     * @param reason Event type which caused the refresh, used for logging by catalog
     * @return false if the table or database did not exist or was not loaded, else
     * returns true.
     * @throws CatalogException
     */
    protected boolean reloadPartition(List<TPartitionKeyValue> tPartSpec, String reason)
        throws CatalogException {
      try {
        boolean result = catalog_.reloadPartitionIfExists(dbName_,
            tblName_, tPartSpec, reason);
        if (!result) {
          debugLog("partition {} on table {} was not refreshed since it does not exist "
                  + "in catalog anymore", HdfsTable.constructPartitionName(tPartSpec),
              getFullyQualifiedTblName());
        } else {
          metrics_.getCounter(MetastoreEventsProcessor.NUMBER_OF_PARTITION_REFRESHES)
              .inc();
          infoLog("Table {} partition {} has been refreshed", getFullyQualifiedTblName(),
              HdfsTable.constructPartitionName(tPartSpec));
        }
        return true;
      } catch (TableNotLoadedException e) {
          debugLog("Partition {} on table {} was not refreshed since it is not loaded",
              HdfsTable.constructPartitionName(tPartSpec), getFullyQualifiedTblName());
      } catch (DatabaseNotFoundException | TableNotFoundException e) {
        debugLog("Refresh of table {} partition {} "
                + "event failed as the database or table is not present in the catalog.",
            getFullyQualifiedTblName(), HdfsTable.constructPartitionName(tPartSpec));
      }
      return false;
    }
  }

  /**
   * Base class for all the database events
   */
  public static abstract class MetastoreDatabaseEvent extends MetastoreEvent {
    MetastoreDatabaseEvent(CatalogServiceCatalog catalogServiceCatalog, Metrics metrics,
        NotificationEvent event) {
      super(catalogServiceCatalog, metrics, event);
      Preconditions.checkNotNull(dbName_, debugString("Database name cannot be null"));
      debugLog("Creating event {} of type {} on database {}", eventId_,
              eventType_, dbName_);
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
  }

  /**
   * MetastoreEvent for CREATE_TABLE event type
   */
  public static class CreateTableEvent extends MetastoreTableEvent {
    /**
     * Prevent instantiation from outside should use MetastoreEventFactory instead
     */
    private CreateTableEvent(CatalogServiceCatalog catalog, Metrics metrics,
        NotificationEvent event) throws MetastoreNotificationException {
      super(catalog, metrics, event);
      Preconditions.checkArgument(MetastoreEventType.CREATE_TABLE.equals(eventType_));
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
    public void process() throws MetastoreNotificationException {
      // check if the table exists already. This could happen in corner cases of the
      // table being dropped and recreated with the same name or in case this event is
      // a self-event (see description of self-event in the class documentation of
      // MetastoreEventsProcessor)
      try {
        if (!catalog_.addTableIfNotExists(dbName_, tblName_)) {
          debugLog(
              "Not adding the table {} since it already exists in catalog", tblName_);
          return;
        }
      } catch (CatalogException e) {
        // if a exception is thrown, it could be due to the fact that the db did not
        // exist in the catalog cache. This could only happen if the previous
        // create_database event for this table errored out
        throw new MetastoreNotificationException(debugString(
            "Unable to add table while processing for table %s because the "
                + "database doesn't exist. This could be due to a previous error while "
                + "processing CREATE_DATABASE event for the database %s",
            getFullyQualifiedTblName(), dbName_), e);
      }
      debugLog("Added a table {}", getFullyQualifiedTblName());
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
                dropTableEvent.eventId_, dropTableEvent.eventType_);
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
                alterTableEvent.eventId_, alterTableEvent.eventType_);
            return true;
          }
        }
      }
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
    InsertEvent(CatalogServiceCatalog catalog, Metrics metrics,
        NotificationEvent event) throws MetastoreNotificationException {
      super(catalog, metrics, event);
      Preconditions.checkArgument(MetastoreEventType.INSERT.equals(eventType_));
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
    public SelfEventContext getSelfEventContext() {
      if (insertPartition_ != null) {
        // create selfEventContext for insert partition event
        List<TPartitionKeyValue> tPartSpec =
            getTPartitionSpecFromHmsPartition(msTbl_, insertPartition_);
        return new SelfEventContext(dbName_, tblName_, Arrays.asList(tPartSpec),
            insertPartition_.getParameters(), eventId_);
      } else {
        // create selfEventContext for insert table event
        return new SelfEventContext(
            dbName_, tblName_, null, msTbl_.getParameters(), eventId_);
      }
    }

    @Override
    public void process() throws MetastoreNotificationException {
      if (isSelfEvent(true)) {
        infoLog("Not processing the event as it is a self-event");
        return;
      }
      // Reload the whole table if it's a transactional table.
      if (AcidUtils.isTransactionalTable(msTbl_.getParameters())) {
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
      List<TPartitionKeyValue> tPartSpec = getTPartitionSpecFromHmsPartition(msTbl_,
          insertPartition_);
      try {
        // Ignore event if table or database is not in catalog. Throw exception if
        // refresh fails. If the partition does not exist in metastore the reload
        // method below removes it from the catalog
        reloadPartition(tPartSpec, "INSERT");
      } catch (CatalogException e) {
        throw new MetastoreNotificationNeedsInvalidateException(debugString("Refresh "
                + "partition on table {} partition {} failed. Event processing cannot "
                + "continue. Issue an invalidate metadata command to reset the event "
                + "processor state.", getFullyQualifiedTblName(),
            HdfsTable.constructPartitionName(tPartSpec)), e);
      }
    }

    /**
     *  Process unpartitioned table inserts
     */
    private void processTableInserts() throws MetastoreNotificationException {
      // For non-partitioned tables, refresh the whole table.
      Preconditions.checkArgument(insertPartition_ == null);
      try {
        // Ignore event if table or database is not in the catalog. Throw exception if
        // refresh fails.
        reloadTableFromCatalog("INSERT", false);
      } catch (CatalogException e) {
        if (e instanceof TableLoadingException &&
            e.getCause() instanceof NoSuchObjectException) {
          LOG.warn(
              "Ignoring the refresh of the table since the table does"
                  + " not exist in metastore anymore");
        } else {
          throw new MetastoreNotificationNeedsInvalidateException(
              debugString("Refresh table {} failed. Event processing "
                  + "cannot continue. Issue an invalidate metadata command to reset "
                  + "the event processor state.", getFullyQualifiedTblName()), e);
        }
      }
    }
  }

  /**
   * MetastoreEvent for ALTER_TABLE event type
   */
  public static class AlterTableEvent extends MetastoreTableEvent {
    protected org.apache.hadoop.hive.metastore.api.Table tableBefore_;
    // the table object after alter operation, as parsed from the NotificationEvent
    protected org.apache.hadoop.hive.metastore.api.Table tableAfter_;
    // true if this alter event was due to a rename operation
    private final boolean isRename_;
    // value of event sync flag for this table before the alter operation
    private final Boolean eventSyncBeforeFlag_;
    // value of the event sync flag if available at this table after the alter operation
    private final Boolean eventSyncAfterFlag_;
    // value of the db flag at the time of event creation
    private final boolean dbFlagVal;

    /**
     * Prevent instantiation from outside should use MetastoreEventFactory instead
     */
    @VisibleForTesting
    AlterTableEvent(CatalogServiceCatalog catalog, Metrics metrics,
        NotificationEvent event) throws MetastoreNotificationException {
      super(catalog, metrics, event);
      Preconditions.checkArgument(MetastoreEventType.ALTER_TABLE.equals(eventType_));
      JSONAlterTableMessage alterTableMessage =
          (JSONAlterTableMessage) MetastoreEventsProcessor.getMessageDeserializer()
              .getAlterTableMessage(event.getMessage());
      try {
        msTbl_ = Preconditions.checkNotNull(alterTableMessage.getTableObjBefore());
        tableAfter_ = Preconditions.checkNotNull(alterTableMessage.getTableObjAfter());
        tableBefore_ = Preconditions.checkNotNull(alterTableMessage.getTableObjBefore());
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

    @Override
    protected SelfEventContext getSelfEventContext() {
      return new SelfEventContext(tableAfter_.getDbName(), tableAfter_.getTableName(),
          tableAfter_.getParameters());
    }

    /**
     * If the ALTER_TABLE event is due a table rename, this method removes the old table
     * and creates a new table with the new name. Else, this just issues a refresh
     * table on the tblName from the event
     */
    @Override
    public void process() throws MetastoreNotificationException, CatalogException {
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
      // in case of table level alters from external systems it is better to do a full
      // refresh  eg. this could be due to as simple as adding a new parameter or a
      // full blown adding or changing column type
      // detect the special case where a table is renamed
      if (!isRename_) {
        // table is not renamed, need to refresh the table if its loaded
        if (!reloadTableFromCatalog("ALTER_TABLE", false)) {
          if (wasEventSyncTurnedOn()) {
            // we received this alter table event on a non-existing table. We also
            // detect that event sync was turned on in this event. This may mean that
            // the table creation was skipped earlier because event sync was turned off
            // we don't really know how many of events we have skipped till now because
            // the sync was disabled all this while before we receive such a event. We
            // error on the side of caution by stopping the event processing and
            // letting the user to issue a invalidate metadata to reset the state
            throw new MetastoreNotificationNeedsInvalidateException(debugString(
                "Detected that event sync was turned on for the table %s "
                    + "and the table does not exist. Event processing cannot be "
                    + "continued further. Issue a invalidate metadata command to reset "
                    + "the event processing state", getFullyQualifiedTblName()));
          }
        }
        return;
      }
      // table was renamed, remove the old table
      infoLog("Found that {} table was renamed. Renaming it by "
              + "remove and adding a new table",
          new TableName(msTbl_.getDbName(), msTbl_.getTableName()));
      TTableName oldTTableName =
          new TTableName(msTbl_.getDbName(), msTbl_.getTableName());
      TTableName newTTableName =
          new TTableName(tableAfter_.getDbName(), tableAfter_.getTableName());

      // Table is renamed if old db and table exist in catalog. If the rename is to a
      // different database, we check if this other database exists in catalog. If
      // either the old table, old database or new database are not in catalog, we skip
      // this event.
      if (!catalog_.renameTableIfExists(oldTTableName, newTTableName)) {
        debugLog("Did not remove old table to rename table {} to {} since "
            + "it does not exist anymore or either the old database or the new "
            + "database don't exist anymore.", qualify(oldTTableName),
            qualify(newTTableName));
      } else {
        infoLog("Renamed old table {} to new table {}.", qualify(oldTTableName),
            qualify(newTTableName));
      }
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

    private boolean canBeSkipped() {
      // Certain alter events just modify some parameters such as
      // "transient_lastDdlTime" in Hive. For eg: the alter table event generated
      // along with insert events. Check if the alter table event is such a trivial
      // event by setting those parameters equal before and after the event and
      // comparing the objects.

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

    /**
     * Prevent instantiation from outside should use MetastoreEventFactory instead
     */
    private DropTableEvent(CatalogServiceCatalog catalog, Metrics metrics,
        NotificationEvent event) throws MetastoreNotificationException {
      super(catalog, metrics, event);
      Preconditions.checkArgument(MetastoreEventType.DROP_TABLE.equals(eventType_));
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
     * TODO : Once HIVE-21595 is available we should rely on table_id for determining a
     * newer incarnation of a previous table.
     */
    @Override
    public void process() {
      Reference<Boolean> tblWasFound = new Reference<>();
      Reference<Boolean> tblMatched = new Reference<>();
      Table removedTable = catalog_.removeTableIfExists(msTbl_, tblWasFound, tblMatched);
      if (removedTable != null) {
        infoLog("Removed table {} ", getFullyQualifiedTblName());
      } else if (!tblWasFound.getRef()) {
        debugLog("Table {} was not removed since it does not exist in catalog anymore.",
            tblName_);
      } else if (!tblMatched.getRef()) {
        infoLog(debugString("Table %s was not removed from "
            + "catalog since the creation time of the table did not match", tblName_));
        metrics_.getCounter(MetastoreEventsProcessor.EVENTS_SKIPPED_METRIC).inc();
      }
    }
  }

  /**
   * MetastoreEvent for CREATE_DATABASE event type
   */
  public static class CreateDatabaseEvent extends MetastoreDatabaseEvent {

    // metastore database object as parsed from NotificationEvent message
    private final Database createdDatabase_;

    /**
     * Prevent instantiation from outside should use MetastoreEventFactory instead
     */
    private CreateDatabaseEvent(CatalogServiceCatalog catalog, Metrics metrics,
        NotificationEvent event) throws MetastoreNotificationException {
      super(catalog, metrics, event);
      Preconditions.checkArgument(MetastoreEventType.CREATE_DATABASE.equals(eventType_));
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
      // if the database already exists in catalog, by definition, it is a later version
      // of the database since metastore will not allow it be created if it was already
      // existing at the time of creation. In such case, it is safe to assume that the
      // already existing database in catalog is a later version with the same name and
      // this event can be ignored
      if (catalog_.addDbIfNotExists(dbName_, createdDatabase_)) {
        infoLog("Successfully added database {}", dbName_);
      } else {
        infoLog("Database {} already exists", dbName_);
      }
    }

    @Override
    public boolean isRemovedAfter(List<MetastoreEvent> events) {
      Preconditions.checkNotNull(events);
      for (MetastoreEvent event : events) {
        if (event.eventType_.equals(MetastoreEventType.DROP_DATABASE)) {
          DropDatabaseEvent dropDatabaseEvent = (DropDatabaseEvent) event;
          if (dbName_.equalsIgnoreCase(dropDatabaseEvent.dbName_)) {
            infoLog("Found database {} is removed later in event {} of type {} ", dbName_,
                dropDatabaseEvent.eventId_, dropDatabaseEvent.eventType_);
            return true;
          }
        }
      }
      return false;
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
    private AlterDatabaseEvent(CatalogServiceCatalog catalog, Metrics metrics,
        NotificationEvent event) throws MetastoreNotificationException {
      super(catalog, metrics, event);
      Preconditions.checkArgument(MetastoreEventType.ALTER_DATABASE.equals(eventType_));
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
    public void process() throws CatalogException, MetastoreNotificationException {
      if (isSelfEvent()) {
        infoLog("Not processing the event as it is a self-event");
        return;
      }
      Preconditions.checkNotNull(alteredDatabase_);
      // If not self event, copy Db object from event to catalog
      if (!catalog_.updateDbIfExists(alteredDatabase_)) {
        // Okay to skip this event. Events processor will not error out.
        debugLog("Update database {} failed as the database is not present in the "
            + "catalog.", alteredDatabase_.getName());
      } else {
        infoLog("Database {} updated after alter database event.",
            alteredDatabase_.getName());
      }
    }

    @Override
    protected SelfEventContext getSelfEventContext() {
      return new SelfEventContext(dbName_, null, alteredDatabase_.getParameters());
    }
  }

  /**
   * MetastoreEvent for the DROP_DATABASE event
   */
  public static class DropDatabaseEvent extends MetastoreDatabaseEvent {

    // Metastore database object as parsed from NotificationEvent message
    private final Database droppedDatabase_;

    /**
     * Prevent instantiation from outside should use MetastoreEventFactory instead
     */
    private DropDatabaseEvent(
        CatalogServiceCatalog catalog, Metrics metrics, NotificationEvent event)
        throws MetastoreNotificationException {
      super(catalog, metrics, event);
      Preconditions.checkArgument(MetastoreEventType.DROP_DATABASE.equals(eventType_));
      JSONDropDatabaseMessage dropDatabaseMessage =
          (JSONDropDatabaseMessage) MetastoreEventsProcessor.getMessageDeserializer()
              .getDropDatabaseMessage(event.getMessage());
      try {
        droppedDatabase_ =
            Preconditions.checkNotNull(dropDatabaseMessage.getDatabaseObject());
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
     * TODO : Once HIVE-21595 is available we should rely on database_id for determining a
     * newer incarnation of a previous database.
     */
    @Override
    public void process() {
      Reference<Boolean> dbFound = new Reference<>();
      Reference<Boolean> dbMatched = new Reference<>();
      Db removedDb = catalog_.removeDbIfExists(droppedDatabase_, dbFound, dbMatched);
      if (removedDb != null) {
        infoLog("Removed Database {} ", dbName_);
      } else if (!dbFound.getRef()) {
        debugLog("Database {} was not removed since it " +
            "did not exist in catalog.", dbName_);
      } else if (!dbMatched.getRef()) {
        infoLog(debugString("Database %s was not removed from catalog since "
            + "the creation time of the Database did not match", dbName_));
        metrics_.getCounter(MetastoreEventsProcessor.EVENTS_SKIPPED_METRIC).inc();
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
    private final List<Partition> addedPartitions_;
    private final List<List<TPartitionKeyValue>> partitionKeyVals_;

    /**
     * Prevent instantiation from outside should use MetastoreEventFactory instead
     */
    private AddPartitionEvent(CatalogServiceCatalog catalog, Metrics metrics,
        NotificationEvent event) throws MetastoreNotificationException {
      super(catalog, metrics, event);
      Preconditions.checkState(eventType_.equals(MetastoreEventType.ADD_PARTITION));
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

    @Override
    public void process() throws MetastoreNotificationException, CatalogException {
      // bail out early if there are not partitions to process
      if (addedPartitions_.isEmpty()) {
        infoLog("Partition list is empty. Ignoring this event.");
        return;
      }
      if (isSelfEvent()) {
        infoLog("Not processing the event as it is a self-event");
        return;
      }
      try {
        // Reload the whole table if it's a transactional table.
        if (AcidUtils.isTransactionalTable(msTbl_.getParameters())) {
          reloadTableFromCatalog("ADD_PARTITION", true);
        } else {
          // HMS adds partitions in a transactional way. This means there may be multiple
          // HMS partition objects in an add_partition event. We try to do the same here
          // by refreshing all those partitions in a loop. If any partition refresh fails,
          // we throw MetastoreNotificationNeedsInvalidateException exception. We skip
          // refresh of the partitions if the table is not present in the catalog.
          infoLog("Trying to refresh {} partitions added to table {} in the event",
              addedPartitions_.size(), getFullyQualifiedTblName());
          //TODO refresh all the partition together instead of looping one by one
          for (Partition partition : addedPartitions_) {
            List<TPartitionKeyValue> tPartSpec =
                getTPartitionSpecFromHmsPartition(msTbl_, partition);
            if (!reloadPartition(tPartSpec, "ADD_PARTITION")) break;
          }
        }
      } catch (CatalogException e) {
        throw new MetastoreNotificationNeedsInvalidateException(debugString("Failed to "
                + "refresh newly added partitions of table {}. Event processing cannot "
                + "continue. Issue an invalidate metadata command to reset event "
                + "processor.", getFullyQualifiedTblName()), e);
      }
    }
  }

  public static class AlterPartitionEvent extends MetastoreTableEvent {
    // the Partition object before alter operation, as parsed from the NotificationEvent
    private final org.apache.hadoop.hive.metastore.api.Partition partitionBefore_;
    // the Partition object after alter operation, as parsed from the NotificationEvent
    private final org.apache.hadoop.hive.metastore.api.Partition partitionAfter_;

    /**
     * Prevent instantiation from outside should use MetastoreEventFactory instead
     */
    private AlterPartitionEvent(CatalogServiceCatalog catalog, Metrics metrics,
        NotificationEvent event) throws MetastoreNotificationException {
      super(catalog, metrics, event);
      Preconditions.checkState(eventType_.equals(MetastoreEventType.ALTER_PARTITION));
      Preconditions.checkNotNull(event.getMessage());
      AlterPartitionMessage alterPartitionMessage =
          MetastoreEventsProcessor.getMessageDeserializer()
              .getAlterPartitionMessage(event.getMessage());

      try {
        partitionBefore_ =
            Preconditions.checkNotNull(alterPartitionMessage.getPtnObjBefore());
        partitionAfter_ =
            Preconditions.checkNotNull(alterPartitionMessage.getPtnObjAfter());
        msTbl_ = alterPartitionMessage.getTableObj();
      } catch (Exception e) {
        throw new MetastoreNotificationException(
            debugString("Unable to parse the alter partition message"), e);
      }
    }

    @Override
    public void process() throws MetastoreNotificationException, CatalogException {
      if (isSelfEvent()) {
        infoLog("Not processing the event as it is a self-event");
        return;
      }

      // Ignore the event if this is a trivial event. See javadoc for
      // isTrivialAlterPartitionEvent() for examples.
      if (canBeSkipped()) {
        infoLog("Not processing this event as it only modifies some partition "
            + "parameters which can be ignored.");
        return;
      }

      // Reload the whole table if it's a transactional table.
      if (AcidUtils.isTransactionalTable(msTbl_.getParameters())) {
        reloadTableFromCatalog("ALTER_PARTITION", true);
      } else {
        // Refresh the partition that was altered.
        Preconditions.checkNotNull(partitionAfter_);
        List<TPartitionKeyValue> tPartSpec = getTPartitionSpecFromHmsPartition(msTbl_,
            partitionAfter_);
        try {
          reloadPartition(tPartSpec, "ALTER_PARTITION");
        } catch (CatalogException e) {
          throw new MetastoreNotificationNeedsInvalidateException(debugString("Refresh "
                  + "partition on table {} partition {} failed. Event processing cannot "
                  + "continue. Issue an invalidate command to reset the event processor "
                  + "state.", getFullyQualifiedTblName(),
              HdfsTable.constructPartitionName(tPartSpec)), e);
        }
      }
    }

    private boolean canBeSkipped() {
      // Certain alter events just modify some parameters such as
      // "transient_lastDdlTime" in Hive. For eg: the alter table event generated
      // along with insert events. Check if the alter table event is such a trivial
      // event by setting those parameters equal before and after the event and
      // comparing the objects.

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
  }

  public static class DropPartitionEvent extends MetastoreTableEvent {
    private final List<Map<String, String>> droppedPartitions_;

    /**
     * Prevent instantiation from outside should use MetastoreEventFactory instead
     */
    private DropPartitionEvent(CatalogServiceCatalog catalog, Metrics metrics,
        NotificationEvent event) throws MetastoreNotificationException {
      super(catalog, metrics, event);
      Preconditions.checkState(eventType_.equals(MetastoreEventType.DROP_PARTITION));
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

    @Override
    public void process() throws MetastoreNotificationException, CatalogException {
      // we have seen cases where a add_partition event is generated with empty
      // partition list (see IMPALA-8547 for details. Make sure that droppedPartitions
      // list is not empty
      if (droppedPartitions_.isEmpty()) {
        infoLog("Partition list is empty. Ignoring this event.");
      }
      try {
        // Reload the whole table if it's a transactional table.
        if (AcidUtils.isTransactionalTable(msTbl_.getParameters())) {
          reloadTableFromCatalog("DROP_PARTITION", true);
        } else {
          // We refresh all the partitions that were dropped from HMS. If a refresh
          // fails, we throw a MetastoreNotificationNeedsInvalidateException
          infoLog("{} partitions dropped from table {}. Refreshing the partitions "
                  + "to remove them from catalog.", droppedPartitions_.size(),
              getFullyQualifiedTblName());
          for (Map<String, String> partSpec : droppedPartitions_) {
            List<TPartitionKeyValue> tPartSpec = new ArrayList<>(partSpec.size());
            for (Map.Entry<String, String> entry : partSpec.entrySet()) {
              tPartSpec.add(new TPartitionKeyValue(entry.getKey(), entry.getValue()));
            }
            if (!reloadPartition(tPartSpec, "DROP_PARTITION")) break;
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
   * An event type which is ignored. Useful for unsupported metastore event types
   */
  public static class IgnoredEvent extends MetastoreEvent {

    /**
     * Prevent instantiation from outside should use MetastoreEventFactory instead
     */
    private IgnoredEvent(
        CatalogServiceCatalog catalog, Metrics metrics, NotificationEvent event) {
      super(catalog, metrics, event);
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
    protected SelfEventContext getSelfEventContext() {
      throw new UnsupportedOperationException("Self-event evaluation is not needed for "
          + "this event type");
    }
  }
}
