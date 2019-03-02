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
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.NotificationEvent;
import org.apache.hadoop.hive.metastore.messaging.AddPartitionMessage;
import org.apache.hadoop.hive.metastore.messaging.AlterPartitionMessage;
import org.apache.hadoop.hive.metastore.messaging.CreateTableMessage;
import org.apache.hadoop.hive.metastore.messaging.json.JSONAlterTableMessage;
import org.apache.hadoop.hive.metastore.messaging.json.JSONCreateDatabaseMessage;
import org.apache.hadoop.hive.metastore.messaging.json.JSONDropTableMessage;
import org.apache.impala.analysis.TableName;
import org.apache.impala.catalog.CatalogException;
import org.apache.impala.catalog.CatalogServiceCatalog;
import org.apache.impala.catalog.Db;
import org.apache.impala.catalog.Table;
import org.apache.impala.common.Metrics;
import org.apache.impala.common.Pair;
import org.apache.impala.common.Reference;
import org.apache.impala.thrift.TTableName;
import org.slf4j.LoggerFactory;
import org.slf4j.Logger;

/**
 * Main class which provides Metastore event objects for various event types. Also
 * provides a MetastoreEventFactory to get or create the event instances for a given event
 * type
 */
public class MetastoreEvents {

  // flag to be set in the table/database parameters to disable event based metadata sync
  public static final String DISABLE_EVENT_HMS_SYNC_KEY = "impala.disableHmsSync";

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
     * creates instance of <code>MetastoreEvent</code> used to process a given
     * event type. If the event type is unknown, returns a IgnoredEvent
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
        case ALTER_DATABASE:
          // alter database events are currently ignored
          return new IgnoredEvent(catalog_, metrics_, event);
        case ADD_PARTITION:
          // add partition events triggers invalidate table currently
          return new AddPartitionEvent(catalog_, metrics_, event);
        case DROP_PARTITION:
          // drop partition events triggers invalidate table currently
          return new DropPartitionEvent(catalog_, metrics_, event);
        case ALTER_PARTITION:
          // alter partition events triggers invalidate table currently
          return new AlterPartitionEvent(catalog_, metrics_, event);
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
     * removed a table, the create event received will try to add the object again.
     * This table will be visible until the drop table event is processed. This can be
     * avoided by "looking ahead" in the event stream to see if the table with the same
     * name was dropped. In such a case, the create event can be ignored
     *
     * @param events NotificationEvents fetched from metastore
     * @return A list of MetastoreEvents corresponding to the given the NotificationEvents
     * @throws MetastoreNotificationException if a NotificationEvent could not be
     * parsed into MetastoreEvent
     */
    List<MetastoreEvent> getFilteredEvents(List<NotificationEvent> events)
        throws MetastoreNotificationException {
      Preconditions.checkNotNull(events);
      if (events.isEmpty()) return Collections.emptyList();

      List<MetastoreEvent> metastoreEvents = new ArrayList<>(events.size());
      for (NotificationEvent event : events) {
        metastoreEvents.add(get(event));
      }
      Iterator<MetastoreEvent> it = metastoreEvents.iterator();
      // filter out the create events which has a corresponding drop event later
      int sizeBefore = metastoreEvents.size();
      int numFilteredEvents = 0;
      int i = 0;
      while (i < metastoreEvents.size()) {
        MetastoreEvent currentEvent = metastoreEvents.get(i);
        if (currentEvent.isRemovedAfter(metastoreEvents.subList(i + 1,
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

    // eventId of the event. Used instead of calling getter on event_ everytime
    protected final long eventId_;

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
      this.dbName_ = Preconditions.checkNotNull(event.getDbName());
      this.metastoreNotificationEvent_ = event;
      this.metrics_ = metrics;
    }

    /**
     * Process this event if it is enabled based on the flags on this object
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
    protected abstract void process() throws MetastoreNotificationException,
        CatalogException;

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
          new StringBuilder(STR_FORMAT_EVENT_ID_TYPE).append(msgFormatString)
              .toString();
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
     * Logs at info level the given log formatted string and its args. The log
     * formatted string should have {} pair at the appropriate location in the string
     * for each arg value provided. This method prepends the event id and event type
     * before logging the message. No-op if the log level is not at INFO

     * @param logFormattedStr
     * @param args
     */
    protected void infoLog(String logFormattedStr, Object... args) {
      if (!LOG.isInfoEnabled()) return;
      String formatString =
          new StringBuilder(LOG_FORMAT_EVENT_ID_TYPE).append(logFormattedStr)
              .toString();
      Object[] formatArgs = getLogFormatArgs(args);
      LOG.info(formatString, formatArgs);
    }

    /**
     * Similar to infoLog excepts logs at debug level
     */
    protected void debugLog(String logFormattedStr, Object... args) {
      if (!LOG.isDebugEnabled()) return;
      String formatString =
          new StringBuilder(LOG_FORMAT_EVENT_ID_TYPE).append(logFormattedStr)
              .toString();
      Object[] formatArgs = getLogFormatArgs(args);
      LOG.debug(formatString, formatArgs);
    }

    /**
     * Search for a inverse event (for example drop_table is a inverse event for
     * create_table) for this event from a given list of notificationEvents starting
     * for the startIndex. This is useful for skipping certain events from processing
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
      tblName_ = Preconditions.checkNotNull(event.getTableName());
      debugLog("Creating event {} of type {} on table {}", eventId_, eventType_,
          getFullyQualifiedTblName());
    }


    /**
     * Util method to return the fully qualified table name which is of the format
     * dbName.tblName for this event
     */
    protected String getFullyQualifiedTblName() {
      return new TableName(dbName_, tblName_).toString();
    }

    /**
     * Util method to issue invalidate on a given table on the catalog. This method
     * atomically invalidates the table if it exists in the catalog. No-op if the table
     * does not exist
     */
    protected boolean invalidateCatalogTable() {
      return catalog_.invalidateTableIfExists(dbName_, tblName_) != null;
    }

    /**
     * Checks if the table level property is set in the parameters of the table from
     * the event. If it is available, it takes precedence over database level flag for
     * this table. If the table level property is not set, returns the value from the
     * database level property.f
     *
     * @return Boolean value of the table property with the key
     * <code>DISABLE_EVENT_HMS_SYNC_KEY</code>. Else, returns the database property
     * which is associated with this table. Returns false if neither of the properties
     * are set.
     */
    @Override
    protected boolean isEventProcessingDisabled() {
      Preconditions.checkNotNull(msTbl_);
      Boolean tblProperty = getHmsSyncProperty(msTbl_);
      if (tblProperty != null) {
        infoLog("Found table level flag {} is set to {}",
            DISABLE_EVENT_HMS_SYNC_KEY, tblProperty.toString());
        return tblProperty;
      }
      // if the tbl property is not set check at db level
      String dbFlagVal = catalog_.getDbProperty(dbName_, DISABLE_EVENT_HMS_SYNC_KEY);
      if (dbFlagVal != null) {
        // no need to spew unnecessary logs. Most tables/databases are expected to not
        // have this flag set when event based HMS polling is enabled
        infoLog("Table level flag is not set. Db level flag {} at Db level is {}",
                DISABLE_EVENT_HMS_SYNC_KEY, dbFlagVal);
      }
      // flag value of null also returns false
      return Boolean.valueOf(dbFlagVal);
    }

    /**
     * Gets the value of the parameter with the key
     * <code>DISABLE_EVENT_HMS_SYNC_KEY</code> from the given table
     *
     * @return the Boolean value of the property with the key
     * <code>DISABLE_EVENT_HMS_SYNC_KEY</code> if it is available else returns null
     */
    public static Boolean getHmsSyncProperty(
        org.apache.hadoop.hive.metastore.api.Table tbl) {
      if (!tbl.isSetParameters()) return null;
      String val = tbl.getParameters().get(DISABLE_EVENT_HMS_SYNC_KEY);
      if (val == null || val.isEmpty()) return null;
      return Boolean.valueOf(val);
    }
  }

  /**
   * Base class for all the database events
   */
  public static abstract class MetastoreDatabaseEvent extends MetastoreEvent {
    MetastoreDatabaseEvent(CatalogServiceCatalog catalogServiceCatalog, Metrics metrics,
        NotificationEvent event) {
      super(catalogServiceCatalog, metrics, event);
      debugLog("Creating event {} of type {} on database {}", eventId_,
              eventType_, dbName_);
    }

    /**
     * Even though there is a database level property
     * <code>DISABLE_EVENT_HMS_SYNC_KEY</code> it is only used for tables within that
     * database. As such this property does not control if the database level DDLs are
     * skipped or not.
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
          MetastoreEventsProcessor.getMessageFactory().getDeserializer()
              .getCreateTableMessage(event.getMessage());
      try {
        msTbl_ = createTableMessage.getTableObj();
      } catch (Exception e) {
        throw new MetastoreNotificationException(
            debugString("Unable to deserialize the event message"), e);
      }
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
          debugLog("Not adding the table {} since it already exists in catalog",
                  tblName_);
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
            infoLog("Found table {} is removed later in event {} type {}",
                tblName_, dropTableEvent.eventId_, dropTableEvent.eventType_);
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
          if (alterTableEvent.isRename_ &&
              dbName_.equalsIgnoreCase(alterTableEvent.msTbl_.getDbName()) &&
              tblName_.equalsIgnoreCase(alterTableEvent.msTbl_.getTableName())) {
            infoLog("Found table {} is renamed later in event {} type {}",
                tblName_, alterTableEvent.eventId_, alterTableEvent.eventType_);
            return true;
          }
        }
      }
      return false;
    }
  }

  /**
   * MetastoreEvent for ALTER_TABLE event type
   */
  public static class AlterTableEvent extends MetastoreTableEvent {

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
          (JSONAlterTableMessage) MetastoreEventsProcessor.getMessageFactory()
              .getDeserializer().getAlterTableMessage(event.getMessage());
      try {
        msTbl_ = Preconditions.checkNotNull(alterTableMessage.getTableObjBefore());
        tableAfter_ = Preconditions.checkNotNull(alterTableMessage.getTableObjAfter());
      } catch (Exception e) {
        throw new MetastoreNotificationException(
            debugString("Unable to parse the alter table message"), e);
      }
      // this is a rename event if either dbName or tblName of before and after object
      // changed
      isRename_ = !msTbl_.getDbName().equalsIgnoreCase(tableAfter_.getDbName())
          || !msTbl_.getTableName().equalsIgnoreCase(tableAfter_.getTableName());
      eventSyncBeforeFlag_ =
          getHmsSyncProperty(msTbl_);
      eventSyncAfterFlag_ = getHmsSyncProperty(tableAfter_);
      dbFlagVal =
          Boolean.valueOf(catalog_.getDbProperty(dbName_, DISABLE_EVENT_HMS_SYNC_KEY));
    }

    /**
     * If the ALTER_TABLE event is due a table rename, this method removes the old table
     * and creates a new table with the new name. Else, this just issues a invalidate
     * table on the tblName from the event//TODO Check if we can rename the existing table
     * in-place
     */
    @Override
    public void process() throws MetastoreNotificationException, CatalogException {
      // in case of table level alters from external systems it is better to do a full
      // invalidate  eg. this could be due to as simple as adding a new parameter or a
      // full blown adding  or changing column type
      // detect the special where a table is renamed
      if (!isRename_) {
        // table is not renamed, need to invalidate
        if (!invalidateCatalogTable()) {
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
          debugLog("Table {} does not need to be "
                  + "invalidated since it does not exist anymore",
              getFullyQualifiedTblName());
        } else {
          infoLog("Table {} is invalidated", getFullyQualifiedTblName());
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

      // atomically rename the old table to new table
      Pair<Boolean, Boolean> result = null;
      result = catalog_.renameOrAddTableIfNotExists(oldTTableName, newTTableName);

      // old table was not found. This could be because catalogD is stale and didn't
      // have any entry for the oldTable
      if (!result.first) {
        debugLog("Did not remove old table to rename table {} to {} since "
                + "it does not exist anymore", qualify(oldTTableName),
            qualify(newTTableName));
      }
      // the new table from the event was not added since it was already present
      if (!result.second) {
        debugLog("Did not add new table name while renaming table {} to {}",
            qualify(oldTTableName), qualify(newTTableName));
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

    private String qualify(TTableName tTableName) {
      return new TableName(tTableName.db_name, tTableName.table_name).toString();
    }

    /**
     * In case of alter table events, it is possible that the alter event is generated
     * because user changed the value of the parameter
     * <code>DISABLE_EVENT_HMS_SYNC_KEY</code>. If the parameter is unchanged, it doesn't
     * matter if you use the before or after table object here since the eventual
     * action is going be invalidate or rename. If however, the parameter is changed,
     * couple of things could happen. The flag changes from unset/false to true or it
     * changes from true to false/unset. In the first case, we want to process the
     * event (and ignore subsequent events on this table). In the second case, we
     * should process the event (as well as all the subsequent events on the table). So
     * we always process this event when the value of the flag is changed.
     *
     * @return true, if this event needs to be skipped. false if this event needs to be
     * processed.
     */
    @Override
    protected boolean isEventProcessingDisabled() {
      // if the event sync flag was changed then we always process this event
      if (!Objects.equals(eventSyncBeforeFlag_, eventSyncAfterFlag_)) {
        infoLog("Before flag value {} after flag value {} changed",
            eventSyncBeforeFlag_, eventSyncAfterFlag_);
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
          (JSONDropTableMessage) MetastoreEventsProcessor.getMessageFactory()
              .getDeserializer().getDropTableMessage(event.getMessage());
      try {
        msTbl_ = Preconditions.checkNotNull(dropTableMessage.getTableObj());
      } catch (Exception e) {
        throw new MetastoreNotificationException(debugString(
            "Could not parse event message. "
                + "Check if %s is set to true in metastore configuration",
            MetastoreEventsProcessor.HMS_ADD_THRIFT_OBJECTS_IN_EVENTS_CONFIG_KEY), e);
      }
    }

    /**
     * Process the drop table event type. If the table from the event doesn't exist in the
     * catalog, ignore the event. If the table exists in the catalog, compares the
     * createTime of the table in catalog with the createTime of the table from the event
     * and remove the catalog table if there is a match. If the catalog table is a
     * incomplete table it is removed as well.
     */
    @Override
    public void process() {
      Reference<Boolean> tblWasFound = new Reference<>();
      Reference<Boolean> tblMatched = new Reference<>();
      Table removedTable =
          catalog_.removeTableIfExists(msTbl_, tblWasFound, tblMatched);
      if (removedTable != null) {
        infoLog("Removed table {} ", getFullyQualifiedTblName());
      } else if (!tblMatched.getRef()) {
        LOG.warn(debugString("Table %s was not removed from "
            + "catalog since the creation time of the table did not match", tblName_));
      } else if (!tblWasFound.getRef()) {
        debugLog("Table {} was not removed since it did not exist in catalog.",
                tblName_);
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
          (JSONCreateDatabaseMessage) MetastoreEventsProcessor.getMessageFactory()
              .getDeserializer().getCreateDatabaseMessage(event.getMessage());
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

    /**
     * Processes the create database event by adding the Db object from the event if it
     * does not exist in the catalog already. TODO we should compare the creationTime of
     * the Database in catalog with the Database in the event to make sure we are ignoring
     * only catalog has the latest Database object. This will be added after HIVE-21077 is
     * fixed and available
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
            infoLog(
                "Found database {} is removed later in event {} of type {} ",
                dbName_, dropDatabaseEvent.eventId_, dropDatabaseEvent.eventType_);
            return true;
          }
        }
      }
      return false;
    }
  }

  /**
   * MetastoreEvent for the DROP_DATABASE event
   */
  public static class DropDatabaseEvent extends MetastoreDatabaseEvent {

    /**
     * Prevent instantiation from outside should use MetastoreEventFactory instead
     */
    private DropDatabaseEvent(
        CatalogServiceCatalog catalog, Metrics metrics, NotificationEvent event) {
      super(catalog, metrics, event);
    }

    /**
     * Process the drop database event. Currently, this handler removes the db object from
     * catalog. TODO Once we have HIVE-21077 we should compare creationTime to make sure
     * that catalog's Db matches with the database object in the event
     */
    @Override
    public void process() {
      // TODO this does not currently handle the case where the was a new instance
      // of database with the same name created in catalog after this database instance
      // was removed. For instance, user does a CREATE db, drop db and create db again
      // with the same dbName. In this case, the drop database event will remove the
      // database instance which is created after this create event. We should add a
      // check to compare the creation time of the database with the creation time in
      // the event to make sure we are removing the right databases object. Unfortunately,
      // database do not have creation time currently. This would be fixed in HIVE-21077
      Db removedDb = catalog_.removeDb(dbName_);
      // if database did not exist in the cache there was nothing to do
      if (removedDb != null) {
        infoLog("Successfully removed database {}", dbName_);
      }
    }
  }

  /**
   * MetastoreEvent for which issues invalidate on a table from the event
   */
  public static abstract class TableInvalidatingEvent extends MetastoreTableEvent {

    /**
     * Prevent instantiation from outside should use MetastoreEventFactory instead
     */
    private TableInvalidatingEvent(
        CatalogServiceCatalog catalog, Metrics metrics, NotificationEvent event) {
      super(catalog, metrics, event);
    }

    /**
     * Issues a invalidate table on the catalog on the table from the event. This
     * invalidate does not fetch information from metastore unlike the invalidate metadata
     * command since event is triggered post-metastore activity. This handler invalidates
     * by atomically removing existing loaded table and replacing it with a
     * IncompleteTable. If the table doesn't exist in catalog this operation is a no-op
     */
    @Override
    public void process() {
      if (invalidateCatalogTable()) {
        infoLog("Table {} is invalidated", getFullyQualifiedTblName());
      } else {
        debugLog("Table {} does not need to be invalidated since "
            + "it does not exist anymore", getFullyQualifiedTblName());
      }
    }
  }

  public static class AddPartitionEvent extends TableInvalidatingEvent {

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
      AddPartitionMessage addPartitionMessage =
          MetastoreEventsProcessor.getMessageFactory().getDeserializer()
              .getAddPartitionMessage(event.getMessage());
      try {
        msTbl_ = addPartitionMessage.getTableObj();
      } catch (Exception ex) {
        throw new MetastoreNotificationException(ex);
      }
    }
  }

  public static class AlterPartitionEvent extends TableInvalidatingEvent {

    /**
     * Prevent instantiation from outside should use MetastoreEventFactory instead
     */
    private AlterPartitionEvent(CatalogServiceCatalog catalog, Metrics metrics,
        NotificationEvent event) throws MetastoreNotificationException {
      super(catalog, metrics, event);
      Preconditions.checkState(eventType_.equals(MetastoreEventType.ALTER_PARTITION));
      Preconditions.checkNotNull(event.getMessage());
      AlterPartitionMessage alterPartitionMessage =
          MetastoreEventsProcessor.getMessageFactory().getDeserializer()
              .getAlterPartitionMessage(event.getMessage());
      try {
        msTbl_ = alterPartitionMessage.getTableObj();
      } catch (Exception ex) {
        throw new MetastoreNotificationException(ex);
      }
    }
  }

  public static class DropPartitionEvent extends TableInvalidatingEvent {

    private final boolean isEventProcessingDisabled_;
    /**
     * Prevent instantiation from outside should use MetastoreEventFactory instead
     */
    private DropPartitionEvent(
        CatalogServiceCatalog catalog, Metrics metrics, NotificationEvent event) {
      super(catalog, metrics, event);
      Preconditions.checkState(eventType_.equals(MetastoreEventType.DROP_PARTITION));
      Preconditions.checkNotNull(event.getMessage());
      //TODO we should use DropPartitionMessage to get the table object here but
      // current DropPartitionMessage does not provide the table object
      isEventProcessingDisabled_ = Boolean.valueOf(
          catalog_.getTableProperty(dbName_, tblName_, DISABLE_EVENT_HMS_SYNC_KEY));
    }

    @Override
    public boolean isEventProcessingDisabled() {
      return isEventProcessingDisabled_;
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
      debugLog("Ignored");
    }

    @Override
    protected boolean isEventProcessingDisabled() {
      return false;
    }
  }
}
