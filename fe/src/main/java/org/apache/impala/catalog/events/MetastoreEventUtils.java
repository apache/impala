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

import com.google.common.base.Preconditions;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.NotificationEvent;
import org.apache.hadoop.hive.metastore.messaging.json.JSONAlterTableMessage;
import org.apache.hadoop.hive.metastore.messaging.json.JSONCreateDatabaseMessage;
import org.apache.hadoop.hive.metastore.messaging.json.JSONDropTableMessage;
import org.apache.impala.analysis.TableName;
import org.apache.impala.catalog.CatalogException;
import org.apache.impala.catalog.CatalogServiceCatalog;
import org.apache.impala.catalog.Db;
import org.apache.impala.catalog.Table;
import org.apache.impala.common.Pair;
import org.apache.impala.common.Reference;
import org.apache.impala.thrift.TTableName;
import org.apache.log4j.Logger;

/**
 * Util class which provides Metastore event objects for various event types. Also
 * provides a MetastoreEventFactory to get or create the event instances for a given event
 * type
 */
public class MetastoreEventUtils {

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
    private static final Logger LOG = Logger.getLogger(MetastoreEventFactory.class);

    // catalog service instance to be used for creating eventHandlers
    private final CatalogServiceCatalog catalog_;

    public MetastoreEventFactory(CatalogServiceCatalog catalog) {
      this.catalog_ = Preconditions.checkNotNull(catalog);
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
        case CREATE_TABLE:
          return new CreateTableEvent(catalog_, event);
        case DROP_TABLE:
          return new DropTableEvent(catalog_, event);
        case ALTER_TABLE:
          return new AlterTableEvent(catalog_, event);
        case CREATE_DATABASE:
          return new CreateDatabaseEvent(catalog_, event);
        case DROP_DATABASE:
          return new DropDatabaseEvent(catalog_, event);
        case ALTER_DATABASE:
          // alter database events are currently ignored
          return new IgnoredEvent(catalog_, event);
        case ADD_PARTITION:
          // add partition events triggers invalidate table currently
          return new TableInvalidatingEvent(catalog_, event);
        case DROP_PARTITION:
          // drop partition events triggers invalidate table currently
          return new TableInvalidatingEvent(catalog_, event);
        case ALTER_PARTITION:
          // alter partition events triggers invalidate table currently
          return new TableInvalidatingEvent(catalog_, event);
        default:
          // ignore all the unknown events by creating a IgnoredEvent
          return new IgnoredEvent(catalog_, event);
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
      List<MetastoreEvent> metastoreEvents = new ArrayList<>(events.size());
      for (NotificationEvent event : events) {
        metastoreEvents.add(get(event));
      }
      Iterator<MetastoreEvent> it = metastoreEvents.iterator();
      // filter out the create events which has a corresponding drop event later
      int fromIndex = 0;
      int numFilteredEvents = 0;
      int inputSize = metastoreEvents.size();
      while (it.hasNext()) {
        MetastoreEvent current = it.next();
        if (fromIndex < metastoreEvents.size() && current.isRemovedAfter(
            metastoreEvents.subList(fromIndex + 1, metastoreEvents.size()))) {
          LOG.info(current.debugString("Filtering out this event since the object is "
              + "either removed or renamed later in the event stream"));
          it.remove();
          numFilteredEvents++;
        }
        fromIndex++;
      }
      LOG.info(String.format("Total number of events received: %d Total number of events "
          + "filtered out: %d", inputSize, numFilteredEvents));
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

    // CatalogServiceCatalog instance on which the event needs to be acted upon
    protected final CatalogServiceCatalog catalog_;

    // the notification received from metastore which is processed by this
    protected final NotificationEvent event_;

    // Logger available for all the sub-classes
    protected final Logger LOG = Logger.getLogger(this.getClass());

    // dbName from the event
    protected final String dbName_;

    // eventId of the event. Used instead of calling getter on event_ everytime
    protected final long eventId_;

    // eventType from the NotificationEvent
    protected final MetastoreEventType eventType_;

    protected final NotificationEvent metastoreNotificationEvent_;

    MetastoreEvent(CatalogServiceCatalog catalogServiceCatalog, NotificationEvent event) {
      this.catalog_ = catalogServiceCatalog;
      this.event_ = event;
      this.eventId_ = event_.getEventId();
      this.eventType_ = MetastoreEventType.from(event.getEventType());
      LOG.debug(String
          .format("Creating event %d of type %s on table %s", event.getEventId(),
              event.getEventType(), event.getTableName()));
      dbName_ = Preconditions.checkNotNull(event.getDbName());
      metastoreNotificationEvent_ = event;
    }

    /**
     * Process the information available in the NotificationEvent to take appropriate
     * action on Catalog
     *
     * @throws MetastoreNotificationException in case of event parsing errors out
     * @throws CatalogException in case catalog operations could not be performed
     */
    abstract void process() throws MetastoreNotificationException, CatalogException;

    /**
     * Helper method to get debug string with helpful event information prepended to the
     * message
     *
     * @param msgFormatString String value to be used in String.format() for the given
     *     message
     * @param args args to the <code>String.format()</code> for the given
     *     msgFormatString
     */
    protected String debugString(String msgFormatString, Object... args) {
      String formatString =
          new StringBuilder("EventId: %d EventType: %s ").append(msgFormatString)
              .toString();
      Object[] formatArgs = new Object[args.length + 2];
      formatArgs[0] = eventId_;
      formatArgs[1] = eventType_;
      int i=2;
      for (Object arg : args) {
        formatArgs[i] = arg;
        i++;
      }
      return String.format(formatString, formatArgs);
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
  }

  /**
   * Base class for all the table events
   */
  public static abstract class MetastoreTableEvent extends MetastoreEvent {

    // tblName from the event
    protected final String tblName_;

    private MetastoreTableEvent(CatalogServiceCatalog catalogServiceCatalog,
        NotificationEvent event) {
      super(catalogServiceCatalog, event);
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
     * Util method to issue invalidate on a given table on the catalog. This method
     * atomically invalidates the table if it exists in the catalog. No-op if the table
     * does not exist
     */
    protected boolean invalidateCatalogTable() {
      return catalog_.invalidateTableIfExists(dbName_, tblName_) != null;
    }
  }

  /**
   * Base class for all the database events
   */
  public static abstract class MetastoreDatabaseEvent extends MetastoreEvent {

    MetastoreDatabaseEvent(CatalogServiceCatalog catalogServiceCatalog,
        NotificationEvent event) {
      super(catalogServiceCatalog, event);
    }
  }

  /**
   * MetastoreEvent for CREATE_TABLE event type
   */
  private static class CreateTableEvent extends MetastoreTableEvent {

    /**
     * Prevent instantiation from outside should use MetastoreEventFactory instead
     */
    private CreateTableEvent(CatalogServiceCatalog catalog, NotificationEvent event) {
      super(catalog, event);
      Preconditions.checkArgument(MetastoreEventType.CREATE_TABLE.equals(eventType_));
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
      boolean tableAdded;
      try {
        tableAdded = catalog_.addTableIfNotExists(dbName_, tblName_);
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
      if (!tableAdded) {
        LOG.debug(
            debugString("Not adding the table %s since it already exists in catalog",
                tblName_));
        return;
      }
      LOG.info(debugString("Added a table %s", getFullyQualifiedTblName()));
    }

    @Override
    public boolean isRemovedAfter(List<MetastoreEvent> events) {
      Preconditions.checkNotNull(events);
      for (MetastoreEvent event : events) {
        if (event.eventType_.equals(MetastoreEventType.DROP_TABLE)) {
          DropTableEvent dropTableEvent = (DropTableEvent) event;
          if (dbName_.equalsIgnoreCase(dropTableEvent.dbName_) && tblName_
              .equalsIgnoreCase(dropTableEvent.tblName_)) {
            LOG.info(debugString("Found table %s is removed later in event %d type %s",
                tblName_, dropTableEvent.eventId_, dropTableEvent.eventType_));
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
          if (alterTableEvent.isRename_ && dbName_
              .equalsIgnoreCase(alterTableEvent.tableBefore_.getDbName()) && tblName_
              .equalsIgnoreCase(alterTableEvent.tableBefore_.getTableName())) {
            LOG.info(debugString("Found table %s is renamed later in event %d type %s",
                tblName_, alterTableEvent.eventId_, alterTableEvent.eventType_));
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
  private static class AlterTableEvent extends MetastoreTableEvent {

    // the table object before alter operation, as parsed from the NotificationEvent
    private final org.apache.hadoop.hive.metastore.api.Table tableBefore_;
    // the table object after alter operation, as parsed from the NotificationEvent
    private final org.apache.hadoop.hive.metastore.api.Table tableAfter_;
    // true if this alter event was due to a rename operation
    private final boolean isRename_;

    /**
     * Prevent instantiation from outside should use MetastoreEventFactory instead
     */
    private AlterTableEvent(CatalogServiceCatalog catalog, NotificationEvent event)
        throws MetastoreNotificationException {
      super(catalog, event);
      Preconditions.checkArgument(MetastoreEventType.ALTER_TABLE.equals(eventType_));
      JSONAlterTableMessage alterTableMessage =
          (JSONAlterTableMessage) MetastoreEventsProcessor.getMessageFactory()
              .getDeserializer().getAlterTableMessage(event.getMessage());
      try {
        tableBefore_ = Preconditions.checkNotNull(alterTableMessage.getTableObjBefore());
        tableAfter_ = Preconditions.checkNotNull(alterTableMessage.getTableObjAfter());
      } catch (Exception e) {
        throw new MetastoreNotificationException(
            debugString("Unable to parse the alter table message"), e);
      }
      // this is a rename event if either dbName or tblName of before and after object
      // changed
      isRename_ = !tableBefore_.getDbName().equalsIgnoreCase(tableAfter_.getDbName())
          || !tableBefore_.getTableName().equalsIgnoreCase(tableAfter_.getTableName());
    }

    /**
     * If the ALTER_TABLE event is due a table rename, this method removes the old table
     * and creates a new table with the new name. Else, this just issues a invalidate
     * table on the tblName from the event//TODO Check if we can rename the existing table
     * in-place
     */
    @Override
    public void process() throws MetastoreNotificationException {
      // in case of table level alters from external systems it is better to do a full
      // invalidate  eg. this could be due to as simple as adding a new parameter or a
      // full blown adding  or changing column type
      // detect the special where a table is renamed
      try {
        if (!isRename_) {
          // table is not renamed, need to invalidate
          if (!invalidateCatalogTable()) {
            LOG.debug(debugString("Table %s does not need to be "
                    + "invalidated since it does not exist anymore",
                getFullyQualifiedTblName()));
          } else {
            LOG.info(debugString("Table %s is invalidated", getFullyQualifiedTblName()));
          }
          return;
        }
        // table was renamed, remove the old table
        LOG.info(debugString("Found that %s table was renamed. Renaming it by "
                + "remove and adding a new table", new TableName(tableBefore_.getDbName(),
            tableBefore_.getTableName())));
        TTableName oldTTableName =
            new TTableName(tableBefore_.getDbName(), tableBefore_.getTableName());
        TTableName newTTableName =
            new TTableName(tableAfter_.getDbName(), tableAfter_.getTableName());

        // atomically rename the old table to new table
        Pair<Boolean, Boolean> result =
            catalog_.renameOrAddTableIfNotExists(oldTTableName, newTTableName);

        // old table was not found. This could be because catalogD is stale and didn't
        // have any entry for the oldTable
        if (!result.first) {
          LOG.debug(debugString("Did not remove old table to rename table %s to %s since "
                  + "it does not exist anymore", qualify(oldTTableName),
              qualify(newTTableName)));
        }
        // the new table from the event was not added since it was already present
        if (!result.second) {
          LOG.debug(
              debugString("Did not add new table name while renaming table %s to %s",
                  qualify(oldTTableName), qualify(newTTableName)));
        }
      } catch (Exception e) {
        throw new MetastoreNotificationException(e);
      }
    }

    private String qualify(TTableName tTableName) {
      return new TableName(tTableName.db_name, tTableName.table_name).toString();
    }
  }

  /**
   * MetastoreEvent for the DROP_TABLE event type
   */
  private static class DropTableEvent extends MetastoreTableEvent {

    // the metastore table object as parsed from the drop table event
    private final org.apache.hadoop.hive.metastore.api.Table droppedTable_;

    /**
     * Prevent instantiation from outside should use MetastoreEventFactory instead
     */
    private DropTableEvent(CatalogServiceCatalog catalog, NotificationEvent event)
        throws MetastoreNotificationException {
      super(catalog, event);
      Preconditions.checkArgument(MetastoreEventType.DROP_TABLE.equals(eventType_));
      JSONDropTableMessage dropTableMessage =
          (JSONDropTableMessage) MetastoreEventsProcessor.getMessageFactory()
              .getDeserializer().getDropTableMessage(event.getMessage());
      try {
        droppedTable_ = Preconditions.checkNotNull(dropTableMessage.getTableObj());
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
          catalog_.removeTableIfExists(droppedTable_, tblWasFound, tblMatched);
      if (removedTable != null) {
        LOG.info(debugString("Removed table %s ", getFullyQualifiedTblName()));
      } else if (!tblMatched.getRef()) {
        LOG.warn(debugString("Table %s was not removed from "
            + "catalog since the creation time of the table did not match", tblName_));
      } else if (!tblWasFound.getRef()) {
        LOG.debug(
            debugString("Table %s was not removed since it did not exist in catalog.",
                tblName_));
      }
    }
  }

  /**
   * MetastoreEvent for CREATE_DATABASE event type
   */
  private static class CreateDatabaseEvent extends MetastoreDatabaseEvent {

    // metastore database object as parsed from NotificationEvent message
    private final Database createdDatabase_;

    /**
     * Prevent instantiation from outside should use MetastoreEventFactory instead
     */
    private CreateDatabaseEvent(CatalogServiceCatalog catalog, NotificationEvent event)
        throws MetastoreNotificationException {
      super(catalog, event);
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
        LOG.info(debugString("Successfully added database %s", dbName_));
      } else {
        LOG.info(debugString("Database %s already exists", dbName_));
      }
    }

    @Override
    public boolean isRemovedAfter(List<MetastoreEvent> events) {
      Preconditions.checkNotNull(events);
      for (MetastoreEvent event : events) {
        if (event.eventType_.equals(MetastoreEventType.DROP_DATABASE)) {
          DropDatabaseEvent dropDatabaseEvent = (DropDatabaseEvent) event;
          if (dbName_.equalsIgnoreCase(dropDatabaseEvent.dbName_)) {
            LOG.info(debugString(
                "Found database %s is removed later in event %d of " + "type %s ",
                dbName_, dropDatabaseEvent.eventId_, dropDatabaseEvent.eventType_));
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
  private static class DropDatabaseEvent extends MetastoreDatabaseEvent {

    /**
     * Prevent instantiation from outside should use MetastoreEventFactory instead
     */
    private DropDatabaseEvent(CatalogServiceCatalog catalog, NotificationEvent event) {
      super(catalog, event);
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
        LOG.info(debugString("Successfully removed database %s", dbName_));
      }
    }
  }

  /**
   * MetastoreEvent for which issues invalidate on a table from the event
   */
  private static class TableInvalidatingEvent extends MetastoreTableEvent {

    /**
     * Prevent instantiation from outside should use MetastoreEventFactory instead
     */
    private TableInvalidatingEvent(CatalogServiceCatalog catalog,
        NotificationEvent event) {
      super(catalog, event);
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
        LOG.info(debugString("Table %s is invalidated", getFullyQualifiedTblName()));
      } else {
        LOG.debug(debugString("Table %s does not need to be invalidated since "
            + "it does not exist anymore", getFullyQualifiedTblName()));
      }
    }
  }

  /**
   * An event type which is ignored. Useful for unsupported metastore event types
   */
  private static class IgnoredEvent extends MetastoreEvent {

    /**
     * Prevent instantiation from outside should use MetastoreEventFactory instead
     */
    private IgnoredEvent(CatalogServiceCatalog catalog, NotificationEvent event) {
      super(catalog, event);
    }

    @Override
    public void process() {
      LOG.debug(debugString("Ignored"));
    }
  }
}
