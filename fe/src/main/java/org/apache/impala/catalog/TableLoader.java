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

package org.apache.impala.catalog;

import com.google.common.base.Preconditions;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.apache.hadoop.hive.metastore.TableType;
import org.apache.hadoop.hive.metastore.api.NoSuchObjectException;
import org.apache.hadoop.hive.metastore.api.NotificationEvent;
import org.apache.impala.catalog.events.MetastoreEvents.CreateTableEvent;
import org.apache.impala.catalog.events.MetastoreEventsProcessor;
import org.apache.impala.common.Metrics;
import org.apache.impala.compat.MetastoreShim;
import org.apache.impala.service.BackendConfig;
import org.apache.impala.util.EventSequence;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.thrift.TException;

import com.google.common.base.Stopwatch;

import org.apache.impala.catalog.MetaStoreClientPool.MetaStoreClient;
import org.apache.impala.util.ThreadNameAnnotator;

import static org.apache.impala.service.CatalogOpExecutor.FETCHED_HMS_EVENT_BATCH;
import static org.apache.impala.service.CatalogOpExecutor.FETCHED_HMS_TABLE;

/**
 * Class that implements the logic for how a table's metadata should be loaded from
 * the Hive Metastore / HDFS / etc.
 */
public class TableLoader {
  private static final Logger LOG = LoggerFactory.getLogger(TableLoader.class);

  private final CatalogServiceCatalog catalog_;

  // Lock used to serialize calls to the Hive MetaStore to work around MetaStore
  // concurrency bugs. Currently used to serialize calls to "getTable()" due to
  // HIVE-5457.
  private static final Object metastoreAccessLock_ = new Object();
  private Metrics metrics_ = new Metrics();

  public TableLoader(CatalogServiceCatalog catalog) {
    Preconditions.checkNotNull(catalog);
    catalog_ = catalog;
    initMetrics(metrics_);
  }

  /**
   * Creates the Impala representation of Hive/HBase metadata for one table.
   * Calls load() on the appropriate instance of Table subclass. If the eventId is not
   * negative, it fetches all events from metastore to find out a CREATE_TABLE
   * event from metastore which is used to set the createEventId of the table.
   * Returns new instance of Table, If there were any errors loading the table metadata
   * an IncompleteTable will be returned that contains details on the error.
   */
  public Table load(Db db, String tblName, long eventId, String reason,
      EventSequence catalogTimeline) {
    Stopwatch sw = Stopwatch.createStarted();
    String fullTblName = db.getName() + "." + tblName;
    String annotation = "Loading metadata for: " + fullTblName + " (" + reason + ")";
    LOG.info(annotation);
    Table table = null;
    boolean syncToLatestEventId =
        BackendConfig.INSTANCE.enableSyncToLatestEventOnDdls();
    // turn all exceptions into TableLoadingException
    List<NotificationEvent> events = null;
    try (ThreadNameAnnotator tna = new ThreadNameAnnotator(annotation);
         MetaStoreClient msClient = catalog_.getMetaStoreClient(catalogTimeline)) {
      org.apache.hadoop.hive.metastore.api.Table msTbl = null;
      // All calls to getTable() need to be serialized due to HIVE-5457.
      Stopwatch hmsLoadSW = Stopwatch.createStarted();
      synchronized (metastoreAccessLock_) {
        msTbl = msClient.getHiveClient().getTable(db.getName(), tblName);
        catalogTimeline.markEvent(FETCHED_HMS_TABLE);
      }
      if (eventId != -1 && catalog_.isEventProcessingActive()) {
        // If the eventId is not -1 it means this table was likely created by Impala.
        // However, since the load operation of the table can happen much later, it is
        // possible that the table was recreated outside Impala and hence the eventId
        // which is stored in the loaded table needs to be updated to the latest.
        // we are only interested in fetching the events if we have a valid eventId
        // for a table. For tables where eventId is unknown are not created by
        // this catalogd and hence the self-event detection logic does not apply.
        events = MetastoreEventsProcessor.getNextMetastoreEventsInBatchesForTable(
            catalog_, eventId, db.getName(), tblName, CreateTableEvent.EVENT_TYPE);
        catalogTimeline.markEvent(FETCHED_HMS_EVENT_BATCH);
      }
      if (events != null && !events.isEmpty()) {
        // if the table was recreated after the table was initially created in the
        // catalogd, we should move the eventId forward to the latest create_table
        // event.
        eventId = events.get(events.size() - 1).getEventId();
      }
      long hmsLoadTime = hmsLoadSW.elapsed(TimeUnit.NANOSECONDS);
      // Check that the Hive TableType is supported
      TableType tableType = TableType.valueOf(msTbl.getTableType());
      if (!MetastoreShim.IMPALA_SUPPORTED_TABLE_TYPES.contains(tableType)) {
        throw new TableLoadingException(String.format(
            "Unsupported table type '%s' for: %s", tableType, fullTblName));
      }

      // Create a table of appropriate type and have it load itself
      table = Table.fromMetastoreTable(db, msTbl);
      if (table == null) {
        throw new TableLoadingException(
            "Unrecognized table type for table: " + fullTblName);
      }
      table.updateHMSLoadTableSchemaTime(hmsLoadTime);
      table.setCreateEventId(eventId);
      long latestEventId = -1;
      if (syncToLatestEventId) {
        // acquire write lock on table since MetastoreEventProcessor.syncToLatestEventId
        // expects current thread to have write lock on the table
        if (!catalog_.tryWriteLock(table, catalogTimeline)) {
          throw new CatalogException("Couldn't acquire write lock on new table object"
              + " created when doing a full table reload of " + table.getFullName());
        }
        catalog_.getLock().writeLock().unlock();
        try {
          latestEventId = msClient.getHiveClient().
              getCurrentNotificationEventId().getEventId();
        } catch (TException e) {
          throw new TableLoadingException("Failed to get latest event id from HMS "
              + "while loading table: " + table.getFullName(), e);
        }
      }
      table.load(false, msClient.getHiveClient(), msTbl, reason, catalogTimeline);
      table.validate();
      if (syncToLatestEventId) {
        LOG.debug("After full reload, table {} is synced atleast till event id {}. "
                + "Checking if there are more events generated for this table "
                + "while the full reload was in progress", table.getFullName(),
            latestEventId);
        table.setLastSyncedEventId(latestEventId);
        // write lock is not required since it is full table reload
        MetastoreEventsProcessor.syncToLatestEventId(catalog_, table,
            catalog_.getEventFactoryForSyncToLatestEvent(), metrics_);
      }
      table.setLastRefreshEventId(latestEventId);
    } catch (TableLoadingException e) {
      table = IncompleteTable.createFailedMetadataLoadTable(db, tblName, e);
    } catch (NoSuchObjectException e) {
      TableLoadingException tableDoesNotExist = new TableLoadingException(
          "Table " + fullTblName + " no longer exists in the Hive MetaStore. " +
          "Run 'invalidate metadata " + fullTblName + "' to update the Impala " +
          "catalog.");
      table = IncompleteTable.createFailedMetadataLoadTable(
          db, tblName, tableDoesNotExist);
    } catch (Throwable e) {
      table = IncompleteTable.createFailedMetadataLoadTable(
          db, tblName, new TableLoadingException(
          "Failed to load metadata for table: " + fullTblName + ". Running " +
          "'invalidate metadata " + fullTblName + "' may resolve this problem.", e));
    } finally {
      if (table != null && table.isWriteLockedByCurrentThread()) {
        table.releaseWriteLock();
      }
    }
    LOG.info("Loaded metadata for: " + fullTblName + " (" +
        sw.elapsed(TimeUnit.MILLISECONDS) + "ms)");
    return table;
  }

  private void initMetrics(Metrics metrics) {
    metrics.addTimer(
        MetastoreEventsProcessor.EVENTS_FETCH_DURATION_METRIC);
    metrics.addTimer(
        MetastoreEventsProcessor.EVENTS_PROCESS_DURATION_METRIC);
    metrics.addMeter(
        MetastoreEventsProcessor.EVENTS_RECEIVED_METRIC);
    metrics.addCounter(
        MetastoreEventsProcessor.EVENTS_SKIPPED_METRIC);
    metrics.addCounter(
        MetastoreEventsProcessor.NUMBER_OF_TABLE_REFRESHES);
    metrics.addCounter(
        MetastoreEventsProcessor.NUMBER_OF_PARTITION_REFRESHES);
    metrics.addCounter(
        MetastoreEventsProcessor.NUMBER_OF_TABLES_ADDED);
    metrics.addCounter(
        MetastoreEventsProcessor.NUMBER_OF_TABLES_REMOVED);
    metrics.addCounter(
        MetastoreEventsProcessor.NUMBER_OF_DATABASES_ADDED);
    metrics.addCounter(
        MetastoreEventsProcessor.NUMBER_OF_DATABASES_REMOVED);
    metrics.addCounter(
        MetastoreEventsProcessor.NUMBER_OF_PARTITIONS_ADDED);
    metrics.addCounter(
        MetastoreEventsProcessor.NUMBER_OF_PARTITIONS_REMOVED);
  }
}
