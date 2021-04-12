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
import java.util.List;
import java.util.SortedMap;
import java.util.TreeMap;
import org.apache.hadoop.hive.common.FileUtils;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.impala.catalog.HdfsTable;
import org.apache.impala.catalog.events.MetastoreEvents.MetastoreEvent;
import org.apache.impala.service.CatalogOpExecutor;

/**
 * This class is used to store delete events for catalog objects (database, tables and
 * partitions). These delete events are added when an object is removed from the catalogd
 * so that when subsequent {@link MetastoreEvent} is received by the
 * {@link MetastoreEventsProcessor} it can ignore it. The {@link CatalogOpExecutor}
 * adds the delete events to the log and the {@link MetastoreEventsProcessor} garbage
 * collects the delete events when the event is received from the metastore.
 */
public class DeleteEventLog {

  private SortedMap<Long, Object> eventLog_;
  //TODO add catalogName in this key when we support metastore catalogs
  // key format for databases "DB:DbName"
  private static final String DB_KEY_FORMAT_STR = "DB:%s";
  //TODO add catalogName in this key when we support metastore catalogs
  // key format for tables "TBL:DbName.tblName"
  private static final String TBL_KEY_FORMAT_STR = "TBL:%s.%s";
  // TODO Add catalog name here.
  // key format for partitions "PART:FullTblName.partName"
  private static final String PART_KEY_FORMAT_STR = "PART:%s.%s";

  public DeleteEventLog() {
    eventLog_ = new TreeMap<>();
  }

  /**
   * Generic util method to add an object in the DeleteEventLog.
   * @param eventId The eventId from hive metastore which maps to this deletion event.
   * @param value The value of the DeleteEventLog entry to be stored.
   */
  public synchronized void addRemovedObject(long eventId, Object value) {
    Preconditions.checkNotNull(value);
    eventLog_.put(eventId, value);
  }

  /**
   * Util method to determine if the given object was removed since the given eventId.
   * @param eventId The eventId after which which we are interested to find the deletion
   *                event.
   * @param value The value of the delete entry.
   * @return True if there exists a delete entry with an event which is strictly greater
   * that eventId and value which is equal to the given value. False otherwise.
   */
  public synchronized boolean wasRemovedAfter(long eventId, Object value) {
    Preconditions.checkNotNull(value);
    return keyExistsAfterEventId(eventId, value);
  }

  private boolean keyExistsAfterEventId(long eventId, Object key) {
    for (Object objectName : eventLog_.tailMap(eventId + 1).values()) {
      if (key.equals(objectName)) {
        return true;
      }
    }
    return false;
  }

  /**
   * Removes all the delete log entries which are less than or equal to the given
   * eventId.
   */
  public synchronized void garbageCollect(long eventId) {
    if (!eventLog_.isEmpty() && eventLog_.firstKey() <= eventId) {
      eventLog_ = new TreeMap<>(eventLog_.tailMap(eventId + 1));
    }
  }

  /**
   * Returns the current size of the delete event log.
   */
  public synchronized int size() {
    return eventLog_.size();
  }

  /**
   * Util method to generate a key for database deletion entry value.
   */
  public static String getDbKey(String dbName) {
    return String
        .format(DB_KEY_FORMAT_STR, dbName).toLowerCase();
  }

  /**
   * Util method to generate a key for table deletion entry value.
   */
  public static String getTblKey(String dbName, String tblName) {
    return String.format(TBL_KEY_FORMAT_STR, dbName, tblName).toLowerCase();
  }

  /**
   * Returns the partition key to be used to add a Partition delete log entry.
   * @param hdfsTable The HdfsTable which the partition belongs to.
   * @param partValues The partition values which represent the partition.
   * @return String value to be used in DeleteEventLog for the given partition
   * identifiers.
   */
  public static String getPartitionKey(HdfsTable hdfsTable, List<String> partValues) {
    return String.format(PART_KEY_FORMAT_STR, hdfsTable.getFullName(),
        FileUtils.makePartName(hdfsTable.getClusteringColNames(), partValues));
  }

  /**
   * Util method to generate a database deletion entry value from a
   * {@link Database} object.
   */
  public static String getKey(Database database) {
    return getDbKey(database.getName());
  }

  /**
   * Util method to generate a table deletion entry value from a {@link Table} object.
   */
  public static String getKey(Table tbl) {
    return String
        .format(TBL_KEY_FORMAT_STR, tbl.getDbName(), tbl.getTableName())
        .toLowerCase();
  }
}
