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

import static org.junit.Assert.assertFalse;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.hive.metastore.api.Catalog;
import org.apache.hadoop.hive.metastore.api.CurrentNotificationEventId;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.InsertEventRequestData;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.NotificationEvent;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.SerDeInfo;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.apache.hadoop.hive.metastore.api.PrincipalType;
import org.apache.hadoop.hive.metastore.messaging.AlterTableMessage;
import org.apache.impala.catalog.MetaStoreClientPool.MetaStoreClient;
import org.apache.thrift.TException;

/**
 * Utils class to create/get objects in HMS
 */
public class MetastoreApiTestUtils {

  public static Database createHmsDatabaseObject(String catName,
      String dbName, Map<String, String> params) {
    Database database = new Database();
    if (catName != null) database.setCatalogName(catName);
    database.setName(dbName);
    database.setDescription("Notification test database");
    database.setOwnerName("NotificationOwner");
    database.setOwnerType(PrincipalType.USER);
    if (params != null && !params.isEmpty()) {
      database.setParameters(params);
    }
    return database;
  }

  public static void addDatabaseParametersInHms(MetaStoreClient msClient, String dbName,
      String key, String val) throws TException {
    Database msDb = msClient.getHiveClient().getDatabase(dbName);
    assertFalse(key + " already exists in the database parameters",
        msDb.getParameters().containsKey(key));
    msDb.putToParameters(key, val);
    msClient.getHiveClient().alterDatabase(dbName, msDb);
  }

  /*
  Get a metastore external table object
   */
  public static org.apache.hadoop.hive.metastore.api.Table getTestTable(String catName,
      String dbName, String tblName, Map<String, String> params, boolean isPartitioned)
      throws MetaException {
    return getTestTable(catName, dbName, tblName, params, isPartitioned,
        org.apache.hadoop.hive.metastore.TableType.EXTERNAL_TABLE.toString());
  }

  /*
  Get a metastore table object. If tableType is non-null, its valid values
  are: EXTERNAL_TABLE, MANAGED_TABLE
  */
  public static org.apache.hadoop.hive.metastore.api.Table getTestTable(String catName,
      String dbName, String tblName, Map<String, String> params, boolean isPartitioned,
      String tableType) throws MetaException {
    org.apache.hadoop.hive.metastore.api.Table tbl =
        new org.apache.hadoop.hive.metastore.api.Table();
    if (catName != null) tbl.setCatName(catName);
    tbl.setDbName(dbName);
    tbl.setTableName(tblName);
    tbl.putToParameters("tblParamKey", "tblParamValue");
    List<FieldSchema> cols = Lists.newArrayList(
        new FieldSchema("c1","string","c1 description"),
        new FieldSchema("c2", "string","c2 description"));

    StorageDescriptor sd = new StorageDescriptor();
    sd.setCols(cols);
    sd.setInputFormat(HdfsFileFormat.PARQUET.inputFormat());
    sd.setOutputFormat(HdfsFileFormat.PARQUET.outputFormat());

    SerDeInfo serDeInfo = new SerDeInfo();
    serDeInfo.setSerializationLib(HdfsFileFormat.PARQUET.serializationLib());
    sd.setSerdeInfo(serDeInfo);
    tbl.setSd(sd);

    if (params != null && !params.isEmpty()) {
      tbl.setParameters(params);
    }
    if (isPartitioned) {
      List<FieldSchema> pcols = Lists.newArrayList(
          new FieldSchema("p1","string","partition p1 description"));
      tbl.setPartitionKeys(pcols);
    }
    if (tableType != null) {
      Preconditions.checkArgument(tableType.equals(
          org.apache.hadoop.hive.metastore.TableType.MANAGED_TABLE.toString()) ||
          tableType.equals(org.apache.hadoop.hive.metastore.TableType.EXTERNAL_TABLE
              .toString()), "Invalid table type " + tableType);
      tbl.setTableType(tableType);
    }
    return tbl;
  }

  public static void createDatabase(MetaStoreClient msClient, String catName,
      String dbName, Map<String, String> params) throws TException {
    Database database = createHmsDatabaseObject(catName, dbName, params);
    msClient.getHiveClient().createDatabase(database);
  }

  /*
   Creates an external table since tableType is null
   */
  public static void createTable(MetaStoreClient msClient, String catName, String dbName,
      String tblName, Map<String, String> params, boolean isPartitioned)
      throws TException {
    createTable(msClient, catName, dbName, tblName, params, isPartitioned, null);
  }

  /*
  Creates a table of type tableType. Valid values of tableType are: EXTERNAL_TABLE,
  MANAGED_TABLE
   */
  public static void createTable(MetaStoreClient msClient, String catName,
      String dbName, String tblName, Map<String, String> params,
      boolean isPartitioned, String tableType) throws TException {
    org.apache.hadoop.hive.metastore.api.Table tbl =
        getTestTable(catName, dbName, tblName, params, isPartitioned, tableType);
    msClient.getHiveClient().createTable(tbl);
  }

  public static void addPartitions(MetaStoreClient msClient, String dbName,
      String tblName, List<List<String>> partitionValues) throws TException {
    // int i = 0;
    List<Partition> partitions = new ArrayList(partitionValues.size());
    org.apache.hadoop.hive.metastore.api.Table msTable =
        msClient.getHiveClient().getTable(dbName, tblName);
    for (List<String> partVals : partitionValues) {
      Partition partition = new Partition();
      partition.setDbName(msTable.getDbName());
      partition.setTableName(msTable.getTableName());
      partition.setSd(msTable.getSd().deepCopy());
      partition.setValues(partVals);
      partitions.add(partition);
    }
    msClient.getHiveClient().add_partitions(partitions);
  }
}
