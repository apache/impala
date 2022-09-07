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

package org.apache.impala.catalog.iceberg;

import static org.apache.impala.catalog.Table.TBL_PROP_EXTERNAL_TABLE_PURGE;
import static org.apache.impala.catalog.Table.TBL_PROP_EXTERNAL_TABLE_PURGE_DEFAULT;

import java.util.Map;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.hadoop.ConfigProperties;
import org.apache.iceberg.hive.HiveCatalog;
import org.apache.impala.catalog.FeIcebergTable;
import org.apache.impala.catalog.IcebergTableLoadingException;
import org.apache.impala.catalog.TableLoadingException;
import org.apache.impala.thrift.TIcebergCatalog;
import org.apache.impala.util.IcebergUtil;

import com.google.common.base.Preconditions;

/**
 * Implementation of IcebergCatalog for tables stored in HiveCatalog.
 */
public class IcebergHiveCatalog implements IcebergCatalog {
  private static IcebergHiveCatalog instance_;

  public synchronized static IcebergHiveCatalog getInstance() {
    if (instance_ == null) {
      instance_ = new IcebergHiveCatalog();
    }
    return instance_;
  }

  private final HiveCatalog hiveCatalog_;

  private IcebergHiveCatalog() {
    setContextClassLoader();
    HiveConf conf = new HiveConf(IcebergHiveCatalog.class);
    conf.setBoolean(ConfigProperties.ENGINE_HIVE_ENABLED, true);
    hiveCatalog_ = new HiveCatalog();
    hiveCatalog_.setConf(conf);
    Map<String, String> properties = IcebergUtil.composeCatalogProperties();
    hiveCatalog_.initialize("ImpalaHiveCatalog", properties);
  }

  @Override
  public Table createTable(
      TableIdentifier identifier,
      Schema schema,
      PartitionSpec spec,
      String location,
      Map<String, String> properties) {
    properties.put(TBL_PROP_EXTERNAL_TABLE_PURGE, properties.getOrDefault(
        TBL_PROP_EXTERNAL_TABLE_PURGE, TBL_PROP_EXTERNAL_TABLE_PURGE_DEFAULT));
    return hiveCatalog_.createTable(identifier, schema, spec, location, properties);
  }

  @Override
  public Table loadTable(FeIcebergTable feTable) throws TableLoadingException {
    Preconditions.checkState(
        feTable.getIcebergCatalog() == TIcebergCatalog.HIVE_CATALOG);
    TableIdentifier tableId = IcebergUtil.getIcebergTableIdentifier(feTable);
    return loadTable(tableId, null, null);
  }

  @Override
  public Table loadTable(TableIdentifier tableId, String tableLocation,
      Map<String, String> properties) throws IcebergTableLoadingException {
    Preconditions.checkState(tableId != null);
    try {
      return hiveCatalog_.loadTable(tableId);
    } catch (Exception e) {
      throw new IcebergTableLoadingException(String.format(
          "Failed to load Iceberg table with id: %s", tableId), e);
    }
  }

  @Override
  public boolean dropTable(FeIcebergTable feTable, boolean purge) {
    Preconditions.checkState(
        feTable.getIcebergCatalog() == TIcebergCatalog.HIVE_CATALOG);
    TableIdentifier tableId = IcebergUtil.getIcebergTableIdentifier(feTable);
    return hiveCatalog_.dropTable(tableId, purge);
  }

  @Override
  public boolean dropTable(String dbName, String tblName, boolean purge) {
    return hiveCatalog_.dropTable(TableIdentifier.of(dbName, tblName), purge);
  }

  @Override
  public void renameTable(FeIcebergTable feTable, TableIdentifier newTableId) {
    TableIdentifier oldTableId = IcebergUtil.getIcebergTableIdentifier(feTable);
    hiveCatalog_.renameTable(oldTableId, newTableId);
  }
}
