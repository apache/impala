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

import com.google.common.base.Preconditions;
import com.google.common.collect.Streams;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.stream.Collectors;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.iceberg.CatalogUtil;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.PartitionSpecParser;
import org.apache.iceberg.Schema;
import org.apache.iceberg.SchemaParser;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.hadoop.ConfigProperties;
import org.apache.impala.catalog.FeIcebergTable;
import org.apache.impala.catalog.IcebergTable;
import org.apache.impala.catalog.IcebergTableLoadingException;
import org.apache.impala.catalog.TableLoadingException;
import org.apache.impala.common.ImpalaRuntimeException;
import org.apache.impala.thrift.TIcebergCatalog;
import org.apache.impala.util.IcebergUtil;

/**
 * Implementation of IcebergCatalog for tables using CatalogUtil API.
 * Supports custom catalog configurations via iceberg.catalog.<name>.* properties.
 */
public class IcebergCatalogUtil implements IcebergCatalog {
  private static IcebergCatalogUtil instance_;

  public static final String CATALOGS_NAME_PROPERTY = "name";
  public static final String CATALOGS_LOCATION_PROPERTY = "location";
  public static final String CATALOG_CONFIG_PREFIX = "iceberg.catalog.";
  // Based on InputFormatConfig.TABLE_IDENTIFIER
  public static final String TABLE_IDENTIFIER = "iceberg.mr.table.identifier";

  public synchronized static IcebergCatalogUtil getInstance() {
    if (instance_ == null) {
      instance_ = new IcebergCatalogUtil();
    }
    return instance_;
  }

  private final Configuration configuration_;

  private IcebergCatalogUtil() {
    configuration_ = new HiveConf(IcebergCatalogUtil.class);
    // We need to set ENGINE_HIVE_ENABLED in order to get Iceberg use the
    // appropriate SerDe and Input/Output format classes.
    configuration_.setBoolean(ConfigProperties.ENGINE_HIVE_ENABLED, true);
  }

  public TIcebergCatalog getUnderlyingCatalogType(String catalogName) {
    String catalogType = getCatalogProperty(
        catalogName, CatalogUtil.ICEBERG_CATALOG_TYPE);
    if (catalogType == null ||
        CatalogUtil.ICEBERG_CATALOG_TYPE_HIVE.equalsIgnoreCase(catalogType)) {
      return TIcebergCatalog.HIVE_CATALOG;
    }
    if (CatalogUtil.ICEBERG_CATALOG_TYPE_HADOOP.equalsIgnoreCase(catalogType)) {
      return TIcebergCatalog.HADOOP_CATALOG;
    }
    if (CATALOGS_LOCATION_PROPERTY.equalsIgnoreCase(catalogType)) {
      return TIcebergCatalog.HADOOP_TABLES;
    }
    return TIcebergCatalog.CATALOGS;
  }

  @Override
  public Table createTable(
      TableIdentifier identifier,
      Schema schema,
      PartitionSpec spec,
      String location,
      Map<String, String> tableProps) throws ImpalaRuntimeException {
    setContextClassLoader();
    String catName = tableProps.get(IcebergTable.ICEBERG_CATALOG);
    Preconditions.checkState(catName != null);
    String catalogType = getCatalogProperty(catName, CatalogUtil.ICEBERG_CATALOG_TYPE);
    if (catalogType == null) {
      throw new ImpalaRuntimeException(
          String.format("Unknown catalog name: %s", catName));
    }

    Catalog catalog = loadCatalog(catName);

    if (identifier != null && location != null) {
      location = null;
    }
    // Set default purge property
    Map<String, String> properties = new HashMap<>(tableProps);
    properties.put(TBL_PROP_EXTERNAL_TABLE_PURGE, tableProps.getOrDefault(
        TBL_PROP_EXTERNAL_TABLE_PURGE, TBL_PROP_EXTERNAL_TABLE_PURGE_DEFAULT));
    // Remove controlling property - IcebergTable.ICEBERG_CATALOG
    properties.remove(IcebergTable.ICEBERG_CATALOG);

    return catalog.createTable(identifier, schema, spec, location, properties);
  }

  @Override
  public Table loadTable(FeIcebergTable feTable) throws TableLoadingException {
    setContextClassLoader();
    TableIdentifier tableId = IcebergUtil.getIcebergTableIdentifier(feTable);
    return loadTable(tableId, feTable.getLocation(),
        feTable.getMetaStoreTable().getParameters());
  }

  @Override
  public Table loadTable(TableIdentifier tableId, String tableLocation,
      Map<String, String> tableProps) throws IcebergTableLoadingException {
    setContextClassLoader();
    String catName = tableProps.get(IcebergTable.ICEBERG_CATALOG);
    Preconditions.checkState(catName != null);

    Catalog catalog = loadCatalog(catName);

    return catalog.loadTable(tableId);
  }

  @Override
  public boolean dropTable(FeIcebergTable feTable, boolean purge) {
    setContextClassLoader();
    if (!purge) return true;

    TableIdentifier tableId = IcebergUtil.getIcebergTableIdentifier(feTable);
    String catName = feTable.getMetaStoreTable().getParameters()
        .get(IcebergTable.ICEBERG_CATALOG);
    Preconditions.checkState(catName != null);

    Catalog catalog = loadCatalog(catName);

    return catalog.dropTable(tableId, purge);
  }

  @Override
  public boolean dropTable(String dbName, String tblName, boolean purge) {
    throw new UnsupportedOperationException(
        "Cannot drop table by name without catalog information");
  }

  @Override
  public void renameTable(FeIcebergTable feTable, TableIdentifier newTableId) {
    setContextClassLoader();
    TableIdentifier oldTableId = IcebergUtil.getIcebergTableIdentifier(feTable);
    String catName = feTable.getMetaStoreTable().getParameters()
        .get(IcebergTable.ICEBERG_CATALOG);
    Preconditions.checkState(catName != null);

    Catalog catalog = loadCatalog(catName);

    catalog.renameTable(oldTableId, newTableId);
  }

  private Catalog loadCatalog(String catalogName) {
    // Build catalog properties from Hadoop configuration
    String keyPrefix = CATALOG_CONFIG_PREFIX + catalogName;
    Map<String, String> catalogProps =
        Streams.stream(configuration_.iterator())
            .filter(e -> e.getKey().startsWith(keyPrefix))
            .collect(Collectors.toMap(
                e -> e.getKey().substring(keyPrefix.length() + 1), Map.Entry::getValue));

    return CatalogUtil.buildIcebergCatalog(catalogName, catalogProps, configuration_);
  }

  /**
   * Returns the value of 'catalogPropertyKey' for the given catalog.
   */
  public String getCatalogProperty(String catalogName, String catalogPropertyKey) {
    String propKey = String.format("%s%s.%s", CATALOG_CONFIG_PREFIX,
        catalogName, catalogPropertyKey);
    return configuration_.get(propKey);
  }
}
