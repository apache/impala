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

import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;

import com.google.common.collect.ImmutableList;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.SessionCatalog;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.rest.HTTPClient;
import org.apache.iceberg.rest.RESTCatalog;
import org.apache.impala.catalog.FeIcebergTable;
import org.apache.impala.catalog.IcebergTableLoadingException;
import org.apache.impala.catalog.TableLoadingException;
import org.apache.impala.util.IcebergUtil;

import com.google.common.collect.ImmutableMap;

/**
 * Implementation of IcebergCatalog for tables stored in HadoopCatalog.
 */
public class IcebergRESTCatalog implements IcebergCatalog {
  private static final String KEY_URI = "iceberg.rest-catalog.uri";
  private static final String KEY_NAME = "iceberg.rest-catalog.name";
  private static final String KEY_CLIENT_ID = "iceberg.rest-catalog.client-id";
  private static final String KEY_CLIENT_SECRET = "iceberg.rest-catalog.client-secret";
  private static final String KEY_WAREHOUSE = "iceberg.rest-catalog.warehouse";

  private final String REST_URI;

  private static IcebergRESTCatalog instance_;
  private final RESTCatalog restCatalog_;

  public synchronized static IcebergRESTCatalog getInstance(
      Properties properties) {
    if (instance_ == null) {
      instance_ = new IcebergRESTCatalog(properties);
    }
    return instance_;
  }

  private IcebergRESTCatalog(Properties properties) {
    setContextClassLoader();

    REST_URI = getRequiredProperty(properties, KEY_URI);
    final String CATALOG_NAME = properties.getProperty(KEY_NAME, "");
    final String CLIENT_ID = properties.getProperty(KEY_CLIENT_ID, "impala");
    final String CLIENT_SECRET = properties.getProperty(KEY_CLIENT_SECRET, "");
    final String CLIENT_CREDS = CLIENT_ID + ":" + CLIENT_SECRET;
    final String WAREHOUSE_LOCATION = properties.getProperty(KEY_WAREHOUSE, "");

    SessionCatalog.SessionContext context =
        new SessionCatalog.SessionContext(
            UUID.randomUUID().toString(),
            "user",
            ImmutableMap.of("credential", CLIENT_CREDS),
            ImmutableMap.of());

    restCatalog_ = new RESTCatalog(context,
        (config) -> HTTPClient.builder(config).uri(REST_URI).build());
    HiveConf conf = new HiveConf(IcebergRESTCatalog.class);
    restCatalog_.setConf(conf);
    restCatalog_.initialize(
        CATALOG_NAME,
        ImmutableMap.of(
            CatalogProperties.URI, REST_URI,
            "credential", CLIENT_CREDS,
            CatalogProperties.WAREHOUSE_LOCATION, WAREHOUSE_LOCATION)
    );
  }

  private String getRequiredProperty(Properties properties, String key) {
    String value = properties.getProperty(key);
    if (value == null) {
      throw new IllegalStateException(
          String.format("Missing property of IcebergRESTCatalog: %s", key));
    }
    return value;
  }

  public String getUri() {
    return REST_URI;
  }

  @Override
  public Table createTable(
      TableIdentifier identifier,
      Schema schema,
      PartitionSpec spec,
      String location,
      Map<String, String> properties) {
    throw new UnsupportedOperationException(
        "CREATE TABLE is not implemented for REST catalog");
  }

  public ImmutableList<String> listNamespaces() {
    ImmutableList.Builder<String> ret = ImmutableList.builder();
    for (Namespace ns : restCatalog_.listNamespaces()) {
      ret.add(ns.toString());
    }
    return ret.build();
  }

  public List<TableIdentifier> listTables(String namespace) {
    return restCatalog_.listTables(Namespace.of(namespace));
  }

  @Override
  public Table loadTable(FeIcebergTable feTable) throws TableLoadingException {
    TableIdentifier tableId = IcebergUtil.getIcebergTableIdentifier(feTable);
    return loadTable(tableId, null, null);
  }

  @Override
  public Table loadTable(TableIdentifier tableId, String tableLocation,
      Map<String, String> properties) throws IcebergTableLoadingException {
    return restCatalog_.loadTable(tableId);
  }

  @Override
  public boolean dropTable(FeIcebergTable feTable, boolean purge) {
    throw new UnsupportedOperationException(
        "DROP TABLE is not implemented for REST catalog");
  }

  @Override
  public boolean dropTable(String dbName, String tblName, boolean purge) {
    throw new UnsupportedOperationException(
        "DROP TABLE is not implemented for REST catalog");
  }

  @Override
  public void renameTable(FeIcebergTable feTable, TableIdentifier newTableId) {
    throw new UnsupportedOperationException(
        "RENAME TABLE is not implemented for REST catalog");
  }
}
