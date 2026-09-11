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

import com.google.common.collect.ImmutableList;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.rest.RESTCatalog;
import org.apache.impala.catalog.FeIcebergTable;
import org.apache.impala.catalog.IcebergTableLoadingException;
import org.apache.impala.catalog.TableLoadingException;
import org.apache.impala.util.IcebergUtil;

/**
 * Implementation of IcebergCatalog for tables stored in a REST catalog.
 */
public class IcebergRESTCatalog implements IcebergCatalog {
  private final String name_;
  private final String REST_URI;

  private final RESTCatalog restCatalog_;

  // Whether credential vending is enabled in the catalog configuration. Some REST
  // catalogs (e.g. Lakekeeper) return storage credentials even when the client does not
  // request access delegation; those are ignored when this is false.
  private final boolean vendedCredentialsEnabled_;

  public IcebergRESTCatalog(Properties properties) {
    setContextClassLoader();

    RESTCatalogProperties restConfig = new RESTCatalogProperties(properties);
    name_ = restConfig.getName();
    REST_URI = restConfig.getUri();
    vendedCredentialsEnabled_ = restConfig.isVendedCredentialsEnabled();
    restCatalog_ = new RESTCatalog();
    restCatalog_.setConf(new HiveConf(IcebergRESTCatalog.class));
    restCatalog_.initialize(
        name_,
        restConfig.getCatalogProperties());
  }

  public String getName() { return name_; }

  public String getUri() {
    return REST_URI;
  }

  /** True when 'iceberg.rest-catalog.vended-credentials-enabled' is set. */
  public boolean isVendedCredentialsEnabled() {
    return vendedCredentialsEnabled_;
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
    // 'tableLocation' and 'properties' are part of the IcebergCatalog interface but are
    // not meaningful for a REST catalog, which resolves everything from 'tableId'.
    return restCatalog_.loadTable(tableId);
  }

  @Override
  public boolean dropTable(FeIcebergTable feTable, boolean purge) {
    return dropTable(feTable.getDb().getName(), feTable.getName(), purge);
  }

  /**
   * Drops the table through the REST catalog. With 'purge' the server is asked to
   * remove the table data as well. Returns false if the table did not exist.
   */
  @Override
  public boolean dropTable(String dbName, String tblName, boolean purge) {
    return restCatalog_.dropTable(TableIdentifier.of(Namespace.of(dbName), tblName),
        purge);
  }

  @Override
  public void renameTable(FeIcebergTable feTable, TableIdentifier newTableId) {
    throw new UnsupportedOperationException(
        "RENAME TABLE is not implemented for REST catalog");
  }
}
