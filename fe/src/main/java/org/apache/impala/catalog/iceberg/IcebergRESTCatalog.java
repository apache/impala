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
 * Implementation of IcebergCatalog for tables stored in HadoopCatalog.
 */
public class IcebergRESTCatalog implements IcebergCatalog {
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

    RESTCatalogProperties restConfig = new RESTCatalogProperties(properties);
    REST_URI = restConfig.getUri();
    restCatalog_ = new RESTCatalog();
    HiveConf conf = new HiveConf(IcebergRESTCatalog.class);
    restCatalog_.setConf(conf);
    restCatalog_.initialize(
        restConfig.getName(),
        restConfig.getCatalogProperties());
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
