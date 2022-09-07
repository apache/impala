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

import java.io.UncheckedIOException;
import java.lang.NullPointerException;
import java.util.Map;

import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.exceptions.NoSuchTableException;
import org.apache.iceberg.hadoop.HadoopCatalog;
import org.apache.impala.catalog.FeIcebergTable;
import org.apache.impala.catalog.IcebergTableLoadingException;
import org.apache.impala.catalog.TableLoadingException;
import org.apache.impala.common.FileSystemUtil;
import org.apache.impala.thrift.TIcebergCatalog;
import org.apache.impala.util.IcebergUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;

/**
 * Implementation of IcebergCatalog for tables stored in HadoopCatalog.
 */
public class IcebergHadoopCatalog implements IcebergCatalog {
  private final static Logger LOG = LoggerFactory.getLogger(IcebergHadoopTables.class);

  private HadoopCatalog hadoopCatalog;

  public IcebergHadoopCatalog(String catalogLocation) {
    setContextClassLoader();
    hadoopCatalog = new HadoopCatalog();
    Map<String, String> props = IcebergUtil.composeCatalogProperties();
    props.put(CatalogProperties.WAREHOUSE_LOCATION, catalogLocation);
    hadoopCatalog.setConf(FileSystemUtil.getConfiguration());
    hadoopCatalog.initialize("", props);
  }

  @Override
  public Table createTable(
      TableIdentifier identifier,
      Schema schema,
      PartitionSpec spec,
      String location,
      Map<String, String> properties) {
    // We pass null as 'location' to let Iceberg decide the table location.
    return hadoopCatalog.createTable(identifier, schema, spec, /*location=*/null,
        properties);
  }

  @Override
  public Table loadTable(FeIcebergTable feTable) throws TableLoadingException {
    Preconditions.checkState(
      feTable.getIcebergCatalog() == TIcebergCatalog.HADOOP_CATALOG);
    TableIdentifier tableId = IcebergUtil.getIcebergTableIdentifier(feTable);
    return loadTable(tableId, null, null);
  }

  @Override
  public Table loadTable(TableIdentifier tableId, String tableLocation,
      Map<String, String> properties) throws IcebergTableLoadingException {
    Preconditions.checkState(tableId != null);
    final int MAX_ATTEMPTS = 5;
    final int SLEEP_MS = 500;
    int attempt = 0;
    while (attempt < MAX_ATTEMPTS) {
      try {
        return hadoopCatalog.loadTable(tableId);
      } catch (NoSuchTableException e) {
        throw new IcebergTableLoadingException(e.getMessage());
      } catch (NullPointerException | UncheckedIOException e) {
        if (attempt == MAX_ATTEMPTS - 1) {
          // Throw exception on last attempt.
          throw new IcebergTableLoadingException(String.format(
              "Could not load Iceberg table %s", tableId), (Exception)e);
        }
        LOG.warn("Caught Exception during Iceberg table loading: {}: {}", tableId, e);
      }
      ++attempt;
      try {
        Thread.sleep(SLEEP_MS);
      } catch (InterruptedException e) {
        // Ignored.
      }
    }
    // We shouldn't really get there, but to make the compiler happy:
    throw new IcebergTableLoadingException(
        String.format("Failed to load Iceberg table with id: %s", tableId));
  }

  @Override
  public boolean dropTable(FeIcebergTable feTable, boolean purge) {
    Preconditions.checkState(
      feTable.getIcebergCatalog() == TIcebergCatalog.HADOOP_CATALOG);
    TableIdentifier tableId = IcebergUtil.getIcebergTableIdentifier(feTable);
    return hadoopCatalog.dropTable(tableId, purge);
  }

  @Override
  public boolean dropTable(String dbName, String tblName, boolean purge) {
    return hadoopCatalog.dropTable(TableIdentifier.of(dbName, tblName), purge);
  }

  @Override
  public void renameTable(FeIcebergTable feTable, TableIdentifier newTableId) {
    TableIdentifier oldTableId = IcebergUtil.getIcebergTableIdentifier(feTable);
    try {
      hadoopCatalog.renameTable(oldTableId, newTableId);
    } catch (UnsupportedOperationException e) {
      throw new UnsupportedOperationException(
          "Cannot rename Iceberg tables that use 'hadoop.catalog' as catalog.");
    }
  }
}
