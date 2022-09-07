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

import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.exceptions.NoSuchTableException;
import org.apache.iceberg.hadoop.HadoopTables;
import org.apache.impala.catalog.FeIcebergTable;
import org.apache.impala.catalog.IcebergTableLoadingException;
import org.apache.impala.catalog.TableLoadingException;
import org.apache.impala.common.FileSystemUtil;
import org.apache.impala.thrift.TIcebergCatalog;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;

/**
 * Implementation of IcebergCatalog for tables stored in HadoopTables.
 */
public class IcebergHadoopTables implements IcebergCatalog {
  private final static Logger LOG = LoggerFactory.getLogger(IcebergHadoopTables.class);

  private static IcebergHadoopTables instance_;

  public synchronized static IcebergHadoopTables getInstance() {
    if (instance_ == null) {
      instance_ = new IcebergHadoopTables();
    }
    return instance_;
  }

  private HadoopTables hadoopTables;

  private IcebergHadoopTables() {
    hadoopTables = new HadoopTables(FileSystemUtil.getConfiguration());
  }

  @Override
  public Table createTable(
      TableIdentifier identifier,
      Schema schema,
      PartitionSpec spec,
      String location,
      Map<String, String> properties) {
    return hadoopTables.create(schema, spec, properties, location);
  }

  @Override
  public Table loadTable(FeIcebergTable feTable) throws TableLoadingException {
    Preconditions.checkState(
        feTable.getIcebergCatalog() == TIcebergCatalog.HADOOP_TABLES);
    return loadTable(null, feTable.getLocation(), null);
  }

  @Override
  public Table loadTable(TableIdentifier tableId, String tableLocation,
      Map<String, String> properties) throws IcebergTableLoadingException {
    Preconditions.checkState(tableLocation != null);
    final int MAX_ATTEMPTS = 5;
    final int SLEEP_MS = 500;
    int attempt = 0;
    while (attempt < MAX_ATTEMPTS) {
      try {
        return hadoopTables.load(tableLocation);
      } catch (NoSuchTableException e) {
        throw new IcebergTableLoadingException(e.getMessage());
      } catch (NullPointerException | UncheckedIOException e) {
        if (attempt == MAX_ATTEMPTS - 1) {
          // Throw exception on last attempt.
          throw new IcebergTableLoadingException(String.format(
              "Could not load Iceberg table at location: %s", tableLocation),
              (Exception)e);
        }
        LOG.warn("Caught Exception during Iceberg table loading at location: {}: {}",
            tableLocation, e);
      }
      ++attempt;
      try {
        Thread.sleep(SLEEP_MS);
      } catch (InterruptedException e) {
        // Ignored.
      }
    }
    // We shouldn't really get there, but to make the compiler happy:
    throw new IcebergTableLoadingException(String.format(
        "Could not load Iceberg table at location: %s", tableLocation));
  }

  @Override
  public boolean dropTable(FeIcebergTable feTable, boolean purge) {
    Preconditions.checkState(
      feTable.getIcebergCatalog() == TIcebergCatalog.HADOOP_TABLES);
    if (purge) {
      // TODO: HadoopTables doesn't have dropTable() in the Iceberg version being used.
      // Un-comment below line when our Iceberg version is newer than 0.9.1 and has the
      // following commit:
      // https://github.com/apache/iceberg/commit/66a37c2793392e6ce9d5d2783b64488527f079fc
      //
      // return hadoopTables.dropTable(feTable.getLocation());
    }
    return true;
  }

  @Override
  public boolean dropTable(String dbName, String tblName, boolean purge) {
    throw new UnsupportedOperationException(
        "Hadoop Tables doesn't support dropping table by name");
  }

  @Override
  public void renameTable(FeIcebergTable feTable, TableIdentifier newTableId) {
    // HadoopTables no renameTable method in Iceberg
    throw new UnsupportedOperationException(
        "Cannot rename Iceberg tables that use 'hadoop.tables' as catalog.");
  }
}
