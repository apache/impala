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

import java.util.Map;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.exceptions.NoSuchTableException;
import org.apache.iceberg.hadoop.HadoopCatalog;
import org.apache.impala.catalog.FeIcebergTable;
import org.apache.impala.catalog.TableLoadingException;
import org.apache.impala.common.FileSystemUtil;
import org.apache.impala.thrift.TIcebergCatalog;
import org.apache.impala.util.IcebergUtil;

import com.google.common.base.Preconditions;

/**
 * Implementation of IcebergCatalog for tables stored in HadoopCatalog.
 */
public class IcebergHadoopCatalog implements IcebergCatalog {
  private HadoopCatalog hadoopCatalog;

  public IcebergHadoopCatalog(String catalogLocation) {
    hadoopCatalog = new HadoopCatalog(FileSystemUtil.getConfiguration(), catalogLocation);
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
    return loadTable(tableId, null);
  }

  @Override
  public Table loadTable(TableIdentifier tableId, String tableLocation)
      throws TableLoadingException {
    Preconditions.checkState(tableId != null);
    try {
      return hadoopCatalog.loadTable(tableId);
    } catch (NoSuchTableException e) {
      throw new TableLoadingException(String.format(
          "Failed to load Iceberg table with id: %s", tableId), e);
    }
  }

  @Override
  public boolean dropTable(FeIcebergTable feTable, boolean purge) {
    Preconditions.checkState(
      feTable.getIcebergCatalog() == TIcebergCatalog.HADOOP_CATALOG);
    TableIdentifier tableId = IcebergUtil.getIcebergTableIdentifier(feTable);
    return hadoopCatalog.dropTable(tableId, purge);
  }
}
