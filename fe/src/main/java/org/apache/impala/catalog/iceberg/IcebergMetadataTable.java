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
import java.util.Set;

import org.apache.commons.lang3.EnumUtils;
import org.apache.iceberg.MetadataTableType;
import org.apache.iceberg.MetadataTableUtils;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.impala.analysis.TableName;
import org.apache.impala.catalog.Column;
import org.apache.impala.catalog.FeCatalogUtils;
import org.apache.impala.catalog.FeIcebergTable;
import org.apache.impala.catalog.FeTable;
import org.apache.impala.catalog.VirtualTable;
import org.apache.impala.common.ImpalaRuntimeException;
import org.apache.impala.thrift.TTableDescriptor;
import org.apache.impala.thrift.TTableStats;
import org.apache.impala.util.IcebergSchemaConverter;

import com.google.common.base.Preconditions;

/**
 * Iceberg metadata tables are predefined tables by Iceberg library. IcebergMetadataTable
 * is the Impala representation of these tables so this data can be queried. The schema of
 * the Iceberg metadata table is available through the Iceberg API. This class creates a
 * table object based on the Iceberg API.
 */
public class IcebergMetadataTable extends VirtualTable {
  private FeIcebergTable baseTable_;
  private String metadataTableName_;

  public IcebergMetadataTable(FeTable baseTable, String metadataTableTypeStr)
      throws ImpalaRuntimeException {
    super(null, baseTable.getDb(), baseTable.getName(), baseTable.getOwnerUser());
    Preconditions.checkArgument(baseTable instanceof FeIcebergTable);
    baseTable_ = (FeIcebergTable) baseTable;
    metadataTableName_ = metadataTableTypeStr;
    MetadataTableType type = MetadataTableType.from(metadataTableTypeStr.toUpperCase());
    Preconditions.checkNotNull(type);
    Table metadataTable = MetadataTableUtils.createMetadataTableInstance(
        baseTable_.getIcebergApiTable(), type);
    Schema metadataTableSchema = metadataTable.schema();
    for (Column col : IcebergSchemaConverter.convertToImpalaSchema(
        metadataTableSchema)) {
      addColumn(col);
    }
  }

  @Override
  public long getNumRows() {
    return -1;
  }

  public FeIcebergTable getBaseTable() {
    return baseTable_;
  }

  @Override
  public String getFullName() {
    return super.getFullName() + "." + metadataTableName_;
  }

  @Override
  public TableName getTableName() {
    return new TableName(db_.getName(), name_, metadataTableName_);
  }

  @Override
  public TTableStats getTTableStats() {
    long totalBytes = 0;
    TTableStats ret = new TTableStats(getNumRows());
    ret.setTotal_file_bytes(totalBytes);
    return ret;
  }

  @Override
  public org.apache.hadoop.hive.metastore.api.Table getMetaStoreTable() {
    return baseTable_.getMetaStoreTable();
  }

  /**
   * Return same descriptor as the base table, but with a schema that corresponds to
   * the metadata table schema.
   */
  @Override
  public TTableDescriptor toThriftDescriptor(int tableId,
      Set<Long> referencedPartitions) {
    TTableDescriptor desc = baseTable_.toThriftDescriptor(tableId, referencedPartitions);
    desc.setColumnDescriptors(FeCatalogUtils.getTColumnDescriptors(this));
    return desc;
  }

  /**
   * Returns true if the table ref is referring to a valid metadata table.
   */
  public static boolean isIcebergMetadataTable(List<String> tblRefPath) {
    if (tblRefPath == null) return false;
    if (tblRefPath.size() != 3) return false;
    String vTableName = tblRefPath.get(2).toUpperCase();
    return EnumUtils.isValidEnum(MetadataTableType.class, vTableName);
  }
}
