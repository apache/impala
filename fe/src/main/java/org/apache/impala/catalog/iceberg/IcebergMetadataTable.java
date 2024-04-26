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
import org.apache.impala.analysis.Analyzer;
import org.apache.impala.analysis.TableName;
import org.apache.impala.catalog.CatalogObject.ThriftObjectType;
import org.apache.impala.catalog.Column;
import org.apache.impala.catalog.FeCatalogUtils;
import org.apache.impala.catalog.FeIcebergTable;
import org.apache.impala.catalog.FeTable;
import org.apache.impala.catalog.IcebergColumn;
import org.apache.impala.catalog.VirtualTable;
import org.apache.impala.common.ImpalaRuntimeException;
import org.apache.impala.thrift.TColumnDescriptor;
import org.apache.impala.thrift.TTableDescriptor;
import org.apache.impala.thrift.TTableStats;
import org.apache.impala.thrift.TTableType;
import org.apache.impala.util.IcebergSchemaConverter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;

/**
 * Iceberg metadata tables are predefined tables by Iceberg library. IcebergMetadataTable
 * is the Impala representation of these tables so this data can be queried. The schema of
 * the Iceberg metadata table is available through the Iceberg API. This class creates a
 * table object based on the Iceberg API.
 */
public class IcebergMetadataTable extends VirtualTable {
  private final static Logger LOG = LoggerFactory.getLogger(IcebergMetadataTable.class);

  // The Iceberg table that is the base of the metadata table.
  private FeIcebergTable baseTable_;

  // Name of the metadata table.
  private String metadataTableName_;

  public IcebergMetadataTable(FeIcebergTable baseTable, String metadataTableTypeStr)
      throws ImpalaRuntimeException {
    super(null, baseTable.getDb(), baseTable.getName(), baseTable.getOwnerUser());
    baseTable_ = baseTable;
    metadataTableName_ = metadataTableTypeStr.toUpperCase();
    MetadataTableType type = MetadataTableType.from(metadataTableTypeStr.toUpperCase());
    Preconditions.checkNotNull(type);
    Table metadataTable = MetadataTableUtils.createMetadataTableInstance(
        baseTable_.getIcebergApiTable(), type);
    Schema metadataTableSchema = metadataTable.schema();
    for (Column col : IcebergSchemaConverter.convertToImpalaSchema(
        metadataTableSchema)) {
      LOG.trace("Adding column: \"{}\" with type: \"{}\" to metadata table.",
          col.getName(), col.getType());
      addColumn(IcebergColumn.cloneWithNullability(
          (IcebergColumn)col, true /*isNullable*/));
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

  public String getMetadataTableName() {
    return metadataTableName_;
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
    TTableDescriptor desc = new TTableDescriptor(tableId, TTableType.ICEBERG_TABLE,
        getTColumnDescriptors(), numClusteringCols_, name_, db_.getName());
    desc.setIcebergTable(FeIcebergTable.Utils.getTIcebergTable(baseTable_,
        ThriftObjectType.DESCRIPTOR_ONLY));
    return desc;
  }

  private List<TColumnDescriptor> getTColumnDescriptors() {
    return FeCatalogUtils.getTColumnDescriptors(this);
  }

  /**
   * Returns true if the table ref is referring to a valid metadata table.
   */
  public static boolean isIcebergMetadataTable(List<String> tblRefPath,
      Analyzer analyzer) {
    if (!canBeIcebergMetadataTable(tblRefPath)) return false;

    TableName virtualTableName = new TableName(tblRefPath.get(0),
        tblRefPath.get(1), tblRefPath.get(2));
    // The catalog table (the base of the virtual table) has been loaded and cached
    // under the name of the virtual table.
    FeTable catalogTable = analyzer.getStmtTableCache().tables.get(virtualTableName);
    // If the metadata table has already been analyzed in the query, the table cache will
    // return the virtual table, not the base table.
    return catalogTable instanceof FeIcebergTable ||
        catalogTable instanceof IcebergMetadataTable;
  }

  /**
   * Returns true if the path could refer to an Iceberg metadata table in a syntactically
   * correct way (also checking that the name of the metadata table is valid). Does not
   * check whether the base table is an Iceberg table, so the path is not guaranteed to
   * actually refer to a valid Iceberg metadata table.
   *
   * This function can be called before analysis is done, when isIcebergMetadataTable()
   * cannot be called.
   */
  public static boolean canBeIcebergMetadataTable(List<String> tblRefPath) {
    if (tblRefPath == null) return false;
    if (tblRefPath.size() < 3) return false;
    String vTableName = tblRefPath.get(2).toUpperCase();
    return EnumUtils.isValidEnum(MetadataTableType.class, vTableName);
  }
}
