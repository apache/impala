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

package org.apache.impala.catalog.local;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.iceberg.Schema;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.types.Types;
import org.apache.impala.analysis.IcebergPartitionSpec;
import org.apache.impala.catalog.Column;
import org.apache.impala.catalog.FeCatalogUtils;
import org.apache.impala.catalog.FeIcebergTable;
import org.apache.impala.catalog.TableLoadingException;
import org.apache.impala.catalog.Type;
import org.apache.impala.thrift.TIcebergTable;
import org.apache.impala.thrift.TTableDescriptor;
import org.apache.impala.thrift.TTableType;
import org.apache.impala.util.IcebergUtil;

import com.google.common.base.Preconditions;
import com.google.errorprone.annotations.Immutable;

/**
 * Iceberg table for LocalCatalog
 */
public class LocalIcebergTable extends LocalTable implements FeIcebergTable {
  private final TableParams tableParams_;
  private final List<IcebergPartitionSpec> partitionSpecs_;

  static LocalTable loadFromIceberg(LocalDb db, Table msTable,
                                    MetaProvider.TableMetaRef ref)
      throws TableLoadingException {
    Preconditions.checkNotNull(db);
    Preconditions.checkNotNull(msTable);
    String fullTableName = msTable.getDbName() + "." + msTable.getTableName();
    TableParams params = new TableParams(msTable);

    List<Column> cols = new ArrayList<>();
    List<FieldSchema> fieldSchemas = new ArrayList<>();
    TableMetadata metadata =
        IcebergUtil.getIcebergTableMetadata(params.icebergTableName_);
    convertColsFromKIceberg(metadata.schema(), cols, fieldSchemas);

    msTable.getSd().setCols(fieldSchemas);

    List<IcebergPartitionSpec> partitionSpecs =
        Utils.loadPartitionSpecByIceberg(metadata);

    ColumnMap cmap = new ColumnMap(cols, /*numClusteringCols=*/0, fullTableName,
                                  false);
    return new LocalIcebergTable(db, msTable, ref, cmap, partitionSpecs);
  }

  private static void convertColsFromKIceberg(Schema schema, List<Column> cols,
                                              List<FieldSchema> fieldSchemas)
      throws TableLoadingException {
    List<Types.NestedField> columns = schema.columns();
    int pos = 0;
    for (Types.NestedField column : columns) {
      Preconditions.checkNotNull(column);
      Type colType = IcebergUtil.toImpalaType(column.type());
      fieldSchemas.add(new FieldSchema(column.name(), colType.toSql().toLowerCase(),
          column.doc()));
      cols.add(new Column(column.name(), colType, column.doc(), pos++));
    }
  }

  private LocalIcebergTable(LocalDb db, Table msTable, MetaProvider.TableMetaRef ref,
                            ColumnMap cmap, List<IcebergPartitionSpec> partitionSpecs) {
    super(db, msTable, ref, cmap);
    tableParams_ = new TableParams(msTable);
    partitionSpecs_ = partitionSpecs;
  }

  @Override
  public String getIcebergTableLocation() {
    return tableParams_.icebergTableName_;
  }

  @Override
  public List<IcebergPartitionSpec> getPartitionSpec() {
    return partitionSpecs_;
  }

  @Override
  public TTableDescriptor toThriftDescriptor(int tableId,
                                             Set<Long> referencedPartitions) {
    TTableDescriptor desc = new TTableDescriptor(tableId, TTableType.ICEBERG_TABLE,
        FeCatalogUtils.getTColumnDescriptors(this),
        getNumClusteringCols(),
        name_, db_.getName());

    TIcebergTable tbl = new TIcebergTable();
    tbl.setTable_location(tableParams_.icebergTableName_);
    tbl.partition_spec = new ArrayList<>();
    for (IcebergPartitionSpec spec : partitionSpecs_) {
      tbl.addToPartition_spec(spec.toThrift());
    }
    desc.setIcebergTable(tbl);
    return desc;
  }

  @Immutable
  private static class TableParams {
    private final String icebergTableName_;

    TableParams(Table msTable) {
      String fullTableName = msTable.getDbName() + "." + msTable.getTableName();
      if (msTable.getSd().isSetLocation()) {
        icebergTableName_ = msTable.getSd().getLocation();
      } else {
        throw new LocalCatalogException("Cannot find iceberg table name for table "
            + fullTableName);
      }
    }
  }
}
