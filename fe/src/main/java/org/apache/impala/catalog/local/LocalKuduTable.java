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
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.impala.analysis.ColumnDef;
import org.apache.impala.analysis.KuduPartitionParam;
import org.apache.impala.catalog.Column;
import org.apache.impala.catalog.FeCatalogUtils;
import org.apache.impala.catalog.FeKuduTable;
import org.apache.impala.catalog.KuduColumn;
import org.apache.impala.catalog.KuduTable;
import org.apache.impala.catalog.TableLoadingException;
import org.apache.impala.catalog.local.MetaProvider.TableMetaRef;
import org.apache.impala.common.ImpalaRuntimeException;
import org.apache.impala.thrift.TKuduTable;
import org.apache.impala.thrift.TTableDescriptor;
import org.apache.impala.thrift.TTableType;
import org.apache.impala.util.KuduUtil;
import org.apache.kudu.ColumnSchema;
import org.apache.kudu.Schema;
import org.apache.kudu.client.KuduClient;
import org.apache.kudu.client.KuduException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.errorprone.annotations.Immutable;

public class LocalKuduTable extends LocalTable implements FeKuduTable {
  private final TableParams tableParams_;
  private final boolean isPrimaryKeyUnique_;
  private final boolean hasAutoIncrementingColumn_;
  private final ImmutableList<String> primaryKeyColumnNames_;
  private final List<KuduPartitionParam> partitionBy_;

  private final org.apache.kudu.client.KuduTable kuduTable_;

  /**
   * Create a new instance based on the table metadata 'msTable' stored
   * in the metastore.
   */
  static LocalTable loadFromKudu(LocalDb db, Table msTable, TableMetaRef ref)
      throws TableLoadingException {
    Preconditions.checkNotNull(db);
    Preconditions.checkNotNull(msTable);
    String fullTableName = msTable.getDbName() + "." + msTable.getTableName();

    TableParams params = new TableParams(msTable);
    org.apache.kudu.client.KuduTable kuduTable = params.openTable();
    List<Column> cols = new ArrayList<>();
    List<FieldSchema> fieldSchemas = new ArrayList<>();
    convertColsFromKudu(kuduTable.getSchema(), cols, fieldSchemas);

    // TODO(todd): update the schema in HMS if it doesn't match? This will
    // no longer be necessary after the Kudu-HMS integration is complete, so
    // maybe not worth implementing here for the LocalCatalog implementation.

    // Use the schema derived from Kudu, rather than the one stored in the HMS.
    msTable.getSd().setCols(fieldSchemas);

    boolean isPrimaryKeyUnique = kuduTable.getSchema().isPrimaryKeyUnique();
    boolean hasAutoIncrementingColumn =
        kuduTable.getSchema().hasAutoIncrementingColumn();
    List<String> pkNames = new ArrayList<>();
    for (ColumnSchema c: kuduTable.getSchema().getPrimaryKeyColumns()) {
      pkNames.add(c.getName().toLowerCase());
    }

    List<KuduPartitionParam> partitionBy = Utils.loadPartitionByParams(kuduTable);

    ColumnMap cmap = new ColumnMap(cols, /*numClusteringCols=*/0, fullTableName,
        /*isFullAcidSchema=*/false);
    return new LocalKuduTable(db, msTable, ref, cmap, kuduTable, isPrimaryKeyUnique,
        pkNames, hasAutoIncrementingColumn, partitionBy);
  }

  public static FeKuduTable createCtasTarget(LocalDb db, Table msTable,
      List<ColumnDef> columnDefs, boolean isPrimaryKeyUnique,
      List<ColumnDef> primaryKeyColumnDefs,
      List<KuduPartitionParam> kuduPartitionParams) throws ImpalaRuntimeException {
    String fullTableName = msTable.getDbName() + "." + msTable.getTableName();

    boolean hasAutoIncrementingColumn = false;
    List<Column> columns = new ArrayList<>();
    List<String> pkNames = new ArrayList<>();
    int pos = 0;
    for (ColumnDef colDef: columnDefs) {
      columns.add(KuduColumn.fromThrift(colDef.toThrift(), pos++));
      // Simulate Kudu engine to add auto-incrementing column as the key column in the
      // temporary KuduTable if the primary key is not unique so that analysis module
      // could find the right position for each column.
      if (!isPrimaryKeyUnique && pos == primaryKeyColumnDefs.size()) {
        columns.add(KuduColumn.createAutoIncrementingColumn(pos++));
        hasAutoIncrementingColumn = true;
      }
    }
    for (ColumnDef pkColDef: primaryKeyColumnDefs) {
      pkNames.add(pkColDef.getColName());
    }
    if (!isPrimaryKeyUnique) {
      // Add auto-incrementing column's name to the list of key column's name
      pkNames.add(Schema.getAutoIncrementingColumnName());
    }

    ColumnMap cmap = new ColumnMap(columns, /*numClusteringCols=*/0, fullTableName,
        /*isFullAcidSchema=*/false);

    return new LocalKuduTable(db, msTable, /*ref=*/null, cmap, /*kuduTable*/null,
        isPrimaryKeyUnique, pkNames, hasAutoIncrementingColumn, kuduPartitionParams);
  }

  private static void convertColsFromKudu(Schema schema, List<Column> cols,
      List<FieldSchema> fieldSchemas) {
    Preconditions.checkArgument(cols.isEmpty());;
    Preconditions.checkArgument(fieldSchemas.isEmpty());;

    int pos = 0;
    for (ColumnSchema colSchema: schema.getColumns()) {
      KuduColumn kuduCol;
      try {
        kuduCol = KuduColumn.fromColumnSchema(colSchema, pos++);
      } catch (ImpalaRuntimeException e) {
        throw new LocalCatalogException(e);
      }
      Preconditions.checkNotNull(kuduCol);
      // Add the HMS column
      fieldSchemas.add(new FieldSchema(kuduCol.getName(),
          kuduCol.getType().toSql().toLowerCase(), /*comment=*/null));
      cols.add(kuduCol);
    }
  }

  private LocalKuduTable(LocalDb db, Table msTable, TableMetaRef ref, ColumnMap cmap,
      org.apache.kudu.client.KuduTable kuduTable, boolean isPrimaryKeyUnique,
      List<String> primaryKeyColumnNames, boolean hasAutoIncrementingColumn,
      List<KuduPartitionParam> partitionBy)  {
    super(db, msTable, ref, cmap);
    kuduTable_ = kuduTable;
    tableParams_ = new TableParams(msTable);
    partitionBy_ = ImmutableList.copyOf(partitionBy);
    isPrimaryKeyUnique_ = isPrimaryKeyUnique;
    primaryKeyColumnNames_ = ImmutableList.copyOf(primaryKeyColumnNames);
    hasAutoIncrementingColumn_ = hasAutoIncrementingColumn;
  }

  @Override
  public String getKuduMasterHosts() {
    return tableParams_.masters_;
  }

  @Override
  public String getKuduTableName() {
    return tableParams_.kuduTableName_;
  }

  /**
   * Return the Kudu table backing this table.
   */
  public org.apache.kudu.client.KuduTable getKuduTable() {
    return kuduTable_;
  }

  @Override
  public boolean isPrimaryKeyUnique() {
    return isPrimaryKeyUnique_;
  }

  @Override
  public boolean hasAutoIncrementingColumn() {
    return hasAutoIncrementingColumn_;
  }

  @Override
  public List<String> getPrimaryKeyColumnNames() {
    return primaryKeyColumnNames_;
  }

  @Override
  public List<KuduPartitionParam> getPartitionBy() {
    return partitionBy_;
  }

  @Override
  public TTableDescriptor toThriftDescriptor(int tableId,
      Set<Long> referencedPartitions) {
    // TODO(todd): the old implementation passes kuduTableName_ instead of name below.
    TTableDescriptor desc = new TTableDescriptor(tableId, TTableType.KUDU_TABLE,
        FeCatalogUtils.getTColumnDescriptors(this),
        getNumClusteringCols(),
        name_, db_.getName());
    desc.setKuduTable(toTKuduTable());
    return desc;
  }

  public TKuduTable toTKuduTable() {
    TKuduTable tbl = new TKuduTable();
    tbl.setIs_primary_key_unique(isPrimaryKeyUnique_);
    tbl.setHas_auto_incrementing(hasAutoIncrementingColumn_);
    tbl.setKey_columns(Preconditions.checkNotNull(primaryKeyColumnNames_));
    tbl.setMaster_addresses(tableParams_.getMastersAsList());
    tbl.setTable_name(tableParams_.kuduTableName_);
    Preconditions.checkNotNull(partitionBy_);
    // IMPALA-5154: partitionBy_ may be empty if Kudu table created outside Impala,
    // partition_by must be explicitly created because the field is required.
    tbl.partition_by = new ArrayList<>();
    for (KuduPartitionParam partitionParam: partitionBy_) {
      tbl.addToPartition_by(partitionParam.toThrift());
    }
    return tbl;
  }

  /**
   * Parsed parameters from the HMS indicating the cluster and table name for
   * a Kudu table.
   */
  @Immutable
  private static class TableParams {
    private final String kuduTableName_;
    private final String masters_;

    TableParams(Table msTable) {
      String fullTableName = msTable.getDbName() + "." + msTable.getTableName();
      Map<String, String> params = msTable.getParameters();
      kuduTableName_ = params.get(KuduTable.KEY_TABLE_NAME);
      if (kuduTableName_ == null) {
        throw new LocalCatalogException("No " + KuduTable.KEY_TABLE_NAME +
            " property found for table " + fullTableName);
      }
      masters_ = params.get(KuduTable.KEY_MASTER_HOSTS);
      if (masters_ == null) {
        throw new LocalCatalogException("No " + KuduTable.KEY_MASTER_HOSTS +
            " property found for table " + fullTableName);
      }
    }

    public List<String> getMastersAsList() {
      return Lists.newArrayList(masters_.split(","));
    }

    public org.apache.kudu.client.KuduTable openTable() throws TableLoadingException {
      KuduClient kuduClient = KuduUtil.getKuduClient(masters_);
      org.apache.kudu.client.KuduTable kuduTable;
      try {
        kuduTable = kuduClient.openTable(kuduTableName_);
      } catch (KuduException e) {
        throw new TableLoadingException(
          String.format("Error opening Kudu table '%s', Kudu error: %s",
              kuduTableName_, e.getMessage()));
      }
      return kuduTable;
    }
  }
}
