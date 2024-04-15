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

package org.apache.impala.catalog;

import java.util.Set;

import org.apache.commons.lang3.BooleanUtils;
import org.apache.hadoop.hive.metastore.IMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.impala.common.InternalException;
import org.apache.impala.service.FeSupport;
import org.apache.impala.thrift.CatalogObjectsConstants;
import org.apache.impala.thrift.TAccessLevel;
import org.apache.impala.thrift.TCatalogObjectType;
import org.apache.impala.thrift.TColumn;
import org.apache.impala.thrift.TResultSet;
import org.apache.impala.thrift.TResultSetMetadata;
import org.apache.impala.thrift.TSystemTable;
import org.apache.impala.thrift.TSystemTableName;
import org.apache.impala.thrift.TTable;
import org.apache.impala.thrift.TTableDescriptor;
import org.apache.impala.thrift.TTableType;
import org.apache.impala.util.EventSequence;
import org.apache.impala.util.TResultRowBuilder;
import com.google.common.base.Preconditions;

/**
 * Represents a system table reflecting backend internal state.
 */
public final class SystemTable extends Table implements FeSystemTable {
  protected SystemTable(org.apache.hadoop.hive.metastore.api.Table msTable, Db db,
      String name, String owner) {
    super(msTable, db, name, owner);
    // System Tables are read-only.
    accessLevel_ = TAccessLevel.READ_ONLY;
  }

  @Override // FeSystemTable
  public TSystemTableName getSystemTableName() {
    return TSystemTableName.valueOf(getName().toUpperCase());
  }

  public static boolean isSystemTable(org.apache.hadoop.hive.metastore.api.Table msTbl) {
    String value = msTbl.getParameters().get(
      CatalogObjectsConstants.TBL_PROP_SYSTEM_TABLE);
    return value != null && BooleanUtils.toBoolean(value);
  }

  @Override
  public TTableDescriptor toThriftDescriptor(int tableId,
      Set<Long> referencedPartitions) {
    // Create thrift descriptors to send to the BE.
    TTableDescriptor tableDescriptor = new TTableDescriptor(tableId,
        TTableType.SYSTEM_TABLE, getTColumnDescriptors(),
        getNumClusteringCols(), getName(), getDb().getName());
    tableDescriptor.setSystemTable(getTSystemTable());
    return tableDescriptor;
  }

  @Override
  public long getNumRows() {
    try {
      // Return an estimate of the number of live queries assuming balanced load across
      // coordinators.
      return FeSupport.NumLiveQueries() * FeSupport.GetCoordinators().getAddressesSize();
    } catch (InternalException e) {
      return super.getNumRows();
    }
  }

  /**
   * Returns a thrift structure for the system table.
   */
  private TSystemTable getTSystemTable() {
    return new TSystemTable(getSystemTableName());
  }

  @Override
  public TCatalogObjectType getCatalogObjectType() {
    return TCatalogObjectType.TABLE;
  }

  @Override
  public void load(boolean reuseMetadata, IMetaStoreClient client,
      org.apache.hadoop.hive.metastore.api.Table msTbl, String reason,
      EventSequence catalogTimeline) throws TableLoadingException {
    int pos = colsByPos_.size();
    // Should be no partition columns.
    Preconditions.checkState(pos == 0);
    for (FieldSchema s: msTbl.getSd().getCols()) {
      Type type = FeCatalogUtils.parseColumnType(s, getName());
      addColumn(new Column(s.getName(), type, s.getComment(), pos++));
    }
  }

  /**
   * Returns a thrift structure representing the table.
   */
  @Override
  public TTable toThrift() {
    TTable table = super.toThrift();
    table.setTable_type(TTableType.SYSTEM_TABLE);
    table.setSystem_table(getTSystemTable());
    return table;
  }

  /**
   * Returns statistics on this table as a tabular result set. Used for the SHOW
   * TABLE STATS statement. The schema of the returned TResultSet is set inside
   * this method.
   */
  @Override // FeSystemTable
  public TResultSet getTableStats() {
    TResultSet result = new TResultSet();
    TResultSetMetadata resultSchema = new TResultSetMetadata();
    resultSchema.addToColumns(new TColumn("#Rows", Type.BIGINT.toThrift()));
    result.setSchema(resultSchema);
    TResultRowBuilder rowBuilder = new TResultRowBuilder();
    rowBuilder.add(getNumRows());
    result.addToRows(rowBuilder.get());
    return result;
  }
}
